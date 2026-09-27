"""Profile-local durable audit ledger for cron execution attempts.

The ledger records what is known about each attempt; it is not a retry queue.
Interrupted attempts become ``unknown`` only after their exact owner process is
proved gone. Terminal states are immutable.

On a PostgreSQL authority profile (levos 0060) the ledger is the profile's
``core_cron_executions`` table and the owner of another pod cannot be probed by
pid, so "proved gone" means its lease ran out: the owning process renews
``lease_expires_at`` on its open attempts while it lives.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import aux_db_path
from hermes_time import now as _hermes_now

# Optional test override. Production resolves the path at transaction time so
# dashboard operations that temporarily enter another profile cannot leak that
# profile's execution records into the import-time home.
EXECUTIONS_FILE: Optional[Path] = None
MAX_TERMINAL_EXECUTIONS = 1000
_TERMINAL_STATES = ("completed", "failed", "unknown")
_lock = threading.RLock()
# Owner id of the attempts this process creates (``process_id`` column).
_PROCESS_ID = uuid.uuid4().hex
logger = logging.getLogger(__name__)

# PostgreSQL authority lease (levos 0060). ``lease_expires_at`` is an epoch
# second on the PostgreSQL server clock, so clock skew between pods does not
# matter. A live owner renews every LEASE_RENEW_SECONDS; another process may
# mark an attempt unknown once its lease is LEASE_SECONDS stale (four missed
# renewals), which is also how long a killed pod's attempts stay "running".
LEASE_SECONDS = 120.0
LEASE_RENEW_SECONDS = 30.0
_SERVER_EPOCH = "EXTRACT(EPOCH FROM clock_timestamp())::float8"
_LEASE_EXPIRED = (
    f" AND (lease_expires_at IS NULL OR lease_expires_at < {_SERVER_EPOCH})"
)
_lease_stop = threading.Event()
_lease_thread: Optional[threading.Thread] = None
_lease_guard = threading.Lock()


def _connect() -> sqlite3.Connection:
    """Open the ledger on the backend the profile selects (levos 0060).

    PostgreSQL authority: table ``core_cron_executions`` in the profile's
    store, no file; a failure raises ``AuxStoreUnavailable``. Otherwise the
    SQLite file, opened and initialized exactly as before.
    """
    from hermes_aux_store import open_aux_store

    path = EXECUTIONS_FILE or aux_db_path("cron/executions.db").resolve()
    return open_aux_store(
        "cron_executions",
        sqlite_path=path,
        initialize=_initialize_schema,
        sqlite_options={"timeout": 5},
    )


def _initialize_schema(conn: sqlite3.Connection) -> None:
    if getattr(conn, "is_postgres", False):
        from hermes_aux_store import aux_schema_transaction

        with aux_schema_transaction(conn, "cron_executions"):
            _create_schema(conn)
            conn.execute(
                "ALTER TABLE executions ADD COLUMN IF NOT EXISTS lease_expires_at REAL"
            )
        return
    from hermes_state import apply_wal_with_fallback

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    apply_wal_with_fallback(conn, db_label="cron/executions.db")
    conn.execute("PRAGMA synchronous=FULL")
    _create_schema(conn)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS executions (
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL,
             source TEXT NOT NULL,
             process_id TEXT NOT NULL,
             pid INTEGER NOT NULL,
             process_started_at INTEGER,
             status TEXT NOT NULL CHECK(status IN
               ('claimed','running','completed','failed','unknown')),
             claimed_at TEXT NOT NULL,
             started_at TEXT,
             finished_at TEXT,
             error TEXT
           )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_job_claimed "
        "ON executions(job_id, claimed_at DESC, id DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_status_claimed "
        "ON executions(status, claimed_at DESC, id DESC)"
    )


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection, commit/rollback on exit, always close.

    ``sqlite3.Connection.__enter__``/``__exit__`` only commit or roll back
    the transaction; it does not close the connection. Relying on that alone
    leaks a connection (and its WAL/SHM file descriptors) on every call,
    since closing then depends on the garbage collector. Schema init runs
    inside ``_connect`` (``open_aux_store``), which closes the connection
    itself when a PRAGMA/DDL step fails after a successful ``connect()``.
    """
    with _lock:
        conn = _connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()


def _record(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None


def _emit_execution_state(
    record: Optional[Dict[str, Any]], *, delivery_outcome: Optional[str] = None
) -> None:
    """Project durable state to monitoring without affecting ledger behavior."""
    try:
        from agent.monitoring.cron_health import emit_execution_state

        emit_execution_state(record, delivery_outcome=delivery_outcome)
    except Exception:
        pass


def _process_start_time(pid: int) -> Optional[int]:
    try:
        from gateway.status import get_process_start_time
        return get_process_start_time(pid)
    except Exception:
        return None


def _owner_is_live(pid: int, started_at: Optional[int]) -> bool:
    try:
        from gateway.status import _pid_exists
        if not _pid_exists(pid):
            return False
    except Exception:
        return True  # fail safe: inability to prove death must not rewrite state
    if started_at is None:
        return pid == os.getpid()
    current = _process_start_time(pid)
    return current is not None and current == started_at


def _prune_unlocked(conn: sqlite3.Connection) -> None:
    limit = max(0, int(MAX_TERMINAL_EXECUTIONS))
    # "No limit" is LIMIT -1 on SQLite and LIMIT ALL on PostgreSQL.
    unbounded = "ALL" if getattr(conn, "is_postgres", False) else "-1"
    conn.execute(
        f"""DELETE FROM executions WHERE id IN (
             SELECT id FROM executions
             WHERE status IN ('completed','failed','unknown')
             ORDER BY claimed_at DESC, id DESC LIMIT {unbounded} OFFSET ?
           )""",
        (limit,),
    )


def _extend_leases(conn, execution_id: Optional[str] = None) -> int:
    """Push the lease of this process's open attempts LEASE_SECONDS ahead."""
    sql = (
        f"UPDATE executions SET lease_expires_at = {_SERVER_EPOCH} + ? "
        "WHERE process_id=? AND status IN ('claimed','running')"
    )
    params: List[Any] = [float(LEASE_SECONDS), _PROCESS_ID]
    if execution_id is not None:
        sql += " AND id=?"
        params.append(execution_id)
    return conn.execute(sql, params).rowcount


def renew_execution_leases() -> int:
    """Renew every open attempt this process owns; 0 off PostgreSQL authority."""
    from hermes_aux_store import aux_store_authority

    if not aux_store_authority():
        return 0
    with _transaction() as conn:
        return _extend_leases(conn)


def _renew_leases_until_stopped() -> None:
    while not _lease_stop.wait(LEASE_RENEW_SECONDS):
        try:
            renew_execution_leases()
        except Exception as exc:
            # Keep trying: a lease that runs out while PostgreSQL is away lets
            # another pod mark the attempt unknown, never run it twice.
            logger.warning("Cron execution lease renewal failed: %s", exc)


def _start_lease_renewer() -> None:
    global _lease_thread
    with _lease_guard:
        if _lease_thread is not None and _lease_thread.is_alive():
            return
        _lease_stop.clear()
        _lease_thread = threading.Thread(
            target=_renew_leases_until_stopped,
            name="cron-execution-lease",
            daemon=True,
        )
        _lease_thread.start()


def _stop_lease_renewer() -> None:
    """Stop the renewal thread (tests; a stopped owner's leases run out)."""
    with _lease_guard:
        thread = _lease_thread
        _lease_stop.set()
    if thread is not None:
        thread.join(timeout=10)


def create_execution(job_id: str, *, source: str) -> Dict[str, Any]:
    """Persist a claimed attempt before executor/provider dispatch."""
    now = _hermes_now().isoformat()
    execution_id = uuid.uuid4().hex
    pid = os.getpid()
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO executions
               (id, job_id, source, process_id, pid, process_started_at,
                status, claimed_at)
               VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?)""",
            (execution_id, str(job_id), str(source), _PROCESS_ID, pid,
             _process_start_time(pid), now),
        )
        leased = getattr(conn, "is_postgres", False)
        if leased:
            _extend_leases(conn, execution_id)
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone()
    if leased:
        _start_lease_renewer()
    record = _record(row)
    _emit_execution_state(record)
    return record  # type: ignore[return-value]


def mark_execution_running(execution_id: str) -> Optional[Dict[str, Any]]:
    """Transition one claimed attempt to running exactly once."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET status='running', started_at=?
               WHERE id=? AND status='claimed'""",
            (now, execution_id),
        )
        if cur.rowcount != 1:
            return None
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone())
    _emit_execution_state(record)
    return record


def finish_execution(
    execution_id: str, *, success: bool, error: Optional[str] = None,
    delivery_outcome: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Write a terminal result once; terminal attempts cannot be rewritten."""
    now = _hermes_now().isoformat()
    status = "completed" if success else "failed"
    detail = None if success else (str(error) if error else "unknown failure")
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET status=?, finished_at=?, error=?
               WHERE id=? AND status IN ('claimed','running')""",
            (status, now, detail, execution_id),
        )
        if cur.rowcount != 1:
            return None
        _prune_unlocked(conn)
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone())
    _emit_execution_state(record, delivery_outcome=delivery_outcome)
    return record


def recover_interrupted_executions() -> int:
    """Mark provably abandoned attempts unknown without scheduling retries.

    PostgreSQL authority: abandoned means another process's lease ran out
    (its owner may live in another pod, where no pid or /proc can see it).
    """
    now = _hermes_now().isoformat()
    changed = 0
    recovered: List[Dict[str, Any]] = []
    with _transaction() as conn:
        leased = getattr(conn, "is_postgres", False)
        if leased:
            rows = conn.execute(
                """SELECT id FROM executions
                   WHERE status IN ('claimed','running') AND process_id<>?"""
                + _LEASE_EXPIRED,
                (_PROCESS_ID,),
            ).fetchall()
            error = (
                "This execution's owner stopped renewing its lease before a durable "
                "terminal state; whether side effects ran is unknown."
            )
        else:
            rows = conn.execute(
                """SELECT id, process_id, pid, process_started_at FROM executions
                   WHERE status IN ('claimed','running')"""
            ).fetchall()
            error = (
                "Scheduler restarted after this execution's owner exited before a durable "
                "terminal state; whether side effects ran is unknown."
            )
        for row in rows:
            if not leased:
                if row["process_id"] == _PROCESS_ID:
                    continue
                if _owner_is_live(int(row["pid"]), row["process_started_at"]):
                    continue
            cur = conn.execute(
                """UPDATE executions SET status='unknown', finished_at=?, error=?
                   WHERE id=? AND status IN ('claimed','running')"""
                + (_LEASE_EXPIRED if leased else ""),
                (now, error, row["id"]),
            )
            changed += cur.rowcount
            if cur.rowcount:
                record = _record(conn.execute(
                    "SELECT * FROM executions WHERE id=?", (row["id"],)
                ).fetchone())
                if record is not None:
                    recovered.append(record)
        if changed:
            _prune_unlocked(conn)
    for record in recovered:
        _emit_execution_state(record)
    return changed


def list_executions(
    *, job_id: Optional[str] = None, limit: int = 50,
    before_claimed_at: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return indexed, newest-first execution history with cursor pagination."""
    clauses: List[str] = []
    params: List[Any] = []
    if job_id is not None:
        clauses.append("job_id=?")
        params.append(str(job_id))
    if before_claimed_at is not None:
        clauses.append("claimed_at < ?")
        params.append(str(before_claimed_at))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(max(1, min(int(limit), 500)))
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT * FROM executions" + where
            + " ORDER BY claimed_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def latest_execution(job_id: str) -> Optional[Dict[str, Any]]:
    rows = list_executions(job_id=job_id, limit=1)
    return rows[0] if rows else None


def latest_executions(job_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Load latest execution for many jobs in one indexed query."""
    clean = [str(job_id) for job_id in dict.fromkeys(job_ids) if job_id]
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    with _transaction() as conn:
        rows = conn.execute(
            f"""SELECT e.* FROM executions e
                WHERE e.job_id IN ({placeholders})
                  AND e.id=(SELECT e2.id FROM executions e2
                            WHERE e2.job_id=e.job_id
                            ORDER BY e2.claimed_at DESC, e2.id DESC LIMIT 1)""",
            clean,
        ).fetchall()
    return {row["job_id"]: dict(row) for row in rows}
