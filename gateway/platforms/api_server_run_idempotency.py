"""Durable idempotency reservations for API server runs.

On a PostgreSQL-authority profile the reservations live in the profile's PostgreSQL store
(``core_run_idempotency``, levos v3) instead of ``runs_idempotency.db``: overlapping pods must not
both admit one request, and a pod's disk dies with it. There is no file or ``:memory:`` fallback
there; an unreachable store raises ``AuxStoreUnavailable`` on each call (the adapter answers 503)
and is reopened on the next one. A run's owner may then be another pod, whose pid cannot be probed
here, so the owner of a reservation holds a lease on the PostgreSQL server clock that its process
renews (the delivery-ledger model, levos 0063); ``status_for_run`` reports ``owner_live`` from it."""

import hmac
import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict


# Keep the extracted store's log records on the API server logger.
logger = logging.getLogger("gateway.platforms.api_server")

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})

_SELECT_BY_KEY = (
    "SELECT fingerprint, run_id, status_json, owner_pid, owner_started, updated_at "
    "FROM run_idempotency WHERE scope=? AND idempotency_key=?")
# ``{greatest}``: SQLite's scalar MAX, PostgreSQL's GREATEST.
_EXTEND_RETENTION_BY_KEY = (
    "UPDATE run_idempotency SET retention_until={greatest}(retention_until, ?) "
    "WHERE scope=? AND idempotency_key=? AND fingerprint=?")
_EXTEND_RETENTION_BY_RUN = (
    "UPDATE run_idempotency SET retention_until={greatest}(retention_until, ?) "
    "WHERE scope=? AND run_id=?")
_TABLE_DDL = """CREATE TABLE IF NOT EXISTS run_idempotency (
                scope TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                run_id TEXT NOT NULL,
                status_json TEXT NOT NULL,
                owner_pid INTEGER NOT NULL DEFAULT 0,
                owner_started INTEGER NOT NULL DEFAULT 0,
                retention_until REAL NOT NULL DEFAULT 0,
                acknowledged_at REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (scope, idempotency_key)
            )"""
_INDEX_DDL = "CREATE UNIQUE INDEX IF NOT EXISTS run_idempotency_run_id ON run_idempotency(run_id)"
_PG_STORE = "run_idempotency"
LEASE_SECONDS = 120.0
LEASE_RENEW_SECONDS = 30.0
_lease_renewer = None
_lease_renewer_guard = threading.Lock()


def _initialize_postgres(conn) -> None:
    from hermes_aux_store import AUX_LEASE_COLUMNS, aux_add_columns, aux_schema_transaction

    with aux_schema_transaction(conn, _PG_STORE):
        conn.execute(_TABLE_DDL)
        aux_add_columns(conn, "core_run_idempotency", tuple(_MIGRATIONS.items()) + AUX_LEASE_COLUMNS)
        conn.execute(_INDEX_DDL)


def renew_run_leases() -> int:
    """Renew the leases of every reservation this process owns (PostgreSQL authority)."""
    from hermes_aux_store import AUX_SERVER_EPOCH, aux_owner_instance, open_aux_postgres

    conn = open_aux_postgres(_PG_STORE, initialize=_initialize_postgres)
    try:
        return conn.execute(
            f"UPDATE run_idempotency SET lease_expires_at={AUX_SERVER_EPOCH} + ? WHERE owner_instance=?",
            (LEASE_SECONDS, aux_owner_instance())).rowcount
    finally:
        conn.close()


def _start_lease_renewer() -> None:
    global _lease_renewer
    from hermes_aux_store import AuxLeaseRenewer

    with _lease_renewer_guard:
        if _lease_renewer is None:
            _lease_renewer = AuxLeaseRenewer("run-idempotency-lease", renew_run_leases,
                                             lambda: LEASE_RENEW_SECONDS, logger)
        renewer = _lease_renewer
    renewer.start()
# Columns added after the first schema shipped; applied when missing.
_MIGRATIONS = {
    "owner_pid": "INTEGER NOT NULL DEFAULT 0",
    "owner_started": "INTEGER NOT NULL DEFAULT 0",
    "retention_until": "REAL NOT NULL DEFAULT 0",
    "acknowledged_at": "REAL"}


def _encode_status(status: Dict[str, Any]) -> str:
    return json.dumps(status, sort_keys=True, separators=(",", ":"))


def _record(run_id, status_json, owner_pid, owner_started, updated_at) -> dict[str, Any]:
    return {
        "run_id": run_id, "status": json.loads(status_json), "owner_pid": int(owner_pid or 0),
        "owner_started": int(owner_started or 0), "updated_at": float(updated_at or 0)}


def _outcome(row, fingerprint):
    """Classify a stored ``(scope, key)`` row against the caller's fingerprint."""
    return ("reused" if hmac.compare_digest(row[0], fingerprint) else "conflict"), _record(*row[1:])


class RunIdempotencyStore:
    """Durable, tenant-scoped reservations for ``POST /v1/runs``: a unique ``(scope, key)`` row
    inserted inside ``BEGIN IMMEDIATE`` so separate workers cannot both admit one request. Only
    fingerprints and public run status are stored — never request bodies or credentials."""

    RETENTION_SECONDS = 24 * 60 * 60
    ACKNOWLEDGED_RETENTION_SECONDS = 24 * 60 * 60

    @property
    def durable(self) -> bool:
        """Whether reservations survive this process."""
        return self._postgres or self._db_path is not None

    def __init__(self, db_path: str = None):
        from hermes_aux_store import aux_store_authority

        self._lock = threading.Lock()
        self._postgres = db_path is None and aux_store_authority()
        if self._postgres:
            self._db_path, self._conn = None, None
            try:
                self._db()
            except Exception:
                logger.error("Run idempotency store unavailable on the PostgreSQL authority store; "
                             "/v1/runs requests will fail until it recovers", exc_info=True)
            return
        if db_path is None:
            try:
                from hermes_cli.config import get_hermes_home
                db_path = str(get_hermes_home() / "runs_idempotency.db")
            except Exception:
                db_path = ":memory:"
        self._db_path = None if db_path == ":memory:" else db_path
        try:
            self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        except Exception as exc:
            # Docker may create the container object before `docker run` fails to start it (e.g. exit code
            # 125 when the daemon isn't ready, or a timeout mid-pull). That orphan is left in "Created"
            # state — which the exited-only orphan reaper (reap_orphan_containers, status=exited) never
            # catches, so it leaks permanently. Remove it by its known name before re-raising. See #7439.
            logger.warning(
                "Run idempotency storage is unavailable; falling back to "
                "process memory, so replay will not survive a restart: %s", exc)
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._db_path = None
        from hermes_state_wal import apply_wal_with_fallback
        apply_wal_with_fallback(self._conn, db_label="runs_idempotency.db")
        self._conn.execute(_TABLE_DDL)
        columns = {str(row[1]) for row in self._conn.execute("PRAGMA table_info(run_idempotency)")}
        for column, ddl in _MIGRATIONS.items():
            if column not in columns:
                self._conn.execute(f"ALTER TABLE run_idempotency ADD COLUMN {column} {ddl}")
        self._conn.execute(_INDEX_DDL)
        self._conn.commit()
        self._tighten_permissions()

    def _db(self):
        """The open connection; on PostgreSQL authority, (re)opened on demand."""
        if self._conn is None:
            from hermes_aux_store import open_aux_postgres

            self._conn = open_aux_postgres(_PG_STORE, initialize=_initialize_postgres)
        return self._conn

    def _sql(self, template: str) -> str:
        return template.format(greatest="GREATEST" if self._postgres else "MAX")

    def _tighten_permissions(self) -> None:
        for suffix in ("", "-wal", "-shm") if self._db_path else ():
            candidate = Path(self._db_path + suffix)
            try:
                if candidate.exists():
                    candidate.chmod(0o600)
            except OSError:
                logger.debug("Failed to restrict run idempotency store permissions", exc_info=True)

    @contextmanager
    def _immediate_txn(self):
        """Hold the lock inside ``BEGIN IMMEDIATE`` (PostgreSQL: a transaction holding the store's
        advisory lock, so other pods serialize too); the body commits, errors roll back."""
        with self._lock:
            conn = self._db()
            if self._postgres:
                from hermes_aux_store import aux_xact_lock

                conn.execute("BEGIN")
                try:
                    aux_xact_lock(conn, "run-idempotency", timeout_seconds=30.0)
                except Exception:
                    conn.rollback()
                    raise
            else:
                conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                conn.rollback()
                raise

    def reserve(self, scope: str, key: str, fingerprint: str, run_id: str, status: Dict[str, Any], *,
                owner_pid: int = 0, owner_started: int = 0, retention_until: float = 0):
        """Atomically reserve a key; return ``(outcome, stored_record)``."""
        now = time.time()
        retention_until = max(0.0, float(retention_until or 0))
        encoded = _encode_status(status)
        with self._immediate_txn():
            self._prune_stale_terminal_locked(now)
            row = self._db().execute(_SELECT_BY_KEY, (scope, key)).fetchone()
            if row is not None:
                if retention_until:
                    self._db().execute(self._sql(_EXTEND_RETENTION_BY_KEY), (retention_until, scope, key, fingerprint))
                self._db().commit()
                return _outcome(row, fingerprint)
            self._db().execute(
                "INSERT INTO run_idempotency("
                "scope,idempotency_key,fingerprint,run_id,status_json,"
                "owner_pid,owner_started,retention_until,created_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                (scope, key, fingerprint, run_id, encoded, int(owner_pid or 0), int(owner_started or 0),
                 retention_until, now, now))
            if self._postgres:
                from hermes_aux_store import AUX_SERVER_EPOCH, aux_owner_instance

                self._db().execute(
                    f"UPDATE run_idempotency SET owner_instance=?, lease_expires_at={AUX_SERVER_EPOCH} + ? "
                    "WHERE scope=? AND idempotency_key=?", (aux_owner_instance(), LEASE_SECONDS, scope, key))
            self._db().commit()
        if self._postgres:
            _start_lease_renewer()
        return "created", _record(run_id, encoded, owner_pid, owner_started, now) | {"status": status}

    def lookup(self, scope: str, key: str, fingerprint: str, *, retention_until: float = 0):
        """Return ``missing``, ``reused`` or ``conflict`` without reserving."""
        now = time.time()
        retention_until = max(0.0, float(retention_until or 0))
        with self._immediate_txn():
            if retention_until:
                self._db().execute(self._sql(_EXTEND_RETENTION_BY_KEY), (retention_until, scope, key, fingerprint))
            self._prune_stale_terminal_locked(now)
            row = self._db().execute(_SELECT_BY_KEY, (scope, key)).fetchone()
            self._db().commit()
        return ("missing", None) if row is None else _outcome(row, fingerprint)

    def _prune_stale_terminal_locked(self, now: float) -> None:
        """Prune aged replay records only once their stored run is terminal (caller holds the
        lock + transaction): a long or disconnected room turn may outlive the retention window."""
        stale = self._db().execute(
            """SELECT scope, idempotency_key, status_json
                 FROM run_idempotency
                WHERE acknowledged_at <= ?
                   OR (retention_until > 0 AND retention_until <= ?)
                   OR (retention_until <= 0 AND updated_at < ?)""",
            (now - self.ACKNOWLEDGED_RETENTION_SECONDS, now, now - self.RETENTION_SECONDS),
        ).fetchall()
        for stale_scope, stale_key, stale_status in stale:
            try:
                terminal = json.loads(stale_status).get("status") in TERMINAL_STATUSES
            except Exception:
                terminal = False
            if terminal:
                self._db().execute(
                    "DELETE FROM run_idempotency WHERE scope=? AND idempotency_key=?", (stale_scope, stale_key))

    def status_for_run(self, scope: str, run_id: str, *, retention_until: float = 0) -> dict[str, Any] | None:
        """Load one durable run status inside its authenticated scope."""
        retention_until = max(0.0, float(retention_until or 0))
        with self._lock:
            if retention_until:
                self._db().execute(self._sql(_EXTEND_RETENTION_BY_RUN), (retention_until, scope, run_id))
                self._db().commit()
            live, live_params = "", ()
            if self._postgres:
                from hermes_aux_store import AUX_LEASE_EXPIRED, aux_owner_instance

                live = f", (owner_instance IS NOT DISTINCT FROM ? OR NOT {AUX_LEASE_EXPIRED})"
                live_params = (aux_owner_instance(),)
            row = self._db().execute(
                f"SELECT status_json, owner_pid, owner_started, updated_at{live} "
                "FROM run_idempotency WHERE scope=? AND run_id=?",
                (*live_params, scope, run_id)).fetchone()
        if row is None:
            return None
        record = {k: v for k, v in _record(None, *row[:4]).items() if k != "run_id"}
        if self._postgres:
            record["owner_live"] = bool(row[4])
        return record

    def extend_retention(self, scope: str, run_id: str, until: float) -> bool:
        """Persist the latest verified recovery horizon for an active grant."""
        checked_until = max(0.0, float(until or 0))
        if not checked_until:
            return False
        with self._lock:
            changed = self._db().execute(self._sql(_EXTEND_RETENTION_BY_RUN), (checked_until, scope, run_id)).rowcount
            self._db().commit()
        return changed == 1

    def owns_run(self, scope: str, run_id: str) -> bool:
        with self._lock:
            row = self._db().execute(
                "SELECT 1 FROM run_idempotency WHERE scope=? AND run_id=?", (scope, run_id)).fetchone()
        return row is not None

    def update_status(self, run_id: str, status: Dict[str, Any]) -> None:
        with self._lock:
            self._db().execute(
                "UPDATE run_idempotency SET status_json=?, updated_at=? WHERE run_id=?",
                (_encode_status(status), time.time(), run_id))
            self._db().commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
