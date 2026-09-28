"""Core auxiliary stores that follow the profile's session backend (levos 0059).

Besides ``state.db`` the core keeps a few side stores in their own SQLite
files: the coding verification ledger (``verification_evidence.db``), the
Responses API chain store (``response_store.db``) and the Discord recovery
ledger (``gateway/discord_message_recovery.db``). While any of those files is
open a profile cannot run on two overlapping pods, so on a PostgreSQL-authority
profile they move next to the core tables in that profile's PostgreSQL store.

``open_aux_store`` is the one selector, and it is the same one ``SessionDB``
and ``hermes_state_writer.open_writer`` use (``HERMES_STATE_BACKEND`` env, then
the active ``config.yaml``):

* authority: a ``sqlite3.Connection``-shaped handle over a fresh PostgreSQL
  ``SessionDB`` of the active profile. The owner's SQLite SQL keeps working:
  the closed table set is renamed into a ``core_<area>_*`` namespace (a
  profile schema must not grow a bare ``meta``), ``INSERT OR REPLACE`` becomes
  a keyed upsert, identity inserts return ``lastrowid`` and ``CREATE TABLE``
  gets PostgreSQL column types. A connection or schema failure raises
  :class:`AuxStoreUnavailable`; nothing falls back to a file or ``:memory:``.
* anything else: the SQLite connection the owner opened before, unchanged.

``projects.db`` is not renamed here: it keeps the 0054 table contract and its
own adapter (``hermes_cli.projects_postgres``), selected by the same predicate
(:func:`aux_store_authority`).

levos 0060 adds the cron stores (``cron_executions``, ``cron_notepad`` and
``cron_jobs``, which has no SQLite form) and the advisory locks that replace
cron's file locks across pods (:class:`AuxSessionLock`, :func:`aux_xact_lock`),
plus their one-shot move, :func:`migrate_cron_to_pg`.
"""

from __future__ import annotations

import contextlib
import hashlib
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple

from hermes_state_writer import _PostgresWriterConnection, postgres_ddl


class AuxStoreUnavailable(RuntimeError):
    """PostgreSQL authority is selected but the auxiliary store cannot serve.

    Raised instead of opening SQLite; the message never carries the DSN.
    """


@dataclass(frozen=True)
class AuxTable:
    sqlite_name: str
    postgres_name: str
    key: Tuple[str, ...]
    identity: Optional[str] = None
    # (column, table): the column holds an ``identity`` id of that table in the
    # same store, so a one-shot migration rewrites it to the re-assigned id.
    reference: Optional[Tuple[str, str]] = None


# Closed set: a store name not listed here is a programming error, and a table
# not listed for its store never reaches the profile schema under a bare name.
AUX_STORES: Mapping[str, Tuple[AuxTable, ...]] = {
    "verification_evidence": (
        AuxTable("meta", "core_verification_meta", ("key",)),
        AuxTable("verification_events", "core_verification_events", ("id",), "id"),
        AuxTable(
            "verification_state",
            "core_verification_state",
            ("session_id", "root"),
            reference=("last_event_id", "verification_events"),
        ),
    ),
    "response_store": (
        AuxTable("responses", "core_response_responses", ("response_id",)),
        AuxTable("conversations", "core_response_conversations", ("name",)),
    ),
    "discord_recovery": (
        AuxTable("discord_messages", "core_discord_messages", ("message_id",)),
        AuxTable("discord_recovery_scans", "core_discord_recovery_scans", ("scan_id",)),
        AuxTable(
            "discord_recovery_cursors", "core_discord_recovery_cursors", ("channel_id",)
        ),
    ),
    # levos 0060: the cron stores. ``cron_jobs`` has no SQLite table (the
    # non-authority store is ``cron/jobs.json``); one row holds one job document.
    # pg3 keeps cron/incidents.py's table in the same ledger file, so it is part of this store.
    "cron_executions": (
        AuxTable("executions", "core_cron_executions", ("id",)),
        AuxTable("cron_incidents", "core_cron_incidents", ("id",)),
    ),
    "cron_notepad": (AuxTable("cron_notepad", "core_cron_notes", ("job_id", "key")),),
    # levos 0068: cron state a run hands to the next one; files, not SQLite,
    # off authority (see ``cron/durable.py`` and ``cron/suggestions.py``).
    "cron_outputs": (AuxTable("cron_outputs", "core_cron_outputs", ("job_id", "kind")),),
    "cron_scripts": (AuxTable("cron_scripts", "core_cron_scripts", ("path",)),),
    "cron_suggestions": (
        AuxTable("cron_suggestions", "core_cron_suggestions", ("id",)),
    ),
    "cron_jobs": (AuxTable("cron_jobs", "core_cron_jobs", ("id",)),),
    # levos 0065: the memory tool's MEMORY.md / USER.md (no SQLite form either;
    # one row per file name, drift snapshots as ``<name>.bak.<ts>`` rows).
    "memory": (AuxTable("memory_files", "core_memory_files", ("name",)),),
    # levos 0066: the credential store (``auth.json``); one row holds one
    # store document, ``profile`` or ``root``. No SQLite form either.
    "auth_store": (AuxTable("auth_store", "core_auth_store", ("name",)),),
    # levos 0067: small operational state (pairing grants, thread participation,
    # dead targets, voice modes, ESTOP, webhook subscriptions, ...), one row per
    # entry. No SQLite form: every other backend keeps the owners' JSON files.
    "aux_kv": (AuxTable("aux_kv", "core_aux_kv", ("namespace", "key")),),
}
_AUX_INDEXES: Mapping[str, Mapping[str, str]] = {
    "verification_evidence": {
        "idx_verification_events_session_root": "idx_core_verification_events_session_root",
    },
    "cron_executions": {
        "idx_executions_job_claimed": "idx_core_cron_executions_job_claimed",
        "idx_executions_status_claimed": "idx_core_cron_executions_status_claimed",
        "idx_executions_occurrence": "idx_core_cron_executions_occurrence",
        "idx_cron_incidents_job": "idx_core_cron_incidents_job",
        "idx_cron_incidents_state": "idx_core_cron_incidents_state",
    },
}

_QUOTED = re.compile(r"('(?:''|[^'])*'|\"(?:\"\"|[^\"])*\")")
_INSERT_OR_REPLACE = re.compile(
    r"\A\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]*)\)", re.IGNORECASE
)
_INSERT_INTO = re.compile(r"\A\s*INSERT\s+INTO\s+(\w+)", re.IGNORECASE)
_DDL = re.compile(r"\A\s*(?:CREATE|ALTER)\s+TABLE\b", re.IGNORECASE)
_PRAGMA = re.compile(r"\A\s*PRAGMA\b", re.IGNORECASE)
_RETURNING = re.compile(r"\bRETURNING\b", re.IGNORECASE)
_INTEGER = re.compile(r"\bINTEGER\b", re.IGNORECASE)


def aux_store_tables(name: str) -> Tuple[AuxTable, ...]:
    try:
        return AUX_STORES[name]
    except KeyError:
        raise ValueError(f"unknown auxiliary store {name!r}") from None


def aux_store_authority() -> bool:
    """True when the active profile's session store is PostgreSQL authority.

    ``probe`` keeps SQLite authority and therefore answers False.
    """
    try:
        import hermes_state_postgres as seam
    except ImportError:
        return False  # base install without the PostgreSQL module
    return seam.resolve_state_backend() == "authority"


def open_aux_store(
    name: str,
    *,
    sqlite_path: Any,
    initialize: Callable[[Any], None],
    sqlite_options: Optional[Dict[str, Any]] = None,
):
    """Open auxiliary store *name* on the backend the active profile selects.

    *initialize* runs on every open, before the handle is returned, and must be
    idempotent (``CREATE TABLE IF NOT EXISTS``). It can branch on the handle's
    ``is_postgres`` attribute to skip SQLite-only pragmas. On SQLite the file's
    parent directory is created and ``sqlite3.connect(sqlite_path,
    **sqlite_options)`` is returned exactly as the owner used to open it.
    """
    aux_store_tables(name)
    if aux_store_authority():
        return _open_postgres(name, initialize)
    path = str(sqlite_path)
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path, **(sqlite_options or {}))
    try:
        initialize(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def _open_postgres(name: str, initialize: Callable[[Any], None]):
    try:
        from hermes_state import SessionDB

        db = SessionDB(read_only=False)
    except Exception as exc:
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: the PostgreSQL authority store could not be opened"
        ) from exc
    if not getattr(db, "_is_postgres", False):
        db.close()
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: PostgreSQL authority is selected but the "
            "session store opened on SQLite; refusing to write the wrong store"
        )
    conn = AuxPostgresConnection(db, name)
    try:
        initialize(conn)
    except Exception as exc:
        conn.close()
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: PostgreSQL schema initialization failed"
        ) from exc
    except BaseException:
        conn.close()
        raise
    return conn


def open_aux_postgres(name: str, *, initialize: Callable[[Any], None]):
    """Open auxiliary store *name* on PostgreSQL authority only (levos 0060).

    For a store whose other form is not SQLite (``cron_jobs`` stays
    ``cron/jobs.json``), so there is no ``sqlite_path`` to open instead. The
    caller picks the backend with :func:`aux_store_authority`; on any other
    backend this raises :class:`AuxStoreUnavailable`.
    """
    aux_store_tables(name)
    if not aux_store_authority():
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r} exists only on PostgreSQL authority"
        )
    return _open_postgres(name, initialize)


# ---------------------------------------------------------------------------
# Advisory locks (levos 0060)
# ---------------------------------------------------------------------------
# A file lock only fences processes that share one filesystem; two pods of a
# profile share nothing but the profile's PostgreSQL store. Advisory lock keys
# are database-wide while profiles share a database under their own schemas,
# so every key is derived from the connection's current schema plus a name.


def aux_lock_key(schema: str, name: str) -> int:
    """Signed 64-bit advisory lock key for *name* inside profile *schema*."""
    digest = hashlib.sha256(f"{schema}\0{name}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _schema_lock_key(conn: Any, name: str) -> int:
    schema = conn.execute("SELECT COALESCE(current_schema(), 'public')").fetchone()[0]
    return aux_lock_key(schema, name)


def aux_xact_lock(conn: Any, name: str, *, timeout_seconds: float) -> None:
    """Take transaction advisory lock *name* inside *conn*'s open transaction.

    COMMIT or ROLLBACK releases it, and so does the server when the
    connection dies. Waiting longer than *timeout_seconds* raises
    :class:`AuxStoreUnavailable`; the aborted transaction must be rolled back.
    """
    key = _schema_lock_key(conn, name)
    conn.execute(
        "SELECT set_config('lock_timeout', ?, true)",
        (f"{max(1, int(timeout_seconds * 1000))}ms",),
    )
    try:
        conn.execute("SELECT pg_advisory_xact_lock(?)", (key,))
    except Exception as exc:
        if getattr(exc, "sqlstate", None) == "55P03":  # lock_not_available
            raise AuxStoreUnavailable(
                f"advisory lock {name!r}: still held by another process after "
                f"{timeout_seconds:g}s"
            ) from exc
        raise


@contextlib.contextmanager
def aux_schema_transaction(conn: Any, store: str) -> Iterator[Any]:
    """Run a PostgreSQL store's DDL in one transaction, serialized per store.

    Two pods opening a store for the first time would otherwise race their
    ``CREATE TABLE IF NOT EXISTS`` into a ``pg_type`` unique violation.
    """
    with conn:
        aux_xact_lock(conn, f"schema:{store}", timeout_seconds=30.0)
        yield conn


def _connect_lock_session(name: str):
    try:
        from hermes_state_postgres import connect_postgres, resolve_postgres_dsn

        dsn = resolve_postgres_dsn()
        if not dsn:
            raise RuntimeError("PostgreSQL authority is not selected")
        return connect_postgres(dsn)
    except Exception as exc:
        raise AuxStoreUnavailable(
            f"advisory lock {name!r}: the PostgreSQL authority store could not be reached"
        ) from exc


class AuxSessionLock:
    """A PostgreSQL session advisory lock on its own dedicated connection.

    The lock lives exactly as long as that connection: :meth:`release`
    unlocks and closes it, and when the holder process dies or the
    connection drops the server releases the lock by itself, so a crashed
    holder never wedges it. The flip side: a holder whose connection is cut
    mid-section is no longer fenced and is not told so; the owner-token
    checks the callers already do under the lock stay the correctness line.
    """

    def __init__(self, name: str):
        self.name = name
        self._conn: Any = None
        self._key: Optional[int] = None

    def acquire(self, *, wait_seconds: float = 0.0, poll_seconds: float = 0.1) -> bool:
        """Try to take the lock, polling up to *wait_seconds*.

        False means another session still holds it. A PostgreSQL failure
        raises :class:`AuxStoreUnavailable`.
        """
        if self._conn is not None:
            raise RuntimeError(f"advisory lock {self.name!r} is already held")
        conn = _connect_lock_session(self.name)
        try:
            key = _schema_lock_key(conn, self.name)
            deadline = time.monotonic() + max(0.0, wait_seconds)
            attempt = "SELECT pg_try_advisory_lock(?)"
            while not conn.execute(attempt, (key,)).fetchone()[0]:
                if time.monotonic() >= deadline:
                    conn.close()
                    return False
                time.sleep(poll_seconds)
        except Exception as exc:
            conn.close()
            raise AuxStoreUnavailable(
                f"advisory lock {self.name!r}: PostgreSQL failed while locking"
            ) from exc
        except BaseException:
            conn.close()
            raise
        self._conn, self._key = conn, key
        return True

    def release(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            conn.execute("SELECT pg_advisory_unlock(?)", (self._key,))
        except Exception:
            pass  # closing the session below releases the lock anyway
        finally:
            conn.close()


@contextlib.contextmanager
def aux_session_lock(
    name: str, *, wait_seconds: float = 0.0, poll_seconds: float = 0.1
) -> Iterator[bool]:
    """Hold :class:`AuxSessionLock` *name* for the block; yields whether it is held."""
    lock = AuxSessionLock(name)
    acquired = lock.acquire(wait_seconds=wait_seconds, poll_seconds=poll_seconds)
    try:
        yield acquired
    finally:
        lock.release()


def connect_aux_postgres(name: str):
    """A plain autocommit connection to the authority profile's store (levos 0066).

    For a store whose owner runs one or two statements per operation on a hot
    path (the credential store) and must not open a ``SessionDB`` each time.
    PostgreSQL authority only; a failure raises :class:`AuxStoreUnavailable`,
    whose message never carries the DSN.
    """
    aux_store_tables(name)
    if not aux_store_authority():
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r} exists only on PostgreSQL authority"
        )
    try:
        from hermes_state_postgres import connect_postgres, resolve_postgres_dsn

        return connect_postgres(resolve_postgres_dsn())
    except Exception as exc:
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: the PostgreSQL authority store could not be reached"
        ) from exc


# ---------------------------------------------------------------------------
# Small operational state as key-value rows (levos 0067)
# ---------------------------------------------------------------------------
# The gateway and agent keep a few small JSON files in the profile home:
# pairing grants, thread participation, dead delivery targets, voice modes,
# the ESTOP sentinel, webhook subscriptions. A pod's disk is its own, so on an
# authority profile each file becomes a *namespace* of ``core_aux_kv`` rows,
# one row per entry: writers touch their own entries instead of replacing a
# whole document another pod may have changed. ``updated_at`` is the
# PostgreSQL server clock (bounded sets trim by it), never a pod clock.
# Callers pick the backend with :func:`aux_store_authority`; these functions
# exist only on authority and raise :class:`AuxStoreUnavailable` otherwise.

AUX_KV_STORE = "aux_kv"
_AUX_KV_NOW = "EXTRACT(EPOCH FROM clock_timestamp())"

# Namespaces, one per former file (the owners and the one-shot move share them).
KV_PAIRING = "pairing"  # one row per former pairing file, key = file name
KV_VOICE_MODE = "voice_mode"
KV_DEAD_TARGETS = "dead_targets"
KV_RICH_SENT = "rich_sent"
KV_DISCORD_NONCONVERSATIONAL = "discord_nonconversational"
KV_ESTOP = "estop"
KV_WEBHOOK_SUBSCRIPTIONS = "webhook_subscriptions"


def kv_threads_namespace(platform: str) -> str:
    """Namespace of ``{platform}_threads.json`` (thread participation)."""
    return f"threads:{platform}"


def _initialize_aux_kv(conn) -> None:
    with aux_schema_transaction(conn, AUX_KV_STORE):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS aux_kv (
                 namespace TEXT NOT NULL,
                 key TEXT NOT NULL,
                 value TEXT NOT NULL,
                 updated_at REAL NOT NULL,
                 PRIMARY KEY (namespace, key)
               )"""
        )


def open_aux_kv():
    """Open the key-value store of the active authority profile."""
    return open_aux_postgres(AUX_KV_STORE, initialize=_initialize_aux_kv)


@contextlib.contextmanager
def _aux_kv_handle(conn: Any, *, write: bool) -> Iterator[Any]:
    """Yield *conn* (the caller's transaction) or a fresh handle; a fresh
    handle writes in its own transaction and is closed afterwards."""
    if conn is not None:
        yield conn
        return
    own = open_aux_kv()
    try:
        if write:
            with own:
                yield own
        else:
            yield own
    finally:
        own.close()


@contextlib.contextmanager
def aux_kv_transaction(
    namespace: str, *, timeout_seconds: float = 30.0
) -> Iterator[Any]:
    """One transaction holding *namespace*'s advisory lock, for a
    read-modify-write of the namespace that is atomic across processes and
    pods. Pass the yielded handle as ``conn=`` to the ``aux_kv_*`` calls."""
    conn = open_aux_kv()
    try:
        with conn:
            aux_xact_lock(conn, f"kv:{namespace}", timeout_seconds=timeout_seconds)
            yield conn
    finally:
        conn.close()


def aux_kv_get(namespace: str, key: str, *, conn: Any = None) -> Optional[str]:
    with _aux_kv_handle(conn, write=False) as handle:
        row = handle.execute(
            "SELECT value FROM aux_kv WHERE namespace = ? AND key = ?",
            (namespace, key),
        ).fetchone()
    return None if row is None else row[0]


def aux_kv_items(namespace: str, *, conn: Any = None) -> List[Tuple[str, str]]:
    """``(key, value)`` pairs of *namespace*, oldest write first."""
    with _aux_kv_handle(conn, write=False) as handle:
        rows = handle.execute(
            "SELECT key, value FROM aux_kv WHERE namespace = ? "
            "ORDER BY updated_at, key",
            (namespace,),
        ).fetchall()
    return [(row[0], row[1]) for row in rows]


def aux_kv_put(
    namespace: str,
    key: str,
    value: str,
    *,
    conn: Any = None,
    keep_existing: bool = False,
) -> bool:
    """Write one entry; True when a row was inserted or changed.

    ``keep_existing`` leaves an existing entry (and its position in the
    write order) untouched, the way a set keeps a member it already has.
    """
    action = (
        "NOTHING"
        if keep_existing
        else "UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
    )
    with _aux_kv_handle(conn, write=True) as handle:
        cursor = handle.execute(
            "INSERT INTO aux_kv (namespace, key, value, updated_at) "
            f"VALUES (?, ?, ?, {_AUX_KV_NOW}) "
            f"ON CONFLICT (namespace, key) DO {action}",
            (namespace, key, value),
        )
        return cursor.rowcount > 0


def aux_kv_delete(namespace: str, key: str, *, conn: Any = None) -> bool:
    with _aux_kv_handle(conn, write=True) as handle:
        cursor = handle.execute(
            "DELETE FROM aux_kv WHERE namespace = ? AND key = ?", (namespace, key)
        )
        return cursor.rowcount > 0


def aux_kv_trim(namespace: str, keep: int, *, conn: Any = None) -> int:
    """Drop all but the *keep* most recently written entries of *namespace*."""
    with _aux_kv_handle(conn, write=True) as handle:
        cursor = handle.execute(
            "DELETE FROM aux_kv WHERE namespace = ? AND key NOT IN ("
            "SELECT key FROM aux_kv WHERE namespace = ? "
            "ORDER BY updated_at DESC, key DESC LIMIT ?)",
            (namespace, namespace, max(0, int(keep))),
        )
        return cursor.rowcount


@contextlib.contextmanager
def aux_connection_lock(
    conn: Any, name: str, *, wait_seconds: float, poll_seconds: float = 0.05
) -> Iterator[None]:
    """Hold session advisory lock *name* on the caller's own connection (levos 0070).

    For DDL that must run on *conn* itself and outside a transaction
    (``CREATE INDEX CONCURRENTLY``), where neither :func:`aux_xact_lock` nor a
    dedicated :class:`AuxSessionLock` connection fits. The wait is a
    ``pg_try_advisory_lock`` poll, never a blocking ``pg_advisory_lock``: a
    statement blocked on the lock keeps a snapshot open, and the holder's
    ``CREATE INDEX CONCURRENTLY`` waits for every older snapshot, so a blocking
    waiter would deadlock with it. Past *wait_seconds* this raises
    :class:`AuxStoreUnavailable`; other PostgreSQL errors propagate unchanged.
    Leaving the block unlocks; a dead holder's session takes the lock with it.
    """
    raw = conn.raw if hasattr(conn, "raw") else conn
    schema = raw.execute("SELECT COALESCE(current_schema(), 'public')").fetchone()[0]
    key = aux_lock_key(schema, name)
    deadline = time.monotonic() + max(0.0, wait_seconds)
    while not raw.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]:
        if time.monotonic() >= deadline:
            raise AuxStoreUnavailable(
                f"advisory lock {name!r}: still held by another process after "
                f"{wait_seconds:g}s"
            )
        time.sleep(poll_seconds)
    unlock = "SELECT pg_advisory_unlock(%s)"
    try:
        yield
    finally:
        try:
            raw.execute(unlock, (key,))
        except Exception:
            try:  # an aborted transaction refuses the unlock until rolled back
                raw.rollback()
                raw.execute(unlock, (key,))
            except Exception:
                pass  # a broken connection's server session released the lock


class AuxPostgresConnection(_PostgresWriterConnection):
    """The writer handle plus the owner's closed SQLite dialect for one store."""

    def __init__(self, db, store: str):
        super().__init__(db)
        self.store = store
        tables = aux_store_tables(store)
        self._tables = {table.postgres_name: table for table in tables}
        renames = {table.sqlite_name: table.postgres_name for table in tables}
        renames.update(_AUX_INDEXES.get(store, {}))
        self._renames = renames
        self._names = re.compile(
            r"\b("
            + "|".join(sorted(map(re.escape, renames), key=len, reverse=True))
            + r")\b"
        )
        self._columns: Dict[str, List[str]] = {}

    def _rename(self, sql: str) -> str:
        parts = _QUOTED.split(sql)
        for index in range(0, len(parts), 2):
            parts[index] = self._names.sub(
                lambda m: self._renames[m.group(1)], parts[index]
            )
        return "".join(parts)

    def _table(self, name: str) -> AuxTable:
        try:
            return self._tables[name.lower()]
        except KeyError:
            raise ValueError(
                f"table {name!r} is not part of auxiliary store {self.store!r}"
            ) from None

    def table_columns(self, table: str) -> List[str]:
        columns = self._columns.get(table)
        if columns is None:
            rows = self._conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = ? "
                "ORDER BY ordinal_position",
                (table,),
            ).fetchall()
            columns = [row[0] for row in rows]
            if not columns:
                raise ValueError(f"auxiliary table {table!r} does not exist")
            self._columns[table] = columns
        return columns

    def _translate(self, sql: str) -> Tuple[str, Optional[str]]:
        sql = self._rename(sql)
        if _PRAGMA.match(sql):
            raise ValueError(
                "SQLite PRAGMA has no meaning on a PostgreSQL auxiliary store"
            )
        if _DDL.match(sql):
            return _INTEGER.sub("BIGINT", postgres_ddl(sql)), None
        replace = _INSERT_OR_REPLACE.match(sql)
        if replace:
            table = self._table(replace.group(1))
            listed = [column.strip() for column in replace.group(2).split(",")]
            # SQLite REPLACE deletes the old row, so columns the statement does
            # not list go back to their defaults; the upsert says so explicitly.
            updates = [f"{c} = EXCLUDED.{c}" for c in listed if c not in table.key]
            updates += [
                f"{c} = DEFAULT"
                for c in self.table_columns(table.postgres_name)
                if c not in listed and c not in table.key
            ]
            sql = (
                re
                .sub(
                    r"\A\s*INSERT\s+OR\s+REPLACE\s+INTO",
                    "INSERT INTO",
                    sql,
                    count=1,
                    flags=re.IGNORECASE,
                )
                .rstrip()
                .rstrip(";")
            )
            action = f"UPDATE SET {', '.join(updates)}" if updates else "NOTHING"
            sql += f" ON CONFLICT ({', '.join(table.key)}) DO {action}"
        insert = _INSERT_INTO.match(sql)
        if insert and not _RETURNING.search(sql):
            identity = self._table(insert.group(1)).identity
            if identity:
                return f"{sql.rstrip().rstrip(';')} RETURNING {identity}", identity
        return sql, None

    def execute(self, sql: str, params: Any = ()):
        translated, returning = self._translate(sql)
        cursor = self._conn.execute(translated, params)
        if returning:
            row = cursor.fetchone()
            cursor.lastrowid = row[0] if row else None
        return cursor

    def executemany(self, sql: str, rows):
        translated, _ = self._translate(sql)
        return self._conn.executemany(translated, rows)

    def executescript(self, sql_script: str):
        from hermes_state_pg_schema import _split_sql_statements

        cursor = None
        for statement in _split_sql_statements(sql_script):
            cursor = self.execute(statement)
        return cursor

    def cursor(self):
        raise NotImplementedError("auxiliary stores use connection.execute()")


# ---------------------------------------------------------------------------
# Change signals (levos 0069)
# ---------------------------------------------------------------------------
# Watchers (the dashboard's tui change watcher, ``hermes mcp serve``) stat
# ``state.db`` / ``cron/jobs.json`` because every writer moves those files. On
# authority no writer touches them, and the other pod of an overlapping
# rollout writes a disk this pod never sees. The signal is read from the
# profile's PostgreSQL store instead, so it moves whichever process wrote.
#
# ``sessions`` is too big to digest every half second (a row-hash scan costs
# ~16 ms at 20k sessions), so it pairs the newest message id (primary-key
# index: exact and immediate for a new turn) with the server's cumulative
# insert/update/delete counters of both tables — any write, like the file
# mtime. Those counters are published when the writing backend goes idle
# (within about a second) and need ``track_counts``, which autovacuum needs
# too. ``cron_jobs`` is small: an exact, order-independent sum of row hashes.

# name -> (table whose absence is a valid state, signal query)
AUX_CHANGE_SIGNALS: Mapping[str, Tuple[str, str]] = {
    "sessions": (
        "sessions",
        "SELECT (SELECT COALESCE(MAX(id), 0) FROM messages), "
        "pg_stat_get_tuples_inserted(s) + pg_stat_get_tuples_updated(s) "
        "+ pg_stat_get_tuples_deleted(s), "
        "pg_stat_get_tuples_inserted(m) + pg_stat_get_tuples_updated(m) "
        "+ pg_stat_get_tuples_deleted(m) "
        "FROM (SELECT 'sessions'::regclass AS s, 'messages'::regclass AS m) AS t",
    ),
    "cron_jobs": (
        "core_cron_jobs",
        "SELECT COUNT(*), COALESCE(SUM(hashtext(ROW(id, position, job)::text)), 0) "
        "FROM core_cron_jobs",
    ),
}
_change_signal_lock = threading.Lock()
_change_signal_connections: Dict[str, Any] = {}


def aux_change_signal(name: str) -> Tuple[Any, ...]:
    """Current digest of change signal *name* in the active profile's store.

    Compare values, never interpret them: equal means nothing watched moved.
    The store's tables are never created here (a missing table is a value of
    its own). Watchers poll every fraction of a second, so one autocommit
    connection per DSN is kept and reused. Not on PostgreSQL authority, or on
    any PostgreSQL failure, raises :class:`AuxStoreUnavailable` — there is no
    file signal to fall back to.
    """
    try:
        table, query = AUX_CHANGE_SIGNALS[name]
    except KeyError:
        raise ValueError(f"unknown change signal {name!r}") from None
    try:
        from hermes_state_postgres import connect_postgres, resolve_postgres_dsn

        dsn = resolve_postgres_dsn()
    except Exception as exc:
        raise AuxStoreUnavailable(
            f"change signal {name!r}: the PostgreSQL authority store could not be resolved"
        ) from exc
    if not dsn:
        raise AuxStoreUnavailable(
            f"change signal {name!r} exists only on PostgreSQL authority"
        )
    with _change_signal_lock:
        try:
            conn = _change_signal_connections.get(dsn)
            if conn is None:
                conn = _change_signal_connections[dsn] = connect_postgres(dsn)
            if conn.execute("SELECT to_regclass(?)", (table,)).fetchone()[0] is None:
                return (name, None)
            return tuple(conn.execute(query).fetchone())
        except Exception as exc:
            stale = _change_signal_connections.pop(dsn, None)
            if stale is not None:
                with contextlib.suppress(Exception):
                    stale.close()
            raise AuxStoreUnavailable(
                f"change signal {name!r}: the PostgreSQL authority store could not be read"
            ) from exc


# ---------------------------------------------------------------------------
# One-shot SQLite -> PostgreSQL move
# ---------------------------------------------------------------------------

_PROJECT_TABLES: Tuple[AuxTable, ...] = (
    # projects keeps the 0054 contract: same table names on both backends.
    AuxTable("projects", "projects", ("id",)),
    AuxTable("project_folders", "project_folders", ("project_id", "path")),
    AuxTable("project_meta", "project_meta", ("key",)),
    AuxTable("discovered_repos", "discovered_repos", ("root",)),
)


class AuxMigrationError(RuntimeError):
    """A source row is missing from PostgreSQL after the copy; rolled back."""


class _DryRun(Exception):
    pass


def _source_files() -> Tuple[Tuple[str, Path], ...]:
    from hermes_constants import aux_db_path, get_hermes_home

    home = get_hermes_home()
    return (
        ("verification_evidence", aux_db_path("verification_evidence.db")),
        ("response_store", aux_db_path("response_store.db")),
        ("discord_recovery", home / "gateway" / "discord_message_recovery.db"),
        ("projects", home / "projects.db"),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def migrate_aux_sqlite_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's four auxiliary SQLite files into PostgreSQL.

    Run inside the profile's own process environment (its pod), after the
    image carrying 0059 is serving: from then on the files are no longer
    written, and a row the live PostgreSQL store already holds wins.

    * Every file is opened ``mode=ro`` on one snapshot and verified byte-equal
      (sha256) afterwards; a missing file is reported and skipped.
    * Each store copies in one PostgreSQL transaction. Rows are inserted with
      ``ON CONFLICT DO NOTHING``; ``verification_events`` rows are matched on
      their content and get fresh ids (``verification_state`` follows them),
      so running twice yields the same row counts.
    * After the copy every source row must be present in PostgreSQL (by key;
      by content for events), else the store rolls back and
      :class:`AuxMigrationError` is raised.
    * ``dry_run`` performs the same copy and checks, then rolls back.
    """
    return _migrate_sources(
        "migrate_aux_sqlite_to_pg", profile, _source_files(), dry_run=dry_run
    )


def _cron_source_files() -> Tuple[Tuple[str, Path], ...]:
    from hermes_constants import aux_db_path, get_hermes_home

    home = get_hermes_home()
    return (
        ("cron_executions", aux_db_path("cron/executions.db")),
        ("cron_notepad", home / "cron" / "notepad.db"),
        ("cron_jobs", home / "cron" / "jobs.json"),
    )


def migrate_cron_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's cron stores into PostgreSQL (levos 0060).

    Same contract as :func:`migrate_aux_sqlite_to_pg` for ``cron/executions.db``,
    ``cron/notepad.db`` and ``cron/jobs.json``: sources are only read and are
    sha256-checked, each store is one PostgreSQL transaction, rows PostgreSQL
    already holds win, a source row missing afterwards rolls the store back
    (:class:`AuxMigrationError`), running twice inserts nothing, ``dry_run``
    rolls back. ``jobs.json`` is merged by job id under the live jobs lock, so
    a pod ticking at the same time cannot interleave with the copy. Migrated
    attempts carry no lease, so a still-open one is later marked unknown.
    """
    return _migrate_sources(
        "migrate_cron_to_pg", profile, _cron_source_files(), dry_run=dry_run
    )


def _auth_source_files() -> Tuple[Tuple[str, Path], ...]:
    from hermes_cli.auth import _auth_file_path, _global_auth_file_path

    sources = [("auth_profile", _auth_file_path())]
    root = _global_auth_file_path()
    if root is not None:
        sources.append(("auth_root", root))
    return tuple(sources)


def migrate_auth_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's credential stores into PostgreSQL (levos 0066).

    The profile ``auth.json`` becomes the ``profile`` row of ``core_auth_store``
    and, in profile mode, the root ``auth.json`` the ``root`` row. Same
    contract as :func:`migrate_cron_to_pg`: sources are only read and are
    sha256-checked, running twice adds nothing, ``dry_run`` writes nothing.
    A store is merged by top-level key, and by provider key inside
    ``providers`` and ``credential_pool``; a key PostgreSQL already holds wins,
    because a pod on this image may already have rotated that token there.
    The merge runs under the store's live advisory lock, so it cannot
    interleave with a refresh on a running pod.
    """
    return _migrate_sources(
        "migrate_auth_to_pg", profile, _auth_source_files(), dry_run=dry_run
    )


def _migrate_sources(
    entrypoint: str,
    profile: str,
    sources: Tuple[Tuple[str, Path], ...],
    *,
    dry_run: bool,
) -> Dict[str, Any]:
    from hermes_state_postgres import _is_active_profile

    if not _is_active_profile(profile):
        raise ValueError(
            f"{entrypoint} runs in the profile's own environment "
            f"(HERMES_PROFILE / HERMES_HOME); {profile!r} is not the active profile"
        )
    if not aux_store_authority():
        raise RuntimeError(
            f"profile {profile!r} is not on PostgreSQL authority; nothing to migrate to"
        )
    report: Dict[str, Any] = {"profile": profile, "dry_run": dry_run, "stores": {}}
    for store, path in sources:
        if not path.is_file():
            report["stores"][store] = {"status": "missing", "path": str(path)}
            continue
        before = _sha256(path)
        result = _migrate_store(store, path, dry_run=dry_run)
        after = _sha256(path)
        if after != before:  # pragma: no cover - mode=ro makes this unreachable
            raise AuxMigrationError(f"{store}: source file changed during migration")
        result.update(path=str(path), sha256=before)
        report["stores"][store] = result
    return report


def _migrate_store(store: str, path: Path, *, dry_run: bool) -> Dict[str, Any]:
    from state_transfer import open_sqlite_snapshot

    if store == "cron_jobs":
        return _migrate_cron_jobs(path, dry_run=dry_run)
    if store.startswith("auth_"):
        return _migrate_auth_store(path, dry_run=dry_run)
    source = open_sqlite_snapshot(path)
    try:
        if store == "projects":
            return _migrate_projects(source, dry_run=dry_run)
        target = _open_postgres(store, _store_initializer(store))
        try:
            tables: Dict[str, Any] = {}
            try:
                with target:
                    ids: Dict[str, Dict[Any, Any]] = {}
                    for table in AUX_STORES[store]:
                        tables[table.sqlite_name] = _copy_table(
                            source,
                            target,
                            table,
                            ids,
                            target_columns=target.table_columns,
                        )
                    if dry_run:
                        raise _DryRun
            except _DryRun:
                pass
            return {"status": "dry_run" if dry_run else "migrated", "tables": tables}
        finally:
            target.close()
    finally:
        source.close()


def _migrate_projects(source: sqlite3.Connection, *, dry_run: bool) -> Dict[str, Any]:
    from hermes_cli import projects_db
    from hermes_cli.projects_persistence import write_txn

    target = projects_db.connect()
    try:
        dialect = getattr(target, "projects_dialect", None)
        if dialect is None:
            raise AuxMigrationError("projects: authority profile opened SQLite")

        def columns(table: str) -> List[str]:
            return [row["name"] for row in dialect.table_info(target, table)]

        tables: Dict[str, Any] = {}
        try:
            with write_txn(target):
                for table in _PROJECT_TABLES:
                    tables[table.sqlite_name] = _copy_table(
                        source, target, table, {}, target_columns=columns
                    )
                if dry_run:
                    raise _DryRun
        except _DryRun:
            pass
        return {"status": "dry_run" if dry_run else "migrated", "tables": tables}
    finally:
        target.close()


def _migrate_cron_jobs(path: Path, *, dry_run: bool) -> Dict[str, Any]:
    """``jobs.json`` → ``core_cron_jobs``, by job id, inside the jobs lock."""
    from cron import jobs as cron_jobs

    try:
        data, _ = cron_jobs._parse_jobs_file(path)  # opens read-only
    except (OSError, ValueError) as exc:
        raise AuxMigrationError(f"cron_jobs: {path.name} is unreadable: {exc}") from exc
    source = data.get("jobs", []) if isinstance(data, dict) else data
    if not isinstance(source, list) or not all(
        isinstance(job, dict) and job.get("id") for job in source
    ):
        raise AuxMigrationError(f"cron_jobs: {path.name} holds a job without an id")
    table: Dict[str, Any] = {}
    try:
        with cron_jobs._jobs_lock():
            stored = cron_jobs.load_jobs()
            present = {str(job["id"]) for job in stored}
            added = [job for job in source if str(job["id"]) not in present]
            if added:
                try:
                    cron_jobs.save_jobs(stored + added)
                except ValueError as exc:  # e.g. a duplicate id in the file
                    raise AuxMigrationError(f"cron_jobs: {exc}; rolled back") from exc
            after = {str(job["id"]) for job in cron_jobs.load_jobs()}
            missing = sum(1 for job in source if str(job["id"]) not in after)
            if missing:
                raise AuxMigrationError(
                    f"jobs: {missing} of {len(source)} source jobs are not in "
                    "PostgreSQL after the copy; rolled back"
                )
            table = {
                "source_rows": len(source),
                "target_rows_before": len(stored),
                "target_rows_after": len(after),
                "inserted": len(added),
                "dropped_columns": [],
            }
            if dry_run:
                raise _DryRun
    except _DryRun:
        pass
    return {"status": "dry_run" if dry_run else "migrated", "tables": {"jobs": table}}


def migrate_memory_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's ``memories/`` files into PostgreSQL (levos 0065).

    ``MEMORY.md`` and ``USER.md`` merge by entry inside the memory tool's own
    cross-pod section: a file PostgreSQL lacks is copied verbatim, otherwise
    the stored entries stay first and in place and each file entry the row
    lacks is appended — so running twice appends nothing and a memory a pod
    wrote before the move survives it. Drift snapshots (``*.bak.<ts>``) are
    copied only when their row is absent. Sources are only read and are
    sha256-checked, each file is one PostgreSQL transaction, a source entry
    missing afterwards rolls that file back (:class:`AuxMigrationError`) and
    ``dry_run`` rolls back. The char limits are not applied: an over-limit
    result is left for the agent to consolidate, as an over-limit file is.
    """
    from hermes_constants import get_hermes_home
    from hermes_state_postgres import _is_active_profile

    if not _is_active_profile(profile):
        raise ValueError(
            "migrate_memory_to_pg runs in the profile's own environment "
            f"(HERMES_PROFILE / HERMES_HOME); {profile!r} is not the active profile"
        )
    if not aux_store_authority():
        raise RuntimeError(
            f"profile {profile!r} is not on PostgreSQL authority; nothing to migrate to"
        )
    memories = get_hermes_home() / "memories"
    sources = [memories / "MEMORY.md", memories / "USER.md"]
    if memories.is_dir():
        sources += sorted(p for p in memories.glob("*.md.bak.*") if p.is_file())
    report: Dict[str, Any] = {"profile": profile, "dry_run": dry_run, "stores": {}}
    for path in sources:
        if not path.is_file():
            report["stores"][path.name] = {"status": "missing", "path": str(path)}
            continue
        before = _sha256(path)
        result = _migrate_memory_file(path, dry_run=dry_run)
        if _sha256(path) != before:  # pragma: no cover - the file is only read
            raise AuxMigrationError(
                f"{path.name}: source file changed during migration"
            )
        result.update(path=str(path), sha256=before)
        report["stores"][path.name] = result
    return report


def _migrate_memory_file(path: Path, *, dry_run: bool) -> Dict[str, Any]:
    from tools.memory_tool import (
        ENTRY_DELIMITER,
        MemoryStore,
        memory_postgres_section,
        read_memory_document,
        write_memory_document,
    )

    try:
        raw = path.read_bytes().decode("utf-8-sig")  # as the memory tool reads it
    except UnicodeDecodeError as exc:
        raise AuxMigrationError(
            f"memory: {path.name} is not valid UTF-8: {exc}"
        ) from exc
    snapshot = ".bak." in path.name
    source = list(dict.fromkeys(MemoryStore._parse_entries(raw)))
    table: Dict[str, Any] = {}
    try:
        with memory_postgres_section(path.name):
            stored = read_memory_document(path.name)
            # Entry counts are of distinct entries, as the memory tool loads them.
            held = list(dict.fromkeys(MemoryStore._parse_entries(stored[0]))) if stored else []
            present = set(held)
            added = [entry for entry in source if entry not in present]
            if stored is None:
                content = raw
            elif snapshot or not added:
                content, added = stored[0], []
            else:
                kept = [stored[0].strip()] if stored[0].strip() else []
                content = ENTRY_DELIMITER.join(kept + added)
            if stored is None or content != stored[0]:
                write_memory_document(path.name, content)
            after = read_memory_document(path.name)
            after_entries = (
                list(dict.fromkeys(MemoryStore._parse_entries(after[0]))) if after else []
            )
            missing = len(set(source) - set(after_entries))
            if after is None or (missing and not snapshot):
                raise AuxMigrationError(
                    f"{path.name}: {missing} of {len(source)} source entries are not in "
                    "PostgreSQL after the copy; rolled back"
                )
            table = {
                "source_entries": len(source),
                "target_entries_before": None if stored is None else len(held),
                "target_entries_after": len(after_entries),
                "inserted": len(source) if stored is None else len(added),
            }
            if dry_run:
                raise _DryRun
    except _DryRun:
        pass
    return {
        "status": "dry_run" if dry_run else "migrated",
        "tables": {"memory_files": table},
    }


_AUTH_MERGED_BY_PROVIDER = ("providers", "credential_pool")
_AUTH_STAMPS = ("version", "updated_at")  # rewritten by every save


def _auth_keys(store: Mapping[str, Any]) -> List[str]:
    keys: List[str] = []
    for key, value in store.items():
        if key in _AUTH_STAMPS:
            continue
        if key in _AUTH_MERGED_BY_PROVIDER and isinstance(value, dict):
            keys.extend(f"{key}.{provider}" for provider in value)
        else:
            keys.append(key)
    return keys


def _migrate_auth_store(path: Path, *, dry_run: bool) -> Dict[str, Any]:
    """One ``auth.json`` → its ``core_auth_store`` row, under the live lock."""
    import json

    from hermes_cli import auth

    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        # The type only: a decode error can quote bytes of a credential.
        raise AuxMigrationError(
            f"auth_store: {path.name} is unreadable ({type(exc).__name__})"
        ) from None
    if not isinstance(raw, dict):
        raise AuxMigrationError(f"auth_store: {path.name} is not a JSON object")
    source = auth._auth_store_from_raw(raw)
    added: List[str] = []
    with auth._auth_store_lock(target_path=path):
        # The file is merged below; the load must not seed the absent row from it first.
        stored = auth._load_auth_store(path, seed_from_file=False)
        before = _auth_keys(stored)
        for key, value in source.items():
            if key in _AUTH_STAMPS:
                continue
            if key in _AUTH_MERGED_BY_PROVIDER and isinstance(value, dict):
                current = stored.setdefault(key, {})
                if not isinstance(current, dict):
                    raise AuxMigrationError(
                        f"auth_store: PostgreSQL {key!r} is not an object; not merging"
                    )
                for provider, state in value.items():
                    if provider not in current:
                        current[provider] = state
                        added.append(f"{key}.{provider}")
            elif key not in stored:
                stored[key] = value
                added.append(key)
        if added and not dry_run:
            auth._save_auth_store(stored, target_path=path)
            stored = auth._load_auth_store(path)
        after = _auth_keys(stored)
    missing = sorted(set(_auth_keys(source)) - set(after))
    if missing:
        raise AuxMigrationError(
            f"auth_store: {len(missing)} source keys are not in PostgreSQL after the copy"
        )
    return {
        "status": "dry_run" if dry_run else "migrated",
        "tables": {
            "auth_store": {
                "source_rows": len(_auth_keys(source)),
                "target_rows_before": len(before),
                "target_rows_after": len(after),
                "inserted": len(added),
                "inserted_keys": added,  # provider names only, never values
                "dropped_columns": [],
            }
        },
    }


def _store_initializer(store: str) -> Callable[[Any], None]:
    if store == "cron_executions":
        from cron.executions import _initialize_schema

        return _initialize_schema
    if store == "cron_notepad":
        from cron.notepad import _initialize_schema

        return _initialize_schema
    if store == "verification_evidence":
        from agent.verification_evidence import _initialize_connection

        return _initialize_connection
    if store == "response_store":
        from gateway.platforms.api_server import _initialize_response_store

        return _initialize_response_store
    from plugins.platforms.discord.recovery import DiscordRecoveryStore

    return DiscordRecoveryStore()._initialize


def _copy_table(
    source: sqlite3.Connection,
    target: Any,
    table: AuxTable,
    ids: Dict[str, Dict[Any, Any]],
    *,
    target_columns: Callable[[str], List[str]],
) -> Dict[str, Any]:
    """Copy one table; *target* SQL uses the SQLite names (the handle renames)."""
    from state_transfer import quote_identifier

    name = table.sqlite_name
    present = source.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone()
    if present is None:
        return {"status": "absent"}
    source_columns = [
        row[1] for row in source.execute(f"PRAGMA table_info({quote_identifier(name)})")
    ]
    available = set(target_columns(table.postgres_name))
    columns = [c for c in source_columns if c in available]
    dropped = [c for c in source_columns if c not in available]
    if not set(table.key) <= set(columns):
        raise AuxMigrationError(f"{name}: key columns {table.key} missing on a side")
    quoted = ", ".join(quote_identifier(c) for c in columns)
    order = ", ".join(quote_identifier(c) for c in table.key)
    rows = [
        tuple(row)
        for row in source.execute(
            f"SELECT {quoted} FROM {quote_identifier(name)} ORDER BY {order}"
        )
    ]
    count = f"SELECT COUNT(*) FROM {name}"
    before = target.execute(count).fetchone()[0]
    identity = table.identity
    if identity:
        inserted, missing = _copy_identity_rows(
            target, name, columns, rows, identity, ids
        )
    else:
        if table.reference:
            column, referenced = table.reference
            index = columns.index(column)
            mapping = ids.get(referenced, {})
            rows = [
                row[:index] + (mapping.get(row[index]),) + row[index + 1 :]
                for row in rows
            ]
        insert = (
            f"INSERT INTO {name} ({quoted}) VALUES ({', '.join('?' for _ in columns)}) "
            "ON CONFLICT DO NOTHING"
        )
        for row in rows:
            target.execute(insert, row)
        positions = [columns.index(c) for c in table.key]
        found = {
            tuple(r) for r in target.execute(f"SELECT {order} FROM {name}").fetchall()
        }
        missing = sum(
            1 for row in rows if tuple(row[i] for i in positions) not in found
        )
        inserted = None
    after = target.execute(count).fetchone()[0]
    if missing:
        raise AuxMigrationError(
            f"{name}: {missing} of {len(rows)} source rows are not in PostgreSQL after "
            "the copy (a conflicting unique value?); rolled back"
        )
    return {
        "source_rows": len(rows),
        "target_rows_before": before,
        "target_rows_after": after,
        "inserted": after - before if inserted is None else inserted,
        "dropped_columns": dropped,
    }


def _copy_identity_rows(target, name, columns, rows, identity, ids):
    """Events get fresh ids: the live store may already have used the old ones."""
    from state_transfer import quote_identifier

    content = [c for c in columns if c != identity]
    select = ", ".join(quote_identifier(c) for c in [identity, *content])
    existing = {
        tuple(row[1:]): row[0]
        for row in target.execute(
            f"SELECT {select} FROM {name} ORDER BY {identity}"
        ).fetchall()
    }
    position = columns.index(identity)
    insert = (
        f"INSERT INTO {name} ({', '.join(quote_identifier(c) for c in content)}) "
        f"VALUES ({', '.join('?' for _ in content)})"
    )
    mapping = ids.setdefault(name, {})
    inserted = 0
    for row in rows:
        values = row[:position] + row[position + 1 :]
        new_id = existing.get(values)
        if new_id is None:
            new_id = target.execute(insert, values).lastrowid
            if new_id is None:
                raise AuxMigrationError(f"{name}: insert returned no id")
            existing[values] = new_id
            inserted += 1
        mapping[row[position]] = new_id
    found = {
        tuple(row[1:])
        for row in target.execute(f"SELECT {select} FROM {name}").fetchall()
    }
    missing = sum(
        1 for row in rows if row[:position] + row[position + 1 :] not in found
    )
    return inserted, missing


_VOICE_MODES = frozenset({"off", "voice_only", "all"})


def _read_json_source(path: Path) -> Any:
    import json

    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError) as exc:
        raise AuxMigrationError(f"{path.name} is unreadable: {exc}") from exc


def _kv_set_entries(path: Path) -> List[Tuple[str, str]]:
    data = _read_json_source(path)
    if not isinstance(data, list):
        raise AuxMigrationError(f"{path.name} is not a JSON list")
    return [(str(item), "") for item in data if str(item).strip()]


def _kv_document_entries(path: Path) -> List[Tuple[str, str]]:
    import json

    data = _read_json_source(path)
    if not isinstance(data, dict):
        raise AuxMigrationError(f"{path.name} is not a JSON object")
    return [
        (str(key), json.dumps(value, ensure_ascii=False))
        for key, value in data.items()
        if isinstance(value, dict)
    ]


def _kv_rich_sent_entries(path: Path) -> List[Tuple[str, str]]:
    data = _read_json_source(path)
    if not isinstance(data, dict):
        raise AuxMigrationError(f"{path.name} is not a JSON object")
    kept = [
        (key, value)
        for key, value in data.items()
        if isinstance(value, dict) and value.get("t")
    ]
    kept.sort(key=lambda item: item[1].get("ts", 0))  # oldest first, like the trim
    return [(str(key), str(value["t"])) for key, value in kept]


def _kv_voice_entries(path: Path) -> List[Tuple[str, str]]:
    data = _read_json_source(path)
    if not isinstance(data, dict):
        raise AuxMigrationError(f"{path.name} is not a JSON object")
    return [
        (str(key), mode)
        for key, mode in data.items()
        if mode in _VOICE_MODES and ":" in str(key)
    ]


def _kv_estop_entries(path: Path) -> List[Tuple[str, str]]:
    import json

    # Any sentinel means engaged, even an empty or corrupt one (agent.estop).
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        body = None
    if not isinstance(body, dict):
        body = {"engaged_at": None, "reason": None}
    return [("ESTOP", json.dumps(body, ensure_ascii=False))]


def _kv_sources() -> List[
    Tuple[str, Path, str, Callable[[Path], List[Tuple[str, str]]]]
]:
    """``(label, file, namespace, entries)`` for every file the owners used."""
    from hermes_constants import get_hermes_home

    home = get_hermes_home()
    sources: List[Tuple[str, Path, str, Callable[[Path], List[Tuple[str, str]]]]] = [
        (
            "voice_mode",
            home / "gateway_voice_mode.json",
            KV_VOICE_MODE,
            _kv_voice_entries,
        ),
        (
            "dead_targets",
            home / "gateway" / "dead_targets.json",
            KV_DEAD_TARGETS,
            _kv_document_entries,
        ),
        (
            "rich_sent",
            home / "state" / "rich_sent_index.json",
            KV_RICH_SENT,
            _kv_rich_sent_entries,
        ),
        (
            "discord_nonconversational",
            home / "gateway" / "discord_nonconversational_messages.json",
            KV_DISCORD_NONCONVERSATIONAL,
            _kv_set_entries,
        ),
        ("estop", home / "ESTOP", KV_ESTOP, _kv_estop_entries),
        (
            "webhook_subscriptions",
            home / "webhook_subscriptions.json",
            KV_WEBHOOK_SUBSCRIPTIONS,
            _kv_document_entries,
        ),
    ]
    for path in sorted(home.glob("*_threads.json")):
        platform = path.name[: -len("_threads.json")]
        sources.append((
            f"threads:{platform}",
            path,
            kv_threads_namespace(platform),
            _kv_set_entries,
        ))
    return sources


def _pairing_source_files() -> List[Path]:
    """Pairing files, the active layout first (its keys win, as the store's
    own split-directory merge does)."""
    from hermes_constants import get_hermes_dir, get_hermes_home

    home = get_hermes_home()
    active = get_hermes_dir("platforms/pairing", "pairing")
    files: List[Path] = []
    for directory in (active, home / "platforms" / "pairing", home / "pairing"):
        if directory.is_dir():
            files += [
                path
                for path in sorted(directory.glob("*.json"))
                if path.is_file() and path not in files
            ]
    return files


def migrate_aux_kv_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's small state files into ``aux_kv`` (levos 0067).

    Same contract as :func:`migrate_aux_sqlite_to_pg`: files are only read and
    sha256-checked, a missing file is reported, an entry PostgreSQL already
    holds wins (a pairing file's keys merge under the ones already stored),
    every source entry must be present afterwards or everything rolls back
    (:class:`AuxMigrationError`), running twice inserts nothing and
    ``dry_run`` rolls back. It is one transaction holding each namespace's
    lock, so a pod writing the same namespace cannot interleave. The
    ``.env`` allowlist mirror has nothing to move: on authority the pairing
    grant rows are the record (see ``gateway.pairing``).
    """
    from hermes_constants import get_hermes_home
    from hermes_state_postgres import _is_active_profile

    if not _is_active_profile(profile):
        raise ValueError(
            f"migrate_aux_kv_to_pg runs in the profile's own environment "
            f"(HERMES_PROFILE / HERMES_HOME); {profile!r} is not the active profile"
        )
    if not aux_store_authority():
        raise RuntimeError(
            f"profile {profile!r} is not on PostgreSQL authority; nothing to migrate to"
        )
    report: Dict[str, Any] = {"profile": profile, "dry_run": dry_run, "stores": {}}
    conn = open_aux_kv()
    try:
        try:
            with conn:
                for label, path, namespace, entries in _kv_sources():
                    report["stores"][label] = _migrate_kv_file(
                        conn, path, namespace, entries
                    )
                pairing = _pairing_source_files()
                if pairing:
                    aux_xact_lock(conn, f"kv:{KV_PAIRING}", timeout_seconds=30.0)
                home = get_hermes_home()
                for path in pairing:
                    label = f"pairing:{path.relative_to(home).as_posix()}"
                    report["stores"][label] = _migrate_pairing_file(conn, path)
                if dry_run:
                    raise _DryRun
        except _DryRun:
            pass
    finally:
        conn.close()
    for result in report["stores"].values():
        if result.get("status") == "copied":
            result["status"] = "dry_run" if dry_run else "migrated"
    return report


def _migrate_kv_file(conn, path: Path, namespace: str, entries) -> Dict[str, Any]:
    if not path.is_file():
        return {"status": "missing", "path": str(path)}
    before = _sha256(path)
    source = entries(path)
    aux_xact_lock(conn, f"kv:{namespace}", timeout_seconds=30.0)
    count = "SELECT COUNT(*) FROM aux_kv WHERE namespace = ?"
    target_before = conn.execute(count, (namespace,)).fetchone()[0]
    for key, value in source:
        aux_kv_put(namespace, key, value, conn=conn, keep_existing=True)
    present = {key for key, _ in aux_kv_items(namespace, conn=conn)}
    missing = sum(1 for key, _ in source if key not in present)
    if missing:
        raise AuxMigrationError(
            f"{namespace}: {missing} of {len(source)} source entries are not in "
            "PostgreSQL after the copy; rolled back"
        )
    if _sha256(path) != before:  # pragma: no cover - the file is only read
        raise AuxMigrationError(f"{namespace}: source file changed during migration")
    target_after = conn.execute(count, (namespace,)).fetchone()[0]
    return {
        "status": "copied",
        "path": str(path),
        "sha256": before,
        "source_rows": len(source),
        "target_rows_before": target_before,
        "target_rows_after": target_after,
        "inserted": target_after - target_before,
    }


def _migrate_pairing_file(conn, path: Path) -> Dict[str, Any]:
    import json

    before = _sha256(path)
    source = _read_json_source(path)
    if not isinstance(source, dict):
        raise AuxMigrationError(f"pairing {path.name} is not a JSON object")
    stored_text = aux_kv_get(KV_PAIRING, path.name, conn=conn)
    stored = json.loads(stored_text) if stored_text else {}
    merged = {**source, **stored}  # PostgreSQL (then the active layout) wins
    added = len(merged) - len(stored)
    if added:
        aux_kv_put(
            KV_PAIRING,
            path.name,
            json.dumps(merged, indent=2, ensure_ascii=False),
            conn=conn,
        )
    after = json.loads(aux_kv_get(KV_PAIRING, path.name, conn=conn) or "{}")
    missing = sum(1 for key in source if key not in after)
    if missing:  # pragma: no cover - the merge above keeps every key
        raise AuxMigrationError(
            f"pairing {path.name}: {missing} keys missing; rolled back"
        )
    if _sha256(path) != before:  # pragma: no cover - the file is only read
        raise AuxMigrationError(f"pairing {path.name}: source changed during migration")
    return {
        "status": "copied",
        "path": str(path),
        "sha256": before,
        "source_rows": len(source),
        "target_rows_before": len(stored),
        "target_rows_after": len(after),
        "inserted": added,
    }


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="One-shot move of the auxiliary SQLite stores into the "
        "profile's PostgreSQL authority store (levos 0059)."
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--cron",
        action="store_true",
        help="move the cron stores (executions, notepad, jobs.json) instead (levos 0060)",
    )
    parser.add_argument(
        "--memory",
        action="store_true",
        help="move the memory files (memories/MEMORY.md, USER.md) instead (levos 0065)",
    )
    parser.add_argument(
        "--auth",
        action="store_true",
        help="move the credential stores (profile and root auth.json) instead (levos 0066)",
    )
    parser.add_argument(
        "--kv",
        action="store_true",
        help="move the small gateway state files (pairing, threads, dead targets, "
        "voice modes, ESTOP, webhook subscriptions, ...) instead (levos 0067)",
    )
    args = parser.parse_args(argv)
    exclusive = [name for name in ("cron", "memory", "auth", "kv") if getattr(args, name)]
    if len(exclusive) > 1:
        parser.error("--" + " / --".join(exclusive) + " are separate moves")
    migrate = migrate_cron_to_pg if args.cron else migrate_aux_sqlite_to_pg
    if args.memory:
        migrate = migrate_memory_to_pg
    if args.auth:
        migrate = migrate_auth_to_pg
    if args.kv:
        migrate = migrate_aux_kv_to_pg
    report = migrate(args.profile, dry_run=args.dry_run)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - operator entrypoint
    raise SystemExit(main())
