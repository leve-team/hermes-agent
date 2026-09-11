"""Transaction-preserving auxiliary writers that follow the profile's backend.

Unlike SessionDB's core write closure, these writers own BEGIN/commit/rollback.
On a SQLite profile, off returns the original SQLite connection without any
additional SQL and on adds recording only after the caller's SQLite schema
bootstrap has completed. On a PostgreSQL-authority profile the same caller
gets a connection into that profile's PostgreSQL store instead: the tables it
bootstraps live next to the core tables, so a pod replacement, PVC move or
profile rollout cannot leave ``async_delegations`` / ``delivery_obligations``
behind in a ``state.db`` nobody reads any more (PG3 0051).
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional, Tuple

from hermes_state_dual import (
    DualWriteReplicator,
    RecordingConnection,
    dual_write_dsn,
    dual_write_enabled,
)

logger = logging.getLogger(__name__)

_REPLACE_TABLES = {"async_delegations", "delivery_obligations"}
_REPLACE_INSERT = re.compile(
    r"\A\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)", re.IGNORECASE
)

# SQLite -> PostgreSQL type mapping for the writers' ``CREATE TABLE`` DDL.
# The rewrites are applied in this order; ``TEXT`` / ``INTEGER`` are shared.
_POSTGRES_DDL_REWRITES: Tuple[Tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", re.IGNORECASE),
     "BIGSERIAL PRIMARY KEY"),
    (re.compile(r"\bREAL\b", re.IGNORECASE), "DOUBLE PRECISION"),
)


def postgres_ddl(sqlite_ddl: str) -> str:
    """Rewrite a writer's SQLite ``CREATE TABLE``/``ADD COLUMN`` DDL for PostgreSQL.

    ``INTEGER PRIMARY KEY AUTOINCREMENT`` -> ``BIGSERIAL PRIMARY KEY``,
    ``REAL`` -> ``DOUBLE PRECISION`` (a PostgreSQL ``REAL`` is float4 and
    would truncate ``time.time()`` stamps), ``TEXT``/``INTEGER`` unchanged.
    """
    for pattern, replacement in _POSTGRES_DDL_REWRITES:
        sqlite_ddl = pattern.sub(replacement, sqlite_ddl)
    return sqlite_ddl


def open_writer(db_path, *, timeout: float, initialize: Callable):
    store = _postgres_store_for(db_path)
    if store is not None:
        connection = _PostgresWriterConnection(store)
        try:
            initialize(connection)
        except BaseException:
            connection.close()
            raise
        return connection
    connection = sqlite3.connect(db_path, timeout=timeout)
    try:
        initialize(connection)
        if not dual_write_enabled():
            return connection
        if connection.in_transaction:
            raise RuntimeError("auxiliary schema bootstrap left an open transaction")
        dual = DualWriteReplicator(connection, dual_write_dsn())
        dual.initialize_source()
        return _WriterConnection(connection, dual)
    except BaseException:
        connection.close()
        raise


def _profile_of(db_path) -> Tuple[Optional[str], bool]:
    """``(profile name, is the active process's own store)`` for a ``state.db``
    inside the Hermes profile tree; ``(None, False)`` for any other file.

    An explicit file outside the tree names a specific SQLite database and is
    honoured as such, exactly like ``SessionDB(db_path=...)``.
    """
    from hermes_constants import get_default_hermes_root, get_hermes_home

    path = Path(db_path).expanduser()
    if path.name != "state.db":
        return None, False
    try:
        resolved = path.resolve()
        if resolved.parent == get_hermes_home().resolve():
            from hermes_cli.profiles import get_active_profile_name

            return get_active_profile_name(), True
        root = get_default_hermes_root().resolve()
    except OSError:
        return None, False
    if resolved.parent == root:
        return "default", False
    if resolved.parent.parent == root / "profiles":
        return resolved.parent.name, False
    return None, False


def _postgres_store_for(db_path) -> Optional[Any]:
    """A writable ``SessionDB`` on PostgreSQL when the profile that owns
    *db_path* runs on PostgreSQL authority; ``None`` when it stays on SQLite.

    The active process's own store follows the same selector as every core
    ``SessionDB()`` in this process (``HERMES_STATE_BACKEND`` env first, then
    the active ``config.yaml``) so the writer-local tables can never split
    from the core tables. A peer profile's store (a caller handing in another
    profile's ``state.db`` path) follows that profile's own ``config.yaml``
    through the backend-aware seam. ``probe`` keeps SQLite authority, and is
    decided before anything opens so the SQLite file never receives a core
    bootstrap from here.
    """
    try:
        import hermes_state_postgres as seam
    except ImportError:
        return None  # base install without the PostgreSQL module
    profile, active = _profile_of(db_path)
    if profile is None:
        return None
    if active:
        if seam.resolve_state_backend() != "authority":
            return None
        from hermes_state import SessionDB

        db = SessionDB(read_only=False)
    else:
        # profile_selects_postgres() also answers True for probe; the writer
        # needs the authority itself.
        if seam.profile_state_backend(profile) != "authority":
            return None
        db = seam.open_store_for_profile(profile, read_only=False)
    if not getattr(db, "_is_postgres", False):
        db.close()
        raise RuntimeError(
            f"profile {profile!r} selects PostgreSQL authority but the store "
            "opened on SQLite; refusing to write the auxiliary tables to the "
            "wrong physical store"
        )
    return db


class _PostgresWriterConnection:
    """``sqlite3.Connection``-shaped handle over a profile's PostgreSQL store.

    Owns the ``SessionDB`` so ``close()`` releases the PostgreSQL connection.
    The adapter connects with autocommit, so ``with conn:`` opens an explicit
    transaction to keep the writers' commit-on-success / rollback-on-error
    contract; statements issued outside a ``with`` block self-commit, which is
    what the schema bootstrap relies on. ``is_postgres`` lets an owner pick
    its dialect-specific DDL and upsert.
    """

    is_postgres = True

    def __init__(self, db):
        self._db = db
        self._conn = db._conn

    def execute(self, sql: str, params: Any = ()):
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, rows):
        return self._conn.executemany(sql, rows)

    def executescript(self, sql_script: str):
        return self._conn.executescript(sql_script)

    def cursor(self):
        return self._conn.cursor()

    @property
    def in_transaction(self) -> bool:
        return bool(getattr(self._conn, "_in_transaction", False))

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        self._db.close()

    def __enter__(self):
        self._conn.execute("BEGIN")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        return False


class _WriterConnection(RecordingConnection):
    def __init__(self, connection, dual):
        super().__init__(connection)
        self._dual = dual

    def _capture(self, sql, params, cursor):
        match = _REPLACE_INSERT.match(sql)
        if match and match[1].lower() in _REPLACE_TABLES:
            table = match[1].lower()
            columns = self._connection.execute(f"PRAGMA table_info({table})").fetchall()
            keys = ", ".join(f'"{column[1]}"' for column in columns if column[5])
            updates = ", ".join(
                f'"{column[1]}" = excluded."{column[1]}"'
                for column in columns
                if not column[5]
            )
            sql = _REPLACE_INSERT.sub(f"INSERT INTO {table}", sql, count=1)
            sql = sql.rstrip().rstrip(";")
            sql += f" ON CONFLICT ({keys}) DO UPDATE SET {updates}"
        super()._capture(sql, params, cursor)

    def commit(self):
        mutations = tuple(self.mutations)
        if mutations:
            self._dual.mark_coverage("open_writer", time.time())
        self._connection.commit()
        self.mutations.clear()
        if not mutations:
            return
        mutation_id = uuid.uuid4().hex
        self._dual.inject("after_source_commit")
        try:
            self._dual.apply(mutation_id, mutations)
        except Exception as exc:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._dual.journal_failure(mutation_id, "open_writer", mutations, exc)
                self._connection.commit()
            except Exception:
                try:
                    self._connection.rollback()
                except Exception:
                    pass
                logger.exception(
                    "auxiliary dual-write failure journal could not be updated"
                )
            else:
                logger.warning(
                    "auxiliary dual-write batch %s journaled for replay", mutation_id
                )

    def rollback(self):
        try:
            return self._connection.rollback()
        finally:
            self.mutations.clear()

    def close(self):
        try:
            return self._connection.close()
        finally:
            self.mutations.clear()

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.rollback()
        else:
            try:
                self.commit()
            except BaseException:
                self.rollback()
                raise
        return False
