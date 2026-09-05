"""Transaction-preserving auxiliary writers for SQLite-primary migration.

Unlike SessionDB's core write closure, these writers own BEGIN/commit/rollback.
Off returns the original SQLite connection without any additional SQL. On adds
recording only after the caller's SQLite schema bootstrap has completed.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
import uuid
from typing import Callable

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


def open_writer(db_path, *, timeout: float, initialize: Callable):
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
