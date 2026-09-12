"""Projects dispatch with no PostgreSQL import on the default SQLite path."""

from __future__ import annotations

import contextlib
import sqlite3

from hermes_cli.sqlite_util import write_txn as _sqlite_write_txn


@contextlib.contextmanager
def write_txn(conn: sqlite3.Connection):
    """Retain the projects transaction signature and reject implicit nesting."""
    dialect = getattr(conn, "projects_dialect", None)
    transaction = dialect.write_txn(conn) if dialect else _sqlite_write_txn(conn)
    with transaction:
        yield conn


def project_columns(conn):
    dialect = getattr(conn, "projects_dialect", None)
    if dialect is not None:
        return dialect.table_info(conn, "projects")
    return conn.execute("PRAGMA table_info(projects)")
