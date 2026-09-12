"""Backend selection and the closed persistence dialect used by kanban.

No PostgreSQL driver is imported on the default SQLite path. Owners select a
backend; domain operations borrow that connection without a host-app import.
"""

from __future__ import annotations

import os

TABLE_KEYS = {
    "tasks": ("id",),
    "task_links": ("parent_id", "child_id"),
    "task_comments": ("id",),
    "task_events": ("id",),
    "task_runs": ("id",),
    "task_attachments": ("id",),
    "kanban_notify_subs": ("task_id", "platform", "chat_id", "thread_id"),
}


def check_table(table):
    if table not in TABLE_KEYS:
        raise ValueError("Table is outside the kanban persistence contract")


def resolve_backend(backend=None, *, env_var="HERMES_KANBAN_BACKEND"):
    selected = backend if backend is not None else os.environ.get(env_var, "sqlite")
    if selected not in ("sqlite", "postgres"):
        raise ValueError(f"{env_var} must be sqlite or postgres")
    return selected


class SQLiteDialect:
    backend = "sqlite"
    uses_sqlite_files = True

    def order_by(self, expression):
        return expression

    def table_info(self, conn, table):
        check_table(table)
        return conn.execute(f"PRAGMA table_info({table})").fetchall()

    def table_exists(self, conn, table):
        check_table(table)
        return (
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            is not None
        )


_SQLITE_DIALECT = SQLiteDialect()


def dialect_for(conn):
    return getattr(conn, "kanban_dialect", _SQLITE_DIALECT)
