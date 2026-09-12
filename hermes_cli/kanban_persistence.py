"""Backend selection and the closed persistence dialect used by kanban.

No PostgreSQL driver is imported on the default SQLite path. Owners select a
backend; domain operations borrow that connection without a host-app import.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterable

_BOARD_DSN_ROUTING: tuple[Callable[[str], str], tuple[str, ...]] | None = None


def normalize_postgres_board(board: str) -> str:
    """Validate a board slug before converting hyphens to schema underscores."""
    if not isinstance(board, str):
        raise ValueError("Invalid PostgreSQL board slug")
    slug = board.strip().lower()
    if re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", slug) is None:
        raise ValueError("Invalid PostgreSQL board slug")
    return slug


def set_board_dsn_resolver(
    resolver: Callable[[str], str] | None, *, boards: Iterable[str] | None = None
) -> None:
    """Register trusted routing and its complete active-board inventory at boot.

    The resolver receives a normalized [a-z0-9_]+ token, not a SQL fragment.
    Schemas must already exist. None clears registration. Register before
    starting threads; subprocesses must register independently.
    """
    global _BOARD_DSN_ROUTING
    if resolver is None:
        if boards is not None:
            raise ValueError("Clearing board routing does not accept boards")
        _BOARD_DSN_ROUTING = None
        return
    if not callable(resolver) or boards is None or isinstance(boards, (str, bytes)):
        raise ValueError("Board DSN resolver requires an explicit board inventory")
    slugs = tuple(normalize_postgres_board(board) for board in boards)
    tokens = tuple(slug.replace("-", "_") for slug in slugs)
    if not slugs or len(set(tokens)) != len(tokens):
        raise ValueError("Board inventory is empty or has colliding schema tokens")
    _BOARD_DSN_ROUTING = (
        resolver,
        tuple(sorted(slugs, key=lambda slug: (slug != "default", slug))),
    )


def has_board_dsn_resolver() -> bool:
    return _BOARD_DSN_ROUTING is not None


def postgres_board_slugs() -> tuple[str, ...]:
    if _BOARD_DSN_ROUTING is None:
        raise ValueError("PostgreSQL boards require a registered board DSN resolver")
    return _BOARD_DSN_ROUTING[1]


def resolve_board_dsn(board: str) -> str:
    routing = _BOARD_DSN_ROUTING
    slug = normalize_postgres_board(board)
    if routing is None:
        raise ValueError("PostgreSQL boards require a registered board DSN resolver")
    resolver, slugs = routing
    if slug not in slugs:
        raise ValueError(f"PostgreSQL board {slug!r} is not registered")
    try:
        dsn = resolver(slug.replace("-", "_"))
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("Empty board DSN")
    except Exception:
        raise RuntimeError(
            f"PostgreSQL DSN resolution failed for board {slug!r}"
        ) from None
    return dsn


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
