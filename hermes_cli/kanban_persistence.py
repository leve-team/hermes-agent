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

#: The store whose dialect honours a service registration. Only the kanban
#: board shares its schema with a host service; a store that reuses the same
#: adapter under another name keeps the closed core list.
SERVICE_TABLE_STORE = "kanban"

#: How a registered table says which board owns one of its rows. ``column`` is
#: a tenant key on the table itself; ``task`` is ownership reached through
#: ``tasks`` by a foreign key. There is deliberately no third spelling for
#: "unknown": a table nobody can attribute to a board must not be registered,
#: because the first cross-board read would then be a silent leak rather than a
#: refusal.
SERVICE_TENANT_SCOPES = ("column", "task")

_SERVICE_DECLARATION_KEYS = frozenset(
    {"key", "identity", "tenant_scope", "tenant_key"}
)
_SQL_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")

#: Registered service tables, keyed by table name. Empty until a host
#: application registers, which is the whole default: the core contract is
#: ``TABLE_KEYS`` and nothing else.
_SERVICE_TABLES = {}


def _service_table_entry(table, declaration):
    """Validate one service table declaration into its stored form."""
    if not isinstance(table, str) or _SQL_NAME.fullmatch(table) is None:
        raise ValueError("Service table names must be lowercase SQL identifiers")
    if table in TABLE_KEYS:
        raise ValueError(f"Service table {table!r} would shadow a core kanban table")
    if not isinstance(declaration, dict):
        raise ValueError(f"Service table {table!r} requires a declaration mapping")
    unknown = sorted(set(declaration) - _SERVICE_DECLARATION_KEYS)
    if unknown:
        raise ValueError(f"Service table {table!r} declares unknown keys: {unknown}")
    key = declaration.get("key")
    if isinstance(key, str) or not isinstance(key, (tuple, list)) or not key:
        raise ValueError(f"Service table {table!r} requires a conflict key sequence")
    key = tuple(key)
    if any(
        not isinstance(column, str) or _SQL_NAME.fullmatch(column) is None
        for column in key
    ):
        raise ValueError(f"Service table {table!r} conflict key must be column names")
    if len(set(key)) != len(key):
        raise ValueError(f"Service table {table!r} repeats a conflict key column")
    identity = declaration.get("identity")
    if identity is not True and identity is not False:
        raise ValueError(f"Service table {table!r} must declare identity as a boolean")
    tenant_scope = declaration.get("tenant_scope")
    if tenant_scope not in SERVICE_TENANT_SCOPES:
        raise ValueError(
            f"Service table {table!r} must declare tenant_scope in"
            f" {SERVICE_TENANT_SCOPES}"
        )
    tenant_key = declaration.get("tenant_key")
    if not isinstance(tenant_key, str) or _SQL_NAME.fullmatch(tenant_key) is None:
        raise ValueError(f"Service table {table!r} must name its tenant key column")
    return {
        "key": key,
        "identity": identity,
        "tenant_scope": tenant_scope,
        "tenant_key": tenant_key,
    }


def set_service_tables(tables=None):
    """Register the host service's own tables on the kanban connection.

    Trusted boot registration, with the same lifetime rules as
    ``set_board_dsn_resolver``: process-global, made once before threads start,
    ``None`` clears it, subprocesses register independently.

    Nothing is admitted implicitly. Each table names the conflict key its
    ``INSERT OR IGNORE``/``INSERT OR REPLACE`` statements resolve against,
    whether its primary key is a generated identity (so a write knows whether
    ``RETURNING id`` is meaningful), and how a row is attributed to a board. A
    table that is not registered stays outside the contract and its writes are
    still refused -- this widens the contract by declaration, it does not open
    it.
    """
    global _SERVICE_TABLES
    if tables is None:
        _SERVICE_TABLES = {}
        return
    if not isinstance(tables, dict) or not tables:
        raise ValueError("Service table registration requires a non-empty mapping")
    _SERVICE_TABLES = {
        table: _service_table_entry(table, declaration)
        for table, declaration in tables.items()
    }


def has_service_tables():
    return bool(_SERVICE_TABLES)


def service_tables():
    """The registered declarations, copied so a caller cannot widen them."""
    return {table: dict(entry) for table, entry in _SERVICE_TABLES.items()}


def service_table_keys():
    return {table: entry["key"] for table, entry in _SERVICE_TABLES.items()}


def service_identity_tables():
    return frozenset(
        table for table, entry in _SERVICE_TABLES.items() if entry["identity"]
    )


def check_table(table):
    if table not in TABLE_KEYS and table not in _SERVICE_TABLES:
        raise ValueError("Table is outside the kanban persistence contract")


def resolve_backend(backend=None, *, env_var="HERMES_KANBAN_BACKEND"):
    """Pick the kanban backend: explicit argument > explicit env > registered resolver > sqlite.

    A registered board DSN resolver *is* the PostgreSQL authority declaration
    (``set_board_dsn_resolver`` is only called after the host app decided the
    service board is authoritative). Until 2026-09-20 this function read only
    the env var, so a process that had registered a resolver still answered
    "sqlite": the host's board_store lineage wrote PostgreSQL while this core
    lineage kept reading SQLite -- cards were created in PG and then could not
    be claimed (404). One decision, two sources of truth. The env var still
    outranks the registration so an operator can force SQLite without
    unregistering anything.
    """
    if backend is not None:
        selected = backend
    else:
        env_choice = os.environ.get(env_var)
        if env_choice:
            selected = env_choice
        elif env_var == "HERMES_KANBAN_BACKEND" and has_board_dsn_resolver():
            selected = "postgres"
        else:
            selected = "sqlite"
    if selected not in ("sqlite", "postgres"):
        raise ValueError(f"{env_var} must be sqlite or postgres")
    return selected


class SQLiteDialect:
    backend = "sqlite"
    uses_sqlite_files = True

    def order_by(self, expression):
        return expression

    def notify_platform_equals(self):
        return "LOWER(platform) = LOWER(?)"

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
