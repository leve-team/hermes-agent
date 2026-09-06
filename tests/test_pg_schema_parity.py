"""Static SQLite/PostgreSQL schema-parity guard.

This is the CI gate that keeps the two backends from drifting apart again.

Background
----------
The two backends managed schema by different mechanisms and only one was
self-healing: SQLite's ``_reconcile_columns()`` diffs live columns against
``SCHEMA_SQL`` on every startup and ADDs what is missing, while Postgres
carried a hand-maintained list of ``ALTER TABLE ... ADD COLUMN`` statements.
The standing invariant was therefore *any column added to SCHEMA_SQL is
automatically live on SQLite and silently absent on Postgres*. Four columns and
one whole table drifted before anyone noticed, because the failure logged at
WARNING and the turn completed normally — the deployment lost its entire session
history to it.

``reconcile_postgres_columns`` closes that at runtime. This file closes it at
CI time, which matters because the runtime fix repairs a *live database* while
this catches the mistake in the *diff*.

Why this file is deliberately Docker-free
-----------------------------------------
``tests/test_pg_parity_smoke.py`` is the behavioral oracle, but it skips
entirely when Docker is unreachable — and it skipped on the machine where this
drift shipped. A guard that only runs when a daemon happens to be up is not a
guard. Everything here is pure string/AST analysis of the two schema literals,
so it runs everywhere, always. The one exception is
``test_reconciler_converges_live_database``, which needs a real server and skips
cleanly without one.
"""

import re
import sqlite3

import pytest

from hermes_state import SCHEMA_SQL, SCHEMA_VERSION, SessionDB
from hermes_state_dual import MIGRATED_TABLES
from hermes_state_postgres import (
    PG_INTEGER_COLUMNS,
    SCHEMA_SQL_POSTGRES,
    _PG_ONLY_MIGRATIONS,
    _pg_column_type,
    plan_postgres_migrations,
    reconcile_postgres_columns,
)

# ---------------------------------------------------------------------------
# Tables in SCHEMA_SQL that intentionally do NOT exist on PostgreSQL. This is
# an allowlist, not a blanket exemption — a NEW table added to
# SCHEMA_SQL and not to SCHEMA_SQL_POSTGRES fails the table-parity test until
# someone either adds it to Postgres or justifies it here. That is the point:
# the exemption has to be argued, not inherited.
# ---------------------------------------------------------------------------
SQLITE_LOCAL_TABLES = set()


def _sqlite_tables() -> dict:
    """Table -> ordered column names, parsed from SCHEMA_SQL by SQLite itself."""
    ref = sqlite3.connect(":memory:")
    try:
        ref.executescript(SCHEMA_SQL)
        out = {}
        for (tbl,) in ref.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall():
            out[tbl] = [r[1] for r in ref.execute(f'PRAGMA table_info("{tbl}")')]
        return out
    finally:
        ref.close()


def _pg_ddl_tables(schema_sql=SCHEMA_SQL_POSTGRES) -> dict:
    """Table -> column names/types declared in the PostgreSQL DDL.

    Hand-parsed rather than executed: the literal is PostgreSQL dialect, so
    SQLite cannot run it and no PG server is required.

    Two things the parse must get right, both of which silently produce phantom
    columns if skipped: ``--`` line comments are stripped first (comment prose
    contains commas, which would fracture into fake column entries), and the
    column list is split on TOP-LEVEL commas only, so inline
    ``REFERENCES``/``PRIMARY KEY (...)`` clauses stay attached to their column.
    """
    # Strip -- line comments before any splitting; their prose contains commas.
    schema = re.sub(r"--[^\n]*", "", schema_sql)

    tables = {}
    for match in re.finditer(
        r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\n\);", schema, re.S
    ):
        table, body = match.group(1), match.group(2)
        parts, depth, current = [], 0, ""
        for ch in body:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                parts.append(current)
                current = ""
            else:
                current += ch
        parts.append(current)

        columns = {}
        for part in parts:
            part = part.strip()
            if not part:
                continue
            head = part.split()[0].upper()
            if head in {"PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT"}:
                continue
            fields = part.split()
            columns[fields[0]] = fields[1].upper()
        tables[table] = columns
    return tables


def _migration_added_columns() -> dict:
    """Table -> columns added by ``ALTER TABLE ... ADD COLUMN`` in the migrations.

    A column may legitimately live only in a migration (it converges an
    existing database) as long as it is ALSO in the CREATE TABLE literal, which
    is what a fresh database gets. ``test_migrations_are_not_the_only_home``
    enforces that pairing.
    """
    added = {}
    for migration in _PG_ONLY_MIGRATIONS:
        for match in re.finditer(
            r"ALTER TABLE (\w+)\s+ADD COLUMN IF NOT EXISTS\s+(\w+)",
            migration.sql,
        ):
            added.setdefault(match.group(1), set()).add(match.group(2))
    return added


def test_every_sqlite_table_exists_on_postgres():
    """No table may be declared for SQLite and silently missing on Postgres.

    ``session_model_usage`` was exactly this failure: declared in SCHEMA_SQL,
    written by ``_record_model_usage`` through the shared SessionDB write path,
    and entirely absent from the Postgres schema. It never reached the error
    logs only because session creation failed first and masked it.
    """
    missing = sorted(
        set(_sqlite_tables()) - set(_pg_ddl_tables()) - SQLITE_LOCAL_TABLES
    )
    assert not missing, (
        f"tables declared in SCHEMA_SQL but absent from SCHEMA_SQL_POSTGRES: "
        f"{missing}. Add them to SCHEMA_SQL_POSTGRES (plus a migration so "
        f"existing databases converge), or add them to SQLITE_LOCAL_TABLES "
        f"with a comment explaining why the Postgres path never touches them."
    )


def test_every_migrated_table_is_declared_in_postgres_schema():
    """COPY/diff targets must have a destination table on PostgreSQL."""
    missing = sorted(set(MIGRATED_TABLES) - set(_pg_ddl_tables()))
    assert not missing, (
        "tables selected for COPY/hash diff but absent from "
        f"SCHEMA_SQL_POSTGRES: {missing}"
    )


def test_every_sqlite_column_exists_on_postgres():
    """The core invariant: declaring a column must reach BOTH backends.

    Counts a column as present if it is in the CREATE TABLE literal or added by
    a migration — either path gets it onto a live database.
    """
    sqlite_tables = _sqlite_tables()
    pg_tables = _pg_ddl_tables()
    migration_added = _migration_added_columns()

    drift = {}
    for table, columns in sqlite_tables.items():
        if table not in pg_tables:
            continue  # covered by the table-parity test above
        present = set(pg_tables[table]) | migration_added.get(table, set())
        missing = [c for c in columns if c not in present]
        if missing:
            drift[table] = missing

    assert not drift, (
        f"columns declared in SCHEMA_SQL but absent from the Postgres schema: "
        f"{drift}. Add them to SCHEMA_SQL_POSTGRES and to a migration. "
        f"reconcile_postgres_columns() would heal a live database at runtime, "
        f"but the declaration should not be missing in the first place."
    )


def test_migrations_are_not_the_only_home_for_a_column():
    """A column added by migration must also be in the CREATE TABLE literal.

    Otherwise a *fresh* database is created without it and only converges on a
    later connect — the exact asymmetry (fresh-vs-existing databases having
    different shapes) this whole guard exists to prevent.
    """
    pg_tables = _pg_ddl_tables()
    orphans = {}
    for table, columns in _migration_added_columns().items():
        if table not in pg_tables:
            continue
        missing = sorted(columns - set(pg_tables[table]))
        if missing:
            orphans[table] = missing

    assert not orphans, (
        f"columns added only by migration and missing from the CREATE TABLE "
        f"literal: {orphans}. A fresh database would not have them."
    )


def _assert_pg_integer_allowlist(schema_sql):
    integers = {
        (table, column)
        for table, columns in _pg_ddl_tables(schema_sql).items()
        for column, data_type in columns.items()
        if data_type == "INTEGER"
    }
    assert integers == PG_INTEGER_COLUMNS, (
        f"unreviewed INTEGER columns: {sorted(integers - PG_INTEGER_COLUMNS)}; "
        f"stale INTEGER allowlist: {sorted(PG_INTEGER_COLUMNS - integers)}"
    )


def test_pg_integer_columns_require_explicit_allowlist():
    _assert_pg_integer_allowlist(SCHEMA_SQL_POSTGRES)


def test_sqlite_postgres_type_width_parity():
    pg_tables = _pg_ddl_tables()
    for table, columns in SessionDB._parse_schema_columns(SCHEMA_SQL).items():
        if table in SQLITE_LOCAL_TABLES:
            continue
        for column, declared in columns.items():
            mapped = _pg_column_type(declared, table=table, column=column)
            assert mapped is not None, (table, column, declared)
            assert pg_tables[table][column] == mapped.split()[0], (
                table, column, declared, pg_tables[table][column]
            )


def test_every_declared_sqlite_type_maps_to_postgres():
    """Every type in SCHEMA_SQL must have a Postgres mapping.

    ``_pg_column_type`` returns None for an unmapped type and the reconciler
    then SKIPS the column — safe, but it means a new type silently disables
    reconciliation for that column. Failing here makes that visible in the diff
    that introduces the type, rather than in a WARNING nobody reads.
    """
    unmapped = []
    for table, columns in SessionDB._parse_schema_columns(SCHEMA_SQL).items():
        for column, declared in columns.items():
            if _pg_column_type(declared) is None:
                unmapped.append(f"{table}.{column} ({declared!r})")

    assert not unmapped, (
        f"column types with no entry in _PG_TYPE_MAP: {unmapped}. "
        f"reconcile_postgres_columns() will skip these columns. Add the type "
        f"to _PG_TYPE_MAP in hermes_state_postgres.py."
    )


def test_pg_type_mapping_preserves_constraints():
    """The type is translated; the NOT NULL / DEFAULT tail is passed through."""
    assert _pg_column_type("TEXT") == "TEXT"
    assert _pg_column_type("REAL") == "DOUBLE PRECISION"
    assert _pg_column_type("REAL NOT NULL DEFAULT 0") == (
        "DOUBLE PRECISION NOT NULL DEFAULT 0"
    )
    assert _pg_column_type("INTEGER NOT NULL DEFAULT 1") == (
        "BIGINT NOT NULL DEFAULT 1"
    )
    assert _pg_column_type(
        "INTEGER NOT NULL DEFAULT 0", table="sessions", column="rewind_count"
    ) == (
        "INTEGER NOT NULL DEFAULT 0"
    )
    # A typeless SQLite column is a real possibility; TEXT is the safe analogue.
    assert _pg_column_type("") == "TEXT"
    # An unknown type is refused, not guessed at.
    assert _pg_column_type("GEOMETRY") is None


def test_drifted_columns_are_declared_and_migrated():
    """Regression lock for the four columns that caused the deployment-wide loss.

    Named explicitly rather than left to the generic tests: these are the ones
    that actually cost data, and a test that names them makes the reason for
    this file legible to whoever reads it next. Asserts both homes — the
    CREATE TABLE literal (fresh databases) and a migration (existing ones).
    """
    pg_tables = _pg_ddl_tables()
    migration_added = _migration_added_columns()

    for table, column in [
        ("sessions", "profile_name"),
        ("sessions", "compression_fallback_streak"),
        ("messages", "effect_disposition"),
        ("messages", "api_content"),
    ]:
        assert column in pg_tables[table], (
            f"{table}.{column} missing from SCHEMA_SQL_POSTGRES — a fresh "
            f"database would not have it"
        )
        assert column in migration_added.get(table, set()), (
            f"{table}.{column} has no migration — existing databases "
            f"would never converge"
        )


def test_session_model_usage_is_created_by_a_migration():
    """The missing table must converge on existing databases, not just fresh ones.

    It is absent from every already-deployed database, so a CREATE TABLE in
    the literal alone would leave every existing installation without it.
    """
    assert "session_model_usage" in _pg_ddl_tables()
    assert any(
        "CREATE TABLE IF NOT EXISTS session_model_usage" in m.sql
        for m in _PG_ONLY_MIGRATIONS
    ), (
        "session_model_usage is declared for fresh databases but no migration "
        "creates it, so existing databases never get it"
    )


def test_migration_versions_are_unique_and_ordered():
    """Duplicate or out-of-order versions silently skip a migration.

    ``plan_postgres_migrations`` selects ``version > current``, so a duplicate
    version means the second one never applies.
    """
    versions = [m.version for m in _PG_ONLY_MIGRATIONS]
    assert len(versions) == len(set(versions)), f"duplicate versions: {versions}"
    assert versions == sorted(versions), f"versions out of order: {versions}"


def test_migration_sql_has_no_semicolons_in_literals():
    """``_apply_single_migration`` splits statements on a naive ``;``.

    A semicolon inside a quoted literal or a ``$$`` block would fracture a
    statement into invalid halves. The migrations use ``DEFAULT ''`` (empty
    literals) which is safe; this locks that in so a future migration carrying
    a semicolon inside a literal fails loudly here instead of at deploy time.
    """
    for migration in _PG_ONLY_MIGRATIONS:
        assert "$$" not in migration.sql, (
            f"v{migration.version} uses a $$ block; the naive ';' split in "
            f"_apply_single_migration would fracture it"
        )
        for literal in re.findall(r"'([^']*)'", migration.sql):
            assert ";" not in literal, (
                f"v{migration.version} has ';' inside the literal {literal!r}; "
                f"the naive split in _apply_single_migration would break it"
            )


# ---------------------------------------------------------------------------
# Reconciler behaviour against a recorded connection.
#
# These need no Docker: a fake connection reports the live columns and records
# the DDL emitted. That is enough to assert WHAT the reconciler does (which
# ALTERs, which tables it refuses to touch, idempotency) even where a real
# server is unavailable — which is most CI runners and was the machine the
# original drift shipped from. ``test_reconciler_converges_live_database``
# below is the real-server counterpart.
# ---------------------------------------------------------------------------

class _RecordingCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _RecordingConn:
    """A connection that reports *live_columns* and records executed DDL."""

    def __init__(self, live_columns):
        self.live_columns = live_columns
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if "information_schema.columns" in sql:
            return _RecordingCursor(
                [(t, c) for t, cols in self.live_columns.items() for c in cols]
            )
        return _RecordingCursor([])


def _live_shape(drop=()):
    """The declared shared shape, minus *drop*, as information_schema rows."""
    tables = _sqlite_tables()
    shape = {}
    for table, columns in tables.items():
        if table in SQLITE_LOCAL_TABLES or table == "session_model_usage":
            continue  # absent on the drifted database, by construction
        shape[table] = [c for c in columns if (table, c) not in drop]
    return shape


def test_reconciler_adds_exactly_the_missing_columns():
    """Reproduces schema drift on a live database and asserts the emitted DDL.

    The reconciler must add the four drifted columns with correct Postgres
    types — note ``compression_fallback_streak`` carries its NOT NULL DEFAULT
    across, so existing rows get 0 rather than a constraint violation.
    """
    dropped = (
        ("sessions", "profile_name"),
        ("sessions", "compression_fallback_streak"),
        ("messages", "effect_disposition"),
        ("messages", "api_content"),
    )
    conn = _RecordingConn(_live_shape(drop=dropped))

    added = reconcile_postgres_columns(conn, SCHEMA_SQL)

    assert sorted(added) == sorted(f"{t}.{c}" for t, c in dropped)

    alters = [s for s in conn.executed if s.startswith("ALTER")]
    assert any(
        'ADD COLUMN IF NOT EXISTS "profile_name" TEXT' in s for s in alters
    )
    assert any(
        'ADD COLUMN IF NOT EXISTS "compression_fallback_streak"'
        " INTEGER NOT NULL DEFAULT 0" in s
        for s in alters
    )


def test_reconciler_never_creates_a_table():
    """ADD COLUMN only — the Chesterton's-fence boundary.

    ``session_model_usage`` is absent from the fake live shape here and must be
    left to the base schema/migration rather than conjured by the reconciler.
    """
    conn = _RecordingConn(_live_shape())

    reconcile_postgres_columns(conn, SCHEMA_SQL)

    assert not [s for s in conn.executed if "CREATE TABLE" in s.upper()]
    assert not [s for s in conn.executed if "session_model_usage" in s]


def test_reconciler_is_idempotent_on_a_converged_database():
    """A converged database must produce zero DDL — it runs on every connect."""
    conn = _RecordingConn(_live_shape())

    assert reconcile_postgres_columns(conn, SCHEMA_SQL) == []
    assert not [s for s in conn.executed if s.startswith("ALTER")]


def test_reconciler_skips_an_unmappable_type_without_emitting_ddl():
    """An unmapped type is skipped, not guessed at.

    A wrong ADD COLUMN is worse than a missing one: it is not idempotently
    repairable. ``test_every_declared_sqlite_type_maps_to_postgres`` is what
    stops such a column from being introduced silently in the first place.
    """
    schema = """
    CREATE TABLE sessions (
        id TEXT PRIMARY KEY,
        exotic GEOMETRY
    );
    """
    conn = _RecordingConn({"sessions": ["id"]})

    added = reconcile_postgres_columns(conn, schema)

    assert added == []
    assert not [s for s in conn.executed if s.startswith("ALTER")]


def test_reconciler_continues_after_one_column_fails():
    """One failing ALTER must not abort the rest — partial convergence beats none.

    The failing column is forced to be the FIRST one attempted, and a second
    missing column follows it in the same table. If the loop broke instead of
    continuing, the later column would never be attempted — which is precisely
    what this asserts. (An earlier version of this test let declaration order
    decide, so the failing column landed last and the assertion held even with
    ``break``; it passed a mutation it should have caught.)
    """
    order = list(SessionDB._parse_schema_columns(SCHEMA_SQL)["sessions"])
    first_missing, second_missing = order[-2], order[-1]

    class _FlakyConn(_RecordingConn):
        def execute(self, sql, params=None):
            if f'"{first_missing}"' in sql and sql.startswith("ALTER"):
                self.executed.append(sql)
                raise RuntimeError("simulated ALTER failure")
            return super().execute(sql, params)

    conn = _FlakyConn(
        _live_shape(
            drop=(("sessions", first_missing), ("sessions", second_missing))
        )
    )

    added = reconcile_postgres_columns(conn, SCHEMA_SQL)

    assert f"sessions.{first_missing}" not in added   # the one that failed
    assert f"sessions.{second_missing}" in added      # attempted anyway


# ---------------------------------------------------------------------------
# Live-server convergence. Skips cleanly without Docker (see module docstring)
# — but when it does run, it is the proof that the reconciler actually repairs
# a drifted database rather than merely being well-formed.
# ---------------------------------------------------------------------------

def test_reconciler_converges_live_database():
    """Drop a column from a live database; the reconciler must put it back.

    This is the end-to-end proof: it reproduces the real failure shape
    (a column present in SCHEMA_SQL and absent from the live table) and asserts
    the reconciler heals it, rather than asserting the healing code merely
    exists.
    """
    psycopg = pytest.importorskip("psycopg", reason="psycopg not installed")
    tc = pytest.importorskip("testcontainers.postgres", reason="testcontainers absent")

    import os
    import socket
    from pathlib import Path

    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

    endpoint = os.environ.get("DOCKER_HOST")
    if not endpoint:
        for sock in (
            Path("/var/run/docker.sock"),
            Path.home() / ".colima" / "default" / "docker.sock",
            Path.home() / ".docker" / "run" / "docker.sock",
        ):
            if sock.exists():
                endpoint = "unix://" + str(sock)
                break
    if not endpoint:
        pytest.skip("no docker endpoint")
    if endpoint.startswith("unix://"):
        path = endpoint[len("unix://"):]
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)
                s.connect(path)
        except OSError:
            pytest.skip("docker daemon not reachable")
    os.environ["DOCKER_HOST"] = endpoint

    from hermes_state_postgres import (
        SCHEMA_SQL_POSTGRES as pg_schema,
        reconcile_postgres_columns,
    )

    with tc.PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url()
        scheme, sep, rest = url.partition("://")
        if sep and scheme in ("postgresql+psycopg2", "postgresql+psycopg"):
            url = "postgresql" + sep + rest

        with psycopg.connect(url, autocommit=True) as conn:
            for statement in pg_schema.split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(statement)

            # Reproduce the production shape: the column is declared in
            # SCHEMA_SQL but missing from the live table.
            conn.execute("ALTER TABLE sessions DROP COLUMN profile_name")
            conn.execute("ALTER TABLE messages DROP COLUMN effect_disposition")

            added = reconcile_postgres_columns(conn, SCHEMA_SQL)

            assert "sessions.profile_name" in added
            assert "messages.effect_disposition" in added

            live = {
                (t, c)
                for t, c in conn.execute(
                    "SELECT table_name, column_name FROM information_schema.columns"
                    " WHERE table_schema = current_schema()"
                ).fetchall()
            }
            assert ("sessions", "profile_name") in live
            assert ("messages", "effect_disposition") in live

            # Idempotent: a second pass is a no-op, not a duplicate-column error.
            assert reconcile_postgres_columns(conn, SCHEMA_SQL) == []

            # Extra state.db ledgers are created by the base schema, not by the
            # ADD-COLUMN reconciler.
            assert any(t == "async_delegations" for t, _ in live)


# ---------------------------------------------------------------------------
# Migration-ledger separation
#
# Postgres-only migrations are numbered in their own small sequence (starting
# at 17) and are recorded in ``pg_migration_version``. The shared
# ``SCHEMA_VERSION`` is recorded in ``schema_version`` and climbs with every
# upstream column addition.
#
# These were once the same table. The moment the shared version passed the
# highest Postgres-only migration number, ``MAX(version)`` made every
# Postgres-only migration look already-applied, and they stopped running.
# The column reconciler hid it for ADD COLUMN, so the visible symptom was only
# the absence of things a reconciler never creates: the pg_trgm extension, the
# GIN indexes, and whole tables.
#
# These tests assert the invariant (two namespaces, never merged), not any
# particular version number, so they stay valid as both counters advance.
# ---------------------------------------------------------------------------


def test_pg_migration_ledger_is_a_separate_table():
    """The Postgres migration ledger must not share ``schema_version``."""
    assert "CREATE TABLE IF NOT EXISTS pg_migration_version" in SCHEMA_SQL_POSTGRES, (
        "pg_migration_version is missing from SCHEMA_SQL_POSTGRES; without it "
        "the Postgres-only migration ledger falls back into schema_version and "
        "collides with the shared SCHEMA_VERSION"
    )


def test_pg_migrations_record_into_the_separate_ledger():
    """``_apply_single_migration`` must write to ``pg_migration_version``.

    Behavioural check via the module's own source of truth: the INSERT target.
    A regression here is silent — migrations would still 'succeed' while
    recording into the shared namespace again.
    """
    import inspect

    from hermes_state_postgres import _apply_single_migration

    src = inspect.getsource(_apply_single_migration)
    assert "INSERT INTO pg_migration_version" in src
    assert "INSERT INTO schema_version" not in src, (
        "_apply_single_migration records the pg-only version into the shared "
        "schema_version table — this is the collision that disabled every "
        "Postgres-only migration"
    )


def test_pg_migration_planning_does_not_read_the_shared_version():
    """``apply_postgres_migrations`` must plan from the pg-only ledger."""
    import inspect

    from hermes_state_postgres import apply_postgres_migrations

    src = inspect.getsource(apply_postgres_migrations)
    assert "postgres_migration_version(" in src
    assert "postgres_schema_version(" not in src, (
        "apply_postgres_migrations plans from the shared schema_version; once "
        "the shared version exceeds the highest pg-only migration number, "
        "every pg-only migration is skipped"
    )


def test_shared_version_has_overtaken_the_pg_only_range():
    """Documents WHY the ledgers must stay separate.

    Not a snapshot of either number: it asserts the relationship that makes a
    single shared namespace unsafe. If this ever fails it means the shared
    version fell below the pg-only range, which would be its own bug.
    """
    highest_pg_only = max(m.version for m in _PG_ONLY_MIGRATIONS)
    assert SCHEMA_VERSION > highest_pg_only, (
        f"shared SCHEMA_VERSION ({SCHEMA_VERSION}) no longer exceeds the "
        f"highest pg-only migration ({highest_pg_only}); the two counters "
        "overlap, which is exactly the condition that made one shared "
        "MAX(version) unsafe"
    )


def test_every_pg_only_migration_statement_is_idempotent():
    """Re-running the list must be a no-op where objects already exist.

    A database predating the ledger split has no ``pg_migration_version`` row,
    so it replans from 0 and replays every migration. That is only safe — and
    only self-healing for a database whose ledger claimed versions its objects
    never got — if every creating statement is guarded.
    """
    unguarded = []
    for migration in _PG_ONLY_MIGRATIONS:
        for statement in migration.sql.split(";"):
            normalized = " ".join(statement.split())
            if not normalized:
                continue
            upper = normalized.upper()
            if not upper.startswith(("CREATE", "ALTER TABLE")):
                continue
            if "IF NOT EXISTS" in upper or "OR REPLACE" in upper:
                continue
            unguarded.append(f"v{migration.version}: {normalized[:80]}")

    assert not unguarded, (
        "unguarded statement(s) in the Postgres-only migrations; replaying the "
        "list on a database that already has these objects would fail:\n  "
        + "\n  ".join(unguarded)
    )


# ---------------------------------------------------------------------------
# Managed-Postgres degradation
#
# Hosted PostgreSQL restricts which extensions a non-superuser may create, and
# some providers ship an empty allow-list by default. Creating pg_trgm then
# fails with a permission / FeatureNotSupported error that retrying will never
# clear.
#
# pg_trgm only ACCELERATES search: the ILIKE path is plain SQL and the FTS path
# is core PostgreSQL tsvector. So the correct behaviour when it cannot be
# installed is to degrade, not to refuse to start. A non-optional v17 made the
# error propagate out of SessionDB.__init__ and the agent could not start at
# all against an otherwise healthy database.
# ---------------------------------------------------------------------------


def test_extension_migration_is_optional():
    """pg_trgm must never be able to block startup.

    Asserts the property (every migration whose SQL creates an EXTENSION is
    optional) rather than pinning v17 specifically, so a future extension
    migration inherits the guard.
    """
    fatal_extension_migrations = [
        m.version
        for m in _PG_ONLY_MIGRATIONS
        if "CREATE EXTENSION" in m.sql.upper() and not m.optional
    ]
    assert not fatal_extension_migrations, (
        "migration(s) "
        f"{fatal_extension_migrations} create an extension with optional=False. "
        "Managed PostgreSQL can deny extension creation to non-superusers; a "
        "fatal migration there means the agent cannot start at all, even "
        "though search works without the extension (ILIKE is plain SQL and "
        "the FTS path is core tsvector)."
    )


def test_index_migrations_that_need_an_extension_are_optional():
    """Indexes using an extension operator class must also be optional.

    They cannot be built when the extension is absent, so a fatal migration
    here reintroduces the startup failure one step later.
    """
    fatal = [
        m.version
        for m in _PG_ONLY_MIGRATIONS
        if "GIN_TRGM_OPS" in m.sql.upper() and not m.optional
    ]
    assert not fatal, (
        f"migration(s) {fatal} build trigram indexes with optional=False; "
        "these fail when pg_trgm could not be installed"
    )


def test_planner_retries_a_gap_below_the_high_water_mark():
    """An optional migration that failed must not be stranded.

    A failing optional migration does not record its version, but later
    migrations still succeed and do. Planning purely on ``> MAX(version)``
    would therefore skip the failed one forever — so a provider-denied
    extension would never be installed even after an operator allow-lists it.
    """
    versions = sorted(m.version for m in _PG_ONLY_MIGRATIONS)
    assert len(versions) >= 3, "need several migrations to model a gap"

    lowest, highest = versions[0], versions[-1]
    # Everything recorded except the lowest: the shape left behind when an
    # early optional migration failed and the rest succeeded.
    applied = set(versions) - {lowest}

    planned = [m.version for m in plan_postgres_migrations(highest, applied)]
    assert lowest in planned, (
        f"v{lowest} was skipped despite never being recorded; a gap below the "
        "high-water mark must be replayed or a failed optional migration is "
        "stranded permanently"
    )

    # Nothing already recorded should be replanned.
    assert not (set(planned) & applied), (
        f"replanned already-applied migrations: {sorted(set(planned) & applied)}"
    )


def test_planner_is_a_noop_when_everything_is_applied():
    """The steady state: no gaps, nothing above the mark, nothing to do."""
    versions = {m.version for m in _PG_ONLY_MIGRATIONS}
    planned = plan_postgres_migrations(max(versions), versions)
    assert planned == [], f"unexpected replanning: {[m.version for m in planned]}"


def test_planner_without_applied_set_preserves_legacy_behaviour():
    """Omitting ``applied`` keeps the plain high-water-mark semantics."""
    versions = sorted(m.version for m in _PG_ONLY_MIGRATIONS)
    planned = [m.version for m in plan_postgres_migrations(versions[0])]
    assert planned == versions[1:], (
        "the no-applied-set call should return strictly the versions above the "
        f"high-water mark, got {planned}"
    )


def test_search_sql_uses_no_trigram_operators():
    """Search must not depend on pg_trgm being installed.

    pg_trgm supplies the GIN indexes that make ILIKE fast; it does not supply
    the search semantics. If a search path ever adopts a trigram operator
    (``%``, ``<->``) or function (``similarity()``, ``word_similarity()``),
    the extension stops being optional and a managed database that forbids it
    can no longer search at all -- so v17 would have to become required again,
    which is the startup failure this guard exists to prevent.

    Checked against the SQL-building functions' own source rather than a live
    server so the guard runs without Docker, and asserts on the query text
    those functions emit rather than on formatting.
    """
    import inspect

    import hermes_state_postgres as hsp

    trigram_tokens = (
        "similarity(",
        "word_similarity(",
        "<->",
        "show_trgm(",
        "gin_trgm_ops",
    )

    offenders = []
    for name in ("_search_messages_ilike", "_search_messages_fts", "_build_where"):
        fn = getattr(hsp, name, None)
        if fn is None:
            continue
        src = inspect.getsource(fn)
        for token in trigram_tokens:
            if token in src:
                offenders.append(f"{name} uses {token!r}")

    assert not offenders, (
        "search path depends on pg_trgm-provided SQL: "
        + "; ".join(offenders)
        + ". The extension must stay an optimization, not a requirement — "
        "managed PostgreSQL can refuse to install it."
    )


# ---------------------------------------------------------------------------
# Migration column-fidelity guard (Blocker 1)
#
# The one-shot SQLite -> PostgreSQL migration used hand-maintained column
# lists (_SESSION_COLUMNS / _MESSAGE_COLUMNS) that had drifted from the live
# schema: 33 of 56 session columns and 18 of 23 message columns were copied,
# silently dropping the rest while reporting "N/N sessions, M/M messages".
#
# The fix replaces those lists with _derive_migration_columns(), which derives
# the column set from SCHEMA_SQL via PRAGMA table_info at runtime.  These
# tests are the static CI gate: they fail when a column is declared in
# SCHEMA_SQL but not present in the derived migration set, without requiring
# Docker or a live PostgreSQL server.
# ---------------------------------------------------------------------------


def test_migration_columns_cover_full_schema():
    """Every column declared in SCHEMA_SQL must be in the derived migration set.

    The primary regression guard for the data-loss bug: a hand-maintained
    column list had drifted to 33/56 session columns and 18/23 message columns,
    silently dropping the rest. The fix derives the set from SCHEMA_SQL at
    runtime; this test asserts that derivation is complete.

    The test is self-updating: any column added to SCHEMA_SQL is automatically
    covered -- there is no list to forget to update.
    """
    from hermes_state_postgres import _derive_migration_columns

    sqlite_tables = _sqlite_tables()

    for table in ("sessions", "messages"):
        declared = sqlite_tables[table]
        derived = _derive_migration_columns(table)
        derived_set = set(derived)
        missing = [c for c in declared if c not in derived_set]
        assert not missing, (
            f"{table}: columns declared in SCHEMA_SQL but absent from "
            f"_derive_migration_columns('{table}'): {missing}. "
            f"These columns would be silently dropped by the migration. "
            f"Do NOT add a hand-maintained fallback list -- fix the derivation."
        )


def test_migration_column_set_matches_pragma_table_info():
    """The derived migration columns must exactly match PRAGMA table_info output.

    Anti-regression for the fix itself: if _derive_migration_columns were
    ever replaced with a hardcoded list (or stopped reading SCHEMA_SQL), the
    list would eventually drift and this test would catch it.  The oracle here
    is an independent fresh sqlite3.connect(':memory:') run -- same data,
    separate code path.
    """
    import sqlite3 as _sqlite3
    from hermes_state import SCHEMA_SQL
    import hermes_state_postgres as hsp

    # Force a fresh derivation (clear cache) to catch any lazy-init bugs.
    orig_cache = hsp._MIGRATION_COLUMNS_CACHE
    hsp._MIGRATION_COLUMNS_CACHE = None
    try:
        for table in ("sessions", "messages"):
            ref = _sqlite3.connect(":memory:")
            try:
                ref.executescript(SCHEMA_SQL)
                expected = [r[1] for r in ref.execute(
                    f'PRAGMA table_info("{table}")'
                )]
            finally:
                ref.close()

            derived = hsp._derive_migration_columns(table)
            assert derived == expected, (
                f"{table}: _derive_migration_columns returned {derived!r} "
                f"but PRAGMA table_info says {expected!r}. The derivation "
                f"must match the authoritative SQLite schema oracle exactly."
            )
    finally:
        hsp._MIGRATION_COLUMNS_CACHE = orig_cache


def test_not_null_defaults_cover_not_null_columns():
    """Every NOT NULL DEFAULT N column in SCHEMA_SQL must be in _NOT_NULL_DEFAULTS.

    When the migration writes a row from an old SQLite backup where a column
    was added later, that column's value will be None.  PostgreSQL rejects an
    explicit NULL for a NOT NULL column even when the schema supplies a DEFAULT,
    because the DEFAULT only applies to omitted columns.  _NOT_NULL_DEFAULTS
    maps (table, column) -> default to substitute.

    Fails when a new NOT NULL DEFAULT N column is added to SCHEMA_SQL without
    a corresponding entry in _NOT_NULL_DEFAULTS -- caught here rather than as
    a NotNullViolation during a real user's migration.

    Columns with no DEFAULT (source, started_at, session_id, role, timestamp)
    are excluded: these are core required fields and a NULL there is a genuinely
    broken row that should fail loudly.
    """
    import sqlite3 as _sqlite3
    from hermes_state import SCHEMA_SQL
    from hermes_state_postgres import _NOT_NULL_DEFAULTS

    ref = _sqlite3.connect(":memory:")
    try:
        ref.executescript(SCHEMA_SQL)
        uncovered = []
        for table in ("sessions", "messages"):
            for row in ref.execute(f'PRAGMA table_info("{table}")'):
                _, name, _, notnull, dflt, _ = row
                if notnull and dflt is not None:
                    if (table, name) not in _NOT_NULL_DEFAULTS:
                        uncovered.append(f"{table}.{name} (DEFAULT {dflt})")
    finally:
        ref.close()

    assert not uncovered, (
        "NOT NULL DEFAULT columns missing from _NOT_NULL_DEFAULTS in "
        "hermes_state_postgres.py: "
        + ", ".join(uncovered)
        + ". Old SQLite rows that predate this column carry NULL, which "
        "PostgreSQL rejects as NotNullViolation during migration. Add the "
        "column and its default to _NOT_NULL_DEFAULTS."
    )
