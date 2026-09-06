"""Offline token-width, required migration and COPY boundary contracts."""

import re
import sqlite3
from contextlib import contextmanager

import pytest

import hermes_state_postgres as postgres
from hermes_state import SCHEMA_SQL, SCHEMA_VERSION
from migrate_state_to_postgres import _copy_batch
from state_transfer import TableSpec, sqlite_table_specs
from tests.test_pg3_copy_contract import assert_copy_merge, pg_primary_keys
from tests.test_pg_schema_parity import _assert_pg_integer_allowlist, _pg_ddl_tables


def counter_columns():
    return {
        (table, column)
        for table, columns in _pg_ddl_tables().items()
        for column in columns
        if column.endswith(("_tokens", "_bytes", "_size")) or column == "token_count"
    }


def migration_alters():
    return postgres._split_sql_statements(postgres._PG_TOKEN_COUNTER_MIGRATION_V18.sql)


def counter_copy_row(table, column, value):
    primary_key = pg_primary_keys(postgres.SCHEMA_SQL_POSTGRES)[table]
    assert column not in primary_key
    spec = TableSpec(table, (*primary_key, column), primary_key)
    types = _pg_ddl_tables()[table]
    row = {
        key: 1 if types[key] == "BIGINT" else f"test-{key}"
        for key in primary_key
    }
    row[column] = value
    return spec, row


class CatalogDriver:
    """Psycopg boundary fake: catalog types, LIKE inheritance and COPY ranges."""

    def __init__(self, *, legacy=False):
        self.tables = _pg_ddl_tables() if legacy else {}
        self.primary_keys = pg_primary_keys(postgres.SCHEMA_SQL_POSTGRES)
        if legacy:
            for table, column in counter_columns():
                if not column.endswith("_bytes"):
                    self.tables[table][column] = "INTEGER"
        self.applied = {migration.version for migration in postgres._PG_ONLY_MIGRATIONS}
        self.version = SCHEMA_VERSION if legacy else None
        self.statements = []
        self.rows = []
        self.description = [("version",)]
        self.rowcount = 0
        self.written = []
        self.staging = {}
        self.copy_columns = []
        self.copy_table = ""
        self.fail_column = None
        self.deny_extension = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self

    def execute(self, sql, params=()):
        self.statements.append(sql)
        self.rows = []
        self.rowcount = 0
        if "information_schema.columns" in sql:
            assert "table_schema = current_schema()" in sql
            self.rows = [
                (table, column, data_type.lower())
                if "data_type" in sql
                else (table, column)
                for table, columns in self.tables.items()
                for column, data_type in columns.items()
            ]
        elif sql.startswith("SELECT MAX(version)"):
            self.rows = [(max(self.applied, default=None),)]
        elif sql.startswith("SELECT version FROM pg_migration_version"):
            self.rows = [(version,) for version in sorted(self.applied)]
        elif sql.startswith("SELECT version FROM schema_version"):
            self.rows = [] if self.version is None else [(self.version,)]
        elif sql.startswith("INSERT INTO schema_version"):
            self.version = params[0]
        elif sql.startswith("INSERT INTO pg_migration_version"):
            self.applied.add(params[0])
        elif sql.startswith("CREATE TEMP TABLE"):
            match = re.fullmatch(
                r'CREATE TEMP TABLE "(\w+)" \(LIKE "(\w+)" INCLUDING DEFAULTS\)'
                r" ON COMMIT DROP",
                sql,
            )
            assert match, sql
            staging, source = match.groups()
            self.staging[staging] = dict(self.tables[source])
        elif sql.startswith("CREATE TABLE IF NOT EXISTS"):
            for table, columns in _pg_ddl_tables(sql + ";").items():
                self.tables.setdefault(table, columns)
        elif sql.startswith("CREATE EXTENSION"):
            if self.deny_extension:
                raise PermissionError("test extension denied")
        elif sql.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX")):
            pass
        elif sql.startswith("ALTER TABLE"):
            if " TYPE BIGINT" in sql:
                match = re.fullmatch(
                    r'ALTER TABLE "?(\w+)"? ALTER COLUMN "?(\w+)"? TYPE BIGINT',
                    sql,
                )
                assert match, sql
                table, column = match.groups()
                if (table, column) == self.fail_column:
                    raise PermissionError("test ALTER denied")
                self.tables[table][column] = "BIGINT"
            else:
                match = re.fullmatch(
                    r'ALTER TABLE "?(\w+)"?\s+ADD COLUMN IF NOT EXISTS'
                    r'\s+"?(\w+)"?\s+(\w+).*',
                    sql,
                    re.S,
                )
                assert match, sql
                table, column, data_type = match.groups()
                self.tables[table].setdefault(column, data_type.upper())
        elif sql.startswith('INSERT INTO "'):
            assert not params, params
            assert_copy_merge(
                sql, tables=self.tables, primary_keys=self.primary_keys,
                copy_table=self.copy_table, copy_columns=self.copy_columns,
            )
            self.rowcount = len(self.written)
        elif re.fullmatch(r'SELECT (?:"\w+", )*"\w+" FROM "\w+" WHERE \((?:"\w+", )*"\w+"\) IN \((?:\(%s(?:, %s)*\)(?:, )?)+\)', sql):
            # _changed_rows 의 대상 PK 안티조인 — 카탈로그 fake 엔 행이 없으므로 전부 신규.
            assert params, "anti-join requires primary-key parameters"
            self.rows = []
        elif sql != "BEGIN" and not sql.startswith("DROP TABLE IF EXISTS"):
            raise AssertionError(f"Unexpected SQL at driver boundary: {sql}")
        return self

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    @contextmanager
    def copy(self, sql):
        match = re.fullmatch(r'COPY "(\w+)" \((.*?)\) FROM STDIN', sql)
        assert match, sql
        self.copy_table, columns = match.groups()
        self.copy_columns = re.findall(r'"(\w+)"', columns)
        yield self

    def write_row(self, values):
        for column, value in zip(self.copy_columns, values, strict=True):
            data_type = self.staging[self.copy_table][column]
            if data_type in {"INTEGER", "BIGINT"} and value is not None:
                bits = 32 if data_type == "INTEGER" else 64
                if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                    raise OverflowError(f"COPY {column} out of range for {data_type}")
        self.written.append(tuple(values))

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.mark.parametrize("table,column", sorted(counter_columns()))
def test_all_token_byte_size_columns_are_bigint(table, column):
    assert _pg_ddl_tables()[table][column] == "BIGINT", (table, column)


def test_v18_covers_every_counter_and_is_required():
    migration = postgres._PG_TOKEN_COUNTER_MIGRATION_V18
    assert migration.version == 18
    assert not migration.optional
    targets = []
    for statement in migration_alters():
        match = re.fullmatch(
            r"ALTER TABLE (\w+) ALTER COLUMN (\w+) TYPE BIGINT", statement
        )
        assert match, statement
        targets.append(match.groups())
    assert set(targets) == counter_columns()
    assert len(targets) == len(set(targets))


def test_historical_create_migrations_do_not_reintroduce_narrow_tokens():
    for migration in postgres._PG_ONLY_MIGRATIONS:
        assert not re.search(r"\b\w+_tokens\s+INTEGER\b", migration.sql)


@pytest.mark.parametrize("column", ["new_tokens", "new_bytes", "new_size", "new_count"])
def test_parity_rejects_new_integer_counters(column):
    schema = postgres.SCHEMA_SQL_POSTGRES.replace(
        "message_count INTEGER", f"{column} INTEGER,\n    message_count INTEGER", 1
    )
    with pytest.raises(AssertionError, match="unreviewed INTEGER columns"):
        _assert_pg_integer_allowlist(schema)
    assert (
        postgres._pg_column_type("INTEGER", table="sessions", column=column) == "BIGINT"
    )


@pytest.mark.parametrize("defer_indexes", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
def test_init_repairs_width_even_with_existing_v18_ledger(legacy, defer_indexes):
    raw = CatalogDriver(legacy=legacy)
    adapter = postgres._PostgresConnection(raw)
    old_ledger = set(raw.applied)
    postgres.init_postgres_schema(adapter, SCHEMA_VERSION, defer_indexes=defer_indexes)
    for table, column in counter_columns():
        assert raw.tables[table][column] == "BIGINT"
    alters = [sql for sql in raw.statements if " TYPE BIGINT" in sql]
    expected = {
        f"ALTER TABLE {table} ALTER COLUMN {column} TYPE BIGINT"
        for table, column in counter_columns()
        if legacy and not column.endswith("_bytes")
    }
    assert set(alters) == expected
    assert raw.applied == old_ledger
    raw.statements.clear()
    postgres.init_postgres_schema(adapter, SCHEMA_VERSION, defer_indexes=defer_indexes)
    assert not any(" TYPE BIGINT" in sql for sql in raw.statements)
    if defer_indexes:
        assert not any(
            "CREATE INDEX" in sql or "CREATE EXTENSION" in sql for sql in raw.statements
        )


@pytest.mark.parametrize("defer_indexes", [False, True])
def test_failed_alter_propagates_and_partial_migration_retries(defer_indexes):
    raw = CatalogDriver(legacy=True)
    raw.fail_column = ("sessions", "cache_read_tokens")
    adapter = postgres._PostgresConnection(raw)
    with pytest.raises(PermissionError, match="test ALTER denied"):
        postgres.init_postgres_schema(
            adapter, SCHEMA_VERSION, defer_indexes=defer_indexes
        )
    assert raw.tables["sessions"]["input_tokens"] == "BIGINT"
    assert raw.tables["sessions"]["cache_read_tokens"] == "INTEGER"
    raw.fail_column = None
    raw.statements.clear()
    postgres.init_postgres_schema(adapter, SCHEMA_VERSION, defer_indexes=defer_indexes)
    assert (
        "ALTER TABLE sessions ALTER COLUMN input_tokens TYPE BIGINT"
        not in raw.statements
    )
    assert (
        "ALTER TABLE sessions ALTER COLUMN cache_read_tokens TYPE BIGINT"
        in raw.statements
    )


@pytest.mark.parametrize("bad_type", [None, "TEXT", "NUMERIC"])
def test_v18_rejects_missing_or_unsupported_counter_types(bad_type):
    raw = CatalogDriver(legacy=True)
    if bad_type is None:
        del raw.tables["sessions"]["cache_read_tokens"]
    else:
        raw.tables["sessions"]["cache_read_tokens"] = bad_type
    with pytest.raises(
        RuntimeError, match=r"PG3 v18 counter width: sessions.cache_read_tokens"
    ):
        postgres.migrate_postgres_token_counters_v18(raw)


def test_missing_token_column_reconciles_as_bigint_before_copy():
    raw = CatalogDriver(legacy=True)
    del raw.tables["sessions"]["cache_read_tokens"]
    postgres.init_postgres_schema(
        postgres._PostgresConnection(raw), SCHEMA_VERSION, defer_indexes=True
    )
    assert raw.tables["sessions"]["cache_read_tokens"] == "BIGINT"
    assert any('"cache_read_tokens" BIGINT DEFAULT 0' in sql for sql in raw.statements)


def test_optional_v17_warning_does_not_suppress_width_repair(caplog):
    raw = CatalogDriver(legacy=True)
    raw.applied.remove(17)
    raw.deny_extension = True
    postgres.init_postgres_schema(postgres._PostgresConnection(raw), SCHEMA_VERSION)
    assert 17 not in raw.applied
    assert "v17 skipped (optional)" in caplog.text
    assert raw.tables["sessions"]["cache_read_tokens"] == "BIGINT"


@pytest.mark.parametrize("table,column", sorted(counter_columns()))
@pytest.mark.parametrize("value", [2**31, 2_690_975_463, 2_507_362_863, 2**63 - 1])
def test_copy_like_inherits_bigint_and_preserves_large_values(table, column, value):
    raw = CatalogDriver(legacy=True)
    adapter = postgres._PostgresConnection(raw)
    postgres.init_postgres_schema(adapter, SCHEMA_VERSION, defer_indexes=True)
    spec, row = counter_copy_row(table, column, value)
    assert _copy_batch(adapter, spec, [row]) == 1
    assert raw.copy_columns == list(spec.columns)
    assert raw.staging[f"_hermes_backfill_{table}"][column] == "BIGINT"
    assert raw.written == [tuple(row[name] for name in spec.columns)]
    assert raw.rollbacks == 0


def test_copy_without_width_repair_reproduces_integer_overflow():
    raw = CatalogDriver(legacy=True)
    spec, row = counter_copy_row("sessions", "cache_read_tokens", 2_690_975_463)
    with pytest.raises(
        OverflowError, match="COPY cache_read_tokens out of range for INTEGER"
    ):
        _copy_batch(raw, spec, [row])
    assert raw.rollbacks == 1
    assert not raw.written


def test_real_sqlite_rows_preserve_reported_values_through_copy():
    source = sqlite3.connect(":memory:")
    source.row_factory = sqlite3.Row
    try:
        source.executescript(SCHEMA_SQL)
        source.execute(
            "INSERT INTO sessions (id, source, started_at, cache_read_tokens)"
            " VALUES (?, ?, ?, ?)",
            ("large-counter", "test", 0, 2_690_975_463),
        )
        source.execute(
            "INSERT INTO session_model_usage (session_id, model, cache_read_tokens)"
            " VALUES (?, ?, ?)",
            ("large-counter", "test-model", 2_507_362_863),
        )
        expected = {
            "sessions": 2_690_975_463,
            "session_model_usage": 2_507_362_863,
        }
        raw = CatalogDriver(legacy=True)
        adapter = postgres._PostgresConnection(raw)
        postgres.init_postgres_schema(adapter, SCHEMA_VERSION, defer_indexes=True)
        for spec in sqlite_table_specs(source):
            assert spec.primary_key == raw.primary_keys[spec.name]
            if spec.name not in expected:
                continue
            row = source.execute(f'SELECT * FROM "{spec.name}"').fetchone()
            raw.written.clear()
            assert _copy_batch(adapter, spec, [row]) == 1
            assert raw.copy_columns == list(spec.columns)
            copied = dict(zip(spec.columns, raw.written[0], strict=True))
            assert copied["cache_read_tokens"] == expected[spec.name]
            assert (
                raw.staging[f"_hermes_backfill_{spec.name}"]["cache_read_tokens"]
                == "BIGINT"
            )
    finally:
        source.close()
