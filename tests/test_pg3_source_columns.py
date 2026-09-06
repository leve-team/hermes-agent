"""Source-only column preservation at offline PostgreSQL driver boundaries."""

import json
import logging
import re
import sqlite3

import pytest

import hermes_state_postgres as postgres
from hermes_state import SCHEMA_SQL, SCHEMA_VERSION, SessionDB
from hermes_state_dual import DualWriteReplicator, RecordingConnection
from migrate_state_to_postgres import (
    InjectedBackfillFault,
    _copy_batch,
    online_backfill,
)
from state_diff import state_diff_connections
from state_reverse import InjectedReverseFault, reverse_backfill
from state_transfer import (
    open_sqlite_snapshot,
    reconcile_transfer_columns,
    sqlite_table_specs,
)
from tests.test_pg3_reverse_tuple_source import TuplePostgresSource
from tests.test_pg_token_counter_width import CatalogDriver


class ExtraColumnCatalog(CatalogDriver):
    def execute(self, sql, params=()):
        added = re.fullmatch(
            r'ALTER TABLE "(\w+)" ADD COLUMN IF NOT EXISTS "(\w+)"'
            r" (DOUBLE PRECISION|BIGINT|TEXT|BYTEA)",
            sql,
        )
        if added:
            table, column, data_type = added.groups()
            if (table, column) == self.fail_column:
                raise PermissionError("test extra ALTER denied")
            self.statements.append(sql)
            self.tables[table].setdefault(column, data_type)
            return self
        if sql.startswith("SET SESSION synchronous_commit"):
            self.statements.append(sql)
            return self
        if sql.startswith("SELECT pg_database_size"):
            self.rows = [(0,)]
            return self
        return super().execute(sql, params)

    def close(self):
        pass


@pytest.fixture
def source():
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    yield connection
    connection.close()


@pytest.fixture
def target():
    raw = ExtraColumnCatalog()
    postgres.init_postgres_schema(
        postgres._PostgresConnection(raw), SCHEMA_VERSION, defer_indexes=True
    )
    raw.statements.clear()
    return raw


def reconcile(source, target):
    return reconcile_transfer_columns(
        source,
        target,
        sqlite_table_specs(source),
        source_dialect="sqlite",
        target_dialect="postgres",
    )


@pytest.mark.parametrize(
    "declared,expected,value",
    [
        ("REAL", "DOUBLE PRECISION", 12.375),
        ("INTEGER", "BIGINT", 2**63 - 1),
        ("TEXT", "TEXT", "보존 ' quoted"),
        ("BLOB", "BYTEA", b"\x00\xff"),
    ],
)
def test_source_pragma_types_are_nullable_idempotent_and_copyable(
    source, target, caplog, declared, expected, value
):
    source.execute(
        f"ALTER TABLE messages ADD COLUMN duration_s {declared} DEFAULT NULL"
    )
    source.execute(
        "INSERT INTO sessions(id, source, started_at) VALUES ('session', 'test', 1)"
    )
    source.executemany(
        "INSERT INTO messages(session_id, role, timestamp, duration_s) VALUES (?, ?, ?, ?)",
        [("session", "user", 1.0, value), ("session", "user", 2.0, None)],
    )
    specs = sqlite_table_specs(source)
    with caplog.at_level(logging.INFO, logger="levos.state_transfer"):
        assert reconcile(source, target) == ["messages.duration_s"]
    assert "messages.duration_s" in caplog.text
    assert [sql for sql in target.statements if "ADD COLUMN" in sql] == [
        f'ALTER TABLE "messages" ADD COLUMN IF NOT EXISTS "duration_s" {expected}'
    ]
    assert all(set(spec.columns) <= set(target.tables[spec.name]) for spec in specs)
    assert target.tables["messages"]["duration_s"] == expected
    spec = next(spec for spec in specs if spec.name == "messages")
    rows = source.execute("SELECT * FROM messages ORDER BY id").fetchall()
    assert _copy_batch(target, spec, rows) == 2
    position = spec.columns.index("duration_s")
    assert [row[position] for row in target.written] == [value, None]
    target.statements.clear()
    assert reconcile(source, target) == []
    assert not any("ADD COLUMN" in sql for sql in target.statements)
    assert source.execute("PRAGMA table_info(messages)").fetchall()[-1][2] == declared


def test_copy_spec_columns_are_subset_of_pg_columns(source, target):
    source.execute("ALTER TABLE messages ADD COLUMN duration_s REAL")
    specs = sqlite_table_specs(source)
    assert "duration_s" not in target.tables["messages"]
    reconcile(source, target)
    for spec in specs:
        assert set(spec.columns) <= set(target.tables[spec.name]), spec.name


def test_source_constraints_are_not_transplanted(source, target):
    source.execute("ALTER TABLE messages ADD COLUMN duration_s REAL NOT NULL DEFAULT 0")
    assert reconcile(source, target) == ["messages.duration_s"]
    assert target.statements[-1] == (
        'ALTER TABLE "messages" ADD COLUMN IF NOT EXISTS "duration_s" DOUBLE PRECISION'
    )
    info = source.execute("PRAGMA table_info(messages)").fetchall()[-1]
    assert info[3] == 1
    assert info[4] == "0"


def test_copy_preserves_99588_synthetic_nonnull_values_and_null(target):
    source = sqlite3.connect(":memory:")
    source.row_factory = sqlite3.Row
    try:
        source.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY, duration_s REAL)")
        values = [(index, index / 8.0) for index in range(1, 99_589)] + [(99_589, None)]
        source.executemany("INSERT INTO messages VALUES (?, ?)", values)
        reconcile(source, target)
        spec = sqlite_table_specs(source)[0]
        assert (
            _copy_batch(
                target, spec, source.execute("SELECT * FROM messages").fetchall()
            )
            == 99_589
        )
        assert target.written == values
        assert sum(row[1] is not None for row in target.written) == 99_588
    finally:
        source.close()


@pytest.mark.parametrize("declaration", ["NUMERIC", "MYSTERY", ""])
def test_unsupported_extra_type_fails_before_any_ddl(source, target, declaration):
    source.execute("ALTER TABLE messages ADD COLUMN duration_s REAL")
    source.execute(f"ALTER TABLE messages ADD COLUMN unsupported {declaration}")
    with pytest.raises(RuntimeError, match="unsupported sqlite type"):
        reconcile(source, target)
    assert not any("ADD COLUMN" in sql for sql in target.statements)


def test_unsafe_extra_identifier_fails_before_ddl(source, target):
    source.execute('ALTER TABLE messages ADD COLUMN "bad;name" REAL')
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        reconcile(source, target)
    assert not any("ADD COLUMN" in sql for sql in target.statements)


def test_missing_target_table_is_not_silently_skipped(source, target):
    del target.tables["messages"]
    with pytest.raises(RuntimeError, match="missing source/target table 'messages'"):
        reconcile(source, target)


def test_existing_extra_does_not_change_type_or_values(source, target):
    source.execute("ALTER TABLE messages ADD COLUMN duration_s REAL")
    target.tables["messages"]["duration_s"] = "DOUBLE PRECISION"
    assert reconcile(source, target) == []
    assert not any("ALTER TABLE" in sql for sql in target.statements)


@pytest.mark.parametrize("deny_alter", [False, True])
def test_online_entrypoint_reconciles_before_like_copy_or_checkpoint(
    tmp_path, deny_alter
):
    path = tmp_path / "source.db"
    checkpoint = tmp_path / "copy.json"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE messages(id INTEGER PRIMARY KEY, duration_s REAL)"
        )
        connection.executemany(
            "INSERT INTO messages VALUES (?, ?)", [(1, 12.375), (2, None)]
        )
    raw = ExtraColumnCatalog()
    raw.fail_column = ("messages", "duration_s") if deny_alter else None
    options = {
        "checkpoint_path": checkpoint,
        "fault_inject_at": "100%",
        "_target_factory": lambda _dsn: postgres._PostgresConnection(raw),
    }
    expected_error = PermissionError if deny_alter else InjectedBackfillFault
    with pytest.raises(expected_error):
        online_backfill(path, "test-only", **options)
    if deny_alter:
        assert not checkpoint.exists()
        assert raw.staging == {}
        raw.fail_column = None
        with pytest.raises(InjectedBackfillFault):
            online_backfill(path, "test-only", **options)
    assert raw.written == [(1, 12.375), (2, None)]
    assert raw.staging["_hermes_backfill_messages"]["duration_s"] == "DOUBLE PRECISION"
    assert json.loads(checkpoint.read_text())["tables"]["messages"]["rows"] == 2
    with open_sqlite_snapshot(path) as snapshot:
        assert [
            tuple(row) for row in snapshot.execute("SELECT * FROM messages")
        ] == raw.written


def seed_extra_messages(path):
    database = SessionDB(db_path=path, dual_write=False)
    database.create_session("session", source="test")
    for timestamp in range(1, 4):
        database.append_message("session", "user", "body", timestamp=float(timestamp))
    database.close()
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE messages ADD COLUMN duration_s REAL")
        connection.execute("ALTER TABLE messages ADD COLUMN legacy_count INTEGER")
        connection.execute("ALTER TABLE messages ADD COLUMN legacy_text TEXT")
        connection.execute("ALTER TABLE messages ADD COLUMN legacy_blob BLOB")
        connection.execute("ALTER TABLE messages ADD COLUMN fts_content TEXT")
        connection.executemany(
            "UPDATE messages SET duration_s=?, legacy_count=?, legacy_text=?,"
            " legacy_blob=?, fts_content='derived' WHERE id=?",
            [
                (12.375, 2**63 - 1, "보존", b"\x00\xff", 1),
                (0.0, -(2**63), "", b"", 2),
                (None, None, None, None, 3),
            ],
        )


@pytest.mark.parametrize("interrupt", [False, True])
@pytest.mark.parametrize("preexisting_fts", [False, True])
def test_reverse_preserves_extra_values_excludes_fts_and_detects_corruption(
    tmp_path, interrupt, preexisting_fts
):
    source_path = tmp_path / "pg-snapshot.db"
    target_path = tmp_path / "rollback.db"
    seed_extra_messages(source_path)
    options = {
        "batch_rows": 1,
        "_source_factory": lambda _dsn: TuplePostgresSource(source_path),
    }
    if preexisting_fts:
        with pytest.raises(InjectedReverseFault):
            reverse_backfill("test-only", target_path, fault_inject_at="1%", **options)
        with sqlite3.connect(target_path) as connection:
            connection.execute(
                "ALTER TABLE messages ADD COLUMN fts_content TEXT DEFAULT 'retained'"
            )
    if interrupt:
        with pytest.raises(InjectedReverseFault):
            reverse_backfill(
                "test-only",
                target_path,
                resume=preexisting_fts,
                fault_inject_at="50%",
                **options,
            )
    summary = reverse_backfill(
        "test-only", target_path, resume=interrupt or preexisting_fts, **options
    )
    assert summary["complete"] is True
    assert summary["diff"]["mismatch_count"] == 0
    source_connection = sqlite3.connect(source_path)
    target_connection = sqlite3.connect(target_path)
    try:
        columns = {
            row[1]: row[2]
            for row in target_connection.execute("PRAGMA table_info(messages)")
        }
        assert ("fts_content" in columns) is preexisting_fts
        if preexisting_fts:
            assert target_connection.execute(
                "SELECT DISTINCT fts_content FROM messages"
            ).fetchall() == [("retained",)]
        assert columns["duration_s"] == "REAL"
        assert columns["legacy_count"] == "INTEGER"
        assert columns["legacy_text"] == "TEXT"
        assert columns["legacy_blob"] == "BLOB"
        query = "SELECT id, duration_s, legacy_count, legacy_text, legacy_blob FROM messages ORDER BY id"
        assert (
            target_connection.execute(query).fetchall()
            == source_connection.execute(query).fetchall()
        )
        target_connection.execute("UPDATE messages SET duration_s=99 WHERE id=1")
        target_connection.commit()
        diff = state_diff_connections(
            source_connection,
            target_connection,
            specs=sqlite_table_specs(
                target_connection, excluded_columns={("messages", "fts_content")}
            ),
            target_dialect="sqlite",
        )
        assert diff["clean"] is False
        assert diff["tables"]["messages"]["differ"] == 1
    finally:
        source_connection.close()
        target_connection.close()
    repaired = reverse_backfill("test-only", target_path, resume=True, **options)
    assert repaired["complete"] is False
    assert repaired["diff"]["mismatch_count"] == 1


def test_reverse_unsupported_pg_extra_fails_without_copying_rows(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "rollback.db"
    seed_extra_messages(source_path)
    with sqlite3.connect(source_path) as connection:
        connection.execute("ALTER TABLE messages ADD COLUMN legacy_json JSON")
    with pytest.raises(RuntimeError, match="unsupported postgres type 'JSON'"):
        reverse_backfill(
            "test-only",
            target_path,
            _source_factory=lambda _dsn: TuplePostgresSource(source_path),
        )
    with sqlite3.connect(target_path) as target:
        assert target.execute("SELECT COUNT(*) FROM messages").fetchone() == (0,)
        assert "duration_s" not in {
            row[1] for row in target.execute("PRAGMA table_info(messages)")
        }


class WritablePostgresBoundary(TuplePostgresSource):
    def __init__(self, path):
        self.connection = sqlite3.connect(path, isolation_level=None)

    def commit(self):
        self.connection.commit()


def test_dual_write_rewriter_preserves_extra_column_and_idempotent_replay(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    seed_extra_messages(source_path)
    seed_extra_messages(target_path)
    source = sqlite3.connect(source_path, isolation_level=None)
    try:
        recorded = RecordingConnection(source)
        recorded.execute("UPDATE messages SET duration_s = ? WHERE id = ?", (91.25, 1))
        replicator = DualWriteReplicator(
            source,
            "test-only",
            connection_factory=lambda _dsn, _timeout: postgres._PostgresConnection(
                WritablePostgresBoundary(target_path)
            ),
        )
        assert replicator.apply("extra-column-update", recorded.mutations) == "applied"
        assert (
            replicator.apply("extra-column-update", recorded.mutations, replay=True)
            == "already_applied"
        )
        with sqlite3.connect(target_path) as target:
            assert target.execute(
                "SELECT duration_s FROM messages WHERE id=1"
            ).fetchone() == (91.25,)
    finally:
        source.close()
