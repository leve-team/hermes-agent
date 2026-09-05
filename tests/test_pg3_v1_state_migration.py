from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import hermes_state_dual
import hermes_state_postgres
from hermes_state import SessionDB
from migrate_state_to_postgres import (
    BackfillBudgetExceeded,
    InjectedBackfillFault,
    _backfill_fts,
    online_backfill,
)
from state_diff import RepairWriter, canonical_row_json, state_diff_connections
from state_reverse import InjectedReverseFault, reverse_backfill
from state_transfer import checkpoint_template, sqlite_table_specs


_EXTRA_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS levos_control_tower_event (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload_text TEXT NOT NULL DEFAULT '',
    origin_session_id TEXT,
    created_at REAL NOT NULL,
    interval_open INTEGER NOT NULL DEFAULT 0 CHECK (interval_open IN (0, 1)),
    responded_at REAL,
    consumed_at REAL,
    consumed_by_session_id TEXT
);
CREATE TABLE IF NOT EXISTS levos_control_tower_delivery (
    event_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    claimed_at REAL NOT NULL,
    injected_at REAL,
    inject_ok INTEGER NOT NULL DEFAULT 0 CHECK (inject_ok IN (0, 1)),
    responded_at REAL,
    PRIMARY KEY (event_id, attempt_no),
    UNIQUE (event_id, session_id),
    FOREIGN KEY (event_id) REFERENCES levos_control_tower_event(event_id)
);
CREATE TABLE IF NOT EXISTS levos_control_tower_role (
    role TEXT PRIMARY KEY CHECK (role = 'control_tower'),
    session_id TEXT NOT NULL,
    source TEXT NOT NULL,
    assigned_at REAL NOT NULL,
    last_event_id TEXT
);
CREATE TABLE IF NOT EXISTS levos_control_tower_forward (
    forward_key TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    control_session_id TEXT NOT NULL,
    target_session_id TEXT NOT NULL,
    text_sha256 TEXT NOT NULL,
    created_at REAL NOT NULL,
    delivered_at REAL,
    FOREIGN KEY (event_id) REFERENCES levos_control_tower_event(event_id)
);
CREATE TABLE IF NOT EXISTS delivery_obligations (
    obligation_id TEXT PRIMARY KEY,
    session_key TEXT NOT NULL,
    platform TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    thread_id TEXT,
    content TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    owner_pid INTEGER,
    owner_started_at INTEGER,
    last_error TEXT
);
"""


def _init_sqlite_replica(path: Path) -> None:
    db = SessionDB(db_path=path)
    db.close()


def _init_extra_state_tables(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_EXTRA_SQLITE_SCHEMA)
        async_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(async_delegations)")
        }
        if "origin_session_id" not in async_columns:
            conn.execute(
                "ALTER TABLE async_delegations ADD COLUMN origin_session_id TEXT"
            )


def _seed_extra_state_tables(path: Path, event_count: int = 190) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executemany(
            "INSERT INTO levos_control_tower_event"
            " (event_id, kind, payload_text, origin_session_id, created_at,"
            " interval_open, responded_at, consumed_at, consumed_by_session_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"event-{index:03d}",
                    "empty_epoch",
                    f"payload-{index}",
                    f"origin-{index}",
                    1_788_569_108.0 + index,
                    0,
                    None,
                    None,
                    None,
                )
                for index in range(event_count)
            ],
        )
        conn.executemany(
            "INSERT INTO levos_control_tower_delivery"
            " (event_id, attempt_no, session_id, claimed_at, injected_at,"
            " inject_ok, responded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"event-{index:03d}",
                    1,
                    f"session-{index:03d}",
                    1_788_569_109.0 + index,
                    1_788_569_110.0 + index,
                    1,
                    None,
                )
                for index in range(event_count)
            ],
        )
        conn.execute(
            "INSERT INTO levos_control_tower_role"
            " (role, session_id, source, assigned_at, last_event_id)"
            " VALUES ('control_tower', 'session-000', 'fixture', ?, 'event-000')",
            (1_787_493_712.0,),
        )
        conn.execute(
            "INSERT INTO levos_control_tower_forward"
            " (forward_key, event_id, control_session_id, target_session_id,"
            " text_sha256, created_at, delivered_at)"
            " VALUES ('forward-000', 'event-000', 'session-000',"
            " 'target-000', ?, ?, NULL)",
            ("0" * 64, 1_788_569_111.0),
        )
        conn.execute(
            "INSERT INTO async_delegations"
            " (delegation_id, origin_session, origin_ui_session_id,"
            " parent_session_id, state, dispatched_at, completed_at, updated_at,"
            " event_json, result_json, delivery_state, delivery_attempts,"
            " delivered_at, owner_pid, owner_started_at, task_json,"
            " delivery_claim, delivery_claimed_at, origin_session_id)"
            " VALUES ('delegation-1', 'origin', '', NULL, 'running', ?, NULL, ?,"
            " NULL, NULL, 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL, 'ui-1')",
            (1_787_664_570.0, 1_787_664_570.0),
        )
        conn.execute(
            "INSERT INTO delivery_obligations"
            " (obligation_id, session_key, platform, chat_id, thread_id, content,"
            " state, attempts, created_at, updated_at, owner_pid,"
            " owner_started_at, last_error)"
            " VALUES ('obligation-1', 'session-key', 'test', 'chat', NULL,"
            " 'content', 'pending', 0, ?, ?, NULL, NULL, NULL)",
            (1_788_569_112.0, 1_788_569_112.0),
        )


def _sqlite_target_factory(path: Path):
    def connect(_dsn: str, _timeout_s: float):
        conn = sqlite3.connect(path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    return connect


def _open_dual(
    monkeypatch: pytest.MonkeyPatch, source_path: Path, target_path: Path
) -> SessionDB:
    _init_sqlite_replica(target_path)
    monkeypatch.setenv("HERMES_STATE_DUAL_WRITE", "1")
    monkeypatch.setenv("HERMES_CORE_PG_DSN", "test-only")
    monkeypatch.setattr(
        hermes_state_dual,
        "connect_dual_target",
        _sqlite_target_factory(target_path),
    )
    return SessionDB(db_path=source_path)


def _ledger_rows(path: Path) -> tuple[list[tuple], list[tuple]]:
    with sqlite3.connect(path) as conn:
        sessions = conn.execute(
            "SELECT id, source, message_count FROM sessions ORDER BY id"
        ).fetchall()
        messages = conn.execute(
            "SELECT id, session_id, role, content, timestamp FROM messages ORDER BY id"
        ).fetchall()
    return sessions, messages


def _full_hash_diff(source_path: Path, target_path: Path, *, repair: bool = False):
    source = sqlite3.connect(source_path, isolation_level=None)
    source.row_factory = sqlite3.Row
    target = sqlite3.connect(target_path, isolation_level=None)
    target.row_factory = sqlite3.Row
    writer_conn = None
    try:
        specs = sqlite_table_specs(source)
        if repair:
            writer_conn = sqlite3.connect(target_path, isolation_level=None)
            writer_conn.row_factory = sqlite3.Row
        return state_diff_connections(
            source,
            target,
            specs=specs,
            target_dialect="sqlite",
            repair_writer=(
                RepairWriter(writer_conn, "sqlite", batch_rows=1000)
                if writer_conn is not None
                else None
            ),
            batch_rows=2,
        )
    finally:
        if writer_conn is not None:
            writer_conn.close()
        target.close()
        source.close()


class _ProcessKilled(BaseException):
    """Simulate an uncatchable process death at a fault-matrix boundary."""


@pytest.mark.parametrize("table", hermes_state_dual.EXTRA_STATE_TABLES)
def test_extra_state_table_mutations_are_dual_write_recording_targets(
    table: str,
) -> None:
    source = sqlite3.connect(":memory:", isolation_level=None)
    source.execute(f'CREATE TABLE "{table}" (id TEXT PRIMARY KEY)')
    recorder = hermes_state_dual.RecordingConnection(source)

    recorder.execute(f'INSERT INTO "{table}" (id) VALUES (?)', ("row-1",))

    assert [
        (mutation.table, mutation.operation) for mutation in recorder.mutations
    ] == [(table, "insert")]


def test_dual_write_explicit_message_id_refreshes_postgres_fts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        rowcount = 1

        def execute(self, _sql, _params):
            return self

        def fetchone(self):
            return (37,)

    calls = []
    monkeypatch.setattr(
        hermes_state_postgres,
        "_update_fts_content",
        lambda conn, message_id, content, tool_name, tool_calls: calls.append(
            (conn, message_id, content, tool_name, tool_calls)
        ),
    )
    connection = object()
    cursor = hermes_state_postgres._PostgresCursor(Cursor(), conn=connection)
    cursor.execute(
        "INSERT OR IGNORE INTO messages "
        "(session_id, role, content, tool_name, tool_calls, timestamp, id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("s1", "user", "body", "tool", "[]", 1.0, 37),
    )
    assert calls == [(connection, 37, "body", "tool", "[]")]


def test_dual_write_replays_same_generated_id_and_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    db = _open_dual(monkeypatch, source_path, target_path)
    try:
        db.create_session("s1", "cli")
        row_id = db.append_message("s1", "user", "same row", timestamp=1234.5)
    finally:
        db.close()

    assert row_id > 0
    assert _ledger_rows(source_path) == _ledger_rows(target_path)
    with sqlite3.connect(source_path) as conn:
        coverage = dict(
            conn.execute(
                "SELECT entrypoint, execution_count FROM _hermes_dual_coverage"
            )
        )
    assert coverage["_insert_session_row"] == 1
    assert coverage["append_message"] == 1


def test_dual_write_failure_is_journaled_and_replay_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    db = _open_dual(monkeypatch, source_path, target_path)
    try:
        db.create_session("s1", "cli")

        def fail_before_apply(point: str) -> None:
            if point == "before_pg_apply":
                raise ConnectionError("replica unavailable")

        db._dual_replicator.fault_inject = fail_before_apply
        row_id = db.append_message("s1", "user", "journal me", timestamp=12.0)
        assert row_id > 0  # PostgreSQL failure is fail-open for the source.

        pending = db._conn.execute(
            "SELECT COUNT(*) FROM _hermes_dual_failures WHERE replayed_at IS NULL"
        ).fetchone()[0]
        assert pending == 1

        db._dual_replicator.fault_inject = None
        first = db._dual_replicator.replay_failures()
        second = db._dual_replicator.replay_failures()
    finally:
        db.close()

    assert first == {"applied": 1, "failed": 0, "pending": 0}
    assert second == {"applied": 0, "failed": 0, "pending": 0}
    assert _ledger_rows(source_path) == _ledger_rows(target_path)


@pytest.mark.parametrize(
    "fault_point",
    [
        "after_source_commit",
        "before_pg_apply",
        "after_pg_commit_before_ack",
        "during_replay",
    ],
)
def test_dual_write_fault_matrix_recovers_to_full_hash_diff_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_point: str,
) -> None:
    source_path = tmp_path / f"source-{fault_point}.db"
    target_path = tmp_path / f"target-{fault_point}.db"
    db = _open_dual(monkeypatch, source_path, target_path)
    try:
        db.create_session("s1", "cli")

        if fault_point == "during_replay":

            def fail_initial_apply(point: str) -> None:
                if point == "before_pg_apply":
                    raise ConnectionError("replica unavailable")

            db._dual_replicator.fault_inject = fail_initial_apply
            db.append_message("s1", "user", "recover me", timestamp=42.0)

            def kill_replay(point: str) -> None:
                if point == "during_replay":
                    raise _ProcessKilled(point)

            db._dual_replicator.fault_inject = kill_replay
            with pytest.raises(_ProcessKilled):
                db._dual_replicator.replay_failures()
            db._dual_replicator.fault_inject = None
            assert db._dual_replicator.replay_failures()["pending"] == 0
        else:

            def kill_at_boundary(point: str) -> None:
                if point == fault_point:
                    raise _ProcessKilled(point)

            db._dual_replicator.fault_inject = kill_at_boundary
            with pytest.raises(_ProcessKilled):
                db.append_message("s1", "user", "recover me", timestamp=42.0)
            db._dual_replicator.fault_inject = None
    finally:
        db.close()

    first_diff = _full_hash_diff(source_path, target_path)
    if not first_diff["clean"]:
        repaired = _full_hash_diff(source_path, target_path, repair=True)
        assert repaired["mismatch_count"] > 0
    final_diff = _full_hash_diff(source_path, target_path)
    assert final_diff["clean"] is True
    assert final_diff["mismatch_count"] == 0


def test_dual_write_rejects_database_local_clock_before_source_commit() -> None:
    source = sqlite3.connect(":memory:", isolation_level=None)
    source.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY, started_at REAL)")
    recorder = hermes_state_dual.RecordingConnection(source)
    source.execute("BEGIN IMMEDIATE")
    with pytest.raises(RuntimeError, match="compute the timestamp once"):
        recorder.execute(
            "INSERT INTO sessions(id, started_at) VALUES (?, strftime('%s', 'now'))",
            ("clock-split",),
        )
    source.rollback()
    assert source.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0


def _seed_sessions(path: Path, count: int) -> None:
    db = SessionDB(db_path=path)
    try:
        for index in range(count):
            session_id = f"s-{index:04d}"
            db.create_session(session_id, "cli")
            db.append_message(
                session_id,
                "user",
                f"payload {index}",
                timestamp=float(index + 1),
            )
    finally:
        db.close()


def test_backfill_resume_after_fifty_percent_fault_uses_checkpoint(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    checkpoint = tmp_path / "backfill.json"
    _seed_sessions(source_path, 20)
    _init_sqlite_replica(target_path)

    def target_factory(_dsn: str):
        conn = sqlite3.connect(target_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    common = {
        "checkpoint_path": checkpoint,
        "batch_rows": 3,
        "budget_bytes": 1024 * 1024 * 1024,
        "_target_factory": target_factory,
        "_initialize_target": lambda _conn: None,
        "_finalize_target": lambda _conn: None,
    }
    with pytest.raises(InjectedBackfillFault):
        online_backfill(
            source_path,
            "test-only",
            fault_inject_at="50%",
            **common,
        )

    summary = online_backfill(
        source_path,
        "test-only",
        resume=True,
        **common,
    )
    assert summary["complete"] is True
    assert summary["source_sessions"] == 20
    assert summary["source_messages"] == 20
    assert _ledger_rows(source_path) == _ledger_rows(target_path)


def test_extra_state_tables_backfill_190_rows_and_join_hash_diff(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    checkpoint = tmp_path / "backfill.json"
    _init_sqlite_replica(source_path)
    _init_sqlite_replica(target_path)
    _init_extra_state_tables(source_path)
    _init_extra_state_tables(target_path)
    _seed_extra_state_tables(source_path)

    def target_factory(_dsn: str):
        conn = sqlite3.connect(target_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    summary = online_backfill(
        source_path,
        "test-only",
        checkpoint_path=checkpoint,
        batch_rows=17,
        budget_bytes=1024 * 1024 * 1024,
        _target_factory=target_factory,
        _initialize_target=lambda _conn: None,
        _finalize_target=lambda _conn: None,
    )

    assert summary["rows_by_table"]["levos_control_tower_event"] == 190
    assert summary["rows_by_table"]["levos_control_tower_delivery"] == 190
    assert summary["rows_by_table"]["async_delegations"] == 1
    assert summary["rows_by_table"]["delivery_obligations"] == 1

    with sqlite3.connect(source_path) as source, sqlite3.connect(target_path) as target:
        source.row_factory = sqlite3.Row
        target.row_factory = sqlite3.Row
        specs = [
            spec
            for spec in sqlite_table_specs(source)
            if spec.name in hermes_state_dual.EXTRA_STATE_TABLES
        ]
        report = state_diff_connections(
            source,
            target,
            specs=specs,
            target_dialect="sqlite",
            batch_rows=19,
        )

    assert set(report["tables"]) == set(hermes_state_dual.EXTRA_STATE_TABLES)
    assert report["clean"] is True
    assert report["mismatch_count"] == 0


def test_backfill_resume_disk_guard_saves_checkpoint_and_uses_rc4_error(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    checkpoint = tmp_path / "backfill.json"
    _seed_sessions(source_path, 1)
    _init_sqlite_replica(target_path)

    def target_factory(_dsn: str):
        return sqlite3.connect(target_path, isolation_level=None)

    with pytest.raises(BackfillBudgetExceeded) as raised:
        online_backfill(
            source_path,
            "test-only",
            checkpoint_path=checkpoint,
            budget_bytes=1,
            _target_factory=target_factory,
            _initialize_target=lambda _conn: None,
            _finalize_target=lambda _conn: None,
        )
    assert raised.value.checkpoint_path == checkpoint
    assert checkpoint.is_file()


def test_backfill_resume_fts_phase_keeps_disk_guard_active(tmp_path: Path) -> None:
    class Target:
        def __init__(self):
            self.raw = self
            self.rows = {
                1: (1, "body one", "tool-one", "calls-one"),
                2: (2, "body two", "tool-two", "calls-two"),
            }
            self.indexed: set[int] = set()
            self.database_size = 2
            self.select_params: list[tuple] = []

        def execute(self, sql, params=()):
            if sql.startswith("SELECT id, content"):
                self.select_params.append(tuple(params))
                last_pk = int(params[0]) if len(params) == 2 else 0
                limit = int(params[-1])
                rows = [
                    row
                    for message_id, row in sorted(self.rows.items())
                    if message_id > last_pk and message_id not in self.indexed
                ][:limit]
                return type("BatchCursor", (), {"fetchall": lambda _self: rows})()
            if sql.startswith("UPDATE messages SET fts_content"):
                self.indexed.add(int(params[1]))
                return type("Cursor", (), {"rowcount": 1})()
            if sql == "BEGIN" or sql.startswith(
                "INSERT INTO hermes_fts_truncations"
            ):
                return type("Cursor", (), {"rowcount": 1})()
            if sql.startswith("SELECT pg_database_size"):
                size = self.database_size
                return type("SizeCursor", (), {"fetchone": lambda _self: (size,)})()
            raise AssertionError(sql)

        def commit(self):
            return None

        def rollback(self):
            return None

    checkpoint = tmp_path / "backfill.json"
    payload = checkpoint_template("test-source", "sqlite-to-postgres")
    target = Target()
    with pytest.raises(BackfillBudgetExceeded) as raised:
        _backfill_fts(
            target,
            1,
            budget_bytes=1,
            checkpoint_path=checkpoint,
            checkpoint=payload,
        )
    assert raised.value.checkpoint_path == checkpoint
    assert checkpoint.is_file()
    saved = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert saved["fts"] == {
        "last_pk": 1,
        "rows": 1,
        "truncated_rows": 0,
        "complete": False,
    }

    target.database_size = 0
    assert _backfill_fts(
        target,
        1,
        budget_bytes=10,
        checkpoint_path=checkpoint,
        checkpoint=saved,
    ) == 2
    assert target.indexed == {1, 2}
    assert (1, 1) in target.select_params
    assert saved["fts"]["complete"] is True


def test_state_diff_hash_detects_missing_extra_and_equal_count_difference(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    checkpoint = tmp_path / "backfill.json"
    _seed_sessions(source_path, 3)
    _init_sqlite_replica(target_path)

    def target_factory(_dsn: str):
        conn = sqlite3.connect(target_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    online_backfill(
        source_path,
        "test-only",
        checkpoint_path=checkpoint,
        budget_bytes=1024 * 1024 * 1024,
        _target_factory=target_factory,
        _initialize_target=lambda _conn: None,
        _finalize_target=lambda _conn: None,
    )
    with sqlite3.connect(target_path) as target:
        target.execute("DELETE FROM messages WHERE id = 1")
        target.execute("UPDATE messages SET content = 'different' WHERE id = 2")
        target.execute(
            "INSERT INTO messages(id, session_id, role, content, timestamp) "
            "VALUES (999, 's-0002', 'user', 'extra', 999.0)"
        )

    source = sqlite3.connect(source_path, isolation_level=None)
    source.row_factory = sqlite3.Row
    target = sqlite3.connect(target_path, isolation_level=None)
    target.row_factory = sqlite3.Row
    specs = [
        spec
        for spec in sqlite_table_specs(source)
        if spec.name in {"sessions", "messages"}
    ]
    try:
        report = state_diff_connections(
            source,
            target,
            specs=specs,
            target_dialect="sqlite",
            batch_rows=2,
        )
        assert report["tables"]["messages"] == {
            "missing": 1,
            "extra": 1,
            "differ": 1,
            "matched": 1,
        }

        writer_conn = sqlite3.connect(target_path, isolation_level=None)
        # Keep the SQLite-test writer transaction open until the read cursor is
        # exhausted. PostgreSQL's MVCC permits bounded mid-stream commits; a
        # second SQLite connection cannot commit while this test cursor holds
        # its read lock.
        writer = RepairWriter(writer_conn, "sqlite", batch_rows=100)
        repaired = state_diff_connections(
            source,
            target,
            specs=specs,
            target_dialect="sqlite",
            repair_writer=writer,
            batch_rows=2,
        )
        writer_conn.close()
        assert repaired["mismatch_count"] >= 3
    finally:
        source.close()
        target.close()

    with sqlite3.connect(source_path) as source, sqlite3.connect(target_path) as target:
        source.row_factory = sqlite3.Row
        target.row_factory = sqlite3.Row
        clean = state_diff_connections(
            source,
            target,
            specs=specs,
            target_dialect="sqlite",
        )
    assert clean["clean"] is True


def test_state_diff_hash_normalization_fixes_numeric_bytes_and_null_rules() -> None:
    encoded = canonical_row_json(
        ("z", "a", "blob", "nothing"),
        {"z": 1.25, "a": 7, "blob": b"\x00\xff", "nothing": None},
    )
    assert encoded == (
        '{"a":{"type":"int","value":"7"},'
        '"blob":{"type":"bytes","value":"00ff"},'
        '"nothing":{"type":"null","value":null},'
        '"z":{"type":"float","value":"1.25"}}'
    )


def test_state_diff_hash_repair_handles_self_referencing_session_order(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source-tree.db"
    target_path = tmp_path / "target-tree.db"
    source = SessionDB(db_path=source_path)
    target = SessionDB(db_path=target_path)
    try:
        source.create_session("z-source-parent", "cli")
        source.create_session(
            "a-source-child", "cli", parent_session_id="z-source-parent"
        )
        target.create_session("a-extra-parent", "cli")
        target.create_session(
            "z-extra-child", "cli", parent_session_id="a-extra-parent"
        )
    finally:
        source.close()
        target.close()

    mismatch = _full_hash_diff(source_path, target_path, repair=True)
    assert mismatch["mismatch_count"] == 4
    clean = _full_hash_diff(source_path, target_path)
    assert clean["clean"] is True
    with sqlite3.connect(target_path) as repaired:
        assert (
            repaired.execute(
                "SELECT parent_session_id FROM sessions WHERE id = 'a-source-child'"
            ).fetchone()[0]
            == "z-source-parent"
        )


def test_state_reverse_backfill_resume_after_fault_finishes_with_hash_diff_zero(
    tmp_path: Path,
) -> None:
    pg_fixture_path = tmp_path / "pg-fixture.db"
    sqlite_target_path = tmp_path / "rollback-state.db"
    checkpoint = tmp_path / "reverse.json"
    source_db = SessionDB(db_path=pg_fixture_path)
    try:
        # Text-PK order deliberately loads the child before its parent, proving
        # reverse backfill defers the self-referencing edge.
        source_db.create_session("z-parent", "cli")
        source_db.create_session("a-child", "cli", parent_session_id="z-parent")
        source_db.append_message("a-child", "user", "rollback payload", timestamp=7.0)
    finally:
        source_db.close()

    def source_factory(_dsn: str):
        conn = sqlite3.connect(pg_fixture_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    with pytest.raises(InjectedReverseFault):
        reverse_backfill(
            "test-only",
            sqlite_target_path,
            checkpoint_path=checkpoint,
            batch_rows=1,
            fault_inject_at="50%",
            _source_factory=source_factory,
        )
    checkpoint_text = checkpoint.read_text(encoding="utf-8")
    assert "test-only" not in checkpoint_text
    assert "postgres-sha256:" in checkpoint_text

    summary = reverse_backfill(
        "test-only",
        sqlite_target_path,
        checkpoint_path=checkpoint,
        resume=True,
        batch_rows=1,
        _source_factory=source_factory,
    )
    assert summary["complete"] is True
    assert summary["diff"]["clean"] is True
    assert summary["diff"]["mismatch_count"] == 0
    with sqlite3.connect(sqlite_target_path) as target:
        assert (
            target.execute(
                "SELECT parent_session_id FROM sessions WHERE id = 'a-child'"
            ).fetchone()[0]
            == "z-parent"
        )

    # A completed checkpoint remains an idempotent, hash-verified resume.
    repeated = reverse_backfill(
        "test-only",
        sqlite_target_path,
        checkpoint_path=checkpoint,
        resume=True,
        batch_rows=1,
        _source_factory=source_factory,
    )
    assert repeated["complete"] is True
    assert repeated["diff"]["mismatch_count"] == 0
