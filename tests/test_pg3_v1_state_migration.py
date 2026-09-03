from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import hermes_state_dual
from hermes_state import SessionDB
from migrate_state_to_postgres import (
    BackfillBudgetExceeded,
    InjectedBackfillFault,
    online_backfill,
)
from state_diff import RepairWriter, canonical_row_json, state_diff_connections
from state_transfer import sqlite_table_specs


def _init_sqlite_replica(path: Path) -> None:
    db = SessionDB(db_path=path)
    db.close()


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
