from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import hermes_state_dual
from hermes_state import SessionDB


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
