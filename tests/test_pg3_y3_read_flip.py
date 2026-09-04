"""Y3 read-flip contracts: sqlite, probe, authority, and go gate."""

from __future__ import annotations

import json
import logging
import sqlite3

import hermes_state_postgres as hsp
from hermes_state import SessionDB
from hermes_state_read import READ_FALLBACK_MARKER
from state_go_candidate import main as go_candidate_main


def _clear_state_controls(monkeypatch) -> None:
    for key in (
        "HERMES_STATE_BACKEND",
        "HERMES_STATE_DATABASE_URL",
        "HERMES_STATE_POSTGRES_DSN",
        "HERMES_CORE_PG_DSN",
        "HERMES_STATE_DUAL_WRITE",
    ):
        monkeypatch.delenv(key, raising=False)


def _clean_diff(rows: int = 1) -> dict:
    return {
        "tables": {
            "sessions": {
                "missing": 0,
                "extra": 0,
                "differ": 0,
                "matched": rows,
            }
        },
        "samples": [],
        "mismatch_count": 0,
        "clean": True,
    }


def _full_coverage(_path, _waive) -> dict:
    return {
        "executed": {"create_session": 1},
        "missing": [],
        "waived": [],
        "unknown_waivers": [],
        "clean": True,
    }


def _write_clean_reverse_report(path) -> None:
    path.write_text(
        json.dumps({
            "complete": True,
            "diff": {"clean": True, "mismatch_count": 0},
        }),
        encoding="utf-8",
    )


def test_default_is_sqlite_and_never_touches_postgres(tmp_path, monkeypatch) -> None:
    _clear_state_controls(monkeypatch)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    calls = []

    def forbidden(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError("default sqlite mode touched PostgreSQL")

    monkeypatch.setattr(hsp, "maybe_open_postgres", forbidden)
    db = SessionDB()
    try:
        db.create_session("sqlite-only", "cli")
        assert db.get_session("sqlite-only")["id"] == "sqlite-only"
        assert db._state_backend_mode == "sqlite"
        assert db._read_probe is None
        assert calls == []
    finally:
        db.close()


def test_probe_pg_failure_marks_and_returns_sqlite(
    tmp_path, monkeypatch, caplog
) -> None:
    _clear_state_controls(monkeypatch)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_STATE_BACKEND", "probe")
    monkeypatch.setenv("HERMES_CORE_PG_DSN", "postgresql://probe.invalid/state")
    pg_fixture_path = tmp_path / "pg-empty.db"
    pg_fixture = SessionDB(db_path=pg_fixture_path)
    pg_fixture.close()
    shadow = {"connection": None}

    def unavailable(*_args, **_kwargs):
        if shadow["connection"] is None:
            raise ConnectionError("synthetic probe outage")
        return shadow["connection"]

    monkeypatch.setattr(hsp, "maybe_open_postgres", unavailable)
    db = SessionDB()
    try:
        db.create_session("sqlite-authority", "cli")
        with caplog.at_level(logging.WARNING):
            row = db.get_session("sqlite-authority")
        assert row is not None and row["id"] == "sqlite-authority"
        markers = [
            record.getMessage()
            for record in caplog.records
            if READ_FALLBACK_MARKER in record.getMessage()
        ]
        assert len(markers) == 1
        assert "sqlite_response=true" in markers[0]

        # Recovery reaches an intentionally row-missing shadow. The mismatch
        # is marked, while the same SQLite authority row is still returned.
        target = sqlite3.connect(pg_fixture_path, isolation_level=None)
        target.row_factory = sqlite3.Row
        shadow["connection"] = target
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            row = db.get_session("sqlite-authority")
        assert row is not None and row["id"] == "sqlite-authority"
        mismatch_markers = [
            record.getMessage()
            for record in caplog.records
            if READ_FALLBACK_MARKER in record.getMessage()
        ]
        assert len(mismatch_markers) == 1
        assert "reason=mismatch" in mismatch_markers[0]
    finally:
        db.close()


def test_authority_row_miss_does_not_revive_sqlite(tmp_path, monkeypatch) -> None:
    _clear_state_controls(monkeypatch)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    sqlite_path = tmp_path / "state.db"
    pg_fixture_path = tmp_path / "pg-empty.db"

    sqlite_db = SessionDB(db_path=sqlite_path)
    sqlite_db.create_session("deleted-on-pg", "cli")
    sqlite_db.close()
    pg_fixture = SessionDB(db_path=pg_fixture_path)
    pg_fixture.close()

    target = sqlite3.connect(pg_fixture_path, isolation_level=None)
    target.row_factory = sqlite3.Row
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv(
        "HERMES_STATE_DATABASE_URL", "postgresql://authority.invalid/state"
    )
    monkeypatch.setattr(hsp, "maybe_open_postgres", lambda *_a, **_k: target)

    authority_db = SessionDB()
    try:
        assert authority_db._is_postgres is True
        assert authority_db.get_session("deleted-on-pg") is None
    finally:
        authority_db.close()


def test_go_candidate_mismatch_fixture_exits_nonzero(tmp_path, capsys) -> None:
    reverse = tmp_path / "reverse.json"
    _write_clean_reverse_report(reverse)
    mismatch = _clean_diff()
    mismatch["tables"]["sessions"].update(matched=0, differ=1)
    mismatch.update(
        mismatch_count=1,
        clean=False,
        samples=[{"kind": "differ", "table": "sessions", "pk": ["s-1"]}],
    )

    rc = go_candidate_main(
        [
            "--sqlite-path",
            str(tmp_path / "source.db"),
            "--dsn",
            "test-only",
            "--reverse-rehearsal-report",
            str(reverse),
        ],
        _diff_runner=lambda *_a, **_k: mismatch,
        _coverage_runner=_full_coverage,
    )
    report = json.loads(capsys.readouterr().out)
    assert rc != 0
    assert report["decision"] == "no-go"
    assert report["mismatch_count"] == 1
    assert report["examples"] == mismatch["samples"]


def test_zero_diff_emits_go_candidate_and_calls_v1_repair(tmp_path, capsys) -> None:
    reverse = tmp_path / "reverse.json"
    _write_clean_reverse_report(reverse)
    calls: list[bool] = []

    def diff_runner(_path, _dsn, *, repair=False):
        calls.append(repair)
        return _clean_diff(rows=7)

    rc = go_candidate_main(
        [
            "--sqlite-path",
            str(tmp_path / "source.db"),
            "--dsn",
            "test-only",
            "--reverse-rehearsal-report",
            str(reverse),
            "--repair",
        ],
        _diff_runner=diff_runner,
        _coverage_runner=_full_coverage,
    )
    report = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert calls == [True, False]
    assert report["decision"] == "go-candidate"
    assert report["sample_rows"] == 7
    assert report["coverage_percent"] == 100.0
    assert report["mismatch_count"] == 0
    assert report["tool_error_count"] == 0
    assert report["reverse_rehearsal_succeeded"] is True
    assert report["reader_seams_complete"] is True
    assert report["reader_seams"]
