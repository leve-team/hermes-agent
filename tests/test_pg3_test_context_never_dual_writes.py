"""결함 #15 — pytest 컨텍스트의 SessionDB 는 env DSN 으로 dual-write 하지 않는다.

2026-09-09: dave 파드(운영 HERMES_CORE_PG_DSN 보유)에서 회귀 세트를 돌리자
테스트 픽스처(s-0000~, a-child, z-parent)가 라이브 PG 복제본에 두 번 들어가
authority 플립 preflight 를 46행 PG-only 로 두 번 막았다.
"""
import sqlite3
from pathlib import Path

import pytest


def test_env_dual_write_is_ignored_under_pytest(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_STATE_DUAL_WRITE", "1")
    monkeypatch.setenv("HERMES_CORE_PG_DSN", "postgresql://must-not-be-used.invalid/x")
    monkeypatch.delenv("HERMES_STATE_DB_GUARD_BYPASS", raising=False)
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        assert db._dual_requested is False
        assert db._dual_mode is False
        # 실제 쓰기 한 번 — DSN 이 invalid 라 dual 이 살아 있으면 저널이 생긴다
        db.create_session("t-1", "cli")
        with sqlite3.connect(tmp_path / "state.db") as raw:
            n = raw.execute(
                "select count(*) from sqlite_master where name='_hermes_dual_failures'"
            ).fetchone()[0]
            rows = raw.execute("select count(*) from _hermes_dual_failures").fetchone()[0] if n else 0
        assert rows == 0, f"pytest 컨텍스트에서 dual-write 저널이 생겼다: {rows}"
    finally:
        db.close()


def test_explicit_dual_write_flag_still_honoured_under_pytest(tmp_path, monkeypatch):
    """명시 dual_write=True 는 테스트가 의도한 것이므로 막지 않는다(기존 dual 테스트 보호)."""
    monkeypatch.delenv("HERMES_STATE_DUAL_WRITE", raising=False)
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db", dual_write=True)
    try:
        assert db._dual_requested is True
    finally:
        db.close()
