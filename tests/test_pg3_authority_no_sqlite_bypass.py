"""0046 — PostgreSQL authority 에서 SQLite 를 여는 쓰기 경로가 없어야 한다.

2026-09-09 Y3 플립 4차: mesh 로 받은 메시지(Y3F_VERIFY_1)가 SQLite id 629560 에
기록되고 PG 에는 없었다. api_server 의 프로필별 세션 DB 캐시가
``SessionDB(db_path=home/state.db)`` 로 파일을 명시해 열었고, 코어는 명시 db_path
를 SQLite 로 유지한다(계약). tui_gateway/web_server 는 이미 backend seam 을 타는데
api_server 와 delegate_tool 만 빠져 있었다.
"""
import sys
import types
from pathlib import Path

import pytest


class _FakePG:
    """SessionDB(postgres_dsn=...) 가 열리는 척 — _is_postgres 만 본다."""
    _is_postgres = True
    db_path = None

    def close(self):
        pass


def _install_fake_state(monkeypatch, tmp_path, *, authority: bool):
    """hermes_state / hermes_state_postgres 를 최소 형상으로 대체한다."""
    calls = []
    hs = types.ModuleType("hermes_state")

    class SessionDB:  # noqa: N801 - 실제 이름 유지
        def __init__(self, db_path=None, read_only=False, postgres_dsn=None, **kw):
            calls.append(("SessionDB", db_path, postgres_dsn))
            self.db_path = db_path
            self._is_postgres = bool(postgres_dsn) or (authority and db_path is None)

        def close(self):
            pass

    hs.SessionDB = SessionDB
    monkeypatch.setitem(sys.modules, "hermes_state", hs)

    hsp = types.ModuleType("hermes_state_postgres")
    hsp.resolve_state_backend = lambda config=None: "authority" if authority else "sqlite"
    hsp.profile_selects_postgres = lambda name: authority

    def open_store_for_profile(name, read_only=False):
        calls.append(("seam", name, read_only))
        return _FakePG()

    hsp.open_store_for_profile = open_store_for_profile
    monkeypatch.setitem(sys.modules, "hermes_state_postgres", hsp)
    return calls


def _load_api_helper():
    import importlib.util

    root = Path(__file__).resolve().parents[1]
    src = (root / "gateway/platforms/api_server.py").read_text()
    # 무거운 모듈 import 없이 헬퍼 함수만 뽑아 실행한다.
    start = src.index("def _open_profile_session_db(home):")
    end = src.index("\nclass ", start)
    ns = {}
    exec(src[start:end], ns)
    return ns["_open_profile_session_db"]


def test_named_profile_under_authority_goes_through_the_seam(monkeypatch, tmp_path):
    calls = _install_fake_state(monkeypatch, tmp_path, authority=True)
    home = tmp_path / "profiles" / "dave"
    home.mkdir(parents=True)
    db = _load_api_helper()(home)
    assert db._is_postgres is True
    assert ("seam", "dave", False) in calls, calls
    assert not any(c[0] == "SessionDB" and c[1] is not None for c in calls), (
        f"authority 인데 명시 db_path 로 SQLite 를 열었다: {calls}"
    )


def test_named_profile_on_sqlite_keeps_the_explicit_path(monkeypatch, tmp_path):
    calls = _install_fake_state(monkeypatch, tmp_path, authority=False)
    home = tmp_path / "profiles" / "opsi"
    home.mkdir(parents=True)
    db = _load_api_helper()(home)
    assert db._is_postgres is False
    assert db.db_path == home / "state.db"


def test_default_profile_under_authority_follows_process_env(monkeypatch, tmp_path):
    calls = _install_fake_state(monkeypatch, tmp_path, authority=True)
    home = tmp_path / ".hermes"  # parent 가 'profiles' 가 아님 → 기본 프로필
    home.mkdir(parents=True)
    db = _load_api_helper()(home)
    assert db._is_postgres is True
    assert ("SessionDB", None, None) in calls


def test_delegate_child_inherits_postgres_authority_not_a_pinned_path():
    root = Path(__file__).resolve().parents[1]
    src = (root / "tools/delegate_tool.py").read_text()
    i = src.index('_parent_db_path = getattr(parent_session_db, "db_path", None)')
    window = src[i:i + 900]
    assert 'getattr(parent_session_db, "_is_postgres", False)' in window, (
        "delegate_tool 이 부모의 물리 저장소 선택을 보지 않고 db_path 를 고정한다"
    )


def test_api_server_cache_is_wired_to_the_helper_not_a_pinned_path():
    """헬퍼가 존재해도 캐시 호출부가 안 쓰면 무의미 — 호출부 배선을 단정한다."""
    root = Path(__file__).resolve().parents[1]
    src = (root / "gateway/platforms/api_server.py").read_text()
    i = src.index("db = self._session_dbs.get(key)")
    window = src[i:i + 400]
    assert "_open_profile_session_db(home)" in window, window
    assert 'SessionDB(db_path=home / "state.db")' not in window, (
        "프로필별 세션 DB 캐시가 여전히 db_path 를 고정해 SQLite 를 연다"
    )


def test_seam_uses_process_env_dsn_only_for_the_active_profile(monkeypatch, tmp_path):
    """활성 프로필 == 대상 프로필일 때만 이 프로세스의 HERMES_STATE_* DSN 을 쓴다."""
    import importlib
    hsp = importlib.import_module("hermes_state_postgres")
    monkeypatch.setattr(hsp, "_is_active_profile", lambda canon: canon == "dave")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", "postgresql://env/dsn")
    seen = {}

    class _FakeDB:
        _is_postgres = True

        def __init__(self, postgres_dsn=None, read_only=False, **kw):
            seen["dsn"] = postgres_dsn

        def close(self):
            pass

    hs = importlib.import_module("hermes_state")
    monkeypatch.setattr(hs, "SessionDB", _FakeDB)
    # profile_dir 준비: config.yaml 만 authority, .env 없음
    profiles = tmp_path / "profiles"
    for name in ("dave", "opsi"):
        d = profiles / name
        d.mkdir(parents=True)
        (d / "config.yaml").write_text("sessions:\n  state_backend: authority\n")
    from hermes_cli import profiles as profiles_mod
    monkeypatch.setattr(profiles_mod, "profile_exists", lambda c: c in ("dave", "opsi"))
    monkeypatch.setattr(profiles_mod, "get_profile_dir", lambda c: profiles / c)
    hsp.open_store_for_profile("dave")
    assert seen.get("dsn") == "postgresql://env/dsn"
    with pytest.raises(RuntimeError, match="no DSN"):
        hsp.open_store_for_profile("opsi")


def test_active_profile_is_recognised_by_hermes_profile_without_hermes_home(monkeypatch):
    """messaging-gateway 형상: HERMES_PROFILE 만 있고 HERMES_HOME 없음 → 활성 프로필."""
    import importlib
    hsp = importlib.import_module("hermes_state_postgres")
    monkeypatch.delenv("HERMES_HOME", raising=False)
    monkeypatch.setenv("HERMES_PROFILE", "dave")
    assert hsp._is_active_profile("dave") is True
    assert hsp._is_active_profile("opsi") is False
    monkeypatch.delenv("HERMES_PROFILE", raising=False)
    # 둘 다 없으면(순수 default 프로필) 어떤 명명 프로필도 활성이 아니다
    assert hsp._is_active_profile("dave") is False
