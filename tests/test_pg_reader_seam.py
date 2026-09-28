"""Profile readers must reach the configured store without consulting a stale file.

The profile/config/reader paths are real. PostgreSQL construction is replaced
with a separate SQLite-backed store here; PostgreSQL protocol and read-only
transaction enforcement are covered by the backend integration tests.
"""

from contextlib import contextmanager
import json
from pathlib import Path

import pytest

import hermes_state
import hermes_state_postgres
from hermes_state_registry import release_or_close
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn


@pytest.fixture
def profile_homes(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    peer = home / "profiles" / "peer"
    peer.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    # Follow the temp profile ContextVar rather than conftest's fixed legacy
    # DEFAULT_DB_PATH override (same pattern as named-profile SessionDB tests).
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)
    for name in ("HERMES_STATE_DATABASE_URL", "HERMES_STATE_POSTGRES_DSN", "HERMES_STATE_BACKEND"):
        monkeypatch.delenv(name, raising=False)
    return home, peer


def _seed(path, sid, text):
    db = hermes_state.SessionDB(db_path=path)
    try:
        db.create_session(sid, source="cli")
        db.set_session_title(sid, "Bot Chat")
        db.append_message(sid, "user", text)
    finally:
        db.close()


def _mock_postgres_stores(monkeypatch, stores):
    real_session_db = hermes_state.SessionDB
    calls = []

    class RemoteStore:
        _is_postgres = True

        def __init__(self, db, logical_path):
            self.db = db
            self.db_path = logical_path

        def __getattr__(self, name):
            return getattr(self.db, name)

    def make_store(*args, postgres_dsn=None, **kwargs):
        if postgres_dsn is None:
            return real_session_db(*args, **kwargs)
        read_only = kwargs.get("read_only", False)
        calls.append((postgres_dsn, read_only))
        logical_path = kwargs.get("db_path") or hermes_state._default_db_path()
        return RemoteStore(
            real_session_db(db_path=stores[postgres_dsn], read_only=read_only), logical_path)

    monkeypatch.setattr(hermes_state, "SessionDB", make_store)
    return calls


@contextmanager
def _owned(db):
    try:
        yield db
    finally:
        release_or_close(db)


def _read_db(context):
    with context as db:
        assert db is not None
        row = db.get_session("peer-chat")
        messages = db.get_messages("peer-chat")
        return row["id"], messages[-1]["content"]


def _read_tool():
    import tools.session_search_tool  # registers the real handler
    from tools.registry import registry

    result = json.loads(registry.dispatch(
        "session_search", {"profile": "peer", "session_id": "peer-chat"}))
    assert result["success"], result
    return result["session_id"], result["messages"][-1]["content"]


def _read_dashboard():
    from hermes_cli.web_server_sessions import _open_session_db_for_profile

    return _read_db(_owned(_open_session_db_for_profile("peer", read_only=True)))


def _read_profiles_http():
    from hermes_cli.web_routers.profiles import get_profiles_sessions

    result = get_profiles_sessions(profile="peer", limit=20, offset=0)
    assert not result["errors"], result
    row = next(row for row in result["sessions"] if row["id"] == "peer-chat")
    return row["id"], row["preview"]


def _sidebar_rows():
    from hermes_cli.web_routers.profiles import get_profiles_sessions_sidebar

    # Expired outer response cache: exercise the persistent per-profile cache
    # without sleeping through its five-second polling interval.
    result = get_profiles_sessions_sidebar.__wrapped__(recents_profile="peer")
    assert not result["errors"], result
    return result["recents"]["sessions"]


def _read_sidebar():
    row = next(row for row in _sidebar_rows() if row["id"] == "peer-chat")
    return row["id"], row["preview"]


def _read_gateway_request(server, peer):
    return _read_db(server._profile_db({"profile": "peer"}))


def _read_gateway_session(server, peer):
    return _read_db(server._session_db({"profile_home": str(peer)}))


def _read_gateway_build(server, peer):
    db, owns = server._profile_session_db(peer)
    assert owns
    return _read_db(_owned(db))


def _read_gateway_startup(server, peer, monkeypatch, dsn=None):
    # Simulate credentials loaded when this peer launched the process.
    with monkeypatch.context() as scoped:
        scoped.setattr(server, "_hermes_home", str(peer))
        scoped.setattr(server, "_db", None)
        if dsn:
            scoped.setenv("HERMES_STATE_DATABASE_URL", dsn)
        try:
            return _read_db(_owned(server._get_db()))
        finally:
            server._db = None


def _read_roster(server, peer):
    result = server.handle_request({
        "id": 1, "method": "profiles.list", "params": {"include_sessions": True}})
    assert "error" not in result, result
    row = next(row for row in result["result"]["profiles"] if row["name"] == "peer")
    canonical = row["canonical_session"]
    assert row["last_session"]["id"] == canonical["id"]
    return canonical["id"], canonical["preview"]


@pytest.mark.parametrize("backend", ["sqlite", "postgres", "postgresql", "pg"])
@pytest.mark.parametrize("reader,read_only", [
    ("tool", True), ("dashboard", True), ("roster", True),
    ("profiles_http", True), ("sidebar", True),
    ("gateway_request", False), ("gateway_session", False),
    ("gateway_build", False), ("gateway_startup", False),
])
def test_profile_readers_reach_the_configured_store(
    profile_homes, tmp_path, monkeypatch, backend, reader, read_only,
):
    home, peer = profile_homes
    _seed(home / "state.db", "launch-chat", "Only in the launch profile")
    store_path = peer / "state.db" if backend == "sqlite" else tmp_path / "remote.db"
    _seed(store_path, "peer-chat", "Persisted in the peer store")
    (peer / "config.yaml").write_text(f"sessions:\n  state_backend: {backend}\n", encoding="utf-8")
    expected_dsn = "postgresql://peer:secret@db.invalid/peer"
    if backend != "sqlite":
        (peer / ".env").write_text(f"HERMES_STATE_DATABASE_URL={expected_dsn}\n", encoding="utf-8")

    real_session_db = hermes_state.SessionDB
    import tui_gateway.server as server

    calls = _mock_postgres_stores(monkeypatch, {expected_dsn: store_path})
    monkeypatch.setattr(server, "_hermes_home", str(home))
    readers = {
        "tool": _read_tool,
        "dashboard": _read_dashboard,
        "profiles_http": _read_profiles_http,
        "sidebar": _read_sidebar,
        "roster": lambda: _read_roster(server, peer),
        "gateway_request": lambda: _read_gateway_request(server, peer),
        "gateway_session": lambda: _read_gateway_session(server, peer),
        "gateway_build": lambda: _read_gateway_build(server, peer),
        "gateway_startup": lambda: _read_gateway_startup(
            server, peer, monkeypatch, expected_dsn if backend != "sqlite" else None),
    }
    assert readers[reader]() == ("peer-chat", "Persisted in the peer store")
    if backend == "sqlite":
        assert calls == []
    else:
        assert calls == [(expected_dsn, read_only)]
        assert not (peer / "state.db").exists()

    if reader == "sidebar":
        with real_session_db(db_path=store_path) as writer:
            writer.create_session("fresh-peer", source="cli")
            writer.append_message("fresh-peer", "user", "A later remote session")
        assert "fresh-peer" in {row["id"] for row in _sidebar_rows()}

    # A bare id remains scoped to the caller; explicit links are the only
    # authorized route to another profile's transcript (#106761).
    import tools.session_search_tool
    from tools.registry import registry

    miss = json.loads(registry.dispatch("session_search", {"session_id": "peer-chat"}))
    assert miss["success"] is False
    assert "Persisted in the peer store" not in json.dumps(miss)


@pytest.mark.parametrize("config", [
    "sessions:\n  state_backend: postgres\n",
    "sessions: [unterminated\n",
])
def test_unusable_profile_config_never_falls_back_to_stale_sqlite(profile_homes, config):
    home, peer = profile_homes
    _seed(home / "state.db", "launch-chat", "Only in the launch profile")
    _seed(peer / "state.db", "peer-chat", "Stale local history")
    (peer / "config.yaml").write_text(config, encoding="utf-8")

    from hermes_cli.web_server_sessions import _open_session_db_for_profile
    import tools.session_search_tool
    from tools.registry import registry

    with pytest.raises(RuntimeError):
        hermes_state_postgres.open_store_for_profile("peer", read_only=True)
    with pytest.raises(RuntimeError):
        _open_session_db_for_profile("peer", read_only=True)
    result = json.loads(registry.dispatch(
        "session_search", {"profile": "peer", "session_id": "peer-chat"}))
    assert result["success"] is False
    assert "Stale local history" not in json.dumps(result)


@pytest.mark.parametrize("foreign_backend", ["sqlite", "postgres"])
def test_launch_env_credentials_and_foreign_context_stay_isolated(
    profile_homes, tmp_path, monkeypatch, foreign_backend,
):
    import hermes_state_registry as registry
    from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override
    import tui_gateway.server as server

    home, peer = profile_homes
    launch_dsn = "postgresql://launch:secret@db.invalid/launch"
    foreign_dsn = "postgresql://peer:secret@db.invalid/peer"
    launch_store = tmp_path / "launch-remote.db"
    foreign_store = peer / "state.db" if foreign_backend == "sqlite" else tmp_path / "peer-remote.db"
    _seed(launch_store, "launch-chat", "Launch remote history")
    _seed(foreign_store, "peer-chat", "Peer history")
    (home / "config.yaml").write_text("sessions:\n  state_backend: postgres\n", encoding="utf-8")
    (peer / "config.yaml").write_text(
        f"sessions:\n  state_backend: {foreign_backend}\n", encoding="utf-8")
    if foreign_backend == "postgres":
        (peer / ".env").write_text(f"HERMES_STATE_DATABASE_URL={foreign_dsn}\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_STATE_DATABASE_URL", launch_dsn)
    monkeypatch.setattr(server, "_hermes_home", str(home))
    monkeypatch.setattr(server, "_db", None)
    calls = _mock_postgres_stores(monkeypatch, {launch_dsn: launch_store, foreign_dsn: foreign_store})

    token = set_hermes_home_override(peer)
    try:
        db = server._get_db()
        assert db is not None
        assert db.db_path == home / "state.db"
        assert db.get_session("launch-chat") is not None
        assert db.get_session("peer-chat") is None
        assert server._get_db() is db
        assert get_hermes_home() == peer
        with _owned(server._open_profile_session_db(home)) as own:
            assert own is not db
            assert own.get_session("launch-chat") is not None
        with _owned(server._open_profile_session_db(peer)) as foreign:
            assert foreign.get_session("peer-chat") is not None
            assert foreign.get_session("launch-chat") is None
            if foreign_backend == "sqlite":
                same = registry.acquire(peer / "state.db")
                assert same is foreign
                registry.release_or_close(same)
                assert foreign.get_session("peer-chat") is not None
        assert get_hermes_home() == peer
    finally:
        reset_hermes_home_override(token)
        if server._db is not None:
            registry.release_or_close(server._db)
            server._db = None
    expected = [(launch_dsn, False), (launch_dsn, False)]
    if foreign_backend == "postgres":
        expected.append((foreign_dsn, False))
        assert not (peer / "state.db").exists()
    assert calls == expected
    assert not (home / "state.db").exists()


def test_explicit_sqlite_registry_paths_share_while_active_postgres_stays_private(
    profile_homes, tmp_path, monkeypatch,
):
    import hermes_state_registry as registry

    home, _peer = profile_homes
    dsn = "postgresql://launch:secret@db.invalid/launch"
    remote = tmp_path / "remote.db"
    _seed(remote, "launch-chat", "Remote history")
    (home / "config.yaml").write_text("sessions:\n  state_backend: postgres\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_STATE_DATABASE_URL", dsn)
    calls = _mock_postgres_stores(monkeypatch, {dsn: remote})

    from hermes_cli.web_server_sessions import _open_session_db_for_profile

    with _owned(_open_session_db_for_profile(None, read_only=True)) as reader:
        assert reader.get_session("launch-chat") is not None
        assert reader.read_only is True
    first, second = registry.acquire(), registry.acquire()
    try:
        assert first is not second
        assert registry.release(first) is False
        registry.release_or_close(first)
        assert first._conn is None
        assert second.get_session("launch-chat") is not None
        sqlite_a = registry.acquire(home / "state.db")
        sqlite_b = registry.acquire(home / "state.db")
        assert sqlite_a is sqlite_b
        assert sqlite_a._is_postgres is False
        sqlite_a.create_session("sqlite-only", source="cli")
        registry.release_or_close(sqlite_a)
        assert sqlite_b.get_session("sqlite-only") is not None
        registry.release_or_close(sqlite_b)
        assert sqlite_b._conn is None
        assert second.get_session("sqlite-only") is None
    finally:
        registry.release_or_close(first)
        registry.release_or_close(second)
        registry.close_all_under(home)
    assert calls == [(dsn, True), (dsn, False), (dsn, False)]


# ---------------------------------------------------------------------------
# levos db60c64db / f1d844e30, on a real PostgreSQL (the fork's ephemeral
# Unix-socket cluster: ``initdb`` / ``pg_ctl`` on PATH or in PG3_PERCENT_PG_BIN)
# ---------------------------------------------------------------------------


def _authority_peer(peer, dsn):
    (peer / "config.yaml").write_text("sessions:\n  state_backend: authority\n", encoding="utf-8")
    (peer / ".env").write_text(f"HERMES_STATE_POSTGRES_DSN={dsn}\n", encoding="utf-8")


def test_readonly_pg_profile_open_rejects_writes_on_the_server(profile_homes, postgres_dsn):
    """``open_store_for_profile(read_only=True)`` reaches PostgreSQL read-only:
    it reads the peer's rows, runs no DDL, and the SERVER refuses an INSERT
    (SQLSTATE 25006) so no row lands; the writable open still writes."""
    import psycopg

    _home, peer = profile_homes
    _authority_peer(peer, postgres_dsn)
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP SCHEMA public CASCADE")
        raw.execute("CREATE SCHEMA public")

    # A read-only open never provisions the schema it is asked to read.
    with pytest.raises(Exception):
        release_or_close(hermes_state_postgres.open_store_for_profile("peer", read_only=True))
    with psycopg.connect(postgres_dsn) as raw:
        assert raw.execute("SELECT to_regclass('sessions')").fetchone()[0] is None

    writer = hermes_state_postgres.open_store_for_profile("peer", read_only=False)
    try:
        assert writer._is_postgres is True
        writer.create_session("ro-seed", source="cli")
    finally:
        release_or_close(writer)

    reader = hermes_state_postgres.open_store_for_profile("peer", read_only=True)
    try:
        assert reader._is_postgres is True
        assert reader.get_session("ro-seed") is not None
        with pytest.raises(Exception) as excinfo:
            reader._conn.execute(
                "INSERT INTO sessions (id, source, started_at) VALUES (?, ?, ?)",
                ("ro-smuggled", "cli", 1.0),
            )
        assert getattr(excinfo.value, "sqlstate", None) == "25006", excinfo.value
    finally:
        try:
            reader._conn.rollback()
        except Exception:
            pass
        release_or_close(reader)
    with psycopg.connect(postgres_dsn) as raw:
        assert raw.execute(
            "SELECT COUNT(*) FROM sessions WHERE id = 'ro-smuggled'"
        ).fetchone()[0] == 0
    assert not (peer / "state.db").exists()


def test_null_safe_cas_statements_run_verbatim_on_postgres(tmp_path, postgres_dsn):
    """The title / api_content CAS statements are journalled as SQLite text and
    replayed by dual-write, so the text itself must be NULL-safe PostgreSQL
    (``col IS ?`` is a syntax error there), not rely on a rewrite layer."""
    import psycopg

    db = hermes_state.SessionDB(db_path=tmp_path / "state.db")
    recorded = []

    class _Recording:
        def __init__(self, conn):
            self._inner = conn

        def execute(self, sql, params=()):
            recorded.append((sql, tuple(params)))
            return self._inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __enter__(self):
            return self._inner.__enter__() and self

        def __exit__(self, *exc):
            return self._inner.__exit__(*exc)

    try:
        db.create_session("cas", source="cli")
        db.append_message("cas", "user", "hello")
        db._conn = _Recording(db._conn)
        assert db.set_session_title("cas", "first") is True
        assert db.set_latest_user_api_content("cas", "hello", "api-bytes") == 1
        db._conn = db._conn._inner
    finally:
        db.close()
    cas = [(sql, params) for sql, params in recorded
           if sql.lstrip().upper().startswith("UPDATE") and " AND " in sql.upper()]
    assert {sql.split()[1] for sql, _ in cas} == {"sessions", "messages"}, recorded

    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS sessions, messages CASCADE")
        raw.execute("CREATE TABLE sessions (id TEXT, title TEXT, title_source TEXT)")
        raw.execute(
            "CREATE TABLE messages (id BIGINT, session_id TEXT, role TEXT, "
            "active INTEGER, content TEXT, api_content TEXT)"
        )
        raw.execute("INSERT INTO sessions VALUES ('cas', NULL, NULL)")
        raw.execute("INSERT INTO messages VALUES (1, 'cas', 'user', 1, 'hello', NULL)")
        for sql, params in cas:
            # Verbatim text; only the placeholder style differs between drivers.
            assert raw.execute(sql.replace("?", "%s"), params).rowcount == 1, sql
        assert raw.execute("SELECT title FROM sessions").fetchone()[0] == "first"
        assert raw.execute("SELECT api_content FROM messages").fetchone()[0] == "api-bytes"
        raw.execute("DROP TABLE sessions, messages")
