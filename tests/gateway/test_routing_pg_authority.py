"""levos 0064 (C05) — the gateway routing index is row-level on PostgreSQL authority.

On ``HERMES_STATE_BACKEND=authority`` two gateways of one profile (the old and
the new pod of a rolling update) share ``gateway_routing`` and nothing else:
each pod has its own disk. A save then writes only the rows this process
changed (no scope-wide DELETE + INSERT), a key lookup re-reads its row, and
``sessions/sessions.json`` is neither read nor written. Every other backend
keeps the whole-index rewrite and the mirror.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips) — the fixture 0060 uses.
"""

from __future__ import annotations

import json
import multiprocessing
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest

from hermes_aux_store import AuxStoreUnavailable
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0064&connect_timeout=1"
)
# Both pods of a profile run with the same HERMES_HOME path (chart), so their
# routing scope — the resolved sessions dir — is the same string. The test
# gives each process its own directory (no shared disk) and pins the scope to
# that production path.
POD_SCOPE = "/var/lib/session-plane/hermes/profiles/concierge/sessions"
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
)


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def authority(monkeypatch, postgres_dsn):
    """Authority profile whose core schema exists, routing table empty."""
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    SessionDB(read_only=False).close()
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DELETE FROM gateway_routing")
    return postgres_dsn


def _source(chat_id: str):
    from gateway.config import Platform
    from gateway.session import SessionSource

    return SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=chat_id,
        chat_name=chat_id,
        chat_type="dm",
        user_id=f"user-{chat_id}",
    )


def _store(home: Path, monkeypatch=None):
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore

    if monkeypatch is not None:
        monkeypatch.setattr(SessionStore, "_routing_scope", lambda self: POD_SCOPE)
    return SessionStore(sessions_dir=home / "sessions", config=GatewayConfig())


def _rows(dsn):
    with psycopg.connect(dsn) as raw:
        found = raw.execute(
            "SELECT session_key, entry_json FROM gateway_routing WHERE scope = %s",
            (POD_SCOPE,),
        ).fetchall()
    return {key: json.loads(entry) for key, entry in found}


def _days_ago(days):
    from gateway.session import _now

    return _now() - timedelta(days=days)


def _files(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


# --------------------------------------------------------------------------
# Two processes = two overlapping pods: own HERMES_HOME each, PG shared.
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")


def _pod(home, commands, replies):
    """One gateway pod: answers routing commands until told to stop."""
    try:
        from gateway.session import SessionStore

        SessionStore._routing_scope = lambda self: POD_SCOPE
        store = _store(Path(home))
        store._ensure_loaded()
        replies.put(("ready", None))
        while True:
            command, chat_id, *args = commands.get(timeout=120)
            if command == "stop":
                store.close_all_db_handles()
                replies.put(("stopped", None))
                return
            if command == "chat":
                entry = store.get_or_create_session(_source(chat_id))
                replies.put(("chat", (entry.session_id, entry.model_override)))
            elif command == "model":
                key = store._generate_session_key(_source(chat_id))
                store.set_model_override(key, {"model": args[0], "provider": "p"})
                replies.put(("model", None))
    except BaseException as exc:
        replies.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


class _Pod:
    def __init__(self, home: Path, monkeypatch):
        home.mkdir(parents=True)
        # spawn copies os.environ at start(): this pod gets its own home.
        monkeypatch.setenv("HERMES_HOME", str(home))
        self.home = home
        self.commands, self.replies = _SPAWN.Queue(), _SPAWN.Queue()
        self.process = _SPAWN.Process(
            target=_pod, args=(str(home), self.commands, self.replies)
        )
        self.process.start()
        assert self._reply() is None

    def _reply(self):
        kind, value = self.replies.get(timeout=120)
        assert kind != "error", value
        return value

    def ask(self, command, chat_id="", *args):
        self.commands.put((command, chat_id, *args))
        return self._reply()

    def stop(self):
        if self.process.is_alive():
            self.ask("stop")
        self.process.join(30)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(5)


def test_two_pods_keep_and_see_each_others_routing_rows(
    authority, tmp_path, monkeypatch
):
    """B-F05: overlapping pods must not erase or miss each other's mappings.

    The old pod A saves after the new pod B has created a key A never loaded,
    then B saves after A did the same. With a scope-wide rewrite each save
    deletes the other pod's key and its /model override; row-level saves keep
    both, and each pod reads the other's key back from the shared row.
    """
    old = _Pod(tmp_path / "pod-old", monkeypatch)
    pods = [old]
    try:
        sid_a, _ = old.ask("chat", "a")
        new = _Pod(tmp_path / "pod-new", monkeypatch)
        pods.append(new)
        sid_b, _ = new.ask("chat", "b")  # the old pod never loaded this key
        sid_c, _ = old.ask("chat", "c")  # the new pod never loaded this key
        old.ask("model", "c", "model-from-old")
        new.ask("model", "b", "model-from-new")

        overlap_rows = _rows(authority)  # both pods alive, both saved last
        assert sorted(e["session_id"] for e in overlap_rows.values()) == sorted([
            sid_a,
            sid_b,
            sid_c,
        ])
        # Each pod resolves the key the other created, /model included.
        assert old.ask("chat", "b") == (
            sid_b,
            {"model": "model-from-new", "provider": "p"},
        )
        old.stop()  # the old pod terminates; its disk is gone with it
        assert new.ask("chat", "c") == (
            sid_c,
            {"model": "model-from-old", "provider": "p"},
        )
    finally:
        for pod in pods:
            pod.stop()

    final = {
        e["session_id"]: e.get("model_override") for e in _rows(authority).values()
    }
    assert final == {
        sid_a: None,
        sid_b: {"model": "model-from-new", "provider": "p"},
        sid_c: {"model": "model-from-old", "provider": "p"},
    }
    # A-F10: no sessions.json mirror on either pod's disk.
    for pod in pods:
        assert not [f for f in _files(pod.home) if f.startswith("sessions/")]


# --------------------------------------------------------------------------
# Single-process contracts
# --------------------------------------------------------------------------


def test_authority_ignores_a_left_behind_sessions_json_and_writes_none(
    authority, tmp_path, monkeypatch
):
    """A-F10: a legacy file on the pod's disk cannot revive a dropped key."""
    home = tmp_path / "pod"
    legacy = home / "sessions" / "sessions.json"
    legacy.parent.mkdir(parents=True)
    ghost = {
        "session_key": "agent:main:telegram:dm:ghost",
        "session_id": "20260101_000000_deadbeef",
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-01T00:00:00",
    }
    legacy.write_text(json.dumps({ghost["session_key"]: ghost}), encoding="utf-8")
    before = legacy.read_bytes()

    store = _store(home, monkeypatch)
    entry = store.get_or_create_session(_source("x"))
    store.update_session(entry.session_key, last_prompt_tokens=7)

    assert store.lookup_by_session_key(ghost["session_key"]) is None
    assert set(_rows(authority)) == {entry.session_key}
    assert _rows(authority)[entry.session_key]["last_prompt_tokens"] == 7
    assert legacy.read_bytes() == before
    assert _files(home / "sessions") == ["sessions.json"]
    store.close_all_db_handles()


def test_authority_drops_only_rows_it_saw_and_follows_other_writers(
    authority, tmp_path, monkeypatch
):
    """Deletes are conditional and lookups re-read: a stale pod's prune or
    reset never removes a mapping another pod has rewritten since."""
    first = _store(tmp_path / "one", monkeypatch)
    second = _store(tmp_path / "two", monkeypatch)
    kept = first.get_or_create_session(_source("kept"))
    idle = first.get_or_create_session(_source("idle"))
    second._ensure_loaded()  # sees both rows

    # The other pod uses "kept" after this pod last read it.
    second.set_model_override(kept.session_key, {"model": "fresh"})
    stale = _days_ago(30)
    with first._lock:
        first._entries[kept.session_key].updated_at = stale
        first._entries[idle.session_key].updated_at = stale
    assert first.prune_old_entries(max_age_days=7) == 2

    rows = _rows(authority)
    assert set(rows) == {kept.session_key}  # idle: unchanged row, deleted
    assert rows[kept.session_key]["model_override"] == {"model": "fresh"}
    # The row is the truth: this pod follows it back instead of recreating.
    assert first.lookup_by_session_key(kept.session_key).session_id == kept.session_id
    assert first.get_model_override(kept.session_key) == {"model": "fresh"}
    # A key the other pod deleted disappears here too.
    assert second.lookup_by_session_key(idle.session_key) is None

    # A reset on one pod is what the other pod routes to next.
    reset = second.reset_session(kept.session_key)
    assert first.get_or_create_session(_source("kept")).session_id == reset.session_id
    first.close_all_db_handles()
    second.close_all_db_handles()


def test_authority_without_postgres_raises_and_writes_no_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    home = tmp_path / "pod"
    store = _store(home, monkeypatch)
    for call in (
        lambda: store.get_or_create_session(_source("x")),
        lambda: store.lookup_by_session_key("agent:main:telegram:dm:x"),
    ):
        with pytest.raises(AuxStoreUnavailable) as raised:
            call()
        assert "nonexistent-0064" not in str(raised.value)
    assert _files(home) == []


@pytest.mark.parametrize("backend", [None, "sqlite"])
def test_non_authority_keeps_the_scope_rewrite_and_the_mirror(
    backend, tmp_path, monkeypatch
):
    import hermes_state

    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", tmp_path / "state.db")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("row-level routing path used off authority")

    monkeypatch.setattr(
        hermes_state.SessionDB, "apply_gateway_routing_changes", forbidden
    )
    monkeypatch.setattr(hermes_state.SessionDB, "load_gateway_routing_entry", forbidden)
    replaced = []
    real_replace = hermes_state.SessionDB.replace_gateway_routing_entries
    monkeypatch.setattr(
        hermes_state.SessionDB,
        "replace_gateway_routing_entries",
        lambda self, entries, **kw: (
            replaced.append(set(entries)),
            real_replace(self, entries, **kw),
        ),
    )
    legacy = tmp_path / "sessions" / "sessions.json"
    legacy.parent.mkdir()
    legacy.write_text(
        json.dumps({
            "agent:main:telegram:dm:old": {
                "session_key": "agent:main:telegram:dm:old",
                "session_id": "20260101_000000_cafebabe",
                "created_at": "2026-01-01T00:00:00",
                "updated_at": "2026-01-01T00:00:00",
            }
        }),
        encoding="utf-8",
    )

    store = _store(tmp_path)
    entry = store.get_or_create_session(_source("y"))

    assert store.lookup_by_session_key("agent:main:telegram:dm:old") is not None
    assert replaced and replaced[-1] == {
        "agent:main:telegram:dm:old",
        entry.session_key,
    }
    assert entry.session_key in json.loads(legacy.read_text(encoding="utf-8"))
    store._db.close()
