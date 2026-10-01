"""levos v3 (t_ee025292) — auto-continue never runs a turn a second time.

opsi's P1 on b48c99e: the broker restarted mid-turn, the core reaped the room's session
on the WS drop while its turn thread kept running, and the broker's ``session.resume``
(no ``source``) found that turn's marker — owned by this very process — and ran the
prompt again as an auto-continue: two turns, two model calls, two assistant rows and a
note. Pinned here on a real PostgreSQL authority store, the real turn thread and a real
AIAgent whose model is a stand-in, through each ``session.resume`` entry point (cold,
eager build, deferred hydration):

* T1 a turn still running in this process is not continued by any resume, whatever
  the session's source, however its record was reaped, and on the file store too;
* T2 a turn whose pod really died (another owner, lease run out) is still continued,
  exactly once, carrying the original prompt and the attempt count;
* T3 a levos-room conversation (created with ``source=levos-room``, resumed without
  one, as the broker does) is never auto-continued, even after its pod died mid-turn,
  and its agents keep the room's platform;
* T4 bot_room, freshness, the crash-loop breaker and the live-owner fences are unchanged;
* T6 a turn judged interrupted that concludes (or is superseded) before the
  continuation is dispatched is not continued.

Runs on the fork's ephemeral PostgreSQL (``initdb`` / ``pg_ctl`` on PATH or in
``PG3_PERCENT_PG_BIN``; missing tools are errors, never skips).
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn
from tests.tui_gateway.test_submit_idempotency_pg import (
    _REAL_RUN_AFTER_AGENT_READY,
    _REAL_TURN_STUBS,
    _clean_env as _clean_env,
    _quiet_gateway,
    authority as authority,
)
from tui_gateway import server
from tui_gateway.session_history import _AUTO_CONTINUE_NOTE_PREFIX
from tui_gateway.transport import bind_transport, reset_transport

_REAL_START_AGENT_BUILD = server._start_agent_build
# Other pods' owner ids (gateway.turn_owner format): never this host, so only their lease decides.
_DEAD_POD = "dead-pod|pid:[1]|1|1"
_LIVE_POD = "live-pod|pid:[1]|1|1"
_ENTRIES = {"cold": {}, "eager": {"eager_build": True}, "hydration": {"defer_history": True}}


class WS:
    """A client socket: the frames it was sent; closed when the client goes away."""

    def __init__(self):
        self.frames: list = []
        self._closed = False

    def write(self, obj: dict) -> bool:
        self.frames.append(obj)
        return not self._closed

    def close(self) -> None:
        self._closed = True


class Model:
    """The provider every agent of the core calls: records each request and answers. After
    :meth:`hold`, the next call stays open until the returned gate is set."""

    def __init__(self, answer: str):
        self.answer = answer
        self.requests: list = []
        self.gate: threading.Event | None = None
        self.entered = threading.Event()
        self._lock = threading.Lock()

    def hold(self) -> threading.Event:
        self.entered.clear()
        self.gate = threading.Event()
        return self.gate

    def __call__(self, **kwargs):
        with self._lock:
            self.requests.append(kwargs["messages"])
            gate, self.gate = self.gate, None
        self.entered.set()
        if gate is not None:
            assert gate.wait(30)
        return SimpleNamespace(
            choices=[SimpleNamespace(
                message=SimpleNamespace(content=self.answer, tool_calls=None), finish_reason="stop")],
            model="test/model",
            usage=SimpleNamespace(prompt_tokens=5, completion_tokens=3, total_tokens=8))


def _agent(db, session_id: str, platform: str, model: Model):
    """A real AIAgent on *db* whose provider is *model* (a loopback endpoint, so a metadata
    probe never leaves the host)."""
    from run_agent import AIAgent

    with (
        patch("model_tools.get_tool_definitions", return_value=[]),
        patch("model_tools.check_toolset_requirements", return_value={}),
        patch("agent.process_bootstrap.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key", base_url="http://127.0.0.1:9/v1", model="test/model",
            quiet_mode=True, skip_context_files=True, skip_memory=True,
            session_db=db, session_id=session_id, platform=platform)
    agent._disable_streaming = True
    agent.client = MagicMock()
    agent.client.chat.completions.create.side_effect = model
    return agent


def _await(condition, what: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.02)


def _call(ws: WS, method: str, params: dict) -> dict:
    """One JSON-RPC request arriving on *ws*."""
    token = bind_transport(ws)
    try:
        return server.handle_request({"id": method, "method": method, "params": params})
    finally:
        reset_transport(token)


class Core:
    """The core under test, observed from outside: ``kinds`` holds the display kind of every
    turn it started (``None`` = a user turn), ``decisions`` every resume's auto-continue
    decision, ``built`` the (session, platform) of every agent it built, ``model`` every
    model request."""

    def __init__(self, db, dsn: str | None, sids: list):
        self.db, self.dsn, self.sids = db, dsn, sids
        self.model = Model("the answer")
        self.kinds: list = []
        self.decisions: list = []
        self.built: list = []
        self.threads: list = []
        self.gates: list = []

    # ── the client's side of the wire ───────────────────────────────────
    def create(self, ws: WS, source: str, **params) -> tuple[str, str]:
        created = _call(ws, "session.create", {"cols": 80, "source": source, **params})["result"]
        self.sids.append(created["session_id"])
        return created["session_id"], created["stored_session_id"]

    def hold(self) -> threading.Event:
        """Hold the next model call open (released at teardown at the latest)."""
        gate = self.model.hold()
        self.gates.append(gate)
        return gate

    def submit(self, ws: WS, sid: str, text: str, *, hold: bool = False) -> dict:
        """Submit *text*; with *hold* the turn's model call stays open until :meth:`release`."""
        gate = self.hold() if hold else None
        self.model.entered.clear()
        reply = _call(ws, "prompt.submit", {"session_id": sid, "text": text})
        assert reply.get("result", {}).get("status") == "streaming", reply
        assert self.model.entered.wait(15), "the turn never reached its model call"
        session = server._sessions[sid]
        if not hold:
            self.settle(session)
        return {"session": session, "gate": gate}

    def release(self, turn: dict) -> None:
        turn["gate"].set()
        self.settle(turn["session"])

    def settle(self, session: dict) -> None:
        _await(lambda: (thread := session.get("_run_thread")) is not None and not thread.is_alive(),
               "the turn thread to finish")

    def settle_all(self) -> None:
        _await(lambda: len(self.threads) == len(self.kinds)
               and all(thread is None or not thread.is_alive() for thread in self.threads),
               "every turn to finish")

    def drop(self, ws: WS, key: str) -> None:
        """The client goes away; wait until the core holds no live record of *key*."""
        ws.close()
        server._close_sessions_for_transport(ws)
        _await(lambda: server._find_live_session_by_key(key, None) is None, "the record to be reaped")

    def resume(self, key: str, entry: str, **params) -> tuple[WS, dict, object]:
        """``session.resume`` the way the broker sends it; returns the client, the reply and
        the auto-continue decision it made (a hydrating resume decides on its own thread)."""
        ws, seen = WS(), len(self.decisions)
        reply = _call(ws, "session.resume", {"session_id": key, **_ENTRIES[entry], **params})
        assert "result" in reply, reply
        _await(lambda: len(self.decisions) > seen, "the resume's auto-continue decision")
        return ws, reply["result"], self.decisions[seen]

    def kickoff_ended(self, reply: dict) -> None:
        """Wait until the resume's continuation kickoff gave up (flag reset) or dispatched its turn."""
        record = server._sessions[reply["session_id"]]
        _await(lambda: record.get("_auto_continue_scheduled") is False or "auto_continue" in self.kinds,
               "the continuation kickoff to end")

    # ── what the stores hold ────────────────────────────────────────────
    def marker(self, key: str) -> dict | None:
        with psycopg.connect(self.dsn, autocommit=True) as raw:
            row = raw.execute(
                "SELECT prompt, started_at, attempts, auto_continue, owner FROM core_tui_turn_markers "
                "WHERE session_key = %s", (key,)).fetchone()
        return None if row is None else dict(zip(("prompt", "started_at", "attempts", "auto_continue", "owner"), row))

    def sql(self, statement: str, *params) -> None:
        """Change exactly one marker row."""
        with psycopg.connect(self.dsn, autocommit=True) as raw:
            assert raw.execute(statement, params).rowcount == 1

    def messages(self, key: str) -> list:
        return [(m["role"], m["content"]) for m in self.db.get_messages(key)]

    def pod_dies_after(self, ws: WS, sid: str, key: str, text: str, monkeypatch) -> None:
        """Run a turn on *sid* whose pod dies before the turn could clear its marker: the
        row stays, its owner is another pod whose lease ran out, and the replacement pod
        holds no live record."""
        with monkeypatch.context() as dying:
            dying.setattr(server, "clear_turn_marker", lambda *_a, **_k: None)
            self.submit(ws, sid, text)
        self.sql("UPDATE core_tui_turn_markers SET owner = %s, lease_expires_at = 0 WHERE session_key = %s",
                 _DEAD_POD, key)
        server._sessions.clear()


class FileCore(Core):
    """The same core off PostgreSQL authority: SQLite state.db, markers in a file."""

    def __init__(self, db, home: Path, sids: list):
        super().__init__(db, None, sids)
        self.home = home

    def marker(self, key: str) -> dict | None:
        path = self.home / "desktop" / "interrupted_turns.json"
        return json.loads(path.read_text(encoding="utf-8")).get(key) if path.exists() else None


def _run_real_turns(core: Core, monkeypatch) -> None:
    """Turns run on the real turn thread and a real AIAgent; agents are built by the real
    deferred / eager build paths with the core's model."""
    # No auxiliary model call (session titles) may leave the test.
    (Path(os.environ["HERMES_HOME"]) / "config.yaml").write_text(
        "auxiliary:\n  title_generation:\n    enabled: false\n", encoding="utf-8")

    def make_agent(sid, key, session_id=None, session_db=None, platform_override=None, **_kwargs):
        platform = server._resolve_agent_platform(platform_override)
        core.built.append((session_id or key, platform))
        return _agent(core.db, session_id or key, platform, core.model)

    real_submit, real_decide = server._run_prompt_submit, server._maybe_schedule_auto_continue

    def run_prompt_submit(rid, sid, session, text, **kwargs):
        core.kinds.append(kwargs.get("display_kind"))
        try:
            return real_submit(rid, sid, session, text, **kwargs)
        finally:
            core.threads.append(session.get("_run_thread"))

    def decide(sid, session, session_key):
        decision = real_decide(sid, session, session_key)
        core.decisions.append(decision)
        return decision

    monkeypatch.setattr(server, "_run_after_agent_ready", _REAL_RUN_AFTER_AGENT_READY)
    monkeypatch.setattr(server, "_start_agent_build", _REAL_START_AGENT_BUILD)
    monkeypatch.setattr(server, "_make_agent", make_agent)
    for name in (*_REAL_TURN_STUBS, "_announce_built_agent", "_start_session_services",
                 "_schedule_mcp_late_refresh"):
        monkeypatch.setattr(server, name, lambda *_a, **_k: None)
    monkeypatch.setattr(server, "_session_info", lambda *_a, **_k: {})
    monkeypatch.setattr(server, "_run_prompt_submit", run_prompt_submit)
    monkeypatch.setattr(server, "_maybe_schedule_auto_continue", decide)
    # A reaped record's turn thread gets a short settle wait instead of 5 s; a parked idle
    # record is reaped right away.
    monkeypatch.setattr(server, "_TURN_SETTLE_BEFORE_CLOSE_SECONDS", 0.2)
    monkeypatch.setattr(server, "_WS_ORPHAN_REAP_GRACE_S", 0.05)


def _wind_down(core: Core) -> None:
    for gate in core.gates:
        gate.set()
    for thread in core.threads:
        if thread is not None:
            thread.join(15)


@pytest.fixture
def core(authority, postgres_dsn, monkeypatch):
    db, _turns, sids = authority
    core = Core(db, postgres_dsn, sids)
    _run_real_turns(core, monkeypatch)
    yield core
    _wind_down(core)
    from tui_gateway import turn_marker

    if turn_marker._pg_renewer is not None:
        turn_marker._pg_renewer.stop()


@pytest.fixture
def file_core(tmp_path, monkeypatch):
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    _quiet_gateway(monkeypatch, db)
    monkeypatch.setattr(server, "_hermes_home", tmp_path)
    core = FileCore(db, tmp_path, [])
    _run_real_turns(core, monkeypatch)
    yield core
    _wind_down(core)
    for sid in core.sids:
        server._sessions.pop(sid, None)
    db.close()


def _auto_continued(core: Core) -> int:
    return core.kinds.count("auto_continue")


def _live_turn_survives_resumes(core: Core, source: str, entry: str, *, close_on_disconnect: bool) -> tuple:
    """A turn held in its model call, its record reaped with the client, then two resumes:
    neither may continue it, nor touch its marker. Returns ``(session key, marker)``."""
    ws = WS()
    sid, key = core.create(ws, source, **({"close_on_disconnect": True} if close_on_disconnect else {}))
    turn = core.submit(ws, sid, "summarise the incident", hold=True)
    marker = core.marker(key)
    assert marker is not None and marker["prompt"] == "summarise the incident"

    # The client goes away: the record is reaped, its turn thread lives on.
    core.drop(ws, key)
    assert turn["session"]["_run_thread"].is_alive()
    for _ in range(2):
        ws, _reply, decision = core.resume(key, entry)
        assert decision is None, "a resume scheduled the live turn's prompt again"
        assert core.marker(key) == marker  # held: not cleared, no attempt spent
        core.drop(ws, key)
    assert _auto_continued(core) == 0 and len(core.model.requests) == 1

    core.release(turn)
    core.settle_all()
    assert core.kinds == [None]  # the one user turn
    assert len(core.model.requests) == 1
    assert core.marker(key) is None  # its own conclusion cleared it
    return key, marker


# T1 — the turn is still running in this process: no resume may continue it
@pytest.mark.parametrize("entry", list(_ENTRIES))
@pytest.mark.parametrize("source", ["desktop", "tui", "levos-room"])
def test_turn_still_running_here_is_not_continued_by_a_resume(core, entry, source):
    from gateway.turn_owner import owner_id

    key, marker = _live_turn_survives_resumes(core, source, entry, close_on_disconnect=True)
    assert marker["owner"] == owner_id()
    assert core.messages(key) == [("user", "summarise the incident"), ("assistant", "the answer")]


# T1 — a detached Desktop record the WS-orphan reaper takes mid-turn (interrupt, then forced reap)
def test_turn_reaped_by_the_ws_orphan_reaper_is_not_continued(core, monkeypatch):
    monkeypatch.setattr(server, "_WS_ORPHAN_ACTIVITY_STALE_S", 0)
    monkeypatch.setattr(server, "_WS_ORPHAN_INTERRUPT_REAP_POLL_S", 0.05)
    monkeypatch.setattr(server, "_WS_ORPHAN_INTERRUPT_REAP_MAX_POLLS", 2)
    key, _marker = _live_turn_survives_resumes(core, "desktop", "cold", close_on_disconnect=False)
    assert [content for role, content in core.messages(key) if role == "user"] == ["summarise the incident"]


# T1 — off PostgreSQL authority the marker is a file; the hold is the same
def test_turn_still_running_here_is_not_continued_on_the_file_store(file_core):
    key, _marker = _live_turn_survives_resumes(file_core, "desktop", "cold", close_on_disconnect=True)
    assert file_core.messages(key) == [("user", "summarise the incident"), ("assistant", "the answer")]


# T2 — the turn's pod really died: auto-continue still resumes it, exactly once
@pytest.mark.parametrize("entry", list(_ENTRIES))
def test_turn_whose_pod_died_is_still_continued_once(core, entry, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "desktop")
    core.pod_dies_after(ws, sid, key, "fix the flaky test", monkeypatch)
    interrupted = core.marker(key)
    requests = len(core.model.requests)

    gate = core.hold()  # keep the continuation open to read its marker
    ws, reply, decision = core.resume(key, entry)
    assert decision == {"attempt": 1, "interrupted_at": interrupted["started_at"]}
    assert core.model.entered.wait(15), "the continuation never reached the model"
    running = core.marker(key)
    assert (running["prompt"], running["attempts"]) == ("fix the flaky test", 1)  # crash-loop breaker input
    gate.set()
    core.settle_all()

    assert core.kinds == [None, "auto_continue"]
    assert len(core.model.requests) == requests + 1
    note = [m for m in core.model.requests[-1] if m.get("role") == "user"][-1]["content"]
    assert note.startswith(_AUTO_CONTINUE_NOTE_PREFIX)
    assert "The interrupted request was:]\n\nfix the flaky test" in note  # the original prompt, once
    assert core.marker(key) is None
    core.drop(ws, key)


# T3 — a levos-room conversation is recovered by the room, never by auto-continue
@pytest.mark.parametrize("entry", list(_ENTRIES))
def test_levos_room_turn_is_not_auto_continued_after_its_pod_died(core, entry, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "levos-room", close_on_disconnect=True)
    core.submit(ws, sid, "first question")
    assert core.db.get_session(key)["source"] == "levos-room"
    core.drop(ws, key)

    # The broker reconnects (session.resume carries no source) and asks again; the pod
    # dies mid-turn.
    ws, resumed, decision = core.resume(key, "cold")
    assert decision is None
    core.pod_dies_after(ws, resumed["session_id"], key, "second question", monkeypatch)
    marker = core.marker(key)
    assert (marker["prompt"], marker["owner"]) == ("second question", _DEAD_POD)
    requests = len(core.model.requests)

    ws, _reply, decision = core.resume(key, entry)
    assert decision is None, "the levos-room turn was auto-continued"
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    assert core.marker(key) == marker  # left for the room's own recovery
    # Every agent of the room ran on the room's platform, created or resumed.
    assert {platform for _session, platform in core.built} == {"levos-room"}
    core.drop(ws, key)


# T3 — a room conversation compressed under an earlier core (its tip row says "tui") is still the room's:
# the broker resumes the key session.create gave it, and that row is a room's
def test_levos_room_compressed_before_this_fix_is_left_to_the_room(core):
    from tui_gateway.turn_marker import record_turn_start

    root, tip = "acroot_" + uuid.uuid4().hex[:8], "actip_" + uuid.uuid4().hex[:8]
    core.db.create_session(root, "levos-room")
    core.db.append_message(root, "user", "before compression")
    core.db.end_session(root, "compression")
    core.db.create_session(tip, "tui", parent_session_id=root)
    core.db.append_message(tip, "user", "after compression")
    record_turn_start(server._hermes_home, tip, "after compression")  # the tip's turn, then its pod died
    core.sql("UPDATE core_tui_turn_markers SET owner = %s, lease_expires_at = 0 WHERE session_key = %s",
             _DEAD_POD, tip)
    marker = core.marker(tip)

    ws, reply, decision = core.resume(root, "cold")
    assert reply["session_key"] == tip  # resumed at the compression tip
    assert decision is None
    assert _auto_continued(core) == 0 and core.model.requests == []
    assert core.marker(tip) == marker
    core.drop(ws, tip)


# T4 — the in-core room driver's sessions keep their exclusion, however the resume names the source
@pytest.mark.parametrize("resume_source", [None, "bot_room"])
def test_bot_room_turn_is_left_to_the_room_driver(core, resume_source, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "bot_room")
    core.pod_dies_after(ws, sid, key, "room task", monkeypatch)
    marker, requests = core.marker(key), len(core.model.requests)

    ws, _reply, decision = core.resume(key, "cold", **({"source": resume_source} if resume_source else {}))
    assert decision is None
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    assert core.marker(key) == marker
    core.drop(ws, key)


# T4 — freshness and the crash-loop breaker still clear an old or exhausted marker
@pytest.mark.parametrize("change", ["stale", "exhausted"])
def test_stale_or_exhausted_marker_is_cleared_not_continued(core, change, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "desktop")
    core.pod_dies_after(ws, sid, key, "old work", monkeypatch)
    update = {"stale": "started_at = started_at - 3600", "exhausted": "attempts = 2"}[change]
    core.sql(f"UPDATE core_tui_turn_markers SET {update} WHERE session_key = %s", key)
    requests = len(core.model.requests)

    ws, _reply, decision = core.resume(key, "cold")
    assert decision is None and core.marker(key) is None
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    core.drop(ws, key)


# T4 — a turn whose owner (another pod) still holds its lease is running there
def test_marker_of_a_live_owner_elsewhere_is_left_alone(core, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "desktop")
    core.pod_dies_after(ws, sid, key, "busy elsewhere", monkeypatch)
    core.sql("UPDATE core_tui_turn_markers SET owner = %s, "
             "lease_expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + 600 WHERE session_key = %s", _LIVE_POD, key)
    marker, requests = core.marker(key), len(core.model.requests)

    ws, _reply, decision = core.resume(key, "cold")
    assert decision is None
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    assert core.marker(key) == marker
    core.drop(ws, key)


# T4 — the session-slot fence (#94778) still refuses a continuation another live owner holds
def test_continuation_refused_by_the_session_slot_fence_keeps_the_marker(core, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "desktop")
    core.pod_dies_after(ws, sid, key, "contended work", monkeypatch)
    marker, requests = core.marker(key), len(core.model.requests)
    monkeypatch.setattr(server, "_ensure_active_session_slot", lambda _sid, _session: "another live owner")

    ws, reply, decision = core.resume(key, "cold")
    assert decision is not None  # judged interrupted; the fence refuses it at dispatch
    core.kickoff_ended(reply)
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    assert core.marker(key) == marker
    core.drop(ws, key)


# T6 — the judged turn concludes, or a newer one takes the key, before the continuation is dispatched
@pytest.mark.parametrize("change", ["concluded", "newer_turn"])
def test_continuation_is_dropped_when_its_marker_changes_before_dispatch(core, change, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "desktop")
    core.pod_dies_after(ws, sid, key, "racing work", monkeypatch)
    marker, requests = core.marker(key), len(core.model.requests)
    statement = {
        "concluded": "DELETE FROM core_tui_turn_markers WHERE session_key = %s",
        "newer_turn": "UPDATE core_tui_turn_markers SET started_at = started_at + 1 WHERE session_key = %s",
    }[change]
    real_wait = server._wait_agent

    def wait_agent(session, rid, timeout=30.0):
        if rid.startswith("__auto_continue__"):
            core.sql(statement, key)  # lands between the decision and the dispatch
        return real_wait(session, rid, timeout=timeout)

    monkeypatch.setattr(server, "_wait_agent", wait_agent)
    ws, reply, decision = core.resume(key, "cold")
    assert decision is not None
    core.kickoff_ended(reply)
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    after = core.marker(key)
    if change == "concluded":
        assert after is None
    else:
        assert after["started_at"] == marker["started_at"] + 1  # the newer turn's, untouched
    core.drop(ws, key)
