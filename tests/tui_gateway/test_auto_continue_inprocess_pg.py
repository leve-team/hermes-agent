"""levos v3 (t_ee025292) — auto-continue never runs a turn a second time.

opsi's P1 on b48c99e: the broker restarted mid-turn, the core reaped the room's session
on the WS drop while its turn thread kept running, and the broker's ``session.resume``
(no ``source``) found that turn's marker — owned by this very process — and ran the
prompt again as an auto-continue: two turns, two model calls, two assistant rows and a
note. Pinned here on a real PostgreSQL authority store, the real turn thread and a real
AIAgent whose model is a stand-in, through each ``session.resume`` entry point (cold,
eager build, deferred hydration):

* T1 a turn still running in this process is not continued by any resume, whatever
  the session's source;
* T3 a levos-room conversation (created with ``source=levos-room``, resumed without
  one, as the broker does) is never auto-continued, even after its pod died mid-turn.

Runs on the fork's ephemeral PostgreSQL (``initdb`` / ``pg_ctl`` on PATH or in
``PG3_PERCENT_PG_BIN``; missing tools are errors, never skips).
"""

from __future__ import annotations

import os
import threading
import time
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
    authority as authority,
)
from tui_gateway import server
from tui_gateway.transport import bind_transport, reset_transport

_REAL_START_AGENT_BUILD = server._start_agent_build
# Another pod's owner id (gateway.turn_owner format): never this host, so only its lease decides.
_DEAD_POD = "dead-pod|pid:[1]|1|1"
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
    """The provider every agent of the core calls: records each request and answers; a
    ``gate`` set before a call holds that one call open until the gate is released."""

    def __init__(self, answer: str):
        self.answer = answer
        self.requests: list = []
        self.gate: threading.Event | None = None
        self.entered = threading.Event()
        self._lock = threading.Lock()

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
    """A real AIAgent on the authority store whose provider is *model* (a loopback endpoint,
    so a metadata probe never leaves the host)."""
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
    decision, ``model`` every model request."""

    def __init__(self, db, dsn: str, sids: list):
        self.db, self.dsn, self.sids = db, dsn, sids
        self.model = Model("the answer")
        self.kinds: list = []
        self.decisions: list = []
        self.threads: list = []
        self.gates: list = []

    # ── the broker's side of the wire ──────────────────────────────────
    def create(self, ws: WS, source: str, **params) -> tuple[str, str]:
        created = _call(ws, "session.create", {"cols": 80, "source": source, **params})["result"]
        self.sids.append(created["session_id"])
        return created["session_id"], created["stored_session_id"]

    def submit(self, ws: WS, sid: str, text: str, *, hold: bool = False) -> dict:
        """Submit *text*; with *hold* the turn's model call stays open until :meth:`release`."""
        self.model.entered.clear()
        gate = None
        if hold:
            gate = self.model.gate = threading.Event()
            self.gates.append(gate)
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

    # ── PostgreSQL, read directly ───────────────────────────────────────
    def marker(self, key: str):
        with psycopg.connect(self.dsn, autocommit=True) as raw:
            return raw.execute(
                "SELECT prompt, started_at, attempts, auto_continue, owner FROM core_tui_turn_markers "
                "WHERE session_key = %s", (key,)).fetchone()

    def messages(self, key: str) -> list:
        with psycopg.connect(self.dsn, autocommit=True) as raw:
            return raw.execute(
                "SELECT role, content FROM messages WHERE session_id = %s ORDER BY id", (key,)).fetchall()

    def stored_source(self, key: str) -> str:
        with psycopg.connect(self.dsn, autocommit=True) as raw:
            return raw.execute("SELECT source FROM sessions WHERE id = %s", (key,)).fetchone()[0]

    def pod_dies_after(self, ws: WS, sid: str, key: str, text: str, monkeypatch) -> None:
        """Run a turn on *sid* whose pod dies before the turn could clear its marker: the
        row stays, its owner is another pod whose lease ran out, and the replacement pod
        holds no live record."""
        with monkeypatch.context() as dying:
            dying.setattr(server, "clear_turn_marker", lambda *_a, **_k: None)
            self.submit(ws, sid, text)
        with psycopg.connect(self.dsn, autocommit=True) as raw:
            assert raw.execute(
                "UPDATE core_tui_turn_markers SET owner = %s, lease_expires_at = 0 WHERE session_key = %s",
                (_DEAD_POD, key)).rowcount == 1
        server._sessions.clear()


@pytest.fixture
def core(authority, postgres_dsn, monkeypatch):
    """Turns run on the real turn thread and a real AIAgent; agents are built by the real
    deferred / eager build paths with the model above."""
    db, _turns, sids = authority
    core = Core(db, postgres_dsn, sids)
    # No auxiliary model call (session titles) may leave the test.
    (Path(os.environ["HERMES_HOME"]) / "config.yaml").write_text(
        "auxiliary:\n  title_generation:\n    enabled: false\n", encoding="utf-8")

    def make_agent(sid, key, session_id=None, session_db=None, platform_override=None, **_kwargs):
        return _agent(db, session_id or key, server._resolve_agent_platform(platform_override), core.model)

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
    yield core
    for gate in core.gates:
        gate.set()
    for thread in core.threads:
        if thread is not None:
            thread.join(15)
    from tui_gateway import turn_marker

    if turn_marker._pg_renewer is not None:
        turn_marker._pg_renewer.stop()


def _auto_continued(core: Core) -> int:
    return core.kinds.count("auto_continue")


# T1 — the turn is still running in this process: no resume may continue it
@pytest.mark.parametrize("entry", list(_ENTRIES))
@pytest.mark.parametrize("source", ["desktop", "tui", "levos-room"])
def test_turn_still_running_here_is_not_continued_by_a_resume(core, entry, source):
    from gateway.turn_owner import owner_id

    ws = WS()
    sid, key = core.create(ws, source, close_on_disconnect=True)
    turn = core.submit(ws, sid, "summarise the incident", hold=True)
    marker = core.marker(key)
    assert marker is not None and marker[0] == "summarise the incident" and marker[4] == owner_id()

    # The broker restarts: the record is reaped with the WS, its turn thread lives on.
    core.drop(ws, key)
    assert turn["session"]["_run_thread"].is_alive()
    for _ in range(2):
        ws, _reply, decision = core.resume(key, entry)
        assert decision is None, "a resume scheduled the live turn's prompt again"
        assert core.marker(key) == marker  # held: not cleared, no attempt spent
        core.drop(ws, key)
    assert _auto_continued(core) == 0 and len(core.model.requests) == 1

    core.release(turn)
    assert core.kinds == [None]  # the one user turn
    assert len(core.model.requests) == 1
    assert core.messages(key) == [("user", "summarise the incident"), ("assistant", "the answer")]
    assert core.marker(key) is None  # its own conclusion cleared it


# T3 — a levos-room conversation is recovered by the room, never by auto-continue
@pytest.mark.parametrize("entry", list(_ENTRIES))
def test_levos_room_turn_is_not_auto_continued_after_its_pod_died(core, entry, monkeypatch):
    ws = WS()
    sid, key = core.create(ws, "levos-room", close_on_disconnect=True)
    core.submit(ws, sid, "first question")
    assert core.stored_source(key) == "levos-room"
    core.drop(ws, key)

    # The broker reconnects (session.resume carries no source) and asks again; the pod
    # dies mid-turn.
    ws, resumed, decision = core.resume(key, "cold")
    assert decision is None
    core.pod_dies_after(ws, resumed["session_id"], key, "second question", monkeypatch)
    marker = core.marker(key)
    assert marker[0] == "second question" and marker[4] == _DEAD_POD
    requests = len(core.model.requests)

    ws, _reply, decision = core.resume(key, entry)
    assert decision is None, "the levos-room turn was auto-continued"
    assert _auto_continued(core) == 0 and len(core.model.requests) == requests
    assert core.marker(key) == marker  # left for the room's own recovery
    core.drop(ws, key)
