"""``session.final`` fires once a session settles on its last answer.

``message.complete`` is per turn, but one conversation spans many turns —
queued input, /goal continuations, background-process and async-subagent
wakeups. A consumer that only wants the conversation's final answer
(selected-session web push, levos patch 0071) needs the core to say "this
session has settled": the turn's answer is still the latest generation after
``FINAL_SETTLE_SECONDS``, nothing is running or queued, no async delegation
is live, and the session is not closing.

Contract pinned here:

* A settled turn emits ``session.final`` exactly once, with six payload keys.
* A turn followed by another turn (queued input, or any new turn started
  while waiting) never emits — only the last one does.
* Live delegations, interrupted turns and pending queue entries suppress it;
  an error turn is still final (``status: "error"``).
* A rejected ``_run_prompt_submit`` does not bump ``final_gen``.
* The emit happens outside ``history_lock`` and never touches ``running``.
"""

from __future__ import annotations

import logging
import threading
import time
import types

import pytest

from tui_gateway import server

SID = "sid-final"
SESSION_KEY = "session-final-key"
TITLE = "Quarterly report chat"
PAYLOAD_KEYS = {"stored_session_id", "gen", "status", "title", "preview", "finished_at"}
_REAL_THREAD = threading.Thread


class _InlineThread:
    """Run the turn synchronously so tests observe its final state."""

    def __init__(self, target=None, daemon=None, args=(), kwargs=None, **_extra):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}

    def start(self):
        if self._target is not None:
            self._target(*self._args, **self._kwargs)

    def is_alive(self):
        return False

    def join(self, timeout=None):
        return None


class _ParkedThread(_InlineThread):
    """An accepted turn that is still in flight: its body never runs."""

    def start(self):
        return None


def _agent(run_conversation):
    return types.SimpleNamespace(
        session_id=SESSION_KEY,
        run_conversation=run_conversation,
        clear_interrupt=lambda: None,
    )


def _session(agent, **extra):
    session = {
        "agent": agent,
        "session_key": SESSION_KEY,
        "history": [],
        "history_lock": threading.Lock(),
        "history_version": 0,
        "running": True,
        "attached_images": [],
        "image_counter": 0,
        "cols": 80,
        "slash_worker": None,
        "show_reasoning": False,
        "tool_progress_mode": "all",
        "inflight_turn": None,
        "final_gen": 0,
        "final_candidate": None,
        **extra,
    }
    server._sessions[SID] = session
    return session


@pytest.fixture()
def emits(monkeypatch):
    captured: list = []
    monkeypatch.setattr(
        server,
        "_emit",
        lambda event, sid, payload=None: captured.append((event, sid, payload)),
    )
    return captured


@pytest.fixture()
def timers(monkeypatch):
    """Every settle check armed during the test, so it can be awaited."""
    armed: list = []
    original = server._schedule_final_settle_check

    def _schedule(sid, session):
        # Turns run inline via a patched threading.Thread, which
        # threading.Timer builds on; the settle check itself must be a real
        # timer thread so it fires after the turn, as in production.
        inline = threading.Thread
        threading.Thread = _REAL_THREAD
        try:
            original(sid, session)
        finally:
            threading.Thread = inline
        armed.append(session["_final_settle_timer"])

    monkeypatch.setattr(server, "_schedule_final_settle_check", _schedule)
    yield armed
    for timer in armed:
        timer.cancel()


@pytest.fixture()
def turn_env(monkeypatch, tmp_path, emits, timers):
    """Neutralize the turn pipeline's environment-heavy side paths."""
    monkeypatch.setattr(server.threading, "Thread", _InlineThread)
    monkeypatch.setattr(server, "_wire_callbacks", lambda sid: None)
    monkeypatch.setattr(server, "_sync_agent_model_with_config", lambda sid, session: None)
    monkeypatch.setattr(server, "_session_cwd", lambda session: str(tmp_path))
    monkeypatch.setattr(server, "_register_session_cwd", lambda session: None)
    monkeypatch.setattr(server, "_tts_stream_begin", lambda: None)
    monkeypatch.setattr(server, "_sync_session_key_after_compress", lambda *a, **k: None)
    monkeypatch.setattr(server, "_get_usage", lambda agent: {})
    monkeypatch.setattr(server, "_load_cfg", lambda: {})
    monkeypatch.setattr(server, "_session_live_title", lambda session, key: TITLE)
    monkeypatch.setattr(server, "_session_has_active_delegations", lambda sid, session=None: False)
    monkeypatch.setattr(server, "FINAL_SETTLE_SECONDS", 0.01)
    yield emits
    server._sessions.pop(SID, None)


def _settle(timers):
    for timer in list(timers):
        timer.join(timeout=5)
        assert not timer.is_alive()


def _events(captured, name):
    return [payload for event, _sid, payload in captured if event == name]


def _finals(captured):
    return [(sid, payload) for event, sid, payload in captured if event == "session.final"]


# ── Settled turn ──────────────────────────────────────────────────────


def test_final_emitted_once_after_settle(turn_env, timers):
    answer = "Here is the full report. " * 20
    session = _session(_agent(lambda *a, **k: {"final_response": answer}))

    server._run_prompt_submit("rid", SID, session, "write the report")
    _settle(timers)

    finals = _finals(turn_env)
    assert len(finals) == 1
    sid, payload = finals[0]
    assert sid == SID
    assert set(payload) == PAYLOAD_KEYS
    assert payload["stored_session_id"] == SESSION_KEY
    assert payload["gen"] == session["final_gen"] == 1
    assert payload["status"] == "complete"
    assert payload["title"] == TITLE
    assert payload["preview"] == answer.strip()[:200]
    assert len(payload["preview"]) == 200
    assert abs(payload["finished_at"] - time.time()) < 60
    # Consumed: a second check has nothing left to announce.
    assert session["final_candidate"] is None
    server._final_settle_check(SID, session)
    assert len(_finals(turn_env)) == 1
    # message.complete is untouched and precedes the final.
    events = [event for event, _sid, _payload in turn_env]
    assert events.count("message.complete") == 1
    assert events.index("message.complete") < events.index("session.final")


# ── Suppression: a later turn wins ────────────────────────────────────


def test_final_suppressed_by_queued_prompt(turn_env, timers):
    """Input queued mid-turn starts the next turn: only that turn is final."""
    holder: dict = {}
    calls: list = []

    def run_conversation(message, **_kwargs):
        calls.append(message)
        session = holder["session"]
        if len(calls) == 1:
            with session["history_lock"]:
                server._enqueue_prompt(session, "and add a summary", None)
            return {"final_response": "first answer"}
        # Outlive the first turn's settle window: its check runs mid-turn.
        time.sleep(0.05)
        return {"final_response": "second answer"}

    session = _session(_agent(run_conversation))
    holder["session"] = session

    server._run_prompt_submit("rid", SID, session, "write the report")
    _settle(timers)

    assert calls == ["write the report", "and add a summary"]
    assert [p["text"] for p in _events(turn_env, "message.complete")] == [
        "first answer",
        "second answer",
    ]
    finals = _finals(turn_env)
    assert len(finals) == 1
    assert finals[0][1]["preview"] == "second answer"
    assert finals[0][1]["gen"] == 2


def test_final_suppressed_by_queued_prompt_pending_at_check(turn_env, timers, monkeypatch):
    """An entry still sitting in the queue at check time means not settled."""
    session = _session(_agent(lambda *a, **k: {"final_response": "answer"}))
    original = server._schedule_final_settle_check

    def _schedule_then_queue(sid, sess):
        original(sid, sess)
        with sess["history_lock"]:
            sess["queued_prompt"] = {"text": "later", "transport": None}

    monkeypatch.setattr(server, "_schedule_final_settle_check", _schedule_then_queue)
    # The entry is waiting to be picked up, not dispatched by this turn.
    monkeypatch.setattr(server, "_drain_queued_prompt", lambda rid, sid, sess: False)

    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    assert _finals(turn_env) == []
    assert session["final_candidate"] is None


def test_final_suppressed_by_new_turn(turn_env, timers, monkeypatch, caplog):
    """A turn started while waiting to settle supersedes the pending answer."""
    session = _session(_agent(lambda *a, **k: {"final_response": "first answer"}))
    original = server._schedule_final_settle_check

    def _schedule_then_start_turn(sid, sess):
        original(sid, sess)
        # A new prompt lands during the settle window; its turn is accepted
        # (generation bumps) and is still running when the check fires.
        monkeypatch.setattr(server.threading, "Thread", _ParkedThread)
        with sess["history_lock"]:
            sess["running"] = True
        assert server._run_prompt_submit("rid", sid, sess, "one more thing") is True

    monkeypatch.setattr(server, "_schedule_final_settle_check", _schedule_then_start_turn)
    with caplog.at_level(logging.INFO, logger=server.logger.name):
        server._run_prompt_submit("rid", SID, session, "write the report")
        _settle(timers)

    assert session["final_gen"] == 2
    assert session["running"] is True
    assert _finals(turn_env) == []
    assert "session.final skipped" in caplog.text


# ── Suppression: background work, interrupt, running, closing ────────


def test_final_suppressed_by_active_delegation(turn_env, timers, monkeypatch):
    delegation_checks: list = []

    def _live(sid, session=None):
        delegation_checks.append(sid)
        return True

    monkeypatch.setattr(server, "_session_has_active_delegations", _live)
    session = _session(_agent(lambda *a, **k: {"final_response": "dispatched a subagent"}))

    server._run_prompt_submit("rid", SID, session, "research this")
    _settle(timers)

    assert delegation_checks == [SID]
    assert _finals(turn_env) == []
    # Dropped, not retried; the check never flips running.
    assert session["final_candidate"] is None
    assert session["running"] is False


def test_final_not_for_interrupted(turn_env, timers):
    session = _session(
        _agent(lambda *a, **k: {"final_response": "partial", "interrupted": True})
    )

    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    assert [p["status"] for p in _events(turn_env, "message.complete")] == ["interrupted"]
    assert timers == []
    assert session["final_candidate"] is None
    assert _finals(turn_env) == []


def test_final_suppressed_while_running(turn_env, timers, monkeypatch):
    """A wakeup that claimed ``running`` before its turn started blocks it."""
    session = _session(_agent(lambda *a, **k: {"final_response": "answer"}))
    original = server._schedule_final_settle_check

    def _schedule_then_claim(sid, sess):
        original(sid, sess)
        with sess["history_lock"]:
            sess["running"] = True

    monkeypatch.setattr(server, "_schedule_final_settle_check", _schedule_then_claim)
    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    assert _finals(turn_env) == []
    assert session["running"] is True


def test_final_suppressed_when_session_closing(turn_env, timers, monkeypatch):
    session = _session(_agent(lambda *a, **k: {"final_response": "answer"}))
    original = server._schedule_final_settle_check

    def _schedule_then_close(sid, sess):
        original(sid, sess)
        with server._sessions_lock:
            sess["_closing"] = True
            server._sessions.pop(sid, None)

    monkeypatch.setattr(server, "_schedule_final_settle_check", _schedule_then_close)
    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    assert _finals(turn_env) == []


def test_final_suppressed_when_session_detached(turn_env, timers, monkeypatch):
    """A record replaced in the registry never announces a final."""
    session = _session(_agent(lambda *a, **k: {"final_response": "answer"}))
    original = server._schedule_final_settle_check

    def _schedule_then_replace(sid, sess):
        original(sid, sess)
        with server._sessions_lock:
            server._sessions[sid] = dict(sess)

    monkeypatch.setattr(server, "_schedule_final_settle_check", _schedule_then_replace)
    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    assert _finals(turn_env) == []


# ── Error turns are final too ─────────────────────────────────────────


def test_final_error_status_emitted(turn_env, timers):
    session = _session(
        _agent(
            lambda *a, **k: {
                "final_response": "",
                "error": "provider 400: invalid model",
                "failed": True,
            }
        )
    )

    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    finals = _finals(turn_env)
    assert len(finals) == 1
    payload = finals[0][1]
    assert payload["status"] == "error"
    assert payload["preview"] == "Error: provider 400: invalid model"
    assert set(payload) == PAYLOAD_KEYS


def test_final_error_status_emitted_for_turn_exception(turn_env, timers):
    def _boom(*_a, **_k):
        raise RuntimeError("connection reset mid-stream")

    session = _session(_agent(_boom))

    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    completes = _events(turn_env, "message.complete")
    assert [p["status"] for p in completes] == ["error"]
    finals = _finals(turn_env)
    assert len(finals) == 1
    assert finals[0][1]["status"] == "error"
    assert finals[0][1]["preview"] == completes[0]["text"][:200]


# ── Generation bookkeeping ────────────────────────────────────────────


def test_final_rejected_submit_does_not_bump_gen(turn_env, timers):
    candidate = {"gen": 3, "text": "t", "status": "complete", "title": "", "finished_at": 1.0}
    session = _session(
        _agent(lambda *a, **k: pytest.fail("a rejected submit must not run")),
        _closing=True,
        final_gen=3,
        final_candidate=candidate,
    )

    assert server._run_prompt_submit("rid", SID, session, "go") is False

    assert session["final_gen"] == 3
    assert session["final_candidate"] is candidate
    assert session["running"] is False


def test_final_rejected_submit_does_not_bump_gen_for_stale_queue(turn_env, timers):
    session = _session(
        _agent(lambda *a, **k: pytest.fail("a rejected submit must not run")),
        final_gen=5,
        _queued_prompt_generation=2,
    )

    assert (
        server._run_prompt_submit("rid", SID, session, "go", queued_prompt_generation=1)
        is False
    )

    assert session["final_gen"] == 5


def test_final_emit_outside_lock(turn_env, timers, monkeypatch):
    lock_free_at_emit: list = []
    session = _session(_agent(lambda *a, **k: {"final_response": "answer"}))
    captured = turn_env

    def _emit(event, sid, payload=None):
        if event == "session.final":
            acquired = session["history_lock"].acquire(blocking=False)
            if acquired:
                session["history_lock"].release()
            lock_free_at_emit.append(acquired)
        captured.append((event, sid, payload))

    monkeypatch.setattr(server, "_emit", _emit)

    server._run_prompt_submit("rid", SID, session, "go")
    _settle(timers)

    assert lock_free_at_emit == [True]
    assert len(_finals(captured)) == 1
