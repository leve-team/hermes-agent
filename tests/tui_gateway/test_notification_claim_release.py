"""A notification path that loses its delivery claim must not strand the session busy.

The poller loop, its shutdown drain and the post-turn drain each set
``session["running"] = True`` under ``history_lock`` *before* calling
``claim_event_delivery``. When the claim comes back ``None`` (another consumer
already owns the durable delegation event) or the claim call itself raises, no
turn is started — so the path must hand ``running`` back. Otherwise every later
user prompt is treated as busy and this session's notifications re-queue
forever.
"""

import queue as _queue_mod
import threading
import types

import pytest

import tools.async_delegation as ad
from tools.process_registry import process_registry
from tui_gateway import server


def _session(agent=None, **extra):
    return {
        "agent": agent if agent is not None else types.SimpleNamespace(),
        "session_key": "session-key",
        "history": [],
        "history_lock": threading.Lock(),
        "history_version": 0,
        "running": False,
        "attached_images": [],
        "image_counter": 0,
        "cols": 80,
        "slash_worker": None,
        "show_reasoning": False,
        "tool_progress_mode": "all",
        **extra,
    }


def _completion_event(proc_id):
    return {
        "type": "completion",
        "session_id": proc_id,
        "command": "echo hello",
        "exit_code": 0,
        "output": "hello",
    }


@pytest.fixture
def isolated_queue(monkeypatch):
    # The poller reads process_registry.completion_queue by attribute, so a
    # fresh queue keeps leaked pollers from other tests out of this one.
    q: _queue_mod.Queue = _queue_mod.Queue()
    monkeypatch.setattr(process_registry, "completion_queue", q)
    return q


@pytest.fixture
def dispatched(monkeypatch):
    """Record (and stub out) any turn a notification path would start."""
    calls = []
    monkeypatch.setattr(server, "_emit", lambda *a, **kw: None)
    monkeypatch.setattr(
        server,
        "_run_prompt_submit",
        lambda rid, sid, session, text, **kw: calls.append(text),
    )
    monkeypatch.setattr(server, "_collect_kanban_notifications", lambda _s: [])
    monkeypatch.setattr(server, "_maybe_fire_tui_loop_tick", lambda *_a: None)
    return calls


def _run_live_poller_once(monkeypatch, sid, session, claim_result):
    """Run exactly one live-loop iteration: the claim stub stops the loop."""
    stop = threading.Event()

    def _claim(evt, consumer):
        stop.set()
        if isinstance(claim_result, Exception):
            raise claim_result
        return claim_result

    monkeypatch.setattr(ad, "claim_event_delivery", _claim)
    server._notification_poller_loop(stop, sid, session)
    return stop


# ── live poller loop ───────────────────────────────────────────────────────


def test_poller_claim_none_releases_running(monkeypatch, isolated_queue, dispatched):
    process_registry._completion_consumed.discard("proc_claim_none")
    isolated_queue.put(_completion_event("proc_claim_none"))
    session = _session()

    stop = _run_live_poller_once(monkeypatch, "sid_claim_none", session, None)

    assert stop.is_set()
    assert dispatched == []
    assert session["running"] is False
    assert isolated_queue.empty()


def test_poller_claim_error_releases_running(monkeypatch, isolated_queue, dispatched):
    process_registry._completion_consumed.discard("proc_claim_err")
    isolated_queue.put(_completion_event("proc_claim_err"))
    session = _session()
    released = []
    monkeypatch.setattr(
        ad, "release_event_delivery", lambda *a: released.append(a)
    )

    stop = _run_live_poller_once(
        monkeypatch, "sid_claim_err", session, RuntimeError("claim db down")
    )

    assert stop.is_set()
    assert dispatched == []
    assert released == []
    assert session["running"] is False


def test_poller_claim_success_still_dispatches(monkeypatch, isolated_queue, dispatched):
    process_registry._completion_consumed.discard("proc_claim_ok")
    isolated_queue.put(_completion_event("proc_claim_ok"))
    session = _session()
    completed = []
    monkeypatch.setattr(
        ad, "complete_event_delivery", lambda evt, claim: completed.append(claim)
    )

    _run_live_poller_once(monkeypatch, "sid_claim_ok", session, "claim-1")

    assert len(dispatched) == 1
    assert completed == ["claim-1"]
    # The stubbed dispatch never settles the turn: running stays owned by it.
    assert session["running"] is True


# ── poller shutdown drain ──────────────────────────────────────────────────


def _run_shutdown_drain(monkeypatch, sid, session, claims):
    stop = threading.Event()
    stop.set()
    pending = list(claims)

    def _claim(evt, consumer):
        result = pending.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(ad, "claim_event_delivery", _claim)
    monkeypatch.setattr(ad, "complete_event_delivery", lambda *a: None)
    server._notification_poller_loop(stop, sid, session)


def test_shutdown_drain_claim_none_releases_running(
    monkeypatch, isolated_queue, dispatched
):
    process_registry._completion_consumed.discard("proc_drain_none")
    isolated_queue.put(_completion_event("proc_drain_none"))
    session = _session()

    _run_shutdown_drain(monkeypatch, "sid_drain_none", session, [None])

    assert dispatched == []
    assert session["running"] is False
    assert isolated_queue.empty()


def test_shutdown_drain_claim_error_releases_running(
    monkeypatch, isolated_queue, dispatched
):
    process_registry._completion_consumed.discard("proc_drain_err")
    isolated_queue.put(_completion_event("proc_drain_err"))
    session = _session()

    _run_shutdown_drain(
        monkeypatch, "sid_drain_err", session, [RuntimeError("claim db down")]
    )

    assert dispatched == []
    assert session["running"] is False


def test_shutdown_drain_claim_none_moves_on_to_next_event(
    monkeypatch, isolated_queue, dispatched
):
    for proc_id in ("proc_drain_a", "proc_drain_b"):
        process_registry._completion_consumed.discard(proc_id)
        isolated_queue.put(_completion_event(proc_id))
    session = _session()

    _run_shutdown_drain(monkeypatch, "sid_drain_next", session, [None, "claim-b"])

    # The lost claim on the first event must not make the second one look
    # like it arrived during a busy turn (re-queued instead of delivered).
    assert len(dispatched) == 1
    assert "proc_drain_b" in dispatched[0]
    assert isolated_queue.empty()


# ── post-turn drain inside _run_prompt_submit ──────────────────────────────


class _Agent:
    model = "test-model"
    provider = "test-provider"

    def __init__(self, turns):
        self._turns = turns

    def clear_interrupt(self):
        return None

    def run_conversation(self, prompt, conversation_history=None, stream_callback=None, **_kwargs):
        self._turns.append(prompt)
        return {"final_response": "", "messages": []}


class _ImmediateThread:
    def __init__(self, target=None, daemon=None, **_kwargs):
        self._target = target

    def start(self):
        if self._target is not None:
            self._target()

    def is_alive(self):
        return False


def _configure_immediate_prompt_run(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(server, "_emit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, "make_stream_renderer", lambda _cols: None)
    monkeypatch.setattr(server, "render_message", lambda _raw, _cols: None)
    monkeypatch.setattr(server, "_wire_callbacks", lambda _sid: None)
    monkeypatch.setattr(server, "_sync_agent_model_with_config", lambda *_args: None)
    monkeypatch.setattr(server, "_session_cwd", lambda _session: str(tmp_path))
    monkeypatch.setattr(server, "_register_session_cwd", lambda _session: None)
    monkeypatch.setattr(server, "_set_session_context", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(server, "_clear_session_context", lambda _tokens: None)
    monkeypatch.setattr(server, "_session_info", lambda *_args: {})
    monkeypatch.setattr(server, "_get_usage", lambda _agent: {})
    monkeypatch.setattr(
        server, "_sync_session_key_after_compress", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(server, "_drain_queued_prompt", lambda *_args: False)
    monkeypatch.setattr(server, "_voice_tts_enabled", lambda: False)
    monkeypatch.setattr(server, "_get_db", lambda: None)


def _run_turn_with_drained(monkeypatch, tmp_path, drained, claims):
    """Run one turn whose post-turn drain yields ``drained`` once."""
    _configure_immediate_prompt_run(monkeypatch, tmp_path)
    batches = [list(drained)]
    monkeypatch.setattr(
        process_registry,
        "drain_notifications",
        lambda **_kw: batches.pop(0) if batches else [],
    )
    pending = list(claims)
    released = []

    def _claim(evt, consumer):
        assert consumer == "tui-post-turn"
        result = pending.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(ad, "claim_event_delivery", _claim)
    monkeypatch.setattr(ad, "complete_event_delivery", lambda *a: None)
    monkeypatch.setattr(ad, "release_event_delivery", lambda *a: released.append(a))

    turns = []
    session = _session(agent=_Agent(turns), session_key="session-post", running=True)
    server._sessions["sid_post"] = session
    try:
        server._run_prompt_submit("rid-post", "sid_post", session, "user-turn")
    finally:
        server._sessions.pop("sid_post", None)
    return session, turns, released


def test_post_turn_claim_none_releases_running(monkeypatch, tmp_path):
    evt = _completion_event("proc_post_none")
    session, turns, _released = _run_turn_with_drained(
        monkeypatch, tmp_path, [(evt, "synth-none")], [None]
    )

    assert turns == ["user-turn"]
    assert session["running"] is False


def test_post_turn_claim_error_releases_running(monkeypatch, tmp_path):
    evt = _completion_event("proc_post_err")
    session, turns, released = _run_turn_with_drained(
        monkeypatch, tmp_path, [(evt, "synth-err")], [RuntimeError("claim db down")]
    )

    assert turns == ["user-turn"]
    assert released == []
    assert session["running"] is False


def test_post_turn_claim_error_moves_on_to_next_event(monkeypatch, tmp_path):
    first = _completion_event("proc_post_a")
    second = _completion_event("proc_post_b")
    session, turns, _released = _run_turn_with_drained(
        monkeypatch,
        tmp_path,
        [(first, "synth-a"), (second, "synth-b")],
        [RuntimeError("claim db down"), "claim-b"],
    )

    assert turns == ["user-turn", "synth-b"]
    assert session["running"] is False
