"""``prompt.submit`` hands the turn the text the user typed: the model input and the stored row.

Web and desktop clients reach the core only through this RPC, so whatever its input sanitising
changes is what the model reads and what the transcript keeps for good (#143444).
"""

import sys
import threading
import types

import pytest

from tests.hermes_cli.test_input_sanitize import ORDINARY_RANGE_TEXTS
from tui_gateway import server


class _ImmediateThread:
    def __init__(self, target=None, daemon=None, **_kwargs):
        self._target = target

    def start(self):
        self._target()


class _RecordingAgent:
    model = "test-model"
    provider = "test-provider"
    session_id = "session-key"
    context_compressor = None

    def __init__(self):
        self.turns = []

    def clear_interrupt(self):
        return None

    def run_conversation(self, prompt, conversation_history=None, stream_callback=None, **kwargs):
        self.turns.append({"model_input": prompt, "stored": kwargs.get("persist_user_message")})
        return {
            "final_response": "ok",
            "messages": [{"role": "user", "content": prompt}, {"role": "assistant", "content": "ok"}],
        }


@pytest.mark.parametrize("text", ORDINARY_RANGE_TEXTS)
def test_submitted_number_ranges_reach_the_model_and_the_transcript_unchanged(monkeypatch, text):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(server, "_ensure_session_db_row", lambda _session: None)
    monkeypatch.setattr(server, "_persist_branch_seed", lambda _session: None)
    monkeypatch.setattr(server, "_session_info", lambda _agent, _session=None: {})
    monkeypatch.setattr(server, "make_stream_renderer", lambda _cols: None)
    monkeypatch.setattr(server, "render_message", lambda _raw, _cols: None)
    monkeypatch.setattr(server, "_emit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, "_load_cfg", lambda: {"dashboard": {"turn_isolation": False}})
    monkeypatch.setitem(
        sys.modules, "agent.title_generator",
        types.SimpleNamespace(maybe_auto_title=lambda *args, **kwargs: None))
    agent = _RecordingAgent()
    server._sessions["sid"] = {
        "agent": agent, "session_key": "session-key", "history": [], "history_lock": threading.Lock(),
        "history_version": 0, "running": False, "attached_images": [], "image_counter": 0, "cols": 80,
        "slash_worker": None, "show_reasoning": False, "tool_progress_mode": "all",
        "model_override": {"model": "test-model", "provider": "test-provider"}}
    try:
        response = server.handle_request(
            {"id": "r1", "method": "prompt.submit", "params": {"session_id": "sid", "text": text}})
    finally:
        server._sessions.pop("sid", None)

    assert response["result"]["status"] == "streaming"
    assert agent.turns == [{"model_input": text, "stored": text}]
