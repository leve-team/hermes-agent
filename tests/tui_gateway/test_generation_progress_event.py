"""``generation.progress`` gateway contract: the agent callback becomes a session event carrying the
payload unchanged, silenced by ``tool_progress: off``, advertised in gateway.capabilities, and kept out
of the replay ring so a long call cannot evict the turn's control events."""

import pytest

from tui_gateway import event_replay, server

_PAYLOAD = {"api_call": 2, "attempt": 1, "phase": "tool_call", "name": "write_file", "tool_index": 0,
            "output_chars": 1400, "phase_chars": 1200, "elapsed_ms": 41000}


@pytest.mark.parametrize("mode, emitted", [("all", True), ("new", True), ("verbose", True), ("off", False)])
def test_generation_progress_event_is_gated_like_tool_generating(monkeypatch, mode, emitted):
    sid = "generation-progress"
    events = []
    monkeypatch.setattr(server, "_emit", lambda event, session_id, payload=None: events.append((event, session_id, payload)))
    monkeypatch.setitem(server._sessions, sid, {"tool_progress_mode": mode})

    server._agent_cbs(sid)["generation_progress_callback"](dict(_PAYLOAD))
    server._agent_cbs(sid)["tool_gen_callback"]("write_file")

    progress = [e for e in events if e[0] == "generation.progress"]
    assert progress == ([("generation.progress", sid, _PAYLOAD)] if emitted else [])
    assert bool(progress) == any(e[0] == "tool.generating" for e in events)


def test_gateway_capabilities_advertise_generation_progress():
    response = server.handle_request({"id": "1", "method": "gateway.capabilities", "params": {}})
    assert response["result"]["generation_progress"] is True


def test_progress_events_are_live_only_in_the_replay_ring(monkeypatch):
    sid = "generation-progress-ring"
    monkeypatch.setattr(event_replay, "_REPLAY_BUFFER_MAX", 4)
    event_replay.reset_replay_state()
    frames = [server._event_frame("tool.start", sid, {"name": "write_file"})]
    frames += [server._event_frame("generation.progress", sid, dict(_PAYLOAD, output_chars=n)) for n in range(10)]
    frames.append(server._event_frame("message.delta", sid, {"text": "done"}))
    for frame in frames:
        event_replay._stamp_event(frame)

    assert [f["params"]["seq"] for f in frames] == list(range(1, 13))  # seq stays monotonic
    assert [e["type"] for e in event_replay.events_since(sid, 0)] == ["tool.start", "message.delta"]
    assert not event_replay.is_truncated(sid, 0)
    event_replay.reset_replay_state()
