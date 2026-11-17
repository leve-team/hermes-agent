"""``generation.progress`` reaches the agent's callback DURING one long model call, on every
streaming wire, with cumulative counts and ``tool_index`` as the tool call's ordinal; external-process
providers report absolute counts on choiceless ``x_hermes_progress`` chunks that also keep the
stale-stream timer alive; nothing is emitted once a /stop is pending."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent import chat_completion_helpers as cch

_PAUSE_S = 5.0  # a stream holds here until the poll thread has reported the counts so far


class _Recorder:
    """Progress callback that releases the paused stream once ``wanted`` counts were reported."""

    def __init__(self, wanted: dict):
        self.wanted, self.payloads, self.reached = wanted, [], threading.Event()

    def __call__(self, payload: dict) -> None:
        self.payloads.append(payload)
        if all(payload.get(k) == v for k, v in self.wanted.items()):
            self.reached.set()

    def pause(self) -> None:
        self.reached.wait(_PAUSE_S)


def _chat_chunk(*, content=None, reasoning=None, tool_calls=None, finish_reason=None):
    delta = SimpleNamespace(content=content, tool_calls=tool_calls, reasoning_content=reasoning, reasoning=None)
    return SimpleNamespace(choices=[SimpleNamespace(index=0, delta=delta, finish_reason=finish_reason)],
                           model="test/model", usage=None)


def _chat_tool_delta(*, name=None, arguments=None, index=0, tc_id=None):
    return SimpleNamespace(index=index, id=tc_id, function=SimpleNamespace(name=name, arguments=arguments))


def _chat_agent(**callbacks):
    from run_agent import AIAgent

    agent = AIAgent(api_key="test-key", base_url="https://openrouter.ai/api/v1", model="test/model",
                    quiet_mode=True, skip_context_files=True, skip_memory=True, **callbacks)
    agent.api_mode = "chat_completions"
    agent._interrupt_requested = False
    return agent


def _call_chat_stream(agent, chunk_source):
    client = MagicMock()
    client.chat.completions.create.side_effect = lambda **_kw: chunk_source()
    with patch("run_agent.AIAgent._create_request_openai_client", return_value=client), \
            patch("run_agent.AIAgent._close_request_openai_client"):
        return agent._interruptible_streaming_api_call({}), client


def _drive_chat(recorder, generating):
    agent = _chat_agent(generation_progress_callback=recorder, tool_gen_callback=generating.append)
    agent._api_call_count = 2

    def chunks():
        yield _chat_chunk(reasoning="abcd")
        yield _chat_chunk(tool_calls=[_chat_tool_delta(name="write_file", arguments='{"pa', tc_id="c1")])
        yield _chat_chunk(tool_calls=[_chat_tool_delta(arguments='th":1}')])
        recorder.pause()
        yield _chat_chunk(tool_calls=[_chat_tool_delta(arguments="")], finish_reason="tool_calls")

    _call_chat_stream(agent, chunks)
    return {"api_call": 2, "attempt": 1, "phase": "tool_call", "name": "write_file", "tool_index": 0,
            "phase_chars": 10, "output_chars": 14}


def _drive_anthropic(recorder, generating):
    from run_agent import AIAgent

    agent = AIAgent(api_key="test-key", base_url="https://example.com/v1", model="claude-opus-4-7",
                    quiet_mode=True, skip_context_files=True, skip_memory=True,
                    generation_progress_callback=recorder, tool_gen_callback=generating.append)
    agent.api_mode = "anthropic_messages"
    agent._api_call_count = 1
    client = MagicMock()
    agent._anthropic_client, agent._anthropic_api_key = client, "test-anthropic-key"
    agent._create_request_anthropic_client = lambda *a, **k: client

    def events():
        yield SimpleNamespace(type="content_block_start", index=0, content_block=SimpleNamespace(type="thinking"))
        yield SimpleNamespace(type="content_block_delta", index=0,
                              delta=SimpleNamespace(type="thinking_delta", thinking="hmm"))
        yield SimpleNamespace(type="content_block_start", index=1,
                              content_block=SimpleNamespace(type="tool_use", name="terminal"))
        yield SimpleNamespace(type="content_block_delta", index=1,
                              delta=SimpleNamespace(type="input_json_delta", partial_json='{"command":'))
        recorder.pause()
        yield SimpleNamespace(type="content_block_delta", index=1,
                              delta=SimpleNamespace(type="input_json_delta", partial_json=' "ls"}'))

    final = MagicMock()
    final.content = [SimpleNamespace(type="tool_use", id="toolu_1", name="terminal", input={"command": "ls"})]
    final.stop_reason, final.usage = "tool_use", SimpleNamespace(input_tokens=1, output_tokens=1)
    stream = MagicMock()
    stream.__iter__ = MagicMock(return_value=events())
    stream.get_final_message = MagicMock(return_value=final)
    manager = MagicMock()
    manager.__enter__ = MagicMock(return_value=stream)
    manager.__exit__ = MagicMock(return_value=False)
    client.messages.stream = MagicMock(return_value=manager)
    agent._interruptible_streaming_api_call({"model": "claude-opus-4-7"})
    return {"api_call": 1, "attempt": 1, "phase": "tool_call", "name": "terminal", "tool_index": 0,
            "phase_chars": 11, "output_chars": 14}


class _BedrockAgent:
    api_mode, provider, model = "bedrock_converse", "bedrock", "anthropic.claude-3-sonnet-20240229-v1:0"
    _interrupt_requested = _disable_streaming = False
    reasoning_callback = stream_delta_callback = None
    _consecutive_stale_streams = 0
    _api_call_count = 4

    def __init__(self, recorder, generating):
        self.generation_progress_callback, self._generating = recorder, generating

    def _has_stream_consumers(self):
        return False

    def _fire_tool_gen_started(self, name):
        self._generating.append(name)

    def _fire_stream_delta(self, text):
        pass

    def _fire_reasoning_delta(self, text):
        pass

    def _buffer_status(self, *a, **k):
        pass

    def _safe_print(self, *a, **k):
        pass


def _drive_bedrock(recorder, generating):
    agent = _BedrockAgent(recorder, generating)

    def events():
        yield {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": "ok"}}}
        yield {"contentBlockStop": {"contentBlockIndex": 0}}
        yield {"contentBlockStart": {"contentBlockIndex": 1, "start": {"toolUse": {"toolUseId": "t1", "name": "patch"}}}}
        yield {"contentBlockDelta": {"contentBlockIndex": 1, "delta": {"toolUse": {"input": '{"diff": "'}}}}
        recorder.pause()
        yield {"contentBlockDelta": {"contentBlockIndex": 1, "delta": {"toolUse": {"input": '"}'}}}}
        yield {"contentBlockStop": {"contentBlockIndex": 1}}
        yield {"messageStop": {"stopReason": "tool_use"}}

    client = SimpleNamespace(converse_stream=lambda **_kw: {"stream": events()})
    with patch("agent.bedrock_adapter._get_bedrock_runtime_client", return_value=client):
        cch.interruptible_streaming_api_call(agent, {"__bedrock_region__": "us-east-1", "modelId": agent.model})
    return {"api_call": 4, "attempt": 1, "phase": "tool_call", "name": "patch", "tool_index": 0,
            "phase_chars": 10, "output_chars": 12}


def _drive_codex(recorder, generating, tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text("{}\n", encoding="utf-8")
    from run_agent import AIAgent

    agent = AIAgent(model="gpt-5.5", provider="openai-codex", api_key="sk-dummy",
                    base_url="https://chatgpt.com/backend-api/codex", quiet_mode=True, skip_context_files=True,
                    skip_memory=True, platform="cli",
                    generation_progress_callback=recorder, tool_gen_callback=generating.append)
    agent.api_mode = "codex_responses"
    agent._api_call_count = 3
    item = {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "read_file", "arguments": ""}

    def events():
        yield SimpleNamespace(type="response.created")
        yield SimpleNamespace(type="response.reasoning_summary_text.delta", delta="plan", summary_index=0)
        yield SimpleNamespace(type="response.output_item.added", output_index=1, item=item)
        yield SimpleNamespace(type="response.function_call_arguments.delta", item_id="fc_1", output_index=1,
                              delta='{"path":')
        recorder.pause()
        done = dict(item, arguments='{"path": "a.py"}')
        yield SimpleNamespace(type="response.output_item.done", output_index=1, item=done)
        yield SimpleNamespace(type="response.completed",
                              response=SimpleNamespace(status="completed", id="resp-1", usage=None))

    client = SimpleNamespace(responses=SimpleNamespace(create=lambda **_kw: events()))
    monkeypatch.setattr(agent, "_create_request_openai_client", lambda **_kw: client)
    monkeypatch.setattr(agent, "_close_request_openai_client", lambda *a, **k: None)
    cch.interruptible_api_call(agent, {"model": "gpt-5.5", "input": "hi"})
    return {"api_call": 3, "attempt": 1, "phase": "tool_call", "name": "read_file", "tool_index": 0,
            "phase_chars": 8, "output_chars": 12}


# wire -> (driver, counts the stream pauses at, the tool whose generation it announces)
_WIRES = {
    "chat_completions": (_drive_chat, {"phase_chars": 10, "output_chars": 14}, "write_file"),
    "anthropic_messages": (_drive_anthropic, {"phase_chars": 11, "output_chars": 14}, "terminal"),
    "bedrock_converse": (_drive_bedrock, {"phase_chars": 10, "output_chars": 12}, "patch"),
    "codex_responses": (_drive_codex, {"phase_chars": 8, "output_chars": 12}, "read_file"),
}


@pytest.mark.parametrize("wire", sorted(_WIRES))
def test_generation_progress_is_reported_mid_call_on_every_wire(wire, tmp_path, monkeypatch):
    drive, at_pause, tool_name = _WIRES[wire]
    recorder, generating = _Recorder(at_pause), []
    args = (recorder, generating) + ((tmp_path, monkeypatch) if wire == "codex_responses" else ())

    expected = drive(*args)

    assert recorder.reached.is_set(), recorder.payloads
    reported = dict(next(p for p in recorder.payloads if p["output_chars"] == expected["output_chars"]))
    elapsed_ms = reported.pop("elapsed_ms")
    assert isinstance(elapsed_ms, int) and elapsed_ms >= 0
    # tool_index is the ordinal (first tool call of the attempt = 0) whatever the wire's block index.
    assert reported == expected
    # Codex now announces tool-call generation at function_call start like the other wires.
    assert generating == [tool_name]


def _progress_chunk(progress):
    # The provider plugin's shape: no id / model / usage, only the progress object.
    return SimpleNamespace(choices=[], x_hermes_progress=progress)


def test_external_process_progress_chunks_feed_progress_and_keep_the_stream_alive(monkeypatch):
    """A buffering external-process provider yields only choiceless progress chunks until its
    process exits; they reach generation.progress and count as stream activity, so a call longer
    than the stale timeout is not killed and re-run. Its final content chunks are not added on top
    of the reported counts, malformed progress is ignored, and time-to-first-byte still measures the
    first content chunk."""
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "2")
    payloads, content_sent_at = [], []
    agent = _chat_agent(generation_progress_callback=payloads.append)
    arguments = '{"path": "a.txt", "content": "' + "x" * 3000 + '"}'

    def chunks():
        for step in range(1, 9):  # ~3.6 s of provider-side work, past the 2 s stale timeout
            time.sleep(0.45)
            yield _progress_chunk({"phase": "tool_call", "name": "write_file", "tool_index": 0,
                                   "phase_chars": 100 * step, "output_chars": 100 * step + 50})
            yield _progress_chunk({"phase": "bogus", "phase_chars": 1, "output_chars": 1})
            yield _progress_chunk("garbage")
            yield _progress_chunk({"phase": "text", "phase_chars": -1, "output_chars": True})
            yield _progress_chunk({"phase": "text", "phase_chars": 1, "output_chars": 2**53})
        content_sent_at.append(time.time())
        yield _chat_chunk(content="Writing it.", tool_calls=[
            _chat_tool_delta(name="write_file", arguments=arguments, tc_id="c1")])
        time.sleep(0.8)  # poll ticks after the content: the reported counts stay authoritative
        yield _chat_chunk(finish_reason="tool_calls")

    response, client = _call_chat_stream(agent, chunks)

    assert response.choices[0].message.tool_calls[0].function.arguments == arguments
    assert client.chat.completions.create.call_count == 1  # never stale-killed and retried
    assert agent._last_api_first_chunk_at >= content_sent_at[0]
    assert payloads and {(p["phase"], p["name"], p["tool_index"]) for p in payloads} == {("tool_call", "write_file", 0)}
    assert all(p["output_chars"] == p["phase_chars"] + 50 and p["output_chars"] <= 850 for p in payloads)


def test_no_progress_is_emitted_once_a_stop_is_pending():
    payloads, first_emit = [], threading.Event()

    def report(payload):
        payloads.append(payload)
        first_emit.set()

    agent = _chat_agent(generation_progress_callback=report)

    def chunks():
        yield _chat_chunk(content="a")
        first_emit.wait(_PAUSE_S)
        time.sleep(1.2)  # past the 1 s throttle
        agent._interrupt_requested = True  # /stop is pending ...
        yield _chat_chunk(content="bcd")  # ... when the counts move
        yield _chat_chunk(content="e", finish_reason="stop")

    with pytest.raises(InterruptedError):
        _call_chat_stream(agent, chunks)
    assert first_emit.is_set()
    assert [p["output_chars"] for p in payloads] == [1]  # the moved counts never went out after /stop
