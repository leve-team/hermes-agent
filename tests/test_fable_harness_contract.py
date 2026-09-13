"""Measure the Fable harness contract at the Anthropic SDK HTTP boundary."""

import copy
import json
from types import SimpleNamespace

import anthropic
import httpx
import pytest

from agent.anthropic_adapter import create_anthropic_message
from agent.context_compressor import (
    COMPRESSED_SUMMARY_METADATA_KEY,
    ContextCompressor,
    SUMMARY_PREFIX,
    _SUMMARY_END_MARKER,
    _redact_compaction_text,
)
from agent.transports.anthropic import AnthropicTransport


MODEL = "claude-fable-5-1"
SUMMARY = "## Historical Task Snapshot\nInvestigate the deployment.\n## Key Decisions\nKeep the rollback."
TOOLS = [{
    "type": "function",
    "function": {
        "name": "read_file",
        "description": "Read a file.",
        "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
    },
}]
PRESERVATION_ITEMS = (
    "any difficulties or problems that came up, and how they were handled or resolved",
    "any possibilities, options, or approaches that were raised, tried, or set aside, and why",
    "anything that was asked for, decided, agreed, ruled out, or established as a preference, constraint, or boundary — stated exactly",
    "exactly where things stand now — what has been covered, settled, or completed so far",
    "anything still open, unresolved, promised, or expected to happen next",
    "specific details that would be hard to reconstruct — names, numbers, dates, exact wording, links or references — kept exactly",
)


@pytest.fixture
def summary_calls(monkeypatch):
    calls = []

    def summarize(**kwargs):
        calls.append(copy.deepcopy(kwargs))
        return SimpleNamespace(choices=[SimpleNamespace(
            message=SimpleNamespace(content=SUMMARY), finish_reason="stop",
        )])

    monkeypatch.setattr("agent.context_compressor.call_llm", summarize)
    return calls


@pytest.fixture
def wire():
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={
            "id": "msg_contract", "type": "message", "role": "assistant",
            "model": MODEL, "content": [{"type": "text", "text": "Recorded."}],
            "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 10, "output_tokens": 2},
        })

    with anthropic.Anthropic(
        api_key="contract-test-only", max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    ) as client:
        def send(messages, model=MODEL, base_url=None):
            kwargs = AnthropicTransport().build_kwargs(
                model=model, messages=messages, tools=copy.deepcopy(TOOLS),
                reasoning_config={"enabled": True, "effort": "high"},
                base_url=base_url,
            )
            create_anthropic_message(client, kwargs, prefer_stream=False)
            return requests[-1]

        yield send


def _compressor(model=MODEL, tail_mode="lean"):
    return ContextCompressor(
        model=model, provider="anthropic", api_mode="anthropic_messages",
        config_context_length=8000, quiet_mode=True, tail_mode=tail_mode,
    )


def _assistant(text, signature):
    thinking = {"type": "thinking", "thinking": "private reasoning", "signature": signature}
    return {
        "role": "assistant", "content": text,
        "reasoning": "private reasoning", "reasoning_content": "private reasoning",
        "reasoning_details": [copy.deepcopy(thinking)],
        "anthropic_content_blocks": [thinking, {"type": "text", "text": text}],
    }


def _history():
    messages = [{"role": "system", "content": [{
        "type": "text", "text": "Stable system instructions.",
        "cache_control": {"type": "ephemeral"},
    }]}]
    for turn in range(30):
        messages.extend([
            {"role": "user", "content": f"Investigate issue {turn}. " + "context " * 100},
            _assistant(f"Resolved issue {turn}. " + "details " * 100, f"old-signature-{turn}"),
        ])
    messages.append({"role": "user", "content": "Check the deployment; do not restart production."})
    return messages


def _blocks(payload):
    return [
        block for message in payload["messages"]
        for block in message["content"] if isinstance(block, dict)
    ]


@pytest.mark.parametrize("model", [MODEL, "anthropic/claude-fable-5.1", "claude-fable-5"])
def test_thinking_display_remains_summarized(wire, model):
    payload = wire([{"role": "user", "content": "Investigate."}], model=model)
    assert payload["thinking"]["display"] == "summarized"


@pytest.mark.parametrize("tail_mode", ["lean", "legacy"])
@pytest.mark.parametrize("previous", [None, "Previous decisions and exact constraints."])
def test_summary_request_preserves_six_items_and_voice_weighting(summary_calls, tail_mode, previous):
    compressor = _compressor(tail_mode=tail_mode)
    compressor._previous_summary = previous
    compressor._generate_summary(_history()[1:5], focus_topic="deployment")
    assert len(summary_calls) == 1
    prompt = summary_calls[0]["messages"][0]["content"]
    for item in PRESERVATION_ITEMS:
        assert item in prompt
    assert "keep what the user said, asked for, shared, or established carefully and close to their own words" in prompt
    assert "your own explanations and reasoning can be condensed much further, to what they concluded or produced" in prompt
    assert "as long as nothing in the six items above is dropped" in prompt
    assert "## Constraints & Preferences" in prompt
    assert "## Errors & Fixes" in prompt
    assert "## Critical Context" in prompt
    assert ("PREVIOUS SUMMARY:" in prompt) == bool(previous)


@pytest.mark.parametrize("replay", ["ordered", "reasoning_details", "content"])
def test_compaction_replays_no_old_thinking(summary_calls, wire, replay):
    messages = _history()
    for message in messages:
        if message["role"] != "assistant":
            continue
        message["anthropic_content_blocks"].insert(1, {
            "type": "redacted_thinking", "data": "old-signature-redacted",
        })
        if replay == "reasoning_details":
            message.pop("anthropic_content_blocks")
        elif replay == "content":
            message["content"] = message.pop("anthropic_content_blocks")
            message.pop("reasoning_details")
    original = copy.deepcopy(messages)
    before = wire(messages)
    assert any(block["type"] == "thinking" for block in _blocks(before))
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    assert summary_calls
    after = wire(compacted)
    assert not any(block["type"] in {"thinking", "redacted_thinking"} for block in _blocks(after))
    assert "old-signature" not in json.dumps(after)
    assert "private reasoning" not in json.dumps(after)
    assert messages == original


@pytest.mark.parametrize("tail_mode", ["lean", "legacy"])
def test_compaction_keeps_system_and_tools_identical(summary_calls, wire, tail_mode):
    messages = _history()
    before = wire(messages)
    compacted = _compressor(tail_mode=tail_mode).compress(messages, current_tokens=100_000, force=True)
    after = wire(compacted)
    for field in ("system", "tools"):
        assert json.dumps(after[field], ensure_ascii=False) == json.dumps(before[field], ensure_ascii=False)


@pytest.mark.parametrize("tail_mode", ["lean", "legacy"])
def test_compaction_has_one_user_summary_then_live_request(summary_calls, wire, tail_mode):
    messages = _history()
    compacted = _compressor(tail_mode=tail_mode).compress(messages, current_tokens=100_000, force=True)
    carriers = [message for message in compacted if message.get(COMPRESSED_SUMMARY_METADATA_KEY)]
    assert len(carriers) == 1
    assert carriers[0]["role"] == "user"
    payload = wire(compacted)
    assert [message["role"] for message in payload["messages"]] == ["user"]
    text = json.dumps(payload["messages"], ensure_ascii=False)
    assert text.count(SUMMARY_PREFIX) == 1
    assert text.index(_SUMMARY_END_MARKER) < text.rindex(messages[-1]["content"])
    assert "Investigate issue 0." in text
    assert "Resolved issue 29." in text


def test_new_thinking_survives_and_requests_append_after_compaction(summary_calls, wire):
    compacted = _compressor().compress(_history(), current_tokens=100_000, force=True)
    compacted.extend([
        _assistant("First fresh answer.", "fresh-signature-one"),
        {"role": "user", "content": "Continue."},
    ])
    first = wire(compacted)
    compacted.extend([
        _assistant("Second fresh answer.", "fresh-signature-two"),
        {"role": "user", "content": "One more check."},
    ])
    second = wire(compacted)
    assert second["messages"][:len(first["messages"])] == first["messages"]
    assert {block.get("signature") for block in _blocks(second) if block["type"] == "thinking"} == {
        "fresh-signature-one", "fresh-signature-two",
    }


@pytest.mark.parametrize("model", [
    "claude-sonnet-4-6", "gpt-5.4", "astra", "claude-fable", "vendor/not-claude-fable-5-1",
])
def test_non_fable_summary_prompt_is_unchanged(summary_calls, model):
    _compressor(model=model)._generate_summary(_history()[1:5])
    prompt = summary_calls[0]["messages"][0]["content"]
    assert not any(item in prompt for item in PRESERVATION_ITEMS)


def test_compaction_preserves_completed_tool_results_as_reference(summary_calls, wire):
    messages = _history()
    tool_turn = _assistant("Inspecting the deployment.", "old-signature-tool")
    tool_turn["tool_calls"] = [{
        "id": "toolu_deployment", "type": "function",
        "function": {"name": "read_file", "arguments": '{"path":"deploy.yaml"}'},
    }]
    tool_turn["anthropic_content_blocks"].append({
        "type": "tool_use", "id": "toolu_deployment",
        "name": "read_file", "input": {"path": "deploy.yaml"},
    })
    messages.extend([
        tool_turn,
        {"role": "tool", "tool_call_id": "toolu_deployment", "content": "replicas: 3; revision: 2026-09-12"},
    ])
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    payload = wire(compacted)
    assert [message["role"] for message in payload["messages"]] == ["user"]
    assert not any(block["type"] in {"thinking", "tool_use", "tool_result"} for block in _blocks(payload))
    text = json.dumps(payload["messages"], ensure_ascii=False)
    assert "replicas: 3; revision: 2026-09-12" in text
    assert "deploy.yaml" in text
    assert text.index(_SUMMARY_END_MARKER) < text.rindex(messages[-3]["content"])


@pytest.mark.parametrize("caption", ["Inspect this screenshot.", ""])
def test_compaction_preserves_live_user_media(summary_calls, wire, caption):
    messages = _history()
    image = {"type": "image_url", "image_url": {"url": "https://example.test/deployment.png"}}
    messages[-1]["content"] = [{"type": "text", "text": caption}, image]
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    payload = wire(compacted)
    blocks = _blocks(payload)
    boundary = next(index for index, block in enumerate(blocks) if _SUMMARY_END_MARKER in block.get("text", ""))
    image_index = next(index for index, block in enumerate(blocks) if block["type"] == "image")
    assert image_index > boundary
    assert blocks[image_index]["source"]["url"] == image["image_url"]["url"]


def test_idle_compaction_does_not_reactivate_finished_user_request(summary_calls, wire):
    messages = _history()[:-1]
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    blocks = _blocks(wire(compacted))
    boundary = next(index for index, block in enumerate(blocks) if _SUMMARY_END_MARKER in block.get("text", ""))
    assert boundary == len(blocks) - 1


def test_second_compaction_carries_prior_protected_context_into_summary_request(summary_calls, wire):
    compressor = _compressor()
    compacted = compressor.compress(_history(), current_tokens=100_000, force=True)
    compacted.extend(_history()[1:])
    second = compressor.compress(compacted, current_tokens=100_000, force=True)
    assert len(summary_calls) == 2
    prompt = summary_calls[-1]["messages"][0]["content"]
    assert "## Preserved Turns (reference only)" in prompt
    assert "Investigate issue 0." in prompt
    payload = wire(second)
    assert [message["role"] for message in payload["messages"]] == ["user"]
    assert "old-signature" not in json.dumps(payload)


def test_short_conversation_without_compaction_keeps_thinking(summary_calls, wire):
    messages = _history()[-3:]
    before = wire(messages)
    compacted = _compressor().compress(messages, force=True)
    assert not summary_calls
    assert wire(compacted) == before


def test_non_fable_compaction_keeps_existing_history_shape(summary_calls, wire):
    model = "claude-sonnet-4-6"
    compacted = _compressor(model=model).compress(_history(), current_tokens=100_000, force=True)
    assert any(message["role"] == "assistant" for message in compacted)
    assert any(block["type"] == "thinking" for block in _blocks(wire(compacted, model=model)))


def test_native_tool_blocks_become_reference_text_at_compaction(summary_calls, wire):
    messages = _history()
    messages[-2]["content"] = [
        {"type": "thinking", "thinking": "private reasoning", "signature": "old-signature-native"},
        {"type": "tool_use", "id": "toolu_native", "name": "read_file", "input": {"path": "native.yaml"}},
        {"type": "text", "text": "Native deployment inspected."},
    ]
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    payload = wire(compacted)
    assert not any(block["type"] in {"thinking", "tool_use", "tool_result"} for block in _blocks(payload))
    assert "native.yaml" in json.dumps(payload)
    assert "old-signature-native" not in json.dumps(payload)


def test_protected_references_are_redacted_and_do_not_drop_matching_assistant_text(summary_calls, wire):
    messages = _history()
    messages[1]["content"] = messages[-1]["content"]
    messages[-2]["content"] = messages[-1]["content"]
    messages[-4]["content"] = "password=super-private-contract-password"
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    blocks = _blocks(wire(compacted))
    boundary = next(index for index, block in enumerate(blocks) if _SUMMARY_END_MARKER in block.get("text", ""))
    references = "\n".join(block.get("text", "") for block in blocks[:boundary])
    assert references.count(messages[-1]["content"]) >= 2
    assert "super-private-contract-password" not in references
    assert _redact_compaction_text(messages[-4]["content"]) in references


def test_image_scaffolding_does_not_become_a_live_user_request(summary_calls, wire):
    messages = _history()[:-1]
    messages.append({
        "role": "user", "_todo_snapshot_synthetic": True,
        "content": [{"type": "image_url", "image_url": {"url": "https://example.test/status.png"}}],
    })
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    blocks = _blocks(wire(compacted))
    boundary = next(index for index, block in enumerate(blocks) if _SUMMARY_END_MARKER in block.get("text", ""))
    assert boundary == len(blocks) - 1


@pytest.mark.parametrize("model", [
    MODEL, "ANTHROPIC/Claude-Fable-5.1", "anthropic.claude-fable-5-1",
    "us.anthropic.claude-fable-5-1-v1:0",
])
def test_normal_turns_preserve_signed_prefix_for_fable_aliases(wire, model):
    messages = [
        {"role": "user", "content": "Investigate."},
        _assistant("First answer.", "first-signature"),
        {"role": "user", "content": "Continue."},
    ]
    messages[1]["anthropic_content_blocks"].insert(1, {
        "type": "redacted_thinking", "data": "redacted-signature",
    })
    first = wire(messages, model=model)
    messages.extend([
        _assistant("Second answer.", "second-signature"),
        {"role": "user", "content": "Continue again."},
    ])
    original = copy.deepcopy(messages)
    second = wire(messages, model=model)
    assert second["messages"][:len(first["messages"])] == first["messages"]
    assert messages == original
    assert any(block.get("data") == "redacted-signature" for block in _blocks(second))


def test_third_party_endpoint_still_strips_fable_signatures(wire):
    payload = wire([
        {"role": "user", "content": "Investigate."},
        _assistant("Done.", "third-party-signature"),
        {"role": "user", "content": "Continue."},
    ], base_url="https://api.minimax.io/anthropic")
    assert not any(block["type"] in {"thinking", "redacted_thinking"} for block in _blocks(payload))


def test_native_pending_tool_use_keeps_live_request(summary_calls, wire):
    messages = _history()
    messages.append({"role": "assistant", "content": [
        {"type": "text", "text": "Inspecting deployment."},
        {"type": "tool_use", "id": "toolu_pending", "name": "read_file",
         "input": {"path": "deployment.yaml"}},
    ]})
    compacted = _compressor().compress(messages, current_tokens=100_000, force=True)
    blocks = _blocks(wire(compacted))
    boundary = next(index for index, block in enumerate(blocks) if _SUMMARY_END_MARKER in block.get("text", ""))
    assert any(messages[-2]["content"] == block.get("text") for block in blocks[boundary + 1:])
    assert not any(block["type"] in {"tool_use", "tool_result"} for block in blocks)


@pytest.mark.parametrize("pending", [True, False])
def test_agent_compaction_boundary_does_not_restore_an_extra_user_anchor(summary_calls, wire, pending):
    from agent.conversation_compression import compress_context

    messages = _history() if pending else _history()[:-1]
    agent = SimpleNamespace(
        context_compressor=_compressor(), model=MODEL, provider="anthropic",
        api_mode="anthropic_messages", session_id="fable-contract-session", platform="cli",
        tools=copy.deepcopy(TOOLS), _compression_feasibility_checked=True,
        compression_in_place=False, _memory_manager=None, _session_db=None,
        _todo_store=SimpleNamespace(format_for_injection=lambda: ""),
        _cached_system_prompt="Stable system instructions.",
        _emit_status=lambda message: None, _emit_warning=lambda message: None,
        _invalidate_system_prompt=lambda: None,
        _build_system_prompt=lambda message: message,
        commit_memory_session=lambda messages: None,
    )
    compacted, system = compress_context(
        agent, messages, "Stable system instructions.", approx_tokens=100_000, force=True,
    )
    assert system == "Stable system instructions."
    assert [message["role"] for message in compacted] == ["system", "user"]
    blocks = _blocks(wire(compacted))
    boundary = next(index for index, block in enumerate(blocks) if _SUMMARY_END_MARKER in block.get("text", ""))
    after_boundary = "\n".join(block.get("text", "") for block in blocks[boundary + 1:])
    if pending:
        assert after_boundary.count(messages[-1]["content"]) == 1
    else:
        assert not after_boundary.strip()


def test_summary_failure_abort_does_not_rebase_prefix(monkeypatch, wire):
    compressor = _compressor()
    compressor.abort_on_summary_failure = True
    monkeypatch.setattr(compressor, "_generate_summary", lambda *args, **kwargs: None)
    messages = _history()
    before = wire(messages)
    compacted = compressor.compress(messages, current_tokens=100_000, force=True)
    assert compressor.compression_count == 0
    assert wire(compacted) == before
