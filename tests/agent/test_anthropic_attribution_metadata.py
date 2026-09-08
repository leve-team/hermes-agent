"""Session-stable attribution on the real Anthropic request-building path."""

from copy import deepcopy
import json
import logging
from types import SimpleNamespace

import anthropic
import httpx
import pytest

from agent import chat_completion_helpers as helpers
from agent.transports.anthropic import AnthropicTransport
from agent.transports.bedrock import BedrockTransport
from agent.transports.chat_completions import ChatCompletionsTransport


SESSION_ID = "20260907_150425_aa9d1f"
USER_ID = "eren.song@withleve.com"
TASK_ID = "t_579498ac"
MESSAGES = [
    {"role": "system", "content": "Do not change this prompt."},
    {"role": "user", "content": "Hello"},
]
TOOLS = [{
    "type": "function",
    "function": {
        "name": "read_file",
        "description": "Read a file",
        "parameters": {"type": "object", "properties": {}},
    },
}]


@pytest.fixture
def agent(monkeypatch):
    monkeypatch.setenv("HERMES_PROFILE", "dave")
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    transport = AnthropicTransport()
    return SimpleNamespace(
        api_mode="anthropic_messages",
        provider="anthropic",
        model="claude-sonnet-4-5",
        session_id=SESSION_ID,
        _user_id=None,
        tools=deepcopy(TOOLS),
        max_tokens=1024,
        reasoning_config=None,
        request_overrides={},
        _is_anthropic_oauth=False,
        _get_transport=lambda: transport,
        _prepare_anthropic_messages_for_api=lambda messages: messages,
        _anthropic_preserve_dots=lambda: False,
    )


def _build(agent):
    return helpers.build_api_kwargs(agent, deepcopy(MESSAGES))


def _parse_attribution(value):
    segments = value.split(":")[:5]
    assert segments[0] == "levos"
    assert len(segments) == 5
    return dict(zip(
        ("profile", "session_id", "user_id", "task_id"),
        (axis or None for axis in segments[1:]),
    ))


def test_all_four_axes(agent, monkeypatch):
    agent._user_id = USER_ID
    monkeypatch.setenv("HERMES_KANBAN_TASK", TASK_ID)

    assert _build(agent)["metadata"]["user_id"] == (
        f"levos:dave:{SESSION_ID}:{USER_ID}:{TASK_ID}"
    )


def test_missing_user_and_task_keep_five_segments(agent):
    value = _build(agent)["metadata"]["user_id"]

    assert value == f"levos:dave:{SESSION_ID}::"
    assert len(value.split(":")) == 5


@pytest.mark.parametrize("profile", ["dave", ""])
def test_missing_session_keeps_empty_segments(agent, monkeypatch, profile):
    monkeypatch.setenv("HERMES_PROFILE", profile)
    del agent.session_id
    del agent._user_id

    assert _build(agent)["metadata"]["user_id"] == f"levos:{profile}:::"


def test_consecutive_requests_are_sticky(agent, monkeypatch):
    agent._user_id = USER_ID
    monkeypatch.setenv("HERMES_KANBAN_TASK", TASK_ID)

    first = _build(agent)["metadata"]["user_id"]
    second = _build(agent)["metadata"]["user_id"]

    assert first == second


def test_sticky_value_survives_mutable_turn_context(agent, monkeypatch):
    agent._user_id = USER_ID
    monkeypatch.setenv("HERMES_KANBAN_TASK", TASK_ID)
    first = _build(agent)["metadata"]["user_id"]
    agent._user_id = "another-user"
    agent._current_task_id = "a-new-turn-uuid"
    monkeypatch.setenv("HERMES_PROFILE", "another-profile")
    monkeypatch.setenv("HERMES_KANBAN_TASK", "another-task")

    assert _build(agent)["metadata"]["user_id"] == first


def test_new_session_does_not_reuse_previous_attribution(agent, monkeypatch):
    first = _build(agent)["metadata"]["user_id"]
    agent.session_id = "new-session"
    agent._user_id = USER_ID
    monkeypatch.setenv("HERMES_KANBAN_TASK", TASK_ID)

    value = _build(agent)["metadata"]["user_id"]

    assert value != first
    assert value == f"levos:dave:new-session:{USER_ID}:{TASK_ID}"


def test_separate_agents_do_not_share_cached_attribution(agent):
    other_agent = SimpleNamespace(**vars(agent))
    first = _build(agent)["metadata"]["user_id"]
    other_agent.session_id = "another-session"
    other_agent._user_id = "another-user"

    assert _build(other_agent)["metadata"]["user_id"] == (
        "levos:dave:another-session:another-user:"
    )
    assert _build(agent)["metadata"]["user_id"] == first


@pytest.mark.parametrize("provider", ["anthropic", "nous"])
def test_axis_resolution_failure_does_not_block_build(agent, caplog, provider):
    class UnavailableUser(SimpleNamespace):
        @property
        def _user_id(self):
            raise RuntimeError("identity unavailable")

    agent.provider = provider
    failing_agent = UnavailableUser(**vars(agent))
    with caplog.at_level(logging.DEBUG, logger=helpers.__name__):
        kwargs = _build(failing_agent)

    assert kwargs["model"] == agent.model
    assert kwargs["messages"]
    assert "metadata" not in kwargs
    assert "attribution metadata merge failed: identity unavailable" in caplog.text
    if provider == "nous":
        assert kwargs["extra_body"]["session_id"] == SESSION_ID


def test_existing_request_fields_are_preserved(agent):
    original_messages = deepcopy(MESSAGES)
    original_tools = deepcopy(agent.tools)
    expected = agent._get_transport().build_kwargs(
        model=agent.model,
        messages=original_messages,
        tools=agent.tools,
        max_tokens=agent.max_tokens,
        reasoning_config=agent.reasoning_config,
        is_oauth=False,
        preserve_dots=False,
    )

    actual = _build(agent)

    assert {"model", "messages", "max_tokens", "system", "tools"} <= actual.keys()
    assert {name: value for name, value in actual.items() if name != "metadata"} == expected
    assert MESSAGES == original_messages
    assert agent.tools == original_tools


def test_existing_metadata_fields_are_preserved(agent):
    kwargs = {"model": agent.model, "metadata": {"other": "kept", "user_id": "old"}}

    result = helpers._merge_attribution_metadata(agent, kwargs)

    assert result is kwargs
    assert kwargs["metadata"] == {
        "other": "kept", "user_id": f"levos:dave:{SESSION_ID}::",
    }


@pytest.mark.parametrize("metadata", [None, "invalid"])
def test_malformed_metadata_does_not_block_build(agent, metadata, caplog):
    kwargs = {"model": agent.model, "metadata": metadata}
    agent._get_transport = lambda: SimpleNamespace(build_kwargs=lambda **params: kwargs)

    with caplog.at_level(logging.DEBUG, logger=helpers.__name__):
        assert _build(agent) is kwargs

    assert kwargs == {"model": agent.model, "metadata": metadata}
    assert "attribution metadata merge failed" in caplog.text


@pytest.mark.parametrize("provider", ["nous", "nous-portal", "nousresearch"])
def test_nous_extra_body_still_merges(agent, provider):
    agent.provider = provider
    expected = helpers._merge_nous_portal_messages_extra_body(agent, {})["extra_body"]

    kwargs = _build(agent)

    assert kwargs["extra_body"] == expected
    assert kwargs["extra_body"]["session_id"] == SESSION_ID
    assert "product=hermes-agent" in kwargs["extra_body"]["tags"]
    assert kwargs["metadata"]["user_id"] == f"levos:dave:{SESSION_ID}::"


@pytest.mark.parametrize("mode", ["chat_completions", "bedrock_converse"])
def test_non_anthropic_modes_do_not_receive_attribution(agent, mode):
    agent.api_mode = mode
    agent.provider = "custom"
    agent.base_url = "https://example.invalid/v1"
    agent._base_url_lower = agent.base_url
    agent._base_url_hostname = "example.invalid"
    agent._is_qwen_portal = lambda: False
    agent._is_openrouter_url = lambda: False
    agent._resolved_api_call_timeout = lambda: 60
    agent._max_tokens_param = lambda count: {"max_tokens": count}
    agent._ollama_num_ctx = None
    agent.openrouter_min_coding_score = None
    agent.providers_allowed = None
    agent.providers_ignored = None
    agent.providers_order = None
    agent.provider_sort = None
    agent.provider_require_parameters = False
    agent.provider_data_collection = None
    agent._prepare_messages_for_non_vision_model = lambda messages: messages
    agent._supports_reasoning_extra_body = lambda: False
    transport = BedrockTransport() if mode == "bedrock_converse" else ChatCompletionsTransport()
    agent._get_transport = lambda: transport

    kwargs = _build(agent)

    assert "metadata" not in kwargs


@pytest.mark.parametrize("user_id, task_id", [(USER_ID, TASK_ID), (None, None)])
def test_proxy_parser_round_trip(agent, monkeypatch, user_id, task_id):
    agent._user_id = user_id
    if task_id:
        monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)

    assert _parse_attribution(_build(agent)["metadata"]["user_id"]) == {
        "profile": "dave", "session_id": SESSION_ID,
        "user_id": user_id, "task_id": task_id,
    }


def test_colons_cannot_shift_attribution_axes(agent, monkeypatch):
    monkeypatch.setenv("HERMES_PROFILE", "team:dave")
    monkeypatch.setenv("HERMES_KANBAN_TASK", "board:task")
    agent.session_id = "session:id"
    agent._user_id = "platform:user"

    value = _build(agent)["metadata"]["user_id"]

    assert len(value.split(":")) == 5
    assert _parse_attribution(value) == {
        "profile": "team_dave", "session_id": "session_id",
        "user_id": "platform_user", "task_id": "board_task",
    }


def test_no_requester_is_invented_from_body_or_ambient_env(agent, monkeypatch):
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "unrelated-session-user")
    agent._current_task_id = "turn-uuid-not-a-kanban-task"
    messages = [{
        "role": "user",
        "content": f"[발신자: Wongeun Song <{USER_ID}>; 현재 브로커 세션 ID: {SESSION_ID}]",
    }]

    value = helpers.build_api_kwargs(agent, messages)["metadata"]["user_id"]

    assert value == f"levos:dave:{SESSION_ID}::"


def test_metadata_survives_sdk_serialization_without_network(agent, monkeypatch):
    from agent.anthropic_adapter import sanitize_anthropic_kwargs

    agent._user_id = USER_ID
    monkeypatch.setenv("HERMES_KANBAN_TASK", TASK_ID)
    bodies = []

    def respond(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={
            "id": "msg_test", "type": "message", "role": "assistant",
            "model": agent.model, "content": [], "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 1},
        })

    with anthropic.Anthropic(
        api_key="test-only", base_url="https://example.invalid", max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    ) as client:
        client.messages.create(**sanitize_anthropic_kwargs(_build(agent)))
        client.messages.create(**sanitize_anthropic_kwargs(_build(agent)))

    assert len(bodies) == 2
    assert bodies[0]["metadata"] == bodies[1]["metadata"] == {
        "user_id": f"levos:dave:{SESSION_ID}:{USER_ID}:{TASK_ID}",
    }
    assert _parse_attribution(bodies[0]["metadata"]["user_id"]) == {
        "profile": "dave", "session_id": SESSION_ID,
        "user_id": USER_ID, "task_id": TASK_ID,
    }
