"""Approved Codex routes survive lazy resolution, config, and model switches."""

import pytest

from agent.auxiliary_client import _compression_threshold_for_model
from agent.context_compressor import ContextCompressor
from agent.model_metadata import get_model_context_length


@pytest.mark.parametrize("model,context,threshold", [
    ("gpt-6-astra", 900000, 0.60),
    ("openai/gpt-6-astra-pro", 900000, 0.60),
    (" GPT-6-ASTRA ", 900000, 0.60),
    ("gpt-5.6-sol", 272000, 0.30),
    ("openai/gpt-5.6-sol-pro", 272000, 0.30),
])
@pytest.mark.parametrize("provider", ["openai-codex", " OPENAI-CODEX "])
@pytest.mark.parametrize("configured", [0.20, 0.50, 0.95])
def test_approved_route(model, context, threshold, provider, configured):
    assert get_model_context_length(
        model, provider=provider, base_url="http://127.0.0.1:9/backend-api/codex",
        config_context_length=123456,
    ) == context
    assert _compression_threshold_for_model(
        model, provider, allow_codex_gpt55_autoraise=False,
    ) == threshold
    compressor = ContextCompressor(
        model=model, provider=provider, threshold_percent=configured,
        base_url="http://127.0.0.1:9/backend-api/codex", quiet_mode=True,
    )
    assert compressor.context_length == context
    assert compressor.threshold_percent == threshold
    assert compressor.threshold_tokens == int(context * threshold)
    compressor.context_length = context - 1
    assert compressor.threshold_percent == threshold


@pytest.mark.parametrize("model,provider,override", [
    ("gpt-6-astra", "openai", None),
    ("gpt-6-astra", "openrouter", None),
    ("gpt-6-astra", "", None),
    ("gpt-5.6-sol", "openai", None),
    ("unrelated-model", "openai-codex", None),
    ("gpt-5.5", "openai-codex", 0.85),
    ("trinity-large-thinking", "openrouter", 0.75),
])
def test_unrelated_routes_keep_existing_policy(model, provider, override):
    assert get_model_context_length(
        model, provider=provider, config_context_length=123456,
    ) == 123456
    assert _compression_threshold_for_model(model, provider) == override
    compressor = ContextCompressor(
        model=model, provider=provider, threshold_percent=0.50,
        config_context_length=123456, quiet_mode=True,
    )
    assert compressor.context_length == 123456
    assert compressor.threshold_percent == 0.75


def test_model_switch_reapplies_route_policy_without_leaking():
    compressor = ContextCompressor(
        model="gpt-6-astra", provider="openai-codex",
        threshold_percent=0.50, quiet_mode=True,
    )
    assert compressor.context_length == 900000
    assert compressor.threshold_percent == 0.60
    for model, provider, context, threshold in [
        ("gpt-5.6-sol", "openai-codex", 272000, 0.30),
        ("gpt-6-astra", "openai", 900000, 0.50),
        ("gpt-6-astra", "openai-codex", 900000, 0.60),
        ("unrelated-model", "openai-codex", 256000, 0.75),
    ]:
        compressor.update_model(model, context, provider=provider)
        assert compressor.context_length == context
        assert compressor.threshold_percent == threshold
        assert compressor.threshold_tokens == int(context * threshold)


@pytest.mark.parametrize("model,context,threshold", [
    ("gpt-6-astra", 900000, 0.60),
    ("gpt-5.6-sol", 272000, 0.30),
])
def test_agent_initialization_uses_fixed_route(model, context, threshold):
    from run_agent import AIAgent

    agent = AIAgent(
        model=model, provider="openai-codex", api_key="test-placeholder",
        base_url="http://127.0.0.1:9/backend-api/codex",
        enabled_toolsets=[], quiet_mode=True, skip_context_files=True,
        skip_memory=True, skip_background_review=True,
    )
    assert agent.context_compressor.context_length == context
    assert agent.context_compressor.threshold_percent == threshold
    assert agent.context_compressor.threshold_tokens == int(context * threshold)
