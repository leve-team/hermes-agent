"""Regression coverage for stalled preflight compression."""

from types import SimpleNamespace

import pytest

from agent.turn_context import (
    PreflightCompressionTimedOut,
    _fail_closed_after_preflight_timeout,
)


def test_preflight_timeout_blocks_unchanged_provider_payload():
    agent = SimpleNamespace(_last_compression_timed_out=True)

    with pytest.raises(PreflightCompressionTimedOut, match="provider call was not sent"):
        _fail_closed_after_preflight_timeout(agent, 190_035)


def test_structural_noop_keeps_existing_preflight_behavior():
    agent = SimpleNamespace(_last_compression_timed_out=False)

    _fail_closed_after_preflight_timeout(agent, 190_035)


@pytest.mark.parametrize("tokens,window,reserve,allowed", [
    (400_000, 900_000, 20_000, True),
    (541_365, 900_000, 20_000, True),
    (880_000, 900_000, 20_000, True),
    (880_001, 900_000, 20_000, False),
    (900_001, 900_000, None, False),
    (900_000, 900_000, None, True),
    (541_365, None, 20_000, False),
    (541_365, True, 20_000, False),
    (541_365, 0, 20_000, False),
    (541_365, 900_000, "unknown", False),
    (-1, 900_000, None, False),
    (True, 900_000, None, False),
    (None, 900_000, None, False),
    ("unknown", 900_000, None, False),
])
def test_timeout_resume_requires_known_input_window(tokens, window, reserve, allowed):
    agent = SimpleNamespace(
        _last_compression_timed_out=True,
        context_compressor=SimpleNamespace(
            context_length=window, max_tokens=reserve, threshold_tokens=540_000,
        ),
    )
    if allowed:
        _fail_closed_after_preflight_timeout(agent, tokens)
    else:
        with pytest.raises(PreflightCompressionTimedOut):
            _fail_closed_after_preflight_timeout(agent, tokens)
    # Continuing a request must never erase the failed compression outcome.
    assert agent._last_compression_timed_out is True
