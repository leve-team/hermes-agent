"""Auxiliary 503 ``pool_unavailable`` must classify as a capacity error.

A relay proxy in front of a subscription backend translates an upstream 429
``usage_limit_reached`` into a 503 whose body carries ``pool_unavailable``.
Before this classification the 503 matched only ``_is_transient_transport_error``,
so the auxiliary call retried the same exhausted provider twice and then gave
up — no fallback was ever attempted, and every auxiliary task on that profile
(title generation, compression, vision) silently failed for days.

These tests pin the classification width (only a 503 *with* the pool body),
the preserved same-provider retry path, and the two fallback gates the
classifier has to open.
"""

from unittest.mock import MagicMock, patch

from agent.auxiliary_client import (
    _is_auth_error,
    _is_connection_error,
    _is_invalid_aux_response_error,
    _is_model_incompatible_error,
    _is_payment_error,
    _is_pool_capacity_error,
    _is_rate_limit_error,
    _is_transient_transport_error,
)

# The exact shape the Codex proxy emits (observed in profiles/opsi agent.log).
POOL_UNAVAILABLE_BODY = (
    "Error code: 503 - {'error': {'type': 'proxy_error', "
    "'code': 'pool_unavailable', 'message': 'pool_unavailable'}}"
)


class _ProxyStatusError(Exception):
    """Stand-in for the OpenAI SDK's APIStatusError.

    The classifier reads the status from either ``.status_code`` or
    ``.response.status_code``; this covers both shapes.
    """

    def __init__(self, message, status_code=None, response=None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


def _status_err(message, status):
    """Build an exception shaped like an OpenAI SDK APIStatusError."""
    return _ProxyStatusError(message, status_code=status)


def _should_fallback(err):
    """Reassemble the ``should_fallback`` disjunction from call_llm()."""
    return (
        _is_auth_error(err)
        or _is_payment_error(err)
        or _is_connection_error(err)
        or _is_rate_limit_error(err)
        or _is_model_incompatible_error(err)
        or _is_invalid_aux_response_error(err)
        or _is_pool_capacity_error(err)
    )


def _is_capacity_error(err):
    """Reassemble the ``is_capacity_error`` disjunction from call_llm().

    Note the missing ``_is_auth_error`` term — auth is not capacity, so it
    does not bypass the explicit-provider gate.
    """
    return (
        _is_payment_error(err)
        or _is_connection_error(err)
        or _is_rate_limit_error(err)
        or _is_model_incompatible_error(err)
        or _is_invalid_aux_response_error(err)
        or _is_pool_capacity_error(err)
    )


def test_pool_capacity_503_detected_and_still_transient():
    """503 + ``pool_unavailable`` is a capacity error AND stays retryable.

    Both halves matter: the classifier has to promote the error past the
    fallback gate *without* removing it from the same-provider retry path,
    which recovers the case where the pool refills between attempts.
    """
    err = _status_err(POOL_UNAVAILABLE_BODY, 503)

    assert _is_pool_capacity_error(err) is True
    # Retry path preserved — _is_transient_transport_error is untouched.
    assert _is_transient_transport_error(err) is True


def test_pool_capacity_matches_all_upstreams_unavailable():
    """The sibling proxy phrasing classifies identically."""
    err = _status_err("Error code: 503 - all upstreams unavailable", 503)

    assert _is_pool_capacity_error(err) is True


def test_pool_capacity_reads_status_from_response_object():
    """Status may live on ``exc.response.status_code`` rather than the exception."""
    err = _ProxyStatusError(POOL_UNAVAILABLE_BODY,
                            response=MagicMock(status_code=503))

    assert err.status_code is None
    assert _is_pool_capacity_error(err) is True


def test_pool_capacity_ignores_unrelated_503():
    """A plain 503 blip is NOT a capacity error — it belongs to the retry path.

    Widening this to every 503 would route ordinary upstream hiccups straight
    to another provider instead of retrying the one the user chose.
    """
    err = _status_err("Error code: 503 - upstream timeout", 503)

    assert _is_pool_capacity_error(err) is False
    # ...but it is still retried on the same provider.
    assert _is_transient_transport_error(err) is True
    assert _should_fallback(err) is False


def test_pool_capacity_requires_503_status():
    """The pool body alone is not enough — the status gate is real."""
    for status in (500, 502):
        err = _status_err(f"Error code: {status} - pool_unavailable", status)
        assert _is_pool_capacity_error(err) is False, f"status {status} must not match"

    # No status at all (bare transport exception carrying the string).
    assert _is_pool_capacity_error(Exception("pool_unavailable")) is False


def test_pool_capacity_opens_both_fallback_gates():
    """503 ``pool_unavailable`` must satisfy ``should_fallback`` AND ``is_capacity_error``.

    ``is_capacity_error`` is the half that matters for the profile that hit
    this: with an explicitly pinned ``resolved_provider`` (not ``auto``) the
    gate is ``should_fallback and (is_auto or is_capacity_error)``, so a term
    in ``should_fallback`` alone would still refuse to fall back.
    """
    err = _status_err(POOL_UNAVAILABLE_BODY, 503)

    assert _should_fallback(err) is True
    assert _is_capacity_error(err) is True

    # Explicit (non-auto) provider: the full gate expression must pass.
    is_auto = "openai-codex" in {"auto", "", None}
    assert is_auto is False
    assert (_should_fallback(err) and (is_auto or _is_capacity_error(err))) is True


def test_pool_capacity_is_the_only_term_that_opens_the_gate():
    """Every pre-existing classifier still rejects this error.

    This is what made the bug invisible: the 503 fell through all six terms.
    If a future change teaches (say) ``_is_rate_limit_error`` to accept 503,
    rate-limit-only side effects would fire on a capacity error — this test
    fails loudly if that happens.
    """
    err = _status_err(POOL_UNAVAILABLE_BODY, 503)

    assert _is_auth_error(err) is False
    assert _is_payment_error(err) is False
    assert _is_connection_error(err) is False
    assert _is_rate_limit_error(err) is False
    assert _is_model_incompatible_error(err) is False
    assert _is_invalid_aux_response_error(err) is False


def test_pool_capacity_503_falls_back_to_next_provider(monkeypatch):
    """End-to-end: an explicit-provider aux call routes around the dead pool.

    Mirrors the reported profile — ``model.provider: openai-codex`` pinned,
    the proxy pool exhausted — and asserts the configured fallback chain is
    actually reached instead of the task being dropped.
    """
    monkeypatch.setenv("OPENROUTER_API_KEY", "or-key")
    # Don't burn real wall-clock on the preserved same-provider retries.
    monkeypatch.setattr("agent.auxiliary_client._TRANSIENT_RETRY_BACKOFF_BASE", 0.0)

    primary_client = MagicMock()
    primary_client.chat.completions.create.side_effect = _status_err(
        POOL_UNAVAILABLE_BODY, 503
    )

    fallback_client = MagicMock()
    fallback_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content="title from fallback"))]
    )

    from agent.auxiliary_client import call_llm

    with patch("agent.auxiliary_client._get_cached_client",
               return_value=(primary_client, "gpt-6-astra")), \
         patch("agent.auxiliary_client._resolve_task_provider_model",
               return_value=("openai-codex", "gpt-6-astra", None, None, None)), \
         patch("agent.auxiliary_client._try_configured_fallback_chain",
               return_value=(fallback_client, "claude-sonnet-5",
                             "fallback_chain[0](anthropic)")) as mock_chain, \
         patch("agent.auxiliary_client._try_main_agent_model_fallback") as mock_main:
        result = call_llm(
            task="title_generation",
            messages=[{"role": "user", "content": "summarize this session"}],
        )

    # Same-provider retries still ran (initial attempt + 2 retries).
    assert primary_client.chat.completions.create.call_count >= 2
    # ...and then the chain was tried rather than the task being dropped.
    mock_chain.assert_called()
    assert fallback_client.chat.completions.create.called
    mock_main.assert_not_called()
    # The fallback provider's response is what the caller gets back.
    assert result.choices[0].message.content == "title from fallback"
