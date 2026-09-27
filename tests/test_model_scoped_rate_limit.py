"""모델 스코프 429 는 계정을 벤치하지 않는다 (실사고 2026-09-26: fable 429 → 계정 2개 소진 → 폴백 무자격 → 무응답).

계정 전체 한도 429 는 종전대로 1시간 벤치, 모델만 막힌 429 는 짧은 쿨다운 + 로테이션 제외.
"""
from __future__ import annotations

import time

import pytest

from agent.credential_pool import (
    EXHAUSTED_TTL_429_SECONDS,
    EXHAUSTED_TTL_SOLE_CREDENTIAL_SECONDS,
    FAILURE_REASON_MODEL_RATE_LIMIT,
    CredentialPool,
    PooledCredential,
    _exhausted_ttl,
    is_model_scoped_rate_limit,
)

MODEL_SCOPED = {
    "status": 429,
    "type": "rate_limit_error",
    "message": "This request would exceed your organization's rate limit for claude-fable-5-1",
    "model": "claude-fable-5-1",
}
ACCOUNT_WIDE = {"status": 429, "type": "rate_limit_error", "message": "Rate limit exceeded for your organization"}


def entry(label: str) -> PooledCredential:
    return PooledCredential(
        provider="anthropic", id=label, label=label, auth_type="oauth", priority=0,
        source="test", access_token="tok-" + label,
    )


def test_model_scoped_body_is_recognised():
    assert is_model_scoped_rate_limit(MODEL_SCOPED) is True


def test_account_wide_body_is_not_model_scoped():
    assert is_model_scoped_rate_limit(ACCOUNT_WIDE) is False
    assert is_model_scoped_rate_limit(None) is False
    assert is_model_scoped_rate_limit({"message": None}) is False


def test_model_scoped_cooldown_is_short_account_wide_stays_long():
    short = _exhausted_ttl(429, failure_reason=FAILURE_REASON_MODEL_RATE_LIMIT)
    assert short == min(EXHAUSTED_TTL_429_SECONDS, EXHAUSTED_TTL_SOLE_CREDENTIAL_SECONDS)
    assert _exhausted_ttl(429) == EXHAUSTED_TTL_429_SECONDS


def test_model_scoped_429_does_not_drain_the_pool():
    """실사고 재현: 모델 스코프 429 를 계정 소진으로 처리하면 2턴 만에 풀이 비고 폴백이 자격증명을 잃는다."""
    pool = CredentialPool("anthropic", [entry("a"), entry("b")])
    for _ in range(3):
        current = pool.select()
        assert current is not None, "모델 스코프 429 로 풀이 비면 안 된다"
        pool.mark_exhausted_and_rotate(
            status_code=429, error_context=MODEL_SCOPED,
            api_key_hint=current.runtime_api_key, failure_reason=FAILURE_REASON_MODEL_RATE_LIMIT,
        )
    assert pool.has_available() is True
    remaining = (pool.next_available_at() or time.time()) - time.time()
    assert remaining <= EXHAUSTED_TTL_SOLE_CREDENTIAL_SECONDS + 5


def test_account_wide_429_still_benches_the_account():
    pool = CredentialPool("anthropic", [entry("a"), entry("b")])
    first = pool.select()
    pool.mark_exhausted_and_rotate(
        status_code=429, error_context=ACCOUNT_WIDE, api_key_hint=first.runtime_api_key,
        failure_reason="rate_limit",
    )
    statuses = {e.label: e.last_status for e in pool.entries()}
    assert statuses[first.label] == "exhausted"
