"""Host watchdog and actual auxiliary call must derive compatible config budgets."""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.parametrize("idle,ceiling,aux", [
    (120, 600, 120), (90, 30, 420), (800, 900, 420),
    (0, 600, 700), (-1, 600, 700), (90, 0, 120), (90, -1, 120),
    (120, 600, float("inf")),
])
def test_host_wait_covers_real_auxiliary_budget(idle, ceiling, aux):
    import math
    # Real config loaders and imports; only the provider I/O is replaced.
    from agent.auxiliary_client import _effective_aux_timeout, call_llm
    from agent.conversation_compression import (
        DEFAULT_CONTEXT_TOTAL_CEILING_SECONDS, resolve_context_compression_timeouts,
    )

    home = Path(os.environ["HERMES_HOME"])
    (home / "config.yaml").write_text(json.dumps({
        "compression": {"context_timeout_seconds": idle,
                        "context_total_ceiling_seconds": ceiling},
        "auxiliary": {"compression": {"timeout": aux}},
    }))
    client = MagicMock()
    client.base_url = "http://127.0.0.1:1/v1"
    response = {"ok": True}
    client.chat.completions.create.return_value = response
    with (
        patch("agent.auxiliary_client._resolve_task_provider_model",
              return_value=("custom", "test-model", None, None, None)),
        patch("agent.auxiliary_client._get_cached_client", return_value=(client, "test-model")),
        patch("agent.auxiliary_client._validate_llm_response", side_effect=lambda r, *a, **k: r),
    ):
        call_llm(task="compression", messages=[{"role": "user", "content": "summarize"}])
    effective = client.chat.completions.create.call_args.kwargs["timeout"]
    assert effective == _effective_aux_timeout("compression", None)
    assert effective >= aux
    host_idle, host_ceiling = resolve_context_compression_timeouts()
    if idle > 0:
        # An unbounded auxiliary config must not remove the finite host deadline.
        assert host_idle == (max(idle, effective) if math.isfinite(effective) else idle)
        assert host_ceiling == max(ceiling if ceiling > 0 else DEFAULT_CONTEXT_TOTAL_CEILING_SECONDS, host_idle)
    else:
        assert (host_idle, host_ceiling) == (idle, ceiling)


def test_configured_wait_allows_delayed_first_content(monkeypatch):
    import threading
    from agent.conversation_compression import (
        resolve_context_compression_timeouts, run_compress_context_with_progress_timeout,
    )

    # Scale the auxiliary floor for this real-thread test. First content arrives
    # after the old host idle budget, but within the auxiliary budget.
    monkeypatch.setattr("agent.auxiliary_client._COMPRESSION_TIMEOUT_FLOOR_SECONDS", 2.0)
    home = Path(os.environ["HERMES_HOME"])
    (home / "config.yaml").write_text(json.dumps({
        "compression": {"context_timeout_seconds": 0.05, "context_total_ceiling_seconds": 5},
        "auxiliary": {"compression": {"timeout": 1}},
    }))
    idle, ceiling = resolve_context_compression_timeouts()
    release = threading.Event()
    finished = threading.Event()
    committed = []
    original = [{"role": "user", "content": "original"}]
    summary = [{"role": "assistant", "content": "summary"}]

    def worker(fence):
        timer = threading.Timer(0.25, release.set)
        timer.start()
        try:
            assert release.wait(5)
            fence.touch_progress()
            committed.append(fence.begin_commit())
            if committed[-1]:
                fence.finish_commit()
            return summary, "stable system"
        finally:
            timer.cancel()
            timer.join(5)
            finished.set()

    try:
        result = run_compress_context_with_progress_timeout(
            worker=worker, messages=original, system_prompt_fallback="stable system",
            idle_timeout_seconds=idle, total_ceiling_seconds=ceiling,
        )
        assert result == (summary, "stable system")
        assert committed == [True]
    finally:
        release.set()
        assert finished.wait(5)
