#!/usr/bin/env bash
# Canonical file-isolated runner; bootstrap test-only packages if the runtime
# image has product dependencies but no development venv. No repo deps change.
set -euo pipefail

test_python=""
for candidate in "$PWD/.venv/bin/python" "$PWD/venv/bin/python" "${HERMES_PYTHON:-}" "$(command -v python)"; do
    if [ -x "$candidate" ] && "$candidate" -c 'import pytest, pytest_asyncio, pytest_timeout, openai' 2>/dev/null; then
        test_python="$candidate"
        break
    fi
done
if [ -z "$test_python" ]; then
    test_env=$(mktemp -d /tmp/hermes-compression-testenv.XXXXXX)
    trap 'rm -rf "$test_env"' EXIT
    runtime_site=$(python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
    python -m venv --system-site-packages "$test_env"
    test_site=$("$test_env/bin/python" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
    printf '%s\n' "$runtime_site" > "$test_site/hermes-runtime.pth"
    "$test_env/bin/python" -m pip install --disable-pip-version-check \
        pytest==8.4.2 pytest-asyncio==0.25.3 pytest-timeout==2.4.0
    test_python="$test_env/bin/python"
fi
export HERMES_PYTHON="$test_python"
export HERMES_TEST_FILE_RETRIES=0

scripts/run_tests.sh -j 3 -q \
    tests/agent/test_preflight_compression_timeout_fail_closed.py \
    tests/agent/test_compression_host_aux_budget.py \
    tests/agent/test_auxiliary_compression_timeout_floor.py \
    tests/agent/test_compress_context_progress_timeout.py \
    tests/agent/test_turn_context.py \
    tests/agent/test_preflight_compression_gate.py \
    tests/agent/test_native_preflight_estimate.py \
    tests/agent/test_usage_anchor.py \
    tests/agent/test_engine_preflight_wire.py \
    tests/agent/test_preflight_lock_defer.py \
    tests/agent/test_compression_worker_isolation_76354.py \
    tests/agent/test_compression_attempt_ownership.py \
    tests/agent/test_compression_attempt_telemetry.py \
    tests/agent/test_compression_attempt_lifecycle.py \
    tests/agent/test_compression_review_76354.py \
    tests/agent/test_compression_stall_interrupt_96775.py \
    tests/agent/test_compression_stall_fallback_78981.py \
    tests/agent/test_compression_fallback_budget.py \
    tests/agent/test_compression_interrupt_protection.py \
    tests/run_agent/test_compression_timeout_terminal.py \
    tests/run_agent/test_compression_timeout_http.py \
    tests/run_agent/test_preflight_timeout_typed_result.py \
    tests/run_agent/test_preflight_compression_cap_e2e.py \
    tests/run_agent/test_compression_lock_defer.py \
    tests/run_agent/test_compression_persistence.py \
    tests/run_agent/test_compression_budget_refund.py \
    tests/run_agent/test_compression_abort_state_reset.py \
    tests/run_agent/test_compression_boundary.py \
    tests/test_compression_watermark_commit.py
