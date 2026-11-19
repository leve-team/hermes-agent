#!/usr/bin/env bash
# Run 6dc05f66 behavior gate: canonical file-isolated runner over the @file
# expansion tests and every suite that exercises the same code path.
# The runtime image ships product dependencies but no pytest, so bootstrap the
# test-only packages into a disposable venv when no development venv exists.
# No repo dependency changes.
set -euo pipefail

test_python=""
for candidate in "$PWD/.venv/bin/python" "$PWD/venv/bin/python" "${HERMES_PYTHON:-}" "$(command -v python)"; do
    if [ -x "$candidate" ] && "$candidate" -c 'import pytest, pytest_asyncio, pytest_timeout, openai' 2>/dev/null; then
        test_python="$candidate"
        break
    fi
done
if [ -z "$test_python" ]; then
    test_env=$(mktemp -d /tmp/hermes-ctxref-testenv.XXXXXX)
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
    tests/agent/test_context_references.py \
    tests/agent/test_context_refs_concurrent.py \
    tests/agent/test_plugin_context_references.py \
    tests/tools/test_credential_files.py \
    tests/tui_gateway/test_attach_does_not_wait_for_agent.py \
    tests/gateway/test_context_ref_expansion_runtime.py \
    tests/cli/test_cli_codex_context_reference.py \
    tests/security/test_gitspawn_config_injection.py

# Same scenario end to end through the real core staging function (file.attach path).
"$test_python" docs/runs/6dc05f66/live_probe.py --once
