# Nested tool session identity

The registered `execute_code` handler accepts the host dispatcher session identity
through keyword context, never through model arguments. Local socket RPC and remote
file RPC forward that identity unchanged to `handle_function_call`. Terminal task
IDs remain resource identifiers, not caller identities. Missing identities are not
replaced with environment values, parent sessions, or task IDs; downstream security
middleware retains its fail-closed validation. Nested file calls still traverse the
normal pre-tool protection hooks.

## Paired deployment required

For Levos, deploy this core change together with the secret-redactor lock-scope
repair (`BUILD=2026-10-03-nested-tool-lock`) in `leve-team/levos`.
**Do not deploy the core fix alone:** the previous redactor holds its session lock
while the outer tool waits on an RPC thread that needs the same lock, deadlocking
otherwise valid nested calls. Only tool execution leaves the lock; pre refresh and
post refresh plus output redaction remain locked. Never disable the guard to deploy.

After approved rollout into new processes, verify direct, nested, and delegated-child
public fixture reads, missing-identity denial, pre-tool file protection, and fixture
secret masking. A successful PR or local test does not establish rollout completion.

## Validation

Run `pytest tests/tools/test_code_execution.py tests/tools/test_code_execution_modes.py
tests/tools/test_code_execution_windows_env.py tests/tools/test_code_execution_session_identity.py`.
Identity fixtures exercise both Unix socket and file RPC with real dispatcher and
file handlers; the remote backend is a disposable local shell, not an actual SSH host.

The developer-local `test_local_redactor_acceptance.py` diagnostic is intentionally
excluded from this change: it references a second local checkout and is not a portable
test. Its separate result is integration evidence only, not a shipped CI contract.