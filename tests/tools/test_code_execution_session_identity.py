"""Nested RPC dispatch must retain the caller, not the terminal task identity."""
import json
import subprocess
import sys

import pytest

import model_tools
from hermes_cli import middleware
from tools import code_execution_tool as execution
from tools import file_tools  # noqa: F401 -- register real file handlers


@pytest.fixture
def protected_fixture(tmp_path, monkeypatch):
    monkeypatch.setenv('TERMINAL_ENV', 'local')
    monkeypatch.setenv('TERMINAL_CWD', str(tmp_path))
    monkeypatch.setattr(execution, '_load_config', lambda: {'mode': 'strict'})
    path = tmp_path / 'public.txt'
    path.write_text('public identity fixture\n')
    observed = []

    def guard(*, args, next_call, session_id='', tool_name='', **context):
        observed.append((tool_name, session_id))
        if not isinstance(session_id, str) or not session_id or len(session_id) > 256:
            return json.dumps({'error': 'session_identity_invalid'})
        return next_call(args)

    monkeypatch.setattr(middleware, '_get_middleware_callbacks', lambda kind: [guard] if kind == 'tool_execution' else [])
    return path, observed


@pytest.fixture(params=['local', 'file-rpc'])
def transport(request, tmp_path, monkeypatch):
    if request.param == 'file-rpc':
        if sys.platform == 'win32':
            pytest.skip('Remote shell fixture requires POSIX')

        class ShellBackend:
            """Real file-RPC transport in a disposable local backend, no SSH needed."""
            def get_temp_dir(self):
                return str(tmp_path)

            def execute(self, command, cwd=None, timeout=None):
                result = subprocess.run(command, shell=True, cwd=cwd or tmp_path,
                                        timeout=timeout, capture_output=True, text=True)
                return {'output': result.stdout, 'returncode': result.returncode}

        from tools import terminal_tool
        monkeypatch.setattr(terminal_tool, '_get_env_config', lambda: {'env_type': 'ssh'})
        monkeypatch.setattr(execution, '_get_or_create_env', lambda task_id: (ShellBackend(), 'ssh'))
    return request.param


@pytest.mark.parametrize('caller', ['parent-session', 'delegate-child-session'])
def test_nested_read_has_exact_caller_identity(protected_fixture, transport, caller):
    path, observed = protected_fixture
    direct = json.loads(model_tools.handle_function_call(
        'read_file', {'path': str(path)}, task_id='terminal-resource', session_id=caller))
    assert 'public identity fixture' in direct['content']
    result = json.loads(model_tools.handle_function_call(
        'execute_code', {'code': f'from hermes_tools import read_file\nprint(read_file({str(path)!r}))'},
        task_id='terminal-resource', session_id=caller, enabled_tools=['read_file']))
    assert result['status'] == 'success'
    assert 'public identity fixture' in result['output']
    assert observed == [('read_file', caller), ('execute_code', caller), ('read_file', caller)]


@pytest.mark.parametrize('missing', [None, '', 123, 'x' * 257])
def test_missing_or_invalid_identity_stays_closed(protected_fixture, transport, monkeypatch, missing):
    path, observed = protected_fixture
    monkeypatch.setenv('HERMES_SESSION_ID', 'unrelated-environment-session')
    # Invoke the registered handler to reach the nested boundary without an
    # outer guard stopping first. No identity is sourced from the resource ID,
    # process environment, or a model-provided top-level argument.
    result = json.loads(execution._execute_code_handler(
        {'code': f'from hermes_tools import write_file\nprint(write_file({str(path)!r}, "changed"))',
         'session_id': 'model-supplied-identity'},
        task_id='unrelated-terminal-resource', session_id=missing, enabled_tools=['write_file']))
    assert result['status'] == 'success'
    assert 'session_identity_invalid' in result['output']
    assert path.read_text() == 'public identity fixture\n'
    assert observed == [('write_file', missing or '')]


def test_nested_pretool_file_guard_still_blocks(protected_fixture, transport, monkeypatch):
    path, observed = protected_fixture
    from hermes_cli import plugins
    calls = []

    def pretool(name, args, **context):
        calls.append((name, context['session_id']))
        if name == 'write_file':
            return 'fixture file policy denied', None
        return None, None

    monkeypatch.setattr(plugins, '_dispatch_pre_tool_call_hooks', pretool)
    result = json.loads(model_tools.handle_function_call(
        'execute_code', {'code': f'from hermes_tools import write_file\nprint(write_file({str(path)!r}, "changed"))'},
        session_id='caller-session', task_id='resource', enabled_tools=['write_file']))
    assert 'fixture file policy denied' in result['output']
    assert path.read_text() == 'public identity fixture\n'
    assert calls == [('execute_code', 'caller-session'), ('write_file', 'caller-session')]
