"""Real AIAgent + public provider factory; no CLI/model/network calls."""
from types import SimpleNamespace
import sys

import httpx
import pytest
from openai.types.chat import ChatCompletion

from providers import get_provider_profile, register_provider
from providers.base import ProviderProfile
from run_agent import AIAgent


class LocalClient:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.api_key = kwargs['api_key']
        self.base_url = kwargs['base_url']
        self.timeout = kwargs.get('timeout')
        self.chat = SimpleNamespace(completions=self)
        self.closed = False
        self.calls = []
        self.instances.append(self)

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get('stream'):
            from openai.types.chat import ChatCompletionChunk
            return iter([ChatCompletionChunk.model_validate({
                'id': 'fixture', 'object': 'chat.completion.chunk', 'created': 0,
                'model': 'fixture-model', 'choices': [{'index': 0, 'finish_reason': 'stop',
                    'delta': {'role': 'assistant', 'content': 'fixture response'}}],
            })])
        return ChatCompletion.model_validate({
            'id': 'fixture', 'object': 'chat.completion', 'created': 0,
            'model': 'fixture-model', 'choices': [{'index': 0, 'finish_reason': 'stop',
                'message': {'role': 'assistant', 'content': 'fixture response'}}],
            'usage': {'prompt_tokens': 2, 'completion_tokens': 3, 'total_tokens': 5},
        })

    def close(self):
        self.closed = True

    def is_closed(self):
        return self.closed


class LocalProfile(ProviderProfile):
    def create_client(self, **kwargs):
        return LocalClient(**kwargs)


@pytest.fixture
def profile(monkeypatch):
    import socket
    def no_network(*args, **kwargs):
        raise AssertionError('external network is forbidden in process-provider tests')
    monkeypatch.setattr(socket.socket, 'connect', no_network)
    import hermes_cli.auth as auth
    monkeypatch.setattr(auth, 'PROVIDER_REGISTRY', dict(auth.PROVIDER_REGISTRY))
    import hermes_cli.runtime_provider as runtime
    monkeypatch.setattr(runtime, 'PROVIDER_REGISTRY', auth.PROVIDER_REGISTRY)
    # Register through the same public seam as an out-of-tree provider plugin.
    get_provider_profile('openrouter')
    import providers
    monkeypatch.setattr(providers, '_REGISTRY', dict(providers._REGISTRY))
    monkeypatch.setattr(providers, '_ALIASES', dict(providers._ALIASES))
    monkeypatch.setattr(providers, '_PROVIDER_LIST_CACHE', None)
    profile = LocalProfile(name='fixture-process', auth_type='external_process',
        api_mode='chat_completions', base_url='process://fixture-process',
        process_command=sys.executable, process_args=('--fixture',),
        process_command_env_vars=('FIXTURE_CLI_PATH',), process_args_env_var='FIXTURE_CLI_ARGS')
    register_provider(profile)
    LocalClient.instances = []
    return profile


def make_agent(profile, **kwargs):
    return AIAgent(provider=profile.name, model='fixture-model', api_mode='chat_completions',
        api_key=profile.name, base_url=profile.base_url, command=sys.executable, args=['--fixture'],
        enabled_toolsets=[], quiet_mode=True, skip_memory=True, skip_context_files=True,
        skip_background_review=True, **kwargs)


def test_real_agent_constructs_provider_client(profile):
    agent = make_agent(profile)
    try:
        assert isinstance(agent.client, LocalClient)
        assert agent.client.kwargs['command'] == sys.executable
        assert agent.client.kwargs['args'] == ['--fixture']
        assert agent.client.base_url == 'process://fixture-process'
        timeout = httpx.Timeout(17, connect=3)
        client = agent._create_openai_client(dict(agent._client_kwargs, timeout=timeout),
            reason='timeout-contract', shared=False)
        assert client.timeout is timeout
        client.close()
        assert agent._get_transport().__class__.__name__ == 'ChatCompletionsTransport'
    finally:
        agent.close()


def test_generic_runtime_and_command_overrides(profile, monkeypatch):
    from hermes_cli.runtime_provider import resolve_runtime_provider
    monkeypatch.setenv('FIXTURE_CLI_PATH', sys.executable)
    monkeypatch.setenv('FIXTURE_CLI_ARGS', '--one "two words"')
    runtime = resolve_runtime_provider(requested=profile.name)
    assert runtime['provider'] == profile.name
    assert runtime['api_key'] == profile.name  # placeholder, not an HTTP credential
    assert runtime['base_url'] == profile.base_url
    assert runtime['command'] == sys.executable
    assert runtime['args'] == ['--one', 'two words']
    agent = AIAgent(model='fixture-model', enabled_toolsets=[], quiet_mode=True,
        skip_memory=True, skip_background_review=True, skip_context_files=True,
        **{k: runtime[k] for k in ('provider', 'api_mode', 'api_key', 'base_url', 'command', 'args')})
    try:
        assert agent.client.kwargs['args'] == ['--one', 'two words']
    finally:
        agent.close()


@pytest.mark.parametrize('factory', [None, 42, lambda **kw: None])
def test_process_factory_never_falls_through_to_http(profile, monkeypatch, factory):
    monkeypatch.setattr(profile, 'create_client', factory)
    with pytest.raises(RuntimeError, match='create_client'):
        make_agent(profile)


def test_agent_without_explicit_credentials_uses_process_runtime(profile):
    agent = AIAgent(provider=profile.name, model='fixture-model', enabled_toolsets=[],
        quiet_mode=True, skip_memory=True, skip_context_files=True, skip_background_review=True)
    try:
        assert isinstance(agent.client, LocalClient)
        assert agent.client.kwargs['command'] == sys.executable
        assert agent.client.kwargs['args'] == ['--fixture']
    finally:
        agent.close()


@pytest.mark.parametrize('streaming', [False, True])
def test_real_agent_text_turn_and_typed_tool_normalization(profile, streaming):
    agent = make_agent(profile, max_iterations=2)
    # Exercise the non-stream transport, not a fake agent or patched core call.
    agent._disable_streaming = not streaming
    try:
        result = agent.run_conversation('fixture question', system_message='Fixture system.')
        assert result['final_response'] == 'fixture response'
        assert any(c.calls for c in LocalClient.instances)
        response = agent.client.create(model='fixture-model', messages=[])
        response.choices[0].finish_reason = 'tool_calls'
        from openai.types.chat import ChatCompletionMessageFunctionToolCall
        response.choices[0].message.tool_calls = [ChatCompletionMessageFunctionToolCall(
            id='fixture-call', type='function', function={'name': 'fixture_tool', 'arguments': '{"x":1}'})]
        normalized = agent._get_transport().normalize_response(response)
        assert normalized.tool_calls[0].id == 'fixture-call'
        assert normalized.tool_calls[0].function.name == 'fixture_tool'
    finally:
        agent.close()
    assert all(c.closed for c in LocalClient.instances)


def test_public_interrupt_closes_inflight_process_client(profile):
    import threading
    agent = make_agent(profile)
    client = agent._create_request_openai_client(reason='interrupt-fixture')
    agent._active_request_abort = lambda reason: agent._abort_request_openai_client(client, reason=reason)
    try:
        thread = threading.Thread(target=agent.interrupt, kwargs={'hard_cancel': True})
        thread.start()
        thread.join(3)
        assert not thread.is_alive()
        assert client.closed
    finally:
        agent._close_request_openai_client(client, reason='interrupt-fixture')
        agent.close()


def test_auxiliary_and_fallback_router_use_public_factory(profile):
    from agent.auxiliary_client import resolve_provider_client
    client, model = resolve_provider_client(profile.name, model='fixture-model')
    assert isinstance(client, LocalClient)
    assert model == 'fixture-model'
    assert client.kwargs['command'] == sys.executable
    client.close()


def test_process_status_reports_own_binary_not_copilot(profile):
    from hermes_cli.auth import get_auth_status
    status = get_auth_status(profile.name)
    assert status['configured']
    assert status['command'] == sys.executable
    assert status['args'] == ['--fixture']
    assert status['auth_verified'] is False  # installed is not proof of authentication


def test_raising_factory_preserves_honest_error(profile, monkeypatch):
    def broken(**kwargs):
        raise ValueError('fixture factory error')
    monkeypatch.setattr(profile, 'create_client', broken)
    with pytest.raises(RuntimeError, match='create_client failed') as error:
        make_agent(profile)
    # Existing init wrapper preserves the factory exception as context.
    assert isinstance(error.value.__context__.__cause__, ValueError)


def test_missing_cli_is_not_reported_as_an_http_key_error(profile, monkeypatch):
    from hermes_cli.auth import AuthError, resolve_external_process_provider_credentials
    monkeypatch.setattr(profile, 'process_command', '/nonexistent-fixture/cli')
    with pytest.raises(AuthError) as error:
        resolve_external_process_provider_credentials(profile.name)
    assert error.value.code == 'missing_external_process_cli'


def test_real_inflight_turn_is_cancelled_through_public_interrupt(profile, monkeypatch):
    import threading
    entered, cancelled = threading.Event(), threading.Event()
    original_close = LocalClient.close
    def blocking_create(self, **kwargs):
        entered.set()
        assert cancelled.wait(5), 'process client was not cancelled'
        raise RuntimeError('fixture process cancelled')
    def close(self):
        original_close(self)
        cancelled.set()
    monkeypatch.setattr(LocalClient, 'create', blocking_create)
    monkeypatch.setattr(LocalClient, 'close', close)
    agent = make_agent(profile, max_iterations=2)
    agent._disable_streaming = True
    results = []
    worker = threading.Thread(target=lambda: results.append(
        agent.run_conversation('fixture cancellation', system_message='Fixture system.')))
    try:
        worker.start()
        assert entered.wait(5)
        agent.interrupt(hard_cancel=True)
        worker.join(5)
        assert cancelled.is_set()
        assert not worker.is_alive()
        assert results and results[0]['interrupted']
    finally:
        agent.close()
        worker.join(5)
