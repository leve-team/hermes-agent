"""levos v3 P2 ④ (t_74963c68) — the turn trusted context seen from a tool handler.

A ticket registered under a turn id is ``tools.trusted_context.current()`` inside a
handler that ``registry.dispatch`` runs for that turn (``_trusted_turn_id``), on whatever
thread runs it, and nowhere else: not after the handler returns, not in a dispatch without
the turn id, not on a thread started from the handler with its contextvars copied (how a
delegate child runs). The turn id never reaches a handler's kwargs, and the ticket never
shows in a repr, a str, a log line, a pickle or a copy.
"""

from __future__ import annotations

import contextvars
import copy
import json
import logging
import pickle
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from tools import trusted_context
from tools.registry import ToolRegistry


@pytest.fixture
def ticket():
    """A ticket registered under a fresh turn id for the test, unregistered after."""
    turn_id, token = f"sess:task:{uuid.uuid4().hex[:8]}", f"TKT-{uuid.uuid4()}"
    tc = trusted_context.TrustedContext(token)
    trusted_context.register(turn_id, tc)
    yield turn_id, token, tc
    trusted_context.unregister(turn_id)


def _schema(name: str) -> dict:
    return {"name": name, "description": name, "parameters": {"type": "object", "properties": {}}}


def _seen_token() -> str | None:
    tc = trusted_context.current()
    return None if tc is None else tc.token


def _registry_with(name: str, handler, **register_kwargs) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(name=name, toolset="tc_test", schema=_schema(name), handler=handler, **register_kwargs)
    return reg


def test_ticket_is_never_rendered(ticket, caplog):
    _turn_id, token, tc = ticket
    caplog.set_level(logging.DEBUG)
    logging.getLogger("tc-test").debug("context %s %r", tc, tc)
    rendered = [repr(tc), str(tc), f"{tc}", f"{tc!r}", "%s" % (tc,), format(tc), caplog.text,
                repr([tc]), repr({"tc": tc})]
    assert all(token not in text for text in rendered)
    assert tc.token == token
    # No value comparison: an equal string elsewhere is not this turn's authority.
    assert tc != trusted_context.TrustedContext(token)
    for leak in (pickle.dumps, copy.copy, copy.deepcopy):
        with pytest.raises(TypeError) as refused:
            leak(tc)
        assert token not in str(refused.value)


def test_handler_sees_the_ticket_only_while_it_runs(ticket):
    turn_id, token, _tc = ticket
    seen = []
    reg = _registry_with("probe", lambda args, **kw: seen.append(_seen_token()) or "{}")

    assert _seen_token() is None
    reg.dispatch("probe", {}, _trusted_turn_id=turn_id)
    assert seen == [token]
    assert _seen_token() is None
    # Without the turn id (plugins' dispatch_tool, execute_code RPC) or with another
    # turn's id (a delegate child, a cron or background turn): nothing.
    reg.dispatch("probe", {})
    reg.dispatch("probe", {}, _trusted_turn_id=f"{turn_id}-other")
    reg.dispatch("probe", {}, _trusted_turn_id=None)
    assert seen == [token, None, None, None]


def test_failing_handler_leaves_nothing_bound(ticket):
    turn_id, token, _tc = ticket
    seen = []

    def boom(args, **kw):
        seen.append(_seen_token())
        raise RuntimeError("handler failed")

    result = json.loads(_registry_with("boom", boom).dispatch("boom", {}, _trusted_turn_id=turn_id))
    assert "handler failed" in result["error"]
    assert seen == [token]
    assert _seen_token() is None


def test_turn_id_never_reaches_a_handler_without_var_kwargs(ticket):
    """Handlers that name their kwargs (no ``**kwargs``) still run: the reserved key is
    the registry's, never the handler's."""
    turn_id, token, _tc = ticket
    calls = []

    def strict(args, task_id=None, session_id=None, user_task=None):
        calls.append((task_id, session_id, user_task, _seen_token()))
        return "{}"

    reg = _registry_with("strict", strict)
    out = reg.dispatch("strict", {}, task_id="t", session_id="s", user_task="u", _trusted_turn_id=turn_id)
    assert out == "{}"
    assert calls == [("t", "s", "u", token)]


def test_async_handler_sees_the_ticket(ticket):
    turn_id, token, _tc = ticket
    seen = []

    async def probe(args, **kw):
        seen.append(_seen_token())
        return "{}"

    reg = _registry_with("aprobe", probe, is_async=True)
    result: list = []
    # A non-main thread without a running loop, like a turn's tool worker.
    worker = threading.Thread(target=lambda: result.append(reg.dispatch("aprobe", {}, _trusted_turn_id=turn_id)))
    worker.start()
    worker.join(10)
    assert result == ["{}"]
    assert seen == [token]


def test_parallel_workers_each_see_the_ticket_and_keep_none_after(ticket):
    turn_id, token, _tc = ticket
    together = threading.Barrier(2, timeout=10)
    seen: list = []

    def probe(args, **kw):
        together.wait()  # both handlers are in flight at once
        seen.append((threading.get_ident(), _seen_token()))
        return "{}"

    reg = _registry_with("probe", probe)

    def after_batch():
        together.wait()  # one probe per worker
        return threading.get_ident(), _seen_token()

    with ThreadPoolExecutor(max_workers=2) as pool:
        for future in [pool.submit(reg.dispatch, "probe", {}, _trusted_turn_id=turn_id) for _ in range(2)]:
            assert future.result(10) == "{}"
        left = [future.result(10) for future in [pool.submit(after_batch) for _ in range(2)]]

    workers = {ident for ident, _ in seen}
    assert len(workers) == 2 and [t for _, t in seen] == [token, token]
    assert {ident for ident, _ in left} == workers
    assert [t for _, t in left] == [None, None]


def test_nested_dispatch_without_the_turn_id_sees_nothing_and_restores(ticket):
    turn_id, token, _tc = ticket
    seen: list = []
    reg = ToolRegistry()
    reg.register(name="inner", toolset="tc_test", schema=_schema("inner"),
                 handler=lambda args, **kw: seen.append(("inner", _seen_token())) or "{}")

    def outer(args, **kw):
        seen.append(("outer", _seen_token()))
        reg.dispatch("inner", {})
        seen.append(("outer-after", _seen_token()))
        return "{}"

    reg.register(name="outer", toolset="tc_test", schema=_schema("outer"), handler=outer)
    reg.dispatch("outer", {}, _trusted_turn_id=turn_id)
    assert seen == [("outer", token), ("inner", None), ("outer-after", token)]
    assert _seen_token() is None


def test_thread_started_with_copied_contextvars_does_not_inherit(ticket):
    """A delegate child runs on a pool worker under ``copy_context().run``: the parent
    handler's ticket must not follow it there."""
    turn_id, token, _tc = ticket
    seen: list = []

    def parent(args, **kw):
        seen.append(_seen_token())
        ctx = contextvars.copy_context()
        child = threading.Thread(target=lambda: ctx.run(lambda: seen.append(_seen_token())))
        child.start()
        child.join(10)
        return "{}"

    _registry_with("parent", parent).dispatch("parent", {}, _trusted_turn_id=turn_id)
    assert seen == [token, None]


def test_turn_table_register_lookup_unregister():
    turn_id = f"sess:task:{uuid.uuid4().hex[:8]}"
    tc = trusted_context.TrustedContext("TKT-x")
    assert trusted_context.lookup(turn_id) is None
    trusted_context.register(turn_id, tc)
    try:
        assert trusted_context.lookup(turn_id) is tc
        assert trusted_context.lookup("") is None and trusted_context.lookup(None) is None
    finally:
        trusted_context.unregister(turn_id)
    assert trusted_context.lookup(turn_id) is None
    trusted_context.unregister(turn_id)  # idempotent
