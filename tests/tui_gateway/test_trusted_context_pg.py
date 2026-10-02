"""levos v3 P2 ④ (t_74963c68) — the ``_levos_turn`` ticket of a ``prompt.submit``.

The broker attaches a user-authority ticket to a submit's params; the core keeps it for
that turn only and hands it to tool handlers only, through
``tools.trusted_context.current()``. Pinned on a real PostgreSQL authority store, the real
turn thread and a real AIAgent whose model is a stand-in calling real registered tools:

* C-10 the ticket is nowhere else: no table of the store (messages, the session's system
  prompt, the submit's ``core_submit_accepts`` row), no file under ``HERMES_HOME``
  (transcripts, logs), no model request, no frame sent to a client, no log record, no
  RPC reply, no compute-host dispatch;
* C-11 a tool handler of the turn reads it — on the sequential path and on each worker
  of a parallel batch — and nothing is bound once the handler returns; after the turn
  (completed, raised, interrupted) the agent holds none and the turn table is empty;
* C-12 a ``delegate_task`` child of that turn, a turn queued behind it, an auto-continue
  turn and a cron-style agent turn running at the same time read nothing;
* C-13 a submit without the key reads nothing.

Runs on the fork's ephemeral PostgreSQL (``initdb`` / ``pg_ctl`` on PATH or in
``PG3_PERCENT_PG_BIN``; missing tools are errors, never skips).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import sql

from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn
from tests.tui_gateway.test_auto_continue_inprocess_pg import WS, _await, _call
from tests.tui_gateway.test_submit_idempotency_pg import (
    _REAL_RUN_AFTER_AGENT_READY,
    _REAL_TURN_STUBS,
    _clean_env as _clean_env,
    authority as authority,
)
from tools import trusted_context
from tools.registry import registry
from tui_gateway import server

PROBE = "tc_probe"
_PROBE_DEF = {"type": "function", "function": {
    "name": PROBE, "description": "records the turn trusted context",
    "parameters": {"type": "object", "properties": {"label": {"type": "string"}, "hold": {"type": "boolean"}}}}}


def _ticket() -> str:
    return f"TKT-{uuid.uuid4()}"


def _response(content: str = "", tool_calls=None):
    return SimpleNamespace(
        choices=[SimpleNamespace(
            message=SimpleNamespace(content=content, tool_calls=tool_calls),
            finish_reason="tool_calls" if tool_calls else "stop")],
        model="test/model",
        usage=SimpleNamespace(prompt_tokens=5, completion_tokens=3, total_tokens=8))


class Model:
    """Stand-in provider of one agent: a user message is answered with ``calls`` (one tool
    batch), a tool result with the final text. Every request is kept."""

    def __init__(self, calls: list):
        self.calls = calls
        self.requests: list = []

    def __call__(self, **kwargs):
        self.requests.append(kwargs)
        if kwargs["messages"][-1].get("role") == "tool" or not self.calls:
            return _response("done")
        return _response(tool_calls=[
            SimpleNamespace(id=f"call_{uuid.uuid4().hex[:8]}", type="function",
                            function=SimpleNamespace(name=name, arguments=json.dumps(args)))
            for name, args in self.calls])


class Probe:
    """``tc_probe`` on the real registry. Its handler names its kwargs (no ``**kwargs``),
    so a turn id leaking into them fails the call. ``seen``: (label, thread, token) per
    call; ``after``: (thread, token) right after each dispatch returned, on its thread."""

    def __init__(self):
        self.seen: list = []
        self.after: list = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.together: threading.Barrier | None = None
        self.during_hold = None

    def handler(self, args, task_id=None, session_id=None, user_task=None):
        tc = trusted_context.current()
        if self.together is not None and str(args.get("label", "")).startswith("par"):
            self.together.wait(10)  # every call of the parallel batch is in flight at once
        self.seen.append((args.get("label"), threading.get_ident(), None if tc is None else tc.token))
        if args.get("hold"):
            self.entered.set()
            if self.during_hold is not None:
                self.during_hold()
            assert self.release.wait(20)
        return json.dumps({"ok": True})

    def tokens(self, label: str) -> list:
        return [token for name, _thread, token in self.seen if name == label]


@pytest.fixture
def probe(monkeypatch):
    from agent import tool_dispatch_helpers

    found = Probe()
    registry.register(name=PROBE, toolset="tc_test", schema=_PROBE_DEF["function"], handler=found.handler)
    monkeypatch.setattr(tool_dispatch_helpers, "_PARALLEL_SAFE_TOOLS",
                        tool_dispatch_helpers._PARALLEL_SAFE_TOOLS | {PROBE})
    real_dispatch = registry.dispatch

    def dispatch(name, args, **kwargs):
        try:
            return real_dispatch(name, args, **kwargs)
        finally:
            tc = trusted_context.current()
            found.after.append((threading.get_ident(), None if tc is None else tc.token))

    monkeypatch.setattr(registry, "dispatch", dispatch)
    yield found
    found.release.set()
    registry.deregister(PROBE)


def _agent(db, session_id: str, model: Model, *, platform: str = "tui", tools=(_PROBE_DEF,)):
    """A real AIAgent on *db* offering *tools*, whose provider is *model* (a loopback
    endpoint, so a metadata probe never leaves the host)."""
    from run_agent import AIAgent

    with (
        patch("model_tools.get_tool_definitions", return_value=list(tools)),
        patch("model_tools.check_toolset_requirements", return_value={}),
        patch("agent.process_bootstrap.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key", base_url="http://127.0.0.1:9/v1", model="test/model",
            quiet_mode=True, skip_context_files=True, skip_memory=True,
            session_db=db, session_id=session_id, platform=platform)
    agent._disable_streaming = True
    agent.client = MagicMock()
    agent.client.chat.completions.create.side_effect = model
    return agent


class Room:
    """One live session of the core, driven over a client socket ``ws``."""

    def __init__(self, db, sids: list, monkeypatch, model: Model, tools=(_PROBE_DEF,)):
        (Path(os.environ["HERMES_HOME"]) / "config.yaml").write_text(
            "auxiliary:\n  title_generation:\n    enabled: false\n", encoding="utf-8")
        self.ws, self.replies = WS(), []
        created = self.rpc("session.create", {"cols": 80, "source": "tui"})["result"]
        sids.append(created["session_id"])
        self.sid, self.key = created["session_id"], created["stored_session_id"]
        self.session = server._sessions[self.sid]
        self.model = model
        self.agent = _agent(db, self.key, model, tools=tools)
        self.session["agent"], self.session["agent_ready"] = self.agent, threading.Event()
        self.session["agent_ready"].set()
        monkeypatch.setattr(server, "_run_after_agent_ready", _REAL_RUN_AFTER_AGENT_READY)
        for name in _REAL_TURN_STUBS:
            monkeypatch.setattr(server, name, lambda *_a, **_k: None)

    def rpc(self, method: str, params: dict) -> dict:
        reply = _call(self.ws, method, params)
        self.replies.append(reply)
        return reply

    def submit(self, text: str, ticket: str | None = None, **params) -> dict:
        if ticket is not None:
            params["_levos_turn"] = ticket
        sent = {"session_id": self.sid, "text": text, **params}
        reply = self.rpc("prompt.submit", sent)
        assert "_levos_turn" not in sent  # taken off the params the core goes on reading
        return reply

    def settle(self) -> None:
        def idle() -> bool:
            thread = self.session.get("_run_thread")
            return not self.session.get("running") and (thread is None or not thread.is_alive())
        _await(lambda: idle() and idle(), "the turn thread to finish")

    def assert_turn_released(self) -> None:
        assert getattr(self.agent, "_current_trusted_context", None) is None
        assert getattr(self.agent, "_pending_trusted_context", None) is None
        assert trusted_context._by_turn == {}


def _store_dump(dsn: str) -> str:
    """Every row of every table of the store, as text."""
    with psycopg.connect(dsn, autocommit=True) as raw:
        tables = raw.execute(
            "SELECT schemaname, tablename FROM pg_tables "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')").fetchall()
        return "\n".join(
            f"{schema}.{table}: " + repr(raw.execute(sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table))).fetchall())
            for schema, table in tables)


def _home_dump() -> str:
    """Every file under HERMES_HOME (transcripts, logs, state files), as text."""
    root = Path(os.environ["HERMES_HOME"])
    return "\n".join(
        path.read_bytes().decode("utf-8", errors="replace") for path in root.rglob("*") if path.is_file())


@pytest.fixture
def all_logs(caplog):
    """Every logger at DEBUG into caplog, non-propagating ones included."""
    loggers = [logging.getLogger()] + [
        logger for logger in logging.root.manager.loggerDict.values() if isinstance(logger, logging.Logger)]
    saved = [(logger, logger.level) for logger in loggers]
    attached = [logger for logger in loggers if not logger.propagate]
    for logger in loggers:
        logger.setLevel(logging.DEBUG)
    for logger in attached:
        logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.DEBUG)
    yield caplog
    for logger in attached:
        logger.removeHandler(caplog.handler)
    for logger, level in saved:
        logger.setLevel(level)


def _log_text(caplog) -> str:
    # caplog.text is formatted when each record was emitted, as a real log handler writes it;
    # getMessage() re-renders args later (a params dict logged before the pop would no
    # longer show the ticket).
    return caplog.text + "\n" + "\n".join(
        f"{record.getMessage()} {record.exc_text or ''} {record.args!r}" for record in caplog.records)


# C-10 / C-11 — a ticketed turn: its handlers read it, sequential and parallel; nothing else holds it
def test_ticket_reaches_only_the_turns_tool_handlers(authority, postgres_dsn, probe, all_logs, monkeypatch):
    db, _turns, sids = authority
    ticket, msg = _ticket(), uuid.uuid4().hex
    model = Model([(PROBE, {"label": "seq"})])
    room = Room(db, sids, monkeypatch, model)

    reply = room.submit("look it up", ticket, client_msg_id=msg)
    assert reply["result"]["status"] == "streaming", reply
    room.settle()
    assert probe.tokens("seq") == [ticket]

    probe.together = threading.Barrier(2)
    model.calls = [(PROBE, {"label": "par-1"}), (PROBE, {"label": "par-2"})]
    assert room.submit("look up two", ticket)["result"]["status"] == "streaming"
    room.settle()
    parallel = [(thread, token) for label, thread, token in probe.seen if label.startswith("par")]
    assert [token for _thread, token in parallel] == [ticket, ticket]
    assert len({thread for thread, _token in parallel}) == 2
    # After each dispatch returned, on the thread that ran it, nothing is bound.
    assert len(probe.after) == 3 and all(token is None for _thread, token in probe.after)
    room.assert_turn_released()

    # The store really holds the turn (so the dumps below are not vacuous) ...
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        assert raw.execute("SELECT count(*) FROM core_submit_accepts WHERE client_msg_id = %s",
                           (msg,)).fetchone()[0] == 1
    assert [m["content"] for m in db.get_messages(room.key) if m["role"] == "user"][:1] == ["look it up"]
    assert len(model.requests) == 4 and room.ws.frames and all_logs.records
    # ... and the ticket is nowhere in it, nor anywhere else the turn left a trace.
    traces = {
        "store": _store_dump(postgres_dsn),
        "home files": _home_dump(),
        "model requests": json.dumps([r["messages"] for r in model.requests], default=str),
        "system prompt": str(room.agent._cached_system_prompt),
        "client frames": json.dumps(room.ws.frames, default=str),
        "rpc replies": json.dumps(room.replies, default=str),
        "logs": _log_text(all_logs),
        "session": repr(room.session),
    }
    assert [name for name, text in traces.items() if ticket in text] == []


# C-11 — every way a turn ends leaves no ticket on the agent and none in the table
@pytest.mark.parametrize("ending", ["raised", "interrupted"])
def test_turn_end_releases_the_ticket(ending, authority, probe, monkeypatch):
    db, _turns, sids = authority
    ticket = _ticket()
    room = Room(db, sids, monkeypatch, Model([(PROBE, {"label": "end", "hold": ending == "interrupted"})]))
    if ending == "raised":
        real_run = room.agent.run_conversation

        def run_then_raise(message, **kwargs):
            real_run(message, **kwargs)
            assert trusted_context._by_turn  # registered while the turn ran
            raise RuntimeError("turn failed after its tools ran")

        room.agent.run_conversation = run_then_raise
    assert room.submit("go", ticket)["result"]["status"] == "streaming"
    if ending == "interrupted":
        assert probe.entered.wait(15)
        assert room.rpc("session.interrupt", {"session_id": room.sid})["result"]["status"] == "interrupted"
        probe.release.set()
    room.settle()
    assert probe.tokens("end") == [ticket]
    room.assert_turn_released()


# C-12 — a delegate child of the ticketed turn reads nothing
def test_delegate_child_does_not_inherit_the_ticket(authority, probe, monkeypatch):
    import tools.delegate_tool as delegate_tool

    db, _turns, sids = authority
    ticket = _ticket()
    delegate_def = {"type": "function", "function": registry.get_schema("delegate_task")}
    room = Room(db, sids, monkeypatch, Model([("delegate_task", {"goal": "look it up"})]),
                tools=(_PROBE_DEF, delegate_def))
    child_model = Model([(PROBE, {"label": "child"})])
    real_build = delegate_tool._build_child_agent

    def build_child(**kwargs):
        child = real_build(**kwargs)
        child.client = MagicMock()
        child.client.chat.completions.create.side_effect = child_model
        child._disable_streaming = True
        child.tools, child.valid_tool_names = [_PROBE_DEF], {PROBE}
        return child

    monkeypatch.setattr(delegate_tool, "_build_child_agent", build_child)
    assert room.submit("delegate it", ticket)["result"]["status"] == "streaming"
    room.settle()
    assert child_model.requests, "the child never ran"
    # The child's model stand-in may be re-asked and call the probe more than once; every
    # call must read nothing.
    assert probe.tokens("child") and set(probe.tokens("child")) == {None}
    room.assert_turn_released()


# C-12 / busy path — a ticket sent to a busy session is dropped; the queued turn reads nothing
def test_queued_turn_behind_a_ticketed_turn_reads_nothing(authority, probe, all_logs, monkeypatch):
    db, _turns, sids = authority
    first, queued = _ticket(), _ticket()
    room = Room(db, sids, monkeypatch, Model([(PROBE, {"label": "first", "hold": True})]))
    assert room.submit("first", first)["result"]["status"] == "streaming"
    assert probe.entered.wait(15)
    room.model.calls = [(PROBE, {"label": "queued"})]
    reply = room.submit("second", queued, queued=True)
    assert reply["result"]["status"] == "queued", reply
    probe.release.set()
    _await(lambda: probe.tokens("queued"), "the queued turn's tool call")
    room.settle()
    assert probe.tokens("first") == [first]
    assert probe.tokens("queued") == [None]
    room.assert_turn_released()
    assert queued not in _log_text(all_logs) + json.dumps(room.ws.frames, default=str) + repr(room.session)


# C-12 — an auto-continue turn and a cron-style agent turn read nothing
def test_background_and_cron_turns_read_nothing(authority, probe, monkeypatch):
    db, _turns, sids = authority
    ticket = _ticket()
    room = Room(db, sids, monkeypatch, Model([(PROBE, {"label": "user", "hold": True})]))
    cron_model = Model([(PROBE, {"label": "cron"})])
    cron_agent = _agent(db, f"cron_{uuid.uuid4().hex[:8]}", cron_model, platform="cron")

    def cron_turn_meanwhile():
        # The ticketed turn is inside its tool now: its ticket is registered.
        assert trusted_context._by_turn
        cron = threading.Thread(target=lambda: cron_agent.run_conversation("cron job", task_id="cron-task"))
        cron.start()
        cron.join(20)

    probe.during_hold = cron_turn_meanwhile
    assert room.submit("user turn", ticket)["result"]["status"] == "streaming"
    assert probe.entered.wait(15)
    probe.release.set()
    room.settle()
    assert probe.tokens("user") == [ticket]
    assert probe.tokens("cron") == [None]

    room.model.calls = [(PROBE, {"label": "auto"})]
    with room.session["history_lock"]:
        room.session["running"] = True
    server._run_prompt_submit("ac", room.sid, room.session, "continue", display_kind="auto_continue")
    _await(lambda: probe.tokens("auto"), "the auto-continue turn's tool call")
    room.settle()
    assert probe.tokens("auto") == [None]
    room.assert_turn_released()


# C-13 — no key: nothing is ever bound; a compute-host turn drops the ticket
def test_submit_without_the_key_reads_nothing(authority, probe, monkeypatch):
    db, _turns, sids = authority
    room = Room(db, sids, monkeypatch, Model([(PROBE, {"label": "plain"})]))
    assert room.submit("no ticket")["result"]["status"] == "streaming"
    room.settle()
    assert probe.tokens("plain") == [None]
    room.assert_turn_released()

    for bad in ("", "   ", 42, {"token": "x"}):
        room.model.calls = [(PROBE, {"label": f"bad-{bad!r}"})]
        assert room.submit("odd ticket", bad)["result"]["status"] == "streaming"
        room.settle()
        assert probe.tokens(f"bad-{bad!r}") == [None]


# C-10 — a refused submit (unknown session, a client-forged author) took the ticket off its params too
def test_refused_submit_takes_the_ticket_off_its_params(authority, all_logs, monkeypatch):
    db, _turns, sids = authority
    room = Room(db, sids, monkeypatch, Model([]))
    for sid, extra in (("no-such-session", {}), (room.sid, {"_turn_author": {"id": "someone"}})):
        ticket = _ticket()
        sent = {"session_id": sid, "text": "hello", "_levos_turn": ticket, **extra}
        reply = room.rpc("prompt.submit", sent)
        assert "error" in reply, reply
        assert "_levos_turn" not in sent
        assert ticket not in json.dumps(reply, default=str) + _log_text(all_logs)
    assert trusted_context._by_turn == {}


def test_compute_host_turn_drops_the_ticket(authority, monkeypatch):
    db, _turns, sids = authority
    ticket = _ticket()
    room = Room(db, sids, monkeypatch, Model([]))
    dispatched: list = []

    def to_compute_host(rid, sid, session, text, **kwargs):
        dispatched.append(repr((rid, sid, text, kwargs, session)))
        with session["history_lock"]:
            session["running"] = False
        return {"jsonrpc": "2.0", "id": rid, "result": {"status": "streaming", "turn_isolation": True}}

    monkeypatch.setattr(server, "_session_uses_compute_host", lambda *_a, **_k: True)
    monkeypatch.setattr(server, "_submit_prompt_to_compute_host", to_compute_host)
    assert room.submit("isolated", ticket)["result"]["status"] == "streaming"
    assert len(dispatched) == 1 and "isolated" in dispatched[0]
    assert ticket not in dispatched[0]
    assert trusted_context._by_turn == {}
    assert getattr(room.agent, "_pending_trusted_context", None) is None
