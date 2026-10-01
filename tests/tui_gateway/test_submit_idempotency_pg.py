"""levos v3 (t_2b6c09df) — client request ids on ``prompt.submit`` / ``session.create``.

On a PostgreSQL-authority profile a ``client_msg_id`` re-send must never become a
second user turn, a message whose owner died before storing it runs exactly once
more, and a ``client_create_id`` re-send answers with the same session ids even
from a process that never saw the first request. Off authority both ids are
accepted and ignored.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors, never
skips). The agent turn is a stand-in that stores the user row the way the
agent's session flush does; every record, lock and message lives in PostgreSQL.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn
from tui_gateway import server
from tui_gateway import submit_idempotency as idem

UNREACHABLE_DSN = "postgresql://nobody@/postgres?host=/nonexistent-t2b6c09df&connect_timeout=1"
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
)


_REAL_RUN_AFTER_AGENT_READY = server._run_after_agent_ready


class FakeTurns:
    """``_run_after_agent_ready`` stand-in: stores the user row (the agent's flush) — or,
    for a resumed turn, only the reply to the stored one — optionally hands ``receipt``
    to the turn's result callback and runs ``after_receipt``, optionally holds the turn
    open on ``gate``, then ends it like the real thread. No ``receipt``: the turn reports
    no result (a core that never calls the callback)."""

    def __init__(self, db):
        self.db = db
        self.texts: list = []
        self.stored_rows: list = []
        self.gate: threading.Event | None = None
        self.persist = True
        self.receipt: dict | None = None
        self.after_receipt = None
        self._lock = threading.Lock()

    def __call__(self, rid, sid, session, text, display_kind, callback, turn_author=None,
                 stored_user_row=None, result_callback=None):
        with self._lock:
            self.texts.append(text)
            self.stored_rows.append(stored_user_row)
        if stored_user_row is not None:
            self.db.append_message(session["session_key"], "assistant", f"reply: {text}")
        elif self.persist:
            self.db.append_message(session["session_key"], "user", text)
        if self.receipt is not None and result_callback is not None:
            result_callback(**self.receipt)
        if self.after_receipt is not None:
            self.after_receipt(session)
        if self.gate is not None:
            assert self.gate.wait(20)
        with session["history_lock"]:
            session["running"] = False
            server._clear_inflight_turn(session)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    yield
    idem._live.clear()


def _quiet_gateway(monkeypatch, db) -> FakeTurns:
    turns = FakeTurns(db)
    monkeypatch.setattr(server, "_get_db", lambda: db)
    monkeypatch.setattr(server, "_schedule_agent_build", lambda _sid: None)
    monkeypatch.setattr(server, "_schedule_session_cap_enforcement", lambda: None)
    monkeypatch.setattr(server, "_register_session_cwd", lambda _session: None)
    monkeypatch.setattr(server, "_start_agent_build", lambda _sid, _session: None)
    monkeypatch.setattr(server, "_restart_completed_failed_agent_build", lambda *_a: False)
    monkeypatch.setattr(server, "_ensure_active_session_slot", lambda _sid, _session: None)
    monkeypatch.setattr(server, "_run_after_agent_ready", turns)
    return turns


@pytest.fixture
def authority(monkeypatch, postgres_dsn):
    """A PostgreSQL-authority profile with its core schema; the idempotency tables
    start absent, as on a profile that predates this change."""
    from hermes_state import SessionDB

    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_submit_accepts, core_submit_session_creates")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    db = SessionDB(read_only=False)
    assert db._is_postgres
    turns = _quiet_gateway(monkeypatch, db)
    sids: list = []
    yield db, turns, sids
    if turns.gate is not None:
        turns.gate.set()
    for sid in sids:
        server._sessions.pop(sid, None)
    db.close()


def _rpc(method: str, params: dict) -> dict:
    return server.handle_request({"id": method, "method": method, "params": params})


def _create(sids: list, **params) -> dict:
    resp = _rpc("session.create", {"cols": 80, "source": "tui", **params})
    assert "result" in resp, resp
    sids.append(resp["result"]["session_id"])
    return resp["result"]


def _submit(sid: str, text: str, **params) -> dict:
    return _rpc("prompt.submit", {"session_id": sid, "text": text, **params})


def _settle(sid: str) -> None:
    """Wait for the submit's turn thread (and, for a claimed id, its record update)."""
    thread = server._sessions[sid].get("_run_thread")
    if thread is not None:
        thread.join(20)
        assert not thread.is_alive()


def _user_rows(db, key: str) -> list:
    return [m["content"] for m in db.get_messages_as_conversation(key) if m.get("role") == "user"]


def _ids() -> str:
    return uuid.uuid4().hex


def _rows(db, key: str, role: str) -> list:
    return [m["content"] for m in db.get_messages_as_conversation(key) if m.get("role") == role]


def _kill_owner(postgres_dsn, key: str, msg: str) -> None:
    """The owning process dies: its lease lapses and nothing of it is live here."""
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(
            "UPDATE core_submit_accepts SET owner = 'dead-pod:1:gone', lease_until = 0 "
            "WHERE client_msg_id = %s", (msg,))
    idem._unregister_live((key, msg))


def _die_after_storing(turns, postgres_dsn, sid: str, key: str, msg: str, text: str, partial=None):
    """Submit *text*; its turn stores the user row (and *partial*, an assistant row the
    dead turn got out), then its process dies before the reply."""
    turns.gate = threading.Event()
    assert _submit(sid, text, client_msg_id=msg)["result"]["status"] == "streaming"
    if partial is not None:
        turns.db.append_message(key, "assistant", partial)
    _kill_owner(postgres_dsn, key, msg)
    turns.gate.set()
    _settle(sid)  # the dead owner's settle is fenced off: nobody completes the record
    turns.gate = None


# ① same id twice -> one user turn, the second answer is the recorded state
def test_resubmitted_client_msg_id_is_one_user_turn(authority):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()

    assert _rpc("prompt.accepted", {"client_msg_id": msg, "session_id": sid})["result"] == {
        "enabled": True, "submit": None, "found": False,
        "result_contract": {"version": idem.RESULT_CONTRACT, "retention_s": idem.RETENTION_SECONDS}}
    first = _submit(sid, "hello there", client_msg_id=msg)["result"]
    assert (first["status"], first["state"], first["stored_session_id"]) == ("streaming", "accepted", key)
    _settle(sid)

    again = _submit(sid, "hello there", client_msg_id=msg)["result"]
    assert again["status"] == "duplicate"
    assert (again["state"], again["running"]) == ("completed", False)
    assert turns.texts == ["hello there"]
    assert _user_rows(db, key) == ["hello there"]
    looked_up = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": key})["result"]
    assert looked_up["submit"]["user_message_id"] == again["user_message_id"] is not None


# ② same id, different text -> machine-readable conflict, no turn
def test_client_msg_id_reused_for_other_text_is_refused(authority):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    assert _submit(sid, "first text", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)

    refused = _submit(sid, "different text", client_msg_id=msg)
    assert refused["error"]["code"] == 4141
    assert refused["error"]["data"]["reason"] == "client_msg_id_conflict"
    assert turns.texts == ["first text"]
    assert _user_rows(db, key) == ["first text"]


# ③ the owner died after the record and before the user row -> the retry runs the turn once
def test_accepted_record_without_message_restarts_exactly_once(authority, postgres_dsn):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    # The record commits and then its process dies before the agent flushes the user row.
    turns.persist = False
    turns.gate = threading.Event()
    assert _submit(sid, "parked message", client_msg_id=msg)["result"]["status"] == "streaming"
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(
            "UPDATE core_submit_accepts SET owner = 'dead-pod:1:gone', lease_until = 0 "
            "WHERE client_msg_id = %s", (msg,))
    idem._unregister_live((key, msg))
    turns.gate.set()
    _settle(sid)  # the dead owner's settle is fenced off: the record stays accepted
    assert _user_rows(db, key) == []
    turns.gate, turns.persist = None, True

    restarted = _submit(sid, "parked message", client_msg_id=msg)["result"]
    assert (restarted["status"], restarted["attempts"]) == ("streaming", 2)
    _settle(sid)
    for _ in range(2):
        assert _submit(sid, "parked message", client_msg_id=msg)["result"]["status"] == "duplicate"
    assert turns.texts == ["parked message", "parked message"]
    assert _user_rows(db, key) == ["parked message"]


def test_live_foreign_owner_is_reported_running_not_restarted(authority, postgres_dsn):
    """An owner on another pod whose lease still runs owns the turn: no second one here."""
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    claim = idem.claim_submit(key, msg, idem.fingerprint("in flight elsewhere"))
    idem._unregister_live(claim.record.key)
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(
            "UPDATE core_submit_accepts SET owner = 'other-pod:7:alive', "
            "lease_until = EXTRACT(EPOCH FROM clock_timestamp()) + 60 WHERE client_msg_id = %s", (msg,))

    reply = _submit(sid, "in flight elsewhere", client_msg_id=msg)["result"]
    assert (reply["status"], reply["state"], reply["running"]) == ("duplicate", "accepted", True)
    assert turns.texts == []


def test_turn_that_stored_nothing_is_rerun_by_the_retry(authority):
    """A turn that ended in this process without its user row (agent init failed) leaves an
    ownerless ``accepted`` record: the retry runs it; a stored message is never rerun."""
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    turns.persist = False
    assert _submit(sid, "try me", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    state = _rpc("prompt.accepted", {"client_msg_id": msg, "session_id": sid})["result"]["submit"]
    assert (state["state"], state["running"]) == ("accepted", False)

    turns.persist = True
    assert _submit(sid, "try me", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    assert _submit(sid, "try me", client_msg_id=msg)["result"]["state"] == "completed"
    assert _user_rows(db, key) == ["try me"]


# ④ two concurrent requests with one id -> one turn
def test_concurrent_submits_of_one_id_start_one_turn(authority):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    turns.gate = threading.Event()
    start, replies = threading.Barrier(4), []

    def send():
        start.wait()
        replies.append(_submit(sid, "same words", client_msg_id=msg))

    senders = [threading.Thread(target=send) for _ in range(4)]
    for sender in senders:
        sender.start()
    for sender in senders:
        sender.join(30)
    statuses = sorted(r["result"]["status"] for r in replies)
    assert statuses == ["duplicate", "duplicate", "duplicate", "streaming"], replies
    # While the owner's turn runs, the recorded state already shows the stored row.
    live = _rpc("prompt.accepted", {"client_msg_id": msg, "session_id": sid})["result"]["submit"]
    assert (live["state"], live["running"]) == ("persisted", True)
    turns.gate.set()
    _settle(sid)
    assert turns.texts == ["same words"]
    assert _user_rows(db, key) == ["same words"]


def test_busy_session_refuses_a_new_id_without_recording_it(authority):
    """Steer/queue are in-memory, so a new id on a running session is refused (retryable)
    and nothing is recorded: the broker keeps the message and re-sends the same id."""
    db, turns, sids = authority
    created = _create(sids)
    sid, key = created["session_id"], created["stored_session_id"]
    turns.gate = threading.Event()
    assert _submit(sid, "long task", client_msg_id=_ids())["result"]["status"] == "streaming"
    parked = _ids()

    busy = _submit(sid, "while busy", client_msg_id=parked)
    assert (busy["error"]["code"], busy["error"]["data"]["reason"]) == (4143, "session_busy")
    assert _rpc("prompt.accepted", {"client_msg_id": parked, "session_id": sid})["result"]["found"] is False
    turns.gate.set()
    _settle(sid)
    turns.gate = None
    assert _submit(sid, "while busy", client_msg_id=parked)["result"]["status"] == "streaming"
    _settle(sid)
    assert _user_rows(db, key) == ["long task", "while busy"]


# ⑤ create twice -> the same session ids
def test_recreated_client_create_id_returns_the_same_session(authority):
    _db, _turns, sids = authority
    create_id = _ids()
    first = _create(sids, client_create_id=create_id)
    again = _create(sids, client_create_id=create_id)

    assert (first["idempotency"], again["idempotency"]) == ("created", "duplicate")
    assert again["session_id"] == first["session_id"]
    assert again["stored_session_id"] == first["stored_session_id"]
    looked_up = _rpc("prompt.accepted", {"client_create_id": create_id})["result"]["create"]
    assert (looked_up["session_id"], looked_up["stored_session_id"], looked_up["live"]) == (
        first["session_id"], first["stored_session_id"], True)


# ⑥ a new server process (pod replacement) -> the same ids are usable
def test_create_replayed_by_a_new_process_reuses_the_ids(authority, monkeypatch):
    db, turns, sids = authority
    create_id = _ids()
    draft = _create(sids, client_create_id=create_id)
    server._sessions.clear()  # the pod is replaced before the first turn: no row, no memory
    monkeypatch.setattr(idem, "OWNER", "replacement-pod:1:new")

    again = _create(sids, client_create_id=create_id)
    assert again["idempotency"] == "recreated"
    assert (again["session_id"], again["stored_session_id"]) == (draft["session_id"], draft["stored_session_id"])
    assert server._sessions[again["session_id"]]["session_key"] == draft["stored_session_id"]
    assert _submit(again["session_id"], "first words", client_msg_id=_ids())["result"]["status"] == "streaming"
    _settle(again["session_id"])
    assert _user_rows(db, draft["stored_session_id"]) == ["first words"]

    server._sessions.clear()  # replaced again, now with a stored row
    resumed = _create(sids, client_create_id=create_id)
    assert resumed["idempotency"] == "resumed"
    assert (resumed["session_id"], resumed["stored_session_id"]) == (draft["session_id"], draft["stored_session_id"])
    assert [m["text"] for m in resumed["messages"] if m["role"] == "user"] == ["first words"]
    assert turns.texts == ["first words"]


# ⑦ not on authority -> the ids are accepted and ignored
def test_ids_are_ignored_off_authority(monkeypatch, tmp_path, postgres_dsn):
    from hermes_state import SessionDB

    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_submit_accepts, core_submit_session_creates")
    db = SessionDB(db_path=tmp_path / "state.db")
    turns = _quiet_gateway(monkeypatch, db)
    sids: list = []
    try:
        create_id = _ids()
        first, again = _create(sids, client_create_id=create_id), _create(sids, client_create_id=create_id)
        plain = _create(sids)
        assert first["session_id"] != again["session_id"]
        assert first.keys() == again.keys() == plain.keys()

        sid, key = first["session_id"], first["stored_session_id"]
        replies = []
        for client_msg_id in ("same-id", "same-id", "x" * 500, 42, None):
            params = {} if client_msg_id is None else {"client_msg_id": client_msg_id}
            replies.append(_submit(sid, "hi", **params))
            _settle(sid)
        assert [r["result"] for r in replies] == [{"status": "streaming"}] * 5
        assert turns.texts == ["hi"] * 5
        assert _user_rows(db, key) == ["hi"] * 5
        assert _rpc("prompt.accepted", {"client_msg_id": "same-id"})["result"] == {
            "enabled": False, "found": False}
        with psycopg.connect(postgres_dsn, autocommit=True) as raw:
            assert raw.execute("SELECT to_regclass('core_submit_accepts')").fetchone()[0] is None
    finally:
        for sid in sids:
            server._sessions.pop(sid, None)
        db.close()


# ⑧ authority whose PostgreSQL cannot be reached -> AuxStoreUnavailable, no file, no turn
def test_unreachable_authority_store_refuses_without_files(monkeypatch):
    from hermes_aux_store import AuxStoreUnavailable

    home = Path(os.environ["HERMES_HOME"])
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    turns = _quiet_gateway(monkeypatch, None)
    sids: list = []
    try:
        sid = _create(sids)["session_id"]  # a draft without an id touches no store
        before = sorted(p for p in home.rglob("*") if p.is_file())  # after the home scaffold
        with pytest.raises(AuxStoreUnavailable):
            idem.open_store()
        live_before = set(server._sessions)
        created = _rpc("session.create", {"cols": 80, "client_create_id": _ids()})
        assert created["error"]["code"] == 5140
        assert created["error"]["data"]["exception"] == "AuxStoreUnavailable"
        assert set(server._sessions) == live_before  # no session without its record

        refused = _submit(sid, "hello", client_msg_id=_ids())
        assert (refused["error"]["code"], refused["error"]["data"]["reason"]) == (5140, "aux_store_unavailable")
        assert refused["error"]["data"]["exception"] == "AuxStoreUnavailable"
        assert server._sessions[sid]["running"] is False
        assert turns.texts == []
        lookup = _rpc("prompt.accepted", {"client_msg_id": "any"})
        assert lookup["error"]["code"] == 5140
        assert sorted(p for p in home.rglob("*") if p.is_file()) == before
    finally:
        for sid in sids:
            server._sessions.pop(sid, None)


# ⑨ persisted + owner died + no reply -> the retry answers the stored message (t_7ceb9994)
def test_stored_message_whose_owner_died_is_resumed_once(authority, postgres_dsn):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    _die_after_storing(turns, postgres_dsn, sid, key, msg, "answer me")
    state = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": key})["result"]["submit"]
    assert (state["state"], state["running"], state["needs_attention"]) == ("persisted", False, "no_reply")

    resumed = _submit(sid, "answer me", client_msg_id=msg)["result"]
    assert (resumed["status"], resumed["attempts"], resumed["running"]) == ("resumed", 2, True)
    assert resumed["user_message_id"] == state["user_message_id"]
    _settle(sid)
    again = _submit(sid, "answer me", client_msg_id=msg)["result"]
    assert (again["status"], again["state"], again["running"]) == ("duplicate", "completed", False)
    assert turns.stored_rows == [None, state["user_message_id"]]
    assert _rows(db, key, "user") == ["answer me"]
    assert _rows(db, key, "assistant") == ["reply: answer me"]


# ⑩ the dead turn already got a (partial) reply or tool call out -> never re-run
def test_stored_message_with_a_partial_reply_is_not_rerun(authority, postgres_dsn):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    _die_after_storing(turns, postgres_dsn, sid, key, msg, "delete the tmp dir", partial="Deleting now…")

    for _ in range(2):
        reply = _submit(sid, "delete the tmp dir", client_msg_id=msg)["result"]
        assert (reply["status"], reply["state"], reply["running"]) == ("duplicate", "persisted", False)
        assert (reply["needs_attention"], reply["attempts"]) == ("partial_reply", 1)
    assert turns.texts == ["delete the tmp dir"]
    assert _rows(db, key, "user") == ["delete the tmp dir"]
    assert _rows(db, key, "assistant") == ["Deleting now…"]


# ⑪ concurrent retries of a dead stored turn -> one resumed turn
def test_concurrent_retries_resume_a_dead_turn_once(authority, postgres_dsn):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    _die_after_storing(turns, postgres_dsn, sid, key, msg, "still there?")
    turns.gate = threading.Event()
    start, replies = threading.Barrier(4), []

    def send():
        start.wait()
        replies.append(_submit(sid, "still there?", client_msg_id=msg)["result"])

    senders = [threading.Thread(target=send) for _ in range(4)]
    for sender in senders:
        sender.start()
    for sender in senders:
        sender.join(30)
    assert sorted(r["status"] for r in replies) == ["duplicate", "duplicate", "duplicate", "resumed"], replies
    assert all(r["running"] for r in replies)
    turns.gate.set()
    _settle(sid)
    assert len([row for row in turns.stored_rows if row is not None]) == 1
    assert _rows(db, key, "user") == ["still there?"]
    assert _rows(db, key, "assistant") == ["reply: still there?"]


def _model_agent(db, session_key: str, answer: str):
    """A real AIAgent on the profile's store whose model is a stand-in recording each request."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    from run_agent import AIAgent

    with (
        patch("model_tools.get_tool_definitions", return_value=[]),
        patch("model_tools.check_toolset_requirements", return_value={}),
        patch("agent.process_bootstrap.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key", base_url="https://openrouter.ai/api/v1", model="test/model",
            quiet_mode=True, skip_context_files=True, skip_memory=True,
            session_db=db, session_id=session_key, platform="tui")
    agent._disable_streaming = True
    agent.client = MagicMock()
    agent.client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(
            message=SimpleNamespace(content=answer, tool_calls=None), finish_reason="stop")],
        model="test/model",
        usage=SimpleNamespace(prompt_tokens=5, completion_tokens=3, total_tokens=8))
    return agent


# ⑫ the resumed turn runs the real core: the model gets the stored message exactly once
def test_resumed_turn_sends_the_stored_message_to_the_model_once(authority, postgres_dsn, monkeypatch):
    db, turns, sids = authority
    create_id = _ids()
    created = _create(sids, client_create_id=create_id)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    assert _submit(sid, "earlier question", client_msg_id=_ids())["result"]["status"] == "streaming"
    _settle(sid)
    db.append_message(key, "assistant", "earlier answer")
    _die_after_storing(turns, postgres_dsn, sid, key, msg, "what is next?")

    server._sessions.clear()  # the pod is replaced
    monkeypatch.setattr(idem, "OWNER", "replacement-pod:1:new")
    resumed_session = _create(sids, client_create_id=create_id)
    assert resumed_session["idempotency"] == "resumed"
    session = server._sessions[sid]
    agent = _model_agent(db, key, "the answer")
    session["agent"], session["agent_ready"] = agent, threading.Event()
    session["agent_ready"].set()
    monkeypatch.setattr(server, "_run_after_agent_ready", _REAL_RUN_AFTER_AGENT_READY)
    for name in ("_wire_callbacks", "_sync_agent_model_with_config", "_sync_agent_compression_with_config",
                 "_apply_pending_model_switch", "_sync_bot_capabilities", "_emit_settled_session_info"):
        monkeypatch.setattr(server, name, lambda *_a, **_k: None)

    reply = _submit(sid, "what is next?", client_msg_id=msg)["result"]
    assert reply["status"] == "resumed"
    _settle(sid)

    sent = agent.client.chat.completions.create.call_args.kwargs["messages"]
    conversation = [(m["role"], m["content"]) for m in sent if m["role"] != "system"]
    assert conversation == [
        ("user", "earlier question"), ("assistant", "earlier answer"), ("user", "what is next?")]
    assert _rows(db, key, "user") == ["earlier question", "what is next?"]
    assert _rows(db, key, "assistant") == ["earlier answer", "the answer"]
    final = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": key})["result"]["submit"]
    assert (final["state"], final["running"], final["attempts"]) == ("completed", False, 2)


# ── turn result receipt (prompt.accepted result contract v1, t_b1dcb36c) ──────────────
# The owned turn records its outcome and its message.complete text on the record before
# the frame goes out, so a broker that lost the frame restores the answer from
# prompt.accepted without running the turn again.

_REAL_TURN_STUBS = ("_wire_callbacks", "_sync_agent_model_with_config", "_sync_agent_compression_with_config",
                    "_apply_pending_model_switch", "_sync_bot_capabilities", "_emit_settled_session_info")


def _accepted(msg: str, key: str = "", **params) -> dict:
    scope = {"stored_session_id": key} if key else {}
    return _rpc("prompt.accepted", {"client_msg_id": msg, **scope, **params})["result"]


def _show(name: str, response: dict) -> None:
    """The response as docs/levos-v3-pg-authority-port.md quotes it (``pytest -s``)."""
    print(f"D1B-RESPONSE {name}: {json.dumps(response, ensure_ascii=False)}")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _real_turns(monkeypatch, db, sid: str, key: str, *, answer: str = "the answer", run=None):
    """Turns of *sid* run on the real turn thread and a real AIAgent; ``run(agent, message,
    kwargs)`` stands in for ``run_conversation`` when given."""
    session = server._sessions[sid]
    agent = _model_agent(db, key, answer)
    if run is not None:
        agent.run_conversation = lambda message, **kwargs: run(agent, message, kwargs)
    session["agent"], session["agent_ready"] = agent, threading.Event()
    session["agent_ready"].set()
    monkeypatch.setattr(server, "_run_after_agent_ready", _REAL_RUN_AFTER_AGENT_READY)
    for name in _REAL_TURN_STUBS:
        monkeypatch.setattr(server, name, lambda *_a, **_k: None)
    return agent


def _watch_complete(monkeypatch, postgres_dsn, msg: str) -> list:
    """Each message.complete payload with the record's result as committed when it went out."""
    seen: list = []
    real_emit = server._emit

    def emit(event, sid, payload=None):
        if event == "message.complete":
            with psycopg.connect(postgres_dsn, autocommit=True) as raw:
                seen.append((payload, raw.execute(
                    "SELECT state, outcome, text_kind, final_text FROM core_submit_accepts "
                    "WHERE client_msg_id = %s", (msg,)).fetchone()))
        return real_emit(event, sid, payload)

    monkeypatch.setattr(server, "_emit", emit)
    return seen


def _assistant_rows(postgres_dsn, key: str) -> list:
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        return raw.execute(
            "SELECT id, content FROM messages WHERE session_id = %s AND role = 'assistant' ORDER BY id",
            (key,)).fetchall()


# T1 — a completed turn: every column, the wire shape, text only on request and in scope
def test_completed_turn_result_is_restored_from_prompt_accepted(authority, postgres_dsn, monkeypatch):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    _real_turns(monkeypatch, db, sid, key, answer="The answer is 42.")
    seen = _watch_complete(monkeypatch, postgres_dsn, msg)
    assert _submit(sid, "what is the answer?", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)

    [(frame, at_emit)] = seen
    [(reply_id, reply_text)] = _assistant_rows(postgres_dsn, key)
    response = _accepted(msg, key, include_result=True)
    _show("complete", response)
    assert response["found"] is True
    assert response["result_contract"] == {"version": 1, "retention_s": idem.RETENTION_SECONDS}
    submit = response["submit"]
    assert (submit["state"], submit["running"], submit["needs_attention"]) == ("completed", False, None)
    assert submit["fingerprint"] == idem.fingerprint("what is the answer?")
    assert (submit["lineage_root"], submit["result_contract"]) == (key, idem.RESULT_CONTRACT)
    result = submit["result"]
    assert result["text"] == frame["text"] == reply_text  # the frame's text, byte for byte
    assert {k: result[k] for k in ("contract", "attempt", "outcome", "text_kind", "text_sha256", "text_chars",
                                    "error", "result_session_id", "assistant_message_id", "history_ref_kind",
                                    "history_persisted", "history_text_match")} == {
        "contract": 1, "attempt": 1, "outcome": "complete", "text_kind": "text",
        "text_sha256": _sha(frame["text"]), "text_chars": len(frame["text"]), "error": None,
        "result_session_id": key, "assistant_message_id": reply_id, "history_ref_kind": "exact",
        "history_persisted": True, "history_text_match": True}
    assert result["expires_at"] >= result["recorded_at"] + idem.RETENTION_SECONDS
    # The result was committed before the frame went out (and before the record completed).
    assert at_emit == ("persisted", "complete", "text", frame["text"])

    meta = _accepted(msg, key)["submit"]["result"]
    assert "text" not in meta and meta == {k: v for k, v in result.items() if k != "text"}
    assert _accepted(msg, include_result=True, session_id=sid)["submit"]["result"]["text"] == frame["text"]
    unscoped = _accepted(msg, include_result=True)["submit"]
    assert "result" not in unscoped and unscoped["result_contract"] == idem.RESULT_CONTRACT
    assert turns.texts == []  # the lookups ran no turn (the real turn ran once, above)
    assert _rows(db, key, "user") == ["what is the answer?"]


def _returned(case: str, db, key: str):
    """``run_conversation`` stand-ins that store rows like the agent's flush and return *case*."""
    def run(agent, message, kwargs):
        user = {"role": "user", "content": message, "_row_id": db.append_message(key, "user", message)}
        if case == "error":
            return {"final_response": "", "error": "provider exploded", "failed": True, "messages": [user]}
        if case == "interrupted":
            row = db.append_message(key, "assistant", "half an answer")
            return {"final_response": "half an answer", "interrupted": True, "messages": [
                user, {"role": "assistant", "content": "half an answer", "_row_id": row}]}
        row = db.append_message(key, "assistant", "")
        return {"final_response": "", "messages": [user, {"role": "assistant", "content": "", "_row_id": row}]}
    return run


# T2 — returned turns: outcome = the frame's status, text = the frame's text
@pytest.mark.parametrize("case, outcome, kind, text, error, persisted", [
    ("error", "error", "text", "Error: provider exploded", "provider exploded", False),
    ("interrupted", "interrupted", "text", "half an answer", None, True),
    ("empty", "complete", "empty", "", None, True),
])
def test_returned_turn_outcome_is_recorded_as_its_frame(
        case, outcome, kind, text, error, persisted, authority, postgres_dsn, monkeypatch):
    db, _turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    _real_turns(monkeypatch, db, sid, key, run=_returned(case, db, key))
    seen = _watch_complete(monkeypatch, postgres_dsn, msg)
    assert _submit(sid, f"{case} please", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)

    [(frame, at_emit)] = seen
    response = _accepted(msg, key, include_result=True)
    _show(case, response)
    submit, result = response["submit"], response["submit"]["result"]
    assert submit["state"] == "completed"
    assert (frame["status"], frame["text"]) == (outcome, text)
    assert (result["outcome"], result["text_kind"], result["text"], result["error"]) == (outcome, kind, text, error)
    assert (result["text_sha256"], result["text_chars"]) == (_sha(text), len(text))
    assert result["history_persisted"] is persisted
    assert result["history_ref_kind"] == ("exact" if persisted else "none")
    assert at_emit[1:3] == (outcome, kind)


def _refuse_context(monkeypatch):
    import agent.context_references as context_references
    import agent.model_metadata as model_metadata

    monkeypatch.setattr(model_metadata, "get_model_context_length", lambda *_a, **_k: 100_000)
    monkeypatch.setattr(context_references, "preprocess_context_references", lambda prompt, **_k: SimpleNamespace(
        blocked=True, warnings=["refused"], message=prompt))


def _raise_in_turn(db, key):
    def run(agent, message, kwargs):
        db.append_message(key, "user", message)
        raise RuntimeError("kaboom")
    return run


# T2 — turns that end without frame text: text_kind "none", the cause in ``error``
@pytest.mark.parametrize("case, error, state", [
    ("exception", "kaboom", "completed"),
    ("context_refused", "Context injection refused.", "accepted"),
    ("agent_init_failed", "agent_init_failed", "accepted"),
    ("cancelled_before_ready", "cancelled_before_ready", "accepted"),
])
def test_turn_without_frame_text_records_why(case, error, state, authority, postgres_dsn, monkeypatch):
    db, _turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    _real_turns(monkeypatch, db, sid, key, run=_raise_in_turn(db, key) if case == "exception" else None)
    if case == "context_refused":
        _refuse_context(monkeypatch)
    elif case == "agent_init_failed":
        monkeypatch.setattr(server, "_wait_agent_for_prompt", lambda *_a: {"error": {"message": "build failed"}})
    elif case == "cancelled_before_ready":
        monkeypatch.setattr(server, "_wait_agent_for_prompt",
                            lambda session, *_a: session.__setitem__("_turn_cancel_requested", True))
    seen = _watch_complete(monkeypatch, postgres_dsn, msg)
    assert _submit(sid, "read @file:secret.txt", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)

    submit = _accepted(msg, key, include_result=True)["submit"]
    result = submit["result"]
    assert (submit["state"], submit["running"]) == (state, False)
    assert (result["outcome"], result["text_kind"], result["text"], result["error"]) == ("error", "none", None, error)
    assert (result["text_sha256"], result["text_chars"], result["history_persisted"]) == (None, None, False)
    for _frame, at_emit in seen:  # an error frame, if any, went out after the result was committed
        assert at_emit[1:3] == ("error", "none")


# T2 — the turn reported nothing: the settle records ``missing``; the completed state is unchanged
def test_turn_without_a_receipt_is_missing(authority):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    assert _submit(sid, "no receipt", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)

    response = _accepted(msg, key, include_result=True)
    _show("missing", response)
    submit, result = response["submit"], response["submit"]["result"]
    assert (submit["state"], submit["needs_attention"], submit["result_contract"]) == ("completed", None, 1)
    assert (result["outcome"], result["text_kind"], result["text"], result["error"]) == (
        "missing", "none", None, "no_receipt")
    assert (result["history_ref_kind"], result["history_persisted"]) == ("none", False)
    assert turns.texts == ["no receipt"]


# T3 — history_persisted comes from the stored rows: none / exact / inferred
def test_history_reference_is_read_from_the_messages_table(authority, postgres_dsn):
    db, turns, sids = authority
    created = _create(sids)
    sid, key = created["session_id"], created["stored_session_id"]

    def turn(receipt, after_receipt=None) -> dict:
        msg = _ids()
        turns.receipt, turns.after_receipt = receipt, after_receipt
        assert _submit(sid, f"turn {msg}", client_msg_id=msg)["result"]["status"] == "streaming"
        _settle(sid)
        submit = _accepted(msg, key)["submit"]
        assert submit["state"] == "completed"  # the record's state never depends on the reply row
        return submit["result"]

    # No reply row: the result stands, the history holds no answer.
    result = turn({"outcome": "complete", "text": "nobody stored me", "error": None})
    assert (result["history_ref_kind"], result["history_persisted"], result["assistant_message_id"]) == (
        "none", False, None)
    # An id the agent reported that the table does not hold.
    result = turn({"outcome": "complete", "text": "lost row", "error": None, "assistant_row_id": 987654321})
    assert (result["history_ref_kind"], result["history_persisted"], result["assistant_message_id"]) == (
        "exact", False, 987654321)
    assert result["history_text_match"] is None
    # The reply row lands after the result: the settle looks the history up once more.
    late = {}
    result = turn({"outcome": "complete", "text": "late words", "error": None},
                  lambda session: late.setdefault("id", db.append_message(key, "assistant", "late words")))
    assert (result["history_ref_kind"], result["history_persisted"], result["history_text_match"]) == (
        "inferred", True, True)
    assert result["assistant_message_id"] == late["id"]
    # A stored reply that is not the frame's text (a footer, a hook): persisted, no text match.
    result = turn({"outcome": "complete", "text": "frame words", "error": None},
                  lambda session: db.append_message(key, "assistant", "stored words"))
    assert (result["history_persisted"], result["history_text_match"]) == (True, False)
    turns.receipt = turns.after_receipt = None


# T4 — compression: kept under the tip, found by the root key (R0-2 reversed)
def test_result_kept_under_the_tip_is_found_by_the_root_key(authority):
    db, turns, sids = authority
    root, tip, msg = "d1broot_" + uuid.uuid4().hex[:8], "d1btip_" + uuid.uuid4().hex[:8], _ids()
    db.create_session(root, "tui")
    db.append_message(root, "user", "before compression")
    db.end_session(root, "compression")
    db.create_session(tip, "tui", parent_session_id=root)
    claim = idem.claim_submit(tip, msg, idem.fingerprint("after compression"))
    db.append_message(tip, "user", "after compression")
    reply = db.append_message(tip, "assistant", "compressed reply")
    assert idem.record_result(claim, {"outcome": "complete", "text": "compressed reply", "error": None,
                                      "result_session_id": tip, "assistant_row_id": reply})
    idem.finish_submit(claim)

    by_root = _accepted(msg, root, include_result=True)
    _show("compression_lineage", by_root)
    submit = by_root["submit"]
    assert by_root["found"] is True
    assert (submit["stored_session_id"], submit["lineage_root"], submit["state"]) == (tip, root, "completed")
    assert (submit["result"]["text"], submit["result"]["result_session_id"]) == ("compressed reply", tip)
    assert (submit["result"]["history_ref_kind"], submit["result"]["history_persisted"]) == ("exact", True)
    assert _accepted(msg, tip, include_result=True)["submit"] == submit
    # A re-send under the root key finds the same record: no second turn.
    again = idem.claim_submit(root, msg, idem.fingerprint("after compression"))
    assert (again.outcome, again.record.session_key) == (idem.DUPLICATE, tip)
    assert turns.texts == []


def _owned(db, text: str):
    key, msg = "d1b_" + uuid.uuid4().hex[:10], _ids()
    db.create_session(key, "tui")
    claim = idem.claim_submit(key, msg, idem.fingerprint(text))
    db.append_message(key, "user", text)
    return claim, key, msg


# T5 — the inline cap is UTF-8 bytes; a text PostgreSQL cannot hold is "unsupported"
@pytest.mark.parametrize("name, text, kind, inline", [
    ("over_cap", "가" * (idem.RESULT_TEXT_MAX // 3 + 1), "omitted", False),
    ("not_a_string", {"parts": ["not", "text"]}, "unsupported", False),
    ("nul", "before\x00after", "unsupported", False),
    ("whitespace", "  \n\t ", "text", True),
])
def test_final_text_kinds(name, text, kind, inline, authority):
    db, _turns, _sids = authority
    claim, key, msg = _owned(db, f"ask {name}")
    assert idem.record_result(claim, {"outcome": "complete", "text": text, "error": None})
    idem.finish_submit(claim)
    result = idem.lookup_submit(msg, key, with_text=True).payload(include_text=True)["result"]
    assert (result["outcome"], result["text_kind"]) == ("complete", kind)
    assert result["text"] == (text if inline else None)
    if isinstance(text, str):
        assert (result["text_sha256"], result["text_chars"]) == (_sha(text), len(text))
    else:
        assert (result["text_sha256"], result["text_chars"]) == (None, None)
    if name == "over_cap":
        assert len(text) < idem.RESULT_TEXT_MAX < len(text.encode("utf-8"))


# T6 — a failed result write never changes the turn: the frame goes out, the settle records it
def test_result_store_failure_falls_back_at_settle(authority, postgres_dsn, monkeypatch):
    db, _turns, sids = authority
    created = _create(sids)
    sid, key = created["session_id"], created["stored_session_id"]
    _real_turns(monkeypatch, db, sid, key, answer="still delivered")
    real_store, failures = idem._store_result, []

    def fail_once(conn, record, receipt, now):
        if not failures:
            failures.append(receipt["outcome"])
            raise RuntimeError("injected result store failure")
        return real_store(conn, record, receipt, now)

    monkeypatch.setattr(idem, "_store_result", fail_once)
    msg = _ids()
    seen = _watch_complete(monkeypatch, postgres_dsn, msg)
    assert _submit(sid, "deliver it", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    [(frame, at_emit)] = seen
    assert (frame["status"], frame["text"], at_emit[1]) == ("complete", "still delivered", None)
    submit = _accepted(msg, key, include_result=True)["submit"]
    assert (submit["state"], submit["result"]["outcome"], submit["result"]["text"]) == (
        "completed", "complete", "still delivered")
    assert failures == ["complete"]

    def always_fail(conn, record, receipt, now):
        if receipt["outcome"] != idem.OUTCOME_MISSING:
            raise RuntimeError("injected result store failure")
        return real_store(conn, record, receipt, now)

    monkeypatch.setattr(idem, "_store_result", always_fail)
    msg = _ids()
    seen = _watch_complete(monkeypatch, postgres_dsn, msg)
    assert _submit(sid, "deliver again", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    [(frame, _at_emit)] = seen
    assert frame["status"] == "complete"
    submit = _accepted(msg, key)["submit"]
    assert (submit["state"], submit["result"]["outcome"], submit["result"]["error"]) == (
        "completed", "missing", "result_store_error")


# T7 — a retry's attempt replaces the result; one attempt never has two
def test_resumed_attempt_replaces_the_dead_attempts_result(authority, postgres_dsn):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    # Attempt 1 records its result, then its process dies before the reply row.
    recorded, turns.gate = threading.Event(), threading.Event()
    turns.receipt = {"outcome": "interrupted", "text": "cut sho", "error": None}
    turns.after_receipt = lambda _session: recorded.set()
    assert _submit(sid, "answer me", client_msg_id=msg)["result"]["status"] == "streaming"
    assert recorded.wait(20)
    _kill_owner(postgres_dsn, key, msg)
    turns.gate.set()
    _settle(sid)
    turns.gate = turns.after_receipt = None
    dead = _accepted(msg, key, include_result=True)["submit"]
    assert (dead["state"], dead["needs_attention"]) == ("persisted", "no_reply")
    assert (dead["result"]["attempt"], dead["result"]["outcome"], dead["result"]["text"]) == (
        1, "interrupted", "cut sho")

    turns.receipt = {"outcome": "complete", "text": "reply: answer me", "error": None}
    assert _submit(sid, "answer me", client_msg_id=msg)["result"]["status"] == "resumed"
    _settle(sid)
    done = _accepted(msg, key, include_result=True)["submit"]
    assert (done["state"], done["attempts"]) == ("completed", 2)
    assert (done["result"]["attempt"], done["result"]["outcome"], done["result"]["text"]) == (
        2, "complete", "reply: answer me")
    assert (done["result"]["history_ref_kind"], done["result"]["history_text_match"]) == ("inferred", True)


def test_restarted_attempt_replaces_the_result_and_an_attempt_keeps_its_first(authority):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    turns.persist = False
    turns.receipt = {"outcome": "error", "text": "", "error": "agent_init_failed", "text_kind": "none"}
    assert _submit(sid, "again?", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    first = _accepted(msg, key)["submit"]
    assert (first["state"], first["result"]["attempt"], first["result"]["error"]) == (
        "accepted", 1, "agent_init_failed")

    turns.persist, turns.receipt = True, {"outcome": "complete", "text": "now it ran", "error": None}
    restarted = _submit(sid, "again?", client_msg_id=msg)["result"]
    assert (restarted["status"], restarted["attempts"]) == ("streaming", 2)
    _settle(sid)
    second = _accepted(msg, key, include_result=True)["submit"]
    assert (second["state"], second["result"]["attempt"], second["result"]["text"]) == ("completed", 2, "now it ran")
    turns.receipt = None

    claim, key, msg = _owned(db, "one attempt")
    receipt = {"outcome": "complete", "text": "first result", "error": None}
    assert idem.record_result(claim, receipt) is True
    assert idem.record_result(claim, {**receipt, "text": "another result"}) is False
    assert idem.record_result(claim, receipt) is True  # the same result again: no-op
    idem.finish_submit(claim, {**receipt, "text": "settle receipt"})
    result = idem.lookup_submit(msg, key, with_text=True).payload(include_text=True)["result"]
    assert (result["attempt"], result["text"]) == (1, "first result")


# T8 — retention: past it the record is gone (found:false); the state fields keep their meaning
def test_pruned_result_is_not_found_and_state_fields_are_unchanged(authority):
    db, turns, sids = authority
    created = _create(sids)
    sid, key, msg = created["session_id"], created["stored_session_id"], _ids()
    turns.receipt = {"outcome": "complete", "text": "kept a week", "error": None}
    assert _submit(sid, "keep me", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    again = _submit(sid, "keep me", client_msg_id=msg)["result"]
    # What a broker decides on (status, state, running, needs_attention) is what it was.
    assert (again["status"], again["state"], again["running"], again["needs_attention"]) == (
        "duplicate", "completed", False, None)
    assert again["result"]["outcome"] == "complete" and "text" not in again["result"]
    assert _accepted(msg, key)["found"] is True

    idem.prune(time.time() + idem.RETENTION_SECONDS + 3600)
    assert _accepted(msg, key) == {
        "enabled": True, "found": False, "submit": None,
        "result_contract": {"version": 1, "retention_s": idem.RETENTION_SECONDS}}
    turns.receipt = None


# A record accepted before the contract (the columns are added to the existing table)
def test_record_from_before_the_contract_has_no_result(authority, postgres_dsn):
    _db, _turns, sids = authority
    created = _create(sids)
    key, msg = created["stored_session_id"], _ids()
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(
            "CREATE TABLE core_submit_accepts (session_key TEXT NOT NULL, client_msg_id TEXT NOT NULL, "
            "fingerprint TEXT NOT NULL, state TEXT NOT NULL, ui_session_id TEXT, "
            "watermark BIGINT NOT NULL DEFAULT 0, user_message_id BIGINT, owner TEXT NOT NULL DEFAULT '', "
            "lease_until DOUBLE PRECISION NOT NULL DEFAULT 0, attempts BIGINT NOT NULL DEFAULT 1, "
            "accepted_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL, "
            "completed_at DOUBLE PRECISION, PRIMARY KEY (session_key, client_msg_id))")
        raw.execute(
            "INSERT INTO core_submit_accepts (session_key, client_msg_id, fingerprint, state, watermark, "
            "user_message_id, owner, lease_until, attempts, accepted_at, updated_at, completed_at) "
            "VALUES (%s, %s, 'fp', 'completed', 0, 1, '', 0, 1, 1.0, "
            "EXTRACT(EPOCH FROM clock_timestamp()), 2.0)", (key, msg))

    response = _accepted(msg, key, include_result=True)
    _show("pre_contract", response)
    submit = response["submit"]
    assert (submit["state"], submit["result_contract"], submit["result"]) == ("completed", None, None)
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        columns = {row[0] for row in raw.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'core_submit_accepts'").fetchall()}
    assert {name for name, _type in idem._RESULT_COLUMNS} <= columns
