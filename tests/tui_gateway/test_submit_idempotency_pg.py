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

import os
import threading
import uuid
from pathlib import Path

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
    for a resumed turn, only the reply to the stored one — optionally holds the turn
    open on ``gate``, then ends it like the real thread."""

    def __init__(self, db):
        self.db = db
        self.texts: list = []
        self.stored_rows: list = []
        self.gate: threading.Event | None = None
        self.persist = True
        self._lock = threading.Lock()

    def __call__(self, rid, sid, session, text, display_kind, callback, turn_author=None,
                 stored_user_row=None):
        with self._lock:
            self.texts.append(text)
            self.stored_rows.append(stored_user_row)
        if stored_user_row is not None:
            self.db.append_message(session["session_key"], "assistant", f"reply: {text}")
        elif self.persist:
            self.db.append_message(session["session_key"], "user", text)
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
        "enabled": True, "submit": None, "found": False}
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
