"""Contract for conditional ``session.undo`` (``expect_row_id`` + ``expect_tail_row_ids``).

A client that read the messages table pins what it agreed to remove; the core either removes exactly
that or refuses with 4004/4009/4033/4034 having changed nothing — including when another process
inserted rows directly into the table that the in-memory history never saw."""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from agent.context_compressor import HISTORICAL_TASK_HEADING, SUMMARY_PREFIX, _SUMMARY_END_MARKER
from hermes_state import SessionDB
from tui_gateway import server

_TURNS = [{"role": "user", "content": "q1"}, {"role": "assistant", "content": "a1"},
          {"role": "user", "content": "q2"}, {"role": "assistant", "content": "a2"}]
_CARRIER = {"role": "user", "content": f"{SUMMARY_PREFIX}\n{HISTORICAL_TASK_HEADING}\nold task\n\n"
                                       f"{_SUMMARY_END_MARKER}\n\nREAL ASK"}


@pytest.fixture()
def live(tmp_path):
    """``install(rows, session_key=...)`` -> (sid, key, session, row_ids) for an idle, loaded session."""
    old_db = server._db
    db = SessionDB(db_path=tmp_path / "state.db")
    installed: list[str] = []

    def install(rows: list[dict], *, session_key: str | None = None):
        sid = f"undo-sid-{len(installed)}"
        installed.append(sid)
        key = f"undo-session-{len(installed)}" if session_key is None else session_key
        row_ids = []
        if key:
            db.create_session(key, source="tui")
            row_ids = [db.append_message(key, **row) for row in rows]
            history = db.get_messages_as_conversation(key)
        else:
            history = [dict(row) for row in rows]
        server._sessions[sid] = session = {
            "agent": SimpleNamespace(_session_messages=list(history), _last_flushed_db_idx=len(history),
                                     _db_flush_scan_prefix=list(history)),
            "attached_images": [], "history": list(history), "history_lock": threading.Lock(),
            "history_version": 0, "running": False, "session_key": key,
        }
        return sid, key, session, row_ids

    server._db = db
    yield db, install
    for sid in installed:
        server._sessions.pop(sid, None)
    server._db = old_db
    db.close()


def _rpc(method: str, **params) -> dict:
    return server.handle_request({"id": "1", "method": method, "params": params})


def _pin(tail: list[int]) -> dict:
    return {"expect_row_id": tail[0], "expect_tail_row_ids": list(tail)}


def _insert_elsewhere(db: SessionDB, key: str, rows: list[dict]) -> list[int]:
    """Rows written by another process straight into the messages table."""
    other = SessionDB(db_path=db.db_path)
    try:
        before = set(other.get_active_message_ids(key))
        other.append_messages_batch(key, rows)
        return [i for i in other.get_active_message_ids(key) if i not in before]
    finally:
        other.close()


def _card(n: int = 1) -> list[dict]:
    return [{"role": "assistant", "content": f"card {i}", "display_only": True} for i in range(n)]


# Each case: (rows, session_key, arrange(db, key, ids, session) -> (params, code, data | None)).
_BAD_PARAMS = [
    {"expect_row_id": None, "expect_tail_row_ids": None}, {"expect_row_id": 3}, {"expect_tail_row_ids": [3, 4]},
    {"expect_row_id": "3", "expect_tail_row_ids": [3]}, {"expect_row_id": 3.0, "expect_tail_row_ids": [3]},
    {"expect_row_id": True, "expect_tail_row_ids": [1]}, {"expect_row_id": 0, "expect_tail_row_ids": [0]},
    {"expect_row_id": -3, "expect_tail_row_ids": [-3]}, {"expect_row_id": 2**63, "expect_tail_row_ids": [2**63]},
    {"expect_row_id": 3, "expect_tail_row_ids": 3}, {"expect_row_id": 3, "expect_tail_row_ids": []},
    {"expect_row_id": 3, "expect_tail_row_ids": [4]}, {"expect_row_id": 3, "expect_tail_row_ids": [3, 3]},
    {"expect_row_id": 3, "expect_tail_row_ids": [3, False]},
    {"expect_row_id": 3, "expect_tail_row_ids": list(range(3, 3 + 4097))},
]
_REFUSALS = {
    **{f"4004-{i}": (_TURNS, None, lambda db, key, ids, s, p=p: (p, 4004, None)) for i, p in enumerate(_BAD_PARAMS)},
    "4009-busy-first": (_TURNS, None, lambda db, key, ids, s: (
        s.update(running=True) or {"expect_row_id": None}, 4009, None)),
    "4033-older-turn": (_TURNS, None, lambda db, key, ids, s: (
        _pin(ids), 4033, {"expected": ids[0], "actual": ids[2]})),
    "4033-other-participant": (_TURNS, None, lambda db, key, ids, s: (
        _pin(ids[2:]), 4033,
        {"expected": ids[2], "actual": _insert_elsewhere(db, key, [{"role": "user", "content": "hi"}])[0]})),
    "4033-loaded-history-diverged": (_TURNS, None, lambda db, key, ids, s: (
        s["history"].extend([{"role": "assistant", "content": "x"}, {"role": "user", "content": "unflushed"}])
        or _pin(ids[2:]), 4033, {"expected": ids[2], "actual": None})),
    "4033-no-user-turn": ([{"role": "assistant", "content": "hello"}], None, lambda db, key, ids, s: (
        _pin(ids), 4033, {"expected": ids[0], "actual": None})),
    "4033-not-persisted": (_TURNS, "", lambda db, key, ids, s: (
        _pin([1, 2]), 4033, {"expected": 1, "actual": None})),
    "4034-row-inserted-directly": (_TURNS, None, lambda db, key, ids, s: (
        _pin(ids[2:]), 4034,
        {"expected": ids[2:], "actual": [*ids[2:], *_insert_elsewhere(db, key, _card())], "actual_truncated": False})),
    "4034-actual-capped": (_TURNS, None, lambda db, key, ids, s: (
        _pin(ids[2:]), 4034,
        {"expected": ids[2:], "actual": [*ids[2:], *_insert_elsewhere(db, key, _card(4097))][:4096],
         "actual_truncated": True})),
}


@pytest.mark.parametrize("case", _REFUSALS.values(), ids=_REFUSALS.keys())
def test_conditional_undo_refusals_change_nothing(live, case):
    db, install = live
    rows, session_key, arrange = case
    sid, key, session, ids = install(rows, session_key=session_key)
    params, code, data = arrange(db, key, ids, session)
    rows_before = db._conn.execute("SELECT id, active FROM messages ORDER BY id").fetchall()
    history_before = [dict(message) for message in session["history"]]

    response = _rpc("session.undo", session_id=sid, **params)

    assert response["error"]["code"] == code
    if data is not None:
        assert response["error"]["data"] == data
    assert db._conn.execute("SELECT id, active FROM messages ORDER BY id").fetchall() == rows_before
    assert session["history"] == history_before


_SUCCESSES = {
    "unconditional": (_TURNS, False, 2), "conditional": (_TURNS, True, 2),
    "marker-in-tail": ([*_TURNS, {"role": "user", "content": "model switched", "display_kind": "notice"}], True, 2),
    "carrier-target": ([_CARRIER, {"role": "assistant", "content": "answer"}], True, 0),
}


@pytest.mark.parametrize("rows, conditional, target", _SUCCESSES.values(), ids=_SUCCESSES.keys())
def test_undo_deactivates_exactly_the_tail_and_reports_what_it_checked(live, rows, conditional, target):
    db, install = live
    sid, key, session, ids = install(rows)

    response = _rpc("session.undo", session_id=sid, **(_pin(ids[target:]) if conditional else {}))

    removed = len(ids) - target
    expected = {"removed": removed}
    if conditional:
        checked = _rpc("gateway.capabilities")["result"]["session_undo_expectations"]
        assert set(checked) == {"expect_row_id", "expect_tail_row_ids"}
        expected.update(target_row_id=ids[target], checked=checked)
    assert response["result"] == expected
    active = db.get_active_message_ids(key)
    assert active[:target] == ids[:target] and not set(active) & set(ids[target:])
