"""R0 (opsi, isolated core) probes of the D1-b turn result receipt, reversed for contract v1 (t_b1dcb36c).

On ``levos/pg3`` 951e8a6 the original probes passed: ``prompt.accepted`` carried no
result, a turn with no reply row was ``completed`` with nothing to say so, and a record
kept under a compression tip was not found by the root key. Each assertion below is the
reverse and fails on that base.

1) ``prompt.accepted`` carries ``result_contract`` and a ``submit.result``.
2) A record kept under the compression tip is found by the root key a broker got at create.
3) A turn that stored only the user row stays ``completed``; its result says the history
   holds no answer (``history_persisted`` false).
4) A lookup runs no turn.
"""

from __future__ import annotations

import uuid

import psycopg

from tests.tui_gateway.test_submit_idempotency_pg import (  # noqa: F401 — pytest fixtures by name
    _clean_env,
    _create,
    _ids,
    _rpc,
    _settle,
    _submit,
    authority,
    postgres_dsn,
)
from tui_gateway import submit_idempotency as idem

_RESULT_KEYS = {
    "contract", "attempt", "outcome", "text_kind", "text_sha256", "text_chars", "error", "result_session_id",
    "assistant_message_id", "history_ref_kind", "history_persisted", "history_text_match", "recorded_at",
    "expires_at"}


def test_r0_accepted_shape_has_the_result_contract(authority):
    db, turns, sids = authority
    c = _create(sids)
    sid, key, msg = c["session_id"], c["stored_session_id"], _ids()
    assert _submit(sid, "R0 shape", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    n_turns = len(turns.texts)
    res = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": key})["result"]
    assert res["result_contract"] == {"version": 1, "retention_s": 604800}
    assert res["submit"]["state"] == "completed"
    assert res["submit"]["result_contract"] == 1
    assert set(res["submit"]["result"]) == _RESULT_KEYS  # no text unless include_result
    assert len(turns.texts) == n_turns  # a lookup makes no turn


def test_r0_completed_without_reply_row_says_so(authority):
    """Only the user row stored (no reply row): still completed, and the result says the
    history holds no answer."""
    db, turns, sids = authority
    c = _create(sids)
    sid, key, msg = c["session_id"], c["stored_session_id"], _ids()
    assert _submit(sid, "R0 no reply", client_msg_id=msg)["result"]["status"] == "streaming"
    _settle(sid)
    rows = [m.get("role") for m in db.get_messages_as_conversation(key)]
    res = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": key})["result"]["submit"]
    assert rows == ["user"] and res["state"] == "completed"
    assert res["result"]["history_persisted"] is False


def test_r0_compression_lineage_root_lookup_finds_the_tip_record(authority, postgres_dsn):
    """A record kept under the compression tip is found by the root key."""
    db, turns, sids = authority
    root = "r0root_" + uuid.uuid4().hex[:8]
    tip = "r0tip_" + uuid.uuid4().hex[:8]
    db.create_session(root, "tui")
    db.end_session(root, "compression")
    db.create_session(tip, "tui", parent_session_id=root)
    msg = _ids()
    now = 1.0
    idem.open_store().close()  # creates the table (lazy initialization)
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(
            "INSERT INTO core_submit_accepts (session_key, client_msg_id, fingerprint, state, watermark, "
            "user_message_id, owner, lease_until, attempts, accepted_at, updated_at, completed_at) "
            "VALUES (%s,%s,'fp','completed',0,1,'',0,1,%s,%s,%s)", (tip, msg, now, now, now))
    by_tip = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": tip})["result"]
    by_root = _rpc("prompt.accepted", {"client_msg_id": msg, "stored_session_id": root})["result"]
    assert by_tip["found"] is True
    assert by_root["found"] is True
    assert (by_root["submit"]["stored_session_id"], by_root["submit"]["lineage_root"]) == (tip, root)
