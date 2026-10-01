"""Client request ids for ``prompt.submit`` / ``session.create`` (levos v3, t_2b6c09df).

The levos broker parks human messages in PostgreSQL while a runtime is being
replaced and re-sends them afterwards. A re-send must not become a second
user turn, and a message whose reply was lost must not be dropped. On a
PostgreSQL-authority profile (:func:`hermes_aux_store.aux_store_authority`)
this module keeps the acceptance records next to the profile's core tables:

* ``submit_accepts`` — (session key, ``client_msg_id``) → text fingerprint,
  state (``accepted`` → ``persisted`` → ``completed``), the highest message
  id of the session lineage at acceptance (the *watermark*: the first user
  row above it is this submit's row), the stored user message id, and the
  owner process with a lease the owner refreshes while its turn is live.
* ``session_creates`` — (profile, ``client_create_id``) → runtime session id
  and stored session key.

A ``persisted`` record whose owner died before any reply row was written is
*resumed* by the next same-text request (t_7ceb9994): the turn runs once more
on the stored user row, which is never written again. A reply row (even a
partial one, or a tool call) after it means the turn already had effects, so it
is not re-run; the record reports ``needs_attention`` instead.

Result receipt (contract v1, t_b1dcb36c): the owned turn writes its terminal
outcome and the exact ``message.complete`` text onto the same record before it
emits that frame, so a broker that missed the frame restores the answer from
``prompt.accepted`` instead of running the turn again. The record is the
public result; the assistant message id it carries is a reference into the
history, checked against the stored rows (never the in-memory history).

Every decision about one id runs in one transaction under a PostgreSQL
advisory transaction lock, so two concurrent requests with the same id
start at most one turn — across threads and across pods. Off authority the
callers never reach this module (the ids are accepted and ignored); on
authority an unreachable PostgreSQL raises
:class:`hermes_aux_store.AuxStoreUnavailable` and nothing falls back to a
file.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import socket
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterator, Optional, Tuple

from hermes_aux_store import (
    aux_add_columns,
    aux_schema_transaction,
    aux_store_authority,
    aux_xact_lock,
    open_aux_postgres,
)

logger = logging.getLogger(__name__)

STORE = "submit_idempotency"
MAX_CLIENT_ID_CHARS = 128
RETENTION_SECONDS = 7 * 24 * 60 * 60
LEASE_SECONDS = 60.0
HEARTBEAT_SECONDS = 20.0
LOCK_TIMEOUT_SECONDS = 30.0
PRUNE_INTERVAL_SECONDS = 60 * 60

STATE_ACCEPTED = "accepted"
STATE_PERSISTED = "persisted"
STATE_COMPLETED = "completed"

# ``needs_attention`` of a ``persisted`` record with no live owner (a dead turn).
NO_REPLY = "no_reply"  # nothing after the user row: a same-id re-send resumes the turn
PARTIAL_REPLY = "partial_reply"  # an assistant/tool row follows it: never re-run
LATER_MESSAGES = "later_messages"  # only later user rows follow it: never re-run

# Turn result receipt. A record accepted before the contract has ``result_contract`` NULL.
RESULT_CONTRACT = 1
RESULT_TEXT_MAX = 2 * 1024 * 1024  # UTF-8 bytes of a final text kept inline; longer: "omitted"
OUTCOME_MISSING = "missing"  # the owned turn ended without a recorded result
TEXT_NONE = "none"  # the turn produced no message.complete text (exception, refusal, early exit)
_RESULT_COLUMNS = (
    ("result_contract", "INTEGER"),
    ("outcome", "TEXT"),  # complete | error | interrupted | missing
    ("text_kind", "TEXT"),  # text | empty | none | omitted | unsupported
    ("final_text", "TEXT"),  # message.complete payload.text verbatim (text / empty only)
    ("final_sha256", "TEXT"),  # of its UTF-8 bytes
    ("final_chars", "INTEGER"),
    ("error_text", "TEXT"),  # outcome error / missing: why
    ("result_session_id", "TEXT"),  # the session (compression tip) the turn wrote to
    ("assistant_message_id", "INTEGER"),
    ("history_ref_kind", "TEXT"),  # exact | inferred | none
    ("history_persisted", "BOOLEAN"),
    ("history_text_match", "BOOLEAN"),  # diagnostic: the stored row's content is the final text
    ("result_attempt", "INTEGER"),
    ("result_recorded_at", "REAL"),
)
_RESULT_META = ", ".join(name for name, _type in _RESULT_COLUMNS if name != "final_text")

# Claim outcomes.
NEW = "new"
RESTART = "restart"
RESUME = "resumed"  # the user row is stored, its reply is not: run the turn on that row
DUPLICATE = "duplicate"
CONFLICT = "conflict"
REFUSED = "refused"

# One token per process: a record owned by it is live exactly while this
# process's registry holds it; any other owner is live while its lease runs.
OWNER = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"

_NOW = "EXTRACT(EPOCH FROM clock_timestamp())"
_ACCEPT_COLUMNS = (
    "session_key, client_msg_id, fingerprint, state, ui_session_id, watermark, "
    "user_message_id, owner, lease_until, attempts, accepted_at, updated_at, completed_at"
)
# Compression continues a conversation in a child row whose parent ended with
# ``end_reason = 'compression'``; the same walk the turn lease uses.
_LINEAGE_UP = (
    "WITH RECURSIVE up(id) AS (SELECT CAST(? AS TEXT) UNION "
    "SELECT p.id FROM sessions c JOIN up ON c.id = up.id "
    "JOIN sessions p ON p.id = c.parent_session_id WHERE p.end_reason = 'compression') "
)
_LINEAGE_DOWN = (
    "WITH RECURSIVE down(id) AS (SELECT CAST(? AS TEXT) UNION "
    "SELECT c.id FROM sessions c JOIN down ON c.parent_session_id = down.id "
    "JOIN sessions p ON p.id = c.parent_session_id WHERE p.end_reason = 'compression') "
)


class ClientIdError(ValueError):
    """A client request id that is not a 1..128 character string."""


def active() -> bool:
    """True when request ids are honoured: the profile is on PostgreSQL authority."""
    return aux_store_authority()


def normalize_client_id(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_CLIENT_ID_CHARS:
        raise ClientIdError(f"{name} must be a string of 1..{MAX_CLIENT_ID_CHARS} characters")
    return value


def fingerprint(text: Any) -> str:
    """sha256 of the submitted text (structured payloads: canonical JSON)."""
    if isinstance(text, str):
        data = text
    else:
        data = json.dumps(text, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _initialize(conn) -> None:
    with aux_schema_transaction(conn, STORE):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS submit_accepts (
                 session_key TEXT NOT NULL,
                 client_msg_id TEXT NOT NULL,
                 fingerprint TEXT NOT NULL,
                 state TEXT NOT NULL,
                 ui_session_id TEXT,
                 watermark INTEGER NOT NULL DEFAULT 0,
                 user_message_id INTEGER,
                 owner TEXT NOT NULL DEFAULT '',
                 lease_until REAL NOT NULL DEFAULT 0,
                 attempts INTEGER NOT NULL DEFAULT 1,
                 accepted_at REAL NOT NULL,
                 updated_at REAL NOT NULL,
                 completed_at REAL,
                 PRIMARY KEY (session_key, client_msg_id)
               )"""
        )
        aux_add_columns(conn, "core_submit_accepts", _RESULT_COLUMNS)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_submit_accepts_msg ON submit_accepts (client_msg_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_submit_accepts_updated ON submit_accepts (updated_at)"
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS session_creates (
                 profile TEXT NOT NULL,
                 client_create_id TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 session_key TEXT NOT NULL,
                 created_at REAL NOT NULL,
                 updated_at REAL NOT NULL,
                 PRIMARY KEY (profile, client_create_id)
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_session_creates_updated ON session_creates (updated_at)"
        )


def open_store():
    """The authority profile's store; :class:`AuxStoreUnavailable` when it cannot serve."""
    return open_aux_postgres(STORE, initialize=_initialize)


def _server_now(conn) -> float:
    return float(conn.execute(f"SELECT {_NOW}").fetchone()[0])


# ---------------------------------------------------------------------------
# Retention (the prune-on-write the other idempotency ledgers use)
# ---------------------------------------------------------------------------

_prune_lock = threading.Lock()
_last_prune = 0.0


def _prune_due() -> bool:
    global _last_prune
    with _prune_lock:
        if time.monotonic() - _last_prune < PRUNE_INTERVAL_SECONDS and _last_prune:
            return False
        _last_prune = time.monotonic()
        return True


def _prune(conn, now: float) -> None:
    """Drop records untouched for :data:`RETENTION_SECONDS`; a live record's
    heartbeat keeps moving ``updated_at`` so it is never pruned mid-turn."""
    cutoff = now - RETENTION_SECONDS
    conn.execute("DELETE FROM submit_accepts WHERE updated_at < ?", (cutoff,))
    conn.execute("DELETE FROM session_creates WHERE updated_at < ?", (cutoff,))


def prune(now: Optional[float] = None) -> None:
    """Run the retention sweep now (also runs at most hourly on writes)."""
    conn = open_store()
    try:
        with conn:
            _prune(conn, _server_now(conn) if now is None else now)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Local liveness registry + lease heartbeat
# ---------------------------------------------------------------------------

_live_lock = threading.Lock()
_live: set = set()  # (session_key, client_msg_id) whose turn runs in this process
_heartbeat: Optional[threading.Thread] = None


def _register_live(key: Tuple[str, str]) -> None:
    global _heartbeat
    with _live_lock:
        _live.add(key)
        if _heartbeat is None or not _heartbeat.is_alive():
            _heartbeat = threading.Thread(
                target=_heartbeat_loop, name="submit-idempotency-lease", daemon=True
            )
            _heartbeat.start()


def _unregister_live(key: Tuple[str, str]) -> None:
    with _live_lock:
        _live.discard(key)


def is_live_here(session_key: str, client_msg_id: str) -> bool:
    with _live_lock:
        return (session_key, client_msg_id) in _live


def _heartbeat_loop() -> None:
    global _heartbeat
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        with _live_lock:
            keys = sorted(_live)
            if not keys:
                _heartbeat = None
                return
        try:
            refresh_leases(keys)
        except Exception:
            logger.warning("submit idempotency: lease refresh failed", exc_info=True)


def refresh_leases(keys) -> None:
    """Extend the lease of this process's live records."""
    conn = open_store()
    try:
        with conn:
            now = _server_now(conn)
            for session_key, client_msg_id in keys:
                conn.execute(
                    "UPDATE submit_accepts SET lease_until = ?, updated_at = ? "
                    "WHERE session_key = ? AND client_msg_id = ? AND owner = ?",
                    (now + LEASE_SECONDS, now, session_key, client_msg_id, OWNER),
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# prompt.submit acceptance records
# ---------------------------------------------------------------------------


@dataclass
class Acceptance:
    session_key: str
    client_msg_id: str
    fingerprint: str
    state: str
    ui_session_id: Optional[str]
    watermark: int
    user_message_id: Optional[int]
    owner: str
    lease_until: float
    attempts: int
    accepted_at: float
    updated_at: float
    completed_at: Optional[float]
    running: bool = False
    needs_attention: Optional[str] = None
    lineage_root: Optional[str] = None
    result_row: dict = field(default_factory=dict)  # the result columns read with the record

    @property
    def key(self) -> Tuple[str, str]:
        return (self.session_key, self.client_msg_id)

    def payload(self, *, result: bool = True, include_text: bool = False) -> dict:
        """The wire view (``stored_session_id`` is the session key it was accepted under).
        ``result``: carry the turn result receipt (session-scoped lookups and claims);
        ``include_text``: with its final text."""
        data = {
            "client_msg_id": self.client_msg_id,
            "state": self.state,
            "running": self.running,
            "stored_session_id": self.session_key,
            "user_message_id": self.user_message_id,
            "accepted_at": self.accepted_at,
            "completed_at": self.completed_at,
            "attempts": self.attempts,
            "needs_attention": self.needs_attention,
            "fingerprint": self.fingerprint,
            "lineage_root": self.lineage_root,
            "result_contract": self.result_row.get("result_contract"),
        }
        if result:
            data["result"] = self.result_view(include_text=include_text)
        return data

    def result_view(self, *, include_text: bool = False) -> Optional[dict]:
        """The recorded result; None before the contract or while no result is recorded."""
        row = self.result_row
        if row.get("result_contract") is None or row.get("outcome") is None:
            return None
        view = {
            "contract": row["result_contract"],
            "attempt": row["result_attempt"],
            "outcome": row["outcome"],
            "text_kind": row["text_kind"],
        }
        if include_text:
            view["text"] = row.get("final_text")
        view.update({
            "text_sha256": row["final_sha256"],
            "text_chars": row["final_chars"],
            "error": row["error_text"],
            "result_session_id": row["result_session_id"],
            "assistant_message_id": row["assistant_message_id"],
            "history_ref_kind": row["history_ref_kind"],
            "history_persisted": row["history_persisted"],
            "history_text_match": row["history_text_match"],
            "recorded_at": row["result_recorded_at"],
            "expires_at": self.updated_at + RETENTION_SECONDS,
        })
        return view


@dataclass
class Claim:
    outcome: str
    record: Optional[Acceptance] = None
    refusal: Any = None  # what ``admit`` returned when it refused

    @property
    def owns_turn(self) -> bool:
        return self.outcome in (NEW, RESTART, RESUME)

    @property
    def stored_user_row(self) -> Optional[int]:
        """The stored user row a ``RESUME`` claim's turn answers (never written again)."""
        return self.record.user_message_id if self.outcome == RESUME and self.record else None


def _acceptance(row, *, with_text: bool = False) -> Acceptance:
    values = {name: row[name] for name in _ACCEPT_COLUMNS.split(", ")}
    values["watermark"] = int(values["watermark"] or 0)
    values["attempts"] = int(values["attempts"] or 1)
    if values["user_message_id"] is not None:
        values["user_message_id"] = int(values["user_message_id"])
    record = Acceptance(**values)
    record.result_row = _result_values(row, with_text=with_text)
    return record


def _result_values(row, *, with_text: bool = False) -> dict:
    names = _RESULT_META.split(", ") + (["final_text"] if with_text else [])
    return {name: row[name] for name in names}


def _record_columns(*, with_text: bool = False) -> str:
    return f"{_ACCEPT_COLUMNS}, {_RESULT_META}" + (", final_text" if with_text else "")


def _lineage_root(conn, session_key: str) -> str:
    rows = conn.execute(_LINEAGE_UP + "SELECT id FROM up", (session_key,)).fetchall()
    ids = [row[0] for row in rows]
    # The walk yields the key first and its ancestors after it; the oldest
    # compression ancestor is the stable name of the conversation.
    return ids[-1] if ids else session_key


def _find(conn, session_key: str, client_msg_id: str, *, with_text: bool = False) -> Optional[Acceptance]:
    """The record of *client_msg_id* anywhere in *session_key*'s conversation: every
    compression descendant of its root, so a record kept under the tip is found by the
    root key a broker got at create time (and by every key in between)."""
    root = _lineage_root(conn, session_key)
    row = conn.execute(
        _LINEAGE_DOWN + f"SELECT {_record_columns(with_text=with_text)} FROM submit_accepts "
        "WHERE client_msg_id = ? AND session_key IN (SELECT id FROM down) "
        "ORDER BY accepted_at LIMIT 1",
        (root, client_msg_id),
    ).fetchone()
    if row is None:
        return None
    record = _acceptance(row, with_text=with_text)
    record.lineage_root = root
    return record


def _watermark(conn, session_key: str) -> int:
    row = conn.execute(
        _LINEAGE_DOWN + "SELECT COALESCE(MAX(m.id), 0) FROM messages m "
        "WHERE m.session_id IN (SELECT id FROM down)",
        (session_key,),
    ).fetchone()
    return int(row[0] or 0)


def _first_user_row(conn, record: Acceptance) -> Optional[int]:
    row = conn.execute(
        _LINEAGE_DOWN + "SELECT m.id FROM messages m "
        "WHERE m.session_id IN (SELECT id FROM down) AND m.role = 'user' AND m.id > ? "
        "ORDER BY m.id LIMIT 1",
        (record.session_key, record.watermark),
    ).fetchone()
    return None if row is None else int(row[0])


def _after_user_row(conn, record: Acceptance) -> str:
    """What the lineage holds after the stored user row of a dead turn."""
    row = conn.execute(
        _LINEAGE_DOWN + "SELECT COALESCE(MAX(CASE WHEN m.role IN ('assistant', 'tool') THEN 2 "
        "WHEN m.role = 'user' THEN 1 ELSE 0 END), 0) FROM messages m "
        "WHERE m.session_id IN (SELECT id FROM down) AND m.id > ?",
        (record.session_key, record.user_message_id),
    ).fetchone()
    return (NO_REPLY, LATER_MESSAGES, PARTIAL_REPLY)[int(row[0] or 0)]


def _is_live(record: Acceptance, now: float) -> bool:
    if not record.owner:
        return False
    if record.owner == OWNER:
        return is_live_here(record.session_key, record.client_msg_id)
    return record.lease_until > now


def _reconcile(conn, record: Acceptance, now: float) -> Acceptance:
    """Move ``accepted`` to ``persisted`` once the user row exists; set ``running``
    and, for a ``persisted`` record nobody runs, ``needs_attention``."""
    if record.state == STATE_ACCEPTED and record.user_message_id is None:
        message_id = _first_user_row(conn, record)
        if message_id is not None:
            conn.execute(
                "UPDATE submit_accepts SET state = ?, user_message_id = ?, updated_at = ? "
                "WHERE session_key = ? AND client_msg_id = ?",
                (STATE_PERSISTED, message_id, now, record.session_key, record.client_msg_id),
            )
            record.state, record.user_message_id, record.updated_at = (
                STATE_PERSISTED, message_id, now)
    record.running = record.state != STATE_COMPLETED and _is_live(record, now)
    if record.state == STATE_PERSISTED and not record.running:
        record.needs_attention = _after_user_row(conn, record)
    return record


def _receipt_columns(receipt: dict) -> dict:
    """Map a turn receipt to result columns. The text is the ``message.complete``
    ``payload.text`` and is stored byte for byte or not at all: a non-string, or a
    string PostgreSQL TEXT cannot hold (NUL, lone surrogate), is ``unsupported``."""
    text, kind = receipt.get("text"), receipt.get("text_kind")
    final = digest = chars = None
    if kind != TEXT_NONE:
        try:
            data = text.encode("utf-8") if isinstance(text, str) else None
        except UnicodeEncodeError:
            data = None
        if data is None:
            kind = "unsupported"
        else:
            digest, chars = hashlib.sha256(data).hexdigest(), len(text)
            if "\x00" in text:
                kind = "unsupported"
            elif len(data) > RESULT_TEXT_MAX:
                kind = "omitted"
            else:
                kind, final = ("empty" if text == "" else "text"), text
    outcome = receipt.get("outcome")
    error = receipt.get("error") if outcome in ("error", OUTCOME_MISSING) else None
    return {
        "outcome": outcome, "text_kind": kind, "final_text": final, "final_sha256": digest,
        "final_chars": chars, "error_text": None if error is None else str(error),
    }


def _history_ref(conn, record: Acceptance, assistant_row_id: Optional[int], final_sha256: Optional[str]) -> dict:
    """The stored assistant row this result refers to, read from the messages table:
    the id the agent's flush stamped (``exact``), else the last assistant row of the
    turn — after its user row, before the next one (``inferred``)."""
    row, kind, message_id = None, "none", None
    if assistant_row_id is not None:
        kind, message_id = "exact", int(assistant_row_id)
        row = conn.execute(
            "SELECT id, content FROM messages WHERE id = ? AND role = 'assistant'", (message_id,)
        ).fetchone()
    elif record.user_message_id is not None:
        row = conn.execute(
            _LINEAGE_DOWN + "SELECT m.id, m.content FROM messages m "
            "WHERE m.session_id IN (SELECT id FROM down) AND m.role = 'assistant' AND m.id > ? "
            "AND m.id < COALESCE((SELECT MIN(u.id) FROM messages u WHERE u.session_id IN "
            "(SELECT id FROM down) AND u.role = 'user' AND u.id > ?), 9223372036854775807) "
            "ORDER BY m.id DESC LIMIT 1",
            (record.session_key, record.user_message_id, record.user_message_id),
        ).fetchone()
        if row is not None:
            kind, message_id = "inferred", int(row[0])
    match = None
    if row is not None and final_sha256 is not None:
        from hermes_state import SessionDB

        content = SessionDB._decode_content(row[1])
        match = isinstance(content, str) and (
            hashlib.sha256(content.encode("utf-8", "surrogatepass")).hexdigest() == final_sha256)
    return {
        "assistant_message_id": message_id, "history_ref_kind": kind,
        "history_persisted": row is not None, "history_text_match": match,
    }


def _store_result(conn, record: Acceptance, receipt: dict, now: float) -> bool:
    """Write *receipt* as the result of the record's current attempt (owner-fenced; an
    attempt that already has a result is never overwritten). True when written."""
    columns = _receipt_columns(receipt)
    columns.update(_history_ref(conn, record, receipt.get("assistant_row_id"), columns["final_sha256"]))
    columns["result_session_id"] = receipt.get("result_session_id") or record.session_key
    names = ("outcome", "text_kind", "final_text", "final_sha256", "final_chars", "error_text",
             "result_session_id", "assistant_message_id", "history_ref_kind", "history_persisted",
             "history_text_match")
    cursor = conn.execute(
        "UPDATE submit_accepts SET result_contract = ?, "
        + "".join(f"{name} = ?, " for name in names)
        + "result_attempt = attempts, result_recorded_at = ?, updated_at = ? "
        "WHERE session_key = ? AND client_msg_id = ? AND owner = ? "
        "AND (result_attempt IS NULL OR result_attempt < attempts)",
        (RESULT_CONTRACT, *(columns[name] for name in names), now, now,
         record.session_key, record.client_msg_id, OWNER),
    )
    return bool(cursor.rowcount)


def _same_result(row: dict, receipt: dict) -> bool:
    columns = _receipt_columns(receipt)
    return (row.get("outcome"), row.get("final_sha256")) == (columns["outcome"], columns["final_sha256"])


def _result_recorded(record: Acceptance) -> bool:
    attempt = record.result_row.get("result_attempt")
    return attempt is not None and attempt >= record.attempts


def _refresh_history(conn, record: Acceptance) -> None:
    """Look the result's history reference up once more (the flush may have landed since)."""
    row = record.result_row
    exact = row.get("assistant_message_id") if row.get("history_ref_kind") == "exact" else None
    history = _history_ref(conn, record, exact, row.get("final_sha256"))
    conn.execute(
        "UPDATE submit_accepts SET assistant_message_id = ?, history_ref_kind = ?, "
        "history_persisted = ?, history_text_match = ? WHERE session_key = ? AND client_msg_id = ?",
        (history["assistant_message_id"], history["history_ref_kind"], history["history_persisted"],
         history["history_text_match"], record.session_key, record.client_msg_id),
    )


@contextlib.contextmanager
def _locked(conn, session_key: str, client_msg_id: str) -> Iterator[float]:
    """Transaction holding the id's advisory lock; yields the server clock."""
    with conn:
        root = _lineage_root(conn, session_key)
        aux_xact_lock(conn, f"submit:{root}\0{client_msg_id}", timeout_seconds=LOCK_TIMEOUT_SECONDS)
        yield _server_now(conn)


def claim_submit(
    session_key: str,
    client_msg_id: str,
    text_fingerprint: str,
    *,
    ui_session_id: str = "",
    admit: Callable[[], Any] = lambda: None,
) -> Claim:
    """Decide what a submit carrying *client_msg_id* does, atomically.

    ``NEW``/``RESTART``: this call owns the turn; *admit* ran inside the
    transaction (it takes the in-process turn slot) and returned None, and
    the record is committed as ``accepted`` with this process as the owner.
    If *admit* returns anything else the transaction rolls back and the
    claim is ``REFUSED`` with that value. ``DUPLICATE``: an earlier request
    with the same text owns (or finished) the turn. ``CONFLICT``: the id was
    used for different text. ``RESTART`` only happens for an ``accepted``
    record with no user row above its watermark and no live owner; ``RESUME``
    only for a ``persisted`` record with no live owner and nothing after its
    user row (:data:`NO_REPLY`) — the turn then runs on the stored row.
    """
    conn = open_store()
    registered = None
    try:
        with _locked(conn, session_key, client_msg_id) as now:
            record = _find(conn, session_key, client_msg_id)
            if record is not None:
                if record.fingerprint != text_fingerprint:
                    return Claim(CONFLICT, record)
                record = _reconcile(conn, record, now)
                if record.running or record.state == STATE_COMPLETED or (
                        record.state == STATE_PERSISTED and record.needs_attention != NO_REPLY):
                    return Claim(DUPLICATE, record)
            refusal = admit()
            if refusal is not None:
                conn.rollback()
                return Claim(REFUSED, record, refusal)
            if record is None:
                watermark = _watermark(conn, session_key)
                conn.execute(
                    f"INSERT INTO submit_accepts ({_ACCEPT_COLUMNS}, result_contract) "
                    "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 1, ?, ?, NULL, ?)",
                    (session_key, client_msg_id, text_fingerprint, STATE_ACCEPTED,
                     ui_session_id or None, watermark, OWNER, now + LEASE_SECONDS, now, now,
                     RESULT_CONTRACT),
                )
                outcome = NEW
                record = _find(conn, session_key, client_msg_id)
            else:
                conn.execute(
                    "UPDATE submit_accepts SET owner = ?, lease_until = ?, attempts = attempts + 1, "
                    "ui_session_id = ?, updated_at = ? WHERE session_key = ? AND client_msg_id = ?",
                    (OWNER, now + LEASE_SECONDS, ui_session_id or record.ui_session_id, now,
                     record.session_key, client_msg_id),
                )
                outcome = RESUME if record.state == STATE_PERSISTED else RESTART
                record = _find(conn, record.session_key, client_msg_id)
            registered = record.key
            _register_live(registered)
            record.running = True
            if _prune_due():
                _prune(conn, now)
        return Claim(outcome, record)
    except BaseException:
        if registered is not None:  # the commit failed: nothing durable owns the turn
            _unregister_live(registered)
        raise
    finally:
        conn.close()


def release_submit(claim: Claim) -> None:
    """Undo a claim whose turn never started (a synchronous failure after the claim).

    A fresh record is deleted so the retry starts clean; a restarted or resumed
    one only loses its owner. Only this owner's record is touched.
    """
    record = claim.record
    if record is None or not claim.owns_turn:
        return
    _unregister_live(record.key)
    conn = open_store()
    try:
        with _locked(conn, record.session_key, record.client_msg_id) as now:
            if claim.outcome == NEW:
                conn.execute(
                    "DELETE FROM submit_accepts WHERE session_key = ? AND client_msg_id = ? "
                    "AND owner = ? AND user_message_id IS NULL",
                    (record.session_key, record.client_msg_id, OWNER),
                )
            else:
                conn.execute(
                    "UPDATE submit_accepts SET owner = '', lease_until = 0, updated_at = ? "
                    "WHERE session_key = ? AND client_msg_id = ? AND owner = ?",
                    (now, record.session_key, record.client_msg_id, OWNER),
                )
    finally:
        conn.close()


def record_result(claim: Claim, receipt: dict) -> bool:
    """Record the owned turn's result *receipt* before its ``message.complete`` frame.

    *receipt*: ``outcome`` (``complete`` / ``error`` / ``interrupted``), ``text`` (the
    frame's ``payload.text``), ``error``, ``result_session_id``, ``assistant_row_id``
    and, for a turn that produced no frame text, ``text_kind="none"``. One result per
    attempt: the same result again is a no-op, a different one is logged and dropped.
    Never raises — a failed write leaves the turn's outcome alone (``finish_submit``
    writes it, or ``missing``). True when the attempt's result is this receipt.
    """
    record = claim.record
    if record is None or not claim.owns_turn:
        return False
    try:
        conn = open_store()
        try:
            with _locked(conn, record.session_key, record.client_msg_id) as now:
                current = _find(conn, record.session_key, record.client_msg_id)
                if current is None or current.owner != OWNER or current.attempts != record.attempts:
                    return False
                current = _reconcile(conn, current, now)
                if _result_recorded(current):
                    if _same_result(current.result_row, receipt):
                        return True
                    logger.warning(
                        "submit idempotency: attempt %d of %r already has a different result; kept",
                        current.attempts, current.client_msg_id)
                    return False
                return _store_result(conn, current, receipt, now)
        finally:
            conn.close()
    except Exception:
        logger.warning("submit idempotency: recording the turn result failed", exc_info=True)
        return False


def _settle_result(conn, record: Acceptance, receipt: Optional[dict], now: float) -> None:
    """``finish_submit``'s part of the result: the attempt ends with a result row — the
    receipt ``record_result`` could not write, else ``missing`` — and an unpersisted
    history reference is looked up once more."""
    if _result_recorded(record):
        if not record.result_row.get("history_persisted"):
            _refresh_history(conn, record)
        return
    reason = "no_receipt"
    if receipt is not None:
        conn.execute("SAVEPOINT submit_result")
        try:
            if _store_result(conn, record, receipt, now):
                conn.execute("RELEASE SAVEPOINT submit_result")
                return
        except Exception:
            logger.warning("submit idempotency: storing the turn result failed", exc_info=True)
        conn.execute("ROLLBACK TO SAVEPOINT submit_result")
        reason = "result_store_error"
    _store_result(conn, record, {"outcome": OUTCOME_MISSING, "text_kind": TEXT_NONE, "error": reason}, now)


def finish_submit(claim: Claim, receipt: Optional[dict] = None) -> Optional[Acceptance]:
    """The owned turn ended in this process: ``completed`` when its user row
    exists, else back to ownerless ``accepted`` so a retry runs it once more.
    Fenced on the owner: a record another process reclaimed is left alone.
    *receipt*: the turn's result, written here when ``record_result`` did not;
    without one the attempt's result is ``missing``.
    """
    record = claim.record
    if record is None or not claim.owns_turn:
        return None
    _unregister_live(record.key)
    conn = open_store()
    try:
        with _locked(conn, record.session_key, record.client_msg_id) as now:
            current = _find(conn, record.session_key, record.client_msg_id)
            if current is None or current.owner != OWNER:
                return current
            current = _reconcile(conn, current, now)
            _settle_result(conn, current, receipt, now)
            current.result_row = _result_values(conn.execute(
                f"SELECT {_RESULT_META} FROM submit_accepts WHERE session_key = ? AND client_msg_id = ?",
                (current.session_key, current.client_msg_id)).fetchone())
            if current.user_message_id is not None:
                conn.execute(
                    "UPDATE submit_accepts SET state = ?, owner = '', lease_until = 0, "
                    "completed_at = ?, updated_at = ? WHERE session_key = ? AND client_msg_id = ?",
                    (STATE_COMPLETED, now, now, current.session_key, current.client_msg_id),
                )
                current.state, current.completed_at = STATE_COMPLETED, now
            else:
                conn.execute(
                    "UPDATE submit_accepts SET owner = '', lease_until = 0, updated_at = ? "
                    "WHERE session_key = ? AND client_msg_id = ?",
                    (now, current.session_key, current.client_msg_id),
                )
            current.owner, current.lease_until, current.running = "", 0.0, False
            return current
    finally:
        conn.close()


def lookup_submit(client_msg_id: str, session_key: str = "", *, with_text: bool = False) -> Optional[Acceptance]:
    """The record for *client_msg_id* (anywhere in *session_key*'s conversation when
    given, else the most recent one of any session), reconciled against the messages.
    ``with_text``: also read the result's final text (session-scoped lookups only)."""
    conn = open_store()
    try:
        with conn:
            if session_key:
                root = _lineage_root(conn, session_key)
                aux_xact_lock(
                    conn, f"submit:{root}\0{client_msg_id}", timeout_seconds=LOCK_TIMEOUT_SECONDS)
                record = _find(conn, session_key, client_msg_id, with_text=with_text)
            else:
                row = conn.execute(
                    f"SELECT {_record_columns()} FROM submit_accepts WHERE client_msg_id = ? "
                    "ORDER BY accepted_at DESC LIMIT 1",
                    (client_msg_id,),
                ).fetchone()
                record = None if row is None else _acceptance(row)
                if record is not None:
                    record.lineage_root = _lineage_root(conn, record.session_key)
            if record is None:
                return None
            return _reconcile(conn, record, _server_now(conn))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# session.create records
# ---------------------------------------------------------------------------


@dataclass
class CreateRecord:
    profile: str
    client_create_id: str
    session_id: str
    session_key: str
    created_at: float
    updated_at: float

    def payload(self) -> dict:
        data = asdict(self)
        data["stored_session_id"] = data.pop("session_key")
        return data


class CreateClaim:
    """The open, advisory-locked transaction of one ``client_create_id``.

    ``existing`` is the committed record, if any. :meth:`record` stores the
    ids of a session this request created; :meth:`rebind` moves the record
    to another runtime id. Both commit with the transaction when the block
    exits cleanly; an exception rolls them back.
    """

    def __init__(self, conn, profile: str, client_create_id: str, now: float,
                 existing: Optional[CreateRecord]):
        self._conn = conn
        self.profile = profile
        self.client_create_id = client_create_id
        self.now = now
        self.existing = existing

    def record(self, session_id: str, session_key: str) -> None:
        self._conn.execute(
            "INSERT INTO session_creates (profile, client_create_id, session_id, session_key, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (profile, client_create_id) DO NOTHING",
            (self.profile, self.client_create_id, session_id, session_key, self.now, self.now),
        )

    def rebind(self, session_id: str) -> None:
        self._conn.execute(
            "UPDATE session_creates SET session_id = ?, updated_at = ? "
            "WHERE profile = ? AND client_create_id = ?",
            (session_id, self.now, self.profile, self.client_create_id),
        )

    def touch(self) -> None:
        self._conn.execute(
            "UPDATE session_creates SET updated_at = ? WHERE profile = ? AND client_create_id = ?",
            (self.now, self.profile, self.client_create_id),
        )

    def stored_row_exists(self, session_key: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM sessions WHERE id = ?", (session_key,)).fetchone() is not None


def _create_record(row) -> CreateRecord:
    return CreateRecord(
        profile=row["profile"], client_create_id=row["client_create_id"],
        session_id=row["session_id"], session_key=row["session_key"],
        created_at=float(row["created_at"]), updated_at=float(row["updated_at"]))


def _select_create(conn, profile: str, client_create_id: str) -> Optional[CreateRecord]:
    row = conn.execute(
        "SELECT profile, client_create_id, session_id, session_key, created_at, updated_at "
        "FROM session_creates WHERE profile = ? AND client_create_id = ?",
        (profile, client_create_id),
    ).fetchone()
    return None if row is None else _create_record(row)


@contextlib.contextmanager
def create_claim(profile: str, client_create_id: str) -> Iterator[CreateClaim]:
    """Serialize every request of (*profile*, *client_create_id*), across pods."""
    conn = open_store()
    try:
        with conn:
            aux_xact_lock(
                conn, f"session-create:{profile}\0{client_create_id}",
                timeout_seconds=LOCK_TIMEOUT_SECONDS)
            now = _server_now(conn)
            yield CreateClaim(conn, profile, client_create_id, now,
                              _select_create(conn, profile, client_create_id))
            if _prune_due():
                _prune(conn, now)
    finally:
        conn.close()


def lookup_create(profile: str, client_create_id: str) -> Optional[CreateRecord]:
    conn = open_store()
    try:
        return _select_create(conn, profile, client_create_id)
    finally:
        conn.close()
