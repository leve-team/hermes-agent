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
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterator, Optional, Tuple

from hermes_aux_store import (
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

# Claim outcomes.
NEW = "new"
RESTART = "restart"
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

    @property
    def key(self) -> Tuple[str, str]:
        return (self.session_key, self.client_msg_id)

    def payload(self) -> dict:
        """The wire view (``stored_session_id`` is the session key it was accepted under)."""
        return {
            "client_msg_id": self.client_msg_id,
            "state": self.state,
            "running": self.running,
            "stored_session_id": self.session_key,
            "user_message_id": self.user_message_id,
            "accepted_at": self.accepted_at,
            "completed_at": self.completed_at,
            "attempts": self.attempts,
        }


@dataclass
class Claim:
    outcome: str
    record: Optional[Acceptance] = None
    refusal: Any = None  # what ``admit`` returned when it refused

    @property
    def owns_turn(self) -> bool:
        return self.outcome in (NEW, RESTART)


def _acceptance(row) -> Acceptance:
    values = {name: row[name] for name in _ACCEPT_COLUMNS.split(", ")}
    values["watermark"] = int(values["watermark"] or 0)
    values["attempts"] = int(values["attempts"] or 1)
    if values["user_message_id"] is not None:
        values["user_message_id"] = int(values["user_message_id"])
    return Acceptance(**values)


def _lineage_root(conn, session_key: str) -> str:
    rows = conn.execute(_LINEAGE_UP + "SELECT id FROM up", (session_key,)).fetchall()
    ids = [row[0] for row in rows]
    # The walk yields the key first and its ancestors after it; the oldest
    # compression ancestor is the stable name of the conversation.
    return ids[-1] if ids else session_key


def _find(conn, session_key: str, client_msg_id: str) -> Optional[Acceptance]:
    row = conn.execute(
        _LINEAGE_UP + f"SELECT {_ACCEPT_COLUMNS} FROM submit_accepts "
        "WHERE client_msg_id = ? AND session_key IN (SELECT id FROM up) "
        "ORDER BY accepted_at LIMIT 1",
        (session_key, client_msg_id),
    ).fetchone()
    return None if row is None else _acceptance(row)


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


def _is_live(record: Acceptance, now: float) -> bool:
    if not record.owner:
        return False
    if record.owner == OWNER:
        return is_live_here(record.session_key, record.client_msg_id)
    return record.lease_until > now


def _reconcile(conn, record: Acceptance, now: float) -> Acceptance:
    """Move ``accepted`` to ``persisted`` once the user row exists; set ``running``."""
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
    return record


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
    record with no user row above its watermark and no live owner.
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
                if record.state != STATE_ACCEPTED or record.running:
                    return Claim(DUPLICATE, record)
            refusal = admit()
            if refusal is not None:
                conn.rollback()
                return Claim(REFUSED, record, refusal)
            if record is None:
                watermark = _watermark(conn, session_key)
                conn.execute(
                    f"INSERT INTO submit_accepts ({_ACCEPT_COLUMNS}) "
                    "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 1, ?, ?, NULL)",
                    (session_key, client_msg_id, text_fingerprint, STATE_ACCEPTED,
                     ui_session_id or None, watermark, OWNER, now + LEASE_SECONDS, now, now),
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
                outcome = RESTART
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

    A fresh record is deleted so the retry starts clean; a restarted one goes
    back to ownerless ``accepted``. Only this owner's record is touched.
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


def finish_submit(claim: Claim) -> Optional[Acceptance]:
    """The owned turn ended in this process: ``completed`` when its user row
    exists, else back to ownerless ``accepted`` so a retry runs it once more.
    Fenced on the owner: a record another process reclaimed is left alone.
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


def lookup_submit(client_msg_id: str, session_key: str = "") -> Optional[Acceptance]:
    """The record for *client_msg_id* (within *session_key*'s lineage when given,
    else the most recent one of any session), reconciled against the messages."""
    conn = open_store()
    try:
        with conn:
            if session_key:
                root = _lineage_root(conn, session_key)
                aux_xact_lock(
                    conn, f"submit:{root}\0{client_msg_id}", timeout_seconds=LOCK_TIMEOUT_SECONDS)
                record = _find(conn, session_key, client_msg_id)
            else:
                row = conn.execute(
                    f"SELECT {_ACCEPT_COLUMNS} FROM submit_accepts WHERE client_msg_id = ? "
                    "ORDER BY accepted_at DESC LIMIT 1",
                    (client_msg_id,),
                ).fetchone()
                record = None if row is None else _acceptance(row)
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
