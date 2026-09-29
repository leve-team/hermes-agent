"""The pending-message spool on a PostgreSQL-authority profile (levos 0062).

``gateway.shutdown_flush`` keeps its recovery payloads under ``<hermes_home>/pending_messages/``.
A pod's disk dies with the pod, so an overlapping or later pod could never recover what it wrote:
on authority the spool is the table ``core_gateway_pending_messages`` in the profile's store
instead. Nothing is written under ``pending_messages/`` and a PostgreSQL failure is not answered
with a file. Recovery and drain claim rows with ``FOR UPDATE SKIP LOCKED`` so two pods never replay
the same one.
"""

from __future__ import annotations

import contextlib
import json
import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

logger = logging.getLogger("gateway.shutdown_flush")

_STORE = "gateway_pending"
LOCATION = "postgres:core_gateway_pending_messages"


def enabled() -> bool:
    from hermes_aux_store import aux_store_authority

    return aux_store_authority()


def _initialize(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, _STORE):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gateway_pending_messages (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 session_key TEXT,
                 reason TEXT,
                 ts REAL,
                 seq INTEGER,
                 payload TEXT NOT NULL,
                 created_at REAL NOT NULL
               )"""
        )


@contextlib.contextmanager
def _queue() -> Iterator[Any]:
    from hermes_aux_store import open_aux_postgres

    conn = open_aux_postgres(_STORE, initialize=_initialize)
    try:
        yield conn
    finally:
        conn.close()


def _insert(conn, payload: Dict[str, Any]) -> str:
    cursor = conn.execute(
        "INSERT INTO gateway_pending_messages "
        "(session_key, reason, ts, seq, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (payload.get("session_key") or payload.get("session_id"), payload.get("reason"),
         payload.get("ts"), payload.get("seq"), json.dumps(payload, default=str), time.time()),
    )
    return f"{LOCATION}/{cursor.lastrowid}"


def publish(payload: Dict[str, Any]) -> str:
    """Store one recovery payload; returns the row's location. Raises on a PostgreSQL failure."""
    with _queue() as conn:
        return _insert(conn, payload)


def publish_many(payloads: List[Dict[str, Any]], *, what: str, reason: str) -> int:
    """Store *payloads* in one transaction; the count stored (0 after a logged failure)."""
    if not payloads:
        return 0
    try:
        with _queue() as conn, conn:
            for payload in payloads:
                _insert(conn, payload)
    except Exception as exc:
        # No file fallback on authority: the pod disk is gone for any successor.
        logger.error("Could not flush %d %s message(s) to PostgreSQL (reason=%s): %s",
                     len(payloads), what, reason, exc)
        return 0
    logger.info("Flushed %d %s message(s) to %s (reason=%s)", len(payloads), what, LOCATION, reason)
    return len(payloads)


def drain_transcript_spool(session_id: str, replay: Callable[[dict], Any], *, cap_reason: str) -> tuple[int, int]:
    """PostgreSQL form of ``shutdown_flush.drain_transcript_spool``. The rows stay locked
    (``SKIP LOCKED`` for other pods) until each one is replayed and deleted."""
    replayed = remaining = 0
    try:
        with _queue() as conn, conn:
            rows = conn.execute(
                "SELECT id, payload FROM gateway_pending_messages WHERE reason = ? AND session_key = ? "
                "ORDER BY ts, seq, id FOR UPDATE SKIP LOCKED",
                (cap_reason, session_id),
            ).fetchall()
            entries = []
            for row in rows:
                try:
                    message = (json.loads(row["payload"]).get("data") or {}).get("message")
                except (ValueError, AttributeError):
                    message = None
                if not isinstance(message, dict):
                    logger.warning("Removing structurally invalid transcript spool row %s/%s", LOCATION, row["id"])
                    conn.execute("DELETE FROM gateway_pending_messages WHERE id = ?", (row["id"],))
                    continue
                entries.append((row["id"], message))
            for idx, (row_id, message) in enumerate(entries):
                try:
                    replay(message)
                except Exception as exc:
                    logger.warning("Replay of spooled transcript message %s/%s for %s failed; "
                                   "keeping it for retry: %s", LOCATION, row_id, session_id, exc)
                    remaining = len(entries) - idx
                    break
                conn.execute("DELETE FROM gateway_pending_messages WHERE id = ?", (row_id,))
                replayed += 1
    except Exception as exc:
        logger.debug("Cannot drain the PostgreSQL transcript spool: %s", exc)
        return 0, 0
    if replayed:
        logger.info("Replayed %d spooled transcript message(s) for %s after DB recovery", replayed, session_id)
    return replayed, remaining


def recover_to_db(session_db, replay_one: Callable[[Any, str, Dict[str, Any]], bool], *,
                  skip_reason: str) -> int:
    """Replay every row except operator snapshots (*skip_reason*) through ``replay_one(db, source,
    payload)``; a replayed row is deleted while this transaction holds its lock, a row that cannot
    be replayed stays for the next pass. A PostgreSQL failure raises."""
    recovered = 0
    own_db: Optional[Any] = None
    try:
        with _queue() as conn, conn:
            rows = conn.execute(
                "SELECT id, payload FROM gateway_pending_messages "
                "WHERE reason IS DISTINCT FROM ? ORDER BY id FOR UPDATE SKIP LOCKED",
                (skip_reason,),
            ).fetchall()
            if rows and session_db is None:
                from hermes_state_registry import acquire

                session_db = own_db = acquire()
            for row in rows:
                source = f"{LOCATION}/{row['id']}"
                try:
                    replayed = replay_one(session_db, source, json.loads(row["payload"]))
                except Exception as exc:
                    logger.warning("Failed to recover pending message from %s: %s", source, exc)
                    continue
                if replayed:
                    conn.execute("DELETE FROM gateway_pending_messages WHERE id = ?", (row["id"],))
                    recovered += 1
    finally:
        if own_db is not None:
            with contextlib.suppress(Exception):
                from hermes_state_registry import release_or_close

                release_or_close(own_db)
    return recovered
