"""Flush pending messages and agent transcripts to disk before shutdown to prevent data loss.

When FTS5 index corruption prevents ``INSERT INTO messages``, the gateway
accumulates messages in ``_pending_messages`` (memory-only) and the live
``agent._session_messages`` cannot be flushed via ``_flush_messages_to_session_db``.
On shutdown, ``.clear()`` discards the only surviving copy — permanent user data loss.

This module provides three hooks:

1. ``flush_pending_to_file()`` — called BEFORE ``_pending_messages.clear()``
   during shutdown.  Serialises any non-empty pending slots to a JSON file
   under ``<hermes_home>/pending_messages/``.

2. ``recover_pending_to_db()`` — called AFTER ``runner.start()`` on startup.
   Reads flush files, inserts messages into state.db via ``SessionDB.append_message``
   (so FTS indexing, session metadata, and display_kind are handled correctly),
   then deletes the flush file on success.

3. ``flush_agent_history_to_file()`` — called from ``_finalize_shutdown_agents``
   when ``_flush_messages_to_session_db`` raises.  Dumps the live
   ``agent._session_messages`` to the same atomic JSON recovery directory.

See issue #72680 for the full incident report.

On a PostgreSQL-authority profile (levos 0062) the spool is the table
``core_gateway_pending_messages`` in the profile's store instead of the
directory: a pod's disk dies with the pod, so an overlapping or later pod could
never recover what it wrote. Nothing is written under ``pending_messages/``
there and a PostgreSQL failure is not answered with a file. Recovery claims
rows with ``FOR UPDATE SKIP LOCKED`` so two pods never replay the same one.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union

logger = logging.getLogger(__name__)

_PG_STORE = "gateway_pending"
_PG_LOCATION = "postgres:core_gateway_pending_messages"
_AGENT_HISTORY_REASON = "shutdown-with-unpersisted-agent-history"


def _pg_authority() -> bool:
    from hermes_aux_store import aux_store_authority

    return aux_store_authority()


def _pg_initialize(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, _PG_STORE):
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
def _pg_queue() -> Iterator[Any]:
    from hermes_aux_store import open_aux_postgres

    conn = open_aux_postgres(_PG_STORE, initialize=_pg_initialize)
    try:
        yield conn
    finally:
        conn.close()


def _pg_insert(conn, payload: Dict[str, Any]) -> str:
    cursor = conn.execute(
        "INSERT INTO gateway_pending_messages "
        "(session_key, reason, ts, seq, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            payload.get("session_key") or payload.get("session_id"),
            payload.get("reason"),
            payload.get("ts"),
            payload.get("seq"),
            json.dumps(payload, default=str),
            time.time(),
        ),
    )
    return f"{_PG_LOCATION}/{cursor.lastrowid}"


def _publish(payload: Dict[str, Any]) -> Union[Path, str]:
    """Store one recovery payload where this profile's successor will look."""
    if _pg_authority():
        with _pg_queue() as conn:
            return _pg_insert(conn, payload)
    return _write_payload(_get_flush_dir(), payload)


def _get_flush_dir():
    """Return the pending-messages flush directory under the active HERMES_HOME."""
    from hermes_constants import get_hermes_home

    flush_dir = get_hermes_home() / "pending_messages"
    flush_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "posix":
        os.chmod(flush_dir, 0o700)
    return flush_dir


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry on platforms that support directory fsync."""
    if os.name != "posix":
        return
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_payload(flush_dir: Path, payload: Dict[str, Any]) -> Path:
    """Atomically write one private, uniquely named recovery payload.

    Returns the path of the published payload file.
    """
    from utils import atomic_json_write

    file_id = uuid.uuid4().hex
    final_path = flush_dir / f"pending-{file_id}.json"
    atomic_json_write(
        final_path,
        payload,
        mode=0o600,
        default=str,
    )

    try:
        _fsync_directory(flush_dir)
    except OSError as exc:
        # The atomically published file is still the only recovery copy.
        # Keep it even if this filesystem cannot persist directory entries.
        logger.debug("Failed to fsync pending-message directory: %s", exc)
    return final_path


def flush_pending_to_file(
    pending: Dict[str, Any],
    *,
    reason: str = "shutdown",
) -> int:
    """Serialise non-empty ``_pending_messages`` slots to disk.

    Parameters
    ----------
    pending:
        The adapter or runner ``_pending_messages`` dict.  Values may be
        ``MessageEvent`` objects (adapter) or plain strings (runner).
    reason:
        Logged context (``shutdown``, ``restart``, etc.).

    Returns
    -------
    int
        Number of sessions flushed.
    """
    if not pending:
        return 0
    if _pg_authority():
        return _pg_flush_pending(pending, reason=reason)

    flush_dir = _get_flush_dir()
    ts = int(time.time())
    flushed = 0

    for session_key, value in list(pending.items()):
        if value is None:
            continue
        try:
            serialised = _serialise_value(value)
            if serialised is None:
                continue
            _write_payload(
                flush_dir,
                {
                    "session_key": session_key,
                    "reason": reason,
                    "ts": ts,
                    "data": serialised,
                },
            )
            flushed += 1
        except Exception as exc:
            logger.debug(
                "Failed to flush pending message for %s: %s",
                session_key, exc,
            )

    if flushed:
        logger.info(
            "Flushed %d pending message(s) to %s (reason=%s)",
            flushed, flush_dir, reason,
        )
    return flushed


def _pg_flush_pending(pending: Dict[str, Any], *, reason: str) -> int:
    """PostgreSQL form of :func:`flush_pending_to_file`: one transaction."""
    ts = int(time.time())
    payloads = []
    for session_key, value in list(pending.items()):
        if value is None:
            continue
        serialised = _serialise_value(value)
        if serialised is None:
            continue
        payloads.append(
            {"session_key": session_key, "reason": reason, "ts": ts, "data": serialised}
        )
    if not payloads:
        return 0
    try:
        with _pg_queue() as conn, conn:
            for payload in payloads:
                _pg_insert(conn, payload)
    except Exception as exc:
        # No file fallback on authority: the pod disk is gone for any successor.
        logger.error(
            "Could not flush %d pending message(s) to PostgreSQL (reason=%s): %s",
            len(payloads), reason, exc,
        )
        return 0
    logger.info(
        "Flushed %d pending message(s) to %s (reason=%s)",
        len(payloads), _PG_LOCATION, reason,
    )
    return len(payloads)


# Reason tag for transcript messages dropped by the in-memory pending cap
# during live operation (#78182). These payloads carry the full transcript
# message dict so they can be replayed verbatim once the DB recovers.
TRANSCRIPT_CAP_DROP_REASON = "transcript_cap_drop"


def spool_dropped_transcript_message(
    session_id: str,
    message: Dict[str, Any],
) -> Optional[Union[Path, str]]:
    """Spool a transcript message evicted by the runtime pending cap.

    Uses the same on-disk pending spool as :func:`flush_pending_to_file`
    (one atomic JSON payload per message under
    ``<hermes_home>/pending_messages/``), so a runtime cap rotation no
    longer silently discards user data while the process stays up
    (#78182). On PostgreSQL authority the spool is the pending-message
    table and the return value names its row.

    Returns the written spool path, or ``None`` when spooling failed —
    callers must degrade to the previous drop-and-log behaviour.
    """
    try:
        return _publish(
            {
                "session_key": session_id,
                "reason": TRANSCRIPT_CAP_DROP_REASON,
                "ts": int(time.time()),
                "seq": next(_TRANSCRIPT_SPOOL_SEQ),
                "data": {
                    "session_id": session_id,
                    "message": message,
                },
            },
        )
    except Exception as exc:
        logger.debug(
            "Failed to spool cap-dropped transcript message for %s: %s",
            session_id, exc,
        )
        return None


# Monotonic tiebreaker so same-second spool files replay in drop order.
_TRANSCRIPT_SPOOL_SEQ = itertools.count()


def drain_transcript_spool(session_id: str, replay) -> tuple[int, int]:
    """Replay cap-dropped transcript messages spooled for *session_id*.

    ``replay(message_dict)`` is invoked for each spooled message in drop
    order; the spool file is deleted only after a successful replay.  On
    the first replay failure the drain stops and remaining files are kept
    for the next attempt (the DB is likely still unhealthy).

    Returns ``(replayed, remaining)`` — messages replayed and spool files
    left behind for a later retry.
    """
    if _pg_authority():
        return _pg_drain_transcript_spool(session_id, replay)
    try:
        flush_dir = _get_flush_dir()
        candidates = list(flush_dir.glob("pending-*.json"))
    except Exception as exc:
        logger.debug("Cannot scan transcript spool: %s", exc)
        return 0, 0

    entries = []
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("reason") != TRANSCRIPT_CAP_DROP_REASON:
            continue
        if payload.get("session_key") != session_id:
            continue
        message = (payload.get("data") or {}).get("message")
        if not isinstance(message, dict):
            logger.warning(
                "Removing structurally invalid transcript spool file %s", path,
            )
            path.unlink(missing_ok=True)
            continue
        entries.append(
            (payload.get("ts", 0), payload.get("seq", 0), path.name, path, message)
        )

    replayed = 0
    ordered = sorted(entries, key=lambda e: e[:3])
    remaining = 0
    for idx, (_ts, _seq, _name, path, message) in enumerate(ordered):
        try:
            replay(message)
        except Exception as exc:
            logger.warning(
                "Replay of spooled transcript message %s for %s failed; "
                "keeping spool file for retry: %s",
                path, session_id, exc,
            )
            remaining = len(ordered) - idx
            break
        path.unlink(missing_ok=True)
        replayed += 1

    if replayed:
        logger.info(
            "Replayed %d spooled transcript message(s) for %s after DB recovery",
            replayed, session_id,
        )
    return replayed, remaining


def _pg_drain_transcript_spool(session_id: str, replay) -> tuple[int, int]:
    """PostgreSQL form of :func:`drain_transcript_spool`.

    The rows stay locked (``SKIP LOCKED`` for other pods) until each one is
    replayed and deleted, so a peer cannot replay the same message.
    """
    replayed = 0
    remaining = 0
    try:
        with _pg_queue() as conn, conn:
            rows = conn.execute(
                "SELECT id, payload FROM gateway_pending_messages "
                "WHERE reason = ? AND session_key = ? "
                "ORDER BY ts, seq, id FOR UPDATE SKIP LOCKED",
                (TRANSCRIPT_CAP_DROP_REASON, session_id),
            ).fetchall()
            entries = []
            for row in rows:
                try:
                    message = (json.loads(row["payload"]).get("data") or {}).get("message")
                except (ValueError, AttributeError):
                    message = None
                if not isinstance(message, dict):
                    logger.warning(
                        "Removing structurally invalid transcript spool row %s/%s",
                        _PG_LOCATION, row["id"],
                    )
                    conn.execute(
                        "DELETE FROM gateway_pending_messages WHERE id = ?", (row["id"],)
                    )
                    continue
                entries.append((row["id"], message))
            for idx, (row_id, message) in enumerate(entries):
                try:
                    replay(message)
                except Exception as exc:
                    logger.warning(
                        "Replay of spooled transcript message %s/%s for %s failed; "
                        "keeping it for retry: %s",
                        _PG_LOCATION, row_id, session_id, exc,
                    )
                    remaining = len(entries) - idx
                    break
                conn.execute("DELETE FROM gateway_pending_messages WHERE id = ?", (row_id,))
                replayed += 1
    except Exception as exc:
        logger.debug("Cannot drain the PostgreSQL transcript spool: %s", exc)
        return 0, 0

    if replayed:
        logger.info(
            "Replayed %d spooled transcript message(s) for %s after DB recovery",
            replayed, session_id,
        )
    return replayed, remaining


def _serialise_value(value: Any) -> Optional[dict]:
    """Convert a pending message value to a JSON-serialisable dict."""
    # MessageEvent objects have a .text attribute and other fields
    if hasattr(value, "text"):
        result: Dict[str, Any] = {"text": getattr(value, "text", "")}
        # Preserve additional fields if present
        for attr in ("session_id", "platform", "sender_id", "sender_name",
                      "reply_to", "media", "raw_event"):
            val = getattr(value, attr, None)
            if val is not None:
                try:
                    json.dumps(val)
                    result[attr] = val
                except (TypeError, ValueError):
                    result[attr] = str(val)
        return result
    # Plain string (runner-level _pending_messages)
    if isinstance(value, str):
        return {"text": value}
    # Dict — try direct serialisation
    if isinstance(value, dict):
        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return {"text": str(value)}
    return {"text": str(value)}


def recover_pending_to_db(
    session_db=None,
) -> int:
    """Recover flushed pending messages into state.db via SessionDB.

    Reads all ``*.json`` files from the flush directory, inserts messages
    using ``SessionDB.append_message`` (so FTS indexing, session metadata
    updates, and all required columns are handled correctly), and deletes
    the flush file on success.

    On PostgreSQL authority the rows of the pending-message table are
    recovered instead (plus, once, any files a pre-0062 gateway left).

    Parameters
    ----------
    session_db:
        An existing ``SessionDB`` instance.  If ``None``, a new one is
        opened on the default ``state.db`` path.

    Returns
    -------
    int
        Number of messages recovered.
    """
    if _pg_authority():
        return _pg_recover_pending_to_db(session_db)
    return _recover_pending_files(session_db)


def _recover_pending_files(session_db=None) -> int:
    flush_dir = _get_flush_dir()
    flush_files = sorted(flush_dir.glob("*.json"))
    if not flush_files:
        return 0

    # Use the provided SessionDB or open one on the default path.
    own_db = False
    if session_db is None:
        from hermes_state import SessionDB
        session_db = SessionDB()
        own_db = True

    def _close_owned_db() -> None:
        if not own_db:
            return
        try:
            session_db.close()
        except Exception:
            pass

    recovered = 0
    for path in flush_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if _replay_payload(payload, session_db, path):
                recovered += 1
                path.unlink(missing_ok=True)
        except BaseException:
            # Shutdown cancellation/interrupt must not strand an owned DB.
            _close_owned_db()
            raise
        except Exception as exc:
            logger.warning(
                "Failed to recover pending message from %s: %s",
                path, exc,
            )
            # Leave the file for next startup retry.

    _close_owned_db()

    if recovered:
        logger.info(
            "Recovered %d pending message(s) from shutdown flush", recovered,
        )
    return recovered


def _replay_payload(payload: Dict[str, Any], session_db, source: Any) -> bool:
    """Append one recovery payload to *session_db*.

    True when it was appended (the caller then deletes it); False when it
    must be preserved. *source* only names the payload in log lines.
    """
    # Agent-history snapshots use a different schema (reason +
    # messages list) and are meant for manual operator recovery,
    # not automatic DB insertion. Skip them silently.
    if payload.get("reason") == _AGENT_HISTORY_REASON:
        return False
    # Cap-dropped transcript payloads carry the full message dict
    # keyed by session_id — replay directly (#78182). This handles
    # spool files that were never drained before a restart.
    if payload.get("reason") == TRANSCRIPT_CAP_DROP_REASON:
        data = payload.get("data", {}) or {}
        spooled_sid = data.get("session_id", "")
        message = data.get("message")
        if not spooled_sid or not isinstance(message, dict):
            logger.warning(
                "Cannot recover structurally invalid transcript spool "
                "file %s; preserved for manual inspection",
                source,
            )
            return False
        session_db.append_message(
            session_id=spooled_sid,
            role=message.get("role", "unknown"),
            content=message.get("content") or "",
            timestamp=message.get("timestamp") or payload.get("ts"),
        )
        return True
    session_key = payload.get("session_key", "")
    data = payload.get("data", {})
    text = data.get("text", "")
    if not text or not session_key:
        logger.warning(
            "Cannot recover structurally invalid pending message from %s; "
            "the flush file has been preserved",
            source,
        )
        return False

    # The session_key is a gateway routing key (e.g.
    # "agent:main:telegram:supergroup:...").  We need the actual
    # session_id (e.g. "20260728_120000_abc123") to append a
    # message row.  Try the session_id field from the serialised
    # data first; fall back to scanning sessions for a matching
    # session_key in the source column.
    session_id = data.get("session_id", "")

    if not session_id:
        # Try to extract from the session_key itself — gateway
        # session keys contain the session_id as the last segment
        # in some formats, but that's not guaranteed.  Log and
        # skip if we can't resolve it.
        logger.warning(
            "Cannot recover pending message for %s: no session_id "
            "in flush file and session_key-to-id resolution is not "
            "available at this recovery stage. The message text is "
            "preserved in %s",
            session_key, source,
        )
        return False

    session_db.append_message(
        session_id=session_id,
        role="user",
        content=text,
        timestamp=payload.get("ts", int(time.time())),
    )
    return True


def _pg_recover_pending_to_db(session_db=None) -> int:
    """PostgreSQL form of :func:`recover_pending_to_db`.

    Each row is appended and deleted while this transaction holds its row
    lock; a concurrent peer skips locked rows, so no message is replayed
    twice. A row that cannot be replayed stays for the next pass.
    """
    from hermes_constants import get_hermes_home

    recovered = 0
    # Files a gateway older than levos 0062 flushed before this profile's
    # pod moved to authority: read (and consumed) once, never written.
    legacy_dir = get_hermes_home() / "pending_messages"
    if legacy_dir.is_dir() and any(legacy_dir.glob("*.json")):
        recovered += _recover_pending_files(session_db)

    own_db = False
    try:
        with _pg_queue() as conn, conn:
            rows = conn.execute(
                "SELECT id, payload FROM gateway_pending_messages "
                "WHERE reason IS DISTINCT FROM ? ORDER BY id FOR UPDATE SKIP LOCKED",
                (_AGENT_HISTORY_REASON,),
            ).fetchall()
            if rows and session_db is None:
                from hermes_state import SessionDB

                session_db = SessionDB()
                own_db = True
            for row in rows:
                source = f"{_PG_LOCATION}/{row['id']}"
                try:
                    replayed = _replay_payload(json.loads(row["payload"]), session_db, source)
                except Exception as exc:
                    logger.warning("Failed to recover pending message from %s: %s", source, exc)
                    continue
                if replayed:
                    conn.execute(
                        "DELETE FROM gateway_pending_messages WHERE id = ?", (row["id"],)
                    )
                    recovered += 1
    finally:
        if own_db:
            try:
                session_db.close()
            except Exception:
                pass

    if recovered:
        logger.info(
            "Recovered %d pending message(s) from shutdown flush", recovered,
        )
    return recovered


def flush_agent_history_to_file(
    session_id: Optional[str],
    history: list,
) -> None:
    """Best-effort dump of an agent's in-memory transcript before teardown.

    Used when ``_flush_messages_to_session_db`` raises (e.g. FTS/SQLite
    index corruption, #72680): the live ``agent._session_messages`` could
    not be written to disk, and a plain debug log would lose it permanently
    when the process exits. Serialize to an atomic JSON file outside the
    broken DB so an operator can salvage the conversation after repairing
    state.db.

    Failures are swallowed — shutdown must never block on a best-effort
    backup.
    """
    if not history:
        return
    try:
        snapshot = []
        for _m in history:
            try:
                snapshot.append(
                    _m if isinstance(_m, (dict, list, str, int, float, bool, type(None)))
                    else str(_m)
                )
            except Exception:
                continue
        _publish(
            {
                "reason": _AGENT_HISTORY_REASON,
                "issue": "#72680",
                "session_id": session_id,
                "count": len(snapshot),
                "messages": snapshot,
            },
        )
        logger.warning(
            "Preserved %d in-memory message(s) for session %s "
            "(possible FTS corruption — recover after repairing state.db)",
            len(snapshot),
            session_id,
        )
    except Exception as _e:
        logger.warning(
            "Agent-history shutdown preservation failed for session %s: %s",
            session_id, _e,
        )
