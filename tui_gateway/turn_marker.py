"""Durable interrupted-turn markers for the desktop/TUI auto-continue path.

A running turn's progress lives only in process memory (the agent flushes to
SQLite at turn end, not mid-turn), so an app/backend/machine death mid-turn
leaves no durable trace of the interrupted prompt. This sidecar is that
trace: a marker is written when a turn starts running and cleared when the
turn concludes — success, handled error, or interrupt all clear it, so only
a process death leaves one behind. ``session.resume`` reads the marker to
decide whether to auto-continue the interrupted turn (see
``_maybe_schedule_auto_continue`` in ``tui_gateway/server.py``).

Markers are stored per ``HERMES_HOME`` (callers pass the session's home so
profile sessions keep their state in their own profile directory) and the
file is bounded: writes prune entries older than ``_MAX_AGE_SECS`` and cap
the total count, so an unlucky streak of crashes can't grow it unboundedly.

Every function is best-effort by design — marker bookkeeping must never
break a turn — so I/O errors degrade to "no marker" instead of raising.

On a PostgreSQL-authority profile (levos 0062) the markers are rows of
``core_tui_turn_markers`` keyed by (home, session_key) instead of the sidecar
file, so a pod that resumes the session can see a marker another pod left.
Each row carries its owner and a lease (``gateway.turn_owner``): a marker
whose owner is another live process is a turn still running there, not an
interrupted one, and is not returned. No file is written there, and a
PostgreSQL failure still degrades to "no marker".
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MARKER_DIR = "desktop"
_MARKER_FILE = "interrupted_turns.json"
_MAX_AGE_SECS = 24 * 3600
_MAX_ENTRIES = 32
# Enough to re-submit any realistic prompt; guards the sidecar against a
# pathological multi-megabyte paste being journaled on every turn.
_MAX_PROMPT_CHARS = 64_000

_lock = threading.Lock()


_PG_STORE = "tui_turn_markers"


def _pg_authority() -> bool:
    from hermes_aux_store import aux_store_authority

    return aux_store_authority()


def _pg_initialize(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, _PG_STORE):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS tui_turn_markers (
                 home TEXT NOT NULL,
                 session_key TEXT NOT NULL,
                 prompt TEXT NOT NULL,
                 started_at REAL NOT NULL,
                 attempts INTEGER NOT NULL,
                 owner TEXT NOT NULL,
                 lease_expires_at REAL NOT NULL,
                 PRIMARY KEY (home, session_key)
               )"""
        )


def _pg_open():
    from hermes_aux_store import open_aux_postgres

    return open_aux_postgres(_PG_STORE, initialize=_pg_initialize)


def _pg_renew() -> int:
    from gateway.turn_owner import LEASE_SECONDS, SERVER_EPOCH, owner_id

    conn = _pg_open()
    try:
        return conn.execute(
            f"UPDATE tui_turn_markers SET lease_expires_at = {SERVER_EPOCH} + ? "
            "WHERE owner = ?",
            (float(LEASE_SECONDS), owner_id()),
        ).rowcount
    finally:
        conn.close()


_pg_renewer = None


def _pg_record(home: str, session_key: str, entry: dict) -> None:
    global _pg_renewer
    from gateway.turn_owner import LEASE_SECONDS, SERVER_EPOCH, LeaseRenewer, owner_id

    conn = _pg_open()
    try:
        with conn:
            conn.execute(
                "INSERT INTO tui_turn_markers "
                "(home, session_key, prompt, started_at, attempts, owner, lease_expires_at) "
                f"VALUES (?, ?, ?, ?, ?, ?, {SERVER_EPOCH} + ?) "
                "ON CONFLICT (home, session_key) DO UPDATE SET prompt = EXCLUDED.prompt, "
                "started_at = EXCLUDED.started_at, attempts = EXCLUDED.attempts, "
                "owner = EXCLUDED.owner, lease_expires_at = EXCLUDED.lease_expires_at",
                (
                    home, session_key, entry["prompt"], entry["started_at"],
                    entry["attempts"], owner_id(), float(LEASE_SECONDS),
                ),
            )
            # The file's bounds: age, then the newest _MAX_ENTRIES per home.
            conn.execute(
                "DELETE FROM tui_turn_markers WHERE home = ? AND started_at < ?",
                (home, entry["started_at"] - _MAX_AGE_SECS),
            )
            conn.execute(
                "DELETE FROM tui_turn_markers WHERE home = ? AND session_key IN ("
                "SELECT session_key FROM tui_turn_markers WHERE home = ? "
                "ORDER BY started_at DESC, session_key OFFSET ?)",
                (home, home, _MAX_ENTRIES),
            )
    finally:
        conn.close()
    if _pg_renewer is None:
        _pg_renewer = LeaseRenewer("tui-turn-marker-lease", _pg_renew)
    _pg_renewer.start()


def _pg_clear(home: str, session_key: str) -> None:
    conn = _pg_open()
    try:
        conn.execute(
            "DELETE FROM tui_turn_markers WHERE home = ? AND session_key = ?",
            (home, session_key),
        )
    finally:
        conn.close()


def _pg_read(home: str, session_key: str) -> dict | None:
    """The row, unless its owner is another process that is still alive."""
    from gateway.turn_owner import SERVER_EPOCH, owner_gone, owner_id

    conn = _pg_open()
    try:
        row = conn.execute(
            "SELECT prompt, started_at, attempts, owner, "
            f"lease_expires_at < {SERVER_EPOCH} AS expired "
            "FROM tui_turn_markers WHERE home = ? AND session_key = ?",
            (home, session_key),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    owner = row["owner"]
    if owner != owner_id() and not (row["expired"] or owner_gone(owner)):
        return None  # running in another live process (another pod)
    return {
        "attempts": row["attempts"],
        "prompt": row["prompt"],
        "started_at": row["started_at"],
    }


def _marker_path(home: Path | str) -> Path:
    return Path(home) / _MARKER_DIR / _MARKER_FILE


def _load(path: Path) -> dict[str, dict]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        logger.debug("unreadable turn-marker file %s; starting fresh", path, exc_info=True)
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if isinstance(v, dict)}


def _prune(entries: dict[str, dict], now: float) -> dict[str, dict]:
    fresh = {
        key: entry
        for key, entry in entries.items()
        if now - float(entry.get("started_at") or 0) <= _MAX_AGE_SECS
    }
    if len(fresh) <= _MAX_ENTRIES:
        return fresh
    newest = sorted(
        fresh.items(),
        key=lambda item: float(item[1].get("started_at") or 0),
        reverse=True,
    )[:_MAX_ENTRIES]
    return dict(newest)


def _store(path: Path, entries: dict[str, dict]) -> None:
    if not entries:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".turn-marker-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(entries, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def record_turn_start(
    home: Path | str, session_key: str, prompt: str, *, attempts: int = 0
) -> None:
    """Persist the marker for a turn that is about to run.

    ``attempts`` counts how many auto-continues led to this run: 0 for a
    user-initiated turn, N for the Nth automatic re-run — the crash-loop
    breaker reads it back on the next resume.
    """
    if not session_key or not prompt:
        return
    now = time.time()
    entry = {
        "attempts": max(0, int(attempts)),
        "prompt": prompt[:_MAX_PROMPT_CHARS],
        "started_at": now,
    }
    try:
        if _pg_authority():
            _pg_record(str(Path(home)), session_key, entry)
            return
        with _lock:
            path = _marker_path(home)
            entries = _prune(_load(path), now)
            entries[session_key] = entry
            _store(path, entries)
    except Exception:
        logger.debug("failed to record turn marker for %s", session_key, exc_info=True)


def clear_turn_marker(home: Path | str, session_key: str) -> None:
    """Remove the marker once its turn concluded (any outcome the client saw)."""
    if not session_key:
        return
    try:
        if _pg_authority():
            _pg_clear(str(Path(home)), session_key)
            return
        with _lock:
            path = _marker_path(home)
            entries = _load(path)
            if session_key not in entries:
                return
            del entries[session_key]
            _store(path, entries)
    except Exception:
        logger.debug("failed to clear turn marker for %s", session_key, exc_info=True)


def read_turn_marker(home: Path | str, session_key: str) -> dict[str, Any] | None:
    """The marker left by a turn that never concluded, or None."""
    if not session_key:
        return None
    try:
        if _pg_authority():
            entry = _pg_read(str(Path(home)), session_key)
        else:
            with _lock:
                entry = _load(_marker_path(home)).get(session_key)
    except Exception:
        return None
    if not isinstance(entry, dict):
        return None
    prompt = str(entry.get("prompt") or "")
    if not prompt.strip():
        return None
    try:
        started_at = float(entry.get("started_at") or 0)
        attempts = max(0, int(entry.get("attempts") or 0))
    except (TypeError, ValueError):
        return None
    return {"attempts": attempts, "prompt": prompt, "started_at": started_at}
