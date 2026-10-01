"""Durable interrupted-turn markers for the desktop/TUI auto-continue path. A running turn's progress
lives only in process memory (the agent flushes to SQLite at turn end), so a marker is written at turn
start and cleared on any conclusion — only a process death leaves one behind, and ``session.resume``
reads it (``_maybe_schedule_auto_continue``). Stored per ``HERMES_HOME`` (profile-aware); writes prune
entries older than ``_MAX_AGE_SECS`` and cap the count so a crash streak can't grow the file. Every
function is best-effort — marker bookkeeping must never break a turn — so I/O errors degrade to "no
marker" instead of raising.

On a PostgreSQL-authority profile (levos 0062) the markers are rows of ``core_tui_turn_markers``
instead of ``desktop/interrupted_turns.json``: the client may resume on another pod than the one that
ran the turn. Each row carries its owner process and a lease on the PostgreSQL server clock (the
gateway turn-lease model, ``gateway.turn_owner``); a marker whose owner is another live process is
still running there, not interrupted, and is not returned. No file is written, and a PostgreSQL
failure still degrades to "no marker".

A turn whose thread still runs in THIS process is not interrupted either, on any store
(``turn_running_here``): a WS drop reaps the session record, not the turn, so the next
``session.resume`` finds the marker of a live turn. The owner id cannot tell: it is this process for
that turn and for a row an earlier process with the same id left behind, and the lease renewer keeps
every row of the id alive (``_pg_renew``). The registry of turn threads can; it dies with the process."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MAX_AGE_SECS = 24 * 3600
_MAX_ENTRIES = 32
# Enough to re-submit any realistic prompt; guards against a multi-megabyte paste being journaled.
_MAX_PROMPT_CHARS = 64_000

_lock = threading.Lock()

# (home, session_key) -> the turn threads of this process running it; a thread counts until it ends.
_running_lock = threading.Lock()
_running: dict[tuple[str, str], list[threading.Thread]] = {}


_PG_STORE = "tui_turn_markers"
_pg_renewer = None


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
                 auto_continue INTEGER NOT NULL,
                 owner TEXT NOT NULL,
                 lease_expires_at REAL NOT NULL,
                 PRIMARY KEY (home, session_key)
               )"""
        )


@contextlib.contextmanager
def _pg_store():
    from hermes_aux_store import open_aux_postgres

    conn = open_aux_postgres(_PG_STORE, initialize=_pg_initialize)
    try:
        yield conn
    finally:
        conn.close()


def _pg_renew() -> int:
    from gateway.turn_owner import LEASE_SECONDS, SERVER_EPOCH, owner_id

    with _pg_store() as conn:
        return conn.execute(
            f"UPDATE tui_turn_markers SET lease_expires_at = {SERVER_EPOCH} + ? WHERE owner = ?",
            (float(LEASE_SECONDS), owner_id()),
        ).rowcount


def _pg_record(home: str, session_key: str, entry: dict) -> None:
    global _pg_renewer
    from gateway.turn_owner import LEASE_SECONDS, SERVER_EPOCH, lease_renewer, owner_id

    with _pg_store() as conn, conn:
        conn.execute(
            "INSERT INTO tui_turn_markers (home, session_key, prompt, started_at, attempts, auto_continue, "
            f"owner, lease_expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, {SERVER_EPOCH} + ?) "
            "ON CONFLICT (home, session_key) DO UPDATE SET prompt = EXCLUDED.prompt, "
            "started_at = EXCLUDED.started_at, attempts = EXCLUDED.attempts, "
            "auto_continue = EXCLUDED.auto_continue, owner = EXCLUDED.owner, "
            "lease_expires_at = EXCLUDED.lease_expires_at",
            (home, session_key, entry["prompt"], entry["started_at"], entry["attempts"],
             int(entry["auto_continue"]), owner_id(), float(LEASE_SECONDS)),
        )
        # The file's bounds: age, then the newest _MAX_ENTRIES per home.
        conn.execute("DELETE FROM tui_turn_markers WHERE home = ? AND started_at < ?",
                     (home, entry["started_at"] - _MAX_AGE_SECS))
        conn.execute(
            "DELETE FROM tui_turn_markers WHERE home = ? AND session_key IN ("
            "SELECT session_key FROM tui_turn_markers WHERE home = ? "
            "ORDER BY started_at DESC, session_key OFFSET ?)",
            (home, home, _MAX_ENTRIES),
        )
    if _pg_renewer is None:
        _pg_renewer = lease_renewer("tui-turn-marker-lease", _pg_renew)
    _pg_renewer.start()


def _pg_clear(home: str, session_key: str) -> None:
    with _pg_store() as conn:
        conn.execute("DELETE FROM tui_turn_markers WHERE home = ? AND session_key = ?", (home, session_key))


def _pg_read(home: str, session_key: str) -> dict | None:
    """The row, unless its owner is another process that is still alive."""
    from gateway.turn_owner import SERVER_EPOCH, owner_gone, owner_id

    with _pg_store() as conn:
        row = conn.execute(
            f"SELECT prompt, started_at, attempts, auto_continue, owner, lease_expires_at < {SERVER_EPOCH} "
            "AS expired FROM tui_turn_markers WHERE home = ? AND session_key = ?",
            (home, session_key),
        ).fetchone()
    if row is None:
        return None
    owner = row["owner"]
    if owner != owner_id() and not (row["expired"] or owner_gone(owner)):
        return None  # running in another live process (another pod)
    return {"attempts": row["attempts"], "prompt": row["prompt"], "started_at": row["started_at"],
            "auto_continue": bool(row["auto_continue"])}


def _marker_path(home: Path | str) -> Path:
    return Path(home) / "desktop" / "interrupted_turns.json"


def _started_at(entry: dict) -> float:
    return float(entry.get("started_at") or 0)


def _load(path: Path) -> dict[str, dict]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        logger.debug("unreadable turn-marker file %s; starting fresh", path, exc_info=True)
        return {}
    return {k: v for k, v in data.items() if isinstance(v, dict)} if isinstance(data, dict) else {}


def _prune(entries: dict[str, dict], now: float) -> dict[str, dict]:
    fresh = {k: e for k, e in entries.items() if now - _started_at(e) <= _MAX_AGE_SECS}
    if len(fresh) <= _MAX_ENTRIES:
        return fresh
    return dict(sorted(fresh.items(), key=lambda item: _started_at(item[1]), reverse=True)[:_MAX_ENTRIES])


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
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _update(home: Path | str, session_key: str, mutate, what: str) -> None:
    """Load → ``mutate(entries)`` → store under the lock; ``mutate`` returns None to skip the write."""
    try:
        with _lock:
            path = _marker_path(home)
            entries = mutate(_load(path))
            if entries is not None:
                _store(path, entries)
    except Exception:
        logger.debug("failed to %s turn marker for %s", what, session_key, exc_info=True)


def _authority_marker(write, session_key: str, what: str) -> bool:
    """Run *write* on PostgreSQL authority; True when the marker lives there (written or not)."""
    try:
        if not _pg_authority():
            return False
        write()
    except Exception:
        logger.debug("failed to %s turn marker for %s", what, session_key, exc_info=True)
    return True


def record_turn_start(home: Path | str, session_key: str, prompt: str, *, attempts: int = 0,
                      auto_continue: bool = True) -> None:
    """Persist the marker for a turn that is about to run. ``attempts`` = how many auto-continues led to
    this run (0 for a user-initiated turn); the crash-loop breaker reads it back on the next resume."""
    if not session_key or not prompt:
        return
    now = time.time()
    entry = {"attempts": max(0, int(attempts)), "prompt": prompt[:_MAX_PROMPT_CHARS], "started_at": now,
             "auto_continue": bool(auto_continue)}
    if _authority_marker(lambda: _pg_record(str(Path(home)), session_key, entry), session_key, "record"):
        return
    _update(home, session_key, lambda entries: {**_prune(entries, now), session_key: entry}, "record")


def clear_turn_marker(home: Path | str, session_key: str) -> None:
    """Remove the marker once its turn concluded (any outcome the client saw)."""
    if not session_key or _authority_marker(lambda: _pg_clear(str(Path(home)), session_key), session_key, "clear"):
        return
    _update(home, session_key, lambda e: {k: v for k, v in e.items() if k != session_key} if session_key in e else None, "clear")


def _prune_running() -> None:
    for key, threads in list(_running.items()):
        if alive := [thread for thread in threads if thread.is_alive()]:
            _running[key] = alive
        else:
            del _running[key]


def note_turn_running(home: Path | str, session_key: str) -> None:
    """Count the calling turn thread as running *session_key* in this process until the thread ends
    (every exit path of the turn, its ``finally`` included, runs before that)."""
    if not session_key:
        return
    with _running_lock:
        _prune_running()
        _running.setdefault((str(Path(home)), session_key), []).append(threading.current_thread())


def turn_running_here(home: Path | str, session_key: str) -> bool:
    """True while another thread of this process runs a turn of *session_key*: its marker is not interrupted.
    The asking thread never counts — it is either that turn itself or done with any turn it ran inline."""
    me = threading.current_thread()
    with _running_lock:
        _prune_running()
        return any(thread is not me for thread in _running.get((str(Path(home)), session_key), ()))


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
        prompt = str(entry.get("prompt") or "") if isinstance(entry, dict) else ""
        if not prompt.strip():
            return None
        return {"attempts": max(0, int(entry.get("attempts") or 0)), "prompt": prompt, "started_at": _started_at(entry),
                "auto_continue": bool(entry.get("auto_continue", True))}
    except Exception:
        return None
