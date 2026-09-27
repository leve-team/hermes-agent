"""Durable state for Discord reconnect message recovery."""

from __future__ import annotations

import datetime as dt
import logging
import os
import sqlite3
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

_DB_FILENAME = "discord_message_recovery.db"
_RETENTION_DAYS = 30


class DiscordRecoveryStore:
    """Small profile-scoped ledger for completed Discord messages.

    SQLite by default. On a PostgreSQL-authority profile the ledger lives in
    that profile's PostgreSQL store (levos 0059): the handle is kept for the
    store's lifetime, every call runs in one transaction, and failures are
    logged and raised instead of being answered with ``default`` -- a caller
    must not mistake an unreachable ledger for "message not seen".
    """

    def __init__(self, hermes_home: Path | None = None) -> None:
        self._lock = threading.Lock()
        self._initialized = False
        self._hermes_home = Path(hermes_home or get_hermes_home())
        self._postgres: Any = None

    def path(self) -> Path:
        directory = self._hermes_home / "gateway"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / _DB_FILENAME

    def call(self, fn: Callable[[sqlite3.Connection], Any], default: Any = None) -> Any:
        from hermes_aux_store import AuxStoreUnavailable

        postgres = False
        try:
            with self._lock:
                conn = self._postgres or self._open()
                if getattr(conn, "is_postgres", False):
                    postgres = True
                    self._postgres = conn
                    return self._call_postgres(fn)
                try:
                    result = fn(conn)
                    conn.commit()
                    return result
                finally:
                    conn.close()
        except AuxStoreUnavailable:
            logger.error("Discord recovery ledger unavailable", exc_info=True)
            raise
        except Exception as exc:
            if postgres:
                logger.error("Discord recovery ledger call failed", exc_info=True)
                raise
            logger.warning("Discord recovery ledger unavailable: %s", exc)
            return default

    def _open(self) -> Any:
        from hermes_aux_store import open_aux_store

        # The seam creates gateway/ only when it opens the SQLite file.
        return open_aux_store(
            "discord_recovery",
            sqlite_path=self._hermes_home / "gateway" / _DB_FILENAME,
            initialize=self._initialize_once,
            sqlite_options={"timeout": 0.1},
        )

    def _call_postgres(self, fn: Callable[[Any], Any]) -> Any:
        conn = self._postgres
        try:
            with conn:
                return fn(conn)
        except BaseException:
            # A broken handle is replaced on the next call rather than reused.
            self._postgres = None
            with suppress(Exception):
                conn.close()
            raise

    def _initialize_once(self, conn: Any) -> None:
        if self._initialized:
            return
        self._initialize(conn)
        self._initialized = True
        if not getattr(conn, "is_postgres", False):
            with suppress(OSError):
                os.chmod(self._hermes_home / "gateway" / _DB_FILENAME, 0o600)

    def close(self) -> None:
        with self._lock:
            conn, self._postgres = self._postgres, None
        if conn is not None:
            with suppress(Exception):
                conn.close()

    def _initialize(self, conn: sqlite3.Connection) -> None:
        if not getattr(conn, "is_postgres", False):
            from hermes_state import apply_wal_with_fallback

            apply_wal_with_fallback(conn, db_label="discord_recovery.db")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_messages (
                message_id TEXT PRIMARY KEY,
                channel_id TEXT,
                thread_id TEXT,
                parent_channel_id TEXT,
                author_id TEXT,
                created_at TEXT,
                status TEXT NOT NULL,
                replied INTEGER NOT NULL DEFAULT 0,
                emoji_ack INTEGER NOT NULL DEFAULT 0,
                outage_response INTEGER NOT NULL DEFAULT 0,
                response_message_id TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_recovery_scans (
                scan_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                channels TEXT NOT NULL,
                window_seconds REAL NOT NULL,
                limit_count INTEGER NOT NULL,
                scanned INTEGER NOT NULL DEFAULT 0,
                missed INTEGER NOT NULL DEFAULT 0,
                dispatched INTEGER NOT NULL DEFAULT 0,
                error TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_recovery_cursors (
                channel_id TEXT PRIMARY KEY,
                last_message_id TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        cutoff = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=_RETENTION_DAYS)
        ).isoformat()
        conn.execute("DELETE FROM discord_messages WHERE updated_at < ?", (cutoff,))
        conn.execute(
            "DELETE FROM discord_recovery_scans "
            "WHERE COALESCE(completed_at, started_at) < ?",
            (cutoff,),
        )
        conn.execute(
            "DELETE FROM discord_recovery_cursors WHERE updated_at < ?",
            (cutoff,),
        )
