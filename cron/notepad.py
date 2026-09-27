"""Per-job durable notepad for cron jobs.

A tiny KV scratchpad each cron job can use to carry state across scheduled
wake-ups (cursors, watermarks, watchlists). Stored in its own profile-local
SQLite file next to the executions ledger, following the same
connection/pragma pattern as ``cron/executions.py`` — or, on a PostgreSQL
authority profile, in the profile's store (``core_cron_notes``, levos 0060).

Size caps (documented contract):

- ``MAX_VALUE_BYTES`` (16 KB): per-key value cap, measured in UTF-8 bytes.
- ``MAX_JOB_TOTAL_BYTES`` (64 KB): per-job cap over the sum of key+value
  bytes. Oversized writes raise ``ValueError`` and leave the store
  untouched — the notepad is prompt-injected each run, so unbounded growth
  would bloat every wake-up's prompt.

Write path is the CLI (``hermes cron notepad <job_id> set <key> <value>``),
which the running agent invokes via its terminal tool; no model tool is
added.

Inspired by: Amp (Sourcegraph) cron notepad (idea-level, proprietary — zero
code).
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

NOTEPAD_FILE = get_hermes_home().resolve() / "cron" / "notepad.db"
MAX_VALUE_BYTES = 16 * 1024
MAX_KEY_CHARS = 128
MAX_JOB_TOTAL_BYTES = 64 * 1024
_lock = threading.RLock()


def _connect() -> sqlite3.Connection:
    """Open the notepad on the backend the profile selects (levos 0060).

    PostgreSQL authority: table ``core_cron_notes`` in the profile's store,
    no file; a failure raises ``AuxStoreUnavailable``. Otherwise the SQLite
    file, opened and initialized exactly as before.
    """
    from hermes_aux_store import open_aux_store

    return open_aux_store(
        "cron_notepad",
        sqlite_path=NOTEPAD_FILE,
        initialize=_initialize_schema,
        sqlite_options={"timeout": 5},
    )


def _initialize_schema(conn: sqlite3.Connection) -> None:
    if getattr(conn, "is_postgres", False):
        from hermes_aux_store import aux_schema_transaction

        with aux_schema_transaction(conn, "cron_notepad"):
            _create_schema(conn)
        return
    from hermes_state import apply_wal_with_fallback

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    apply_wal_with_fallback(conn, db_label="cron/notepad.db")
    _create_schema(conn)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS cron_notepad (
             job_id TEXT NOT NULL,
             key TEXT NOT NULL,
             value TEXT NOT NULL,
             updated_at TEXT NOT NULL,
             PRIMARY KEY (job_id, key)
           )"""
    )


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection, commit/rollback on exit, always close.

    Mirrors ``cron.executions._transaction``: schema init runs inside
    ``_connect`` (``open_aux_store``), which closes the connection itself
    when a PRAGMA/DDL step fails instead of leaking it.
    """
    with _lock:
        conn = _connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()


def _validate(job_id: str, key: str, value: str) -> None:
    if not str(job_id):
        raise ValueError("job_id must be non-empty")
    if not key:
        raise ValueError("key must be non-empty")
    if len(key) > MAX_KEY_CHARS:
        raise ValueError(f"key too long (max {MAX_KEY_CHARS} characters)")
    if len(value.encode("utf-8")) > MAX_VALUE_BYTES:
        raise ValueError(
            f"value too large (max {MAX_VALUE_BYTES} bytes per key)"
        )


def set_note(job_id: str, key: str, value: str) -> Dict[str, Any]:
    """Upsert one key. Raises ValueError when a size cap would be exceeded."""
    job_id, key, value = str(job_id), str(key), str(value)
    _validate(job_id, key, value)
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        if getattr(conn, "is_postgres", False):
            # UTF-8 byte length, as CAST(... AS BLOB) measures it on SQLite.
            byte_sum = "OCTET_LENGTH(key) + OCTET_LENGTH(value)"
        else:
            byte_sum = """LENGTH(CAST(key AS BLOB))
                 + LENGTH(CAST(value AS BLOB))"""
        row = conn.execute(
            f"""SELECT COALESCE(SUM({byte_sum}), 0)
               FROM cron_notepad WHERE job_id=? AND key<>?""",
            (job_id, key),
        ).fetchone()
        other_bytes = int(row[0])
        entry_bytes = len(key.encode("utf-8")) + len(value.encode("utf-8"))
        if other_bytes + entry_bytes > MAX_JOB_TOTAL_BYTES:
            raise ValueError(
                f"notepad full: job '{job_id}' would exceed "
                f"{MAX_JOB_TOTAL_BYTES} bytes total; delete unused keys first"
            )
        conn.execute(
            """INSERT INTO cron_notepad (job_id, key, value, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(job_id, key)
               DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
            (job_id, key, value, now),
        )
    return {"job_id": job_id, "key": key, "value": value, "updated_at": now}


def get_note(job_id: str, key: str) -> Optional[str]:
    with _transaction() as conn:
        row = conn.execute(
            "SELECT value FROM cron_notepad WHERE job_id=? AND key=?",
            (str(job_id), str(key)),
        ).fetchone()
    return None if row is None else row["value"]


def delete_note(job_id: str, key: str) -> bool:
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM cron_notepad WHERE job_id=? AND key=?",
            (str(job_id), str(key)),
        )
    return cur.rowcount > 0


def list_notes(job_id: str) -> List[Dict[str, Any]]:
    """All entries for one job, sorted by key."""
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT job_id, key, value, updated_at FROM cron_notepad "
            "WHERE job_id=? ORDER BY key",
            (str(job_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def clear_notepad(job_id: str) -> int:
    """Delete every key for one job (e.g. on job removal). Returns row count.

    Called from ``cron.jobs.remove_job`` so deleted jobs don't orphan their
    rows. No-ops without creating the DB when no notepad file exists yet
    (a PostgreSQL authority profile has no file and always deletes).
    """
    if not NOTEPAD_FILE.exists():
        from hermes_aux_store import aux_store_authority

        if not aux_store_authority():
            return 0
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM cron_notepad WHERE job_id=?", (str(job_id),)
        )
    return cur.rowcount


def render_notepad_section(job_id: str) -> str:
    """Render a job's notepad as a prompt section, or '' when empty/unavailable.

    Empty notepad MUST return the empty string so jobs that never use the
    feature get a byte-identical prompt (prompt-cache + drift safety).
    """
    try:
        notes = list_notes(job_id)
    except Exception:
        return ""
    if not notes:
        return ""
    lines = [f"- {note['key']}: {note['value']}" for note in notes]
    return (
        "## Job notepad (persistent across runs)\n"
        "This durable scratchpad survives between scheduled runs of this "
        "job. Update it via the CLI, e.g.:\n"
        f"`hermes cron notepad {job_id} set <key> <value>` "
        f"(also: get/delete/list; `hermes cron notepad {job_id} delete "
        "<key>` removes an entry).\n\n" + "\n".join(lines) + "\n\n"
    )
