"""Suggested cron jobs — proposed automations the user accepts with one tap.

A *suggestion* is a ready-to-run cron job spec that Hermes surfaces to the
user, who accepts it (creates the real cron job) or dismisses it (latched so
it is never re-offered). This is the single surface every automation proposal
flows through, regardless of where it came from:

  * ``catalog``  — a curated starter automation (daily briefing, important-mail
                   monitor, weekly digest, ...).
  * ``blueprint``   — the user installed a skill that carries a ``blueprint:`` block
                   (see ``tools/blueprints.py``); installing it registers a
                   suggestion instead of auto-scheduling.
  * ``usage``    — the background self-improvement review noticed a recurring
                   ask that a scheduled job would serve.
  * ``integration`` — the user connected an account (Gmail, GitHub, ...) and
                   the obvious automations for that surface are offered.

Accepting a suggestion just calls the existing ``cron.jobs.create_job`` with
the stored ``job_spec`` — there is NO second job engine. Suggestions never
auto-create jobs; acceptance is always explicit (consent-first). Dismissed
suggestions latch by a stable ``dedup_key`` so the same proposal is not
re-offered after the user says no.

Storage mirrors ``cron/jobs.py``: ``~/.hermes/cron/suggestions.json``, atomic
writes, an in-process lock, and 0600 perms. On a PostgreSQL-authority profile
(levos 0068) the records live one row per suggestion in the profile's store
(``core_cron_suggestions``) and the lock is a PostgreSQL advisory lock, so two
pods of the profile see and latch the same suggestions; no file is written.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now
from utils import atomic_replace

logger = logging.getLogger(__name__)

# Per-profile by design (issue #4707): suggestions live alongside the active
# profile's cron store. Anchor on get_hermes_home() (profile home), not the
# shared default root. See cron/jobs.py for the full rationale.
CRON_DIR = get_hermes_home().resolve() / "cron"
SUGGESTIONS_FILE = CRON_DIR / "suggestions.json"

# In-process lock protecting load->modify->save cycles (the background review
# fork and the main agent can both write).
_suggestions_lock = threading.Lock()
# The PostgreSQL connection of this thread's open section (authority only).
_section = threading.local()
_LOCK_TIMEOUT_SECONDS = 30.0

# Cap pending suggestions so the list never becomes a nag wall. When full,
# new suggestions are dropped (the user should clear the backlog first).
MAX_PENDING = 5

VALID_SOURCES = frozenset({"catalog", "blueprint", "usage", "integration"})
_STATUS_PENDING = "pending"
_STATUS_ACCEPTED = "accepted"
_STATUS_DISMISSED = "dismissed"


def _secure_file(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _ensure_dir() -> None:
    CRON_DIR.mkdir(parents=True, exist_ok=True)


def _initialize_table(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, "cron_suggestions"):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS cron_suggestions (
                 id TEXT PRIMARY KEY,
                 position INTEGER NOT NULL,
                 record TEXT NOT NULL,
                 updated_at TEXT NOT NULL
               )"""
        )


def _open_store():
    from hermes_aux_store import open_aux_postgres

    return open_aux_postgres("cron_suggestions", initialize=_initialize_table)


@contextlib.contextmanager
def _suggestions_section() -> Iterator[None]:
    """Serialize a load->modify->save cycle.

    Off authority: the in-process lock. On PostgreSQL authority additionally
    one transaction holding ``pg_advisory_xact_lock`` for the whole cycle, so
    a cycle on another pod cannot interleave; a wait beyond
    ``_LOCK_TIMEOUT_SECONDS`` raises ``AuxStoreUnavailable``.
    """
    with _suggestions_lock:
        from hermes_aux_store import aux_store_authority, aux_xact_lock

        if not aux_store_authority():
            yield
            return
        conn = _open_store()
        try:
            with conn:
                aux_xact_lock(
                    conn, "cron-suggestions", timeout_seconds=_LOCK_TIMEOUT_SECONDS
                )
                _section.conn = conn
                try:
                    yield
                finally:
                    _section.conn = None
        finally:
            conn.close()


def _pg_read(conn) -> List[Dict[str, Any]]:
    return [
        json.loads(row["record"])
        for row in conn.execute(
            "SELECT record FROM cron_suggestions ORDER BY position, id"
        ).fetchall()
    ]


def _pg_write(conn, suggestions: List[Dict[str, Any]]) -> None:
    """Make the stored rows equal *suggestions*, in order; unchanged rows stay."""
    documents: Dict[str, tuple] = {}
    for position, record in enumerate(suggestions):
        record_id = str(record.get("id") or "")
        if not record_id:
            raise ValueError("a suggestion without an id cannot be stored")
        if record_id in documents:
            raise ValueError(f"duplicate suggestion id {record_id!r}")
        documents[record_id] = (position, json.dumps(record, ensure_ascii=False))
    stored = {
        row["id"]: (row["position"], row["record"])
        for row in conn.execute(
            "SELECT id, position, record FROM cron_suggestions"
        ).fetchall()
    }
    for record_id in stored.keys() - documents.keys():
        conn.execute("DELETE FROM cron_suggestions WHERE id = ?", (record_id,))
    now = _hermes_now().isoformat()
    for record_id, (position, document) in documents.items():
        if stored.get(record_id) == (position, document):
            continue
        conn.execute(
            """INSERT INTO cron_suggestions (id, position, record, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (id) DO UPDATE SET position = EXCLUDED.position,
                 record = EXCLUDED.record, updated_at = EXCLUDED.updated_at""",
            (record_id, position, document, now),
        )


def _load_raw() -> Dict[str, Any]:
    conn = getattr(_section, "conn", None)
    if conn is not None:
        return {"suggestions": _pg_read(conn)}
    from hermes_aux_store import aux_store_authority

    if aux_store_authority():
        conn = _open_store()
        try:
            return {"suggestions": _pg_read(conn)}
        finally:
            conn.close()
    if not SUGGESTIONS_FILE.exists():
        return {"suggestions": []}
    try:
        with open(SUGGESTIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("suggestions.json unreadable (%s); starting empty", e)
        return {"suggestions": []}
    if isinstance(data, dict) and isinstance(data.get("suggestions"), list):
        return data
    if isinstance(data, list):
        return {"suggestions": data}
    logger.warning("suggestions.json malformed; starting empty")
    return {"suggestions": []}


def _save_raw(suggestions: List[Dict[str, Any]]) -> None:
    conn = getattr(_section, "conn", None)
    if conn is not None:
        _pg_write(conn, suggestions)
        return
    from hermes_aux_store import aux_store_authority

    if aux_store_authority():
        raise RuntimeError("cron suggestions are written only inside _suggestions_section()")
    _ensure_dir()
    fd, tmp_path = tempfile.mkstemp(dir=str(SUGGESTIONS_FILE.parent), suffix=".tmp", prefix=".sugg_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                {"suggestions": suggestions, "updated_at": _hermes_now().isoformat()},
                f,
                indent=2,
            )
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp_path, SUGGESTIONS_FILE)
        _secure_file(SUGGESTIONS_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_suggestions() -> List[Dict[str, Any]]:
    """Return all suggestion records (any status)."""
    return _load_raw().get("suggestions", [])


def list_pending() -> List[Dict[str, Any]]:
    """Return pending suggestions in creation order (oldest first)."""
    return [s for s in load_suggestions() if s.get("status") == _STATUS_PENDING]


def add_suggestion(
    *,
    title: str,
    description: str,
    source: str,
    job_spec: Dict[str, Any],
    dedup_key: str,
) -> Optional[Dict[str, Any]]:
    """Register a pending suggestion. Returns the record, or None if skipped.

    Skipped when: the source is unknown, the same ``dedup_key`` was already
    dismissed or accepted (never re-offer), an identical pending suggestion
    exists, or the pending list is full (``MAX_PENDING``).

    ``job_spec`` is a dict of kwargs for ``cron.jobs.create_job`` — accepting
    the suggestion passes it straight through, so there is no second schema to
    keep in sync.
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"unknown suggestion source: {source!r}")
    if not title.strip() or not dedup_key.strip():
        raise ValueError("title and dedup_key are required")

    with _suggestions_section():
        suggestions = _load_raw().get("suggestions", [])

        # Never re-offer something the user already saw and decided on, and
        # never duplicate a still-pending proposal.
        for existing in suggestions:
            if existing.get("dedup_key") == dedup_key:
                if existing.get("status") in (_STATUS_DISMISSED, _STATUS_ACCEPTED):
                    return None
                if existing.get("status") == _STATUS_PENDING:
                    return None

        pending_count = sum(1 for s in suggestions if s.get("status") == _STATUS_PENDING)
        if pending_count >= MAX_PENDING:
            logger.info("Suggestion backlog full (%d); dropping %r", MAX_PENDING, title)
            return None

        record = {
            "id": uuid.uuid4().hex[:12],
            "title": title.strip(),
            "description": description.strip(),
            "source": source,
            "job_spec": job_spec,
            "dedup_key": dedup_key.strip(),
            "status": _STATUS_PENDING,
            "created_at": _hermes_now().isoformat(),
        }
        suggestions.append(record)
        _save_raw(suggestions)
        return record


def get_suggestion(ref: str) -> Optional[Dict[str, Any]]:
    """Resolve a suggestion by id, 1-based pending index, or title (exact)."""
    suggestions = load_suggestions()
    # By id.
    for s in suggestions:
        if s.get("id") == ref:
            return s
    # By 1-based pending index.
    if ref.isdigit():
        pending = [s for s in suggestions if s.get("status") == _STATUS_PENDING]
        idx = int(ref) - 1
        if 0 <= idx < len(pending):
            return pending[idx]
    # By exact title (case-insensitive).
    for s in suggestions:
        if s.get("title", "").lower() == ref.lower():
            return s
    return None


def _set_status(suggestion_id: str, status: str) -> bool:
    with _suggestions_section():
        suggestions = _load_raw().get("suggestions", [])
        changed = False
        for s in suggestions:
            if s.get("id") == suggestion_id:
                s["status"] = status
                s["resolved_at"] = _hermes_now().isoformat()
                changed = True
                break
        if changed:
            _save_raw(suggestions)
        return changed


def dismiss_suggestion(ref: str) -> bool:
    """Dismiss a suggestion (latched — never re-offered for its dedup_key)."""
    s = get_suggestion(ref)
    if not s:
        return False
    return _set_status(s["id"], _STATUS_DISMISSED)


def accept_suggestion(ref: str, *, origin: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Accept a suggestion: create the real cron job from its ``job_spec``.

    Returns the created cron job dict, or None if the suggestion isn't found /
    not pending. The job_spec is passed straight to ``cron.jobs.create_job``;
    an ``origin`` (platform/chat) is merged so "origin" delivery routes back to
    the chat where the user accepted.
    """
    s = get_suggestion(ref)
    if not s or s.get("status") != _STATUS_PENDING:
        return None

    from cron.scheduler import (
        CronSchedulerRegistrationError,
        create_job_with_scheduler_registration,
    )

    spec = dict(s.get("job_spec") or {})
    if origin is not None and "origin" not in spec:
        spec["origin"] = origin

    try:
        job = create_job_with_scheduler_registration(**spec)
    except CronSchedulerRegistrationError:
        # The job is already durable. Resolve the suggestion so retrying the
        # same acceptance cannot create another local copy.
        _set_status(s["id"], _STATUS_ACCEPTED)
        raise
    _set_status(s["id"], _STATUS_ACCEPTED)
    return job


def clear_resolved() -> int:
    """Drop accepted/dismissed records from disk. Returns the count removed.

    Pending suggestions and the dedup memory of dismissed ones are the only
    things that matter long-term, but dismissed records must be RETAINED for
    their dedup_key (so they aren't re-offered). This only prunes ACCEPTED
    records, which have served their purpose once the job exists.
    """
    with _suggestions_section():
        suggestions = _load_raw().get("suggestions", [])
        kept = [s for s in suggestions if s.get("status") != _STATUS_ACCEPTED]
        removed = len(suggestions) - len(kept)
        if removed:
            _save_raw(kept)
        return removed
