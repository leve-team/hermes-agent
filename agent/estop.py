"""Global emergency stop (ESTOP) — a resumable pause for NEW work only.

``hermes pause`` writes a sentinel file at ``$HERMES_HOME/ESTOP``;
``hermes resume`` removes it. While the sentinel exists:

* the cron scheduler skips dispatching due jobs (``cron/scheduler.py:tick``),
* the embedded kanban dispatcher skips spawning workers
  (``gateway/kanban_watchers.py``),
* new gateway turns get a brief "Hermes is paused" reply instead of an
  agent run (``gateway/run.py:_handle_message``).

In-flight work is NEVER killed — this is pause-new-work, not panic/exit.
The check is a single ``os.stat`` so callers may run it every tick; no
caching beyond the OS is performed, so engaging/disengaging takes effect on
the very next check.

The sentinel body is optional JSON ``{"reason": ..., "engaged_at": ...}``.
A corrupt or empty file still counts as engaged (fail safe): the pause must
hold even if the file was created by ``touch ~/.hermes/ESTOP``.

On a PostgreSQL-authority profile (levos 0067) the sentinel is a row of the
profile's store (``aux_kv`` namespace ``estop``) instead of a file: every pod
of the profile pauses together, and a pod started on a fresh disk is still
paused. Each check is one indexed read; a store that cannot answer counts as
engaged, as an unreadable sentinel does.

Ported from: gastownhall/gastown estop.go (MIT). Related prior art:
#26778 (/panic — kill/exit semantics; deliberately different, ours is
resumable) and #44617 (interrupting in-flight cron; deliberately out of
scope here).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

SENTINEL_NAME = "ESTOP"

logger = logging.getLogger(__name__)

# Per-component "logged already for this engagement" flags so a paused
# dispatch loop logs once per engagement instead of once per tick.
_log_lock = threading.Lock()
_logged_components: set[str] = set()


def _hermes_home() -> Path:
    """Resolve the active HERMES_HOME (profile-aware) at call time."""
    try:
        from hermes_constants import get_hermes_home
        return get_hermes_home()
    except Exception:
        return Path(os.path.expanduser("~/.hermes"))


def sentinel_path() -> Path:
    """Path of the ESTOP sentinel under the active HERMES_HOME."""
    return _hermes_home() / SENTINEL_NAME


def _kv_namespace() -> Optional[str]:
    """``aux_kv`` namespace on PostgreSQL authority, None on every other backend."""
    try:
        from hermes_aux_store import KV_ESTOP, aux_store_authority
    except ImportError:
        return None
    return KV_ESTOP if aux_store_authority() else None


def location() -> str:
    """Where the sentinel lives, for operator-facing messages."""
    if _kv_namespace() is not None:
        return "the profile's PostgreSQL store"
    return str(sentinel_path())


def _kv_read(namespace: str) -> Optional[str]:
    """The stored sentinel body, None when not engaged. Raises on store failure."""
    from hermes_aux_store import aux_kv_get

    return aux_kv_get(namespace, SENTINEL_NAME)


def is_engaged() -> bool:
    """Cheap check (one stat): is the global emergency stop engaged?

    Fail SAFE on stat errors: if we cannot determine whether the sentinel
    exists (permission error, transient I/O failure on HERMES_HOME), report
    engaged. The module contract is that the pause must hold even when the
    sentinel is unreadable — a fail-open here would silently lift an
    operator's emergency stop exactly when the filesystem is misbehaving.
    The same holds for a PostgreSQL store that cannot answer.
    """
    try:
        namespace = _kv_namespace()
        if namespace is not None:
            return _kv_read(namespace) is not None
        return sentinel_path().exists()
    except Exception as exc:
        if not isinstance(exc, OSError):
            logger.warning(
                "ESTOP state unreadable (%s) — treating the pause as engaged",
                type(exc).__name__,
            )
        return True


def engage(reason: Optional[str] = None) -> Path:
    """Create the ESTOP sentinel. Idempotent; re-engaging updates the file."""
    path = sentinel_path()
    payload = {
        "engaged_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason or None,
    }
    namespace = _kv_namespace()
    if namespace is not None:
        from hermes_aux_store import aux_kv_put

        # No best-effort here: a pause that did not reach the store must fail.
        aux_kv_put(namespace, SENTINEL_NAME, json.dumps(payload, indent=2))
        return path
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError:
        # Best effort: an empty/partial sentinel still pauses (fail safe).
        try:
            path.touch(exist_ok=True)
        except OSError:
            pass
    return path


def disengage() -> bool:
    """Remove the ESTOP sentinel. Returns True if a pause was lifted."""
    namespace = _kv_namespace()
    if namespace is not None:
        from hermes_aux_store import aux_kv_delete

        return aux_kv_delete(namespace, SENTINEL_NAME)
    try:
        sentinel_path().unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def get_state() -> Optional[dict]:
    """Return ``{"reason": ..., "engaged_at": ...}`` or None when not engaged.

    A sentinel with an unreadable/corrupt body still reports engaged, with
    both fields None — the pause is authoritative, the metadata is not.
    """
    namespace = _kv_namespace()
    if namespace is not None:
        try:
            body = _kv_read(namespace)
        except Exception:
            return {"reason": None, "engaged_at": None}  # fail safe, as is_engaged
        if body is None:
            return None
    else:
        path = sentinel_path()
        if not path.exists():
            return None
    reason = None
    engaged_at = None
    try:
        raw = json.loads(
            body if namespace is not None else path.read_text(encoding="utf-8")
        )
        if isinstance(raw, dict):
            reason = raw.get("reason") or None
            engaged_at = raw.get("engaged_at") or None
    except (OSError, ValueError):
        pass
    return {"reason": reason, "engaged_at": engaged_at}


def paused_reply() -> Optional[str]:
    """Short user-facing notice for new gateway turns, or None if not paused."""
    state = get_state()
    if state is None:
        return None
    reason = state.get("reason")
    if reason:
        return (
            f"⏸️ Hermes is paused ({reason}). New work is on hold; "
            "run `hermes resume` to pick things back up."
        )
    return (
        "⏸️ Hermes is paused. New work is on hold; "
        "run `hermes resume` to pick things back up."
    )


def check_paused(component: str, logger: logging.Logger) -> bool:
    """Return True when engaged, logging once per engagement per component.

    Dispatch loops call this every tick; the log fires on the disengaged→
    engaged transition for that component and re-arms after a resume, so a
    long pause doesn't spam one line per tick.
    """
    if not is_engaged():
        with _log_lock:
            _logged_components.discard(component)
        return False
    with _log_lock:
        first = component not in _logged_components
        if first:
            _logged_components.add(component)
    if first:
        state = get_state() or {}
        reason = state.get("reason")
        suffix = f" (reason: {reason})" if reason else ""
        logger.info(
            "%s dispatch paused by global emergency stop%s — remove with "
            "`hermes resume` (%s)",
            component,
            suffix,
            location(),
        )
    return True


def _reset_log_state_for_tests() -> None:
    """Clear the log-once bookkeeping (test isolation helper)."""
    with _log_lock:
        _logged_components.clear()
