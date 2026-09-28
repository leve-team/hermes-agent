"""Global emergency stop (ESTOP) — a resumable pause for NEW work only.

``hermes pause`` writes a sentinel at ``$HERMES_HOME/ESTOP``; ``hermes resume``
removes it. While it exists the cron scheduler, kanban dispatcher and new gateway
turns skip work; in-flight work is never killed. The check is one or two uncached
``os.stat`` calls (process home + fleet root when they differ). The body is optional
JSON ``{"reason", "engaged_at"}``; a corrupt/empty file still counts as engaged
(fail safe, e.g. ``touch ~/.hermes/ESTOP``). Ported from gastownhall/gastown estop.go (MIT).

On a PostgreSQL-authority profile (levos 0067) the sentinel is a row of the profile's store
(``aux_kv`` namespace ``estop``) instead of a file: every pod of the profile pauses together, and
a pod started on a fresh disk is still paused. Each check is one indexed read; a store that cannot
answer counts as engaged, as an unreadable sentinel does.
"""

from __future__ import annotations

import json
import logging
import threading
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Same profile-aware / fleet-root resolvers the file-safety guards use (fail-open to ~/.hermes).
from agent.file_safety import _hermes_home_path as _hermes_home, _hermes_root_path as _canonical_root

SENTINEL_NAME = "ESTOP"

logger = logging.getLogger(__name__)

# Per-component "logged already for this engagement" flags: log once per engagement, not per tick.
_log_lock = threading.Lock()
_logged_components: set[str] = set()


def sentinel_path() -> Path:
    """Path of the ESTOP sentinel this process would write on `hermes pause`."""
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


def _candidate_sentinel_paths() -> list:
    """Profile home first, then the fleet root if it is a different directory: a profile
    gateway (HERMES_HOME=~/.hermes/profiles/<n>) must still honor an operator's ~/.hermes/ESTOP."""
    primary = sentinel_path()
    try:
        root = _canonical_root() / SENTINEL_NAME
    except Exception:
        return [primary]
    try:
        distinct = root.resolve() != primary.resolve()
    except Exception:
        # Non-Path test doubles fail .resolve(); plain equality still dedupes.
        distinct = root != primary
    return [primary, root] if distinct else [primary]


def is_engaged() -> bool:
    """True if ANY candidate sentinel exists; fail SAFE (True) on stat errors — and, on PostgreSQL
    authority, on a store that cannot answer."""
    try:
        namespace = _kv_namespace()
        if namespace is not None:
            return _kv_read(namespace) is not None
    except Exception as exc:
        logger.warning("ESTOP state unreadable (%s) — treating the pause as engaged", type(exc).__name__)
        return True
    saw_stat_error = False
    for path in _candidate_sentinel_paths():
        try:
            if path.exists():
                return True
        except OSError:
            saw_stat_error = True
    return saw_stat_error


def engage(reason: Optional[str] = None) -> Path:
    """Create the ESTOP sentinel. Idempotent; re-engaging updates the file."""
    path = sentinel_path()
    payload = {"engaged_at": datetime.now(timezone.utc).isoformat(), "reason": reason or None}
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
        with suppress(OSError):  # Best effort: an empty/partial sentinel still pauses (fail safe).
            path.touch(exist_ok=True)
    return path


def disengage() -> bool:
    """Remove every visible sentinel (process-local and fleet-root)."""
    namespace = _kv_namespace()
    if namespace is not None:
        from hermes_aux_store import aux_kv_delete

        return aux_kv_delete(namespace, SENTINEL_NAME)
    lifted = False
    for path in _candidate_sentinel_paths():
        try:
            path.unlink()
            lifted = True
        except (OSError, AttributeError):
            continue
    return lifted


def get_state() -> Optional[dict]:
    """Return ``{"reason", "engaged_at"}`` or None when not engaged; an unreadable/corrupt
    body still reports engaged with both fields None."""
    namespace = _kv_namespace()
    if namespace is not None:
        try:
            body = _kv_read(namespace)
        except Exception:
            return {"reason": None, "engaged_at": None}  # fail safe, as is_engaged
        if body is None:
            return None
        state = {"reason": None, "engaged_at": None}
        with suppress(ValueError):
            raw = json.loads(body)
            if isinstance(raw, dict):
                state = {"reason": raw.get("reason") or None, "engaged_at": raw.get("engaged_at") or None}
        return state
    if not is_engaged():
        return None
    state = {"reason": None, "engaged_at": None}
    found = False
    for path in _candidate_sentinel_paths():
        try:
            if not path.exists():
                continue
        except OSError:
            return state
        except AttributeError:
            continue
        found = True
        with suppress(OSError, ValueError, AttributeError):
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                state = {"reason": raw.get("reason") or None, "engaged_at": raw.get("engaged_at") or None}
                break
    return state if found else None


def paused_reply() -> Optional[str]:
    """Short user-facing notice for new gateway turns, or None if not paused."""
    state = get_state()
    if state is None:
        return None
    tag = f" ({state['reason']})" if state.get("reason") else ""
    return f"⏸️ Hermes is paused{tag}. New work is on hold; run `hermes resume` to pick things back up."


def check_paused(component: str, logger: logging.Logger) -> bool:
    """Return True when engaged, logging once per engagement per component (re-armed after a resume)."""
    if not is_engaged():
        with _log_lock:
            _logged_components.discard(component)
        return False
    with _log_lock:
        first = component not in _logged_components
        _logged_components.add(component)
    if first:
        reason = (get_state() or {}).get("reason")
        suffix = f" (reason: {reason})" if reason else ""
        logger.info(
            "%s dispatch paused by global emergency stop%s — remove with `hermes resume` (%s)",
            component, suffix, location(),
        )
    return True


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
import os  # noqa: F401,E402
# ---- END PLUGIN-COMPAT ----
