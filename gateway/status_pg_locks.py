"""Scoped (credential) locks on a PostgreSQL-authority profile (levos 0061).

Two pods of one profile share nothing but the profile's PostgreSQL store, so a
lock file under ``gateway-locks/`` (and a PID only meaningful in its own
namespace) cannot keep them from opening the same bot token at once. On
authority the scoped lock is a session advisory lock keyed by the profile
schema plus scope and credential hash (``hermes_aux_store.AuxSessionLock``).
The server releases it when the holder's connection ends, so a killed pod
never wedges it. Within one process the lock is re-entrant per owner, like the
file lock's same-PID reacquire: a reconnecting adapter can take it while the
previous adapter object is still tearing down, and the lock is unlocked only
when its last owner releases it.

``gateway.status.acquire_scoped_lock`` / ``release_scoped_lock`` route here on
authority; every other backend keeps the lock file.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class _PostgresScopedLock:
    lock: Any
    owners: set[int]


_POSTGRES_SCOPED_LOCKS: dict[tuple[str, str], _PostgresScopedLock] = {}
_POSTGRES_SCOPED_LOCKS_GUARD = threading.Lock()


def scoped_lock_uses_postgres() -> bool:
    """True when scoped locks live in the profile's PostgreSQL authority store."""
    from hermes_aux_store import aux_store_authority

    return aux_store_authority()


def _scope_key(scope: str, identity: str) -> tuple[str, str]:
    from gateway.status import _scope_hash

    return scope, _scope_hash(identity)


def postgres_scoped_lock_name(scope: str, identity: str) -> str:
    return f"gateway-scope:{scope}:{_scope_key(scope, identity)[1]}"


def holds_postgres_scoped_lock(scope: str, identity: str) -> bool:
    """True when this process registered the lock (whatever the backend now says)."""
    with _POSTGRES_SCOPED_LOCKS_GUARD:
        return _scope_key(scope, identity) in _POSTGRES_SCOPED_LOCKS


def _owner_token(owner: Any) -> int:
    return 0 if owner is None else id(owner)


def acquire_postgres_scoped_lock(
    scope: str, identity: str, *, owner: Any = None
) -> tuple[bool, Optional[dict[str, Any]]]:
    """Take the profile's advisory lock for *scope* + *identity* without waiting.

    Returns ``(False, record)`` while another session (another pod) holds it.
    A PostgreSQL failure raises ``hermes_aux_store.AuxStoreUnavailable``;
    nothing falls back to a lock file.
    """
    from hermes_aux_store import AuxSessionLock

    key = _scope_key(scope, identity)
    with _POSTGRES_SCOPED_LOCKS_GUARD:
        held = _POSTGRES_SCOPED_LOCKS.get(key)
        if held is not None:
            held.owners.add(_owner_token(owner))
            return True, None
        lock = AuxSessionLock(postgres_scoped_lock_name(scope, identity))
        if not lock.acquire():
            return False, {"scope": scope, "identity_hash": key[1], "backend": "postgres"}
        _POSTGRES_SCOPED_LOCKS[key] = _PostgresScopedLock(lock, {_owner_token(owner)})
    return True, None


def release_postgres_scoped_lock(scope: str, identity: str, *, owner: Any = None) -> None:
    """Drop *owner*'s claim; the last owner's release unlocks the advisory lock."""
    key = _scope_key(scope, identity)
    with _POSTGRES_SCOPED_LOCKS_GUARD:
        held = _POSTGRES_SCOPED_LOCKS.get(key)
        if held is None:
            return
        held.owners.discard(_owner_token(owner))
        if held.owners:
            return
        del _POSTGRES_SCOPED_LOCKS[key]
    held.lock.release()


def ensure_postgres_scoped_lock(scope: str, identity: str) -> bool:
    """True while this process still holds the advisory lock for *scope* + *identity*.

    A dropped session loses the lock silently (the server frees it), so a
    lost lock is taken again at once when nobody else took it meanwhile.
    False means another session holds it now, PostgreSQL cannot be reached,
    or this process never held it — the caller must stop using the credential.
    """
    from hermes_aux_store import AuxSessionLock, AuxStoreUnavailable

    key = _scope_key(scope, identity)
    with _POSTGRES_SCOPED_LOCKS_GUARD:
        held = _POSTGRES_SCOPED_LOCKS.get(key)
        if held is None:
            return False
        if held.lock.held():
            return True
        held.lock.release()
        lock = AuxSessionLock(postgres_scoped_lock_name(scope, identity))
        try:
            reacquired = lock.acquire()
        except AuxStoreUnavailable:
            reacquired = False
        if reacquired:
            held.lock = lock
            logger.warning(
                "Scoped lock %s: PostgreSQL session was lost and the lock was taken again",
                scope,
            )
        return reacquired
