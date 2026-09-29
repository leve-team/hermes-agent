"""The adapter's credential lock on a PostgreSQL-authority profile (levos 0061).

Pods of one profile share no disk, so on authority the platform lock taken by
``BasePlatformAdapter._acquire_platform_lock`` is the profile's advisory lock
(``gateway.status_pg_locks``). The holder is another pod, not a local PID:
there is nothing to take over with ``--replace``. While held, a watch task
proves the lock every ``_PLATFORM_LOCK_CHECK_SECONDS`` and disconnects the
adapter through the retryable fatal-error path once it is gone, because a lost
lock lets another pod connect the same credential.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_PLATFORM_LOCK_CHECK_SECONDS = 15.0


class PlatformPostgresLockMixin:
    """PostgreSQL half of the platform lock; mixed into ``BasePlatformAdapter``."""

    _platform_lock_postgres: bool = False
    _platform_lock_watch_task: Optional[asyncio.Task] = None

    def _acquire_postgres_platform_lock(self, scope: str, identity: str, resource_desc: str) -> bool:
        """Take the profile's PostgreSQL lock for this credential.

        A pod that loses the race reports a retryable conflict and the
        reconnect watcher tries again after the holder disconnects and
        unlocks. A PostgreSQL failure is the same retryable conflict — never a
        lock file.
        """
        from gateway.status_pg_locks import acquire_postgres_scoped_lock
        from hermes_aux_store import AuxStoreUnavailable

        try:
            acquired, _existing = acquire_postgres_scoped_lock(scope, identity, owner=self)
        except AuxStoreUnavailable as exc:
            message = f"{resource_desc} ownership could not be checked: {exc}"
            acquired = False
        else:
            message = (f"{resource_desc} is held by another gateway of this profile "
                       "(PostgreSQL lock); retrying after it disconnects.")
        if not acquired:
            self._platform_lock_identity = None
            logger.warning('[%s] %s', self.name, message)
            self._set_fatal_error(f'{scope}_lock', message, retryable=True)
            return False
        self._platform_lock_postgres = True
        self._start_platform_lock_watch()
        return True

    @property
    def _platform_lock_fences_pods(self) -> bool:
        """True while this adapter holds its credential lock in PostgreSQL."""
        return bool(getattr(self, '_platform_lock_postgres', False))

    def _start_platform_lock_watch(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = getattr(self, '_platform_lock_watch_task', None)
        if task is not None and not task.done():
            return
        self._platform_lock_watch_task = loop.create_task(self._watch_platform_lock())

    async def _watch_platform_lock(self) -> None:
        """Disconnect through the fatal-error path once the lock is lost."""
        from gateway.status_pg_locks import ensure_postgres_scoped_lock

        while True:
            await asyncio.sleep(_PLATFORM_LOCK_CHECK_SECONDS)
            identity = getattr(self, '_platform_lock_identity', None)
            if not identity or not self._platform_lock_fences_pods:
                return
            scope = self._platform_lock_scope
            if await asyncio.to_thread(ensure_postgres_scoped_lock, scope, identity):
                continue
            message = (f"{scope} PostgreSQL lock was lost; disconnecting so another "
                       "gateway of this profile cannot share the credential")
            logger.error('[%s] %s', self.name, message)
            self._set_fatal_error(f'{scope}_lock_lost', message, retryable=True)
            await self._notify_fatal_error()
            return

    def _release_postgres_platform_lock(self, identity: str) -> None:
        watch = getattr(self, '_platform_lock_watch_task', None)
        self._platform_lock_watch_task = None
        if watch is not None and not watch.done():
            try:
                current = asyncio.current_task()
            except RuntimeError:
                current = None
            if watch is not current:
                watch.cancel()
        from gateway.status_pg_locks import release_postgres_scoped_lock

        release_postgres_scoped_lock(self._platform_lock_scope, identity, owner=self)
        self._platform_lock_postgres = False
