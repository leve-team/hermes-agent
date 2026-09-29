"""Turn recovery across overlapping pods on PostgreSQL authority (levos 0062), bound onto
``GatewayRunner`` via the MRO.

Off authority the previous gateway's exit is judged by the pod-local ``.clean_shutdown`` receipt.
On authority a peer pod may still run the turns this pod can see, and this pod's disk is not the
peer's: the decision moves to the PostgreSQL turn leases (``gateway.turn_owner``). Startup settles
only claimed markers (no 120 s recency fallback — every gateway that runs on authority writes exact
markers), a supervised watcher repeats the pass every lease-renew period because a peer usually
exits after this pod started, a drain timeout hands resume_pending sessions over as lease rows, and
the exit receipt is the outcome recorded on this process's leases.

``gateway.run`` internals are imported lazily inside method bodies (import cycle).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Iterable, Optional

# Log-record parity with the origin module.
logger = logging.getLogger("gateway.run")


class GatewayTurnLeasesMixin:
    """Startup recovery, peer sweep, hand-over and exit receipt of the turn leases."""

    @staticmethod
    def _turn_leases_on_postgres() -> bool:
        from gateway import turn_owner

        return turn_owner.enabled()

    @staticmethod
    def _active_turn_marker_max_age() -> int:
        from gateway.run import _float_env

        agent_timeout = max(1.0, _float_env("HERMES_AGENT_TIMEOUT", 1800))
        return max(60 * 60, int(agent_timeout * 2))

    async def _recover_sessions_after_previous_run(self) -> None:
        """Settle the turns the previous gateway left behind (startup)."""
        from gateway.run import _hermes_home

        clean_marker = _hermes_home / ".clean_shutdown"
        if self._turn_leases_on_postgres():
            await self._recover_turns_from_leases(clean_marker)
            return
        # SKIP after a clean exit — the previous process already drained.
        if clean_marker.exists():
            logger.info("Previous gateway exited cleanly — skipping session suspension")
            try:
                discarded = await self._consume_clean_shutdown_marker(clean_marker)
            except Exception as exc:
                logger.error(
                    "Clean-start marker cleanup failed; refusing startup so the "
                    "clean-exit receipt cannot mask a later unclean exit: %s", exc,
                )
                raise RuntimeError("clean-start recovery cleanup failed") from exc
            if discarded:
                logger.info("Discarded %d orphan active-turn marker(s) after clean shutdown", discarded)
            return
        # Exact turn markers + 120s recency fallback for marker-less older turns.
        exact, fallback = await self._recover_unclean_sessions()
        if exact + fallback:
            logger.info(
                "Marked %d in-flight session(s) as resumable from previous run "
                "(%d exact, %d legacy)", exact + fallback, exact, fallback,
            )

    async def _recover_turns_from_leases(self, legacy_marker: Path) -> int:
        """Startup recovery on authority. ``.clean_shutdown`` is only read, once, for markers of a
        gateway older than levos 0062 (no lease); it is never written on authority."""
        legacy_clean = legacy_marker.exists()
        resumable = 0
        try:
            resumable = await self.async_session_store.recover_turns_from_leases(
                self._active_turn_marker_max_age(), legacy_clean=legacy_clean)
        except Exception as exc:
            logger.warning("Turn-lease recovery on startup failed: %s", exc)
        if legacy_clean:
            try:
                legacy_marker.unlink()
            except OSError as exc:
                logger.warning("Could not remove the legacy clean-shutdown marker: %s", exc)
        if resumable:
            logger.info("Marked %d session(s) resumable from dead or finished turn owners", resumable)
        return resumable

    async def _turn_lease_watcher(self, interval: Optional[float] = None) -> None:
        """Take over what a dead or finished peer left in PostgreSQL: overlapping pods start before
        the old one exits, so the startup pass cannot see what the old pod leaves when it drains or
        dies. Spawned only on authority."""
        from gateway import turn_owner

        while self._running:
            await asyncio.sleep(turn_owner.LEASE_RENEW_SECONDS if interval is None else interval)
            if not self._running or self._draining:
                return
            await self._sweep_turn_leases()

    async def _sweep_turn_leases(self) -> int:
        resumable = 0
        try:
            resumable = await self.async_session_store.recover_turns_from_leases(
                self._active_turn_marker_max_age())
        except Exception as exc:
            logger.warning("Turn-lease sweep failed: %s", exc)
        if resumable:
            logger.info("Resuming %d session(s) left by a dead or finished peer", resumable)
            self._schedule_resume_pending_sessions()
        try:
            from gateway.shutdown_flush import recover_pending_to_db

            recovered = await asyncio.to_thread(recover_pending_to_db)
            if recovered:
                logger.info("Recovered %d pending message(s) left by a peer", recovered)
        except Exception as exc:
            logger.warning("Pending-message sweep failed: %s", exc)
        return resumable

    async def _hand_over_resume_pending(self, session_keys: Iterable[str]) -> None:
        """Drain timeout on authority: the peer that resumes these sessions is another pod; leave it
        a row it can claim once this process is gone."""
        keys = list(session_keys)
        if not keys or not self._turn_leases_on_postgres():
            return
        try:
            await self.async_session_store.hand_over_turns(keys)
        except Exception as exc:
            logger.warning("Could not hand %d interrupted session(s) over to a peer gateway: %s", len(keys), exc)

    def _record_exit_receipt(self, *, timed_out: bool) -> None:
        """The clean-exit receipt: ``.clean_shutdown`` off authority, the outcome on this process's
        turn leases on authority (a peer pod never sees this disk)."""
        from gateway.run import _hermes_home

        if self._turn_leases_on_postgres():
            from gateway import turn_owner

            try:
                turn_owner.release(turn_owner.OUTCOME_INTERRUPTED if timed_out else turn_owner.OUTCOME_CLEAN)
            except Exception as exc:
                logger.warning(
                    "Could not release this gateway's turn leases; peers take them over when the "
                    "leases run out: %s", exc)
            return
        # Clean-shutdown marker skips suspend_recently_active() next boot; a timed-out drain left
        # half-finished sessions, so no marker — the next startup suspends them.
        if not timed_out:
            with contextlib.suppress(Exception):
                (_hermes_home / ".clean_shutdown").touch()
        else:
            logger.info(
                "Skipping .clean_shutdown marker — drain timed out with "
                "interrupted agents; next startup will suspend recently active sessions."
            )
