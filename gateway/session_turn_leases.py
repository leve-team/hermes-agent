"""SessionStore side of the PostgreSQL turn leases (levos 0062). Mixin bound onto ``SessionStore``
via the MRO.

``.clean_shutdown`` cannot decide a peer pod's turns, so on authority a turn marker is settled only
after its lease was claimed from a dead or finished owner (``gateway.turn_owner.claim_dead``). The
routing entry is re-read from PostgreSQL and written back one row at a time, so a peer's live turns
and rows are never touched.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from gateway.session import SessionEntry

# Log-record parity with the origin module.
logger = logging.getLogger("gateway.session")


class SessionTurnLeasesMixin:
    """Recovery, hand-over and lease bookkeeping of turn markers on PostgreSQL authority."""

    @staticmethod
    def _drop_turn_lease(token: str) -> None:
        """Best-effort: a lease row left behind is renewed by this process and released as
        ``clean`` at exit, which a peer then discards."""
        from gateway import turn_owner

        if not turn_owner.is_leased(token):
            return
        try:
            turn_owner.drop(token)
        except Exception as exc:
            logger.warning("Could not drop the turn lease of a concluded turn: %s", exc)

    def recover_turns_from_leases(self, max_age_seconds: int = 60 * 60, *,
                                  legacy_clean: Optional[bool] = None) -> int:
        """Settle the markers whose lease this process claimed; run at startup and periodically.

        *legacy_clean* (startup only) settles markers written before levos 0062, which have no
        lease: ``True`` discards them (the previous gateway left ``.clean_shutdown``), ``False``
        resumes them. Returns the number of sessions made resumable.
        """
        from gateway import turn_owner

        resumable = 0
        if legacy_clean is not None:
            resumable += self._settle_legacy_turn_markers(max_age_seconds, discard=legacy_clean)
        claims = turn_owner.claim_dead(self._routing_scope())
        if not claims:
            return resumable
        loader = self._routing_db_method("load_gateway_routing_entries")
        if loader is None:
            return resumable
        # Re-read from PostgreSQL: the peer may have created or changed these entries after this
        # pod loaded its routing index.
        routing = loader(scope=self._routing_scope())
        for claim in claims:
            if self._adopt_claimed_turn(claim, routing.get(claim.session_key), max_age_seconds):
                resumable += 1
        return resumable

    def _settle_legacy_turn_markers(self, max_age_seconds: int, *, discard: bool) -> int:
        from gateway import turn_owner
        from gateway.session_lifecycle import _now

        now = _now()
        promoted = 0
        with self._lock:
            self._ensure_loaded_locked()
            for key, entry in list(self._entries.items()):
                token = entry.active_turn_token
                if not token or turn_owner.is_leased(token):
                    continue
                if discard:
                    entry.active_turn_token = None
                    entry.active_turn_started_at = None
                elif self._settle_turn_marker(entry, now, max_age_seconds):
                    promoted += 1
                self._save_entry(key, lock_held=True)
        return promoted

    def _adopt_claimed_turn(self, claim: Any, entry_json: Optional[str], max_age_seconds: int) -> bool:
        """Settle the routing entry (as stored now) a claimed lease pointed at. True when the
        session is left resumable for ``GatewayRunner._schedule_resume_pending_sessions``."""
        from gateway import turn_owner
        from gateway.session_lifecycle import _now

        key = claim.session_key
        fresh: Optional[SessionEntry] = (
            self._routing_entry_from_json(key, entry_json) if entry_json else None)
        if fresh is None:
            return False
        if claim.handoff:
            # The draining owner armed resume_pending and its turn unwound. A marker means someone
            # has started a newer turn since.
            if fresh.active_turn_token or fresh.suspended or not fresh.resume_pending:
                return False
            with self._lock:
                self._ensure_loaded_locked()
                self._entries[key] = fresh
            return True
        if fresh.active_turn_token != claim.token:
            return False  # concluded, or superseded by a newer turn
        with self._lock:
            self._ensure_loaded_locked()
            if claim.outcome == turn_owner.OUTCOME_CLEAN:
                fresh.active_turn_token = None
                fresh.active_turn_started_at = None
                resumable = False
            else:
                self._settle_turn_marker(fresh, _now(), max_age_seconds)
                resumable = fresh.resume_pending and not fresh.suspended
            self._entries[key] = fresh
            self._save_entry(key, lock_held=True)
        return resumable

    def hand_over_turns(self, session_keys: List[str]) -> int:
        """Leave the resume_pending sessions of a draining gateway to a peer."""
        from gateway import turn_owner

        return turn_owner.hand_over(scope=self._routing_scope(), session_keys=session_keys)
