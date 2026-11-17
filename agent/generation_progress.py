"""``generation.progress`` reporting for one model call — the mutable boundary around the pure tally.

The provider stream feeds ``add`` / ``adopt`` from its worker thread; the request's existing poll
thread calls ``poll()`` (~every 0.3 s) while the call is live, which emits through
``agent.generation_progress_callback`` at most once per second and only when the counts moved.
Without that callback no reporter exists, so the stream hot path pays one ``is None`` check.

``api_call`` is which model call of the turn this is; ``attempt`` counts stream attempts of that
model call (core stream retries, turn-level retries, fallback) and restarts the counts.
``elapsed_ms`` runs from the model call's first attempt.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from agent.generation_meter import Tally, adopt, advance, progress_due, progress_key, progress_payload

logger = logging.getLogger(__name__)


class GenerationProgressReporter:
    def __init__(self, callback: Callable[[dict], Any], api_call: int, attempts: dict,
                 clock: Callable[[], float] = time.monotonic):
        self._callback, self._clock, self._attempts = callback, clock, attempts
        self.api_call = api_call
        self.attempt = attempts["attempt"]
        self._tally = Tally()  # each update swaps one immutable value: the poll thread never reads a torn one
        self._last_emit_at: Optional[float] = None
        self._last_key: Optional[tuple] = None

    def start_attempt(self) -> None:
        """A new stream attempt regenerates the output: next ``attempt``, counts from zero."""
        self._attempts["attempt"] += 1
        self.attempt = self._attempts["attempt"]
        self._tally = Tally()

    def add(self, phase: str, n_chars: int, *, name: Optional[str] = None, tool_key: Any = None) -> None:
        self._tally = advance(self._tally, phase, n_chars, name=name, tool_key=tool_key)

    def adopt(self, reported: dict[str, Any]) -> None:
        self._tally = adopt(reported)

    def poll(self) -> None:
        """Emit the current counts when due; a failing display callback never breaks the request."""
        now = self._clock()
        payload = progress_payload(self._tally, api_call=self.api_call, attempt=self.attempt,
                                   elapsed_ms=int((now - self._attempts["started_at"]) * 1000))
        if not progress_due(self._last_emit_at, self._last_key, payload, now):
            return
        self._last_emit_at, self._last_key = now, progress_key(payload)
        try:
            self._callback(payload)
        except Exception:
            logger.debug("generation_progress_callback raised", exc_info=True)


def open_generation_progress(agent: Any, clock: Callable[[], float] = time.monotonic
                             ) -> Optional[GenerationProgressReporter]:
    """The reporter for the agent's current model call, or None when nobody listens. Attempt
    numbering and the start time survive retries of the same model call (same turn, same api_call)."""
    callback = getattr(agent, "generation_progress_callback", None)
    if callback is None:
        return None
    api_call = int(getattr(agent, "_api_call_count", 0) or 0)
    model_call = (getattr(agent, "_current_turn_id", "") or "", api_call)
    attempts = getattr(agent, "_generation_attempts", None)
    if not isinstance(attempts, dict) or attempts.get("model_call") != model_call:
        attempts = {"model_call": model_call, "attempt": 0, "started_at": clock()}
        agent._generation_attempts = attempts
    return GenerationProgressReporter(callback, api_call, attempts, clock)
