"""How much the model has written so far in one stream attempt, by phase — the counts behind
``generation.progress``.

Pure: ``Tally`` is immutable and every function returns a new value; no I/O and no clock. The
mutable holder lives in ``agent.generation_progress.GenerationProgressReporter``.

``tool_index`` is the tool call's ORDINAL within the attempt (0, 1, …) on every wire: feeders pass
their own wire key (content-block index, ``output_index``, chat delta index) and the tally maps it.
Counts are characters, never tokens.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

GENERATION_PHASES = frozenset({"reasoning", "tool_call", "text"})
PROGRESS_MIN_INTERVAL_S = 1.0
MAX_REPORTED_COUNT = 2**53 - 1  # exact in a JSON number on every client
MAX_TOOL_NAME_CHARS = 256


@dataclass(frozen=True)
class Tally:
    phase: Optional[str] = None
    name: Optional[str] = None
    tool_index: Optional[int] = None
    phase_chars: int = 0
    output_chars: int = 0
    # Wire key and name per tool ordinal seen in this attempt.
    tool_keys: tuple = ()
    tool_names: tuple = ()
    # The provider reported absolute counts: they are authoritative for the rest of the attempt.
    provider_reported: bool = False


def advance(tally: Tally, phase: str, n_chars: int, *, name: Optional[str] = None, tool_key: Any = None) -> Tally:
    """Count ``n_chars`` more output in ``phase``. For ``tool_call``, an unseen ``tool_key`` — or a
    different ``name`` at a known key (Ollama reuses index 0 per call) — starts the next ordinal."""
    if tally.provider_reported:
        return tally
    keys, names, index = tally.tool_keys, tally.tool_names, None
    if phase == "tool_call":
        index = next((i for i in range(len(keys) - 1, -1, -1) if keys[i] == tool_key), None)
        if index is None or (name is not None and names[index] is not None and names[index] != name):
            index, keys, names = len(keys), keys + (tool_key,), names + (name,)
        elif name is not None and names[index] is None:
            names = names[:index] + (name,) + names[index + 1:]
    same = phase == tally.phase and index == tally.tool_index
    return Tally(phase=phase, name=names[index] if index is not None else None, tool_index=index,
                 phase_chars=(tally.phase_chars if same else 0) + n_chars, output_chars=tally.output_chars + n_chars,
                 tool_keys=keys, tool_names=names)


def adopt(reported: dict[str, Any]) -> Tally:
    """Absolute cumulative counts reported by the provider (see ``parse_provider_progress``)."""
    return Tally(phase=reported["phase"], name=reported["name"], tool_index=reported["tool_index"],
                 phase_chars=reported["phase_chars"], output_chars=reported["output_chars"], provider_reported=True)


def progress_payload(tally: Tally, *, api_call: int, attempt: int, elapsed_ms: int) -> Optional[dict[str, Any]]:
    """The ``generation.progress`` payload; None before any output was seen."""
    if tally.phase is None:
        return None
    payload: dict[str, Any] = {"api_call": api_call, "attempt": attempt, "phase": tally.phase}
    if tally.phase == "tool_call":
        if tally.name is not None:
            payload["name"] = tally.name
        if tally.tool_index is not None:
            payload["tool_index"] = tally.tool_index
    payload.update(output_chars=tally.output_chars, phase_chars=tally.phase_chars, elapsed_ms=max(0, elapsed_ms))
    return payload


def progress_key(payload: dict[str, Any]) -> tuple:
    """What must change for a payload to be worth emitting again (elapsed time alone is not)."""
    return tuple(payload.get(k) for k in
                 ("api_call", "attempt", "phase", "name", "tool_index", "phase_chars", "output_chars"))


def progress_due(last_emit_at: Optional[float], last_key: Optional[tuple], payload: Optional[dict[str, Any]],
                 now: float, min_interval_s: float = PROGRESS_MIN_INTERVAL_S) -> bool:
    """Throttle: at most once per ``min_interval_s`` and only when the counts moved."""
    if payload is None:
        return False
    if last_emit_at is not None and now - last_emit_at < min_interval_s:
        return False
    return progress_key(payload) != last_key


def parse_provider_progress(raw: Any) -> Optional[dict[str, Any]]:
    """Validate a provider-reported ``x_hermes_progress`` object (mapping or attribute namespace) into
    ``adopt`` input. Anything malformed or out of range returns None — ignored, never raised."""
    fields = raw if isinstance(raw, dict) else getattr(raw, "__dict__", None)
    if not isinstance(fields, dict):
        return None
    phase, name, tool_index = fields.get("phase"), fields.get("name"), fields.get("tool_index")
    phase_chars, output_chars = fields.get("phase_chars"), fields.get("output_chars")
    if not isinstance(phase, str) or phase not in GENERATION_PHASES:
        return None
    if not (_is_count(phase_chars) and _is_count(output_chars)):
        return None
    if name is not None and not (isinstance(name, str) and len(name) <= MAX_TOOL_NAME_CHARS):
        return None
    if tool_index is not None and not _is_count(tool_index):
        return None
    return {"phase": phase, "phase_chars": phase_chars, "output_chars": output_chars,
            "name": name or None, "tool_index": tool_index}


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_REPORTED_COUNT
