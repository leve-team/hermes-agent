"""Caller-pinned preconditions for ``session.undo``: ``expect_row_id`` + ``expect_tail_row_ids``.

A client that reads the messages table and then asks the core to undo needs the core to refuse,
never silently widen the undo, when the transcript moved in between (another participant's turn,
rows inserted directly by another process, the core's own turn). Parsing and the error mapping are
pure; the durable reads run in ``session_workdir._rewind_active_session_history`` and the tail is
pinned inside ``SessionDB.rewind_to_message``'s write transaction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from hermes_state_errors import RewindExpectationMismatch

# Advertised by ``gateway.capabilities`` (``session_undo_expectations``) and echoed as ``checked``. A module
# constant, not a config flag: an older core ignores unknown params and undoes unconditionally, so a client
# must not send them unless this build lists them. Sent together or not at all.
SESSION_UNDO_EXPECTATIONS = ("expect_row_id", "expect_tail_row_ids")

INVALID_PARAMS_CODE = 4004
TARGET_MISMATCH_CODE = 4033
TAIL_MISMATCH_CODE = 4034
# One undone turn is the user row plus its reply and tool traffic. Also caps ``actual`` in a 4034.
MAX_EXPECT_TAIL_ROW_IDS = 4096
_MAX_ROW_ID = 2**63 - 1  # SQLite rowid / PostgreSQL bigint ceiling


class UndoExpectationParamError(ValueError):
    """Malformed ``expect_*`` params; nothing was read or changed."""


@dataclass(frozen=True)
class UndoExpectation:
    """The rewind target row and the exact ordered active ids the undo may deactivate (target first)."""

    expect_row_id: int
    expect_tail_row_ids: tuple[int, ...]

    def check_target(self, actual_row_id: Optional[int]) -> None:
        """Raise unless ``actual_row_id`` (None: the core has no durable target) is the expected row."""
        if actual_row_id != self.expect_row_id:
            raise RewindExpectationMismatch("expect_row_id", self.expect_row_id, actual_row_id)


def _row_id(name: str, value: Any) -> int:
    # type() not isinstance(): JSON true is a bool, which isinstance(.., int) would accept as 1.
    if type(value) is not int or not 1 <= value <= _MAX_ROW_ID:
        raise UndoExpectationParamError(f"{name} must be a positive integer row id")
    return value


def parse_undo_expectation(params: Mapping[str, Any]) -> Optional[UndoExpectation]:
    """``UndoExpectation`` when both keys are present, None when neither is. Presence decides: a present
    key with any invalid value, ``null`` included, is an error, never an unconditional undo."""
    present = [name for name in SESSION_UNDO_EXPECTATIONS if name in params]
    if not present:
        return None
    if len(present) != len(SESSION_UNDO_EXPECTATIONS):
        raise UndoExpectationParamError("expect_row_id and expect_tail_row_ids must be sent together")
    row_id, raw_tail = _row_id("expect_row_id", params["expect_row_id"]), params["expect_tail_row_ids"]
    if not isinstance(raw_tail, list) or not 1 <= len(raw_tail) <= MAX_EXPECT_TAIL_ROW_IDS:
        raise UndoExpectationParamError(
            f"expect_tail_row_ids must be a list of 1..{MAX_EXPECT_TAIL_ROW_IDS} row ids")
    tail = tuple(_row_id("expect_tail_row_ids[]", value) for value in raw_tail)
    if tail[0] != row_id:
        raise UndoExpectationParamError("expect_tail_row_ids must start with expect_row_id")
    if any(earlier >= later for earlier, later in zip(tail, tail[1:])):
        raise UndoExpectationParamError("expect_tail_row_ids must be strictly increasing")
    return UndoExpectation(row_id, tail)


def undo_expectation_error(expectation: UndoExpectation, exc: RewindExpectationMismatch) -> tuple[int, str, dict]:
    """``(code, message, data)`` for a conditional undo refused before commit. 4034: the rows from the target
    on differ (``actual`` = the active ids found in the rewind transaction, capped). 4033 otherwise: the target
    is another row (``actual`` its id) or the core could not confirm one (``actual`` null)."""
    if exc.condition == "expected_tail_row_ids":
        actual = list(exc.actual)
        return TAIL_MISMATCH_CODE, "undo refused: the active rows from expect_row_id onward changed", {
            "expected": list(expectation.expect_tail_row_ids), "actual": actual[:MAX_EXPECT_TAIL_ROW_IDS],
            "actual_truncated": len(actual) > MAX_EXPECT_TAIL_ROW_IDS}
    actual = exc.actual if exc.condition == "expect_row_id" else None
    return TARGET_MISMATCH_CODE, f"undo refused: expect_row_id is not the confirmed last user turn ({exc})", {
        "expected": expectation.expect_row_id, "actual": actual}
