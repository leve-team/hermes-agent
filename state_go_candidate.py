"""Compose the PG3 read-authority go-candidate report from V1 tools."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Optional

from hermes_state_dual import dual_write_dsn
from hermes_state_read import expected_reader_entrypoints
from state_diff import coverage_report, run_state_diff


RC_GO_CANDIDATE = 0
RC_NO_GO = 1
RC_TOOL_ERROR = 2


def _row_sample_count(report: dict[str, Any]) -> int:
    return sum(
        int(counts.get("matched", 0))
        + int(counts.get("missing", 0))
        + int(counts.get("extra", 0))
        + int(counts.get("differ", 0))
        for counts in report.get("tables", {}).values()
    )


def _coverage_percent(report: dict[str, Any]) -> float:
    executed = len(report.get("executed", {}))
    missing = len(report.get("missing", []))
    waived = len(report.get("waived", []))
    total = executed + missing + waived
    return 100.0 if total == 0 else round(executed * 100.0 / total, 2)


def _load_reverse_rehearsal(path: Path) -> bool:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("complete"), bool):
        raise ValueError("V1 reverse rehearsal report must contain boolean complete")
    diff = payload.get("diff")
    if not isinstance(diff, dict) or not isinstance(diff.get("clean"), bool):
        raise ValueError("V1 reverse rehearsal report must contain boolean diff.clean")
    try:
        mismatch_count = int(diff["mismatch_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            "V1 reverse rehearsal report must contain integer diff.mismatch_count"
        ) from exc
    return payload["complete"] and diff["clean"] and mismatch_count == 0


def build_go_candidate_report(
    sqlite_path: Path,
    dsn: str,
    reverse_rehearsal_report: Path,
    *,
    repair: bool = False,
    coverage_waive: Optional[Path] = None,
    diff_runner: Callable[..., dict[str, Any]] = run_state_diff,
    coverage_runner: Callable[..., dict[str, Any]] = coverage_report,
) -> dict[str, Any]:
    """Call V1 diff/repair + coverage APIs and apply the draft Y3 gate."""

    tool_errors: list[dict[str, str]] = []
    repair_report: Optional[dict[str, Any]] = None
    diff_report: dict[str, Any] = {}
    coverage: dict[str, Any] = {}
    reverse_rehearsal_succeeded = False

    try:
        if repair:
            repair_report = diff_runner(sqlite_path, dsn, repair=True)
        # A repair's own report describes the pre-repair delta. A fresh full
        # diff is the only value permitted to drive the decision.
        diff_report = diff_runner(sqlite_path, dsn, repair=False)
    except Exception as exc:
        tool_errors.append({"tool": "state_diff", "error_type": type(exc).__name__})

    try:
        coverage = coverage_runner(sqlite_path, coverage_waive)
    except Exception as exc:
        tool_errors.append({
            "tool": "coverage_report",
            "error_type": type(exc).__name__,
        })

    try:
        reverse_rehearsal_succeeded = _load_reverse_rehearsal(reverse_rehearsal_report)
    except Exception as exc:
        tool_errors.append({
            "tool": "reverse_rehearsal",
            "error_type": type(exc).__name__,
        })

    from hermes_state import SessionDB

    reader_seams = sorted(expected_reader_entrypoints(SessionDB))
    reader_seams_complete = bool(reader_seams)
    coverage_percent = _coverage_percent(coverage)
    coverage_complete = (
        coverage_percent == 100.0
        and not coverage.get("missing", [])
        and not coverage.get("waived", [])
        and not coverage.get("unknown_waivers", [])
    )
    mismatch_count = int(diff_report.get("mismatch_count", -1))
    go_candidate = (
        not tool_errors
        and coverage_complete
        and mismatch_count == 0
        and reverse_rehearsal_succeeded
        and reader_seams_complete
    )
    return {
        "decision": "go-candidate" if go_candidate else "no-go",
        "gate_status": "draft-pending-eren-opsi-confirmation",
        "sample_rows": _row_sample_count(diff_report),
        "mismatch_count": mismatch_count,
        "examples": list(diff_report.get("samples", [])),
        "coverage_percent": coverage_percent,
        "coverage": coverage,
        "tool_error_count": len(tool_errors),
        "tool_errors": tool_errors,
        "reverse_rehearsal_succeeded": reverse_rehearsal_succeeded,
        "reader_seams_complete": reader_seams_complete,
        "reader_seams": reader_seams,
        "repair_requested": repair,
        "repair_report": repair_report,
    }


def main(
    argv: Optional[list[str]] = None,
    *,
    _diff_runner: Callable[..., dict[str, Any]] = run_state_diff,
    _coverage_runner: Callable[..., dict[str, Any]] = coverage_report,
) -> int:
    parser = argparse.ArgumentParser(prog="state_go_candidate")
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--dsn")
    parser.add_argument("--reverse-rehearsal-report", required=True)
    parser.add_argument("--coverage-waive")
    parser.add_argument("--repair", action="store_true")
    args = parser.parse_args(argv)

    dsn = (args.dsn or dual_write_dsn() or "").strip()
    if not dsn:
        print(
            json.dumps(
                {
                    "decision": "no-go",
                    "tool_error_count": 1,
                    "tool_errors": [
                        {"tool": "dsn", "error_type": "MissingConfiguration"}
                    ],
                },
                sort_keys=True,
            )
        )
        return RC_TOOL_ERROR

    report = build_go_candidate_report(
        Path(args.sqlite_path),
        dsn,
        Path(args.reverse_rehearsal_report),
        repair=args.repair,
        coverage_waive=(Path(args.coverage_waive) if args.coverage_waive else None),
        diff_runner=_diff_runner,
        coverage_runner=_coverage_runner,
    )
    print(json.dumps(report, sort_keys=True))
    if report["tool_error_count"]:
        return RC_TOOL_ERROR
    return RC_GO_CANDIDATE if report["decision"] == "go-candidate" else RC_NO_GO


if __name__ == "__main__":
    sys.exit(main())
