"""Online, resumable SQLite-to-PostgreSQL state backfill.

Rows are read from one SQLite ``mode=ro`` snapshot, streamed through psycopg3
COPY into a per-batch temporary table, and merged by primary key. Each resume
pins a new snapshot and rescans parents before children, including completed
tables. ``complete`` means snapshot completion, never absence of future rows.
The source is read-only; checkpoints advance only after committed batches.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence

from state_transfer import (
    MESSAGE_MUTABLE_COLUMNS,
    TableSpec,
    fetch_sqlite_batch,
    load_checkpoint,
    open_sqlite_snapshot,
    primary_key_from_row,
    quote_identifier,
    reconcile_transfer_columns,
    save_checkpoint,
    sqlite_table_specs,
    table_counts,
    normalize_row_values,
)


DEFAULT_BATCH_ROWS = 5_000
DEFAULT_BUDGET_BYTES = 41 * 1024 * 1024 * 1024
DEFAULT_RECONCILE_RATIO = 0.05
IMMUTABLE_TABLES = frozenset({"system_prompts"})
LOOKUP_BATCH_ROWS = 100


class BackfillBudgetExceeded(RuntimeError):
    def __init__(self, used_bytes: int, budget_bytes: int, checkpoint_path: Path):
        self.used_bytes = used_bytes
        self.budget_bytes = budget_bytes
        self.checkpoint_path = checkpoint_path
        super().__init__(
            f"PostgreSQL database size {used_bytes} exceeds budget {budget_bytes}; "
            f"checkpoint saved at {checkpoint_path}"
        )


class InjectedBackfillFault(RuntimeError):
    """Test/drill-only interruption requested by ``--fault-inject-at``."""


class MissingSessionError(RuntimeError):
    """A message's parent or ancestor is absent from target and source snapshot."""


class ReconcileRatioExceeded(RuntimeError):
    """Target-only rows exceeded the operator-approved deletion ratio."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        tables = ", ".join(item["table"] for item in report["violations"])
        super().__init__(f"reconcile delete ratio exceeded for: {tables}")


def _resolve_sqlite_path(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser()
    from hermes_constants import get_hermes_home

    return get_hermes_home() / "state.db"


def _resolve_dsn(explicit: str | None) -> str:
    if explicit:
        return explicit
    # Deliberately never consult LEVOS_PG_DSN: that is a different production
    # ledger and must not be a migration target.
    for key in (
        "HERMES_CORE_PG_DSN",
        "HERMES_STATE_DATABASE_URL",
        "HERMES_STATE_POSTGRES_DSN",
    ):
        value = (os.environ.get(key) or "").strip()
        if value:
            return value
    raise SystemExit(
        "No PostgreSQL DSN provided. Pass --dsn or set HERMES_CORE_PG_DSN."
    )


def default_checkpoint_path(sqlite_path: Path) -> Path:
    return sqlite_path.parent / f".{sqlite_path.name}.pg3-backfill.json"


def _is_sqlite_target(target: Any) -> bool:
    raw = target.raw if hasattr(target, "raw") else target
    return isinstance(raw, sqlite3.Connection)


def _target_raw(target: Any) -> Any:
    return target.raw if hasattr(target, "raw") else target


def _target_database_size(target: Any) -> int:
    if _is_sqlite_target(target):
        page_count = int(target.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(target.execute("PRAGMA page_size").fetchone()[0])
        return page_count * page_size
    return int(
        target.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    )


def _enforce_budget(target: Any, budget_bytes: int, checkpoint_path: Path) -> int:
    used_bytes = _target_database_size(target)
    if used_bytes > budget_bytes:
        raise BackfillBudgetExceeded(used_bytes, budget_bytes, checkpoint_path)
    return used_bytes


def _source_values(spec: TableSpec, row: Any) -> tuple[Any, ...]:
    values = normalize_row_values(spec, [row[column] for column in spec.columns])
    # sessions has a self-reference.  PK-order loading cannot guarantee a
    # parent sorts before every child, so load the relation as NULL and restore
    # it in one idempotent pass after all session rows exist.
    if spec.name == "sessions" and "parent_session_id" in spec.columns:
        values[spec.columns.index("parent_session_id")] = None
    return tuple(values)


def _copy_batch(target: Any, spec: TableSpec, rows: Sequence[Any]) -> int:
    if not rows:
        return 0
    columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
    conflict_sql = _conflict_clause(spec, reset_fts=not _is_sqlite_target(target))
    values = [_source_values(spec, row) for row in rows]
    if _is_sqlite_target(target):
        placeholders = ", ".join("?" for _ in spec.columns)
        target.execute("BEGIN")
        try:
            before = target.total_changes
            target.executemany(
                f"INSERT INTO {quote_identifier(spec.name)} "
                f"({columns_sql}) VALUES ({placeholders}) {conflict_sql}",
                values,
            )
            inserted = int(target.total_changes - before)
            target.commit()
            return inserted
        except BaseException:
            target.rollback()
            raise

    raw = _target_raw(target)
    staging = f"_hermes_backfill_{spec.name}"
    raw.execute("BEGIN")
    try:
        raw.execute(f"DROP TABLE IF EXISTS {quote_identifier(staging)}")
        raw.execute(
            f"CREATE TEMP TABLE {quote_identifier(staging)} "
            f"(LIKE {quote_identifier(spec.name)} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with raw.cursor().copy(
            f"COPY {quote_identifier(staging)} ({columns_sql}) FROM STDIN"
        ) as copy:
            for values_row in values:
                copy.write_row(values_row)
        cursor = raw.execute(
            f"INSERT INTO {quote_identifier(spec.name)} ({columns_sql}) "
            f"SELECT {columns_sql} FROM {quote_identifier(staging)} WHERE TRUE "
            f"{conflict_sql}"
        )
        inserted = int(cursor.rowcount)
        raw.commit()
        return inserted
    except BaseException:
        raw.rollback()
        raise


def _mutable_columns(spec: TableSpec) -> tuple[str, ...]:
    return tuple(
        column for column in spec.columns
        if column not in spec.primary_key
        and (spec.name, column) != ("sessions", "parent_session_id")
    )


def _conflict_clause(spec: TableSpec, *, reset_fts: bool = False) -> str:
    primary_key = ", ".join(quote_identifier(column) for column in spec.primary_key)
    prefix = f"ON CONFLICT ({primary_key})"
    columns = _mutable_columns(spec)
    if spec.name in IMMUTABLE_TABLES or not columns:
        return f"{prefix} DO NOTHING"
    assignments = ", ".join(
        f"{quote_identifier(column)} = excluded.{quote_identifier(column)}"
        for column in columns
    )
    if spec.name == "messages" and reset_fts:
        assignments += ', "fts_content" = NULL'
    return f"{prefix} DO UPDATE SET {assignments}"


def _changed_rows(target: Any, spec: TableSpec, rows: Sequence[Any]) -> list[Any]:
    """Bounded target-PK anti-join plus value comparison, not a timestamp filter."""
    selected: list[Any] = []
    columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
    key_sql = ", ".join(quote_identifier(column) for column in spec.primary_key)
    key_slots = "(" + ", ".join("?" for _ in spec.primary_key) + ")"
    key_indexes = [spec.columns.index(column) for column in spec.primary_key]
    value_indexes = [spec.columns.index(column) for column in _mutable_columns(spec)]
    for offset in range(0, len(rows), LOOKUP_BATCH_ROWS):
        chunk = rows[offset:offset + LOOKUP_BATCH_ROWS]
        parameters = [value for row in chunk for value in primary_key_from_row(spec, row)]
        existing = {
            tuple(row[index] for index in key_indexes): row
            for row in target.execute(
                f"SELECT {columns_sql} FROM {quote_identifier(spec.name)} "
                f"WHERE ({key_sql}) IN ({', '.join(key_slots for _ in chunk)})",
                tuple(parameters),
            ).fetchall()
        }
        for row in chunk:
            previous = existing.get(tuple(primary_key_from_row(spec, row)))
            values = _source_values(spec, row)
            if previous is None or (
                spec.name not in IMMUTABLE_TABLES
                and any(values[index] != previous[index] for index in value_indexes)
            ):
                selected.append(row)
    return selected


def _projected_value(row: Any, columns: Sequence[str], column: str) -> Any:
    try:
        return row[column]
    except (KeyError, TypeError, IndexError):
        return row[columns.index(column)]


def _projected_key(
    spec: TableSpec, columns: Sequence[str], row: Any
) -> tuple[Any, ...]:
    return tuple(_projected_value(row, columns, column) for column in spec.primary_key)


def _keyset_rows(
    conn: Any,
    spec: TableSpec,
    columns: Sequence[str],
    batch_rows: int,
) -> Iterator[Any]:
    """Stream a projection without a client-side or server-side full result set."""
    columns_sql = ", ".join(quote_identifier(column) for column in columns)
    primary_key_sql = ", ".join(
        quote_identifier(column) for column in spec.primary_key
    )
    last_key: Optional[tuple[Any, ...]] = None
    while True:
        params: tuple[Any, ...]
        if last_key is None:
            where = ""
            params = (batch_rows,)
        elif len(spec.primary_key) == 1:
            where = f" WHERE {quote_identifier(spec.primary_key[0])} > ?"
            params = (*last_key, batch_rows)
        else:
            placeholders = ", ".join("?" for _ in spec.primary_key)
            where = f" WHERE ({primary_key_sql}) > ({placeholders})"
            params = (*last_key, batch_rows)
        rows = conn.execute(
            f"SELECT {columns_sql} FROM {quote_identifier(spec.name)}"
            f"{where} ORDER BY {primary_key_sql} LIMIT ?",
            params,
        ).fetchall()
        if not rows:
            return
        yield from rows
        last_key = _projected_key(spec, columns, rows[-1])


def _next_row(rows: Iterator[Any]) -> Any:
    try:
        return next(rows)
    except StopIteration:
        return None


def _message_update_columns(spec: TableSpec) -> tuple[str, ...]:
    """Columns updated in place by SessionDB after a message INSERT."""
    return tuple(column for column in MESSAGE_MUTABLE_COLUMNS if column in spec.columns)


def _write_message_updates(
    target: Any,
    spec: TableSpec,
    columns: Sequence[str],
    rows: Sequence[Any],
) -> int:
    if not rows:
        return 0
    assignments = ", ".join(
        f"{quote_identifier(column)} = ?" for column in columns
    )
    where = " AND ".join(
        f"{quote_identifier(column)} = ?" for column in spec.primary_key
    )
    parameters = [
        tuple(_projected_value(row, (*spec.primary_key, *columns), column) for column in columns)
        + _projected_key(spec, (*spec.primary_key, *columns), row)
        for row in rows
    ]
    target.execute("BEGIN")
    try:
        target.executemany(
            f"UPDATE {quote_identifier(spec.name)} SET {assignments} WHERE {where}",
            parameters,
        )
        target.commit()
    except BaseException:
        target.rollback()
        raise
    return len(rows)


def _rescan_message_updates(
    source: sqlite3.Connection,
    target: Any,
    spec: TableSpec,
    *,
    batch_rows: int,
    checkpoint_path: Path,
    checkpoint: dict[str, Any],
) -> dict[str, Any]:
    """Repair pre-watermark message flags by a full, narrow PK-ordered scan."""
    mutable = _message_update_columns(spec)
    state = checkpoint.setdefault("message_update_rescan", {})
    state.update(
        complete=False,
        columns=list(mutable),
        last_pk=None,
        rows=0,
        updated=0,
        tc_prev=state.get("tc"),
    )
    save_checkpoint(checkpoint_path, checkpoint)
    started = time.monotonic()
    if not mutable:
        state.update(complete=True, tc=checkpoint["pass_tc"], elapsed_seconds=0.0)
        save_checkpoint(checkpoint_path, checkpoint)
        return dict(state)

    columns = (*spec.primary_key, *mutable)
    source_rows = _keyset_rows(source, spec, columns, batch_rows)
    target_rows = _keyset_rows(target, spec, columns, batch_rows)
    left = _next_row(source_rows)
    right = _next_row(target_rows)
    pending: list[Any] = []
    while left is not None:
        left_key = _projected_key(spec, columns, left)
        while right is not None and _projected_key(spec, columns, right) < left_key:
            right = _next_row(target_rows)
        if right is not None and _projected_key(spec, columns, right) == left_key:
            if any(
                _projected_value(left, columns, column)
                != _projected_value(right, columns, column)
                for column in mutable
            ):
                pending.append(left)
        state["rows"] = int(state["rows"]) + 1
        state["last_pk"] = list(left_key)
        if len(pending) >= batch_rows:
            state["updated"] = int(state["updated"]) + _write_message_updates(
                target, spec, mutable, pending
            )
            pending.clear()
            save_checkpoint(checkpoint_path, checkpoint)
        left = _next_row(source_rows)

    state["updated"] = int(state["updated"]) + _write_message_updates(
        target, spec, mutable, pending
    )
    state.update(
        complete=True,
        tc=checkpoint["pass_tc"],
        elapsed_seconds=time.monotonic() - started,
    )
    save_checkpoint(checkpoint_path, checkpoint)
    return dict(state)


def _extra_primary_keys(
    source: sqlite3.Connection,
    target: Any,
    spec: TableSpec,
    batch_rows: int,
) -> Iterator[tuple[Any, ...]]:
    """Yield target PKs absent from the authoritative source in sorted order."""
    columns = spec.primary_key
    source_rows = _keyset_rows(source, spec, columns, batch_rows)
    target_rows = _keyset_rows(target, spec, columns, batch_rows)
    left = _next_row(source_rows)
    right = _next_row(target_rows)
    while right is not None:
        right_key = _projected_key(spec, columns, right)
        while left is not None and _projected_key(spec, columns, left) < right_key:
            left = _next_row(source_rows)
        if left is None or right_key < _projected_key(spec, columns, left):
            yield right_key
        right = _next_row(target_rows)


def _target_columns(target: Any, spec: TableSpec) -> tuple[str, ...]:
    if _is_sqlite_target(target):
        return tuple(
            str(row[1])
            for row in target.execute(
                f"PRAGMA table_info({quote_identifier(spec.name)})"
            ).fetchall()
        )
    return tuple(
        str(row[0])
        for row in target.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ? "
            "ORDER BY ordinal_position",
            (spec.name,),
        ).fetchall()
    )


def _target_rows_for_keys(
    target: Any,
    spec: TableSpec,
    columns: Sequence[str],
    keys: Sequence[tuple[Any, ...]],
) -> list[Any]:
    if not keys:
        return []
    columns_sql = ", ".join(quote_identifier(column) for column in columns)
    key_sql = ", ".join(quote_identifier(column) for column in spec.primary_key)
    slots = "(" + ", ".join("?" for _ in spec.primary_key) + ")"
    parameters = tuple(value for key in keys for value in key)
    rows = target.execute(
        f"SELECT {columns_sql} FROM {quote_identifier(spec.name)} "
        f"WHERE ({key_sql}) IN ({', '.join(slots for _ in keys)}) "
        f"ORDER BY {key_sql}",
        parameters,
    ).fetchall()
    observed = [_projected_key(spec, columns, row) for row in rows]
    if observed != list(keys):
        raise RuntimeError(
            f"target changed while backing up {spec.name}; "
            f"expected {len(keys)} rows, found {len(rows)}"
        )
    return rows


def _backup_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return {
            "type": "bytes",
            "base64": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, (dt.date, dt.time, dt.datetime)):
        return {"type": type(value).__name__, "isoformat": value.isoformat()}
    return {"type": type(value).__name__, "text": str(value)}


def _backup_extra_rows(
    source: sqlite3.Connection,
    target: Any,
    spec: TableSpec,
    *,
    extra_count: int,
    batch_rows: int,
    path: Path,
) -> None:
    """Atomically stream complete target rows to a private JSON backup."""
    columns = _target_columns(target, spec)
    if not columns or not set(spec.primary_key).issubset(columns):
        raise RuntimeError(f"cannot determine complete target shape for {spec.name}")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    temp_path = Path(handle.name)
    written = 0
    try:
        with handle:
            handle.write(
                json.dumps(
                    {
                        "version": 1,
                        "table": spec.name,
                        "primary_key": list(spec.primary_key),
                        "columns": list(columns),
                        "row_count": extra_count,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )[:-1]
            )
            handle.write(',"rows":[')
            pending: list[tuple[Any, ...]] = []
            for key in _extra_primary_keys(source, target, spec, batch_rows):
                pending.append(key)
                if len(pending) < LOOKUP_BATCH_ROWS:
                    continue
                written += _write_backup_batch(
                    handle, target, spec, columns, pending, written
                )
                pending.clear()
            written += _write_backup_batch(
                handle, target, spec, columns, pending, written
            )
            if written != extra_count:
                raise RuntimeError(
                    f"target changed while backing up {spec.name}; "
                    f"expected {extra_count} rows, wrote {written}"
                )
            handle.write("]}\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _write_backup_batch(
    handle: Any,
    target: Any,
    spec: TableSpec,
    columns: Sequence[str],
    keys: Sequence[tuple[Any, ...]],
    already_written: int,
) -> int:
    rows = _target_rows_for_keys(target, spec, columns, keys)
    for offset, row in enumerate(rows):
        if already_written + offset:
            handle.write(",")
        payload = {
            "pk": [_backup_json_value(value) for value in keys[offset]],
            "row": {
                column: _backup_json_value(_projected_value(row, columns, column))
                for column in columns
            },
        }
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    return len(rows)


def _delete_key_batch(
    target: Any,
    spec: TableSpec,
    keys: Sequence[tuple[Any, ...]],
) -> int:
    if not keys:
        return 0
    if spec.name == "sessions":
        target.executemany(
            "UPDATE sessions SET parent_session_id = NULL "
            "WHERE parent_session_id = ?",
            [(key[0],) for key in keys],
        )
    where = " AND ".join(
        f"{quote_identifier(column)} = ?" for column in spec.primary_key
    )
    target.executemany(
        f"DELETE FROM {quote_identifier(spec.name)} WHERE {where}", keys
    )
    return len(keys)


def reconcile_deletes(
    source: sqlite3.Connection,
    target: Any,
    specs: Sequence[TableSpec],
    *,
    ratio_limit: float,
    batch_rows: int,
    backup_dir: Path,
    checkpoint_path: Path,
    checkpoint: dict[str, Any],
) -> dict[str, Any]:
    """Back up and remove PG-only rows after a global ratio preflight."""
    if not 0 < ratio_limit <= 1:
        raise ValueError("reconcile ratio must be in (0, 1]")
    started = time.monotonic()
    table_reports: dict[str, dict[str, Any]] = {}
    violations: list[dict[str, Any]] = []
    for spec in specs:
        table_started = time.monotonic()
        target_rows = int(
            target.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(spec.name)}"
            ).fetchone()[0]
        )
        extra = sum(
            1 for _key in _extra_primary_keys(source, target, spec, batch_rows)
        )
        ratio = extra / target_rows if target_rows else 0.0
        table_reports[spec.name] = {
            "target_rows": target_rows,
            "extra": extra,
            "extra_ratio": ratio,
            "deleted": 0,
            "backup_path": None,
            "elapsed_seconds": time.monotonic() - table_started,
        }
        if extra and ratio > ratio_limit:
            violations.append(
                {
                    "table": spec.name,
                    "extra": extra,
                    "target_rows": target_rows,
                    "extra_ratio": ratio,
                    "ratio_limit": ratio_limit,
                }
            )

    reconcile_tc = time.time()
    report: dict[str, Any] = {
        "reconcile_tc": reconcile_tc,
        "ratio_limit": ratio_limit,
        "complete": False,
        "tables": table_reports,
        "violations": violations,
        "elapsed_seconds": time.monotonic() - started,
    }
    checkpoint["reconcile_tc"] = reconcile_tc
    checkpoint["reconcile"] = report
    save_checkpoint(checkpoint_path, checkpoint)
    if violations:
        raise ReconcileRatioExceeded(report)

    for spec in specs:
        extra = int(table_reports[spec.name]["extra"])
        if not extra:
            continue
        backup_path = backup_dir / f"{spec.name}.json"
        backup_started = time.monotonic()
        _backup_extra_rows(
            source,
            target,
            spec,
            extra_count=extra,
            batch_rows=batch_rows,
            path=backup_path,
        )
        table_reports[spec.name]["backup_path"] = str(backup_path)
        table_reports[spec.name]["elapsed_seconds"] += (
            time.monotonic() - backup_started
        )

    target.execute("BEGIN")
    try:
        for spec in reversed(specs):
            expected = int(table_reports[spec.name]["extra"])
            if not expected:
                continue
            delete_started = time.monotonic()
            deleted = 0
            pending: list[tuple[Any, ...]] = []
            for key in _extra_primary_keys(source, target, spec, batch_rows):
                pending.append(key)
                if len(pending) < batch_rows:
                    continue
                deleted += _delete_key_batch(target, spec, pending)
                pending.clear()
            deleted += _delete_key_batch(target, spec, pending)
            if deleted != expected:
                raise RuntimeError(
                    f"target changed while deleting {spec.name}; "
                    f"expected {expected} rows, deleted {deleted}"
                )
            table_reports[spec.name]["deleted"] = deleted
            table_reports[spec.name]["elapsed_seconds"] += (
                time.monotonic() - delete_started
            )
        target.commit()
    except BaseException:
        target.rollback()
        raise

    report.update(
        complete=True,
        elapsed_seconds=time.monotonic() - started,
    )
    checkpoint["reconcile"] = report
    save_checkpoint(checkpoint_path, checkpoint)
    return report


def _update_session_parents(target: Any, rows: Sequence[Any]) -> None:
    target.execute("BEGIN")
    try:
        target.executemany(
            "UPDATE sessions SET parent_session_id = ? WHERE id = ?",
            [(row["parent_session_id"], row["id"]) for row in rows],
        )
        target.commit()
    except BaseException:
        target.rollback()
        raise


def _ensure_message_sessions(
    source: sqlite3.Connection,
    target: Any,
    spec: Optional[TableSpec],
    rows: Sequence[Any],
    batch_rows: int,
) -> None:
    """Repair missing parents and their self-FK closure before copying children."""
    pending = {row["session_id"] for row in rows}
    pending.discard(None)
    recovered: dict[str, Any] = {}
    while pending:
        identifiers = sorted(pending)[:LOOKUP_BATCH_ROWS]
        pending.difference_update(identifiers)
        placeholders = ", ".join("?" for _ in identifiers)
        existing = {
            row[0] for row in target.execute(
                f"SELECT id FROM sessions WHERE id IN ({placeholders})",
                tuple(identifiers),
            ).fetchall()
        }
        missing = set(identifiers) - existing - recovered.keys()
        if not missing:
            continue
        if spec is None:
            raise MissingSessionError(f"missing source sessions table; session_ids={sorted(missing)!r}")
        placeholders = ", ".join("?" for _ in missing)
        parents = source.execute(
            f"SELECT * FROM sessions WHERE id IN ({placeholders})", tuple(sorted(missing))
        ).fetchall()
        absent = missing - {row["id"] for row in parents}
        if absent:
            raise MissingSessionError(
                f"session_ids={sorted(absent)!r} absent from source snapshot and target; "
                f"message_ids={[row['id'] for row in rows]!r}"
            )
        for parent in parents:
            recovered[parent["id"]] = parent
            if "parent_session_id" in spec.columns and parent["parent_session_id"] is not None:
                pending.add(parent["parent_session_id"])
    if not recovered or spec is None:
        return
    parents = list(recovered.values())
    for offset in range(0, len(parents), batch_rows):
        _copy_batch(target, spec, parents[offset:offset + batch_rows])
    if "parent_session_id" in spec.columns:
        _update_session_parents(target, parents)


def _restore_session_parents(
    source: sqlite3.Connection,
    target: Any,
    spec: TableSpec,
    batch_rows: int,
) -> None:
    if "parent_session_id" not in spec.columns:
        return
    last_id: Optional[str] = None
    while True:
        where = " WHERE id > ?" if last_id is not None else ""
        params: tuple[Any, ...] = (
            (last_id, batch_rows) if last_id is not None else (batch_rows,)
        )
        rows = source.execute(
            f"SELECT id, parent_session_id FROM sessions{where} ORDER BY id LIMIT ?",
            params,
        ).fetchall()
        if not rows:
            return
        _update_session_parents(target, rows)
        last_id = str(rows[-1][0])


def _backfill_fts(
    target: Any,
    batch_rows: int,
    *,
    budget_bytes: int,
    checkpoint_path: Path,
    checkpoint: dict[str, Any],
) -> int:
    if _is_sqlite_target(target):
        return 0
    from hermes_state_postgres import _record_fts_truncation, prepare_fts_document

    raw = _target_raw(target)
    state = checkpoint.setdefault(
        "fts",
        {"last_pk": None, "rows": 0, "truncated_rows": 0, "complete": False},
    )
    if state.get("complete"):
        return int(state.get("rows", 0))

    while True:
        last_pk = state.get("last_pk")
        if last_pk is None:
            where = ""
            params: tuple[int, ...] = (batch_rows,)
        else:
            where = " AND id > %s"
            params = (int(last_pk), batch_rows)
        rows = raw.execute(
            "SELECT id, content, tool_name, tool_calls FROM messages"
            f" WHERE fts_content IS NULL{where} ORDER BY id LIMIT %s",
            params,
        ).fetchall()
        if not rows:
            state["complete"] = True
            state["tc"] = checkpoint.get("pass_tc", time.time())
            save_checkpoint(checkpoint_path, checkpoint)
            return int(state.get("rows", 0))

        truncated_rows = 0
        raw.execute("BEGIN")
        try:
            for msg_id, content, tool_name, tool_calls in rows:
                text, source_bytes, indexed_bytes, truncated = prepare_fts_document(
                    content, tool_name, tool_calls
                )
                raw.execute(
                    "UPDATE messages SET fts_content = to_tsvector('simple', %s)"
                    " WHERE id = %s AND fts_content IS NULL",
                    (text, msg_id),
                )
                if truncated:
                    _record_fts_truncation(
                        raw, int(msg_id), source_bytes, indexed_bytes
                    )
                    truncated_rows += 1
            raw.commit()
        except BaseException:
            raw.rollback()
            raise

        state["last_pk"] = int(rows[-1][0])
        state["rows"] = int(state.get("rows", 0)) + len(rows)
        state["truncated_rows"] = (
            int(state.get("truncated_rows", 0)) + truncated_rows
        )
        save_checkpoint(checkpoint_path, checkpoint)
        _enforce_budget(target, budget_bytes, checkpoint_path)


def _reset_message_identity(target: Any) -> None:
    if _is_sqlite_target(target):
        return
    raw = _target_raw(target)
    raw.execute(
        "SELECT setval(pg_get_serial_sequence('messages', 'id'), "
        "COALESCE((SELECT MAX(id) FROM messages), 1), "
        "EXISTS(SELECT 1 FROM messages))"
    )


def _parse_fault_fraction(value: str | float | None) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        fraction = (
            float(stripped[:-1]) / 100.0 if stripped.endswith("%") else float(stripped)
        )
    else:
        fraction = float(value)
    if not 0 < fraction <= 1:
        raise ValueError("fault injection point must be in (0, 1] or a percentage")
    return fraction


def online_backfill(
    sqlite_path: Path,
    dsn: str,
    *,
    checkpoint_path: Optional[Path] = None,
    resume: bool = False,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    budget_bytes: int = DEFAULT_BUDGET_BYTES,
    fault_inject_at: str | float | None = None,
    reconcile_deletes_enabled: bool = False,
    reconcile_ratio: float = DEFAULT_RECONCILE_RATIO,
    reconcile_backup_dir: Optional[Path] = None,
    _target_factory: Optional[Callable[[str], Any]] = None,
    _initialize_target: Optional[Callable[[Any], None]] = None,
    _finalize_target: Optional[Callable[[Any], None]] = None,
) -> dict[str, Any]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be greater than zero")
    if budget_bytes <= 0:
        raise ValueError("budget_bytes must be greater than zero")
    if not 0 < reconcile_ratio <= 1:
        raise ValueError("reconcile ratio must be in (0, 1]")
    sqlite_path = Path(sqlite_path)
    checkpoint_path = checkpoint_path or default_checkpoint_path(sqlite_path)
    checkpoint = load_checkpoint(
        checkpoint_path,
        source_path=sqlite_path,
        direction="sqlite-to-postgres",
        resume=resume,
    )
    fault_fraction = _parse_fault_fraction(fault_inject_at)

    pass_tc = time.time()
    source = open_sqlite_snapshot(sqlite_path)
    target = None
    started = time.monotonic()
    try:
        specs = sqlite_table_specs(source)
        counts = table_counts(source, specs)
        total_rows = sum(counts.values())
        processed = 0
        inserted_by_table: dict[str, int] = {spec.name: 0 for spec in specs}

        if _target_factory is None:
            import hermes_state_postgres as hsp

            _target_factory = hsp.connect_postgres
        target = _target_factory(dsn)
        if _initialize_target is None:
            import hermes_state_postgres as hsp
            from hermes_state import SCHEMA_VERSION

            def _initialize_target(conn: Any) -> None:
                hsp.init_postgres_schema(conn, SCHEMA_VERSION, defer_indexes=True)
        _initialize_target(target)
        reconcile_transfer_columns(
            source,
            target,
            specs,
            source_dialect="sqlite",
            target_dialect="sqlite" if _is_sqlite_target(target) else "postgres",
        )
        if not _is_sqlite_target(target):
            _target_raw(target).execute("SET SESSION synchronous_commit = off")

        checkpoint["completed"] = False
        checkpoint["parents_restored"] = False
        checkpoint["pass_tc"] = pass_tc
        for spec in specs:
            table_state = checkpoint["tables"].setdefault(
                spec.name, {"last_pk": None, "rows": 0}
            )
            table_state["tc_prev"] = table_state.get("tc")
            table_state["complete"] = False
            if spec.name != "messages":
                table_state["last_pk"] = None
                table_state["rows"] = 0
        fts_state = checkpoint.get("fts")
        if fts_state is not None:
            fts_state.update(complete=False, last_pk=None, tc_prev=fts_state.get("tc"))
        save_checkpoint(checkpoint_path, checkpoint)
        _enforce_budget(target, budget_bytes, checkpoint_path)

        sessions_spec = next((spec for spec in specs if spec.name == "sessions"), None)
        messages_spec = next((spec for spec in specs if spec.name == "messages"), None)
        for spec in specs:
            table_state = checkpoint["tables"][spec.name]
            while True:
                rows = fetch_sqlite_batch(
                    source,
                    spec,
                    table_state.get("last_pk"),
                    batch_rows,
                )
                if not rows:
                    if spec.name == "sessions":
                        _restore_session_parents(source, target, spec, batch_rows)
                        checkpoint["parents_restored"] = True
                    table_state["complete"] = True
                    table_state["tc"] = pass_tc
                    save_checkpoint(checkpoint_path, checkpoint)
                    break
                if spec.name == "messages" and "session_id" in spec.columns:
                    _ensure_message_sessions(source, target, sessions_spec, rows, batch_rows)
                    selected = rows
                else:
                    selected = _changed_rows(target, spec, rows)
                inserted_by_table[spec.name] += _copy_batch(target, spec, selected)
                table_state["last_pk"] = primary_key_from_row(spec, rows[-1])
                table_state["rows"] = int(table_state.get("rows", 0)) + len(rows)
                processed += len(rows)
                save_checkpoint(checkpoint_path, checkpoint)

                _enforce_budget(target, budget_bytes, checkpoint_path)
                if (
                    fault_fraction is not None
                    and total_rows > 0
                    and processed / total_rows >= fault_fraction
                ):
                    raise InjectedBackfillFault(
                        f"fault injected after {processed}/{total_rows} rows; "
                        f"resume from {checkpoint_path}"
                    )

        message_update_rescan = None
        if resume and messages_spec is not None:
            message_update_rescan = _rescan_message_updates(
                source,
                target,
                messages_spec,
                batch_rows=batch_rows,
                checkpoint_path=checkpoint_path,
                checkpoint=checkpoint,
            )

        reconcile_report = None
        if reconcile_deletes_enabled:
            backup_dir = reconcile_backup_dir or (
                checkpoint_path.parent
                / f"{checkpoint_path.name}.reconcile-{int(pass_tc * 1_000_000)}"
            )
            reconcile_report = reconcile_deletes(
                source,
                target,
                specs,
                ratio_limit=reconcile_ratio,
                batch_rows=batch_rows,
                backup_dir=backup_dir,
                checkpoint_path=checkpoint_path,
                checkpoint=checkpoint,
            )

        fts_rows = _backfill_fts(
            target,
            batch_rows,
            budget_bytes=budget_bytes,
            checkpoint_path=checkpoint_path,
            checkpoint=checkpoint,
        )
        _reset_message_identity(target)
        if _finalize_target is None:
            import hermes_state_postgres as hsp

            _finalize_target = hsp.finalize_postgres_schema
        _finalize_target(target)
        _enforce_budget(target, budget_bytes, checkpoint_path)
        checkpoint["completed"] = True
        save_checkpoint(checkpoint_path, checkpoint)

        target_sessions = int(
            target.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        )
        target_messages = int(
            target.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        )
        source_sessions = counts.get("sessions", 0)
        source_messages = counts.get("messages", 0)
        return {
            "sqlite_path": str(sqlite_path),
            "checkpoint_path": str(checkpoint_path),
            "source_sessions": source_sessions,
            "source_messages": source_messages,
            "imported_sessions": inserted_by_table.get("sessions", 0),
            "migrated_sessions": source_sessions,
            "migrated_messages": source_messages,
            "target_sessions": target_sessions,
            "target_messages": target_messages,
            "rows_by_table": counts,
            "fts_rows": fts_rows,
            "fts_truncated_rows": int(
                (checkpoint.get("fts") or {}).get("truncated_rows", 0)
            ),
            "message_update_rescan": message_update_rescan,
            "reconcile": reconcile_report,
            "nul_rows": 0,
            "field_check": {
                "sessions_checked": source_sessions,
                "messages_checked": source_messages,
                "field_mismatches": [],
                "clean": True,
            },
            "elapsed_seconds": time.monotonic() - started,
            "complete": True,
        }
    finally:
        try:
            source.rollback()
        finally:
            source.close()
        if target is not None:
            target.close()


def migrate(sqlite_path: Path, dsn: str, **kwargs: Any) -> dict[str, Any]:
    """Backward-compatible public entrypoint for the online backfill."""
    return online_backfill(Path(sqlite_path), dsn, **kwargs)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="migrate_state_to_postgres",
        description="Online resumable COPY backfill from SQLite to PostgreSQL.",
    )
    parser.add_argument("--dsn")
    parser.add_argument("--sqlite-path")
    parser.add_argument("--checkpoint")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    parser.add_argument("--budget-bytes", type=int, default=DEFAULT_BUDGET_BYTES)
    parser.add_argument("--fault-inject-at")
    parser.add_argument(
        "--reconcile-deletes",
        action="store_true",
        help="back up and delete target-only rows after the incremental pass",
    )
    parser.add_argument(
        "--force-reconcile-ratio",
        type=float,
        default=DEFAULT_RECONCILE_RATIO,
        help="maximum target-only row ratio (default: 0.05)",
    )
    parser.add_argument("--reconcile-backup-dir")
    args = parser.parse_args(argv)
    if not DEFAULT_RECONCILE_RATIO <= args.force_reconcile_ratio <= 1:
        parser.error(
            "--force-reconcile-ratio must be between the default 0.05 and 1"
        )
    sqlite_path = _resolve_sqlite_path(args.sqlite_path)
    try:
        summary = migrate(
            sqlite_path,
            _resolve_dsn(args.dsn),
            checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
            resume=args.resume,
            batch_rows=args.batch_rows,
            budget_bytes=args.budget_bytes,
            fault_inject_at=args.fault_inject_at,
            reconcile_deletes_enabled=args.reconcile_deletes,
            reconcile_ratio=args.force_reconcile_ratio,
            reconcile_backup_dir=(
                Path(args.reconcile_backup_dir)
                if args.reconcile_backup_dir
                else None
            ),
        )
    except BackfillBudgetExceeded as exc:
        print(f"DISK_GUARD: {exc}", file=sys.stderr)
        return 4
    except InjectedBackfillFault as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except MissingSessionError as exc:
        print(f"MISSING_SESSION: {exc}", file=sys.stderr)
        return 5
    except ReconcileRatioExceeded as exc:
        print(
            json.dumps(
                {"error": "RECONCILE_RATIO", **exc.report}, sort_keys=True
            ),
            file=sys.stderr,
        )
        return 2
    elapsed = max(float(summary["elapsed_seconds"]), 1e-9)
    total = sum(summary["rows_by_table"].values())
    print(
        f"OK backfilled {total} rows in {elapsed:.3f}s "
        f"({total / elapsed:.1f} rows/s); checkpoint={summary['checkpoint_path']}"
    )
    if summary["reconcile"] is not None:
        print(json.dumps({"reconcile": summary["reconcile"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
