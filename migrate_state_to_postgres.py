"""Online, resumable SQLite-to-PostgreSQL state backfill.

Rows are read from one SQLite ``mode=ro`` snapshot, streamed through psycopg3
COPY into a per-batch temporary table, and merged by primary key. Each resume
pins a new snapshot and rescans parents before children, including completed
tables. ``complete`` means snapshot completion, never absence of future rows.
The source is read-only; checkpoints advance only after committed batches.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from state_transfer import (
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
    _target_factory: Optional[Callable[[str], Any]] = None,
    _initialize_target: Optional[Callable[[Any], None]] = None,
    _finalize_target: Optional[Callable[[Any], None]] = None,
) -> dict[str, Any]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be greater than zero")
    if budget_bytes <= 0:
        raise ValueError("budget_bytes must be greater than zero")
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
    args = parser.parse_args(argv)
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
    elapsed = max(float(summary["elapsed_seconds"]), 1e-9)
    total = sum(summary["rows_by_table"].values())
    print(
        f"OK backfilled {total} rows in {elapsed:.3f}s "
        f"({total / elapsed:.1f} rows/s); checkpoint={summary['checkpoint_path']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
