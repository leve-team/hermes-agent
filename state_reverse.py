"""Resumable PostgreSQL-to-SQLite reverse backfill for rollback rehearsal."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from state_diff import state_diff_connections
from state_transfer import (
    TableSpec,
    load_checkpoint,
    primary_key_from_row,
    quote_identifier,
    save_checkpoint,
    sqlite_table_specs,
)


DEFAULT_BATCH_ROWS = 5_000


class InjectedReverseFault(RuntimeError):
    """Drill-only reverse-backfill interruption."""


def _safe_source_identity(dsn: str) -> str:
    # Never persist a credential-bearing DSN in the checkpoint.
    return "postgres-sha256:" + hashlib.sha256(dsn.encode("utf-8")).hexdigest()


def default_checkpoint_path(sqlite_path: Path) -> Path:
    return sqlite_path.parent / f".{sqlite_path.name}.pg3-reverse.json"


def _open_postgres_snapshot(dsn: str) -> Any:
    import psycopg
    from psycopg.rows import dict_row

    conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
    conn.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    return conn


def _row_value(row: Any, column: str, index: int) -> Any:
    try:
        return row[column]
    except (KeyError, TypeError, IndexError):
        return row[index]


def _fetch_source_batch(
    source: Any,
    spec: TableSpec,
    last_primary_key: Optional[Sequence[Any]],
    batch_rows: int,
    *,
    dialect: str,
) -> list[Any]:
    columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
    pk_sql = ", ".join(quote_identifier(column) for column in spec.primary_key)
    params: list[Any] = []
    where = ""
    if last_primary_key is not None:
        placeholders = ", ".join("?" for _ in spec.primary_key)
        where = f" WHERE ({pk_sql}) > ({placeholders})"
        params.extend(last_primary_key)
    params.append(batch_rows)
    sql = (
        f"SELECT {columns_sql} FROM {quote_identifier(spec.name)}"
        f"{where} ORDER BY {pk_sql} LIMIT ?"
    )
    if dialect == "postgres":
        sql = sql.replace("?", "%s")
    return source.execute(sql, tuple(params)).fetchall()


def _upsert_batch(
    target: sqlite3.Connection, spec: TableSpec, rows: Sequence[Any]
) -> None:
    if not rows:
        return
    columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
    placeholders = ", ".join("?" for _ in spec.columns)
    conflict_sql = ", ".join(quote_identifier(column) for column in spec.primary_key)
    mutable = [column for column in spec.columns if column not in spec.primary_key]
    action = "DO NOTHING"
    if mutable:
        action = "DO UPDATE SET " + ", ".join(
            f"{quote_identifier(column)} = excluded.{quote_identifier(column)}"
            for column in mutable
        )
    values: list[tuple[Any, ...]] = []
    for row in rows:
        row_values = [
            _row_value(row, column, index) for index, column in enumerate(spec.columns)
        ]
        if spec.name == "sessions" and "parent_session_id" in spec.columns:
            row_values[spec.columns.index("parent_session_id")] = None
        values.append(tuple(row_values))
    target.execute("BEGIN IMMEDIATE")
    try:
        target.executemany(
            f"INSERT INTO {quote_identifier(spec.name)} ({columns_sql}) "
            f"VALUES ({placeholders}) ON CONFLICT ({conflict_sql}) {action}",
            values,
        )
        target.commit()
    except BaseException:
        target.rollback()
        raise


def _restore_session_parents(
    source: Any,
    target: sqlite3.Connection,
    batch_rows: int,
    *,
    source_dialect: str,
) -> None:
    last_id: Optional[str] = None
    while True:
        where = " WHERE id > ?" if last_id is not None else ""
        params: tuple[Any, ...] = (
            (last_id, batch_rows) if last_id is not None else (batch_rows,)
        )
        sql = f"SELECT id, parent_session_id FROM sessions{where} ORDER BY id LIMIT ?"
        if source_dialect == "postgres":
            sql = sql.replace("?", "%s")
        rows = source.execute(sql, params).fetchall()
        if not rows:
            return
        updates = [
            (_row_value(row, "parent_session_id", 1), _row_value(row, "id", 0))
            for row in rows
            if _row_value(row, "parent_session_id", 1) is not None
        ]
        if updates:
            target.execute("BEGIN IMMEDIATE")
            try:
                target.executemany(
                    "UPDATE sessions SET parent_session_id = ? WHERE id = ?",
                    updates,
                )
                target.commit()
            except BaseException:
                target.rollback()
                raise
        last_id = str(_row_value(rows[-1], "id", 0))


def _initialize_sqlite_target(path: Path) -> None:
    from hermes_state import SessionDB

    # An operator may run rollback tooling from a pod where dual-write remains
    # exported.  This explicit override prevents the target rehearsal file from
    # recursively writing back into PostgreSQL.
    db = SessionDB(db_path=path, dual_write=False)
    db.close()


def _reset_sqlite_identity(target: sqlite3.Connection) -> None:
    try:
        target.execute("DELETE FROM sqlite_sequence WHERE name = 'messages'")
        target.execute(
            "INSERT INTO sqlite_sequence(name, seq) "
            "SELECT 'messages', COALESCE(MAX(id), 0) FROM messages"
        )
        target.commit()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc).lower():
            raise


def reverse_backfill(
    dsn: str,
    sqlite_path: Path,
    *,
    checkpoint_path: Optional[Path] = None,
    resume: bool = False,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    fault_inject_at: str | float | None = None,
    _source_factory: Optional[Callable[[str], Any]] = None,
) -> dict[str, Any]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be greater than zero")
    sqlite_path = Path(sqlite_path)
    checkpoint_path = checkpoint_path or default_checkpoint_path(sqlite_path)
    checkpoint_existed = checkpoint_path.exists()
    source_identity = _safe_source_identity(dsn)
    checkpoint = load_checkpoint(
        checkpoint_path,
        source_path=source_identity,
        direction="postgres-to-sqlite",
        resume=resume,
    )
    if isinstance(fault_inject_at, str):
        stripped = fault_inject_at.strip()
        fault_fraction = (
            float(stripped[:-1]) / 100.0 if stripped.endswith("%") else float(stripped)
        )
    elif fault_inject_at is None:
        fault_fraction = None
    else:
        fault_fraction = float(fault_inject_at)
    if fault_fraction is not None and not 0 < fault_fraction <= 1:
        raise ValueError("fault injection point must be in (0, 1]")

    if not checkpoint_existed:
        _initialize_sqlite_target(sqlite_path)
        # Persist the transfer identity before any source row is read.  On
        # resume, reopening the rollback file through SessionDB would run
        # normal SQLite-local schema/FTS maintenance and could manufacture a
        # target-only state_meta row that is absent from the PG snapshot.
        save_checkpoint(checkpoint_path, checkpoint)
    elif not sqlite_path.is_file():
        raise RuntimeError(
            f"reverse checkpoint exists but SQLite target is missing: {sqlite_path}"
        )
    target = sqlite3.connect(sqlite_path, isolation_level=None)
    target.row_factory = sqlite3.Row
    target.execute("PRAGMA foreign_keys=ON")
    source = (_source_factory or _open_postgres_snapshot)(dsn)
    source_dialect = "sqlite" if isinstance(source, sqlite3.Connection) else "postgres"
    started = time.monotonic()
    try:
        specs = sqlite_table_specs(target)
        counts = {
            spec.name: int(
                _row_value(
                    source.execute(
                        f"SELECT COUNT(*) AS count FROM {quote_identifier(spec.name)}"
                    ).fetchone(),
                    "count",
                    0,
                )
            )
            for spec in specs
        }
        total = sum(counts.values())
        processed = sum(
            int((checkpoint["tables"].get(spec.name) or {}).get("rows", 0))
            for spec in specs
        )
        for spec in specs:
            table_state = checkpoint["tables"].setdefault(
                spec.name, {"last_pk": None, "rows": 0, "complete": False}
            )
            if table_state.get("complete"):
                continue
            while True:
                rows = _fetch_source_batch(
                    source,
                    spec,
                    table_state.get("last_pk"),
                    batch_rows,
                    dialect=source_dialect,
                )
                if not rows:
                    table_state["complete"] = True
                    save_checkpoint(checkpoint_path, checkpoint)
                    break
                _upsert_batch(target, spec, rows)
                table_state["last_pk"] = primary_key_from_row(spec, rows[-1])
                table_state["rows"] = int(table_state.get("rows", 0)) + len(rows)
                processed += len(rows)
                save_checkpoint(checkpoint_path, checkpoint)
                if (
                    fault_fraction is not None
                    and total > 0
                    and processed / total >= fault_fraction
                ):
                    raise InjectedReverseFault(
                        f"fault injected after {processed}/{total} rows"
                    )

        if not checkpoint.get("parents_restored"):
            _restore_session_parents(
                source,
                target,
                batch_rows,
                source_dialect=source_dialect,
            )
            checkpoint["parents_restored"] = True
            save_checkpoint(checkpoint_path, checkpoint)
        _reset_sqlite_identity(target)
        diff = state_diff_connections(
            source,
            target,
            specs=specs,
            source_dialect=source_dialect,
            target_dialect="sqlite",
            batch_rows=batch_rows,
        )
        checkpoint["completed"] = diff["clean"]
        save_checkpoint(checkpoint_path, checkpoint)
        return {
            "sqlite_path": str(sqlite_path),
            "checkpoint_path": str(checkpoint_path),
            "rows_by_table": counts,
            "elapsed_seconds": time.monotonic() - started,
            "diff": diff,
            "complete": diff["clean"],
        }
    finally:
        try:
            source.rollback()
        except Exception:
            pass
        source.close()
        target.close()


def _resolve_dsn(value: Optional[str]) -> str:
    dsn = value or (os.environ.get("HERMES_CORE_PG_DSN") or "").strip()
    if not dsn:
        raise RuntimeError("pass --dsn or set HERMES_CORE_PG_DSN")
    return dsn


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="state_reverse")
    parser.add_argument("--dsn")
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--checkpoint")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    parser.add_argument("--fault-inject-at")
    args = parser.parse_args(argv)
    try:
        report = reverse_backfill(
            _resolve_dsn(args.dsn),
            Path(args.sqlite_path),
            checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
            resume=args.resume,
            batch_rows=args.batch_rows,
            fault_inject_at=args.fault_inject_at,
        )
    except Exception as exc:
        print(f"state_reverse failed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
