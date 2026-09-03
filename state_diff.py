"""Streaming hash diff and repair for SQLite-primary Hermes state."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Sequence

from hermes_state_dual import (
    DualWriteReplicator,
    dual_write_dsn,
    expected_mutation_entrypoints,
    load_coverage,
)
from state_transfer import (
    TableSpec,
    open_sqlite_snapshot,
    quote_identifier,
    sqlite_table_specs,
)


RC_MATCH = 0
RC_DIFFERENT = 1
RC_UNREACHABLE = 2
RECENT_WATERMARK_SECONDS = 300.0
DEFAULT_BATCH_ROWS = 2_000
SAMPLE_LIMIT = 20


def _normalize_value(value: Any) -> dict[str, Any]:
    """Represent DB values with stable, type-explicit JSON rules."""
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bool):
        return {"type": "int", "value": repr(int(value))}
    if isinstance(value, int):
        return {"type": "int", "value": repr(value)}
    if isinstance(value, float):
        return {"type": "float", "value": repr(value)}
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return {"type": "bytes", "value": bytes(value).hex()}
    if isinstance(value, dt.datetime):
        return {"type": "datetime", "value": value.isoformat()}
    return {"type": "str", "value": str(value)}


def canonical_row_json(
    columns: Sequence[str], row: Any, *, ignored_columns: Iterable[str] = ()
) -> str:
    ignored = frozenset(ignored_columns)
    normalized = {
        column: _normalize_value(_row_value(row, column, index))
        for index, column in enumerate(columns)
        if column not in ignored
    }
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def row_hash(columns: Sequence[str], row: Any) -> str:
    return hashlib.sha256(canonical_row_json(columns, row).encode("utf-8")).hexdigest()


def _row_value(row: Any, column: str, index: int) -> Any:
    try:
        return row[column]
    except (KeyError, TypeError, IndexError):
        return row[index]


def _row_dict(spec: TableSpec, row: Any) -> dict[str, Any]:
    return {
        column: _row_value(row, column, index)
        for index, column in enumerate(spec.columns)
    }


def _key(spec: TableSpec, row: Any) -> tuple[Any, ...]:
    values = _row_dict(spec, row)
    return tuple(values[column] for column in spec.primary_key)


def _pg_sql(sql: str, dialect: str) -> str:
    return sql.replace("?", "%s") if dialect == "postgres" else sql


def _iter_table(
    conn: Any,
    spec: TableSpec,
    *,
    dialect: str,
    since: Optional[float],
    cutoff: Optional[float],
    batch_rows: int,
) -> Iterator[Any]:
    columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
    pk_sql = ", ".join(quote_identifier(column) for column in spec.primary_key)
    clauses: list[str] = []
    params: list[Any] = []
    if since is not None:
        clauses.append(f"{quote_identifier('updated_at')} >= ?")
        params.append(since)
        clauses.append(f"{quote_identifier('updated_at')} <= ?")
        params.append(cutoff)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    cursor = conn.cursor()
    try:
        cursor.execute(
            _pg_sql(
                f"SELECT {columns_sql} FROM {quote_identifier(spec.name)}"
                f"{where} ORDER BY {pk_sql}",
                dialect,
            ),
            tuple(params),
        )
        while True:
            rows = cursor.fetchmany(batch_rows)
            if not rows:
                return
            yield from rows
    finally:
        cursor.close()


def _next(iterator: Iterator[Any]) -> Any:
    try:
        return next(iterator)
    except StopIteration:
        return None


def _append_sample(
    samples: list[dict[str, Any]], kind: str, spec: TableSpec, key: tuple[Any, ...]
) -> None:
    if len(samples) >= SAMPLE_LIMIT:
        return
    samples.append({"kind": kind, "table": spec.name, "pk": list(key)})


class RepairWriter:
    def __init__(self, conn: Any, dialect: str, batch_rows: int):
        self.conn = conn
        self.dialect = dialect
        self.batch_rows = batch_rows
        self.pending = 0

    def upsert(self, spec: TableSpec, row: Any) -> None:
        values = _row_dict(spec, row)
        columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
        placeholders = ", ".join("?" for _ in spec.columns)
        conflict_sql = ", ".join(
            quote_identifier(column) for column in spec.primary_key
        )
        mutable = [column for column in spec.columns if column not in spec.primary_key]
        if mutable:
            update_sql = ", ".join(
                f"{quote_identifier(column)} = excluded.{quote_identifier(column)}"
                for column in mutable
            )
            action = f"DO UPDATE SET {update_sql}"
        else:
            action = "DO NOTHING"
        self._begin_if_needed()
        self.conn.execute(
            _pg_sql(
                f"INSERT INTO {quote_identifier(spec.name)} ({columns_sql}) "
                f"VALUES ({placeholders}) ON CONFLICT ({conflict_sql}) {action}",
                self.dialect,
            ),
            tuple(values[column] for column in spec.columns),
        )
        if self.dialect == "postgres" and spec.name == "messages":
            self.conn.execute(
                "UPDATE messages SET fts_content = to_tsvector('simple', "
                "concat_ws(' ', content, tool_name, tool_calls)) WHERE id = %s",
                (values["id"],),
            )
        self._after_operation()

    def delete(self, spec: TableSpec, key: tuple[Any, ...]) -> None:
        where = " AND ".join(
            f"{quote_identifier(column)} = ?" for column in spec.primary_key
        )
        self._begin_if_needed()
        self.conn.execute(
            _pg_sql(
                f"DELETE FROM {quote_identifier(spec.name)} WHERE {where}",
                self.dialect,
            ),
            key,
        )
        self._after_operation()

    def _begin_if_needed(self) -> None:
        if self.pending == 0:
            self.conn.execute("BEGIN")

    def _after_operation(self) -> None:
        self.pending += 1
        if self.pending >= self.batch_rows:
            self.flush()

    def flush(self) -> None:
        if self.pending:
            self.conn.commit()
            self.pending = 0

    def rollback(self) -> None:
        if self.pending:
            self.conn.rollback()
            self.pending = 0


def _compare_table(
    source: Any,
    target: Any,
    spec: TableSpec,
    *,
    source_dialect: str,
    target_dialect: str,
    since: Optional[float],
    cutoff: Optional[float],
    batch_rows: int,
    report: dict[str, Any],
    writer: Optional[RepairWriter],
    repair_extra_only: bool = False,
) -> None:
    source_rows = _iter_table(
        source,
        spec,
        dialect=source_dialect,
        since=since,
        cutoff=cutoff,
        batch_rows=batch_rows,
    )
    target_rows = _iter_table(
        target,
        spec,
        dialect=target_dialect,
        since=since,
        cutoff=cutoff,
        batch_rows=batch_rows,
    )
    left = _next(source_rows)
    right = _next(target_rows)
    table_report = report["tables"].setdefault(
        spec.name, {"missing": 0, "extra": 0, "differ": 0, "matched": 0}
    )
    while left is not None or right is not None:
        left_key = _key(spec, left) if left is not None else None
        right_key = _key(spec, right) if right is not None else None
        if right is None or (
            left is not None and left_key is not None and left_key < right_key
        ):
            if not repair_extra_only:
                table_report["missing"] += 1
                _append_sample(report["samples"], "missing", spec, left_key)
                if writer is not None:
                    writer.upsert(spec, left)
            left = _next(source_rows)
            continue
        if left is None or (right_key is not None and right_key < left_key):
            if repair_extra_only:
                if writer is not None:
                    writer.delete(spec, right_key)
            else:
                table_report["extra"] += 1
                _append_sample(report["samples"], "extra", spec, right_key)
            right = _next(target_rows)
            continue
        if not repair_extra_only:
            if row_hash(spec.columns, left) == row_hash(spec.columns, right):
                table_report["matched"] += 1
            else:
                table_report["differ"] += 1
                _append_sample(report["samples"], "differ", spec, left_key)
                if writer is not None:
                    writer.upsert(spec, left)
        left = _next(source_rows)
        right = _next(target_rows)


def state_diff_connections(
    source: Any,
    target: Any,
    *,
    specs: Optional[Sequence[TableSpec]] = None,
    source_dialect: str = "sqlite",
    target_dialect: str = "postgres",
    since: Optional[float] = None,
    cutoff: Optional[float] = None,
    repair_writer: Optional[RepairWriter] = None,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> dict[str, Any]:
    if specs is None:
        if source_dialect != "sqlite":
            raise ValueError("specs are required when the source is not SQLite")
        specs = sqlite_table_specs(source)
    if since is not None and cutoff is None:
        cutoff = time.time() - RECENT_WATERMARK_SECONDS
    selected = [spec for spec in specs if since is None or "updated_at" in spec.columns]
    report: dict[str, Any] = {
        "mode": "incremental" if since is not None else "full",
        "since": since,
        "cutoff": cutoff,
        "tables": {},
        "skipped_without_updated_at": [
            spec.name
            for spec in specs
            if since is not None and "updated_at" not in spec.columns
        ],
        "samples": [],
    }
    try:
        for spec in selected:
            _compare_table(
                source,
                target,
                spec,
                source_dialect=source_dialect,
                target_dialect=target_dialect,
                since=since,
                cutoff=cutoff,
                batch_rows=batch_rows,
                report=report,
                writer=repair_writer,
            )
        if repair_writer is not None:
            repair_writer.flush()
            # Delete target-only rows in reverse dependency order so messages
            # and usage rows disappear before their parent session.
            for spec in reversed(selected):
                _compare_table(
                    source,
                    target,
                    spec,
                    source_dialect=source_dialect,
                    target_dialect=target_dialect,
                    since=since,
                    cutoff=cutoff,
                    batch_rows=batch_rows,
                    report=report,
                    writer=repair_writer,
                    repair_extra_only=True,
                )
            repair_writer.flush()
    except BaseException:
        if repair_writer is not None:
            repair_writer.rollback()
        raise
    report["mismatch_count"] = sum(
        values["missing"] + values["extra"] + values["differ"]
        for values in report["tables"].values()
    )
    report["clean"] = report["mismatch_count"] == 0
    return report


def _open_postgres(dsn: str, *, read_only: bool) -> Any:
    import psycopg
    from psycopg.rows import dict_row

    conn = psycopg.connect(dsn, autocommit=read_only, row_factory=dict_row)
    if read_only:
        conn.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    return conn


def run_state_diff(
    sqlite_path: Path,
    dsn: str,
    *,
    since: Optional[float] = None,
    repair: bool = False,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    _target_factory: Optional[Any] = None,
) -> dict[str, Any]:
    source = open_sqlite_snapshot(sqlite_path)
    target = None
    writer_conn = None
    try:
        specs = sqlite_table_specs(source)
        factory = _target_factory or _open_postgres
        if _target_factory is None:
            target = factory(dsn, read_only=True)
            if repair:
                writer_conn = factory(dsn, read_only=False)
        else:
            target = factory(dsn, True)
            if repair:
                writer_conn = factory(dsn, False)
        writer = (
            RepairWriter(
                writer_conn,
                "sqlite" if isinstance(writer_conn, sqlite3.Connection) else "postgres",
                batch_rows,
            )
            if writer_conn is not None
            else None
        )
        return state_diff_connections(
            source,
            target,
            specs=specs,
            target_dialect=(
                "sqlite" if isinstance(target, sqlite3.Connection) else "postgres"
            ),
            since=since,
            repair_writer=writer,
            batch_rows=batch_rows,
        )
    finally:
        source.rollback()
        source.close()
        if target is not None:
            target.close()
        if writer_conn is not None:
            writer_conn.close()


def _parse_since(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.timestamp()


def _load_waivers(path: Optional[Path]) -> set[str]:
    if path is None:
        return set()
    text = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
    if not isinstance(payload, list) or not all(
        isinstance(item, str) for item in payload
    ):
        raise ValueError(
            "coverage waive file must be a JSON string list or one name per line"
        )
    return set(payload)


def coverage_report(sqlite_path: Path, waive_path: Optional[Path]) -> dict[str, Any]:
    from hermes_state import SessionDB

    conn = sqlite3.connect(sqlite_path)
    try:
        counts = load_coverage(conn)
    finally:
        conn.close()
    expected = expected_mutation_entrypoints(SessionDB)
    waived = _load_waivers(waive_path)
    unknown_waivers = sorted(waived - expected)
    missing = sorted(expected - set(counts) - waived)
    return {
        "executed": dict(sorted(counts.items())),
        "missing": missing,
        "waived": sorted(waived & expected),
        "unknown_waivers": unknown_waivers,
        "clean": not missing and not unknown_waivers,
    }


def _resolve_sqlite_path(value: Optional[str]) -> Path:
    if value:
        return Path(value).expanduser()
    from hermes_constants import get_hermes_home

    return get_hermes_home() / "state.db"


def _resolve_dsn(value: Optional[str]) -> str:
    dsn = value or dual_write_dsn()
    if not dsn:
        raise RuntimeError("pass --dsn or set HERMES_CORE_PG_DSN")
    return dsn


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="state_diff")
    parser.add_argument("--sqlite-path")
    parser.add_argument("--dsn")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true")
    mode.add_argument("--since")
    parser.add_argument("--repair", action="store_true")
    parser.add_argument("--coverage", action="store_true")
    parser.add_argument("--coverage-waive")
    parser.add_argument("--replay-failures", action="store_true")
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    args = parser.parse_args(argv)
    sqlite_path = _resolve_sqlite_path(args.sqlite_path)
    try:
        dsn = _resolve_dsn(args.dsn)
        if args.replay_failures:
            with closing(sqlite3.connect(sqlite_path, isolation_level=None)) as source:
                replicator = DualWriteReplicator(source, dsn)
                replicator.initialize_source()
                replay = replicator.replay_failures()
                print(json.dumps({"replay": replay}, sort_keys=True))
        since = _parse_since(args.since) if args.since else None
        report = run_state_diff(
            sqlite_path,
            dsn,
            since=since,
            repair=args.repair,
            batch_rows=args.batch_rows,
        )
        if args.repair:
            report["post_repair"] = run_state_diff(
                sqlite_path,
                dsn,
                since=since,
                repair=False,
                batch_rows=args.batch_rows,
            )
            report["clean"] = report["post_repair"]["clean"]
        if args.coverage:
            report["coverage"] = coverage_report(
                sqlite_path,
                Path(args.coverage_waive) if args.coverage_waive else None,
            )
            report["clean"] = report["clean"] and report["coverage"]["clean"]
        print(json.dumps(report, sort_keys=True))
        return RC_MATCH if report["clean"] else RC_DIFFERENT
    except Exception as exc:
        print(f"state_diff unavailable ({type(exc).__name__}): {exc}", file=sys.stderr)
        return RC_UNREACHABLE


if __name__ == "__main__":
    sys.exit(main())
