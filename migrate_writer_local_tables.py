"""One-shot, idempotent move of the writer-local ledgers from SQLite to PostgreSQL.

``async_delegations`` and ``delivery_obligations`` kept landing in the profile's
``state.db`` after the PostgreSQL authority flip because their writers opened
SQLite directly (PG3 0051 makes them follow the profile backend). This script
carries the rows that accumulated in that file into the profile's PostgreSQL
store so the delegation queue and delivery ledger do not fork.

Run it in the STOP window, before the first start of the image that carries
0051, with the gateway stopped::

    python migrate_writer_local_tables.py --sqlite-path <profile>/state.db \
        --dsn "$HERMES_STATE_POSTGRES_DSN"

Contract:

* The SQLite file is opened read-only on one WAL snapshot; it is never written.
* Every row is upserted by primary key. A row already present in PostgreSQL is
  updated only when the SQLite copy carries a strictly newer ``updated_at``
  (every writer bumps that column on each mutation), so re-running is a no-op
  and a row the live PostgreSQL writer has since advanced is never rolled back.
* PostgreSQL schema is owned by the core: the target tables must already exist
  (the authority gateway created them on its first open). A missing table is an
  error, not a silent skip. Columns the SQLite file lacks take the PostgreSQL
  defaults; SQLite-only columns are reported and left behind.
* ``--dry-run`` reports what would change and rolls back.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence

from migrate_state_to_postgres import _resolve_dsn, _resolve_sqlite_path
from state_transfer import open_sqlite_snapshot, quote_identifier

WRITER_LOCAL_TABLES: tuple[tuple[str, str], ...] = (
    ("async_delegations", "delegation_id"),
    ("delivery_obligations", "obligation_id"),
)
BATCH_ROWS = 500


def _sqlite_columns(source: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in source.execute(f"PRAGMA table_info({quote_identifier(table)})")]


def _postgres_columns(target: Any, table: str) -> list[str]:
    rows = target.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema = current_schema() AND table_name = ?
           ORDER BY ordinal_position""",
        (table,),
    ).fetchall()
    return [row[0] for row in rows]


def _upsert_sql(table: str, columns: Sequence[str], primary_key: str) -> str:
    quoted = [quote_identifier(column) for column in columns]
    updates = ", ".join(
        f"{name} = excluded.{name}" for column, name in zip(columns, quoted)
        if column != primary_key
    )
    target = quote_identifier(table)
    return (
        f"INSERT INTO {target} ({', '.join(quoted)}) "
        f"VALUES ({', '.join('?' for _ in quoted)}) "
        f"ON CONFLICT ({quote_identifier(primary_key)}) DO UPDATE SET {updates} "
        f"WHERE excluded.updated_at > {target}.updated_at"
    )


def migrate_table(
    source: sqlite3.Connection, target: Any, table: str, primary_key: str
) -> dict[str, Any]:
    present = source.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    if present is None:
        return {"table": table, "present_in_sqlite": False, "rows": 0, "applied": 0,
                "unchanged": 0, "skipped_columns": []}
    pg_columns = _postgres_columns(target, table)
    if not pg_columns:
        raise RuntimeError(
            f"PostgreSQL table {table!r} is missing; start the authority gateway "
            f"once (or run the core schema init) before migrating"
        )
    sqlite_columns = _sqlite_columns(source, table)
    for required in (primary_key, "updated_at"):
        if required not in sqlite_columns or required not in pg_columns:
            raise RuntimeError(f"{table!r} lacks {required!r} on one side; refusing")
    columns = [column for column in sqlite_columns if column in pg_columns]
    skipped = [column for column in sqlite_columns if column not in pg_columns]
    sql = _upsert_sql(table, columns, primary_key)
    select = (
        f"SELECT {', '.join(quote_identifier(column) for column in columns)} "
        f"FROM {quote_identifier(table)} ORDER BY {quote_identifier(primary_key)}"
    )
    rows = applied = 0
    cursor = source.execute(select)
    while True:
        batch = cursor.fetchmany(BATCH_ROWS)
        if not batch:
            break
        rows += len(batch)
        for row in batch:
            applied += max(int(target.execute(sql, tuple(row)).rowcount), 0)
    return {"table": table, "present_in_sqlite": True, "rows": rows, "applied": applied,
            "unchanged": rows - applied, "skipped_columns": skipped}


def migrate(sqlite_path: Path, dsn: str, *, dry_run: bool = False) -> dict[str, Any]:
    from hermes_state_postgres import connect_postgres

    source = open_sqlite_snapshot(sqlite_path)
    try:
        target = connect_postgres(dsn)
        try:
            target.execute("BEGIN")
            try:
                tables = [
                    migrate_table(source, target, table, primary_key)
                    for table, primary_key in WRITER_LOCAL_TABLES
                ]
            except BaseException:
                target.rollback()
                raise
            if dry_run:
                target.rollback()
            else:
                target.commit()
        finally:
            target.close()
    finally:
        source.close()
    return {"sqlite_path": str(sqlite_path), "dry_run": dry_run, "tables": tables}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="migrate_writer_local_tables",
        description="Idempotent SQLite -> PostgreSQL move of async_delegations and "
                    "delivery_obligations (PG3 0051).",
    )
    parser.add_argument("--sqlite-path")
    parser.add_argument("--dsn")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        summary = migrate(
            _resolve_sqlite_path(args.sqlite_path), _resolve_dsn(args.dsn),
            dry_run=args.dry_run,
        )
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
