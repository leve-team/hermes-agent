"""Shared primitives for bounded, resumable Hermes state transfers."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from hermes_state_dual import MIGRATED_TABLES


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]


def quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def open_sqlite_snapshot(path: Path) -> sqlite3.Connection:
    """Open a read-only transaction whose first read pins one WAL snapshot."""
    if not path.is_file():
        raise FileNotFoundError(f"SQLite state database not found: {path}")
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("BEGIN")
    # Pin the snapshot now, before target schema work or count probes.
    conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
    return conn


def sqlite_table_specs(conn: sqlite3.Connection) -> list[TableSpec]:
    """Return PG3-migrated tables in dependency-safe load order."""
    available = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    specs: list[TableSpec] = []
    for table in MIGRATED_TABLES:
        if table not in available:
            continue
        info = conn.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()
        columns = tuple(str(row[1]) for row in info)
        primary_key = tuple(
            str(row[1])
            for row in sorted(
                (row for row in info if int(row[5]) > 0), key=lambda row: int(row[5])
            )
        )
        if not primary_key:
            raise RuntimeError(f"migrated state table {table!r} has no primary key")
        specs.append(TableSpec(table, columns, primary_key))
    return specs


def fetch_sqlite_batch(
    conn: sqlite3.Connection,
    spec: TableSpec,
    last_primary_key: Optional[Sequence[Any]],
    batch_rows: int,
) -> list[sqlite3.Row]:
    columns_sql = ", ".join(quote_identifier(column) for column in spec.columns)
    pk_sql = ", ".join(quote_identifier(column) for column in spec.primary_key)
    params: list[Any] = []
    where = ""
    if last_primary_key is not None:
        if len(last_primary_key) != len(spec.primary_key):
            raise ValueError(f"invalid watermark for table {spec.name}")
        placeholders = ", ".join("?" for _ in spec.primary_key)
        where = f" WHERE ({pk_sql}) > ({placeholders})"
        params.extend(last_primary_key)
    params.append(batch_rows)
    return conn.execute(
        f"SELECT {columns_sql} FROM {quote_identifier(spec.name)}"
        f"{where} ORDER BY {pk_sql} LIMIT ?",
        tuple(params),
    ).fetchall()


def primary_key_from_row(spec: TableSpec, row: Any) -> list[Any]:
    return [row[column] for column in spec.primary_key]


def _checkpoint_source_identity(source: Path | str) -> str:
    return str(source.resolve()) if isinstance(source, Path) else str(source)


def checkpoint_template(source_path: Path | str, direction: str) -> dict[str, Any]:
    return {
        "version": 1,
        "direction": direction,
        "source": _checkpoint_source_identity(source_path),
        "tables": {},
        "completed": False,
    }


def load_checkpoint(
    path: Path, *, source_path: Path | str, direction: str, resume: bool
) -> dict[str, Any]:
    if not path.exists():
        return checkpoint_template(source_path, direction)
    if not resume:
        raise RuntimeError(
            f"checkpoint already exists at {path}; pass --resume to continue it"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != 1:
        raise RuntimeError(f"unsupported checkpoint version at {path}")
    if payload.get("direction") != direction:
        raise RuntimeError(f"checkpoint direction does not match {direction}")
    if payload.get("source") != _checkpoint_source_identity(source_path):
        raise RuntimeError("checkpoint belongs to a different source database")
    if not isinstance(payload.get("tables"), dict):
        raise RuntimeError("checkpoint table watermarks are malformed")
    return payload


def save_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    """Atomically replace a small JSON watermark file beside its destination."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    temp_path = Path(handle.name)
    try:
        with handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def table_counts(
    conn: sqlite3.Connection, specs: Iterable[TableSpec]
) -> dict[str, int]:
    return {
        spec.name: int(
            conn.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(spec.name)}"
            ).fetchone()[0]
        )
        for spec in specs
    }
