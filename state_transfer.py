"""Shared primitives for bounded, resumable Hermes state transfers."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from hermes_state_dual import MIGRATED_TABLES


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
logger = logging.getLogger("levos.state_transfer")
POSTGRES_DERIVED_COLUMNS = frozenset({("messages", "fts_content")})
_SQLITE_TO_POSTGRES_TYPES = {
    "REAL": "DOUBLE PRECISION",
    "INTEGER": "BIGINT",
    "TEXT": "TEXT",
    "BLOB": "BYTEA",
}
_POSTGRES_TO_SQLITE_TYPES = {
    postgres: sqlite for sqlite, postgres in _SQLITE_TO_POSTGRES_TYPES.items()
}


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


def sqlite_table_specs(
    conn: sqlite3.Connection,
    *,
    excluded_columns: Iterable[tuple[str, str]] = (),
) -> list[TableSpec]:
    """Return PG3-migrated tables in dependency-safe load order."""
    available = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    excluded = set(excluded_columns)
    specs: list[TableSpec] = []
    for table in MIGRATED_TABLES:
        if table not in available:
            continue
        info = conn.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()
        columns = tuple(
            str(row[1]) for row in info if (table, str(row[1])) not in excluded
        )
        primary_key = tuple(
            str(row[1])
            for row in sorted(
                (row for row in info if int(row[5]) > 0), key=lambda row: int(row[5])
            )
        )
        if not primary_key or not set(primary_key).issubset(columns):
            raise RuntimeError(f"migrated state table {table!r} has no primary key")
        specs.append(TableSpec(table, columns, primary_key))
    return specs


def _transfer_column_types(
    conn: Any, dialect: str, specs: Sequence[TableSpec]
) -> dict[str, dict[str, str]]:
    raw = getattr(conn, "raw", conn)
    tables: dict[str, dict[str, str]] = {spec.name: {} for spec in specs}
    if dialect == "sqlite":
        for table in tables:
            tables[table] = {
                str(row[1]): str(row[2]).strip().upper()
                for row in raw.execute(
                    f"PRAGMA table_info({quote_identifier(table)})"
                ).fetchall()
            }
    elif dialect == "postgres":
        for table, column, data_type in raw.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = current_schema()"
        ).fetchall():
            if table in tables:
                tables[table][column] = data_type.upper()
    else:
        raise ValueError(f"unsupported transfer dialect: {dialect!r}")
    return tables


def reconcile_transfer_columns(
    source: Any,
    target: Any,
    specs: Sequence[TableSpec],
    *,
    source_dialect: str,
    target_dialect: str,
    excluded_columns: Iterable[tuple[str, str]] = (),
) -> list[str]:
    """Preserve source-only columns as nullable columns before transferring rows.

    Inspect the pinned source snapshot, never the core DDL. Unsupported extra
    types, absent tables and DDL errors fail closed before COPY or watermarks.
    Defaults and constraints are deliberately not imported across dialects.
    """
    source_types = _transfer_column_types(source, source_dialect, specs)
    target_types = _transfer_column_types(target, target_dialect, specs)
    excluded = set(excluded_columns)
    pending: list[tuple[str, str]] = []
    for table, columns in source_types.items():
        if not columns or not target_types[table]:
            raise RuntimeError(
                f"transfer schema: missing source/target table {table!r}"
            )
        for column, declared_type in columns.items():
            if column in target_types[table] or (table, column) in excluded:
                continue
            sqlite_type = (
                declared_type
                if source_dialect == "sqlite"
                else _POSTGRES_TO_SQLITE_TYPES.get(declared_type)
            )
            if sqlite_type not in _SQLITE_TO_POSTGRES_TYPES:
                raise RuntimeError(
                    f"transfer schema: unsupported {source_dialect} type"
                    f" {declared_type!r} for {table}.{column}"
                )
            target_type = (
                sqlite_type
                if target_dialect == "sqlite"
                else _SQLITE_TO_POSTGRES_TYPES[sqlite_type]
            )
            guard = " IF NOT EXISTS" if target_dialect == "postgres" else ""
            pending.append((
                f"{table}.{column}",
                f"ALTER TABLE {quote_identifier(table)} ADD COLUMN{guard}"
                f" {quote_identifier(column)} {target_type}",
            ))
    raw = getattr(target, "raw", target)
    for name, statement in pending:
        raw.execute(statement)
        logger.info("transfer schema: accepted extra column %s (%s)", name, statement)
    return [name for name, _statement in pending]



# ── 레거시 NUL 접두 정규화 (결함 #5, 2026-09-06) ─────────────────────────────
# 구버전 코어는 구조화 content 를 "\x00json:" 접두로 저장했다. 현행 코어
# (hermes_state.py SessionDB._CONTENT_JSON_PREFIX) 는 "\x01json:" 로 쓰고 읽기는
# 양쪽을 동등하게 받는다. PostgreSQL text 는 NUL 을 거부하므로 이관 시 레거시
# 접두를 현행 접두로 바꾼다 — 디코드 결과가 같으므로 의미 보존이다.
# 접두가 아닌 위치의 NUL 은 데이터 결함이므로 조용히 지우지 않고 예외로 올린다.
LEGACY_CONTENT_JSON_PREFIX = "\x00json:"
CURRENT_CONTENT_JSON_PREFIX = "\x01json:"


class SourceValueError(ValueError):
    """소스 값이 대상 백엔드에 표현 불가능하고 안전한 정규화도 없을 때."""


def normalize_legacy_content_prefix(value: Any, *, table: str = "", column: str = "", key: Any = None) -> Any:
    """``\x00json:`` 접두 문자열을 ``\x01json:`` 로 바꾼다. 그 외 NUL 은 거부."""
    if not isinstance(value, str) or "\x00" not in value:
        return value
    if value.startswith(LEGACY_CONTENT_JSON_PREFIX):
        rest = value[len(LEGACY_CONTENT_JSON_PREFIX):]
        if "\x00" not in rest:
            return CURRENT_CONTENT_JSON_PREFIX + rest
    raise SourceValueError(
        f"NUL byte outside legacy json prefix in {table or '?'}.{column or '?'} key={key!r}; "
        "refusing to strip or truncate"
    )


def normalize_row_values(spec: "TableSpec", values: list[Any]) -> list[Any]:
    """행 값 리스트에 컬럼별 정규화를 적용한다(현재는 NUL 접두 하나)."""
    key = tuple(values[spec.columns.index(pk)] for pk in spec.primary_key if pk in spec.columns) or None
    return [
        normalize_legacy_content_prefix(v, table=spec.name, column=c, key=key)
        for c, v in zip(spec.columns, values)
    ]

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
    """Read named rows or tuple rows projected in ``spec.columns`` order."""
    try:
        return [row[column] for column in spec.primary_key]
    except (KeyError, TypeError, IndexError):
        return [row[spec.columns.index(column)] for column in spec.primary_key]


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
