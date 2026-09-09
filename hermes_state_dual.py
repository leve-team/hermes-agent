"""SQLite-primary dual-write support for the Hermes state ledger.

The live :class:`hermes_state.SessionDB` write closure still commits to SQLite
first.  This module records only the shared-ledger DML issued by that closure,
then replays the batch to PostgreSQL.  PostgreSQL failure is fail-open for the
user write and is made durable in SQLite's ``_hermes_dual_failures`` journal.

This is intentionally a transition mechanism, not a read router.  Reads stay
on SQLite until a later cutover explicitly changes the backend.
"""

from __future__ import annotations

import inspect
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable, Optional, Sequence


DUAL_TIMEOUT_SECONDS = 2.0
# 한 배치가 이 예산 안에 못 들어갈 것으로 보이면 동기 복제를 시도하지 않고 곧바로
# 저널에 넣는다(fail-open 의 취지 — primary 는 절대 붙잡지 않는다).
#
# 2026-09-09 X1-p-18b 실측: `archive_and_compact` 는 세션 하나를 통째로 비활성화하며
# mutation 383~463 건 + `UPDATE messages SET active=0 … WHERE session_id=? AND active=1`
# 이 1,351 행 1.46 초였다. 여기에 INSERT 400 건이 같은 배치에 붙어 2 초를 넘겼고,
# 그 실패가 뒤따르는 작은 배치(단독 재현 시 0.02 초)까지 연쇄로 무너뜨렸다.
# 임계값은 그 실측의 하한 쪽에 둔다: mutation 수는 100 (관측된 대량 배치의 1/4),
# rowcount 합은 200 (1,351 의 1/6). 통상 배치는 mutation 2 · rowcount 1 이라
# 임계에 한참 못 미친다 — 일상 경로는 종전과 동일하게 동기 복제된다.
# replay 는 백그라운드다 — primary 쓰기 경로가 아니므로 2 초 예산을 물려받을 이유가
# 없다. 오히려 물려받으면 예산 초과로 저널에 들어간 배치가 replay 에서도 같은
# 지점에서 취소돼 영원히 안 빠진다(2026-09-09 X1-p-19: 저널 #4 attempts 2, mutation
# 684 개·1,442 행이 2 초에 두 번 취소). 동기 경로의 2 초 계약은 그대로다.
DUAL_REPLAY_TIMEOUT_SECONDS = 300.0
DUAL_BATCH_MUTATION_LIMIT = 100
DUAL_BATCH_ROWCOUNT_LIMIT = 200


def batch_exceeds_budget(mutations: "Sequence[Mutation]") -> bool:
    """동기 복제를 시도하지 않고 저널로 보낼 배치인지."""
    if len(mutations) > DUAL_BATCH_MUTATION_LIMIT:
        return True
    total = sum(max(0, m.expected_rowcount) for m in mutations)
    return total > DUAL_BATCH_ROWCOUNT_LIMIT


class DualBatchDeferred(RuntimeError):
    """예산 초과로 동기 복제를 건너뛰고 저널에 맡긴 배치."""


# Core tables that have a PostgreSQL counterpart.  Keep this tuple stable: the
# extra ledgers below are owned by writers outside SessionDB and have a
# different cutover risk even though they share the same physical state.db.
CORE_TABLES = (
    "system_prompts",
    "sessions",
    "messages",
    "session_model_usage",
    "state_meta",
    "gateway_routing",
    "compression_locks",
    "session_turn_leases",
    "gateway_hygiene_state",
)

# Additional state.db ledgers included in PG3 schema, COPY, hash diff, and the
# mutation recorder.  Their current writers still open SQLite directly, so
# they only reach the recorder after those writers move behind SessionDB; the
# migration tooling must not silently omit their existing rows in the meantime.
EXTRA_STATE_TABLES = (
    "levos_control_tower_event",
    "levos_control_tower_delivery",
    "levos_control_tower_role",
    "levos_control_tower_forward",
    "async_delegations",
    "delivery_obligations",
)

MIGRATED_TABLES = CORE_TABLES + EXTRA_STATE_TABLES

_MIGRATED_TABLE_SET = frozenset(MIGRATED_TABLES)
_GENERATED_PRIMARY_KEYS = {"messages": "id"}
_MUTATION_RE = re.compile(
    r"\b(INSERT(?:\s+OR\s+\w+)?\s+INTO|UPDATE|DELETE\s+FROM)\s+"
    r"[\"`\[]?([A-Za-z_]\w*)",
    re.IGNORECASE,
)
_DATABASE_TIME_RE = re.compile(
    r"\b(?:now\s*\(|current_timestamp\b|current_date\b|current_time\b)"
    r"|strftime\s*\([^)]*['\"]now['\"]",
    re.IGNORECASE,
)

SOURCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS _hermes_dual_failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mutation_id TEXT NOT NULL UNIQUE,
    created_at REAL NOT NULL,
    entrypoint TEXT NOT NULL,
    mutations_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error_type TEXT,
    replayed_at REAL
);
CREATE INDEX IF NOT EXISTS idx_hermes_dual_failures_pending
    ON _hermes_dual_failures(replayed_at, id);
CREATE TABLE IF NOT EXISTS _hermes_dual_coverage (
    entrypoint TEXT PRIMARY KEY,
    execution_count INTEGER NOT NULL,
    last_executed_at REAL NOT NULL
);
"""

TARGET_SCHEMA = """
CREATE TABLE IF NOT EXISTS _hermes_dual_applied (
    mutation_id TEXT PRIMARY KEY,
    applied_at DOUBLE PRECISION NOT NULL
)
"""


class DualApplyMismatch(RuntimeError):
    """The replica affected a different number of rows than SQLite."""


@dataclass(frozen=True)
class Mutation:
    sql: str
    params: Any
    table: str
    operation: str
    expected_rowcount: int

    def to_jsonable(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["params"] = _json_encode(self.params)
        return payload

    @classmethod
    def from_jsonable(cls, payload: dict[str, Any]) -> "Mutation":
        return cls(
            sql=str(payload["sql"]),
            params=_json_decode(payload.get("params")),
            table=str(payload["table"]),
            operation=str(payload["operation"]),
            expected_rowcount=int(payload.get("expected_rowcount", -1)),
        )


def dual_write_enabled() -> bool:
    return (os.environ.get("HERMES_STATE_DUAL_WRITE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def dual_write_dsn() -> str:
    """Return the dedicated shadow DSN; never borrow the levos ledger DSN."""
    return (os.environ.get("HERMES_CORE_PG_DSN") or "").strip()


def connect_dual_target(dsn: str, timeout_s: float = DUAL_TIMEOUT_SECONDS) -> Any:
    """Open one bounded-lifetime PostgreSQL adapter connection.

    A fresh connection per batch makes the two-second failure boundary simple:
    no unhealthy handle can poison the next primary write.  The server-side
    statement and lock timeouts bound query execution; libpq's connect timeout
    bounds an unreachable server.
    """
    if not dsn:
        raise RuntimeError("HERMES_CORE_PG_DSN is required in dual-write mode")
    import psycopg
    from hermes_state_postgres import _PostgresConnection

    timeout_ms = max(1, int(timeout_s * 1000))
    raw = psycopg.connect(
        dsn,
        autocommit=True,
        connect_timeout=max(1, int(timeout_s)),
        options=f"-c statement_timeout={timeout_ms} -c lock_timeout={timeout_ms}",
    )
    # Reconnect is deliberately disabled for a replay transaction.  An unknown
    # transaction outcome is resolved by the mutation-id ledger on the next
    # journal replay, never by swapping connections mid-transaction.
    return _PostgresConnection(raw, dsn=None)


def _json_encode(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__hermes_type__": "bytes", "hex": value.hex()}
    if isinstance(value, memoryview):
        return {"__hermes_type__": "bytes", "hex": value.tobytes().hex()}
    if isinstance(value, tuple):
        return {"__hermes_type__": "tuple", "items": [_json_encode(v) for v in value]}
    if isinstance(value, list):
        return [_json_encode(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_encode(v) for k, v in value.items()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    # SQLite adapters may hand back bytearray-like values.  Refuse an opaque
    # repr: it cannot be faithfully replayed.
    try:
        raw = bytes(value)
    except Exception as exc:
        raise TypeError(
            f"unsupported dual-write parameter type: {type(value).__name__}"
        ) from exc
    return {"__hermes_type__": "bytes", "hex": raw.hex()}


def _json_decode(value: Any) -> Any:
    if isinstance(value, list):
        return [_json_decode(v) for v in value]
    if isinstance(value, dict):
        kind = value.get("__hermes_type__")
        if kind == "bytes":
            return bytes.fromhex(str(value["hex"]))
        if kind == "tuple":
            return tuple(_json_decode(v) for v in value.get("items", []))
        return {k: _json_decode(v) for k, v in value.items()}
    return value


def _mutation_target(sql: str) -> tuple[Optional[str], Optional[str]]:
    match = _MUTATION_RE.search(sql)
    if match is None:
        return None, None
    keyword = match.group(1).upper()
    operation = (
        "insert"
        if keyword.startswith("INSERT")
        else ("delete" if keyword.startswith("DELETE") else "update")
    )
    return match.group(2).lower(), operation


def _matching_paren(text: str, opening: int) -> int:
    depth = 0
    quote: Optional[str] = None
    for index in range(opening, len(text)):
        char = text[index]
        if quote:
            if char == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    continue
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
    raise ValueError("unbalanced INSERT column/value list")


def _bind_generated_primary_key(
    sql: str, params: Any, table: str, lastrowid: Optional[int]
) -> tuple[str, Any]:
    column = _GENERATED_PRIMARY_KEYS.get(table)
    if column is None or lastrowid is None:
        return sql, params
    match = _MUTATION_RE.search(sql)
    if match is None or not match.group(1).upper().startswith("INSERT"):
        return sql, params
    column_open = sql.find("(", match.end())
    if column_open < 0:
        raise ValueError(f"dual-write requires an explicit column list for {table}")
    column_close = _matching_paren(sql, column_open)
    columns = [
        part.strip().strip('"`[]')
        for part in sql[column_open + 1 : column_close].split(",")
    ]
    if column.lower() in {item.lower() for item in columns}:
        return sql, params
    values_match = re.search(r"\bVALUES\s*\(", sql[column_close + 1 :], re.IGNORECASE)
    if values_match is None:
        raise ValueError(
            f"dual-write cannot bind generated {table}.{column} without VALUES"
        )
    value_open = column_close + 1 + values_match.end() - 1
    value_close = _matching_paren(sql, value_open)
    rewritten = (
        sql[:column_close]
        + f", {column}"
        + sql[column_close:value_close]
        + ", ?"
        + sql[value_close:]
    )
    if isinstance(params, tuple):
        return rewritten, params + (lastrowid,)
    if isinstance(params, list):
        return rewritten, [*params, lastrowid]
    raise TypeError("dual-write generated-id INSERT requires positional parameters")


def _portable_null_safe_predicates(sql: str) -> str:
    """저널에 기록된 옛 `col IS ?` 술어를 replay 시 이식 가능한 형태로 바꾼다.

    SQLite 는 `col IS ?` 에 NULL 을 바인딩해 NULL-safe 등가로 쓰지만 PostgreSQL 은
    `IS $1` 을 문법 오류로 거부한다(결함 #11). 코드는 0043 에서 고쳤지만 저널은 SQL
    **문자열 원문**을 저장하므로 이미 쌓인 행은 옛 문장을 그대로 재실행한다
    (2026-09-09 X1-p-19: `_set_session_title` 4 건이 SyntaxError 로 영구 잔존).
    replay 경로에서만 정규화한다 — 새 쓰기는 애초에 올바른 SQL 을 만든다.
    """
    return re.sub(
        r"\bIS\s+\?(?!\s*(?:NULL|NOT))",
        "IS NOT DISTINCT FROM ?",
        sql,
        flags=re.IGNORECASE,
    )


_STATE_DEPENDENT_GC = (
    "DELETE FROM system_prompts WHERE NOT EXISTS",
)


def _is_state_dependent_gc(sql: str) -> bool:
    """True for garbage-collection statements whose row count is a function
    of the *replica's* current state, not of this mutation.

    ``_delete_unreferenced_system_prompts`` deletes every prompt no session
    references. How many that is depends on which sessions/prompts the
    replica already holds, which legitimately differs from the primary
    while journal entries are pending or replayed late. Treating the
    primary's rowcount as a contract for the replica therefore fails the
    whole batch — and because the batch also carries the INSERT that would
    have stored the prompt, the prompt never reaches PostgreSQL
    (2026-09-09 Y3-n: 3 prompts missing, journal #706/708/710 all failed
    on this DELETE's rowcount). The GC is idempotent and self-correcting:
    whatever it deletes is by definition unreferenced on that store, and
    the full diff / flag rescan verify convergence. The data-carrying
    statements in the same batch keep their strict contract.
    """
    head = " ".join(sql.split())
    return any(head.startswith(prefix) for prefix in _STATE_DEPENDENT_GC)


def _idempotent_insert(sql: str) -> str:
    if "ON CONFLICT" in sql.upper() or re.search(
        r"\bINSERT\s+OR\b", sql, re.IGNORECASE
    ):
        return sql
    return re.sub(
        r"\bINSERT\s+INTO\b", "INSERT OR IGNORE INTO", sql, count=1, flags=re.IGNORECASE
    )


class RecordingCursor:
    def __init__(self, cursor: Any, owner: "RecordingConnection"):
        self._cursor = cursor
        self._owner = owner

    def execute(self, sql: str, params: Any = ()) -> "RecordingCursor":
        self._cursor.execute(sql, params or ())
        self._owner._capture(sql, params or (), self._cursor)
        return self

    def executemany(self, sql: str, rows: Iterable[Any]) -> "RecordingCursor":
        total = 0
        for params in rows:
            self._cursor.execute(sql, params)
            self._owner._capture(sql, params, self._cursor)
            total += max(self._cursor.rowcount, 0)
        self._rowcount_override = total
        return self

    @property
    def rowcount(self) -> int:
        return getattr(self, "_rowcount_override", self._cursor.rowcount)

    @property
    def lastrowid(self) -> Optional[int]:
        return self._cursor.lastrowid

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class RecordingConnection:
    """Connection proxy that records shared DML without changing query results."""

    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection
        self.mutations: list[Mutation] = []

    def execute(self, sql: str, params: Any = ()) -> RecordingCursor:
        cursor = RecordingCursor(self._connection.cursor(), self)
        return cursor.execute(sql, params)

    def executemany(self, sql: str, rows: Iterable[Any]) -> RecordingCursor:
        cursor = RecordingCursor(self._connection.cursor(), self)
        return cursor.executemany(sql, rows)

    def cursor(self, *args: Any, **kwargs: Any) -> RecordingCursor:
        return RecordingCursor(self._connection.cursor(*args, **kwargs), self)

    def executescript(self, sql_script: str) -> Any:
        # Schema/feature scripts are SQLite-local.  Shared online mutations use
        # parameter-bound execute/executemany and are the only supported dual
        # surface.
        return self._connection.executescript(sql_script)

    def _capture(self, sql: str, params: Any, cursor: Any) -> None:
        table, operation = _mutation_target(sql)
        if table not in _MIGRATED_TABLE_SET or operation is None:
            return
        if _DATABASE_TIME_RE.search(sql):
            raise RuntimeError(
                "dual-write mutation uses database-local time; compute the timestamp once "
                "in Python and bind it to both stores"
            )
        replay_sql, replay_params = _bind_generated_primary_key(
            sql, params, table, cursor.lastrowid
        )
        if operation == "insert":
            replay_sql = _idempotent_insert(replay_sql)
        self.mutations.append(
            Mutation(
                sql=replay_sql,
                params=replay_params,
                table=table,
                operation=operation,
                expected_rowcount=int(cursor.rowcount),
            )
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class DualWriteReplicator:
    """Apply recorded source transactions and maintain their failure journal."""

    def __init__(
        self,
        source: sqlite3.Connection,
        dsn: str,
        *,
        timeout_s: float = DUAL_TIMEOUT_SECONDS,
        connection_factory: Optional[Callable[[str, float], Any]] = None,
        fault_inject: Optional[Callable[[str], None]] = None,
    ):
        self.source = source
        self.dsn = dsn
        self.timeout_s = timeout_s
        self.connection_factory = connection_factory or connect_dual_target
        self.fault_inject = fault_inject

    def initialize_source(self) -> None:
        self.source.executescript(SOURCE_SCHEMA)

    def new_batch(self) -> tuple[str, RecordingConnection]:
        return uuid.uuid4().hex, RecordingConnection(self.source)

    def inject(self, point: str) -> None:
        if self.fault_inject is not None:
            self.fault_inject(point)

    def mark_coverage(self, entrypoint: str, now: float) -> None:
        self.source.execute(
            "INSERT INTO _hermes_dual_coverage(entrypoint, execution_count, last_executed_at) "
            "VALUES (?, 1, ?) ON CONFLICT(entrypoint) DO UPDATE SET "
            "execution_count = _hermes_dual_coverage.execution_count + 1, "
            "last_executed_at = excluded.last_executed_at",
            (entrypoint, now),
        )

    def apply(
        self,
        mutation_id: str,
        mutations: Sequence[Mutation],
        *,
        replay: bool = False,
    ) -> str:
        if not mutations:
            return "empty"
        if not replay and batch_exceeds_budget(mutations):
            # 동기 경로에서만 미룬다. replay 는 백그라운드라 예산 밖이며, 미루면
            # 저널이 영원히 안 비므로 반드시 시도한다.
            raise DualBatchDeferred(
                f"batch of {len(mutations)} mutation(s) / "
                f"{sum(max(0, m.expected_rowcount) for m in mutations)} row(s) "
                "exceeds the synchronous dual-write budget"
            )
        self.inject("during_replay" if replay else "before_pg_apply")
        timeout_s = DUAL_REPLAY_TIMEOUT_SECONDS if replay else self.timeout_s
        target = self.connection_factory(self.dsn, timeout_s)
        timer: Optional[threading.Timer] = None
        try:
            target.execute(TARGET_SCHEMA)
            target.commit()
            raw = getattr(target, "raw", None)
            cancel = getattr(raw, "cancel", None) if raw is not None else None
            if callable(cancel):
                timer = threading.Timer(timeout_s, cancel)
                timer.daemon = True
                timer.start()
            target.execute("BEGIN")
            marker = target.execute(
                "INSERT INTO _hermes_dual_applied(mutation_id, applied_at) "
                "VALUES (?, ?) ON CONFLICT(mutation_id) DO NOTHING",
                (mutation_id, time.time()),
            )
            if marker.rowcount == 0:
                target.rollback()
                return "already_applied"
            for mutation in mutations:
                sql = (
                    _portable_null_safe_predicates(mutation.sql)
                    if replay
                    else mutation.sql
                )
                cursor = target.execute(sql, mutation.params)
                # replay 는 저널 기록 시점보다 늦게 돈다. 조건부 UPDATE/DELETE
                # (`WHERE … AND active = 1` 류)는 "그 시점에 참인 행"을 대상으로
                # 하므로 늦은 replay 에서 rowcount 가 줄어드는 것이 정상이다
                # (2026-09-09 저널 #4: expected 871 vs 실제 다름 → 영구 복구 불가).
                # 그래서 replay 에서는 초과만 이상으로 본다. 동기 경로의 엄격한
                # 등가 계약은 그대로다.
                mismatch = (
                    cursor.rowcount > mutation.expected_rowcount
                    if replay
                    else cursor.rowcount != mutation.expected_rowcount
                )
                if (
                    mutation.operation in {"update", "delete"}
                    and mutation.expected_rowcount >= 0
                    and mismatch
                    and not _is_state_dependent_gc(mutation.sql)
                ):
                    raise DualApplyMismatch(
                        f"{mutation.table} {mutation.operation} affected "
                        f"{cursor.rowcount} replica row(s), expected "
                        f"{mutation.expected_rowcount}"
                    )
            target.commit()
            self.inject("after_pg_commit_before_ack")
            return "applied"
        except BaseException:
            try:
                target.rollback()
            except Exception:
                pass
            raise
        finally:
            if timer is not None:
                timer.cancel()
            target.close()

    def journal_failure(
        self,
        mutation_id: str,
        entrypoint: str,
        mutations: Sequence[Mutation],
        exc: BaseException,
    ) -> None:
        payload = json.dumps(
            [mutation.to_jsonable() for mutation in mutations],
            sort_keys=True,
            separators=(",", ":"),
        )
        self.source.execute(
            "INSERT INTO _hermes_dual_failures("
            "mutation_id, created_at, entrypoint, mutations_json, last_error_type) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT(mutation_id) DO NOTHING",
            (mutation_id, time.time(), entrypoint, payload, type(exc).__name__),
        )

    def replay_failures(self, limit: Optional[int] = None) -> dict[str, int]:
        sql = (
            "SELECT id, mutation_id, mutations_json FROM _hermes_dual_failures "
            "WHERE replayed_at IS NULL ORDER BY id"
        )
        params: tuple[Any, ...] = ()
        if limit is not None:
            sql += " LIMIT ?"
            params = (max(0, int(limit)),)
        rows = self.source.execute(sql, params).fetchall()
        applied = failed = 0
        for row in rows:
            journal_id, mutation_id, payload = row[0], row[1], row[2]
            mutations = [Mutation.from_jsonable(item) for item in json.loads(payload)]
            try:
                self.apply(mutation_id, mutations, replay=True)
            except Exception as exc:
                self.source.execute(
                    "UPDATE _hermes_dual_failures SET attempts = attempts + 1, "
                    "last_error_type = ? WHERE id = ?",
                    (type(exc).__name__, journal_id),
                )
                self.source.commit()
                failed += 1
                continue
            self.source.execute(
                "UPDATE _hermes_dual_failures SET attempts = attempts + 1, "
                "replayed_at = ?, last_error_type = NULL WHERE id = ?",
                (time.time(), journal_id),
            )
            self.source.commit()
            applied += 1
        return {"applied": applied, "failed": failed, "pending": len(rows) - applied}


def expected_mutation_entrypoints(session_db_type: type) -> set[str]:
    """Discover public SessionDB methods whose bytecode calls `_execute_write`."""

    def references_execute_write(code: Any) -> bool:
        if "_execute_write" in code.co_names:
            return True
        return any(
            inspect.iscode(constant) and references_execute_write(constant)
            for constant in code.co_consts
        )

    expected: set[str] = set()
    for base in session_db_type.__mro__:
        for name, value in vars(base).items():
            code = getattr(value, "__code__", None)
            if code is not None and references_execute_write(code):
                expected.add(name)
    return expected


def load_coverage(source: sqlite3.Connection) -> dict[str, int]:
    try:
        rows = source.execute(
            "SELECT entrypoint, execution_count FROM _hermes_dual_coverage"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {str(row[0]): int(row[1]) for row in rows}
