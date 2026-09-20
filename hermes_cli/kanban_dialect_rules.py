"""levos `packages/guild-kit-py/sqlite_dialect_rules.py` 와 **같은 규칙표**다.

이 파일은 그 모듈의 vendoring 이다 — 포크는 levos 를 import 할 수 없다. 사본이
둘이면 반드시 갈라지므로, levos 의
`deploy/levos-kanban/tests/test_core_fork_wiring.py` 가 (1) 모듈 docstring 을 뺀
**소스 전체의 바이트 동일성**과 (2) 입력→출력 표 30+ 케이스를 둘 다 대조한다.
어느 한쪽만 고치면 그 테스트가 실패한다. 규칙을 바꾸려면 levos 정본을 고치고
이 파일을 다시 vendoring 한 패치를 같은 변경에 묶어라.

원본 docstring 의 배경 설명은 levos 정본에 있다. 요지만 옮긴다: PG 어댑터가 둘
이고(이 포크의 `KanbanPostgresCursor`, levos 의 `svc_store.PostgresStore`) 둘은
서로 다른 커넥션 계보를 든다. 19차 정지 창의 claim 500 은 그 비대칭이었다 —
`PRAGMA database_list` 가 이 어댑터로 들어와 psycopg 에 그대로 넘어갔다.

계약 셋: 미지의 `PRAGMA` 는 조용히 삼키지 않고 `NotSupportedError`,
`BEGIN IMMEDIATE` 는 무시하지 않고 트랜잭션 경계를 지키며(이미 열려 있으면
no-op), `ATTACH` 는 흉내내지 않고 거부한다.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

#: 허용목록 밖은 여기서 막힌다. ``sqlite3`` 의 예외를 그대로 쓴다 — 두 어댑터의
#: 호출부는 SQLite 예외 계층을 잡도록 쓰여 있고, 새 예외 타입을 만들면 그
#: 호출부가 전부 새 타입을 알아야 한다.
NotSupportedError = sqlite3.NotSupportedError

# ── 판정 종류 (kind) ────────────────────────────────────────────────────────
#
# ``absorb`` 는 (kind, payload) 를 돌려준다. payload 의 뜻은 kind 마다 다르고,
# 아래 상수 이름이 그 계약의 정본이다.

#: 방언이 아니다 — 어댑터가 하던 대로 자기 파이프라인에 태운다. payload ``None``.
PASS = "pass"
#: payload 의 SQL 을 **대신** 보낸다. 바인딩 파라미터는 그대로 따라간다.
SQL = "sql"
#: payload 는 표 이름. 어댑터가 자기 카탈로그 질의로 답한다(``table_info_sql``).
TABLE_INFO = "table_info"
#: 트랜잭션을 연다. payload ``None``.
BEGIN = "begin"
#: 트랜잭션을 커밋한다. payload ``None``.
COMMIT = "commit"
#: 트랜잭션을 되돌린다. payload ``None``.
ROLLBACK = "rollback"
#: 이미 그 상태다 — 아무것도 하지 않고 0행으로 답한다. payload ``None``.
NOOP = "noop"
#: 세션을 읽기 전용으로(payload ``True``)/해제(payload ``False``)하고 0행.
READ_ONLY = "read_only"

#: 0행을 돌려주는 자리에 보낼 문장. 상수인 이유는 어댑터가 커서를 늘 살아 있는
#: 상태로 유지해야 하기 때문이다 — 행이 없는 것과 커서가 없는 것은 다르다.
EMPTY_RESULT_SQL = "SELECT 1 WHERE false"

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PRAGMA = re.compile(r"^PRAGMA\s+(?:[\"'\[]?(\w+)[\"'\]]?\s*\.\s*)?(\w+)\s*(.*)$", re.I | re.S)
_TABLE_INFO_ARGUMENT = re.compile(r"^\(\s*[\"'\[]?(\w+)[\"'\]]?\s*\)$")
_BEGIN = re.compile(r"^BEGIN(?:\s+(?:DEFERRED|IMMEDIATE|EXCLUSIVE))?(?:\s+TRANSACTION)?$", re.I)
_COMMIT = re.compile(r"^(?:COMMIT|END)(?:\s+TRANSACTION)?$", re.I)
_ROLLBACK = re.compile(r"^ROLLBACK(?:\s+TRANSACTION)?$", re.I)
_ATTACH = re.compile(r"^(?:ATTACH|DETACH)\b", re.I)
_SQLITE_CATALOG = re.compile(r"\bsqlite_(?:master|schema)\b", re.I)

#: ``sqlite_master`` 의 PG 등가. ``svc_store`` 가 18차 창에서 쓰던 문장 그대로다
#: — 표만 아는 관계라 트리거 질문에는 답하지 않는다(그 분리가 의도다).
_CATALOG_SUBQUERY = (
    "(SELECT table_name AS name, 'table' AS type FROM information_schema.tables"
    " WHERE table_schema = current_schema() AND table_type = 'BASE TABLE')"
)

#: 인자 없는 조회형 ``PRAGMA`` → 그 자리에서 답이 되는 SELECT.
#:
#: 값은 "PG 세션에서 참인 답"이지 SQLite 의 기본값이 아니다. ``foreign_keys`` 가
#: 1 인 것은 PG 가 FK 를 끌 수 없기 때문이고, ``integrity_check`` 가 ``ok`` 인
#: 것은 PG 에 찢어진 페이지를 물을 자리가 없기 때문이다.
_PRAGMA_QUERIES = {
    "foreign_keys": "SELECT 1 AS foreign_keys",
    "journal_mode": "SELECT 'wal' AS journal_mode",
    # PG 의 대응물은 연결 시 걸리는 lock_timeout 이다. SQLite 의 busy handler 는
    # 없으므로 0 — "기다리지 않는다" 가 이 세션의 사실이다.
    "busy_timeout": "SELECT 0 AS timeout",
    # SQLite 의 FULL(2). PG 는 커밋을 늘 내구적으로 쓴다.
    "synchronous": "SELECT 2 AS synchronous",
    # 파일 헤더의 페이지 크기다. 포크 코어는 이 질문의 호출부
    # (`_check_file_length_invariant`)를 `uses_sqlite_files` 로 막지만, 그 가드가
    # 없는 코어 판(0.18.0)은 보드 커넥션에 그대로 묻는다. PG 의 정직한 등가는
    # 블록 크기이고, 답을 주면 그 호출부가 "파일이 없다" 로 조용히 건너뛴다 —
    # 거절하면 커밋 경로 전체가 500 이 된다.
    "page_size": "SELECT current_setting('block_size')::bigint AS page_size",
    "quick_check": "SELECT 'ok' AS quick_check",
    "integrity_check": "SELECT 'ok' AS integrity_check",
    "user_version": "SELECT 0 AS user_version",
    # (busy, log, checkpointed) — 체크포인트할 WAL 사이드카가 없다.
    "wal_checkpoint": "SELECT 0 AS busy, 0 AS log, 0 AS checkpointed",
    "query_only": (
        "SELECT CASE WHEN current_setting('default_transaction_read_only') = 'on'"
        " THEN 1 ELSE 0 END AS query_only"
    ),
}

#: 값을 **거는** 형태(``PRAGMA x = y``)가 허용되는 이름. PG 세션에는 걸 자리가
#: 없거나(journal/synchronous/busy) 이미 그렇게 걸려 있으므로 전부 no-op 이다.
_PRAGMA_ASSIGNMENTS = frozenset({
    "foreign_keys",
    "journal_mode",
    "busy_timeout",
    "synchronous",
    "user_version",
})

#: 그중 SQLite 가 **새 값을 한 행으로 돌려주는** 것. 나머지는 0행이다.
#: 값이 아니라 행 수가 계약인 자리가 있다 — ``journal_mode=WAL`` 의 답을 읽어
#: 저널 모드를 확인하는 호출부가 SQLite 쪽에 실재한다.
_PRAGMA_ASSIGNMENT_ANSWERS = frozenset({"journal_mode"})

#: 함수 호출 형태(``PRAGMA wal_checkpoint(TRUNCATE)``)가 허용되는 이름.
_PRAGMA_CALLS = frozenset({"wal_checkpoint", "quick_check", "integrity_check"})


def sql_literal(value: str) -> str:
    """SQL 문자열 리터럴 한 개. 규칙표가 만드는 문장에만 쓴다."""
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value or ""):
        raise NotSupportedError("unsupported SQLite dialect identifier")
    return value


def table_info_sql(table: str, *, schema: str | None = None) -> str:
    """``PRAGMA table_info(t)`` 와 같은 6열(cid·name·type·notnull·dflt_value·pk).

    ``schema`` 가 ``None`` 이면 세션의 ``search_path`` 첫 스키마
    (``current_schema()``)를 본다 — 코어 보드 세션은 자기 스키마를 정적으로
    알지 못하고 DSN 의 ``options=-c search_path=`` 로 받는다.

    ``svc_store`` 는 이 렌더러를 쓰지 않는다. 그쪽은 ``trust_eval`` 의 명시
    ``rowid`` 컬럼을 가려야 해서 자기 렌더러를 유지한다(스토어 고유 사정).
    **무엇을 흡수하는가**(``absorb``)는 같고, 그 답을 어떤 문장으로 만드는지만
    스토어가 정한다.
    """
    target = sql_literal(_identifier(table))
    scope = "current_schema()" if schema is None else sql_literal(schema)
    return (
        "SELECT (column_info.ordinal_position - 1) AS cid,"
        " column_info.column_name AS name,"
        " column_info.data_type AS type,"
        " CASE WHEN column_info.is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,"
        " column_info.column_default AS dflt_value,"
        " CASE WHEN EXISTS ("
        "SELECT 1 FROM pg_index AS idx"
        " JOIN pg_attribute AS pk_attribute"
        " ON pk_attribute.attrelid = idx.indrelid"
        " AND pk_attribute.attnum = ANY(idx.indkey)"
        " WHERE idx.indisprimary"
        f" AND idx.indrelid = to_regclass({scope} || '.' || {target})"
        " AND pk_attribute.attname = column_info.column_name"
        ") THEN 1 ELSE 0 END AS pk"
        " FROM information_schema.columns AS column_info"
        f" WHERE column_info.table_schema = {scope}"
        f" AND column_info.table_name = {target}"
        " ORDER BY column_info.ordinal_position"
    )


def database_list_sql(*, schema: str | None = None) -> str:
    """``PRAGMA database_list`` 의 1행 ``(seq, name, file)``.

    호출부가 ``[2]`` 로 "보드 경로"를 꺼내므로(``execution_migration`` 의 claim
    핀 경로) 파일 경로 자리에 **스키마명 문자열**을 돌려준다. PG 세션에서 그
    자리의 정직한 답이 그것이다 — 이 보드가 사는 곳의 이름.
    """
    scope = "current_schema()" if schema is None else sql_literal(schema)
    return f"SELECT 0 AS seq, 'main' AS name, {scope} AS file"


def _absorb_pragma(sql: str, *, schema: str | None) -> tuple[str, Any]:
    match = _PRAGMA.match(sql)
    if match is None:
        raise NotSupportedError("unsupported SQLite pragma")
    _alias, name, argument = match.groups()
    name = name.lower()
    argument = (argument or "").strip()

    if name == "table_info":
        target = _TABLE_INFO_ARGUMENT.match(argument)
        if target is None:
            raise NotSupportedError("unsupported SQLite pragma")
        return TABLE_INFO, _identifier(target[1])
    if name == "database_list":
        if argument:
            raise NotSupportedError("unsupported SQLite pragma")
        return SQL, database_list_sql(schema=schema)
    if name == "query_only":
        if not argument:
            return SQL, _PRAGMA_QUERIES["query_only"]
        value = argument.lstrip("=").strip().lower()
        if value in ("on", "1", "true"):
            return READ_ONLY, True
        if value in ("off", "0", "false"):
            return READ_ONLY, False
        raise NotSupportedError("unsupported SQLite pragma")
    if not argument:
        if name not in _PRAGMA_QUERIES:
            raise NotSupportedError("unsupported SQLite pragma")
        return SQL, _PRAGMA_QUERIES[name]
    if argument.startswith("="):
        if name not in _PRAGMA_ASSIGNMENTS:
            raise NotSupportedError("unsupported SQLite pragma")
        if name in _PRAGMA_ASSIGNMENT_ANSWERS:
            return SQL, _PRAGMA_QUERIES[name]
        return SQL, EMPTY_RESULT_SQL
    if argument.startswith("(") and name in _PRAGMA_CALLS:
        # `PRAGMA wal_checkpoint(TRUNCATE)` 형태. 붙잡을 사이드카가 없다.
        return SQL, _PRAGMA_QUERIES[name]
    raise NotSupportedError("unsupported SQLite pragma")


def absorb(sql: str, *, schema: str | None = None, in_transaction: bool = False) -> tuple[str, Any]:
    """SQLite 방언 한 문장을 PG 어댑터가 실행할 수 있는 판정으로 바꾼다.

    돌려주는 것은 ``(kind, payload)`` 이고 kind 는 이 모듈의 상수 중 하나다.
    방언이 아니면 ``(PASS, None)`` — 이 함수는 SQL 을 일반적으로 번역하지
    않는다(바인딩·타입·``INSERT OR`` 는 각 어댑터의 몫이다).

    허용목록 밖의 ``PRAGMA`` 와 ``ATTACH``/``DETACH`` 는
    ``NotSupportedError`` 다. 조용한 no-op 은 새 벽을 만들 뿐이다.
    """
    statement = (sql or "").strip()
    while statement.endswith(";"):
        statement = statement[:-1].rstrip()
    if not statement:
        return PASS, None
    head = statement.upper()
    if head.startswith("PRAGMA"):
        return _absorb_pragma(statement, schema=schema)
    if _ATTACH.match(statement):
        raise NotSupportedError(
            "ATTACH is not available on PostgreSQL; the sibling store shares this schema"
        )
    if _BEGIN.match(statement):
        return (NOOP, None) if in_transaction else (BEGIN, None)
    if _COMMIT.match(statement):
        return COMMIT, None
    if _ROLLBACK.match(statement):
        return ROLLBACK, None
    if _SQLITE_CATALOG.search(statement):
        return SQL, _SQLITE_CATALOG.sub(_CATALOG_SUBQUERY, statement)
    return PASS, None


def absorb_statement(
    sql: str,
    *,
    schema: str | None,
    in_transaction: bool,
    begin: Any,
    commit: Any,
    rollback: Any,
    set_read_only: Any,
) -> str:
    """규칙표를 어댑터 한 줄로 — 두 계층이 **같은 함수**를 부른다.

    돌려주는 것은 "이 자리에서 실제로 보낼 문장" 하나다. 트랜잭션 제어는
    넘겨받은 콜러블로 수행하고, 그 자리에는 0행 문장을 돌려준다 — 커서가 늘
    살아 있어야 호출부의 ``fetchone()`` 이 ``None`` 을 받는다(예외가 아니라).

    어댑터가 이 함수 대신 ``absorb`` 를 직접 읽어 자기 분기를 적으면 그 분기가
    갈라진다. 갈라질 자리를 하나로 줄이는 것이 이 함수의 전부다.
    """
    kind, payload = absorb(sql, schema=schema, in_transaction=in_transaction)
    if kind == PASS:
        return sql
    if kind == SQL:
        return payload
    if kind == TABLE_INFO:
        return table_info_sql(payload, schema=schema)
    if kind == BEGIN:
        begin()
    elif kind == COMMIT:
        commit()
    elif kind == ROLLBACK:
        rollback()
    elif kind == READ_ONLY:
        set_read_only(payload)
    return EMPTY_RESULT_SQL
