from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from hermes_state import SessionDB
from hermes_state_postgres import (
    _PG_ONLY_MIGRATIONS,
    _build_tsquery,
    _replace_prefix_terms,
    _update_fts_content,
    _search_messages_fts,
    prepare_fts_document,
    search_messages_postgres,
)


@pytest.fixture
def sqlite_contract_db(tmp_path: Path) -> tuple[SessionDB, dict[str, int]]:
    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("cli-session", source="cli")
    db.create_session("slack-session", source="slack")

    ids = {
        "deploy": db.append_message(
            "cli-session", "user", "deploy service safely", timestamp=10.0
        ),
        "deployment": db.append_message(
            "cli-session", "assistant", "deployment checklist", timestamp=20.0
        ),
        "phrase": db.append_message(
            "cli-session", "user", "blue green release", timestamp=30.0
        ),
        "phrase_gap": db.append_message(
            "cli-session", "assistant", "blue noisy green release", timestamp=40.0
        ),
        "negative": db.append_message(
            "slack-session", "user", "python service", timestamp=50.0
        ),
        "excluded": db.append_message(
            "slack-session", "assistant", "python java bridge", timestamp=60.0
        ),
        "tool_name": db.append_message(
            "cli-session",
            "tool",
            "tool completed",
            tool_name="deploy_probe",
            timestamp=70.0,
        ),
        "tool_calls": db.append_message(
            "cli-session",
            "assistant",
            "tool request",
            tool_calls='[{"command":"release_canary"}]',
            timestamp=80.0,
        ),
    }
    try:
        yield db, ids
    finally:
        db.close()


@pytest.mark.parametrize(
    ("sqlite_query", "present", "absent"),
    [
        ("deploy", {"deploy"}, {"deployment"}),
        ("deploy*", {"deploy", "deployment", "tool_name"}, set()),
        ('"blue green"', {"phrase"}, {"phrase_gap"}),
        ("blue OR python", {"phrase", "phrase_gap", "negative", "excluded"}, set()),
        # Backend-neutral ``python -java`` maps to FTS5's binary NOT spelling.
        ("python NOT java", {"negative"}, {"excluded"}),
    ],
    ids=["word", "prefix", "phrase", "or", "negative"],
)
def test_sqlite_query_contract(
    sqlite_contract_db: tuple[SessionDB, dict[str, int]],
    sqlite_query: str,
    present: set[str],
    absent: set[str],
) -> None:
    db, ids = sqlite_contract_db
    found = {row["id"] for row in db.search_messages(sqlite_query, limit=20)}
    assert {ids[name] for name in present} <= found
    assert not ({ids[name] for name in absent} & found)


def test_sqlite_indexes_tool_fields_and_applies_column_filters(
    sqlite_contract_db: tuple[SessionDB, dict[str, int]],
) -> None:
    db, ids = sqlite_contract_db

    assert [
        row["id"] for row in db.search_messages("release_canary", role_filter=["assistant"])
    ] == [ids["tool_calls"]]
    assert [
        row["id"]
        for row in db.search_messages("python", source_filter=["slack"], role_filter=["user"])
    ] == [ids["negative"]]
    assert not db.search_messages("python", exclude_sources=["slack"])


def test_sqlite_limit_and_temporal_sort_are_bounds(
    sqlite_contract_db: tuple[SessionDB, dict[str, int]],
) -> None:
    db, ids = sqlite_contract_db

    assert [row["id"] for row in db.search_messages("blue", limit=1, sort="newest")] == [
        ids["phrase_gap"]
    ]
    assert [row["id"] for row in db.search_messages("blue", limit=1, sort="oldest")] == [
        ids["phrase"]
    ]


class _Rows:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _TsqueryConnection:
    def __init__(self, websearch_result: str):
        self._conn = self
        self._fts_col_available = True
        self.websearch_result = websearch_result
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params=()):
        bound = tuple(params)
        self.calls.append((sql, bound))
        if "websearch_to_tsquery" in sql:
            return _Rows([(self.websearch_result,)])
        if "plainto_tsquery" in sql:
            return _Rows()
        if "to_tsquery" in sql:
            term = bound[0].removesuffix(":*")
            return _Rows([(f"'{term}':*",)])
        return _Rows()


@pytest.mark.parametrize(
    ("query", "websearch_result", "expected"),
    [
        ("deploy", "'deploy'", "'deploy'"),
        ("deploy*", "'zzhermesprefix0zz'", "('deploy':*)"),
        ('"blue green"', "'blue' <-> 'green'", "'blue' <-> 'green'"),
        ("blue OR green", "'blue' | 'green'", "'blue' | 'green'"),
        ("blue -green", "'blue' & !'green'", "'blue' & !'green'"),
    ],
    ids=["word", "prefix", "phrase", "or", "negative"],
)
def test_postgres_query_contract(
    query: str, websearch_result: str, expected: str
) -> None:
    conn = _TsqueryConnection(websearch_result)
    assert _build_tsquery(conn, query) == expected
    if query == "deploy*":
        assert any(
            "to_tsquery" in sql and params == ("deploy:*",)
            for sql, params in conn.calls
        )


def test_postgres_prefix_placeholder_preserves_websearch_boolean_tree() -> None:
    conn = _TsqueryConnection(
        "'release' | ('zzhermesprefix0zz' & !'legacy')"
    )
    assert _build_tsquery(conn, "release OR deploy* -legacy") == (
        "'release' | (('deploy':*) & !'legacy')"
    )


def test_postgres_prefix_placeholders_cannot_collide_with_query_text() -> None:
    query = "zzhermesprefix0zz zzhermesprefix1zz first* second*"
    rewritten, mappings = _replace_prefix_terms(query)
    placeholders = [placeholder for placeholder, _term in mappings]

    assert len(placeholders) == len(set(placeholders)) == 2
    assert all(placeholder not in query for placeholder in placeholders)
    assert all(placeholder in rewritten for placeholder in placeholders)


class _SearchConnection(_TsqueryConnection):
    def __init__(self, websearch_result: str = "'needle'"):
        super().__init__(websearch_result)
        self.search_calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params=()):
        if "_to_tsquery" in sql or "to_tsquery" in sql:
            return super().execute(sql, params)
        bound = tuple(params)
        self.search_calls.append((sql, bound))
        return _Rows()


def test_postgres_null_rows_are_auxiliary_not_a_global_ilike_downgrade() -> None:
    conn = _SearchConnection()
    _search_messages_fts(
        conn,
        lambda value: value,
        "blue -green",
        "'blue' & !'green'",
        source_filter=["cli"],
        exclude_sources=None,
        role_filter=["user"],
        limit=7,
        offset=2,
        sort_norm="newest",
        include_inactive=False,
    )

    sql, params = conn.search_calls[-1]
    assert "m.fts_content IS NOT NULL AND m.fts_content @@" in sql
    assert "m.fts_content IS NULL AND" in sql
    assert "ILIKE" in sql
    assert "COALESCE(ts_rank(m.fts_content" in sql
    assert "s.source IN" in sql and "m.role IN" in sql
    assert params[0] == "'blue' & !'green'"
    assert "%blue%" in params and "%green%" in params
    assert params[-2:] == (7, 2)


def test_postgres_cjk_uses_ilike_when_pg_trgm_is_unavailable() -> None:
    conn = _SearchConnection("'数据库连接'")
    assert search_messages_postgres(
        conn, lambda value: value, "数据库连接", limit=20
    ) == []

    sql, params = conn.search_calls[-1]
    assert "ILIKE" in sql
    assert "fts_content @@" not in sql
    assert "%数据库连接%" in params
    trigram_migrations = [
        migration
        for migration in _PG_ONLY_MIGRATIONS
        if "pg_trgm" in migration.sql or "gin_trgm_ops" in migration.sql
    ]
    assert trigram_migrations and all(
        migration.optional for migration in trigram_migrations
    )


def test_live_and_backfill_document_uses_all_fields_and_utf8_byte_bound() -> None:
    text, source_bytes, indexed_bytes, truncated = prepare_fts_document(
        "body", "deploy_probe", '{"command":"release_canary"}', max_bytes=24
    )
    assert text.startswith("body deploy_probe")
    assert source_bytes > indexed_bytes
    assert indexed_bytes <= 24
    assert truncated is True

    multibyte, _, multibyte_bytes, multibyte_truncated = prepare_fts_document(
        "데이터베이스", None, None, max_bytes=7
    )
    assert multibyte.encode("utf-8").decode("utf-8") == multibyte
    assert multibyte_bytes <= 7
    assert multibyte_truncated is True


def test_live_oversized_document_is_indexed_and_persistently_listed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hermes_state_postgres

    class Raw:
        def __init__(self):
            self.calls: list[tuple[str, tuple]] = []
            self.transaction_events: list[str] = []

        def execute(self, sql: str, params=()):
            self.calls.append((sql, tuple(params)))
            return _Rows()

        @contextmanager
        def transaction(self):
            self.transaction_events.append("begin")
            try:
                yield
            except BaseException:
                self.transaction_events.append("rollback")
                raise
            else:
                self.transaction_events.append("commit")

    raw = Raw()
    conn = type(
        "Connection",
        (),
        {"_conn": raw, "_fts_col_available": True},
    )()
    monkeypatch.setattr(hermes_state_postgres, "FTS_INDEX_MAX_BYTES", 32)

    _update_fts_content(conn, 41, "body", "tool", "x" * 100)

    assert raw.transaction_events == ["begin", "commit"]
    update_sql, update_params = raw.calls[0]
    manifest_sql, manifest_params = raw.calls[1]
    assert "SET fts_content = to_tsvector" in update_sql
    assert len(update_params[0].encode("utf-8")) <= 32
    assert update_params[1] == 41
    assert "INSERT INTO hermes_fts_truncations" in manifest_sql
    assert manifest_params[0] == 41
    assert manifest_params[1] > manifest_params[2]


def test_live_fts_failure_rolls_back_only_the_nested_transaction() -> None:
    class Raw:
        def __init__(self):
            self.transaction_events: list[str] = []

        def execute(self, sql: str, params=()):
            del params
            if "SET fts_content" in sql:
                raise RuntimeError("derived index failure")
            return _Rows()

        @contextmanager
        def transaction(self):
            self.transaction_events.append("begin")
            try:
                yield
            except BaseException:
                self.transaction_events.append("rollback")
                raise

    raw = Raw()
    conn = type(
        "Connection",
        (),
        {"_conn": raw, "_fts_col_available": True},
    )()

    _update_fts_content(conn, 42, "body", None, None)

    assert raw.transaction_events == ["begin", "rollback"]
