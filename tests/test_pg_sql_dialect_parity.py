"""Exercise SQL emitted by real state operations and their PostgreSQL bindings."""

import pytest

from hermes_state import SessionDB
from hermes_state_pg_sql import _translate_sql


class _BindingCheckedConnection:
    """Validate real driver binding, then execute the original statement on SQLite.

    This catches dropped/reordered placeholders without requiring a server.
    PostgreSQL engine semantics are covered by test_pg_parity_smoke.
    """

    def __init__(self, connection):
        from psycopg._queries import PostgresQuery
        from psycopg.adapt import Transformer

        self.connection = connection
        self.query = PostgresQuery(Transformer())
        self.statements = []

    def execute(self, sql, params=()):
        self.query.convert(_translate_sql(sql), params)
        self.statements.append(self.query.query)
        return self.connection.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self.connection, name)


@pytest.fixture
def db(tmp_path, monkeypatch):
    pytest.importorskip("psycopg")
    monkeypatch.setenv("HERMES_PG_ADAPTER_STRICT", "1")
    store = SessionDB(tmp_path / "state.db")
    store._conn = _BindingCheckedConnection(store._conn)
    try:
        yield store
    finally:
        store.close()


@pytest.mark.parametrize("absolute", [False, True])
@pytest.mark.parametrize("previous", [None, 0.0, 1.5])
@pytest.mark.parametrize("reported", [None, 0.0, 0.25])
def test_cost_updates_preserve_null_and_bind_all_fields(db, absolute, previous, reported):
    db.create_session("usage", "cli")
    db._conn.execute("UPDATE sessions SET actual_cost_usd = ? WHERE id = ?", (previous, "usage"))
    db.update_token_counts(
        "usage", input_tokens=11, output_tokens=7, actual_cost_usd=reported,
        cost_status="actual", cost_source="provider", pricing_version="test-price",
        model="test-model", billing_provider="test-provider", api_call_count=1,
        absolute=absolute)
    row = db.get_session("usage")
    expected = previous if reported is None else reported if absolute else (previous or 0) + reported
    assert row["actual_cost_usd"] == expected
    assert row["input_tokens"] == 11
    assert row["output_tokens"] == 7
    assert row["cost_status"] == "actual"
    assert row["cost_source"] == "provider"
    assert row["pricing_version"] == "test-price"
    assert row["model"] == "test-model"
    assert row["billing_provider"] == "test-provider"
    assert row["api_call_count"] == 1


def test_model_usage_upserts_accumulate_the_same_route(db):
    db.create_session("usage", "cli")
    for _ in range(2):
        db.update_token_counts(
            "usage", model="model-a", billing_provider="provider-a", input_tokens=13,
            output_tokens=5, actual_cost_usd=0.25, api_call_count=1,
            cost_status="actual", cost_source="provider")
    row = db._conn.execute("SELECT * FROM session_model_usage WHERE session_id = ?", ("usage",)).fetchone()
    assert row["input_tokens"] == 26
    assert row["output_tokens"] == 10
    assert row["actual_cost_usd"] == 0.5
    assert row["api_call_count"] == 2
    assert row["cost_status"] == "actual"
    assert row["cost_source"] == "provider"


def test_nested_session_metadata_is_usable_after_repeated_upserts(db):
    db.create_session("parent", "cli", model_config={"temperature": 0.4})
    db.create_session("child", "cli", parent_session_id="parent", model_config={"_delegate_from": "parent"})
    db.create_session("child", "cli", parent_session_id="parent", model_config={"temperature": 0.2})
    db.append_message("child", "user", "A message with 100% literal question marks?")
    child = db.get_session("child")
    assert child["parent_session_id"] == "parent"
    assert db.get_messages("child")[0]["content"].endswith("question marks?")
    assert db._conn.statements


def test_strict_translation_refuses_unsupported_json_paths(monkeypatch):
    monkeypatch.setenv("HERMES_PG_ADAPTER_STRICT", "1")
    with pytest.raises(RuntimeError, match="json_extract"):
        _translate_sql("SELECT json_extract(model_config, ?) FROM sessions")


def test_strict_mode_forbids_ifnull(monkeypatch):
    """``IFNULL`` is SQLite-only; the portable spelling is ``COALESCE``. The levos display_only
    guard in ``_fetch_conversation_rows`` once emitted ``IFNULL(display_only, 0) = 0``, which
    PostgreSQL rejects at parse time; strict mode names it so a regression fails here."""
    from hermes_state_pg_sql import _STRICT_FORBIDDEN
    assert "ifnull(" in _STRICT_FORBIDDEN
    monkeypatch.setenv("HERMES_PG_ADAPTER_STRICT", "1")
    with pytest.raises(RuntimeError, match="ifnull"):
        _translate_sql("SELECT 1 FROM messages WHERE IFNULL(display_only, 0) = 0")
    out = _translate_sql("SELECT 1 FROM messages WHERE COALESCE(display_only, 0) = 0")
    assert "COALESCE(display_only, 0) = 0" in out


def test_no_ifnull_in_sql_emitting_state_modules():
    import re
    from pathlib import Path
    repo = Path(__file__).resolve().parents[1]
    offenders = []
    for path in sorted(repo.glob("hermes_state*.py")) + [repo / "tui_gateway" / "server.py"]:
        source = path.read_text(encoding="utf-8")
        for m in re.finditer(r"\bIFNULL\s*\(", source, flags=re.IGNORECASE):
            line_start = source.rfind("\n", 0, m.start()) + 1
            line = source[line_start : source.find("\n", m.start())]
            # The strict-mode denylist names the token as a quoted literal; that is the guard
            # against IFNULL, not a use of it.
            if '"ifnull("' in line or "'ifnull('" in line:
                continue
            offenders.append(f"{path.name}:{source[: m.start()].count(chr(10)) + 1}")
    assert not offenders, "SQLite-only IFNULL( in SQL-emitting code — use COALESCE(: " + ", ".join(offenders)
