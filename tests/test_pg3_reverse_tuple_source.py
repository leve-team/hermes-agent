"""Exercise reverse backfill with psycopg-shaped tuple rows, without PostgreSQL."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from hermes_state import SessionDB
from state_reverse import InjectedReverseFault, reverse_backfill
from state_transfer import TableSpec, primary_key_from_row


@pytest.mark.parametrize("row_kind", ["tuple", "sqlite-row", "dict"])
@pytest.mark.parametrize(
    ("primary_key", "expected"),
    [(("second_key",), [22]), (("second_key", "first_key"), [22, 11])],
)
def test_primary_key_uses_column_positions_and_key_order(
    row_kind, primary_key, expected
):
    spec = TableSpec("sample", ("first_key", "payload", "second_key"), primary_key)
    connection = sqlite3.connect(":memory:")
    try:
        connection.row_factory = sqlite3.Row if row_kind == "sqlite-row" else None
        row = connection.execute(
            "SELECT 11 AS first_key, 'payload' AS payload, 22 AS second_key"
        ).fetchone()
        if row_kind == "dict":
            row = dict(zip(spec.columns, row))
        assert primary_key_from_row(spec, row) == expected
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("row", "error"), [(("payload",), IndexError), ({"payload": "value"}, KeyError)]
)
def test_primary_key_rejects_rows_missing_key_values(row, error):
    spec = TableSpec("sample", ("payload", "identity"), ("identity",))
    with pytest.raises(error):
        primary_key_from_row(spec, row)


class TuplePostgresCursor(sqlite3.Cursor):
    def execute(self, sql, params=()):
        assert "?" not in sql
        return super().execute(sql.replace("%s", "?"), params)


class TuplePostgresSource:
    """Adapt only the DB boundary; keep PostgreSQL placeholder and tuple contracts."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(
            path.resolve().as_uri() + "?mode=ro", uri=True, isolation_level=None
        )
        self.connection.row_factory = None
        self.connection.execute("BEGIN")
        assert isinstance(self.connection.execute("SELECT 1").fetchone(), tuple)

    def execute(self, sql, params=()):
        return self.cursor().execute(sql, params)

    def cursor(self):
        return self.connection.cursor(factory=TuplePostgresCursor)

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


@pytest.mark.parametrize("interrupt", [False, True])
def test_reverse_tuple_source_completes_and_resumes(tmp_path, monkeypatch, interrupt):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    monkeypatch.setenv("HERMES_STATE_BACKEND", "sqlite")
    monkeypatch.setenv("HERMES_STATE_DUAL_WRITE", "0")
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "reverse.db"
    checkpoint_path = tmp_path / "reverse.json"
    source_db = SessionDB(db_path=source_path, dual_write=False)
    try:
        source_db.create_session("z-parent", "cli")
        source_db.create_session("a-child", "cli", parent_session_id="z-parent")
        source_db.append_message("a-child", "user", "tuple payload", timestamp=7.0)
        source_db.append_message(
            "z-parent", "assistant", "parent payload", timestamp=8.0
        )
    finally:
        source_db.close()
    with sqlite3.connect(source_path) as source:
        source.executemany(
            "INSERT INTO gateway_routing (scope, session_key, entry_json, updated_at)"
            " VALUES (?, ?, ?, ?)",
            [("scope", "a-child", "{}", 7.0), ("scope", "z-parent", "{}", 8.0)],
        )

    def source_factory(_dsn):
        return TuplePostgresSource(source_path)

    options = {
        "checkpoint_path": checkpoint_path,
        "batch_rows": 1,
        "_source_factory": source_factory,
    }
    if interrupt:
        with pytest.raises(InjectedReverseFault):
            reverse_backfill("test-only", target_path, fault_inject_at="50%", **options)
        partial = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        assert partial["completed"] is False
        assert any(table["last_pk"] is not None for table in partial["tables"].values())

    summary = reverse_backfill("test-only", target_path, resume=interrupt, **options)
    assert summary["complete"] is True
    assert summary["diff"]["clean"] is True
    assert summary["diff"]["mismatch_count"] == 0
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["completed"] is True
    assert all(table["complete"] for table in checkpoint["tables"].values())
    assert checkpoint["tables"]["sessions"]["last_pk"] == ["z-parent"]
    assert checkpoint["tables"]["gateway_routing"]["last_pk"] == ["scope", "z-parent"]
    assert checkpoint["tables"]["messages"]["rows"] == 2
    with sqlite3.connect(target_path) as target:
        assert target.execute(
            "SELECT parent_session_id FROM sessions WHERE id = ?", ("a-child",)
        ).fetchone() == ("z-parent",)
        assert target.execute("PRAGMA foreign_key_check").fetchall() == []

    repeated = reverse_backfill("test-only", target_path, resume=True, **options)
    assert repeated["complete"] is True
    assert repeated["diff"]["mismatch_count"] == 0
    assert repeated["rows_by_table"] == summary["rows_by_table"]
