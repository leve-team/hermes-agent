"""Strict COPY merge oracle, executable without Hermes core or PostgreSQL."""

import ast
import re
from pathlib import Path

import pytest

from hermes_state_dual import MIGRATED_TABLES
from migrate_state_to_postgres import _conflict_clause
from state_transfer import TableSpec


def pg_primary_keys(schema):
    schema = re.sub(r"--[^\n]*", "", schema)
    keys = {}
    for table, body in re.findall(
        r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\n\);", schema, re.S
    ):
        composite = re.search(r"\bPRIMARY KEY\s*\(([^)]+)\)", body)
        if composite:
            keys[table] = tuple(column.strip() for column in composite[1].split(","))
        elif "PRIMARY KEY" in body:
            inline = re.findall(r"^\s*(\w+)\s+[^,\n]*\bPRIMARY KEY\b", body, re.M)
            assert len(inline) == 1, (table, body)
            keys[table] = tuple(inline)
    return keys


def assert_copy_merge(sql, *, tables, primary_keys, copy_table, copy_columns):
    match = re.fullmatch(
        r'INSERT INTO "(\w+)" \((.*?)\) SELECT (.*?) FROM "(\w+)"'
        r" WHERE TRUE ON CONFLICT \((.*?)\) (DO NOTHING|DO UPDATE SET .+)",
        sql,
    )
    assert match, sql
    table, inserted, selected, staging, conflict, action = match.groups()
    columns_sql = ", ".join(f'"{column}"' for column in copy_columns)
    assert inserted == selected == columns_sql, sql
    assert len(copy_columns) == len(set(copy_columns)), copy_columns
    assert table in tables and table in primary_keys, table
    assert set(copy_columns) <= set(tables[table]), copy_columns
    assert staging == copy_table == f"_hermes_backfill_{table}", sql
    primary_key = primary_keys[table]
    assert set(primary_key) <= set(copy_columns), (table, copy_columns, primary_key)
    assert conflict == ", ".join(f'"{column}"' for column in primary_key), sql
    mutable = [
        column for column in copy_columns
        if column not in primary_key
        and (table, column) != ("sessions", "parent_session_id")
    ]
    if table == "system_prompts" or not mutable:
        expected = "DO NOTHING"
    else:
        expected = "DO UPDATE SET " + ", ".join(
            f'"{column}" = excluded."{column}"' for column in mutable
        )
        if table == "messages":
            expected += ', "fts_content" = NULL'
    assert action == expected, sql


@pytest.fixture
def primary_keys():
    source = Path(__file__).resolve().parents[1] / "hermes_state_postgres.py"
    module = ast.parse(source.read_text())
    schema = next(
        ast.literal_eval(statement.value)
        for statement in module.body
        if isinstance(statement, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "SCHEMA_SQL_POSTGRES"
            for target in statement.targets
        )
    )
    return pg_primary_keys(schema)


def merge_sql(spec):
    columns = ", ".join(f'"{column}"' for column in spec.columns)
    return (
        f'INSERT INTO "{spec.name}" ({columns}) SELECT {columns}'
        f' FROM "_hermes_backfill_{spec.name}" WHERE TRUE '
        + _conflict_clause(spec, reset_fts=True)
    )


def check_merge(sql, spec, primary_keys):
    assert_copy_merge(
        sql, tables={spec.name: spec.columns}, primary_keys=primary_keys,
        copy_table=f"_hermes_backfill_{spec.name}", copy_columns=spec.columns,
    )


def test_fts_manifest_uses_message_id_not_byte_counter(primary_keys):
    assert primary_keys["hermes_fts_truncations"] == ("message_id",)
    assert "hermes_fts_truncations" not in MIGRATED_TABLES
    assert primary_keys["session_model_usage"] == (
        "session_id", "model", "billing_provider", "billing_base_url", "billing_mode", "task",
    )


@pytest.mark.parametrize(
    "table,values",
    [
        ("system_prompts", ("prompt",)),
        ("sessions", ()),
        ("sessions", ("title", "parent_session_id")),
        ("messages", ("content",)),
        ("session_model_usage", ("cache_read_tokens",)),
        ("hermes_fts_truncations", ("indexed_bytes",)),
    ],
)
def test_merge_accepts_exact_pk_policy(primary_keys, table, values):
    spec = TableSpec(table, (*primary_keys[table], *values), primary_keys[table])
    check_merge(merge_sql(spec), spec, primary_keys)


@pytest.mark.parametrize(
    "old,new",
    [
        ('("id", "content") SELECT', '("content", "id") SELECT'),
        ('SELECT "id", "content"', 'SELECT "content"'),
        ('SELECT "id", "content"', 'SELECT "content", "id"'),
        ('FROM "_hermes_backfill_messages"', 'FROM "_hermes_backfill_sessions"'),
        ('ON CONFLICT ("id")', 'ON CONFLICT'),
        ('ON CONFLICT ("id")', 'ON CONFLICT ("content")'),
        ('ON CONFLICT ("id")', 'ON CONFLICT ("id", "content")'),
        ('DO UPDATE SET "content" = excluded."content", "fts_content" = NULL', 'DO NOTHING'),
        ('excluded."content"', 'excluded."id"'),
        (', "fts_content" = NULL', ''),
        ('"fts_content" = NULL', '"id" = excluded."id"'),
    ],
)
def test_merge_rejects_inexact_columns_pk_or_updates(primary_keys, old, new):
    spec = TableSpec("messages", ("id", "content"), ("id",))
    sql = merge_sql(spec)
    assert old in sql
    with pytest.raises(AssertionError):
        check_merge(sql.replace(old, new, 1), spec, primary_keys)


def test_merge_rejects_synthetic_counter_primary_key(primary_keys):
    spec = TableSpec("hermes_fts_truncations", ("indexed_bytes",), ("indexed_bytes",))
    with pytest.raises(AssertionError):
        check_merge(merge_sql(spec), spec, primary_keys)
