"""Exercise the schema execution path without a PostgreSQL server."""

import re

import pytest

import hermes_state_postgres as postgres


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=()):
        self.statements.append(sql)


def assert_ddl_statements(statements):
    assert statements
    broken = []
    for statement in statements:
        uncommented = re.sub(r"--[^\n]*", "", statement).strip()
        first_token = next(iter(uncommented.split()), "").upper()
        if first_token not in {
            "CREATE",
            "ALTER",
            "INSERT",
            "DROP",
            "COMMENT",
            "DO",
            "GRANT",
        }:
            broken.append(uncommented or statement)
    assert not broken, "Broken schema statements:\n" + "\n".join(broken)


def test_schema_executescript_emits_complete_ddl():
    raw = RecordingCursor()
    cursor = postgres._PostgresCursor(raw)
    assert cursor.executescript(postgres.SCHEMA_SQL_POSTGRES) is cursor
    assert_ddl_statements(raw.statements)


def test_deferred_schema_emits_complete_ddl():
    statements = postgres._postgres_schema_statements(indexes=False)
    assert_ddl_statements(statements)
    raw = RecordingCursor()
    postgres._PostgresCursor(raw).executescript(";\n".join(statements))
    assert_ddl_statements(raw.statements)


def test_schema_helper_emits_complete_ddl():
    assert_ddl_statements(postgres._split_sql_statements(postgres.SCHEMA_SQL_POSTGRES))


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        ("", []),
        (" ; ; \n", []),
        ("-- only a comment;", []),
        ("/* only; /* nested; */ comment */", []),
        ("SELECT 1; -- trailing;", ["SELECT 1"]),
        ("-- leading;\nSELECT 1; SELECT 2", ["SELECT 1", "SELECT 2"]),
        ("SELECT -- inline;\r\n1;", ["SELECT  \r\n1"]),
        ("-- leading;\rSELECT 1;", ["SELECT 1"]),
        ("SELECT/* outer; /* inner; */ end */1;", ["SELECT 1"]),
        ("SELECT 'a; -- /* b */'; SELECT 2;", ["SELECT 'a; -- /* b */'", "SELECT 2"]),
        ("SELECT 'it''s; intact';", ["SELECT 'it''s; intact'"]),
        ('SELECT "semi;""--name";', ['SELECT "semi;""--name"']),
        (
            r"SELECT E'it\'s; -- intact'; SELECT 2;",
            [r"SELECT E'it\'s; -- intact'", "SELECT 2"],
        ),
        (r"SELECT 'backslash\'; SELECT 2;", [r"SELECT 'backslash\'", "SELECT 2"]),
        (
            "DO $$ BEGIN PERFORM 1; END; $$; SELECT 2;",
            ["DO $$ BEGIN PERFORM 1; END; $$", "SELECT 2"],
        ),
        (
            "DO $body$ BEGIN PERFORM '$$;'; -- quote ' ;\nEND; $body$;",
            ["DO $body$ BEGIN PERFORM '$$;'; -- quote ' ;\nEND; $body$"],
        ),
        ("SELECT $_tag1$/* ; */ -- ;$_tag1$;", ["SELECT $_tag1$/* ; */ -- ;$_tag1$"]),
        (
            "SELECT $tag$inside $TAG$; still inside$tag$;",
            ["SELECT $tag$inside $TAG$; still inside$tag$"],
        ),
        ("SELECT name$tag$; SELECT 2;", ["SELECT name$tag$", "SELECT 2"]),
        ("SELECT $1; SELECT 2;", ["SELECT $1", "SELECT 2"]),
    ],
)
def test_split_sql_statements_preserves_quoted_semicolons(script, expected):
    statements = postgres._split_sql_statements(script)
    assert len(statements) == len(expected)
    assert statements == expected


@pytest.mark.parametrize(
    "script",
    ["SELECT 'open;", 'SELECT "open;', "DO $$open;", "DO $tag$open;", "/* open;"],
)
def test_unterminated_sql_fails_before_execution(script):
    raw = RecordingCursor()
    with pytest.raises(ValueError, match="Unterminated SQL"):
        postgres._PostgresCursor(raw).executescript("SELECT 1; " + script)
    assert raw.statements == []


def test_synthetic_schema_uses_same_splitter_in_both_paths(monkeypatch):
    script = """
-- SQLite authority or dual-write; it is rebuilt/maintained with fts_content.
CREATE TABLE sample (value TEXT DEFAULT 'literal; -- not a comment');
/* Index comment; /* nested; */ still a comment */
CREATE INDEX sample_value ON sample (value);
CREATE UNIQUE INDEX sample_unique ON sample (value);
COMMENT ON TABLE sample IS 'it''s; intact';
DO $$ BEGIN PERFORM 'body; -- literal'; END; $$;
"""
    monkeypatch.setattr(postgres, "SCHEMA_SQL_POSTGRES", script)
    all_statements = postgres._split_sql_statements(script)
    assert len(all_statements) == 5
    raw = RecordingCursor()
    postgres._PostgresCursor(raw).executescript(script)
    assert raw.statements == all_statements
    tables = postgres._postgres_schema_statements(indexes=False)
    indexes = postgres._postgres_schema_statements(indexes=True)
    assert tables == [all_statements[0], *all_statements[3:]]
    assert indexes == all_statements[1:3]
    deferred_raw = RecordingCursor()
    postgres._PostgresCursor(deferred_raw).executescript(";\n".join(tables))
    assert deferred_raw.statements == tables
