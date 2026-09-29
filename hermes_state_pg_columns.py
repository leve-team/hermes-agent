"""Declared columns of the SQLite ``SCHEMA_SQL`` without opening SQLite (levos v3).

``reconcile_postgres_columns`` ADDs to PostgreSQL every column ``SCHEMA_SQL`` declares.
``SessionSchemaMixin._parse_schema_columns`` answers that by executing the script in
``sqlite3.connect(":memory:")``; on a PostgreSQL-authority profile no SQLite connection is opened
at all, so the PostgreSQL path reads the ``CREATE TABLE`` statements itself. The result is the
same mapping ``PRAGMA table_info`` produces — ``{table: {column: "<type> [NOT NULL] [DEFAULT x]"}}``
with SQLite's spelling of the declared type (the source text) and default (a parenthesised
expression without its outer parentheses) — which a test holds equal to the SQLite parse.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

_CREATE_TABLE = re.compile(
    r"\A\s*CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[\w.]+)\s*\(",
    re.IGNORECASE,
)
_TOKEN = re.compile(
    r"\s*(?:(?P<string>'(?:''|[^'])*')|(?P<quoted>\"(?:\"\"|[^\"])*\"|`[^`]*`|\[[^\]]*\])"
    r"|(?P<number>[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)|(?P<word>\w+)|(?P<open>\()|(?P<other>[^\s\w(]))"
)
_TABLE_CONSTRAINTS = {"CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"}
_COLUMN_CONSTRAINTS = {
    "CONSTRAINT", "PRIMARY", "NOT", "NULL", "UNIQUE", "CHECK", "DEFAULT", "COLLATE",
    "REFERENCES", "GENERATED", "AS",
}


def _unquote(name: str) -> str:
    if name[:1] in "\"`[" and len(name) >= 2:
        return name[1:-1].replace('""', '"')
    return name


def _group_end(text: str, start: int) -> int:
    """Index just past the parenthesised group opening at *start* (quotes respected)."""
    depth, position = 0, start
    while position < len(text):
        char = text[position]
        if char in "'\"`":
            position = text.index(char, position + 1) + 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return position + 1
        position += 1
    raise ValueError("unbalanced parentheses in CREATE TABLE")


def _tokens(text: str) -> List[Tuple[str, str, int, int]]:
    """``(kind, text, start, end)`` tokens; a parenthesised group is one ``group`` token."""
    tokens, position = [], 0
    while True:
        match = _TOKEN.match(text, position)
        if match is None or match.end() == position:
            return tokens
        kind = match.lastgroup
        start = match.start(kind)
        end = _group_end(text, start) if kind == "open" else match.end()
        tokens.append(("group" if kind == "open" else kind, text[start:end], start, end))
        position = end


def _split_items(body: str) -> List[str]:
    """The body of a ``CREATE TABLE (...)`` split at top-level commas."""
    items, depth, quote, start = [], 0, "", 0
    for position, char in enumerate(body):
        if quote:
            quote = "" if char == quote else quote
        elif char in "'\"`":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items.append(body[start:position])
            start = position + 1
    items.append(body[start:])
    return [item.strip() for item in items if item.strip()]


def _column(item: str) -> Tuple[str, str, bool, bool, Optional[str]]:
    """``(name, declared type, not null, primary key, default)`` of one column definition."""
    tokens = _tokens(item)
    name = _unquote(tokens[0][1])
    index, type_start, type_end = 1, None, None
    while index < len(tokens) and not (
            tokens[index][0] == "word" and tokens[index][1].upper() in _COLUMN_CONSTRAINTS):
        if type_start is None:
            type_start = tokens[index][2]
        type_end = tokens[index][3]
        index += 1
    declared = item[type_start:type_end] if type_start is not None else ""
    not_null = primary = False
    default: Optional[str] = None
    while index < len(tokens):
        word = tokens[index][1].upper() if tokens[index][0] == "word" else ""
        if word == "NOT" and index + 1 < len(tokens) and tokens[index + 1][1].upper() == "NULL":
            not_null, index = True, index + 2
            continue
        if word == "PRIMARY":
            primary = True
        elif word == "DEFAULT" and index + 1 < len(tokens):
            kind, text = tokens[index + 1][0], tokens[index + 1][1]
            default = text[1:-1] if kind == "group" else text
            index += 2
            continue
        index += 1
    return name, declared, not_null, primary, default


def _table_primary_key(item: str) -> List[str]:
    tokens = _tokens(item)
    words = [token[1].upper() if token[0] == "word" else "" for token in tokens]
    for index in range(len(tokens) - 2):
        if words[index] == "PRIMARY" and words[index + 1] == "KEY" and tokens[index + 2][0] == "group":
            return [_unquote(part.split()[0]) for part in _split_items(tokens[index + 2][1][1:-1])]
    return []


def declared_schema_columns(schema_sql: str) -> Dict[str, Dict[str, str]]:
    """``SessionSchemaMixin._parse_schema_columns(schema_sql)`` for the ``CREATE TABLE``
    statements of *schema_sql*, computed without SQLite."""
    from hermes_state_pg_schema import _split_sql_statements

    tables: Dict[str, Dict[str, str]] = {}
    for statement in _split_sql_statements(schema_sql):
        match = _CREATE_TABLE.match(statement)
        if match is None:
            continue
        body_start = match.end() - 1
        body = statement[body_start + 1:_group_end(statement, body_start) - 1]
        columns, primary = [], set()
        for item in _split_items(body):
            first = _tokens(item)[0][1].upper()
            if first in _TABLE_CONSTRAINTS:
                primary.update(_table_primary_key(item))
                continue
            columns.append(_column(item))
        declared: Dict[str, str] = {}
        for name, type_text, not_null, is_primary, default in columns:
            parts = [type_text] if type_text else []
            if not_null and not (is_primary or name in primary):
                parts.append("NOT NULL")
            if default is not None:
                parts.append(f"DEFAULT {default}")
            declared[name] = " ".join(parts)
        tables.setdefault(_unquote(match.group("name")), declared)
    return tables
