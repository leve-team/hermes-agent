#!/usr/bin/env python3
"""Measure PostgreSQL message-search recall against a read-only SQLite sample."""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

QUERY_KINDS = ("word", "prefix", "phrase", "or", "negative")
DEFAULT_K = 20
DEFAULT_THRESHOLD = 0.9
DEFAULT_PER_KIND = 10
DEFAULT_SAMPLE_ROWS = 5_000
_WORD_RE = re.compile(r"[^\W_]{4,}", re.UNICODE)
_BOOLEAN_WORDS = {"and", "or", "not"}


class RecallInputError(RuntimeError):
    pass


@dataclass(frozen=True)
class QueryCase:
    kind: str
    query: str


def open_sqlite_sample(path: Path) -> sqlite3.Connection:
    path = path.resolve()
    if not path.is_file():
        raise RecallInputError(f"SQLite sample does not exist: {path}")
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    required = {"messages", "messages_fts", "sessions"}
    present = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        )
    }
    missing = sorted(required - present)
    if missing:
        connection.close()
        raise RecallInputError(
            "SQLite sample is missing search objects: " + ", ".join(missing)
        )
    return connection


def sqlite_native_query(query: str) -> str:
    """Map portable unary exclusion to FTS5's binary NOT spelling."""
    return re.sub(r"(?<!\S)-(?=[^\s-])", "NOT ", query)


def sqlite_top_ids(
    connection: sqlite3.Connection, query: str, *, k: int = DEFAULT_K
) -> list[int]:
    rows = connection.execute(
        """
        SELECT m.id
          FROM messages_fts
          JOIN messages m ON m.id = messages_fts.rowid
          JOIN sessions s ON s.id = m.session_id
         WHERE messages_fts MATCH ?
           AND (m.active = 1 OR m.compacted = 1)
         ORDER BY bm25(messages_fts), m.timestamp DESC, m.id DESC
         LIMIT ?
        """,
        (sqlite_native_query(query), k),
    ).fetchall()
    return [int(row[0]) for row in rows]


def _message_tokens(connection: sqlite3.Connection, sample_rows: int):
    token_rows: list[list[str]] = []
    frequencies: Counter[str] = Counter()
    row_membership: dict[str, set[int]] = defaultdict(set)
    rows = connection.execute(
        "SELECT content, tool_name, tool_calls FROM messages ORDER BY id LIMIT ?",
        (sample_rows,),
    ).fetchall()
    for row_index, row in enumerate(rows):
        text = " ".join(str(value or "") for value in row)
        tokens = [
            token.casefold()
            for token in _WORD_RE.findall(text)
            if token.casefold() not in _BOOLEAN_WORDS
        ]
        token_rows.append(tokens)
        frequencies.update(tokens)
        for token in set(tokens):
            row_membership[token].add(row_index)
    ordered = sorted(frequencies, key=lambda token: (-frequencies[token], token))
    return token_rows, ordered, row_membership


def _take_searchable(
    connection: sqlite3.Connection,
    kind: str,
    candidates: Iterable[str],
    per_kind: int,
) -> list[QueryCase]:
    selected: list[QueryCase] = []
    seen: set[str] = set()
    for query in candidates:
        if query in seen:
            continue
        seen.add(query)
        try:
            hits = sqlite_top_ids(connection, query)
        except sqlite3.Error:
            continue
        if hits:
            selected.append(QueryCase(kind, query))
        if len(selected) == per_kind:
            return selected
    return selected


def build_query_corpus(
    connection: sqlite3.Connection,
    *,
    per_kind: int = DEFAULT_PER_KIND,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
) -> list[QueryCase]:
    """Derive a deterministic, non-empty query corpus from the SQLite sample."""
    if per_kind < 10:
        raise ValueError("per_kind must be at least 10")
    if sample_rows <= 0:
        raise ValueError("sample_rows must be greater than zero")

    token_rows, words, membership = _message_tokens(connection, sample_rows)
    if not words:
        raise RecallInputError("SQLite sample has no searchable word tokens")

    phrases = (
        f'"{left} {right}"'
        for tokens in token_rows
        for left, right in zip(tokens, tokens[1:])
        if left != right
    )
    prefixes = (
        f"{token[:length]}*"
        for token in words
        for length in range(len(token) - 1, 2, -1)
    )
    ors = (
        f"{left} OR {right}"
        for index, left in enumerate(words)
        for right in words[index + 1 :]
    )
    negatives = (
        f"{positive} -{excluded}"
        for positive in words
        for excluded in words
        if positive != excluded and membership[positive] - membership[excluded]
    )
    candidates = {
        "word": iter(words),
        "prefix": prefixes,
        "phrase": phrases,
        "or": ors,
        "negative": negatives,
    }

    corpus: list[QueryCase] = []
    short: dict[str, int] = {}
    for kind in QUERY_KINDS:
        cases = _take_searchable(connection, kind, candidates[kind], per_kind)
        corpus.extend(cases)
        if len(cases) < per_kind:
            short[kind] = len(cases)
    if short:
        detail = ", ".join(f"{kind}={count}" for kind, count in short.items())
        raise RecallInputError(
            f"sample cannot supply {per_kind} non-empty queries per kind: {detail}"
        )
    return corpus


SearchIds = Callable[[str, int], Sequence[int]]


def evaluate_recall(
    corpus: Sequence[QueryCase],
    reference_search: SearchIds,
    candidate_search: SearchIds,
    *,
    k: int = DEFAULT_K,
) -> dict:
    if k <= 0:
        raise ValueError("k must be greater than zero")
    cases: list[dict[str, object]] = []
    scores: list[float] = []
    by_kind: dict[str, list[float]] = defaultdict(list)
    for case in corpus:
        reference = list(dict.fromkeys(reference_search(case.query, k)))[:k]
        candidate = set(list(dict.fromkeys(candidate_search(case.query, k)))[:k])
        if not reference:
            raise RecallInputError(
                f"reference query unexpectedly returned no rows: {case.query}"
            )
        recall = len(set(reference) & candidate) / len(reference)
        by_kind[case.kind].append(recall)
        scores.append(recall)
        cases.append({
            "kind": case.kind,
            "query": case.query,
            "reference_count": len(reference),
            "candidate_count": len(candidate),
            "recall": recall,
        })
    return {
        "k": k,
        "query_count": len(cases),
        "recall": statistics.fmean(scores) if scores else 0.0,
        "by_kind": {
            kind: {
                "query_count": len(by_kind[kind]),
                "recall": (statistics.fmean(by_kind[kind]) if by_kind[kind] else 0.0),
            }
            for kind in QUERY_KINDS
        },
        "cases": cases,
    }


def _print_report(report: dict, *, threshold: float, mode: str) -> bool:
    k = int(report["k"])
    print(f"mode={mode} queries={report['query_count']} threshold={threshold:.3f}")
    for kind in QUERY_KINDS:
        item = report["by_kind"][kind]
        print(f"{kind}: queries={item['query_count']} recall@{k}={item['recall']:.4f}")
    passed = float(report["recall"]) >= threshold
    print(f"overall recall@{k}={report['recall']:.4f} {'PASS' if passed else 'FAIL'}")
    return passed


def _postgres_search(dsn: str) -> tuple[SearchIds, Callable[[], None]]:
    from hermes_state_postgres import connect_postgres, search_messages_postgres

    try:
        connection = connect_postgres(dsn)
    except Exception as exc:
        raise RecallInputError(
            f"PostgreSQL connection failed ({type(exc).__name__})"
        ) from exc

    def search(query: str, k: int) -> Sequence[int]:
        try:
            rows = search_messages_postgres(
                connection,
                lambda value: value,
                query,
                limit=k,
                offset=0,
            )
        except Exception as exc:
            raise RecallInputError(
                f"PostgreSQL search failed ({type(exc).__name__})"
            ) from exc
        return [int(row["id"]) for row in rows]

    return search, connection.close


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare SQLite and PostgreSQL message-search recall@20."
    )
    parser.add_argument("sqlite_db", type=Path, help="SQLite sample state.db")
    parser.add_argument(
        "--pg-dsn",
        help="PostgreSQL DSN (default: HERMES_CORE_PG_DSN)",
    )
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--k", type=int, default=DEFAULT_K)
    parser.add_argument("--per-kind", type=int, default=DEFAULT_PER_KIND)
    parser.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    args = parser.parse_args(argv)

    if not 0.0 <= args.threshold <= 1.0:
        parser.error("--threshold must be between 0 and 1")
    if args.k <= 0:
        parser.error("--k must be greater than zero")
    if args.per_kind < 10:
        parser.error("--per-kind must be at least 10")

    sqlite_connection = None
    candidate_close: Callable[[], None] = lambda: None
    try:
        sqlite_connection = open_sqlite_sample(args.sqlite_db)
        corpus = build_query_corpus(
            sqlite_connection,
            per_kind=args.per_kind,
            sample_rows=args.sample_rows,
        )

        def sqlite_search(query: str, k: int) -> Sequence[int]:
            return sqlite_top_ids(sqlite_connection, query, k=k)

        dsn = (args.pg_dsn or os.environ.get("HERMES_CORE_PG_DSN") or "").strip()
        candidate_search: SearchIds
        if args.self_check or not dsn:
            if not dsn:
                print(
                    "SKIP postgres: no --pg-dsn or HERMES_CORE_PG_DSN; running self-check"
                )
            candidate_search = sqlite_search
            mode = "sqlite-self-check"
        else:
            candidate_search, candidate_close = _postgres_search(dsn)
            mode = "sqlite-vs-postgres"

        report = evaluate_recall(
            corpus,
            sqlite_search,
            candidate_search,
            k=args.k,
        )
        return 0 if _print_report(report, threshold=args.threshold, mode=mode) else 1
    except (RecallInputError, RuntimeError, ValueError, sqlite3.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        candidate_close()
        if sqlite_connection is not None:
            sqlite_connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
