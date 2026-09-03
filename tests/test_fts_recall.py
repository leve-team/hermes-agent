from __future__ import annotations

from collections import Counter
from pathlib import Path

from hermes_state import SessionDB
from scripts.fts_recall import (
    QUERY_KINDS,
    QueryCase,
    build_query_corpus,
    evaluate_recall,
    main,
    open_sqlite_sample,
)


_WORDS = [
    "amber",
    "bronze",
    "cobalt",
    "denim",
    "ember",
    "fuchsia",
    "garnet",
    "hazel",
    "indigo",
    "jadeite",
    "khaki",
    "lilac",
    "magenta",
    "navyblue",
    "ochre",
    "pearl",
    "quartz",
    "russet",
    "saffron",
    "tealish",
    "umber",
    "violet",
    "walnut",
    "xanthic",
    "yellow",
    "zircon",
]


def _sample_database(path: Path) -> None:
    db = SessionDB(db_path=path)
    try:
        db.create_session("sample", source="cli")
        for index, word in enumerate(_WORDS):
            next_word = _WORDS[(index + 1) % len(_WORDS)]
            db.append_message(
                "sample",
                "user" if index % 2 == 0 else "assistant",
                f"{word} {next_word} deployment record {index}",
                tool_name=f"probe_{word}",
                tool_calls=f'{{"marker":"{next_word}"}}',
                timestamp=float(index + 1),
            )
    finally:
        db.close()


def test_corpus_has_ten_nonempty_queries_of_every_contract_kind(tmp_path: Path) -> None:
    path = tmp_path / "sample.db"
    _sample_database(path)
    connection = open_sqlite_sample(path)
    try:
        corpus = build_query_corpus(connection)
    finally:
        connection.close()

    counts = Counter(case.kind for case in corpus)
    assert counts == {kind: 10 for kind in QUERY_KINDS}
    assert len({case.query for case in corpus}) == 50


def test_recall_uses_sqlite_top_set_as_denominator() -> None:
    corpus = [QueryCase("word", "needle")]

    report = evaluate_recall(
        corpus,
        lambda _query, _k: [1, 2, 3, 4],
        lambda _query, _k: [2, 4, 9],
        k=20,
    )

    assert report["recall"] == 0.5
    assert report["cases"][0]["reference_count"] == 4


def test_cli_without_postgres_marks_skip_and_self_check_is_one(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    path = tmp_path / "sample.db"
    _sample_database(path)
    monkeypatch.delenv("HERMES_CORE_PG_DSN", raising=False)

    assert main([str(path)]) == 0

    output = capsys.readouterr().out
    assert "SKIP postgres" in output
    assert "queries=50" in output
    assert "overall recall@20=1.0000 PASS" in output


def test_cli_returns_one_when_postgres_recall_is_below_threshold(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    path = tmp_path / "sample.db"
    _sample_database(path)
    monkeypatch.setattr(
        "scripts.fts_recall._postgres_search",
        lambda _dsn: (lambda _query, _k: [], lambda: None),
    )

    assert main([str(path), "--pg-dsn", "test-only"]) == 1

    output = capsys.readouterr().out
    assert "mode=sqlite-vs-postgres" in output
    assert "overall recall@20=0.0000 FAIL" in output
