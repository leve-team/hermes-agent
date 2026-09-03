from __future__ import annotations

from pathlib import Path

import pytest

from hermes_state import SessionDB


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
