"""Real-engine regressions for the 0056 kanban SQLite-dialect absorption layer.

The adapter used to hand ``PRAGMA``/``BEGIN IMMEDIATE``/``sqlite_master`` to
psycopg verbatim. Host callers write those spellings against whichever board
connection they hold, so the failure was one 500 per spelling rather than one
bug. These cases run the statements on a real PostgreSQL board and assert the
answers, plus the two refusals that must stay refusals.
"""

from __future__ import annotations

import contextlib

import psycopg
import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_dialect_rules as rules
from tests.hermes_cli.persistence_pg_support import pg_dsn as pg_dsn, pg_server as pg_server


@pytest.fixture
def board(pg_dsn, tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / "profiles" / "worker").mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    for name in (
        "HERMES_KANBAN_DB",
        "HERMES_KANBAN_BOARD",
        "HERMES_KANBAN_BACKEND",
        "HERMES_KANBAN_POSTGRES_DSN",
    ):
        monkeypatch.delenv(name, raising=False)
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        yield conn


def test_database_list_answers_with_the_schema_the_session_is_bound_to(board):
    """The claim path reads ``row[2]`` as "where this board lives"."""
    row = board.execute("PRAGMA database_list").fetchone()
    assert tuple(row) == (0, "main", "public")


def test_table_info_answers_the_six_columns_callers_read_by_position(board):
    columns = board.execute("PRAGMA table_info(tasks)").fetchall()
    names = [row[1] for row in columns]
    assert "id" in names and "status" in names
    assert [row[0] for row in columns] == list(range(len(columns)))
    by_name = {row[1]: row for row in columns}
    assert by_name["id"][5] == 1, "the primary key must report pk=1"
    assert by_name["status"][5] == 0
    assert board.execute("PRAGMA table_info(no_such_table)").fetchall() == []


def test_begin_immediate_opens_a_real_transaction_and_rollback_undoes_it(board):
    """Degrading this to autocommit would silently commit half a unit of work."""
    board.execute("BEGIN IMMEDIATE")
    assert board.in_transaction
    board.execute(
        "INSERT INTO tasks (id, title, status, created_at) VALUES (?, ?, ?, ?)",
        ("t_absorb_rollback", "rolled back", "ready", 100),
    )
    board.execute("ROLLBACK")
    assert board.in_transaction is False
    assert (
        board.execute(
            "SELECT COUNT(*) FROM tasks WHERE id = ?", ("t_absorb_rollback",)
        ).fetchone()[0]
        == 0
    )


def test_a_nested_begin_is_a_no_op_rather_than_an_error(board):
    """Callers open their own transaction over one the adapter already holds."""
    board.execute("BEGIN IMMEDIATE")
    board.execute("BEGIN IMMEDIATE")
    assert board.in_transaction
    board.execute(
        "INSERT INTO tasks (id, title, status, created_at) VALUES (?, ?, ?, ?)",
        ("t_absorb_nested", "committed", "ready", 100),
    )
    board.execute("COMMIT")
    assert board.in_transaction is False
    assert (
        board.execute(
            "SELECT title FROM tasks WHERE id = ?", ("t_absorb_nested",)
        ).fetchone()[0]
        == "committed"
    )


def test_the_schema_catalog_question_answers_instead_of_failing(board):
    assert (
        board.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", ("tasks",)
        ).fetchone()
        is not None
    )
    assert (
        board.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("no_such_table",),
        ).fetchone()
        is None
    )


def test_file_pragmas_answer_without_pretending_to_have_a_file(board):
    assert board.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert board.execute("PRAGMA foreign_keys=ON").fetchall() == []
    assert board.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    assert board.execute("PRAGMA busy_timeout=5000").fetchall() == []
    assert board.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert board.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0


def test_an_unknown_pragma_is_refused_rather_than_silently_ignored(board):
    """A quiet no-op answers the caller's question wrongly and it never finds out."""
    with pytest.raises(rules.NotSupportedError):
        board.execute("PRAGMA writable_schema=ON")


def test_attach_is_refused_rather_than_imitated(board):
    """The sibling store shares this schema; an imitated ATTACH points nowhere."""
    with pytest.raises(rules.NotSupportedError):
        board.execute("ATTACH DATABASE '/tmp/telemetry.db' AS telemetry")


def test_ordinary_statements_are_untouched_by_the_absorber(board):
    """The layer is a closed rule table, not a general SQL rewriter."""
    board.execute(
        "INSERT INTO tasks (id, title, status, created_at) VALUES (?, ?, ?, ?)",
        ("t_absorb_plain", "plain", "ready", 100),
    )
    board.commit()
    assert (
        board.execute(
            "SELECT status FROM tasks WHERE id = ?", ("t_absorb_plain",)
        ).fetchone()[0]
        == "ready"
    )
    assert rules.absorb("SELECT 1 FROM tasks") == (rules.PASS, None)


def test_the_sqlite_backend_is_unchanged_by_the_absorber(tmp_path):
    """SQLite still answers its own dialect; nothing was routed away from it."""
    with contextlib.closing(kb.connect(tmp_path / "kanban.db")) as conn:
        assert conn.execute("PRAGMA database_list").fetchone()[2].endswith(
            "kanban.db"
        )
        assert {row[1] for row in conn.execute("PRAGMA table_info(tasks)")} >= {
            "id",
            "status",
        }
        assert (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks'"
            ).fetchone()
            is not None
        )


def test_the_adapter_refuses_a_pragma_psycopg_would_have_refused_anyway(pg_dsn):
    """Without the layer this is what every absorbed spelling looked like."""
    with psycopg.connect(pg_dsn, autocommit=True) as raw:
        with pytest.raises(psycopg.errors.SyntaxError):
            raw.execute("PRAGMA database_list")
