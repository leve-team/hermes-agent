"""Real SQLite/PostgreSQL contract tests for the kanban persistence seam."""

from __future__ import annotations

import contextlib
import concurrent.futures
import asyncio
from py_pglite import PGliteConfig, PGliteManager
import json
import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import psycopg
import pytest

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_persistence import dialect_for, set_board_dsn_resolver
from hermes_cli.kanban_postgres import KanbanPostgresConnection
from tests.hermes_cli.persistence_pg_support import (
    pg_dsn as pg_dsn,
    pg_server as pg_server,
)


@pytest.fixture
def routed_boards(pg_server, tmp_path, monkeypatch):
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", "postgres")
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_BOARD", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_POSTGRES_DSN", raising=False)
    boards = ("default", "other-guild")
    with psycopg.connect(pg_server, autocommit=True) as raw:
        for board in boards:
            schema = sql.Identifier("svc_kanban_" + board.replace("-", "_"))
            raw.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(schema))
            raw.execute(sql.SQL("CREATE SCHEMA {}").format(schema))

    def resolver(token):
        return make_conninfo(pg_server, options=f"-c search_path=svc_kanban_{token}")

    set_board_dsn_resolver(resolver, boards=boards)
    try:
        yield home, resolver
    finally:
        set_board_dsn_resolver(None)


def test_pg_board_cli_lists_real_subprocesses(routed_boards, pg_server):
    home, _ = routed_boards
    for board in ("default", "other-guild"):
        assert kb.init_db(board=board) is None
        with contextlib.closing(kb.connect(board=board)) as conn:
            task_id = kb.create_task(conn, title=board)
            conn.execute("UPDATE tasks SET id = ? WHERE id = ?", ("same-id", task_id))
            kb.add_comment(conn, "same-id", author="test", body=board)

    bootstrap = """
import os
from psycopg.conninfo import make_conninfo
from hermes_cli.kanban_persistence import set_board_dsn_resolver
set_board_dsn_resolver(
    lambda token: make_conninfo(os.environ['TEST_PG_DSN'], options=f'-c search_path=svc_kanban_{token}'),
    boards=('default', 'other-guild'),
)
from hermes_cli.main import main
main()
"""
    for board, args in (("default", []), ("other-guild", ["--board", "other-guild"])):
        command = ["kanban", *args, "list", "--json"]
        result = subprocess.run(
            [sys.executable, "-c", bootstrap, *command],
            cwd=Path(__file__).resolve().parents[2],
            env={**os.environ, "TEST_PG_DSN": pg_server},
            capture_output=True,
            text=True,
            timeout=60,
        )
        print("CLI:", " ".join(command), "EXIT=", result.returncode)
        print(result.stdout)
        assert result.returncode == 0, result.stderr
        tasks = json.loads(result.stdout)
        assert [(task["id"], task["title"]) for task in tasks] == [("same-id", board)]
        with contextlib.closing(kb.connect(board=board)) as conn:
            assert [
                (row["id"], row["body"])
                for row in conn.execute("SELECT id, body FROM task_comments").fetchall()
            ] == [(1, board)]
    assert not list(home.rglob("*.db"))
    assert not kb.boards_root().exists()


def test_pg_running_census_counts_other_schema_without_sqlite_files(routed_boards):
    home, _ = routed_boards
    with contextlib.closing(kb.connect(board="default")) as conn:
        kb.create_task(conn, title="idle")
        assert kb.count_running_tasks(conn) == 0
    with contextlib.closing(kb.connect(board="other-guild")) as conn:
        task_id = kb.create_task(conn, title="running elsewhere")
        assert kb.claim_task(conn, task_id, claimer="worker") is not None
        assert kb.count_running_tasks(conn) == 1
    assert not list(home.rglob("*.db"))
    count = kb.count_running_tasks_other_boards("default")
    print("CENSUS: default=0 other-guild=1 other_boards(default)=", count)
    assert count == 1
    assert kb.count_running_tasks_other_boards("other-guild") == 0


@pytest.mark.asyncio
async def test_pg_gateway_watcher_dispatches_each_registered_board(
    routed_boards, monkeypatch
):
    from gateway.kanban_watchers import GatewayKanbanWatchersMixin
    from hermes_cli.plugins import get_plugin_manager

    home, _ = routed_boards
    (home / "config.yaml").write_text(
        json.dumps({
            "kanban": {
                "dispatch_in_gateway": True,
                "auto_decompose": False,
                "max_in_progress": 2,
                "reconcile_orphans": False,
            }
        }),
        encoding="utf-8",
    )
    for board, count in (("default", 1), ("other-guild", 2)):
        with contextlib.closing(kb.connect(board=board)) as conn:
            for number in range(count):
                kb.create_task(conn, title=f"{board}-{number}")
            conn.execute("UPDATE tasks SET status = 'todo'")
    runner = GatewayKanbanWatchersMixin()
    runner._running = True
    ticks = {}
    finished = asyncio.Event()
    loop = asyncio.get_running_loop()

    def observe(**payload):
        ticks[payload["board"]] = payload["result"].promoted
        if len(ticks) == 2:
            runner._running = False
            loop.call_soon_threadsafe(finished.set)

    manager = get_plugin_manager()
    monkeypatch.setitem(manager._hooks, "on_kanban_dispatch_tick", [observe])
    watcher = asyncio.create_task(runner._kanban_dispatcher_watcher())
    try:
        await asyncio.wait_for(finished.wait(), timeout=30)
        await asyncio.wait_for(watcher, timeout=10)
    finally:
        runner._running = False
        if not watcher.done():
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
        runner._release_kanban_dispatcher_lock()
    print("GATEWAY PG TICKS:", ticks)
    assert ticks == {"default": 1, "other-guild": 2}
    assert not list(home.rglob("*.db"))


def test_pg_running_census_connection_failure_is_not_zero(routed_boards, tmp_path):
    from psycopg.conninfo import make_conninfo

    _, resolver = routed_boards
    set_board_dsn_resolver(
        lambda token: make_conninfo(resolver(token), host=str(tmp_path / "no-socket")),
        boards=("default", "other-guild"),
    )
    with pytest.raises(
        RuntimeError, match="census failed for board 'other-guild'"
    ) as error:
        kb.count_running_tasks_other_boards("default")
    print("CENSUS FAILURE:", str(error.value))
    assert str(tmp_path) not in str(error.value)


def test_pg_running_count_query_failure_is_not_zero(routed_boards):
    with contextlib.closing(kb.connect(board="other-guild")) as conn:
        conn.execute("DROP TABLE tasks")
        with pytest.raises(psycopg.errors.UndefinedTable):
            kb.count_running_tasks(conn)


def test_pg_missing_inventory_fails_closed(monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", "postgres")
    with pytest.raises(ValueError, match="resolver"):
        kb.count_running_tasks_other_boards("default")
    with pytest.raises(ValueError, match="resolver"):
        kb.list_boards()


@pytest.mark.parametrize(
    "slug",
    [
        "../other",
        "other;DROP SCHEMA public",
        "other'",
        'other"',
        "other%20",
        "other,public",
        "other\\name",
        "a\nb",
        "",
        "a" * 64,
    ],
)
def test_pg_board_injection_rejected_before_resolver(routed_boards, slug, monkeypatch):
    calls = []

    def resolver(token):
        calls.append(token)
        raise AssertionError("Invalid slugs must not reach the resolver")

    set_board_dsn_resolver(resolver, boards=("default", "other-guild"))
    with pytest.raises(ValueError, match="slug"):
        kb.connect(board=slug)
    monkeypatch.setenv("HERMES_KANBAN_BOARD", slug)
    with pytest.raises(ValueError, match="slug"):
        kb.init_db()
    assert not calls


def test_pg_normalization_collision_and_unknown_board(routed_boards, monkeypatch):
    _, resolver = routed_boards
    with pytest.raises(ValueError, match="colliding"):
        set_board_dsn_resolver(resolver, boards=("other-guild", "other_guild"))
    assert [meta["slug"] for meta in kb.list_boards()] == ["default", "other-guild"]
    with pytest.raises(ValueError, match="not registered"):
        kb.connect(board="missing")
    monkeypatch.setenv("HERMES_KANBAN_BOARD", " OTHER-GUILD ")
    with contextlib.closing(kb.connect()) as conn:
        assert (
            conn.execute("SELECT current_schema()").fetchone()[0]
            == "svc_kanban_other_guild"
        )
    with kb.scoped_current_board("default"):
        with contextlib.closing(kb.connect()) as conn:
            assert (
                conn.execute("SELECT current_schema()").fetchone()[0]
                == "svc_kanban_default"
            )


@pytest.mark.parametrize("value", [None, "", "  "])
def test_pg_empty_resolver_does_not_fall_back_to_env(
    routed_boards, pg_server, monkeypatch, value
):
    monkeypatch.setenv("HERMES_KANBAN_POSTGRES_DSN", pg_server)
    set_board_dsn_resolver(lambda token: value, boards=("default",))
    with pytest.raises(RuntimeError, match="resolution failed"):
        kb.connect()


def test_pg_selector_conflicts_and_explicit_sqlite_override(
    routed_boards, tmp_path, monkeypatch
):
    _, resolver = routed_boards
    with pytest.raises(ValueError, match="conflicts"):
        kb.connect(postgres_dsn=resolver("default"))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "forbidden.db"))
    with pytest.raises(ValueError, match="SQLite path"):
        kb.init_db()
    with contextlib.closing(
        kb.connect(tmp_path / "sqlite.db", backend="sqlite")
    ) as conn:
        assert isinstance(conn, sqlite3.Connection)
    assert not (tmp_path / "forbidden.db").exists()


@pytest.mark.parametrize(
    "options",
    [
        "",
        "-c search_path=missing",
        "-c search_path=public,other",
        "-c search_path=public;DROP",
        "-c search_path=" + "a" * 64,
    ],
)
def test_pg_board_schema_must_be_explicit_safe_and_present(
    routed_boards, pg_server, options
):
    from psycopg.conninfo import make_conninfo

    set_board_dsn_resolver(
        lambda token: make_conninfo(pg_server, options=options),
        boards=("default",),
    )
    with pytest.raises(RuntimeError, match="connection failed for board 'default'"):
        kb.init_db()
    with psycopg.connect(pg_server) as raw:
        assert (
            raw.execute(
                "SELECT count(*) FROM pg_tables WHERE schemaname IN (%s, %s)",
                ("svc_kanban_default", "missing"),
            ).fetchone()[0]
            == 0
        )


def test_pg_resolver_exception_does_not_expose_dsn(routed_boards):
    def resolver(token):
        raise RuntimeError("postgresql://test-user:test-secret@example.invalid/db")

    set_board_dsn_resolver(resolver, boards=("default", "other-guild"))
    with pytest.raises(
        RuntimeError, match="resolution failed for board 'default'"
    ) as error:
        kb.init_db()
    assert "test-secret" not in str(error.value)
    with pytest.raises(RuntimeError, match="census failed for board 'other-guild'"):
        kb.count_running_tasks_other_boards("default")


def test_pg_explicit_backend_uses_registered_board_not_sqlite_discovery(
    routed_boards, monkeypatch
):
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", "sqlite")
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "other-guild")
    with contextlib.closing(kb.connect(backend="postgres")) as conn:
        assert (
            conn.execute("SELECT current_schema()").fetchone()[0]
            == "svc_kanban_other_guild"
        )


class KanbanPGliteManager(PGliteManager):
    """Adapt py-pglite 0.5.3 startup to the pinned socket server API."""

    def _generate_unix_js_content(self, ext_requires_str, extensions_obj_str):
        source = super()._generate_unix_js_content(ext_requires_str, extensions_obj_str)
        for old, new in (
            ("const db = new PGlite({", "const db = await PGlite.create({"),
            ("path: SOCKET_PATH,", "path: SOCKET_PATH, maxConnections: 16,"),
        ):
            assert source.count(old) == 1
            source = source.replace(old, new)
        return source


@pytest.fixture(scope="module")
def pg_server(tmp_path_factory):
    work_dir = tmp_path_factory.mktemp("kanban-pg")
    (work_dir / "package.json").write_text(
        json.dumps({
            "private": True,
            "dependencies": {
                "@electric-sql/pglite": "0.3.16",
                "@electric-sql/pglite-socket": "0.0.22",
            },
        }),
        encoding="utf-8",
    )
    manager = KanbanPGliteManager(PGliteConfig(work_dir=work_dir))
    try:
        manager.start()
        dsn = manager.get_connection_string().replace(
            "postgresql+psycopg://", "postgresql://"
        )
        with psycopg.connect(dsn) as raw:
            print("KANBAN_REAL_PG:", raw.execute("SELECT version()").fetchone()[0])
        yield dsn
    finally:
        manager.stop()


@pytest.fixture
def pg_dsn(pg_server):
    with psycopg.connect(pg_server, autocommit=True) as raw:
        raw.execute("DROP SCHEMA public CASCADE")
        raw.execute("CREATE SCHEMA public")
        raw.execute("SET search_path = public")
    return pg_server


@pytest.fixture(params=["sqlite", "postgres"])
def connection(request, tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_KANBAN_BACKEND", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_POSTGRES_DSN", raising=False)
    if request.param == "sqlite":
        conn = kb.connect(tmp_path / "kanban.db")
    else:
        conn = kb.connect(
            backend="postgres", postgres_dsn=request.getfixturevalue("pg_dsn")
        )
    try:
        yield conn
    finally:
        conn.close()


def test_create_claim_update_list_roundtrip(connection):
    task_id = kb.create_task(
        connection, title="quote ' ? % 한글", body='{"nested":null}'
    )
    assert kb.get_task(connection, task_id).status == "ready"
    assert kb.assign_task(connection, task_id, "coder")
    claimed = kb.claim_task(connection, task_id, claimer="worker-1")
    assert claimed.status == "running"
    assert claimed.current_run_id > 0
    assert kb.claim_task(connection, task_id, claimer="worker-2") is None
    assert kb.heartbeat_claim(connection, task_id, claimer="worker-1", ttl_seconds=1234)
    assert kb.set_model_override(connection, task_id, "example-model")
    listed = kb.list_tasks(connection, assignee="coder", status="running")
    assert [task.id for task in listed] == [task_id]
    assert listed[0].title == "quote ' ? % 한글"
    assert listed[0].model_override == "example-model"
    comment_id = kb.add_comment(connection, task_id, author="test", body="comment")
    assert kb.list_comments(connection, task_id)[0].id == comment_id
    assert connection.execute("SELECT count(*) FROM task_runs").fetchone()[0] == 1


def test_write_rollback_and_nested_savepoints(connection):
    with pytest.raises(ValueError, match="outer abort"):
        with kb.write_txn(connection):
            task_id = kb.create_task(connection, title="outer")
            assert connection.in_transaction
            with pytest.raises(RuntimeError, match="nest|Nested"):
                with kb.write_txn(connection):
                    pytest.fail("implicit nesting accepted")
            with pytest.raises(ValueError, match="inner abort"):
                with kb.write_txn(connection, allow_nested=True):
                    kb.add_comment(
                        connection, task_id, author="test", body="not durable"
                    )
                    raise ValueError("inner abort")
            assert kb.list_comments(connection, task_id) == []
            with kb.write_txn(connection, allow_nested=True):
                kb.add_comment(
                    connection, task_id, author="test", body="still not durable"
                )
            raise ValueError("outer abort")
    assert not connection.in_transaction
    assert kb.list_tasks(connection) == []
    assert connection.execute("SELECT count(*) FROM task_events").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM task_comments").fetchone()[0] == 0
    assert kb.create_task(connection, title="connection reusable")


@pytest.mark.parametrize("expected", [None, "worker ? % ' one", "other"])
def test_null_safe_binding(connection, expected):
    task_id = kb.create_task(connection, title="nullable CAS")
    with kb.write_txn(connection):
        connection.execute(
            "UPDATE tasks SET claim_lock = ? WHERE id = ?", (expected, task_id)
        )
        updated = connection.execute(
            "UPDATE tasks SET body = ? WHERE id = ? AND claim_lock IS ?",
            ("accepted", task_id, expected),
        )
        assert updated.rowcount == 1
        assert (
            connection.execute(
                "SELECT id FROM tasks WHERE claim_lock IS NOT ?", (expected,)
            ).fetchone()
            is None
        )
        literal = connection.execute(
            "SELECT 'IS ? %s 50%' AS literal, ? AS value", ("?%",)
        ).fetchone()
        assert dict(literal) == {"literal": "IS ? %s 50%", "value": "?%"}


def test_identity_ignore_replace_executemany(connection):
    task_id = kb.create_task(connection, title="full replacement")
    with kb.write_txn(connection):
        connection.executemany(
            "INSERT OR IGNORE INTO task_links (parent_id, child_id) VALUES (?, ?)",
            [(task_id, "child"), (task_id, "child")],
        )
        assert connection.execute("SELECT count(*) FROM task_links").fetchone()[0] == 1
        connection.execute(
            "INSERT INTO kanban_notify_subs "
            "(task_id, platform, chat_id, created_at, last_event_id, user_id) VALUES (?, ?, ?, ?, ?, ?)",
            (task_id, "test", "chat", 1, 99, "old-user"),
        )
        connection.execute(
            "INSERT OR REPLACE INTO kanban_notify_subs "
            "(task_id, platform, chat_id, created_at) VALUES (?, ?, ?, ?)",
            (task_id, "test", "chat", 2),
        )
    row = connection.execute("SELECT * FROM kanban_notify_subs").fetchone()
    assert row["last_event_id"] == 0
    assert row["user_id"] is None
    assert row["created_at"] == 2
    first = kb.add_comment(connection, task_id, author="test", body="first")
    with kb.write_txn(connection):
        connection.execute("DELETE FROM task_comments WHERE id = ?", (first,))
    second = kb.add_comment(connection, task_id, author="test", body="second")
    assert second > first


def test_constraint_error_rolls_back(connection):
    with pytest.raises(sqlite3.IntegrityError):
        with kb.write_txn(connection):
            kb.create_task(connection, title="rolled back")
            connection.execute(
                "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?, ?, ?, ?)",
                ("missing", None, "invalid", 1),
            )
    assert kb.list_tasks(connection) == []
    assert not connection.in_transaction


def test_catalog_dispatch_and_sqlite_file_guards(connection, tmp_path):
    dialect = dialect_for(connection)
    for table in (
        "tasks",
        "task_links",
        "task_comments",
        "task_events",
        "task_runs",
        "task_attachments",
        "kanban_notify_subs",
    ):
        assert dialect.table_exists(connection, table)
        assert dialect.table_info(connection, table)
    assert not kb._table_has_drifted(connection, "task_comments")
    with pytest.raises(ValueError, match="outside the kanban"):
        dialect.table_info(connection, "tasks); DROP TABLE tasks; --")
    with pytest.raises(ValueError, match="outside the kanban"):
        dialect.table_exists(connection, "tasks' OR '1'='1")
    kb._check_file_length_invariant(connection)
    kb._maybe_checkpoint_wal(connection, tmp_path / "absent.db")


def test_sort_dispatch_preserves_null_placement(connection):
    assigned = kb.create_task(connection, title="assigned", assignee="coder")
    unassigned = kb.create_task(connection, title="unassigned")
    assert [task.id for task in kb.list_tasks(connection, order_by="assignee")] == [
        unassigned,
        assigned,
    ]
    for order in kb.VALID_SORT_ORDERS:
        assert {task.id for task in kb.list_tasks(connection, order_by=order)} == {
            assigned,
            unassigned,
        }
    with kb.write_txn(connection):
        connection.execute(
            "UPDATE tasks SET created_at = ? WHERE id = ?", (2**33, assigned)
        )
    assert kb.get_task(connection, assigned).created_at == 2**33


def test_plain_connection_context_and_explicit_rollback(connection):
    task_id = kb.create_task(connection, title="before")
    connection.execute("BEGIN")
    assert connection.in_transaction
    connection.execute(
        "UPDATE tasks SET title = ? WHERE id = ?", ("discarded", task_id)
    )
    connection.rollback()
    assert kb.get_task(connection, task_id).title == "before"
    with connection:
        connection.execute("BEGIN")
        connection.execute(
            "UPDATE tasks SET title = ? WHERE id = ?", ("durable", task_id)
        )
    assert not connection.in_transaction
    assert kb.get_task(connection, task_id).title == "durable"


def test_postgres_env_and_reconnect(pg_dsn, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", "postgres")
    monkeypatch.setenv("HERMES_KANBAN_POSTGRES_DSN", pg_dsn)
    with kb.connect_closing() as conn:
        task_id = kb.create_task(conn, title="persistent")
    with kb.connect_closing() as conn:
        assert kb.get_task(conn, task_id).title == "persistent"
    with pytest.raises(ValueError, match="path or board"):
        kb.connect(tmp_path / "must-not-exist" / "kanban.db")
    with pytest.raises(ValueError, match="SQLite file"):
        kb.init_db(tmp_path / "must-not-exist" / "kanban.db")
    assert not (tmp_path / "must-not-exist").exists()


@pytest.mark.parametrize(
    "backend,dsn",
    [
        ("invalid", None),
        ("", None),
        ("postgres", ""),
        ("postgres", "  "),
        ("sqlite", "unused"),
    ],
)
def test_invalid_selection_fails_before_files(backend, dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "absent"))
    with pytest.raises(ValueError):
        kb.connect(backend=backend, postgres_dsn=dsn)
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("selector", ["HERMES_KANBAN_DB", "HERMES_KANBAN_BOARD"])
def test_pg_rejects_ambiguous_board_selector(selector, monkeypatch):
    monkeypatch.setenv(selector, "other-board")
    with pytest.raises(ValueError, match="path or board"):
        kb.connect(backend="postgres")


def test_explicit_sqlite_override_and_no_dsn_autoselect(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_POSTGRES_DSN", "not-a-real-dsn")
    with contextlib.closing(kb.connect(tmp_path / "default.db")) as conn:
        assert isinstance(conn, sqlite3.Connection)
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", "postgres")
    with contextlib.closing(
        kb.connect(tmp_path / "override.db", backend="sqlite")
    ) as conn:
        assert isinstance(conn, sqlite3.Connection)


def test_concurrent_claim_has_one_winner(pg_dsn):
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as setup:
        task_id = kb.create_task(setup, title="two writers")
    barrier = threading.Barrier(2)

    def claim(conn, claimer):
        barrier.wait(timeout=20)
        claimed = kb.claim_task(conn, task_id, claimer=claimer)
        return claimed.claim_lock if claimed else None

    with contextlib.ExitStack() as stack:
        writers = [
            stack.enter_context(
                contextlib.closing(kb.connect(backend="postgres", postgres_dsn=pg_dsn))
            )
            for _ in range(2)
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, writers, ("first", "second")))
    assert sum(result is not None for result in results) == 1
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        assert conn.execute("SELECT count(*) FROM task_runs").fetchone()[0] == 1
        assert (
            conn.execute(
                "SELECT count(*) FROM task_events WHERE kind = 'claimed'"
            ).fetchone()[0]
            == 1
        )


def test_pg_initialization_is_atomic(pg_dsn):
    with contextlib.closing(
        KanbanPostgresConnection(psycopg.connect(pg_dsn, autocommit=True))
    ) as conn:
        with pytest.raises(psycopg.errors.UndefinedTable):
            conn.kanban_dialect.initialize(
                conn,
                kb.SCHEMA_SQL + "SELECT * FROM absent_table;",
                kb._migrate_add_optional_columns,
            )
        assert not conn.kanban_dialect.table_exists(conn, "tasks")
        assert not conn.in_transaction


def test_pg_no_reconnect_after_closed_handle(pg_dsn):
    conn = kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    conn.close()
    with pytest.raises(psycopg.OperationalError):
        kb.create_task(conn, title="must not replay")


def test_pg_rejects_unmapped_dialects(pg_dsn):
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        with pytest.raises(psycopg.errors.SyntaxError):
            conn.execute("PRAGMA unknown_setting")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT OR IGNORE INTO task_comments (task_id, author, body, created_at) "
                "VALUES (?, ?, ?, ?)",
                ("task", None, "must fail", 1),
            )
        assert not conn.in_transaction


def test_canonical_schema_columns_match_sqlite(pg_dsn, tmp_path):
    with contextlib.ExitStack() as stack:
        sqlite_conn = stack.enter_context(
            contextlib.closing(kb.connect(tmp_path / "canonical.db"))
        )
        pg_conn = stack.enter_context(
            contextlib.closing(kb.connect(backend="postgres", postgres_dsn=pg_dsn))
        )
        tables = [
            row[0]
            for row in sqlite_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'"
            )
        ]
        for table in tables:
            sqlite_info = dialect_for(sqlite_conn).table_info(sqlite_conn, table)
            pg_info = dialect_for(pg_conn).table_info(pg_conn, table)
            assert [(row["name"], row["type"], bool(row["pk"])) for row in pg_info] == [
                (row["name"], row["type"], bool(row["pk"])) for row in sqlite_info
            ]


def test_pg_additive_migration_and_running_backfill(pg_dsn):
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        task_id = kb.create_task(conn, title="legacy")
        with kb.write_txn(conn):
            conn.execute("ALTER TABLE tasks DROP COLUMN max_retries")
            conn.execute("ALTER TABLE kanban_notify_subs DROP COLUMN delivery_mode")
            conn.execute(
                "UPDATE tasks SET status = 'running', claim_lock = ? WHERE id = ?",
                ("legacy", task_id),
            )
            conn.execute(
                "INSERT INTO kanban_notify_subs (task_id, platform, chat_id, created_at) VALUES (?, ?, ?, ?)",
                (task_id, "gateway", "chat", 1),
            )
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        assert kb.get_task(conn, task_id).current_run_id is not None
        assert kb.get_task(conn, task_id).max_retries is None
        assert (
            conn.execute("SELECT delivery_mode FROM kanban_notify_subs").fetchone()[0]
            == "notify+wake"
        )
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        assert conn.execute("SELECT count(*) FROM task_runs").fetchone()[0] == 1


def test_pg_drift_fails_without_destructive_rebuild(pg_dsn):
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as conn:
        task_id = kb.create_task(conn, title="preserve me")
        kb.add_comment(conn, task_id, author="test", body="do not drop")
        conn.execute("ALTER TABLE task_comments ALTER COLUMN id DROP IDENTITY")
    with pytest.raises(RuntimeError, match="schema drift"):
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    with psycopg.connect(pg_dsn) as raw:
        assert (
            raw.execute("SELECT body FROM task_comments").fetchone()[0] == "do not drop"
        )


def test_pg_borrowed_transaction_keeps_outer_ownership(pg_dsn):
    with contextlib.closing(
        kb.connect(backend="postgres", postgres_dsn=pg_dsn)
    ) as owner:
        with pytest.raises(ValueError, match="owner abort"):
            with owner.raw.transaction():
                borrowed = KanbanPostgresConnection(owner.raw)
                kb.create_task(borrowed, title="borrowed")
                assert owner.in_transaction
                with pytest.raises(RuntimeError, match="own transaction"):
                    borrowed.kanban_dialect.initialize(
                        borrowed, kb.SCHEMA_SQL, kb._migrate_add_optional_columns
                    )
                raise ValueError("owner abort")
        assert kb.list_tasks(owner) == []
