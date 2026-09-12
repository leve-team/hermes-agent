"""Real-engine regressions for the 0057 kanban SQL audit."""

from __future__ import annotations

import contextlib
import json
import asyncio
import logging
import os
import threading
import sqlite3
import argparse
import signal
from pathlib import Path

import psycopg
import pytest

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_persistence import dialect_for, set_board_dsn_resolver
from tests.hermes_cli.persistence_pg_support import pg_dsn as pg_dsn, pg_server as pg_server


@pytest.fixture
def stores(pg_dsn, tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / "profiles" / "worker").mkdir(parents=True)
    (home / "config.yaml").write_text(json.dumps({"kanban": {"review_dispatch": True}}))
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_BOARD", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_BACKEND", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_POSTGRES_DSN", raising=False)
    with contextlib.ExitStack() as stack:
        yield {
            "sqlite": stack.enter_context(contextlib.closing(kb.connect(home / "kanban.db"))),
            "postgres": stack.enter_context(contextlib.closing(kb.connect(
                backend="postgres", postgres_dsn=pg_dsn,
            ))),
        }


def insert_task(conn, task_id, *, priority=0, status="ready", title=None, assignee="worker", completed_at=None):
    conn.execute(
        "INSERT INTO tasks (id, title, status, assignee, priority, created_at, completed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (task_id, title or task_id, status, assignee, priority, 100, completed_at),
    )


@pytest.mark.parametrize("lane", ["ready", "review"])
def test_dispatch_null_priority_selects_same_card(stores, lane):
    selections = {}
    for backend, conn in stores.items():
        insert_task(conn, "null-priority", priority=None, status=lane)
        insert_task(conn, "high-priority", priority=100, status=lane)
        conn.commit()
        spawned = []

        def spawn(task, workspace, board=None):
            spawned.append(task.id)
            return None

        result = kb.dispatch_once(conn, spawn_fn=spawn, max_spawn=1, reconcile_orphans=False)
        selections[backend] = [entry[0] for entry in result.spawned]
        print(f"DISPATCH: backend={backend} lane={lane} selected={selections[backend]} callback={spawned}")
        assert spawned == selections[backend] == ["high-priority"]
        assert conn.execute("SELECT priority FROM tasks WHERE id = ?", ("null-priority",)).fetchone()[0] is None
        assert kb.get_task(conn, "null-priority").status == lane
    assert selections["sqlite"] == selections["postgres"]


@pytest.mark.parametrize("limit", [-3, -1, 0, 1, 2, None])
def test_list_limit_matches_sqlite(stores, limit):
    expected = ["high", "low"] if not limit or limit < 0 else ["high", "low"][:limit]
    for backend, conn in stores.items():
        insert_task(conn, "low", priority=None)
        insert_task(conn, "high", priority=100)
        actual = [task.id for task in kb.list_tasks(conn, limit=limit)]
        print(f"LIST LIMIT: backend={backend} limit={limit} ids={actual}")
        assert actual == expected


def test_parent_result_null_time_matches_sqlite(stores):
    for backend, conn in stores.items():
        insert_task(conn, "child")
        for task_id, completed_at in (("no-time", None), ("has-time", 100)):
            insert_task(conn, task_id, status="done", completed_at=completed_at)
            conn.execute("INSERT INTO task_links (parent_id, child_id) VALUES (?, ?)", (task_id, "child"))
        actual = [item[0] for item in kb.parent_results(conn, "child")]
        print(f"PARENT RESULTS: backend={backend} ids={actual}")
        assert actual == ["no-time", "has-time"]


@pytest.mark.parametrize("sort", ["title", "assignee", "created", "created-desc"])
def test_text_sort_matches_binary_sqlite(stores, sort):
    values = ["ä", "a", "Z", "z", "A", "!", "한글"]
    results = {}
    for backend, conn in stores.items():
        for value in values:
            insert_task(conn, value, title=value, assignee=value)
        results[backend] = [task.id for task in kb.list_tasks(conn, order_by=sort)]
        assert results[backend] == sorted(values, reverse=sort == "created-desc")
        expression = dialect_for(conn).order_by(kb.VALID_SORT_ORDERS[sort])
        if backend == "postgres":
            assert 'COLLATE "C"' in expression
        print(f"TEXT ORDER: backend={backend} sort={sort} ids={results[backend]}")
    assert results["sqlite"] == results["postgres"]


def test_graph_text_order_and_invalidation_match(stores):
    expected = ["A", "Z", "a", "ä"]
    for backend, conn in stores.items():
        insert_task(conn, "root", status="done")
        insert_task(conn, "leaf")
        for task_id in reversed(expected):
            insert_task(conn, task_id, status="done")
            kb.link_tasks(conn, "root", task_id)
            kb.link_tasks(conn, task_id, "leaf")
        assert kb.parent_ids(conn, "leaf") == expected
        assert kb.child_ids(conn, "root") == expected
        graph = kb.task_graph_contexts(conn, ["root", "leaf"])
        assert [row["id"] for row in graph["leaf"]["parents"]] == expected
        assert [row["id"] for row in graph["root"]["children"]] == expected
        context = kb.build_worker_context(conn, "leaf")
        assert [context.index(f"### {task_id}") for task_id in expected] == sorted(
            context.index(f"### {task_id}") for task_id in expected
        )
        invalidated = kb.invalidate_descendants_for_parent_reopen(conn, "root", author="test")
        actual = [item["id"] for item in invalidated["invalidated"]]
        print(f"GRAPH: backend={backend} invalidated={actual}")
        assert actual == sorted(expected + ["leaf"])


@pytest.mark.parametrize("count", [2, 6])
def test_role_history_null_time_and_limit_match(stores, count):
    contexts = {}
    for backend, conn in stores.items():
        insert_task(conn, "target")
        for number in range(count):
            task_id = f"history-{number}"
            insert_task(conn, task_id, status="done")
            conn.execute(
                "INSERT INTO task_runs (task_id, profile, status, started_at, ended_at, outcome, summary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, "worker", "done", 1, None if number == 0 else number, "completed", task_id),
            )
        contexts[backend] = kb.build_worker_context(conn, "target")
        expected = list(reversed(range(1, count)))[:5]
        if count < 5:
            expected.append(0)
        actual = [line.split(" — ")[0][2:] for line in contexts[backend].splitlines() if line.startswith("- history-")]
        print(f"ROLE HISTORY: backend={backend} ids={actual}")
        assert actual == [f"history-{number}" for number in expected]
    assert contexts["sqlite"] == contexts["postgres"]


def test_closed_sort_rejects_unknown_sql(stores):
    for expression in ("title; DROP TABLE tasks", "random() ASC", "unknown ASC", "id ASC, title COLLATE x"):
        with pytest.raises(ValueError, match="Unknown kanban sort"):
            dialect_for(stores["postgres"]).order_by(expression)


@pytest.fixture
def routed_store(request, pg_dsn, tmp_path, monkeypatch):
    backend = request.param
    home = tmp_path / "routed"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", backend)
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_BOARD", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_POSTGRES_DSN", raising=False)
    if backend == "postgres":
        from psycopg.conninfo import make_conninfo

        with psycopg.connect(pg_dsn, autocommit=True) as raw:
            raw.execute("DROP SCHEMA IF EXISTS svc_kanban_default CASCADE")
            raw.execute("CREATE SCHEMA svc_kanban_default")
        set_board_dsn_resolver(
            lambda token: make_conninfo(pg_dsn, options=f"-c search_path=svc_kanban_{token}"),
            boards=("default",),
        )
    try:
        with contextlib.closing(kb.connect(board="default")):
            yield backend, home
    finally:
        set_board_dsn_resolver(None)


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
@pytest.mark.parametrize("failure", ["inventory", "connect"])
@pytest.mark.asyncio
async def test_gateway_health_error_is_not_idle(routed_store, monkeypatch, caplog, failure):
    from gateway.kanban_watchers import GatewayKanbanWatchersMixin
    from hermes_cli.plugins import get_plugin_manager

    backend, home = routed_store
    (home / "config.yaml").write_text(json.dumps({"kanban": {
        "dispatch_in_gateway": True, "auto_decompose": False,
    }}))
    runner = GatewayKanbanWatchersMixin()
    runner._running = True
    ticks = []

    def after_tick(**payload):
        ticks.append(payload["board"])
        if failure == "inventory":
            os.environ["HERMES_KANBAN_BACKEND"] = "invalid"
        else:
            os.environ["HERMES_KANBAN_DB"] = str(home)
        runner._running = False

    monkeypatch.setitem(get_plugin_manager()._hooks, "on_kanban_dispatch_tick", [after_tick])
    try:
        with caplog.at_level(logging.ERROR):
            await asyncio.wait_for(runner._kanban_dispatcher_watcher(), timeout=30)
    finally:
        runner._running = False
        runner._release_kanban_dispatcher_lock()
    assert ticks == ["default"]
    assert "kanban dispatcher: unexpected watcher error" in caplog.text
    print(f"HEALTH ERROR: backend={backend} failure={failure} outcome=logged-unknown")


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
def test_daemon_callback_error_is_reported(routed_store, capsys):
    stop = threading.Event()

    def on_tick(result):
        stop.set()
        raise RuntimeError("health probe unavailable")

    kb.run_daemon(stop_event=stop, interval=0, on_tick=on_tick)
    assert "health probe unavailable" in capsys.readouterr().err
    print(f"DAEMON ERROR: backend={routed_store[0]} outcome=reported")


class StopOnLog(logging.Handler):
    def __init__(self, runner, message):
        super().__init__()
        self.runner = runner
        self.message = message

    def emit(self, record):
        if self.message in record.getMessage():
            self.runner._running = False


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
@pytest.mark.parametrize("failure", ["inventory", "query"])
@pytest.mark.asyncio
async def test_auto_decompose_error_is_not_empty(routed_store, monkeypatch, caplog, failure):
    from gateway.kanban_watchers import GatewayKanbanWatchersMixin

    backend, home = routed_store
    (home / "config.yaml").write_text(json.dumps({"kanban": {
        "dispatch_in_gateway": True, "auto_decompose": True,
    }}))
    if failure == "inventory":
        monkeypatch.setenv("HERMES_KANBAN_BACKEND", "invalid")
    else:
        with contextlib.closing(kb.connect()) as conn:
            conn.execute("ALTER TABLE tasks RENAME COLUMN priority TO broken_priority")
            conn.commit()
    runner = GatewayKanbanWatchersMixin()
    runner._running = True
    logger = logging.getLogger("gateway.run")
    handler = StopOnLog(runner, "kanban dispatcher: unexpected watcher error")
    logger.addHandler(handler)
    try:
        with caplog.at_level(logging.ERROR):
            await asyncio.wait_for(runner._kanban_dispatcher_watcher(), timeout=30)
    finally:
        runner._running = False
        runner._release_kanban_dispatcher_lock()
        logger.removeHandler(handler)
    assert handler.message in caplog.text
    assert "kanban dispatcher: tick failed on board" not in caplog.text
    print(f"DECOMPOSE ERROR: backend={backend} failure={failure} outcome=logged-unknown")


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
@pytest.mark.asyncio
async def test_notifier_inventory_error_is_reported(routed_store, monkeypatch, caplog):
    from gateway.config import Platform
    from gateway.kanban_watchers import GatewayKanbanWatchersMixin

    backend, _ = routed_store
    runner = GatewayKanbanWatchersMixin()
    runner._running = True
    runner._kanban_notifier_profile = "default"
    runner.adapters = {Platform.TELEGRAM: object()}
    monkeypatch.setenv("HERMES_KANBAN_BACKEND", "invalid")
    logger = logging.getLogger("gateway.run")
    handler = StopOnLog(runner, "kanban notifier tick failed")
    logger.addHandler(handler)
    try:
        with caplog.at_level(logging.WARNING):
            await asyncio.wait_for(runner._kanban_notifier_watcher(interval=1), timeout=30)
    finally:
        runner._running = False
        logger.removeHandler(handler)
    assert handler.message in caplog.text
    print(f"NOTIFIER ERROR: backend={backend} outcome=logged-unknown")


def test_default_assignment_storage_error_is_not_unassigned(stores):
    for backend, conn in stores.items():
        insert_task(conn, "unassigned", assignee=None)
        conn.execute("DROP TABLE task_events")
        conn.commit()
        with pytest.raises((sqlite3.DatabaseError, psycopg.Error)):
            kb.dispatch_once(conn, default_assignee="worker", max_spawn=1, reconcile_orphans=False)
        assert kb.get_task(conn, "unassigned").assignee is None
        print(f"ASSIGN ERROR: backend={backend} outcome=raised-and-rolled-back")


def test_reassign_only_maps_claim_refusal_to_false(stores):
    for backend, conn in stores.items():
        insert_task(conn, "claim-me")
        conn.commit()
        assert kb.claim_task(conn, "claim-me", claimer="worker") is not None
        assert kb.reassign_task(conn, "claim-me", "other") is False
        with kb.write_txn(conn):
            message = "cannot nest" if backend == "postgres" else "already inside a transaction"
            with pytest.raises(RuntimeError, match=message):
                kb.reassign_task(conn, "claim-me", "other")
        assert kb.get_task(conn, "claim-me").assignee == "worker"
        print(f"REASSIGN: backend={backend} claim=False transaction-error=raised")


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
def test_cli_list_inventory_error_is_not_empty(routed_store, monkeypatch, pg_dsn):
    from hermes_cli import kanban as cli

    backend, _ = routed_store
    if backend == "postgres":
        from psycopg.conninfo import make_conninfo

        set_board_dsn_resolver(None)
        monkeypatch.setenv("HERMES_KANBAN_POSTGRES_DSN", make_conninfo(
            pg_dsn, options="-c search_path=svc_kanban_default",
        ))
        error = ValueError
    else:
        root = kb.boards_root()
        root.mkdir(parents=True, exist_ok=True)
        original = Path.iterdir

        def unavailable(path):
            if path == root:
                raise OSError("inventory unavailable")
            return original(path)

        monkeypatch.setattr(Path, "iterdir", unavailable)
        error = OSError
    parser = argparse.ArgumentParser()
    cli.build_parser(parser.add_subparsers(dest="command"))
    args = parser.parse_args(["kanban", "list"])
    with pytest.raises(error):
        cli._cmd_list(args)
    print(f"CLI LIST ERROR: backend={backend} outcome=raised")


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
def test_cli_daemon_health_error_is_not_idle(routed_store, monkeypatch, capsys):
    from hermes_cli import kanban as cli
    from hermes_cli.plugins import get_plugin_manager

    backend, home = routed_store
    handlers = {}
    monkeypatch.setattr(signal, "signal", lambda number, handler: handlers.setdefault(number, handler))

    def after_tick(**payload):
        os.environ["HERMES_KANBAN_DB"] = str(home)
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setitem(get_plugin_manager()._hooks, "on_kanban_dispatch_tick", [after_tick])
    parser = argparse.ArgumentParser()
    cli.build_parser(parser.add_subparsers(dest="command"))
    args = parser.parse_args(["kanban", "daemon", "--force", "--interval", "1"])
    assert cli._cmd_daemon(args) == 0
    captured = capsys.readouterr()
    assert "Traceback" in captured.err
    assert "_ready_queue_nonempty" in captured.err
    print(f"CLI HEALTH ERROR: backend={backend} outcome=reported")


def test_status_order_matches_sqlite(stores):
    for backend, conn in stores.items():
        for status in kb.VALID_STATUSES:
            insert_task(conn, status, status=status)
        actual = [task.status for task in kb.list_tasks(conn, order_by="status", include_archived=True)]
        assert actual == sorted(kb.VALID_STATUSES)
        print(f"STATUS ORDER: backend={backend} statuses={actual}")


def test_inventory_keeps_multiline_sql_and_handlers():
    from scripts.kanban_dialect_audit import inventory

    report = inventory("sample.py", '''
def probe(conn, limit):
    try:
        query = "SELECT * " "FROM tasks ORDER BY priority DESC"
        query += f" LIMIT {limit}"
        return conn.execute(query).fetchall()
    except Exception:
        return []
''')
    assert [item["kind"] for item in report["nodes"]] == ["sql", "except"]
    assert report["nodes"][0]["source"] == "conn.execute(query)"
    assert "LIMIT {limit}" in report["nodes"][1]["protected"]
    assert report["grep"][0]["line"] == 4


@pytest.mark.parametrize("routed_store", ["sqlite", "postgres"], indirect=True)
def test_notify_platform_case_folding_matches_sqlite(routed_store):
    backend, _ = routed_store
    with contextlib.closing(kb.connect()) as conn:
        task_id = kb.create_task(conn, title="platform casing")
        for platform in ("TUI", "ä", "Ä", "İ", "i"):
            kb.add_notify_sub(conn, task_id=task_id, platform=platform, chat_id=platform)
    actual = {platform: kb.count_notify_subs(platform=platform) for platform in ("tui", "ä", "Ä", "İ", "i")}
    print(f"PLATFORM CASE: backend={backend} counts={actual}")
    assert actual == {"tui": 1, "ä": 1, "Ä": 1, "İ": 1, "i": 1}
