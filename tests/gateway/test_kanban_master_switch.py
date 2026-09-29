"""levos v3 (t_fb9c7b9e) — ``kanban.enabled`` / ``HERMES_KANBAN_ENABLED`` turns the built-in kanban off.

Off, no runtime entry point opens the kanban store: the gateway spawns neither kanban watcher and
their ticks return at once, the tui session poller skips its kanban poll, the ``kanban_*`` tools
are hidden and ``/kanban`` only says kanban is off — even when a ``kanban.db`` with subscriptions
already sits in the kanban home (the live v3 PVC case). On (the default) the same entry points
reach the store exactly as before; each off-test runs its on-twin so the recorders are proven to
see the store access they guard against.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_notify as kbn
from hermes_cli.kanban_switch import kanban_disabled_reason, kanban_enabled
# Imported up front: their import-time state.db setup (the process registry restores delegation
# completions) is not the poller and not kanban.
from tools import process_registry as _process_registry  # noqa: F401
from tui_gateway import server

SESSION_KEY = "tui-master-switch-session"
# The store's entry points (module, attribute); each is wrapped to record its calls.
_STORE_ENTRY_POINTS = (
    (kb, "kanban_db_path"), (kb, "list_boards"), (kb, "kanban_home"), (kbc, "connect"),
    (kbn, "count_notify_subs"),
)


def _write_config(home, text: str) -> None:
    (home / "config.yaml").write_text(text, encoding="utf-8")


@pytest.fixture
def home(tmp_path, monkeypatch):
    """HERMES_HOME == HERMES_KANBAN_HOME holding a live ``kanban.db``: one task with a tui and a
    telegram subscription and a pending terminal event, like a v3 PVC from before the switch."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    for key in ("HERMES_KANBAN_BACKEND", "HERMES_KANBAN_DB", "HERMES_KANBAN_ENABLED"):
        monkeypatch.delenv(key, raising=False)
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="pre-switch task", assignee="worker")
        kbn.add_notify_sub(conn, task_id=tid, platform="tui", chat_id=SESSION_KEY)
        kbn.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="chat-1")
        kb.complete_task(conn, tid, summary="done before the switch")
    return home


@pytest.fixture
def kanban_off(home):
    _write_config(home, "kanban:\n  enabled: false\n")
    return home


@pytest.fixture
def store_calls(monkeypatch):
    """Every ``sqlite3.connect`` and kanban store entry-point call, by name (calls pass through)."""
    calls: list[str] = []
    real_sqlite_connect = sqlite3.connect

    def sqlite_connect(*args, **kwargs):
        calls.append(f"sqlite3.connect({args[0] if args else kwargs.get('database')})")
        return real_sqlite_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", sqlite_connect)
    for module, name in _STORE_ENTRY_POINTS:
        real = getattr(module, name)

        def recorder(*args, _real=real, _name=f"{module.__name__}.{name}", **kwargs):
            calls.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(module, name, recorder)
    return calls


def _db_state(home) -> tuple:
    db = home / "kanban.db"
    stat = db.stat()
    return stat.st_mtime_ns, stat.st_size, sorted(p.name for p in home.iterdir() if p.name.startswith("kanban"))


# --------------------------------------------------------------------------
# The switch itself (④ env overrides config)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(("config", "env", "reason"), [
    ("", None, None),
    ("kanban:\n  enabled: true\n", None, None),
    ("kanban:\n  enabled: false\n", None, "config kanban.enabled=false"),
    ("kanban:\n  enabled: 'off'\n", None, "config kanban.enabled=false"),
    ("", "0", "HERMES_KANBAN_ENABLED env"),
    ("kanban:\n  enabled: true\n", "off", "HERMES_KANBAN_ENABLED env"),
    ("kanban:\n  enabled: false\n", "1", None),
    ("kanban:\n  enabled: false\n", "yes", None),
    ("kanban:\n  enabled: false\n", "", "config kanban.enabled=false"),
])
def test_env_overrides_config(home, monkeypatch, config, env, reason):
    _write_config(home, config)
    if env is not None:
        monkeypatch.setenv("HERMES_KANBAN_ENABLED", env)
    assert kanban_disabled_reason() == reason
    assert kanban_enabled() is (reason is None)


def test_default_config_keeps_kanban_on():
    from hermes_cli.config_defaults import DEFAULT_CONFIG

    assert DEFAULT_CONFIG["kanban"]["enabled"] is True


# --------------------------------------------------------------------------
# ① gateway + tui poller open nothing when off (existing kanban.db)
# --------------------------------------------------------------------------


def _runner():
    from gateway.config import Platform
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: MagicMock()}
    runner._kanban_sub_fail_counts = {}
    runner._kanban_notifier_profile = "default"
    runner._failed_platforms = []
    spawned: list[str] = []
    runner._spawn_supervised = lambda _factory, name, **_kw: spawned.append(name)
    runner._spawn_reconnect_watcher = lambda: spawned.append("reconnect_watcher")
    runner._turn_leases_on_postgres = lambda: False
    runner._scale_to_zero_should_arm = lambda: False
    runner._log_scale_to_zero_not_armed_reason = lambda: None
    return runner, spawned


def _gateway_boot_and_ticks(runner, monkeypatch) -> list:
    """Gateway start (watcher spawn), one notifier watcher run and one tick, one dispatcher run."""
    from gateway.kanban_watchers_notifier import _notifier_collect

    runner._start_spawn_background_watchers()
    sleeps: list = []

    async def one_tick_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 2:  # the initial delay, then the first between-ticks sleep
            runner._running = False

    monkeypatch.setattr(asyncio, "sleep", one_tick_sleep)
    asyncio.run(runner._kanban_notifier_watcher(interval=1))
    runner._running, sleeps[:] = True, []
    asyncio.run(runner._kanban_dispatcher_watcher())
    runner._release_kanban_dispatcher_lock()
    return _notifier_collect(runner, kb, notifier_profile="default", gc_due=True, gc_retention_days=30)


def test_gateway_off_spawns_no_kanban_watcher_and_opens_no_store(kanban_off, store_calls, monkeypatch, caplog):
    before = _db_state(kanban_off)
    runner, spawned = _runner()
    with caplog.at_level(logging.INFO):
        deliveries = _gateway_boot_and_ticks(runner, monkeypatch)

    assert deliveries == []
    assert not [name for name in spawned if name.startswith("kanban")], spawned
    assert "session_housekeeping_watcher" in spawned and "handoff_watcher" in spawned
    assert "kanban: disabled via config kanban.enabled=false" in caplog.text
    assert store_calls == []
    assert _db_state(kanban_off) == before
    assert not (kanban_off / "kanban" / ".dispatcher.lock").exists()


def test_gateway_on_spawns_both_watchers_and_polls_the_store(home, store_calls, monkeypatch):
    runner, spawned = _runner()
    _gateway_boot_and_ticks(runner, monkeypatch)

    assert {"kanban_notifier_watcher", "kanban_dispatcher_watcher"} <= set(spawned)
    assert "hermes_cli.kanban_db_notify.count_notify_subs" in store_calls
    assert any(call.startswith("sqlite3.connect(") for call in store_calls)


_OTHER_POLLS = ("_poll_bot_live_delivery_once", "_maybe_fire_tui_loop_tick", "_maybe_fire_tui_heartbeat_tick")


def _run_tui_poller(monkeypatch) -> list:
    """One tui session notification poller iteration; returns the kanban-poll attempts. The
    iteration's other polls (bot live delivery, /loop, /heartbeat — they read state.db, not
    kanban) are replaced by markers so the recorder sees only the kanban side; the off-test
    asserts they still run."""
    attempts: list = []
    for other in _OTHER_POLLS:
        monkeypatch.setattr(server, other,
                            lambda sid, session, _name=other: session.setdefault("_other_polls", set()).add(_name))
    real_collect = server._collect_kanban_notifications

    def collect(session):
        attempts.append(session.get("session_key"))
        return real_collect(session)

    monkeypatch.setattr(server, "_collect_kanban_notifications", collect)
    monkeypatch.setattr(server, "_emit", lambda *a, **k: None)
    monkeypatch.setattr(server, "_notif_submit", lambda *a, **k: None)
    session = {"session_key": SESSION_KEY, "history_lock": threading.Lock(), "running": False}
    stop = threading.Event()
    thread = threading.Thread(target=server._notification_poller_loop, args=(stop, "sid-switch", session),
                              daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not attempts and time.monotonic() < deadline:
        time.sleep(0.05)
    stop.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert session["_other_polls"] == set(_OTHER_POLLS)
    return attempts


def test_tui_poller_off_skips_the_kanban_poll(kanban_off, store_calls, monkeypatch):
    before = _db_state(kanban_off)
    assert _run_tui_poller(monkeypatch) == [SESSION_KEY]  # the tick ran, the kanban poll was skipped
    assert store_calls == []
    assert _db_state(kanban_off) == before


def test_tui_poller_on_polls_the_subscribed_board(home, store_calls, monkeypatch):
    assert _run_tui_poller(monkeypatch) == [SESSION_KEY]
    assert "hermes_cli.kanban_db_notify.count_notify_subs" in store_calls
    assert "hermes_cli.kanban_db_connect.connect" in store_calls


# --------------------------------------------------------------------------
# ② tools and /kanban
# --------------------------------------------------------------------------


def _kanban_tool_names() -> set:
    import tools.kanban_tools  # noqa: F401 - registers the tools
    from tools.registry import invalidate_check_fn_cache, registry
    from toolsets import resolve_toolset

    invalidate_check_fn_cache()
    schema = registry.get_definitions(set(resolve_toolset("kanban")), quiet=True)
    return {s["function"]["name"] for s in schema if s.get("function", {}).get("name", "").startswith("kanban_")}


@pytest.mark.parametrize("worker", [False, True], ids=["orchestrator-toolset", "dispatcher-worker"])
def test_kanban_tools_hidden_when_off(home, monkeypatch, worker):
    if worker:
        monkeypatch.setenv("HERMES_KANBAN_TASK", "t_switch")
    _write_config(home, "toolsets: [kanban]\n")
    assert _kanban_tool_names(), "control: the kanban toolset exposes its tools when on"
    _write_config(home, "toolsets: [kanban]\nkanban:\n  enabled: false\n")
    assert _kanban_tool_names() == set()
    monkeypatch.setenv("HERMES_KANBAN_ENABLED", "1")
    assert _kanban_tool_names(), "env re-enables over config"


def test_worker_bridges_do_nothing_when_off(kanban_off, store_calls, monkeypatch):
    from tools import kanban_tools

    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_switch")
    monkeypatch.setattr(kanban_tools, "_auto_heartbeat_last_attempt", 0.0)
    monkeypatch.setattr(kanban_tools, "_comment_poll_last_attempt", 0.0)
    assert kanban_tools.heartbeat_current_worker_from_env() is False
    assert kanban_tools.inject_new_comments_from_env(SimpleNamespace(steer=lambda _note: True)) is False
    assert store_calls == []


def _kanban_event(text: str):
    from gateway.config import Platform

    source = SimpleNamespace(platform=Platform.TELEGRAM, chat_id="chat-1", chat_type="dm", thread_id=None,
                             user_id="u1")
    return SimpleNamespace(text=text, source=source, message_id="1", reply_to_message_id=None)


def test_gateway_slash_kanban_off_only_says_so(kanban_off, store_calls, monkeypatch):
    from agent.i18n import t
    from gateway.run import GatewayRunner
    import hermes_cli.kanban as kanban_cli

    ran: list = []
    monkeypatch.setattr(kanban_cli, "run_slash", lambda text: ran.append(text) or "")
    runner = object.__new__(GatewayRunner)
    out = asyncio.run(GatewayRunner._handle_kanban_command(runner, _kanban_event('/kanban create "x"')))

    assert out == t("gateway.kanban.disabled")
    assert "HERMES_KANBAN_ENABLED" in out
    assert ran == [] and store_calls == []


def test_gateway_slash_kanban_on_runs_the_cli(home, monkeypatch):
    from gateway.run import GatewayRunner
    import hermes_cli.kanban as kanban_cli

    ran: list = []
    monkeypatch.setattr(kanban_cli, "run_slash", lambda text: ran.append(text) or "listing")
    runner = object.__new__(GatewayRunner)
    out = asyncio.run(GatewayRunner._handle_kanban_command(runner, _kanban_event("/kanban list")))

    assert out == "listing" and ran == ["list"]


def test_cli_and_tui_slash_kanban_off_only_says_so(kanban_off, store_calls, monkeypatch, capsys):
    from agent.i18n import t
    from hermes_cli.cli_commands_mixin import CLICommandsMixin
    import hermes_cli.kanban as kanban_cli

    ran: list = []
    monkeypatch.setattr(kanban_cli, "run_slash", lambda text: ran.append(text) or "")
    CLICommandsMixin._handle_kanban_command(object(), "/kanban list")

    assert capsys.readouterr().out.strip() == t("gateway.kanban.disabled")
    assert ran == [] and store_calls == []


def test_chat_launch_pins_no_board_when_off(kanban_off, store_calls, monkeypatch):
    from hermes_cli.main_tui_launch import _pin_kanban_board_env

    monkeypatch.delenv("HERMES_KANBAN_BOARD", raising=False)
    _pin_kanban_board_env()

    assert "HERMES_KANBAN_BOARD" not in os.environ
    assert store_calls == []
