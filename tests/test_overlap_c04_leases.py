"""levos 0063 — row ownership across overlapping pods is a PostgreSQL lease.

``delivery_obligations`` (gateway final responses owed to a platform) and
``async_delegations`` (background subagents) already live in the profile's
PostgreSQL store on ``HERMES_STATE_BACKEND=authority`` (0051), but their owner
was ``owner_pid`` + process start time and "owner dead" was decided by probing
the local kernel. During a rolling deploy the new pod cannot see the old pod's
pids, so it took over the old pod's live rows: it re-sent owed replies
(B-F07) and closed running delegations as ``unknown`` the moment it booted
(B-F08). Now the owner is a per-process instance id with a lease the owner
renews; a row is taken over only after that lease ran out.

A "pod" here is a spawned process with its own ``HERMES_HOME`` (nothing on
disk is shared, only PostgreSQL) whose pid probes see only its own process
table: separate PID namespaces cannot be built on the test worker (unprivileged
``unshare --pid`` works, but remounting ``/proc`` is refused, so ``/proc`` and
psutil would still show the peer). The parent orchestrates and only reads
PostgreSQL. Runs on the fork's ephemeral PostgreSQL (``initdb``/``pg_ctl`` on
PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors, never skips).
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import queue
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from gateway import delivery_ledger
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn
from tools import async_delegation

_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_PROFILE",
)
# Short enough that a test outlives several leases: a renewing owner keeps
# its rows across more than two lease lengths, a dead one loses them.
LEASE, RENEW = 2.0, 0.5
_SPAWN = multiprocessing.get_context("spawn")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    yield
    delivery_ledger._stop_lease_renewer()
    async_delegation._reset_for_tests()


@pytest.fixture
def authority(monkeypatch, postgres_dsn):
    """Authority profile whose core session schema exists (as in production)."""
    from hermes_state import SessionDB

    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS delivery_obligations, async_delegations")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    SessionDB(read_only=False).close()
    return postgres_dsn


def _pods(tmp_path, *names):
    homes = [tmp_path / name for name in names]
    for home in homes:
        home.mkdir()
    return [str(home) for home in homes]


def _sqlite_files(home) -> list:
    """SQLite files a pod wrote (the ownership rows must be in PostgreSQL)."""
    return sorted(str(p.relative_to(home)) for p in Path(home).rglob("*.db*"))


def _rows(dsn, sql, params=()):
    with psycopg.connect(dsn) as raw:
        return raw.execute(sql, params).fetchall()


# --------------------------------------------------------------------------
# Pod processes. Children are spawned, so they inherit the authority env.
# --------------------------------------------------------------------------


def _enter_pod(home):
    """Become a pod: own HERMES_HOME, pid probes that see only this process."""
    os.environ["HERMES_HOME"] = home
    import gateway.status as status

    me = os.getpid()
    start_time = status.get_process_start_time
    status._pid_exists = lambda pid: int(pid) == me
    status.get_process_start_time = lambda pid: (
        start_time(pid) if int(pid) == me else None
    )
    delivery_ledger.LEASE_SECONDS, delivery_ledger.LEASE_RENEW_SECONDS = LEASE, RENEW
    async_delegation.LEASE_SECONDS = LEASE
    async_delegation.LEASE_RENEW_SECONDS = RENEW


def _child(target, events, *args):
    try:
        target(*args, events)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


def _spawn(target, events, *args):
    process = _SPAWN.Process(target=_child, args=(target, events, *args))
    process.start()
    return process


def _await(events, kind, *, timeout=90):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out waiting for {kind!r}"
        event, value = events.get(timeout=remaining)
        assert event != "error", f"a pod process failed: {value}"
        if event == kind:
            return value


def _stop(processes):
    for process in processes:
        if process.is_alive():
            process.kill()
        process.join(10)


def _old_pod_owes_replies(home, events):
    """The old pod has two final replies in flight: one not yet sent, one
    mid-send. It stays up (renewing) until the parent kills it."""
    _enter_pod(home)
    for obligation_id in ("owed-pending", "owed-attempting"):
        delivery_ledger.record_obligation(
            obligation_id=obligation_id,
            session_key=f"agent:main:telegram:dm:{obligation_id}",
            platform="telegram",
            chat_id="42",
            thread_id=None,
            content=f"answer for {obligation_id}",
        )
    delivery_ledger.mark_attempting("owed-attempting")
    events.put(("recorded", os.getpid()))
    time.sleep(300)


def _new_pod_sweeps(home, commands, events):
    """The new pod's gateway startup sweep, run once per command."""
    _enter_pod(home)
    events.put(("ready", os.getpid()))
    while commands.get(timeout=120):
        claimed = delivery_ledger.sweep_recoverable(deliverable_platforms={"telegram"})
        events.put(("swept", sorted(row["obligation_id"] for row in claimed)))


def _old_pod_runs_delegation(home, finish, events):
    """The old pod booted, then dispatched a background delegation that runs
    until *finish* is set; its completion lands on its own queue."""
    _enter_pod(home)
    from tools.process_registry import process_registry

    def runner():
        if not finish.wait(120):
            raise TimeoutError("never told to finish")
        return {"status": "completed", "summary": "the real result"}

    handle = async_delegation.dispatch_async_delegation(
        goal="research",
        context=None,
        toolsets=None,
        role="leaf",
        model=None,
        session_key="agent:main:telegram:dm:7",
        runner=runner,
    )
    events.put(("dispatched", handle["delegation_id"]))
    completion = process_registry.completion_queue.get(timeout=150)
    events.put(("completed", (completion["delegation_id"], completion["status"])))
    time.sleep(300)


def _new_pod_boots(home, commands, events):
    """The new pod imports the process registry (its boot restores durable
    completions), then reports what reached its completion queue."""
    _enter_pod(home)
    from tools.process_registry import process_registry

    completions = process_registry.completion_queue
    events.put(("booted", os.getpid()))
    while commands.get(timeout=120):
        seen = []
        while True:
            try:
                event = completions.get_nowait()
            except queue.Empty:
                break
            seen.append((
                event["delegation_id"],
                event["status"],
                bool(event.get("restored")),
            ))
        events.put(("queue", seen))


# --------------------------------------------------------------------------
# B-F07: delivery obligations
# --------------------------------------------------------------------------


def test_new_pod_does_not_resend_a_live_old_pod_reply(authority, tmp_path):
    old_home, new_home = _pods(tmp_path, "old-pod", "new-pod")
    events, commands = _SPAWN.Queue(), _SPAWN.Queue()
    old = _spawn(_old_pod_owes_replies, events, old_home)
    new = None
    try:
        _await(events, "recorded")
        new = _spawn(_new_pod_sweeps, events, new_home, commands)
        _await(events, "ready")
        time.sleep(2.5 * LEASE)  # the old pod must keep its rows by renewing
        commands.put("sweep")
        assert _await(events, "swept") == []  # nothing re-sent while it lives
        assert _rows(
            authority,
            "SELECT obligation_id, state, attempts FROM delivery_obligations "
            "ORDER BY obligation_id",
        ) == [("owed-attempting", "attempting", 0), ("owed-pending", "pending", 0)]

        old.kill()  # the old pod dies with the replies still owed
        old.join(10)
        time.sleep(LEASE + 0.5)
        commands.put("sweep")
        assert _await(events, "swept") == ["owed-attempting", "owed-pending"]
        commands.put(None)
    finally:
        _stop([p for p in (old, new) if p is not None])
    owners = _rows(
        authority,
        "SELECT DISTINCT owner_instance, attempts FROM delivery_obligations",
    )
    assert len(owners) == 1 and owners[0][1] == 1  # claimed once, by the new pod
    assert _sqlite_files(old_home) == [] and _sqlite_files(new_home) == []


def test_restarted_gateway_redelivers_once_the_old_lease_runs_out(
    authority, tmp_path, monkeypatch
):
    """The startup sweep skips a row another process still leases; the gateway
    sweeps again when that lease runs out instead of waiting for a restart."""
    import gateway.status as status
    from gateway.config import Platform
    from gateway.run import GatewayRunner

    def no_pid_probe(*_args):
        raise AssertionError("authority ownership must not probe pids or /proc")

    monkeypatch.setattr(status, "_pid_exists", no_pid_probe)
    monkeypatch.setattr(status, "get_process_start_time", no_pid_probe)
    (old_home,) = _pods(tmp_path, "old-pod")
    events = _SPAWN.Queue()
    old = _spawn(_old_pod_owes_replies, events, old_home)
    sent = []

    async def send(*, chat_id, content, metadata=None):
        sent.append((chat_id, content))
        return SimpleNamespace(success=True)

    async def clear_resume_pending(_session_key):
        return None

    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: SimpleNamespace(send=send)}
    runner.session_store = object()
    runner._async_session_store = SimpleNamespace(
        _store=runner.session_store, clear_resume_pending=clear_resume_pending
    )
    runner._background_tasks = set()
    runner._running = True

    async def boot_then_wait():
        assert await runner._redeliver_pending_obligations() == 0  # old pod alive
        task = runner._obligation_resweep_task
        assert task is not None and not task.done()
        old.kill()
        await asyncio.wait_for(task, timeout=30)
        while runner._background_tasks:  # the resweep's own redelivery
            await asyncio.sleep(0.1)

    try:
        _await(events, "recorded")
        asyncio.run(boot_then_wait())
    finally:
        _stop([old])
    assert sorted(sent) == [
        ("42", "answer for owed-pending"),  # never sent: plain
        ("42", delivery_ledger.RECOVERED_MARKER + "answer for owed-attempting"),
    ]
    assert _rows(authority, "SELECT DISTINCT state FROM delivery_obligations") == [
        ("delivered",)
    ]


# --------------------------------------------------------------------------
# B-F08: background delegations
# --------------------------------------------------------------------------


def test_new_pod_boot_leaves_a_live_old_pod_delegation_running(authority, tmp_path):
    old_home, new_home = _pods(tmp_path, "old-pod", "new-pod")
    events, commands, finish = _SPAWN.Queue(), _SPAWN.Queue(), _SPAWN.Event()
    old = _spawn(_old_pod_runs_delegation, events, old_home, finish)
    new = None
    try:
        delegation_id = _await(events, "dispatched")
        time.sleep(2.5 * LEASE)  # the old pod must keep it by renewing
        new = _spawn(_new_pod_boots, events, new_home, commands)
        _await(events, "booted")
        commands.put("drain")
        assert _await(events, "queue") == []  # no "unknown" completion queued
        assert _rows(authority, "SELECT state FROM async_delegations") == [("running",)]

        finish.set()  # the old pod finishes it for real
        assert _await(events, "completed") == (delegation_id, "completed")
        time.sleep(LEASE + 1.5)
        commands.put("drain")
        assert _await(events, "queue") == []
        commands.put(None)
    finally:
        _stop([p for p in (old, new) if p is not None])
    assert _rows(authority, "SELECT state, delivery_state FROM async_delegations") == [
        ("completed", "pending")
    ]
    assert _sqlite_files(old_home) == [] and _sqlite_files(new_home) == []


def test_new_pod_recovers_a_delegation_once_its_dead_owner_lease_runs_out(
    authority, tmp_path
):
    """Restore runs once at boot; the owner dying later still ends the
    delegation as ``unknown`` in the running new pod, exactly once."""
    old_home, new_home = _pods(tmp_path, "old-pod", "new-pod")
    events, commands, finish = _SPAWN.Queue(), _SPAWN.Queue(), _SPAWN.Event()
    old = _spawn(_old_pod_runs_delegation, events, old_home, finish)
    new = None
    try:
        delegation_id = _await(events, "dispatched")
        new = _spawn(_new_pod_boots, events, new_home, commands)
        _await(events, "booted")
        old.kill()  # the old pod dies mid-run, after the new pod booted
        old.join(10)
        time.sleep(LEASE + 3.0)
        commands.put("drain")
        assert _await(events, "queue") == [(delegation_id, "unknown", True)]
        time.sleep(LEASE)
        commands.put("drain")
        assert _await(events, "queue") == []
        commands.put(None)
    finally:
        _stop([p for p in (old, new) if p is not None])
    assert _rows(authority, "SELECT state FROM async_delegations") == [("unknown",)]


# --------------------------------------------------------------------------
# Lease bookkeeping and the unchanged SQLite path
# --------------------------------------------------------------------------


def test_leases_are_postgres_rows_renewed_by_their_owner(authority, monkeypatch):
    monkeypatch.setattr(delivery_ledger, "LEASE_SECONDS", LEASE)
    monkeypatch.setattr(delivery_ledger, "LEASE_RENEW_SECONDS", RENEW)
    delivery_ledger.record_obligation(
        obligation_id="own",
        session_key="s",
        platform="telegram",
        chat_id="1",
        thread_id=None,
        content="x",
    )
    first = _rows(authority, "SELECT lease_expires_at FROM delivery_obligations")[0][0]
    time.sleep(3 * RENEW)
    ((owner, renewed),) = _rows(
        authority, "SELECT owner_instance, lease_expires_at FROM delivery_obligations"
    )
    import hermes_aux_store

    assert owner == hermes_aux_store.aux_owner_instance()
    assert renewed > first
    # This process never claims its own row, and reports no foreign wait.
    assert delivery_ledger.sweep_recoverable() == []
    assert delivery_ledger.seconds_until_recoverable() is None
    delivery_ledger.mark_delivered("own")
    assert delivery_ledger.renew_obligation_leases() == 0
    columns = {
        row[0]
        for row in _rows(
            authority,
            "SELECT column_name || ':' || data_type FROM information_schema.columns "
            "WHERE table_name IN ('delivery_obligations', 'async_delegations') "
            "AND column_name IN ('owner_instance', 'lease_expires_at')",
        )
    }
    assert columns == {"owner_instance:text", "lease_expires_at:double precision"}


def test_sqlite_profiles_keep_pid_ownership_and_no_lease_columns(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    delivery_ledger.record_obligation(
        obligation_id="o",
        session_key="s",
        platform="telegram",
        chat_id="1",
        thread_id=None,
        content="x",
    )
    async_delegation._persist_dispatch({
        "delegation_id": "d",
        "session_key": "s",
        "dispatched_at": time.time(),
    })
    connection = sqlite3.connect(tmp_path / "state.db")
    try:
        for table in ("delivery_obligations", "async_delegations"):
            columns = {
                row[1] for row in connection.execute(f"PRAGMA table_info({table})")
            }
            assert "owner_instance" not in columns and "lease_expires_at" not in columns
    finally:
        connection.close()
    assert delivery_ledger.seconds_until_recoverable() is None
    assert async_delegation._seconds_until_recoverable() is None
    assert not {
        thread.name for thread in threading.enumerate() if thread.is_alive()
    } & {"delivery-obligation-lease", "async-delegation-lease"}
    # The pid probe still decides: this live process keeps both rows.
    assert delivery_ledger.sweep_recoverable() == []
    assert async_delegation.recover_abandoned_delegations() == 0
