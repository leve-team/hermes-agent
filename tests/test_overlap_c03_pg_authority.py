"""levos 0062 — overlapping pods: turn ownership, pending messages and tui
interrupted-turn markers follow the profile's PostgreSQL authority.

Two pods of one profile overlap during a rolling update. Each has its own disk
and they share only PostgreSQL, so these tests run every "pod" as a spawned
process with its OWN ``HERMES_HOME`` (nothing on disk is shared) against one
real PostgreSQL. The routing scope — the profile's ``sessions`` path, which
is the same string on every pod — is pinned to one value in every child.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips), like levos 0060.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest

from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0062&connect_timeout=1"
)
POD_SCOPE = "/var/lib/session-plane/hermes/profiles/p/sessions"
# The tui marker home: the same path string on both pods. Children chdir into
# their own disk first, so the unpatched file code writes two different files.
POD_PROFILE_HOME = "profile-home"
C03_TABLES = (
    "core_gateway_turn_leases",
    "core_gateway_pending_messages",
    "core_tui_turn_markers",
)
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
)


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    yield
    try:
        from gateway import turn_owner

        turn_owner._renewer.stop()
    except ImportError:  # the same scenarios run on the unpatched tree (EVIDENCE)
        pass


@pytest.fixture
def pg(postgres_dsn):
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(f"DROP TABLE IF EXISTS {', '.join(C03_TABLES)}")
        present = [
            table
            for table in ("gateway_routing", "messages", "sessions")
            if raw.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0]
        ]
        if present:
            raw.execute(f"TRUNCATE {', '.join(present)} CASCADE")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg):
    """Authority profile whose core session schema exists, as in production."""
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
    SessionDB(read_only=False).close()
    return pg


def _files(root: Path):
    """The files C03 moves off the disk (other stores are other cards')."""
    return sorted(
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file()
        and (
            path.name == ".clean_shutdown"
            or {"pending_messages", "desktop"} & set(path.relative_to(root).parts)
        )
    )


def _routing(scope=POD_SCOPE):
    from hermes_state import SessionDB

    db = SessionDB(read_only=False)
    try:
        return {
            key: json.loads(value)
            for key, value in db.load_gateway_routing_entries(scope=scope).items()
        }
    finally:
        db.close()


def _lease_rows(dsn):
    with psycopg.connect(dsn) as raw:
        return raw.execute(
            "SELECT token, session_key, outcome FROM core_gateway_turn_leases ORDER BY token"
        ).fetchall()


def _source(chat_id):
    from gateway.config import Platform
    from gateway.session import SessionSource

    return SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, user_id="u1")


def _pod_store(sessions_dir):
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore

    return SessionStore(sessions_dir=Path(sessions_dir), config=GatewayConfig())


def _bare_runner(store, scheduled=None):
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.session_store = store
    runner._running = True
    runner._draining = False
    if scheduled is not None:
        runner._schedule_resume_pending_sessions = lambda platform=None: (
            scheduled.append(1)
        )
    return runner


def _resumable(store):
    with store._lock:
        store._ensure_loaded_locked()
        return {
            key: entry.resume_reason
            for key, entry in store._entries.items()
            if entry.resume_pending
        }


# --------------------------------------------------------------------------
# Two pods = two spawned processes, each on its own HERMES_HOME (own disk).
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")


def _child(target, events, *args):
    """Process entry point: run *target*, report any failure as an event."""
    try:
        target(events, *args)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


def _pod(monkeypatch, tmp_path, name, target, events, *args):
    """Start *target* as pod *name*: its own HERMES_HOME, same PostgreSQL."""
    home = tmp_path / name
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    process = _SPAWN.Process(target=_child, args=(target, events, str(home), *args))
    process.start()
    return process, home


def _await(events, done, *, timeout=90):
    got = []
    deadline = time.monotonic() + timeout
    while not done(got):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out; events so far: {got}"
        got.append(events.get(timeout=remaining))
        assert not _kinds(got, "error"), f"a child process failed: {got}"
    return got


def _kinds(got, kind):
    return [value for event, value in got if event == kind]


def _stop(processes):
    for process in processes:
        process.join(30)
        if process.is_alive():
            process.kill()
            process.join(5)


def _pod_setup(home, *, lease=None, host=None):
    """Pin the shared routing scope; optionally shorten leases / fake a pod name."""
    from gateway.session import SessionStore

    os.chdir(home)
    SessionStore._routing_scope = lambda self: POD_SCOPE
    if lease is not None or host is not None:
        import socket

        from gateway import turn_owner

        if lease is not None:
            turn_owner.LEASE_SECONDS, turn_owner.LEASE_RENEW_SECONDS = lease
        if host is not None:
            socket.gethostname = lambda: host


def _running_turn_pod(events, home, control, lease, host):
    """Old pod: one turn running, one session idle but active a moment ago."""
    _pod_setup(home, lease=lease, host=host)
    store = _pod_store(Path(home) / "sessions")
    live = store.get_or_create_session(_source("live-chat")).session_key
    token = store.mark_turn_active(live)
    idle = store.get_or_create_session(_source("idle-chat")).session_key
    events.put(("ready", {"live": live, "idle": idle, "token": token}))
    control.get()  # keeps running (and renewing) until the parent kills it


def _starting_pod(events, home, control, lease, host):
    """New pod: the startup pass while the old pod still runs, then sweeps."""
    _pod_setup(home, lease=lease, host=host)
    store = _pod_store(Path(home) / "sessions")
    scheduled: list = []
    runner = _bare_runner(store, scheduled)
    asyncio.run(runner._recover_sessions_after_previous_run())
    events.put((
        "startup",
        {"resumable": _resumable(store), "home": _files(Path(home))},
    ))
    while True:
        command = control.get()
        if command == "exit":
            return
        resumable = asyncio.run(runner._sweep_turn_leases())
        events.put((
            "sweep",
            {
                "resumable": resumable,
                "scheduled": len(scheduled),
                "state": _resumable(store),
            },
        ))


@pytest.mark.parametrize(
    "other_pod",
    [False, True],
    ids=["dead-owner-in-this-pod", "dead-owner-in-another-pod"],
)
def test_new_pod_leaves_a_live_peer_turn_alone_and_resumes_it_after_the_peer_dies(
    authority, monkeypatch, tmp_path, other_pod
):
    """B-F06 / A-F07. Unpatched, the new pod finds no ``.clean_shutdown`` on its
    fresh disk, promotes the old pod's RUNNING turn to resume_pending (and so
    runs it a second time) and arms every session active in the last 120 s."""
    lease = (3.0, 0.5) if other_pod else None
    events, old_control, new_control = (_SPAWN.Queue() for _ in range(3))
    old, _ = _pod(
        monkeypatch,
        tmp_path,
        "old-pod",
        _running_turn_pod,
        events,
        old_control,
        lease,
        "old-pod" if other_pod else None,
    )
    ready = _kinds(_await(events, lambda got: _kinds(got, "ready")), "ready")[0]
    new, new_home = _pod(
        monkeypatch,
        tmp_path,
        "new-pod",
        _starting_pod,
        events,
        new_control,
        lease,
        "new-pod" if other_pod else None,
    )
    try:
        startup = _kinds(_await(events, lambda got: _kinds(got, "startup")), "startup")[
            0
        ]
        assert startup["resumable"] == {}, "the new pod armed a live peer's sessions"
        routing = _routing()
        assert routing[ready["live"]]["active_turn_token"] == ready["token"]
        assert not routing[ready["live"]].get("resume_pending")
        assert not routing[ready["idle"]].get("resume_pending")
        assert not any(name.endswith(".clean_shutdown") for name in startup["home"])

        if other_pod:
            # The owner keeps renewing: sweeping past a full lease claims nothing.
            deadline = time.monotonic() + lease[0] + 1.5
            while time.monotonic() < deadline:
                new_control.put("sweep")
                sweep = _kinds(
                    _await(events, lambda got: _kinds(got, "sweep")), "sweep"
                )[0]
                assert sweep["resumable"] == 0, sweep
                time.sleep(0.5)

        old.kill()
        old.join(10)
        started = time.monotonic()
        while True:
            new_control.put("sweep")
            got = _await(events, lambda got: _kinds(got, "sweep"))
            sweep = _kinds(got, "sweep")[0]
            if sweep["resumable"]:
                break
            assert time.monotonic() - started < (lease[0] + 5 if other_pod else 5), (
                sweep
            )
            time.sleep(0.3)
        assert sweep == {
            "resumable": 1,
            "scheduled": 1,
            "state": {ready["live"]: "restart_interrupted"},
        }
        routing = _routing()
        assert routing[ready["live"]]["active_turn_token"] is None
        assert routing[ready["live"]]["resume_reason"] == "restart_interrupted"
        assert not routing[ready["idle"]].get("resume_pending")
        assert _lease_rows(authority) == []
        new_control.put("exit")
    finally:
        _stop([old, new])
    assert not (new_home / ".clean_shutdown").exists()


def _crashed_pod(events, home, count):
    _pod_setup(home)
    store = _pod_store(Path(home) / "sessions")
    keys = []
    for index in range(count):
        key = store.get_or_create_session(_source(f"chat-{index}")).session_key
        store.mark_turn_active(key)
        keys.append(key)
    events.put(("ready", keys))
    time.sleep(3600)


def _sweeping_pod(events, home, barrier):
    _pod_setup(home)
    store = _pod_store(Path(home) / "sessions")
    runner = _bare_runner(store, [])
    barrier.wait(60)
    events.put(("swept", asyncio.run(runner._sweep_turn_leases())))


def test_two_peers_sweeping_one_dead_pod_resume_each_turn_once(
    authority, monkeypatch, tmp_path
):
    events = _SPAWN.Queue()
    crashed, _ = _pod(monkeypatch, tmp_path, "crashed", _crashed_pod, events, 5)
    keys = _kinds(_await(events, lambda got: _kinds(got, "ready")), "ready")[0]
    crashed.kill()
    crashed.join(10)
    barrier = _SPAWN.Barrier(2)
    peers = [
        _pod(monkeypatch, tmp_path, f"peer-{index}", _sweeping_pod, events, barrier)[0]
        for index in range(2)
    ]
    try:
        swept = _kinds(
            _await(events, lambda got: len(_kinds(got, "swept")) == 2), "swept"
        )
    finally:
        _stop(peers)
    assert sum(swept) == 5, swept
    routing = _routing()
    assert {k: routing[k]["resume_reason"] for k in keys} == dict.fromkeys(
        keys, "restart_interrupted"
    )
    assert _lease_rows(authority) == []


def _draining_pod(events, home, outcome):
    """Old pod ending with the drain timing out (``interrupted``) or cleanly."""
    from gateway import turn_owner

    _pod_setup(home)
    store = _pod_store(Path(home) / "sessions")
    stuck = store.get_or_create_session(_source("stuck-chat")).session_key
    token = store.mark_turn_active(stuck)
    keys = {"stuck": stuck, "stuck_token": token}
    if outcome == turn_owner.OUTCOME_INTERRUPTED:
        # _stop_impl's timeout branch: arm, hand over, interrupt → the turn
        # unwinds and clears its own marker.
        unwound = store.get_or_create_session(_source("unwound-chat")).session_key
        unwound_token = store.mark_turn_active(unwound)
        store.mark_resume_pending(unwound, "shutdown_timeout")
        assert store.hand_over_turns([unwound]) == 1
        assert store.clear_turn_active(unwound, unwound_token)
        keys["unwound"] = unwound
    turn_owner.release(outcome)
    events.put(("exited", keys))


def _peer_pod(events, home, control):
    _pod_setup(home)
    store = _pod_store(Path(home) / "sessions")
    scheduled: list = []
    runner = _bare_runner(store, scheduled)
    asyncio.run(runner._recover_sessions_after_previous_run())  # loads routing now
    events.put(("started", None))
    control.get()
    resumable = asyncio.run(runner._sweep_turn_leases())
    events.put((
        "swept",
        {
            "resumable": resumable,
            "scheduled": len(scheduled),
            "state": _resumable(store),
        },
    ))


@pytest.mark.parametrize("outcome", ["interrupted", "clean"])
def test_the_peer_takes_over_what_a_draining_pod_leaves(
    authority, monkeypatch, tmp_path, outcome
):
    """``.clean_shutdown`` replaced: the old pod exits AFTER the new one started
    (rolling order) and its leases carry the outcome instead of a file."""
    events, control = _SPAWN.Queue(), _SPAWN.Queue()
    peer, _ = _pod(monkeypatch, tmp_path, "peer", _peer_pod, events, control)
    _await(events, lambda got: _kinds(got, "started"))
    old, old_home = _pod(monkeypatch, tmp_path, "old", _draining_pod, events, outcome)
    try:
        keys = _kinds(_await(events, lambda got: _kinds(got, "exited")), "exited")[0]
        old.join(30)
        # The receipt: every lease the old pod held, expired with its outcome.
        expected = {(keys["stuck"], outcome)}
        if "unwound" in keys:
            expected.add((keys["unwound"], outcome))  # the hand-over row
        assert {(row[1], row[2]) for row in _lease_rows(authority)} == expected
        control.put("sweep")
        swept = _kinds(_await(events, lambda got: _kinds(got, "swept")), "swept")[0]
    finally:
        _stop([peer, old])
    routing = _routing()
    assert routing[keys["stuck"]]["active_turn_token"] is None
    if outcome == "interrupted":
        assert swept["state"] == {
            keys["stuck"]: "restart_interrupted",
            keys["unwound"]: "shutdown_timeout",
        }
        assert swept["scheduled"] == 1
    else:
        assert swept == {"resumable": 0, "scheduled": 0, "state": {}}
        assert not routing[keys["stuck"]].get("resume_pending")
    assert _lease_rows(authority) == []
    assert not (old_home / ".clean_shutdown").exists()


def _flushing_pod(events, home, session_id):
    from gateway.shutdown_flush import flush_pending_to_file

    _pod_setup(home)
    flushed = flush_pending_to_file(
        {
            "agent:main:telegram:dm:u1": {
                "text": "sent while the turn ran",
                "session_id": session_id,
            }
        },
        reason="shutdown",
    )
    events.put(("flushed", {"count": flushed, "files": _files(Path(home))}))


def _recovering_pod(events, home, barrier):
    from gateway.shutdown_flush import recover_pending_to_db

    _pod_setup(home)
    events.put(("started", recover_pending_to_db()))  # before the old pod flushes
    barrier.wait(60)  # the old pod flushed and exited
    events.put(("recovered", recover_pending_to_db()))


def test_pending_messages_of_an_exiting_pod_reach_the_transcript_once(
    authority, monkeypatch, tmp_path
):
    """A-F08. Unpatched, the old pod flushes to ``pending_messages/`` on its own
    disk and the peers never see it: the only copy is lost with the pod."""
    from hermes_state import SessionDB

    db = SessionDB(read_only=False)
    db.create_session(session_id="s-overlap", source="telegram")
    db.close()
    events = _SPAWN.Queue()
    barrier = _SPAWN.Barrier(3)
    peers = [
        _pod(monkeypatch, tmp_path, f"peer-{index}", _recovering_pod, events, barrier)[
            0
        ]
        for index in range(2)
    ]
    started = _kinds(
        _await(events, lambda got: len(_kinds(got, "started")) == 2), "started"
    )
    assert started == [0, 0]
    old, _ = _pod(monkeypatch, tmp_path, "old", _flushing_pod, events, "s-overlap")
    try:
        flushed = _kinds(_await(events, lambda got: _kinds(got, "flushed")), "flushed")[
            0
        ]
        old.join(30)
        barrier.wait(60)
        recovered = _kinds(
            _await(events, lambda got: len(_kinds(got, "recovered")) == 2), "recovered"
        )
    finally:
        _stop(peers + [old])
    assert sorted(recovered) == [0, 1], "the flushed message never reached a peer"
    assert flushed == {"count": 1, "files": []}
    db = SessionDB(read_only=False)
    try:
        contents = [m["content"] for m in db.get_messages("s-overlap")]
    finally:
        db.close()
    assert contents == ["sent while the turn ran"]
    for index in range(2):
        assert not (tmp_path / f"peer-{index}" / "pending_messages").exists()


def _tui_turn_pod(events, home, control, host):
    from tui_gateway.turn_marker import record_turn_start

    _pod_setup(home, lease=(3.0, 0.5) if host else None, host=host)
    record_turn_start(POD_PROFILE_HOME, "tui-session", "summarise the incident")
    events.put(("recorded", _files(Path(home))))
    control.get()


def _tui_resume_pod(events, home, control, host):
    from tui_gateway.turn_marker import read_turn_marker

    _pod_setup(home, lease=(3.0, 0.5) if host else None, host=host)
    while control.get() == "read":
        events.put(("read", read_turn_marker(POD_PROFILE_HOME, "tui-session")))


@pytest.mark.parametrize("other_pod", [False, True], ids=["same-pod", "other-pod"])
def test_tui_marker_is_resumed_on_the_peer_only_after_its_owner_died(
    authority, monkeypatch, tmp_path, other_pod
):
    """A-F09. Unpatched, the marker is a file on the pod that ran the turn:
    the pod the client resumes on never sees it and the prompt is lost."""
    events, turn_control, resume_control = (_SPAWN.Queue() for _ in range(3))
    turn, _ = _pod(
        monkeypatch,
        tmp_path,
        "turn-pod",
        _tui_turn_pod,
        events,
        turn_control,
        "turn-pod" if other_pod else None,
    )
    resume, _ = _pod(
        monkeypatch,
        tmp_path,
        "resume-pod",
        _tui_resume_pod,
        events,
        resume_control,
        "resume-pod" if other_pod else None,
    )
    try:
        recorded = _kinds(
            _await(events, lambda got: _kinds(got, "recorded")), "recorded"
        )[0]
        if other_pod:
            time.sleep(4.5)  # past the 3 s lease: only the renewal keeps it alive
        resume_control.put("read")
        alive = _kinds(_await(events, lambda got: _kinds(got, "read")), "read")[0]
        turn.kill()
        turn.join(10)
        started = time.monotonic()
        while True:
            resume_control.put("read")
            marker = _kinds(_await(events, lambda got: _kinds(got, "read")), "read")[0]
            if marker is not None or time.monotonic() - started > (
                8 if other_pod else 3
            ):
                break
            time.sleep(0.3)
        resume_control.put("exit")
    finally:
        _stop([turn, resume])
    assert marker is not None, "the interrupted prompt never reached the resuming pod"
    assert marker["prompt"] == "summarise the incident"
    assert marker["attempts"] == 0
    assert alive is None, "a turn still running on the peer looked interrupted"
    assert recorded == []


# --------------------------------------------------------------------------
# One process: contracts of each store on and off authority.
# --------------------------------------------------------------------------


def _graceful_stop(runner_home, monkeypatch):
    """Drive GatewayRunner.stop() the way tests/gateway/test_clean_shutdown_marker does."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    monkeypatch.setattr("gateway.run._hermes_home", runner_home)
    runner = object.__new__(GatewayRunner)
    runner._restart_requested = False
    runner._restart_detached = False
    runner._restart_via_service = False
    runner._restart_task_started = False
    runner._running = True
    runner._draining = False
    runner._stop_task = None
    runner._running_agents = {}
    runner._pending_messages = {}
    runner._pending_approvals = {}
    runner._background_tasks = set()
    runner._shutdown_event = MagicMock()
    runner._restart_drain_timeout = 5
    runner._exit_code = None
    runner._exit_reason = None
    runner.adapters = {}
    runner.config = GatewayConfig()
    with (
        patch(
            "gateway.run.GatewayRunner._drain_active_agents",
            new_callable=AsyncMock,
            return_value=([], False),
        ),
        patch("gateway.run.GatewayRunner._finalize_shutdown_agents"),
        patch("gateway.run.GatewayRunner._update_runtime_status"),
        patch("gateway.status.remove_pid_file"),
        patch("tools.process_registry.process_registry") as registry,
        patch("tools.terminal_tool.cleanup_all_environments"),
        patch("tools.browser_tool_lifecycle.cleanup_all_browsers"),
    ):
        registry.kill_all = MagicMock()
        asyncio.run(runner.stop())


def test_authority_keeps_turns_pending_and_markers_off_the_disk(
    authority, tmp_path, monkeypatch
):
    from gateway import shutdown_flush, turn_owner
    from hermes_state import SessionDB
    from tui_gateway import turn_marker

    home = Path(os.environ["HERMES_HOME"])
    store = _pod_store(tmp_path / "sessions")
    key = store.get_or_create_session(_source("c")).session_key
    token = store.mark_turn_active(key)
    assert token.startswith(turn_owner.TOKEN_PREFIX)
    assert [row[0] for row in _lease_rows(authority)] == [token]
    assert store.clear_turn_active(key, token)
    assert _lease_rows(authority) == []

    # A lease left behind (its drop failed) is released clean at exit and
    # then discarded by the next claimant, never resumed.
    orphan = store.mark_turn_active(key)
    with patch.object(turn_owner, "drop", side_effect=RuntimeError("down")):
        assert store.clear_turn_active(key, orphan)
    _graceful_stop(home, monkeypatch)
    assert [(row[0], row[2]) for row in _lease_rows(authority)] == [(orphan, "clean")]
    assert store.recover_turns_from_leases() == 0
    assert _lease_rows(authority) == []

    db = SessionDB(read_only=False)
    db.create_session(session_id="s1", source="telegram")
    db.close()
    assert (
        shutdown_flush.flush_pending_to_file(
            {"k": {"text": "hi", "session_id": "s1"}, "none": None}, reason="shutdown"
        )
        == 1
    )
    location = shutdown_flush.spool_dropped_transcript_message(
        "s1", {"role": "user", "content": "capped"}
    )
    assert str(location).startswith("postgres:core_gateway_pending_messages/")
    shutdown_flush.flush_agent_history_to_file("s1", [{"role": "user", "content": "x"}])
    replayed: list = []
    assert shutdown_flush.drain_transcript_spool("s1", replayed.append) == (1, 0)
    assert replayed == [{"role": "user", "content": "capped"}]
    assert shutdown_flush.recover_pending_to_db() == 1
    assert shutdown_flush.recover_pending_to_db() == 0  # idempotent
    with psycopg.connect(authority) as raw:
        left = raw.execute(
            "SELECT reason FROM core_gateway_pending_messages"
        ).fetchall()
    assert left == [("shutdown-with-unpersisted-agent-history",)]  # operator copy

    turn_marker.record_turn_start(tmp_path, "tui", "prompt", attempts=1)
    assert turn_marker.read_turn_marker(tmp_path, "tui")["attempts"] == 1  # own turn
    turn_marker.clear_turn_marker(tmp_path, "tui")
    assert turn_marker.read_turn_marker(tmp_path, "tui") is None
    turn_marker._pg_renewer.stop()

    assert _files(home) == []
    assert not (tmp_path / "desktop").exists()
    assert not (tmp_path / "pending_messages").exists()


def test_authority_without_postgres_raises_or_degrades_and_writes_no_file(
    monkeypatch, tmp_path, caplog
):
    from gateway import shutdown_flush
    from hermes_aux_store import AuxStoreUnavailable
    from tui_gateway import turn_marker

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    home = Path(os.environ["HERMES_HOME"])
    store = _pod_store(tmp_path / "sessions")
    store._db = None
    with pytest.raises(AuxStoreUnavailable) as raised:
        store.mark_turn_active("k")
    assert "nonexistent-0062" not in str(raised.value)
    with pytest.raises(AuxStoreUnavailable):
        store.recover_turns_from_leases()

    assert shutdown_flush.flush_pending_to_file({"k": "text"}, reason="shutdown") == 0
    assert "Could not flush 1 pending message(s) to PostgreSQL" in caplog.text
    assert "nonexistent-0062" not in caplog.text
    assert (
        shutdown_flush.spool_dropped_transcript_message("s", {"role": "user"}) is None
    )
    assert shutdown_flush.drain_transcript_spool("s", lambda m: None) == (0, 0)
    with pytest.raises(AuxStoreUnavailable):
        shutdown_flush.recover_pending_to_db()
    turn_marker.record_turn_start(tmp_path, "tui", "prompt")
    assert turn_marker.read_turn_marker(tmp_path, "tui") is None
    assert _files(home) == []
    assert _files(tmp_path) == []


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_files(monkeypatch, tmp_path, backend):
    import hermes_aux_store
    from gateway import shutdown_flush
    from tui_gateway import turn_marker

    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)
    monkeypatch.setattr(
        hermes_aux_store,
        "open_aux_postgres",
        MagicMock(side_effect=AssertionError("PostgreSQL store opened off authority")),
    )
    home = Path(os.environ["HERMES_HOME"])
    store = _pod_store(tmp_path / "sessions")
    store._db = None
    key = store.get_or_create_session(_source("c")).session_key
    token = store.mark_turn_active(key)
    assert ":" not in token
    assert store.clear_turn_active(key, token)
    assert shutdown_flush.flush_pending_to_file({"k": "text"}) == 1
    assert len(list((home / "pending_messages").glob("pending-*.json"))) == 1
    turn_marker.record_turn_start(tmp_path, "tui", "prompt")
    assert (tmp_path / "desktop" / "interrupted_turns.json").exists()
    assert turn_marker.read_turn_marker(tmp_path, "tui")["prompt"] == "prompt"
    _graceful_stop(home, monkeypatch)
    assert (home / ".clean_shutdown").exists()


def test_first_authority_boot_settles_markers_and_files_of_the_previous_gateway(
    authority, monkeypatch, tmp_path
):
    """Markers and pending files written before 0062 have no lease: the one
    ``.clean_shutdown`` the old gateway may have left decides them once."""
    from gateway import shutdown_flush, turn_owner
    from hermes_state import SessionDB

    home = Path(os.environ["HERMES_HOME"])
    monkeypatch.setattr("gateway.run._hermes_home", home)
    store = _pod_store(tmp_path / "sessions")
    keys = [store.get_or_create_session(_source(f"c{i}")).session_key for i in range(2)]

    def legacy_markers():
        # keys[0]: written by a pre-0062 gateway; keys[1]: a lease that vanished.
        for key, token in zip(keys, ("0" * 32, turn_owner.TOKEN_PREFIX + "gone")):
            store._mark_turn_active(key, token)

    legacy_markers()
    (home / ".clean_shutdown").touch()
    asyncio.run(_bare_runner(store)._recover_sessions_after_previous_run())
    routing = _routing(store._routing_scope())
    assert routing[keys[0]]["active_turn_token"] is None  # discarded
    assert not routing[keys[0]].get("resume_pending")
    assert routing[keys[1]]["active_turn_token"] == turn_owner.TOKEN_PREFIX + "gone"
    assert not (home / ".clean_shutdown").exists()

    legacy_markers()
    asyncio.run(_bare_runner(store)._recover_sessions_after_previous_run())
    routing = _routing(store._routing_scope())
    assert routing[keys[0]]["resume_reason"] == "restart_interrupted"
    assert not routing[keys[1]].get("resume_pending")  # leased: never guessed

    db = SessionDB(read_only=False)
    db.create_session(session_id="s-legacy", source="telegram")
    db.close()
    legacy = home / "pending_messages"
    legacy.mkdir()
    (legacy / "pending-old.json").write_text(
        json.dumps({
            "session_key": "k",
            "reason": "shutdown",
            "ts": 1,
            "data": {"text": "from the old pod", "session_id": "s-legacy"},
        })
    )
    assert shutdown_flush.recover_pending_to_db() == 1
    assert list(legacy.iterdir()) == []
    assert shutdown_flush.recover_pending_to_db() == 0
