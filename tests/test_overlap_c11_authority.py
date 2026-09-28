"""levos 0069 — no state.db file dependency on a PostgreSQL-authority profile.

Two overlapping pods of one profile share the profile's PostgreSQL store and
nothing else: each has its own disk. On ``HERMES_STATE_BACKEND=authority``:

* the dashboard's startup reconcile leaves no ``state.db`` on the pod's disk,
  ACP history goes to the profile's store, and ``/api/status`` still counts
  sessions without the file;
* the tui change watcher (``cron.changed`` / ``sessions.changed``) and
  ``hermes mcp serve``'s EventBridge see writes another pod made, because
  their signal is ``hermes_aux_store.aux_change_signal`` instead of a file
  mtime.

Every other backend keeps its files. Runs on the fork's ephemeral,
Unix-socket-only PostgreSQL (``initdb`` / ``pg_ctl`` on PATH or in
``PG3_PERCENT_PG_BIN``; missing tools are errors, never skips).

levos/pg3 (0.21.2) port: the dashboard reconcile is in
``hermes_cli.web_server_lifecycle``, the status count in
``hermes_cli.web_routers.status``, the session opener in
``hermes_cli.web_server_sessions``; the watcher signatures live in
``tui_gateway/change_watcher.py`` (published onto ``tui_gateway.server``) and
mcp serve's signal is ``_read_state_db_mtime``. Cron jobs reach PostgreSQL
only with levos 0060 (``cron.jobs._jobs_store_is_pg``); the cases that create
a cron job skip without it.
"""

from __future__ import annotations

import multiprocessing
import os
import time
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

import hermes_aux_store as aux
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0069&connect_timeout=1"
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
_SPAWN = multiprocessing.get_context("spawn")


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    yield
    _drop_signal_connections()


def _require_cron_on_postgres():
    from cron import jobs as cron_jobs

    if not hasattr(cron_jobs, "_jobs_store_is_pg"):
        pytest.skip(
            "cron jobs follow PostgreSQL authority only with levos 0060 "
            "(cron.jobs._jobs_store_is_pg), which is not on this branch"
        )


def _drop_signal_connections():
    with aux._change_signal_lock:
        for conn in aux._change_signal_connections.values():
            conn.close()
        aux._change_signal_connections.clear()


@pytest.fixture
def authority(monkeypatch, postgres_dsn):
    """An authority profile whose core session schema exists, as in production."""
    from hermes_state import SessionDB

    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP SCHEMA public CASCADE")
        raw.execute("CREATE SCHEMA public")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    SessionDB(read_only=False).close()
    return postgres_dsn


@pytest.fixture
def pods(tmp_path):
    """Two pod disks: nothing on one is visible from the other."""
    homes = (tmp_path / "pod-old", tmp_path / "pod-new")
    for home in homes:
        home.mkdir()
    return homes


def _files(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def _state_files(root: Path):
    return [
        f for f in _files(root) if f.startswith("state.db") or f.endswith("jobs.json")
    ]


# --------------------------------------------------------------------------
# Two processes = two overlapping pods. Children are spawned with this test's
# environment (authority DSN); each then points HERMES_HOME at its own disk.
# --------------------------------------------------------------------------


def _child(target, home, events, *args):
    """Process entry point: become the pod on *home*, run *target*."""
    os.environ["HERMES_HOME"] = str(home)
    try:
        target(events, *args)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


def _spawn(target, home, events, *args):
    process = _SPAWN.Process(target=_child, args=(target, home, events, *args))
    process.start()
    return process


def _await(events, done, *, timeout=120):
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


def _acp_pod(events, release):
    """Old pod: an ACP client opens a session; the pod stays up (overlap)."""
    from acp_adapter.session import SessionManager

    manager = SessionManager(agent_factory=lambda: SimpleNamespace(model="acp-model"))
    state = manager.create_session(cwd="/work")
    # pg3: an empty session stays ephemeral; the first completed prompt persists it.
    state.history.append({"role": "user", "content": "hello over ACP"})
    manager.save_session(state.session_id)
    events.put(("acp_session", state.session_id))
    release.wait(60)


def _dashboard_pod(events, session_id):
    """New pod on an empty disk: dashboard start, then its status/session reads."""
    from hermes_cli import web_server_lifecycle, web_server_sessions
    from hermes_cli.web_routers import status

    web_server_lifecycle._eager_reconcile_own_session_db()
    db = web_server_sessions._open_session_db_for_profile(None, read_only=True)
    try:
        row = db.get_session(session_id)
    finally:
        db.close()
    events.put(("seen", row and row["source"]))
    events.put(("active", status._count_status_active_sessions()))


def test_new_pod_dashboard_leaves_no_state_db_and_sees_old_pod_acp_history(
    authority, pods
):
    """A-F31: the old pod's ACP session is in PostgreSQL; the new pod's
    dashboard start creates no state.db and its status/session reads see it."""
    old_home, new_home = pods
    events, release = _SPAWN.Queue(), _SPAWN.Event()
    old = _spawn(_acp_pod, old_home, events, release)
    try:
        session_id = _kinds(_await(events, bool), "acp_session")[0]
        new = _spawn(_dashboard_pod, new_home, events, session_id)
        try:
            got = _await(events, lambda got: bool(_kinds(got, "active")))
        finally:
            _stop([new])
    finally:
        release.set()
        _stop([old])

    # One comparison, so a failure shows every symptom at once.
    assert {
        "new pod reads the ACP session": _kinds(got, "seen"),
        "new pod counts it active": _kinds(got, "active")[0] >= 1,
        "old pod disk": _state_files(old_home),
        "new pod disk": _state_files(new_home),
    } == {
        "new pod reads the ACP session": ["acp"],
        "new pod counts it active": True,
        "old pod disk": [],
        "new pod disk": [],
    }


def _watcher_pod(events, go, written):
    """Pod B: the dashboard's tui change watcher and an mcp serve EventBridge."""
    import mcp_serve
    from tui_gateway import server

    server._hermes_home = os.environ["HERMES_HOME"]
    broadcasts = []
    server._broadcast_global_event = lambda event, payload=None: broadcasts.append(
        event
    )
    server._broadcast_watched_changes(now=0.0)  # first sighting seeds silently
    bridge = mcp_serve.EventBridge()
    bridge._establish_baseline()
    db = mcp_serve._get_session_db()
    go.set()
    written.wait(60)
    server._broadcast_watched_changes(now=10.0)
    try:
        bridge._poll_once(db)
    finally:
        db.close()
    events.put(("tui", sorted(set(broadcasts))))
    events.put(("mcp", [e["content"] for e in bridge.poll_events()["events"]]))


def _writer_pod(events, go, written):
    """Pod A: a messaging turn lands and a cron job is created."""
    from cron import jobs as cron_jobs
    from hermes_state import SessionDB

    go.wait(60)
    db = SessionDB(read_only=False)
    try:
        db.create_session(
            "20260927_000000_c11",
            "telegram",
            session_key="agent:main:telegram:dm:c11",
            chat_id="c11",
        )
        db.append_message("20260927_000000_c11", "user", "hello from the other pod")
    finally:
        db.close()
    cron_jobs.create_job(prompt="p", schedule="every 1h", name="c11")
    written.set()
    events.put(("written", os.getpid()))


def test_watchers_on_one_pod_see_writes_from_the_other_pod(authority, pods):
    """B-F16: pod A writes a turn and a cron job; pod B's tui watcher fires
    cron.changed + sessions.changed and its mcp bridge delivers the message.
    Neither pod has state.db or cron/jobs.json."""
    _require_cron_on_postgres()
    writer_home, watcher_home = pods
    events, go, written = _SPAWN.Queue(), _SPAWN.Event(), _SPAWN.Event()
    processes = [
        _spawn(_watcher_pod, watcher_home, events, go, written),
        _spawn(_writer_pod, writer_home, events, go, written),
    ]
    try:
        got = _await(events, lambda got: bool(_kinds(got, "mcp")))
    finally:
        written.set()
        _stop(processes)

    assert {
        "tui broadcasts": _kinds(got, "tui"),
        "mcp events": _kinds(got, "mcp"),
        "writer pod disk": _state_files(writer_home),
        "watcher pod disk": _state_files(watcher_home),
    } == {
        "tui broadcasts": [["cron.changed", "sessions.changed"]],
        "mcp events": [["hello from the other pod"]],
        "writer pod disk": [],
        "watcher pod disk": [],
    }


def _session_writer_pod(events, go, written):
    """Pod A without cron: only a messaging turn lands."""
    from hermes_state import SessionDB

    go.wait(60)
    db = SessionDB(read_only=False)
    try:
        db.create_session(
            "20260927_000000_c11s",
            "telegram",
            session_key="agent:main:telegram:dm:c11s",
            chat_id="c11s",
        )
        db.append_message("20260927_000000_c11s", "user", "a turn from the other pod")
    finally:
        db.close()
    written.set()
    events.put(("written", os.getpid()))


def test_watchers_on_one_pod_see_session_writes_from_the_other_pod(authority, pods):
    """pg3 port, sessions half of B-F16 (runs without levos 0060): pod A
    writes a turn; pod B's tui watcher fires sessions.changed (and nothing
    for cron, whose signal reads the absent core_cron_jobs) and its mcp bridge
    delivers the message. Neither pod has state.db."""
    writer_home, watcher_home = pods
    events, go, written = _SPAWN.Queue(), _SPAWN.Event(), _SPAWN.Event()
    processes = [
        _spawn(_watcher_pod, watcher_home, events, go, written),
        _spawn(_session_writer_pod, writer_home, events, go, written),
    ]
    try:
        got = _await(events, lambda got: bool(_kinds(got, "mcp")))
    finally:
        written.set()
        _stop(processes)

    assert {
        "tui broadcasts": _kinds(got, "tui"),
        "mcp events": _kinds(got, "mcp"),
        "writer pod disk": _state_files(writer_home),
        "watcher pod disk": _state_files(watcher_home),
    } == {
        "tui broadcasts": [["sessions.changed"]],
        "mcp events": [["a turn from the other pod"]],
        "writer pod disk": [],
        "watcher pod disk": [],
    }


# --------------------------------------------------------------------------
# aux_change_signal contract (single process)
# --------------------------------------------------------------------------


def _settled(name, *, quiet=1.5, timeout=15):
    """The signal once no write is still being published (quiet for *quiet* s)."""
    deadline = time.monotonic() + timeout
    value = aux.aux_change_signal(name)
    while True:
        time.sleep(quiet)
        again = aux.aux_change_signal(name)
        if again == value:
            return value
        assert time.monotonic() < deadline, f"{name} signal never settled"
        value = again


def _moved(name, previous, *, timeout=10):
    """The first signal value that differs from *previous*."""
    deadline = time.monotonic() + timeout
    while (value := aux.aux_change_signal(name)) == previous:
        assert time.monotonic() < deadline, f"{name} signal did not move"
        time.sleep(0.1)
    return value


def test_sessions_signal_moves_on_every_write_and_rests_otherwise(authority):
    """A new turn moves the newest message id at once; metadata-only writes
    move the table counters once the writer publishes them. Each write here
    comes over a second after the previous one, the pace at which PostgreSQL
    publishes a backend's counters right away (a faster burst lands within
    its ten-second idle interval)."""
    from hermes_state import SessionDB

    stable = _settled("sessions")
    assert aux.aux_change_signal("sessions") == stable

    db = SessionDB(read_only=False)  # the open itself writes (schema check)
    try:
        time.sleep(1.1)
        db.create_session("s1", "cli")
        created = _moved("sessions", stable)
        time.sleep(1.1)
        db.append_message("s1", "user", "one")
        appended = aux.aux_change_signal("sessions")  # no wait: exact
        assert appended[0] > created[0]
        time.sleep(1.1)
        before = aux.aux_change_signal("sessions")
        db.set_session_title("s1", "renamed")  # metadata only, no message
        renamed = _moved("sessions", before)
        assert renamed[0] == appended[0]
        time.sleep(1.1)
        db.delete_session("s1")
        _moved("sessions", renamed)
    finally:
        db.close()


def test_cron_signal_reads_a_missing_table_without_creating_it(authority):
    _require_cron_on_postgres()
    from cron import jobs as cron_jobs

    assert aux.aux_change_signal("cron_jobs") == ("cron_jobs", None)
    with psycopg.connect(authority) as raw:
        assert raw.execute("SELECT to_regclass('core_cron_jobs')").fetchone()[0] is None

    job = cron_jobs.create_job(prompt="p", schedule="every 1h", name="one")
    first = aux.aux_change_signal("cron_jobs")
    assert first[0] == 1
    cron_jobs.pause_job(job["id"])
    assert aux.aux_change_signal("cron_jobs") != first


def test_change_signal_survives_a_dropped_connection(authority):
    before = aux.aux_change_signal("sessions")
    (conn,) = aux._change_signal_connections.values()
    pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
    with psycopg.connect(authority, autocommit=True) as raw:
        raw.execute("SELECT pg_terminate_backend(%s)", (pid,))
    deadline = time.monotonic() + 10
    while True:
        try:
            assert aux.aux_change_signal("sessions") == before
            break
        except aux.AuxStoreUnavailable:
            assert time.monotonic() < deadline  # the next call reconnects


def test_change_signal_off_authority_or_unreachable_raises(monkeypatch):
    with pytest.raises(ValueError):
        aux.aux_change_signal("state.db")
    with pytest.raises(aux.AuxStoreUnavailable, match="only on PostgreSQL authority"):
        aux.aux_change_signal("sessions")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    with pytest.raises(aux.AuxStoreUnavailable) as caught:
        aux.aux_change_signal("cron_jobs")
    assert "nonexistent-0069" not in str(caught.value)


def test_authority_watchers_skip_a_tick_on_postgres_failure(monkeypatch):
    """No file signal on authority: a PostgreSQL failure raises (the tui loop
    skips the tick, the mcp loop logs and retries) instead of stat'ing."""
    import mcp_serve
    from tui_gateway import server

    home = Path(os.environ["HERMES_HOME"])
    (home / "cron").mkdir(exist_ok=True)
    (home / "cron" / "jobs.json").write_text("[]", encoding="utf-8")
    (home / "state.db").write_text("x", encoding="utf-8")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    monkeypatch.setattr(server, "_hermes_home", str(home))
    for sig in (
        server._cron_sig,
        server._sessions_sig,
        mcp_serve._read_state_db_mtime,
    ):
        with pytest.raises(aux.AuxStoreUnavailable):
            sig()


# --------------------------------------------------------------------------
# Every other backend keeps its files
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite"])
def test_non_authority_keeps_the_state_db_file_paths(monkeypatch, backend):
    import mcp_serve
    import hermes_state_postgres
    from acp_adapter.session import SessionManager
    from hermes_cli import web_server_lifecycle
    from hermes_cli.web_routers import status
    from tui_gateway import server

    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def no_postgres(*_args, **_kwargs):
        raise AssertionError("a non-authority profile must not reach PostgreSQL")

    monkeypatch.setattr(hermes_state_postgres, "connect_postgres", no_postgres)
    home = Path(os.environ["HERMES_HOME"])
    monkeypatch.setattr(server, "_hermes_home", str(home))

    assert status._count_status_active_sessions() == 0  # no file: no bootstrap
    assert not (home / "state.db").exists()
    web_server_lifecycle._eager_reconcile_own_session_db()
    assert (home / "state.db").is_file()

    manager = SessionManager(agent_factory=lambda: SimpleNamespace(model="m"))
    db = manager._get_db()
    assert Path(db.db_path) == home / "state.db"

    assert server._sessions_sig() == max(
        (home / name).stat().st_mtime_ns
        for name in ("state.db", "state.db-wal")
        if (home / name).exists()
    )
    assert server._cron_sig() is None
    (home / "cron").mkdir(exist_ok=True)
    (home / "cron" / "jobs.json").write_text("[]", encoding="utf-8")
    assert server._cron_sig() == (home / "cron" / "jobs.json").stat().st_mtime_ns
    assert mcp_serve._read_state_db_mtime() == (home / "state.db").stat().st_mtime
