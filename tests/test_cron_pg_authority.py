"""levos 0060 — cron stores and locks follow the profile's PostgreSQL authority.

On ``HERMES_STATE_BACKEND=authority`` the cron execution ledger, the job
notepad and the job list live in the profile's PostgreSQL store, and the tick,
jobs and fire locks are PostgreSQL advisory locks, so two pods of one profile
can overlap: nothing is written under ``cron/`` and each tick / fire happens
once. Every other backend keeps its files and flocks.

Adapted to levos/pg3 (0.21.2): the ledger has a handoff lifecycle (a worker adopts a claimed
attempt) whose lease is covered here too, and ``cron/incidents.py`` shares the ledger store
(``core_cron_incidents``).

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips). The py-pglite fixture of 0059 cannot carry these contracts: it
serves every socket connection from ONE backend session, so a second
connection wins ``pg_try_advisory_lock`` on a key the first one holds, and it
parks every other connection while one holds an open transaction.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import sqlite3
import time
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest

import hermes_aux_store as aux
from cron import executions, incidents, notepad
from cron import jobs as cron_jobs
from hermes_time import now as hermes_now
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0060&connect_timeout=1"
)
CRON_TABLES = (
    "core_cron_executions",
    "core_cron_incidents",
    "core_cron_notes",
    "core_cron_jobs",
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
def _cron_home(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HERMES_MODEL", "test-cron-default-model")
    home = Path(os.environ["HERMES_HOME"])
    # The notepad path is frozen at import; point it at this test's home.
    monkeypatch.setattr(notepad, "NOTEPAD_FILE", home / "cron" / "notepad.db")
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", None)
    monkeypatch.setattr(incidents, "EXECUTIONS_FILE", None)
    yield home
    executions._stop_lease_renewer()


@pytest.fixture
def pg(postgres_dsn):
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(f"DROP TABLE IF EXISTS {', '.join(CRON_TABLES)}")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg):
    """Authority profile whose core session schema exists (as in production)
    while cron's tables do not yet; children then race only cron's own DDL."""
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
    SessionDB(read_only=False).close()
    return pg


def _files(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def _rows(dsn, table):
    with psycopg.connect(dsn) as raw:
        return raw.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _make_due(job_id):
    """Put a created job's next run half a minute in the past."""
    with cron_jobs._jobs_lock():
        stored = cron_jobs.load_jobs()
        for job in stored:
            if job["id"] == job_id:
                job["next_run_at"] = (hermes_now() - timedelta(seconds=30)).isoformat()
        cron_jobs.save_jobs(stored)


# --------------------------------------------------------------------------
# (a) round trip on PostgreSQL, no file under cron/
# --------------------------------------------------------------------------


def test_authority_roundtrips_the_three_cron_stores_without_files(
    authority, _cron_home, monkeypatch
):
    first = cron_jobs.create_job(prompt="first", schedule="every 1h", name="one")
    second = cron_jobs.create_job(prompt="second", schedule="every 2h", name="two")
    assert [j["id"] for j in cron_jobs.load_jobs()] == [first["id"], second["id"]]
    cron_jobs.update_job(second["id"], {"name": "zwei"})
    assert cron_jobs.get_job(second["id"])["name"] == "zwei"
    assert cron_jobs.pause_job(first["id"])["state"] == "paused"
    with psycopg.connect(authority) as raw:
        stored = raw.execute(
            "SELECT id, position, job::jsonb ->> 'name' FROM core_cron_jobs ORDER BY position"
        ).fetchall()
    assert stored == [(first["id"], 0, "one"), (second["id"], 1, "zwei")]

    # A stale writer (loaded before a concurrent create) must not drop it.
    stale = cron_jobs.load_jobs()
    third = cron_jobs.create_job(prompt="third", schedule="every 3h", name="three")
    cron_jobs.save_jobs(stale)
    assert third["id"] in {j["id"] for j in cron_jobs.load_jobs()}

    notepad.set_note(first["id"], "cursor", "42")
    notepad.set_note(first["id"], "wm", "ü" * 10)
    assert notepad.get_note(first["id"], "cursor") == "42"
    assert [n["key"] for n in notepad.list_notes(first["id"])] == ["cursor", "wm"]
    monkeypatch.setattr(notepad, "MAX_JOB_TOTAL_BYTES", 30)
    with pytest.raises(ValueError, match="notepad full"):
        notepad.set_note(first["id"], "big", "x" * 10)  # 3 + 10 + 22 bytes of ü > 30
    assert notepad.delete_note(first["id"], "cursor") is True
    assert notepad.get_note(first["id"], "cursor") is None

    monkeypatch.setattr(executions, "MAX_TERMINAL_EXECUTIONS", 1)
    records = [
        executions.create_execution(first["id"], source="builtin") for _ in range(3)
    ]
    assert executions.mark_execution_running(records[0]["id"])["status"] == "running"
    assert executions.mark_execution_running(records[0]["id"]) is None
    for record in records:
        executions.finish_execution(record["id"], success=True)
    assert executions.finish_execution(records[0]["id"], success=False) is None
    history = executions.list_executions(job_id=first["id"])
    assert (
        len(history) == 1 and history[0]["status"] == "completed"
    )  # pruned via LIMIT ALL
    assert (
        executions.latest_executions([first["id"]])[first["id"]]["id"]
        == history[0]["id"]
    )
    listed = {j["id"]: j for j in cron_jobs.list_jobs(include_disabled=True)}
    assert listed[first["id"]]["latest_execution"]["id"] == history[0]["id"]

    incident_id, is_new = incidents.upsert_incident(first["id"], "boom: 401")
    assert is_new and incidents.upsert_incident(first["id"], "boom: 401")[1] is False
    assert incidents.ack_incident(incident_id) is True
    assert [i["state"] for i in incidents.list_incidents()] == ["closed"]

    assert cron_jobs.remove_job(first["id"]) is True
    assert notepad.list_notes(first["id"]) == []  # clear_notepad reached PostgreSQL
    assert {j["id"] for j in cron_jobs.load_jobs()} == {second["id"], third["id"]}
    assert _rows(authority, "core_cron_executions") == 1
    assert _rows(authority, "core_cron_incidents") == 1
    assert _files(_cron_home / "cron") == []


def test_authority_curator_backup_reads_jobs_from_postgres(authority, tmp_path):
    from agent.curator_backup import _backup_cron_jobs_into

    cron_jobs.create_job(prompt="kept", schedule="every 1h", name="kept")
    info = _backup_cron_jobs_into(tmp_path)
    assert info["backed_up"] is True and info["jobs_count"] == 1
    assert '"kept"' in (tmp_path / "cron-jobs.json").read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# (e) authority without PostgreSQL fails loudly and writes no file
# --------------------------------------------------------------------------


def test_authority_without_postgres_raises_and_creates_no_file(monkeypatch, _cron_home):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    from cron.scheduler import tick

    for call in (
        cron_jobs.load_jobs,
        lambda: cron_jobs.create_job(prompt="x", schedule="every 1h"),
        lambda: cron_jobs.claim_job_for_fire("job"),
        lambda: notepad.set_note("job", "k", "v"),
        lambda: notepad.clear_notepad("job"),
        lambda: executions.create_execution("job", source="builtin"),
        lambda: executions.adopt_claimed_execution("attempt"),
        executions.recover_interrupted_executions,
        lambda: incidents.upsert_incident("job", "boom"),
        lambda: tick(verbose=False),
    ):
        with pytest.raises(aux.AuxStoreUnavailable) as caught:
            call()
        assert "nonexistent-0060" not in str(caught.value)
    assert _files(_cron_home / "cron") == []


# --------------------------------------------------------------------------
# (f) every other backend keeps the files and flocks
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_cron_files_and_flocks(
    monkeypatch, _cron_home, backend
):
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def no_postgres(*_args, **_kwargs):
        raise AssertionError("a non-authority profile must not reach PostgreSQL")

    monkeypatch.setattr(aux, "_open_postgres", no_postgres)
    monkeypatch.setattr(aux, "_connect_lock_session", no_postgres)
    from cron.scheduler import tick

    job = cron_jobs.create_job(prompt="x", schedule="every 1h", name="file")
    notepad.set_note(job["id"], "k", "v")
    record = executions.create_execution(job["id"], source="builtin")
    executions.finish_execution(record["id"], success=True)
    assert cron_jobs.claim_job_for_fire(job["id"]) is True
    assert tick(verbose=False) == 0

    files = _files(_cron_home / "cron")
    for name in (
        "jobs.json",
        "notepad.db",
        "executions.db",
        ".jobs.lock",
        ".tick.lock",
    ):
        assert name in files
    assert any(f.startswith(".fire-") and f.endswith(".lock") for f in files)
    assert "lease_expires_at" not in executions.list_executions()[0]


# --------------------------------------------------------------------------
# Two processes on one profile (= two overlapping pods). Children are spawned,
# so they inherit this test's environment (HERMES_HOME + authority DSN).
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")


def _child(target, events, *args):
    """Process entry point: run *target*, report any failure as an event."""
    try:
        target(*args, events)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


def _spawn(target, events, *args):
    process = _SPAWN.Process(target=_child, args=(target, events, *args))
    process.start()
    return process


def _await(events, done, *, timeout=120):
    """Collect ``(kind, value)`` events until ``done(events)`` holds."""
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


def _tick_child(start, events):
    import cron.scheduler as scheduler
    from cron.executions import finish_execution

    real_get_due_jobs = scheduler.get_due_jobs

    def scanning():
        events.put(("scan", os.getpid()))
        return real_get_due_jobs()

    def run_one_job(job, **_kwargs):
        # Stands in for the agent run; long enough that the other process's
        # tick lands while this one still holds the tick lock (sync=True).
        events.put(("run", job["id"]))
        time.sleep(3)
        finish_execution(job["execution_id"], success=True)
        return True

    scheduler.get_due_jobs = scanning
    scheduler.run_one_job = run_one_job
    events.put(("ready", os.getpid()))
    start.wait(60)
    events.put(("ticked", scheduler.tick(verbose=False, sync=True)))


def test_two_processes_ticking_one_profile_run_a_due_job_once(authority, _cron_home):
    """(b) Overlapping ticks: one scans and runs, the other returns 0 untouched."""
    job = cron_jobs.create_job(prompt="once", schedule="every 1h", name="tick")
    _make_due(job["id"])
    events, start = _SPAWN.Queue(), _SPAWN.Event()
    processes = [_spawn(_tick_child, events, start) for _ in range(2)]
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        start.set()
        got = _await(events, lambda got: len(_kinds(got, "ticked")) == 2)
    finally:
        _stop(processes)

    assert sorted(_kinds(got, "ticked")) == [0, 1]
    assert len(_kinds(got, "scan")) == 1  # the loser never got past the tick lock
    assert _kinds(got, "run") == [job["id"]]
    assert _rows(authority, "core_cron_executions") == 1
    assert [e["status"] for e in executions.list_executions()] == ["completed"]
    assert not any(
        f.endswith((".lock", ".db", "jobs.json")) for f in _files(_cron_home)
    )


def _claim_child(job_id, start, hold, events):
    events.put(("ready", os.getpid()))
    start.wait(60)
    events.put(("claimed", bool(cron_jobs.claim_job_for_fire(job_id))))
    # Stay alive like a pod would: pg3 releases a claim whose same-host owner pid has exited,
    # and both children share this host.
    hold.wait(60)


def test_two_processes_claiming_one_due_job_fire_it_once(authority, _cron_home):
    """(c) Concurrent claim of the same due job: exactly one process wins."""
    job = cron_jobs.create_job(prompt="once", schedule="every 1h", name="claim")
    _make_due(job["id"])
    events, start, hold = _SPAWN.Queue(), _SPAWN.Event(), _SPAWN.Event()
    processes = [_spawn(_claim_child, events, job["id"], start, hold) for _ in range(2)]
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        start.set()
        got = _await(events, lambda got: len(_kinds(got, "claimed")) == 2)
    finally:
        hold.set()
        _stop(processes)

    assert sorted(_kinds(got, "claimed")) == [False, True]
    assert cron_jobs.get_job(job["id"])["fire_claim"]["by"]


def _fence_child(job_id, owner, release, events):
    with cron_jobs.fire_claim_fence(job_id, expected_owner=owner) as owns:
        events.put(("fenced", owns))
        release.wait(60)  # the network send the fence must cover
    events.put(("released", os.getpid()))


def test_fire_fence_holds_across_processes_and_dies_with_its_holder(
    authority, monkeypatch
):
    """(c) The fire fence spans the send in another process; a killed holder frees it."""
    job = cron_jobs.create_job(prompt="send", schedule="every 1h", name="fence")
    owner = cron_jobs.claim_job_for_fire(job["id"], return_job=True)["fire_claim"]["by"]
    monkeypatch.setattr(cron_jobs, "_JOBS_LOCK_TIMEOUT_SECONDS", 0.5)

    events, release = _SPAWN.Queue(), _SPAWN.Event()
    holder = _spawn(_fence_child, events, job["id"], owner, release)
    try:
        assert _kinds(_await(events, lambda got: bool(got)), "fenced") == [True]
        # Another pod cannot finish the run or heartbeat while the send is out.
        assert (
            cron_jobs.mark_job_run(job["id"], True, expected_fire_owner=owner) is False
        )
        assert cron_jobs.get_job(job["id"])["fire_claim"]["by"] == owner
        release.set()
        _await(events, lambda got: bool(_kinds(got, "released")))
    finally:
        _stop([holder])
    assert cron_jobs.mark_job_run(job["id"], True, expected_fire_owner=owner) is True

    never = _SPAWN.Event()
    crashed = _spawn(_fence_child, events, job["id"], "anyone", never)
    try:
        _await(events, lambda got: bool(_kinds(got, "fenced")))
        with cron_jobs._fire_job_lock(job["id"]) as acquired:
            assert acquired is False
        crashed.kill()  # dies mid-send, never unlocks
        crashed.join(10)
        deadline = time.monotonic() + 10
        while True:
            with cron_jobs._fire_job_lock(job["id"]) as acquired:
                if acquired:
                    break
            assert time.monotonic() < deadline, (
                "the dead holder's fence was not released"
            )
    finally:
        _stop([crashed])


def test_fire_fence_blocks_the_claim_heartbeat_of_another_process(authority, monkeypatch):
    """Original (c) assertion: ``heartbeat_fire_claim`` is False while another pod holds the
    fence."""
    pytest.skip(
        "levos/pg3 deliberately takes heartbeat_fire_claim out of the fire fence (the run "
        "thread holds the fence across delivery and the heartbeat thread would report a false "
        "ownership loss); it is serialized by _jobs_lock only, on every backend"
    )
    job = cron_jobs.create_job(prompt="send", schedule="every 1h", name="fence")
    owner = cron_jobs.claim_job_for_fire(job["id"], return_job=True)["fire_claim"]["by"]
    monkeypatch.setattr(cron_jobs, "_JOBS_LOCK_TIMEOUT_SECONDS", 0.5)
    events, release = _SPAWN.Queue(), _SPAWN.Event()
    holder = _spawn(_fence_child, events, job["id"], owner, release)
    try:
        assert _kinds(_await(events, lambda got: bool(got)), "fenced") == [True]
        assert cron_jobs.heartbeat_fire_claim(job["id"], expected_owner=owner) is False
        release.set()
        _await(events, lambda got: bool(_kinds(got, "released")))
    finally:
        _stop([holder])


def _create_child(count, start, events):
    events.put(("ready", os.getpid()))
    start.wait(60)
    ids = [
        cron_jobs.create_job(prompt=f"p{i}", schedule="every 1h")["id"]
        for i in range(count)
    ]
    events.put(("created", ids))


def test_two_processes_creating_jobs_lose_none(authority):
    """Jobs lock: concurrent load→modify→save sections from two pods never drop a
    job, and two pods creating the cron table at once do not collide."""
    events, start = _SPAWN.Queue(), _SPAWN.Event()
    processes = [_spawn(_create_child, events, 15, start) for _ in range(2)]
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        start.set()
        got = _await(events, lambda got: len(_kinds(got, "created")) == 2)
    finally:
        _stop(processes)
    created = {job_id for ids in _kinds(got, "created") for job_id in ids}
    assert len(created) == 30
    assert {j["id"] for j in cron_jobs.load_jobs()} == created


# --------------------------------------------------------------------------
# (d) execution ownership is a lease, not a pid
# --------------------------------------------------------------------------


def _owner_child(stop_renewing, events):
    executions.LEASE_SECONDS = 2.0
    executions.LEASE_RENEW_SECONDS = 0.5
    record = executions.create_execution("leased", source="builtin")
    executions.mark_execution_running(record["id"])
    events.put(("running", record["id"]))
    stop_renewing.wait(120)
    executions._stop_lease_renewer()  # alive, but no longer vouching for the run
    events.put(("silent", record["id"]))
    time.sleep(120)


def test_lease_keeps_a_renewing_owner_and_expires_a_silent_or_dead_one(
    authority, monkeypatch
):
    def no_pid_probe(*_args):
        raise AssertionError("authority recovery must not probe pids or /proc")

    monkeypatch.setattr(executions, "_owner_is_live", no_pid_probe)
    events = _SPAWN.Queue()
    silent_signal, killed_signal = _SPAWN.Event(), _SPAWN.Event()
    silent = _spawn(_owner_child, events, silent_signal)
    killed = _spawn(_owner_child, events, killed_signal)
    try:
        got = _await(events, lambda got: len(_kinds(got, "running")) == 2)
        running = set(_kinds(got, "running"))
        for _ in range(3):  # 4.5s: more than two lease lengths
            time.sleep(1.5)
            assert executions.recover_interrupted_executions() == 0
        assert {e["status"] for e in executions.list_executions(job_id="leased")} == {
            "running"
        }

        silent_signal.set()
        _await(events, lambda got: bool(_kinds(got, "silent")))
        killed.kill()  # the pod dies mid-run
        killed.join(10)
        time.sleep(2.5)
        assert silent.is_alive()
        assert executions.recover_interrupted_executions() == 2
    finally:
        silent.kill()
        _stop([silent, killed])
    rows = executions.list_executions(job_id="leased")
    assert {e["id"] for e in rows} == running
    assert {e["status"] for e in rows} == {"unknown"}
    assert all("lease" in e["error"] for e in rows)


def _handoff_child(done, events):
    executions.LEASE_SECONDS = 1.0
    executions.LEASE_RENEW_SECONDS = 0.25
    handed = []
    for _ in range(2):
        record = executions.create_execution("handoff", source="builtin")
        assert executions.mark_execution_handoff_pending(record["id"]) is not None
        handed.append(record["id"])
    executions._stop_lease_renewer()  # the dispatcher goes away after spawning the worker
    events.put(("handed", handed))
    done.wait(120)


def test_lease_follows_a_handoff_and_spares_it_during_the_adoption_grace(
    authority, monkeypatch
):
    """pg3 lifecycle: a dispatcher hands a claimed attempt to a worker in another process. The
    adopting worker owns the lease from then on; an unadopted handoff whose dispatcher stopped
    renewing stays claimed through the adoption grace (server clock) and only then turns
    unknown."""

    def no_pid_probe(*_args):
        raise AssertionError("authority recovery must not probe pids or /proc")

    monkeypatch.setattr(executions, "_owner_is_live", no_pid_probe)
    monkeypatch.setattr(executions, "HANDOFF_ADOPTION_GRACE_SECONDS", 4.0)
    events, done = _SPAWN.Queue(), _SPAWN.Event()
    child = _spawn(_handoff_child, events, done)
    try:
        (handed,) = _kinds(_await(events, lambda got: bool(got)), "handed")
        started = time.monotonic()
        adopted, orphan = handed
        record = executions.adopt_claimed_execution(adopted)
        assert record["status"] == "running"
        assert record["process_id"] == executions._PROCESS_ID
        assert executions.adopt_claimed_execution(adopted) is None  # single adoption gate
        with psycopg.connect(authority) as raw:
            ahead = raw.execute(
                "SELECT lease_expires_at - EXTRACT(EPOCH FROM clock_timestamp())::float8 "
                "FROM core_cron_executions WHERE id = %s",
                (adopted,),
            ).fetchone()[0]
        assert ahead > executions.LEASE_SECONDS - 10  # the adopter's lease, not the child's

        time.sleep(1.5)  # the orphan's lease is out, its adoption grace is not
        assert executions.recover_interrupted_executions() == 0
        assert executions.get_execution(orphan)["status"] == "claimed"
        time.sleep(max(0.0, 4.5 - (time.monotonic() - started)))
        assert executions.recover_interrupted_executions() == 1
    finally:
        done.set()
        _stop([child])
    orphaned = executions.get_execution(orphan)
    assert orphaned["status"] == "unknown" and "lease" in orphaned["error"]
    assert orphaned["handoff_pending"] == 0 and orphaned["handoff_started_at"] is None
    assert executions.get_execution(adopted)["status"] == "running"
    assert executions.finish_execution(adopted, success=True)["status"] == "completed"


# --------------------------------------------------------------------------
# (g) one-shot move of the cron files into PostgreSQL
# --------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _seed_cron_files(home: Path) -> dict:
    job = cron_jobs.create_job(prompt="old", schedule="every 1h", name="from-file")
    other = cron_jobs.create_job(prompt="other", schedule="every 2h", name="other")
    notepad.set_note(job["id"], "cursor", "7")
    executions.create_execution(job["id"], source="builtin")  # left open
    done = executions.create_execution(other["id"], source="builtin")
    executions.finish_execution(done["id"], success=True)
    incidents.upsert_incident(other["id"], "boom: 401 unauthorized")
    paths = {
        "cron_executions": home / "cron" / "executions.db",
        "cron_notepad": home / "cron" / "notepad.db",
        "cron_jobs": home / "cron" / "jobs.json",
    }
    for name in ("cron_executions", "cron_notepad"):
        conn = sqlite3.connect(paths[name])
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    return {"job": job, "other": other, "paths": paths}


def test_cron_migration_is_idempotent_and_leaves_the_sources_untouched(
    pg, monkeypatch, _cron_home
):
    with monkeypatch.context() as before_cutover:  # the old pod owned these
        before_cutover.setattr(executions, "_PROCESS_ID", "pre-cutover-pod")
        seeded = _seed_cron_files(_cron_home)
    paths = seeded["paths"]
    digests = {name: _sha256(path) for name, path in paths.items()}
    monkeypatch.setenv("HERMES_PROFILE", "custom")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)

    # Since the cutover the live store already rewrote one job: PostgreSQL wins.
    with cron_jobs._jobs_lock():
        cron_jobs.save_jobs([dict(seeded["other"], name="live")])

    dry = aux.migrate_cron_to_pg("custom", dry_run=True)
    assert {s["status"] for s in dry["stores"].values()} == {"dry_run"}
    assert dry["stores"]["cron_jobs"]["tables"]["jobs"]["inserted"] == 1
    assert dry["stores"]["cron_executions"]["tables"]["executions"]["inserted"] == 2
    assert dry["stores"]["cron_executions"]["tables"]["cron_incidents"]["inserted"] == 1
    assert _rows(pg, "core_cron_executions") == 0
    assert _rows(pg, "core_cron_incidents") == 0
    assert _rows(pg, "core_cron_jobs") == 1

    first = aux.migrate_cron_to_pg("custom", dry_run=False)
    counts = {table: _rows(pg, table) for table in CRON_TABLES}
    second = aux.migrate_cron_to_pg("custom", dry_run=False)
    assert {table: _rows(pg, table) for table in CRON_TABLES} == counts
    assert counts == {
        "core_cron_executions": 2,
        "core_cron_incidents": 1,
        "core_cron_notes": 1,
        "core_cron_jobs": 2,
    }
    # The ledgers were empty in PostgreSQL: every source row landed, one row each.
    for store, table in (
        ("cron_executions", "executions"),
        ("cron_executions", "cron_incidents"),
        ("cron_notepad", "cron_notepad"),
    ):
        copied = first["stores"][store]["tables"][table]
        assert copied["source_rows"] == copied["inserted"] == copied["target_rows_after"]
    # jobs.json: 2 source jobs; the one PostgreSQL already held is not duplicated.
    jobs_copy = first["stores"]["cron_jobs"]["tables"]["jobs"]
    assert (jobs_copy["source_rows"], jobs_copy["inserted"]) == (2, 1)
    assert jobs_copy["target_rows_after"] == jobs_copy["source_rows"] == 2
    for store in second["stores"].values():
        for table in store["tables"].values():
            assert table["inserted"] == 0
            assert table["target_rows_after"] >= table["source_rows"]
    assert {name: first["stores"][name]["sha256"] for name in paths} == digests
    assert {name: _sha256(path) for name, path in paths.items()} == digests

    names = {j["id"]: j["name"] for j in cron_jobs.load_jobs()}
    assert names == {seeded["job"]["id"]: "from-file", seeded["other"]["id"]: "live"}
    assert notepad.get_note(seeded["job"]["id"], "cursor") == "7"
    # The attempt left open in the file carries no lease: recovery closes it.
    assert executions.recover_interrupted_executions() == 1
    assert {e["status"] for e in executions.list_executions()} == {
        "unknown",
        "completed",
    }


def _source_counts(paths) -> dict:
    """Entries in the cron source files, counted independently of the migration code."""
    counts = {}
    for name, table in (
        ("cron_executions", "executions"),
        ("cron_executions", "cron_incidents"),
        ("cron_notepad", "cron_notepad"),
    ):
        conn = sqlite3.connect(f"file:{paths[name]}?mode=ro", uri=True)
        try:
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        finally:
            conn.close()
    document = json.loads(paths["cron_jobs"].read_text(encoding="utf-8"))
    counts["jobs"] = len(document["jobs"])
    return counts


def test_cron_migration_lands_exactly_the_source_entry_count_in_postgres(
    pg, monkeypatch, _cron_home
):
    """Card t_36694f21: the number of entries the migration starts from (jobs in jobs.json,
    execution rows, incident rows, notepad rows) equals the number of rows that land in
    PostgreSQL — reported by the migration and counted in PostgreSQL itself."""
    with monkeypatch.context() as before_cutover:
        before_cutover.setattr(executions, "_PROCESS_ID", "pre-cutover-pod")
        seeded = _seed_cron_files(_cron_home)
        extra = cron_jobs.create_job(prompt="third", schedule="every 3h", name="third")
        notepad.set_note(extra["id"], "a", "1")
        notepad.set_note(extra["id"], "b", "ü")
        for _ in range(3):
            record = executions.create_execution(extra["id"], source="builtin")
            executions.finish_execution(record["id"], success=False, error="boom")
        incidents.upsert_incident(extra["id"], "timed out")
    paths = seeded["paths"]
    for name in ("cron_executions", "cron_notepad"):
        conn = sqlite3.connect(paths[name])
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    source = _source_counts(paths)
    assert source == {"executions": 5, "cron_incidents": 2, "cron_notepad": 3, "jobs": 3}
    monkeypatch.setenv("HERMES_PROFILE", "custom")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)

    report = aux.migrate_cron_to_pg("custom", dry_run=False)
    landed = {
        "executions": _rows(pg, "core_cron_executions"),
        "cron_incidents": _rows(pg, "core_cron_incidents"),
        "cron_notepad": _rows(pg, "core_cron_notes"),
        "jobs": _rows(pg, "core_cron_jobs"),
    }
    assert landed == source
    reported = {
        table: copied
        for store in report["stores"].values()
        for table, copied in store["tables"].items()
    }
    assert set(reported) == set(source)
    for table, count in source.items():
        assert reported[table]["source_rows"] == count, table
        assert reported[table]["inserted"] == count, table
        assert reported[table]["target_rows_before"] == 0, table
        assert reported[table]["target_rows_after"] == count, table
    assert {j["id"] for j in cron_jobs.load_jobs()} == {
        seeded["job"]["id"], seeded["other"]["id"], extra["id"]
    }
    assert _source_counts(paths) == source  # the sources are untouched


def test_cron_migration_rolls_back_bad_jobs_and_reports_missing_files(
    authority, monkeypatch, _cron_home
):
    monkeypatch.setenv("HERMES_PROFILE", "custom")
    report = aux.migrate_cron_to_pg("custom", dry_run=False)
    assert {s["status"] for s in report["stores"].values()} == {"missing"}
    with pytest.raises(ValueError, match="not the active profile"):
        aux.migrate_cron_to_pg("dave", dry_run=True)

    live = cron_jobs.create_job(prompt="live", schedule="every 1h", name="live")
    source = _cron_home / "cron" / "jobs.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    twice = {"id": "dup", "name": "a", "schedule": {"kind": "interval", "minutes": 5}}
    source.write_text(
        '{"jobs": [%s, %s]}' % (json.dumps(twice), json.dumps(dict(twice, name="b"))),
        encoding="utf-8",
    )
    digest = _sha256(source)
    with pytest.raises(aux.AuxMigrationError, match="duplicate cron job id"):
        aux.migrate_cron_to_pg("custom", dry_run=False)
    assert _sha256(source) == digest
    source.write_text('{"jobs": [{"name": "no id"}]}', encoding="utf-8")
    with pytest.raises(aux.AuxMigrationError, match="without an id"):
        aux.migrate_cron_to_pg("custom", dry_run=False)
    assert [j["id"] for j in cron_jobs.load_jobs()] == [live["id"]]
    assert _rows(authority, "core_cron_jobs") == 1
