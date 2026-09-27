"""levos 0070 — core schema creation and migration run under a PostgreSQL lock.

Every writable PostgreSQL open (``SessionDB``, and through it every 0059/0060
auxiliary store open) runs ``init_postgres_schema``: base DDL, the pg-only
migration ledger, the column reconciler and the v18 width repair. Two pods of
one profile share nothing but the profile's PostgreSQL schema, so when they
overlap (first boot on an empty schema, or a new image carrying a migration
next to the old pod) two processes run that DDL at once. The run is now
serialized per profile schema by a session advisory lock on the DDL
connection itself; a waiter polls, so a holder's ``CREATE INDEX CONCURRENTLY``
never waits on it.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips). Children are spawned processes that share only the DSN — each
has its own ``HERMES_HOME``, like two pods with their own disks.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
import time
from pathlib import Path

import psycopg
import pytest

import hermes_aux_store as aux
import hermes_state_postgres as hsp
from hermes_state_common import SCHEMA_VERSION
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

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
_LOCK = "schema:core"


@pytest.fixture(autouse=True)
def _no_backend_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def profile_dsn(postgres_dsn, request):
    """A DSN whose search path is a fresh, empty profile schema.

    ``public`` stays on the path behind it so the database-wide pg_trgm
    extension (installed once here) resolves for the v18 GIN indexes.
    """
    schema = "c20_" + request.node.name.split("[")[0][-40:].lower()
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        raw.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.execute(f"CREATE SCHEMA {schema}")
    return psycopg.conninfo.make_conninfo(
        postgres_dsn, options=f"-c search_path={schema},public"
    )


def _query(dsn, sql):
    with psycopg.connect(dsn, autocommit=True) as raw:
        return raw.execute(sql).fetchall()


def _advisory_locks(dsn):
    [(count,)] = _query(
        dsn, "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory'"
    )
    return count


# --------------------------------------------------------------------------
# Child processes (= pods). Each gets its own HERMES_HOME and only the DSN.
# --------------------------------------------------------------------------


def _child(target, events, home, *args):
    os.environ["HERMES_HOME"] = str(home)
    try:
        target(*args, events)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))


def _spawn(target, events, home, *args):
    Path(home).mkdir(parents=True, exist_ok=True)
    process = _SPAWN.Process(target=_child, args=(target, events, str(home), *args))
    process.start()
    return process


def _await(events, done, *, timeout=120):
    got = []
    deadline = time.monotonic() + timeout
    while not done(got):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out; events so far: {got}"
        got.append(events.get(timeout=remaining))
    return got


def _kinds(got, kind):
    return [value for event, value in got if event == kind]


def _finished(count):
    return lambda got: len(_kinds(got, "done")) + len(_kinds(got, "error")) == count


def _stop(processes):
    for process in processes:
        process.join(30)
        if process.is_alive():
            process.kill()
            process.join(5)


def _init_child(dsn, start, events):
    conn = hsp.connect_postgres(dsn)
    events.put(("ready", os.getpid()))
    start.wait(60)
    try:
        hsp.init_postgres_schema(conn, SCHEMA_VERSION)
    finally:
        conn.close()
    events.put(("done", os.getpid()))


# A migration the "new image" carries. Its body is not idempotent on purpose:
# the trace shows how many times it actually ran and whether runs overlapped.
_NEW_MIGRATION_SQL = (
    "INSERT INTO c20_trace (pid, step) VALUES (pg_backend_pid(), 'start');\n"
    "SELECT pg_sleep(1.5);\n"
    "INSERT INTO c20_trace (pid, step) VALUES (pg_backend_pid(), 'end')"
)


def _new_image_child(dsn, start, events):
    hsp._PG_ONLY_MIGRATIONS = [
        *hsp._PG_ONLY_MIGRATIONS,
        hsp.PostgresMigration(version=9070, sql=_NEW_MIGRATION_SQL),
    ]
    _init_child(dsn, start, events)


def _holder_child(dsn, release, events):
    conn = hsp.connect_postgres(dsn)
    with aux.aux_connection_lock(conn, _LOCK, wait_seconds=5):
        events.put(("held", os.getpid()))
        release.wait(120)
    conn.close()
    events.put(("done", os.getpid()))


# --------------------------------------------------------------------------
# (1) two pods boot on an empty profile schema at the same moment
# --------------------------------------------------------------------------


def test_two_processes_creating_an_empty_schema_both_open_it(profile_dsn, tmp_path):
    """Unlocked, one of the two first opens dies on a catalog unique violation
    (``pg_type`` / ``schema_version``) and that pod's SessionDB fails to open."""
    events, start = _SPAWN.Queue(), _SPAWN.Event()
    processes = [
        _spawn(_init_child, events, tmp_path / f"pod{n}", profile_dsn, start)
        for n in range(2)
    ]
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        start.set()
        got = _await(events, _finished(2))
    finally:
        _stop(processes)

    assert _kinds(got, "error") == []
    assert _query(profile_dsn, "SELECT version FROM schema_version") == [
        (SCHEMA_VERSION,)
    ]
    recorded = sorted(
        v for (v,) in _query(profile_dsn, "SELECT version FROM pg_migration_version")
    )
    assert recorded == sorted(m.version for m in hsp._PG_ONLY_MIGRATIONS)
    invalid = _query(
        profile_dsn,
        "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
        " WHERE NOT i.indisvalid",
    )
    assert invalid == []  # no CONCURRENTLY build was aborted half-way
    assert _advisory_locks(profile_dsn) == 0
    # The fence is not a file on either pod's disk.
    pods = [tmp_path / "pod0", tmp_path / "pod1"]
    assert not [p for pod in pods for p in pod.rglob("*.lock")]


# --------------------------------------------------------------------------
# (2) a new image with a pending migration overlaps the old pod
# --------------------------------------------------------------------------


def test_two_processes_with_a_new_migration_run_it_once(profile_dsn, tmp_path):
    """Unlocked, both processes see the migration pending and run its body
    concurrently — twice, interleaved."""
    conn = hsp.connect_postgres(profile_dsn)
    hsp.init_postgres_schema(conn, SCHEMA_VERSION)  # the running profile's schema
    conn.execute(
        "CREATE TABLE c20_trace (pid INTEGER, step TEXT, at TIMESTAMPTZ DEFAULT clock_timestamp())"
    )
    conn.close()

    events, start = _SPAWN.Queue(), _SPAWN.Event()
    processes = [
        _spawn(_new_image_child, events, tmp_path / f"pod{n}", profile_dsn, start)
        for n in range(2)
    ]
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        start.set()
        got = _await(events, _finished(2))
    finally:
        _stop(processes)

    assert _kinds(got, "error") == []
    trace = _query(profile_dsn, "SELECT step FROM c20_trace ORDER BY at")
    assert trace == [("start",), ("end",)]
    assert _query(
        profile_dsn, "SELECT COUNT(*) FROM pg_migration_version WHERE version = 9070"
    ) == [(1,)]
    assert _advisory_locks(profile_dsn) == 0


# --------------------------------------------------------------------------
# (3) the lock's edges: dead holder, timeout, failure, other profiles
# --------------------------------------------------------------------------


def test_a_killed_holder_releases_the_schema_lock(profile_dsn, tmp_path):
    events, release = _SPAWN.Queue(), _SPAWN.Event()
    holder = _spawn(_holder_child, events, tmp_path / "old", profile_dsn, release)
    try:
        _await(events, lambda got: _kinds(got, "held"))
        os.kill(holder.pid, signal.SIGKILL)
        holder.join(10)
        conn = hsp.connect_postgres(profile_dsn)
        started = time.monotonic()
        hsp.init_postgres_schema(conn, SCHEMA_VERSION)
        conn.close()
    finally:
        _stop([holder])
    assert time.monotonic() - started < 5
    assert _query(profile_dsn, "SELECT version FROM schema_version") == [
        (SCHEMA_VERSION,)
    ]


def test_a_held_lock_times_out_loudly_and_spares_other_profiles(
    postgres_dsn, profile_dsn, tmp_path, monkeypatch
):
    other = psycopg.conninfo.make_conninfo(
        postgres_dsn, options="-c search_path=c20_other,public"
    )
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP SCHEMA IF EXISTS c20_other CASCADE")
        raw.execute("CREATE SCHEMA c20_other")
    monkeypatch.setattr(hsp, "SCHEMA_LOCK_WAIT_SECONDS", 1.0)
    events, release = _SPAWN.Queue(), _SPAWN.Event()
    holder = _spawn(_holder_child, events, tmp_path / "old", profile_dsn, release)
    try:
        _await(events, lambda got: _kinds(got, "held"))
        conn = hsp.connect_postgres(profile_dsn)
        for run in (
            lambda: hsp.init_postgres_schema(conn, SCHEMA_VERSION),
            lambda: hsp.finalize_postgres_schema(conn),
        ):
            with pytest.raises(aux.AuxStoreUnavailable) as raised:
                run()
            assert "schema:core" in str(raised.value)
            assert "host=" not in str(raised.value) and "/" not in str(raised.value)
        # Nothing ran while it waited, and the waiter holds no lock afterwards.
        assert _query(
            profile_dsn,
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()",
        ) == [(0,)]
        # Another profile (schema) of the same database is not blocked.
        other_conn = hsp.connect_postgres(other)
        hsp.init_postgres_schema(other_conn, SCHEMA_VERSION)
        other_conn.close()
        release.set()
        _await(events, _finished(1))
        hsp.init_postgres_schema(conn, SCHEMA_VERSION)
        conn.close()
    finally:
        release.set()
        _stop([holder])
    assert _advisory_locks(profile_dsn) == 0


def test_a_failing_migration_raises_and_releases_the_lock(profile_dsn, monkeypatch):
    conn = hsp.connect_postgres(profile_dsn)
    hsp.init_postgres_schema(conn, SCHEMA_VERSION)
    monkeypatch.setattr(
        hsp,
        "_PG_ONLY_MIGRATIONS",
        [
            *hsp._PG_ONLY_MIGRATIONS,
            hsp.PostgresMigration(version=9071, sql="SELECT 1/0"),
        ],
    )
    with pytest.raises(psycopg.errors.DivisionByZero):
        hsp.init_postgres_schema(conn, SCHEMA_VERSION)
    assert _advisory_locks(profile_dsn) == 0  # the same, still-open session let go
    monkeypatch.setattr(hsp, "SCHEMA_LOCK_WAIT_SECONDS", 1.0)
    monkeypatch.setattr(hsp, "_PG_ONLY_MIGRATIONS", hsp._PG_ONLY_MIGRATIONS[:-1])
    other = hsp.connect_postgres(profile_dsn)
    hsp.init_postgres_schema(other, SCHEMA_VERSION)
    other.close()
    conn.close()


# --------------------------------------------------------------------------
# (4) installations that never open PostgreSQL are untouched
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite"])
def test_sqlite_installs_never_take_the_schema_lock(backend, tmp_path, monkeypatch):
    from hermes_state import SessionDB

    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("a SQLite install took the PostgreSQL schema lock")

    monkeypatch.setattr(aux, "aux_connection_lock", forbidden)
    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        assert not getattr(db, "_is_postgres", False)
    finally:
        db.close()
    assert (tmp_path / "state.db").is_file()
