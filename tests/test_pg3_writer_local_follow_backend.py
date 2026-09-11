"""PG3 0051 — writer-local tables follow the profile backend.

``SessionDB.open_writer`` (``gateway.delivery_ledger`` / ``tools.async_delegation``)
must write ``delivery_obligations`` / ``async_delegations`` into the profile's
PostgreSQL store on a PostgreSQL-authority profile and into ``state.db`` on a
SQLite profile; the only observable difference is where the rows live.

Needs the patched fork core on ``PYTHONPATH``, ``psycopg``, and ``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN`` — the same ephemeral, Unix-socket
only PostgreSQL recipe as ``test_pg3_authority_percent_binding.py``. Missing
dependencies are errors, never skips, and no external DSN is accepted.
"""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

import hermes_state_writer
import migrate_writer_local_tables
from gateway import delivery_ledger
from hermes_state import SessionDB
from hermes_state_postgres import SCHEMA_SQL_POSTGRES
from tools import async_delegation

if not hasattr(hermes_state_writer, "postgres_ddl"):  # pragma: no cover
    raise RuntimeError("hermes_state_writer on PYTHONPATH predates PG3 0051")

WRITER_TABLES = ("async_delegations", "delivery_obligations")
_STATE_ENV = (
    "HERMES_PROFILE",
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
)


def _run(arguments):
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"{Path(arguments[0]).name} failed: {result.stdout}{result.stderr}")


@pytest.fixture(scope="module")
def postgres_dsn(tmp_path_factory):
    """Ephemeral PostgreSQL on a private Unix socket; stopped and deleted after."""
    import psycopg

    binary_root = os.environ.get("PG3_PERCENT_PG_BIN", "")
    initdb = str(Path(binary_root) / "initdb") if binary_root else shutil.which("initdb")
    pg_ctl = str(Path(binary_root) / "pg_ctl") if binary_root else shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        raise RuntimeError("Isolated PostgreSQL tools required: initdb and pg_ctl")
    root = tmp_path_factory.mktemp("pg3-writer-local")
    data, socket = root / "data", root / "socket"
    socket.mkdir(mode=0o700)
    _run([initdb, "-D", str(data), "-A", "trust", "-U", "pg3_writer", "--no-locale",
          "--encoding=UTF8"])
    _run([pg_ctl, "-D", str(data), "-l", str(root / "postgres.log"), "-o",
          f"-F -c listen_addresses='' -c unix_socket_directories='{socket}' "
          "-c unix_socket_permissions=0700", "-w", "start"])
    try:
        yield psycopg.conninfo.make_conninfo(
            host=str(socket), dbname="postgres", user="pg3_writer", connect_timeout=5,
        )
    finally:
        _run([pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop"])


@pytest.fixture
def raw(postgres_dsn):
    import psycopg

    connection = psycopg.connect(postgres_dsn, autocommit=True)
    connection.execute("DROP TABLE IF EXISTS async_delegations, delivery_obligations")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def tree(tmp_path, monkeypatch, postgres_dsn, raw):
    """``<root>/profiles/{alpha,beta,gamma}``: SQLite, PostgreSQL authority, probe."""
    root = tmp_path / "hermes"
    for name in ("alpha", "beta", "gamma"):
        (root / "profiles" / name).mkdir(parents=True)
    (root / "profiles/beta/config.yaml").write_text("sessions:\n  state_backend: authority\n")
    (root / "profiles/beta/.env").write_text(f"HERMES_STATE_POSTGRES_DSN={postgres_dsn}\n")
    (root / "profiles/gamma/config.yaml").write_text("sessions:\n  state_backend: probe\n")
    (root / "profiles/gamma/.env").write_text(f"HERMES_CORE_PG_DSN={postgres_dsn}\n")
    for key in _STATE_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HERMES_HOME", str(root))
    return root


def _activate(monkeypatch, tree, profile, *, backend=None, dsn=None):
    """Run as *profile* the way the session-plane launcher does: HERMES_HOME
    points into the profile and the pod env selects the backend."""
    monkeypatch.setenv("HERMES_HOME", str(tree / "profiles" / profile))
    monkeypatch.setenv("HERMES_PROFILE", profile)
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)
    if dsn:
        monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", dsn)


def _sqlite_files(profile_dir: Path) -> list[str]:
    return sorted(p.name for p in profile_dir.iterdir() if p.name.startswith("state.db"))


def _count(raw, table: str) -> int:
    return raw.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _dispatch(delegation_id: str, **extra) -> None:
    record = {"delegation_id": delegation_id, "session_key": "sess", "dispatched_at": 1.0,
              "goal": f"goal-{delegation_id}"}
    record.update(extra)
    async_delegation._persist_dispatch(record)


# ---------------------------------------------------------------------------
# (1) SQLite profile: the file is created and written, PostgreSQL stays empty
# ---------------------------------------------------------------------------

def test_sqlite_profile_writes_the_state_file(tree, monkeypatch, raw):
    _activate(monkeypatch, tree, "alpha")
    delivery_ledger.record_obligation(
        obligation_id="o1", session_key="k", platform="telegram", chat_id="c",
        thread_id=None, content="hello",
    )
    _dispatch("d1")
    state = tree / "profiles/alpha/state.db"
    assert state.is_file()
    with sqlite3.connect(state) as sqlite_conn:
        assert sqlite_conn.execute("SELECT state FROM delivery_obligations").fetchall() == [("pending",)]
        assert sqlite_conn.execute("SELECT state FROM async_delegations").fetchall() == [("running",)]
    assert type(SessionDB.open_writer(
        state, timeout=10, initialize=delivery_ledger._initialize_schema,
    )) is sqlite3.Connection
    assert raw.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ANY(%s)",
        (list(WRITER_TABLES),),
    ).fetchone()[0] == 0


# ---------------------------------------------------------------------------
# (2) PostgreSQL-authority profile: rows land in PostgreSQL, no file appears
# ---------------------------------------------------------------------------

def test_authority_profile_writes_postgres_and_creates_no_state_file(
    tree, monkeypatch, raw, postgres_dsn,
):
    _activate(monkeypatch, tree, "beta", backend="authority", dsn=postgres_dsn)
    beta = tree / "profiles/beta"
    handle = SessionDB.open_writer(
        beta / "state.db", timeout=10, initialize=delivery_ledger._initialize_schema,
    )
    try:
        assert handle.is_postgres is True
        assert not isinstance(handle, sqlite3.Connection)
    finally:
        handle.close()
    delivery_ledger.record_obligation(
        obligation_id="o1", session_key="k", platform="telegram", chat_id="c",
        thread_id="t", content="hello",
    )
    _dispatch("d1")
    assert raw.execute(
        "SELECT obligation_id, state, thread_id FROM delivery_obligations"
    ).fetchall() == [("o1", "pending", "t")]
    assert raw.execute(
        "SELECT delegation_id, state, delivery_state FROM async_delegations"
    ).fetchall() == [("d1", "running", "pending")]
    assert _sqlite_files(beta) == []


def test_authority_without_a_dsn_fails_closed_and_creates_no_file(tree, monkeypatch):
    _activate(monkeypatch, tree, "beta", backend="authority")
    (tree / "profiles/beta/.env").unlink()
    with pytest.raises(RuntimeError, match="DSN"):
        delivery_ledger.record_obligation(
            obligation_id="o1", session_key="k", platform="telegram", chat_id="c",
            thread_id=None, content="hello",
        )
    assert _sqlite_files(tree / "profiles/beta") == []


# ---------------------------------------------------------------------------
# (3) A peer profile's path follows that profile's own config.yaml
# ---------------------------------------------------------------------------

def test_peer_profile_path_follows_that_profiles_config(tree, monkeypatch, raw):
    # Root-homed process (the bg_eventd / launcher shape): nothing in this
    # process's env selects PostgreSQL, only beta's own config.yaml + .env do.
    for profile, expect_postgres in (("alpha", False), ("beta", True), ("gamma", False)):
        handle = SessionDB.open_writer(
            tree / "profiles" / profile / "state.db", timeout=10,
            initialize=async_delegation._initialize_schema,
        )
        try:
            assert bool(getattr(handle, "is_postgres", False)) is expect_postgres, profile
            if expect_postgres:
                assert not isinstance(handle, sqlite3.Connection)
            else:
                assert type(handle) is sqlite3.Connection
        finally:
            handle.close()
    assert _sqlite_files(tree / "profiles/beta") == []
    assert _sqlite_files(tree / "profiles/alpha") == ["state.db"]
    assert _count(raw, "async_delegations") == 0


def test_explicit_file_outside_the_profile_tree_stays_sqlite(
    tree, monkeypatch, tmp_path, postgres_dsn,
):
    _activate(monkeypatch, tree, "beta", backend="authority", dsn=postgres_dsn)
    handle = SessionDB.open_writer(
        tmp_path / "elsewhere.db", timeout=10, initialize=delivery_ledger._initialize_schema,
    )
    try:
        assert type(handle) is sqlite3.Connection
    finally:
        handle.close()


# ---------------------------------------------------------------------------
# (4) The writers' real functions round-trip identically on both backends
# ---------------------------------------------------------------------------

def _delivery_script(monkeypatch) -> dict:
    dl = delivery_ledger
    out = {}
    record = lambda oid, content: dl.record_obligation(  # noqa: E731
        obligation_id=oid, session_key="k", platform="telegram", chat_id="c",
        thread_id=None, content=content,
    )
    record("o1", "first")
    dl.mark_attempting("o1")
    dl.mark_failed("o1", "boom")
    out["rows_after_fail"] = _debug_rows()
    with monkeypatch.context() as ctx:  # the owning gateway is gone
        ctx.setattr(dl, "_owner_alive", lambda *_: False)
        out["claimed"] = [
            {k: v for k, v in row.items() if k != "content"}
            for row in dl.sweep_recoverable(deliverable_platforms={"telegram"})
        ]
        out["claimed_for_other_platform"] = dl.sweep_recoverable(
            deliverable_platforms={"discord"},
        )
    dl.mark_delivered("o1")
    record("o2", "second")
    dl.mark_failed("o2", "rejected")
    record("o2", "second again")  # REPLACE semantics: last_error resets
    out["rows_after_replace"] = _debug_rows()
    dl._prune(now=10 ** 12)  # retention: delivered o1 goes, pending o2 stays
    out["rows_after_prune"] = _debug_rows()
    return out


def _debug_rows() -> list:
    return [
        {k: v for k, v in row.items() if k not in ("created_at", "updated_at")}
        for row in json.loads(delivery_ledger.debug_rows())
    ]


def _delegation_script(monkeypatch) -> dict:
    ad = async_delegation
    out = {}
    _dispatch("d1")
    ad._persist_completion(
        {"delegation_id": "d1", "status": "completed", "completed_at": 2.0},
        {"status": "completed", "summary": "done"},
    )
    out["claim"] = ad.claim_completion_delivery("d1", "claim-a")
    out["claim_twice"] = ad.claim_completion_delivery("d1", "claim-b")
    out["release"] = ad.release_completion_delivery("d1", "claim-a")
    out["reclaim"] = ad.claim_completion_delivery("d1", "claim-b")
    out["complete"] = ad.complete_completion_delivery("d1", "claim-b")
    out["mark_delivered_again"] = ad.mark_completion_delivered("d1")
    out["d1"] = _durable("d1")
    _dispatch("d2")
    with monkeypatch.context() as ctx:  # d2's owner process died
        import gateway.status as status

        ctx.setattr(status, "_pid_exists", lambda _pid: False)
        out["recovered"] = ad.recover_abandoned_delegations()
    restored: "queue.Queue" = queue.Queue()
    out["restored"] = ad.restore_undelivered_completions(restored)
    events = [restored.get_nowait() for _ in range(restored.qsize())]
    out["restored_events"] = [
        (event["delegation_id"], event["status"], event.get("restored")) for event in events
    ]
    out["d2"] = _durable("d2")
    out["drop_unclaimed"] = ad.drop_completion_delivery("d2", "nobody")
    out["unknown_row"] = ad.get_durable_delegation("missing")
    _dispatch("d1", goal="re-dispatched")  # REPLACE semantics: completion resets
    out["d1_redispatched"] = _durable("d1")
    ad._prune_durable_records()
    out["d1_after_prune"] = _durable("d1")
    return out


def _durable(delegation_id: str):
    row = async_delegation.get_durable_delegation(delegation_id)
    if row is None:
        return None
    return {k: v for k, v in row.items() if k not in ("dispatched_at", "completed_at")}


EXPECTED_DELIVERY = {
    "rows_after_fail": [
        {"id": "o1", "session": "k", "state": "failed", "attempts": 0, "last_error": "boom"},
    ],
    "claimed": [
        {"obligation_id": "o1", "session_key": "k", "platform": "telegram", "chat_id": "c",
         "thread_id": None, "needs_marker": True, "attempts": 1},
    ],
    "claimed_for_other_platform": [],
    "rows_after_replace": [
        {"id": "o2", "session": "k", "state": "pending", "attempts": 0, "last_error": None},
        {"id": "o1", "session": "k", "state": "delivered", "attempts": 1, "last_error": None},
    ],
    "rows_after_prune": [
        {"id": "o2", "session": "k", "state": "pending", "attempts": 0, "last_error": None},
    ],
}

EXPECTED_DELEGATION = {
    "claim": True,
    "claim_twice": False,
    "release": True,
    "reclaim": True,
    "complete": True,
    "mark_delivered_again": False,
    "d1": {"delegation_id": "d1", "origin_session": "sess", "state": "completed",
           "result": {"status": "completed", "summary": "done"},
           "delivery_state": "delivered", "delivery_attempts": 2, "origin_session_id": ""},
    "recovered": 1,
    "restored": 1,
    "restored_events": [("d2", "unknown", True)],
    "d2": {"delegation_id": "d2", "origin_session": "sess", "state": "unknown",
           "result": {"status": "unknown", "summary": None,
                      "error": "Delegation owner exited before recording a terminal result; "
                               "outcome unknown."},
           "delivery_state": "pending", "delivery_attempts": 0, "origin_session_id": ""},
    "drop_unclaimed": False,
    "unknown_row": None,
    "d1_redispatched": {"delegation_id": "d1", "origin_session": "sess", "state": "running",
                        "result": None, "delivery_state": "pending", "delivery_attempts": 0,
                        "origin_session_id": ""},
    "d1_after_prune": {"delegation_id": "d1", "origin_session": "sess", "state": "running",
                       "result": None, "delivery_state": "pending", "delivery_attempts": 0,
                       "origin_session_id": ""},
}


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_writer_functions_round_trip_on_both_backends(
    backend, tree, monkeypatch, raw, postgres_dsn,
):
    if backend == "postgres":
        _activate(monkeypatch, tree, "beta", backend="authority", dsn=postgres_dsn)
    else:
        _activate(monkeypatch, tree, "alpha")
    assert _delivery_script(monkeypatch) == EXPECTED_DELIVERY
    assert _delegation_script(monkeypatch) == EXPECTED_DELEGATION
    if backend == "postgres":
        assert _sqlite_files(tree / "profiles/beta") == []
        assert raw.execute(
            "SELECT completed_at, result_json, delivery_claim, delivered_at "
            "FROM async_delegations WHERE delegation_id = 'd1'"
        ).fetchone() == (None, None, None, None)
        assert raw.execute(
            "SELECT last_error FROM delivery_obligations WHERE obligation_id = 'o2'"
        ).fetchone() == (None,)
    else:
        # Nothing opened PostgreSQL: the tables the fixture dropped stay absent.
        assert raw.execute("SELECT to_regclass('public.async_delegations')").fetchone() == (None,)
        assert _sqlite_files(tree / "profiles/alpha") == ["state.db"]


# ---------------------------------------------------------------------------
# (5) DDL mapping: the writers' PostgreSQL DDL matches the core's X1a schema
# ---------------------------------------------------------------------------

def test_postgres_ddl_mapping():
    ddl = hermes_state_writer.postgres_ddl
    assert ddl("id INTEGER PRIMARY KEY AUTOINCREMENT") == "id BIGSERIAL PRIMARY KEY"
    assert ddl("created_at REAL NOT NULL, x REAL") == (
        "created_at DOUBLE PRECISION NOT NULL, x DOUBLE PRECISION"
    )
    assert ddl("name TEXT NOT NULL DEFAULT '', n INTEGER") == (
        "name TEXT NOT NULL DEFAULT '', n INTEGER"
    )
    assert ddl("realm TEXT, unreal INTEGER") == "realm TEXT, unreal INTEGER"


def _columns(raw, schema: str, table: str) -> list[tuple[str, str]]:
    return raw.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    ).fetchall()


def test_writer_postgres_ddl_matches_core_schema(raw):
    for schema in ("core_x1a", "writer_0051"):
        raw.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.execute(f"CREATE SCHEMA {schema}")
    raw.execute("SET search_path = core_x1a")
    for table in WRITER_TABLES:
        match = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\);", SCHEMA_SQL_POSTGRES, re.S)
        assert match, table
        raw.execute(match.group(0))
    raw.execute("SET search_path = writer_0051")
    for schema_sql in (delivery_ledger._SCHEMA, async_delegation._SCHEMA):
        translated = hermes_state_writer.postgres_ddl(schema_sql)
        assert re.search(r"\bREAL\b", translated) is None
        raw.execute(translated)
    raw.execute("SET search_path = public")
    for table in WRITER_TABLES:
        assert _columns(raw, "writer_0051", table) == _columns(raw, "core_x1a", table), table
    assert "double precision" in {t for _, t in _columns(raw, "writer_0051", "delivery_obligations")}
    for schema in ("core_x1a", "writer_0051"):
        raw.execute(f"DROP SCHEMA {schema} CASCADE")


def test_postgres_bootstrap_skips_sqlite_pragmas(tree, monkeypatch, raw, postgres_dsn):
    _activate(monkeypatch, tree, "beta", backend="authority", dsn=postgres_dsn)
    statements = []

    class _Spy:
        is_postgres = True

        def execute(self, sql, params=()):
            statements.append(" ".join(sql.split()))
            return _Rows()

    class _Rows:
        def fetchall(self):
            return []

    for initialize in (delivery_ledger._initialize_schema, async_delegation._initialize_schema):
        statements.clear()
        initialize(_Spy())
        assert statements, initialize
        assert not any("PRAGMA" in s.upper() for s in statements), statements
        assert statements[0].startswith("CREATE TABLE IF NOT EXISTS ")
        assert "DOUBLE PRECISION" in statements[0] and " REAL" not in statements[0]


# ---------------------------------------------------------------------------
# (6) One-shot SQLite -> PostgreSQL move of the rows a flipped profile left behind
# ---------------------------------------------------------------------------

def test_migration_script_is_idempotent_and_never_regresses_newer_rows(
    tree, monkeypatch, raw, postgres_dsn, capsys,
):
    _activate(monkeypatch, tree, "alpha")  # rows accumulate in SQLite first
    delivery_ledger.record_obligation(
        obligation_id="o-old", session_key="k", platform="telegram", chat_id="c",
        thread_id=None, content="left behind",
    )
    _dispatch("d-old")
    _dispatch("d-live")
    state = tree / "profiles/alpha/state.db"
    # The authority gateway already created the tables and advanced d-live.
    _activate(monkeypatch, tree, "beta", backend="authority", dsn=postgres_dsn)
    _dispatch("d-live", goal="newer in postgres")
    live_before = raw.execute(
        "SELECT task_json, updated_at FROM async_delegations WHERE delegation_id = 'd-live'"
    ).fetchone()

    def run(*extra):
        code = migrate_writer_local_tables.main(
            ["--sqlite-path", str(state), "--dsn", postgres_dsn, *extra]
        )
        return code, json.loads(capsys.readouterr().out)

    code, first = run()
    assert code == 0
    assert {t["table"]: (t["rows"], t["applied"], t["unchanged"]) for t in first["tables"]} == {
        "async_delegations": (2, 1, 1), "delivery_obligations": (1, 1, 0),
    }
    code, second = run()
    assert code == 0
    assert all(t["applied"] == 0 for t in second["tables"])
    assert raw.execute(
        "SELECT task_json, updated_at FROM async_delegations WHERE delegation_id = 'd-live'"
    ).fetchone() == live_before
    assert raw.execute("SELECT obligation_id, content FROM delivery_obligations").fetchall() == [
        ("o-old", "left behind"),
    ]
    assert sorted(r[0] for r in raw.execute("SELECT delegation_id FROM async_delegations")) == [
        "d-live", "d-old",
    ]
    code, dry = run("--dry-run")
    assert code == 0 and dry["dry_run"] is True
    with sqlite3.connect(state) as sqlite_conn:  # the source was never written
        assert sqlite_conn.execute("SELECT COUNT(*) FROM async_delegations").fetchone() == (2,)
    raw.execute("DROP TABLE delivery_obligations")
    assert migrate_writer_local_tables.main(["--sqlite-path", str(state), "--dsn", postgres_dsn]) == 2
    assert "delivery_obligations" in capsys.readouterr().err


def test_migration_script_reports_absent_source_tables(tmp_path, raw, postgres_dsn, capsys):
    empty = tmp_path / "state.db"
    sqlite3.connect(empty).close()
    assert migrate_writer_local_tables.main(["--sqlite-path", str(empty), "--dsn", postgres_dsn]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert [t["present_in_sqlite"] for t in summary["tables"]] == [False, False]
