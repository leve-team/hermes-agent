from __future__ import annotations

import json
import sqlite3

import pytest

import hermes_state_dual
from gateway import delivery_ledger
from hermes_state import SessionDB
from tools import async_delegation


@pytest.mark.parametrize("owner", [async_delegation, delivery_ledger])
def test_writer_off_preserves_bootstrap_sql_and_connection(
    owner, tmp_path, monkeypatch
):
    monkeypatch.delenv("HERMES_STATE_DUAL_WRITE", raising=False)
    traces = []
    snapshots = []
    for recorded in (False, True):
        trace = []

        def initialize(connection):
            connection.set_trace_callback(trace.append)
            owner._initialize_schema(connection)

        path = tmp_path / f"{recorded}.db"
        if recorded:
            connection = SessionDB.open_writer(path, timeout=10, initialize=initialize)
        else:
            connection = sqlite3.connect(path, timeout=10)
            initialize(connection)
        try:
            assert type(connection) is sqlite3.Connection
            assert connection.isolation_level == ""
            assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 10000
            connection.execute("BEGIN IMMEDIATE")
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            connection.rollback()
            snapshots.append(list(connection.iterdump()))
            traces.append(trace)
        finally:
            connection.close()
    assert traces[0] == traces[1]
    assert snapshots[0] == snapshots[1]


@pytest.fixture
def replica(tmp_path, monkeypatch):
    path = tmp_path / "replica.db"
    connection = sqlite3.connect(path)
    async_delegation._initialize_schema(connection)
    delivery_ledger._initialize_schema(connection)
    connection.close()
    monkeypatch.setenv("HERMES_STATE_DUAL_WRITE", "1")
    monkeypatch.setattr(
        hermes_state_dual, "connect_dual_target", lambda *_args: sqlite3.connect(path)
    )
    return path


def _rows(path, table):
    connection = sqlite3.connect(path)
    try:
        return connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    finally:
        connection.close()


def test_writer_public_mutations_replace_and_prune_reach_replica(replica):
    record = {
        "delegation_id": "dispatch",
        "session_key": "origin",
        "dispatched_at": 1.0,
    }
    async_delegation._persist_dispatch(record)
    async_delegation._persist_completion(
        {"delegation_id": "dispatch", "completed_at": 2.0}, {"text": "result"}
    )
    assert async_delegation.claim_completion_delivery("dispatch", "claim")
    assert async_delegation.complete_completion_delivery("dispatch", "claim")
    async_delegation._persist_dispatch(record)
    source = async_delegation._db_path()
    assert _rows(source, "async_delegations") == _rows(replica, "async_delegations")
    assert async_delegation.get_durable_delegation("dispatch")["completed_at"] is None
    obligation = dict(
        obligation_id="obligation",
        session_key="session",
        platform="slack",
        chat_id="chat",
        thread_id=None,
    )
    delivery_ledger.record_obligation(**obligation, content="first")
    delivery_ledger.mark_failed("obligation", "previous failure")
    delivery_ledger.record_obligation(**obligation, content="reset")
    assert _rows(source, "delivery_obligations") == _rows(
        replica, "delivery_obligations"
    )
    assert _rows(replica, "delivery_obligations")[0][-1] is None
    delivery_ledger.mark_attempting("obligation")
    delivery_ledger.mark_delivered("obligation")
    delivery_ledger._prune(now=10**12)
    async_delegation._delete_durable_delegation("dispatch")
    assert (
        _rows(source, "async_delegations") == _rows(replica, "async_delegations") == []
    )
    assert (
        _rows(source, "delivery_obligations")
        == _rows(replica, "delivery_obligations")
        == []
    )


@pytest.mark.parametrize("abort", ["rollback", "close", "exception", "commit_failure"])
def test_writer_never_replicates_aborted_batch(replica, tmp_path, abort):
    path = tmp_path / "source.db"
    connection = SessionDB.open_writer(
        path, timeout=10, initialize=delivery_ledger._initialize_schema
    )
    statement = (
        "INSERT INTO delivery_obligations "
        "(obligation_id,session_key,platform,chat_id,content,state,created_at,updated_at) "
        "VALUES ('abort','session','slack','chat','text','pending',1,1)"
    )
    try:
        if abort == "commit_failure":
            connection.execute("PRAGMA foreign_keys=ON")
            connection.executescript(
                "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
                "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);"
            )
            with pytest.raises(sqlite3.IntegrityError):
                with connection:
                    connection.execute(statement)
                    connection.execute("INSERT INTO child VALUES (1)")
        elif abort == "exception":
            with pytest.raises(ValueError):
                with connection:
                    connection.execute(statement)
                    raise ValueError("abort")
        else:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(statement)
            getattr(connection, abort)()
        if abort != "close":
            connection.commit()
    finally:
        connection.close()
    assert (
        _rows(path, "delivery_obligations")
        == _rows(replica, "delivery_obligations")
        == []
    )


def test_writer_pg_failure_journals_one_transaction_and_replays(
    replica, tmp_path, monkeypatch
):
    source = tmp_path / "source.db"

    def unavailable(*_args):
        raise ConnectionError("fixture replica offline")

    monkeypatch.setattr(hermes_state_dual, "connect_dual_target", unavailable)
    connection = SessionDB.open_writer(
        source, timeout=10, initialize=delivery_ledger._initialize_schema
    )
    try:
        with connection:
            connection.execute(
                "INSERT INTO delivery_obligations "
                "(obligation_id,session_key,platform,chat_id,content,state,created_at,updated_at) "
                "VALUES ('failure','session','slack','chat','text','pending',1,1)"
            )
            connection.execute(
                "UPDATE delivery_obligations SET attempts=attempts+1 WHERE obligation_id=?",
                ("failure",),
            )
        failures = connection.execute(
            "SELECT mutations_json FROM _hermes_dual_failures"
        ).fetchall()
        assert len(failures) == 1
        assert len(json.loads(failures[0][0])) == 2
        assert len(_rows(source, "delivery_obligations")) == 1
        assert _rows(replica, "delivery_obligations") == []
        dual = hermes_state_dual.DualWriteReplicator(
            connection, "", connection_factory=lambda *_args: sqlite3.connect(replica)
        )
        assert dual.replay_failures() == {"applied": 1, "failed": 0, "pending": 0}
        assert dual.replay_failures() == {"applied": 0, "failed": 0, "pending": 0}
        connection.execute("UPDATE _hermes_dual_failures SET replayed_at=NULL")
        connection.commit()
        assert dual.replay_failures() == {"applied": 1, "failed": 0, "pending": 0}
        assert _rows(source, "delivery_obligations") == _rows(
            replica, "delivery_obligations"
        )
    finally:
        connection.close()


def test_writer_does_not_commit_bootstrap_transaction(replica, tmp_path):
    path = tmp_path / "bootstrap.db"

    def initialize(connection):
        connection.execute("CREATE TABLE bootstrap(value TEXT)")
        connection.execute("INSERT INTO bootstrap VALUES ('must roll back')")

    with pytest.raises(RuntimeError, match="bootstrap left an open transaction"):
        SessionDB.open_writer(path, timeout=10, initialize=initialize)
    assert _rows(path, "bootstrap") == []


def test_writer_journal_failure_does_not_undo_committed_source(
    replica, tmp_path, monkeypatch, caplog
):
    path = tmp_path / "journal-failure.db"

    def unavailable(*_args):
        raise ConnectionError("fixture replica offline")

    monkeypatch.setattr(hermes_state_dual, "connect_dual_target", unavailable)
    connection = SessionDB.open_writer(
        path, timeout=10, initialize=delivery_ledger._initialize_schema
    )
    try:
        connection.execute(
            "CREATE TRIGGER reject_journal BEFORE INSERT ON _hermes_dual_failures "
            "BEGIN SELECT RAISE(ABORT, 'fixture journal full'); END"
        )
        with connection:
            connection.execute(
                "INSERT INTO delivery_obligations "
                "(obligation_id,session_key,platform,chat_id,content,state,created_at,updated_at) "
                "VALUES ('journal','session','slack','chat','text','pending',1,1)"
            )
        assert len(_rows(path, "delivery_obligations")) == 1
        assert "failure journal could not be updated" in caplog.text
        assert (
            connection.execute("SELECT COUNT(*) FROM _hermes_dual_failures").fetchone()[
                0
            ]
            == 0
        )
    finally:
        connection.close()
