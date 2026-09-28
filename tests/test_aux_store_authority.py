"""levos 0059 — core auxiliary stores follow the profile's PostgreSQL authority.

On ``HERMES_STATE_BACKEND=authority`` the verification ledger, the Responses
API store, the Discord recovery ledger and the projects store must live in the
profile's PostgreSQL store and never open SQLite (file or ``:memory:``); an
unreachable store fails loudly. Every other backend keeps its SQLite files.
Runs against the ephemeral Unix-socket PostgreSQL fixture (``postgres_dsn``).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

import hermes_aux_store as aux
from agent import verification_evidence as ve
from gateway.config import PlatformConfig
from gateway.platforms import api_server
from gateway.platforms.base import (
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)
from hermes_cli import projects_db
from hermes_cli.projects_postgres import ProjectsPostgresConnection
from hermes_constants import get_hermes_home
from plugins.platforms.discord.adapter import DiscordAdapter
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0059&connect_timeout=1"
)
AUX_TABLES = {
    "core_verification_meta",
    "core_verification_events",
    "core_verification_state",
    "core_response_responses",
    "core_response_conversations",
    "core_discord_messages",
    "core_discord_recovery_scans",
    "core_discord_recovery_cursors",
}


@pytest.fixture(autouse=True)
def _backend_env(monkeypatch):
    for key in (
        "HERMES_STATE_BACKEND",
        "HERMES_STATE_DATABASE_URL",
        "HERMES_STATE_POSTGRES_DSN",
        "HERMES_CORE_PG_DSN",
        "HERMES_PROJECTS_BACKEND",
        "HERMES_PROJECTS_POSTGRES_DSN",
        "HERMES_AUX_DB_DIR",
        "HERMES_PROFILE",
    ):
        monkeypatch.delenv(key, raising=False)
    # pg3 only writes the verification ledger while verify-on-stop is on.
    monkeypatch.setenv("HERMES_VERIFY_ON_STOP", "1")


@pytest.fixture
def pg_dsn(postgres_dsn):
    """A clean ``public`` schema per test on the module's PostgreSQL server."""
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP SCHEMA public CASCADE")
        raw.execute("CREATE SCHEMA public")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg_dsn):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg_dsn)
    return pg_dsn


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "pyproject.toml").write_text("[tool.pytest.ini_options]\n")
    return root


@pytest.fixture
def discord(monkeypatch):
    monkeypatch.setenv("DISCORD_MISSED_MESSAGE_BACKFILL", "true")
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fake-token"))
    yield adapter
    adapter._discord_recovery_store.close()


def _db_files() -> list[str]:
    home = get_hermes_home()
    return sorted(str(p.relative_to(home)) for p in home.rglob("*.db*"))


def _pg_tables(dsn: str) -> set[str]:
    with psycopg.connect(dsn, autocommit=True) as raw:
        rows = raw.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
        ).fetchall()
    return {row[0] for row in rows}


def _pg_count(dsn: str, table: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as raw:
        return raw.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _message(message_id: int, channel_id: int = 123):
    channel = SimpleNamespace(id=channel_id, parent_id=None)
    return SimpleNamespace(
        id=message_id,
        channel=channel,
        author=SimpleNamespace(id=42),
        created_at=datetime.now(timezone.utc),
    )


def _discord_roundtrip(adapter) -> None:
    """Drive every ledger statement shape the adapter issues."""
    message = _message(1)
    adapter._record_discord_message_seen(message, status="queued")
    assert adapter._discord_message_has_active_claim("1") is True
    event = MessageEvent(
        text="hi", message_type=MessageType.TEXT, raw_message=message, message_id="1"
    )
    adapter._record_discord_processing_start(event, emoji_ack=True)
    adapter._record_recovery_attempt(message, status="processing", error=None)
    adapter._record_discord_processing_complete(event, ProcessingOutcome.SUCCESS)
    adapter._record_discord_response(
        reply_to="1",
        result=SendResult(success=True, message_id="900"),
        content="done",
        final=True,
    )
    assert adapter._discord_message_is_persistently_complete("1") is True
    assert adapter._discord_recovery_cursor("123") == "1"
    scan = adapter._record_recovery_scan_start({"123"})
    adapter._record_recovery_scan_complete(
        scan, status="ok", scanned=1, missed=0, dispatched=0
    )
    # INSERT OR REPLACE keeps SQLite REPLACE semantics: unlisted columns reset.
    adapter._with_discord_recovery_db(
        lambda conn: conn.execute(
            "INSERT OR REPLACE INTO discord_recovery_scans (scan_id, started_at, status, "
            "channels, window_seconds, limit_count) VALUES (?, ?, ?, ?, ?, ?)",
            (scan, "2026-09-27", "running", "[]", 1.5, 3),
        )
    )
    row = adapter._with_discord_recovery_db(
        lambda conn: conn.execute(
            "SELECT completed_at, scanned, window_seconds FROM discord_recovery_scans WHERE scan_id=?",
            (scan,),
        ).fetchone()
    )
    assert tuple(row) == (None, 0, 1.5)


def _response_roundtrip(store) -> None:
    store.put(
        "resp_1",
        {"response": {"id": "resp_1"}, "session_id": "s1", "instructions": "be brief"},
    )
    store.set_conversation("chat", "resp_1")
    store.set_conversation("chat", "resp_1")
    assert store.get("resp_1")["instructions"] == "be brief"
    assert store.get_conversation("chat") == "resp_1"
    store.put("resp_2", {"n": 2})
    store.put("resp_3", {"n": 3})
    assert len(store) == 2  # max_size=2 evicts the least recently used
    assert store.get("resp_1") is None
    assert store.get_conversation("chat") is None
    assert store.delete("resp_3") is True
    assert store.delete("resp_3") is False
    assert len(store) == 1


def _verification_roundtrip(workspace) -> None:
    ve.record_verify_run(root=workspace, session_id="s1", ok=True, output="ok")
    assert ve.verification_status(session_id="s1", cwd=workspace)["status"] == "passed"
    ve.mark_workspace_edited(session_id="s1", cwd=workspace, paths=["a.py"])
    status = ve.verification_status(session_id="s1", cwd=workspace)
    assert status["status"] == "stale"
    assert status["changed_paths"] == ["a.py"]
    assert status["evidence"]["kind"] == "verify"
    ve.record_verify_run(root=workspace, session_id="s1", ok=False, output="boom")
    assert ve.verification_status(session_id="s1", cwd=workspace)["status"] == "failed"


def _projects_roundtrip() -> str:
    with projects_db.connect_closing() as conn:
        project_id = projects_db.create_project(
            conn, name="Aux ' ? %", folders=["/srv/aux"]
        )
        assert projects_db.get_project(conn, project_id).name == "Aux ' ? %"
        return project_id


# (a) authority + PGlite: all four stores round-trip in PostgreSQL ------------


def test_authority_roundtrips_all_four_stores_in_postgres(
    authority, workspace, discord
):
    _verification_roundtrip(workspace)
    store = api_server.ResponseStore(max_size=2)
    assert isinstance(store._conn, aux.AuxPostgresConnection)
    _response_roundtrip(store)
    store.close()
    _discord_roundtrip(discord)
    _projects_roundtrip()
    with projects_db.connect_closing() as conn:
        assert isinstance(conn, ProjectsPostgresConnection)

    tables = _pg_tables(authority)
    assert AUX_TABLES <= tables
    assert {"projects", "project_folders", "project_meta", "discovered_repos"} <= tables
    # No bare SQLite table name leaks into the profile schema.
    assert not tables & {
        "meta",
        "verification_events",
        "responses",
        "conversations",
        "discord_messages",
    }
    assert _pg_count(authority, "core_verification_events") == 2
    assert _pg_count(authority, "core_discord_messages") == 1
    assert _db_files() == []


def test_postgres_handle_rejects_sqlite_only_and_foreign_sql(authority):
    conn = aux.open_aux_store(
        "response_store",
        sqlite_path="unused.db",
        initialize=api_server._initialize_response_store,
    )
    try:
        assert isinstance(conn, aux.AuxPostgresConnection)
        with pytest.raises(ValueError, match="PRAGMA"):
            conn.execute("PRAGMA journal_mode=WAL")
        with pytest.raises(ValueError, match="not part of auxiliary store"):
            conn.execute("INSERT INTO sessions (id) VALUES (?)", ("x",))
        # Quoted literals are data, not identifiers.
        conn.execute(
            "INSERT OR REPLACE INTO responses (response_id, data, accessed_at) VALUES (?, 'responses', ?)",
            ("r", 1.0),
        )
        assert conn.execute("SELECT data FROM responses").fetchone()[0] == "responses"
    finally:
        conn.close()
    with pytest.raises(ValueError, match="unknown auxiliary store"):
        aux.open_aux_store("state", sqlite_path="x.db", initialize=lambda conn: None)


# (b) authority without PostgreSQL: loud failure, zero SQLite files -----------


@pytest.mark.asyncio
async def test_authority_without_postgres_raises_and_creates_no_db_file(
    monkeypatch, workspace, discord
):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)

    with pytest.raises(aux.AuxStoreUnavailable):
        ve.record_verify_run(root=workspace, session_id="s1", ok=True)
    with pytest.raises(aux.AuxStoreUnavailable):
        ve.verification_status(session_id="s1", cwd=workspace)

    store = api_server.ResponseStore()
    assert store._conn is None
    with pytest.raises(aux.AuxStoreUnavailable):
        store.get("resp_1")
    with pytest.raises(aux.AuxStoreUnavailable):
        store.put("resp_1", {})

    with pytest.raises(aux.AuxStoreUnavailable):
        discord._with_discord_recovery_db(
            lambda conn: conn.execute("SELECT 1").fetchone(), False
        )
    with pytest.raises(aux.AuxStoreUnavailable):
        discord._discord_message_is_persistently_complete("1")

    with pytest.raises(
        sqlite3.OperationalError, match="PostgreSQL projects connection"
    ):
        projects_db.connect()

    # The adapter answers such a request with 503 and no internal detail.
    # pg3's adapter also opens the /v1/runs idempotency store
    # (runs_idempotency.db), which is not one of the 0059 stores; keep it in
    # memory so the file check below still covers exactly those four stores.
    run_store = api_server.RunIdempotencyStore
    monkeypatch.setattr(
        api_server, "RunIdempotencyStore", lambda: run_store(":memory:")
    )
    adapter = api_server.APIServerAdapter(PlatformConfig(enabled=True, extra={}))
    app = web.Application(
        middlewares=[
            api_server.security_headers_middleware,
            api_server.aux_store_unavailable_middleware,
        ]
    )
    app.router.add_get("/v1/responses/{response_id}", adapter._handle_get_response)
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/v1/responses/resp_1")
        body = await response.json()
    assert response.status == 503
    assert body["error"]["code"] == "store_unavailable"
    assert "nonexistent" not in json.dumps(body)
    assert response.headers["X-Content-Type-Options"] == "nosniff"

    assert _db_files() == []


def test_authority_projects_rejects_contradicting_selection(
    authority, tmp_path, monkeypatch
):
    with pytest.raises(ValueError, match="SQLite db_path"):
        projects_db.connect(tmp_path / "projects.db")
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "sqlite")
    with pytest.raises(ValueError, match="contradicts"):
        projects_db.connect()
    assert not (tmp_path / "projects.db").exists()
    assert _db_files() == []


# (c) non-authority keeps SQLite ---------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_sqlite_files(
    backend, monkeypatch, workspace, discord, pg_dsn
):
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)
    if backend == "probe":
        monkeypatch.setenv("HERMES_CORE_PG_DSN", pg_dsn)
    _verification_roundtrip(workspace)
    store = api_server.ResponseStore(max_size=2)
    assert isinstance(store._conn, sqlite3.Connection)
    _response_roundtrip(store)
    store.close()
    _discord_roundtrip(discord)
    _projects_roundtrip()
    with projects_db.connect_closing() as conn:
        assert isinstance(conn, sqlite3.Connection)

    files = _db_files()
    for name in (
        "verification_evidence.db",
        "response_store.db",
        "gateway/discord_message_recovery.db",
        "projects.db",
    ):
        assert name in files
    assert not AUX_TABLES & _pg_tables(pg_dsn)


def test_non_authority_response_store_keeps_its_memory_fallback(monkeypatch):
    def unwritable(*args, **kwargs):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(Path, "mkdir", unwritable)
    store = api_server.ResponseStore()
    store.put("resp_1", {"n": 1})
    assert store.get("resp_1") == {"n": 1}
    assert store._db_path is None


# (d) the response store never uses :memory: on authority ----------------------


def test_authority_response_store_never_opens_sqlite(authority, monkeypatch):
    # SessionDB's own PostgreSQL column reconcile parses SCHEMA_SQL with a
    # throwaway in-memory SQLite (a DDL parser, not a store); only the store
    # code paths are tracked here.
    store_modules = {"gateway.platforms.api_server", "hermes_aux_store"}
    opened = []
    real_connect = sqlite3.connect

    def tracking_connect(*args, **kwargs):
        caller = sys._getframe(1).f_globals.get("__name__")
        if caller in store_modules:
            opened.append((caller, args[0] if args else kwargs.get("database")))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)
    store = api_server.ResponseStore(db_path=":memory:")
    assert isinstance(store._conn, aux.AuxPostgresConnection)
    store.put("resp_1", {"n": 1})
    assert store.get("resp_1") == {"n": 1}
    store.close()

    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    with pytest.raises(aux.AuxStoreUnavailable):
        api_server.ResponseStore().get("resp_1")
    assert opened == []
    assert _pg_count(authority, "core_response_responses") == 1


# (e) one-shot migration: idempotent, verified, source untouched ---------------


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _seed_sqlite(workspace, discord) -> dict[str, Path]:
    _verification_roundtrip(workspace)
    ve.record_verify_run(root=workspace, session_id="s2", ok=True)
    store = api_server.ResponseStore(max_size=10)
    store.put("resp_1", {"n": 1})
    store.put("resp_2", {"n": 2})
    store.set_conversation("chat", "resp_2")
    store.close()
    _discord_roundtrip(discord)
    discord._discord_recovery_store.close()
    _projects_roundtrip()
    home = get_hermes_home()
    return {
        "verification_evidence": home / "verification_evidence.db",
        "response_store": home / "response_store.db",
        "discord_recovery": home / "gateway" / "discord_message_recovery.db",
        "projects": home / "projects.db",
    }


def _checkpoint(paths) -> dict[str, str]:
    for path in paths.values():
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    return {name: _sha256(path) for name, path in paths.items()}


def test_migration_is_idempotent_and_leaves_the_sources_untouched(
    pg_dsn, monkeypatch, workspace, discord
):
    paths = _seed_sqlite(workspace, discord)
    digests = _checkpoint(paths)
    monkeypatch.setenv("HERMES_PROFILE", "custom")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg_dsn)

    # The live authority store already holds a newer event under id 1.
    ve.record_verify_run(root=workspace, session_id="live", ok=False)

    dry = aux.migrate_aux_sqlite_to_pg("custom", dry_run=True)
    assert dry["stores"]["response_store"]["status"] == "dry_run"
    assert dry["stores"]["response_store"]["tables"]["responses"]["inserted"] == 2
    assert _pg_count(pg_dsn, "core_response_responses") == 0
    assert _pg_count(pg_dsn, "projects") == 0

    first = aux.migrate_aux_sqlite_to_pg("custom", dry_run=False)
    counts = {
        table: _pg_count(pg_dsn, table)
        for table in sorted(AUX_TABLES | {"projects", "project_folders"})
    }
    second = aux.migrate_aux_sqlite_to_pg("custom", dry_run=False)
    assert {table: _pg_count(pg_dsn, table) for table in counts} == counts

    events = first["stores"]["verification_evidence"]["tables"]["verification_events"]
    assert events["source_rows"] == 3
    assert events["inserted"] == 3
    assert events["target_rows_after"] == 4
    for store in second["stores"].values():
        for table in store["tables"].values():
            assert table["inserted"] == 0
            assert table["target_rows_after"] >= table["source_rows"]
    assert counts["core_response_responses"] == 2
    assert counts["core_discord_messages"] == 1
    assert counts["projects"] == 1

    # References follow the re-assigned event ids.
    assert ve.verification_status(session_id="s1", cwd=workspace)["status"] == "failed"
    assert ve.verification_status(session_id="s2", cwd=workspace)["status"] == "passed"
    assert (
        ve.verification_status(session_id="live", cwd=workspace)["status"] == "failed"
    )
    store = api_server.ResponseStore()
    assert store.get_conversation("chat") == "resp_2"
    store.close()

    assert {name: _sha256(path) for name, path in paths.items()} == digests
    assert first["stores"]["projects"]["sha256"] == digests["projects"]


def test_migration_reports_missing_files_and_refuses_other_profiles(
    authority, monkeypatch
):
    monkeypatch.setenv("HERMES_PROFILE", "custom")
    report = aux.migrate_aux_sqlite_to_pg("custom", dry_run=False)
    assert {store["status"] for store in report["stores"].values()} == {"missing"}
    with pytest.raises(ValueError, match="not the active profile"):
        aux.migrate_aux_sqlite_to_pg("dave", dry_run=True)
    assert _db_files() == []


def test_migration_rolls_back_a_store_when_a_row_cannot_land(pg_dsn, monkeypatch):
    _projects_roundtrip()
    source = get_hermes_home() / "projects.db"
    digest = _checkpoint({"projects": source})["projects"]
    monkeypatch.setenv("HERMES_PROFILE", "custom")
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg_dsn)
    with projects_db.connect_closing() as conn:
        # Same slug, different id: the source row cannot land.
        projects_db.create_project(conn, name="Aux ' ? %", folders=["/srv/other"])

    with pytest.raises(aux.AuxMigrationError, match="not in PostgreSQL"):
        aux.migrate_aux_sqlite_to_pg("custom", dry_run=False)
    assert _pg_count(pg_dsn, "projects") == 1
    assert _pg_count(pg_dsn, "project_folders") == 1
    assert _sha256(source) == digest
