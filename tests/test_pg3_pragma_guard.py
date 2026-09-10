"""PG3 #16: real SessionDB paths, a recording PG driver, and temporary SQLite."""

import socket
import sqlite3

import pytest


class RecordingPG:
    """Driver boundary behind the real PostgreSQL SQL/row adapter; no network."""

    def __init__(self):
        self.sql = []
        self.closed = False
        self.meta = {}

    def cursor(self):
        return RecordingCursor(self)

    def close(self):
        self.closed = True

    def commit(self):
        return None

    def rollback(self):
        return None


class RecordingCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self.rows = []

    def execute(self, sql, params=()):
        self.connection.sql.append(sql)
        if sql == "SELECT 42":
            self.description = [("answer",)]
            self.rows = [(42,)]
        elif sql.startswith("SELECT column_name FROM information_schema.columns"):
            assert params == ("messages",)
            self.description = [("column_name",)]
            self.rows = [
                (name,) for name in ("id", "session_id", "content", "extra_column")
            ]
        elif sql.startswith("SELECT id, content FROM messages"):
            self.description = [("id",), ("content",)]
            self.rows = [(1, "[terminal]")]
        elif sql.startswith("SELECT value FROM state_meta WHERE key ="):
            self.description = [("value",)]
            value = self.connection.meta.get(params[0])
            self.rows = [] if value is None else [(value,)]
        elif sql.startswith("INSERT INTO state_meta (key, value)"):
            self.connection.meta[params[0]] = params[1]
        elif sql.startswith("SELECT s.id FROM sessions s WHERE"):
            self.description = [("id",)]
            self.rows = [("expired-session",)]
        elif sql == "BEGIN" or sql.startswith(
            (
                "UPDATE sessions SET parent_session_id = NULL",
                "DELETE FROM messages WHERE session_id =",
                "DELETE FROM sessions WHERE id =",
                "DELETE FROM system_prompts WHERE NOT EXISTS",
            )
        ):
            self.rows = []
        else:
            raise AssertionError(f"Unexpected SQL sent to PostgreSQL: {sql}")
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows


@pytest.fixture
def open_store(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("HERMES_STATE_BACKEND", "sqlite")
    monkeypatch.setenv("HERMES_PG_ADAPTER_STRICT", "0")
    for name in (
        "HERMES_CORE_PG_DSN",
        "HERMES_STATE_POSTGRES_DSN",
        "HERMES_STATE_DATABASE_URL",
        "HERMES_STATE_DUAL_WRITE",
    ):
        monkeypatch.delenv(name, raising=False)

    def reject_network(*args, **kwargs):
        raise AssertionError("Network access is forbidden in this regression")

    monkeypatch.setattr(socket.socket, "connect", reject_network)
    monkeypatch.setattr(socket, "create_connection", reject_network)

    import hermes_state
    import hermes_state_postgres

    stores = []

    def open_database(backend, read_only=False):
        if backend == "postgres":
            raw = RecordingPG()
            adapter = hermes_state_postgres._PostgresConnection(raw)

            def connect(requested_read_only, schema_version, *, dsn_override):
                assert requested_read_only is read_only
                assert schema_version == hermes_state.SCHEMA_VERSION
                assert dsn_override == "recording-pg-no-network"
                return adapter

            monkeypatch.setattr(hermes_state_postgres, "maybe_open_postgres", connect)
            store = hermes_state.SessionDB(
                postgres_dsn="recording-pg-no-network",
                read_only=read_only,
                dual_write=False,
            )
            assert store._is_postgres is True
            assert store._state_backend_mode == "authority"
            assert store._conn.execute("SELECT 42").fetchone()[0] == 42
            raw.sql.clear()
            statements = raw.sql
        else:
            path = tmp_path / f"sqlite-{len(stores)}.db"
            if read_only:
                hermes_state.SessionDB(path, dual_write=False).close()
            store = hermes_state.SessionDB(path, read_only=read_only, dual_write=False)
            assert store._is_postgres is False
            assert isinstance(store._conn, sqlite3.Connection)
            statements = []
            store._conn.set_trace_callback(statements.append)
        stores.append(store)
        return store, statements

    yield open_database
    for store in reversed(stores):
        store.close()


@pytest.mark.parametrize("backend", ["postgres", "sqlite"])
@pytest.mark.parametrize("read_only", [False, True])
def test_close_checkpoint_backend_and_read_only(open_store, backend, read_only):
    store, statements = open_store(backend, read_only)
    connection = store._conn
    store.close()
    expected = (
        ["PRAGMA wal_checkpoint(PASSIVE)"]
        if backend == "sqlite" and not read_only
        else []
    )
    assert statements == expected
    assert store._conn is None
    if backend == "postgres":
        assert connection.raw.closed
    store.close()
    assert statements == expected


@pytest.mark.parametrize("backend", ["postgres", "sqlite"])
def test_checkpoint_helper_backend(open_store, backend):
    store, statements = open_store(backend)
    store._try_wal_checkpoint()
    assert statements == (
        ["PRAGMA wal_checkpoint(PASSIVE)"] if backend == "sqlite" else []
    )


@pytest.mark.parametrize("backend", ["postgres", "sqlite"])
def test_vacuum_backend(open_store, backend):
    store, statements = open_store(backend)
    result = store.vacuum()
    if backend == "postgres":
        assert statements == []
        assert result == 0
    else:
        maintenance = [
            sql for sql in statements if sql.startswith(("PRAGMA", "VACUUM"))
        ]
        assert maintenance == [
            "PRAGMA wal_checkpoint(PASSIVE)",
            "VACUUM",
            "PRAGMA wal_checkpoint(TRUNCATE)",
        ]
        assert result > 0


@pytest.mark.parametrize("backend", ["postgres", "sqlite"])
def test_fts_storage_backend(open_store, backend):
    store, statements = open_store(backend)
    result = store.optimize_fts_storage()
    if backend == "postgres":
        assert result == {"ok": False, "reason": "fts5_unavailable"}
        assert statements == []
    else:
        assert result["ok"] is True
        assert result["vacuumed"] is True
        assert "VACUUM" in statements
        assert "PRAGMA wal_checkpoint(PASSIVE)" in statements
        assert not any("TRUNCATE" in sql for sql in statements)


def test_fts_storage_postgres_guard_is_independent_of_capability_flag(open_store):
    store, statements = open_store("postgres")
    store._fts_enabled = True
    assert store.optimize_fts_storage() == {"ok": False, "reason": "fts5_unavailable"}
    assert statements == []


@pytest.mark.parametrize(
    "method, expected",
    [
        ("_fts_teardown_trash_step", False),
        ("_demote_legacy_fts_to_trash", 0),
        ("optimize_fts", 0),
        ("rebuild_fts", 0),
    ],
)
def test_fts_helpers_postgres_do_not_send_sql(open_store, method, expected):
    store, statements = open_store("postgres")
    assert getattr(store, method)() == expected
    assert statements == []


@pytest.mark.parametrize("backend", ["postgres", "sqlite"])
def test_logical_size_backend(open_store, backend):
    store, statements = open_store(backend)
    result = store.logical_size_bytes()
    if backend == "postgres":
        assert result is None
        assert statements == []
    else:
        assert result > 0
        assert statements == ["PRAGMA page_count", "PRAGMA page_size"]


@pytest.mark.parametrize("backend", ["postgres", "sqlite"])
def test_message_columns_backend_and_cache(open_store, backend):
    store, statements = open_store(backend)
    columns = store._message_column_names(store._conn)
    assert {"id", "session_id", "content"} <= set(columns)
    if backend == "postgres":
        assert columns == ["id", "session_id", "content", "extra_column"]
        assert not any("PRAGMA" in sql for sql in statements)
        assert "ORDER BY ordinal_position" in statements[0]
    else:
        assert statements == ["PRAGMA table_info(messages)"]
    before = list(statements)
    assert store._message_column_names(store._conn) == columns
    assert statements == before


def test_postgres_topic_migration_refuses_before_ddl(open_store):
    store, statements = open_store("postgres")
    with pytest.raises(NotImplementedError, match="backend-specific migration"):
        store.apply_telegram_topic_migration()
    assert len(statements) == 1
    assert statements[0].startswith("SELECT value FROM state_meta")


def test_postgres_provisioned_topic_schema_needs_no_sqlite_migration(open_store):
    store, statements = open_store("postgres")
    store._conn.raw.meta["telegram_dm_topic_schema_version"] = "2"
    store.apply_telegram_topic_migration()
    assert len(statements) == 1
    assert statements[0].startswith("SELECT value FROM state_meta")


def test_sqlite_topic_migration_keeps_fk_inspection(open_store):
    store, statements = open_store("sqlite")
    store.apply_telegram_topic_migration()
    assert "PRAGMA foreign_key_list('telegram_dm_topic_bindings')" in statements
    assert store.get_meta("telegram_dm_topic_schema_version") == "2"


def test_postgres_marker_backup_refuses_before_mutation(open_store):
    store, statements = open_store("postgres")
    with pytest.raises(NotImplementedError, match="external backup"):
        store.purge_stale_tool_call_markers()
    assert len(statements) == 1
    assert statements[0].startswith("SELECT id, content FROM messages")


def test_sqlite_marker_backup_still_runs_vacuum_into(open_store):
    store, statements = open_store("sqlite")
    store.create_session("pragma-guard", "cli")
    store.append_message(
        "pragma-guard", "assistant", "[terminal]", tool_calls=[{"id": "call"}]
    )
    statements.clear()
    result = store.purge_stale_tool_call_markers()
    assert result["rows_affected"] == 1
    assert result["backup_path"] is not None
    assert any(sql.startswith("VACUUM INTO ") for sql in statements)


def test_postgres_auto_prune_does_not_claim_vacuum(open_store):
    store, statements = open_store("postgres")
    result = store.maybe_auto_prune_and_vacuum(retention_days=0)
    assert result["pruned"] == 1
    assert result["vacuumed"] is False
    assert store.get_meta("last_vacuum") is None
    assert store.get_meta("last_auto_prune") is not None
    assert not any(sql.startswith(("PRAGMA", "VACUUM")) for sql in statements)
