"""Defect #6: real SQLite source/target with FK enforcement, no Hermes core or PG."""

import json
import re
import sqlite3

import pytest

import migrate_state_to_postgres as migration
from hermes_state_dual import MIGRATED_TABLES
from state_transfer import open_sqlite_snapshot, sqlite_table_specs


SCHEMA = """
CREATE TABLE system_prompts (hash TEXT PRIMARY KEY, prompt TEXT NOT NULL);
CREATE TABLE sessions (
    id TEXT PRIMARY KEY, started_at REAL, last_activity_at REAL, title TEXT,
    parent_session_id TEXT REFERENCES sessions(id)
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id),
    content TEXT NOT NULL
);
CREATE TABLE session_model_usage (
    session_id TEXT REFERENCES sessions(id), model TEXT, input_tokens INTEGER,
    billing_provider TEXT DEFAULT '', billing_base_url TEXT DEFAULT '',
    billing_mode TEXT DEFAULT '', task TEXT DEFAULT '',
    PRIMARY KEY (session_id, model, billing_provider, billing_base_url, billing_mode, task)
);
CREATE TABLE state_meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE gateway_routing (
    scope TEXT, session_key TEXT, entry_json TEXT, updated_at REAL,
    PRIMARY KEY (scope, session_key)
);
CREATE TABLE compression_locks (session_id TEXT PRIMARY KEY, holder TEXT, acquired_at REAL);
CREATE TABLE session_turn_leases (conversation_id TEXT PRIMARY KEY, holder TEXT, acquired_at REAL);
CREATE TABLE gateway_hygiene_state (session_key TEXT PRIMARY KEY, failure_streak INTEGER);
CREATE TABLE levos_control_tower_event (event_id TEXT PRIMARY KEY, created_at REAL, payload_text TEXT);
CREATE TABLE levos_control_tower_delivery (
    event_id TEXT REFERENCES levos_control_tower_event(event_id), attempt_no INTEGER,
    claimed_at REAL, inject_ok INTEGER, PRIMARY KEY (event_id, attempt_no)
);
CREATE TABLE levos_control_tower_role (role TEXT PRIMARY KEY, session_id TEXT, assigned_at REAL);
CREATE TABLE levos_control_tower_forward (
    forward_key TEXT PRIMARY KEY, event_id TEXT REFERENCES levos_control_tower_event(event_id),
    delivered_at REAL
);
CREATE TABLE async_delegations (delegation_id TEXT PRIMARY KEY, updated_at REAL, state TEXT);
CREATE TABLE delivery_obligations (obligation_id TEXT PRIMARY KEY, updated_at REAL, state TEXT);
"""


class ObservedTarget(sqlite3.Connection):
    """Observe and inject failures only at the real target driver boundary."""

    harness = None
    last_write_table = None

    def executemany(self, sql, parameters):
        match = re.match(r'INSERT INTO "(\w+)"', sql)
        self.last_write_table = match.group(1) if match else None
        return super().executemany(sql, parameters)

    def commit(self):
        super().commit()
        if self.harness.fail_commit_table and self.last_write_table == self.harness.fail_commit_table:
            self.harness.fail_commit_table = None
            raise OSError("driver interrupted after commit, before checkpoint")

    def execute(self, sql, parameters=()):
        if sql.startswith("SELECT id FROM sessions WHERE id IN"):
            self.harness.parent_checks.append(
                {row[0] for row in super().execute("SELECT id FROM sessions")}
            )
            if self.harness.evict_once:
                for session_id in self.harness.evict_once:
                    super().execute("DELETE FROM sessions WHERE id = ?", (session_id,))
                self.harness.evict_once = []
        if sql == "PRAGMA page_count" and self.harness.write_after_pin:
            self.harness.add_session("after-pin", message_id=99)
            self.harness.write_after_pin = False
        return super().execute(sql, parameters)


class TransferHarness:
    def __init__(self, directory):
        self.source = directory / "source.db"
        self.target = directory / "target.db"
        self.checkpoint = directory / "checkpoint.json"
        self.parent_checks = []
        self.evict_once = []
        self.write_after_pin = False
        self.fail_commit_table = None
        for path in (self.source, self.target):
            with sqlite3.connect(path) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.executescript(SCHEMA)

    def connect(self, _dsn):
        connection = sqlite3.connect(self.target, isolation_level=None, factory=ObservedTarget)
        connection.harness = self
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @staticmethod
    def schema_ready(connection):
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    def run(self, **options):
        return migration.online_backfill(
            self.source, "offline", checkpoint_path=self.checkpoint,
            batch_rows=2, _target_factory=self.connect,
            _initialize_target=self.schema_ready, _finalize_target=self.schema_ready,
            **options,
        )

    def add_session(self, session_id, message_id=None, parent=None):
        with sqlite3.connect(self.source) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
                (session_id, 1.0, 1.0, "initial", parent),
            )
            if message_id is not None:
                connection.execute(
                    "INSERT INTO messages VALUES (?, ?, ?)", (message_id, session_id, "hello")
                )

    def saved(self):
        return json.loads(self.checkpoint.read_text())

    def target_rows(self, sql):
        with sqlite3.connect(self.target) as connection:
            return connection.execute(sql).fetchall()

    def assert_same(self):
        with sqlite3.connect(self.source) as source, sqlite3.connect(self.target) as target:
            assert target.execute("PRAGMA foreign_key_check").fetchall() == []
            for table in MIGRATED_TABLES:
                assert sorted(source.execute(f'SELECT * FROM "{table}"').fetchall()) == sorted(
                    target.execute(f'SELECT * FROM "{table}"').fetchall()
                ), table


@pytest.fixture
def transfer(tmp_path):
    return TransferHarness(tmp_path)


@pytest.mark.parametrize("session_id", ["000-backdated", "zzz", "20260906_122448_19aa12", "quoted'parent"])
@pytest.mark.parametrize("legacy_checkpoint", [False, True])
def test_completed_sessions_rescan_before_messages(transfer, session_id, legacy_checkpoint):
    transfer.add_session("middle", message_id=1)
    transfer.run()
    previous = transfer.saved()
    assert previous["tables"]["sessions"]["complete"]
    if legacy_checkpoint:
        for state in previous["tables"].values():
            state.pop("tc")
            state.pop("tc_prev")
        transfer.checkpoint.write_text(json.dumps(previous))
    transfer.add_session(session_id, message_id=2, parent="middle")
    transfer.parent_checks.clear()

    transfer.run(resume=True)

    assert transfer.parent_checks == [{"middle", session_id}]
    transfer.assert_same()
    saved = transfer.saved()
    assert saved["tables"]["sessions"]["tc_prev"] == previous["tables"]["sessions"].get("tc")
    assert saved["tables"]["messages"]["last_pk"] == [2]
    assert all(state["complete"] and state["tc"] == saved["pass_tc"] for state in saved["tables"].values())


def test_incomplete_string_watermark_is_not_incremental_boundary(transfer):
    transfer.add_session("aaa")
    transfer.add_session("middle")
    transfer.add_session("zzz", message_id=1)
    with pytest.raises(migration.InjectedBackfillFault):
        transfer.run(fault_inject_at="40%")
    assert transfer.saved()["tables"]["sessions"]["last_pk"] == ["middle"]
    assert not transfer.saved()["tables"]["sessions"]["complete"]
    transfer.add_session("000-late", message_id=2)

    transfer.run(resume=True)

    transfer.assert_same()


def test_partial_messages_resume_after_completed_sessions(transfer):
    transfer.add_session("middle", message_id=1)
    with sqlite3.connect(transfer.source) as source:
        source.executemany(
            "INSERT INTO messages VALUES (?, 'middle', 'hello')", [(index,) for index in range(2, 6)]
        )
    with pytest.raises(migration.InjectedBackfillFault):
        transfer.run(fault_inject_at="50%")
    saved = transfer.saved()
    assert saved["tables"]["sessions"]["complete"]
    assert not saved["tables"]["messages"]["complete"]
    assert saved["tables"]["messages"]["last_pk"] == [2]
    transfer.add_session("20260906_122448_19aa12", message_id=6)

    transfer.run(resume=True)

    transfer.assert_same()


@pytest.mark.parametrize("table", ["sessions", "messages"])
def test_committed_batch_without_checkpoint_is_safe_to_repeat(transfer, table):
    transfer.add_session("middle", message_id=1)
    transfer.fail_commit_table = table
    with pytest.raises(OSError, match="after commit, before checkpoint"):
        transfer.run()
    assert transfer.saved()["tables"][table]["last_pk"] is None
    assert transfer.target_rows(f'SELECT COUNT(*) FROM "{table}"') == [(1,)]

    transfer.run(resume=True)

    transfer.assert_same()


def test_snapshot_is_stable_but_next_pass_sees_concurrent_write(transfer):
    transfer.add_session("middle", message_id=1)
    transfer.write_after_pin = True

    transfer.run()

    assert transfer.target_rows("SELECT id FROM messages") == [(1,)]
    transfer.run(resume=True)
    transfer.assert_same()


def test_updates_backdated_keys_parent_clear_and_two_idempotent_resumes(transfer):
    transfer.add_session("parent")
    transfer.add_session("child", message_id=1, parent="parent")
    with sqlite3.connect(transfer.source) as source:
        source.executescript("""
            INSERT INTO system_prompts VALUES ('hash', 'prompt');
            INSERT INTO session_model_usage (session_id, model, input_tokens) VALUES ('child', 'model', 1);
            INSERT INTO state_meta VALUES ('key', 'before');
            INSERT INTO gateway_routing VALUES ('scope', 'key', '{}', 1);
            INSERT INTO compression_locks VALUES ('child', 'before', 1);
            INSERT INTO session_turn_leases VALUES ('child', 'before', 1);
            INSERT INTO gateway_hygiene_state VALUES ('child', 1);
            INSERT INTO levos_control_tower_event VALUES ('zzz', 1, 'before');
            INSERT INTO levos_control_tower_delivery VALUES ('zzz', 1, 1, 0);
            INSERT INTO levos_control_tower_role VALUES ('role', 'child', 1);
            INSERT INTO levos_control_tower_forward VALUES ('zzz', 'zzz', NULL);
            INSERT INTO async_delegations VALUES ('zzz', 1, 'before');
            INSERT INTO delivery_obligations VALUES ('zzz', 1, 'before');
        """)
    transfer.run()
    with sqlite3.connect(transfer.source) as source:
        source.executescript("""
            UPDATE sessions SET title='after', last_activity_at=-1, parent_session_id=NULL;
            UPDATE session_model_usage SET input_tokens=9;
            UPDATE state_meta SET value='after';
            UPDATE gateway_routing SET entry_json='{"new":true}', updated_at=-1;
            UPDATE compression_locks SET holder='after';
            UPDATE session_turn_leases SET holder='after';
            UPDATE gateway_hygiene_state SET failure_streak=9;
            UPDATE levos_control_tower_event SET payload_text='after';
            INSERT INTO levos_control_tower_event VALUES ('000', -1, 'late');
            UPDATE levos_control_tower_delivery SET inject_ok=1;
            INSERT INTO levos_control_tower_delivery VALUES ('000', 1, -1, 0);
            UPDATE levos_control_tower_role SET session_id='parent';
            UPDATE levos_control_tower_forward SET delivered_at=2;
            UPDATE async_delegations SET state='after';
            INSERT INTO async_delegations VALUES ('000', -1, 'late');
            UPDATE delivery_obligations SET state='after';
        """)

    transfer.run(resume=True)
    transfer.assert_same()
    for _ in range(2):
        summary = transfer.run(resume=True)
        assert summary["imported_sessions"] == 0
        transfer.assert_same()


def test_foreign_key_repair_copies_parent_and_ancestor_first(transfer):
    transfer.add_session("middle", message_id=1)
    transfer.run()
    transfer.add_session("ancestor")
    transfer.add_session("late", message_id=2, parent="ancestor")
    transfer.evict_once = ["late", "ancestor"]

    transfer.run(resume=True)

    transfer.assert_same()


def test_foreign_key_repair_handles_self_reference(transfer):
    transfer.add_session("self", message_id=1, parent="self")
    transfer.evict_once = ["self"]

    transfer.run()

    transfer.assert_same()


def test_source_orphan_fails_without_advancing_message_checkpoint(transfer):
    transfer.add_session("middle", message_id=1)
    transfer.run()
    with sqlite3.connect(transfer.source) as source:
        source.execute("INSERT INTO messages VALUES (2, 'deleted-parent', 'orphan')")

    with pytest.raises(migration.MissingSessionError, match="deleted-parent.*message_ids=\\[2\\]"):
        transfer.run(resume=True)

    saved = transfer.saved()
    assert not saved["completed"]
    assert not saved["tables"]["messages"]["complete"]
    assert saved["tables"]["messages"]["last_pk"] == [1]
    assert transfer.target_rows("SELECT id FROM messages") == [(1,)]
    transfer.add_session("deleted-parent")
    transfer.run(resume=True)
    transfer.assert_same()


def test_failed_resume_revokes_global_and_fts_completion(transfer):
    transfer.add_session("middle", message_id=1)
    transfer.run()
    previous = transfer.saved()
    previous["fts"] = {"complete": True, "last_pk": 1, "tc": previous["pass_tc"]}
    transfer.checkpoint.write_text(json.dumps(previous))

    with pytest.raises(migration.BackfillBudgetExceeded):
        transfer.run(resume=True, budget_bytes=1)

    saved = transfer.saved()
    assert not saved["completed"]
    assert not saved["fts"]["complete"] and saved["fts"]["last_pk"] is None
    assert saved["fts"]["tc_prev"] == previous["pass_tc"]
    assert not saved["tables"]["sessions"]["complete"]
    assert saved["tables"]["sessions"]["tc"] == previous["pass_tc"]


def test_dependency_order_covers_all_fifteen_tables(transfer):
    source = open_sqlite_snapshot(transfer.source)
    try:
        names = [spec.name for spec in sqlite_table_specs(source)]
        assert names == list(MIGRATED_TABLES)
        assert names[:3] == ["system_prompts", "sessions", "messages"]
        assert names.index("levos_control_tower_event") < names.index("levos_control_tower_delivery")
        assert names.index("levos_control_tower_event") < names.index("levos_control_tower_forward")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            source.execute("DELETE FROM sessions")
    finally:
        source.close()


class CopyStream:
    def __init__(self, connection, sql):
        self.connection = connection
        self.table, self.columns = re.fullmatch(r'COPY "(\w+)" \((.*)\) FROM STDIN', sql).groups()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def write_row(self, values):
        placeholders = ", ".join("?" for _ in values)
        self.connection.execute(
            f'INSERT INTO "{self.table}" ({self.columns}) VALUES ({placeholders})', values
        )


class CopyDriver:
    """Emulate only psycopg COPY/temp-table protocol over a real SQLite target."""

    def __init__(self, connection):
        self.connection = connection
        self.statements = []
        self.connection.execute("ALTER TABLE messages ADD COLUMN fts_content TEXT")

    def cursor(self):
        return self

    def copy(self, sql):
        self.statements.append(sql)
        return CopyStream(self.connection, sql)

    def execute(self, sql, parameters=()):
        self.statements.append(sql)
        match = re.fullmatch(
            r'CREATE TEMP TABLE "(\w+)" \(LIKE "(\w+)" INCLUDING DEFAULTS\) ON COMMIT DROP', sql
        )
        if match:
            staging, table = match.groups()
            return self.connection.execute(f'CREATE TEMP TABLE "{staging}" AS SELECT * FROM "{table}" WHERE 0')
        return self.connection.execute(sql, parameters)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()


@pytest.mark.parametrize("postgres_protocol", [False, True])
def test_pk_conflict_policies_and_constraint_rollback(transfer, postgres_protocol):
    transfer.add_session("middle", message_id=1)
    transfer.run()
    source = open_sqlite_snapshot(transfer.source)
    connection = sqlite3.connect(transfer.target, isolation_level=None)
    connection.execute("PRAGMA foreign_keys=ON")
    target = CopyDriver(connection) if postgres_protocol else connection
    try:
        specs = {spec.name: spec for spec in sqlite_table_specs(source)}
        session = dict(source.execute("SELECT * FROM sessions").fetchone())
        session["title"] = "changed"
        assert migration._copy_batch(target, specs["sessions"], [session]) == 1
        assert connection.execute("SELECT title FROM sessions").fetchone() == ("changed",)
        message = {"id": 1, "session_id": "middle", "content": "changed"}
        assert migration._copy_batch(target, specs["messages"], [message]) == 1
        assert connection.execute("SELECT content FROM messages").fetchone() == ("changed",)
        prompt = {"hash": "immutable", "prompt": "original"}
        assert migration._copy_batch(target, specs["system_prompts"], [prompt]) == 1
        assert migration._copy_batch(target, specs["system_prompts"], [dict(prompt, prompt="changed")]) == 0
        assert connection.execute("SELECT prompt FROM system_prompts").fetchone() == ("original",)
        with pytest.raises(sqlite3.IntegrityError):
            migration._copy_batch(target, specs["messages"], [
                {"id": 2, "session_id": "middle", "content": "valid"},
                {"id": 3, "session_id": "middle", "content": None},
            ])
        assert connection.execute("SELECT COUNT(*) FROM messages").fetchone() == (1,)
        connection.execute("CREATE UNIQUE INDEX unique_title ON sessions(title)")
        with pytest.raises(sqlite3.IntegrityError):
            migration._copy_batch(target, specs["sessions"], [dict(session, id="other")])
        if postgres_protocol:
            assert any('ON CONFLICT ("id") DO UPDATE' in sql for sql in target.statements)
            assert any('ON CONFLICT ("hash") DO NOTHING' in sql for sql in target.statements)
            assert connection.execute("SELECT fts_content FROM messages").fetchone() == (None,)
    finally:
        source.close()
        connection.close()


def test_bounded_antijoin_handles_more_than_one_lookup_chunk(transfer):
    with sqlite3.connect(transfer.source) as source:
        source.executemany(
            "INSERT INTO sessions VALUES (?, 1, 1, 'value', NULL)",
            [(f"session-{index}",) for index in range(205)],
        )
    transfer.run()
    source = open_sqlite_snapshot(transfer.source)
    target = transfer.connect("offline")
    try:
        spec = next(spec for spec in sqlite_table_specs(source) if spec.name == "sessions")
        rows = source.execute("SELECT * FROM sessions").fetchall()
        assert migration._changed_rows(target, spec, rows) == []
        target.execute("DELETE FROM sessions WHERE id = ?", ("session-200",))
        assert [row["id"] for row in migration._changed_rows(target, spec, rows)] == ["session-200"]
    finally:
        source.close()
        target.close()
