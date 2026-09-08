"""OFF-window delete reconciliation against throwaway SQLite targets."""

import json
import re
import sqlite3

import pytest

import migrate_state_to_postgres as migration
import state_diff
from state_transfer import TableSpec


SCHEMA = """
CREATE TABLE system_prompts (hash TEXT PRIMARY KEY, prompt TEXT NOT NULL);
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    parent_session_id TEXT REFERENCES sessions(id)
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    content TEXT,
    observed INTEGER DEFAULT 0,
    display_only INTEGER DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    compacted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE session_model_usage (
    session_id TEXT REFERENCES sessions(id),
    model TEXT NOT NULL,
    PRIMARY KEY (session_id, model)
);
"""


class DeleteObservedTarget(sqlite3.Connection):
    harness = None

    def executemany(self, sql, parameters):
        match = re.match(r'DELETE FROM "(\w+)"', sql)
        if match:
            self.harness.delete_order.append(match.group(1))
        return super().executemany(sql, parameters)


class ReconcileHarness:
    def __init__(self, directory):
        self.source = directory / "source.db"
        self.target = directory / "target.db"
        self.checkpoint = directory / "checkpoint.json"
        self.backups = directory / "backups"
        self.delete_order = []
        for path in (self.source, self.target):
            with sqlite3.connect(path) as conn:
                conn.execute("PRAGMA foreign_keys=ON")
                conn.executescript(SCHEMA)

    def connect(self, _dsn):
        conn = sqlite3.connect(
            self.target,
            isolation_level=None,
            factory=DeleteObservedTarget,
        )
        conn.row_factory = sqlite3.Row
        conn.harness = self
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @staticmethod
    def schema_ready(conn):
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    def run(self, **options):
        return migration.online_backfill(
            self.source,
            "offline",
            checkpoint_path=self.checkpoint,
            batch_rows=2,
            budget_bytes=1024 * 1024 * 1024,
            _target_factory=self.connect,
            _initialize_target=self.schema_ready,
            _finalize_target=self.schema_ready,
            **options,
        )

    def seed(self):
        with sqlite3.connect(self.source) as conn:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executemany(
                "INSERT INTO sessions VALUES (?, NULL)",
                [("keep",), ("doomed",)],
            )
            conn.executemany(
                "INSERT INTO messages (id, session_id, content) VALUES (?, ?, ?)",
                [(1, "keep", "retained"), (2, "doomed", "backup me")],
            )
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('doomed', 'model')"
            )
        self.run()
        with sqlite3.connect(self.target) as conn:
            conn.execute("INSERT INTO system_prompts VALUES ('extra', 'immutable value')")

    def delete_authoritative_session(self):
        with sqlite3.connect(self.source) as conn:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("DELETE FROM session_model_usage WHERE session_id='doomed'")
            conn.execute("DELETE FROM messages WHERE session_id='doomed'")
            conn.execute("DELETE FROM sessions WHERE id='doomed'")

    def rows(self, sql):
        with sqlite3.connect(self.target) as conn:
            return conn.execute(sql).fetchall()


@pytest.fixture
def reconcile(tmp_path):
    harness = ReconcileHarness(tmp_path)
    harness.seed()
    harness.delete_authoritative_session()
    return harness


def test_reconcile_backs_up_then_deletes_children_before_sessions(reconcile):
    summary = reconcile.run(
        resume=True,
        reconcile_deletes_enabled=True,
        reconcile_ratio=1.0,
        reconcile_backup_dir=reconcile.backups,
    )

    assert reconcile.rows("SELECT id FROM messages ORDER BY id") == [(1,)]
    assert reconcile.rows("SELECT id FROM sessions ORDER BY id") == [("keep",)]
    assert reconcile.rows("SELECT * FROM session_model_usage") == []
    assert reconcile.rows("SELECT * FROM system_prompts") == []
    assert reconcile.delete_order.index("messages") < reconcile.delete_order.index("sessions")
    assert summary["reconcile"]["complete"] is True
    assert summary["reconcile"]["tables"]["messages"]["deleted"] == 1
    assert json.loads(reconcile.checkpoint.read_text())["reconcile_tc"] > 0

    backup_path = reconcile.backups / "messages.json"
    backup = json.loads(backup_path.read_text())
    assert backup["table"] == "messages"
    assert backup["primary_key"] == ["id"]
    assert backup["row_count"] == 1
    assert backup["rows"] == [{
        "pk": [2],
        "row": {
            "id": 2,
            "session_id": "doomed",
            "content": "backup me",
            "observed": 0,
            "display_only": 0,
            "active": 1,
            "compacted": 0,
        },
    }]


def test_reconcile_ratio_guard_is_global_and_deletes_nothing(reconcile):
    with pytest.raises(migration.ReconcileRatioExceeded) as caught:
        reconcile.run(
            resume=True,
            reconcile_deletes_enabled=True,
            reconcile_ratio=0.05,
            reconcile_backup_dir=reconcile.backups,
        )

    assert {item["table"] for item in caught.value.report["violations"]} == {
        "system_prompts", "sessions", "messages", "session_model_usage"
    }
    assert reconcile.rows("SELECT id FROM messages ORDER BY id") == [(1,), (2,)]
    assert reconcile.rows("SELECT id FROM sessions ORDER BY id") == [
        ("doomed",), ("keep",)
    ]
    assert reconcile.rows("SELECT hash FROM system_prompts") == [("extra",)]
    assert reconcile.delete_order == []
    assert not reconcile.backups.exists()
    saved = json.loads(reconcile.checkpoint.read_text())
    assert saved["completed"] is False
    assert saved["reconcile_tc"] == saved["reconcile"]["reconcile_tc"]


def test_reconcile_ratio_cli_returns_two_with_table_list(monkeypatch, tmp_path, capsys):
    report = {
        "violations": [{"table": "messages"}],
        "tables": {},
        "complete": False,
    }

    def blocked(*_args, **_kwargs):
        raise migration.ReconcileRatioExceeded(report)

    monkeypatch.setattr(migration, "migrate", blocked)
    rc = migration.main([
        "--sqlite-path", str(tmp_path / "state.db"),
        "--dsn", "test-only",
        "--reconcile-deletes",
    ])

    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"] == "RECONCILE_RATIO"
    assert payload["violations"] == [{"table": "messages"}]


def _diff_connection(rows):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE messages ("
        "id INTEGER PRIMARY KEY, session_id TEXT, content TEXT, observed INTEGER, "
        "display_only INTEGER, active INTEGER, compacted INTEGER)"
    )
    conn.executemany("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    return conn


def test_full_diff_two_pass_matches_one_pass_for_every_mismatch_kind():
    source_rows = [
        (1, "s", "same", 0, 0, 1, 0),
        (2, "s", "source content", 0, 0, 1, 0),
        (3, "s", "same flags", 0, 0, 0, 1),
        (4, "s", "missing", 0, 0, 1, 0),
    ]
    target_rows = [
        (1, "s", "same", 0, 0, 1, 0),
        (2, "s", "target content", 0, 0, 1, 0),
        (3, "s", "same flags", 0, 0, 1, 0),
        (5, "s", "extra", 0, 0, 1, 0),
    ]
    spec = TableSpec(
        "messages",
        ("id", "session_id", "content", "observed", "display_only", "active", "compacted"),
        ("id",),
    )
    source = _diff_connection(source_rows)
    target = _diff_connection(target_rows)
    try:
        two_pass = state_diff.state_diff_connections(
            source, target, specs=[spec], target_dialect="sqlite", batch_rows=2
        )
        one_pass = state_diff.state_diff_connections(
            source,
            target,
            specs=[spec],
            target_dialect="sqlite",
            batch_rows=2,
            two_pass_full=False,
        )
    finally:
        source.close()
        target.close()

    assert two_pass["tables"] == one_pass["tables"] == {
        "messages": {"missing": 1, "extra": 1, "differ": 2, "matched": 1}
    }
    assert two_pass["mismatch_count"] == one_pass["mismatch_count"] == 4
    assert two_pass["clean"] is one_pass["clean"] is False
    assert sorted(two_pass["samples"], key=lambda item: (item["kind"], item["pk"])) == sorted(
        one_pass["samples"], key=lambda item: (item["kind"], item["pk"])
    )


def test_full_diff_second_pass_is_required_to_detect_content_mutation():
    spec = TableSpec(
        "messages",
        ("id", "session_id", "content", "active", "compacted"),
        ("id",),
    )
    source = _diff_connection([(1, "s", "authority", 0, 0, 1, 0)])
    target = _diff_connection([(1, "s", "mutated", 0, 0, 1, 0)])
    source_sql = []
    target_sql = []
    source.set_trace_callback(source_sql.append)
    target.set_trace_callback(target_sql.append)
    try:
        report = state_diff.state_diff_connections(
            source, target, specs=[spec], target_dialect="sqlite"
        )
    finally:
        source.close()
        target.close()

    assert report["tables"]["messages"] == {
        "missing": 0, "extra": 0, "differ": 1, "matched": 0
    }
    selects = [sql for sql in source_sql + target_sql if sql.startswith("SELECT")]
    assert any('SELECT "id", "active", "compacted"' in sql for sql in selects)
    assert any('SELECT "id", "session_id", "content"' in sql for sql in selects)
