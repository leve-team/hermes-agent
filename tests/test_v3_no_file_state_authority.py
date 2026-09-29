"""levos v3 (t_aa3728da) — a PostgreSQL-authority runtime leaves no operational state on disk.

One spawned process plays a v3 pod on a fresh ``HERMES_HOME`` whose profile selects
``sessions.state_backend: authority`` on the fork's ephemeral PostgreSQL: it starts the messaging
gateway (``gateway.run.start_gateway``), creates a tui session and submits a prompt through the
tui gateway's JSON-RPC handler (the model is a local OpenAI-compatible stand-in), closes and
resumes that session (each session's notification poller ticks while idle), runs one cron tick
with a due job, takes and releases a messaging adapter's credential lock, asks the dashboard's
``/api/status`` once (t_0eeaa6f9), and shuts the gateway down. ``sqlite3.connect`` is replaced
by a recorder before anything is imported. ``run_pod`` is shared with
:mod:`tests.test_v3_legacy_state_db_untouched` (the same pod over a pre-authority ``state.db``).

Afterwards no store file may exist under the home or the gateway lock directory — ``*.db``,
``*.sqlite*``, ``*-wal``, ``*-shm``, ``gateway-locks/``, ``sessions.json``,
``channel_directory.json``, ``gateway_state.json``, ``desktop/interrupted_turns.json``,
``cron/deliveries.db`` — and ``sqlite3.connect`` (``:memory:`` included) must not have been
called. Logs, caches, skills and the process identity files (``gateway.pid`` …) are not store
state and are not checked. The pod runs twice: with kanban on its own PostgreSQL backend, and as
the v3 deployment with the built-in kanban off (``kanban.enabled: false``, t_fb9c7b9e) and
``HERMES_KANBAN_BACKEND`` unset, staying up past the kanban watchers' first ticks. The per-store
contracts (E–H) are pinned below with their off-authority counterparts.
"""

from __future__ import annotations

import http.server
import json
import os
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import psycopg
import pytest

from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

REPO_ROOT = Path(__file__).resolve().parents[1]
UNREACHABLE_DSN = "postgresql://nobody@/postgres?host=/nonexistent-v3&connect_timeout=1"
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
)
# Store files a v3 pod must not leave (relative globs, checked recursively).
FORBIDDEN = (
    "*.db", "*.sqlite", "*.sqlite3", "*.sqlite-*", "*-wal", "*-shm",
    "sessions.json", "channel_directory.json", "gateway_state.json",
)
FORBIDDEN_PATHS = ("gateway-locks", "desktop/interrupted_turns.json", "cron/deliveries.db")


def _leftovers(*roots: Path) -> list:
    found = set()
    for root in roots:
        if not root.exists():
            continue
        for pattern in FORBIDDEN:
            found.update(str(p) for p in root.rglob(pattern))
        for relative in FORBIDDEN_PATHS:
            found.update(str(p) for p in root.rglob(relative))
    return sorted(found)


class _Model(http.server.BaseHTTPRequestHandler):
    """OpenAI chat-completions stand-in: one short answer per request."""

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
        message = {"role": "assistant", "content": "v3 pod reply"}
        body = {
            "id": "chatcmpl-v3", "object": "chat.completion", "created": 1, "model": "test-model",
            "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        }
        if request.get("stream"):
            body["object"] = "chat.completion.chunk"
            body["choices"][0]["delta"] = body["choices"][0].pop("message")
            raw, kind = ("data: " + json.dumps(body) + "\n\ndata: [DONE]\n\n").encode(), "text/event-stream"
        else:
            raw, kind = json.dumps(body).encode(), "application/json"
        self.send_response(200)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.send_error(404)

    def log_message(self, *_args):
        return None


@pytest.fixture
def model_url():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Model)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1"
    finally:
        server.shutdown()
        server.server_close()


# The pod: everything below runs in a fresh interpreter so the recorder sees every connect.
_POD = textwrap.dedent(r'''
    import sqlite3, sys, traceback
    CALLS = []
    _real_connect = sqlite3.connect

    def _recording_connect(*args, **kwargs):
        target = args[0] if args else kwargs.get("database")
        CALLS.append({"target": str(target), "stack": "".join(traceback.format_stack(limit=14)[:-1])})
        return _real_connect(*args, **kwargs)

    sqlite3.connect = _recording_connect

    import asyncio, json, os, signal, threading, time
    from pathlib import Path

    report = {"steps": []}
    BOOT = time.monotonic()
    home = Path(os.environ["HERMES_HOME"])

    def step(name, fn):
        started = time.monotonic()
        try:
            report["steps"].append([name, "ok", fn()])
        except BaseException as exc:
            report["steps"].append([name, "error", f"{type(exc).__name__}: {exc}"])
        report.setdefault("seconds", {})[name] = round(time.monotonic() - started, 1)

    TUI = {}

    def tui_turn():
        from tui_gateway import server
        created = server.handle_request({"id": "c", "method": "session.create",
                                         "params": {"cols": 80, "source": "tui"}})
        sid = created["result"]["session_id"]
        submitted = server.handle_request({"id": "s", "method": "prompt.submit",
                                           "params": {"session_id": sid, "text": "hello v3 pod"}})
        if "error" in submitted:
            raise RuntimeError(submitted["error"])
        session = server._sessions[sid]
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            thread = session.get("_run_thread")
            if thread is not None and not thread.is_alive() and not session.get("running"):
                break
            time.sleep(0.2)
        # Idle with its agent: the session's notification poller (bot-live, /loop, kanban) ticks.
        time.sleep(2)
        TUI.update(sid=sid, key=session["session_key"])
        db = server._get_db()
        rows = db.get_messages_as_conversation(session["session_key"])
        return [row.get("role") for row in rows]

    def tui_resume():
        from tui_gateway import server
        server.handle_request({"id": "x", "method": "session.close",
                               "params": {"session_id": TUI["sid"]}})
        resumed = server.handle_request({"id": "r", "method": "session.resume",
                                         "params": {"session_id": TUI["key"], "cols": 80}})
        if "error" in resumed:
            raise RuntimeError(resumed["error"])
        time.sleep(2)  # the resumed session's poller ticks too
        return bool(resumed["result"].get("session_id"))

    def api_status():
        from fastapi.testclient import TestClient
        from hermes_cli.web_server import app
        return TestClient(app).get("/api/status").status_code

    def cron_tick():
        from cron import jobs, scheduler
        job = jobs.create_job(prompt="say hi", schedule="every 1m", name="v3-guard", deliver="local")
        jobs.update_job(job["id"], {"next_run_at": "2000-01-01T00:00:00+00:00"})
        return scheduler.tick(verbose=False)

    def adapter_lock():
        from gateway.config import Platform, PlatformConfig
        from gateway.platforms.base import BasePlatformAdapter

        class LockOnly(BasePlatformAdapter):
            def __init__(self):
                super().__init__(PlatformConfig(enabled=True, token="tok"), Platform.SLACK)
            async def connect(self, *, is_reconnect=False):
                return True
            async def disconnect(self):
                self._release_platform_lock()
            async def send(self, chat_id, content, reply_to=None, metadata=None):
                raise NotImplementedError
            async def get_chat_info(self, chat_id):
                return {}

        adapter = LockOnly()
        held = adapter._acquire_platform_lock("slack-app-token", "xapp-v3-guard", "Slack app token")
        fenced = adapter._platform_lock_fences_pods
        adapter._release_platform_lock()
        return [held, fenced]

    def gateway_state():
        from gateway.status import read_runtime_status
        return (read_runtime_status() or {}).get("gateway_state")

    def driver():
        deadline = time.monotonic() + 120
        while gateway_state() != "running" and time.monotonic() < deadline:
            time.sleep(0.2)
        up_at = time.monotonic()
        report.setdefault("seconds", {})["gateway_up"] = round(up_at - BOOT, 1)
        step("runtime_status", gateway_state)
        step("tui_turn", tui_turn)
        step("tui_resume", tui_resume)
        step("cron_tick", cron_tick)
        step("adapter_lock", adapter_lock)
        step("api_status", api_status)
        # Stay up past the gateway's background watcher delays (kanban: 5 s, then a tick).
        while time.monotonic() - up_at < float(os.environ.get("V3_POD_MIN_UP_SECONDS") or 0):
            time.sleep(0.2)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=driver, daemon=True).start()
    from gateway.run import start_gateway
    try:
        report["gateway"] = repr(asyncio.run(start_gateway(verbosity=None)))
    except SystemExit as exc:
        report["gateway"] = f"SystemExit({exc.code})"
    report.setdefault("seconds", {})["total"] = round(time.monotonic() - BOOT, 1)
    report["sqlite_connects"] = CALLS
    Path(os.environ["V3_POD_REPORT"]).write_text(json.dumps(report), encoding="utf-8")
    os._exit(0)
''')


def run_pod(postgres_dsn: str, model_url: str, tmp_path: Path, *, kanban: str, seed=None) -> dict:
    """Run :data:`_POD` on a fresh authority ``HERMES_HOME`` (``tmp_path/home``) and return its
    report. ``seed(home)`` runs after the config is written and before the pod starts."""
    home = tmp_path / "home"
    locks = tmp_path / "xdg-state"
    home.mkdir()
    (home / "config.yaml").write_text(
        "sessions:\n  state_backend: authority\n"
        f"model:\n  provider: custom\n  base_url: {model_url}\n  default: test-model\n"
        "  api_mode: chat_completions\n"
        "memory:\n  memory_enabled: false\n  user_profile_enabled: false\n"
        "terminal:\n  env: local\n"
        + ("kanban:\n  enabled: false\n" if kanban == "off" else ""),
        encoding="utf-8",
    )
    if seed is not None:
        seed(home)
    report_path = tmp_path / "report.json"
    env = {key: os.environ[key] for key in ("PATH", "LD_LIBRARY_PATH", "TMPDIR") if key in os.environ}
    if kanban == "postgres":
        env.update(HERMES_KANBAN_BACKEND="postgres", HERMES_KANBAN_POSTGRES_DSN=postgres_dsn)
    else:  # long enough for the kanban dispatcher's and notifier's first ticks if they ran
        env.update(V3_POD_MIN_UP_SECONDS="12")
    env.update(
        HOME=str(tmp_path), HERMES_HOME=str(home), XDG_STATE_HOME=str(locks),
        HERMES_STATE_POSTGRES_DSN=postgres_dsn, OPENAI_BASE_URL=model_url,
        OPENAI_API_KEY="local-test-only", PYTHONPATH=str(REPO_ROOT), PYTHONDONTWRITEBYTECODE="1",
        LANG="C.UTF-8", TZ="UTC", V3_POD_REPORT=str(report_path), NO_PROXY="127.0.0.1,localhost",
    )
    result = subprocess.run(
        [sys.executable, "-c", _POD], env=env, cwd=str(tmp_path), capture_output=True, text=True,
        encoding="utf-8",
        timeout=600,
    )
    assert report_path.exists(), result.stdout[-4000:] + result.stderr[-8000:]
    return json.loads(report_path.read_text(encoding="utf-8"))


def assert_pod_steps(report: dict) -> None:
    steps = {name: (status, detail) for name, status, detail in report["steps"]}
    assert steps["runtime_status"] == ("ok", "running"), steps  # read back from PostgreSQL
    assert steps["tui_turn"][0] == "ok", steps
    assert "assistant" in steps["tui_turn"][1] and "user" in steps["tui_turn"][1], steps
    assert steps["tui_resume"] == ("ok", True), steps
    assert steps["cron_tick"] == ("ok", 1), steps
    assert steps["adapter_lock"] == ("ok", [True, True]), steps
    assert steps["api_status"] == ("ok", 200), steps


def format_connects(connects: list) -> str:
    return "\n\n".join(
        f"{c['target']}\n" + "".join(c["stack"].splitlines(keepends=True)[-6:]) for c in connects)


@pytest.mark.parametrize("kanban", ["postgres", "off"])
def test_v3_pod_on_authority_keeps_no_state_files_and_opens_no_sqlite(
    postgres_dsn, model_url, tmp_path, kanban
):
    """``kanban``: ``postgres`` = kanban on its own PostgreSQL backend; ``off`` = the v3
    deployment (``kanban.enabled: false``, t_fb9c7b9e) with ``HERMES_KANBAN_BACKEND`` unset, so
    the default kanban backend would be SQLite if anything reached it."""
    report = run_pod(postgres_dsn, model_url, tmp_path, kanban=kanban)
    home, locks = tmp_path / "home", tmp_path / "xdg-state"
    assert_pod_steps(report)
    connects = report["sqlite_connects"]
    assert connects == [], format_connects(connects)
    assert _leftovers(home, locks) == []
    if kanban == "off":  # nothing kanban-shaped at all: no db, no board dir, no dispatcher lock
        assert sorted(str(p) for p in tmp_path.rglob("kanban*")) == []
    with psycopg.connect(postgres_dsn) as raw:
        assert raw.execute(
            "SELECT count(*) FROM messages WHERE content = 'hello v3 pod'").fetchone()[0] >= 1


# --------------------------------------------------------------------------
# Per-store contracts on and off authority (E: new 0.21.2 stores, F: state
# checks, G: hosted rooms, H: SQLite memory plugins, runtime status, schema).
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def authority(monkeypatch, postgres_dsn):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    return postgres_dsn


def _home() -> Path:
    return Path(os.environ["HERMES_HOME"])


def _no_sqlite(monkeypatch):
    import sqlite3

    def refuse(*args, **_kwargs):
        raise AssertionError(f"sqlite3.connect({args[:1]!r}) on PostgreSQL authority")

    monkeypatch.setattr(sqlite3, "connect", refuse)


def test_cron_delivery_queue_is_postgres_rows_with_lease_fencing(authority, monkeypatch):
    from cron import delivery_queue as dq

    with psycopg.connect(authority, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_cron_deliveries, core_cron_delivery_tombstones")
    _no_sqlite(monkeypatch)
    assert dq.enqueue("e1", {"id": "j"}, "hello")["status"] == "pending"
    assert dq.enqueue("e1", {"id": "j"}, "hello")["status"] == "pending"  # idempotent
    sent = []
    assert dq.drain(lambda job, content, failure: sent.append(content)) == 1
    assert sent == ["hello"] and dq.get_status("e1")["status"] == "delivered"

    dq.enqueue("e2", {"id": "j"}, "owned elsewhere")
    with psycopg.connect(authority, autocommit=True) as raw:
        # Another pod claimed it and still holds its lease: not ours to fence.
        raw.execute("UPDATE core_cron_deliveries SET status='delivering', owner_process_id='peer', "
                    "owner_pid=1, lease_expires_at=EXTRACT(EPOCH FROM clock_timestamp()) + 600 "
                    "WHERE execution_id='e2'")
        assert dq.recover_abandoned() == 0
        raw.execute("UPDATE core_cron_deliveries SET lease_expires_at=0 WHERE execution_id='e2'")
    assert dq.recover_abandoned() == 1
    assert dq.get_status("e2")["status"] == "unknown"
    assert not list(_home().rglob("deliveries.db*"))


def test_cron_delivery_queue_stays_sqlite_off_authority():
    from cron import delivery_queue as dq

    assert dq.enqueue("e1", {"id": "j"}, "hello")["status"] == "pending"
    assert (_home() / "cron" / "deliveries.db").exists()


def test_run_idempotency_is_postgres_rows_with_owner_leases(authority, monkeypatch):
    from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore

    with psycopg.connect(authority, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_run_idempotency")
    _no_sqlite(monkeypatch)
    store, peer = RunIdempotencyStore(), RunIdempotencyStore()
    assert store.durable is True
    outcome, _ = store.reserve("s", "k", "fp", "run-1", {"status": "running"}, owner_pid=1)
    assert outcome == "created"
    assert peer.reserve("s", "k", "fp", "run-2", {"status": "running"})[0] == "reused"
    assert peer.reserve("s", "k", "other", "run-3", {"status": "running"})[0] == "conflict"
    assert store.status_for_run("s", "run-1")["owner_live"] is True
    with psycopg.connect(authority, autocommit=True) as raw:
        # The owner is another pod whose lease ran out: its run is no longer live.
        raw.execute("UPDATE core_run_idempotency SET owner_instance='peer-pod', lease_expires_at=0")
    assert store.status_for_run("s", "run-1")["owner_live"] is False
    store.close()
    peer.close()
    assert not list(_home().rglob("runs_idempotency.db*"))


def test_run_idempotency_stays_sqlite_off_authority():
    from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore

    store = RunIdempotencyStore()
    assert store.reserve("s", "k", "fp", "run-1", {"status": "running"})[0] == "created"
    assert "owner_live" not in store.status_for_run("s", "run-1")
    store.close()
    assert (_home() / "runs_idempotency.db").exists()


def test_state_checks_probe_postgres_on_authority(authority, monkeypatch):
    from gateway.lifecycle_ledger import check_state_db_integrity
    from gateway.readiness import _probe_state_db
    from hermes_state import SessionDB

    SessionDB(read_only=False).close()  # the core schema exists
    _no_sqlite(monkeypatch)
    assert _probe_state_db(_home()) == {"status": "ok", "backend": "postgres"}
    assert check_state_db_integrity(_home()) == "ok"
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    degraded = _probe_state_db(_home())
    assert degraded["status"] == "degraded" and "nonexistent" not in json.dumps(degraded)
    verdict = check_state_db_integrity(_home())
    assert verdict.startswith("check-failed") and "nonexistent" not in verdict
    assert not (_home() / "state.db").exists()


def test_state_checks_keep_the_file_off_authority():
    from gateway.lifecycle_ledger import check_state_db_integrity
    from gateway.readiness import _probe_state_db

    assert _probe_state_db(_home()) == {"status": "ok", "detail": "not initialized"}
    assert check_state_db_integrity(_home()) == "absent"


def test_hosted_rooms_are_refused_on_authority(authority):
    from gateway import hosted_rooms
    from tui_gateway import methods_groups

    assert hosted_rooms.hosted_rooms_enabled() is False
    with pytest.raises(hosted_rooms.HostedRoomsDisabledError):
        hosted_rooms.default_db_path()
    assert methods_groups.start_hosted_room_service() is None
    assert not list(_home().parent.rglob("shared-state.db*"))


def test_hosted_rooms_keep_shared_state_off_authority():
    from gateway import hosted_rooms

    assert hosted_rooms.hosted_rooms_enabled() is True
    assert hosted_rooms.default_db_path().name == "shared-state.db"


def test_sqlite_memory_plugins_are_an_explicit_error_on_authority(authority, monkeypatch):
    from hermes_aux_store import AuxStoreUnavailable
    from plugins.memory.holographic.store import MemoryStore
    from plugins.memory.retaindb import _WriteQueue

    _no_sqlite(monkeypatch)
    with pytest.raises(AuxStoreUnavailable, match="memory_store.db"):
        MemoryStore()
    with pytest.raises(AuxStoreUnavailable, match="retaindb_queue.db"):
        _WriteQueue(object(), _home() / "retaindb_queue.db")
    assert not list(_home().rglob("*.db"))


def test_sqlite_memory_plugin_works_off_authority():
    from plugins.memory.holographic.store import MemoryStore

    store = MemoryStore()
    store.close()
    assert (_home() / "memory_store.db").exists()


def test_runtime_status_is_a_postgres_row_on_authority(authority, monkeypatch):
    from gateway import status

    _no_sqlite(monkeypatch)
    status.write_runtime_status(gateway_state="running", active_agents=2)
    record = status.read_runtime_status()
    assert (record["gateway_state"], record["active_agents"]) == ("running", 2)
    assert not (_home() / "gateway_state.json").exists()


def test_runtime_status_stays_a_file_off_authority():
    from gateway import status

    status.write_runtime_status(gateway_state="running")
    assert json.loads((_home() / "gateway_state.json").read_text(encoding="utf-8"))["gateway_state"] == "running"


def test_postgres_schema_reconcile_reads_the_columns_sqlite_would():
    """The PostgreSQL path parses SCHEMA_SQL itself (no ``sqlite3.connect(":memory:")``);
    it must declare exactly what SQLite's PRAGMA table_info reports."""
    from hermes_state_common import SCHEMA_SQL
    from hermes_state_pg_columns import declared_schema_columns
    from hermes_state_schema import SessionSchemaMixin

    edge = ("CREATE TABLE t (a INTEGER, b  VARCHAR( 10 ) NOT NULL DEFAULT (strftime('%s','now')), "
            "c double   precision DEFAULT -1.5, d TEXT DEFAULT 'x,y', e, f TEXT NOT NULL, "
            "PRIMARY KEY (a, f)); CREATE TABLE IF NOT EXISTS \"q\" (id INTEGER PRIMARY KEY "
            "AUTOINCREMENT, x TEXT CHECK (x IN ('a','b')) NOT NULL, -- note, with a comma\n"
            " y REAL DEFAULT 0.0, FOREIGN KEY (x) REFERENCES t(d))")
    for schema in (SCHEMA_SQL, edge):
        assert declared_schema_columns(schema) == SessionSchemaMixin._parse_schema_columns(schema)
