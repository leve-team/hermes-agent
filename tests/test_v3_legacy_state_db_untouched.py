"""levos v3 (t_0eeaa6f9) — an authority pod never opens the legacy ``state.db`` left on its PVC.

A profile moved to ``sessions.state_backend: authority`` keeps the ``state.db`` it wrote before
the switch (operators do not delete it). On opsi-v3 (2026-09-30) the new runtime still bumped
``state.db-wal`` / ``state.db-shm`` minutes after it started: paths that "open ``state.db`` only
when the file exists" assumed an authority pod has no such file. Even a read-only SQLite open of
a WAL database writes ``-shm`` and may checkpoint ``-wal``, so "it only reads" is no excuse.

The pod scenario of :mod:`tests.test_v3_no_file_state_authority` (gateway start, tui turn,
session resume with the session notification poller ticking, cron tick, messaging lock,
dashboard ``/api/status``, shutdown; kanban off as deployed) runs on a home that already holds a
WAL-mode ``state.db`` with sessions (one titled ``Bot Chat``) and live ``-wal`` / ``-shm``
sidecars, and a Bot Chat live-delivery mailbox. The three files must keep their existence, size and mtime, and ``sqlite3.connect``
must not be called at all. The readers that used to open the file answer from PostgreSQL
instead (Bot Chat owner and compression lineage, the a2a forwarded-session lookup); the
pre-authority ones keep their SQLite path off authority (their own test modules).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn
from tests.test_v3_no_file_state_authority import (
    REPO_ROOT,
    _leftovers,
    assert_pod_steps,
    format_connects,
    model_url as model_url,
    run_pod,
)

LEGACY = ("state.db", "state.db-wal", "state.db-shm")

# Writes the pre-authority store the way a v2 pod left it: SQLite schema, WAL journal, committed
# rows still in ``-wal`` (the process ends without the close-time checkpoint).
_LEGACY_WRITER = textwrap.dedent(r'''
    import os, sqlite3, sys
    from pathlib import Path
    from hermes_state import SessionDB

    path = Path(sys.argv[1])
    raw = sqlite3.connect(path)
    assert raw.execute("PRAGMA journal_mode=wal").fetchone()[0] == "wal"
    raw.close()
    db = SessionDB(db_path=path)
    for index, title in enumerate(("Bot Chat", "v2 chat", "v2 notes")):
        sid = db.create_session(f"v2-legacy-{index}", "tui")
        db.set_session_title(sid, title)
        db.append_message(sid, "user", f"legacy message {index}")
    raw = sqlite3.connect(path)
    raw.execute("PRAGMA wal_autocheckpoint=0")
    raw.execute("UPDATE sessions SET title = title")
    raw.commit()
    os._exit(0)
''')


def _seed_legacy_state_db(home: Path) -> None:
    env = {key: os.environ[key] for key in ("PATH", "LD_LIBRARY_PATH", "TMPDIR") if key in os.environ}
    env.update(HERMES_HOME=str(home.parent / "legacy-writer-home"), HOME=str(home.parent),
               PYTHONPATH=str(REPO_ROOT), PYTHONDONTWRITEBYTECODE="1")
    subprocess.run([sys.executable, "-c", _LEGACY_WRITER, str(home / "state.db")], env=env,
                   check=True, capture_output=True, timeout=120)
    # A profile that took Bot Chat live deliveries before the switch keeps its mailbox too.
    (home / "runtime" / "bot_live_delivery").mkdir(parents=True, mode=0o700)


def _stat(home: Path) -> dict:
    stats = {}
    for name in LEGACY:
        path = home / name
        stats[name] = (path.stat().st_size, path.stat().st_mtime_ns) if path.exists() else None
    return stats


def test_authority_pod_leaves_the_legacy_state_db_untouched(postgres_dsn, model_url, tmp_path):
    home = tmp_path / "home"
    before = {}

    def seed(home: Path) -> None:
        _seed_legacy_state_db(home)
        before.update(_stat(home))

    report = run_pod(postgres_dsn, model_url, tmp_path, kanban="off", seed=seed)

    assert all(before[name] is not None for name in LEGACY), before  # a live WAL store
    assert_pod_steps(report)
    connects = report["sqlite_connects"]
    assert connects == [], format_connects(connects)
    assert _stat(home) == before
    legacy = {str(home / name) for name in LEGACY}
    assert [path for path in _leftovers(home, tmp_path / "xdg-state") if path not in legacy] == []
    # The pod's conversation went to PostgreSQL, not into the legacy file.
    steps = {name: detail for name, _status, detail in report["steps"]}
    assert "assistant" in steps["tui_turn"], json.dumps(report["steps"])


# --------------------------------------------------------------------------
# The readers that opened the file, in process: on authority they answer from PostgreSQL.
# --------------------------------------------------------------------------


@pytest.fixture
def authority_home(monkeypatch, postgres_dsn):
    """The test's ``HERMES_HOME`` on PostgreSQL authority, holding a legacy WAL ``state.db``
    (grown sparse past the FTS-optimize notice's 0.5 GB floor). Any ``sqlite3.connect`` is
    recorded and refused; the legacy files must be unchanged afterwards."""
    import sqlite3

    for key in ("HERMES_STATE_DATABASE_URL", "HERMES_CORE_PG_DSN", "HERMES_STATE_DUAL_WRITE",
                "HERMES_PROFILE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    home = Path(os.environ["HERMES_HOME"])
    _seed_legacy_state_db(home)
    os.truncate(home / "state.db", 600 * 1024 ** 2)
    before, calls = _stat(home), []

    def refuse(*args, **_kwargs):
        calls.append(args[:1])
        raise AssertionError(f"sqlite3.connect({args[:1]!r}) on PostgreSQL authority")

    monkeypatch.setattr(sqlite3, "connect", refuse)
    yield home
    assert calls == []
    assert _stat(home) == before


def test_bot_chat_owner_and_lineage_come_from_postgres_on_authority(authority_home):
    from hermes_cli.active_sessions import transfer_active_session, try_acquire_active_session
    from hermes_state import SessionDB
    from tools import bot_live_delivery as mailbox

    db = SessionDB()
    assert db._is_postgres
    db.create_session(session_id="pg-chat", source="tui")
    db.set_session_title("pg-chat", "Bot Chat")
    meta = dict(live_session_id="live", bot_live_delivery_consumer=True)
    lease, refusal = try_acquire_active_session(session_id="pg-chat", surface="tui", config={},
                                               registry_home=authority_home, metadata=meta)
    assert refusal is None
    try:
        owner = mailbox.find_canonical_live_owner(authority_home)
        assert owner["session_id"] == "pg-chat"  # not the legacy file's "Bot Chat" (v2-legacy-0)
        queued = mailbox.deliver_to_live_owner(authority_home, owner, "before compression")
        db.end_session("pg-chat", "compression")
        db.create_session(session_id="pg-tip", source="tui", parent_session_id="pg-chat")
        assert transfer_active_session(lease, session_id="pg-tip", metadata=meta)
        current = mailbox.find_canonical_live_owner(authority_home)
        assert current["session_id"] == "pg-tip"
        claim = mailbox.claim_pending_delivery(authority_home, current)  # lineage via PostgreSQL
        assert claim["delivery_id"] == queued["delivery_id"]
    finally:
        lease.release()
        db.close()


def test_a2a_forwarded_session_lookup_and_title_use_postgres_on_authority(
    authority_home, monkeypatch
):
    import time

    from hermes_state import SessionDB
    from plugins.platforms.a2a import adapter

    monkeypatch.setattr(adapter, "_profile_home", lambda profile: str(authority_home))
    db = SessionDB()
    started = time.time()
    db.create_session(session_id="pg-a2a", source="a2a")
    db.create_session(session_id="pg-titled", source="a2a")
    db.set_session_title("pg-titled", "a2a-dev-ctx")
    by_title = adapter._state_db(
        "dev", "SELECT id FROM sessions WHERE title = ? ORDER BY started_at DESC LIMIT 1",
        ("a2a-dev-ctx",), "lookup")
    latest = adapter._state_db(
        "dev", "SELECT id FROM sessions WHERE source = 'a2a' AND started_at >= ? "
        "AND title IS NULL ORDER BY started_at DESC LIMIT 1", (started - 2.0,), "latest")
    adapter._state_db("dev", "UPDATE sessions SET title = ? WHERE id = ?", ("a2a-dev-new", "pg-a2a"),
                      "title", commit=True)
    assert (by_title, latest) == ("pg-titled", "pg-a2a")
    assert db.get_session("pg-a2a")["title"] == "a2a-dev-new"
    db.close()


def test_status_and_update_notices_skip_sqlite_fts_on_authority(authority_home, capsys):
    import asyncio

    from hermes_cli.update_cmd_maint import _print_fts_optimize_available_notice
    from hermes_cli.web_routers.status import _advisory_pressure

    status = {}
    asyncio.run(_advisory_pressure(status, authority_home))
    assert "fts_rebuild" not in status
    _print_fts_optimize_available_notice()
    assert capsys.readouterr().out == ""
