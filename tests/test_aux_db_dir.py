"""``HERMES_AUX_DB_DIR`` relocates only the regenerable auxiliary SQLite files.

Contract (docs/state-backend-aux-databases.md): unset keeps every file under
``HERMES_HOME`` exactly as before; set moves ``cron/executions.db``,
``response_store.db`` and ``verification_evidence.db`` — and nothing else —
under the given directory. The authoritative stores (``cron/notepad.db``,
``projects.db``) must ignore it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from hermes_constants import AUX_DB_DIR_ENV, aux_db_path, get_hermes_home


@pytest.fixture
def home(monkeypatch, tmp_path):
    h = tmp_path / "home"
    h.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(h))
    monkeypatch.delenv(AUX_DB_DIR_ENV, raising=False)
    return h


def test_aux_db_path_defaults_to_hermes_home(home):
    assert aux_db_path("cron/executions.db") == home / "cron" / "executions.db"
    assert aux_db_path("response_store.db") == home / "response_store.db"
    assert aux_db_path("verification_evidence.db") == home / "verification_evidence.db"


def test_aux_db_path_blank_value_is_unset(home, monkeypatch):
    monkeypatch.setenv(AUX_DB_DIR_ENV, "   ")
    assert aux_db_path("response_store.db") == home / "response_store.db"


def test_aux_db_path_honours_override(home, monkeypatch, tmp_path):
    aux = tmp_path / "scratch"
    monkeypatch.setenv(AUX_DB_DIR_ENV, str(aux))
    assert aux_db_path("cron/executions.db") == aux / "cron" / "executions.db"
    assert aux_db_path("response_store.db") == aux / "response_store.db"
    assert get_hermes_home() == home, "the override must not touch HERMES_HOME"


def test_executions_ledger_lands_in_aux_dir(home, monkeypatch, tmp_path):
    from cron import executions

    aux = tmp_path / "scratch"
    monkeypatch.setenv(AUX_DB_DIR_ENV, str(aux))
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", None)

    record = executions.create_execution("job-1", source="test")
    assert record["job_id"] == "job-1"
    assert (aux / "cron" / "executions.db").is_file()
    assert not (home / "cron" / "executions.db").exists()


def test_executions_ledger_stays_home_without_override(home, monkeypatch):
    from cron import executions

    monkeypatch.setattr(executions, "EXECUTIONS_FILE", None)
    executions.create_execution("job-2", source="test")
    assert (home / "cron" / "executions.db").is_file()


def test_verification_evidence_lands_in_aux_dir(home, monkeypatch, tmp_path):
    from agent import verification_evidence as ve

    aux = tmp_path / "scratch"
    monkeypatch.setenv(AUX_DB_DIR_ENV, str(aux))
    assert ve._db_path() == aux / "verification_evidence.db"
    conn = ve._connect()
    try:
        assert Path(conn.execute("PRAGMA database_list").fetchone()[2]) == (
            aux / "verification_evidence.db"
        )
    finally:
        conn.close()
    assert not (home / "verification_evidence.db").exists()


def test_response_store_lands_in_aux_dir(home, monkeypatch, tmp_path):
    pytest.importorskip("aiohttp")
    from gateway.platforms.api_server import ResponseStore

    aux = tmp_path / "scratch" / "nested"  # parent must be created on open
    monkeypatch.setenv(AUX_DB_DIR_ENV, str(aux))
    store = ResponseStore()
    try:
        assert store._db_path == str(aux / "response_store.db"), (
            "ResponseStore fell back to :memory: instead of the aux dir"
        )
        assert (aux / "response_store.db").is_file()
    finally:
        store._conn.close()
    assert not (home / "response_store.db").exists()


def test_authoritative_stores_ignore_aux_dir(home, monkeypatch, tmp_path):
    """notepad.db and projects.db are authoritative: never relocated."""
    from hermes_cli import projects_db

    aux = tmp_path / "scratch"
    monkeypatch.setenv(AUX_DB_DIR_ENV, str(aux))

    assert projects_db.projects_db_path() == home / "projects.db"

    # notepad resolves its file at import time from HERMES_HOME and never
    # consults the aux dir, so the constant can only point under a home.
    import cron.notepad as notepad

    assert notepad.NOTEPAD_FILE.name == "notepad.db"
    assert aux not in notepad.NOTEPAD_FILE.parents


def test_aux_dir_files_are_real_sqlite(home, monkeypatch, tmp_path):
    from cron import executions

    aux = tmp_path / "scratch"
    monkeypatch.setenv(AUX_DB_DIR_ENV, str(aux))
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", None)
    executions.create_execution("job-3", source="test")
    with sqlite3.connect(aux / "cron" / "executions.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 1
