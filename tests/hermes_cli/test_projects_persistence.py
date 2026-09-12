"""Real SQLite/PostgreSQL projects parity, failure and ownership contracts."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from hermes_cli import projects_db as projects
from hermes_cli.projects_postgres import ProjectsPostgresConnection
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.hermes_cli.persistence_pg_support import (
    pg_dsn as pg_dsn,
    pg_server as pg_server,
    postgres_server,
)


@pytest.fixture(params=["sqlite", "postgres"])
def backend(request, monkeypatch):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", request.param)
    monkeypatch.delenv("HERMES_PROJECTS_POSTGRES_DSN", raising=False)
    if request.param == "postgres":
        monkeypatch.setenv(
            "HERMES_PROJECTS_POSTGRES_DSN", request.getfixturevalue("pg_dsn")
        )
    return request.param


@pytest.fixture
def connection(backend):
    with projects.connect_closing() as conn:
        if backend == "postgres":
            assert isinstance(conn, ProjectsPostgresConnection)
        else:
            assert isinstance(conn, sqlite3.Connection)
        yield conn


def test_create_update_folder_archive_delete_roundtrip(connection, tmp_path):
    root = str(tmp_path / "quote ' ? % 한글")
    child = str(tmp_path / "child")
    project_id = projects.create_project(
        connection,
        name="한글 ? % ' workspace",
        folders=[root],
        icon="🛠",
        description='{"nested":null}',
        board_slug="core",
    )
    project = projects.get_project(connection, project_id)
    assert project.to_dict() == {
        "id": project_id,
        "slug": "workspace",
        "name": "한글 ? % ' workspace",
        "created_at": project.created_at,
        "description": '{"nested":null}',
        "icon": "🛠",
        "color": None,
        "board_slug": "core",
        "primary_path": root,
        "archived": False,
        "folders": [
            {
                "path": root,
                "label": None,
                "is_primary": True,
                "added_at": project.created_at,
            }
        ],
    }
    assert projects.get_project(connection, project.slug).id == project_id
    assert projects.project_for_path(connection, root + "/src").id == project_id
    assert projects.update_project(
        connection, project_id, name="renamed", icon="", color="blue"
    )
    assert not projects.update_project(connection, project_id)
    projects.add_folder(connection, project_id, child, label="first", is_primary=True)
    projects.add_folder(connection, project_id, child, label="updated")
    assert len(projects.get_project(connection, project_id).folders) == 2
    assert projects.get_project(connection, project_id).folders[0].label == "updated"
    assert projects.set_primary(connection, project_id, root)
    assert not projects.set_primary(connection, project_id, str(tmp_path / "missing"))
    assert projects.remove_folder(connection, project_id, root)
    assert projects.get_project(connection, project_id).primary_path == child
    assert projects.archive_project(connection, project_id)
    assert projects.list_projects(connection) == []
    assert projects.project_for_path(connection, child) is None
    assert projects.list_projects(connection, include_archived=True)[0].archived
    assert projects.restore_project(connection, project_id)
    assert projects.delete_project(connection, project_id)
    assert connection.execute("SELECT count(*) FROM project_folders").fetchone()[0] == 0
    assert projects.get_project(connection, project_id) is None
    assert not projects.delete_project(connection, project_id)


def test_meta_discovery_upsert_policy_and_wide_epochs(connection, tmp_path):
    project_id = projects.create_project(connection, name="kept")
    projects.set_active(connection, project_id)
    projects.set_active(connection, project_id)
    assert projects.get_active_id(connection) == project_id
    roots = [(str(tmp_path / "one"), "? % ' 한글"), (str(tmp_path / "two"), None)]
    assert (
        projects.record_discovered_repos(connection, roots, policy_key="policy-a") == 2
    )
    projects.record_discovered_repos(connection, [(roots[0][0], "changed")])
    assert len(projects.list_discovered_repos(connection)) == 2
    with projects.write_txn(connection):
        connection.execute("UPDATE projects SET created_at = ?", (2**33,))
        connection.execute("UPDATE discovered_repos SET last_seen = ?", (2**33,))
    assert projects.get_project(connection, project_id).created_at == 2**33
    assert all(
        row["last_seen"] == 2**33 for row in projects.list_discovered_repos(connection)
    )
    assert not projects.reconcile_discovered_repos_policy(connection, "policy-a")
    assert projects.reconcile_discovered_repos_policy(connection, "policy-b")
    assert projects.list_discovered_repos(connection) == []
    assert projects.get_project(connection, project_id) is not None
    projects.record_discovered_repos(connection, roots)
    projects.record_discovered_repos(connection, roots[:1], replace=True)
    assert len(projects.list_discovered_repos(connection)) == 1
    projects.clear_discovered_repos(connection, policy_key="policy-c")
    assert projects.get_discovery_policy_key(connection) == "policy-c"
    projects.set_active(connection, None)
    assert projects.get_active_id(connection) is None


def test_failures_rollback_and_reuse(connection, tmp_path):
    with pytest.raises(ValueError, match="empty"):
        projects.create_project(connection, name=" ")
    with pytest.raises(ValueError, match="slug"):
        projects.create_project(connection, name="bad", slug="../bad")
    project_id = projects.create_project(
        connection, name="same", folders=[str(tmp_path)]
    )
    with pytest.raises(ValueError, match="already belongs"):
        projects.create_project(connection, name="duplicate", folders=[str(tmp_path)])
    second_id = projects.create_project(connection, name="same")
    assert projects.get_project(connection, second_id).slug == "same-2"
    with pytest.raises(sqlite3.IntegrityError):
        with projects.write_txn(connection):
            connection.execute(
                "UPDATE projects SET name = ? WHERE id = ?", ("rollback", project_id)
            )
            connection.execute(
                "INSERT INTO project_folders (project_id, path, added_at) VALUES (?, ?, ?)",
                ("missing", "/invalid", 1),
            )
    assert projects.get_project(connection, project_id).name == "same"
    with pytest.raises(sqlite3.IntegrityError):
        with projects.write_txn(connection):
            connection.execute(
                "UPDATE projects SET slug = ? WHERE id = ?", ("same", second_id)
            )
    assert projects.update_project(connection, project_id, name="reusable")


def test_nested_transaction_is_rejected_without_stealing_owner(connection):
    with pytest.raises(ValueError, match="outer abort"):
        with projects.write_txn(connection):
            connection.execute(
                "INSERT INTO project_meta (key, value) VALUES (?, ?)",
                ("outer", "value"),
            )
            with pytest.raises(
                (RuntimeError, sqlite3.OperationalError), match="nest|transaction"
            ):
                projects.set_active(connection, "inner")
            assert connection.in_transaction
            raise ValueError("outer abort")
    assert connection.execute("SELECT count(*) FROM project_meta").fetchone()[0] == 0
    assert not connection.in_transaction


def test_reconnect_and_close_preserve_committed_data(backend):
    with projects.connect_closing() as conn:
        project_id = projects.create_project(conn, name="durable")
    with pytest.raises((sqlite3.ProgrammingError, psycopg.OperationalError)):
        projects.list_projects(conn)
    with projects.connect_closing() as reopened:
        assert projects.get_project(reopened, project_id).name == "durable"


def test_project_tools_response_contract(backend, tmp_path):
    from tools import project_tools

    root = str(tmp_path / "workspace")
    created = json.loads(project_tools.project_create("Tool Project", root))
    assert created == {
        "success": True,
        "id": created["id"],
        "slug": "tool-project",
        "name": "Tool Project",
        "primary_path": root,
    }
    assert (
        json.loads(project_tools.project_create("Duplicate", root))["id"]
        == created["id"]
    )
    assert json.loads(project_tools.project_list()) == {
        "active_id": created["id"],
        "projects": [
            {
                "id": created["id"],
                "slug": "tool-project",
                "name": "Tool Project",
                "primary_path": root,
                "active": True,
            }
        ],
    }
    assert json.loads(project_tools.project_create(" ")) == {
        "success": False,
        "error": "name is required",
    }


def test_default_sqlite_does_not_import_postgres(tmp_path):
    environment = dict(os.environ, HERMES_HOME=str(tmp_path))
    environment.pop("HERMES_PROJECTS_BACKEND", None)
    environment["HERMES_PROJECTS_POSTGRES_DSN"] = "not-a-dsn"
    environment["HERMES_KANBAN_BACKEND"] = "postgres"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sqlite3, sys; from hermes_cli import projects_db; "
            "conn = projects_db.connect(); assert isinstance(conn, sqlite3.Connection); "
            "assert conn.execute('PRAGMA foreign_keys').fetchone()[0] == 1; "
            "conn.close(); assert 'psycopg' not in sys.modules; "
            "assert 'hermes_cli.projects_postgres' not in sys.modules",
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "projects.db").is_file()


@pytest.mark.parametrize("backend_value", ["", "pg", "POSTGRES", " sqlite "])
def test_invalid_backend_fails_before_file_creation(
    monkeypatch, tmp_path, backend_value
):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", backend_value)
    with pytest.raises(ValueError, match="HERMES_PROJECTS_BACKEND"):
        projects.connect(tmp_path / "absent" / "projects.db")
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("dsn", [None, "", " "])
def test_missing_dsn_does_not_fall_back(monkeypatch, dsn):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    monkeypatch.setenv("HERMES_KANBAN_POSTGRES_DSN", "not-a-projects-dsn")
    if dsn is None:
        monkeypatch.delenv("HERMES_PROJECTS_POSTGRES_DSN", raising=False)
    else:
        monkeypatch.setenv("HERMES_PROJECTS_POSTGRES_DSN", dsn)
    with pytest.raises(ValueError, match="HERMES_PROJECTS_POSTGRES_DSN"):
        projects.connect()
    assert not projects.projects_db_path().exists()


def test_failed_connection_does_not_fall_back(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    monkeypatch.setenv(
        "HERMES_PROJECTS_POSTGRES_DSN",
        make_conninfo(host=str(tmp_path), dbname="missing"),
    )
    with pytest.raises(
        sqlite3.OperationalError, match="connection or initialization failed"
    ) as raised:
        projects.connect()
    assert isinstance(raised.value.__cause__, psycopg.OperationalError)
    assert str(tmp_path) not in str(raised.value)
    assert not projects.projects_db_path().exists()


def test_postgres_rejects_explicit_sqlite_path(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    with pytest.raises(ValueError, match="db_path"):
        projects.connect(tmp_path / "other-profile" / "projects.db")
    assert not (tmp_path / "other-profile").exists()


def test_pg_legacy_columns_and_initialization_failure(pg_dsn, monkeypatch):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    monkeypatch.setenv("HERMES_PROJECTS_POSTGRES_DSN", pg_dsn)
    with psycopg.connect(pg_dsn, autocommit=True) as raw:
        raw.execute(
            "CREATE TABLE projects (id TEXT PRIMARY KEY, slug TEXT UNIQUE NOT NULL, "
            "name TEXT NOT NULL, description TEXT, created_at BIGINT NOT NULL, "
            "archived BIGINT NOT NULL DEFAULT 0)"
        )
        raw.execute(
            "INSERT INTO projects (id, slug, name, created_at) VALUES ('old', 'old', 'old', 1)"
        )
    with projects.connect_closing() as conn:
        assert projects.get_project(conn, "old").icon is None
        assert projects.update_project(conn, "old", icon="kept", board_slug="legacy")
    with projects.connect_closing() as conn:
        assert projects.get_project(conn, "old").icon == "kept"
    with psycopg.connect(pg_dsn, autocommit=True) as raw:
        raw.execute("DROP SCHEMA public CASCADE")
        raw.execute("CREATE SCHEMA public")
        raw.execute("CREATE TABLE project_folders (wrong TEXT)")
    with pytest.raises(
        sqlite3.OperationalError, match="connection or initialization failed"
    ) as raised:
        projects.connect()
    assert isinstance(raised.value.__cause__, psycopg.errors.UndefinedColumn)
    with psycopg.connect(pg_dsn) as raw:
        assert raw.execute("SELECT to_regclass('projects')").fetchone()[0] is None


def test_pg_borrowed_savepoint_keeps_outer_rollback(pg_dsn, monkeypatch):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    monkeypatch.setenv("HERMES_PROJECTS_POSTGRES_DSN", pg_dsn)
    with projects.connect_closing():
        pass
    with psycopg.connect(pg_dsn, autocommit=True, prepare_threshold=None) as raw:
        conn = ProjectsPostgresConnection(raw)
        with pytest.raises(ValueError, match="abort"):
            with raw.transaction():
                with conn.projects_dialect.write_txn(conn, allow_nested=True):
                    conn.execute(
                        "INSERT INTO project_meta (key, value) VALUES (?, ?)",
                        ("owned", "outer"),
                    )
                with pytest.raises(sqlite3.IntegrityError):
                    with conn.projects_dialect.write_txn(conn, allow_nested=True):
                        conn.execute(
                            "INSERT INTO project_meta (key, value) VALUES (?, ?)",
                            ("owned", "duplicate"),
                        )
                assert (
                    conn.execute(
                        "SELECT value FROM project_meta WHERE key = ?", ("owned",)
                    ).fetchone()[0]
                    == "outer"
                )
                raise ValueError("abort")
        assert raw.execute("SELECT count(*) FROM project_meta").fetchone()[0] == 0


def test_pg_profile_namespaces_do_not_share_data(pg_dsn):
    with psycopg.connect(pg_dsn, autocommit=True) as raw:
        raw.execute("CREATE SCHEMA profile_one")
        raw.execute("CREATE SCHEMA profile_two")
    namespaces = ["profile_one", "profile_two"]
    for namespace in namespaces:
        with psycopg.connect(pg_dsn, autocommit=True, prepare_threshold=None) as raw:
            raw.execute("SELECT set_config('search_path', %s, false)", (namespace,))
            conn = ProjectsPostgresConnection(raw)
            conn.projects_dialect.initialize(
                conn, projects.SCHEMA_SQL, projects._migrate_add_optional_columns
            )
            assert projects.list_projects(conn) == []
            projects.create_project(conn, name=namespace, slug="same")
    for namespace in namespaces:
        with psycopg.connect(pg_dsn, autocommit=True, prepare_threshold=None) as raw:
            raw.execute("SELECT set_config('search_path', %s, false)", (namespace,))
            conn = ProjectsPostgresConnection(raw)
            assert [project.name for project in projects.list_projects(conn)] == [
                namespace
            ]
    with psycopg.connect(pg_dsn, autocommit=True) as raw:
        raw.execute("SET search_path TO public")
    assert not projects.projects_db_path().exists()


def test_pg_rejects_unmapped_tables_and_pragma(pg_dsn, monkeypatch):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    monkeypatch.setenv("HERMES_PROJECTS_POSTGRES_DSN", pg_dsn)
    with projects.connect_closing() as conn:
        with pytest.raises(ValueError, match="projects persistence"):
            conn.execute("INSERT INTO tasks (id) VALUES (?)", ("foreign",))
        with pytest.raises(psycopg.errors.SyntaxError):
            conn.execute("PRAGMA foreign_keys=OFF")
        assert (
            conn.execute("SELECT ? AS value", ("? % 한글",)).fetchone()["value"]
            == "? % 한글"
        )


def test_pg_profile_dsn_switch_uses_separate_databases(
    pg_dsn, monkeypatch, tmp_path_factory
):
    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    with postgres_server(tmp_path_factory.mktemp("second-profile-pg")) as second_dsn:
        for dsn, name in [(pg_dsn, "one"), (second_dsn, "two")]:
            monkeypatch.setenv("HERMES_PROJECTS_POSTGRES_DSN", dsn)
            with projects.connect_closing() as conn:
                assert projects.list_projects(conn) == []
                projects.create_project(conn, name=name, slug="same")
        for dsn, name in [(pg_dsn, "one"), (second_dsn, "two")]:
            monkeypatch.setenv("HERMES_PROJECTS_POSTGRES_DSN", dsn)
            with projects.connect_closing() as conn:
                assert projects.get_project(conn, "same").name == name


def test_profile_override_does_not_reuse_launch_profile_dsn(backend, tmp_path):
    with projects.connect_closing() as conn:
        projects.create_project(conn, name="launch profile")
    token = set_hermes_home_override(tmp_path / "other-profile")
    try:
        if backend == "postgres":
            with pytest.raises(ValueError, match="another profile override"):
                projects.connect()
            assert not projects.projects_db_path().exists()
        else:
            with projects.connect_closing() as conn:
                assert projects.list_projects(conn) == []
    finally:
        reset_hermes_home_override(token)
    with projects.connect_closing() as conn:
        assert [project.name for project in projects.list_projects(conn)] == [
            "launch profile"
        ]


def test_cli_and_tui_response_contract(backend, tmp_path, capsys):
    from hermes_cli import projects_cmd

    parser = argparse.ArgumentParser()
    projects_cmd.build_parser(parser.add_subparsers(dest="command"))
    arguments = parser.parse_args([
        "project",
        "create",
        "CLI Project",
        str(tmp_path),
        "--use",
    ])
    assert projects_cmd.projects_command(arguments) == 0
    assert "cli-project" in capsys.readouterr().out
    from tui_gateway import server

    with projects.connect_closing() as conn:
        project = projects.get_project(conn, "cli-project").to_dict()
    assert server._methods["projects.list"](1, {}) == {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {"projects": [project], "active_id": project["id"]},
    }
    assert server._methods["projects.get"](2, {"id": project["id"]})["result"] == {
        "project": project
    }
    missing = server._methods["projects.get"](3, {"id": "missing"})
    assert missing["error"] == {"code": 5062, "message": "no such project"}


def test_read_only_failure_does_not_fall_back(connection, backend):
    if backend == "sqlite":
        connection.execute("PRAGMA query_only=ON")
    else:
        connection.execute("SET default_transaction_read_only=on")
    try:
        with pytest.raises((
            sqlite3.OperationalError,
            psycopg.errors.ReadOnlySqlTransaction,
        )):
            projects.create_project(connection, name="must not persist")
        assert projects.list_projects(connection) == []
    finally:
        if backend == "sqlite":
            connection.execute("PRAGMA query_only=OFF")
        else:
            connection.execute("SET default_transaction_read_only=off")
    assert projects.create_project(connection, name="reusable")


def test_sqlite_legacy_columns_upgrade_without_data_loss(monkeypatch, tmp_path):
    monkeypatch.delenv("HERMES_PROJECTS_BACKEND", raising=False)
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE projects (id TEXT PRIMARY KEY, slug TEXT UNIQUE NOT NULL, "
            "name TEXT NOT NULL, description TEXT, created_at INTEGER NOT NULL, "
            "archived INTEGER NOT NULL DEFAULT 0)"
        )
        raw.execute(
            "INSERT INTO projects (id, slug, name, created_at) VALUES ('old', 'old', 'old', 1)"
        )
    with projects.connect_closing(path) as conn:
        assert projects.get_project(conn, "old").icon is None
        assert projects.update_project(conn, "old", icon="kept", board_slug="legacy")
    with projects.connect_closing(path) as conn:
        assert projects.get_project(conn, "old").icon == "kept"


def test_tui_pg_connection_error_is_redacted(monkeypatch, tmp_path):
    from tui_gateway import server

    monkeypatch.setenv("HERMES_PROJECTS_BACKEND", "postgres")
    monkeypatch.setenv(
        "HERMES_PROJECTS_POSTGRES_DSN",
        make_conninfo(host=str(tmp_path), dbname="missing"),
    )
    response = server._methods["projects.list"](1, {})
    assert response["error"] == {
        "code": 5061,
        "message": "PostgreSQL projects connection or initialization failed",
    }
    assert str(tmp_path) not in json.dumps(response)
    assert not projects.projects_db_path().exists()
