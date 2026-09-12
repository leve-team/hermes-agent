"""Closed projects dialect using the existing kanban PostgreSQL adapter."""

import sqlite3

from hermes_cli.kanban_postgres import (
    KanbanPostgresConnection,
    PostgresDialect,
    open_postgres,
)
from hermes_constants import get_hermes_home, get_process_hermes_home, hermes_home_key


class ProjectsPostgresDialect(PostgresDialect):
    table_keys = {
        "projects": ("id",),
        "project_folders": ("project_id", "path"),
        "project_meta": ("key",),
        "discovered_repos": ("root",),
    }
    identity_tables = frozenset()
    store_name = "projects"


class ProjectsPostgresConnection(KanbanPostgresConnection):
    projects_dialect = kanban_dialect = ProjectsPostgresDialect()


def open_projects_postgres(schema_sql, migrate):
    if hermes_home_key(get_hermes_home()) != hermes_home_key(get_process_hermes_home()):
        raise ValueError(
            "PostgreSQL projects DSN cannot serve another profile override"
        )
    try:
        return open_postgres(
            schema_sql,
            migrate,
            env_var="HERMES_PROJECTS_POSTGRES_DSN",
            connection_type=ProjectsPostgresConnection,
        )
    except ValueError:
        raise
    except Exception as exc:
        raise sqlite3.OperationalError(
            "PostgreSQL projects connection or initialization failed"
        ) from exc
