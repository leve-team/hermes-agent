"""Core auxiliary stores that follow the profile's session backend (levos 0059).

Besides ``state.db`` the core keeps a few side stores in their own SQLite
files: the coding verification ledger (``verification_evidence.db``), the
Responses API chain store (``response_store.db``) and the Discord recovery
ledger (``gateway/discord_message_recovery.db``). While any of those files is
open a profile cannot run on two overlapping pods, so on a PostgreSQL-authority
profile they move next to the core tables in that profile's PostgreSQL store.

``open_aux_store`` is the one selector, and it is the same one ``SessionDB``
and ``hermes_state_writer.open_writer`` use (``HERMES_STATE_BACKEND`` env, then
the active ``config.yaml``):

* authority: a ``sqlite3.Connection``-shaped handle over a fresh PostgreSQL
  ``SessionDB`` of the active profile. The owner's SQLite SQL keeps working:
  the closed table set is renamed into a ``core_<area>_*`` namespace (a
  profile schema must not grow a bare ``meta``), ``INSERT OR REPLACE`` becomes
  a keyed upsert, identity inserts return ``lastrowid`` and ``CREATE TABLE``
  gets PostgreSQL column types. A connection or schema failure raises
  :class:`AuxStoreUnavailable`; nothing falls back to a file or ``:memory:``.
* anything else: the SQLite connection the owner opened before, unchanged.

``projects.db`` is not renamed here: it keeps the 0054 table contract and its
own adapter (``hermes_cli.projects_postgres``), selected by the same predicate
(:func:`aux_store_authority`).
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from hermes_state_writer import _PostgresWriterConnection, postgres_ddl


class AuxStoreUnavailable(RuntimeError):
    """PostgreSQL authority is selected but the auxiliary store cannot serve.

    Raised instead of opening SQLite; the message never carries the DSN.
    """


@dataclass(frozen=True)
class AuxTable:
    sqlite_name: str
    postgres_name: str
    key: Tuple[str, ...]
    identity: Optional[str] = None
    # (column, table): the column holds an ``identity`` id of that table in the
    # same store, so a one-shot migration rewrites it to the re-assigned id.
    reference: Optional[Tuple[str, str]] = None


# Closed set: a store name not listed here is a programming error, and a table
# not listed for its store never reaches the profile schema under a bare name.
AUX_STORES: Mapping[str, Tuple[AuxTable, ...]] = {
    "verification_evidence": (
        AuxTable("meta", "core_verification_meta", ("key",)),
        AuxTable("verification_events", "core_verification_events", ("id",), "id"),
        AuxTable(
            "verification_state",
            "core_verification_state",
            ("session_id", "root"),
            reference=("last_event_id", "verification_events"),
        ),
    ),
    "response_store": (
        AuxTable("responses", "core_response_responses", ("response_id",)),
        AuxTable("conversations", "core_response_conversations", ("name",)),
    ),
    "discord_recovery": (
        AuxTable("discord_messages", "core_discord_messages", ("message_id",)),
        AuxTable("discord_recovery_scans", "core_discord_recovery_scans", ("scan_id",)),
        AuxTable(
            "discord_recovery_cursors", "core_discord_recovery_cursors", ("channel_id",)
        ),
    ),
}
_AUX_INDEXES: Mapping[str, Mapping[str, str]] = {
    "verification_evidence": {
        "idx_verification_events_session_root": "idx_core_verification_events_session_root",
    },
}

_QUOTED = re.compile(r"('(?:''|[^'])*'|\"(?:\"\"|[^\"])*\")")
_INSERT_OR_REPLACE = re.compile(
    r"\A\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]*)\)", re.IGNORECASE
)
_INSERT_INTO = re.compile(r"\A\s*INSERT\s+INTO\s+(\w+)", re.IGNORECASE)
_DDL = re.compile(r"\A\s*(?:CREATE|ALTER)\s+TABLE\b", re.IGNORECASE)
_PRAGMA = re.compile(r"\A\s*PRAGMA\b", re.IGNORECASE)
_RETURNING = re.compile(r"\bRETURNING\b", re.IGNORECASE)
_INTEGER = re.compile(r"\bINTEGER\b", re.IGNORECASE)


def aux_store_tables(name: str) -> Tuple[AuxTable, ...]:
    try:
        return AUX_STORES[name]
    except KeyError:
        raise ValueError(f"unknown auxiliary store {name!r}") from None


def aux_store_authority() -> bool:
    """True when the active profile's session store is PostgreSQL authority.

    ``probe`` keeps SQLite authority and therefore answers False.
    """
    try:
        import hermes_state_postgres as seam
    except ImportError:
        return False  # base install without the PostgreSQL module
    return seam.resolve_state_backend() == "authority"


def open_aux_store(
    name: str,
    *,
    sqlite_path: Any,
    initialize: Callable[[Any], None],
    sqlite_options: Optional[Dict[str, Any]] = None,
):
    """Open auxiliary store *name* on the backend the active profile selects.

    *initialize* runs on every open, before the handle is returned, and must be
    idempotent (``CREATE TABLE IF NOT EXISTS``). It can branch on the handle's
    ``is_postgres`` attribute to skip SQLite-only pragmas. On SQLite the file's
    parent directory is created and ``sqlite3.connect(sqlite_path,
    **sqlite_options)`` is returned exactly as the owner used to open it.
    """
    aux_store_tables(name)
    if aux_store_authority():
        return _open_postgres(name, initialize)
    path = str(sqlite_path)
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path, **(sqlite_options or {}))
    try:
        initialize(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def _open_postgres(name: str, initialize: Callable[[Any], None]):
    try:
        from hermes_state import SessionDB

        db = SessionDB(read_only=False)
    except Exception as exc:
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: the PostgreSQL authority store could not be opened"
        ) from exc
    if not getattr(db, "_is_postgres", False):
        db.close()
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: PostgreSQL authority is selected but the "
            "session store opened on SQLite; refusing to write the wrong store"
        )
    conn = AuxPostgresConnection(db, name)
    try:
        initialize(conn)
    except Exception as exc:
        conn.close()
        raise AuxStoreUnavailable(
            f"auxiliary store {name!r}: PostgreSQL schema initialization failed"
        ) from exc
    except BaseException:
        conn.close()
        raise
    return conn


class AuxPostgresConnection(_PostgresWriterConnection):
    """The writer handle plus the owner's closed SQLite dialect for one store."""

    def __init__(self, db, store: str):
        super().__init__(db)
        self.store = store
        tables = aux_store_tables(store)
        self._tables = {table.postgres_name: table for table in tables}
        renames = {table.sqlite_name: table.postgres_name for table in tables}
        renames.update(_AUX_INDEXES.get(store, {}))
        self._renames = renames
        self._names = re.compile(
            r"\b("
            + "|".join(sorted(map(re.escape, renames), key=len, reverse=True))
            + r")\b"
        )
        self._columns: Dict[str, List[str]] = {}

    def _rename(self, sql: str) -> str:
        parts = _QUOTED.split(sql)
        for index in range(0, len(parts), 2):
            parts[index] = self._names.sub(
                lambda m: self._renames[m.group(1)], parts[index]
            )
        return "".join(parts)

    def _table(self, name: str) -> AuxTable:
        try:
            return self._tables[name.lower()]
        except KeyError:
            raise ValueError(
                f"table {name!r} is not part of auxiliary store {self.store!r}"
            ) from None

    def table_columns(self, table: str) -> List[str]:
        columns = self._columns.get(table)
        if columns is None:
            rows = self._conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = ? "
                "ORDER BY ordinal_position",
                (table,),
            ).fetchall()
            columns = [row[0] for row in rows]
            if not columns:
                raise ValueError(f"auxiliary table {table!r} does not exist")
            self._columns[table] = columns
        return columns

    def _translate(self, sql: str) -> Tuple[str, Optional[str]]:
        sql = self._rename(sql)
        if _PRAGMA.match(sql):
            raise ValueError(
                "SQLite PRAGMA has no meaning on a PostgreSQL auxiliary store"
            )
        if _DDL.match(sql):
            return _INTEGER.sub("BIGINT", postgres_ddl(sql)), None
        replace = _INSERT_OR_REPLACE.match(sql)
        if replace:
            table = self._table(replace.group(1))
            listed = [column.strip() for column in replace.group(2).split(",")]
            # SQLite REPLACE deletes the old row, so columns the statement does
            # not list go back to their defaults; the upsert says so explicitly.
            updates = [f"{c} = EXCLUDED.{c}" for c in listed if c not in table.key]
            updates += [
                f"{c} = DEFAULT"
                for c in self.table_columns(table.postgres_name)
                if c not in listed and c not in table.key
            ]
            sql = (
                re
                .sub(
                    r"\A\s*INSERT\s+OR\s+REPLACE\s+INTO",
                    "INSERT INTO",
                    sql,
                    count=1,
                    flags=re.IGNORECASE,
                )
                .rstrip()
                .rstrip(";")
            )
            action = f"UPDATE SET {', '.join(updates)}" if updates else "NOTHING"
            sql += f" ON CONFLICT ({', '.join(table.key)}) DO {action}"
        insert = _INSERT_INTO.match(sql)
        if insert and not _RETURNING.search(sql):
            identity = self._table(insert.group(1)).identity
            if identity:
                return f"{sql.rstrip().rstrip(';')} RETURNING {identity}", identity
        return sql, None

    def execute(self, sql: str, params: Any = ()):
        translated, returning = self._translate(sql)
        cursor = self._conn.execute(translated, params)
        if returning:
            row = cursor.fetchone()
            cursor.lastrowid = row[0] if row else None
        return cursor

    def executemany(self, sql: str, rows):
        translated, _ = self._translate(sql)
        return self._conn.executemany(translated, rows)

    def executescript(self, sql_script: str):
        from hermes_state_postgres import _split_sql_statements

        cursor = None
        for statement in _split_sql_statements(sql_script):
            cursor = self.execute(statement)
        return cursor

    def cursor(self):
        raise NotImplementedError("auxiliary stores use connection.execute()")


# ---------------------------------------------------------------------------
# One-shot SQLite -> PostgreSQL move
# ---------------------------------------------------------------------------

_PROJECT_TABLES: Tuple[AuxTable, ...] = (
    # projects keeps the 0054 contract: same table names on both backends.
    AuxTable("projects", "projects", ("id",)),
    AuxTable("project_folders", "project_folders", ("project_id", "path")),
    AuxTable("project_meta", "project_meta", ("key",)),
    AuxTable("discovered_repos", "discovered_repos", ("root",)),
)


class AuxMigrationError(RuntimeError):
    """A source row is missing from PostgreSQL after the copy; rolled back."""


class _DryRun(Exception):
    pass


def _source_files() -> Tuple[Tuple[str, Path], ...]:
    from hermes_constants import aux_db_path, get_hermes_home

    home = get_hermes_home()
    return (
        ("verification_evidence", aux_db_path("verification_evidence.db")),
        ("response_store", aux_db_path("response_store.db")),
        ("discord_recovery", home / "gateway" / "discord_message_recovery.db"),
        ("projects", home / "projects.db"),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def migrate_aux_sqlite_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's four auxiliary SQLite files into PostgreSQL.

    Run inside the profile's own process environment (its pod), after the
    image carrying 0059 is serving: from then on the files are no longer
    written, and a row the live PostgreSQL store already holds wins.

    * Every file is opened ``mode=ro`` on one snapshot and verified byte-equal
      (sha256) afterwards; a missing file is reported and skipped.
    * Each store copies in one PostgreSQL transaction. Rows are inserted with
      ``ON CONFLICT DO NOTHING``; ``verification_events`` rows are matched on
      their content and get fresh ids (``verification_state`` follows them),
      so running twice yields the same row counts.
    * After the copy every source row must be present in PostgreSQL (by key;
      by content for events), else the store rolls back and
      :class:`AuxMigrationError` is raised.
    * ``dry_run`` performs the same copy and checks, then rolls back.
    """
    from hermes_state_postgres import _is_active_profile

    if not _is_active_profile(profile):
        raise ValueError(
            f"migrate_aux_sqlite_to_pg runs in the profile's own environment "
            f"(HERMES_PROFILE / HERMES_HOME); {profile!r} is not the active profile"
        )
    if not aux_store_authority():
        raise RuntimeError(
            f"profile {profile!r} is not on PostgreSQL authority; nothing to migrate to"
        )
    report: Dict[str, Any] = {"profile": profile, "dry_run": dry_run, "stores": {}}
    for store, path in _source_files():
        if not path.is_file():
            report["stores"][store] = {"status": "missing", "path": str(path)}
            continue
        before = _sha256(path)
        result = _migrate_store(store, path, dry_run=dry_run)
        after = _sha256(path)
        if after != before:  # pragma: no cover - mode=ro makes this unreachable
            raise AuxMigrationError(f"{store}: source file changed during migration")
        result.update(path=str(path), sha256=before)
        report["stores"][store] = result
    return report


def _migrate_store(store: str, path: Path, *, dry_run: bool) -> Dict[str, Any]:
    from state_transfer import open_sqlite_snapshot

    source = open_sqlite_snapshot(path)
    try:
        if store == "projects":
            return _migrate_projects(source, dry_run=dry_run)
        target = _open_postgres(store, _store_initializer(store))
        try:
            tables: Dict[str, Any] = {}
            try:
                with target:
                    ids: Dict[str, Dict[Any, Any]] = {}
                    for table in AUX_STORES[store]:
                        tables[table.sqlite_name] = _copy_table(
                            source,
                            target,
                            table,
                            ids,
                            target_columns=target.table_columns,
                        )
                    if dry_run:
                        raise _DryRun
            except _DryRun:
                pass
            return {"status": "dry_run" if dry_run else "migrated", "tables": tables}
        finally:
            target.close()
    finally:
        source.close()


def _migrate_projects(source: sqlite3.Connection, *, dry_run: bool) -> Dict[str, Any]:
    from hermes_cli import projects_db
    from hermes_cli.projects_persistence import write_txn

    target = projects_db.connect()
    try:
        dialect = getattr(target, "projects_dialect", None)
        if dialect is None:
            raise AuxMigrationError("projects: authority profile opened SQLite")

        def columns(table: str) -> List[str]:
            return [row["name"] for row in dialect.table_info(target, table)]

        tables: Dict[str, Any] = {}
        try:
            with write_txn(target):
                for table in _PROJECT_TABLES:
                    tables[table.sqlite_name] = _copy_table(
                        source, target, table, {}, target_columns=columns
                    )
                if dry_run:
                    raise _DryRun
        except _DryRun:
            pass
        return {"status": "dry_run" if dry_run else "migrated", "tables": tables}
    finally:
        target.close()


def _store_initializer(store: str) -> Callable[[Any], None]:
    if store == "verification_evidence":
        from agent.verification_evidence import _initialize_connection

        return _initialize_connection
    if store == "response_store":
        from gateway.platforms.api_server import _initialize_response_store

        return _initialize_response_store
    from plugins.platforms.discord.recovery import DiscordRecoveryStore

    return DiscordRecoveryStore()._initialize


def _copy_table(
    source: sqlite3.Connection,
    target: Any,
    table: AuxTable,
    ids: Dict[str, Dict[Any, Any]],
    *,
    target_columns: Callable[[str], List[str]],
) -> Dict[str, Any]:
    """Copy one table; *target* SQL uses the SQLite names (the handle renames)."""
    from state_transfer import quote_identifier

    name = table.sqlite_name
    present = source.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone()
    if present is None:
        return {"status": "absent"}
    source_columns = [
        row[1] for row in source.execute(f"PRAGMA table_info({quote_identifier(name)})")
    ]
    available = set(target_columns(table.postgres_name))
    columns = [c for c in source_columns if c in available]
    dropped = [c for c in source_columns if c not in available]
    if not set(table.key) <= set(columns):
        raise AuxMigrationError(f"{name}: key columns {table.key} missing on a side")
    quoted = ", ".join(quote_identifier(c) for c in columns)
    order = ", ".join(quote_identifier(c) for c in table.key)
    rows = [
        tuple(row)
        for row in source.execute(
            f"SELECT {quoted} FROM {quote_identifier(name)} ORDER BY {order}"
        )
    ]
    count = f"SELECT COUNT(*) FROM {name}"
    before = target.execute(count).fetchone()[0]
    identity = table.identity
    if identity:
        inserted, missing = _copy_identity_rows(
            target, name, columns, rows, identity, ids
        )
    else:
        if table.reference:
            column, referenced = table.reference
            index = columns.index(column)
            mapping = ids.get(referenced, {})
            rows = [
                row[:index] + (mapping.get(row[index]),) + row[index + 1 :]
                for row in rows
            ]
        insert = (
            f"INSERT INTO {name} ({quoted}) VALUES ({', '.join('?' for _ in columns)}) "
            "ON CONFLICT DO NOTHING"
        )
        for row in rows:
            target.execute(insert, row)
        positions = [columns.index(c) for c in table.key]
        found = {
            tuple(r) for r in target.execute(f"SELECT {order} FROM {name}").fetchall()
        }
        missing = sum(
            1 for row in rows if tuple(row[i] for i in positions) not in found
        )
        inserted = None
    after = target.execute(count).fetchone()[0]
    if missing:
        raise AuxMigrationError(
            f"{name}: {missing} of {len(rows)} source rows are not in PostgreSQL after "
            "the copy (a conflicting unique value?); rolled back"
        )
    return {
        "source_rows": len(rows),
        "target_rows_before": before,
        "target_rows_after": after,
        "inserted": after - before if inserted is None else inserted,
        "dropped_columns": dropped,
    }


def _copy_identity_rows(target, name, columns, rows, identity, ids):
    """Events get fresh ids: the live store may already have used the old ones."""
    from state_transfer import quote_identifier

    content = [c for c in columns if c != identity]
    select = ", ".join(quote_identifier(c) for c in [identity, *content])
    existing = {
        tuple(row[1:]): row[0]
        for row in target.execute(
            f"SELECT {select} FROM {name} ORDER BY {identity}"
        ).fetchall()
    }
    position = columns.index(identity)
    insert = (
        f"INSERT INTO {name} ({', '.join(quote_identifier(c) for c in content)}) "
        f"VALUES ({', '.join('?' for _ in content)})"
    )
    mapping = ids.setdefault(name, {})
    inserted = 0
    for row in rows:
        values = row[:position] + row[position + 1 :]
        new_id = existing.get(values)
        if new_id is None:
            new_id = target.execute(insert, values).lastrowid
            if new_id is None:
                raise AuxMigrationError(f"{name}: insert returned no id")
            existing[values] = new_id
            inserted += 1
        mapping[row[position]] = new_id
    found = {
        tuple(row[1:])
        for row in target.execute(f"SELECT {select} FROM {name}").fetchall()
    }
    missing = sum(
        1 for row in rows if row[:position] + row[position + 1 :] not in found
    )
    return inserted, missing


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="One-shot move of the auxiliary SQLite stores into the "
        "profile's PostgreSQL authority store (levos 0059)."
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    report = migrate_aux_sqlite_to_pg(args.profile, dry_run=args.dry_run)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - operator entrypoint
    raise SystemExit(main())
