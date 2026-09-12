# Projects persistence

`hermes_cli.projects_db` remains a per-profile store, separate from kanban and
from any host application's projects database. Its public function signatures,
dataclass dictionaries, CLI output and TUI/tool payloads are unchanged.

## Selection and ownership

- Unset `HERMES_PROJECTS_BACKEND` means `sqlite`. The existing path resolver,
  WAL/DELETE fallback, foreign keys, per-path initialization cache and SQLite
  transaction helper remain authoritative. The default path imports no PG driver.
- Only the exact values `sqlite` and `postgres` are accepted. An empty/unknown
  value fails before creating a file. A DSN alone does not enable PostgreSQL.
- `HERMES_PROJECTS_BACKEND=postgres` requires `HERMES_PROJECTS_POSTGRES_DSN`.
  Kanban/state/service environment variables are not alternative selectors or
  fallback DSNs. Connection, initialization and write failures never fall back
  to SQLite; failed statements and ambiguous commits are never replayed.
- Projects connection/initialization failures expose a stable
  `sqlite3.OperationalError` message to CLI/TUI callers instead of driver
  diagnostics containing the DSN's host or socket path. The original exception
  remains chained for internal diagnosis. Configuration errors remain ValueError;
  kanban's error contract is unchanged.
- The DSN must select a database or pre-provisioned search path dedicated to the
  **launch profile**. Profile names and filesystem paths are not interpolated
  into SQL. Do not give different profiles the same projects namespace.
- An explicit `db_path` with PG selection is rejected. A context-local profile
  override different from `get_process_hermes_home()` is also rejected before
  connecting. A TUI serving multiple profiles needs a separately reviewed trusted
  DSN router before those cross-profile requests can use PG; they must not reuse
  the launch profile's DSN or silently revert to SQLite.

These environment selectors are the explicit fork-patch contract, not a new
general config.yaml feature. No deployment environment or secret is changed here.

## Implementation

There are three narrow domain seams: `connect`, the imported `write_txn`, and
the optional-column catalog query. All CRUD SQL and `SCHEMA_SQL` stay unchanged.
`projects_persistence` delegates SQLite transactions to `sqlite_util`.
`projects_postgres` specializes the 0053 `PostgresDialect` and
`KanbanPostgresConnection`, reusing connection lifecycle, binding lexer, rows,
cursor, DDL translation and transactions. The legacy internal `kanban_dialect`
attribute names the shared adapter contract; `projects_dialect` references the
same projects-specific instance. Kanban's original closed table set and default
configuration do not change.

The projects dialect contains only `projects` (id), `project_folders`
(project_id/path), `project_meta` (key), and `discovered_repos` (root), with no
identity tables. INTEGER declarations become BIGINT; qmark parameters become
driver bindings; the canonical folder INSERT OR IGNORE uses ON CONFLICT on its
composite key. Foreign keys and ON DELETE CASCADE are real database constraints.
The existing metadata/discovery upserts and executemany need no domain rewrites.
This is not a general SQLite emulator: arbitrary PRAGMAs, REPLACE delete-trigger
semantics, ATTACH, host-owned tables and schema repair are not supported contracts.

PG initialization is atomic on each owned connection and includes additive
optional-column migration. A transaction-scoped advisory lock is keyed by the
current database/schema and `:hermes.projects`, separate from kanban's lock.
`write_txn(conn)` rejects implicit nesting. A caller already owning a raw PG
transaction may explicitly use the adapter dialect's `allow_nested=True` to own
a savepoint; success does not commit the outer transaction. Wrapping a raw
connection neither initializes schema nor commits/closes it. Bootstrap schema
before borrowing. No read-only fallback or grants policy is added.

## Verification and rollout limits

Install the fork's pinned dev, postgres and kanban-postgres-test dependencies;
the existing py-pglite 0.5.3 extra and lockfile need no changes. Run:

```sh
HERMES_TEST_WORKERS=3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh \
  tests/hermes_cli/test_projects_persistence.py \
  tests/hermes_cli/test_kanban_persistence.py \
  tests/hermes_cli/test_projects_db.py \
  tests/hermes_cli/test_projects_cli.py \
  tests/tui_gateway/test_projects_rpc.py
```

Fixtures use real SQLite files and local py-pglite servers, with pinned PGlite
0.3.16/pglite-socket 0.0.22 shared with kanban's tests. The socket server does not
apply libpq startup `options`; schema tests use SQL SET on borrowed real handles,
and DSN-isolation tests use two independent servers. Production search-path
provisioning, native PG startup options, network failure/commit-response-loss,
cross-profile routing, COPY, writer fencing and deployment are not validated or
performed by this patch. Validate those before opt-in; keep SQLite until an
approved migration verifies all four tables for each profile.
