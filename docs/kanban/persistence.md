# Kanban persistence seam

SQLite remains the default. This is a core persistence contract, not a board
migration or a deployment switch. Domain functions still take the existing
connection and task IDs; no guild argument or host-application import is added.

## Connection ownership

`kanban_db.connect(backend="postgres", postgres_dsn=dsn)` selects PostgreSQL.
Without explicit arguments, selection uses `HERMES_KANBAN_BACKEND` (`sqlite` or
`postgres`, default `sqlite`) and `HERMES_KANBAN_POSTGRES_DSN`. A DSN alone does
not select PostgreSQL. Invalid selection, missing/empty PG DSN, or an explicit
PG DSN with SQLite selected fails before filesystem creation. Driver/connection
errors do not fall back to SQLite. No configuration file or environment is
written. A bootstrap can resolve its own configuration and pass these keywords.

Without board registration, the PG DSN still identifies **one board namespace**:
explicit SQLite paths/boards and `HERMES_KANBAN_DB`/`HERMES_KANBAN_BOARD` are
rejected, and `init_db()` requires registration. The legacy direct-DSN
`connect()` path remains available. `connect_closing()` supports environment
selection.

## Board-aware entry points (0055)

A trusted outer bootstrap registers routing before CLI dispatch or gateway
threads start, in **each process** (including spawned workers):

```python
from psycopg.conninfo import make_conninfo
from hermes_cli.kanban_persistence import set_board_dsn_resolver

def board_dsn(token):
    schema = "svc_kanban_" + token
    if len(schema) > 63:
        raise ValueError("Board schema identifier is too long")
    return make_conninfo(service_dsn, options=f"-c search_path={schema}")

set_board_dsn_resolver(board_dsn, boards=("default", "other-guild"))
```

`service_dsn` comes from the outer application, not a core default. Select
`HERMES_KANBAN_BACKEND=postgres` in that process's existing bootstrap configuration.
Registration alone does not change the backend. This is not an environment-file
edit or a deployment instruction. No host application is imported by core.

Registration requires a complete, nonempty, active-board inventory. Core strips
and lowercases slugs, validates `[a-z0-9][a-z0-9_-]{0,62}`, then passes the
`[a-z0-9_]+` token (`-` becomes `_`) to the resolver. Rejecting duplicate tokens
prevents `other-guild`/`other_guild` and case aliases from silently colliding.
Unknown/invalid boards fail before the callback; failed registration leaves the
previous registration intact. `set_board_dsn_resolver(None)` clears routing.
Do not mutate process-global routing while workers are active. Archive/CRUD,
inventory completeness, distinct schema mappings and privileges belong to the
bootstrap, not filesystem metadata or schema-catalog guessing.

Each resolver DSN must carry exactly `options=-c search_path=<schema>` (URL-encode
when constructing a URI). The complete schema identifier must match
`[a-z0-9_]{1,63}`. Multiple schemas, extra options and missing schemas are rejected.
The connection explicitly binds `set_config('search_path', schema, false)` and
verifies `current_schema()` **before DDL**. It never creates schemas or falls
back to `public`. This also covers socket proxies that ignore startup options.
The resolver is trusted configuration, not an authorization mechanism.

`connect(board=...)` creates and owns one connection in the selected schema.
Without `board`, selection is scoped CLI override → `HERMES_KANBAN_BOARD` →
the current-board pointer → `default`; invalid, empty or unregistered selectors
fail rather than routing to another board. Existence and `list_boards()` use the
injected inventory (active boards only, independent of `include_archived`).
Explicit SQLite paths, `HERMES_KANBAN_DB`, or explicit `postgres_dsn` conflict
with registered routing. The resolver is authoritative over the legacy DSN env;
empty returns never fall back to it. `init_db()` closes its PG connection and
returns `None`; its unchanged SQLite branch returns the file `Path`.

`kanban list` and `kanban --board other-guild list` run through the real CLI
after bootstrap registration; simply setting a DSN in an unbootstrapped CLI is
not enough. The gateway dispatcher enumerates the same inventory and connects
separately for each slug. It does not use SQLite file fingerprints for PG.
PG `count_running_tasks_other_boards()` excludes the selected slug and sums
the other registered schemas without file-existence checks. Enumeration,
resolution, connection and query failures propagate; board failures carry the
slug, not DSN/password/driver details. No partial count or disguised zero is
returned, so the dispatch tick cannot use it to overcommit the host cap.
SQLite's historical counting/error policy and all its connection code remain
unchanged. PG census callers must select the PG backend in their bootstrap.

Worker environment handoff, filesystem board CRUD/repair, attachment/workspace
metadata, local dispatch locks and K/L/T owner-UoW wiring are not migrated by
this entry-point patch.
The two-board watcher test disables decomposition and uses unassigned tasks:
it proves real board connects/ticks, not a full worker spawn or notification.

### Notification subscription probe (0056)

`count_notify_subs()` now resolves `HERMES_KANBAN_BACKEND` before inspecting
SQLite files. PostgreSQL opens through the existing `connect(db_path, board=...)`
contract, so registered board selection, schema validation and selector conflicts
are identical. The owned connection is always closed; profile ownership and
platform/chat/thread filters share the same parameterized COUNT query. An empty
profile allowlist uses a portable false predicate, not SQLite's integer boolean.
No signature or default-backend change is required.

Only a successful count of zero permits the notifier to skip collection.
As with the 0055 running-task census, PG resolution, connection, missing-schema
and query failures are errors, never zero. The notifier treats errors as unknown
and attempts its normal backend open/collection; an unsuccessful fallback remains
a logged failure for a later tick, not a claim that the board has no subscribers.

SQLite keeps its read-only, no-initialization probe: a missing file or legacy
missing subscription table still returns integer zero. PG deliberately uses the
existing initialized connection rather than introducing a second resolver or
read-only connection API: it can initialize tables within a precreated schema,
but cannot create a missing schema or fall back to public/SQLite. The PG probe is
therefore not the SQLite zero-write optimization; zero skips the subsequent
collector open only. A concurrent subscription arriving after a real zero count
is observed on a later tick, as before.

Real PGlite tests store one subscription and assert probe=1, cover empty boards,
missing schema/socket, filters and selector errors, and run the notifier through
delivery/cursor advancement with no SQLite DB files. A transient probe connection
failure must still deliver via the subsequent real open. Only the external
messaging adapter is replaced; this does not validate deployed credentials,
worker spawn, wake delivery or multi-host dispatch ownership.

The optional driver is imported only when PostgreSQL is selected. The PG
connection implements execute/executemany/executescript, cursor fetching,
integer/name row access, rowcount/lastrowid, commit/rollback/close, in_transaction
and the non-closing connection context manager. Like the existing kanban SQLite
handle, it is autocommit outside an explicit transaction. A driver connection
can also be wrapped by `KanbanPostgresConnection` at an outer bootstrap: this
constructor does not initialize schema or acquire a separate connection.

## Transactions and dialect

`write_txn` checks the existing delegated-child mutation guard before dispatch.
SQLite retains BEGIN IMMEDIATE, its busy-boundary retry policy, explicit
savepoints and post-commit file guard. PG uses psycopg transaction/savepoint
contexts and a database/current-schema scoped transaction advisory lock. All
core multi-statement writers therefore retain single-writer semantics, not
just the final claim CAS. Other writers sharing that namespace must cooperate
with this transaction contract. There is no statement replay, reconnect or
automatic write retry; commit failure/unknown outcome propagates.

Nested domain operations still require explicit `allow_nested=True`; successful
inner savepoints are not durable until the owner commits. Functions whose
post-commit effects forbid nesting retain that prohibition. Schema initialization
uses its own transaction and lock, with the existing running-task backfill in
an explicit inner savepoint. Initialization inside a borrowed UoW is rejected.
PG lock/statement timeouts are 5/30 seconds; connect timeout is 5 seconds.
Prepared statements are disabled for transaction-pool/socket compatibility.

The canonical seven-table `SCHEMA_SQL` and existing additive migrations remain
the only DDL source. INTEGER becomes BIGINT; the four AUTOINCREMENT primary
keys become BIGINT GENERATED BY DEFAULT AS IDENTITY. TEXT/JSON-as-TEXT, defaults
and keys remain unchanged. No FK, guild column, JSONB column or schema is invented.
Legacy SQLite destructive rebuilds remain SQLite-only. PG shape/identity drift
requires an explicit migration instead of silently dropping data.

State-backend reuse is limited to its binding lexer, script splitter, row and
cursor contract. Kanban does not reuse session schema/FTS rewrites or reconnect
behavior. The lexer's optional null-safe mode translates unquoted `IS ?` and
`IS NOT ?` without changing the state backend's default. Kanban's closed insert
set supports exact PK ON CONFLICT targets, generated lastrowid, and full-row
REPLACE reset-to-default semantics. It does not emulate SQLite REPLACE triggers
or OR IGNORE suppression of NOT NULL/CHECK failures. Unknown dialects are not
silently ignored. Catalog access, initialization, file-maintenance guards and
NULL sort placement have explicit dispatch. Core JSON is handled in Python and
timestamps are bound integer epochs: this tip has no SQL json_extract,
datetime('now') or FTS call to translate.

## Verification and handoff

### Dialect audit (0057)

Dispatcher ready/review selection uses the same closed `order_by()` dialect as
task listing. PostgreSQL adds SQLite-compatible NULL placement without changing
stored priorities. Text task/graph keys and task title/status/assignee sorting
use PostgreSQL `COLLATE "C"`, matching the canonical SQLite BINARY collation;
numeric run/event IDs never receive a text collation. Notification platform
matching does not depend on the PostgreSQL locale:
`notify_platform_equals()` preserves SQLite's ASCII-only LOWER behavior, keeping
non-ASCII platform strings distinct rather than broadening subscription counts.
Nullable parent completion
and role-history timestamps also use the dialect. A missing role-history end
time renders as `unknown time`, not a fabricated epoch. Ordering ties without a
declared tie-breaker remain unspecified. Nonpositive task-list limits retain
SQLite's unbounded-list behavior without sending a negative LIMIT to PostgreSQL.

Inventory and ordinary storage failures in notifier, gateway health,
auto-decomposition and CLI list/daemon paths are not empty/idle results. They
propagate to the existing error/reporting boundary. A default-assignment write
failure rolls back and fails the tick instead of reporting an unassigned card.
Reassignment maps only the specific live-claim refusal to False, not transaction
composition errors. Known SQLite corruption still reaches the existing board
quarantine; health is explicitly unknown (`None`) and does not reset the stuck
counter. Neither general PostgreSQL failures nor other SQLite query errors use
that corruption exception.

`scripts/kanban_dialect_audit.py --revision <commit>` emits every execute call,
exception handler, suppress block, lexical hit and source hash in the three
audited files. With no revision it audits the working tree. It is an inventory,
not a generic SQL translator or a claim that arbitrary dynamic SQL is safe.
The 0057 handoff classifies each baseline site, including standard SQL and
SQLite-only administration/repair, legacy JSON tolerance, best-effort observer
and filesystem cleanup, and already-guarded SQLite counting/probes. PostgreSQL
schema drift continues to require an explicit migration; this patch does not
implement management CLI migration, distributed locks, or deployment bootstrap.

Run `scripts/run_tests.sh tests/hermes_cli/test_kanban_dialect_audit.py` for the
real SQLite/PostgreSQL parity, failure and inventory regressions. The dispatch
test actually claims and calls an injected process-spawn boundary, not dry-run.

Install the pinned dev, postgres and kanban-postgres-test extras. The new test
file requires Node/npm and py-pglite 0.5.3 (absence is an error, not a skip); its
throwaway socket fixture pins PGlite 0.3.16 and pglite-socket 0.0.22. Run:

```sh
HERMES_TEST_WORKERS=3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_db.py \
  tests/hermes_cli/test_kanban_db_init.py \
  tests/hermes_cli/test_kanban_db_repair.py \
  tests/hermes_cli/test_kanban_persistence.py \
  tests/test_pg_sql_dialect_parity.py
```

The two-writer CAS test initializes two real handles before a barrier and races
their claim transactions; exactly one task/run/claimed event wins. Simultaneous
cold connection setup is not covered: PGlite's single-backend socket multiplexer
deadlocks on that setup pattern. Native multi-backend PG startup concurrency,
network-loss/commit-response ambiguity, service privileges and deployment remain
separate validation obligations. Sequence gaps after PG rollback are allowed.

The board entry-point tests in `test_kanban_persistence.py` exercise real CLI
subprocesses with bootstrap registration, two schemas with identical IDs,
the live gateway dispatcher loop, other-board counts, missing sockets, failed
queries and hostile slugs/DSN options. The pinned PGlite socket server ignores
startup options and shares session state across clients: explicit schema setup
is tested, but simultaneous per-session `search_path` isolation requires native
PostgreSQL validation. Do not infer that guarantee from the PGlite fixture.

An outer migration must map `(guild, id)`/board routing, choose the service
database/schema, initialize before the UoW, and adapt K/L/T repositories to the
same owner connection. This seam neither changes the core key contract nor
authorizes environment changes, data COPY, deployment or a storage-authority flip.
