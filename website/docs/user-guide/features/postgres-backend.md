---
sidebar_position: 18
title: "PostgreSQL State Backend"
description: "Optional PostgreSQL backend for session and state storage, in place of the default single-file SQLite database"
---

# PostgreSQL State Backend

Hermes stores sessions, messages, and agent state in a single SQLite file
(`~/.hermes/state.db`) by default. That is the right choice for a personal
install: zero setup, zero moving parts, and it comfortably handles tens of
thousands of messages.

Some deployments outgrow it. If you run Hermes across several hosts against
shared state, on a container filesystem where SQLite's locking semantics do not
hold, or under an operational policy that requires a managed database, you can
point session storage at an external PostgreSQL server instead.

This backend is **opt-in and off by default**. Installs that do not configure it
never load the driver and never pay for it.

## When to use it

Consider PostgreSQL when any of these apply:

- **Multiple hosts share one state store.** Several Hermes processes (gateway,
  dashboard, cron) running on different machines against the same sessions.
- **The filesystem is unsuitable for SQLite.** Network filesystems and some
  container volumes do not provide the locking or `mmap` guarantees SQLite needs.
- **Operational policy requires a managed database.** Backups, point-in-time
  restore, failover, and monitoring handled by existing database infrastructure.

Stay on the default SQLite backend otherwise. It is faster for a single-host
install, needs no server, and is the more thoroughly exercised path.

## Requirements

- **PostgreSQL 14 or newer.**
- **The `postgres` extra**, which installs the `psycopg` driver:

  ```bash
  pip install 'hermes-agent[postgres]'
  # or, from a source checkout:
  uv sync --extra postgres
  ```

  Hermes will also attempt to install the driver on first use if it is missing,
  so an existing install that flips the config key generally does not need a
  manual step. Environments that block outbound PyPI access should install the
  extra ahead of time. The published Docker image ships with it already baked in.

- **The `pg_trgm` extension** is recommended but not required. Hermes tries to
  create it on first connect (`CREATE EXTENSION IF NOT EXISTS pg_trgm`); it
  backs the GIN trigram indexes that accelerate substring search.

  If the connecting role may not create extensions, Hermes logs a warning and
  continues — search still works, because the `ILIKE` path is plain SQL and the
  full-text path uses core PostgreSQL `tsvector`. Only the trigram acceleration
  is lost, which matters once the message table is large. Hermes re-attempts the
  extension on every connect, so enabling it later is picked up automatically
  with no migration to run by hand.

  On self-managed PostgreSQL, a superuser can install it once per database:
  `CREATE EXTENSION IF NOT EXISTS pg_trgm;`

  See [Enabling `pg_trgm` on managed PostgreSQL](#enabling-pg_trgm-on-managed-postgresql)
  below when the provider restricts extensions.

The database user needs `CREATE` on the target database — Hermes manages its own
tables, indexes, and migrations.

### Enabling `pg_trgm` on managed PostgreSQL

Hosted providers only let non-superusers create extensions from an allow-list.
Hermes runs fine without `pg_trgm`, but enabling it is recommended before the
message table grows, because it is what keeps substring search off a sequential
scan.

**Azure Database for PostgreSQL — Flexible Server.** The `azure.extensions`
server parameter is empty by default, and `CREATE EXTENSION` fails with:

```
ERROR: extension "pg_trgm" is not allow-listed for users in
       Azure Database for PostgreSQL
```

Add it to the allow-list, preserving any extensions already listed — the value
is a comma-separated list and setting it replaces the whole thing:

```bash
# Inspect the current value first so you don't drop existing entries.
az postgres flexible-server parameter show \
  --resource-group <rg> --server-name <server> \
  --name azure.extensions --query value -o tsv

az postgres flexible-server parameter set \
  --resource-group <rg> --server-name <server> \
  --name azure.extensions --value pg_trgm
```

In the portal the parameter is under **Server parameters**; search for
`azure.extensions` and pick `pg_trgm` from the dropdown rather than the "All"
tab, where it is not listed.

`azure.extensions` is a dynamic parameter, so the change generally applies
without a restart — confirm with:

```bash
az postgres flexible-server parameter show \
  --resource-group <rg> --server-name <server> \
  --name azure.extensions \
  --query "{value:value,dynamic:isDynamicConfig,restartPending:isConfigPendingRestart}"
```

If `restartPending` is true, or `CREATE EXTENSION` still reports the extension
as not allow-listed, restart the server and retry:

```bash
az postgres flexible-server restart --resource-group <rg> --name <server>
```

Allow-listing makes the extension *available*; it does not install it.
Extensions are per-database, so each database needs its own `CREATE EXTENSION`.
Hermes does this itself on the next connect — restart the agent, or just let it
reconnect, and it will install `pg_trgm` and build the trigram indexes with no
further action. To confirm:

```sql
SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm';   -- expect 1
SELECT count(*) FROM pg_indexes WHERE indexname LIKE '%\_trgm';  -- expect 3
```

**Other managed providers.** Amazon RDS and Aurora ship `pg_trgm` in the
default `rds.allowed_extensions`, so `CREATE EXTENSION` normally just works.
Google Cloud SQL supports it without an allow-list. In both cases the role
still needs sufficient privilege on the target database.

## Enabling it

Two settings, both under `sessions:` in `~/.hermes/config.yaml`:

```yaml
sessions:
  state_backend: postgres
  postgres_dsn: "postgresql://hermes:secret@db.example.com:5432/hermes?sslmode=require"
```

Or via the CLI:

```bash
hermes config set sessions.state_backend postgres
hermes config set sessions.postgres_dsn 'postgresql://...'
```

The DSN is passed to the driver **unchanged**, so TLS mode, host, port, and
credentials are entirely yours to specify. Use `sslmode=require` (or stricter)
for anything crossing a network.

### Environment variables

All three take precedence over the corresponding `config.yaml` keys, which is
convenient for containers and CI:

| Variable | Purpose |
|---|---|
| `HERMES_STATE_DATABASE_URL` | PostgreSQL DSN |
| `HERMES_STATE_POSTGRES_DSN` | Alternate name for the same value |
| `HERMES_STATE_BACKEND` | `sqlite` (default) or `postgres` |

Resolution order for the DSN is: `HERMES_STATE_DATABASE_URL` →
`HERMES_STATE_POSTGRES_DSN` → `sessions.postgres_dsn`. For backend selection it
is `HERMES_STATE_BACKEND` → `sessions.state_backend`.

Because the DSN carries credentials, it belongs in `~/.hermes/.env` (or your
orchestrator's secret store) rather than in `config.yaml` when you use the
environment-variable form.

## Migrating existing sessions

The `hermes migrate state-to-postgres` subcommand performs an online,
resumable backfill of an existing SQLite state database into PostgreSQL:

```bash
hermes migrate state-to-postgres --dsn 'postgresql://...' --yes \
  [--sqlite-path PATH] [--checkpoint PATH] [--resume] \
  [--batch-rows 5000] [--budget-bytes 44023414784]
```

Running without `--dsn` resolves the target from the
`HERMES_STATE_DATABASE_URL` / `HERMES_STATE_POSTGRES_DSN` environment
variables, or from `sessions.postgres_dsn` in `config.yaml` when
`sessions.state_backend: postgres` is already set. `--sqlite-path` defaults
to `state.db` under your Hermes home.

Use `-y` / `--yes` to skip the confirmation prompt in scripts or CI:

```bash
hermes migrate state-to-postgres --dsn 'postgresql://...' --yes
```

The equivalent direct invocation is:

```bash
python -m migrate_state_to_postgres --dsn 'postgresql://...' \
  [--sqlite-path PATH] [--checkpoint PATH] [--resume]
```

Its properties, by design:

- **Source-safe snapshot.** SQLite is opened with `mode=ro`, and one read
  transaction pins the source snapshot. No local database copy is made.
- **Bounded, resumable COPY.** Each table is streamed in primary-key order
  through psycopg `COPY`. An atomically replaced JSON checkpoint records each
  committed `(table, last_pk)` watermark. `--resume` continues that checkpoint,
  and `ON CONFLICT DO NOTHING` makes a repeated batch idempotent.
- **Disk guard.** PostgreSQL size is checked before loading and after every
  batch. The default budget is 41 GiB; exceeding `--budget-bytes` preserves the
  checkpoint and exits with status 4.
- **Bulk-load indexes.** Secondary and GIN indexes are built after COPY. The
  command also fills every `messages.fts_content IS NULL` row and resets the
  message identity sequence after importing explicit SQLite ids.

The checkpoint identifies the source file and cannot be reused for another
source. It is not a change-data-capture log: enable dual-write before starting
an online backfill, and use a final quiesced full diff before cutover.

### SQLite-primary dual-write validation

`HERMES_STATE_DUAL_WRITE=1` is a transition mode. It keeps SQLite as the read
and write authority, then replays each committed mutation synchronously to the
PostgreSQL DSN in `HERMES_CORE_PG_DSN`. A replica failure is fail-open for the
primary operation and is recorded in SQLite's `_hermes_dual_failures` table for
idempotent replay. It does not select the PostgreSQL read backend and does not
change the default `sessions.state_backend: sqlite`.

Use the dedicated migration DSN; the dual-write and validation tools never
borrow an unrelated application's PostgreSQL DSN.

```bash
export HERMES_STATE_DUAL_WRITE=1
export HERMES_CORE_PG_DSN='postgresql://...'

# Replay journaled transactions, then compare Python-normalized row hashes.
python -m state_diff --sqlite-path ~/.hermes/state.db --replay-failures --full

# Repair missing/different/extra target rows from the SQLite authority.
python -m state_diff --sqlite-path ~/.hermes/state.db --full --repair

# Report dual-write mutation entrypoints not exercised in the validation window.
python -m state_diff --sqlite-path ~/.hermes/state.db --full --coverage \
  [--coverage-waive reviewed-waivers.json]
```

`state_diff` exits 0 for parity, 1 for a mismatch, and 2 when either store is
unavailable. `--since` accepts an epoch timestamp or ISO-8601 value and checks
only tables with `updated_at`; it excludes the newest five minutes so in-flight
dual writes do not create false alarms. A full repair compares cross-database
snapshots, so stop or otherwise quiesce source writers before using its result
as a cutover or rollback decision. Never reuse an earlier full-diff result.

To rehearse rollback into a new SQLite file, use the resumable reverse tool. Its
checkpoint stores only a SHA-256 identity for the credential-bearing DSN, and a
successful run finishes with a full PG-to-SQLite hash comparison:

```bash
python -m state_reverse --dsn "$HERMES_CORE_PG_DSN" \
  --sqlite-path /safe/path/rollback-state.db [--checkpoint PATH] [--resume]
```

Recommended sequence:

1. Provision an empty target and enable SQLite-primary dual-write.
2. Run or resume the online backfill while Hermes continues serving SQLite
   reads. If the disk guard exits 4, increase capacity or choose a reviewed
   budget before resuming; do not simply disable the guard.
3. Replay failures and run incremental hash checks throughout the validation
   window. Exercise every write entrypoint or record a reviewed waiver.
4. Quiesce writes and run a new full diff/repair/full-diff sequence. Rehearse
   reverse backfill and verify its final hash result.
5. Only then set `sessions.state_backend: postgres` in a separately controlled
   cutover. Keep the SQLite authority until the rollback window closes.

## Behavioral notes

**Failure is loud, not silent.** If the DSN is wrong or the server is
unreachable, opening the session store raises. Hermes does **not** quietly fall
back to SQLite — a silent fallback would write to a different database than the
one you configured and split your history across two stores.

**Search uses native full-text indexing, with a bounded `ILIKE` auxiliary
path.** SQLite's FTS5 index has no direct PostgreSQL equivalent, so the backend
builds a `tsvector` column (`messages.fts_content`) with a GIN index. The indexed
document is the same for live writes and backfill: `content`, `tool_name`, and
`tool_calls`, separated by spaces. Queries use the `simple` dictionary
(lowercasing, no stemming) and preserve words, `deploy*` prefixes, quoted
phrases, `OR`, and `-term` exclusions. See `docs/search-contract.md` for the
portable syntax and ordering contract.

Rows written *before* the full-text column existed may have
`fts_content IS NULL`. They do not force every indexed row into a full-table
scan: indexed rows continue through `fts_content @@ tsquery`, while only NULL
rows use a parameter-bound `ILIKE` predicate in the same query. CJK substring
queries also use `ILIKE`, corresponding to SQLite's trigram route. The optional
`pg_trgm` GIN indexes accelerate those predicates; without the extension the
result contract is unchanged, but PostgreSQL may sequential-scan them.

The migration command backfills NULL rows in primary-key chunks (default
5,000), commits one chunk, and atomically records `fts.last_pk`, processed row
count, truncation count, and completion in the same JSON checkpoint used by the
COPY phases. Resume after interruption with the original checkpoint:

```bash
python -m migrate_state_to_postgres \
  --sqlite-path ~/.hermes/state.db \
  --checkpoint /safe/path/state.pg3-backfill.json \
  --batch-rows 5000 --resume
```

PostgreSQL limits a `tsvector` input to roughly 1 MiB. Hermes leaves the
canonical message untouched and indexes at most 512 KiB of the derived UTF-8
document. Truncated rows are persistently listed without storing their content:

```sql
SELECT message_id, source_bytes, indexed_bytes, recorded_at
  FROM hermes_fts_truncations
 ORDER BY message_id;
```

This derived manifest and `fts_content` are not dual-write authority. A fresh
database populated only through live PostgreSQL writes normally has no NULL
rows, but the same byte bound protects those writes as well.

**Two independent schema version counters.** `SCHEMA_VERSION` governs the shared
and SQLite schema and is recorded in the `schema_version` table. The PostgreSQL
backend keeps its own migration list with its own counter, recorded separately
in `pg_migration_version`. The two numbers are deliberately unrelated — do not
expect them to match, and do not merge the tables. (They shared one table
originally, which meant that once the shared version climbed past the highest
Postgres-only migration number, every Postgres migration looked already-applied
and was skipped.)

Every Postgres-only migration statement is `IF NOT EXISTS`-guarded, so a
database with no `pg_migration_version` row simply replays the list; existing
objects are left alone and anything missing is created.

**Read-only opens read PostgreSQL, but cannot write to it.** Read-only callers
— the dashboard's status and session listing, cron history, usage analytics,
and resume lookup — are served from the same physical store as the live write
path. Routing them to a local `state.db` instead would report on a different
database than the one being written to.

Because those callers are not the owner of the store, a read-only open is
restricted in three ways:

- It runs no DDL. Schema is created and migrated only by writable opens.
- It fails, rather than provisioning, when the schema is absent — or when the
  store's recorded migration version is *older* than the running build
  expects, since queries may reference columns it does not have. A store
  *newer* than the running build is accepted: the schema only grows, so an
  older reader's queries still work, and refusing it would break rolling
  upgrades.
- The connection sets `default_transaction_read_only`, so the server itself
  rejects any write with SQLSTATE `25006`. The prohibition is enforced by
  PostgreSQL, not by convention.

**Structured message content uses a `U+0001` sentinel prefix.** PostgreSQL's
`text` type cannot store `NUL`, so the marker distinguishing JSON-encoded
multimodal content from plain strings uses `U+0001` on write. The legacy
`NUL` prefix is still accepted on read, so rows written by older versions decode
correctly on both backends.

## Verifying

After enabling, confirm the backend is actually engaged:

```bash
# Sessions should appear in the target database, not in state.db
psql "$HERMES_STATE_DATABASE_URL" -c 'SELECT count(*) FROM sessions;'
psql "$HERMES_STATE_DATABASE_URL" -c 'SELECT count(*) FROM messages;'
```

Then start a session, send a message, and re-run the message count — it should
increase. A count that stays flat while Hermes appears healthy means the backend
did not engage and writes are still going to SQLite; check that
`sessions.state_backend` is `postgres` and that no stale `HERMES_STATE_BACKEND`
is overriding it.

## Troubleshooting

**`PostgreSQL state backend requires psycopg`** — the `postgres` extra is not
installed and the automatic install did not succeed. Install it explicitly:
`pip install 'hermes-agent[postgres]'`.

**`permission denied to create extension "pg_trgm"`** or **`extension "pg_trgm"
is not allow-listed`** — the connecting role cannot create extensions. This is
**not fatal**: Hermes logs a warning, skips the extension and its trigram
indexes, and runs with search intact but unaccelerated. Fix it at your leisure —
on self-managed PostgreSQL have a superuser run
`CREATE EXTENSION IF NOT EXISTS pg_trgm;` against the target database; on a
managed provider see
[Enabling `pg_trgm` on managed PostgreSQL](#enabling-pg_trgm-on-managed-postgresql).
Hermes retries on the next connect and installs it once permitted.

**Search feels slow on a large database** — check whether the trigram indexes
exist (`SELECT count(*) FROM pg_indexes WHERE indexname LIKE '%\_trgm';`,
expect 3). Zero means `pg_trgm` was never installed, so substring search is
sequential-scanning. Enable the extension as above.

**Turn errors mentioning connection failures** — a managed server restarting for
maintenance surfaces as transient contention and is reported as a retryable
condition rather than storage damage. Retry the message; if it persists, check
server availability and connection limits.

**Search returns nothing** — confirm the backend engaged (see *Verifying*
above). Zero results with a healthy-looking gateway is the classic symptom of
writes landing in one store while reads come from another.
