# levos v3 — PostgreSQL authority patches ported to `levos/pg3` (0.21.2)

Card `t_36694f21`. The v3 concierge runtime builds its core from
`levos/pg3` (0.21.2). The PostgreSQL-authority patches proven on v2 live only
on `levos/pg3-020` (0.20.5): with `sessions.state_backend: authority` the
auxiliary stores, cron, memory, credentials, the gateway's small state files
and `state.db` itself stop touching files / SQLite and use the profile's
PostgreSQL store. The two branches diverged at `da0fc1d1a` (87 / 9693
commits), every cherry-pick conflicts, so this port re-implements the same
contracts on the 0.21.2 code structure. Nothing else from `levos/pg3-020` or
`main` is brought in, and 0061–0064 (messaging lock, turn ownership, delivery
obligations, routing index) stay out of scope.

## 1. Structural map: original (`levos/pg3-020`) → `levos/pg3`

`levos/pg3` split most god-files into modules, so the same function is often
somewhere else. Line numbers are the original post-patch hunk line and the
`levos/pg3` base (`6db2490ff`) definition line.

### Minor fixes

| Original | `levos/pg3` | Difference |
|---|---|---|
| `hermes_state_postgres.py:2228` `open_store_for_profile` (db60c64db) | `hermes_state_postgres.py:1204` `open_store_for_profile` → `open_store_for_home:1197` | Already forwards `read_only` (`SessionDB(..., read_only=read_only, postgres_dsn=dsn)`); only the test contract is ported. |
| `hermes_state.py:8526` `_set_session_title` CAS (f1d844e30) | `hermes_state_titles.py:113` | Title mixin split out of `hermes_state.py`; still `title IS ?`. The PG translator (`hermes_state_pg_sql.py:172`) and journal replay (`hermes_state_dual.py:309`) rewrite it, the SQL text itself did not. |
| `hermes_state.py:10547` `set_latest_user_api_content` CAS | `hermes_state_messages.py:687` (+ row-addressed sibling `:714` `set_message_api_content`, new in pg3) | Same. |

### 0059 — auxiliary stores

| Original | `levos/pg3` | Difference |
|---|---|---|
| `hermes_aux_store.py` (new) | `hermes_aux_store.py` (new) | `_split_sql_statements` lives in `hermes_state_pg_schema.py` (not re-exported by `hermes_state_postgres`). `hermes_state_writer.py` is byte-identical, so `_PostgresWriterConnection` / `postgres_ddl` are reused as is. |
| `agent/verification_evidence.py:68` `_connect` | `agent/verification_evidence.py:122` `_connect` | Module shortened (592 lines); same connect/initialize split. |
| `gateway/platforms/api_server.py:898` `ResponseStore` | `gateway/platforms/api_server.py:720` `ResponseStore` | Adapter split into mixins (`OpenAICompatRoutesMixin`); `ResponseStore` stayed in `api_server.py`. |
| `gateway/platforms/api_server.py:7645` `APIServerAdapter` | `gateway/platforms/api_server.py:1155` | Same class, 6k lines shorter. |
| `hermes_cli/projects_db.py:164` `connect` | `hermes_cli/projects_db.py` `connect` | Already routes PG through `projects_persistence`; the selector changes to `aux_store_authority`. |
| `hermes_cli/projects_postgres.py:28,39` | `hermes_cli/projects_postgres.py:24,28` | Reused, not re-implemented (kanban adapter base). |
| `plugins/platforms/discord/adapter.py:2204` `DiscordAdapter` | `plugins/platforms/discord/adapter.py:997` | Media split into `DiscordMediaMixin`. |
| `plugins/platforms/discord/recovery.py:23` | `plugins/platforms/discord/recovery.py:22` | Same shape. |

### 0060 + 0068 — cron

| Original | `levos/pg3` | Difference |
|---|---|---|
| `cron/executions.py:54..314` | `cron/executions.py:36..265` | pg3 added `handoff_started_at` CAS (`:296`) and a longer lifecycle; the PG lease must cover it. |
| `cron/jobs.py:273` `_jobs_lock_file`, `:427` `_jobs_lock`, `:512` `_fire_job_lock` | `cron/jobs.py:199`, `:239`, `:293` | Same names. |
| `cron/jobs.py:1439` `load_jobs`, `:1496` `_peek_jobs_unlocked`, `:1624` `_save_jobs_unlocked` | `cron/jobs.py:1238`, `:1287`, `:1394` | Same names. |
| `cron/jobs.py:2503` `remove_job`, `:3778` `save_job_output` | `cron/jobs.py:2141`, `:3162` | Same names. |
| `cron/notepad.py:43..183` | `cron/notepad.py:41..129` | Same shape. |
| `cron/scheduler.py:1472` `_get_lock_paths`, `:7048` `tick` | `cron/scheduler.py:1028`, `:3693` | Same names. |
| `cron/scheduler.py:3859` `_run_job_script` | `cron/scheduler_script.py:317` | Extracted module. |
| `cron/scheduler.py:4141` `_build_job_prompt` | `cron/scheduler_prompt.py:203` | Extracted module. |
| `cron/monitor.py:95,109` | `cron/monitor.py:67,77` | Same. |
| `cron/suggestions.py:83..379` | `cron/suggestions.py:58..216` | Same names. |
| `tools/cronjob_tools.py:630` `_validate_cron_script_path` | `tools/cronjob_job_args.py:291` | Extracted module. |
| `tools/cronjob_tools.py:1282` `cronjob` | `tools/cronjob_tools.py:856` | Same. |
| `agent/curator_backup.py:98` `_backup_cron_jobs_into` | `agent/curator_backup.py:61` | Same. |
| `cron/durable.py` (new) | `cron/durable.py` (new) | — |

### 0065 — memory, 0066 — credentials

| Original | `levos/pg3` | Difference |
|---|---|---|
| `tools/memory_tool.py:364` `MemoryStore` | `tools/memory_tool_store.py:66` `MemoryStore` | Store class extracted; `tools/memory_tool.py` keeps the tool surface. |
| `agent/learning_graph.py:198` `_memory_cards` | `agent/learning_graph.py:129` | Same. |
| `agent/learning_mutations.py:76` `_locate_memory(source, gidx)` | `agent/learning_mutations.py:34` `_locate_memory(node_id)` | Signature differs (node id parsed inside). |
| `hermes_cli/auth.py:1129,1287,1452,1535` | `hermes_cli/auth.py:496,633,651,735` | Same names (`_load_global_auth_store`, `_auth_store_lock`, `_load_auth_store`, `_save_auth_store`). |

### 0067 — gateway small state, 0069 — state.db file dependency, 0070 — schema lock

| Original | `levos/pg3` | Difference |
|---|---|---|
| `agent/estop.py:67..229` | `agent/estop.py:31..122` | Same functions. |
| `gateway/dead_targets.py:64` | `gateway/dead_targets.py:47` | Same class. |
| `gateway/pairing.py:332,496` | `gateway/pairing.py:227,336` | Same. |
| `gateway/platforms/helpers.py:269` `ThreadParticipationTracker` | `gateway/platforms/helpers.py:82` | Same. |
| `gateway/platforms/webhook.py:505` | `gateway/platforms/webhook.py:152` | Same class. |
| `gateway/rich_sent_store.py:43..103` | `gateway/rich_sent_store.py:58,79` | Same. |
| `gateway/run.py:7434` `GatewayRunner` (voice modes) | `gateway/run.py:3328` `GatewayRunner` | Same class, 25k lines shorter. |
| `hermes_cli/subcommands/pause.py:19,38` | `hermes_cli/subcommands/pause.py:13,31` | Same. |
| `hermes_cli/webhook.py:43..317` | `hermes_cli/webhook.py:22..184` | Same. |
| `plugins/platforms/discord/adapter.py:333` tracker | `plugins/platforms/discord/adapter.py:431` | Same class. |
| `acp_adapter/session.py:415` `SessionManager` | `acp_adapter/session.py:148` | Same. |
| `hermes_cli/web_server.py:341` `_eager_reconcile_own_session_db` | `hermes_cli/web_server_lifecycle.py:173` | Extracted module. |
| `hermes_cli/web_server.py:1876` `_count_status_active_sessions` | `hermes_cli/web_routers/status.py:72` | Extracted router. |
| `mcp_serve.py:515` `EventBridge` | `mcp_serve.py:264` | Same. |
| `tui_gateway/server.py:3988,4003` `_cron_sig` / `_sessions_sig` | `tui_gateway/change_watcher.py:118` (`_sessions_sig`; no `_cron_sig` yet) | Watcher extracted from `server.py`. |
| `hermes_state_postgres.py:1034` `init_postgres_schema`, `:1111` `finalize_postgres_schema` | `hermes_state_pg_schema.py:1002`, `:1036` | Schema module extracted. |

### Tests

The original tests use the fork's ephemeral Unix-socket PostgreSQL
(`initdb` / `pg_ctl` on `PATH` or in `PG3_PERCENT_PG_BIN`, fixture
`tests/test_pg3_writer_local_follow_backend.py::postgres_dsn`), which
`levos/pg3` already carries unchanged; 0059's test used py-pglite.

## 2. Ported items

"Tests" run on the fork's ephemeral PostgreSQL (`initdb`/`pg_ctl`,
`PG3_PERCENT_PG_BIN`); every case below ran on a real PostgreSQL 16 server.

| Patch | pg3 implementation | Test | Skip |
|---|---|---|---|
| db60c64db read_only reaches PG | already on pg3 (`hermes_state_postgres.open_store_for_home`); contract tests only | `tests/test_pg_reader_seam.py::test_readonly_pg_profile_open_rejects_writes_on_the_server` (real PG: no DDL, 25006, no row), `::test_readonly_pg_profile_open_uses_readonly_connection` (driver fake); `tests/test_pg_parity_smoke.py::test_a7c_readonly_pg_profile_open_rejects_insert` (original, Docker) | a7c: the whole parity module skips without a Docker daemon (pg3 convention, unchanged) |
| f1d844e30 NULL-safe CAS | `hermes_state_titles.py` (`_set_session_title`), `hermes_state_messages.py` (`set_latest_user_api_content`, `set_message_api_content`) | `tests/test_pg_reader_seam.py::test_null_safe_cas_statements_run_verbatim_on_postgres` (red on base: PG `SyntaxError at $4`) | — |
| shared | `hermes_aux_store.py` = original after 0070 minus 0061 (`AuxSessionLock.held`), 0062 (turn/pending/tui stores), 0063 (lease helpers) | all below | — |
| 0059 aux stores | `hermes_aux_store.open_aux_store`; `agent/verification_evidence.py` (`_connect`, `_initialize_connection`); `gateway/platforms/api_server.py` (`ResponseStore._db`, `_initialize_response_store`, 503 `aux_store_unavailable_middleware`); `hermes_cli/projects_db.py:connect` → existing `projects_postgres`/`kanban_postgres` adapter with the session DSN (no second implementation); `plugins/platforms/discord/recovery.py`, `adapter.py` (close + PG boolean binds) | `tests/test_aux_store_authority.py` (moved from py-pglite to the real PG fixture) | — |
| 0060 cron stores/locks | `cron/executions.py` (PG ledger incl. pg3's handoff/adopt lifecycle, server-clock leases, `IS NOT DISTINCT FROM` CAS), `cron/incidents.py` (pg3 keeps incidents in `executions.db`: `core_cron_incidents`), `cron/notepad.py`, `cron/jobs.py` (`core_cron_jobs`, `_jobs_lock` xact lock, `_fire_job_lock` session lock), `cron/scheduler.py:tick` (`cron-tick` lock), `agent/curator_backup.py` | `tests/test_cron_pg_authority.py` | 1: `heartbeat_fire_claim` fenced by the fire lock — pg3 deliberately takes the heartbeat out of that fence (serialised by `_jobs_lock` on every backend) |
| 0068 cron outputs/scripts/suggestions | `cron/durable.py` (new), `cron/jobs.py` (`remove_job`, `save_job_output`), `cron/monitor.py`, `cron/scheduler_prompt.py` (`_usable_context_output`), `cron/scheduler_script.py` (`_resolve_script_path` restore), `cron/suggestions.py`, `tools/cronjob_job_args.py` (`_store_cron_scripts`), `tools/cronjob_tools.py` | `tests/test_cron_overlap_c10.py` | — |
| 0065 memory | `tools/memory_tool_store.py` (`MemoryStore` authority branch + PG helpers), `tools/memory_tool.py` (re-exports), `agent/learning_graph.py`, `agent/learning_mutations.py` | `tests/test_memory_pg_authority.py` | — |
| 0066 credentials | `hermes_cli/auth.py` (`_load_auth_store`, `_save_auth_store`, `_auth_store_lock`, `_load_global_auth_store`, `_auth_pg_*`) + first-load seed `_auth_pg_seed` | `tests/test_auth_pg_authority.py` | — |
| 0067 gateway small state | `agent/estop.py`, `gateway/dead_targets.py`, `gateway/pairing.py`, `gateway/platforms/helpers.py`, `gateway/platforms/webhook.py`, `gateway/rich_sent_store.py`, `gateway/run_voice.py` (pg3's voice mixin; `gateway/run.py` itself needed no change), `hermes_cli/subcommands/pause.py`, `hermes_cli/webhook.py` (incl. whole-set `_save_subscriptions` used by the dashboard's `web_routers/ops.py`), `plugins/platforms/discord/adapter.py` (non-conversational tracker) | `tests/test_aux_kv_pg_authority.py` | — |
| 0069 state.db file dependency | `acp_adapter/session.py`, `hermes_cli/web_server_lifecycle.py`, `hermes_cli/web_routers/status.py`, `mcp_serve.py` (`_read_state_db_mtime`), `tui_gateway/change_watcher.py` (`_sessions_sig`, new `_cron_sig`); `tui_gateway/server.py` needed no change | `tests/test_overlap_c11_authority.py` | — |
| 0070 core schema lock | `hermes_state_pg_schema.py` (`init_postgres_schema` / `finalize_postgres_schema` under `schema:core`, `SCHEMA_LOCK_WAIT_SECONDS = 60`, old body `_init_postgres_schema_locked`) | `tests/test_core_schema_pg_lock.py`; fakes in `tests/test_pg_schema_parity.py`, `tests/test_pg_token_counter_width.py` answer the lock statements | `test_pg_token_counter_width.py` does not collect on `levos/pg3` before or after this change (imports a helper pg3 no longer has); pre-existing, left as is |

### Card additions beyond the original patches

- **Credential seed (0066).** The v3 launcher copies `/run/secrets/hermes/auth.json`
  to `HERMES_HOME/auth.json` only when that file is absent. On authority, the
  first load of a store whose PostgreSQL row (`profile` / `root`) does not exist
  imports the local file once (`INSERT … ON CONFLICT DO NOTHING` under the
  store's advisory lock, same validation as `migrate_auth_to_pg`; an invalid
  file raises instead of seeding an empty store). An existing row always wins:
  the local file never overwrites PostgreSQL, and the file is neither written
  nor removed. Tests: `test_authority_first_load_seeds_an_absent_row_once_from_the_local_file`,
  `test_authority_existing_row_wins_over_the_local_file` (A in PG, B on disk →
  A read back, document and `updated_at` unchanged),
  `test_authority_seed_refuses_an_invalid_local_file`.
- **First-run migration counts.** Besides "a re-run inserts 0", the one-shot
  moves assert *source entries on the first run = entries in PostgreSQL*:
  credentials (`test_auth_migration_first_run_reflects_every_source_key_in_postgres`),
  memory (`tests/test_memory_pg_authority.py`), cron (executions, incidents,
  notes, jobs in `tests/test_cron_pg_authority.py`) and the gateway key-value
  files (`test_kv_migration_is_idempotent_and_leaves_the_sources_untouched`).
- **Cron incidents.** pg3 added an incidents table to `executions.db`; on
  authority it moves with the ledger (`core_cron_incidents`), otherwise a failing
  job would still create a file under `cron/`.

## 3. No SQLite / file fallback on authority

Every authority branch opens through `open_aux_store`, `open_aux_postgres`,
`connect_aux_postgres`, `aux_kv_*`, `AuxSessionLock` / `aux_xact_lock` or
`aux_change_signal`; each raises `AuxStoreUnavailable` (message without the
DSN) when PostgreSQL cannot serve, and none of them opens SQLite, `:memory:` or
a file. The only deliberate fail-safe answers are the ones the original
defines: ESTOP reads as *engaged* and the rich-send index as *absent* when the
store cannot answer. Each test module runs authority against an unreachable
DSN and asserts the raise plus an empty state directory
(`test_authority_without_postgres_*`, `test_change_signal_off_authority_or_unreachable_raises`).
Non-authority backends (`sqlite`, `probe`) keep their files, SQLite databases
and flocks; each module has a `test_non_authority_*` case.

The one-shot moves stay operator actions, as in the original:
`python -m hermes_aux_store --profile <p> [--cron|--memory|--auth|--kv] [--dry-run]`
(plus `cron.durable.migrate_cron_files_to_pg` for 0068's files). Only the
credential store seeds itself (above).

## 4. Out of scope / remaining

- 0061 messaging single connection, 0062 turn ownership / pending messages /
  tui markers, 0063 delivery obligations / async delegation leases, 0064
  routing index — next cards. Their files (`gateway/status.py`,
  `gateway/platforms/base.py`, `gateway/session.py`, `gateway/turn_owner.py`,
  `gateway/delivery_ledger.py`, `tools/async_delegation.py`,
  `tui_gateway/turn_marker.py`, `plugins/platforms/telegram/`) are untouched.
- Stores that are new in 0.21.2 and were not part of any v2 patch still use the
  pod disk on authority: `gateway/platforms/api_server_run_idempotency.py`
  (`runs_idempotency.db`) and `cron/delivery_queue.py` (restart-safe worker
  delivery queue, pid-based ownership).
- `docs/quality/rule-index.md` referenced by the card does not exist in this
  repository (no `docs/quality/`); the root `AGENTS.md` rules (tests through
  real imports against a temp `HERMES_HOME`, no new `HERMES_*` env var,
  behaviour-contract tests) were followed instead.

## 5. Verification (2026-09-28, this branch)

- Port tests on a real PostgreSQL 16.4 (`initdb`/`pg_ctl` binaries,
  `PG3_PERCENT_PG_BIN`):
  `python -m pytest tests/test_aux_store_authority.py tests/test_cron_pg_authority.py tests/test_cron_overlap_c10.py tests/test_memory_pg_authority.py tests/test_auth_pg_authority.py tests/test_aux_kv_pg_authority.py tests/test_overlap_c11_authority.py tests/test_core_schema_pg_lock.py tests/test_pg_reader_seam.py -q -p no:warnings`
  → 140 passed, 1 skipped (the `heartbeat_fire_claim` case above), 0 failed.
- No regression against `origin/levos/pg3`: `scripts/run_tests.sh -j 3`
  (per-file isolation) over `tests/cron tests/gateway tests/test_hermes_state*.py
  tests/tools tests/hermes_cli tests/tui_gateway tests/acp tests/acp_adapter
  tests/agent` on both trees: 36636 passed / 119 failed / 333 skipped on
  each, the same failing files except two timing tests
  (`test_terminal_timeout_output`, `test_browser_use_cli`) that fail
  identically on both trees when run alone. The root `tests/test_pg*.py` /
  `tests/test_hermes_state_pg*.py` files: the same 85 failing cases on both.
  Two regressions this comparison found were fixed on the branch
  (`DeadTargetRegistry` resolving the backend in its constructor;
  a corrupt `config.yaml` breaking the credential pool on SQLite installs).
