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
  routing index — next cards (done by `t_aa3728da`, §6). Their files (`gateway/status.py`,
  `gateway/platforms/base.py`, `gateway/session.py`, `gateway/turn_owner.py`,
  `gateway/delivery_ledger.py`, `tools/async_delegation.py`,
  `tui_gateway/turn_marker.py`, `plugins/platforms/telegram/`) are untouched.
- Stores that are new in 0.21.2 and were not part of any v2 patch still use the
  pod disk on authority (closed by `t_aa3728da`, §6): `gateway/platforms/api_server_run_idempotency.py`
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

## 6. v3 file-state closure

Card `t_aa3728da`. Goal: a PostgreSQL-authority (`sessions.state_backend:
authority`) v3 runtime leaves no operational state on the pod disk, so a pod
started anew continues from PostgreSQL and two briefly overlapping pods of one
profile do not collide. Base: `levos/pg3` `9bb784cf6`. The v2 patches
0061–0064 live on `levos/pg3-020` (`fcc1dda98`, `c20c7837e`, `1081cacdd`,
`9cf9ff439`) and are re-implemented on the 0.21.2 structure; E–H have no v2
original.

### 6.1 Audit: where authority still opened a file or SQLite (base `9bb784cf6`)

Line numbers are the base tree. "Opens" means the authority path reaches the
file/SQLite call with no PostgreSQL branch in between.

| Bundle | File:line (base) | What is opened on authority | v2 original |
|---|---|---|---|
| A 0061 messaging single connection | `gateway/status.py:351` `_get_lock_dir`, `:1285` `acquire_scoped_lock` (`:1327` `_write_json_excl`), `:1333` `release_scoped_lock` | `gateway-locks/<scope>-<hash>.lock` under `XDG_STATE_HOME` — pod-local, so two pods connect the same bot token | `fcc1dda98` |
| | `gateway/platforms/base.py:2119` `_acquire_platform_lock`, `:2157` `_release_platform_lock` | same lock file; `--replace` takeover by local PID | |
| | `plugins/platforms/telegram/adapter.py:2930` (acquire), `:3129` (release before polling stopped), `:2279` (409 retry `drop_pending_updates=True`), `:2875`/`:2907` (cold boot drops the Bot API queue) | lock file; the next pod's cold boot deletes the messages sent during the handoff | |
| | `plugins/platforms/discord/adapter.py:1227` / `:1861`, `plugins/platforms/slack/adapter.py:1634` / `:1756` | lock file (order already lock → transport → close → unlock) | |
| B 0062 turn ownership / pending messages / tui markers | `gateway/session_lifecycle.py:128` `mark_turn_active`, `:140` `clear_turn_active`, `:150` `recover_interrupted_turns` | turn markers decided by the pod-local `.clean_shutdown` receipt | `c20c7837e` |
| | `gateway/run_startup.py:949` (`.clean_shutdown` read/unlink), `:650` `_recover_unclean_sessions` (120 s recency fallback) | `HERMES_HOME/.clean_shutdown` | |
| | `gateway/run_shutdown.py:1926` | writes `.clean_shutdown` | |
| | `gateway/shutdown_flush.py:34` `_get_flush_dir`, `:44` `_write_payload`, `:115` spool, `:133` drain, `:201` `recover_pending_to_db` (callers `gateway/run_shutdown.py:1150,1823,1827`, `gateway/run.py:5388`, `gateway/platforms/base.py`, `gateway/session_transcript.py`) | `HERMES_HOME/pending_messages/pending-*.json` | |
| | `tui_gateway/turn_marker.py:31` `_marker_path`, `:58` `_store` | `HERMES_HOME/desktop/interrupted_turns.json` | |
| | `gateway/session_state.py`, `gateway/run_watchers.py` | nothing (in-memory state; the watcher only prunes through `SessionStore`) | |
| C 0063 delivery obligations / async delegations | `gateway/delivery_ledger.py:146` `_connect`, `:261` `_owner_alive`; `tools/async_delegation.py:85` `_connect`, `:328` `recover_abandoned_delegations` | no file (pg3 0051 already writes both tables to PostgreSQL via `SessionDB.open_writer`), but ownership is a local pid probe — see 6.4 | `1081cacdd` |
| D 0064 routing index | `gateway/session_persistence.py:61` `acquire(path)` | **`state.db` on SQLite**: an explicit path makes `SessionDB` select SQLite (`hermes_state.py:471`, `hermes_state_registry.py:207`), so the gateway's `SessionStore` and everything borrowing its handle ran on SQLite | `9cf9ff439` |
| | `gateway/session_persistence.py:288` `_import_legacy_sessions_json`, `:438`/`:454` whole-scope `replace_gateway_routing_entries`, `:460` `_save_sessions_json` | `sessions/sessions.json` read and mirrored; a whole-scope rewrite erases a peer pod's keys | |
| | `gateway/channel_directory.py:40` `_directory_path`, `:167` write, `:364` `_build_from_sessions_json` | `HERMES_HOME/channel_directory.json`, `sessions/sessions.json` | |
| | `hermes_state_sessions.py` | no `sessions.json` access (only a docstring at `:378`); nothing to change | |
| E 0.21.2 new stores | `cron/delivery_queue.py:77` `_db_path`, `:85` `sqlite3.connect` (enqueue `cron/scheduler_delivery.py`, drain `cron/scheduler.py`) | `cron/deliveries.db` | none |
| | `gateway/platforms/api_server_run_idempotency.py:67`, `:72`, `:81` (`:memory:` fallback) | `runs_idempotency.db` | none |
| F state checks | `gateway/readiness.py:24` `_probe_state_db` (`:32` connect) | opens `state.db` read-only whenever the file exists | none |
| | `gateway/lifecycle_ledger.py:172` `check_state_db_integrity` (`:184` connect) | same | none |
| G hosted rooms | `gateway/hosted_rooms.py:398` `default_db_path`, `:434`/`:438` `_connect`, `:970` probe; `gateway/hosted_rooms_common.py:99`, `:119`; `gateway/hosted_room_policy_checkpoint.py:115` (worker started by `gateway/run_startup.py` `_ensure_hosted_room_worker`) | `shared-state.db` (root of the profiles tree) | none |
| H memory plugin | `plugins/memory/holographic/store.py:103`, `:116` | `memory_store.db` when `memory.provider: holographic` | none |

Runtime paths found beyond the card's table (same audit):

| File:line (base) | What | Disposition |
|---|---|---|
| `hermes_state_pg_schema.py:1258` → `hermes_state_schema.py:707` | every PostgreSQL connect parses the SQLite `SCHEMA_SQL` in `sqlite3.connect(":memory:")` (and caches it in `cache/schema_columns.json`) | parsed without SQLite on the PostgreSQL path (6.3) |
| `hermes_cli/goals.py:597` `_acquire_session_db` | `acquire(home/"state.db")` → SQLite `state.db` for the goal manager | follows the home's backend (6.3) |
| `plugins/memory/retaindb/__init__.py:204`, `:345` | `retaindb_queue.db` when `memory.provider: retaindb` | explicit error on authority, like H |
| `gateway/status.py:984` `write_runtime_status` | `gateway_state.json` | see 6.3 |
| `tools/bot_live_delivery.py:37`, `:162`; `plugins/platforms/a2a/adapter.py:136` | open `state.db` when the file exists — **wrong assumption**: an authority pod keeps the `state.db` it wrote before the switch, and these paths opened it (opsi-v3, 2026-09-30) | closed by `t_0eeaa6f9` (6.7): PostgreSQL on authority, the file never opened; `hermes_cli/web_routers/status.py` `/api/status` and `hermes_cli/update_cmd_maint.py` (FTS notice) had the same shape |
| `hermes_cli/observability/shared_metrics.py:307` | telemetry SQLite, only behind its opt-in | unchanged; listed as remaining |
| process identity / restart bookkeeping: `gateway.pid`, `gateway.lock`, `.gateway-takeover.json`, `.gateway-planned-stop.json`, `gateway-starts.log`, `.restart_failure_counts`, `.restart_pending.json`, `.restart_notify.json`, `state/gateway.lifecycle.json`, `state/` heartbeat, `.drain_request.json`, control-socket pointer, `processes.json`, cron ticker heartbeat / output / audit files, `spawn-trees/` | per-process, per-pod liveness and operator files | not operational state shared across pods; listed as remaining |

Excluded as the card says: kanban (`kanban*.py`), backup / recovery / doctor CLIs, one-shot migrations, evals, scripts, tests.

### 6.2 Implementation, per bundle

Every authority branch below opens through the existing `hermes_aux_store`
seam (`open_aux_postgres`, `aux_kv_*`, `AuxSessionLock`, `aux_xact_lock`,
`aux_schema_transaction`, `connect_aux_postgres`) and raises
`AuxStoreUnavailable` (no DSN in the message) when PostgreSQL cannot serve;
none opens SQLite, `:memory:` or a file. Off authority (`sqlite`, `probe`)
every path is the base code.

| Bundle | Implementation | Tests (real PostgreSQL) |
|---|---|---|
| prerequisite | `hermes_state_postgres.open_authority_store_for_db_path` / `home_selects_authority`; used by `gateway/session_persistence.py` (`_open_authority_store`) and `hermes_cli/goals.py` (`_acquire_session_db`) — the gateway's `SessionStore` (and the runner handle that borrows it) and the goal manager open the authority store instead of a SQLite `state.db` | `tests/gateway/test_routing_pg_authority.py::test_session_store_follows_the_profile_backend` |
| A 0061 | `gateway/status_pg_locks.py` (new: acquire / release / ensure, re-entrant per owner), `gateway/status.py` (`acquire_scoped_lock` / `release_scoped_lock` route there), `gateway/platforms/platform_lock.py` (new mixin: PostgreSQL platform lock, retryable `<scope>_lock`, watch task → `<scope>_lock_lost`), `gateway/platforms/base.py` (routing only), `plugins/platforms/telegram/adapter.py` (unlock after polling stopped; keep the Bot API queue on cold boot and 409 retry), `hermes_aux_store.AuxSessionLock.held()`; Discord / Slack already lock → open → close → unlock | `tests/test_messaging_lock_pg_authority.py` (18: two spawned pods per platform, handoff order, lost session retaken / disconnect, cold-boot queue) |
| B 0062 | `gateway/turn_owner.py` (new: lease rows, `claim_dead`, `hand_over`, `release`), `gateway/session_turn_leases.py` (new `SessionStore` mixin: `recover_turns_from_leases`, legacy markers, `hand_over_turns`), `gateway/session_lifecycle.py` (`mark_turn_active` leases first, `clear_turn_active` drops; `_settle_turn_marker`), `gateway/run_turn_leases.py` (new `GatewayRunner` mixin: startup recovery without the 120 s fallback, supervised `turn_lease_watcher` that also recovers a peer's pending messages, drain-timeout hand-over, exit receipt = lease outcome), `gateway/run_startup.py` / `gateway/run_shutdown.py` (call sites), `gateway/shutdown_flush.py` + `gateway/shutdown_flush_pg.py` (new: `core_gateway_pending_messages`, `FOR UPDATE SKIP LOCKED`, legacy files recovered once), `tui_gateway/turn_marker.py` (`core_tui_turn_markers` with owner/lease, incl. pg3's `auto_continue`), `hermes_aux_store` (stores + `AuxLeaseRenewer`) | `tests/test_overlap_c03_pg_authority.py` (14) |
| C 0063 | see 6.4; `gateway/delivery_ledger.py`, `tools/async_delegation.py`, `gateway/run_turn_leases.py` (`_schedule_obligation_resweep`, called from `run_startup._claim_pending_obligations`), `hermes_aux_store` (`AUX_LEASE_COLUMNS`, `AUX_LEASE_EXPIRED`, `aux_owner_instance`, `aux_add_columns`) | `tests/test_overlap_c04_leases.py` (6); `tests/test_pg3_writer_local_follow_backend.py` ends an owner by lease on PostgreSQL |
| D 0064 | `hermes_state_gateway.py` (`apply_gateway_routing_changes`, `load_gateway_routing_entry`), `gateway/session_persistence.py` (row view, row-level writes, `_refresh_routing_key`, no `sessions.json` import/mirror, no sessions dir), `gateway/session.py` / `gateway/session_transcript.py` (keyed entry points refresh their row); beyond v2: `gateway/channel_directory.py` = one `core_aux_kv` row per platform (a pod writes only platforms it built) | `tests/gateway/test_routing_pg_authority.py` (9) |
| E | `cron/delivery_queue.py` (`core_cron_deliveries` / `core_cron_delivery_tombstones`, advisory-locked transactions, claim lease instead of pid probe); `gateway/platforms/api_server_run_idempotency.py` (`core_run_idempotency`, advisory-locked reserve/lookup, owner lease → `owner_live`) + `gateway/platforms/api_server_runs.py` (uses `owner_live`) | `tests/test_v3_no_file_state_authority.py::test_cron_delivery_queue_*`, `::test_run_idempotency_*` |
| F | `hermes_state_postgres.probe_authority_store` (one short connection; ok / schema absent / unreachable); `gateway/readiness.py` (`_probe_state_db`), `gateway/lifecycle_ledger.py` (`check_state_db_integrity`) | `::test_state_checks_*` |
| G | refused (decision below): `gateway/hosted_rooms.py` (`hosted_rooms_enabled`, `HostedRoomsDisabledError` from `default_db_path`), `tui_gateway/methods_groups.py` (`start_hosted_room_service` → None), `gateway/run_startup.py` (no room worker / watcher) | `::test_hosted_rooms_*` |
| H | explicit error: `plugins/memory/holographic/store.py` (`MemoryStore`), `plugins/memory/retaindb/__init__.py` (`_WriteQueue`) raise `AuxStoreUnavailable` naming the file before any connect | `::test_sqlite_memory_plugin*` |
| guard | `tests/test_v3_no_file_state_authority.py::test_v3_pod_on_authority_keeps_no_state_files_and_opens_no_sqlite`; what it found: `gateway_state.json` → `gateway/status_pg_runtime.py` (a `core_aux_kv` row keyed by pod hostname + record path over one cached connection, `gateway/status.py` `write_runtime_status` / `read_runtime_status`); `sqlite3.connect(":memory:")` on every PostgreSQL connect → `hermes_state_pg_columns.declared_schema_columns` (SQLite-free `CREATE TABLE` reader used by `reconcile_postgres_columns`) | `::test_runtime_status_*`, `::test_postgres_schema_reconcile_reads_the_columns_sqlite_would` |

**G decision.** Hosted rooms (Group Chat) are ~4.5k lines of room
coordination over the install-root SQLite `shared-state.db` (driver, replicas,
peers, policy checkpoints, grants). An authority pod must not keep that file
and overlapping pods would each coordinate their own copy, while a PostgreSQL
port of the whole state machine is a feature of its own. The feature is
therefore refused on authority, loudly: `default_db_path()` raises
`HostedRoomsDisabledError` (a `HostedRoomError`, so the legacy prompt fence
treats a `Group: …` title as "not a hosted room"), the service does not start,
the gateway logs one info line, room grant routes answer with their existing
error responses. Off authority nothing changes.

### 6.3 Regression guard

`tests/test_v3_no_file_state_authority.py` spawns one pod process (fresh
`HERMES_HOME`, `sessions.state_backend: authority`, real PostgreSQL,
`sqlite3.connect` replaced by a recorder before any import) that runs
`gateway.run.start_gateway` → tui `session.create` + `prompt.submit` against a
local OpenAI-compatible stand-in → one `cron.scheduler.tick` with a due job →
a messaging adapter's credential lock taken and released → SIGTERM shutdown.
It asserts the steps succeeded (the runtime status read back from PostgreSQL
says `running`, the turn stored user + assistant rows in PostgreSQL, the tick
ran one job, the lock was the PostgreSQL one), zero `sqlite3.connect` calls
(`:memory:` included), and none of `*.db`, `*.sqlite*`, `*-wal`, `*-shm`,
`gateway-locks/`, `sessions.json`, `channel_directory.json`,
`gateway_state.json`, `desktop/interrupted_turns.json`, `cron/deliveries.db`
under the home or the XDG lock directory. Not checked (not store state): logs,
`cache/`, `skills/`, `SOUL.md`, process identity and liveness files. On the
base tree the same pod records 8 `sqlite3.connect` calls (`shared-state.db` ×4,
`state.db` ×3, `:memory:` ×1) and leaves `state.db`, `shared-state.db`,
`gateway_state.json`, `channel_directory.json` and a `gateway-locks/` file.

Kanban: a v3 deployment sets `kanban.enabled: false` (operator decision, eren
2026-09-29; §6.6), so nothing opens a kanban store. Should kanban be turned
back on, the pod must also set `HERMES_KANBAN_BACKEND=postgres`: with the
default SQLite kanban backend the gateway's dispatcher and the tui
notification poller open `kanban.db`. The guard runs the pod both ways
(kanban on its PostgreSQL backend; kanban off with `HERMES_KANBAN_BACKEND`
unset, kept up past the kanban watchers' first ticks).

### 6.4 C: pg3's PostgreSQL tables vs the v2 lease contract

| Contract (v2 `1081cacdd`) | `levos/pg3` before this card | Added here |
|---|---|---|
| rows live in the profile's PostgreSQL store | yes (0051 `SessionDB.open_writer`) | — |
| owner identity | `owner_pid` + `owner_started_at` (local kernel) | `owner_instance` (per-process id) + `lease_expires_at` (server clock), added idempotently |
| owner renews while alive | no | `AuxLeaseRenewer`, every 30 s to 120 s ahead (both tables) |
| takeover rule | pid probe (`_owner_alive`, `_pid_exists`) — another pod's pids look dead | only another instance's row whose lease ran out, re-checked inside the guarded `UPDATE`; no pid probe on PostgreSQL |
| a live-lease row skipped at startup | waits for the next restart | gateway `_schedule_obligation_resweep` after `seconds_until_recoverable()`; delegations: deferred recovery timer on the completion queue |
| this process's own rows (pg3-only: `sweep_failed_for_runtime`, `pending_flood_retries`, `release_runtime_claim`) | pid + start time | also `owner_instance` on PostgreSQL (another pod's namespace can repeat both) |
| flood-adopt claim (pg3-only) | pid-guarded | same lease guard and lease stamp as the claim |

### 6.5 Remaining (not closed by this card)

- **Process identity / liveness and operator files** stay pod-local by design
  (each describes one process on one pod): `gateway.pid`, `gateway.lock`,
  `gateway.sock`, `.gateway-takeover.json`, `.gateway-planned-stop.json`,
  `gateway-starts.log`, `.restart_*`, `.update_*`, `.drain_request.json`,
  `state/gateway.lifecycle.json`, `state/gateway.heartbeat`,
  `runtime/active_sessions.json` (+ `.lock`, the per-host session slot cap),
  `processes.json`, `spawn-trees/`, `cron/ticker_heartbeat`,
  `cron/ticker_last_success`, `cron/catch_up_occurrences`,
  `cron/usage_audit.jsonl`, `cron/output/*.md` (also mirrored to PostgreSQL
  by 0068), `.skills_prompt_snapshot.json`, `.update_check`.
- **Readers that stat or open `gateway_state.json` directly** instead of
  `gateway.status.read_runtime_status` see no record on authority:
  `hermes_cli/container_boot.py`, `hermes_cli/service_manager.py`,
  `hermes_cli/web_server_cron.py`, `hermes_cli/gateway_windows.py` and the tui
  change watcher's `platforms.changed` signal (file mtime).
- **Pre-existing JSONL transcript fallback**: when the session store cannot
  open, `SessionStore` still falls back to `sessions/*.jsonl` (not added here;
  the routing index itself raises `AuxStoreUnavailable`).
- ~~`tools/bot_live_delivery.py` and `plugins/platforms/a2a/adapter.py` open
  `state.db` only when the file exists (never on an authority pod)~~ — the
  assumption was wrong (an authority pod keeps its pre-switch `state.db`);
  closed by `t_0eeaa6f9`, 6.7. `hermes_cli/observability/shared_metrics.py`
  opens its SQLite only behind the telemetry opt-in.
- Kanban (`HERMES_KANBAN_BACKEND` selects its own backend; see 6.3; turned
  off for v3 by 6.6) and the hosted-room PostgreSQL port (G) are separate work.

### 6.6 kanban master switch

Card `t_fb9c7b9e` (it asks for "§6.5"; 6.5 already lists t_aa3728da's
remaining items, so this is 6.6). Operator decision (eren, 2026-09-29): v3 turns the built-in
kanban off (it is to be replaced later). Base: `levos/pg3` `ea213f19b`. The
existing `kanban.dispatch_in_gateway: false` /
`HERMES_KANBAN_DISPATCH_IN_GATEWAY=0` stops only the dispatcher
(`gateway/kanban_watchers.py:197-209`); on a PVC that already holds
`kanban.db` the notifier and every tui session still open it read-only every
5 s. Live v3 (09-29): `HERMES_KANBAN_BACKEND` unset, zero kanban rows, the
gateway logs `kanban dispatcher: embedded in gateway (interval=60.0s)` and the
PVC has `kanban.db`.

**Runtime entry points into the kanban store** (line numbers: base tree). The
store is reached only through `kanban_db_connect.connect`
(`hermes_cli/kanban_db_connect.py:677`, backend by
`kanban_persistence.resolve_backend` `:209`: argument >
`HERMES_KANBAN_BACKEND` > routed-board resolver > `sqlite`), `kanban_db.list_boards`
(`hermes_cli/kanban_db.py:642`), `kanban_db_path` (`:522`), `kanban_home`
(`:390`) and `kanban_db_notify.count_notify_subs`
(`hermes_cli/kanban_db_notify.py:193`, a `sqlite3.connect(...?mode=ro)` at
`:245` whenever the file exists).

| # | File:line (base) | Entry point | Reaches | Cadence |
|---|---|---|---|---|
| 1 | `gateway/run_startup.py:1291` `_PRE_RECONNECT_WATCHERS`, `:1299` `_start_spawn_background_watchers` | spawns `_kanban_notifier_watcher` and `_kanban_dispatcher_watcher` | #2-#5 | gateway start |
| 2 | `gateway/kanban_watchers.py:60` `_kanban_notifier_watcher` → `gateway/kanban_watchers_notifier.py:272` `_notifier_collect` → `:171` `collect` | notifier tick | `list_boards` (`gateway/kanban_watchers_common.py:39`), `kanban_db_path`, `count_notify_subs` (`:197`), `connect` (`:246`) when a board has subscriptions | every 5 s |
| 3 | `gateway/kanban_watchers.py:111` `_kanban_sub_op` (`_kanban_advance` / `_kanban_unsub` / `_kanban_rewind`) | notifier delivery | `connect` | per delivered event (only from #2) |
| 4 | `gateway/kanban_watchers.py:185` `_kanban_dispatcher_boot` | dispatcher boot | `kanban_home()/kanban/.dispatcher.lock` | gateway start |
| 5 | `gateway/kanban_watchers.py:233` `_kanban_dispatcher_watcher` → `gateway/kanban_watchers_dispatcher.py:189`, `:228` | dispatcher tick | `reap_worker_zombies`, `list_boards`, `connect`, `dispatch_once`, auto-decompose | every `dispatch_interval_seconds` (60) |
| 6 | `gateway/slash_commands.py:337` `_handle_kanban_command` (plain command, `gateway/run_busy.py:760`) | `/kanban` on a messaging platform | `hermes_cli.kanban.run_slash` | per command |
| 7 | `gateway/slash_commands.py:378` `_kanban_auto_subscribe` | `/kanban create` | `connect` (`:398`), `add_notify_sub` | per command (only from #6) |
| 8 | `tui_gateway/server.py:1028` → `tui_gateway/session_notifications.py:546` `_notification_poller_loop` (`:579`) → `:364` `_notif_poll_kanban` → `:336` `_collect_kanban_notifications` → `:299` `_kb_poll_board` | tui session notification poller | `list_boards` (`:351`), `kanban_db_path`, `count_notify_subs` (`:306`), `connect` when subscribed | every 5 s (`:121`) per live session |
| 9 | `hermes_cli/cli_commands_mixin.py:1816` `_handle_kanban_command` (tui `slash.exec` → slash worker → HermesCLI) | `/kanban` in the tui / classic CLI | `run_slash` | per command |
| 10 | `hermes_cli/main_tui_launch.py:824` `_pin_kanban_board_env` | `hermes chat` / tui launch | `get_current_board` (board file / PostgreSQL board list) | per launch |
| 11 | `tools/kanban_tools.py:64` `_visible` (check_fn of the 14 tools registered at `:991`, toolset `kanban`); handlers through `:200` `_board`; `:912` `_maybe_auto_subscribe` | kanban tools | `connect` per call | per tool call, when exposed |
| 12 | `tools/kanban_tools.py:418` `heartbeat_current_worker_from_env`, `:453` `inject_new_comments_from_env` (from `agent/activity_tracking.py:76-80`) | agent activity | `connect` | ≤ 60 s / 6 s, only with `HERMES_KANBAN_TASK` |
| 13 | `agent/turn_finalizer.py:42` `_record_kanban_budget_exhausted`; `cli.py:4013` `_run_kanban_goal_loop_q`, `:4168` `_collect_kanban_task_images` | dispatcher-spawned worker process | `connect` | only with `HERMES_KANBAN_TASK` (a worker the dispatcher spawned) |
| 14 | `plugins/kanban/dashboard/plugin_api.py:84` `_conn`, `:1694` `stream_events` (0.3 s tail, `:1629`) | dashboard kanban plugin (`hermes dashboard`) | `init_db`, `connect` | per HTTP request / open socket |

Only env or path handling, no store access: `gateway/platforms/base.py`
`_kanban_root` (attachment allow-list), `agent/prompt_builder.py` /
`agent/system_prompt.py` / `model_tools.py` (`HERMES_KANBAN_*` env and the
toolset name), `hermes_cli/doctor_platform.py` (reads file headers under
`hermes doctor`), `cron/scheduler.py` (env scrubbing), plugin hook names.

**Switch.** `kanban.enabled` (default `true`, `hermes_cli/config_defaults.py`)
with the env override `HERMES_KANBAN_ENABLED` (`0/false/no/off` turns it off
like `HERMES_KANBAN_DISPATCH_IN_GATEWAY`; `1/true/yes/on` turns it back on
over config; anything else defers to config). One decision function,
`hermes_cli/kanban_switch.py` `kanban_enabled()` / `kanban_disabled_reason()`,
reads only env and config (no `kanban_db` import). On, every path is the base
code.

| # | Off: what happens | Where |
|---|---|---|
| 1-5 | the gateway spawns neither kanban watcher and logs one line `kanban: disabled via config kanban.enabled=false; no dispatcher or notifier in this gateway` (or `via HERMES_KANBAN_ENABLED env`); the watcher, dispatcher boot and notifier tick also return before any store call when called anyway, and a notifier started while on stops reaching boards on its next tick after the switch goes off | `gateway/run_startup.py` `_start_spawn_background_watchers`; `gateway/kanban_watchers.py` `_kanban_notifier_watcher`, `_kanban_dispatcher_boot`; `gateway/kanban_watchers_notifier.py` `_notifier_collect` |
| 6-7 | `/kanban` answers `gateway.kanban.disabled` ("Kanban is turned off in this runtime …", all locales) | `gateway/slash_commands.py` `_handle_kanban_command` |
| 8 | the tui session poller skips its kanban poll; the bot-live, `/loop` and `/heartbeat` polls and the completion queue run as before | `tui_gateway/session_notifications.py` `_collect_kanban_notifications` |
| 9 | `/kanban` in the tui and the classic CLI prints the same notice | `hermes_cli/cli_commands_mixin.py` `_handle_kanban_command` |
| 10 | no board is pinned | `hermes_cli/main_tui_launch.py` `_pin_kanban_board_env` |
| 11 | every `kanban_*` tool's check_fn is False (orchestrator toolset and dispatcher workers alike), so no handler is reachable | `tools/kanban_tools.py` `_visible` |
| 12 | the worker heartbeat and comment bridges return False | `tools/kanban_tools.py` |
| 13 | not gated: only a process the dispatcher spawned (`HERMES_KANBAN_TASK`) reaches these, and an off runtime spawns none | — |
| 14 | gated (`t_0940d7c8`): `hermes dashboard` (v3 also runs `--isolated`) does not mount the kanban plugin API under `/api/plugins/kanban/` and logs one line `Plugin kanban: API not mounted, kanban disabled (…)`, so no request reaches `_conn` / `stream_events`; the tab's static assets stay and its API calls get 404. Other plugins and `plugins.disabled` are unchanged | `hermes_cli/web_server_dashboard.py` `_plugin_api_mount_skip_reason` |

The existing `kanban.dispatch_in_gateway` / `HERMES_KANBAN_DISPATCH_IN_GATEWAY`
keep their meaning while kanban is on. The switch never touches an existing
`kanban.db` (not moved, not deleted, not opened) and adds no fallback.
Re-enabling at runtime reaches the tools, `/kanban` and the tui poller at
once; the gateway watchers need a restart (they are decided at startup).

**Tests.** `tests/gateway/test_kanban_master_switch.py` (21): with a
pre-existing `kanban.db` (subscriptions + a pending event) in
`HERMES_HOME` = `HERMES_KANBAN_HOME` and the switch off, the gateway watcher
spawn, a notifier watcher run and tick, a dispatcher run and one tui poller
iteration make zero `sqlite3.connect` and zero `kanban_db_path` / `connect` /
`count_notify_subs` / `list_boards` / `kanban_home` calls and leave the file
(mtime, size, no `-wal` / `-shm`) unchanged; the tools are hidden, `/kanban`
(gateway, CLI / tui) only says so, the worker bridges no-op, no board is
pinned; env overrides config both ways. Each off-test has an on-twin that
shows the same entry point reaching the store. Reverting any one gated file to
the base tree fails its test. `tests/test_v3_no_file_state_authority.py`: the
pod guard's `off` variant (above, 6.3); on the base tree it records `kanban.db`
connects from the dispatcher (`kanban_db_connect._open_configured`) and the
tui poller (`count_notify_subs`, `?mode=ro`) and leaves `home/kanban`.

`tests/hermes_cli/test_dashboard_kanban_master_switch.py` (row 14): off by env
or config, `_plugin_api_mount_skip_reason` returns `kanban disabled (…)` for
the kanban plugin only; on (default, env `1`) it returns None as before and
`plugins.disabled` still wins. Mounting the plugin APIs on a fresh app over a
`HERMES_HOME` holding a `kanban.db` gives zero `/api/plugins/kanban/` paths in
`/openapi.json`, zero `sqlite3.connect` and an unchanged file off, and the
kanban paths on. On the base tree the off tests fail (the router mounts).
`docs/quality/rule-index.md` is still absent in this repository; the root
`AGENTS.md` rules were followed as for t_fb9c7b9e (§4).

### 6.7 legacy state.db on an authority pod

Card `t_0eeaa6f9`. Base: `levos/pg3` `5f99dca14`. Live (infraops opsi,
2026-09-30 01:45 KST): after opsi-v3 switched to the new core (runtime start
16:45:01Z) the PVC's pre-authority store was still written —
`state.db-wal` 16:49:18Z, `state.db-shm` 16:49:54Z — during the acceptance run
(one new conversation, one resumed session, a memory recall); kanban, the
dashboard kanban API and hosted rooms were off as designed. Every profile moved
to v3 (v2 ones included) keeps its old `state.db` (it is not deleted), so the
§6.1 rows "open `state.db` only when the file exists (never on an authority
pod)" assumed something false. A read-only SQLite open is no exception: on a
WAL database it maps and writes `-shm` and may checkpoint `-wal`.

**Reproduction.** `tests/test_v3_legacy_state_db_untouched.py` runs the §6.3
pod (kanban off, as deployed; the scenario now also closes and resumes the tui
session with its notification poller ticking, and asks the dashboard's
`/api/status` once) on a home that already holds a WAL-mode `state.db` with
three sessions (one titled `Bot Chat`) and live `-wal` / `-shm` sidecars, and
asserts that the three files keep existence, size and mtime and that
`sqlite3.connect` is never called. On the base tree it fails; the recorder's
connects (target, innermost repo frame):

```
file:…/home/state.db?mode=ro   tools/bot_live_delivery.py:39 find_canonical_live_owner      (×3-5: tui_gateway/session_notifications.py:510 _poll_bot_live_delivery_once, per session poller tick)
file:…/home/state.db?mode=ro   hermes_cli/web_routers/status.py:381 _advisory_pressure       (/api/status)
:memory:                       hermes_state_portability.py:138 _compact_session_cols → hermes_state_schema.py:707 _parse_schema_columns   (/api/status → status.py:87 _count_status_active_sessions → list_sessions_rich)
```

The `:memory:` connect is not about the legacy file: the §6.3 guard with the
added `/api/status` step fails on the base tree with that one connect too (both
kanban variants).

**Audit** (base tree; runtime paths that reach `state.db` by path, gated only
by the file existing — the full sweep of `state.db` / `DEFAULT_DB_PATH` /
`SessionDB(db_path=…)` / `hermes_state_registry.acquire(path)` /
`sqlite3.connect` outside tests, evals, scripts and the excluded operator
CLIs). `acquire(path)` never routes to PostgreSQL by itself; every caller below
that is not listed already asks `open_authority_store_for_db_path`,
`home_selects_postgres` / `open_store_for_home`, `aux_store_authority()`,
`resolve_state_backend()` or `SessionDB.open_writer` first, or only stats the
file (`mcp_serve.py`, `tui_gateway/change_watcher.py`).

| File:line (base) | Reached from | Data on authority | Disposition |
|---|---|---|---|
| `tools/bot_live_delivery.py:37-39` `find_canonical_live_owner` | tui session poller (`tui_gateway/session_notifications.py:510`, every idle tick of every session), cron live delivery (`cron/scheduler_delivery.py:697`), bot DM tool (`tools/bot_mode_dm.py`) | Bot Chat session and compression tip are in PostgreSQL | PostgreSQL store (read-only) on authority; the legacy file's `Bot Chat` is not consulted |
| `tools/bot_live_delivery.py:162` `_matches` (via `claim_pending_delivery`) | same | compression lineage in PostgreSQL | same |
| `plugins/platforms/a2a/adapter.py:129` `_state_db` (`:544`, `:562`, `:566`) | a2a forwarding to a local profile (writes the forwarded session's title) | `sessions` in PostgreSQL | the statement runs on the target profile's PostgreSQL store; if that cannot open, `""` + debug log (as before for an unusable file) |
| `hermes_cli/web_routers/status.py:377-383` `_advisory_pressure` | dashboard `/api/status` | FTS5 rebuild progress is SQLite state | skipped on authority (`fts_rebuild` omitted) |
| `hermes_cli/update_cmd_maint.py:157` `_print_fts_optimize_available_notice` | end of `hermes update` (also spawned by the gateway `/update` and the dashboard) | SQLite FTS5 layout | skipped on authority |
| `hermes_state_portability.py:138` `_compact_session_cols` → `hermes_state_schema.py:707` | `/api/status` → `_count_status_active_sessions` → `list_sessions_rich(compact_rows=True)` on PostgreSQL | — (`sqlite3.connect(":memory:")` to parse `SCHEMA_SQL`) | parsed by `hermes_state_pg_columns.declared_schema_columns` (same columns, same order; §6.2 guard) |

**Implementation.** The branch is `home_selects_authority` through the
existing seam, never the file's existence:

- `hermes_state_postgres.open_authority_store_for_db_path(db_path, *,
  read_only=False)` — new keyword; the active home opens `SessionDB(read_only=…)`,
  another home `open_store_for_home(home, read_only=…)`; still None off authority.
- `tools/bot_live_delivery.py`: `_authority_store(home)` (read-only) is used by
  `find_canonical_live_owner` and `_matches`; off authority both keep their
  SQLite `state.db` path unchanged.
- `plugins/platforms/a2a/adapter.py`: `_authority_state_db` runs the same
  statement on the store (`?` placeholders are translated by the PostgreSQL
  connection; writes open read-write and commit).
- `hermes_cli/web_routers/status.py`, `hermes_cli/update_cmd_maint.py`: return
  before the file check on authority.
- `hermes_state_portability.py`: SQLite-free schema parse for the compact
  session projection (both backends; identical result).

No path deletes, moves, renames or creates the legacy file; nothing falls back
to SQLite, `:memory:` or a file.

**Tests** (real PostgreSQL 16.4 and 16.15, `PG3_PERCENT_PG_BIN`):
`tests/test_v3_legacy_state_db_untouched.py` —
`test_authority_pod_leaves_the_legacy_state_db_untouched` (the pod above; the
seeded home also carries a pre-switch Bot Chat mailbox), `test_bot_chat_owner_and_lineage_come_from_postgres_on_authority`
(owner = the PostgreSQL `Bot Chat`, not the legacy file's; a queued envelope is
claimed across a PostgreSQL compression), `test_a2a_forwarded_session_lookup_and_title_use_postgres_on_authority`,
`test_status_and_update_notices_skip_sqlite_fts_on_authority` (legacy file
grown sparse past the notice's 0.5 GB floor). Each runs with `sqlite3.connect`
recorded and refused and asserts the legacy triple unchanged. Reverting the
source files to the base tree fails all four. The §6.3 guard
(`tests/test_v3_no_file_state_authority.py`) gains the resume and
`/api/status` steps and shares its pod through `run_pod`. Off authority the
existing module tests keep the SQLite behaviour
(`tests/tools/test_bot_live_owner_delivery.py`, `tests/plugins/test_a2a_plugin.py`,
`tests/hermes_cli/test_fts_optimize_notice.py`).

No-regression sweep (file by file, no xdist; base and head run one after the
other): `tests/gateway`, `tests/tui_gateway`,
`tests/tools/test_bot_live_owner_delivery.py`,
`tests/hermes_cli/test_web_server*.py` and the four `tests/plugins/test_a2a_*.py`
files — the only failing files on the head are
`tests/gateway/test_shutdown_cache_cleanup.py` (3) and
`tests/gateway/test_shutdown_executor_quiesce.py` (3), which fail identically on
the base tree; the PostgreSQL-backed files there (`test_routing_pg_authority.py`,
`test_submit_idempotency_pg.py`) pass on both with `PG3_PERCENT_PG_BIN` set.

**Remaining** (not runtime, or a different shape):

- `hermes update`'s pre-update snapshot and post-update integrity guard
  (`hermes_cli/update_cmd_maint.py` `_verify_and_restore_one_state_db`,
  `_verify_state_db_after_snapshot`) still check — and on a failed check may
  restore a snapshot over — an existing `state.db` of every home, authority or
  not. They belong to backup / recovery, excluded as in 6.1 (an operator
  update, though the gateway `/update` and the dashboard can spawn it).
- `hermes approvals suggest` (`hermes_cli/approvals_suggest.py`) scans
  `state.db` when it exists (CLI only; its data is in PostgreSQL on authority).
- The dashboard console's `sessions repair` (`hermes_cli/console_engine.py`)
  is the repair CLI (excluded).
- `home_selects_postgres` / `profile_selects_postgres` read only the target
  home's `config.yaml`: a profile on authority only through the process env
  (`HERMES_STATE_BACKEND`), addressed by name or home (api_server named-profile
  branch, `_open_session_db_for_profile(<own name>)`, dashboard profile
  sidebar), is read as SQLite. v3 selects the backend in `config.yaml`, where
  these follow it.
- Each idle tui session's poller resolves the Bot Chat owner every tick
  (0.5 s), so on authority it opens a short read-only PostgreSQL connection per
  tick (about 9 ms against a local socket), where the SQLite pod opened the
  file per tick. Skipping the lookup while the mailbox is empty would need a
  poller change (its tests stub the claim), left for a follow-up.

`docs/quality/rule-index.md` is still absent in this repository; the root
`AGENTS.md` rules were followed (real imports against a temp `HERMES_HOME`, a
real PostgreSQL, behaviour-contract tests red on the base tree, no new
`HERMES_*` env var).
