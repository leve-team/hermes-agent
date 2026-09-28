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
