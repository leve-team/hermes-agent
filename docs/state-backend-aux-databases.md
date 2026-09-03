# Auxiliary SQLite databases outside `state.db` — PostgreSQL migration strategy

Scope of the PostgreSQL state backend (`sessions.state_backend: postgres`) is
`state.db` only: sessions, messages, compression locks, turn leases, FTS. The
profile directory holds six more SQLite files that the backend and
`migrate_state_to_postgres.py` never touch (P0 gap inventory §5e). This
document fixes what happens to each one, measured from the code that owns it.

Measurement basis (2026-09-03, `levos/pg3`): owner module found with
`grep -rn "<file>" --include=*.py` excluding `tests/`; access pattern read
from the owner; live observation from the session-plane pod (dave, 09-03):
the only SQLite files with open handles at the sampled instant were
`state.db` and its WAL — the six files below existed (12 KB – 1 MB) but were
not open.

## Classification

| File (relative to `HERMES_HOME`) | Owner module | What it holds | Access | Class | Disposition |
|---|---|---|---|---|---|
| `cron/executions.db` | `cron/executions.py` (consumers: `cron/scheduler.py`, `cron/scheduler_provider.py`, `cron/jobs.py`, `hermes_cli/cron.py`) | Audit ledger of cron attempts (`claimed/running/completed/failed/unknown`), capped at 1000 terminal rows. Rows carry the local `pid` + process start time; dead-owner recovery asks the **local** kernel. Docstring: "it is not a retry queue". | Written on every cron dispatch/finish; read by the dashboard and the scheduler's claim sweep. | **regenerable** | `HERMES_AUX_DB_DIR`-relocatable. A fresh pod starts an empty ledger; the in-memory claim table is the primary guard and the PID semantics are host-local anyway. |
| `cron/notepad.db` | `cron/notepad.py` (cleared by `cron/jobs.py` on job delete) | Per-job KV state carried across wake-ups: cursors, watermarks, watchlists (≤ 64 KB per job). Written only through `hermes cron notepad … set`. | Rare writes, prompt-injected on every run of the job. | **authoritative** | Stays on durable storage. Loss silently resets every job's cursor. Not relocated by this change; PG migration is a follow-up card. |
| `projects.db` | `hermes_cli/projects_db.py` (consumers: desktop grouping, kanban worktrees, `hermes_cli/backup.py` lists it as a user-created store) | User-created Project entities (name, folders, board binding). | Low frequency, user-driven. | **authoritative** | Stays on durable storage. Not relocated. PG migration is a follow-up card. |
| `response_store.db` | `gateway/platforms/api_server.py::ResponseStore` | LRU cache (`MAX_STORED_RESPONSES`) of Responses-API state keyed by `response_id`, so `previous_response_id` can be honoured. The class already falls back to `:memory:` when the file cannot be opened. | Hot only while the OpenAI-compatible API server is in use. | **regenerable** (cache) | `HERMES_AUX_DB_DIR`-relocatable. Loss = clients lose `previous_response_id` continuity across a pod replacement, the same outcome the existing `:memory:` fallback already accepts. |
| `verification_evidence.db` | `agent/verification_evidence.py` | Passive audit trail of what the coding agent proved (test/lint commands + outcome); 30-day retention, per-root and total caps. Docstring: "never blocks completion". | Written after qualifying terminal commands; read to summarise evidence. | **regenerable** | `HERMES_AUX_DB_DIR`-relocatable. |
| `<root>/kanban.db` (+ `kanban/boards/<slug>/kanban.db`) | `hermes_cli/kanban_db.py` (root-anchored, shared across profiles) | Kanban board. | — | **out of scope** here | Owned by the levos guild server through its own API. Already relocatable through the existing `HERMES_KANBAN_HOME`. Untouched. |

`memory_store.db` (holographic memory facts) is also listed by `backup.py`
but was not part of the inventory; it is authoritative and untouched.

## `HERMES_AUX_DB_DIR`

`hermes_constants.aux_db_path(relative)` resolves the three regenerable
files:

* unset / empty (default): `<HERMES_HOME>/<relative>` — byte-identical to the
  previous behaviour;
* set: `<HERMES_AUX_DB_DIR>/<relative>` (`~` expanded). Owners create the
  parent directory on first open, so pointing it at an empty `emptyDir` works.

The directory is not profile-qualified. One process serves one profile in
the intended deployment (one pod per profile); a host that runs several
profiles must give each process its own value or leave the variable unset.

Authoritative files (`cron/notepad.db`, `projects.db`) deliberately ignore
the variable; a follow-up card decides whether they move to PostgreSQL or to a
durable volume. `hermes_cli/backup.py` still looks for the regenerable files
under `HERMES_HOME`; a relocated copy is simply absent from backups, which is
acceptable for cache/audit data and is the reason the knob is limited to that
class.

## Follow-ups (not in this change)

* PG (or durable-volume) strategy for `cron/notepad.db` and `projects.db`.
* Whether `backup.py` should follow `HERMES_AUX_DB_DIR` for completeness.
