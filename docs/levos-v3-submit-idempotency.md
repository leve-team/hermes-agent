# levos v3: client request ids for `prompt.submit` / `session.create`

Card t_2b6c09df. Base: `levos/pg3` (0.21.2). The broker (levos session plane)
relays chat into the core dashboard WebSocket through `prompt.submit` and
`session.create`; while a runtime is being replaced it parks human messages in
PostgreSQL `pending_input` and re-sends them afterwards. Without a client
request id the core cannot tell a re-send from a new message.

## 1. Current flow and where an acceptance record can go

Line numbers are `levos/pg3` at `cb8af8b1ee`, before this change.

### `prompt.submit` (`tui_gateway/methods_prompt.py:544`)

1. `_typed_stop_phrase_response` (voice stop phrase) and `_sess_nowait`
   (`tui_gateway/server.py:1180`): the runtime `session_id` must be live in
   this process's `_sessions`, else 4001.
2. Hosted-room proof / legacy group fence, then
   `_ensure_active_session_slot` (`methods_prompt.py:577`, 4090).
3. Transport re-bind under `_session_resume_lock`.
4. Busy loop (`methods_prompt.py:611-620`): a running session hands the text
   to `_handle_busy_submit` (`tui_gateway/session_auto_continue.py:240`):
   steer / redirect / in-memory queue, reply `{"status": "queued"|...}`.
   Nothing about that text is durable until the live turn picks it up.
5. `_lock_in_submit_turn` (`methods_prompt.py:511`): under `history_lock`,
   optional truncation (`replace_messages`, which re-inserts survivor rows
   with new ids), then `running=True` and `_start_inflight_turn`
   (in-memory only). Note the busy check in 4 and this lock are separate
   acquisitions, so two concurrent idle submits both reach this step today.
6. `_persist_session_row_for_submit` (`methods_prompt.py:441`) →
   `session_workdir._ensure_session_db_row` (`tui_gateway/session_workdir.py:241`):
   the `sessions` row is written here, lazily, on the first prompt.
7. Agent build is started, then a thread runs `_run_after_agent_ready`
   (`methods_prompt.py:474`); the RPC returns `{"status": "streaming"}`
   (`methods_prompt.py:662`) **before** anything of the message is stored.
8. The thread waits for the agent build, then `_run_prompt_submit`
   (`tui_gateway/prompt_turn.py:791`) starts a second thread (`:898`) that
   writes the crash marker (`_record_turn_marker`, a file in the profile
   home) and calls the agent (`_invoke_agent`, `:837`). The user message row
   is written by the agent's own session flush
   (`agent/turn_context.py:928-930` stages it,
   `agent/session_persistence.py:338` `_flush_messages_to_session_db`
   writes it), i.e. seconds after the RPC answered, after the agent build.

### `session.create` (`tui_gateway/methods_session.py:324`)

A fresh runtime sid (`_new_runtime_ids`, 8 hex) and stored key
(`_new_session_key`) are minted, the record goes into `_sessions`
(`:341`), and — unless the request is seeded — **no `sessions` row is
written** (`:366-381`): the row appears at step 6 of the first
`prompt.submit`. The reply carries `session_id` (runtime) and
`stored_session_id` (key).

### Windows

For `prompt.submit` the only durable trace of an accepted message is the
user row written at step 8. Between the RPC reply (step 7) and that write
a process death loses the message while the broker already dropped it from
`pending_input`; a lost reply makes the broker re-send it and step 8 then
writes it twice.

Where the acceptance record is written decides which window remains:

* **After the turn starts / after the reply** ("accepted but no record"):
  a death between thread start and the record leaves a turn the broker was
  told about with nothing to deduplicate against. Rejected.
* **Before the turn thread starts, committed before the reply, carrying a
  live-owner lease** (chosen): the remaining window is "record but no
  message" — the process died after committing the record and before the
  agent flushed the user row. The record says `accepted`, carries a
  message-id watermark (the highest `messages.id` of the session lineage at
  acceptance) and an owner lease; once the lease has lapsed and no user
  row above the watermark exists, the retry re-runs the turn exactly once
  (the claim is serialized by a PostgreSQL advisory transaction lock).
* The claim sits after the busy check (step 4) and before
  `_lock_in_submit_turn` (step 5). A busy session is refused (not queued)
  for id-carrying submits because steer / redirect / the queue are
  in-memory and cannot be made durable here; truncation is refused with an
  id because it re-inserts survivor rows above the watermark.

For `session.create` the window is "create answered but the id exists
nowhere": the record (profile, `client_create_id`) → (runtime sid, stored
key) is committed in the same advisory-locked transaction as the
in-memory registration, so a retry after a pod replacement finds the ids
and either resumes the stored row or re-creates the draft under the same
ids.

## 2. Contract

Only on a PostgreSQL-authority profile (`sessions.state_backend: authority`,
`hermes_aux_store.aux_store_authority()`). Off authority (SQLite, `probe`)
both parameters are accepted and ignored — not even validated — and every
request behaves exactly as one without them. On authority an unreachable
store is refused whole with **5140** (`data.reason = "aux_store_unavailable"`,
`data.exception = "AuxStoreUnavailable"`, `retryable: true`): no turn, no
session, no SQLite or file fallback.

Storage: the authority-only aux store `submit_idempotency`
(`hermes_aux_store.AUX_STORES`), created lazily on first open under the
store's schema advisory lock like every other aux store:

| table | key | columns |
|---|---|---|
| `core_submit_accepts` | (`session_key`, `client_msg_id`) | `fingerprint` (sha256 of the submitted text; structured payloads: canonical JSON), `state`, `ui_session_id`, `watermark`, `user_message_id`, `owner`, `lease_until`, `attempts`, `accepted_at`, `updated_at`, `completed_at` |
| `core_submit_session_creates` | (`profile`, `client_create_id`) | `session_id` (runtime), `session_key` (stored), `created_at`, `updated_at` |

Times are the PostgreSQL server clock. Retention: records untouched for 7
days are deleted by a prune that runs at most hourly on writes (the
prune-on-write of the other idempotency ledgers); a live record's heartbeat
keeps moving `updated_at`.

### `prompt.submit` + `client_msg_id` (string, 1–128 chars)

* The key is the session's **stored** key, and a lookup also matches records
  of its compression ancestors, so a key rotated by compression still finds
  the record.
* The claim and the in-process turn lock-in (`_lock_in_submit_turn`) run in
  **one** transaction under `pg_advisory_xact_lock(submit:<lineage root>,<id>)`,
  committed before the thread starts and before the reply.
* Replies:

| situation | reply |
|---|---|
| new id | `{"status": "streaming", "client_msg_id", "state": "accepted", "running": true, "stored_session_id", "attempts": 1, ...}` |
| same id, same text, record `persisted`/`completed`, or its owner is live | `{"status": "duplicate", "state", "running", "stored_session_id", "user_message_id", "accepted_at", "completed_at", "attempts"}` — no new turn |
| same id, other text | error **4141**, `data.reason = "client_msg_id_conflict"` + the record |
| `accepted`, no user row above the watermark, no live owner | the turn runs again: `{"status": "streaming", ..., "attempts": n+1}` |
| id malformed | error **4140**, `data.reason = "invalid_client_id"` |
| session running (another turn) | error **4143**, `data.reason = "session_busy"`, `retryable: true`; nothing recorded |
| with truncation params / compute-host isolation | error **4142**, `data.reason = "client_msg_id_unsupported"`, `data.with` |

"Live owner": the owner is this process and the turn is in its registry, or
another process whose lease (`lease_until`, 60 s, refreshed every 20 s by a
per-process heartbeat while its turn runs) has not lapsed.

### State transitions

| from | event | to |
|---|---|---|
| — | claim (new id) | `accepted`, owner = this process, lease running |
| `accepted` | a `user` row above the watermark appears in the session lineage (seen by any read: duplicate, `prompt.accepted`, settle) | `persisted`, `user_message_id` set |
| `accepted`/`persisted` | the owner's turn thread ends and the user row exists | `completed`, owner cleared |
| `accepted` | the owner's turn thread ends without a user row (agent init failed, cancelled before start) | `accepted`, owner cleared → the next retry restarts |
| `accepted` | owner died (lease lapsed), no user row | unchanged until a retry: restart, `attempts + 1` |
| `persisted` | owner died | stays `persisted` (never re-run: the message is stored) |
| `accepted` (fresh) | the turn could not start after the claim (session row write failed) | record deleted |

### `session.create` + `client_create_id` (string, 1–128 chars)

Key: (`profile` param or the launch profile, `client_create_id`). The record
and the in-memory registration commit together under
`pg_advisory_xact_lock(session-create:<profile>,<id>)`. The reply is
session.create's (or session.resume's) plus `client_create_id`,
`stored_session_id` and `idempotency`:

| `idempotency` | when | ids |
|---|---|---|
| `created` | first request | new |
| `duplicate` | the runtime session is live in this process | same `session_id` + `stored_session_id` (via session.resume's reuse-live path: this caller's transport is attached) |
| `resumed` | not live here, a stored `sessions` row exists (a turn already ran) | same stored key; resumed like `session.resume` under the recorded runtime `session_id` when that id is free (else a new one, and the record follows it) |
| `recreated` | not live here and no stored row (a draft whose process is gone before its first turn) | same `session_id` + `stored_session_id`, re-created from this request's params |

### `prompt.accepted` (new, pool-dispatched)

Params: `client_msg_id` and/or `client_create_id`; to scope a message id,
`session_id` (runtime, if live) or `stored_session_id` / `session_key`
(without them the most recent record of that id is returned). Reply:
`{"enabled": true, "found": bool, "submit": <record>|null, "create":
{"session_id", "stored_session_id", "profile", "client_create_id",
"created_at", "updated_at", "live"}|null}`; the read also promotes
`accepted` → `persisted` when the row exists. Off authority:
`{"enabled": false, "found": false}`.

## 3. Broker order (levos session plane)

For each parked message (one stable `client_msg_id` per message, kept in
`pending_input` with the message; one stable `client_create_id` per
conversation it creates):

1. Session: `session.create` with the same `client_create_id` every time
   (it is safe to call on every reconnect). Use the returned `session_id`
   for this connection; keep `stored_session_id`.
2. Before re-sending: `prompt.accepted {client_msg_id, stored_session_id}`.
   * `submit.state` `persisted`/`completed` → delivered; drop it from
     `pending_input`.
   * `submit.running` → the owner is still working (or its lease has not
     lapsed yet); wait and ask again (≥ 60 s covers a dead owner's lease).
   * `found: false`, or `accepted` and not running → go to 3.
3. `prompt.submit {session_id, text, client_msg_id}` with the **same text
   and id**. `streaming` → wait for `message.complete`; `duplicate` → act on
   its `state` as in 2; 4143 → retry after the current turn; 4141 → a
   broker bug (two texts under one id), do not retry; 5140 → the store is
   down, keep the message parked.
4. Drop the message from `pending_input` only after `persisted`/`completed`
   was observed (reply or `prompt.accepted`).

## 4. Remaining limits

* **Busy sessions are refused, not queued**, for id-carrying submits:
  steer/redirect/queue live in memory. The broker re-sends after the turn.
* **"Accepted, no row, lease running"**: after an owner dies, its record
  reads `running` until the 60 s lease lapses; only then does a retry
  restart the turn.
* The user row is identified as the **first `user` row above the
  watermark** in the session lineage. A different writer adding a `user` row
  to the same session between the claim and the agent's flush (another
  client typing into the same session on another pod) would be taken for
  this submit's row.
* Truncation (rewind/edit) and compute-host isolated turns do not take an
  id (4142).
* The fingerprint is over the submitted text only; attachments staged with
  `image.attach*` before the submit are not part of it.
* `prompt.submit` and `session.create` stay inline RPCs, so an id-carrying
  request pays its PostgreSQL round trips on the socket reader thread; a
  `resumed` create also reads the transcript there.
* `recreated` uses the retry's params; a broker must re-send the same
  create params (title, cwd, model) with the same `client_create_id`.
* Two pods serving one profile at once can each hold a live copy of a
  replayed session; the ids and the PostgreSQL rows stay single.
