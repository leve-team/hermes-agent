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
