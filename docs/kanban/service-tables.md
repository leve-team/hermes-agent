# Service-owned kanban tables

`TABLE_KEYS` in `hermes_cli/kanban_persistence.py` is the closed list of the
seven tables the kanban core owns. On SQLite that list never mattered to anyone
else: a host application that put its own tables in the same board file simply
wrote to them, because `sqlite3` has no contract to consult.

On PostgreSQL it does matter. `KanbanPostgresCursor._prepare` resolves every
`INSERT` against the dialect before it reaches psycopg — it has to, because
`INSERT OR IGNORE` and `INSERT OR REPLACE` have no PostgreSQL spelling without
knowing the conflict target, and `lastrowid` has none without knowing whether
the primary key is a generated identity. A table the dialect does not know is
therefore refused rather than guessed at.

That refusal is correct and stays. What was missing is a way for the owner of
the schema to say *this table is also mine*.

## Registration

```python
from hermes_cli.kanban_persistence import set_service_tables

set_service_tables({
    "claim_attempts": {
        "key": ("attempt_id",),
        "identity": False,
        "tenant_scope": "task",
        "tenant_key": "card_id",
    },
})
```

Same lifetime rules as `set_board_dsn_resolver`: process-global, registered at
boot before threads start, `None` clears it, subprocesses register on their
own. It is a declaration made by the process owner, not configuration read from
the environment, and not something a statement can trigger.

Every field is required and validated at registration:

| field | meaning | refused when |
| --- | --- | --- |
| `key` | conflict target for `INSERT OR IGNORE`/`OR REPLACE` | empty, not a sequence, repeats a column, or is not made of SQL identifiers |
| `identity` | is the primary key a generated identity (`RETURNING id` is meaningful) | not exactly `True`/`False` |
| `tenant_scope` | `column` (tenant key on this table) or `task` (ownership reached through `tasks`) | anything else, including omitting it |
| `tenant_key` | the column that carries the scope | not a SQL identifier |

A table name that is already in `TABLE_KEYS` is refused: a registration widens
the contract, it never redefines the core's own keys.

There is no `tenant_scope` value meaning "unknown". Nine boards can share one
schema, so a table nobody can attribute to a board would make the first
cross-board read a silent leak. Not registering it is the safe answer, and it
is the default.

## What registration does not do

* It does not open the contract. `check_table` still refuses every table that
  is neither in `TABLE_KEYS` nor registered, and the refusal message is
  unchanged.
* It does not reach other stores. `PostgresDialect.service_table_keys` is gated
  on `store_name == SERVICE_TABLE_STORE`, so a dialect that reuses this adapter
  under another name (projects) keeps its own closed metadata.
* It does not touch SQLite. `SQLiteDialect` never consulted `TABLE_KEYS` for
  writes and still does not.
* It does not create or migrate anything. The registered tables must already
  exist in the schema the connection opens; registration only says how the
  adapter should write to them.
