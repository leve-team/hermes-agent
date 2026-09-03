# Message search contract

This contract is the backend-neutral surface of `SessionDB.search_messages`.
SQLite FTS5 and PostgreSQL use different tokenizers and rankers, so parity means
query semantics and recall, not identical ordering or snippets.

## Indexed document

Each message is one searchable document made from the same three nullable
fields, separated by spaces:

1. `content`
2. `tool_name`
3. `tool_calls`

The canonical message row is never shortened to satisfy an index limit. An
index implementation may store a bounded prefix of this derived document as
long as it records the affected message id and byte counts.

## Query language

| Form | Contract | SQLite FTS5 | PostgreSQL |
|---|---|---|---|
| `deploy` | Match a case-folded word token. Adjacent terms are implicit AND. | `messages_fts MATCH` | `to_tsvector('simple') @@ websearch_to_tsquery('simple')` |
| `deploy*` | Match word tokens beginning with `deploy`. A suffix `*` is special only on an unquoted word token. | native FTS5 prefix | placeholder-preserved `websearch_to_tsquery`, then `deploy:*` |
| `"blue green"` | Match the two tokens adjacent and in order. | native FTS5 phrase | native web-search phrase (`<->`) |
| `blue OR green` | Match either operand. Upper-case `OR` is the portable spelling; implicit AND binds within each arm. | native FTS5 boolean | native web-search boolean (`|`) |
| `blue -green` | Require `blue` and exclude `green`. A negative-only query is outside this contract. | adapter-equivalent `blue NOT green` | native web-search negative (`!`) |

The public spelling of exclusion is `-term`. SQLite FTS5 has no unary-minus
operator (it interprets it as a column expression), so parity tools translate
that spelling to FTS5's binary `NOT` without changing the SQLite index or its
tokenizer. Existing direct SQLite callers may continue to use `NOT`.

Balanced phrases, prefix terms, OR arms, and exclusions may be combined. An
invalid or token-empty query returns no rows after both native parser attempts;
it must not broaden into an unfiltered scan.

### Substring/trigram route

SQLite routes eligible CJK substring searches to `messages_fts_trigram` (or its
documented fallback). PostgreSQL expresses the corresponding substring
predicate with parameter-bound `ILIKE` over all three indexed fields. The
`pg_trgm` GIN indexes accelerate those predicates when the optional extension
is installed. If it is unavailable, the same `ILIKE` result contract remains
available through a sequential scan; startup and search do not fail merely
because `pg_trgm` is absent.

## Filters and visibility

Filters are applied after the text predicate and use bound values:

- `source_filter=[...]` includes only sessions whose `source` is in the list.
- `exclude_sources=[...]` excludes those session sources.
- `role_filter=[...]` includes only matching message roles.
- by default, live rows (`active=1`) and compaction history
  (`compacted=1`) are visible; rewind-only rows are hidden.
- `include_inactive=True` removes that visibility predicate.

Filter lists in this contract are non-empty. The caller-facing session-search
tool validates and bounds its own inputs.

## Bounds and ordering

`limit` is an upper bound on returned rows (default 20), and `offset` is applied
after filtering and ordering. The storage API does not impose a smaller global
cap because internal discovery deliberately asks for a wider candidate set.

- default: backend-native relevance first; ties and exact cross-backend order
  are intentionally unspecified.
- `sort="newest"`: timestamp descending, then backend relevance.
- `sort="oldest"`: timestamp ascending, then backend relevance.

Snippets are bounded previews and may differ in highlighting. Context contains
at most the preceding message, the matching message, and the following message
from the same session.

## Migration window

PostgreSQL does not demote the whole table to `ILIKE` when one legacy row has a
NULL `fts_content`. Indexed rows keep using `tsvector`; only NULL rows use the
equivalent parameter-bound `ILIKE` auxiliary predicate. Results from both arms
are filtered, ordered, and bounded together. This preserves old-row recall
without turning one incomplete row into a full-table substring search.

Backend acceptance is measured with recall@20. SQLite top-20 message ids are
the reference set; PostgreSQL ordering may differ, so exact rank equality is
not a requirement.
