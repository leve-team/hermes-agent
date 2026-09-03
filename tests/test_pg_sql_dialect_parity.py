"""Static SQLite-dialect gate for the PostgreSQL backend.

Companion to ``test_pg_schema_parity.py``: that file keeps *columns* from
drifting between the two backends; this one keeps *SQL dialect* from drifting.

Background
----------
``hermes_state.py`` writes SQLite-flavoured SQL and
``hermes_state_postgres._translate_sql`` rewrites the closed set of idioms that
PostgreSQL spells differently. When a new call site introduces an idiom the
translator does not know, the statement is passed through verbatim and
PostgreSQL rejects it at *parse* time::

    function json_type(text, unknown) does not exist

That failure is not contained to the expression. It aborts the whole statement,
including the plain ``INSERT`` arm of an upsert whose ``ON CONFLICT`` clause was
the only thing referencing the unknown function — so ``create_session`` never
inserts a row, every subsequent ``append_message`` violates
``messages_session_id_fkey``, and the turn dies with
``reason=session_persistence_failed``. The failure is also quiet: every layer
catches and logs at WARNING so that an accounting loss cannot kill a turn, so
it can recur indefinitely without surfacing to the operator.

Why this file reads source text (a deliberate, narrow exception)
----------------------------------------------------------------
The project's testing guidance says not to write tests that read source code,
because such tests pass when the implementation is subtly broken and fail on
correct refactors. That reasoning is sound and this file is a deliberate,
argued exception to it rather than an oversight — so the case is made here
rather than left for a reviewer to reconstruct.

The behavioural oracle for this class of bug is
``tests/test_pg_parity_smoke.py``, which runs real SQL against a real
PostgreSQL container. It would catch every defect guarded here. But it needs
``testcontainers`` plus a reachable Docker daemon, and it skips cleanly when
either is missing — so in a pipeline without Docker it reports ``skipped`` on
every run and has never executed. A guard that silently skips is worse than no
guard: it reports green and nobody looks.

Two properties keep this file away from the failure mode the rule targets:

* It does not assert on source *text*. Source is used only to LOCATE the SQL
  the state layer emits; every assertion then feeds that SQL through the real
  ``_translate_sql`` or executes it against a real in-memory SQLite database
  and asserts on the RESULT. A subtly broken translator fails here.
* What it guards is invisible to any test that does not reach PostgreSQL. Each
  defect below (untranslated JSON function, bare ``?`` in an ``IS NULL``
  predicate, ambiguous ``ON CONFLICT DO UPDATE`` right-hand side) is valid
  SQLite and only fails against PostgreSQL, so the default-backend suite
  cannot see it.

The cost is real: extracting SQL from source by anchor means a large enough
refactor of these call sites requires updating the anchors here. That is
accepted in exchange for an always-on guard against a class of bug that
otherwise reaches production silently.
"""

import pathlib
import re

import pytest

from hermes_state_postgres import _SQLITE_JSON_FNS, _STRICT_FORBIDDEN, _translate_sql

REPO = pathlib.Path(__file__).resolve().parent.parent

# The modules that emit SQL through the backend-agnostic connection wrapper.
SQL_EMITTING_MODULES = (
    "hermes_state.py",
    "hermes_state_schema.py",
    "hermes_state_common.py",
    "hermes_state_search.py",
)


def _iter_sql_calls(name: str, text: str):
    """Yield each ``name(...)`` call found in *text* (source or SQL)."""
    for m in re.finditer(rf"\b{name}\s*\(", text, flags=re.IGNORECASE):
        yield m.start()


def _joined_string_literals(source: str) -> str:
    """Concatenate adjacent Python string literals so SQL reads contiguously.

    Call sites build SQL by implicit adjacent-literal concatenation::

        "AND json_extract(COALESCE(child.model_config, '{}'), "
        "                 '$._reset_from') IS NULL "

    Read from raw source, that single ``json_extract(...)`` call spans a
    quote-newline-quote boundary and looks unterminated — the translator (which
    only ever sees the runtime-joined string) would be judged against text it
    never receives. Splice the literals back together first so the gate
    analyses what actually reaches PostgreSQL, not how it was typed.
    """
    return re.sub(r"\"\s*\n\s*\"", "", re.sub(r"'\s*\n\s*'", "", source))


def _balanced_call(source: str, idx: int) -> str | None:
    """Return the full ``fn(...)`` call starting at *idx*, or None if unbalanced.

    A fixed-width window is the wrong instrument here: ``json_set(COALESCE(...),
    '$.k', <expr>)`` spanning more characters than the window gets truncated
    mid-call, the translator can't match the shape, and the gate reports a
    false positive on code that is actually fine. Slicing to the call's own
    balanced close-paren makes the detector's scope match the thing it judges.
    """
    open_idx = source.find("(", idx)
    if open_idx == -1:
        return None
    depth = 0
    for i in range(open_idx, min(len(source), open_idx + 2000)):
        if source[i] == "(":
            depth += 1
        elif source[i] == ")":
            depth -= 1
            if depth == 0:
                return source[idx : i + 1]
    return None


def test_every_sqlite_json_call_in_the_tree_is_translatable():
    """No SQL-emitting module may contain a JSON call the translator drops.

    This is the class-level gate. It does not enumerate the four functions that
    happened to break first — it asserts that *every* occurrence
    of *every* SQLite JSON function in the SQL-emitting modules survives
    ``_translate_sql`` with no SQLite spelling left behind.
    """
    offenders = []
    for module in SQL_EMITTING_MODULES:
        path = REPO / module
        if not path.exists():  # pragma: no cover - tree layout guard
            continue
        source = _joined_string_literals(path.read_text(encoding="utf-8"))
        for fn in _SQLITE_JSON_FNS:
            for idx in _iter_sql_calls(fn, source):
                # Slice from the call to its own balanced close-paren rather
                # than a fixed window: a fixed window can cut a long call in
                # half and report a translatable shape as untranslated.
                fragment = _balanced_call(source, idx)
                if fragment is None:
                    continue
                if f"{fn}(" in _translate_sql(fragment).lower().replace(" ", ""):
                    line = source[:idx].count("\n") + 1
                    offenders.append(f"{module}:~{line} {fragment[:70]}")

    assert not offenders, (
        "SQLite JSON idioms that _translate_sql leaves untranslated — these "
        "abort the entire statement on PostgreSQL at parse time:\n  "
        + "\n  ".join(offenders)
        + "\nAdd the shape to _translate_json_call, or rewrite the call site."
    )


def test_translator_never_emits_a_bare_question_mark():
    """A translation that emits ``?`` is silently corrupted into ``%s``.

    ``_translate_sql`` performs ``translated.replace("?", "%s")`` as its final
    step, so the jsonb existence operators (``?``, ``?|``, ``?&``) cannot be
    used in any rewrite — they would become paramstyle placeholders and the
    statement would fail with an argument-count error far from its cause.
    ``jsonb_exists()`` is the function-form equivalent.
    """
    samples = [
        "SELECT json_type(model_config, '$._reset_from') FROM sessions",
        "SELECT json_extract(model_config, '$._reset_from') FROM sessions",
        "UPDATE sessions SET model_config = "
        "json_set(COALESCE(model_config, '{}'), '$._k', parent_session_id)",
        "SELECT json_remove(model_config, '$._k') FROM sessions",
        "SELECT json_valid(model_config) FROM sessions",
    ]
    for sql in samples:
        out = _translate_sql(sql)
        assert "%s" not in out, (
            f"translation of {sql!r} produced a paramstyle placeholder from an "
            f"emitted '?': {out!r} — use jsonb_exists(), never the ? operator"
        )


@pytest.mark.parametrize("fn", _SQLITE_JSON_FNS)
def test_strict_mode_forbids_every_known_sqlite_json_function(fn):
    """Strict mode must name each function, so an untranslated one fails loudly.

    Without this the strict-mode allowlist and the translator's coverage can
    drift: a function absent from ``_STRICT_FORBIDDEN`` passes strict mode
    untranslated and reaches PostgreSQL as invalid SQL.
    """
    assert fn in _STRICT_FORBIDDEN, (
        f"{fn} is translated but not listed in _STRICT_FORBIDDEN; strict mode "
        f"would not catch an untranslated occurrence"
    )


def test_no_ifnull_in_sql_emitting_modules():
    """``IFNULL`` is SQLite-only; the portable spelling is ``COALESCE``.

    The levos session-plane hotfix introduced ``IFNULL(display_only, 0) = 0``
    into ``get_messages_as_conversation`` (the ``/history`` and ``/context``
    read path), which PostgreSQL rejects at parse time. The translator does
    not rewrite it, so the call sites themselves must not emit it. Scanned
    modules: every SQL-emitting SessionDB module plus tui_gateway/server.py,
    which the hotfix also touched.
    """
    offenders = []
    for module in (*SQL_EMITTING_MODULES, "tui_gateway/server.py"):
        path = REPO / module
        if not path.exists():  # pragma: no cover - tree layout guard
            continue
        source = path.read_text(encoding="utf-8")
        for m in re.finditer(r"\bIFNULL\s*\(", source, flags=re.IGNORECASE):
            line = source[: m.start()].count("\n") + 1
            offenders.append(f"{module}:{line}")
    assert not offenders, (
        "SQLite-only IFNULL( in SQL-emitting code — use COALESCE(: "
        + ", ".join(offenders)
    )


def test_strict_mode_forbids_ifnull(monkeypatch):
    """Strict mode names ``ifnull(`` so a regression fails in the parity suite."""
    assert "ifnull(" in _STRICT_FORBIDDEN
    monkeypatch.setenv("HERMES_PG_ADAPTER_STRICT", "1")
    with pytest.raises(RuntimeError, match="ifnull"):
        _translate_sql("SELECT 1 FROM messages WHERE IFNULL(display_only, 0) = 0")
    # The portable spelling passes through untouched.
    out = _translate_sql("SELECT 1 FROM messages WHERE COALESCE(display_only, 0) = 0")
    assert "COALESCE(display_only, 0) = 0" in out


def test_known_call_sites_translate_to_postgres_spelling():
    """The four known-divergent shapes, pinned by their translations."""
    cases = {
        "SELECT json_type(sessions.model_config, '$._reset_from') IS NOT NULL":
            "jsonb_typeof",
        "SELECT json_remove(model_config, '$.browser_model_lock')": "::text",
        "UPDATE sessions SET model_config = json_set("
        "COALESCE(model_config, '{}'), '$._delegate_from', parent_session_id)":
            "jsonb_set",
        "SELECT json_valid(model_config)": "IS JSON",
    }
    for sql, expected in cases.items():
        out = _translate_sql(sql)
        assert expected in out, f"{sql!r} -> {out!r} (expected {expected!r})"
        for fn in _SQLITE_JSON_FNS:
            assert f"{fn}(" not in out.lower(), (
                f"{sql!r} still contains SQLite {fn}( after translation: {out!r}"
            )


def test_token_accounting_sql_has_no_bare_placeholder_predicate():
    """``WHEN ? IS NULL`` gives PostgreSQL nothing to infer a parameter type from.

    PostgreSQL resolves parameter types at prepare time; a placeholder alone in
    an ``IS NULL`` predicate has no anchor, so the prepare fails with
    ``could not determine data type of parameter $N`` before any row is touched.
    SQLite infers it from the bound value at runtime, so this shape is invisible
    on the default backend. Every occurrence loses the entire per-call
    accounting write.
    """
    source = (REPO / "hermes_state.py").read_text(encoding="utf-8")
    hits = [
        source[:m.start()].count("\n") + 1
        for m in re.finditer(r"WHEN\s+\?\s+IS\s+(?:NOT\s+)?NULL", source)
    ]
    assert not hits, (
        "bare '? IS NULL' predicate(s) at hermes_state.py line(s) "
        f"{hits} — PostgreSQL cannot infer the parameter type. Use "
        "COALESCE(?, <column>) or CAST(? AS <type>) IS NULL instead."
    )


def test_update_token_counts_placeholders_match_params():
    """Both branches must bind exactly as many params as they have placeholders.

    The ``$7`` fix removed a placeholder from each branch; the params tuple had
    ``actual_cost_usd`` twice to feed it. Dropping one without the other shifts
    every later parameter by one — ``cost_status`` into ``pricing_version`` and
    so on — which is worse than the bug being fixed and would not raise on
    SQLite.
    """
    source = (REPO / "hermes_state.py").read_text(encoding="utf-8")
    start = source.index("def update_token_counts(")
    end = source.index("def _record_model_usage(", start)
    body = source[start:end]

    branches = re.findall(r'sql = """(.*?)"""', body, flags=re.DOTALL)
    assert len(branches) == 2, f"expected 2 SQL branches, found {len(branches)}"

    params_match = re.search(r"params = \((.*?)\n        \)", body, flags=re.DOTALL)
    assert params_match, "params tuple not found in update_token_counts"
    n_params = len([
        line for line in params_match.group(1).splitlines() if line.strip()
    ])

    for label, sql in zip(("absolute", "delta"), branches):
        assert sql.count("?") == n_params, (
            f"update_token_counts {label} branch binds {n_params} params but "
            f"has {sql.count('?')} placeholders"
        )


# --- ON CONFLICT DO UPDATE ambiguity ----------------------------------------
#
# PostgreSQL rejects a bare column on the RHS of DO UPDATE SET: the name is
# ambiguous between the target row and `excluded`. SQLite resolves it to the
# target, so the statement is valid there and the divergence is invisible
# until it runs against Postgres.
#
# The failure surfaces as:
#     async token accounting: apply failed: column reference
#     "api_call_count" is ambiguous
#
# Qualifying with the table name (`session_model_usage.col`) is valid on BOTH
# backends -- verified on real PostgreSQL 16 and SQLite.
#
# Two of the ten affected columns wrap the bare reference in a function:
#     cost_status = COALESCE(excluded.cost_status, cost_status)
# so the split must respect parentheses. A naive comma split hides them.

_STATE_MODULES = (
    "hermes_state.py",
    "hermes_state_search.py",
    "hermes_state_schema.py",
)


def _split_set_assignments(text):
    """Split a SET clause on top-level commas only (not inside parens)."""
    parts, depth, buf = [], 0, []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append("".join(buf))
    return parts


def _bare_rhs_columns(assignments):
    """Column names referenced bare (unqualified) on the RHS of an assignment."""
    hits = []
    for chunk in _split_set_assignments(assignments):
        if "=" not in chunk:
            continue
        lhs, _, rhs = chunk.partition("=")
        stripped = lhs.strip().strip('"')
        if not stripped:
            continue
        col = stripped.split()[-1]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
            continue
        probe = re.sub(r"\b\w+\.\w+", "", rhs)
        probe = re.sub(r"'[^']*'", "", probe)
        if re.search(rf"\b{re.escape(col)}\b", probe):
            hits.append(col)
    return hits


@pytest.mark.parametrize("module", _STATE_MODULES)
def test_on_conflict_do_update_has_no_bare_column_rhs(module):
    """No DO UPDATE SET may reference a column bare on the RHS.

    Qualify with the table name so the statement is unambiguous on Postgres
    and unchanged on SQLite.
    """
    path = REPO / module
    if not path.exists():
        pytest.skip(f"{module} not present")

    source = _joined_string_literals(path.read_text(encoding="utf-8"))
    findings = []
    for m in re.finditer(r"DO\s+UPDATE\s+SET\b", source, flags=re.I):
        tail = source[m.end():m.end() + 2000]
        stop = len(tail)
        for pattern in (r'"""', r'"', r"WHERE\b", r"RETURNING\b"):
            mm = re.search(pattern, tail, flags=re.I)
            if mm and mm.start() < stop:
                stop = mm.start()
        cols = _bare_rhs_columns(tail[:stop])
        if cols:
            line = source[: m.start()].count("\n") + 1
            findings.append(f"{module}:~{line} -> {sorted(set(cols))}")

    assert not findings, (
        "ambiguous ON CONFLICT DO UPDATE SET assignment(s); PostgreSQL cannot "
        "tell the target row from `excluded`. Qualify with the table name:\n  "
        + "\n  ".join(findings)
    )


# --- SQLite behavioural equivalence -----------------------------------------
#
# The $7 fix rewrote SQL that SQLite also executes (``_translate_sql`` is
# Postgres-only, so SQLite sees the raw string). A rewrite that resolves the
# Postgres type error but changes SQLite semantics trades a loud bug for a
# silent one.
#
# This is not hypothetical. The first attempted fix used
# ``COALESCE(actual_cost_usd, 0) + COALESCE(?, 0.0)``, which turns a NULL cost
# with a NULL delta into ``0.0`` where the original ``CASE`` left it NULL.
# ``sessions.actual_cost_usd`` is nullable on both backends, so that state is
# reachable, and every one of the 391 tests in the state suite passes with the
# divergence present — verified by mutation. Nothing else in the repo can see it.
#
# The expression is extracted from the live source rather than pinned here, so
# the guard tracks HEAD instead of silently rotting against a copy.

_ORIGINAL_CASE = {
    "absolute": (
        "CASE WHEN ? IS NULL THEN actual_cost_usd ELSE ? END"
    ),
    "delta": (
        "CASE WHEN ? IS NULL THEN actual_cost_usd "
        "ELSE COALESCE(actual_cost_usd, 0) + ? END"
    ),
}


def _actual_cost_exprs():
    """Pull the live ``actual_cost_usd`` assignment from each SQL branch."""
    source = (REPO / "hermes_state.py").read_text(encoding="utf-8")
    start = source.index("def update_token_counts(")
    end = source.index("def _record_model_usage(", start)
    body = source[start:end]

    branches = re.findall(r'sql = """(.*?)"""', body, flags=re.DOTALL)
    assert len(branches) == 2, f"expected 2 SQL branches, found {len(branches)}"

    out = {}
    for label, sql in zip(("absolute", "delta"), branches):
        m = re.search(
            r"actual_cost_usd = (.*?),\n\s+cost_status", sql, flags=re.DOTALL
        )
        assert m, f"actual_cost_usd assignment not found in {label} branch"
        out[label] = " ".join(m.group(1).split())
    return out


def _eval_sqlite(expr, cost, start):
    import sqlite3

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE s (id TEXT PRIMARY KEY, actual_cost_usd REAL)")
        conn.execute("INSERT INTO s VALUES ('x', ?)", (start,))
        conn.execute(
            f"UPDATE s SET actual_cost_usd = {expr} WHERE id = ?",
            (cost,) * expr.count("?") + ("x",),
        )
        return conn.execute(
            "SELECT actual_cost_usd FROM s WHERE id='x'"
        ).fetchone()[0]
    finally:
        conn.close()


def test_actual_cost_usd_sqlite_semantics_unchanged():
    """The rewritten expression must equal the original CASE on SQLite.

    Covers the NULL-column/NULL-param cell specifically: that is the one the
    first fix attempt got wrong, and the one no other test in the repo reaches.
    """
    exprs = _actual_cost_exprs()
    failures = []
    for label, expr in exprs.items():
        original = _ORIGINAL_CASE[label]
        for start in (None, 0.0, 1.5):
            for cost in (None, 0.25):
                got = _eval_sqlite(expr, cost, start)
                want = _eval_sqlite(original, cost, start)
                if got != want:
                    failures.append(
                        f"{label}: start={start!r} cost={cost!r} "
                        f"original={want!r} rewritten={got!r}"
                    )
    assert not failures, (
        "rewritten actual_cost_usd diverges from original CASE semantics on "
        "SQLite:\n  " + "\n  ".join(failures)
    )
