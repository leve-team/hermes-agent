"""levos 0068 — the cron files one run hands to the next follow PostgreSQL authority.

Two overlapping pods of an authority profile share the profile's PostgreSQL
store and nothing else: each has its own disk. The two-process tests below
give every child its own ``HERMES_HOME`` (its own disk) and only the DSN in
common, and check that what one pod wrote is what the other pod's next run
reads — the latest output (``context_from``), the monitor baseline, the body
of a job script, and the cron suggestions with their dismiss latch. Every
other backend keeps its files.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL, like 0060
(``initdb`` / ``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are
errors, never skips).
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import psycopg
import pytest

import hermes_aux_store as aux
from cron import executions, monitor, notepad
from cron import jobs as cron_jobs
from tests.test_cron_pg_authority import (
    _SPAWN,
    _await,
    _files,
    _kinds,
    _spawn,
    _stop,
)
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0068&connect_timeout=1"
)
TABLES = (
    "core_cron_outputs",
    "core_cron_scripts",
    "core_cron_suggestions",
    "core_cron_executions",
    "core_cron_notes",
    "core_cron_jobs",
)
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
)


def _bind_suggestions():
    """``cron.suggestions`` freezes its file path at import; re-bind it to the
    current ``HERMES_HOME`` (what a fresh pod process does on import)."""
    from cron import suggestions

    home = Path(os.environ["HERMES_HOME"]).resolve()
    suggestions.CRON_DIR = home / "cron"
    suggestions.SUGGESTIONS_FILE = home / "cron" / "suggestions.json"
    return suggestions


@pytest.fixture(autouse=True)
def _cron_home(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HERMES_MODEL", "test-cron-default-model")
    home = Path(os.environ["HERMES_HOME"])
    monkeypatch.setattr(notepad, "NOTEPAD_FILE", home / "cron" / "notepad.db")
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", None)
    from cron import suggestions

    monkeypatch.setattr(suggestions, "CRON_DIR", home.resolve() / "cron")
    monkeypatch.setattr(
        suggestions, "SUGGESTIONS_FILE", home.resolve() / "cron" / "suggestions.json"
    )
    yield home
    executions._stop_lease_renewer()


@pytest.fixture
def pg(postgres_dsn):
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute(f"DROP TABLE IF EXISTS {', '.join(TABLES)}")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg):
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
    SessionDB(read_only=False).close()
    return pg


@pytest.fixture
def pods(tmp_path, monkeypatch):
    """Spawn a child on its own disk: *home* becomes its ``HERMES_HOME``."""

    def spawn(name, target, events, *args):
        home = tmp_path / f"pod-{name}"
        (home / "scripts").mkdir(parents=True, exist_ok=True)
        with monkeypatch.context() as env:
            env.setenv("HERMES_HOME", str(home))
            return _spawn(target, events, *args), home

    return spawn


def _scalar(dsn, sql, params=()):
    with psycopg.connect(dsn) as raw:
        row = raw.execute(sql, params).fetchone()
        return None if row is None else row[0]


# --------------------------------------------------------------------------
# Two pods (A-F16): the latest output reaches the next run on the other pod
# --------------------------------------------------------------------------


def _output_writer_pod(job_id, text, events):
    written = cron_jobs.save_job_output(job_id, text)
    events.put(("written", str(written)))


def _prompt_reader_pod(job_id, go, events):
    from cron.scheduler import _build_job_prompt

    events.put(("ready", os.getpid()))
    go.wait(60)
    events.put(("prompt", _build_job_prompt(cron_jobs.get_job(job_id))))


def test_two_pods_context_from_sees_the_output_the_other_pod_wrote(authority, pods):
    """A-F16: pod A runs a continuity job; its next run lands on pod B (alive at
    the same time, own disk) and must get A's output as its previous run."""
    job = cron_jobs.create_job(
        prompt="report what changed",
        schedule="every 1h",
        name="brief",
        context_from=["self"],
    )
    events, go = _SPAWN.Queue(), _SPAWN.Event()
    reader, home_b = pods("b", _prompt_reader_pod, events, job["id"], go)
    writer = None
    try:
        _await(events, lambda got: bool(_kinds(got, "ready")))
        writer, home_a = pods(
            "a", _output_writer_pod, events, job["id"], "REPORT-FROM-POD-A: 3 new items"
        )
        got = _await(events, lambda got: bool(_kinds(got, "written")))
        go.set()
        got += _await(events, lambda got: bool(_kinds(got, "prompt")))
    finally:
        _stop([p for p in (reader, writer) if p is not None])

    (written,) = _kinds(got, "written")
    assert Path(written).is_relative_to(home_a)  # the run history stays pod-local
    assert not (home_b / "cron" / "output").exists()
    (prompt,) = _kinds(got, "prompt")
    assert "## Your previous run's output" in prompt
    assert "REPORT-FROM-POD-A: 3 new items" in prompt


# --------------------------------------------------------------------------
# Two pods (A-F16): the monitor diff baseline is the other pod's snapshot
# --------------------------------------------------------------------------


def _monitor_pod(job_id, source_output, go, events):
    monitor._run_monitor_source = lambda job: (True, source_output)
    events.put(("ready", os.getpid()))
    go.wait(60)
    outcome = monitor.check_monitor(cron_jobs.get_job(job_id))
    events.put(("outcome", (outcome.first_run, outcome.changed, outcome.context_block)))


def test_two_pods_monitor_diffs_against_the_other_pods_baseline(authority, pods):
    job = cron_jobs.create_job(
        prompt="watch",
        schedule="every 1h",
        name="mon",
        monitor_url="https://example.invalid/x",
    )
    events, go_a, go_b = _SPAWN.Queue(), _SPAWN.Event(), _SPAWN.Event()
    pod_a, home_a = pods("a", _monitor_pod, events, job["id"], "alpha\nbeta\n", go_a)
    pod_b, home_b = pods("b", _monitor_pod, events, job["id"], "alpha\ngamma\n", go_b)
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        go_a.set()
        first = _await(events, lambda got: bool(_kinds(got, "outcome")))
        go_b.set()
        second = _await(events, lambda got: bool(_kinds(got, "outcome")))
    finally:
        _stop([pod_a, pod_b])

    assert _kinds(first, "outcome")[0][:2] == (True, True)  # baseline on pod A
    first_run, changed, block = _kinds(second, "outcome")[0]
    assert changed and not first_run
    assert "MONITOR CHANGE DETECTED" in block
    assert "-beta" in block and "+gamma" in block  # diffed against A's snapshot
    for home in (home_a, home_b):
        assert not list(home.rglob("monitor_last_output.txt"))
    assert (
        _scalar(
            authority, "SELECT content FROM core_cron_outputs WHERE kind = 'monitor'"
        )
        == "alpha\ngamma\n"
    )


# --------------------------------------------------------------------------
# Two pods (A-F17): a script written on one pod runs on the other
# --------------------------------------------------------------------------

_SCRIPT = b"print('collected-on-pod-a')\n"


def _script_author_pod(go, events):
    from hermes_constants import get_hermes_home
    from tools.cronjob_tools import cronjob

    script = get_hermes_home() / "scripts" / "collect" / "probe.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_bytes(_SCRIPT)
    script.chmod(0o750)
    result = json.loads(
        cronjob(
            action="create",
            prompt="summarize",
            schedule="every 1h",
            name="probe",
            script="collect/probe.py",
        )
    )
    events.put(("created", result.get("job_id") or result))
    go.wait(60)  # stays up while the other pod fires the job
    events.put(("done", os.getpid()))


def _script_runner_pod(job_id, events):
    from cron.scheduler import _run_job_script
    from hermes_constants import get_hermes_home

    job = cron_jobs.get_job(job_id)
    ok, output = _run_job_script(job["script"])
    local = get_hermes_home() / "scripts" / job["script"]
    events.put((
        "ran",
        (
            ok,
            output,
            local.read_bytes() if local.exists() else None,
            stat.S_IMODE(local.stat().st_mode) if local.exists() else None,
        ),
    ))


def test_two_pods_fire_a_script_written_on_the_other_pod(authority, pods):
    events, go = _SPAWN.Queue(), _SPAWN.Event()
    author, home_a = pods("a", _script_author_pod, events, go)
    runner = None
    try:
        got = _await(events, lambda got: bool(_kinds(got, "created")))
        (job_id,) = _kinds(got, "created")
        assert isinstance(job_id, str), job_id
        runner, home_b = pods("b", _script_runner_pod, events, job_id)
        got = _await(events, lambda got: bool(_kinds(got, "ran")))
        go.set()
    finally:
        _stop([p for p in (author, runner) if p is not None])

    ok, output, body, mode = _kinds(got, "ran")[0]
    assert (ok, output) == (True, "collected-on-pod-a")
    assert body == _SCRIPT and mode == 0o750  # written back on pod B's own disk
    assert (home_a / "scripts" / "collect" / "probe.py").read_bytes() == _SCRIPT


# --------------------------------------------------------------------------
# Two pods (A-F15): one suggestion list, one cap, one dismiss latch
# --------------------------------------------------------------------------


def _suggestion_pod(name, go, events):
    suggestions = _bind_suggestions()
    events.put(("ready", os.getpid()))
    go.wait(60)
    added = []
    for index in range(suggestions.MAX_PENDING):
        record = suggestions.add_suggestion(
            title=f"{name}-{index}",
            description="d",
            source="catalog",
            job_spec={"prompt": "p", "schedule": "every 1h"},
            dedup_key=f"{name}-{index}",
        )
        added.append(record["id"] if record else None)
    events.put(("added", added))


def _latch_pod(dedup_key, events):
    suggestions = _bind_suggestions()
    record = suggestions.add_suggestion(
        title="again",
        description="d",
        source="catalog",
        job_spec={"prompt": "p", "schedule": "every 1h"},
        dedup_key=dedup_key,
    )
    events.put(("offered", record is not None))


def test_two_pods_share_one_suggestion_list_cap_and_latch(authority, pods):
    suggestions = _bind_suggestions()
    events, go = _SPAWN.Queue(), _SPAWN.Event()
    pod_a, home_a = pods("a", _suggestion_pod, events, "a", go)
    pod_b, home_b = pods("b", _suggestion_pod, events, "b", go)
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        go.set()
        got = _await(events, lambda got: len(_kinds(got, "added")) == 2)
    finally:
        _stop([pod_a, pod_b])

    accepted = [i for added in _kinds(got, "added") for i in added if i]
    pending = suggestions.list_pending()
    assert len(accepted) == suggestions.MAX_PENDING  # one cap across both pods
    assert sorted(s["id"] for s in pending) == sorted(accepted)

    dismissed = pending[0]
    assert suggestions.dismiss_suggestion(dismissed["id"]) is True
    latch, _ = pods("c", _latch_pod, events, dismissed["dedup_key"])
    try:
        again = _await(events, lambda got: bool(_kinds(got, "offered")))
    finally:
        _stop([latch])
    assert _kinds(again, "offered") == [False]  # the dismiss latch holds on a new pod
    for home in (home_a, home_b):
        assert not (home / "cron" / "suggestions.json").exists()


# --------------------------------------------------------------------------
# One process, authority: round trips, removal, edits, failures. The tests
# above use only interfaces the base commit has, so they also reproduce the
# overlap failures on it; the ones below import the 0068 module.
# --------------------------------------------------------------------------


def test_authority_round_trips_without_state_files(authority, _cron_home):
    from cron import durable

    from cron.scheduler import _run_job_script

    job = cron_jobs.create_job(prompt="p", schedule="every 1h", name="rt")
    cron_jobs.save_job_output(job["id"], "first")
    cron_jobs.save_job_output(job["id"], "second\x00run")
    assert durable.load_output(job["id"]) == "second\ufffdrun"
    monitor._write_last_output(job["id"], "baseline")
    assert monitor._read_last_output(job["id"]) == "baseline"

    script = _cron_home / "scripts" / "edit.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("print('v1')\n")
    assert _run_job_script("edit.py") == (True, "v1")
    script.write_text("print('v2')\n")  # the agent edits the file in place
    assert _run_job_script("edit.py") == (True, "v2")  # the local file wins
    assert (
        _scalar(
            authority, "SELECT convert_from(content, 'UTF8') FROM core_cron_scripts"
        )
        == "print('v2')\n"
    )
    script.unlink()
    assert _run_job_script("edit.py") == (True, "v2")  # written back
    ok, message = _run_job_script("never-written.py")
    assert not ok and message.startswith("Script not found")

    assert cron_jobs.remove_job(job["id"]) is True
    assert durable.load_output(job["id"]) is None
    assert durable.load_output(job["id"], durable.MONITOR) is None
    state = [f for f in _files(_cron_home / "cron") if not f.startswith("output/")]
    assert state == []  # only the pod-local run history is a file


def test_authority_without_postgres_fails_loudly(monkeypatch, _cron_home):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    from cron.scheduler import _build_job_prompt, _run_job_script

    script = _cron_home / "scripts" / "present.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("print('x')\n")
    ok, message = _run_job_script("present.py")
    assert not ok and message.startswith("Script store unavailable")
    assert "nonexistent-0068" not in message
    suggestions = _bind_suggestions()
    for call in (
        lambda: cron_jobs.save_job_output("abc123abc123", "out"),
        lambda: _build_job_prompt({
            "id": "abc123abc123",
            "prompt": "p",
            "context_from": ["self"],
        }),
        suggestions.load_suggestions,
        lambda: suggestions.add_suggestion(
            title="t", description="d", source="catalog", job_spec={}, dedup_key="k"
        ),
    ):
        with pytest.raises(aux.AuxStoreUnavailable) as caught:
            call()
        assert "nonexistent-0068" not in str(caught.value)
    assert not (_cron_home / "cron" / "suggestions.json").exists()


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_files(monkeypatch, _cron_home, backend):
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def no_postgres(*_args, **_kwargs):
        raise AssertionError("a non-authority profile must not reach PostgreSQL")

    monkeypatch.setattr(aux, "_open_postgres", no_postgres)
    monkeypatch.setattr(aux, "_connect_lock_session", no_postgres)
    from cron.scheduler import _build_job_prompt, _run_job_script

    job = cron_jobs.create_job(
        prompt="p", schedule="every 1h", name="files", context_from=["self"]
    )
    cron_jobs.save_job_output(job["id"], "local-output")
    assert "local-output" in _build_job_prompt(cron_jobs.get_job(job["id"]))
    monitor._write_last_output(job["id"], "snap")
    assert monitor._read_last_output(job["id"]) == "snap"
    suggestions = _bind_suggestions()
    assert suggestions.add_suggestion(
        title="t", description="d", source="catalog", job_spec={}, dedup_key="k"
    )
    (_cron_home / "scripts").mkdir(exist_ok=True)
    (_cron_home / "scripts" / "s.py").write_text("print('s')\n")
    assert _run_job_script("s.py") == (True, "s")
    ok, message = _run_job_script("missing.py")
    assert not ok and message.startswith("Script not found")

    files = _files(_cron_home / "cron")
    assert "suggestions.json" in files
    assert f"output/{job['id']}/monitor_last_output.txt" in files
    assert any(
        f.startswith(f"output/{job['id']}/") and f.endswith(".md") for f in files
    )


# --------------------------------------------------------------------------
# One-shot move of the pre-0068 files
# --------------------------------------------------------------------------


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _seed_files(home):
    job_dir = home / "cron" / "output" / "0123456789ab"
    job_dir.mkdir(parents=True)
    older, newer = (
        job_dir / "2026-09-01_00-00-00.md",
        job_dir / "2026-09-02_00-00-00.md",
    )
    older.write_text("old run")
    newer.write_text("new run")
    os.utime(older, (1_000, 1_000))
    os.utime(newer, (2_000, 2_000))
    (job_dir / "monitor_last_output.txt").write_text("snap")
    (home / "cron" / "output" / "not a job").mkdir()
    scripts = home / "scripts" / "sub"
    scripts.mkdir(parents=True)
    (scripts / "a.sh").write_bytes(b"echo a\n")
    (home / "scripts" / "b.py").write_bytes(b"print('b')\n")
    (home / "scripts" / "escape.py").symlink_to(home / "cron" / "suggestions.json")
    (home / "cron" / "suggestions.json").write_text(
        json.dumps({
            "suggestions": [
                {"id": "s1", "title": "one", "dedup_key": "k1", "status": "pending"},
                {"id": "s2", "title": "two", "dedup_key": "k2", "status": "dismissed"},
            ]
        })
    )
    return [
        newer,
        older,
        job_dir / "monitor_last_output.txt",
        scripts / "a.sh",
        home / "scripts" / "b.py",
        home / "cron" / "suggestions.json",
    ]


def test_cron_files_migration_is_idempotent_and_leaves_sources_untouched(
    authority, _cron_home, monkeypatch
):
    from cron import durable

    monkeypatch.setenv("HERMES_PROFILE", "c10")
    sources = _seed_files(_cron_home)
    digests = [_sha(p) for p in sources]
    # A pod that already ran since the image switch holds a newer latest output.
    durable.save_output("0123456789ab", durable.LATEST, "ran since")

    dry = durable.migrate_cron_files_to_pg("c10", dry_run=True)
    assert dry["stores"]["cron_scripts"]["status"] == "dry_run"
    assert _scalar(authority, "SELECT COUNT(*) FROM core_cron_scripts") == 0

    first = durable.migrate_cron_files_to_pg("c10", dry_run=False)
    second = durable.migrate_cron_files_to_pg("c10", dry_run=False)
    assert first["stores"]["cron_outputs"]["tables"]["cron_outputs"]["inserted"] == 1
    assert first["stores"]["cron_scripts"]["tables"]["cron_scripts"]["inserted"] == 2
    assert (
        first["stores"]["cron_suggestions"]["tables"]["cron_suggestions"]["inserted"]
        == 2
    )
    for store in ("cron_outputs", "cron_scripts", "cron_suggestions"):
        assert second["stores"][store]["tables"][store]["inserted"] == 0

    assert durable.load_output("0123456789ab") == "ran since"  # PostgreSQL wins
    assert durable.load_output("0123456789ab", durable.MONITOR) == "snap"
    assert sorted(
        r[0]
        for r in psycopg.connect(authority).execute(
            "SELECT path FROM core_cron_scripts"
        )
    ) == ["b.py", "sub/a.sh"]
    suggestions = _bind_suggestions()
    assert [s["id"] for s in suggestions.load_suggestions()] == ["s1", "s2"]
    assert [_sha(p) for p in sources] == digests

    with pytest.raises(ValueError, match="not the active profile"):
        durable.migrate_cron_files_to_pg("other", dry_run=False)


def test_cron_files_migration_rolls_back_a_bad_suggestions_file(
    authority, _cron_home, monkeypatch
):
    from cron import durable

    monkeypatch.setenv("HERMES_PROFILE", "c10")
    path = _cron_home / "cron" / "suggestions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([{"id": "d", "title": "x"}, {"id": "d", "title": "y"}]))
    before = _sha(path)
    with pytest.raises(aux.AuxMigrationError, match="duplicate suggestion id"):
        durable.migrate_cron_files_to_pg("c10", dry_run=False)
    assert _bind_suggestions().load_suggestions() == []
    assert _sha(path) == before
    path.write_text(json.dumps({"suggestions": [{"title": "no id"}]}))
    with pytest.raises(aux.AuxMigrationError, match="without an id"):
        durable.migrate_cron_files_to_pg("c10", dry_run=False)
    path.unlink()
    report = durable.migrate_cron_files_to_pg("c10", dry_run=False)
    assert report["stores"]["cron_suggestions"]["status"] == "missing"
