"""levos 0065 — the memory tool's MEMORY.md / USER.md follow PostgreSQL authority.

On ``HERMES_STATE_BACKEND=authority`` the built-in memory lives in the
profile's PostgreSQL store (``core_memory_files``) and its read→modify→write
is fenced by a PostgreSQL advisory lock, so two pods of one profile — each
with its own empty disk — share one memory: neither overwrites the other's
additions, and a pod that starts after the other one is gone still sees them.
Nothing is written under ``memories/``. Every other backend keeps its files
and flocks.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips), the same fixture 0060 uses.
"""

from __future__ import annotations

import contextlib
import hashlib
import multiprocessing
import os
import queue
import time
from pathlib import Path

import psycopg
import pytest

import hermes_aux_store as aux
from agent import learning_graph, learning_mutations
from tools.memory_tool import ENTRY_DELIMITER, MemoryStore
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0065&connect_timeout=1"
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


@pytest.fixture(autouse=True)
def _home(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    return Path(os.environ["HERMES_HOME"])


@pytest.fixture
def pg(postgres_dsn):
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_memory_files")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg):
    """Authority profile whose core session schema exists (as in production)
    while the memory table does not yet; pods then race only its DDL."""
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
    SessionDB(read_only=False).close()
    return pg


def _files(root: Path):
    if not root.exists():
        return []
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def _rows(dsn):
    with psycopg.connect(dsn) as raw:
        return dict(
            raw.execute("SELECT name, content FROM core_memory_files").fetchall()
        )


def _entries(text):
    return [e.strip() for e in text.split(ENTRY_DELIMITER) if e.strip()]


# --------------------------------------------------------------------------
# (a) round trip on PostgreSQL, nothing under memories/
# --------------------------------------------------------------------------


def test_authority_roundtrips_memory_without_files(authority, _home):
    store = MemoryStore()
    store.load_from_disk()
    assert store.add("memory", "Project uses pytest")["success"]
    assert store.add("memory", "Deploys go through Argo")["success"]
    assert store.add("user", "Prefers Korean replies")["success"]
    assert store.replace("memory", "pytest", "Project uses pytest -q")["success"]
    batch = store.apply_batch(
        "memory",
        [
            {"action": "remove", "old_text": "Argo"},
            {"action": "add", "content": "Staging first"},
        ],
    )
    assert batch["success"]
    assert store.remove("user", "Korean")["success"]
    assert store.add("user", "Name is Won")["success"]
    rows = _rows(authority)
    assert _entries(rows["MEMORY.md"]) == ["Project uses pytest -q", "Staging first"]
    assert _entries(rows["USER.md"]) == ["Name is Won"]

    fresh = MemoryStore()
    fresh.load_from_disk()
    assert fresh.memory_entries == ["Project uses pytest -q", "Staging first"]
    assert "Name is Won" in fresh.format_for_system_prompt("user")

    # The journey graph and its edit/delete read and write the same rows.
    cards = learning_graph._memory_cards()
    assert [(c["source"], c["title"]) for c in cards] == [
        ("memory", "Project uses pytest -q"),
        ("memory", "Staging first"),
        ("profile", "Name is Won"),
    ]
    assert all(isinstance(c["timestamp"], int) for c in cards)
    assert (
        learning_mutations.node_detail("memory:profile:2")["content"] == "Name is Won"
    )
    assert learning_mutations.edit_node("memory:memory:1", "Staging first, then prod")[
        "ok"
    ]
    assert learning_mutations.delete_node("memory:memory:0")["ok"]
    assert _entries(_rows(authority)["MEMORY.md"]) == ["Staging first, then prod"]

    # The drift guard still refuses a rewrite over foreign content; its
    # snapshot is a row, not a .bak file.
    drifted = "Staging first, then prod\n§\n" + "x" * 2300
    with psycopg.connect(authority) as raw:
        raw.execute(
            "UPDATE core_memory_files SET content = %s WHERE name = 'MEMORY.md'",
            (drifted,),
        )
    refused = store.remove("memory", "Staging")
    assert refused["success"] is False
    assert refused["drift_backup"].startswith("core_memory_files/MEMORY.md.bak.")
    rows = _rows(authority)
    assert rows["MEMORY.md"] == drifted
    assert rows[refused["drift_backup"].split("/", 1)[1]] == drifted

    assert _files(_home / "memories") == []


# --------------------------------------------------------------------------
# (e) authority without PostgreSQL fails loudly and writes no file
# --------------------------------------------------------------------------


def test_authority_without_postgres_raises_and_creates_no_file(monkeypatch, _home):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    store = MemoryStore()
    store.load_from_disk()  # read-only: an unreadable store loads as empty
    assert store.format_for_system_prompt("memory") is None
    for call in (
        lambda: store.add("memory", "fact"),
        lambda: store.apply_batch("user", [{"action": "add", "content": "fact"}]),
        lambda: learning_mutations.delete_node("memory:memory:0"),
        learning_graph._memory_cards,
    ):
        with pytest.raises(aux.AuxStoreUnavailable) as caught:
            call()
        assert "nonexistent-0065" not in str(caught.value)
    assert _files(_home / "memories") == []


# --------------------------------------------------------------------------
# (f) every other backend keeps the files and the flock
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_memory_files_and_flock(monkeypatch, _home, backend):
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def no_postgres(*_args, **_kwargs):
        raise AssertionError("a non-authority profile must not reach PostgreSQL")

    monkeypatch.setattr(aux, "_open_postgres", no_postgres)
    store = MemoryStore()
    store.load_from_disk()
    assert store.add("memory", "first")["success"]
    assert store.add("memory", "second")["success"]
    assert store.add("user", "someone")["success"]
    assert learning_mutations.edit_node("memory:memory:1", "second, edited")["ok"]
    assert learning_mutations.delete_node("memory:memory:0")["ok"]
    memories = _home / "memories"
    assert (memories / "MEMORY.md").read_text(encoding="utf-8") == "second, edited"
    assert (memories / "USER.md").read_text(encoding="utf-8") == "someone"
    assert {"MEMORY.md.lock", "USER.md.lock"} <= set(_files(memories))


# --------------------------------------------------------------------------
# Two pods of one profile: each has its own disk (its own HERMES_HOME) and
# they share only PostgreSQL. Children are spawned and inherit the authority
# DSN; this test uses only the memory tool's pre-0065 surface, so the same
# scenario run on the base commit shows the defect it fixes.
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")
_ADDS_PER_POD = 12


def _pod(home, name, start, added, events):
    try:
        os.environ["HERMES_HOME"] = home
        from tools.memory_tool import MemoryStore

        # A session that started before the other pod wrote anything.
        session = MemoryStore()
        session.load_from_disk()
        events.put(("ready", name))
        start.wait(60)
        for i in range(_ADDS_PER_POD):
            result = session.add("memory", f"{name} fact {i}")
            assert result["success"], result
        assert session.add("user", f"{name} saw the user")["success"]
        events.put(("added", name))
        added.wait(60)
        # The next session on this pod: its snapshot is loaded fresh.
        later = MemoryStore()
        later.load_from_disk()
        events.put((
            "loaded",
            {
                "pod": name,
                "memory": later.memory_entries,
                "user": later.user_entries,
                "prompt": later.format_for_system_prompt("memory") or "",
            },
        ))
    except BaseException as exc:
        events.put(("error", f"{name}: {type(exc).__name__}: {exc}"))
        raise


def _await(events, done, *, timeout=120):
    got = []
    deadline = time.monotonic() + timeout
    while not done(got):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out; events so far: {got}"
        got.append(events.get(timeout=remaining))
        assert not [v for k, v in got if k == "error"], f"a pod failed: {got}"
    return got


def _count(got, kind):
    return sum(1 for k, _ in got if k == kind)


def test_two_pods_sharing_only_postgres_keep_and_see_each_others_memories(
    authority, tmp_path, monkeypatch
):
    homes = {name: tmp_path / name for name in ("pod-a", "pod-b", "pod-new")}
    for home in homes.values():
        home.mkdir()
    events, start, added = _SPAWN.Queue(), _SPAWN.Event(), _SPAWN.Event()
    pods = [
        _SPAWN.Process(target=_pod, args=(str(homes[n]), n, start, added, events))
        for n in ("pod-a", "pod-b")
    ]
    for pod in pods:
        pod.start()
    try:
        _await(events, lambda got: _count(got, "ready") == 2)
        start.set()  # both pods add at the same time
        _await(events, lambda got: _count(got, "added") == 2)
        added.set()
        got = _await(events, lambda got: _count(got, "loaded") == 2)
    finally:
        for pod in pods:
            pod.join(30)
            if pod.is_alive():
                pod.kill()
                pod.join(5)

    expected = {
        f"{n} fact {i}" for n in ("pod-a", "pod-b") for i in range(_ADDS_PER_POD)
    }
    users = {"pod-a saw the user", "pod-b saw the user"}
    for loaded in [v for k, v in got if k == "loaded"]:
        # Each pod's next session sees the other pod's additions, none lost.
        assert set(loaded["memory"]) == expected, loaded["pod"]
        assert set(loaded["user"]) == users, loaded["pod"]
        assert "pod-a fact 0" in loaded["prompt"] and "pod-b fact 0" in loaded["prompt"]

    # The old pods are gone with their disks; a new pod on an empty disk
    # still has every memory.
    monkeypatch.setenv("HERMES_HOME", str(homes["pod-new"]))
    new_pod = MemoryStore()
    new_pod.load_from_disk()
    assert set(new_pod.memory_entries) == expected
    assert set(new_pod.user_entries) == users
    for home in homes.values():
        assert _files(home / "memories") == []


def _slow_writer(home, inside, release, events):
    """Pod A: holds its reload→save section open until *release* is set."""
    try:
        os.environ["HERMES_HOME"] = home
        from tools.memory_tool import MemoryStore

        # pg3: ``_mutate`` persists through ``_write_file`` (no ``save_to_disk``).
        real_write = MemoryStore._write_file

        def paused_write(path, entries):
            inside.set()
            assert release.wait(60), "never released"
            real_write(path, entries)

        MemoryStore._write_file = staticmethod(paused_write)
        store = MemoryStore()
        store.load_from_disk()
        assert store.add("memory", "from pod A")["success"]
        events.put(("done", "pod-a"))
    except BaseException as exc:
        events.put(("error", f"pod-a: {type(exc).__name__}: {exc}"))
        raise


def _plain_writer(home, events):
    try:
        os.environ["HERMES_HOME"] = home
        from tools.memory_tool import MemoryStore

        store = MemoryStore()
        store.load_from_disk()
        assert store.add("memory", "from pod B")["success"]
        events.put(("done", "pod-b"))
    except BaseException as exc:
        events.put(("error", f"pod-b: {type(exc).__name__}: {exc}"))
        raise


def test_a_pod_writing_waits_for_the_other_pods_open_section(authority, tmp_path):
    """The cross-pod fence, deterministically: pod B's add cannot complete while
    pod A is between its reload and its save, and so cannot be overwritten by
    A's save from a view that lacks it."""
    events, inside, release = _SPAWN.Queue(), _SPAWN.Event(), _SPAWN.Event()
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    slow = _SPAWN.Process(
        target=_slow_writer, args=(str(tmp_path / "a"), inside, release, events)
    )
    slow.start()
    plain = None
    try:
        assert inside.wait(60), "pod A never reached its save"
        plain = _SPAWN.Process(target=_plain_writer, args=(str(tmp_path / "b"), events))
        plain.start()
        early = []
        with contextlib.suppress(queue.Empty):
            early.append(events.get(timeout=3))  # pod B, if nothing fences it
        assert early == [], f"pod B finished while pod A held the section: {early}"
        release.set()
        _await(events, lambda got: _count(got, "done") == 2)
    finally:
        release.set()
        for process in (slow, plain):
            if process is None:
                continue
            process.join(30)
            if process.is_alive():
                process.kill()
                process.join(5)
    assert _entries(_rows(authority)["MEMORY.md"]) == ["from pod A", "from pod B"]


# --------------------------------------------------------------------------
# (g) one-shot move of the files: idempotent, sources untouched
# --------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_memory_migration_merges_by_entry_is_idempotent_and_leaves_files(
    authority, _home, monkeypatch
):
    monkeypatch.setenv("HERMES_PROFILE", "dave")
    memories = _home / "memories"
    memories.mkdir(exist_ok=True)
    (memories / "MEMORY.md").write_bytes(
        "\ufeffold fact\n§\nshared fact\n§\nold fact".encode("utf-8")
    )
    (memories / "MEMORY.md.bak.1700000000").write_text(
        "snapshot text", encoding="utf-8"
    )
    sources = {p: _sha256(p) for p in memories.iterdir()}

    # A pod on the new image wrote before the move ran.
    live = MemoryStore()
    live.load_from_disk()
    assert live.add("memory", "live fact")["success"]
    assert live.add("memory", "shared fact")["success"]

    dry = aux.migrate_memory_to_pg("dave", dry_run=True)
    assert dry["stores"]["MEMORY.md"]["status"] == "dry_run"
    assert _entries(_rows(authority)["MEMORY.md"]) == ["live fact", "shared fact"]

    first = aux.migrate_memory_to_pg("dave", dry_run=False)
    assert first["stores"]["USER.md"]["status"] == "missing"
    table = first["stores"]["MEMORY.md"]["tables"]["memory_files"]
    assert table == {
        "source_entries": 2,
        "target_entries_before": 2,
        "target_entries_after": 3,
        "inserted": 1,
    }
    rows = _rows(authority)
    assert _entries(rows["MEMORY.md"]) == ["live fact", "shared fact", "old fact"]
    assert rows["MEMORY.md.bak.1700000000"] == "snapshot text"

    second = aux.migrate_memory_to_pg("dave", dry_run=False)
    assert second["stores"]["MEMORY.md"]["tables"]["memory_files"]["inserted"] == 0
    assert _rows(authority) == rows
    assert {p: _sha256(p) for p in memories.iterdir()} == sources
    # The command line reaches the same function.
    assert aux.main(["--profile", "dave", "--memory"]) == 0

    with pytest.raises(ValueError, match="not the active profile"):
        aux.migrate_memory_to_pg("opsi", dry_run=False)
    (memories / "USER.md").write_bytes(b"\xff\xfe broken")
    with pytest.raises(aux.AuxMigrationError, match="not valid UTF-8"):
        aux.migrate_memory_to_pg("dave", dry_run=False)
    assert "USER.md" not in _rows(authority)


def test_memory_migration_into_an_empty_store_copies_the_file_verbatim(
    authority, _home, monkeypatch
):
    monkeypatch.setenv("HERMES_PROFILE", "dave")
    memories = _home / "memories"
    memories.mkdir(exist_ok=True)
    text = "Name is Won\n§\nLikes short answers"
    (memories / "USER.md").write_text(text, encoding="utf-8")
    report = aux.migrate_memory_to_pg("dave", dry_run=False)
    assert report["stores"]["USER.md"]["tables"]["memory_files"]["inserted"] == 2
    assert _rows(authority)["USER.md"] == text
    store = MemoryStore()
    store.load_from_disk()
    assert store.user_entries == ["Name is Won", "Likes short answers"]


def test_memory_migration_first_run_reflects_every_source_entry_in_postgres(
    authority, _home, monkeypatch
):
    """The entries the first run migrates are exactly the entries PostgreSQL
    then holds (per file, into an empty store); a re-run inserts nothing."""
    monkeypatch.setenv("HERMES_PROFILE", "dave")
    memories = _home / "memories"
    memories.mkdir(exist_ok=True)
    files = {
        "MEMORY.md": "alpha\n§\nbeta\n§\ngamma\n§\nbeta",
        "USER.md": "\ufeffName is Won\n§\nLikes short answers",
    }
    for name, text in files.items():
        (memories / name).write_text(text, encoding="utf-8")

    first = aux.migrate_memory_to_pg("dave", dry_run=False)
    rows = _rows(authority)
    for name, expected in (("MEMORY.md", 3), ("USER.md", 2)):
        table = first["stores"][name]["tables"]["memory_files"]
        assert table["source_entries"] == expected
        assert table["inserted"] == expected
        assert table["target_entries_before"] is None
        assert table["target_entries_after"] == expected
        # The row is the file verbatim (a duplicate line included); the entries
        # it reflects are the distinct ones, as the memory tool loads them.
        assert rows[name] == files[name].lstrip("\ufeff")
        reflected = list(dict.fromkeys(_entries(rows[name])))
        assert len(reflected) == expected
        assert reflected == list(dict.fromkeys(_entries(files[name].lstrip("\ufeff"))))
    store = MemoryStore()
    store.load_from_disk()
    assert store.memory_entries == ["alpha", "beta", "gamma"]
    assert store.user_entries == ["Name is Won", "Likes short answers"]

    second = aux.migrate_memory_to_pg("dave", dry_run=False)
    for name in files:
        assert second["stores"][name]["tables"]["memory_files"]["inserted"] == 0
    assert _rows(authority) == rows
