"""levos 0067 — small gateway state files follow the profile's PostgreSQL authority.

On ``HERMES_STATE_BACKEND=authority`` the pairing grants (and no ``.env``
allowlist mirror), thread participation, Discord non-conversational ids, dead
delivery targets, the rich-send index, voice modes, the ESTOP sentinel and the
webhook subscriptions are ``core_aux_kv`` rows of the profile's store, so two
pods that share nothing but PostgreSQL see one state. Every other backend
keeps its files.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips), like 0060.

levos/pg3 (0.21.2) port: voice modes live in ``gateway/run_voice.py``
(``GatewayVoiceMixin``), the Discord tracker's ``mark_many`` is a coroutine,
``DeadTargetRegistry`` has no ``all_dead`` (the stored entry is read from
PostgreSQL directly) and the rich-send index also keeps attachment pairs
(``rich_sent:media``).
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import time
from pathlib import Path

import psycopg
import pytest

import hermes_aux_store as aux
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0067&connect_timeout=1"
)
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
    "TELEGRAM_ALLOWED_USERS",
)
# Every file the owners write on a non-authority backend (relative to HERMES_HOME).
STATE_FILES = (
    "platforms/pairing",
    "pairing",
    "discord_threads.json",
    "gateway/discord_nonconversational_messages.json",
    "gateway/dead_targets.json",
    "state/rich_sent_index.json",
    "gateway_voice_mode.json",
    "ESTOP",
    "webhook_subscriptions.json",
    ".env",
)


@pytest.fixture(autouse=True)
def _home(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    home = Path(os.environ["HERMES_HOME"])
    # Frozen at import from whichever HERMES_HOME was active then.
    import gateway.pairing as pairing
    from gateway.run import GatewayRunner

    monkeypatch.setattr(pairing, "PAIRING_DIR", home / "platforms" / "pairing")
    monkeypatch.setattr(
        GatewayRunner, "_VOICE_MODE_PATH", home / "gateway_voice_mode.json"
    )
    return home


@pytest.fixture
def pg(postgres_dsn):
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_aux_kv")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg):
    """Authority profile whose core session schema exists (as in production)
    while ``core_aux_kv`` does not yet."""
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
    SessionDB(read_only=False).close()
    return pg


def _state_files(home: Path):
    """The owners' state files present (a scaffolded empty directory is none)."""

    def written(path: Path) -> bool:
        if path.is_dir():
            return any(child.is_file() for child in path.rglob("*"))
        return path.exists()

    return sorted(name for name in STATE_FILES if written(home / name))


def _kv_rows(dsn, namespace):
    with psycopg.connect(dsn) as raw:
        return raw.execute(
            "SELECT key, value FROM core_aux_kv WHERE namespace = %s "
            "ORDER BY updated_at, key",
            (namespace,),
        ).fetchall()


def _runner():
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._voice_mode = runner._load_voice_modes()
    return runner


def _webhook_adapter():
    from gateway.config import PlatformConfig
    from gateway.platforms.webhook import WebhookAdapter

    return WebhookAdapter(PlatformConfig(enabled=True, extra={"secret": "global"}))


def _route(prompt):
    return {"secret": "route-secret", "prompt": prompt, "events": []}


# --------------------------------------------------------------------------
# (a) round trip on PostgreSQL, no state file
# --------------------------------------------------------------------------


def test_authority_pairing_lives_in_postgres_without_env_mirror(
    authority, _home, monkeypatch
):
    from gateway.pairing import PairingStore

    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "operator")
    store = PairingStore()
    code = store.generate_code("telegram", "111", "Alice")
    assert code and store.list_pending("telegram")[0]["user_id"] == "111"
    assert store.approve_code("telegram", "WRONGCOD") is None
    assert store.approve_code("telegram", code) == {
        "user_id": "111",
        "user_name": "Alice",
    }
    assert store.is_approved("telegram", "111")
    assert [u["user_id"] for u in PairingStore().list_approved()] == ["111"]
    assert store.generate_code("telegram", "111") is None  # rate limited
    store._record_rate_limit("telegram", "222")
    assert store._is_rate_limited("telegram", "222")

    # The grant row is the record; no pod-local allowlist mirror.
    assert os.environ["TELEGRAM_ALLOWED_USERS"] == "operator"
    names = [key for key, _ in _kv_rows(authority, aux.KV_PAIRING)]
    assert sorted(names) == [
        "_rate_limits.json",
        "telegram-approved.json",
        "telegram-pending.json",
    ]
    assert store.revoke("telegram", "111") is True
    assert not PairingStore().is_approved("telegram", "111")
    assert store.clear_pending() == 0

    # An explicit other profile never shares the active profile's grants.
    other = PairingStore(profile="someone-else")
    assert not other.is_approved("telegram", "111")
    other._approve_user("telegram", "333")
    assert other.is_approved("telegram", "333")
    assert not store.is_approved("telegram", "333")
    assert _state_files(_home) == []


def test_authority_sets_targets_voice_estop_webhooks_without_files(authority, _home):
    from agent import estop
    from gateway import rich_sent_store
    from gateway.config import Platform
    from gateway.dead_targets import DeadTargetRegistry
    from gateway.platforms.helpers import ThreadParticipationTracker
    from hermes_cli import webhook
    from plugins.platforms.discord.adapter import (
        _DiscordNonConversationalMessageTracker,
    )

    threads = ThreadParticipationTracker("discord", max_tracked=3)
    for thread_id in ("t1", "t2", "t3", "t4"):
        threads.mark(thread_id)
    threads.mark("t3")  # already a member: keeps its place
    assert [k for k, _ in _kv_rows(authority, "threads:discord")] == ["t2", "t3", "t4"]
    reloaded = ThreadParticipationTracker("discord", max_tracked=3)
    assert "t4" in reloaded and "t1" not in reloaded

    noise = _DiscordNonConversationalMessageTracker(max_tracked=2)
    asyncio.run(noise.mark_many(["m1", "m2", "m3", ""]))
    assert "m3" in _DiscordNonConversationalMessageTracker() and "m1" not in noise

    dead = DeadTargetRegistry()
    assert dead.mark_dead("telegram", "-100", "Forbidden") is True
    assert dead.mark_dead("telegram", "-100", "Forbidden again") is False
    assert DeadTargetRegistry().is_dead("Telegram", "-100")
    stored = dict(_kv_rows(authority, aux.KV_DEAD_TARGETS))
    assert json.loads(stored["telegram:-100"])["reason"] == "Forbidden again"
    assert DeadTargetRegistry().clear("telegram", "-100") is True
    assert (
        not dead.is_dead("telegram", "-100") and dead.clear("telegram", "-100") is False
    )

    rich_sent_store.record("chat", 7, "briefing text")
    assert rich_sent_store.lookup("chat", 7) == "briefing text"
    assert rich_sent_store.lookup("chat", 8) is None
    # pg3: attachment pairs name pod-local files; a missing file is filtered out.
    attachment = _home.parent / "attachment-0067.png"
    attachment.write_bytes(b"png")
    rich_sent_store.record_media(
        "chat", 7, [(str(attachment), "image/png"), (str(_home / "gone.png"), "")]
    )
    assert rich_sent_store.lookup_media("chat", 7) == [(str(attachment), "image/png")]
    assert rich_sent_store.lookup_media("chat", 8) == []
    assert rich_sent_store.lookup("chat", 7) == "briefing text"

    runner = _runner()
    runner._voice_mode[runner._voice_key(Platform.TELEGRAM, "42")] = "all"
    runner._save_voice_modes()
    assert _runner()._voice_mode == {"telegram:42": "all"}

    assert estop.is_engaged() is False and estop.get_state() is None
    estop.engage("maintenance")
    assert estop.is_engaged() and estop.get_state()["reason"] == "maintenance"
    assert "maintenance" in estop.paused_reply()
    assert estop.location() == "the profile's PostgreSQL store"
    assert estop.disengage() is True and estop.disengage() is False
    assert estop.is_engaged() is False

    webhook._store_subscription("deploys", _route("ship it"))
    adapter = _webhook_adapter()
    adapter._reload_dynamic_routes()
    assert adapter._routes["deploys"]["prompt"] == "ship it"
    webhook._delete_subscription("deploys")
    adapter._reload_dynamic_routes()
    assert "deploys" not in adapter._routes and webhook._load_subscriptions() == {}

    assert _state_files(_home) == []


def test_authority_whole_set_webhook_save_replaces_rows_without_a_file(authority, _home):
    """The dashboard's webhook routes load the whole set, edit it and save it back through
    ``_save_subscriptions``; on authority that save is the namespace's rows, never the file."""
    from hermes_cli import webhook

    webhook._store_subscription("keep", _route("keep"))
    webhook._store_subscription("drop", _route("drop"))
    subs = webhook._load_subscriptions()
    del subs["drop"]
    subs["keep"]["enabled"] = False
    subs["added"] = _route("added")
    webhook._save_subscriptions(subs)

    assert webhook._load_subscriptions() == subs
    assert sorted(key for key, _ in _kv_rows(authority, aux.KV_WEBHOOK_SUBSCRIPTIONS)) == [
        "added", "keep"]
    adapter = _webhook_adapter()
    adapter._reload_dynamic_routes()
    assert sorted(adapter._routes) == ["added", "keep"]
    assert _state_files(_home) == []


# --------------------------------------------------------------------------
# (e) authority without PostgreSQL: loud, or fail-safe where the module says so
# --------------------------------------------------------------------------


def test_authority_without_postgres_raises_or_fails_safe_and_writes_no_file(
    monkeypatch, _home
):
    from agent import estop
    from gateway import rich_sent_store
    from gateway.dead_targets import DeadTargetRegistry
    from gateway.pairing import PairingStore
    from gateway.platforms.helpers import ThreadParticipationTracker
    from hermes_cli import webhook

    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    store = PairingStore()
    for call in (
        lambda: store.is_approved("telegram", "1"),
        lambda: store.generate_code("telegram", "1"),
        lambda: ThreadParticipationTracker("discord"),
        lambda: estop.engage("x"),
        estop.disengage,
        lambda: webhook._store_subscription("x", _route("x")),
        webhook._load_subscriptions,
        _runner,
    ):
        with pytest.raises(aux.AuxStoreUnavailable) as caught:
            call()
        assert "nonexistent-0067" not in str(caught.value)

    # The kill switch fails safe: an unanswerable store counts as paused.
    assert estop.is_engaged() is True
    assert estop.get_state() == {"reason": None, "engaged_at": None}
    # Best-effort owners degrade to memory / no-op, never to a file.
    dead = DeadTargetRegistry()
    assert dead.mark_dead("telegram", "-1") is True and dead.is_dead("telegram", "-1")
    rich_sent_store.record("chat", 1, "text")
    assert rich_sent_store.lookup("chat", 1) is None
    assert _state_files(_home) == []


# --------------------------------------------------------------------------
# (f) every other backend keeps its files
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_state_files(monkeypatch, _home, backend):
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def no_postgres(*_args, **_kwargs):
        raise AssertionError("a non-authority profile must not reach PostgreSQL")

    monkeypatch.setattr(aux, "_open_postgres", no_postgres)
    from agent import estop
    from gateway import rich_sent_store
    from gateway.config import Platform
    from gateway.dead_targets import DeadTargetRegistry
    from gateway.pairing import PairingStore
    from gateway.platforms.helpers import ThreadParticipationTracker
    from hermes_cli import webhook
    from plugins.platforms.discord.adapter import (
        _DiscordNonConversationalMessageTracker,
    )

    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "operator")
    store = PairingStore()
    store.approve_code("telegram", store.generate_code("telegram", "111"))
    assert os.environ["TELEGRAM_ALLOWED_USERS"] == "operator,111"  # .env mirror kept
    ThreadParticipationTracker("discord").mark("t1")
    asyncio.run(_DiscordNonConversationalMessageTracker().mark_many(["m1"]))
    DeadTargetRegistry().mark_dead("telegram", "-1")
    rich_sent_store.record("chat", 1, "text")
    runner = _runner()
    runner._voice_mode[runner._voice_key(Platform.TELEGRAM, "42")] = "all"
    runner._save_voice_modes()
    estop.engage("x")
    webhook._store_subscription("deploys", _route("x"))

    assert _state_files(_home) == sorted(
        name for name in STATE_FILES if name != "pairing"
    )
    assert "telegram-approved.json" in os.listdir(_home / "platforms" / "pairing")


# --------------------------------------------------------------------------
# Two processes = two overlapping pods: each has its OWN HERMES_HOME (no shared
# file), only the PostgreSQL store is shared. The same scenario on the sqlite
# backend reproduces the inventory's problem (A-F11..A-F14); on authority it
# is gone.
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")


def _child(target, events, *args):
    try:
        target(events, *args)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


def _spawn_in_home(monkeypatch, home: Path, target, events, *args):
    """Start *target* in a child whose HERMES_HOME is *home* from its first import."""
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    process = _SPAWN.Process(target=_child, args=(target, events, *args))
    process.start()
    return process


def _await(events, done, *, timeout=120):
    got = []
    deadline = time.monotonic() + timeout
    while not done(got):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out; events so far: {got}"
        got.append(events.get(timeout=remaining))
        assert not [v for k, v in got if k == "error"], f"a child failed: {got}"
    return got


def _values(got, kind):
    return [value for event, value in got if event == kind]


def _stop(processes):
    for process in processes:
        process.join(30)
        if process.is_alive():
            process.kill()
            process.join(5)


def _old_pod(events, start, wrote, checked):
    """Pod A: already serving; writes state, then sees what pod B undid."""
    from agent import estop
    from gateway import rich_sent_store
    from gateway.config import Platform
    from gateway.dead_targets import DeadTargetRegistry
    from gateway.pairing import PairingStore
    from gateway.platforms.helpers import ThreadParticipationTracker
    from hermes_cli import webhook
    from plugins.platforms.discord.adapter import (
        _DiscordNonConversationalMessageTracker,
    )

    store, dead = PairingStore(), DeadTargetRegistry()
    events.put(("ready", "old"))
    start.wait(60)
    store.approve_code("telegram", store.generate_code("telegram", "111", "Alice"))
    ThreadParticipationTracker("discord").mark("thread-1")
    asyncio.run(_DiscordNonConversationalMessageTracker().mark_many(["status-1"]))
    dead.mark_dead("telegram", "-100", "Forbidden")
    rich_sent_store.record("chat", 7, "briefing")
    runner = _runner()
    runner._voice_mode[runner._voice_key(Platform.DISCORD, "vc")] = "all"
    runner._save_voice_modes()
    estop.engage("deploy freeze")
    webhook._store_subscription("deploys", _route("ship it"))
    wrote.set()
    checked.wait(60)
    events.put((
        "old-after",
        {
            "approved": store.is_approved("telegram", "111"),
            "dead": dead.is_dead("telegram", "-100"),
            "paused": estop.is_engaged(),
        },
    ))


def _new_pod(events, start, wrote, checked):
    """Pod B: started alongside A; reads what A wrote, then undoes some of it."""
    from agent import estop
    from gateway import rich_sent_store
    from gateway.dead_targets import DeadTargetRegistry
    from gateway.pairing import PairingStore
    from gateway.platforms.helpers import ThreadParticipationTracker
    from plugins.platforms.discord.adapter import (
        _DiscordNonConversationalMessageTracker,
    )

    store, dead = PairingStore(), DeadTargetRegistry()
    threads = ThreadParticipationTracker("discord")  # loaded before A marked
    noise = _DiscordNonConversationalMessageTracker()
    adapter = _webhook_adapter()
    events.put(("ready", "new"))
    wrote.wait(60)
    adapter._reload_dynamic_routes()
    events.put((
        "new-sees",
        {
            "approved": store.is_approved("telegram", "111"),
            "thread": "thread-1" in threads,
            "noise": "status-1" in noise,
            "dead": dead.is_dead("telegram", "-100"),
            "rich": rich_sent_store.lookup("chat", 7),
            "voice": _runner()._voice_mode.get("discord:vc"),
            "paused": estop.is_engaged(),
            "webhook": "deploys" in adapter._routes,
        },
    ))
    store.revoke("telegram", "111")
    dead.clear("telegram", "-100")
    estop.disengage()
    checked.set()


@pytest.mark.parametrize("backend", ["sqlite", "authority"])
def test_two_pods_share_gateway_state_only_through_postgres(
    backend, pg, tmp_path, monkeypatch
):
    monkeypatch.setenv("HERMES_STATE_BACKEND", backend)
    if backend == "authority":
        from hermes_state import SessionDB

        monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
        SessionDB(read_only=False).close()
    events = _SPAWN.Queue()
    start, wrote, checked = _SPAWN.Event(), _SPAWN.Event(), _SPAWN.Event()
    flags = (start, wrote, checked)
    processes = [
        _spawn_in_home(monkeypatch, tmp_path / "pod-a", _old_pod, events, *flags),
        _spawn_in_home(monkeypatch, tmp_path / "pod-b", _new_pod, events, *flags),
    ]
    try:
        # Both pods are up (B has loaded its state) before A writes anything.
        _await(events, lambda got: len(_values(got, "ready")) == 2)
        start.set()
        # Queue order across processes is not guaranteed: wait for both reports.
        got = _await(
            events,
            lambda got: _values(got, "new-sees") and _values(got, "old-after"),
        )
    finally:
        _stop(processes)

    seen, after = _values(got, "new-sees")[0], _values(got, "old-after")[0]
    if backend == "sqlite":
        # The inventory's problem: B knows nothing A decided, A keeps what B undid.
        assert seen == {
            "approved": False,
            "thread": False,
            "noise": False,
            "dead": False,
            "rich": None,
            "voice": None,
            "paused": False,
            "webhook": False,
        }
        assert after == {"approved": True, "dead": True, "paused": True}
        assert (tmp_path / "pod-a" / "ESTOP").exists()
    else:
        assert seen == {
            "approved": True,
            "thread": True,
            "noise": True,
            "dead": True,
            "rich": "briefing",
            "voice": "all",
            "paused": True,
            "webhook": True,
        }
        assert after == {"approved": False, "dead": False, "paused": False}
        for pod in ("pod-a", "pod-b"):
            assert _state_files(tmp_path / pod) == []


def _approver(events, start, prefix, count):
    from gateway.pairing import PairingStore
    from gateway.platforms.helpers import ThreadParticipationTracker

    store, threads = PairingStore(), ThreadParticipationTracker("discord")
    events.put(("ready", prefix))
    start.wait(60)
    for index in range(count):
        user = f"{prefix}{index}"
        code = store.generate_code("telegram", user)
        assert code, f"no code for {user}"
        assert store.approve_code("telegram", code), (
            f"pending request of {user} was lost"
        )
        threads.mark(f"{prefix}-thread-{index}")
    events.put(("done", prefix))


def test_two_pods_approving_at_once_lose_no_grant(authority, tmp_path, monkeypatch):
    """Each approval rewrites the platform's grant document; the pairing
    namespace lock keeps two pods' read-modify-writes from dropping one."""
    events, start = _SPAWN.Queue(), _SPAWN.Event()
    processes = [
        _spawn_in_home(monkeypatch, tmp_path / name, _approver, events, start, name, 12)
        for name in ("a", "b")
    ]
    try:
        _await(events, lambda got: len(_values(got, "ready")) == 2)
        start.set()
        _await(events, lambda got: len(_values(got, "done")) == 2)
    finally:
        _stop(processes)

    approved = json.loads(
        dict(_kv_rows(authority, aux.KV_PAIRING))["telegram-approved.json"]
    )
    assert sorted(approved) == sorted(f"{p}{i}" for p in "ab" for i in range(12))
    assert len(_kv_rows(authority, "threads:discord")) == 24


# --------------------------------------------------------------------------
# One-shot move of the files
# --------------------------------------------------------------------------


def _write_legacy_files(home: Path):
    for directory in ("platforms/pairing", "pairing", "gateway", "state"):
        (home / directory).mkdir(parents=True, exist_ok=True)
    files = {
        "platforms/pairing/telegram-approved.json": {"111": {"user_name": "new"}},
        "pairing/telegram-approved.json": {"111": {"user_name": "old"}, "222": {}},
        "discord_threads.json": ["t1", "t2"],
        "gateway/discord_nonconversational_messages.json": ["m1"],
        "gateway/dead_targets.json": {"telegram:-1": {"reason": "gone"}},
        "state/rich_sent_index.json": {
            "c:2": {"t": "new", "ts": 2},
            "c:1": {"t": "old", "ts": 1},
        },
        "gateway_voice_mode.json": {"telegram:1": "all", "legacy": "all", "x:1": "bad"},
        "webhook_subscriptions.json": {"deploys": _route("ship it")},
    }
    for name, data in files.items():
        (home / name).write_text(json.dumps(data), encoding="utf-8")
    (home / "ESTOP").write_text("", encoding="utf-8")  # a bare `touch` still pauses
    return {name: (home / name).read_bytes() for name in [*files, "ESTOP"]}


def test_kv_migration_is_idempotent_and_leaves_the_sources_untouched(
    authority, _home, monkeypatch
):
    from agent import estop
    from gateway import rich_sent_store
    from gateway.pairing import PairingStore
    from gateway.platforms.helpers import ThreadParticipationTracker
    from hermes_constants import get_hermes_dir

    monkeypatch.setenv("HERMES_PROFILE", "p0067")
    before = _write_legacy_files(_home)
    # Both layouts hold grants: the one the store would use wins, the other merges in.
    active = get_hermes_dir("platforms/pairing", "pairing").parent.name
    aux.aux_kv_put(aux.KV_VOICE_MODE, "telegram:1", "off")  # the live store wins

    dry = aux.migrate_aux_kv_to_pg("p0067", dry_run=True)
    assert dry["stores"]["voice_mode"]["status"] == "dry_run"
    assert aux.aux_kv_items(aux.KV_PAIRING) == []

    first = aux.migrate_aux_kv_to_pg("p0067", dry_run=False)
    # Every source entry of the first run is a core_aux_kv row afterwards: the
    # entries the files hold (counted here, not taken from the report) equal
    # both the report's source count and the rows that landed.
    namespaces = {
        "threads:discord": ("threads:discord", 2),
        "discord_nonconversational": (aux.KV_DISCORD_NONCONVERSATIONAL, 1),
        "dead_targets": (aux.KV_DEAD_TARGETS, 1),
        "rich_sent": (aux.KV_RICH_SENT, 2),
        "voice_mode": (aux.KV_VOICE_MODE, 1),  # "legacy" / "x:1": "bad" are skipped
        "estop": (aux.KV_ESTOP, 1),
        "webhook_subscriptions": (aux.KV_WEBHOOK_SUBSCRIPTIONS, 1),
    }
    for label, (namespace, entries) in namespaces.items():
        assert first["stores"][label]["source_rows"] == entries, label
        assert len(_kv_rows(authority, namespace)) == entries, label
        # Only voice_mode had a live row before the move (and keeps it).
        assert first["stores"][label]["inserted"] == (
            0 if label == "voice_mode" else entries
        ), label
    pairing_rows = dict(_kv_rows(authority, aux.KV_PAIRING))
    assert sorted(pairing_rows) == ["telegram-approved.json"]
    assert sorted(json.loads(pairing_rows["telegram-approved.json"])) == ["111", "222"]
    second = aux.migrate_aux_kv_to_pg("p0067", dry_run=False)
    assert all(s["status"] in {"migrated", "missing"} for s in first["stores"].values())
    assert [s.get("inserted", 0) for s in second["stores"].values()] == [0] * len(
        second["stores"]
    )
    assert first["stores"]["threads:discord"]["inserted"] == 2
    assert {
        label: report["inserted"]
        for label, report in first["stores"].items()
        if label.startswith("pairing:")
    } == {
        "pairing:platforms/pairing/telegram-approved.json": 1
        if active == "platforms"
        else 0,
        "pairing:pairing/telegram-approved.json": 2 if active != "platforms" else 1,
    }
    assert PairingStore().list_approved("telegram") == [
        {
            "platform": "telegram",
            "user_id": "111",
            "user_name": "new" if active == "platforms" else "old",
        },
        {"platform": "telegram", "user_id": "222"},
    ]
    assert "t2" in ThreadParticipationTracker("discord")
    assert rich_sent_store.lookup("c", 1) == "old"
    assert [k for k, _ in _kv_rows(authority, aux.KV_RICH_SENT)] == ["c:1", "c:2"]
    assert dict(aux.aux_kv_items(aux.KV_VOICE_MODE)) == {"telegram:1": "off"}
    assert estop.is_engaged() and estop.get_state()["reason"] is None
    assert {name: (_home / name).read_bytes() for name in before} == before

    with pytest.raises(ValueError, match="not the active profile"):
        aux.migrate_aux_kv_to_pg("someone-else", dry_run=True)


def test_kv_migration_rolls_back_a_bad_file_and_reports_missing_ones(
    authority, _home, monkeypatch
):
    monkeypatch.setenv("HERMES_PROFILE", "p0067")
    report = aux.migrate_aux_kv_to_pg("p0067", dry_run=False)
    assert {s["status"] for s in report["stores"].values()} == {"missing"}

    (_home / "gateway_voice_mode.json").write_text(
        '{"telegram:1": "all"}', encoding="utf-8"
    )
    (_home / "webhook_subscriptions.json").write_text("[not json", encoding="utf-8")
    with pytest.raises(aux.AuxMigrationError, match="webhook_subscriptions.json"):
        aux.migrate_aux_kv_to_pg("p0067", dry_run=False)
    assert aux.aux_kv_items(aux.KV_VOICE_MODE) == []  # rolled back with the bad file
