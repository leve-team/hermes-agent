"""levos 0061 — one messaging connection per profile credential across pods.

Two overlapping pods of one profile share nothing but the profile's
PostgreSQL store. On ``HERMES_STATE_BACKEND=authority`` the adapters' token
lock is the profile's session advisory lock (``gateway.status_pg_locks``), taken
before the platform transport opens and released only after it closed; no
file is written under ``gateway-locks/``. Telegram keeps the Bot API's queued
updates on a cold boot and on a 409 retry under that lock. Every other
backend keeps the file lock and its behaviour.

The two-process tests give each "pod" its own ``HERMES_HOME`` and lock
directory (no shared file) and fake only the platform transports: a
``commands.Bot`` / Socket Mode handler that records when it opens and closes,
and a local Bot API server with Telegram's long-poll queue. Runs on the
fork's ephemeral PostgreSQL (``initdb``/``pg_ctl`` on PATH or in
``PG3_PERCENT_PG_BIN``; missing tools are errors, never skips).
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs

import psycopg
import pytest

import hermes_aux_store as aux
from gateway import status, status_pg_locks
from gateway.platforms import platform_lock
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0061&connect_timeout=1"
)
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_PROFILE",
    "HERMES_GATEWAY_LOCK_DIR",
)
_PROXY_ENV = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
TOKENS = {
    "discord": "discord-token-0061",
    "slack": "xoxb-0061",
    "telegram": "123456:telegram-token-0061",
}
LOCK_CODES = {
    "discord": "discord-bot-token_lock",
    "slack": "slack-app-token_lock",
    "telegram": "telegram-bot-token_lock",
}
TRANSPORT_CLOSE_SECONDS = 0.5


@pytest.fixture(autouse=True)
def _backend_env(monkeypatch, tmp_path):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "gateway-locks"))
    yield
    with status_pg_locks._POSTGRES_SCOPED_LOCKS_GUARD:
        held = list(status_pg_locks._POSTGRES_SCOPED_LOCKS.values())
        status_pg_locks._POSTGRES_SCOPED_LOCKS.clear()
    for entry in held:
        entry.lock.release()


@pytest.fixture
def authority(monkeypatch, postgres_dsn):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", postgres_dsn)
    return postgres_dsn


def _lock_key(dsn, scope, identity):
    with psycopg.connect(dsn) as raw:
        schema = raw.execute("SELECT COALESCE(current_schema(), 'public')").fetchone()[
            0
        ]
    return aux.aux_lock_key(schema, status_pg_locks.postgres_scoped_lock_name(scope, identity))


def _files(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


# --------------------------------------------------------------------------
# gateway.status: the scoped lock on authority
# --------------------------------------------------------------------------


def test_authority_scoped_lock_is_an_advisory_lock_and_writes_no_file(
    authority, tmp_path
):
    key = _lock_key(authority, "telegram-bot-token", "tok")
    lock_dir = tmp_path / "gateway-locks"

    assert status.acquire_scoped_lock("telegram-bot-token", "tok") == (True, None)
    with psycopg.connect(authority, autocommit=True) as other:
        # Another session (= another pod) cannot take it ...
        assert (
            other.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
            is False
        )
        # ... this process re-enters it per owner, like the file lock's same-PID rule.
        first, second = object(), object()
        assert status_pg_locks.acquire_postgres_scoped_lock(
            "telegram-bot-token", "tok", owner=first
        )[0]
        assert status_pg_locks.acquire_postgres_scoped_lock(
            "telegram-bot-token", "tok", owner=second
        )[0]
        status_pg_locks.release_postgres_scoped_lock("telegram-bot-token", "tok", owner=first)
        status.release_scoped_lock("telegram-bot-token", "tok")
        assert (
            other.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
            is False
        )
        status_pg_locks.release_postgres_scoped_lock("telegram-bot-token", "tok", owner=second)
        assert (
            other.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
            is True
        )
        acquired, record = status.acquire_scoped_lock("telegram-bot-token", "tok")
        assert acquired is False
        assert record == {
            "scope": "telegram-bot-token",
            "identity_hash": status._scope_hash("tok"),
            "backend": "postgres",
        }
    assert not lock_dir.exists() or _files(lock_dir) == []


def test_authority_scoped_lock_without_postgres_raises_and_writes_no_file(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    with pytest.raises(aux.AuxStoreUnavailable) as caught:
        status.acquire_scoped_lock("discord-bot-token", "tok")
    assert "nobody" not in str(caught.value) and "nonexistent" not in str(caught.value)
    assert not (tmp_path / "gateway-locks").exists()

    adapter = _LockOnlyAdapter()
    assert (
        adapter._acquire_platform_lock("discord-bot-token", "tok", "Discord bot token")
        is False
    )
    assert adapter.fatal_error_code == "discord-bot-token_lock"
    assert adapter.fatal_error_retryable is True
    assert not (tmp_path / "gateway-locks").exists()


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_the_file_lock(monkeypatch, tmp_path, backend):
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)

    def no_postgres(*_args, **_kwargs):
        raise AssertionError("PostgreSQL lock used outside authority")

    monkeypatch.setattr(status_pg_locks, "acquire_postgres_scoped_lock", no_postgres)
    adapter = _LockOnlyAdapter()
    assert (
        adapter._acquire_platform_lock("slack-app-token", "tok", "Slack app token")
        is True
    )
    assert adapter._platform_lock_fences_pods is False
    assert len(_files(tmp_path / "gateway-locks")) == 1
    adapter._release_platform_lock()
    assert _files(tmp_path / "gateway-locks") == []


class _LockOnlyAdapter(BasePlatformAdapter):
    """Just the base lock plumbing; no transport."""

    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="tok"), Platform.SLACK)

    async def connect(self, *, is_reconnect: bool = False) -> bool:  # pragma: no cover
        return True

    async def disconnect(self) -> None:  # pragma: no cover
        self._release_platform_lock()

    async def send(
        self, chat_id, content, reply_to=None, metadata=None
    ):  # pragma: no cover
        raise NotImplementedError

    async def get_chat_info(self, chat_id):  # pragma: no cover
        return {}


@pytest.mark.asyncio
async def test_lost_session_is_retaken_when_free_and_disconnects_when_taken(
    authority, monkeypatch
):
    """A dropped lock session frees the lock server-side. The holder takes it
    again when nobody else did; when another pod took it, the adapter goes
    through the retryable fatal path (the runner disconnects and requeues)."""
    monkeypatch.setattr(platform_lock, "_PLATFORM_LOCK_CHECK_SECONDS", 0.05)
    adapter = _LockOnlyAdapter()
    fatal = AsyncMock()
    adapter.set_fatal_error_handler(fatal)
    key = _lock_key(authority, "slack-app-token", "tok")

    def kill_lock_session():
        entry = status_pg_locks._POSTGRES_SCOPED_LOCKS[
            ("slack-app-token", status._scope_hash("tok"))
        ]
        backend_pid = entry.lock._conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        with psycopg.connect(authority, autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))
        return entry

    assert (
        adapter._acquire_platform_lock("slack-app-token", "tok", "Slack app token")
        is True
    )
    assert adapter._platform_lock_fences_pods is True

    entry = kill_lock_session()
    assert entry.lock.held() is False
    for _ in range(100):
        await asyncio.sleep(0.02)
        if entry.lock.held():
            break
    assert entry.lock.held() is True  # retaken on a new session
    fatal.assert_not_awaited()

    kill_lock_session()
    with psycopg.connect(authority, autocommit=True) as other:
        assert (
            other.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
            is True
        )
        for _ in range(100):
            await asyncio.sleep(0.02)
            if fatal.await_count:
                break
        fatal.assert_awaited_once_with(adapter)
        assert adapter.fatal_error_code == "slack-app-token_lock_lost"
        assert adapter.fatal_error_retryable is True
        adapter._release_platform_lock()


@pytest.mark.asyncio
async def test_telegram_conflict_retry_keeps_the_queue_under_the_postgres_lock(
    monkeypatch,
):
    """409 retry: drop_pending_updates=True (#75017) only without the PG lock."""
    from plugins.platforms.telegram.adapter import TelegramAdapter

    async def instant(_seconds):
        return None

    captured = []

    async def start_polling_once(app, *, drop_pending_updates, error_callback, **_):
        captured.append(drop_pending_updates)

    for fenced in (False, True):
        adapter = TelegramAdapter(PlatformConfig(enabled=True, token="***"))
        adapter._platform_lock_postgres = fenced
        adapter._app = SimpleNamespace(updater=SimpleNamespace(running=False))
        monkeypatch.setattr(adapter, "_start_polling_once", start_polling_once)
        monkeypatch.setattr(adapter, "_drain_polling_connections", AsyncMock())
        monkeypatch.setattr("plugins.platforms.telegram.adapter.asyncio.sleep", instant)
        await adapter._handle_polling_conflict(RuntimeError("Conflict"))
        assert adapter._drop_pending_on_connect(False) is (not fenced)
        assert adapter._drop_pending_on_connect(True) is False
    assert captured == [True, False]


# --------------------------------------------------------------------------
# Two pods: each child has its own HERMES_HOME and lock directory; only the
# PostgreSQL store (authority) is shared.
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")


class _Pod:
    def __init__(self, name, platform, root, backend, dsn, events, extra=None):
        self.name = name
        self.commands = _SPAWN.Queue()
        self.process = _SPAWN.Process(
            target=_pod_main,
            args=(
                name,
                platform,
                str(root / name),
                backend,
                dsn,
                self.commands,
                events,
                extra or {},
            ),
        )
        self.process.start()

    def send(self, *command):
        self.commands.put(command)

    def stop(self):
        self.commands.put(("exit",))
        self.process.join(30)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(5)


def _pod_main(name, platform, home, backend, dsn, commands, events, extra):
    try:
        _pod_env(Path(home), backend, dsn)
        asyncio.run(_pod_loop(name, platform, commands, events, extra))
    except BaseException as exc:
        events.put(("error", name, f"{type(exc).__name__}: {exc}"))
        raise


def _pod_env(home: Path, backend, dsn):
    home.mkdir(parents=True, exist_ok=True)
    for key in (*_BACKEND_ENV, *_PROXY_ENV):
        os.environ.pop(key, None)
    os.environ["HERMES_HOME"] = str(home)
    os.environ["HERMES_TEST_ISOLATION"] = str(home)
    os.environ["HERMES_GATEWAY_LOCK_DIR"] = str(home / "gateway-locks")
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = "127.0.0.1,localhost"
    os.environ["HERMES_STATE_BACKEND"] = backend
    if backend == "authority":
        os.environ["HERMES_STATE_POSTGRES_DSN"] = dsn
    os.environ["DISCORD_COMMAND_SYNC_POLICY"] = "off"
    os.environ["SLACK_APP_TOKEN"] = "xapp-0061"
    os.environ["HERMES_TELEGRAM_DISABLE_FALLBACK_IPS"] = "1"


async def _pod_loop(name, platform, commands, events, extra):
    loop = asyncio.get_running_loop()
    adapter = None
    while True:
        command, *args = await loop.run_in_executor(None, commands.get)
        if command == "exit":
            if adapter is not None and adapter.is_connected:
                await adapter.disconnect()
            return
        if command == "connect":
            # A fresh adapter per attempt, as the gateway's reconnect watcher does.
            (is_reconnect,) = args
            adapter = _ADAPTERS[platform](name, events, extra)
            ok = await adapter.connect(is_reconnect=is_reconnect)
            events.put(("connect", name, ok, adapter.fatal_error_code))
        elif command == "disconnect":
            await adapter.disconnect()
            events.put(("disconnected", name, time.time()))


def _report_unlock(adapter_class):
    """Subclass that reports whether its transport was closed at unlock time."""

    class Reporting(adapter_class):
        def _release_platform_lock(self):
            if getattr(self, "_platform_lock_identity", None):
                self._pod_events.put((
                    "unlock",
                    self._pod_name,
                    time.time(),
                    self._pod_transport_closed(),
                ))
            super()._release_platform_lock()

    return Reporting


def _discord_adapter(name, events, _extra):
    import plugins.platforms.discord.adapter as module

    class PodBot:
        """A commands.Bot whose gateway session is a local stand-in."""

        def __init__(self, *, intents=None, **_kwargs):
            self.intents = intents
            self.application_id = 999
            self.user = SimpleNamespace(id=999, name=name)
            self.tree = SimpleNamespace(
                command=lambda *a, **k: lambda fn: fn,
                add_command=lambda *a, **k: None,
                get_commands=lambda *a, **k: [],
            )
            self.closed = False
            self._stop = asyncio.Event()

        def event(self, fn):
            setattr(self, fn.__name__, fn)
            return fn

        def is_closed(self):
            return self.closed

        def is_ready(self):
            return not self.closed

        async def start(self, _token):
            events.put(("open", name, time.time()))
            await self.on_ready()
            await self._stop.wait()

        async def close(self):
            await asyncio.sleep(TRANSPORT_CLOSE_SECONDS)
            self.closed = True
            self._stop.set()
            events.put(("closed", name, time.time()))

    bots = []

    def make_bot(**kwargs):
        bots.append(PodBot(**kwargs))
        return bots[-1]

    module.commands.Bot = make_bot
    adapter = _report_unlock(module.DiscordAdapter)(
        PlatformConfig(enabled=True, token=TOKENS["discord"])
    )
    adapter._liveness_interval_seconds = 0
    adapter._pod_name, adapter._pod_events = name, events
    adapter._pod_transport_closed = lambda: all(bot.closed for bot in bots)
    return adapter


def _slack_adapter(name, events, _extra):
    import plugins.platforms.slack.adapter as module

    handlers = []

    class PodSocketModeHandler:
        def __init__(self, app, app_token, proxy=None):
            self.client = SimpleNamespace(is_connected=lambda: True, proxy=proxy)
            self.closed = False
            self._stop = asyncio.Event()
            handlers.append(self)

        async def start_async(self):
            events.put(("open", name, time.time()))
            await self._stop.wait()

        async def close_async(self):
            await asyncio.sleep(TRANSPORT_CLOSE_SECONDS)
            self.closed = True
            self._stop.set()
            events.put(("closed", name, time.time()))

    def decorator(*_args, **_kwargs):
        return lambda fn: fn

    app = MagicMock()
    app.event = app.command = app.action = app.shortcut = app.view = decorator
    app.client = AsyncMock()
    web_client = AsyncMock()
    web_client.auth_test = AsyncMock(
        return_value={
            "user_id": "U0061",
            "user": name,
            "team_id": "T0061",
            "team": "pods",
        }
    )
    module.AsyncApp = lambda *a, **k: app
    module.AsyncWebClient = lambda *a, **k: web_client
    module.AsyncSocketModeHandler = PodSocketModeHandler
    adapter = _report_unlock(module.SlackAdapter)(
        PlatformConfig(enabled=True, token=TOKENS["slack"])
    )
    adapter._pod_name, adapter._pod_events = name, events
    adapter._pod_transport_closed = lambda: all(h.closed for h in handlers)
    return adapter


def _telegram_adapter(name, events, extra):
    from plugins.platforms.telegram.adapter import TelegramAdapter

    adapter = _report_unlock(TelegramAdapter)(
        PlatformConfig(
            enabled=True,
            token=TOKENS["telegram"],
            extra={"base_url": f"{extra['bot_api']}/{name}/bot"},
        )
    )

    async def ignore(_event):
        return None

    adapter.set_message_handler(ignore)
    adapter._pod_name, adapter._pod_events = name, events

    def transport_closed():
        app = adapter._app
        return app is None or not (app.updater and app.updater.running)

    adapter._pod_transport_closed = transport_closed
    return adapter


_ADAPTERS = {
    "discord": _discord_adapter,
    "slack": _slack_adapter,
    "telegram": _telegram_adapter,
}


def _await(events, got, done, *, timeout=60):
    """Append events to *got* until ``done(got)`` holds."""
    deadline = time.monotonic() + timeout
    while not done(got):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out; events so far: {got}"
        try:
            got.append(events.get(timeout=min(remaining, 1.0)))
        except Exception:
            continue
        assert not [e for e in got if e[0] == "error"], f"a pod failed: {got}"
    return got


def _drain(events, got):
    """Append whatever is already queued."""
    while True:
        try:
            got.append(events.get(timeout=0.2))
        except Exception:
            return got


def _of(got, kind, name=None):
    return [e for e in got if e[0] == kind and (name is None or e[1] == name)]


class FakeBotApi:
    """Telegram's getUpdates queue, per bot token, served to path-prefixed pods.

    Model: an update stays queued until a later getUpdates confirms it with
    ``offset``; a new getUpdates ends a still-waiting one with 409 Conflict;
    ``deleteWebhook(drop_pending_updates=true)`` empties the queue. Long polls
    are held for at most ``HOLD`` seconds (Telegram holds up to ``timeout``).
    """

    HOLD = 1.0

    def __init__(self):
        self.cond = threading.Condition()
        self.pending = []
        self.next_id = 1
        self.waiter = 0
        self.poller = None
        self.polls = []  # (pod, time)
        self.conflicts = []  # (terminated pod, terminating pod)
        self.served = {}  # update_id -> [pods]
        self.dropped = []
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return None

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length).decode("utf-8", "replace")
                if "json" in (self.headers.get("Content-Type") or ""):
                    params = json.loads(raw or "{}")
                else:
                    params = {k: v[0] for k, v in parse_qs(raw).items()}
                _, pod, _token, method = self.path.split("/", 3)
                code, body = api.call(pod, method, params)
                payload = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def post(self):
        with self.cond:
            update_id = self.next_id
            self.next_id += 1
            self.pending.append({
                "update_id": update_id,
                "message": {
                    "message_id": update_id,
                    "date": int(time.time()),
                    "chat": {"id": 61, "type": "private"},
                    "from": {"id": 61, "is_bot": False, "first_name": "u"},
                    "text": f"m{update_id}",
                },
            })
            self.cond.notify_all()
        return update_id

    def call(self, pod, method, params):
        method = method.lower()
        if method == "getme":
            return 200, {
                "ok": True,
                "result": {
                    "id": 123456,
                    "is_bot": True,
                    "first_name": "Pods",
                    "username": "pods_bot",
                },
            }
        if method == "deletewebhook":
            if str(params.get("drop_pending_updates", "")).lower() in ("true", "1"):
                with self.cond:
                    self.dropped += [u["update_id"] for u in self.pending]
                    self.pending.clear()
            return 200, {"ok": True, "result": True}
        if method == "getupdates":
            return self._get_updates(pod, params)
        return 200, {"ok": True, "result": True}

    def _get_updates(self, pod, params):
        offset = int(params.get("offset") or 0)
        timeout = float(params.get("timeout") or 0)
        with self.cond:
            if offset:
                self.pending = [u for u in self.pending if u["update_id"] >= offset]
            self.waiter += 1
            self.poller = pod
            me = self.waiter
            self.polls.append((pod, time.time()))
            self.cond.notify_all()
            deadline = time.monotonic() + min(timeout, self.HOLD)
            while True:
                if self.waiter != me:
                    self.conflicts.append((pod, self.poller))
                    return 409, {
                        "ok": False,
                        "error_code": 409,
                        "description": (
                            "Conflict: terminated by other getUpdates request; "
                            "make sure that only one bot instance is running"
                        ),
                    }
                if self.pending:
                    for update in self.pending:
                        self.served.setdefault(update["update_id"], []).append(pod)
                    return 200, {"ok": True, "result": list(self.pending)}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return 200, {"ok": True, "result": []}
                self.cond.wait(remaining)

    def wait_served(self, update_ids, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.cond:
                if all(u in self.served or u in self.dropped for u in update_ids):
                    return
            time.sleep(0.1)


def _cross_pod(conflicts):
    """409s one pod's poll dealt the other's (a pod's own follow-up poll
    superseding its abandoned one is not a competing poller)."""
    return [(victim, by) for victim, by in conflicts if victim != by]


@pytest.fixture
def bot_api():
    api = FakeBotApi()
    yield api
    api.close()


def _pods(platform, tmp_path, backend, dsn, events, bot_api=None):
    extra = {"bot_api": bot_api.url} if bot_api else {}
    return [
        _Pod(name, platform, tmp_path / "pods", backend, dsn, events, extra)
        for name in ("old", "new")
    ]


@pytest.mark.parametrize("backend", ["sqlite", "authority"])
@pytest.mark.parametrize("platform", ["discord", "slack", "telegram"])
def test_two_pods_open_one_transport_per_credential(
    platform, backend, tmp_path, postgres_dsn, bot_api
):
    """B-F02/B-F03/B-F09 (+B-F04's trigger): the new pod boots while the old
    one is connected. With per-pod lock files both open the same credential;
    under the profile's PostgreSQL lock only the holder does."""
    events, got = _SPAWN.Queue(), []
    old, new = _pods(platform, tmp_path, backend, postgres_dsn, events, bot_api)
    try:
        old.send("connect", False)
        _await(events, got, lambda got: _of(got, "connect", "old"))
        polls_before = len(bot_api.polls)
        new.send("connect", False)
        _await(events, got, lambda got: _of(got, "connect", "new"))
        time.sleep(3)  # let a second poller (if any) meet the first
        new_polls = [pod for pod, _ in bot_api.polls[polls_before:] if pod == "new"]
        _drain(events, got)
    finally:
        old.stop()
        new.stop()

    ((_, _, new_ok, new_code),) = _of(got, "connect", "new")
    opened = {e[1] for e in _of(got, "open")}
    if backend == "sqlite":
        assert new_ok is True
        if platform == "telegram":
            assert new_polls and _cross_pod(
                bot_api.conflicts
            )  # two pollers, 409 served
        else:
            assert opened == {"old", "new"}  # two sessions on one token
    else:
        assert (new_ok, new_code) == (False, LOCK_CODES[platform])
        assert new_polls == [] and _cross_pod(bot_api.conflicts) == []
        if platform != "telegram":
            assert opened == {"old"}
    assert (
        not (tmp_path / "pods" / "old" / "gateway-locks").exists()
        or backend == "sqlite"
    )


@pytest.mark.parametrize("platform", ["discord", "slack", "telegram"])
def test_handoff_unlocks_only_after_the_transport_closed(
    platform, tmp_path, authority, bot_api
):
    """Drain: the old pod closes its transport, then unlocks; the new pod's
    next attempt connects. Neither pod ever writes a lock file."""
    events, got = _SPAWN.Queue(), []
    old, new = _pods(platform, tmp_path, "authority", authority, events, bot_api)
    try:
        old.send("connect", False)
        _await(events, got, lambda got: _of(got, "connect", "old"))
        new.send("connect", False)
        _await(events, got, lambda got: _of(got, "connect", "new"))
        old.send("disconnect")
        _await(events, got, lambda got: _of(got, "disconnected", "old"))
        new.send("connect", True)
        _await(events, got, lambda got: len(_of(got, "connect", "new")) == 2)
        if platform != "telegram":
            _await(events, got, lambda got: _of(got, "open", "new"))
    finally:
        old.stop()
        new.stop()

    assert [e[2] for e in _of(got, "connect", "old")] == [True]
    assert [e[2] for e in _of(got, "connect", "new")] == [False, True]
    ((_, _, unlocked_at, transport_closed),) = _of(got, "unlock", "old")
    assert transport_closed is True, "the old pod unlocked while its transport was open"
    if platform != "telegram":
        ((_, _, closed_at),) = _of(got, "closed", "old")
        ((_, _, opened_at),) = _of(got, "open", "new")
        assert closed_at <= unlocked_at < opened_at
    for pod in ("old", "new"):
        assert not (tmp_path / "pods" / pod / "gateway-locks").exists()


@pytest.mark.parametrize("backend", ["sqlite", "authority"])
def test_telegram_cold_boot_after_a_handoff_keeps_the_queued_messages(
    backend, tmp_path, postgres_dsn, bot_api
):
    """B-F04 cold boot: messages sent between the old pod's disconnect and the
    new pod's first connect wait in the Bot API queue. The file-lock install
    still drops them on a cold boot; under the PostgreSQL lock they arrive."""
    events, got = _SPAWN.Queue(), []
    old, new = _pods("telegram", tmp_path, backend, postgres_dsn, events, bot_api)
    try:
        old.send("connect", False)
        _await(events, got, lambda got: _of(got, "connect", "old"))
        before = bot_api.post()
        bot_api.wait_served([before])
        old.send("disconnect")
        _await(events, got, lambda got: _of(got, "disconnected", "old"))
        gap = [bot_api.post(), bot_api.post()]
        new.send("connect", False)
        _await(events, got, lambda got: _of(got, "connect", "new"))
        bot_api.wait_served(gap)
    finally:
        old.stop()
        new.stop()

    assert _of(got, "connect", "new")[0][2] is True
    assert bot_api.served[before] == ["old"]
    if backend == "sqlite":
        assert bot_api.dropped == gap  # reproduced: the cold boot deleted them
    else:
        assert bot_api.dropped == []
        assert [bot_api.served.get(u, [None])[0] for u in gap] == ["new", "new"]
