"""levos 0066 — the credential store follows the profile's PostgreSQL authority.

On ``HERMES_STATE_BACKEND=authority`` the profile ``auth.json`` (credential
pool included) and the root ``auth.json`` are rows of ``core_auth_store`` and
the auth-store lock is a PostgreSQL advisory lock, so two pods of one profile
share one single-use refresh-token chain: one of them refreshes, the other
re-reads under the lock and adopts the rotated token. Nothing is read from or
written to ``auth.json`` / ``auth.lock`` there. Every other backend keeps its
files and flocks.

Each "pod" below gets its own HERMES_HOME tree (``<tmp>/<pod>/hermes/profiles/
dave``, own HOME and CODEX_HOME), so the two processes share no file — only
PostgreSQL, as overlapping pods on emptyDir do. The token endpoint is a local
fake that burns each refresh token on first use, like the Codex endpoint.

Runs on the fork's ephemeral, Unix-socket-only PostgreSQL (``initdb`` /
``pg_ctl`` on PATH or in ``PG3_PERCENT_PG_BIN``; missing tools are errors,
never skips) — the same fixture and spawn pattern as 0060.
"""

from __future__ import annotations

import base64
import hashlib
import json
import multiprocessing
import os
import signal
import stat
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

import psycopg
import pytest

import hermes_aux_store as aux
from hermes_cli import auth
from tests.test_pg3_writer_local_follow_backend import postgres_dsn as postgres_dsn

UNREACHABLE_DSN = (
    "postgresql://nobody@/postgres?host=/nonexistent-0066&connect_timeout=1"
)
_BACKEND_ENV = (
    "HERMES_STATE_BACKEND",
    "HERMES_STATE_DATABASE_URL",
    "HERMES_STATE_POSTGRES_DSN",
    "HERMES_CORE_PG_DSN",
    "HERMES_STATE_DUAL_WRITE",
    "HERMES_AUX_DB_DIR",
    "HERMES_PROFILE",
    "HERMES_SHARED_AUTH_DIR",
    "HERMES_CODEX_BASE_URL",
)
_FILE_ARTIFACTS = ("auth.json", "auth.lock", ".corrupt", ".tmp.")


def _jwt(exp: float, serial: int) -> str:
    """An unsigned JWT-shaped access token; only ``exp`` is read."""

    def part(value):
        raw = json.dumps(value, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return f"{part({'alg': 'none'})}.{part({'exp': int(exp), 'n': serial})}.sig"


def _codex_store(refresh_token: str, *, expired: bool = True) -> dict:
    exp = time.time() - 60 if expired else time.time() + 3600
    return {
        "version": 1,
        "active_provider": "openai-codex",
        "providers": {
            "openai-codex": {
                "tokens": {
                    "access_token": _jwt(exp, 0),
                    "refresh_token": refresh_token,
                },
                "last_refresh": "2026-09-01T00:00:00Z",
                "auth_mode": "chatgpt",
            }
        },
    }


def _make_pod(root: Path, name: str, seed: dict | None = None) -> dict:
    """A pod's private disk; *seed* is the auth.json the pod boots with."""
    pod = root / name
    home = pod / "hermes" / "profiles" / "dave"
    home.mkdir(parents=True)
    (pod / "codex").mkdir()
    if seed is not None:
        (home / "auth.json").write_text(json.dumps(seed, indent=2) + "\n")
    return {
        "HERMES_HOME": str(home),
        "HERMES_PROFILE": "dave",
        "HOME": str(pod),
        "CODEX_HOME": str(pod / "codex"),
        "NO_PROXY": "127.0.0.1",
    }


def _enter(monkeypatch, pod: dict) -> None:
    for key, value in pod.items():
        monkeypatch.setenv(key, value)


def _file_artifacts(root: Path):
    return sorted(
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and any(marker in p.name for marker in _FILE_ARTIFACTS)
    )


def _row(dsn, name):
    with psycopg.connect(dsn) as raw:
        try:
            found = raw.execute(
                "SELECT document FROM core_auth_store WHERE name = %s", (name,)
            ).fetchone()
        except psycopg.errors.UndefinedTable:
            return None
    return found[0] if found else None


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in _BACKEND_ENV:
        monkeypatch.delenv(key, raising=False)
    auth.invalidate_nous_auth_status_cache()
    yield
    auth.invalidate_nous_auth_status_cache()


@pytest.fixture
def pg(postgres_dsn):
    with psycopg.connect(postgres_dsn, autocommit=True) as raw:
        raw.execute("DROP TABLE IF EXISTS core_auth_store")
    return postgres_dsn


@pytest.fixture
def authority(monkeypatch, pg):
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
    return pg


# --------------------------------------------------------------------------
# (a) round trip on PostgreSQL, no auth.json / auth.lock anywhere
# --------------------------------------------------------------------------


def test_authority_roundtrips_profile_root_and_pool_without_files(
    authority, tmp_path, monkeypatch
):
    pod = _make_pod(tmp_path, "podA")
    _enter(monkeypatch, pod)
    assert auth._load_auth_store() == {"version": 1, "providers": {}}
    assert auth._load_global_auth_store() == {}

    with auth._auth_store_lock():
        with auth._auth_store_lock():  # reentrant in one thread, as the flock
            store = auth._load_auth_store()
            store["providers"]["openai-codex"] = {"tokens": {"refresh_token": "rt-a"}}
            store["active_provider"] = "openai-codex"
            saved_at = auth._save_auth_store(store)
    assert saved_at == Path(pod["HERMES_HOME"]) / "auth.json"  # nominal, unwritten
    # The row holds exactly what auth.json would have held.
    assert _row(authority, "profile") == json.dumps(store, indent=2) + "\n"
    assert auth._load_auth_store()["providers"]["openai-codex"]["tokens"] == {
        "refresh_token": "rt-a"
    }

    # Root store: read fallback for providers the profile lacks, and the
    # write-through of a root-sourced grant lands in the root row only.
    root_path = auth._global_auth_file_path()
    assert root_path == tmp_path / "podA" / "hermes" / "auth.json"
    auth._persist_provider_state_to_store(
        "xai-oauth", {"refresh_token": "x-1"}, root_path
    )
    assert auth._load_global_auth_store()["providers"] == {
        "xai-oauth": {"refresh_token": "x-1"}
    }
    assert auth._load_provider_state(auth._load_auth_store(), "xai-oauth") == {
        "refresh_token": "x-1"
    }
    assert "xai-oauth" not in json.loads(_row(authority, "profile"))["providers"]

    # The credential pool is a key of the same document.
    auth.write_credential_pool(
        "openrouter", [{"id": "p1", "source": "manual", "access_token": "k"}]
    )
    assert [e["id"] for e in auth.read_credential_pool("openrouter")] == ["p1"]
    assert "openrouter" in json.loads(_row(authority, "profile"))["credential_pool"]

    with pytest.raises(ValueError, match="neither the profile nor the root"):
        auth._load_auth_store(tmp_path / "elsewhere" / "auth.json")
    assert _file_artifacts(tmp_path) == []


def test_authority_refuses_a_corrupt_document(authority, tmp_path, monkeypatch):
    _enter(monkeypatch, _make_pod(tmp_path, "podA"))
    auth._save_auth_store({"providers": {}})
    with psycopg.connect(authority, autocommit=True) as raw:
        raw.execute("UPDATE core_auth_store SET document = '{broken'")
    with pytest.raises(ValueError, match="not valid JSON"):
        auth._load_auth_store()
    assert _file_artifacts(tmp_path) == []


def test_authority_without_postgres_raises_and_creates_no_file(tmp_path, monkeypatch):
    _enter(monkeypatch, _make_pod(tmp_path, "podA", seed=_codex_store("rt-seed")))
    monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
    monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", UNREACHABLE_DSN)
    seed = Path(os.environ["HERMES_HOME"]) / "auth.json"
    before = seed.read_bytes()

    calls = (
        auth._load_auth_store,
        lambda: auth._save_auth_store({"providers": {}}),
        lambda: auth._auth_store_lock().__enter__(),
        lambda: auth.resolve_codex_runtime_credentials(),
    )
    for call in calls:
        with pytest.raises(aux.AuxStoreUnavailable) as caught:
            call()
        assert "nonexistent-0066" not in str(caught.value)
    # The read-only root fallback keeps its "never breaks a profile read" contract.
    assert auth._load_global_auth_store() == {}
    # The seed file on the pod disk is neither read as a fallback nor touched.
    assert seed.read_bytes() == before
    assert _file_artifacts(tmp_path) == ["podA/hermes/profiles/dave/auth.json"]


# --------------------------------------------------------------------------
# (b) the lock fences other processes and threads, dies with its holder
# --------------------------------------------------------------------------

_SPAWN = multiprocessing.get_context("spawn")


def _child(target, pod, events, *args):
    """Process entry point: become *pod*, run *target*, report failures."""
    os.environ.update(pod)
    try:
        target(events, *args)
    except BaseException as exc:
        events.put(("error", f"{type(exc).__name__}: {exc}"))
        raise


def _spawn(target, pod, events, *args):
    process = _SPAWN.Process(target=_child, args=(target, pod, events, *args))
    process.start()
    return process


def _await(events, done, *, timeout=120):
    got = []
    deadline = time.monotonic() + timeout
    while not done(got):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"timed out; events so far: {got}"
        got.append(events.get(timeout=remaining))
        assert not _kinds(got, "error"), f"a child process failed: {got}"
    return got


def _kinds(got, kind):
    return [value for event, value in got if event == kind]


def _stop(processes):
    for process in processes:
        process.join(30)
        if process.is_alive():
            process.kill()
            process.join(5)


def _hold_lock_child(events):
    with auth._auth_store_lock():
        events.put(("held", os.getpid()))
        time.sleep(120)


def test_authority_lock_fences_other_pods_and_dies_with_its_holder(
    authority, tmp_path, monkeypatch
):
    _enter(monkeypatch, _make_pod(tmp_path, "podA"))
    events = _SPAWN.Queue()
    holder = _spawn(_hold_lock_child, _make_pod(tmp_path, "podB"), events)
    try:
        (pid,) = _kinds(_await(events, lambda got: _kinds(got, "held")), "held")
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="auth store lock"):
            with auth._auth_store_lock(timeout_seconds=1.0):
                pass
        assert time.monotonic() - started >= 1.0
        # The root store has its own lock, as it has its own lock file.
        with auth._auth_store_lock(target_path=auth._global_auth_file_path()):
            pass
        os.kill(pid, signal.SIGKILL)
        holder.join(10)
        with auth._auth_store_lock(timeout_seconds=10.0):  # the server let go
            # ...and another thread of this process is fenced like a pod.
            outcome = []

            def other_thread():
                try:
                    with auth._auth_store_lock(timeout_seconds=1.0):
                        outcome.append("acquired")
                except TimeoutError:
                    outcome.append("timeout")

            thread = threading.Thread(target=other_thread)
            thread.start()
            thread.join(10)
            assert outcome == ["timeout"]
    finally:
        _stop([holder])
    assert _file_artifacts(tmp_path) == []


# --------------------------------------------------------------------------
# (c) two pods, one single-use refresh token (A-F02 / B-F12)
# --------------------------------------------------------------------------


class _TokenEndpoint:
    """Codex-like token endpoint: a refresh token works exactly once."""

    def __init__(self, first: str = "rt-0"):
        self.valid = first
        self.posts = []
        self._issued = 0
        self._lock = threading.Lock()
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                form = parse_qs(self.rfile.read(length).decode())
                status, body = endpoint.exchange(form.get("refresh_token", [""])[0])
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/oauth/token"

    def exchange(self, refresh_token: str):
        with self._lock:
            self.posts.append(refresh_token)
            if refresh_token != self.valid:
                return 401, {
                    "error": {"code": "refresh_token_reused", "message": "reused"}
                }
            self._issued += 1
            self.valid = f"rt-{self._issued}"
            issued = (_jwt(time.time() + 3600, self._issued), self.valid)
        # Slow enough that the other pod reaches the lock mid-refresh.
        time.sleep(1.0)
        return 200, {"access_token": issued[0], "refresh_token": issued[1]}

    def close(self):
        self._server.shutdown()
        self._server.server_close()


def _refresh_child(events, token_url, path, start):
    """One pod resolving Codex credentials while the token is expiring."""
    from agent.credential_pool import load_pool

    auth.CODEX_OAUTH_TOKEN_URL = token_url
    if path == "pool":
        pool = load_pool("openai-codex")
    events.put(("ready", os.getpid()))
    start.wait(60)
    try:
        if path == "runtime":
            token = auth.resolve_codex_runtime_credentials()["api_key"]
        else:
            entry = pool.select()
            token = entry.access_token if entry is not None else None
    except auth.AuthError as exc:
        events.put(("result", ("failed", exc.code)))
        return
    expired = auth._codex_access_token_is_expiring(token, 0) if token else True
    events.put(("result", ("failed", None) if expired else ("ok", token)))


def _boot_pod(monkeypatch, pod):
    """What a pod does before its gateway starts: import its seed (C08)."""
    with monkeypatch.context() as scope:
        _enter(scope, pod)
        return aux.migrate_auth_to_pg("dave", dry_run=False)


@pytest.mark.parametrize("path", ["runtime", "pool"])
@pytest.mark.parametrize("backend", ["authority", "disk"])
def test_two_pods_refreshing_one_single_use_token(
    backend, path, pg, tmp_path, monkeypatch
):
    """Before 0066 each pod's auth.json and flock are its own ("disk", which
    is what every backend did): both pods spend ``rt-0`` and the later one gets
    ``refresh_token_reused``. On authority they share one chain: one POST, and
    both pods end up with the same fresh token."""
    seed = _codex_store("rt-0")
    pods = [_make_pod(tmp_path, name, seed) for name in ("podA", "podB")]
    if backend == "authority":
        monkeypatch.setenv("HERMES_STATE_BACKEND", "authority")
        monkeypatch.setenv("HERMES_STATE_POSTGRES_DSN", pg)
        reports = [_boot_pod(monkeypatch, pod) for pod in pods]
        inserted = [
            r["stores"]["auth_profile"]["tables"]["auth_store"]["inserted"]
            for r in reports
        ]
        assert inserted == [2, 0]  # the second pod's seed adds nothing
    endpoint = _TokenEndpoint("rt-0")
    events, start = _SPAWN.Queue(), _SPAWN.Event()
    processes = [
        _spawn(_refresh_child, pod, events, endpoint.url, path, start) for pod in pods
    ]
    try:
        _await(events, lambda got: len(_kinds(got, "ready")) == 2)
        start.set()
        results = _kinds(
            _await(events, lambda got: len(_kinds(got, "result")) == 2), "result"
        )
    finally:
        _stop(processes)
        endpoint.close()

    if backend == "disk":
        assert endpoint.posts == ["rt-0", "rt-0"]
        assert sorted(kind for kind, _ in results) == ["failed", "ok"]
        assert (
            _file_artifacts(tmp_path).count("podA/hermes/profiles/dave/auth.lock") == 1
        )
        return
    assert endpoint.posts == ["rt-0"]
    assert [kind for kind, _ in results] == ["ok", "ok"]
    assert results[0][1] == results[1][1]
    stored = json.loads(_row(pg, "profile"))["providers"]["openai-codex"]["tokens"]
    assert stored["refresh_token"] == "rt-1" == endpoint.valid
    assert stored["access_token"] == results[0][1]
    # The pods' seed files were never read nor rewritten: still rt-0.
    for pod in pods:
        on_disk = json.loads((Path(pod["HERMES_HOME"]) / "auth.json").read_text())
        assert on_disk == seed
    assert [
        p for p in _file_artifacts(tmp_path) if not p.endswith("dave/auth.json")
    ] == []


# --------------------------------------------------------------------------
# (d) every other backend keeps auth.json and the flock
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, "sqlite", "probe"])
def test_non_authority_keeps_auth_json_and_flock(backend, tmp_path, monkeypatch):
    _enter(monkeypatch, _make_pod(tmp_path, "podA"))
    if backend:
        monkeypatch.setenv("HERMES_STATE_BACKEND", backend)
        monkeypatch.setenv("HERMES_CORE_PG_DSN", UNREACHABLE_DSN)

    def refuse(*_args, **_kwargs):
        raise AssertionError("PostgreSQL must not be touched off authority")

    monkeypatch.setattr(aux, "connect_aux_postgres", refuse)
    monkeypatch.setattr(aux.AuxSessionLock, "acquire", refuse)
    with auth._auth_store_lock():
        store = auth._load_auth_store()
        store["providers"]["openai-codex"] = {"tokens": {"refresh_token": "rt-a"}}
        auth._save_auth_store(store)
    home = Path(os.environ["HERMES_HOME"])
    assert json.loads((home / "auth.json").read_text())["providers"]["openai-codex"]
    assert stat.S_IMODE((home / "auth.json").stat().st_mode) == 0o600
    assert (home / "auth.lock").exists()
    root_path = auth._global_auth_file_path()
    auth._persist_provider_state_to_store(
        "xai-oauth", {"refresh_token": "x"}, root_path
    )
    assert root_path.is_file() and root_path.with_suffix(".lock").exists()


# --------------------------------------------------------------------------
# (e) one-shot move: idempotent, PostgreSQL wins, sources untouched
# --------------------------------------------------------------------------


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_auth_migration_is_idempotent_and_never_regresses_postgres(
    authority, tmp_path, monkeypatch, capsys
):
    seed = _codex_store("rt-0")
    seed["credential_pool"] = {"openrouter": [{"id": "p1", "source": "manual"}]}
    seed["suppressed_sources"] = {"anthropic": ["claude_code"]}
    pod = _make_pod(tmp_path, "podA", seed)
    root_seed = {"version": 1, "providers": {"xai-oauth": {"refresh_token": "x-0"}}}
    (tmp_path / "podA" / "hermes" / "auth.json").write_text(json.dumps(root_seed))
    _enter(monkeypatch, pod)
    profile_file = Path(pod["HERMES_HOME"]) / "auth.json"
    root_file = auth._global_auth_file_path()
    sums = (_sha(profile_file), _sha(root_file))

    dry = aux.migrate_auth_to_pg("dave", dry_run=True)
    assert dry["stores"]["auth_profile"]["status"] == "dry_run"
    assert dry["stores"]["auth_profile"]["tables"]["auth_store"]["inserted"] == 4
    assert _row(authority, "profile") is None and _row(authority, "root") is None

    first = aux.migrate_auth_to_pg("dave", dry_run=False)
    table = first["stores"]["auth_profile"]["tables"]["auth_store"]
    assert sorted(table["inserted_keys"]) == [
        "active_provider",
        "credential_pool.openrouter",
        "providers.openai-codex",
        "suppressed_sources",
    ]
    assert first["stores"]["auth_root"]["tables"]["auth_store"]["inserted"] == 1
    assert first["stores"]["auth_profile"]["sha256"] == sums[0]
    assert auth._load_provider_state(auth._load_auth_store(), "xai-oauth") == {
        "refresh_token": "x-0"
    }

    # A pod on this image rotates the token in PostgreSQL ...
    with auth._auth_store_lock():
        store = auth._load_auth_store()
        store["providers"]["openai-codex"]["tokens"]["refresh_token"] = "rt-9"
        auth._save_auth_store(store)
    # ... and a later boot with the stale seed (a new pod's disk) adds nothing.
    again = aux.migrate_auth_to_pg("dave", dry_run=False)
    assert again["stores"]["auth_profile"]["tables"]["auth_store"]["inserted"] == 0
    assert again["stores"]["auth_root"]["tables"]["auth_store"]["inserted"] == 0
    tokens = auth._load_auth_store()["providers"]["openai-codex"]["tokens"]
    assert tokens["refresh_token"] == "rt-9"
    assert (_sha(profile_file), _sha(root_file)) == sums

    assert aux.main(["--profile", "dave", "--auth", "--dry-run"]) == 0
    printed = capsys.readouterr().out
    assert json.loads(printed)["stores"]["auth_profile"]["status"] == "dry_run"
    assert "rt-" not in printed and "x-0" not in printed  # names, never values
    with pytest.raises(SystemExit):
        aux.main(["--profile", "dave", "--auth", "--cron"])


def test_auth_migration_refuses_bad_sources_and_foreign_profiles(
    authority, tmp_path, monkeypatch
):
    pod = _make_pod(tmp_path, "podA")
    _enter(monkeypatch, pod)
    report = aux.migrate_auth_to_pg("dave", dry_run=False)
    assert {s["status"] for s in report["stores"].values()} == {"missing"}

    with pytest.raises(ValueError, match="not the active profile"):
        aux.migrate_auth_to_pg("other", dry_run=False)

    profile_file = Path(pod["HERMES_HOME"]) / "auth.json"
    profile_file.write_text('["not", "an", "object"]')
    with pytest.raises(aux.AuxMigrationError, match="not a JSON object"):
        aux.migrate_auth_to_pg("dave", dry_run=False)
    profile_file.write_text('{"providers": {"openai-codex": {"tokens": "rt-secret"')
    with pytest.raises(aux.AuxMigrationError) as caught:
        aux.migrate_auth_to_pg("dave", dry_run=False)
    assert "rt-secret" not in str(caught.value)
    assert _row(authority, "profile") is None

    monkeypatch.setenv("HERMES_STATE_BACKEND", "sqlite")
    with pytest.raises(RuntimeError, match="not on PostgreSQL authority"):
        aux.migrate_auth_to_pg("dave", dry_run=False)
