"""levos v3 (t_0940d7c8) — the kanban master switch also keeps the dashboard off the kanban store.

``hermes dashboard`` mounts the bundled kanban plugin's router under ``/api/plugins/kanban/``;
its routes open (and create) ``kanban.db`` per request. With ``kanban.enabled: false`` /
``HERMES_KANBAN_ENABLED=0`` the router is not mounted at all, so no request can reach the store.
On (the default) the mount is exactly as before, and other plugins ignore the switch.
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hermes_cli import kanban_db_connect as kbc
from hermes_cli import web_server
import hermes_cli.web_server_dashboard as dashboard
from hermes_cli.web_server_dashboard import _plugin_api_mount_skip_reason

KANBAN = {"source": "bundled", "name": "kanban"}
KANBAN_PREFIX = "/api/plugins/kanban/"


@pytest.fixture
def home(tmp_path, monkeypatch):
    """HERMES_HOME == HERMES_KANBAN_HOME with no config and a ``kanban.db`` from before the switch."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    for key in ("HERMES_KANBAN_BACKEND", "HERMES_KANBAN_DB", "HERMES_KANBAN_ENABLED"):
        monkeypatch.delenv(key, raising=False)
    with kbc.connect_closing():
        pass
    assert (home / "kanban.db").exists()
    return home


def _kanban_off_by_config(home) -> None:
    (home / "config.yaml").write_text("kanban:\n  enabled: false\n", encoding="utf-8")


# --------------------------------------------------------------------------
# The mount gate
# --------------------------------------------------------------------------

def test_env_off_skips_the_kanban_api(home, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_ENABLED", "0")
    assert _plugin_api_mount_skip_reason(KANBAN, set(), set()) == "kanban disabled (HERMES_KANBAN_ENABLED env)"


def test_config_off_skips_the_kanban_api(home):
    _kanban_off_by_config(home)
    assert _plugin_api_mount_skip_reason(KANBAN, set(), set()) == "kanban disabled (config kanban.enabled=false)"


@pytest.mark.parametrize("env", [None, "1"])
def test_on_mounts_the_kanban_api_as_before(home, monkeypatch, env):
    if env is not None:
        monkeypatch.setenv("HERMES_KANBAN_ENABLED", env)
    assert _plugin_api_mount_skip_reason(KANBAN, set(), set()) is None


def test_explicit_plugin_disable_still_wins(home):
    assert _plugin_api_mount_skip_reason(KANBAN, set(), {"kanban"}) == "explicitly disabled"


@pytest.mark.parametrize("off", ["env", "config"])
def test_other_plugins_ignore_the_switch(home, monkeypatch, off):
    if off == "env":
        monkeypatch.setenv("HERMES_KANBAN_ENABLED", "0")
    else:
        _kanban_off_by_config(home)
    assert _plugin_api_mount_skip_reason({"source": "bundled", "name": "example"}, set(), set()) is None
    assert _plugin_api_mount_skip_reason({"source": "user", "name": "mine"}, {"mine"}, set()) is None
    assert _plugin_api_mount_skip_reason({"source": "user", "name": "mine"}, set(), set()) == "not in plugins.enabled"


# --------------------------------------------------------------------------
# The mounted app
# --------------------------------------------------------------------------

def _mount_on_fresh_app(monkeypatch) -> tuple[list[str], list[str]]:
    """Run the dashboard's plugin API mount on a fresh app; return (kanban openapi paths, sqlite connects)."""
    fresh = FastAPI()
    monkeypatch.setattr(web_server, "app", fresh)
    monkeypatch.setattr(web_server, "_dashboard_plugins_cache", None)
    connects: list[str] = []
    real_connect = sqlite3.connect

    def recording_connect(*args, **kwargs):
        connects.append(str(args[0] if args else kwargs.get("database")))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", recording_connect)
    dashboard._mount_plugin_api_routes()
    paths = TestClient(fresh).get("/openapi.json").json()["paths"]
    return [p for p in paths if p.startswith(KANBAN_PREFIX)], connects


def test_off_app_has_no_kanban_routes_and_opens_no_store(home, monkeypatch):
    _kanban_off_by_config(home)
    before = (home / "kanban.db").stat().st_mtime_ns
    kanban_paths, connects = _mount_on_fresh_app(monkeypatch)
    assert kanban_paths == []
    assert connects == []
    assert (home / "kanban.db").stat().st_mtime_ns == before


def test_on_app_mounts_the_kanban_routes(home, monkeypatch):
    kanban_paths, _ = _mount_on_fresh_app(monkeypatch)
    assert kanban_paths
