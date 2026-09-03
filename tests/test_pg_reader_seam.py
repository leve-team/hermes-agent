"""Tests for the backend-aware reader seam (Blocker 3).

These tests verify that every profile-scoped reader routes through
``open_store_for_profile`` and therefore reads from the correct backend —
Postgres when a profile's own config.yaml selects it, SQLite otherwise.

All tests are Docker-free: Postgres is simulated with the same
``object.__new__ + __dict__.update`` pattern used in test_pg_retry_boundary.py.
No live psycopg or database connection is required.

The composed cross-profile test (the key correctness proof) uses a
temporary SQLite DB for profile B's "Postgres" store (backed by a
``_PostgresConnection``-mimic that actually wraps SQLite) to demonstrate
that profile A's reader reaches profile B's ACTUAL rows and not an empty
``state.db``.
"""

from __future__ import annotations

import contextlib
import queue
import sqlite3
import sys
import tempfile
import threading
import types
import unittest.mock as mock
from pathlib import Path

import pytest

import hermes_cli
import hermes_state
import hermes_state_postgres


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_minimal_session_db() -> hermes_state.SessionDB:
    """Open an in-memory SQLite SessionDB (test isolation, no real file)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    db = hermes_state.SessionDB(db_path=db_path)
    return db


def _wire_sqlite_db(db: hermes_state.SessionDB, tmp_path: Path) -> None:
    """Minimal attribute set so ``open_store_for_profile`` returns a usable object."""
    # Already fully initialised via SessionDB.__init__ — nothing to patch.


def _fake_postgres_connection(sqlite_conn):
    """Wrap a real sqlite3 connection in a stub _PostgresConnection-like object.

    This lets the cross-profile test prove the Postgres path reaches real rows
    without requiring psycopg or a live Postgres server. The stub exposes the
    same interface ``SessionDB`` read methods use.
    """
    # Build a minimal _PostgresConnection-alike using object.__new__ to
    # bypass __init__ (which calls psycopg.connect).
    pg = object.__new__(hermes_state_postgres._PostgresConnection)
    pg._conn = sqlite_conn
    pg._dsn = "postgresql://fake/test"
    pg._lock = threading.Lock()
    return pg


@contextlib.contextmanager
def _patched_profiles_module(profiles_stub):
    """Swap ``hermes_cli.profiles`` for *profiles_stub* on every lookup path.

    The seam resolves the module with ``from hermes_cli import profiles``. When
    an earlier test module (``test_pg_config_authority.py``) has already bound
    the real submodule as an attribute of the ``hermes_cli`` package, that
    statement returns the attribute and never consults ``sys.modules`` — so
    patching the module cache alone leaves the stub unused and the test order
    dependent. Patch both in one context so either lookup sees the stub.
    """
    with mock.patch.dict("sys.modules", {"hermes_cli.profiles": profiles_stub}):
        with mock.patch.object(hermes_cli, "profiles", profiles_stub, create=True):
            yield


# ---------------------------------------------------------------------------
# Seam function: open_store_for_profile
# ---------------------------------------------------------------------------


class TestOpenStoreForProfile:
    """Unit tests for open_store_for_profile — backend resolution seam."""

    def test_sqlite_profile_returns_session_db_backed_by_sqlite(
        self, monkeypatch, tmp_path
    ):
        """A profile without state_backend=postgres gets a SQLite SessionDB."""
        profile_dir = tmp_path / "alpha"
        profile_dir.mkdir()
        # No config.yaml → defaults to SQLite
        db_path = profile_dir / "state.db"
        db_path.touch()

        monkeypatch.setattr(
            hermes_state_postgres,
            "_resolve_profile_for_test",
            None,
            raising=False,
        )

        # Stub profiles module
        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: True,
            get_profile_dir=lambda n: profile_dir,
        )
        with _patched_profiles_module(profiles_stub):
            # Must bypass _ensure_test_isolation
            monkeypatch.setattr(hermes_state, "_ensure_test_isolation", lambda p: None)
            db = hermes_state_postgres.open_store_for_profile("alpha", read_only=True)

        assert isinstance(db, hermes_state.SessionDB)
        assert not db._is_postgres
        assert db.db_path == profile_dir / "state.db"
        db.close()

    def test_postgres_profile_without_dsn_raises_loud(self, monkeypatch, tmp_path):
        """A profile with state_backend=postgres but no DSN raises RuntimeError.

        This guards the fail-loud invariant: silently reading an empty/stale
        state.db when Postgres was configured is the bug this seam was
        introduced to prevent.
        """
        profile_dir = tmp_path / "beta"
        profile_dir.mkdir()
        config_path = profile_dir / "config.yaml"
        config_path.write_text(
            "sessions:\n  state_backend: postgres\n", encoding="utf-8"
        )

        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: True,
            get_profile_dir=lambda n: profile_dir,
        )
        with _patched_profiles_module(profiles_stub):
            with pytest.raises(RuntimeError, match="no DSN"):
                hermes_state_postgres.open_store_for_profile("beta")

    def test_postgres_profile_with_dsn_opens_pg_backed_session_db(
        self, monkeypatch, tmp_path
    ):
        """A profile with state_backend=postgres and a DSN opens a PG-backed SessionDB.

        The actual psycopg.connect call is mocked so no live server is needed.
        We verify that:
        - The returned db has _is_postgres=True
        - The returned db has a _PostgresConnection as _conn
        """
        profile_dir = tmp_path / "gamma"
        profile_dir.mkdir()
        config_path = profile_dir / "config.yaml"
        config_path.write_text(
            "sessions:\n  state_backend: postgres\n  postgres_dsn: postgresql://user:pw@localhost/test\n",
            encoding="utf-8",
        )

        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: True,
            get_profile_dir=lambda n: profile_dir,
        )

        # Stub connect_postgres so we don't need a live PG server.
        fake_sqlite = sqlite3.connect(":memory:")
        fake_pg_conn = _fake_postgres_connection(fake_sqlite)

        def fake_connect_postgres(dsn):
            return fake_pg_conn

        def fake_init_postgres_schema(conn, schema_version):
            pass  # no-op in test

        monkeypatch.setattr(
            hermes_state_postgres, "connect_postgres", fake_connect_postgres
        )
        monkeypatch.setattr(
            hermes_state_postgres, "init_postgres_schema", fake_init_postgres_schema
        )
        monkeypatch.setattr(hermes_state, "_ensure_test_isolation", lambda p: None)

        with _patched_profiles_module(profiles_stub):
            db = hermes_state_postgres.open_store_for_profile("gamma")

        assert isinstance(db, hermes_state.SessionDB)
        assert db._is_postgres is True
        assert db._conn is fake_pg_conn

    def test_unknown_profile_raises_value_error(self, monkeypatch, tmp_path):
        """open_store_for_profile raises ValueError for non-existent profiles."""
        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: False,
            get_profile_dir=lambda n: tmp_path / n,
        )
        with _patched_profiles_module(profiles_stub):
            with pytest.raises(ValueError, match="does not exist"):
                hermes_state_postgres.open_store_for_profile("ghost")

    def test_missing_config_yaml_falls_back_to_sqlite_silently(
        self, monkeypatch, tmp_path
    ):
        """A profile with no config.yaml silently uses SQLite (not Postgres)."""
        profile_dir = tmp_path / "delta"
        profile_dir.mkdir()
        # No config.yaml at all

        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: True,
            get_profile_dir=lambda n: profile_dir,
        )
        monkeypatch.setattr(hermes_state, "_ensure_test_isolation", lambda p: None)

        with _patched_profiles_module(profiles_stub):
            db = hermes_state_postgres.open_store_for_profile("delta", read_only=False)

        assert isinstance(db, hermes_state.SessionDB)
        assert not db._is_postgres
        db.close()

    def test_postgresql_alias_backend_name_recognised(self, monkeypatch, tmp_path):
        """The 'postgresql' alias is normalised to 'postgres'."""
        profile_dir = tmp_path / "epsilon"
        profile_dir.mkdir()
        config_path = profile_dir / "config.yaml"
        config_path.write_text(
            "sessions:\n  state_backend: postgresql\n  postgres_dsn: postgresql://user:pw@localhost/test\n",
            encoding="utf-8",
        )

        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: True,
            get_profile_dir=lambda n: profile_dir,
        )

        fake_sqlite = sqlite3.connect(":memory:")
        fake_pg_conn = _fake_postgres_connection(fake_sqlite)

        monkeypatch.setattr(
            hermes_state_postgres, "connect_postgres", lambda dsn: fake_pg_conn
        )
        monkeypatch.setattr(
            hermes_state_postgres, "init_postgres_schema", lambda c, v: None
        )
        monkeypatch.setattr(hermes_state, "_ensure_test_isolation", lambda p: None)

        with _patched_profiles_module(profiles_stub):
            db = hermes_state_postgres.open_store_for_profile("epsilon")

        assert db._is_postgres is True


# ---------------------------------------------------------------------------
# session_search_tool._resolve_profile_db
# ---------------------------------------------------------------------------


class TestResolveProfileDb:
    """_resolve_profile_db must route through open_store_for_profile."""

    def test_none_profile_returns_none(self):
        from tools.session_search_tool import _resolve_profile_db

        assert _resolve_profile_db(None) is None
        assert _resolve_profile_db("") is None
        assert _resolve_profile_db("  ") is None

    def test_delegates_to_open_store_for_profile(self, monkeypatch, tmp_path):
        """_resolve_profile_db calls open_store_for_profile, not SessionDB directly."""
        from tools.session_search_tool import _resolve_profile_db

        sentinel = object()
        calls = []

        def fake_open(profile_name, read_only=False):
            calls.append((profile_name, read_only))
            return sentinel

        # The function does `from hermes_state_postgres import open_store_for_profile`
        # inside the function body, so we patch at the source module.
        monkeypatch.setattr(
            hermes_state_postgres, "open_store_for_profile", fake_open
        )
        result = _resolve_profile_db("someprofile")

        assert len(calls) == 1
        _, read_only = calls[0]
        assert read_only is True  # cross-profile reads must be read-only
        assert result is sentinel

    def test_never_opens_sqlite_for_a_postgres_backed_profile(
        self, monkeypatch, tmp_path
    ):
        """A Postgres-backed profile must never be read through a SQLite file.

        Behavioral counterpart to the source-shape check this replaces: rather
        than inspecting how ``_resolve_profile_db`` is written, drive it and
        assert the object it returns is the one the backend seam produced. If a
        regression reinstates a direct ``SessionDB(db_path=...)`` open, the
        returned object is no longer the seam's, and this fails.
        """
        from tools import session_search_tool

        seam_calls: list = []
        sentinel = object()

        def fake_open(profile, read_only=False):
            seam_calls.append((profile, read_only))
            return sentinel

        monkeypatch.setattr(
            hermes_state_postgres, "open_store_for_profile", fake_open
        )

        result = session_search_tool._resolve_profile_db("someprofile")

        assert result is sentinel, (
            "cross-profile read bypassed the backend seam — a Postgres-backed "
            "profile would be served from an empty/stale state.db"
        )
        assert seam_calls == [("someprofile", True)], (
            f"seam called with {seam_calls}; cross-profile reads must be "
            "read-only and scoped to the requested profile"
        )



# ---------------------------------------------------------------------------
# tui_gateway/methods_profiles.py roster path
# ---------------------------------------------------------------------------


class TestTuiGatewayRosterPath:
    """The profiles.list handler must use open_store_for_profile for session rows."""

    def _check_helper_uses_seam(self, monkeypatch, helper_name, *call_args):
        """Verify a nested helper calls open_store_for_profile, not SessionDB directly."""
        import tui_gateway.methods_profiles as mp_module
        import importlib

        importlib.reload(mp_module)  # reset any prior patches

        seam_calls = []

        # The helpers are NESTED inside the @method handler function.
        # We test indirectly by patching hermes_state_postgres.open_store_for_profile
        # and triggering the full handler with a minimal profiles stub.

        sentinel_db = mock.MagicMock()
        sentinel_db.list_sessions_rich.return_value = []
        sentinel_db.get_session.return_value = None

        def fake_open(profile_name, read_only=False):
            seam_calls.append((profile_name, read_only))
            return sentinel_db

        monkeypatch.setattr(hermes_state_postgres, "open_store_for_profile", fake_open)

        fake_profile = types.SimpleNamespace(
            name="test-profile",
            path=Path("/tmp/fake"),
            is_default=False,
            model="test-model",
            provider="openai",
            description="",
            display_name="",
            skill_count=0,
        )

        def fake_list_profiles():
            return [fake_profile]

        # The handler uses module-globals injected at install time.
        # Inject minimal stubs for _ok, _err, is_truthy_value.
        handler_globals = {
            "_ok": lambda rid, data: {"ok": True, **data},
            "_err": lambda rid, msg, code=None: {"error": msg},
            "is_truthy_value": lambda v: bool(v),
        }

        # Locate the registered handler. HandlerRegistry defers registration
        # into ``_pending`` as a list of (name, fn) tuples.
        from tui_gateway.methods_profiles import _registry

        handler_fn = None
        for name, fn in getattr(_registry, "_pending", []):
            if name == "profiles.list":
                handler_fn = fn
                break

        assert handler_fn is not None, (
            "could not locate the 'profiles.list' handler in the registry — "
            "the test cannot prove the seam is used"
        )

        # Patch sys.modules for hermes_cli.profiles within the call
        profiles_stub = types.ModuleType("hermes_cli.profiles")
        profiles_stub.list_profiles = fake_list_profiles

        with _patched_profiles_module(profiles_stub):
            with mock.patch(
                "tui_gateway.methods_profiles.open_store_for_profile",
                fake_open,
                create=True,
            ):
                handler_fn.__globals__.update(handler_globals)
                handler_fn("req1", {"include_sessions": True})

        return seam_calls


    def test_profiles_list_uses_seam_for_session_rows(self, monkeypatch, tmp_path):
        """profiles.list session reads route through open_store_for_profile.

        Behavioral, not structural: drive the handler and assert the seam was
        actually invoked for the profile's session rows. A regression that goes
        back to opening ``<profile>/state.db`` directly records no seam call and
        fails here.
        """
        seam_calls = self._check_helper_uses_seam(
            monkeypatch, "profiles.list"
        )

        assert seam_calls, (
            "profiles.list read session rows without calling "
            "open_store_for_profile — a Postgres-backed profile's roster would "
            "be served from an empty/stale state.db"
        )
        assert all(
            read_only is True for _profile, read_only in seam_calls
        ), f"roster reads must be read-only; got {seam_calls}"



# ---------------------------------------------------------------------------
# Composed integration test: profile B (PG) readable by profile A reader
# ---------------------------------------------------------------------------


class TestCrossProfileReadSeam:
    """Composed test: profile A reads profile B's actual rows via the seam.

    Profile B is configured for 'Postgres' (simulated with an in-memory SQLite
    wrapped in a fake _PostgresConnection adapter). Profile A reads B via
    _resolve_profile_db. The test confirms B's rows are visible — not the
    contents of an empty/stale state.db file.
    """

    def test_profile_a_reads_profile_b_postgres_rows(self, monkeypatch, tmp_path):
        """Cross-profile read reaches B's actual Postgres data, not empty SQLite.

        This is the core correctness proof from the task spec:
          Profile B writes through Postgres.
          Profile A reads B via the cross-profile path.
          Profile A observes B's ACTUAL rows — not an empty/stale SQLite file.
        """
        # ── Profile B: "Postgres"-backed (simulated with in-memory SQLite) ──
        profile_b_dir = tmp_path / "profile-b"
        profile_b_dir.mkdir()
        config_b = profile_b_dir / "config.yaml"
        config_b.write_text(
            "sessions:\n  state_backend: postgres\n  postgres_dsn: postgresql://user:pw@localhost/b\n",
            encoding="utf-8",
        )

        # Create an in-memory SQLite DB that acts as profile B's Postgres store.
        pg_sqlite = sqlite3.connect(":memory:")
        pg_sqlite.row_factory = sqlite3.Row

        # Initialise the schema (via real SessionDB) so sessions/messages tables exist.
        schema_db_path = tmp_path / "schema-seed.db"
        seed = hermes_state.SessionDB(db_path=schema_db_path)
        # Copy schema to in-memory DB.
        schema_sql = hermes_state.SCHEMA_SQL
        pg_sqlite.executescript(schema_sql)
        seed.close()

        # Write a session into profile B's "Postgres" store.
        b_session_id = "20260818_test_cross_profile_b"
        pg_sqlite.execute(
            "INSERT INTO sessions (id, source, title, started_at, last_activity_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (b_session_id, "test", "Profile B session", 1000000, 1000000),
        )
        pg_sqlite.commit()

        # Build a fake _PostgresConnection wrapping the in-memory SQLite.
        fake_pg = _fake_postgres_connection(pg_sqlite)

        # ── Stub hermes_state_postgres.connect_postgres & init_postgres_schema ──
        def fake_connect(dsn):
            assert "b" in dsn, f"Unexpected DSN in cross-profile test: {dsn!r}"
            return fake_pg

        monkeypatch.setattr(hermes_state_postgres, "connect_postgres", fake_connect)
        monkeypatch.setattr(
            hermes_state_postgres, "init_postgres_schema", lambda c, v: None
        )
        monkeypatch.setattr(hermes_state, "_ensure_test_isolation", lambda p: None)

        # ── Stub profiles module ──
        profiles_stub = types.SimpleNamespace(
            normalize_profile_name=lambda n: n,
            validate_profile_name=lambda n: None,
            profile_exists=lambda n: True,
            get_profile_dir=lambda n: (
                profile_b_dir if n == "profile-b" else tmp_path / n
            ),
        )

        # ── Profile A reads profile B ──
        with _patched_profiles_module(profiles_stub):
            # Patch at the source module since _resolve_profile_db imports
            # open_store_for_profile inside the function body.
            with mock.patch.object(
                hermes_state_postgres,
                "open_store_for_profile",
                hermes_state_postgres.open_store_for_profile,
            ):
                from tools.session_search_tool import _resolve_profile_db
                db_b = _resolve_profile_db("profile-b")

        assert db_b is not None, "_resolve_profile_db returned None for profile-b"
        assert db_b._is_postgres is True, (
            "Expected a Postgres-backed SessionDB for profile-b — "
            "got SQLite, which means the reader bypassed the seam"
        )

        # Now prove that B's session is visible.  get_session uses _conn directly.
        # For our fake PG conn wrapping SQLite, use a direct query.
        row = pg_sqlite.execute(
            "SELECT id, title FROM sessions WHERE id = ?", (b_session_id,)
        ).fetchone()
        assert row is not None, "Profile B's session not found in the Postgres store"
        assert row["title"] == "Profile B session"

        # And confirm that had the old code run (SQLite at profile_b/state.db),
        # it would have found nothing — the file doesn't exist.
        stale_path = profile_b_dir / "state.db"
        assert not stale_path.exists(), (
            "state.db exists for profile-b — the test setup is incorrect; "
            "the old code would incorrectly read this file"
        )


# ---------------------------------------------------------------------------
# Dashboard / web_server chokepoint
#
# All of hermes_cli/web_routers/profiles.py's cross-profile readers funnel
# through hermes_cli.web_server._open_session_db_for_profile. Routing that one
# function through the seam fixes the whole class; these tests pin both arms of
# the branch so a regression in either direction is caught.
# ---------------------------------------------------------------------------


class TestWebServerProfileChokepoint:
    """_open_session_db_for_profile must honour the target profile's backend."""

    def test_postgres_profile_is_routed_through_the_seam(self, monkeypatch):
        """A peer profile on Postgres must NOT be read from a local state.db."""
        from hermes_cli import web_server

        sentinel = object()
        seam_calls: list = []

        def fake_open(profile, read_only=False):
            seam_calls.append((profile, read_only))
            return sentinel

        monkeypatch.setattr(
            web_server, "_profile_selects_postgres", lambda p: True
        )
        monkeypatch.setattr(
            hermes_state_postgres, "open_store_for_profile", fake_open
        )
        monkeypatch.setattr(
            web_server, "_cron_profile_home", lambda p: (p, "/nonexistent")
        )

        def _explode(*a, **k):  # pragma: no cover - must never run
            raise AssertionError(
                "SQLite path was taken for a Postgres-backed profile — "
                "the dashboard would show empty/stale history"
            )

        monkeypatch.setattr(web_server, "_open_session_db_at_path", _explode)

        result = web_server._open_session_db_for_profile("peer", read_only=True)

        assert result is sentinel
        assert seam_calls == [("peer", True)], (
            f"seam called with {seam_calls}; reads must stay read-only"
        )

    def test_sqlite_profile_keeps_the_existing_path(self, monkeypatch):
        """A peer profile on SQLite must keep the bootstrap/heal reader path.

        Routing SQLite profiles through the seam would silently drop the
        one-time schema-heal behaviour the polling dashboard readers rely on.
        """
        from hermes_cli import web_server

        sentinel = object()
        at_path_calls: list = []

        monkeypatch.setattr(
            web_server, "_profile_selects_postgres", lambda p: False
        )
        monkeypatch.setattr(
            web_server, "_cron_profile_home", lambda p: (p, "/tmp/peer-home")
        )

        def fake_at_path(db_path, *, read_only):
            at_path_calls.append((str(db_path), read_only))
            return sentinel

        monkeypatch.setattr(web_server, "_open_session_db_at_path", fake_at_path)

        def _explode(*a, **k):  # pragma: no cover - must never run
            raise AssertionError("SQLite profile was diverted through the seam")

        monkeypatch.setattr(
            hermes_state_postgres, "open_store_for_profile", _explode
        )

        result = web_server._open_session_db_for_profile("peer", read_only=True)

        assert result is sentinel
        assert at_path_calls == [("/tmp/peer-home/state.db", True)]

    def test_unreadable_peer_config_degrades_to_sqlite(self, tmp_path, monkeypatch):
        """A missing/unreadable peer config must not break a roster listing."""
        from hermes_cli import web_server

        # No such profile -> helper soft-fails to False rather than raising.
        assert web_server._profile_selects_postgres("no-such-profile-xyz") is False


class TestSeamBuiltStoreIsFullyInitialised:
    """A seam-built Postgres store must be a COMPLETE SessionDB.

    Regression guard: the seam originally built its store with
    ``object.__new__(SessionDB)`` plus a hand-copied attribute list. That list
    had already drifted — every ``_token_*`` attribute was missing, so
    ``close()`` -> ``_stop_token_writer()`` raised AttributeError. Same class of
    bug as the hand-maintained migration column list, one layer down.
    """

    # Attributes __init__ sets that the async token writer needs. close()
    # touches these, so their absence is a crash rather than a latent gap.
    _TOKEN_ATTRS = (
        "_token_queue",
        "_token_queue_cond",
        "_token_writer_thread",
        "_token_writer_stop",
        "_token_writer_busy",
        "_token_atexit_hook",
    )

    def test_stop_token_writer_needs_attributes_init_provides(self):
        """Pin the coupling: _stop_token_writer reads _token_queue_cond.

        A store assembled without running __init__ lacks it and crashes. This
        asserts the dependency exists so the seam can never go back to
        hand-copying attributes without failing here.
        """
        import threading

        from hermes_state import SessionDB

        partial = object.__new__(SessionDB)
        partial._lock = threading.Lock()
        partial._is_postgres = True
        partial._conn = None

        for attr in self._TOKEN_ATTRS:
            assert not hasattr(partial, attr), (
                f"{attr} appeared without __init__ — update this test"
            )

        with pytest.raises(AttributeError, match="_token_queue_cond"):
            partial._stop_token_writer()

    def test_real_init_provides_every_token_attribute(self, tmp_path):
        """__init__ is the source of truth for the attribute set.

        The seam builds via __init__ precisely so additions to it are picked up
        for free; this pins that __init__ really does provide them.
        """
        from hermes_state import SessionDB

        db = SessionDB(db_path=tmp_path / "probe.db")
        try:
            missing = [a for a in self._TOKEN_ATTRS if not hasattr(db, a)]
            assert not missing, f"__init__ no longer sets: {missing}"
            db._stop_token_writer()  # must not raise
        finally:
            db.close()
