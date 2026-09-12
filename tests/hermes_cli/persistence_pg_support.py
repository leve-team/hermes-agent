"""Real, pinned py-pglite fixtures shared by the core store contract tests."""

import contextlib
import json

import psycopg
import pytest
from py_pglite import PGliteConfig, PGliteManager


class CorePGliteManager(PGliteManager):
    """Adapt py-pglite 0.5.3 startup to the pinned socket server API."""

    def _generate_unix_js_content(self, ext_requires_str, extensions_obj_str):
        source = super()._generate_unix_js_content(ext_requires_str, extensions_obj_str)
        for old, new in (
            ("const db = new PGlite({", "const db = await PGlite.create({"),
            ("path: SOCKET_PATH,", "path: SOCKET_PATH, maxConnections: 16,"),
        ):
            assert source.count(old) == 1
            source = source.replace(old, new)
        return source


@contextlib.contextmanager
def postgres_server(work_dir):
    (work_dir / "package.json").write_text(
        json.dumps({
            "private": True,
            "dependencies": {
                "@electric-sql/pglite": "0.3.16",
                "@electric-sql/pglite-socket": "0.0.22",
            },
        }),
        encoding="utf-8",
    )
    manager = CorePGliteManager(PGliteConfig(work_dir=work_dir))
    try:
        manager.start()
        dsn = manager.get_connection_string().replace(
            "postgresql+psycopg://", "postgresql://"
        )
        with psycopg.connect(dsn) as raw:
            assert "PostgreSQL" in raw.execute("SELECT version()").fetchone()[0]
        yield dsn
    finally:
        manager.stop()


@pytest.fixture(scope="module")
def pg_server(tmp_path_factory):
    with postgres_server(tmp_path_factory.mktemp("core-store-pg")) as dsn:
        yield dsn


@pytest.fixture
def pg_dsn(pg_server):
    with psycopg.connect(pg_server, autocommit=True) as raw:
        raw.execute("DROP SCHEMA public CASCADE")
        raw.execute("CREATE SCHEMA public")
    return pg_server
