"""Executable percent-binding regressions; require an ephemeral local PostgreSQL.

No external DSN is accepted. initdb and pg_ctl must be on PATH or in
PG3_PERCENT_PG_BIN. Run with the snapshot and isolated fork on PYTHONPATH.
Missing dependencies are errors, never skips.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class AuthorityPercentBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workspace = tempfile.TemporaryDirectory(prefix="pg3-percent-")
        cls.addClassCleanup(cls.workspace.cleanup)
        root = Path(cls.workspace.name)
        cls.home = root / "home"
        cls.home.mkdir()
        environment = {
            key: os.environ[key]
            for key in ("PATH", "LD_LIBRARY_PATH", "LANG", "PG3_PERCENT_PG_BIN")
            if key in os.environ
        }
        environment.update(HOME=str(cls.home), HERMES_HOME=str(cls.home))
        isolation = patch.dict(os.environ, environment, clear=True)
        isolation.start()
        cls.addClassCleanup(isolation.stop)
        import psycopg
        import hermes_state_postgres
        from hermes_state import SessionDB

        cls.driver = psycopg
        cls.adapter = hermes_state_postgres
        cls.session_db = SessionDB
        binary_root = os.environ.get("PG3_PERCENT_PG_BIN", "")
        initdb = str(Path(binary_root) / "initdb") if binary_root else shutil.which("initdb")
        pg_ctl = str(Path(binary_root) / "pg_ctl") if binary_root else shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise RuntimeError("Isolated PostgreSQL tools required: initdb and pg_ctl")
        cls.data = root / "data"
        cls.socket = root / "socket"
        cls.socket.mkdir(mode=0o700)
        cls._command([initdb, "-D", str(cls.data), "-A", "trust", "-U", "pg3_percent",
                      "--no-locale", "--encoding=UTF8"])
        cls._command([
            pg_ctl, "-D", str(cls.data), "-l", str(root / "postgres.log"),
            "-o", f"-F -c listen_addresses='' -c unix_socket_directories='{cls.socket}' "
            "-c unix_socket_permissions=0700", "-w", "start",
        ])
        cls.addClassCleanup(cls._command, [pg_ctl, "-D", str(cls.data), "-m", "immediate",
                                          "-w", "stop"])
        cls.dsn = psycopg.conninfo.make_conninfo(
            host=str(cls.socket), dbname="postgres", user="pg3_percent", connect_timeout=5,
        )
        cls.raw = psycopg.connect(cls.dsn, autocommit=True)
        cls.addClassCleanup(cls.raw.close)

    @staticmethod
    def _command(arguments):
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError(f"{Path(arguments[0]).name} failed: {result.stdout}{result.stderr}")

    def setUp(self):
        self.cursor = self.adapter._PostgresCursor(self.raw.cursor())
        self.addCleanup(self.cursor._cursor.close)

    def test_isolation_proven_by_server(self):
        self.assertEqual(self.raw.execute("SHOW data_directory").fetchone()[0], str(self.data))
        self.assertEqual(self.raw.execute("SHOW listen_addresses").fetchone()[0], "")
        self.assertEqual(self.raw.execute("SHOW unix_socket_directories").fetchone()[0], str(self.socket))
        self.assertEqual(self.raw.execute("SELECT inet_server_addr()").fetchone(), (None,))
        self.assertFalse(any(key in os.environ for key in (
            "HERMES_STATE_DATABASE_URL", "HERMES_STATE_POSTGRES_DSN", "HERMES_CORE_PG_DSN",
            "DATABASE_URL", "PGHOST", "PGSERVICE", "PGPASSWORD",
        )))

    def test_no_params_none_and_empty(self):
        sql = "SELECT '100%', '%s %b %t %(name)s', '%%', 'what?', 11 % 4, 'abc' LIKE 'a%'"
        expected = ("100%", "%s %b %t %(name)s", "%%", "what?", 3, True)
        self.assertEqual(tuple(self.cursor.execute(sql).fetchone()), expected)
        for params in (None, (), [], {}):
            with self.subTest(params=params):
                self.assertEqual(tuple(self.cursor.execute(sql, params).fetchone()), expected)

    def test_qmark_binding_preserves_attack_values(self):
        value = "50%' OR 1=1; DROP TABLE sessions; -- %s %b %t %(name)s ? %%"
        row = self.cursor.execute("SELECT ?, 'literal %', ? LIKE '50%'", (value, value)).fetchone()
        self.assertEqual(tuple(row), (value, "literal %", True))

    def test_native_positional_formats(self):
        row = self.cursor.execute("SELECT %s::text, %b::int, %t::text, '%s/%b/%t'",
                                  ("quote'%", 7, "%%")).fetchone()
        self.assertEqual(tuple(row), ("quote'%", 7, "%%", "%s/%b/%t"))

    def test_native_named_formats_and_repeated_names(self):
        row = self.cursor.execute(
            "SELECT %(value)s::text, %(number)b::int, %(text)t::text, %(value)s::text, '%(value)s'",
            {"value": "x'%", "number": 9, "text": "%%"},
        ).fetchone()
        self.assertEqual(tuple(row), ("x'%", 9, "%%", "x'%", "%(value)s"))

    def test_quotes_comments_and_dollar_bodies(self):
        sql = """SELECT 'it''s %s?', E'it\\'s %b?', $$%t ? %%$$,
            $body$quote ' %s ?$body$, ? AS "name%t?" /* %s ? /* %b */ %t */
            -- %s ? %(ignored)s
        """
        row = self.cursor.execute(sql, ("bound'%",)).fetchone()
        self.assertEqual(tuple(row), ("it's %s?", "it's %b?", "%t ? %%", "quote ' %s ?", "bound'%"))
        self.assertEqual(row["name%t?"], "bound'%")

    def test_sqlite_semantics_wildcards_modulo_and_escaped_quotes(self):
        with sqlite3.connect(":memory:") as reference:
            for sql, params in (
                ("SELECT '%%', '%s', 'it''s %', 17%5, 17 % 5", ()),
                ("SELECT ? LIKE 'ab%', ? LIKE '%tail', ? LIKE '%middle%'", ("abc", "tail", "a-middle-z")),
                ("SELECT 'abc' LIKE ?, ? % 5, '%' = ?", ("a%", 17, "%")),
                ("SELECT ? LIKE '100!%%' ESCAPE '!'", ("100% done",)),
                ("SELECT '%s' AS \"percent%%\", ? -- %s ?\n", ("100%'",)),
            ):
                with self.subTest(sql=sql):
                    expected = reference.execute(sql, params).fetchone()
                    self.assertEqual(tuple(self.cursor.execute(sql, params).fetchone()), expected)

    def test_existing_unquoted_double_percent_escape(self):
        self.assertEqual(tuple(self.cursor.execute("SELECT 17 %% 5, ?", ("%%",)).fetchone()), (2, "%%"))

    def test_executemany_positional_generator(self):
        self.cursor.execute("CREATE TEMP TABLE percent_batch (value text, marker text, remainder int)")
        self.addCleanup(self.raw.execute, "DROP TABLE percent_batch")
        values = ("one'%", "two%%", "%s %b %t ?")
        self.cursor.executemany("INSERT INTO percent_batch VALUES (?, '%% %s', 17 % 5)",
                                ((value,) for value in values))
        self.assertEqual([tuple(row) for row in self.cursor.execute(
            "SELECT * FROM percent_batch ORDER BY ctid").fetchall()],
            [(value, "%% %s", 2) for value in values])
        self.assertIsNone(self.cursor.lastrowid)

    def test_executemany_named_and_empty_rows(self):
        self.cursor.execute("CREATE TEMP TABLE percent_named (value text, marker text)")
        self.addCleanup(self.raw.execute, "DROP TABLE percent_named")
        self.cursor.executemany("INSERT INTO percent_named VALUES (%(value)s, '100%')",
                                [{"value": "a'%"}, {"value": "b%%"}])
        self.cursor.executemany("INSERT INTO percent_named VALUES ('%s', '%%')", [(), ()])
        self.cursor.executemany("INSERT INTO percent_named VALUES ('%b', '%%')", [None, None])
        self.cursor.executemany("INSERT INTO percent_named VALUES (%b, %t)", [("native'%", "100%")])
        self.cursor.executemany("THIS IS INVALID %s SQL", [])
        self.assertEqual([tuple(row) for row in self.cursor.execute(
            "SELECT * FROM percent_named ORDER BY ctid").fetchall()],
            [("a'%", "100%"), ("b%%", "100%"), ("%s", "%%"), ("%s", "%%"),
             ("%b", "%%"), ("%b", "%%"), ("native'%", "100%")])

    def test_parameter_errors_propagate(self):
        for sql, params in (
            ("SELECT ?", ()), ("SELECT '%s'", ("extra",)),
            ("SELECT %(missing)s", {"other": "value"}),
            ("SELECT %s, %(named)s", ("mixed",)),
        ):
            with self.subTest(sql=sql), self.assertRaises(self.driver.ProgrammingError):
                self.cursor.execute(sql, params)
        with self.assertRaises(self.driver.ProgrammingError):
            self.cursor.executemany("SELECT ?", [()])

    def test_sql_errors_propagate(self):
        for sql in ("SELECT 'unclosed %s", "SELECT 1 /* unclosed %s", "SELECT $body$unclosed %s"):
            with self.subTest(sql=sql), self.assertRaises(self.driver.errors.SyntaxError):
                self.cursor.execute(sql)
        with self.assertRaises(self.driver.errors.DivisionByZero):
            self.cursor.execute("SELECT 5 % ?", (0,))

    def test_list_sessions_rich_filters_and_pagination(self):
        with patch.dict(os.environ, {"HERMES_STATE_BACKEND": "authority",
                                     "HERMES_STATE_POSTGRES_DSN": self.dsn}):
            database = self.session_db()
        self.addCleanup(database.close)
        self.assertTrue(database._is_postgres)
        self.assertEqual(database._state_backend_mode, "authority")
        for index, source in enumerate(("cli", "api", "cli"), start=1):
            session_id = f"percent-session-{index}"
            database.create_session(session_id, source)
            database.set_session_title(session_id, f"Title {index} 100%'")
            database.append_message(session_id, "user", f"Preview {index} 100%'", timestamp=index + 10)
            self.raw.execute("UPDATE sessions SET started_at = %s, last_activity_at = %s WHERE id = %s",
                             (index, index + 10, session_id))
        rows = database.list_sessions_rich()
        self.assertEqual([row["id"] for row in rows], ["percent-session-3", "percent-session-2", "percent-session-1"])
        self.assertEqual([row["message_count"] for row in rows], [1, 1, 1])
        self.assertEqual([row["preview"] for row in rows], [f"Preview {index} 100%'" for index in (3, 2, 1)])
        self.assertEqual([row["id"] for row in database.list_sessions_rich(source="cli", limit=1, offset=1)],
                         ["percent-session-1"])
        self.assertEqual([row["id"] for row in database.list_sessions_rich(sources=["api"])], ["percent-session-2"])
        self.assertEqual([row["id"] for row in database.list_sessions_rich(exclude_sources=["api"])],
                         ["percent-session-3", "percent-session-1"])
        self.assertEqual(database.list_sessions_rich(offset=3), [])
        self.assertEqual(database.list_sessions_rich(source="missing'%"), [])
        self.assertEqual([row["id"] for row in database.list_sessions_rich(
            order_by_last_active=True, search_query="100%'", limit=1, offset=1)], ["percent-session-2"])

    def test_explicit_sqlite_stays_local(self):
        path = self.home / "explicit-sqlite.db"
        with patch.dict(os.environ, {"HERMES_STATE_BACKEND": "authority",
                                     "HERMES_STATE_POSTGRES_DSN": self.dsn}):
            database = self.session_db(db_path=path)
        self.addCleanup(database.close)
        self.assertFalse(database._is_postgres)
        database.create_session("sqlite-percent", "cli")
        database.append_message("sqlite-percent", "user", "sqlite 100%'")
        self.assertEqual(database.list_sessions_rich()[0]["preview"], "sqlite 100%'")
        self.assertTrue(path.is_file())


if __name__ == "__main__":
    unittest.main(verbosity=2)
