"""Read-flip control and SQLite-authority PostgreSQL probes.

The probe is deliberately a connection adapter instead of a second set of
``SessionDB`` methods.  Every state reader already reaches either the main
connection or a connection borrowed by ``SessionDB._read_ctx``; wrapping
those two seams avoids a hand-maintained copy of the reader surface.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import threading
from typing import Any, Callable, Iterator, Optional

from state_diff import canonical_row_json


READ_FALLBACK_MARKER = "PG_MIG_READ_FALLBACK_V1"
MODE_SQLITE = "sqlite"
MODE_PROBE = "probe"
MODE_AUTHORITY = "authority"


def normalize_read_mode(value: Any) -> str:
    """Return the canonical read mode, rejecting unknown control values."""

    mode = str(value or MODE_SQLITE).strip().lower()
    if mode in {"postgres", "postgresql", "pg"}:
        return MODE_AUTHORITY
    if mode in {MODE_SQLITE, MODE_PROBE, MODE_AUTHORITY}:
        return mode
    raise RuntimeError(
        "sessions.state_backend / HERMES_STATE_BACKEND must be one of "
        "sqlite, probe, authority (postgres/pg/postgresql are authority aliases); "
        f"got {mode!r}"
    )


def expected_reader_entrypoints(session_db_type: type) -> set[str]:
    """Discover SessionDB methods that contain a state SELECT.

    This mirrors V1's bytecode-derived write-entrypoint inventory.  It avoids
    a source-text test and, more importantly, avoids a list that silently
    drifts whenever a reader is added or moved.
    """

    def contains_select(code: Any) -> bool:
        if any(
            isinstance(constant, str) and constant.lstrip().upper().startswith("SELECT")
            for constant in code.co_consts
        ):
            return True
        return any(
            inspect.iscode(constant) and contains_select(constant)
            for constant in code.co_consts
        )

    readers: set[str] = set()
    for base in session_db_type.__mro__:
        for name, value in vars(base).items():
            code = getattr(value, "__code__", None)
            if code is not None and contains_select(code):
                readers.add(name)
    return readers


def _query_id(sql: str) -> str:
    normalized = " ".join(str(sql).split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def _cursor_columns(cursor: Any) -> tuple[str, ...]:
    description = getattr(cursor, "description", None)
    if description is None and hasattr(cursor, "_columns"):
        return tuple(cursor._columns())
    return tuple(str(item[0]) for item in (description or ()))


def _rows_equal(
    primary_cursor: Any,
    primary_rows: list[Any],
    shadow_cursor: Any,
    shadow_rows: list[Any],
) -> bool:
    if len(primary_rows) != len(shadow_rows):
        return False
    primary_columns = _cursor_columns(primary_cursor)
    shadow_columns = _cursor_columns(shadow_cursor)
    if primary_columns != shadow_columns:
        return False
    return all(
        canonical_row_json(primary_columns, primary)
        == canonical_row_json(shadow_columns, shadow)
        for primary, shadow in zip(primary_rows, shadow_rows)
    )


def _close_cursor(cursor: Any) -> None:
    if cursor is None:
        return
    close = getattr(cursor, "close", None)
    if close is None:
        close = getattr(getattr(cursor, "_cursor", None), "close", None)
    if close is not None:
        try:
            close()
        except Exception:
            pass


class _ProbeCursor:
    """Cursor returning only SQLite rows while comparing PostgreSQL rows."""

    def __init__(self, primary: Any, owner: "ProbeReadConnection") -> None:
        self._primary = primary
        self._owner = owner
        self._shadow: Any = None
        self._query_id = "unknown"
        self._shadow_locked = False
        self._marker_emitted = False

    def execute(self, sql: str, params: Any = ()) -> "_ProbeCursor":
        self._release_shadow()
        self._marker_emitted = False
        self._primary.execute(sql, params or ())
        self._query_id = _query_id(sql)
        if self._owner._in_transaction or not str(sql).lstrip().upper().startswith(
            "SELECT"
        ):
            return self
        self._owner._probe._lock.acquire()
        self._shadow_locked = True
        self._owner._cursors.add(self)
        try:
            shadow_conn = self._owner._probe._connection()
            self._shadow = shadow_conn.cursor()
            self._shadow.execute(sql, params or ())
        except Exception as exc:
            self._emit("postgres_error", exc)
            self._owner._probe._discard_connection()
            self._release_shadow()
        return self

    def executemany(self, sql: str, seq_of_params: Any) -> "_ProbeCursor":
        self._release_shadow()
        self._primary.executemany(sql, seq_of_params)
        return self

    def fetchone(self) -> Any:
        try:
            primary = self._primary.fetchone()
        except BaseException:
            self._release_shadow()
            raise
        if self._shadow is None:
            return primary
        try:
            shadow = self._shadow.fetchone()
            if not _rows_equal(
                self._primary,
                [] if primary is None else [primary],
                self._shadow,
                [] if shadow is None else [shadow],
            ):
                self._emit("mismatch")
        except Exception as exc:
            self._emit("postgres_error", exc)
            self._owner._probe._discard_connection()
        finally:
            self._release_shadow()
        return primary

    def fetchmany(self, size: Optional[int] = None) -> list[Any]:
        try:
            primary = (
                self._primary.fetchmany(size)
                if size is not None
                else self._primary.fetchmany()
            )
        except BaseException:
            self._release_shadow()
            raise
        if self._shadow is None:
            return primary
        try:
            shadow = (
                self._shadow.fetchmany(size)
                if size is not None
                else self._shadow.fetchmany()
            )
            if not _rows_equal(self._primary, primary, self._shadow, shadow):
                self._emit("mismatch")
        except Exception as exc:
            self._emit("postgres_error", exc)
            self._owner._probe._discard_connection()
        finally:
            self._release_shadow()
        return primary

    def fetchall(self) -> list[Any]:
        try:
            primary = self._primary.fetchall()
        except BaseException:
            self._release_shadow()
            raise
        if self._shadow is None:
            return primary
        try:
            shadow = self._shadow.fetchall()
            if not _rows_equal(self._primary, primary, self._shadow, shadow):
                self._emit("mismatch")
        except Exception as exc:
            self._emit("postgres_error", exc)
            self._owner._probe._discard_connection()
        finally:
            self._release_shadow()
        return primary

    def __iter__(self) -> Iterator[Any]:
        try:
            while True:
                primary = self._primary.fetchone()
                if self._shadow is None:
                    if primary is None:
                        return
                    yield primary
                    continue
                try:
                    shadow = self._shadow.fetchone()
                    if not _rows_equal(
                        self._primary,
                        [] if primary is None else [primary],
                        self._shadow,
                        [] if shadow is None else [shadow],
                    ):
                        self._emit("mismatch")
                except Exception as exc:
                    self._emit("postgres_error", exc)
                    self._owner._probe._discard_connection()
                    self._release_shadow()
                if primary is None:
                    return
                yield primary
        finally:
            self._release_shadow()

    def _emit(self, reason: str, exc: Optional[BaseException] = None) -> None:
        if self._marker_emitted:
            return
        self._marker_emitted = True
        error_type = type(exc).__name__ if exc is not None else "none"
        self._owner._probe._logger.warning(
            "%s mode=probe reason=%s query=%s error_type=%s sqlite_response=true",
            READ_FALLBACK_MARKER,
            reason,
            self._query_id,
            error_type,
        )

    def _release_shadow(self) -> None:
        _close_cursor(self._shadow)
        self._shadow = None
        if self._shadow_locked:
            self._shadow_locked = False
            self._owner._probe._lock.release()
        self._owner._cursors.discard(self)

    def close(self) -> None:
        self._release_shadow()
        _close_cursor(self._primary)
        self._owner._cursors.discard(self)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._primary, name)


class ProbeReadConnection:
    """Connection proxy: writes stay SQLite; SELECTs are compared with PG."""

    def __init__(self, primary: Any, probe: "PostgresReadProbe") -> None:
        self._primary = primary
        self._probe = probe
        self._in_transaction = False
        self._cursors: set[_ProbeCursor] = set()

    def cursor(self, *args: Any, **kwargs: Any) -> _ProbeCursor:
        return _ProbeCursor(self._primary.cursor(*args, **kwargs), self)

    def execute(self, sql: str, params: Any = ()) -> _ProbeCursor:
        cursor = self.cursor().execute(sql, params)
        head = str(sql).strip().upper()
        if head.startswith("BEGIN") or head.startswith("START TRANSACTION"):
            self._in_transaction = True
        elif head.startswith("COMMIT") or head.startswith("ROLLBACK"):
            self._in_transaction = False
        return cursor

    def executemany(self, sql: str, seq_of_params: Any) -> _ProbeCursor:
        return self.cursor().executemany(sql, seq_of_params)

    def executescript(self, sql_script: str) -> Any:
        return self._primary.executescript(sql_script)

    def commit(self) -> Any:
        try:
            return self._primary.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> Any:
        try:
            return self._primary.rollback()
        finally:
            self._in_transaction = False

    def close_probe_cursors(self) -> None:
        for cursor in list(self._cursors):
            cursor.close()

    def close(self) -> Any:
        self.close_probe_cursors()
        return self._primary.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._primary, name)


class PostgresReadProbe:
    """Own the lazy, serialized PostgreSQL side of read probes."""

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._logger = logger or logging.getLogger("hermes.state.read_flip")
        self._lock = threading.RLock()
        self._conn: Any = None

    def _connection(self) -> Any:
        if self._conn is None:
            self._conn = self._connection_factory()
        return self._conn

    def _discard_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def wrap(self, primary: Any) -> ProbeReadConnection:
        return ProbeReadConnection(primary, self)

    def close(self) -> None:
        with self._lock:
            self._discard_connection()
