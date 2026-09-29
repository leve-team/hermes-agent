"""The gateway runtime status on a PostgreSQL-authority profile (levos v3).

``gateway_state.json`` is the running gateway's health record (state, platforms, active agents)
that ``hermes status``, the dashboard and the readiness probe read. On authority the pod keeps no
state files, so the record is a ``core_aux_kv`` row instead, keyed by the pod (hostname — the pod
name under Kubernetes) and the record's path: each pod's gateway still reports for itself, and a
reader in that pod reads its own gateway's record. The gateway rewrites it on every transition and
turn, so one autocommit connection is kept and reused rather than opening the store each time.
Only the active profile's own record lives here; another profile's record on authority reads as
absent (it is not on this pod's disk either).
"""

from __future__ import annotations

import contextlib
import json
import logging
import socket
import threading
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

NAMESPACE = "gateway_runtime_status"
# Rows of pods that are gone are dropped once more than this many exist (newest kept).
_MAX_RECORDS = 64

_lock = threading.Lock()
_connections: Dict[str, Any] = {}


def uses_postgres(path: Path) -> bool:
    """True when *path* is the active profile's record and that profile is on authority."""
    from hermes_constants import get_hermes_home
    from hermes_aux_store import aux_store_authority

    try:
        active = Path(path).parent.resolve() == Path(get_hermes_home()).resolve()
    except OSError:
        return False
    return active and aux_store_authority()


def _key(path: Path) -> str:
    try:
        canonical = str(Path(path).resolve())
    except OSError:
        canonical = str(path)
    return f"{socket.gethostname()}:{canonical}"


def _run(sql: str, params: tuple) -> Any:
    """Execute on the cached connection; a failure drops it and raises ``AuxStoreUnavailable``."""
    from hermes_aux_store import AuxStoreUnavailable, open_aux_kv
    from hermes_state_postgres import connect_postgres, resolve_postgres_dsn

    try:
        dsn = resolve_postgres_dsn()
    except Exception as exc:
        raise AuxStoreUnavailable("gateway runtime status: the authority store could not be resolved") from exc
    with _lock:
        try:
            conn = _connections.get(dsn)
            if conn is None:
                open_aux_kv().close()  # creates core_aux_kv once, under its schema lock
                conn = _connections[dsn] = connect_postgres(dsn)
            return conn.execute(sql, params).fetchall()
        except Exception as exc:
            stale = _connections.pop(dsn, None)
            if stale is not None:
                with contextlib.suppress(Exception):
                    stale.close()
            raise AuxStoreUnavailable(
                "gateway runtime status: the PostgreSQL authority store could not be reached") from exc


def read(path: Path) -> Optional[dict[str, Any]]:
    """The record, or None when absent or unreadable (like a missing or corrupt file)."""
    try:
        rows = _run("SELECT value FROM core_aux_kv WHERE namespace = ? AND key = ?", (NAMESPACE, _key(path)))
    except Exception as exc:
        logger.debug("gateway runtime status read failed: %s", exc)
        return None
    if not rows:
        return None
    try:
        record = json.loads(rows[0][0])
    except ValueError:
        return None
    return record if isinstance(record, dict) else None


def write(path: Path, payload: dict[str, Any]) -> None:
    """Store the record; a PostgreSQL failure raises ``AuxStoreUnavailable`` (no file)."""
    _run(
        "INSERT INTO core_aux_kv (namespace, key, value, updated_at) "
        "VALUES (?, ?, ?, EXTRACT(EPOCH FROM clock_timestamp())) "
        "ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at "
        "RETURNING 1",
        (NAMESPACE, _key(path), json.dumps(payload)),
    )
    _run(
        "DELETE FROM core_aux_kv WHERE namespace = ? AND key NOT IN (SELECT key FROM core_aux_kv "
        "WHERE namespace = ? ORDER BY updated_at DESC, key DESC LIMIT ?) RETURNING 1",
        (NAMESPACE, NAMESPACE, _MAX_RECORDS),
    )
