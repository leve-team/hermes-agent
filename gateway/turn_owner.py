"""Who owns a running turn, across pods (levos 0062).

On a PostgreSQL-authority profile two gateway pods can overlap during a
rolling update. Each has its own disk and they share only the profile's
PostgreSQL store, so neither a ``.clean_shutdown`` file nor a local pid can
say whether a turn marker left in ``gateway_routing`` belongs to a process
that is still running it. This module answers that with a lease row per
running turn in ``core_gateway_turn_leases``:

* :func:`acquire` inserts the row (owner, lease ``LEASE_SECONDS`` ahead on
  the PostgreSQL server clock) BEFORE the routing marker is written, and
  :func:`drop` deletes it AFTER the marker is cleared. A daemon thread of the
  owning process renews its rows every ``LEASE_RENEW_SECONDS``.
* At exit the process expires its remaining rows at once with an outcome
  (:func:`release`): ``clean`` = drop the marker without resuming (the old
  clean-shutdown receipt), ``interrupted`` = resume. A killed process stops
  renewing and its rows run out after ``LEASE_SECONDS``.
* :func:`claim_dead` hands every expired/released row of a routing scope to
  exactly one claimant (``DELETE ... RETURNING`` on the row as it was read).

The owner id names the pod: ``hostname|pid namespace|pid|process start``.
Kubernetes sets the hostname to the pod name, so a claimant in the same pod
and PID namespace may also treat a vanished owner pid as dead without waiting
for the lease; it never judges another pod's pid.

:class:`LeaseRenewer` and :func:`owner_id`/:func:`owner_gone` are shared with
the tui interrupted-turn markers (``tui_gateway/turn_marker.py``).
"""

from __future__ import annotations

import contextlib
import logging
import os
import socket
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, List, Optional

logger = logging.getLogger(__name__)

# Same values and server clock as the cron execution lease (levos 0060).
LEASE_SECONDS = 120.0
LEASE_RENEW_SECONDS = 30.0
SERVER_EPOCH = "EXTRACT(EPOCH FROM clock_timestamp())::float8"

# Routing markers written with a lease carry this prefix; the token is opaque
# to everything else (compare-and-swap equality only). A marker without it was
# written by a gateway older than levos 0062 and has no lease row.
TOKEN_PREFIX = "l1:"
# Rows a draining gateway leaves for sessions it marked resume_pending.
HANDOFF_PREFIX = "handoff:"

OUTCOME_CLEAN = "clean"
OUTCOME_INTERRUPTED = "interrupted"

_STORE = "gateway_turns"


def enabled() -> bool:
    """True when the active profile keeps its session store on PostgreSQL."""
    from hermes_aux_store import aux_store_authority

    return aux_store_authority()


def new_token() -> str:
    return TOKEN_PREFIX + uuid.uuid4().hex


def is_leased(token: Optional[str]) -> bool:
    return bool(token) and str(token).startswith((TOKEN_PREFIX, HANDOFF_PREFIX))


# ---------------------------------------------------------------------------
# Owner identity
# ---------------------------------------------------------------------------

_owner_lock = threading.Lock()
_owner: Optional[tuple[int, str]] = None


def _pid_namespace() -> str:
    try:
        return os.readlink("/proc/self/ns/pid")
    except OSError:
        return ""


def owner_id() -> str:
    """This process as ``hostname|pid namespace|pid|start time``."""
    global _owner
    pid = os.getpid()
    with _owner_lock:
        if _owner is None or _owner[0] != pid:
            from gateway.status import get_process_start_time

            start = get_process_start_time(pid)
            _owner = (
                pid,
                "|".join((
                    socket.gethostname(),
                    _pid_namespace(),
                    str(pid),
                    str(start or ""),
                )),
            )
        return _owner[1]


def owner_gone(owner: str) -> bool:
    """True only when *owner* provably ran in this pod and PID namespace and
    its process is gone; anything else is left to the lease."""
    try:
        host, namespace, pid_text, start_text = str(owner).split("|")
        pid, start = int(pid_text), int(start_text)
    except ValueError:
        return False
    if not namespace or host != socket.gethostname() or namespace != _pid_namespace():
        return False
    if pid == os.getpid():
        return owner != owner_id()
    from gateway.status import get_process_start_time

    return get_process_start_time(pid) != start


class LeaseRenewer:
    """One daemon thread per process that calls *renew* every interval."""

    def __init__(self, name: str, renew: Callable[[], Any]):
        self.name = name
        self._renew = renew
        self._guard = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._pid = 0

    def start(self) -> None:
        with self._guard:
            alive = self._thread is not None and self._thread.is_alive()
            if alive and self._pid == os.getpid():
                return
            self._stop = threading.Event()
            self._pid = os.getpid()
            self._thread = threading.Thread(
                target=self._run, args=(self._stop,), name=self.name, daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        with self._guard:
            thread, self._thread = self._thread, None
            self._stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=10)

    def _run(self, stop: threading.Event) -> None:
        while not stop.wait(LEASE_RENEW_SECONDS):
            try:
                self._renew()
            except Exception as exc:
                # Keep trying: a lease that runs out while PostgreSQL is away
                # lets another pod resume the turn, which is what a dead owner
                # would get as well.
                logger.warning("%s: lease renewal failed: %s", self.name, exc)


# ---------------------------------------------------------------------------
# Gateway turn leases
# ---------------------------------------------------------------------------


def _initialize(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, _STORE):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gateway_turn_leases (
                 token TEXT PRIMARY KEY,
                 scope TEXT NOT NULL,
                 session_key TEXT NOT NULL,
                 owner TEXT NOT NULL,
                 started_at REAL NOT NULL,
                 lease_expires_at REAL NOT NULL,
                 outcome TEXT
               )"""
        )


@contextlib.contextmanager
def _store() -> Iterator[Any]:
    from hermes_aux_store import open_aux_postgres

    conn = open_aux_postgres(_STORE, initialize=_initialize)
    try:
        yield conn
    finally:
        conn.close()


def _insert(conn, token: str, scope: str, session_key: str) -> None:
    conn.execute(
        "INSERT INTO gateway_turn_leases "
        "(token, scope, session_key, owner, started_at, lease_expires_at) "
        f"VALUES (?, ?, ?, ?, {SERVER_EPOCH}, {SERVER_EPOCH} + ?)",
        (token, scope, session_key, owner_id(), float(LEASE_SECONDS)),
    )


def renew() -> int:
    """Push this process's unreleased leases LEASE_SECONDS ahead."""
    with _store() as conn:
        return conn.execute(
            f"UPDATE gateway_turn_leases SET lease_expires_at = {SERVER_EPOCH} + ? "
            "WHERE owner = ? AND outcome IS NULL",
            (float(LEASE_SECONDS), owner_id()),
        ).rowcount


_renewer = LeaseRenewer("gateway-turn-lease", renew)


def acquire(token: str, *, scope: str, session_key: str) -> None:
    """Lease *token* to this process. Raises when PostgreSQL cannot serve."""
    with _store() as conn:
        _insert(conn, token, scope, session_key)
    _renewer.start()


def drop(token: str) -> bool:
    """Forget this process's lease on a concluded turn."""
    with _store() as conn:
        return bool(
            conn.execute(
                "DELETE FROM gateway_turn_leases WHERE token = ? AND owner = ?",
                (token, owner_id()),
            ).rowcount
        )


def hand_over(*, scope: str, session_keys: Iterable[str]) -> int:
    """Leave a row per resume_pending session for a peer to pick up once this
    process has released (or lost) its leases."""
    keys = [key for key in dict.fromkeys(session_keys) if key]
    if not keys:
        return 0
    with _store() as conn:
        with conn:
            for key in keys:
                _insert(conn, HANDOFF_PREFIX + uuid.uuid4().hex, scope, key)
    _renewer.start()
    return len(keys)


def release(outcome: str) -> int:
    """Expire every lease of this process now, recording how it ended."""
    if outcome not in (OUTCOME_CLEAN, OUTCOME_INTERRUPTED):
        raise ValueError(f"unknown turn outcome {outcome!r}")
    _renewer.stop()
    with _store() as conn:
        return conn.execute(
            "UPDATE gateway_turn_leases SET lease_expires_at = 0, outcome = ? "
            "WHERE owner = ? AND outcome IS NULL",
            (outcome, owner_id()),
        ).rowcount


@dataclass(frozen=True)
class Claim:
    token: str
    session_key: str
    outcome: Optional[str]

    @property
    def handoff(self) -> bool:
        return self.token.startswith(HANDOFF_PREFIX)


def claim_dead(scope: str) -> List[Claim]:
    """Take every lease of *scope* whose owner is gone, each by one claimant.

    A row is dead when it was released, its lease ran out, or its owner ran in
    this pod and has exited. The delete matches the row exactly as read, so an
    owner that renewed in between keeps it and a concurrent claimant gets 0 rows.
    """
    me = owner_id()
    claims: List[Claim] = []
    with _store() as conn:
        rows = conn.execute(
            "SELECT token, session_key, owner, outcome, lease_expires_at, "
            f"lease_expires_at < {SERVER_EPOCH} AS expired "
            "FROM gateway_turn_leases WHERE scope = ?",
            (scope,),
        ).fetchall()
        for row in rows:
            if row["owner"] == me and row["outcome"] is None:
                continue
            if not (row["expired"] or owner_gone(row["owner"])):
                continue
            taken = conn.execute(
                "DELETE FROM gateway_turn_leases WHERE token = ? AND owner = ? "
                "AND lease_expires_at = ? AND outcome IS NOT DISTINCT FROM ? "
                "RETURNING token",
                (row["token"], row["owner"], row["lease_expires_at"], row["outcome"]),
            ).fetchone()
            if taken is not None:
                claims.append(Claim(row["token"], row["session_key"], row["outcome"]))
    return claims
