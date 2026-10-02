"""Turn trusted context: the user-authority ticket a broker attaches to one ``prompt.submit``
(``_levos_turn``), readable by that turn's tool handlers and nothing else.

``prompt.submit`` pops the ticket off its params and wraps it; ``_run_prompt_submit`` stages it
on the agent; ``_bind_turn_identity`` consumes the staged value once and registers it under the
turn id; ``registry.dispatch`` binds ``lookup(turn id)`` on the calling thread for one handler
call; the turn's ``finally`` unregisters it. Consumers call :func:`current`.

Thread-local, never a ContextVar: tool pools and delegate children run under a copied context,
and a child agent must not inherit its parent's authority. See
docs/levos-v3-pg-authority-port.md §8.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional


class TrustedContext:
    """One turn's ticket, reachable only as ``.token``: it renders as ``<redacted>``,
    compares by identity, and refuses pickle and copy (a pickled or copied ticket is one
    the turn's ``finally`` cannot take back)."""

    __slots__ = ("_token",)

    def __init__(self, token: str) -> None:
        self._token = token

    @property
    def token(self) -> str:
        return self._token

    def __repr__(self) -> str:
        return "<redacted>"

    __str__ = __repr__

    def __reduce_ex__(self, protocol):
        raise TypeError("TrustedContext cannot be pickled or copied")


_lock = threading.Lock()
_by_turn: Dict[str, TrustedContext] = {}
_local = threading.local()


def register(turn_id: str, tc: TrustedContext) -> None:
    with _lock:
        _by_turn[turn_id] = tc


def unregister(turn_id: Optional[str]) -> None:
    with _lock:
        _by_turn.pop(turn_id or "", None)


def lookup(turn_id: Optional[str]) -> Optional[TrustedContext]:
    if not turn_id:
        return None
    with _lock:
        return _by_turn.get(turn_id)


def current() -> Optional[TrustedContext]:
    """The ticket of the turn whose tool handler is running on this thread, else None."""
    return getattr(_local, "tc", None)


def _bind(tc: Optional[TrustedContext]) -> Optional[TrustedContext]:
    """Bind *tc* on this thread; returns the previous binding for :func:`_reset`."""
    previous = current()
    _local.tc = tc
    return previous


def _reset(previous: Optional[TrustedContext]) -> None:
    _local.tc = previous
