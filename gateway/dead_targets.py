"""Persistent registry of delivery targets that are confirmed unreachable.

When a messaging platform reports that a target chat is permanently gone — a
deleted group (``Forbidden: the group chat was deleted``), a bot kicked/blocked,
or a deactivated user — re-sending to it on every cron tick or every fan-out
delivery wastes a send attempt against the platform's flood-control envelope and
spams the logs.  This registry lets the delivery layer short-circuit a target it
has already proven dead, while staying self-healing: any successful send to that
target clears the flag, so a user who re-adds the bot (or restores the chat)
recovers automatically with no manual cleanup.

Scope is deliberately narrow.  Only *whole-chat* deaths are recorded — the
``forbidden`` and chat-level ``not_found`` (``chat not found``) error kinds.
Thread/topic-level ``not_found`` is NOT recorded here: the adapters already
self-heal that by retrying without ``reply_to`` (see the Telegram adapter's
reply-target-deleted path), and a deleted topic does not mean the parent chat is
dead.

The store is a small JSON file under the active profile's HERMES_HOME so each
profile keeps its own dead set.  Reads/writes are best-effort: a corrupt or
unwritable file degrades to an in-memory-only registry rather than raising on
the delivery path.

On a PostgreSQL-authority profile (levos 0067) the set lives in the profile's
store instead (``aux_kv`` namespace ``dead_targets``) and every check reads it,
so a target one pod proved dead — or revived — counts the same on every pod.
A store failure degrades to this process's in-memory set, as a bad file does.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from hermes_cli.config import get_hermes_home

logger = logging.getLogger(__name__)

# Error kinds (from gateway.platforms.base.classify_send_error) that mean the
# *whole chat* is unreachable, not a transient or thread-level problem.
_DEAD_ERROR_KINDS = frozenset({"forbidden", "not_found"})


def _normalize(platform: str, chat_id: str) -> str:
    """Canonical key for a (platform, chat_id) pair."""
    return f"{str(platform).strip().lower()}:{str(chat_id).strip()}"


class DeadTargetRegistry:
    """Thread-safe, persistent set of confirmed-dead delivery targets.

    Keyed on ``platform:chat_id``.  Stores the reason and a timestamp for
    observability.  Self-healing: :meth:`clear` (called on a successful send)
    removes the flag.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self._lock = threading.RLock()
        self._dead: Dict[str, Dict[str, object]] = {}
        self._kv_namespace: Optional[str] = None
        if path is not None:
            self._path = path
        else:
            self._path = get_hermes_home() / "gateway" / "dead_targets.json"
            self._kv_namespace = _kv_namespace()
        if self._kv_namespace is None:
            self._load()

    # -- persistence -------------------------------------------------------

    def _kv(self, operation: str, call, fallback):
        """Run *call* on the PostgreSQL set; on failure log and use *fallback*."""
        try:
            return call()
        except Exception as exc:
            logger.warning(
                "dead_targets: PostgreSQL %s failed (%s) — using this process's "
                "in-memory set", operation, type(exc).__name__,
            )
            return fallback()

    def _load(self) -> None:
        try:
            if self._path.exists():
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    # Only keep well-shaped entries.
                    self._dead = {
                        k: v for k, v in raw.items() if isinstance(v, dict)
                    }
        except (OSError, ValueError) as exc:
            logger.debug("dead_targets: could not load %s (%s) — starting empty",
                         self._path, exc)
            self._dead = {}

    def _flush_locked(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._dead, indent=2), encoding="utf-8")
            tmp.replace(self._path)
        except OSError as exc:
            # Best-effort: keep the in-memory state, don't break delivery.
            logger.debug("dead_targets: could not persist %s (%s)", self._path, exc)

    # -- public API --------------------------------------------------------

    @staticmethod
    def is_dead_error_kind(error_kind: Optional[str]) -> bool:
        """Return True when ``error_kind`` denotes a permanent whole-chat death."""
        return bool(error_kind) and error_kind in _DEAD_ERROR_KINDS

    def is_dead(self, platform: str, chat_id: Optional[str]) -> bool:
        if not chat_id:
            return False
        key = _normalize(platform, chat_id)
        with self._lock:
            if self._kv_namespace is not None:
                from hermes_aux_store import aux_kv_get

                return self._kv(
                    "read",
                    lambda: aux_kv_get(self._kv_namespace, key) is not None,
                    lambda: key in self._dead,
                )
            return key in self._dead

    def mark_dead(self, platform: str, chat_id: Optional[str],
                  reason: str = "") -> bool:
        """Record a target as confirmed-dead.  Returns True if newly added."""
        if not chat_id:
            return False
        key = _normalize(platform, chat_id)
        with self._lock:
            existed = key in self._dead
            self._dead[key] = {
                "platform": str(platform).strip().lower(),
                "chat_id": str(chat_id),
                "reason": str(reason)[:200],
                "marked_at": time.time(),
            }
            if self._kv_namespace is not None:
                existed = self._kv(
                    "write",
                    lambda: self._kv_put(key, self._dead[key]),
                    lambda: existed,
                )
            else:
                self._flush_locked()
        if not existed:
            logger.info(
                "dead_targets: marked %s as unreachable (%s) — future deliveries "
                "to this target will be skipped until a send succeeds",
                key, reason or "no reason given",
            )
        return not existed

    def clear(self, platform: str, chat_id: Optional[str]) -> bool:
        """Remove a target's dead flag (self-healing).  Returns True if it was set."""
        if not chat_id:
            return False
        key = _normalize(platform, chat_id)
        with self._lock:
            if self._kv_namespace is not None:
                from hermes_aux_store import aux_kv_delete

                known = self._dead.pop(key, None) is not None
                cleared = self._kv(
                    "write", lambda: aux_kv_delete(self._kv_namespace, key), lambda: known
                )
                if cleared:
                    logger.info("dead_targets: cleared %s (delivery succeeded again)", key)
                return cleared
            if key in self._dead:
                del self._dead[key]
                self._flush_locked()
                logger.info("dead_targets: cleared %s (delivery succeeded again)", key)
                return True
        return False

    def all_dead(self) -> Dict[str, Dict[str, object]]:
        """Snapshot of the current dead set (for diagnostics / `hermes` CLI)."""
        with self._lock:
            snapshot = {k: dict(v) for k, v in self._dead.items()}
            if self._kv_namespace is not None:
                return self._kv("read", self._kv_all, lambda: snapshot)
            return snapshot

    def _kv_put(self, key: str, entry: Dict[str, object]) -> bool:
        """Store *entry*; True when *key* was already dead."""
        from hermes_aux_store import aux_kv_get, aux_kv_put, aux_kv_transaction

        with aux_kv_transaction(self._kv_namespace) as conn:
            existed = aux_kv_get(self._kv_namespace, key, conn=conn) is not None
            aux_kv_put(self._kv_namespace, key, json.dumps(entry), conn=conn)
        return existed

    def _kv_all(self) -> Dict[str, Dict[str, object]]:
        from hermes_aux_store import aux_kv_items

        result: Dict[str, Dict[str, object]] = {}
        for key, value in aux_kv_items(self._kv_namespace):
            try:
                entry = json.loads(value)
            except ValueError:
                continue
            if isinstance(entry, dict):
                result[key] = entry
        return result


def _kv_namespace() -> Optional[str]:
    """``aux_kv`` namespace on PostgreSQL authority, None on every other backend."""
    try:
        from hermes_aux_store import KV_DEAD_TARGETS, aux_store_authority
    except ImportError:
        return None
    return KV_DEAD_TARGETS if aux_store_authority() else None
