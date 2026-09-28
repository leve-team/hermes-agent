"""Local index of what we've sent (and, for WhatsApp, received) keyed by ``(chat_id, message_id)``.

Telegram does NOT echo a rich message's content back in ``reply_to_message`` (``.text``/``.caption``
empty, ``.api_kwargs`` None), and WhatsApp quotes carry only the quoted message's id (Cloud API) or a
thumbnail stub (Baileys) — never the original bytes. So a reply to something we sent arrives with no
quotable text and no way to re-fetch a quoted attachment. We remember ``message_id -> text`` and
``message_id -> [(local_path, mime)]`` at send/receive time and look them up by ``reply_to_id`` on
inbound. Best-effort and dependency-free: every operation swallows errors and degrades to a no-op /
``None`` / ``[]`` so it can never break a send or an inbound message.

On a PostgreSQL-authority profile (levos 0067) the index lives in the profile's store (``aux_kv``
namespace ``rich_sent``, the text as the row value), so a reply that lands on another pod than the
send still finds the quoted text. Attachment pairs go to ``rich_sent:media`` (a JSON list); they name
pod-local files, so another pod's lookup finds the row but filters the paths out as missing.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

_MAX_ENTRIES = 1000
_MAX_TEXT_CHARS = 2000


def _store_path() -> str:
    from hermes_constants import get_hermes_home  # honors the active profile override
    return os.path.join(str(get_hermes_home()), "state", "rich_sent_index.json")


def _kv_namespace() -> Optional[str]:
    """``aux_kv`` namespace on PostgreSQL authority, None on every other backend."""
    try:
        from hermes_aux_store import KV_RICH_SENT, aux_store_authority
    except ImportError:
        return None
    return KV_RICH_SENT if aux_store_authority() else None


def _kv_record(namespace: str, key: str, value: str) -> None:
    from hermes_aux_store import aux_kv_put, aux_kv_transaction, aux_kv_trim

    with aux_kv_transaction(namespace) as conn:
        aux_kv_put(namespace, key, value, conn=conn)
        aux_kv_trim(namespace, _MAX_ENTRIES, conn=conn)


def _load(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _update(chat_id, message_id, fields: dict) -> None:
    """Merge ``fields`` into the ``(chat_id, message_id)`` entry. No-op on any failure."""
    path = _store_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = _load(path)
        key = f"{chat_id}:{message_id}"
        entry = data.get(key)
        entry = entry if isinstance(entry, dict) else {}
        data[key] = {**entry, **fields, "ts": int(time.time())}
        if len(data) > _MAX_ENTRIES:  # trim oldest by timestamp
            for k, _ in sorted(data.items(), key=lambda kv: kv[1].get("ts", 0))[: len(data) - _MAX_ENTRIES]:
                data.pop(k, None)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp, path)  # atomic; tolerates concurrent writers racing
    except Exception:
        return


def record(chat_id, message_id, text: Optional[str]) -> None:
    """Persist ``text`` for ``(chat_id, message_id)``. No-op on any failure."""
    if not text or message_id is None or chat_id is None:
        return
    try:
        namespace = _kv_namespace()
        if namespace is not None:
            _kv_record(namespace, f"{chat_id}:{message_id}", text[:_MAX_TEXT_CHARS])
            return
    except Exception:
        return
    _update(chat_id, message_id, {"t": text[:_MAX_TEXT_CHARS]})


def record_media(chat_id, message_id, media: list[tuple[str, str]]) -> None:
    """Persist local attachment ``(path, mime)`` pairs for ``(chat_id, message_id)``."""
    if not media or message_id is None or chat_id is None:
        return
    pairs = [[str(p), str(mt or "")] for p, mt in media if p]
    try:
        namespace = _kv_namespace()
        if namespace is not None:
            _kv_record(f"{namespace}:media", f"{chat_id}:{message_id}", json.dumps(pairs, ensure_ascii=False))
            return
    except Exception:
        return
    _update(chat_id, message_id, {"m": pairs})


def _entry(chat_id, message_id) -> dict:
    if message_id is None or chat_id is None:
        return {}
    entry = _load(_store_path()).get(f"{chat_id}:{message_id}")
    return entry if isinstance(entry, dict) else {}


_FILE = object()  # _kv_lookup: not on PostgreSQL authority, read the file


def _kv_lookup(chat_id, message_id, suffix: str = ""):
    """The ``(chat_id, message_id)`` row on PostgreSQL authority (None when absent or on a store
    failure), ``_FILE`` on every other backend."""
    try:
        namespace = _kv_namespace()
        if namespace is None:
            return _FILE
        if message_id is None or chat_id is None:
            return None
        from hermes_aux_store import aux_kv_get

        return aux_kv_get(f"{namespace}{suffix}", f"{chat_id}:{message_id}")
    except Exception:
        return None


def lookup(chat_id, message_id) -> Optional[str]:
    """Return stored text for ``(chat_id, message_id)`` or ``None``."""
    text = _kv_lookup(chat_id, message_id)
    if text is _FILE:
        return _entry(chat_id, message_id).get("t") or None
    return text or None


def lookup_media(chat_id, message_id) -> list[tuple[str, str]]:
    """Return stored ``(path, mime)`` pairs whose file still exists (attachments may be temp files)."""
    stored = _kv_lookup(chat_id, message_id, ":media")
    if stored is _FILE:
        pairs = _entry(chat_id, message_id).get("m") or []
    else:
        try:
            pairs = json.loads(stored or "[]")
        except ValueError:
            pairs = []
    return [(p, mt) for p, mt in pairs if isinstance(p, str) and os.path.isfile(p)]
