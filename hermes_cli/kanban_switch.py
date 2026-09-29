"""Master switch for the built-in kanban: ``kanban.enabled`` / ``HERMES_KANBAN_ENABLED``.

Off, no runtime entry point opens the kanban store (``kanban.db`` or the PostgreSQL kanban
backend): the gateway does not start its dispatcher or notifier, the tui notification poller
skips its kanban poll, the ``kanban_*`` tools are hidden and ``/kanban`` only says so. On (the
default) nothing changes. Kept free of ``kanban_db`` imports so the check itself touches no store.
"""

from __future__ import annotations

import os
from typing import Optional

ENV_VAR = "HERMES_KANBAN_ENABLED"
# Same off set as HERMES_KANBAN_DISPATCH_IN_GATEWAY; an on value lets the env re-enable a
# config that turned kanban off.
_OFF = frozenset({"0", "false", "no", "off"})
_ON = frozenset({"1", "true", "yes", "on"})


def kanban_disabled_reason() -> Optional[str]:
    """Why the built-in kanban is off (for the one log line), or None when it is on.

    The env var wins over config in both directions; an unreadable config keeps the default (on).
    """
    raw = os.environ.get(ENV_VAR, "").strip().lower()
    if raw in _OFF:
        return f"{ENV_VAR} env"
    if raw in _ON:
        return None
    try:
        from hermes_cli.config import cfg_get, load_config_readonly
        value = cfg_get(load_config_readonly(), "kanban", "enabled", default=True)
    except Exception:
        return None
    if value is False or value == 0 or (isinstance(value, str) and value.strip().lower() in _OFF):
        return "config kanban.enabled=false"
    return None


def kanban_enabled() -> bool:
    """True unless ``kanban.enabled: false`` or ``HERMES_KANBAN_ENABLED`` is off."""
    return kanban_disabled_reason() is None
