"""CLI handlers for ``hermes migrate ...``.

Exposes two subcommands:

* ``hermes migrate xai`` — diagnoses and (with --apply) rewrites references to
  xAI models retired on May 15, 2026.
* ``hermes migrate state-to-postgres`` — online, resumable COPY backfill of the
  SQLite state database into a PostgreSQL backend.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from hermes_cli.colors import Colors, color
from hermes_cli.config import load_config


def cmd_migrate(args: Any) -> int:
    """Dispatcher for ``hermes migrate <subtype>``."""
    sub = getattr(args, "migrate_type", None)
    if sub == "xai":
        return cmd_migrate_xai(args)
    if sub == "state-to-postgres":
        return cmd_migrate_state_to_postgres(args)

    print(
        "usage: hermes migrate {xai,state-to-postgres} [options]",
        file=sys.stderr,
    )
    return 2


def cmd_migrate_xai(args: Any) -> int:
    """Run xAI May-15 model migration in dry-run or apply mode."""
    from hermes_cli.xai_retirement import (
        MIGRATION_GUIDE_URL,
        RETIREMENT_DATE,
        apply_migration,
        find_retired_xai_refs,
        format_issue,
    )

    apply = bool(getattr(args, "apply", False))
    no_backup = bool(getattr(args, "no_backup", False))

    config = load_config()
    issues = find_retired_xai_refs(config)

    print()
    print(color(
        f"◆ xAI Model Retirement Migration ({RETIREMENT_DATE})",
        Colors.CYAN, Colors.BOLD,
    ))
    print()

    if not issues:
        print(f"  {color('✓', Colors.GREEN)} No retired xAI models in config — nothing to migrate.")
        return 0

    print(f"  Found {len(issues)} retired xAI model reference(s):")
    print()
    for issue in issues:
        print(f"    {color('⚠', Colors.YELLOW)} {format_issue(issue)}")
    print()
    print(f"    {color('→', Colors.CYAN)} Migration guide: {MIGRATION_GUIDE_URL}")
    print()

    config_path = _resolve_config_path()

    if not apply:
        print(color("Dry-run mode — no changes written.", Colors.DIM))
        print(color(
            "Re-run with `hermes migrate xai --apply` to rewrite "
            f"{config_path} in-place (backup created automatically).",
            Colors.DIM,
        ))
        return 0

    if not config_path or not config_path.exists():
        print(
            f"  {color('✗', Colors.RED)} Could not locate config.yaml "
            f"(looked at: {config_path})",
            file=sys.stderr,
        )
        return 1

    try:
        result = apply_migration(
            config_path=config_path,
            issues=issues,
            backup=not no_backup,
        )
    except Exception as exc:
        print(
            f"  {color('✗', Colors.RED)} Migration failed: {exc}",
            file=sys.stderr,
        )
        return 1

    if not result.config_changed:
        print(f"  {color('⚠', Colors.YELLOW)} No changes written.")
        return 0

    if result.backup_path is not None:
        print(f"  {color('✓', Colors.GREEN)} Backup: {result.backup_path}")
    print(
        f"  {color('✓', Colors.GREEN)} Updated {len(result.issues_resolved)} "
        f"slot(s) in {result.file_path}"
    )
    print()
    print(color(
        "Run `hermes doctor` to confirm no retired xAI models remain.",
        Colors.DIM,
    ))
    return 0


def cmd_migrate_state_to_postgres(args: Any) -> int:
    """Copy the SQLite state database into a PostgreSQL backend.

    Wraps :func:`migrate_state_to_postgres.migrate` — the migration logic
    lives entirely in that standalone module; this handler only handles
    argument resolution, user confirmation, and result reporting.
    """
    import os
    import urllib.parse

    # Lazy import keeps the postgres extra optional for unrelated migrate
    # subcommands (e.g. xai).  Import the whole module so mocks can target
    # the module-level names via ``migrate_state_to_postgres.<name>``.
    try:
        import migrate_state_to_postgres as _m2pg
    except ImportError as exc:
        print(
            f"  {color('✗', Colors.RED)} Could not import migration module: {exc}",
            file=sys.stderr,
        )
        return 1

    # --- Resolve SQLite source path ---
    sqlite_path = _m2pg._resolve_sqlite_path(getattr(args, "sqlite_path", None))

    # --- Resolve PostgreSQL DSN ---
    explicit_dsn: str | None = getattr(args, "dsn", None)
    if explicit_dsn:
        dsn: str | None = explicit_dsn
    else:
        # Check env vars first (same order as the standalone script).
        dsn = None
        for key in ("HERMES_STATE_DATABASE_URL", "HERMES_STATE_POSTGRES_DSN"):
            val = (os.environ.get(key) or "").strip()
            if val:
                dsn = val
                break

        if not dsn:
            # Fall back to the config-based resolver so a user who already set
            # sessions.state_backend: postgres can run the command bare.
            try:
                from hermes_state_postgres import resolve_postgres_dsn

                config = load_config()
                dsn = resolve_postgres_dsn(config)
            except Exception:
                dsn = None

    if not dsn:
        print(
            f"  {color('✗', Colors.RED)} No PostgreSQL DSN found.\n"
            "  Provide one with --dsn, set the HERMES_STATE_DATABASE_URL or\n"
            "  HERMES_STATE_POSTGRES_DSN environment variable, or set\n"
            "  sessions.state_backend: postgres and sessions.postgres_dsn\n"
            "  in config.yaml.",
            file=sys.stderr,
        )
        return 1

    yes: bool = bool(getattr(args, "yes", False))
    is_tty: bool = sys.stdin.isatty()

    # Non-interactive without --yes: refuse rather than hang.
    if not is_tty and not yes:
        print(
            f"  {color('✗', Colors.RED)} stdin is not a TTY and --yes was not passed.\n"
            "  Re-run with -y / --yes to confirm the migration non-interactively.",
            file=sys.stderr,
        )
        return 1

    # --- Build a redacted representation of the target for display ---
    try:
        parsed = urllib.parse.urlparse(dsn)
        # Mask any password in the netloc.
        if parsed.password:
            safe_netloc = parsed.hostname or ""
            if parsed.port:
                safe_netloc += f":{parsed.port}"
            redacted = parsed._replace(
                netloc=f"{parsed.username}:***@{safe_netloc}"
                if parsed.username
                else f"***@{safe_netloc}",
            ).geturl()
        else:
            redacted = dsn
    except Exception:
        # If the DSN is not a URL (e.g. key=value format), redact conservatively.
        redacted = "<DSN — details hidden>"

    # --- Confirmation prompt ---
    if not yes:
        print()
        print(color("◆ SQLite → PostgreSQL State Migration", Colors.CYAN, Colors.BOLD))
        print()
        print(f"  Source : {sqlite_path}")
        print(f"  Target : {redacted}")
        print()
        print(
            color(
                "  This will copy all sessions and messages into the target database.\n"
                "  The SQLite source is opened read-only and is never modified.",
                Colors.DIM,
            )
        )
        print()
        try:
            answer = input("  Proceed? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            print("  Aborted.", file=sys.stderr)
            return 1
        if answer not in ("y", "yes"):
            print("  Aborted.")
            return 1
        print()

    # --- Run the migration ---
    try:
        summary = _m2pg.migrate(
            sqlite_path,
            dsn,
            checkpoint_path=(
                Path(args.checkpoint) if getattr(args, "checkpoint", None) else None
            ),
            resume=bool(getattr(args, "resume", False)),
            batch_rows=int(
                getattr(args, "batch_rows", _m2pg.DEFAULT_BATCH_ROWS)
            ),
            budget_bytes=int(
                getattr(args, "budget_bytes", _m2pg.DEFAULT_BUDGET_BYTES)
            ),
            fault_inject_at=getattr(args, "fault_inject_at", None),
        )
    except _m2pg.BackfillBudgetExceeded as exc:
        print(f"  {color('✗', Colors.RED)} DISK_GUARD: {exc}", file=sys.stderr)
        return 4
    except _m2pg.InjectedBackfillFault as exc:
        print(f"  {color('✗', Colors.RED)} {exc}", file=sys.stderr)
        return 3
    except SystemExit as exc:
        # migrate() raises SystemExit for user-facing errors (missing file,
        # missing postgres extra, etc.).
        print(f"  {color('✗', Colors.RED)} {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        # Catch psycopg / connection errors so no raw traceback reaches the user.
        exc_type = type(exc).__name__
        print(
            f"  {color('✗', Colors.RED)} Migration failed ({exc_type}): {exc}\n"
            "  Check that the DSN is correct and the PostgreSQL server is reachable.",
            file=sys.stderr,
        )
        return 1

    # --- Report results ---
    # Compare against the counts scoped to THIS migration, not the target's
    # whole-table totals: a target that already holds rows would otherwise
    # satisfy any >= check no matter how much of the source was dropped.
    src_s = summary["source_sessions"]
    src_m = summary["source_messages"]
    got_s = summary["migrated_sessions"]
    got_m = summary["migrated_messages"]
    dst_s = summary["target_sessions"]
    dst_m = summary["target_messages"]

    sessions_ok = got_s == src_s
    messages_ok = got_m == src_m
    ok = sessions_ok and messages_ok and summary.get("nul_rows", 0) == 0

    if ok:
        print(
            f"  {color('✓', Colors.GREEN)} Migration complete.\n"
            f"    Sessions : {got_s}/{src_s} migrated\n"
            f"    Messages : {got_m}/{src_m} migrated\n"
            f"    Target now holds {dst_s} sessions / {dst_m} messages in total\n"
            f"    SQLite source left untouched: {summary['sqlite_path']}"
        )
    else:
        print(
            f"  {color('⚠', Colors.YELLOW)} Migration INCOMPLETE — rows are missing "
            "from the target. Do not switch backends yet.\n"
            f"    Sessions : {got_s}/{src_s} migrated"
            + (f"  {color('← MISSING', Colors.YELLOW)}" if not sessions_ok else "")
            + f"\n    Messages : {got_m}/{src_m} migrated"
            + (f"  {color('← MISSING', Colors.YELLOW)}" if not messages_ok else "")
            + f"\n    nul_rows : {summary.get('nul_rows', 0)}"
            + f"\n    SQLite source left untouched: {summary['sqlite_path']}\n"
            + "\n  Rows keep their original SQLite ids and are inserted with "
            "ON CONFLICT DO NOTHING,\n  so this usually means the target already "
            "contains rows with the same ids.\n  Migrate into an empty database."
        )
        return 1

    return 0


def _resolve_config_path() -> Path:
    """Best-effort: locate the active config.yaml on disk."""
    from hermes_cli.config import get_hermes_home

    return get_hermes_home() / "config.yaml"
