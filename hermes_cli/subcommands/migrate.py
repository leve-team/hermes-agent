"""``hermes migrate`` subcommand parser."""

from __future__ import annotations


def build_migrate_parser(subparsers) -> None:
    """Attach the ``migrate`` subcommand to ``subparsers``."""
    from hermes_cli.migrate import cmd_migrate, cmd_migrate_xai
    from hermes_cli.migrate_postgres import cmd_migrate_state_to_postgres

    migrate_parser = subparsers.add_parser(
        "migrate", help="Migrate configuration, models, or state databases",
        description="Diagnose and (optionally) rewrite the active config.yaml to "
            "replace references to retired models or deprecated settings; or "
            "copy an existing SQLite state database into a PostgreSQL backend.")
    migrate_subparsers = migrate_parser.add_subparsers(dest="migrate_type")

    migrate_xai = migrate_subparsers.add_parser(
        "xai", help="Migrate xAI models scheduled for retirement on May 15, 2026",
        description="Scan config.yaml for references to xAI models retiring on "
            "May 15, 2026 and, with --apply, rewrite them in-place to the "
            "official replacements per the xAI migration guide. The original "
            "config.yaml is backed up before any rewrite.")
    migrate_xai.add_argument(
        "--apply", action="store_true",
        help="Rewrite config.yaml in-place (default: dry-run, no writes)")
    migrate_xai.add_argument(
        "--no-backup", action="store_true",
        help="Skip the timestamped backup of config.yaml when applying")
    migrate_xai.set_defaults(func=cmd_migrate_xai)

    migrate_s2pg = migrate_subparsers.add_parser(
        "state-to-postgres",
        help="Online resumable COPY backfill from SQLite to PostgreSQL",
        description=(
            "Online, resumable migration of session/state data from one "
            "read-only SQLite snapshot into PostgreSQL COPY batches. The "
            "source is never copied or modified; committed batch watermarks "
            "make --resume idempotent."))
    migrate_s2pg.add_argument(
        "--dsn", metavar="TEXT",
        help=(
            "PostgreSQL DSN (postgresql://...). When omitted, resolved from "
            "HERMES_STATE_DATABASE_URL / HERMES_STATE_POSTGRES_DSN env vars "
            "or sessions.postgres_dsn in config.yaml."))
    migrate_s2pg.add_argument(
        "--sqlite-path", metavar="PATH", dest="sqlite_path",
        help="Source SQLite state.db path (default: <hermes home>/state.db).")
    migrate_s2pg.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip the confirmation prompt (required for non-interactive use).")
    migrate_s2pg.add_argument(
        "--checkpoint", metavar="PATH",
        help="Atomic watermark file (default: beside state.db).")
    migrate_s2pg.add_argument(
        "--resume", action="store_true",
        help="Resume an existing checkpoint.")
    migrate_s2pg.add_argument(
        "--batch-rows", type=int, default=5000,
        help="Rows per COPY transaction (default: 5000).")
    migrate_s2pg.add_argument(
        "--budget-bytes", type=int, default=41 * 1024 * 1024 * 1024,
        help="Stop with rc=4 after this PostgreSQL size (default: 41 GiB).")
    migrate_s2pg.add_argument(
        "--fault-inject-at", metavar="PERCENT",
        help="Drill-only interruption point, for example 50%%.")
    migrate_s2pg.set_defaults(func=cmd_migrate_state_to_postgres)
    migrate_parser.set_defaults(func=cmd_migrate)
