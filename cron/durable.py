"""Cron state one run hands to the next, on PostgreSQL authority (levos 0068).

Two overlapping pods of an authority profile share nothing but the profile's
PostgreSQL store. The job rows, locks and ledgers already live there (0060),
but three things the next run reads were still files on the disk of whichever
pod wrote them:

* the latest output of a job — what ``context_from`` injects into the next
  prompt (``core_cron_outputs`` kind ``latest``). The per-run
  ``cron/output/<job>/*.md`` history stays a pod-local work product;
* the monitor baseline ``monitor_last_output.txt`` — the text the next change
  is diffed against (kind ``monitor``);
* the body of a job script under ``scripts/`` (``core_cron_scripts``). The
  cronjob tool stores only the path; the agent writes the file with its file
  or terminal tools. The local file wins while it exists (the image re-seeds
  managed scripts on every boot); the stored copy is written back when a pod's
  disk does not have it.

``cron/suggestions.json`` follows the same authority inside
``cron/suggestions.py``. Off authority nothing here is reached. On authority a
PostgreSQL failure raises :class:`hermes_aux_store.AuxStoreUnavailable`;
nothing falls back to a file.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from hermes_time import now as _hermes_now

LATEST = "latest"
MONITOR = "monitor"


def authority() -> bool:
    from hermes_aux_store import aux_store_authority

    return aux_store_authority()


def _initialize_outputs(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, "cron_outputs"):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS cron_outputs (
                 job_id TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 content TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 PRIMARY KEY (job_id, kind)
               )"""
        )


def _initialize_scripts(conn) -> None:
    from hermes_aux_store import aux_schema_transaction

    with aux_schema_transaction(conn, "cron_scripts"):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS cron_scripts (
                 path TEXT PRIMARY KEY,
                 content BYTEA NOT NULL,
                 sha256 TEXT NOT NULL,
                 mode INTEGER NOT NULL,
                 updated_at TEXT NOT NULL
               )"""
        )


@contextlib.contextmanager
def _transaction(store: str, initialize: Callable[[Any], None]) -> Iterator[Any]:
    from hermes_aux_store import open_aux_postgres

    conn = open_aux_postgres(store, initialize=initialize)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Latest output and monitor baseline
# ---------------------------------------------------------------------------


def save_output(job_id: str, kind: str, content: str) -> None:
    """Make *content* the job's stored *kind* output (``LATEST`` or ``MONITOR``)."""
    # A PostgreSQL TEXT value cannot hold NUL; everything else is kept as is.
    text = str(content).replace("\x00", "\ufffd")
    with _transaction("cron_outputs", _initialize_outputs) as conn:
        conn.execute(
            """INSERT INTO cron_outputs (job_id, kind, content, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (job_id, kind) DO UPDATE
               SET content = EXCLUDED.content, updated_at = EXCLUDED.updated_at""",
            (str(job_id), kind, text, _hermes_now().isoformat()),
        )


def load_output(job_id: str, kind: str = LATEST) -> Optional[str]:
    with _transaction("cron_outputs", _initialize_outputs) as conn:
        row = conn.execute(
            "SELECT content FROM cron_outputs WHERE job_id = ? AND kind = ?",
            (str(job_id), kind),
        ).fetchone()
    return None if row is None else row["content"]


def delete_outputs(job_id: str) -> None:
    with _transaction("cron_outputs", _initialize_outputs) as conn:
        conn.execute("DELETE FROM cron_outputs WHERE job_id = ?", (str(job_id),))


# ---------------------------------------------------------------------------
# Script bodies
# ---------------------------------------------------------------------------


def _script_key(path: Path, scripts_dir: Path) -> str:
    """Stored key: *path* relative to the resolved scripts dir (raises if outside)."""
    return path.relative_to(scripts_dir).as_posix()


def _upsert_script(conn, key: str, data: bytes, mode: int) -> bool:
    digest = hashlib.sha256(data).hexdigest()
    row = conn.execute(
        "SELECT sha256, mode FROM cron_scripts WHERE path = ?", (key,)
    ).fetchone()
    if row is not None and (row["sha256"], row["mode"]) == (digest, mode):
        return False
    conn.execute(
        """INSERT INTO cron_scripts (path, content, sha256, mode, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (path) DO UPDATE
           SET content = EXCLUDED.content, sha256 = EXCLUDED.sha256,
               mode = EXCLUDED.mode, updated_at = EXCLUDED.updated_at""",
        (key, data, digest, mode, _hermes_now().isoformat()),
    )
    return True


def capture_script(path: Path, scripts_dir: Path) -> bool:
    """Store the body of the existing script *path*; True when it changed.

    *path* and *scripts_dir* are resolved, *path* inside *scripts_dir* (the
    run guard's containment check has already passed).
    """
    key = _script_key(path, scripts_dir)
    data = path.read_bytes()
    mode = stat.S_IMODE(path.stat().st_mode)
    with _transaction("cron_scripts", _initialize_scripts) as conn:
        return _upsert_script(conn, key, data, mode)


def restore_script(path: Path, scripts_dir: Path) -> bool:
    """Write the stored body of the missing script *path*; False if none is stored."""
    from utils import atomic_replace

    key = _script_key(path, scripts_dir)
    with _transaction("cron_scripts", _initialize_scripts) as conn:
        row = conn.execute(
            "SELECT content, mode FROM cron_scripts WHERE path = ?", (key,)
        ).fetchone()
    if row is None:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent), prefix=".restore_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(bytes(row["content"]))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, int(row["mode"]) & 0o777)
        atomic_replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return True


# ---------------------------------------------------------------------------
# One-shot move of the files an authority profile wrote before 0068
# ---------------------------------------------------------------------------


def _digests(paths: List[Path]) -> Dict[str, str]:
    from hermes_aux_store import _sha256

    return {str(path): _sha256(path) for path in paths}


def _output_sources(output_dir: Path) -> Tuple[List[Tuple[str, str, str]], List[Path]]:
    """``(job_id, kind, content)`` rows; the latest ``*.md`` is chosen the way
    ``context_from`` chose it (newest mtime)."""
    from cron.jobs import _job_output_dir
    from cron.monitor import _SNAPSHOT_FILENAME

    rows: List[Tuple[str, str, str]] = []
    files: List[Path] = []
    if not output_dir.is_dir():
        return rows, files
    for job_dir in sorted(output_dir.iterdir()):
        if job_dir.is_symlink() or not job_dir.is_dir():
            continue
        try:
            _job_output_dir(job_dir.name)
        except ValueError:
            continue
        runs = sorted(
            (f for f in job_dir.glob("*.md") if f.is_file()),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        picked = [(LATEST, runs[0])] if runs else []
        snapshot = job_dir / _SNAPSHOT_FILENAME
        if snapshot.is_file():
            picked.append((MONITOR, snapshot))
        for kind, source in picked:
            text = source.read_text(encoding="utf-8", errors="replace")
            rows.append((job_dir.name, kind, text.replace("\x00", "\ufffd")))
            files.append(source)
    return rows, files


def _script_sources(scripts_dir: Path) -> List[Path]:
    if not scripts_dir.is_dir():
        return []
    root = scripts_dir.resolve()
    found = []
    for candidate in sorted(scripts_dir.rglob("*")):
        resolved = candidate.resolve()
        if not resolved.is_file() or not resolved.is_relative_to(root):
            continue  # the run guard would refuse it too
        found.append(resolved)
    return sorted(set(found))


def _copy_report(before: int, after: int, source_rows: int) -> Dict[str, Any]:
    return {
        "source_rows": source_rows,
        "target_rows_before": before,
        "target_rows_after": after,
        "inserted": after - before,
    }


def _migrate_outputs(output_dir: Path, *, dry_run: bool) -> Dict[str, Any]:
    from hermes_aux_store import AuxMigrationError, _DryRun

    rows, files = _output_sources(output_dir)
    before_digests = _digests(files)
    report: Dict[str, Any] = {}
    try:
        with _transaction("cron_outputs", _initialize_outputs) as conn:
            count = "SELECT COUNT(*) FROM cron_outputs"
            before = conn.execute(count).fetchone()[0]
            now = _hermes_now().isoformat()
            for job_id, kind, text in rows:
                conn.execute(
                    """INSERT INTO cron_outputs (job_id, kind, content, updated_at)
                       VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING""",
                    (job_id, kind, text, now),
                )
            stored = {
                (row["job_id"], row["kind"])
                for row in conn.execute(
                    "SELECT job_id, kind FROM cron_outputs"
                ).fetchall()
            }
            missing = sum(1 for job_id, kind, _ in rows if (job_id, kind) not in stored)
            if missing or _digests(files) != before_digests:
                raise AuxMigrationError(
                    f"cron_outputs: {missing} of {len(rows)} source outputs are not in "
                    "PostgreSQL after the copy, or a source changed; rolled back"
                )
            report = _copy_report(before, conn.execute(count).fetchone()[0], len(rows))
            if dry_run:
                raise _DryRun
    except _DryRun:
        pass
    return {
        "status": "dry_run" if dry_run else "migrated",
        "path": str(output_dir),
        "files": len(files),
        "tables": {"cron_outputs": report},
    }


def _migrate_scripts(scripts_dir: Path, *, dry_run: bool) -> Dict[str, Any]:
    from hermes_aux_store import AuxMigrationError, _DryRun

    files = _script_sources(scripts_dir)
    root = scripts_dir.resolve()
    before_digests = _digests(files)
    report: Dict[str, Any] = {}
    try:
        with _transaction("cron_scripts", _initialize_scripts) as conn:
            count = "SELECT COUNT(*) FROM cron_scripts"
            before = conn.execute(count).fetchone()[0]
            keys = [_script_key(path, root) for path in files]
            present = {
                row["path"]
                for row in conn.execute("SELECT path FROM cron_scripts").fetchall()
            }
            for key, path in zip(keys, files):
                if key not in present:  # a body PostgreSQL already holds wins
                    _upsert_script(
                        conn, key, path.read_bytes(), stat.S_IMODE(path.stat().st_mode)
                    )
            stored = {
                row["path"]
                for row in conn.execute("SELECT path FROM cron_scripts").fetchall()
            }
            missing = sum(1 for key in keys if key not in stored)
            if missing or _digests(files) != before_digests:
                raise AuxMigrationError(
                    f"cron_scripts: {missing} of {len(files)} scripts are not in "
                    "PostgreSQL after the copy, or a source changed; rolled back"
                )
            report = _copy_report(before, conn.execute(count).fetchone()[0], len(files))
            if dry_run:
                raise _DryRun
    except _DryRun:
        pass
    return {
        "status": "dry_run" if dry_run else "migrated",
        "path": str(scripts_dir),
        "files": len(files),
        "tables": {"cron_scripts": report},
    }


def _migrate_suggestions(path: Path, *, dry_run: bool) -> Dict[str, Any]:
    """``suggestions.json`` → ``core_cron_suggestions``, by id, inside the
    suggestions lock (a pod adding one meanwhile cannot interleave)."""
    from cron import suggestions
    from hermes_aux_store import AuxMigrationError, _DryRun, _sha256

    if not path.is_file():
        return {"status": "missing", "path": str(path)}
    digest = _sha256(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AuxMigrationError(
            f"cron_suggestions: {path.name} is unreadable: {exc}"
        ) from exc
    source = data.get("suggestions", []) if isinstance(data, dict) else data
    if not isinstance(source, list) or not all(
        isinstance(record, dict) and record.get("id") for record in source
    ):
        raise AuxMigrationError(
            f"cron_suggestions: {path.name} holds a record without an id"
        )
    report: Dict[str, Any] = {}
    try:
        with suggestions._suggestions_section():
            stored = suggestions._load_raw()["suggestions"]
            present = {str(record["id"]) for record in stored}
            added = [record for record in source if str(record["id"]) not in present]
            try:
                suggestions._save_raw(stored + added)
            except ValueError as exc:  # e.g. a duplicate id in the file
                raise AuxMigrationError(
                    f"cron_suggestions: {exc}; rolled back"
                ) from exc
            after = {
                str(record["id"]) for record in suggestions._load_raw()["suggestions"]
            }
            missing = sum(1 for record in source if str(record["id"]) not in after)
            if missing or _sha256(path) != digest:
                raise AuxMigrationError(
                    f"cron_suggestions: {missing} of {len(source)} source records are "
                    "not in PostgreSQL after the copy, or the file changed; rolled back"
                )
            report = _copy_report(len(stored), len(after), len(source))
            if dry_run:
                raise _DryRun
    except _DryRun:
        pass
    return {
        "status": "dry_run" if dry_run else "migrated",
        "path": str(path),
        "sha256": digest,
        "tables": {"cron_suggestions": report},
    }


def migrate_cron_files_to_pg(profile: str, *, dry_run: bool) -> Dict[str, Any]:
    """Copy the active authority profile's cron files into PostgreSQL (levos 0068).

    Same contract as ``hermes_aux_store.migrate_cron_to_pg``: run in the
    profile's own environment after the image carrying 0068 serves; sources
    are only read and must hash the same afterwards; each store copies in one
    PostgreSQL transaction; a row PostgreSQL already holds wins (a pod that ran
    since is newer); a source row missing afterwards rolls the store back
    (``AuxMigrationError``); running twice inserts nothing; ``dry_run`` rolls
    back. Sources: per job directory the newest ``*.md`` and the monitor
    baseline, every regular file under ``scripts/`` the run guard would accept,
    and ``cron/suggestions.json``.
    """
    from cron import suggestions
    from cron.jobs import get_cron_output_dir
    from hermes_constants import get_hermes_home
    from hermes_state_postgres import _is_active_profile

    if not _is_active_profile(profile):
        raise ValueError(
            "migrate_cron_files_to_pg runs in the profile's own environment "
            f"(HERMES_PROFILE / HERMES_HOME); {profile!r} is not the active profile"
        )
    if not authority():
        raise RuntimeError(
            f"profile {profile!r} is not on PostgreSQL authority; nothing to migrate to"
        )
    return {
        "profile": profile,
        "dry_run": dry_run,
        "stores": {
            "cron_outputs": _migrate_outputs(get_cron_output_dir(), dry_run=dry_run),
            "cron_scripts": _migrate_scripts(
                get_hermes_home() / "scripts", dry_run=dry_run
            ),
            "cron_suggestions": _migrate_suggestions(
                suggestions.SUGGESTIONS_FILE, dry_run=dry_run
            ),
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="One-shot move of the cron output, monitor, script and "
        "suggestion files into the profile's PostgreSQL authority store (levos 0068)."
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    report = migrate_cron_files_to_pg(args.profile, dry_run=args.dry_run)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - operator entrypoint
    raise SystemExit(main())
