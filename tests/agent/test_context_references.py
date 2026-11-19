from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


@pytest.fixture
def sample_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "Hermes Tests")
    _git(repo, "config", "user.email", "tests@example.com")

    (repo / "src").mkdir()
    (repo / "src" / "main.py").write_text(
        "def alpha():\n"
        "    return 'a'\n\n"
        "def beta():\n"
        "    return 'b'\n",
        encoding="utf-8",
    )
    (repo / "src" / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "README.md").write_text("# Demo\n", encoding="utf-8")
    (repo / "blob.bin").write_bytes(b"\x00\x01\x02binary")

    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "initial")

    (repo / "src" / "main.py").write_text(
        "def alpha():\n"
        "    return 'changed'\n\n"
        "def beta():\n"
        "    return 'b'\n",
        encoding="utf-8",
    )
    (repo / "src" / "helper.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(repo, "add", "src/helper.py")
    return repo


def test_parse_typed_references_ignores_emails_and_handles():
    from agent.context_references import parse_context_references

    message = (
        "email me at user@example.com and ping @teammate "
        "but include @file:src/main.py:1-2 plus @diff and @git:2 "
        "and @url:https://example.com/docs"
    )

    refs = parse_context_references(message)

    assert [ref.kind for ref in refs] == ["file", "diff", "git", "url"]
    assert refs[0].target == "src/main.py"
    assert refs[0].line_start == 1
    assert refs[0].line_end == 2
    assert refs[2].target == "2"








def test_folder_listing_falls_back_when_rg_is_blocked(sample_repo: Path):
    from agent.context_references import preprocess_context_references

    real_run = subprocess.run

    def blocked_rg(*args, **kwargs):
        cmd = args[0] if args else kwargs.get("args")
        if isinstance(cmd, list) and cmd and cmd[0] == "rg":
            raise PermissionError("rg blocked by policy")
        return real_run(*args, **kwargs)

    with patch("agent.context_references.subprocess.run", side_effect=blocked_rg):
        result = preprocess_context_references(
            "Review @folder:src/",
            cwd=sample_repo,
            context_length=100_000,
        )

    assert result.expanded
    assert "src/" in result.message
    assert "main.py" in result.message
    assert "helper.py" in result.message
    assert not result.warnings






def test_missing_file_becomes_warning(sample_repo: Path):
    from agent.context_references import preprocess_context_references

    result = preprocess_context_references(
        "Check @file:nope.txt",
        cwd=sample_repo,
        context_length=100_000,
    )

    assert result.expanded
    assert len(result.warnings) == 1
    assert "not found" in result.message.lower()


def test_oversized_text_file_falls_back_to_tool_readable_path(tmp_path: Path):
    from agent.context_references import preprocess_context_references

    payload = tmp_path / "large.txt"
    payload.write_text("FULL-CONTENT-MARKER\n" + ("x" * 8_000), encoding="utf-8")

    result = preprocess_context_references(
        f"Inspect @file:{payload.name}",
        cwd=tmp_path,
        context_length=1_000,
    )

    assert result.expanded
    assert not result.blocked
    assert str(payload) in result.message
    assert "too large to inline safely" in result.message
    assert "read_file" in result.message
    assert "FULL-CONTENT-MARKER" not in result.message
    # The fallback block alone carries the message — a companion warning would
    # repeat "too large to inline safely" under --- Context Warnings ---.
    assert not result.warnings


def test_file_line_range_is_applied_before_oversized_fallback(tmp_path: Path):
    from agent.context_references import preprocess_context_references

    payload = tmp_path / "large.txt"
    payload.write_text(
        "first line\nsecond line\n" + "\n".join("x" * 200 for _ in range(100)),
        encoding="utf-8",
    )

    result = preprocess_context_references(
        f"Inspect @file:{payload.name}:1-2",
        cwd=tmp_path,
        context_length=1_000,
    )

    assert result.expanded
    assert not result.blocked
    assert "first line\nsecond line" in result.message
    assert "too large to inline safely" not in result.message


def test_multiple_individually_safe_files_still_obey_aggregate_limit(tmp_path: Path):
    from agent.context_references import preprocess_context_references

    for name in ("first.txt", "second.txt"):
        (tmp_path / name).write_text("x" * 1_200, encoding="utf-8")

    result = preprocess_context_references(
        "Inspect @file:first.txt and @file:second.txt",
        cwd=tmp_path,
        context_length=1_000,
    )

    assert result.blocked
    assert not result.expanded
    assert "context injection refused" in "\n".join(result.warnings)


def test_binary_reference_block_maps_host_attachment_to_container_path(tmp_path: Path, monkeypatch):
    """Docker backend: a staged binary attachment's host path is rendered as the
    bind-mounted in-container path so the agent's tools can read it.

    Regression test for #76577 — the container has its own filesystem, so the
    gateway host path would dangle inside the sandbox.
    """
    from agent.context_references import preprocess_context_references

    hermes_home = tmp_path / ".hermes"
    attachments = hermes_home / "attachments"
    attachments.mkdir(parents=True)
    payload = attachments / "archive.zip"
    payload.write_bytes(b"PK\x03\x04binary-zip-bytes")

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_ENV", "docker")

    result = preprocess_context_references(
        f"Read the attachment @file:{payload}",
        cwd=tmp_path,
        context_length=100_000,
    )

    assert result.expanded
    # Default container base for the docker backend is /root/.hermes.
    assert "/root/.hermes/attachments/archive.zip" in result.message
    assert "binary file, not inlined" in result.message


def test_oversized_text_reference_maps_host_attachment_to_container_path(
    tmp_path: Path, monkeypatch
):
    from agent.context_references import preprocess_context_references

    hermes_home = tmp_path / ".hermes"
    attachments = hermes_home / "attachments"
    attachments.mkdir(parents=True)
    payload = attachments / "large.txt"
    payload.write_text("x" * 8_000, encoding="utf-8")

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_ENV", "docker")

    result = preprocess_context_references(
        f"Read the attachment @file:{payload}",
        cwd=tmp_path,
        context_length=1_000,
    )

    assert result.expanded
    assert not result.blocked
    attached_context = result.message.split("--- Attached Context ---", 1)[1]
    assert "/root/.hermes/attachments/large.txt" in attached_context
    assert "too large to inline safely" in result.message


def test_binary_reference_block_keeps_host_path_on_local_backend(tmp_path: Path, monkeypatch):
    """Local backend: no translation — the agent's tools run on the host."""
    from agent.context_references import preprocess_context_references

    hermes_home = tmp_path / ".hermes"
    attachments = hermes_home / "attachments"
    attachments.mkdir(parents=True)
    payload = attachments / "archive.zip"
    payload.write_bytes(b"PK\x03\x04binary-zip-bytes")

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_ENV", "local")

    result = preprocess_context_references(
        f"Read the attachment @file:{payload}",
        cwd=tmp_path,
        context_length=100_000,
    )

    assert result.expanded
    assert str(payload) in result.message
    assert "/root/.hermes/attachments/" not in result.message


# ── Staged attachments outside the workspace (upstream #110174) ─────────────
# Production layout: the gateway stages uploads in ``<profile home>/attachments``
# while the session works in ``<profile home>/work`` — the staging dir is NOT
# under the workspace, so allowed_root alone used to refuse every staged file.


def _staged_attachment_layout(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    """``(hermes_home, workspace)`` with the workspace NOT containing the staging dirs."""
    root = tmp_path.resolve()
    hermes_home = root / "profile"
    workspace = hermes_home / "work"
    workspace.mkdir(parents=True)
    (hermes_home / "attachments").mkdir()
    monkeypatch.setenv("HOME", str(root / "home"))
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_ENV", "local")
    return hermes_home, workspace


def _attached_context(message: str) -> str:
    """Everything the expansion appended (warnings + blocks), minus the user's own text."""
    return message.split("\n\n--- ", 1)[1] if "\n\n--- " in message else ""


def test_staged_binary_attachment_outside_workspace_expands_without_warning(tmp_path: Path, monkeypatch):
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    payload = hermes_home / "attachments" / "clip.mp4"
    payload.write_bytes(b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"\x00" * 64)

    result = preprocess_context_references(
        f"What is in this clip? @file:{payload}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert result.warnings == []
    assert "--- Context Warnings ---" not in result.message
    assert "outside the allowed workspace" not in result.message
    attached = _attached_context(result.message)
    assert "binary file, not inlined" in attached
    assert f"available on disk at `{payload}`" in attached


def test_staged_text_attachment_outside_workspace_is_inlined(tmp_path: Path, monkeypatch):
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    payload = hermes_home / "attachments" / "report.html"
    payload.write_text("<h1>STAGED-HTML-BODY</h1>\n", encoding="utf-8")

    result = preprocess_context_references(
        f"Summarise @file:{payload}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert result.warnings == []
    assert "--- Context Warnings ---" not in result.message
    attached = _attached_context(result.message)
    assert f"📄 @file:{payload}" in attached
    assert "```\n<h1>STAGED-HTML-BODY</h1>" in attached
    assert "not inlined" not in attached


def test_path_outside_workspace_and_staging_dirs_is_still_refused(tmp_path: Path, monkeypatch):
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    elsewhere = hermes_home.parent / "elsewhere"
    elsewhere.mkdir()
    outside = elsewhere / "notes.txt"
    outside.write_text("OUTSIDE-WORKSPACE-BODY\n", encoding="utf-8")
    # A non-staging sibling inside HERMES_HOME is not widened either.
    inside_home = hermes_home / "notes.txt"
    inside_home.write_text("HERMES-HOME-BODY\n", encoding="utf-8")

    result = preprocess_context_references(
        f"Read @file:{outside} and @file:{inside_home}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert sum("path is outside the allowed workspace" in warning for warning in result.warnings) == 2
    assert "OUTSIDE-WORKSPACE-BODY" not in result.message
    assert "HERMES-HOME-BODY" not in result.message
    assert "--- Attached Context ---" not in result.message


def test_credential_file_in_hermes_home_outside_workspace_is_still_refused(tmp_path: Path, monkeypatch):
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    auth_json = hermes_home / "auth.json"
    auth_json.write_text('{"openai": "sk-AUTHJSON-SECRET"}\n', encoding="utf-8")
    # ``..`` out of the staging dir must not smuggle the credential store through either.
    traversal = hermes_home / "attachments" / ".." / "auth.json"

    result = preprocess_context_references(
        f"inspect @file:{auth_json} and @file:{traversal}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert len(result.warnings) == 2
    assert "sk-AUTHJSON-SECRET" not in result.message
    assert "--- Attached Context ---" not in result.message


def test_read_denied_name_under_staging_dir_is_still_refused(tmp_path: Path, monkeypatch):
    """Staging only lifts the workspace-root check; the credential deny-list still applies."""
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    staged_env = hermes_home / "attachments" / ".env"
    staged_env.write_text("DB_PASSWORD=STAGED-ENV-SECRET\n", encoding="utf-8")

    result = preprocess_context_references(
        f"inspect @file:{staged_env}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert sum("sensitive credential" in warning for warning in result.warnings) == 1
    assert "STAGED-ENV-SECRET" not in result.message
    assert "--- Attached Context ---" not in result.message


def test_symlink_under_staging_dir_cannot_escape_to_outside_file(tmp_path: Path, monkeypatch):
    """The staging check runs on the RESOLVED path, so a staged symlink grants nothing."""
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    elsewhere = hermes_home.parent / "elsewhere"
    elsewhere.mkdir()
    target = elsewhere / "private.txt"
    target.write_text("SYMLINK-TARGET-BODY\n", encoding="utf-8")
    link = hermes_home / "attachments" / "link.txt"
    link.symlink_to(target)

    result = preprocess_context_references(
        f"Read @file:{link}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert sum("path is outside the allowed workspace" in warning for warning in result.warnings) == 1
    assert "SYMLINK-TARGET-BODY" not in result.message


def test_staging_lookup_failure_keeps_workspace_refusal(tmp_path: Path, monkeypatch):
    """If the staging-dir lookup raises, the path is treated as NOT staged (fails closed)."""
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    payload = hermes_home / "attachments" / "report.html"
    payload.write_text("<h1>STAGED-HTML-BODY</h1>\n", encoding="utf-8")

    def _boom(*_args, **_kwargs):
        raise RuntimeError("mount resolution failed")

    monkeypatch.setattr("tools.credential_files.get_cache_directory_mounts", _boom)

    result = preprocess_context_references(
        f"Summarise @file:{payload}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert sum("path is outside the allowed workspace" in warning for warning in result.warnings) == 1
    assert "<h1>STAGED-HTML-BODY</h1>" not in result.message


def test_staged_attachment_follows_session_profile_home_override(tmp_path: Path, monkeypatch):
    """Multiplexed gateway: the staging dirs are the SESSION profile's (context-local
    HERMES_HOME override installed for the turn), not the launch profile's."""
    from agent.context_references import preprocess_context_references
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    launch_home, _ = _staged_attachment_layout(tmp_path, monkeypatch)
    other_profile_upload = launch_home / "attachments" / "other-profile.html"
    other_profile_upload.write_text("<p>OTHER-PROFILE-BODY</p>\n", encoding="utf-8")
    profile_home = launch_home.parent / "profiles" / "dave"
    workspace = profile_home / "work"
    workspace.mkdir(parents=True)
    (profile_home / "attachments").mkdir()
    payload = profile_home / "attachments" / "clip.mp4"
    payload.write_bytes(b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"\x00" * 64)

    token = set_hermes_home_override(profile_home)
    try:
        result = preprocess_context_references(
            f"Compare @file:{payload} with @file:{other_profile_upload}",
            cwd=workspace,
            allowed_root=workspace,
            context_length=100_000,
        )
    finally:
        reset_hermes_home_override(token)

    attached = _attached_context(result.message)
    assert f"available on disk at `{payload}`" in attached
    assert len(result.warnings) == 1
    assert str(other_profile_upload) in result.warnings[0]
    assert "path is outside the allowed workspace" in result.warnings[0]
    assert "OTHER-PROFILE-BODY" not in result.message


def test_folder_reference_to_staging_dir_still_exposes_no_listing(tmp_path: Path, monkeypatch):
    """``@folder:`` is untouched: a staging dir outside the workspace yields no listing."""
    from agent.context_references import preprocess_context_references

    hermes_home, workspace = _staged_attachment_layout(tmp_path, monkeypatch)
    (hermes_home / "attachments" / "other-session-upload.pdf").write_bytes(b"%PDF-1.7\n\x00")

    result = preprocess_context_references(
        f"List @folder:{hermes_home / 'attachments'}",
        cwd=workspace,
        allowed_root=workspace,
        context_length=100_000,
    )

    assert len(result.warnings) == 1
    assert "other-session-upload.pdf" not in result.message
    assert "--- Attached Context ---" not in result.message
















@pytest.mark.asyncio
async def test_blocks_canonical_read_denylist_credential_stores(tmp_path: Path, monkeypatch):
    """@file expansion must honour the canonical read deny-list.

    The narrow in-module list historically missed the real credential stores
    (provider keys, OAuth tokens, MCP tokens, project-local .env). Because the
    gateway routes untrusted remote message text through reference expansion,
    a chat peer could otherwise attach `@file:~/.hermes/auth.json` and read the
    operator's keys into context. These must all be refused, with their secret
    bodies kept out of the expanded message.
    """
    from agent.context_references import preprocess_context_references_async

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    hermes_home = tmp_path / ".hermes"
    (hermes_home).mkdir(parents=True)

    auth_json = hermes_home / "auth.json"
    auth_json.write_text('{"openai": "sk-AUTHJSON-SECRET"}\n', encoding="utf-8")

    oauth = hermes_home / ".anthropic_oauth.json"
    oauth.write_text('{"access_token": "OAUTH-SECRET"}\n', encoding="utf-8")

    mcp_token = hermes_home / "mcp-tokens" / "github.json"
    mcp_token.parent.mkdir(parents=True)
    mcp_token.write_text('{"token": "MCP-TOKEN-SECRET"}\n', encoding="utf-8")

    project_env = tmp_path / "project" / ".env"
    project_env.parent.mkdir(parents=True)
    project_env.write_text("DB_PASSWORD=ENV-SECRET\n", encoding="utf-8")

    result = await preprocess_context_references_async(
        "inspect @file:.hermes/auth.json and @file:.hermes/.anthropic_oauth.json "
        "and @file:.hermes/mcp-tokens/github.json and @file:project/.env",
        cwd=tmp_path,
        allowed_root=tmp_path,
        context_length=100_000,
    )

    assert result.expanded
    for secret in (
        "sk-AUTHJSON-SECRET",
        "OAUTH-SECRET",
        "MCP-TOKEN-SECRET",
        "ENV-SECRET",
    ):
        assert secret not in result.message
    assert sum("sensitive credential" in warning for warning in result.warnings) == 4


@pytest.mark.asyncio
async def test_canonical_guard_fails_closed_when_lookup_raises(tmp_path: Path, monkeypatch):
    """If the canonical read guard raises, the reference must fail CLOSED.

    The guard exists specifically to cover credential stores the narrow local
    list misses (auth.json, ...). If get_read_block_error ever raised, silently
    falling through to the local list would re-open that exact hole — and the
    gateway feeds untrusted remote text here, so a chat peer could then attach
    auth.json. The reference must be refused and the secret kept out of the
    expanded message.
    """
    from agent.context_references import preprocess_context_references_async

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir(parents=True)
    auth_json = hermes_home / "auth.json"
    auth_json.write_text('{"openai": "sk-AUTHJSON-SECRET"}\n', encoding="utf-8")

    def _boom(_path):
        raise RuntimeError("guard resolution failed")

    monkeypatch.setattr("agent.file_safety.get_read_block_error", _boom)

    result = await preprocess_context_references_async(
        "inspect @file:.hermes/auth.json",
        cwd=tmp_path,
        allowed_root=tmp_path,
        context_length=100_000,
    )

    assert "sk-AUTHJSON-SECRET" not in result.message
    assert any(
        "credential deny-list" in warning or "sensitive credential" in warning
        for warning in result.warnings
    )


@pytest.mark.parametrize(
    "value",
    [
        "/tmp/plain.png",
        "/Users/me/Library/Application Support/Hermes/composer-images/a.png",
        r"C:\Users\John Doe\Pictures\cat.png",
        "/tmp/report (final).pdf",
        "/tmp/it's here.png",
        '/tmp/say "hi".png',
    ],
)
def test_format_reference_value_round_trips_through_the_parser(value):
    """Whatever the path contains, the formatted ref must parse back whole —
    an unquoted value stops at the first space and strands the tail as text."""
    from agent.context_references import REFERENCE_PATTERN, format_reference_value

    match = REFERENCE_PATTERN.search(f"@file:{format_reference_value(value)}")

    assert match is not None
    assert match.group("value").strip("`\"'") == value
