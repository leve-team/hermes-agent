#!/usr/bin/env python
"""Loopback live probe for run 6dc05f66 (staged ``@file:`` outside the workspace).

Not a production probe: it drives the REAL core path in an isolated temp profile —
the ``file.attach`` RPC handler (``tui_gateway.server._methods``) stages bytes into
``<profile home>/attachments`` and returns the ``@file:`` ref text, and
``agent.context_references.preprocess_context_references`` expands that text with
``cwd = allowed_root = <profile home>/work`` under the profile HERMES_HOME override,
with the same arguments ``tui_gateway/prompt_turn.py`` passes.

    python docs/runs/6dc05f66/live_probe.py --serve 18762   # GET /probe -> 200 only if every check holds
    python docs/runs/6dc05f66/live_probe.py --once          # print the JSON, exit 0/1
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MP4_BYTES = b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"\x00" * 64


def _data_url(mime: str, payload: bytes) -> str:
    return f"data:{mime};base64,{base64.b64encode(payload).decode()}"


def run_probe() -> dict:
    from agent.context_references import preprocess_context_references
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tui_gateway import server

    root = Path(tempfile.mkdtemp(prefix="levos-6dc05f66-probe-")).resolve()
    sid = f"probe-{root.name}"
    try:
        profile_home = root / "profile"
        workspace = profile_home / "work"
        workspace.mkdir(parents=True)
        # A session RECORD is all file.attach needs (never the agent): same shape as
        # tests/tui_gateway/test_attach_does_not_wait_for_agent.py::building_session.
        server._sessions[sid] = {
            "agent": None, "agent_ready": threading.Event(), "agent_error": None,
            "attached_images": [], "cwd": str(workspace), "history": [],
            "history_lock": threading.RLock(), "history_version": 0, "image_counter": 0,
            "profile_home": str(profile_home), "running": False, "session_key": sid,
            "transport": None,
        }

        # No model is involved in staging or expansion: keep the attach RPC from kicking
        # off a real agent build in this credential-less probe process (probe-local only).
        server._start_agent_build = lambda _sid, _session: None

        def _attach(mime: str, payload: bytes, name: str) -> dict:
            response = server._methods["file.attach"](
                1, {"session_id": sid, "data_url": _data_url(mime, payload), "name": name})
            if "error" in response:
                raise RuntimeError(f"file.attach failed: {response['error']}")
            return response["result"]

        def _expand(message: str):
            return preprocess_context_references(
                message, cwd=str(workspace), allowed_root=str(workspace), context_length=100_000)

        # file.attach runs BEFORE the turn installs the profile override (as in production).
        clip_result = _attach("video/mp4", MP4_BYTES, "clip.mp4")
        page_result = _attach("text/html", b"<h1>STAGED-HTML-BODY</h1>\n", "report.html")
        clip, clip_uploaded = Path(clip_result["path"]), clip_result["uploaded"]

        token = set_hermes_home_override(profile_home)
        try:
            binary = _expand(f"What is in this clip? {clip_result['ref_text']}")
            text = _expand(f"Summarise {page_result['ref_text']}")

            outside = root / "elsewhere.txt"
            outside.write_text("OUTSIDE-WORKSPACE-BODY\n", encoding="utf-8")
            auth_json = profile_home / "auth.json"
            auth_json.write_text('{"openai": "sk-PROBE-SECRET"}\n', encoding="utf-8")
            staged_env = profile_home / "attachments" / ".env"
            staged_env.write_text("DB_PASSWORD=PROBE-ENV-SECRET\n", encoding="utf-8")
            refused = _expand(f"Read @file:{outside} and @file:{auth_json} and @file:{staged_env}")
        finally:
            reset_hermes_home_override(token)

        checks = {
            "staged_under_profile_attachments": clip.parent == profile_home / "attachments" and clip_uploaded,
            "staging_dir_outside_workspace": workspace not in clip.parents,
            "binary_no_warnings": binary.warnings == [],
            "binary_block_with_path": "binary file, not inlined" in binary.message
            and f"available on disk at `{clip}`" in binary.message,
            "text_no_warnings": text.warnings == [],
            "text_inlined": "<h1>STAGED-HTML-BODY</h1>" in text.message,
            "outside_still_refused": sum(
                "path is outside the allowed workspace" in w for w in refused.warnings) == 2,
            "staged_env_denied": sum("sensitive credential" in w for w in refused.warnings) == 1,
            "no_secret_or_outside_body_exposed": not any(
                needle in refused.message
                for needle in ("OUTSIDE-WORKSPACE-BODY", "sk-PROBE-SECRET", "PROBE-ENV-SECRET")),
        }
        return {"ok": all(checks.values()), "checks": checks,
                "binary_warnings": binary.warnings, "text_warnings": text.warnings,
                "refused_warnings": len(refused.warnings)}
    finally:
        server._sessions.pop(sid, None)
        shutil.rmtree(root, ignore_errors=True)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - http.server API
        if self.path.split("?", 1)[0] != "/probe":
            self.send_error(404)
            return
        try:
            body = run_probe()
        except Exception as exc:  # a broken probe must read as a failure, never as 200
            body = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        payload = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(200 if body.get("ok") else 500)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):  # keep the service log quiet
        pass


def main(argv: list[str]) -> int:
    # Isolated launch home: never touch a real ~/.hermes from the probe process.
    launch_home = tempfile.mkdtemp(prefix="levos-6dc05f66-probe-home-")
    os.environ["HERMES_HOME"] = launch_home
    os.environ["HOME"] = launch_home
    os.environ.setdefault("TERMINAL_ENV", "local")
    if argv[:1] == ["--serve"] and len(argv) == 2:
        ThreadingHTTPServer(("127.0.0.1", int(argv[1])), _Handler).serve_forever()
        return 0
    if argv == ["--once"]:
        body = run_probe()
        print(json.dumps(body, ensure_ascii=False, indent=2))
        return 0 if body["ok"] else 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
