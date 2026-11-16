"""Real SDK/HTTP turn flow after a fenced compression timeout, in a temporary home.

Also runnable with --serve PORT: /probe returns the result of the same real-path
smoke check for managed verification (no external provider or production state).
"""

from __future__ import annotations

import copy
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pytest


def exercise_timeout(entry):
    from openai import OpenAI
    from run_agent import AIAgent
    from agent.conversation_compression import context_compression_timed_out

    requests = []

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            if self.path != "/v1/chat/completions":
                # Metadata probes are not model inference requests.
                self.send_error(404)
                return
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            response = {
                "id": "probe-response", "object": "chat.completion", "created": 1,
                "model": "probe-model",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "continued"},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 1, "total_tokens": 101},
            }
            streaming = requests[-1].get("stream")
            if streaming:
                response["object"] = "chat.completion.chunk"
                response["choices"][0]["delta"] = response["choices"][0].pop("message")
                body = ("data: " + json.dumps(response) + "\n\ndata: [DONE]\n\n").encode()
            else:
                body = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream" if streaming else "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    url = f"http://127.0.0.1:{server.server_port}/v1"
    release = threading.Event()
    finished = threading.Event()
    attempts = []
    commits = []
    tools = [{"type": "function", "function": {
        "name": "probe_tool", "description": "A stable tool schema " * 100,
        "parameters": {"type": "object", "properties": {}},
    }}]
    client = OpenAI(api_key="local-probe-only", base_url=url, max_retries=0)
    try:
        with (
            patch("model_tools.get_tool_definitions", return_value=tools),
            patch("model_tools.check_toolset_requirements", return_value={}),
            patch("agent.process_bootstrap.OpenAI", return_value=client),
        ):
            agent = AIAgent(
                api_key="local-probe-only", base_url=url, provider="custom", model="probe-model",
                max_tokens=1000, quiet_mode=True, skip_context_files=True, skip_memory=True,
                save_trajectories=False, max_iterations=2,
            )
        agent.client = client
        agent._cached_system_prompt = ("Stable system prompt. " * 100).strip()
        agent.tools = tools
        agent.tool_delay = 0
        compressor = agent.context_compressor
        compressor.context_length = 20_000
        compressor.threshold_tokens = 1000
        # Prior provider usage makes the real compressor's preflight actionable;
        # a first-turn heuristic alone is deliberately deferred by production.
        compressor.last_real_prompt_tokens = 4000
        compressor.protect_first_n = 0
        compressor.protect_last_n = 0
        history = [{"role": "user", "content": ("old request " * 500).strip()},
                   {"role": "assistant", "content": ("old response " * 500).strip()}]
        original = copy.deepcopy(history)
        session_id = agent.session_id
        cached_prompt = agent._cached_system_prompt

        def slow_worker(_agent, messages, system_message, **kwargs):
            attempts.append(1)
            fence = kwargs["commit_fence"]
            try:
                assert release.wait(10), "probe failed to release compression worker"
                # A misbehaving late worker may mutate its private snapshot, but
                # must never publish this after the host has resumed the request.
                messages[0]["content"] = "late worker mutation"
                commits.append(fence.begin_commit())
                return [{"role": "assistant", "content": "late summary"}], "changed prompt"
            finally:
                finished.set()

        with (
            patch("agent.conversation_compression.compress_context", side_effect=slow_worker),
            # Scale only the wait, not the predicate, estimators, HTTP, or cancellation fence.
            patch("agent.conversation_compression.resolve_context_compression_timeouts", return_value=(0.05, 0.2)),
            patch("agent.conversation_compression.resolve_compression_fallback_route", return_value=None),
            patch("agent.turn_context._should_run_preflight_estimate", return_value=entry == "turn_start"),
            patch.object(agent, "_persist_session"),
            patch.object(agent, "_save_trajectory"),
            patch.object(agent, "_cleanup_task_resources"),
        ):
            result = agent.run_conversation("continue", conversation_history=history)
            release.set()
            assert finished.wait(5)
        assert attempts == [1]
        assert commits == [False]
        assert context_compression_timed_out(agent)
        assert len(requests) == 1
        assert result["final_response"] == "continued"
        assert not result.get("compression_exhausted", False)
        assert history == original
        assert agent.session_id == session_id
        assert agent._cached_system_prompt == cached_prompt
        assert compressor.compression_count == 0
        assert compressor._consecutive_timeout_failures == 1
        assert compressor.get_active_compression_failure_cooldown() is not None
        sent = requests[0]
        assert sent["tools"] == tools
        assert sent["messages"][0]["content"] == cached_prompt
        assert [m["role"] for m in sent["messages"]] == ["system", "user", "assistant", "user"]
        assert sent["messages"][1]["content"] == original[0]["content"]
        assert sent["messages"][2]["content"] == original[1]["content"]
        assert agent._api_call_count == 1
        return {"entry": entry, "provider_requests": len(requests), "compression_attempts": len(attempts),
                "late_commit": commits[0], "original_preserved": True, "result": result["final_response"]}
    finally:
        release.set()
        client.close()
        server.shutdown()
        server.server_close()
        server_thread.join(5)


@pytest.mark.parametrize("entry", ["turn_start", "pre_api"])
def test_timeout_resumes_over_http_without_late_commit(entry):
    exercise_timeout(entry)


if __name__ == "__main__":
    import argparse
    import os
    import tempfile

    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", type=int, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="hermes-timeout-probe-") as home:
        os.environ["HERMES_HOME"] = home
        receipt = {"checks": [exercise_timeout(entry) for entry in ("turn_start", "pre_api")]}

        class Probe(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps(receipt).encode()
                self.send_response(200 if self.path == "/probe" else 404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        ThreadingHTTPServer(("127.0.0.1", args.serve), Probe).serve_forever()
