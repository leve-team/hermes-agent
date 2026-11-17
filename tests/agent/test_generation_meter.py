"""Contract of the ``generation.progress`` counts (pure tally) and their reporter (throttle, attempts)."""

from types import SimpleNamespace

from agent.generation_meter import Tally, adopt, advance, progress_payload
from agent.generation_progress import open_generation_progress


def test_tally_counts_chars_per_phase_with_tool_ordinals():
    t = advance(Tally(), "reasoning", 40)
    t = advance(t, "tool_call", 0, name="write_file", tool_key=1)  # wire key: content block 1
    t = advance(t, "tool_call", 30, tool_key=1)  # argument deltas carry no name
    t = advance(t, "tool_call", 20, tool_key=1)
    assert progress_payload(t, api_call=3, attempt=1, elapsed_ms=1500) == {
        "api_call": 3, "attempt": 1, "phase": "tool_call", "name": "write_file", "tool_index": 0,
        "output_chars": 90, "phase_chars": 50, "elapsed_ms": 1500}

    t = advance(t, "text", 3)
    t = advance(t, "tool_call", 7, name="terminal", tool_key=4)  # next tool: next ordinal, phase restarts
    assert (t.name, t.tool_index, t.phase_chars, t.output_chars) == ("terminal", 1, 7, 100)
    t = advance(t, "tool_call", 5, name="patch", tool_key=4)  # same wire key, new name (Ollama reuses index 0)
    assert (t.name, t.tool_index, t.phase_chars) == ("patch", 2, 5)
    text = progress_payload(advance(t, "text", 1), api_call=3, attempt=1, elapsed_ms=0)
    assert "name" not in text and "tool_index" not in text

    # Provider-reported absolute counts win for the rest of the attempt: the provider's final content
    # chunks must not be added on top (that doubled output_chars at the end of a buffered call).
    reported = adopt({"phase": "tool_call", "name": None, "tool_index": 0, "phase_chars": 500, "output_chars": 900})
    after_content = advance(advance(reported, "text", 400), "tool_call", 500, tool_key=0)
    assert progress_payload(after_content, api_call=1, attempt=1, elapsed_ms=0)["output_chars"] == 900


def test_reporter_emits_changed_counts_at_most_once_per_second_and_restarts_per_attempt():
    now, payloads = [100.0], []
    agent = SimpleNamespace(generation_progress_callback=payloads.append, _api_call_count=2, _current_turn_id="t1")
    reporter = open_generation_progress(agent, clock=lambda: now[0])
    reporter.start_attempt()
    reporter.poll()
    assert payloads == []  # nothing written yet

    reporter.add("text", 12)
    reporter.poll()
    reporter.add("text", 8)
    now[0] = 100.6
    reporter.poll()  # throttled
    now[0] = 101.0
    reporter.poll()
    now[0] = 130.0
    reporter.poll()  # unchanged counts are never re-sent
    assert [(p["output_chars"], p["elapsed_ms"]) for p in payloads] == [(12, 0), (20, 1000)]

    # A retry of the same model call (new reporter) continues the attempt numbering and the clock.
    retry = open_generation_progress(agent, clock=lambda: now[0])
    retry.start_attempt()
    retry.add("reasoning", 5)
    retry.poll()
    assert {k: payloads[-1][k] for k in ("api_call", "attempt", "output_chars", "elapsed_ms")} == {
        "api_call": 2, "attempt": 2, "output_chars": 5, "elapsed_ms": 30000}

    agent.generation_progress_callback = None
    assert open_generation_progress(agent) is None  # no listener: no reporter, no counting at all
