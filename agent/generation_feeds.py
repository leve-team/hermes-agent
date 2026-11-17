"""Wire shapes -> ``GenerationProgressReporter``: one feeder per provider stream format.

Each feeder reads only the output-bearing parts of one chunk/event (text, reasoning, tool-call
name/arguments) and passes their sizes with the wire's own tool key. They run only when a reporter
exists (``metered_events``) and skip unknown shapes, so they never raise into the stream loop.
The Codex Responses feeder lives beside its assembler in ``agent.codex_runtime``.
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Optional

from agent.generation_meter import parse_provider_progress
from agent.message_content import flatten_message_text

PROVIDER_PROGRESS_FIELD = "x_hermes_progress"


def chars_of(value: Any) -> int:
    return len(value) if isinstance(value, str) else 0


def tool_name_of(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def _field(obj: Any, name: str) -> Any:
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def metered_events(events: Iterable[Any], progress, feed) -> Iterator[Any]:
    """Pass ``events`` through unchanged and in order, feeding each to ``progress`` first."""
    for event in events:
        feed(progress, event)
        yield event


def is_provider_progress_chunk(chunk: Any) -> bool:
    """A choiceless chunk that only reports progress: stream activity, but not content (it must not
    count as the first output byte)."""
    return not _field(chunk, "choices") and _field(chunk, PROVIDER_PROGRESS_FIELD) is not None


def feed_chat_chunk(progress, chunk: Any) -> None:
    """Chat Completions chunk. A choiceless chunk may carry ``x_hermes_progress`` — absolute counts from
    a provider that buffers its real deltas (only the provider object's own attribute / key counts:
    an HTTP upstream's unknown JSON fields land in pydantic ``model_extra`` and are not trusted)."""
    choices = _field(chunk, "choices")
    if not choices:
        raw = _field(chunk, PROVIDER_PROGRESS_FIELD)
        reported = parse_provider_progress(raw) if raw is not None else None
        if reported is not None:
            progress.adopt(reported)
        return
    delta = getattr(choices[0], "delta", None)
    if delta is None:
        return
    reasoning = getattr(delta, "reasoning_content", None) or getattr(delta, "reasoning", None)
    if chars_of(reasoning):
        progress.add("reasoning", len(reasoning))
    content = flatten_message_text(getattr(delta, "content", None), sep="")
    if content:
        progress.add("text", len(content))
    tool_calls = getattr(delta, "tool_calls", None)
    for tc in tool_calls if isinstance(tool_calls, (list, tuple)) else ():
        fn = getattr(tc, "function", None)
        progress.add("tool_call", chars_of(getattr(fn, "arguments", None)),
                     name=tool_name_of(getattr(fn, "name", None)), tool_key=getattr(tc, "index", None))


def _anthropic_block_start(progress, event: Any) -> None:
    block = getattr(event, "content_block", None)
    if getattr(block, "type", None) == "tool_use":
        progress.add("tool_call", 0, name=tool_name_of(getattr(block, "name", None)), tool_key=getattr(event, "index", None))


_ANTHROPIC_DELTAS = {  # delta type -> (phase, text attribute)
    "input_json_delta": ("tool_call", "partial_json"), "text_delta": ("text", "text"),
    "thinking_delta": ("reasoning", "thinking"),
}


def _anthropic_block_delta(progress, event: Any) -> None:
    delta = getattr(event, "delta", None)
    phase, attr = _ANTHROPIC_DELTAS.get(getattr(delta, "type", None), (None, None))
    if phase == "tool_call":
        progress.add(phase, chars_of(getattr(delta, attr, None)), tool_key=getattr(event, "index", None))
    elif phase is not None:
        progress.add(phase, chars_of(getattr(delta, attr, None)))


_ANTHROPIC_EVENTS = {"content_block_start": _anthropic_block_start, "content_block_delta": _anthropic_block_delta}


def feed_anthropic_event(progress, event: Any) -> None:
    """Anthropic Messages stream event."""
    handler = _ANTHROPIC_EVENTS.get(getattr(event, "type", None))
    if handler is not None:
        handler(progress, event)


def feed_bedrock_event(progress, event: Any) -> None:
    """Bedrock ``converse_stream`` event dict (``contentBlockStart`` / ``contentBlockDelta``)."""
    if not isinstance(event, dict):
        return
    start = event.get("contentBlockStart")
    if isinstance(start, dict):
        tool = _field(start.get("start"), "toolUse")
        if isinstance(tool, dict):
            progress.add("tool_call", 0, name=tool_name_of(tool.get("name")), tool_key=start.get("contentBlockIndex"))
        return
    block_delta = event.get("contentBlockDelta")
    delta = block_delta.get("delta") if isinstance(block_delta, dict) else None
    if not isinstance(delta, dict):
        return
    if "text" in delta:
        progress.add("text", chars_of(delta["text"]))
    elif isinstance(delta.get("toolUse"), dict):
        progress.add("tool_call", chars_of(delta["toolUse"].get("input")), tool_key=block_delta.get("contentBlockIndex"))
    elif isinstance(delta.get("reasoningContent"), dict):
        progress.add("reasoning", chars_of(delta["reasoningContent"].get("text")))
