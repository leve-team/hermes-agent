"""levos 2026-09-30: user turns lost / hidden around in-place compaction.

Three defects, one incident (opsi portal chat, session 20260919_140939_95f2df):

1. LOSS — a ``[System: model switch]`` marker (role=user) directly followed by a
   real user message is merged INTO the marker by the consecutive-user repair,
   so the survivor keeps ``display_kind=model_switch`` and every display
   projection hides the user's words (14 turns lost in one session).
2. DISPLAY — ``session.history`` / resume display read only ``active=1`` rows;
   in-place compaction archives the pre-compaction turns as
   ``active=0, compacted=1`` so they vanish from the transcript, while the
   re-inserted tail copies (same content, later id) appear out of order.
3. TRANSPORT — the compaction carrier row (summary + preserved turns + base64
   screenshot, ~2 MB) was shipped to the browser as a user bubble.
"""
from __future__ import annotations

from agent.agent_runtime_helpers import repair_message_sequence
from tui_gateway import server
from tui_gateway.server import _bound_display_messages

MARKER = (
    "[System: The active model for this chat has changed to gpt-6-astra via provider "
    "openai-codex. From this point forward, use this runtime metadata when answering "
    "questions about what model/provider is active.]"
)
USER = "[발신자: Wongeun Song <eren@example.com>; 현재 브로커 세션 ID: s1]\n현재 구조에서 멈추지 않고 계속 일하게 하고 싶어."


def test_repair_keeps_user_row_when_merging_with_model_switch_marker():
    msgs = [
        {"role": "user", "content": "안녕"},
        {"role": "assistant", "content": "네"},
        {"role": "user", "content": MARKER, "display_kind": "model_switch"},
        {"role": "user", "content": USER},
        {"role": "assistant", "content": "알겠습니다"},
    ]
    repairs = repair_message_sequence(None, msgs)
    assert repairs == 1
    merged = msgs[2]
    # The USER row survives (no model_switch display_kind); the notice text is folded in.
    assert merged.get("display_kind") != "model_switch"
    assert merged["content"].startswith(MARKER)
    assert USER in merged["content"]
    # Display projection renders the user's words, not a hidden marker.
    shown = server._history_to_messages(msgs)
    user_texts = [m["text"] for m in shown if m["role"] == "user"]
    assert any(USER in t for t in user_texts)
    assert not any(t.lstrip().startswith("[System:") for t in user_texts)
    assert all(m.get("display_kind") != "model_switch" for m in shown if m["role"] == "user")


def test_repair_still_merges_two_real_user_rows_into_first():
    msgs = [{"role": "user", "content": "a"}, {"role": "user", "content": "b"}]
    assert repair_message_sequence(None, msgs) == 1
    assert msgs == [{"role": "user", "content": "a\n\nb"}]


def test_pure_marker_row_stays_hidden():
    msgs = [{"role": "user", "content": MARKER, "display_kind": "model_switch"}]
    assert server._history_to_messages(msgs) == []


def test_legacy_merged_marker_row_renders_user_words():
    # Rows already persisted with the merged shape (marker + user, display_kind=model_switch).
    msgs = [{"role": "user", "content": MARKER + "\n\n" + USER, "display_kind": "model_switch", "_row_id": 7}]
    shown = server._history_to_messages(msgs)
    assert len(shown) == 1 and shown[0]["text"] == USER and "display_kind" not in shown[0]


def test_compaction_carrier_is_not_shipped_as_a_bubble():
    from agent.context_compressor import (
        COMPRESSED_SUMMARY_METADATA_KEY,
        _INFLIGHT_TASK_REPLAY_HEADER,
        _SUMMARY_END_MARKER,
    )
    live = "[발신자: eren; 현재 브로커 세션 ID: s1]\n어떻게 됐는지 보고해 봐."
    carrier = {
        "role": "user",
        COMPRESSED_SUMMARY_METADATA_KEY: True,
        "content": [
            {"type": "text", "text": "[CONTEXT COMPACTION — REFERENCE ONLY] summary…\n## Preserved Turns (reference only)\n[USER] x"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64," + "A" * 5000}},
            {"type": "text", "text": "\n\n" + _SUMMARY_END_MARKER + "\n\n" + _INFLIGHT_TASK_REPLAY_HEADER + "\n" + live},
        ],
    }
    # Original user row already rendered (compacted display read) → carrier collapses entirely.
    shown = server._history_to_messages([{"role": "user", "content": live, "_row_id": 1}, carrier])
    assert [m["text"] for m in shown if m["role"] == "user"] == [live]
    # Without the original, only the live ask survives — never the summary or the image bytes.
    shown = server._history_to_messages([carrier])
    assert len(shown) == 1 and shown[0]["text"] == live and "data:image" not in shown[0]["text"]


def test_dedupe_compaction_copies_keeps_original_order():
    rows = [
        {"id": 1, "role": "user", "content": "q", "timestamp": 10.0, "tool_call_id": None, "tool_calls": None, "tool_name": None},
        {"id": 2, "role": "assistant", "content": "a", "timestamp": 11.0, "tool_call_id": None, "tool_calls": None, "tool_name": None},
        {"id": 3, "role": "user", "content": "[CONTEXT COMPACTION] …", "timestamp": 12.0, "tool_call_id": None, "tool_calls": None, "tool_name": None},
        {"id": 4, "role": "user", "content": "q", "timestamp": 10.0, "tool_call_id": None, "tool_calls": None, "tool_name": None},  # tail copy
    ]
    from hermes_state import SessionDB
    out = SessionDB._dedupe_compaction_copies(rows)
    assert [r["id"] for r in out] == [1, 2, 3]


def test_bound_display_messages_default_and_opt_out():
    msgs = [{"role": "user", "text": str(i)} for i in range(1500)]
    got, truncated = _bound_display_messages(msgs, {})
    assert truncated and len(got) == 1000 and got[-1]["text"] == "1499"
    got, truncated = _bound_display_messages(msgs, {"limit": 0})
    assert not truncated and len(got) == 1500
