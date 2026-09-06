"""결함 #5 — 레거시 ``\\x00json:`` 접두 행이 PostgreSQL text(NUL 거부)로 이관될 때.

실측(2026-09-06, dave primary state.db 540,923행): NUL 포함 행 137건, 전부 첫 바이트
하나·``\\x00json:`` 접두·접두 제거 후 JSON 디코드 가능. 코어 ``SessionDB`` 는 쓰기
접두를 ``\\x01json:`` 로 바꿨고(hermes_state.py ``_CONTENT_JSON_PREFIX``) 읽기는
양쪽을 동등하게 받는다. 이관 정규화는 의미 보존이며 실PG 없이 검증한다.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import migrate_state_to_postgres as migrate
import state_diff
import state_transfer as st
from hermes_state import SessionDB

LEGACY = "\x00json:" + json.dumps([{"type": "text", "text": "[발신자] 안녕"}, {"type": "image", "data": "iVBOR"}])
CURRENT = "\x01json:" + LEGACY[len("\x00json:"):]


def test_prefix_constants_match_core():
    assert st.LEGACY_CONTENT_JSON_PREFIX == SessionDB._CONTENT_JSON_PREFIX_LEGACY
    assert st.CURRENT_CONTENT_JSON_PREFIX == SessionDB._CONTENT_JSON_PREFIX


def test_normalize_rewrites_legacy_prefix_and_preserves_decode():
    out = st.normalize_legacy_content_prefix(LEGACY, table="messages", column="content", key=(13715,))
    assert out == CURRENT
    assert "\x00" not in out
    assert SessionDB._decode_content(out) == SessionDB._decode_content(LEGACY)


@pytest.mark.parametrize("value", ["plain text", CURRENT, None, 7, b"\x00bytes", ""])
def test_normalize_passthrough_for_non_legacy(value):
    assert st.normalize_legacy_content_prefix(value) is value or st.normalize_legacy_content_prefix(value) == value


@pytest.mark.parametrize("value", ["abc\x00def", "\x00notjson", "\x00json:{\"a\":\"\x00\"}", "\x00\x00json:[]"])
def test_normalize_refuses_nul_outside_prefix(value):
    with pytest.raises(st.SourceValueError) as info:
        st.normalize_legacy_content_prefix(value, table="messages", column="content", key=(1,))
    assert "messages.content" in str(info.value) and "key=(1,)" in str(info.value)


def _spec():
    return st.TableSpec("messages", ("id", "session_id", "role", "content"), ("id",))


def test_copy_source_values_normalize_legacy_rows():
    spec = _spec()
    row = {"id": 13715, "session_id": "s", "role": "user", "content": LEGACY}
    values = migrate._source_values(spec, row)
    assert values[3] == CURRENT and all("\x00" not in v for v in values if isinstance(v, str))


def test_copy_source_values_fail_closed_on_foreign_nul():
    spec = _spec()
    row = {"id": 5, "session_id": "s", "role": "user", "content": "x\x00y"}
    with pytest.raises(st.SourceValueError):
        migrate._source_values(spec, row)


def test_diff_hash_treats_legacy_and_current_as_equal():
    cols = ("id", "content")
    legacy = {"id": 1, "content": LEGACY}
    current = {"id": 1, "content": CURRENT}
    assert state_diff.row_hash(cols, legacy) == state_diff.row_hash(cols, current)
    other = {"id": 1, "content": CURRENT + "x"}
    assert state_diff.row_hash(cols, legacy) != state_diff.row_hash(cols, other)


def test_copy_into_sqlite_target_end_to_end(tmp_path: Path):
    """실PG 대신 SQLite target 으로 COPY 경로를 태워 137행 상당의 레거시 행이 현행
    접두로 안착하고 코어 디코드가 원본과 같음을 본다."""
    src = tmp_path / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content TEXT)")
    conn.executemany("INSERT INTO messages VALUES (?,?,?,?)",
                     [(i, "s", "user", LEGACY if i % 4 == 0 else f"plain {i}") for i in range(1, 549)])
    conn.commit()
    conn.close()
    spec = _spec()
    source = st.open_sqlite_snapshot(src)
    rows = st.fetch_sqlite_batch(source, spec, None, 1000)
    values = [migrate._source_values(spec, r) for r in rows]
    assert len(values) == 548
    legacy_rows = [v for v in values if isinstance(v[3], str) and v[3].startswith("\x01json:")]
    assert len(legacy_rows) == 137
    assert all("\x00" not in v[3] for v in values)
    assert SessionDB._decode_content(legacy_rows[0][3]) == SessionDB._decode_content(LEGACY)


def test_reverse_keeps_current_prefix_unchanged():
    """PG→SQLite 역백필은 ``\\x01json:`` 을 그대로 쓴다(코어가 읽는다). 역정규화 없음."""
    assert st.normalize_legacy_content_prefix(CURRENT) == CURRENT
