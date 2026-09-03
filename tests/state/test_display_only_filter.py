"""``get_messages_as_conversation`` display-only filter (levos hotfix).

Pins the SQLite semantics of the ``COALESCE(display_only, 0) = 0`` clause
that replaced the SQLite-only ``IFNULL`` spelling: display-only rows are
excluded by default, rows written before the column existed (NULL) are kept,
and ``include_display_only=True`` returns everything.
"""

from hermes_state import SessionDB


def test_display_only_rows_are_filtered_with_portable_coalesce(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    db.create_session("s", source="test")
    db.append_message("s", "user", "kept")
    db.append_message("s", "assistant", "shown only", display_only=True)
    legacy_id = db.append_message("s", "assistant", "pre-column row")
    # Simulate a row written before display_only existed.
    db._execute_write(
        lambda c: c.execute(
            "UPDATE messages SET display_only = NULL WHERE id = ?", (legacy_id,)
        )
    )

    default = [m["content"] for m in db.get_messages_as_conversation("s")]
    assert default == ["kept", "pre-column row"]

    everything = [
        m["content"]
        for m in db.get_messages_as_conversation("s", include_display_only=True)
    ]
    assert everything == ["kept", "shown only", "pre-column row"]
