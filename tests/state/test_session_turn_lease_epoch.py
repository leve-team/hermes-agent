"""``session_turn_leases.lease_epoch`` — monotonic fencing token (levos S1).

The holder string alone cannot tell "the same holder, still owning" from
"the same holder string, re-issued after a reclaim" (PID reuse, a process
that lost and re-took its own lease while a stale flush thread was still
alive). The epoch can: every acquire advances it, refresh never does, and a
transcript write that presents an older epoch is refused with
``StaleLeaseError`` inside the write transaction.
"""

import os
import sqlite3
import time

import pytest

from hermes_state import (
    SessionDB,
    SessionTurnLeaseLostError,
    StaleLeaseError,
    classify_persistence_error,
)
from hermes_state_common import StaleLeaseError as CommonStaleLeaseError


def _row(db, conversation_id):
    with db._read_ctx() as conn:
        row = conn.execute(
            "SELECT holder, expires_at, lease_epoch FROM session_turn_leases "
            "WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()
    return dict(row) if row is not None else None


def test_lease_epoch_error_is_a_lost_lease_for_every_existing_handler():
    assert StaleLeaseError is CommonStaleLeaseError
    assert issubclass(StaleLeaseError, SessionTurnLeaseLostError)
    assert classify_persistence_error(StaleLeaseError("x")) == "turn_lease"


def test_lease_epoch_normal_acquire_write_release(tmp_path):
    """① Happy path: acquire -> read epoch -> fenced write lands."""
    db = SessionDB(tmp_path / "state.db")
    db.create_session("shared", source="test")
    holder = f"pid={os.getpid()}:turn=owner"

    assert db.session_turn_lease_epoch("shared", holder) is None
    assert db.try_acquire_session_turn_lease("shared", holder, ttl_seconds=5)
    epoch = db.session_turn_lease_epoch("shared", holder)
    assert epoch == 1

    assert db.append_messages_batch(
        "shared",
        [{"role": "user", "content": "fenced"}],
        turn_lease_holder=holder,
        turn_lease_epoch=epoch,
    ) == 1
    assert db.append_message(
        "shared",
        "assistant",
        "fenced single",
        turn_lease_holder=holder,
        turn_lease_epoch=epoch,
    )
    assert [m["content"] for m in db.get_messages("shared")] == [
        "fenced",
        "fenced single",
    ]

    db.release_session_turn_lease("shared", holder)
    assert db.session_turn_lease_epoch("shared", holder) is None
    # Tombstone, not deletion: the epoch keeps counting across a release.
    assert _row(db, "shared") == {"holder": "", "expires_at": 0, "lease_epoch": 2}


def test_lease_epoch_expired_lease_taken_by_other_holder_fences_old_writer(tmp_path):
    """② Expiry -> another holder acquires -> the old holder's write is refused."""
    db = SessionDB(tmp_path / "state.db")
    db.create_session("shared", source="test")
    stale = f"pid={os.getpid()}:turn=stale"
    taker = f"pid={os.getpid()}:turn=taker"

    assert db.try_acquire_session_turn_lease("shared", stale, ttl_seconds=0.05)
    stale_epoch = db.session_turn_lease_epoch("shared", stale)
    assert stale_epoch == 1
    time.sleep(0.12)

    assert db.try_acquire_session_turn_lease("shared", taker, ttl_seconds=5)
    assert db.session_turn_lease_epoch("shared", taker) == 2
    assert db.session_turn_lease_epoch("shared", stale) is None

    with pytest.raises(SessionTurnLeaseLostError, match="turn lease lost"):
        db.append_messages_batch(
            "shared",
            [{"role": "assistant", "content": "late stale reply"}],
            turn_lease_holder=stale,
            turn_lease_epoch=stale_epoch,
        )
    assert db.get_messages("shared") == []

    assert db.append_messages_batch(
        "shared",
        [{"role": "assistant", "content": "taker reply"}],
        turn_lease_holder=taker,
        turn_lease_epoch=2,
    ) == 1


def test_lease_epoch_same_holder_string_reissued_after_expiry_is_stale(tmp_path):
    """② (hard case) Same holder string re-acquires after expiry.

    Holder-only fencing cannot see this: the row's holder matches the stale
    writer. The epoch moved from 1 to 2, so the old token is refused with
    ``StaleLeaseError`` while the new incarnation writes normally.
    """
    db = SessionDB(tmp_path / "state.db")
    db.create_session("shared", source="test")
    holder = "pid=4242:turn=reused"

    assert db.try_acquire_session_turn_lease("shared", holder, ttl_seconds=0.05)
    old_epoch = db.session_turn_lease_epoch("shared", holder)
    time.sleep(0.12)
    assert db.try_acquire_session_turn_lease("shared", holder, ttl_seconds=5)
    new_epoch = db.session_turn_lease_epoch("shared", holder)
    assert (old_epoch, new_epoch) == (1, 2)

    with pytest.raises(StaleLeaseError, match="lease_epoch 1 is stale"):
        db.append_messages_batch(
            "shared",
            [{"role": "assistant", "content": "from the dead incarnation"}],
            turn_lease_holder=holder,
            turn_lease_epoch=old_epoch,
        )
    with pytest.raises(StaleLeaseError):
        db.append_message(
            "shared",
            "assistant",
            "single from the dead incarnation",
            turn_lease_holder=holder,
            turn_lease_epoch=old_epoch,
        )
    assert db.get_messages("shared") == []

    assert db.append_messages_batch(
        "shared",
        [{"role": "assistant", "content": "live incarnation"}],
        turn_lease_holder=holder,
        turn_lease_epoch=new_epoch,
    ) == 1
    # Holder-only callers (no epoch) keep the pre-fencing behaviour.
    assert db.append_messages_batch(
        "shared",
        [{"role": "assistant", "content": "legacy caller"}],
        turn_lease_holder=holder,
    ) == 1


def test_lease_epoch_refresh_keeps_epoch(tmp_path):
    """③ Refresh extends expiry and never advances the epoch."""
    db = SessionDB(tmp_path / "state.db")
    db.create_session("shared", source="test")
    holder = f"pid={os.getpid()}:turn=owner"

    assert db.try_acquire_session_turn_lease("shared", holder, ttl_seconds=1)
    before = _row(db, "shared")
    assert db.refresh_session_turn_lease("shared", holder, ttl_seconds=60)
    assert db.refresh_session_turn_lease(
        "shared", holder, ttl_seconds=60, lease_epoch=1
    )
    after = _row(db, "shared")
    assert after["lease_epoch"] == before["lease_epoch"] == 1
    assert after["expires_at"] > before["expires_at"]
    assert db.session_turn_lease_epoch("shared", holder) == 1

    # A refresher that outlived a reissue must not keep the new lease alive.
    assert not db.refresh_session_turn_lease(
        "shared", holder, ttl_seconds=60, lease_epoch=7
    )
    assert db.append_messages_batch(
        "shared",
        [{"role": "user", "content": "still epoch 1"}],
        turn_lease_holder=holder,
        turn_lease_epoch=1,
    ) == 1


def test_lease_epoch_is_monotonic_across_release_and_reacquire(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    db.create_session("shared", source="test")
    first = f"pid={os.getpid()}:turn=first"
    second = f"pid={os.getpid()}:turn=second"

    assert db.try_acquire_session_turn_lease("shared", first, ttl_seconds=5)
    assert db.session_turn_lease_epoch("shared", first) == 1
    db.release_session_turn_lease("shared", first)
    # Idempotent: a second release by a non-owner leaves the tombstone alone.
    db.release_session_turn_lease("shared", first)
    assert _row(db, "shared")["lease_epoch"] == 2

    assert db.try_acquire_session_turn_lease("shared", second, ttl_seconds=5)
    assert db.session_turn_lease_epoch("shared", second) == 3
    # The released holder's token is behind the row: refused on holder first.
    with pytest.raises(SessionTurnLeaseLostError):
        db.append_message(
            "shared", "user", "after release", turn_lease_holder=first,
            turn_lease_epoch=1,
        )
    # A stale token on the live holder string is refused on the epoch.
    with pytest.raises(StaleLeaseError):
        db.append_message(
            "shared", "user", "old token", turn_lease_holder=second,
            turn_lease_epoch=1,
        )
    db.release_session_turn_lease("shared", second, lease_epoch=99)
    assert db.session_turn_lease_epoch("shared", second) == 3, (
        "release fenced on a wrong epoch must be a no-op"
    )
    db.release_session_turn_lease("shared", second, lease_epoch=3)
    assert _row(db, "shared")["lease_epoch"] == 4


def test_lease_epoch_column_is_reconciled_onto_a_pre_fencing_database(tmp_path):
    """A state.db created before the column existed gains it on open."""
    path = tmp_path / "state.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE session_turn_leases ("
        " conversation_id TEXT PRIMARY KEY, holder TEXT NOT NULL,"
        " acquired_at REAL NOT NULL, expires_at REAL NOT NULL);"
        "INSERT INTO session_turn_leases VALUES ('legacy', 'pid=1:turn=x', 1, 2);"
    )
    conn.commit()
    conn.close()

    db = SessionDB(path)
    cols = {
        r[1]
        for r in db._conn.execute("PRAGMA table_info(session_turn_leases)").fetchall()
    }
    assert "lease_epoch" in cols
    assert _row(db, "legacy")["lease_epoch"] == 0
    db.create_session("legacy", source="test")
    # The legacy (expired) row is reclaimed in place and the epoch starts
    # counting from the backfilled default.
    assert db.try_acquire_session_turn_lease("legacy", "pid=2:turn=y", ttl_seconds=5)
    assert db.session_turn_lease_epoch("legacy", "pid=2:turn=y") == 1
