"""Cross-process handoff coverage for session-turn lease epochs."""

from __future__ import annotations

import multiprocessing
import queue
import time
from pathlib import Path

from hermes_state import SessionDB, StaleLeaseError


_SESSION_ID = "shared-rollout-session"
_REUSED_HOLDER = "pid=1:turn=rollout"


def _pod_a(
    db_path: str,
    acquired: multiprocessing.synchronize.Event,
    successor_acquired: multiprocessing.synchronize.Event,
    stale_attempted: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    db = SessionDB(Path(db_path))
    try:
        ok = db.try_acquire_session_turn_lease(
            _SESSION_ID, _REUSED_HOLDER, ttl_seconds=0.1
        )
        epoch = db.session_turn_lease_epoch(_SESSION_ID, _REUSED_HOLDER)
        results.put(("pod_a_acquired", ok, epoch))
        acquired.set()
        if not successor_acquired.wait(timeout=10):
            results.put(("pod_a_write", "successor_timeout"))
            return
        try:
            db.append_message(
                _SESSION_ID,
                "assistant",
                "late pod A write",
                turn_lease_holder=_REUSED_HOLDER,
                turn_lease_epoch=epoch,
            )
        except StaleLeaseError:
            results.put(("pod_a_write", "StaleLeaseError"))
        else:
            results.put(("pod_a_write", "accepted"))
        finally:
            stale_attempted.set()
    finally:
        db.close()


def _pod_b(
    db_path: str,
    acquired: multiprocessing.synchronize.Event,
    successor_acquired: multiprocessing.synchronize.Event,
    stale_attempted: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    if not acquired.wait(timeout=10):
        results.put(("pod_b_acquired", False, None))
        return
    time.sleep(0.25)
    db = SessionDB(Path(db_path))
    try:
        ok = db.try_acquire_session_turn_lease(
            _SESSION_ID, _REUSED_HOLDER, ttl_seconds=5
        )
        epoch = db.session_turn_lease_epoch(_SESSION_ID, _REUSED_HOLDER)
        results.put(("pod_b_acquired", ok, epoch))
        successor_acquired.set()
        if not ok:
            return
        db.append_message(
            _SESSION_ID,
            "assistant",
            "pod B owns epoch 2",
            turn_lease_holder=_REUSED_HOLDER,
            turn_lease_epoch=epoch,
        )
        results.put((
            "pod_b_write",
            [message["content"] for message in db.get_messages(_SESSION_ID)],
        ))
        stale_attempted.wait(timeout=10)
        db.release_session_turn_lease(_SESSION_ID, _REUSED_HOLDER, lease_epoch=epoch)
    finally:
        db.close()


def test_pod_handoff_rejects_old_epoch_write_across_processes(tmp_path):
    """Pod B's reacquire fences Pod A even when PID/holder text is reused."""
    db_path = tmp_path / "state.db"
    setup = SessionDB(db_path)
    setup.create_session(_SESSION_ID, source="integration-test")
    setup.close()

    ctx = multiprocessing.get_context("spawn")
    acquired = ctx.Event()
    successor_acquired = ctx.Event()
    stale_attempted = ctx.Event()
    results = ctx.Queue()
    pod_a = ctx.Process(
        target=_pod_a,
        args=(
            str(db_path),
            acquired,
            successor_acquired,
            stale_attempted,
            results,
        ),
        name="pod-a",
    )
    pod_b = ctx.Process(
        target=_pod_b,
        args=(
            str(db_path),
            acquired,
            successor_acquired,
            stale_attempted,
            results,
        ),
        name="pod-b",
    )

    pod_a.start()
    pod_b.start()
    pod_a.join(timeout=15)
    pod_b.join(timeout=15)

    if pod_a.is_alive():
        pod_a.terminate()
        pod_a.join(timeout=2)
    if pod_b.is_alive():
        pod_b.terminate()
        pod_b.join(timeout=2)
    assert pod_a.exitcode == 0
    assert pod_b.exitcode == 0

    observed = {}
    try:
        for _ in range(4):
            item = results.get(timeout=2)
            observed[item[0]] = item[1:]
    except queue.Empty as exc:
        raise AssertionError(f"missing child result; observed={observed}") from exc
    finally:
        results.close()
        results.join_thread()

    assert observed == {
        "pod_a_acquired": (True, 1),
        "pod_b_acquired": (True, 2),
        "pod_a_write": ("StaleLeaseError",),
        "pod_b_write": (["pod B owns epoch 2"],),
    }
