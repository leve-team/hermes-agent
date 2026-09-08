"""0044 — dual-write 배치 예산과 replay-safe rowcount 계약.

2026-09-09 X1-p-18b 실측이 근거다: `archive_and_compact` 가 mutation 383~463 건과
1,351 행 UPDATE(1.46 초)를 한 배치로 밀어 2 초 예산을 넘겼고, 그 실패가 뒤따르는
작은 배치(단독 0.02 초)까지 연쇄로 무너뜨렸다. 저널 #4 는 `expected_rowcount=871`
계약이 3 시간 뒤 replay 시점에 안 맞아 영구 복구 불가가 됐다.
"""
import sqlite3
import pytest

from hermes_state_dual import (
    DUAL_BATCH_MUTATION_LIMIT,
    DUAL_BATCH_ROWCOUNT_LIMIT,
    DualApplyMismatch,
    DualBatchDeferred,
    DualWriteReplicator,
    Mutation,
    batch_exceeds_budget,
)


def _mutation(operation="insert", rowcount=1, table="messages"):
    return Mutation(
        sql=f"{'INSERT INTO' if operation == 'insert' else 'UPDATE'} {table} SET x = ?",
        params=(1,),
        table=table,
        operation=operation,
        expected_rowcount=rowcount,
    )


def test_small_batch_stays_synchronous():
    assert batch_exceeds_budget([_mutation(), _mutation()]) is False


def test_batch_over_mutation_limit_is_deferred():
    batch = [_mutation(rowcount=1) for _ in range(DUAL_BATCH_MUTATION_LIMIT + 1)]
    assert batch_exceeds_budget(batch) is True


def test_batch_over_rowcount_limit_is_deferred():
    batch = [_mutation(operation="update", rowcount=DUAL_BATCH_ROWCOUNT_LIMIT + 1)]
    assert batch_exceeds_budget(batch) is True


def test_archive_and_compact_shape_is_deferred():
    """실측 형상: UPDATE 1,351행 + INSERT 380건."""
    batch = [_mutation(operation="update", rowcount=1351)]
    batch += [_mutation() for _ in range(380)]
    assert batch_exceeds_budget(batch) is True


def _replicator(tmp_path):
    source = sqlite3.connect(tmp_path / "source.db", isolation_level=None)
    replicator = DualWriteReplicator(source, "postgresql://unused/none")
    replicator.initialize_source()
    return replicator, source


def test_apply_refuses_oversized_batch_without_touching_postgres(tmp_path):
    """예산 초과 배치는 연결조차 열지 않고 DualBatchDeferred 로 저널에 넘긴다."""
    replicator, source = _replicator(tmp_path)
    opened = []
    replicator.connection_factory = lambda *a, **k: opened.append(1)
    batch = [_mutation(operation="update", rowcount=DUAL_BATCH_ROWCOUNT_LIMIT + 10)]
    with pytest.raises(DualBatchDeferred):
        replicator.apply("m-oversized", batch)
    assert opened == [], "예산 초과 배치가 PG 연결을 열었다"
    source.close()


def test_replay_of_oversized_batch_is_attempted(tmp_path):
    """replay 는 예산 밖 — 미루면 저널이 영원히 안 비므로 반드시 시도한다."""
    replicator, source = _replicator(tmp_path)
    opened = []

    def factory(*args, **keywords):
        opened.append(1)
        raise RuntimeError("connection attempted")

    replicator.connection_factory = factory
    batch = [_mutation(operation="update", rowcount=DUAL_BATCH_ROWCOUNT_LIMIT + 10)]
    with pytest.raises(RuntimeError, match="connection attempted"):
        replicator.apply("m-oversized", batch, replay=True)
    assert opened == [1], "replay 가 예산 검사에 막혔다"
    source.close()
