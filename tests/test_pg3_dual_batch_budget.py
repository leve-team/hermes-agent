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


def test_replay_uses_a_generous_timeout_not_the_primary_budget(tmp_path):
    """replay 는 primary 를 붙잡지 않으므로 2초 예산을 물려받으면 안 된다.

    물려받으면 예산 초과로 저널에 들어간 배치가 replay 에서도 같은 지점에서
    취소돼 영원히 안 빠진다(X1-p-19 저널 #4: mutation 684·1,442행, attempts 2).
    """
    from hermes_state_dual import DUAL_REPLAY_TIMEOUT_SECONDS, DUAL_TIMEOUT_SECONDS

    assert DUAL_REPLAY_TIMEOUT_SECONDS > DUAL_TIMEOUT_SECONDS
    replicator, source = _replicator(tmp_path)
    seen = []

    def factory(dsn, timeout_s):
        seen.append(timeout_s)
        raise RuntimeError("stop here")

    replicator.connection_factory = factory
    with pytest.raises(RuntimeError):
        replicator.apply("m-sync", [_mutation()])
    with pytest.raises(RuntimeError):
        replicator.apply("m-replay", [_mutation()], replay=True)
    assert seen == [DUAL_TIMEOUT_SECONDS, DUAL_REPLAY_TIMEOUT_SECONDS]
    source.close()


def test_journalled_sqlite_null_safe_predicate_is_normalised_on_replay():
    """저널은 SQL 원문을 저장하므로 옛 `col IS ?` 가 그대로 남는다(결함 #11 잔재)."""
    from hermes_state_dual import _portable_null_safe_predicates as fix

    old = (
        "UPDATE sessions SET title = ?, title_source = ? "
        "WHERE id = ? AND title IS ? AND title_source IS ?"
    )
    got = fix(old)
    assert "IS NOT DISTINCT FROM ?" in got
    assert got.count("IS NOT DISTINCT FROM ?") == 2
    # 리터럴 IS NULL / IS NOT NULL 은 건드리지 않는다
    assert fix("WHERE a IS NULL AND b IS NOT NULL") == "WHERE a IS NULL AND b IS NOT NULL"
    # 이미 이식 가능한 문장은 그대로
    already = "WHERE a IS NOT DISTINCT FROM ?"
    assert fix(already) == already


def test_apply_normalises_journalled_sql_on_replay_only(tmp_path):
    """정규화가 apply 의 replay 경로에 실제로 배선돼 있는지(함수 존재만으론 부족)."""
    replicator, source = _replicator(tmp_path)
    executed = []

    class _Cursor:
        rowcount = 1

    class _Target:
        def execute(self, sql, params=()):
            executed.append(sql)
            return _Cursor()

        def commit(self): pass
        def rollback(self): pass
        def close(self): pass
        raw = None

    replicator.connection_factory = lambda *a, **k: _Target()
    legacy = Mutation(
        sql="UPDATE sessions SET title = ? WHERE id = ? AND title IS ?",
        params=("t", "s", None),
        table="sessions",
        operation="update",
        expected_rowcount=1,
    )
    replicator.apply("m-replay-sql", [legacy], replay=True)
    replayed = [q for q in executed if "UPDATE sessions" in q]
    assert replayed and "IS NOT DISTINCT FROM ?" in replayed[0], replayed

    executed.clear()
    replicator.apply("m-sync-sql", [legacy])
    synced = [q for q in executed if "UPDATE sessions" in q]
    assert synced and "IS NOT DISTINCT FROM ?" not in synced[0], synced
    source.close()


def test_state_dependent_gc_rowcount_never_fails_the_batch(tmp_path):
    """결함 #17 — `DELETE FROM system_prompts WHERE NOT EXISTS(...)` 의 rowcount 는
    복제본 상태의 함수라 primary 값과 달라도 정상이다. 같은 배치의 INSERT 가
    프롬프트를 나르므로 이 DELETE 때문에 배치가 죽으면 프롬프트가 영원히 PG 에
    못 간다(2026-09-09 Y3-n: 누락 3건, 저널 706/708/710).
    """
    from hermes_state_dual import _is_state_dependent_gc

    gc = ("DELETE FROM system_prompts WHERE NOT EXISTS (SELECT 1 FROM sessions "
          "WHERE sessions.system_prompt_hash = system_prompts.hash)")
    assert _is_state_dependent_gc(gc)
    assert _is_state_dependent_gc("DELETE FROM system_prompts\n  WHERE NOT EXISTS (x)")
    assert not _is_state_dependent_gc("DELETE FROM messages WHERE id = ?")
    assert not _is_state_dependent_gc("UPDATE sessions SET title = ? WHERE id = ?")

    replicator, source = _replicator(tmp_path)
    executed = []

    class _Cursor:
        def __init__(self, n): self.rowcount = n

    class _Target:
        raw = None
        def execute(self, sql, params=()):
            executed.append(sql)
            # INSERT 1행, GC 는 primary 가 0 을 기대했지만 복제본에선 3행 지움
            return _Cursor(3 if "NOT EXISTS" in sql else 1)
        def commit(self): pass
        def rollback(self): pass
        def close(self): pass

    replicator.connection_factory = lambda *a, **k: _Target()
    batch = [
        Mutation(sql="INSERT INTO system_prompts (hash, prompt) VALUES (?, ?)",
                 params=("h", "p"), table="system_prompts", operation="insert", expected_rowcount=1),
        Mutation(sql=gc, params=(), table="system_prompts", operation="delete", expected_rowcount=0),
    ]
    assert replicator.apply("m-gc", batch) == "applied"
    assert any(q.startswith("DELETE FROM system_prompts") for q in executed)

    # 데이터 문장은 여전히 엄격 — 같은 배치에서 UPDATE 가 어긋나면 죽는다
    strict = [Mutation(sql="UPDATE sessions SET title = ? WHERE id = ?", params=("t", "s"),
                       table="sessions", operation="update", expected_rowcount=0)]
    with pytest.raises(DualApplyMismatch):
        replicator.apply("m-strict", strict)
    source.close()
