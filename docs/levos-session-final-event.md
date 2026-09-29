# `session.final` — 정착한 세션의 마지막 답변 (levos patch 0071)

카드: `t_3abc9c7c` · 요청자: eren.song@withleve.com · 등록 세션: `20260928_142438_a00807`
대상 코어: `levos/pg3-020` (0.20.5, 기준 `7dfe6309ff9dbecd1768def9fe8adf503a80c831`)

levos 패치 번호는 `levos/pg3-020` 의 마지막 패치 0070 다음인 **0071** 이다.
이 패치는 `tui_gateway/server.py` 한 파일만 바꾸고, 브로커·포털·화면 쪽
소비는 별도 levos 카드(`세션 최종답변 웹푸시 — levos 쪽`)의 몫이다.
`levos/pg3`(0.21.2, v3) 는 파일 구조가 달라 대상이 아니다.

## 왜 필요한가

`message.complete` 는 턴당 1회다. 한 "대화"는 여러 턴으로 이어진다 — 턴 중
큐잉된 사용자 입력(`_drain_queued_prompt`), `/goal` 연속, 백그라운드 프로세스
완료·비동기 서브에이전트 완료 각성(post-turn drain, 알림 폴러). 턴마다 푸시하면
중간 답변이 스팸이 된다. 그래서 코어가 "이 세션이 정착했다"를 판정해
`session.final` 을 한 번 낸다. 기존 `message.complete`·`session.info` 의
payload 와 발행 시점은 바뀌지 않는다.

## 이벤트 계약

envelope 는 기존 `_emit` 그대로다(`params.session_id` = live UI sid).

```json
{"jsonrpc": "2.0", "method": "event", "params": {
  "type": "session.final", "session_id": "<live sid>",
  "payload": {
    "stored_session_id": "<session_key>",
    "gen": 3,
    "status": "complete",
    "title": "<세션 제목, 없으면 빈 문자열>",
    "preview": "<최종 답변 앞 200자>",
    "finished_at": 1790000000.0
  }}}
```

- `status` 는 `complete` 또는 `error` 뿐이다. `interrupted` 로 끝난 턴은
  후보가 되지 않는다. 반환된 오류(provider 4xx 등)와 턴 예외 모두 `error`.
- `gen` 은 세션의 턴 세대(`final_gen`)다. 같은 세션에서 단조 증가한다.
- `preview` 는 그 턴 `message.complete` 의 `text` 를 앞뒤 공백 제거 후 200자.
- `finished_at` 은 턴 정리 시각(epoch 초)이다. 발행 시각이 아니다.

## 발행 조건

1. 세션 dict 에 `final_gen`(int, 기본 0)·`final_candidate`(dict|None).
2. `_run_prompt_submit` 이 턴 실행을 확정하는 지점(`history_lock` 안,
   `_closing`·stale queued generation 거절 분기 뒤)에서 `final_gen += 1`,
   `final_candidate = None`. 거절된 제출은 세대를 올리지 않는다.
3. 턴 정리 끝(`_emit_settled_session_info` 직후)에 그 턴의 결과로
   `final_candidate = {gen, text, status, title, finished_at}` 를 기록하고
   정착 검사 1건을 예약한다(세션별 `threading.Timer` 1개, 이미 있으면 교체).
   기록 시점에 세대가 이미 바뀌었으면 기록하지 않는다.
4. `FINAL_SETTLE_SECONDS`(모듈 상수, 60초) 뒤 검사가 한 번 돈다. 모두 참일
   때만 발행한다: `running` False, `queued_prompt`·`queued_prompts` 없음,
   후보가 있고 `gen == final_gen`, `_session_has_active_delegations` False
   (예외 시 True → 보수적 탈락), `_closing` 아님이고 세션이 레지스트리
   `_sessions[sid]` 에 그대로 있음.
5. 통과하면 후보를 `history_lock` 안에서 소비(`None`)하고 lock 밖에서
   `_emit("session.final", ...)`. 불통과면 후보를 버리고 재시도하지 않는다.
   다음에 끝나는 턴이 자기 후보를 만든다.
6. 검사는 `running` 을 바꾸지 않고 새 턴을 시작하지 않는다. 레지스트리·위임
   조회는 다른 lock 과 상태 DB 를 탈 수 있어 `history_lock` 밖에서 하고,
   싼 조건은 소비 직전에 lock 안에서 다시 확인한다.
7. 탈락은 `session.final skipped: reason=<r> sid=<sid>` INFO 한 줄로 남는다.
   `reason` ∈ `running`, `queued`, `superseded`, `delegations`, `closing`,
   `no_candidate`.

설정 env var 는 없다(AGENTS.md: 새 `HERMES_*` 금지). 대기 시간은 상수다.

## 보장 수준 — best-effort

운영자가 수용한 수준(설계 v4.1)은 best-effort 다. 드물게 1건 누락되거나,
대화 중간에 한 번 먼저 울릴 수 있다. 화면은 세션당 알림 1칸을 교체하므로
허용한다. 이 패치는 "정확히 1회"를 위해 분산 경계를 막지 않는다.

알려진 경쟁 구간 두 곳:

- **폴러가 큐에서 꺼낸 뒤 턴 시작 전.** 알림 폴러·post-turn drain 이 완료
  이벤트를 꺼내 `running` 을 잡기 전 사이에 검사가 돌면, 큐도 `running` 도
  비어 보여 직전 답변이 먼저 발행될 수 있다(중간에 한 번 먼저 울림).
- **프로세스 exited 와 큐 적재 사이.** 백그라운드 프로세스가 끝났지만
  완료 알림이 아직 큐에 적재되지 않은 틈에 검사가 돌면 같은 이유로 먼저
  발행될 수 있다. 이어지는 턴은 자기 후보를 따로 만든다.

폴러·post-turn drain 의 claim 실패 시 `running` 고착(기존 결함)은 이 패치
범위 밖이다. 그 상태의 세션은 `reason=running` 으로 탈락한다(누락 쪽).

## 범위 밖

- **compute-host 경로**(`_submit_prompt_to_compute_host`)로 실행된 턴은
  후보를 만들지 않는다. 현재 운영 설정에 compute_host 가 없다.
- 브로커의 웹푸시, 포털·화면 표시는 levos 쪽 카드가 한다.

## 검증

`tests/tui_gateway/test_session_final_event.py` 가 계약을 고정한다: 정상
정착 1회와 payload 6키, 큐잉 입력으로 이어진 턴·정착 대기 중 새 턴의 이전
후보 억제, 활성 위임·interrupted·running·queued·closing/분리 억제, error
턴 발행, 거절된 제출의 세대 불변, lock 밖 emit. 대기는 상수를 0.01초로
줄여 실시간 대기 없이 돈다.
