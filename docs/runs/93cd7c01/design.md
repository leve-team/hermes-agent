# 설계: 도구 호출별 실제 소요시간을 대화 기록에 영속 저장 (run 93cd7c01 / s1)

## 0. 대상 base 와 차단 사항 (먼저 읽을 것)
- 브리프 대상은 운영 포크 **`levos/pg3` (ead5725)** 이다. 이 run 의 작업 브랜치 `run/93cd7c01` 은 `main`(e629c90) 에서 갈라졌고,
  `main` 에는 `tui_gateway/tool_progress.py`, `hermes_state_messages.py`, `tui_gateway/prompt_turn.py` 가 **존재하지 않는다**
  (`levos/pg3` 는 `main` 보다 33,982 커밋 앞서 있고 공통 조상 관계도 아니다).
- 따라서 이 설계와 plan.json 의 파일 경로는 모두 **`levos/pg3` 기준**이다. 이 브랜치(main 기준)에 그대로 구현하면 대상 파일이 없어 구현할 수 없다.
- 브리프 지시("올바른 base 선택·검증·반영 경로 확인 전 코드 실행/머지 금지")에 따라 `plan.json` 의 `needs_replan=true` 로 둔다.
  구현(s3/s4)은 `levos/pg3` 를 base 로 한 작업 브랜치가 준비되고, 그 브랜치의 머지/운영 코어 반영 경로가 확인된 뒤에만 진행한다.
  현재 propose.py 에 base 옵션이 없고 landing_policy.external_cd_repos 에 이 레포 설정이 없으므로 **배포 경로 미지원 = 차단**으로 보고한다.
- 운영 DB 구조/권한 변경은 **필요 없다**(아래 2절: 기존 `messages.display_metadata` TEXT/JSON 열 재사용, 새 테이블·열·인덱스 없음).

## 1. 현황 (levos/pg3 ead5725 정찰)
- `tui_gateway/tool_progress.py::_on_tool_start` 가 `session["tool_started_at"][tool_call_id] = time.time()` 을 기록하고,
  `_on_tool_complete` 가 이를 pop 하여 `duration_s = time.time() - started_at if started_at else None` 을 계산해 **`tool.complete` 이벤트 payload 에만** 싣는다.
  - 문제: (a) 저장되지 않음 → 새로고침/재접속/프로세스 재시작 후 소실, (b) 벽시계(time.time) 기반이라 시계 보정 시 음수/왜곡 가능,
    (c) `if started_at` 진리값 검사(0.0 은 사실상 없지만 의미상 `is not None` 이 맞다).
- 도구 결과 메시지 행(role=`tool`, `tool_call_id`)은 에이전트 쪽 `agent/session_persistence.py` flush 가 `append_messages_batch` 로 쓴다.
  행 dict 에는 이미 `display_metadata` 가 실려 저장된다. `tool_complete_callback` 시점에는 해당 행이 아직 DB 에 없을 수 있다.
- `hermes_state_messages.py` 에는 이미 `display_metadata` 를 행 단위로 읽고-병합-쓰는 패턴(`set_message_reaction`, `_SET_DISPLAY_META_SQL`)이 있고,
  읽기 경로(`get_messages`, `_rows_to_conversation`, `tui_gateway/session_history.py`)는 `display_metadata` 를 dict 로 디코드해 그대로 내보낸다.
- 표시 중복제거 키 `_display_dedupe_key` 는 role/content/timestamp/tool_call_id/tool_calls/tool_name 만 쓰고 비-user 행의 display_metadata 는 쓰지 않는다
  → 메타데이터를 추가해도 **표시 identity 해시가 바뀌지 않는다**.
- 압축: `archive_and_compact` 는 (1) 동시 꼬리 행은 SQL 열 복제(`_clone_message_rows`, display_metadata 보존)하지만
  (2) `compacted_messages`(에이전트 메모리 dict)를 `_insert_message_rows` 로 새로 넣는다. 게이트웨이가 DB 에만 찍은 소요시간은 메모리 dict 에 없으므로
  (2)의 경로로 재삽입되는 tool 행에서는 **유실**된다. `replace_messages` 도 같은 문제.
- 턴 종료 후 처리 지점: `tui_gateway/prompt_turn.py::_absorb_turn_result` 가 이미 `agent._session_db` 로 "이번 턴에 막 flush 된 행" 에 표시 메타데이터를 찍는다
  (`set_latest_matching_message_display_kind`). 같은 지점을 재사용한다.

## 2. 대안과 선택
| 안 | 내용 | 판단 |
|---|---|---|
| A. 새 테이블 `tool_call_timings` | (session_id, tool_call_id, duration) | 기각: 운영 PG 스키마/권한 변경 필요, 브리프 "필요 없이 새 테이블/열 금지" |
| B. 새 열 `messages.tool_duration_s` | 열 추가 + PG 마이그레이션 | 기각: 동일 이유, SQLite/PG 양쪽 스키마·import·portability 수정 범위 확대 |
| C. 에이전트(tool_executor)의 `tool_duration` 을 tool 메시지 dict 의 display_metadata 에 실음 | 저장 경로 자연스러움 | 기각: 브리프가 계측 지점을 게이트웨이 `_on_tool_start/_on_tool_complete` 로 지정. 또 `_unfinished_tool_result` 는 타임아웃 시 `timeout_s`, 미완 시 `0.0` 을 **합성**하므로 "없는 시간 추정 금지/미완을 0초로 채우지 않음" 위반. agent/ 범위 확대도 됨 |
| **D. 기존 `display_metadata` 에 키 1개 추가, 게이트웨이가 턴 종료 후 tool 행에 병합 기록** | 실측은 게이트웨이, 저장은 기존 열 | **선택** |

display_metadata 는 "화면 표시용 메타데이터(모델 입력 아님)" 이고 소요시간은 화면 표시용 사실값이므로 의미 왜곡이 없다.
결과 본문(content)·api_content·tool_calls 는 손대지 않는다.

## 3. 저장 계약 (후속 API/SDK 복원이 그대로 쓸 계약)
- 위치: `messages.display_metadata` JSON 객체의 키 **`tool_duration_s`**.
- 대상 행: `role = 'tool'` 이고 `tool_call_id` 가 일치하는 행, 그리고 **같은 `session_id`** 의 행만(다른 세션/프로필 격리; 프로필별 세션 DB 는 기존대로 분리).
- 값: JSON number, **초 단위**, 유한(finite)·비음수(≥0) float. `0.0` 은 "실측 0초" 로 유효하다.
- 측정: 게이트웨이 프로세스의 `time.monotonic()` 으로 `_on_tool_start`→`_on_tool_complete` 의 같은 `tool_call_id` 쌍을 매칭.
  `tool.complete` 이벤트의 `duration_s` 도 이 동일 값을 쓴다(이벤트 키·의미는 기존 그대로: 초, 결측 시 키 없음).
- 결측 의미: **키가 없으면 "모름"**. 과거 행, 시작 누락, 실행 중 미완, 중단, 게이트웨이가 찍기 전 프로세스 종료 → 모두 키 없음. `null`/`0` 으로 채우지 않는다.
- 실패 결과(error 결과)도 실제로 시작·완료가 짝지어졌으면 기록한다(성공/실패와 무관한 시간 사실).
- 중복 완료: 시작 기록은 첫 완료가 pop 하므로 두 번째 완료는 시작 누락 → 기록 안 함. DB 병합은 **first-wins**(이미 키가 있으면 덮어쓰지 않음).
- 같은 이름 병렬 호출: 키가 `tool_call_id` 이므로 서로 섞이지 않는다.
- 같은 `tool_call_id` 의 tool 행이 압축 세대로 여러 개(archived 원본 + active 사본)면 **표시 가능한 모든 세대에** 동일 값을 찍는다.
- 다른 display_metadata 키(reactions 등)는 보존(read-merge-write).
- 모델 입력: display_metadata 는 기존대로 API 메시지로 가지 않는다. content 에 시간 문자열을 섞지 않는다.

## 4. 동작
1. `_on_tool_start`: 기존 `tool_started_at`(time.time) 은 다른 소비자 호환을 위해 그대로 두고, 같은 dict 에 monotonic 값을 넣도록 바꾼다
   (tool_started_at 의 소비자는 `tool_progress.py` 하나뿐 — server.py/methods_session.py/compute_host.py/agent_callbacks.py 는 빈 dict 초기화만).
2. `_on_tool_complete`: pop 한 시작값이 `is not None` 이면 `duration = max(0.0, monotonic() - started)` 를 계산하고, 유한값이면
   (a) payload `duration_s` 로 내보내고 (b) `session["tool_durations_pending"][tool_call_id] = duration` 에 쌓는다(이미 있으면 유지: first-wins).
   이벤트 emit 조건(tool_progress off 등)과 무관하게 (b)는 항상 수행한다.
3. `_absorb_turn_result`(턴 종료, 이번 턴 flush 직후): `db.set_tool_call_durations(current_session_id, pending)` 호출.
   반환된 "적용된 tool_call_id 집합" 은 pending 에서 제거, 행이 아직 없는 id 는 남겨 다음 턴 종료 때 재시도. 예외는 debug 로그 후 무시(턴 실패로 만들지 않음).
   pending 은 상한(예: 512개) 초과 시 오래된 것부터 버린다(결측으로 남음).
4. `hermes_state_messages.SessionMessagesMixin`(기존 mixin) 공개 메서드 2개 추가:
   - `set_tool_call_durations(session_id, durations: Mapping[str, float]) -> set[str]`:
     입력값 검증(str id, finite, ≥0; 아니면 무시), `SELECT id, tool_call_id, display_metadata FROM messages WHERE session_id=? AND role='tool' AND tool_call_id IN (...)`,
     키 없는 행만 병합 후 `_SET_DISPLAY_META_SQL`. 한 write txn. SQLite/PG 공통 SQL 만 사용(JSON 함수 미사용, Python 병합).
   - `get_tool_call_durations(session_id) -> dict[str, float]`: 같은 세션 tool 행의 저장값 조회(후속 API 계약 확인·테스트용 읽기 헬퍼).
5. 압축/재작성 보존: `archive_and_compact` 와 `replace_messages` 의 `_do` 안에서 **archive 전에** 해당 세션 tool 행의 `tool_duration_s` 를 수집하고,
   `_insert_message_rows` 직전 role='tool' 이고 같은 tool_call_id 이며 해당 키가 없는 입력 dict 의 **복사본**에 병합한다(호출자 dict 는 변형하지 않음).
   내용(content/api_content/tool_calls/timestamp) 은 그대로 → 표시 identity 동일.

## 5. 시험 (plan.json tests 와 1:1)
- 게이트웨이: 같은 이름 병렬 2건 ID 격리 / 0초(monotonic 고정) 저장 / 실패 결과도 저장 / 시작 누락 시 이벤트·pending 모두 없음 /
  중복 완료 first-wins / 다른 세션 sid 의 시작이 섞이지 않음 / 미완(시작만) 은 턴 종료 후에도 저장 안 함 / tool_progress off 여도 pending 기록 /
  `tool.complete` payload duration_s 와 pending 값 동일.
- 저장소(SQLite, 그리고 기존 cross-backend 하네스가 있으면 PG): 병합·first-wins·reactions 보존·다른 세션 비침범·비유한/음수 거부 /
  close 후 재오픈(새 SessionDB 인스턴스 = 프로세스 재시작 동등) 동일값 / `archive_and_compact`·`replace_messages` 후 active 행에 동일값 및 content·api_content·display identity 동일 /
  과거 행(키 없음)은 압축 후에도 키 없음 / `get_messages_as_conversation` 결과 content 에 시간 문자열 없음.
- 계약 fixture: `tests/fixtures/tool_duration_contract.json` 의 사례(정상/0초/결측/거부값)를 저장→조회 왕복으로 검증.

## 6. 범위 밖 (하지 않음)
- UI(desktop/ui-tui)·HTTP/RPC API·SDK 복원 연결: 후속 단계(브리프 3분해 중 2·3단계).
- agent/tool_executor 의 합성 duration, 새 테이블/열, Dockerfile/차트/.github/의존성.
- 하위 세션으로 분기하는 압축(hermes_state_compression 의 세션 분할 경로)에서의 계승: 정찰 결과 tool 행 사본은 같은 세션 in-place 압축이 주 경로이므로 이번엔 제외, 후속 작업에 적는다.

## 모듈과 의존성
- 재사용한 기존 모듈: `hermes_state_messages.py`(SessionMessagesMixin: `_encode/_decode_display_metadata`, `_SET_DISPLAY_META_SQL`, `_execute_write`, `_insert_message_rows`),
  `tui_gateway/tool_progress.py`(_on_tool_start/_on_tool_complete), `tui_gateway/prompt_turn.py`(_absorb_turn_result 의 기존 턴 후 DB 스탬프 지점).
- 새로 만든 모듈과 이유: 없음(새 파일은 테스트·계약 문서·fixture 뿐). 저장 로직은 같은 변경 이유를 가진 기존 `hermes_state_messages.py` 에 둔다.
- 공개 인터페이스: `SessionDB.set_tool_call_durations(session_id, durations) -> set[str]`, `SessionDB.get_tool_call_durations(session_id) -> dict[str, float]`,
  저장 계약 `display_metadata["tool_duration_s"]`(초, finite ≥0, 결측=키 없음), 기존 `tool.complete.duration_s` 이벤트(의미 불변).
- 의존 방향: `tui_gateway/tool_progress.py` → (세션 dict 만) ; `tui_gateway/prompt_turn.py` → `SessionDB` 공개 메서드 → `hermes_state_messages.py`. 저장소는 게이트웨이를 import 하지 않는다. 순환 없음.
- 후속 작업: (1) 세션 기록 API/SDK 복원에서 `tool_duration_s` 노출(2·3단계), (2) 하위 세션 분할 압축 경로의 계승, (3) `levos/pg3` base 작업 브랜치 생성 지원(propose.py base 옵션 / landing_policy.external_cd_repos 등록).
