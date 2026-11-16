# 실행 결과와 인계 상태

## 결과

양수 host 예산이 기존 compression auxiliary 실효 예산보다 짧아지지 않게 했다. timeout 뒤에는 출력 예약을 포함해 모델 window 내임이 확인된 요청만 turn-start와 pre-API 양쪽에서 계속한다. 원문을 다시 압축하거나 timeout을 성공으로 바꾸지 않는다. 불명/초과/실제 provider overflow의 차단 계약을 유지했다.

기준 및 최종 후보 준비 직전 원격 `levos/pg3` 확인값은 `ead5725180770242a1562d30a83631ee7d7f0fe6`이다. 정책의 후보 합성/검증/병합이 이후 동시 변경을 보존해야 한다. upstream 출처와 유지한 포크 차이는 `design.md`에 있다.

## 환경

- `/opt/hermes/.venv`에는 제품 의존성이 있지만 pytest가 없었다. 작업용 `.venv`를 만들고 기존 runtime site-packages를 연결했다.
- 시험 전용 패키지: pytest 8.4.2, pytest-asyncio 0.25.3, pytest-timeout 2.4.0. 제품 의존성/lockfile을 변경하지 않았다.
- 모든 시험은 `scripts/run_tests.sh`, `HERMES_TEST_FILE_RETRIES=0`으로 실행했다. 파일별 subprocess 격리, 임시 HERMES_HOME, credential 제거를 유지했다. gate bootstrap도 같은 runner를 사용한다.

## 실행 명령과 결과

아래 경로는 저장소 루트 기준이다. `P`는 통과, `F`는 실패이며 중간 실패를 PASS로 바꾸지 않았다. 파일에 담긴 상세 기록은 `evidence/`에 있다. 중간 디버깅 로그 네 개는 실패 이름과 원본 SHA-256을 남기고 요약했다.

1. **수정 전 RED** (`red.txt`, 19 P / 10 F):
   `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/agent/test_compression_host_aux_budget.py tests/agent/test_preflight_compression_timeout_fail_closed.py tests/run_agent/test_compression_timeout_terminal.py -j 2 -q`
   host/aux 연결 2건, 안전 window 재개 4건, 두 실제 turn-loop 분기의 provider 호출 4건이 기준 제품 코드에서 실패했다.
2. **최초 이식 후** (`first-green.txt`, 73 P / 5 F): 위 세 파일에 `tests/agent/test_compress_context_progress_timeout.py tests/agent/test_auxiliary_compression_timeout_floor.py tests/agent/test_turn_context.py tests/run_agent/test_preflight_timeout_typed_result.py`를 추가, 동일 runner `-j 2 -q`. 두 고정 예산 기대와 세 oversized fixture가 새 계약과 불일치했다. `design.md`의 의미 검토에 따라 보강했다.
3. **HTTP 개발 시험** (`targeted.txt`, 78 P / 2 F; `http-flow.txt`, 12 P / 2 F): 같은 runner에 `tests/run_agent/test_compression_timeout_http.py`를 추가했다. 초기 fixture의 preflight defer/SSE 응답, 메타데이터 POST 집계, trailing-whitespace 문제를 수정했다. 제품 assertion을 제거하지 않았다.
4. **관련 회귀** (`regression.txt`, 98 P / 2 F):
   `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/run_agent/test_compression_timeout_http.py tests/agent/test_compression_host_aux_budget.py tests/agent/test_compression_worker_isolation_76354.py tests/agent/test_compression_attempt_ownership.py tests/agent/test_compression_stall_interrupt_96775.py tests/agent/test_compression_fallback_budget.py tests/agent/test_preflight_lock_defer.py tests/run_agent/test_compression_lock_defer.py tests/run_agent/test_compression_persistence.py tests/run_agent/test_compression_budget_refund.py tests/run_agent/test_preflight_compression_cap_e2e.py tests/agent/test_native_preflight_estimate.py tests/agent/test_usage_anchor.py tests/agent/test_compression_attempt_telemetry.py tests/agent/test_compression_interrupt_protection.py -j 3 -q`
   두 실패는 위 HTTP fixture의 trailing space였고 다음 실행에서 해결했다. 나머지 lock/원본/fence/ownership/예산/anchor 회귀는 통과했다.
5. **HTTP 해결 확인** (`http-flow-final.txt`, 18 P / 0 F):
   `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/run_agent/test_compression_timeout_http.py tests/agent/test_preflight_compression_timeout_fail_closed.py -j 2 -q`
6. **출력 예약 보강 확인** (`reservation.txt`, 39 P / 0 F):
   `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/agent/test_preflight_compression_timeout_fail_closed.py tests/run_agent/test_compression_timeout_terminal.py tests/run_agent/test_compression_timeout_http.py tests/agent/test_compression_host_aux_budget.py -j 2 -q`
   Anthropic/profile/Gemini 실제 transport cap과 경계를 비교했다. turn-start 후 조립된 요청이 커지는 경우도 차단했다.
7. **수명주기 및 저장 회귀** (`lifecycle.txt`, 103 P / 0 F):
   `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/agent/test_compression_attempt_lifecycle.py tests/agent/test_compression_review_76354.py tests/test_compression_watermark_commit.py tests/agent/test_engine_preflight_wire.py tests/agent/test_compression_stall_fallback_78981.py tests/agent/test_compress_context_progress_timeout.py tests/agent/test_turn_context.py tests/run_agent/test_preflight_timeout_typed_result.py tests/run_agent/test_compression_abort_state_reset.py tests/run_agent/test_compression_boundary.py tests/agent/test_preflight_compression_gate.py -j 3 -q`
8. **첫 콘텐츠 지연 RED 재확인** (`delayed-first-content-red.txt`, 0 P / 1 F):
   `git worktree add --detach /tmp/hermes-timeout-red-base ead5725180770242a1562d30a83631ee7d7f0fe6` 후 새 host budget 시험만 복사했다. 해당 worktree에서 `HERMES_PYTHON=/tmp/lr-_y8lv8nu/.venv/bin/python HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/agent/test_compression_host_aux_budget.py -k test_configured_wait_allows_delayed_first_content -j 1 -q` 실행. 구 host idle 이후/aux 예산 이전 첫 콘텐츠가 도착해도 원본 반환으로 실패했다.
9. **host/aux 최종 확인** (`budgets-final.txt`, 14 P / 0 F):
   `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/agent/test_compression_host_aux_budget.py tests/agent/test_auxiliary_compression_timeout_floor.py -j 2 -q`
   실 config → 실제 auxiliary call timeout 전파, 더 큰 명시값, idle 비활성, 비양수 ceiling, 비유한 auxiliary가 유한 host 기한을 없애지 않는 경우, 지연 첫 콘텐츠 후 정상 commit을 포함한다.
10. **timeout만 compressed 표시에서 제외한 최소 변경 확인** (`compaction-final.txt`, 42 P / 0 F):
    `HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/agent/test_turn_context.py tests/agent/test_engine_preflight_wire.py tests/agent/test_preflight_lock_defer.py tests/run_agent/test_compression_timeout_http.py tests/run_agent/test_compression_timeout_terminal.py -j 3 -q`
11. **관리형 HTTP live probe** (`live-probe.json`, HTTP 200):
    `env -i HOME="$HOME" PATH="/usr/bin:/bin" LANG=C.UTF-8 TZ=UTC PYTHONPATH="$PWD" .venv/bin/python tests/run_agent/test_compression_timeout_http.py --serve 18761`를 `lifetime=service`, TCP readiness `127.0.0.1:18761`로 시작했다. 실행 ID `e38a38e0b647485cb52e2ff119ad848a`, ready=true를 확인했다. `curl --fail --silent --show-error --max-time 10 http://127.0.0.1:18761/probe` 응답에서 두 경로 각각 provider 요청 1회, 압축 시도 1회, late_commit=false, original_preserved=true, continued를 확인했다. 운영 배포 probe가 아닌 격리 코어 probe다.

한 번의 전체 suite 실행으로 위 숫자를 합산하지 않는다. 최종 후보의 통합 필수 검사는 `delivery.json`에 선언한 29개 관련 파일이며, 러너가 합성 후보에서 실행한다. 엔진은 그 등록 gate를 재실행하지 않는다. 서비스 probe는 두 진입점에서 실제 SDK/HTTP 전송, 원본·session·prompt 보존, 한 번의 압축 시도와 늦은 커밋 거부를 확인한 뒤 `/probe`를 제공한다.

## 완료 경계

후보 commit과 verification-ready 신호는 **검사 요청**이다. 이 문서를 작성한 시점에는 최종 러너 PASS/`levos/pg3` merge receipt가 아직 없다. 러너가 기록할 최종 결과를 우선한다. 이미지 빌드, wrapper 핀/빌드, Dave 런타임 배포 및 실제 운영 대화 압축/도구 왕복은 병합 후 운영 경로에서 수행할 일이며, 여기서 실행하거나 완료했다고 주장하지 않는다.
