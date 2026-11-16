# 압축 시간초과 선별 이식

## 기준과 출처

- 대상: `leve-team/hermes-agent:levos/pg3`, 기준 `ead5725180770242a1562d30a83631ee7d7f0fe6`.
- 공식 소스: [NousResearch/hermes-agent @ 4162ecf11f611400fa6e5c42d2f360188ce50b39](https://github.com/NousResearch/hermes-agent/tree/4162ecf11f611400fa6e5c42d2f360188ce50b39).
- 해당 스냅샷의 `agent/conversation_compression_deadline.py:24-57`과 `agent/turn_context.py:438-459`에서 양수 host 예산 상향과 timeout 후 명확한 window 내 요청 재개 동작을 선별했다. `conversation_compression.py::request_exceeds_model_window`가 필요한 선행 판정이다.
- 이 revision의 커밋 제목은 cache clock 수정이다. **이 SHA 전체를 timeout 수정 커밋으로 cherry-pick했다고 주장하지 않는다.** 공식 스냅샷에서 위 동작만 현재 포크에 이식했다. 파일 분리, fallback 확대, deterministic escalation, prompt-reading deadline 연장 등 나머지 upstream 변화는 가져오지 않았다.

## 예산과 요청 크기 계약

`resolve_context_compression_timeouts`는 기존 `_effective_aux_timeout("compression", None)`을 그대로 호출한다. 양수 idle과 ceiling은 실효 보조 예산 이상으로 올리며, 더 큰 명시값과 idle <= 0의 비활성 의미를 보존한다. 양수가 아닌 ceiling은 기존처럼 기본 ceiling을 사용한다. 비유한 보조 예산은 host의 유한 기한을 없애지 않는다. 보조 모델, 추론강도, 프로필, 설정 스키마는 바꾸지 않는다.

크기 추정은 기존 경로를 유지한다.

1. turn-start `_preflight_request_tokens`: 유효한 provider usage anchor, native Responses의 checkpoint-pruned payload, 일반 system+messages+tools 추정 순서.
2. pre-API `assemble_request` / `_midturn_request_pressure_tokens`: 실제 조립된 메시지(시스템 포함)와 도구, native wire 추정, 유효 anchor 또는 마지막 실제 prompt 사용량 floor.
3. 이 수치는 입력 크기다. 이전 응답의 completion은 anchor에서 현재 입력으로 포함되며, 다음 요청의 출력 예약과 다르다.

공통 판정은 soft compression threshold가 아닌 `context_length − output reservation`과 비교한다. 경계의 동등값은 허용하고 초과는 차단한다. window/추정치/출력 예약이 불명 또는 잘못된 타입이면 `None`으로 차단한다. 음수 추정과 bool도 안전 근거로 삼지 않는다. 시스템/도구 토큰은 다시 더하지 않고, compressor/agent/일회성 cap은 합산하지 않고 보수적인 최댓값을 한 번만 예약한다.

포크의 `ContextCompressor.max_tokens`(native Gemini 기본 예약 포함)를 유지한다. 실제 wire가 cap을 추가하는 Anthropic 기본값, provider profile 기본값, Gemini thinking 상향은 기존 transport 해석 함수를 읽는다. 명시 request output override도 포함한다. 요청 조립 함수를 미리 호출해서 일회성 설정이나 캐시 상태를 소비하지 않는다. cap 해석 실패는 불명으로 처리한다. 명시 cap이 없는 일반 wire는 기존의 provider-default 계약을 유지한다. 이 수정은 추정기를 교체하거나 provider의 실제 overflow보다 추정을 우선시키지 않는다.

## 두 호출 경로와 보존하는 차이

- turn-start: timeout + 무진행 뒤, 공통 판정이 정확히 False인 경우만 계속한다. `blocked`로 이번 턴의 중복 proactive 압축을 막고, 실패한 pass를 `compressed=True`로 기록하지 않는다.
- pre-API: 조립된 요청으로 같은 판정을 다시 한다. turn-start 이후 injection/tool 결과로 커졌다면 차단한다. 자체 압축 timeout에서도 같은 정책을 적용한다. 전송할 요청의 API 횟수/iteration budget은 환불하지 않으며, 차단할 때는 기존 typed terminal과 환불을 유지한다.
- 실제 provider overflow recovery는 기존 실패 경로를 유지한다. `provider_overflow_preflight`는 추정이 작아도 재개를 허용하지 않는다.
- 원문, session ID, PostgreSQL authority, 단일 압축 lock/lease, worker snapshot, commit fence, 취소 후 late commit 거부, 진행 감지, 전체 deadline, cooldown, 승인 경계, prompt cache 및 역할 순서는 기존 구현을 유지한다. timeout을 성공으로 바꾸거나 실패한 요약을 커밋하지 않는다.

## 기존 시험 변경의 의미 검토

- `test_compress_context_progress_timeout.py`의 두 고정값 검사는 host/aux 예산 관계로 바꿨다. 비활성 시험은 그대로다. 의도된 동작 변경을 검사하며 게이트를 느슨하게 만들지 않는다.
- `test_turn_context.py`의 8,000자 입력은 실제 4,000-token window보다 작았다. 32,000자로 늘려 원래의 **oversized payload 차단** 주장을 실제로 성립시킨다. 예외와 미전송 기대는 유지한다.
- 두 run-agent terminal fixture는 tiny transcript와 threshold=1만으로 oversized를 가정했다. window=1도 명시해 실제 초과로 만들었다. 미전송, typed result, 실패 기대를 유지한다.
- 삭제/skip/only 또는 보안 게이트 완화는 없다. 새 허용/차단 matrix와 실제 HTTP 흐름이 이 fixture 변경을 독립적으로 보완한다.

## 검증과 인계

실행 명령/결과는 `results.md`와 `evidence/`에 기록한다. HTTP 시험은 격리 HERMES_HOME, 실제 import/SDK/HTTP/turn loop/호스트 timeout/fence를 사용한다. 외부 LLM 대신 로컬 provider와 지연 worker를 사용하므로 운영 LLM 배포 증거는 아니다. 저장 계층 회귀는 격리 SessionDB에서 실행하며 운영 PostgreSQL에 쓰지 않는다.

이 run의 완료 범위는 코어 검증과 정책이 고정한 `levos/pg3` 병합이다. 병합 이후 코어 이미지 → sessionplane wrapper 핀/빌드 → Dave 런타임 배포 → 보존 대화와 실제 압축/도구 왕복 확인이 이어져야 한다. 병합만으로 배포 완료를 선언하지 않는다.
