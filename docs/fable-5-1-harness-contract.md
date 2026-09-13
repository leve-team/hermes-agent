# Fable 5.1 하네스 계약 — levos/pg3

카드: `t_666391d3` · 요청자: Wongeun Song <eren.song@withleve.com>
등록 세션: `20260912_135241_ce840a` · 검증일: 2026-09-13

## ① 요구사항과 제약

- 대상: `leve-team/hermes-agent`, 빌드 기준 브랜치 **`levos/pg3`**.
- 이식 기준 revision: `3b82e3e657fd9cba938868f66e6293563a46e44d`,
  `pyproject.toml` 버전 **0.20.5**. 작업 브랜치는 `feature/t_666391d3`이다.
- 원본: 카드 `t_b1fd6aa5`, 커밋 `2c5df9043165ad32179c25635144a8406673d611`
  (작성자 `eks-worker`, main 기준 `e629c900a87622ddcc31f67a4b4a756b239fbaf0`, 0.21.0).
  원본 계약 문자열과 테스트를 재사용했다. main의 커밋들을 병합하지 않았다.
- 이 문서의 §3·§6은 카드에 전재된 계약을 가리킨다. 외부 Fable 가이드나
  선행 감사 문서를 이번 작업에서 독립적으로 재검증했다고 주장하지 않는다.
- 루트 `AGENTS.md`를 읽었다. `docs/quality/rule-index.md`는 main 기준과 pg3
  모두에 없어 규칙 ID는 **미확인**이다. 확인 가능한 캐시 안정성, 최소 변경,
  행동 계약 테스트, 실제 import 및 임시 `HERMES_HOME` 격리 지침을 적용했다.
- 버전, `thinking.display`, 설정 기본값, 도구 스키마, PG/state 및 gateway 파일은
  변경하지 않는다. 커밋·push·merge·이미지 빌드는 워커 작업에 포함하지 않는다.

## ② 설계 대안과 선택 근거

1. main 커밋 전체 cherry-pick은 선택하지 않았다. pg3에는 추출 모듈
   `agent/anthropic_message_convert.py`가 없으므로 기존 adapter 안에 이식한다.
2. 모든 모델의 thinking을 항상 유지하거나 삭제하지 않는다. 정상 Fable 턴의
   서명 유지와 Fable 배치 컴팩션의 새 체크포인트 생성만 분리해서 적용한다.
   기존 third-party/Kimi/DeepSeek 서명 처리 예외도 그대로 유지한다.
3. 보호 head/tail을 삭제하지 않고 단일 user 체크포인트의 참조 자료로 바꾼다.
   과거 assistant/tool 역할과 reasoning sidecar는 재전송하지 않는다.
   진행 중인 user 요청만 종료 마커 뒤에 두며 이미지 전용 요청도 포함한다.
4. pg3에 이미 있는 `_SYNTHETIC_USER_FLAGS`와 `_is_real_user_message`를 재사용한다.
   없는 진행 요청 판별과 replay 마커만 `context_compressor.py`에 추가했다.
5. **지정 네 파일 외 최소 호환 수정 한 곳이 필요했다.** 실제 `compress_context`
   경로를 검사하니 pg3의 `_ensure_compressed_has_user_turn`이 새 체크포인트 뒤에
   user 요청을 다시 추가했다. 진행 요청은 중복되고 완료 요청은 재활성화됐다.
   `agent/conversation_compression.py`에서 새 체크포인트의 명시적인 replay 결정을
   존중하도록 가드를 추가했다. 이 가드가 없는 구현은 SDK 경계 이전부터 연속
   user 행을 만들었다. 관련 없는 main의 anchor 복원 기능은 가져오지 않았다.

## ③ 구현과 측정 경계

아래 파일:줄은 이 pg3 이식본 기준이다.

| 계약 | 구현 위치 | 행동 검증 |
| --- | --- | --- |
| §3 정상 턴 thinking 유지 | `agent/anthropic_adapter.py:1772`, `agent/anthropic_adapter.py:2618` | `claude-fable-` 접두 판별, provider 접두·Bedrock profile 정규화, 연속 HTTP 요청의 messages prefix 및 signed/redacted thinking 유지 |
| §6 요약 6항목 보존 | `agent/context_compressor.py:50`, `agent/context_compressor.py:4708` | 원 커밋 문자열과 정확히 동일. Fable 메인 모델에서만 공통 preamble에 추가. lean/legacy, 최초/반복 요약, focus topic을 포함한 실제 요약 호출 인자 검사 |
| §3 컴팩션 체크포인트 | `agent/context_compressor.py:7076`, `agent/context_compressor.py:7126`, `agent/context_compressor.py:7975` | 성공한 배치 컴팩션의 system/tools 동일성, 단일 user carrier, 과거 thinking/tool 블록 부재, 보호 참조와 live 요청 경계 검사 |
| pg3 상위 호출 호환 | `agent/conversation_compression.py:2126` | 실제 `compress_context` → compressor → transport → Anthropic SDK HTTP JSON까지 검사. 진행 요청 중복과 완료 요청 재활성화 방지 |
| 기존 display 고정 | `tests/test_fable_harness_contract.py:124` | HTTP 요청의 `thinking.display == "summarized"` 확인. 설정이나 UI는 변경하지 않음 |

요약 6항목은 문제와 해결, 대안과 채택/기각 이유, 정확한 요청·결정·선호·경계,
현재 상태, 미해결·약속·다음 예정, 재구성 어려운 정확한 세부이다. 사용자 발화는
원문에 가깝게, assistant 설명은 결론 위주로 압축한다. 비밀정보 redaction과
사용자 발화가 없는 세션의 provenance 규칙은 여전히 우선한다.

`_INFLIGHT_REPLAY_MERGED_KEY`는 새 Fable carrier에 명시적으로 기록한다.
`True`는 진행 요청을 경계 뒤에 병합했다는 뜻이고, `False`는 재개할 요청이
없다는 뜻이다. 상위 anchor 복원은 이 **결정의 존재**를 확인한다. 일반 모델의
기존 carrier에는 이 키를 추가하지 않으므로 기존 anchor 동작이 유지된다.

`tests/test_fable_harness_contract.py`는 원본 26케이스를 전부 유지한다.
pg3 API로도 성립하므로 삭제·skip 없이 추가 반례를 포함해 **39케이스**를 실행한다.
HTTP는 `httpx.MockTransport`가 기록하되 실제 `AnthropicTransport`, adapter,
Anthropic SDK의 직렬화를 통과한다. 외부 모델 호출이나 유료 API는 필요 없다.
요약 LLM만 fixture 응답으로 대체한다. 따라서 요약 **지시문 계약**을 검증하며,
실제 모델이 언제나 여섯 항목을 완벽하게 보존한다는 품질 보장은 하지 않는다.

## ④ 자기 리뷰와 검증 기록

- 수정 전 원본 테스트: **20 failed, 6 passed**. 원 계약이 pg3에서 깨짐을 재현했다.
- 첫 이식에서 누락된 `copy` import로 **16 failed, 10 passed**가 발생했다.
  import 추가 후 원본 **26 passed**를 확인했다.
- 추가 공격 테스트에서 native pending `tool_use`를 완료 답변으로 오인하는 1건과
  상위 user anchor 중복/재활성화 2건을 재현했다: **3 failed, 32 passed**.
  세 반례 수정 후 **35 passed**, 비-Fable 요약 대조 확장 후 **39 passed**.
- 회귀: 계약을 포함한 **12 files, 403 passed, 0 failed**, 종료코드 0.
  non-Fable adapter, Kimi, thinking 순서/비활성화, 기존 compressor, historical media,
  fallback, zero-user provenance, concurrent fork, trajectory compressor를 포함한다.
- 뮤테이션: Fable thinking 보존 분기 되돌림 → **5 failed**;
  6항목 preamble 제거 → **4 failed**; pg3 상위 anchor 가드 제거 → **2 failed**.
  각 실행의 종료코드는 1이며 예상된 반례이다. 뮤테이션은 모두 복원한다.
- 변경 Python 파일의 저장소 ruff 규칙 `PLW1514` 검사와 `git diff --check`는
  통과했다. 검사 제외·baseline·noqa·skip을 추가하지 않았다.

재현 명령 (`.venv`에는 pyproject 핀과 같은 `pytest==9.1.1`,
`pytest-asyncio==1.3.0`, `anthropic==0.87.0`, `openai==2.24.0` 등을 설치):

```sh
scripts/run_tests.sh tests/test_fable_harness_contract.py \
  tests/agent/test_anthropic_adapter.py \
  tests/agent/test_anthropic_kimi_signed_thinking_replay.py \
  tests/test_trajectory_compressor.py \
  tests/agent/test_context_compressor.py \
  tests/agent/test_compressed_summary_metadata.py \
  tests/agent/test_compressor_historical_media.py \
  tests/agent/test_compression_fallback_budget.py \
  tests/agent/test_anthropic_thinking_block_order.py \
  tests/agent/test_anthropic_thinking_disable.py \
  tests/agent/test_compression_concurrent_fork.py \
  tests/agent/test_context_compressor_zero_user_provenance.py \
  -q -j 2 --file-retries 0

.venv/bin/ruff check agent/anthropic_adapter.py agent/context_compressor.py \
  agent/conversation_compression.py tests/test_fable_harness_contract.py
git diff --check
git merge-base --is-ancestor origin/levos/pg3 HEAD
python3 "$LEVOS_WORKER_QUALITY_TOOL"
```

검증 환경의 한계:

- 카드의 pytest 명령도 그대로 실행했지만 **종료코드 4, no tests ran**이었다.
  `tests/test_compaction_prompt_rebuild.py`와 `tests/test_compaction_tool_refresh.py`는
  pg3에 없다. 가짜 테스트를 만들거나 main 파일을 가져오지 않고, 위 pg3 회귀와
  실제 상위 compaction 경계 테스트로 검증했다. 최초 로컬 실행의 `fire` 의존성
  누락은 venv에 저장소 핀을 설치한 뒤 재실행하여 해결했다.
- `./scripts/check`도 pg3에 없다. 최종 worker 도구는 지정 명령으로 실행하며
  `.git/worker-quality.json` 및 `.git/worker-quality.log`를 도구가 직접 기록한다.
  전체 품질 게이트의 신규 위반 0 여부는 **미확인**이다. 대체 검사 성공을 전체
  게이트 성공으로 간주하지 않는다. 정식 검사 경로 복구 후 같은 명령으로
  성공을 확인하기 전에는 병합하지 않는다.
- 실행 로그: `/tmp/t_666391d3-final-regression.log`,
  `/tmp/t_666391d3-requested-command.log`,
  `/tmp/t_666391d3-mutation-thinking.log`, `/tmp/t_666391d3-mutation-summary.log`,
  `/tmp/t_666391d3-mutation-anchor.log`.
