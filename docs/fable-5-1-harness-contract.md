# Fable 5.1 하네스 계약

카드: `t_b1fd6aa5` · 요청자: Wongeun Song · 검증일: 2026-09-13

## ① 요구사항과 적용 범위

- 원문: [Anthropic Fable 5.1 prompting guide](https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-fable-5-1).
- 선행 감사: [levos develop의 fable-5-1-harness-audit-2026-09.md](https://github.com/leve-team/levos/blob/develop/docs/operations/fable-5-1-harness-audit-2026-09.md), 카드 `t_f8d05c5d`.
- 감사 원문은 이 체크아웃에 없으며 levos 저장소 접근도 인증 실패했다. 따라서 감사의 `F:` 파일:줄 인용은 **미확인**이다. 아래 절 번호와 기존 판정은 이 카드에 전재된 내용을 기준으로 한다. 공식 가이드는 `.md` 제공본으로 직접 확인했다.
- 대상 포크의 기본 브랜치는 `git ls-remote --symref origin HEAD`로 확인한 `main`이다. 작업 기준은 `e629c900a87622ddcc31f67a4b4a756b239fbaf0`이다. 카드의 `0.20.5`와 달리 이 기준의 `pyproject.toml`은 이미 `0.21.0`이다. 버전 파일은 변경하지 않았다.
- 루트 `AGENTS.md`를 확인했다. 요청된 `docs/quality/rule-index.md`는 존재하지 않아 해당 인덱스의 규칙 ID는 **미확인**이다. 확인 가능한 지침인 최소 변경, 비-Fable 경로 보존, 프롬프트 캐시 안정성, 실제 import/임시 `HERMES_HOME`, 동작 계약 테스트, 표준 테스트 러너를 적용했다. 소스 문자열 검색으로 통과시키는 테스트는 추가하지 않았다.
- UI, 설정값(`threshold`, `protect_first_n`, `protect_last_n` 등), 모델 버전, 코어 도구 스키마는 변경하지 않는다. `thinking.display`도 변경하지 않는다.

## ② 설계 대안과 선택 근거

1. 기존 `_prune_stale_reasoning_replay`만 테스트하는 방식은 선택하지 않았다. 이 함수는 Codex의 `codex_reasoning_items`를 처리하며 Anthropic의 서명된 thinking을 제거하지 않는다.
2. 모든 턴에서 thinking을 무조건 삭제하는 방식도 선택하지 않았다. 컴팩션 이후 새로 생성된 블록까지 지우면 정상 append-only replay가 깨진다.
3. **성공한 Fable 배치 컴팩션에서만 새 체크포인트를 만든다.** 기존 보호 구간과 요약 생성 구조는 유지한다. 보호된 head/tail의 사용자 발화, assistant의 답변, 도구 호출 인자·결과는 요약의 참조 구역으로 옮긴다. 이전 assistant 턴과 reasoning sidecar 자체는 새 요청에 넣지 않는다. 진행 중인 사용자 요청은 요약 종료 경계 뒤에 둔다.
4. 요약과 새 요청은 논리적으로 분리하되 하나의 `user` 메시지 안에서 종료 마커로 구분한다. 연속 `user` 역할이나 가짜 assistant 메시지를 만들지 않는다. 아직 답하지 않은 이미지 전용 요청도 이 경계 뒤에 유지하며, 이미 답변한 요청은 다시 활성화하지 않는다.
5. 정상 Fable 턴에서는 과거의 서명된 thinking을 유지한다. 현재 체크아웃은 변환기가 `agent/anthropic_message_convert.py`로 추출되어 있어, 해당 파일의 Fable 조건도 최소 수정했다. 비-Fable 모델의 기존 서명 처리 분기는 유지한다.

## ③ 가이드 / 구현 / 실측 테스트

테스트 파일은 `tests/test_fable_harness_contract.py`이다.

| 감사 항목 | 가이드 요구 | 포크 구현 | 테스트와 판정 |
| --- | --- | --- | --- |
| §1 진행 업데이트 | 클라이언트가 비어 있지 않은 thinking을 수신하려면 `display`를 요청하고 UI가 표시해야 한다. | `build_anthropic_kwargs`의 기존 `thinking.display="summarized"`를 유지한다. `updates`나 beta 헤더를 새로 설정하지 않는다. | `test_thinking_display_remains_summarized`: Fable 5/5.1 및 provider-prefix 표기의 SDK 요청값 고정 **충족**. UI 표시 여부는 **미확인**, 감사의 UI 미충족 판정은 닫지 않는다. |
| §3 컴팩션 이후 이력 | 과거 prefix를 교체하면서 그 prefix에 바인딩된 thinking을 재전송하지 않는다. 클라이언트 컴팩션은 요약과 새 사용자 턴으로 시작할 수 있다. | `_rebase_fable_compaction`이 한 user 체크포인트를 조립한다. 보호된 내용은 참조 데이터이며 기존 assistant/tool 턴으로 replay하지 않는다. Fable에서는 system에 컴팩션 안내문을 덧붙이지 않는다. | `test_compaction_replays_no_old_thinking`, `test_compaction_keeps_system_and_tools_identical`, `test_compaction_has_one_user_summary_then_live_request`: 실제 SDK HTTP JSON에서 thinking 미재전송, system/tools 동일성, 단일 user 요약과 종료 경계 이후의 요청을 확인하여 **측정 경로에서 충족**. |
| §3 정상 후속 턴 | 새로운 assistant 응답을 추가할 때 이전에 보낸 대화 prefix를 편집하지 않는다. | `_manage_thinking_signatures`가 Fable의 이전 assistant 서명 블록을 단지 최신 턴이 아니라는 이유로 제거하지 않는다. 새 체크포인트에서 생성된 thinking은 보존한다. | `test_new_thinking_survives_and_requests_append_after_compaction`: 연속 요청의 messages prefix가 동일하고 새 서명이 살아 있음을 확인하여 **측정 경로에서 충족**. |
| §6 요약 보존 | 문제/해소, 대안/시도/기각과 이유, 정확한 요청/결정/합의/기각/선호/제약/경계, 현재 위치, 미해결/약속/다음 예정, 재구성 어려운 정확한 세부를 보존한다. 사용자 발화에 더 큰 비중을 둔다. | `_FABLE_SUMMARY_PRESERVATION`을 Fable 메인 모델의 `_generate_summary` 공통 preamble에 넣는다. 최초·반복 요약과 lean·legacy 모두 적용한다. 기존 템플릿, verbatim user section, Anchor Index는 유지한다. 비밀정보 redaction 및 무사용자 출처 규칙이 우선한다. | `test_summary_request_preserves_six_items_and_voice_weighting`: 실제 요약 호출 인자에서 6항목과 발화 가중 지시를 확인하여 **지시문 계약 충족**. 특정 LLM 출력이 항상 모든 사실을 보존한다는 판정은 하지 않는다. |

### 측정한 최종 요청 형태

```text
system: 컴팩션 전과 동일
tools:  컴팩션 전과 동일
messages:
  - role: user
    content:
      - CONTEXT COMPACTION 안내 + 기존 구조의 요약
      - Preserved Turns: 보호된 과거 내용 (참조 데이터)
      - END OF CONTEXT SUMMARY
      - 아직 답하지 않은 사용자 요청 + 해당 첨부물 (있는 경우)
  # 이후의 새 assistant/user 턴은 이 prefix 뒤에 추가
```

`reasoning`, `reasoning_content`, `reasoning_details`,
`anthropic_content_blocks`는 체크포인트 메시지에 복사하지 않는다.
native content의 `thinking`/`redacted_thinking`도 제거한다. 도구 호출·결과는
참조 데이터로 남기며 typed `tool_use`/`tool_result`로 재전송하지 않는다.
참조 텍스트와 도구 인자는 기존 strict redactor를 거친다.

### 실측의 경계

실제 `ContextCompressor.compress` → `AnthropicTransport.build_kwargs` →
`build_anthropic_kwargs` → `create_anthropic_message` → Anthropic SDK를 호출한다.
SDK의 `httpx.MockTransport`에서 **직렬화가 끝난 HTTP body**를 캡처한다.
요약 LLM 응답만 결정적으로 대체하며 compressor/adapter/serializer는 대체하지 않는다.
테스트 환경은 기존 `tests/conftest.py`의 임시 `HERMES_HOME`을 사용한다.

이 검증은 라이브 Anthropic 서버의 서명 검증, SSE/UI 렌더링, 전체 `AIAgent`
루프의 모든 알림/캐시 마커 변환, opt-in micro-compaction의 전체 수명주기를
검증하지 않는다. 해당 경로의 포괄적 append-only 준수는 **미확인**이다.
서버의 `input_transformations` 로그를 수집했다거나 실제 계정에서 400이
사라졌다고 주장하지 않는다. §3 판정은 위 배치 컴팩션 및 SDK 요청 경계에 한정한다.

## ④ 자기 리뷰와 반례

- 새 테스트를 수정 전 코드에 실행하여 8 failed / 4 passed를 재현했다. 누락된 지시문, 과거 thinking 재전송, system 변경, assistant 역할의 요약, 정상 후속 요청의 thinking 삭제가 각각 드러났다.
- `ordered`, `reasoning_details`, native content의 replay 형식 및 `redacted_thinking`을 검사한다. 입력 메시지도 deep copy와 비교하여 변환 중 원본 변경을 잡는다.
- 짧아서 컴팩션하지 않는 대화에서는 thinking을 유지하고, 완료된 대화의 요청은 다시 활성화하지 않는다.
- 도구 작업 도중 컴팩션하더라도 호출 인자와 완료된 결과를 참조 데이터로 보존한다. 본문 native tool 블록이 user 요청에 그대로 새어 나가지 않는지도 검사한다.
- 이미지 전용 user 턴이 참조 구역에 묻히는 반례를 발견하고 Fable 경로에서 수정했다. 텍스트가 있는 이미지와 없는 이미지 모두 종료 경계 뒤에 유지된다.
- 합성 이미지 알림을 새 요청으로 보거나, 알림 때문에 과거의 완료된 요청을 다시 활성화하는 반례도 발견했다. Fable의 미디어 포함 판별에서는 기존 `_SYNTHETIC_USER_FLAGS`를 재사용하여 알림을 제외한 뒤 완료 여부를 판정한다. 이미 체크포인트에 병합한 실제 미완료 요청은 유지한다.
- 반복 컴팩션은 이전 보호 구간을 다음 요약 입력으로 전달한다. 최신 요청과 우연히 본문이 같은 이전 user/assistant 내용은 삭제하지 않는다.
- redactor가 반드시 `[REDACTED]`라는 문자열을 출력한다는 잘못된 테스트 가정을 발견했다. raw secret 부재와 기존 strict redactor 결과와의 일치라는 동작 계약으로 바로잡았다.
- 비-Fable 요약 지시와 기존 컴팩션 replay 형태를 대조한다. 비-Fable 기존 테스트도 함께 실행한다.

## 재현 명령과 검증 기록

README의 표준 명령은 `scripts/run_tests.sh`이다. 이 워커에는 로컬 venv가 없어
러너가 지원하는 `HERMES_PYTHON`으로 준비된 외부 환경을 지정했다.
부족했던 `pytest==9.1.1`, `pytest-asyncio==1.3.0`, `anthropic==0.87.0`은
`pyproject.toml`의 핀과 일치하게 그 환경에 설치했다. 저장소 의존성 파일은 변경하지 않았다.

```sh
HERMES_PYTHON=/opt/levos-prebake/venv-hermes/bin/python \
  scripts/run_tests.sh tests/test_fable_harness_contract.py -q --file-retries 0

HERMES_PYTHON=/opt/levos-prebake/venv-hermes/bin/python \
  scripts/run_tests.sh tests/test_fable_harness_contract.py \
  $(rg --files tests | rg 'compress.*\.py$') \
  tests/agent/test_anthropic_adapter.py \
  tests/agent/test_anthropic_thinking_block_order.py \
  tests/agent/test_anthropic_thinking_disable.py -q -j 4

/opt/levos-prebake/venv-quality/bin/ruff check \
  agent/anthropic_adapter.py agent/anthropic_message_convert.py \
  agent/context_compressor.py tests/test_fable_harness_contract.py

git diff --check
git diff --check origin/main...HEAD
python3 "$LEVOS_WORKER_QUALITY_TOOL"
```

- 계약 단독 최종: **26 passed, 0 failed**, 종료코드 0 (`/tmp/fable-scaffolding-fixed.log`).
- 관련 테스트 최종 실행: **107 files, 1,070 passed, 0 failed**, 종료코드 0, 85.5초 (`/tmp/fable-compression-suite-reviewed.log`). 계약 테스트를 포함한 수치이다. 중간 실행도 각각 1,065 및 1,069 passed / 0 failed였다.
- 변경 Python 파일의 저장소 설정 ruff 검사: **신규 위반 0**, 종료코드 0. 확인한 강제 규칙은 `PLW1514`, `ASYNC210`, `ASYNC220`, `ASYNC221`, `ASYNC251`이다. baseline/ignore/skip을 추가하지 않았다.
- 작업 중 실패 기록: 최초 패치 실행기가 PATH에 없어 테스트 파일 생성 전 러너가 `No test files to run`으로 종료했다. 설치된 패치 실행기를 찾아 해결했으며 이 실행을 테스트 성공으로 집계하지 않았다. 이후 수정 전 8개 실패, 이미지 전용 요청 1개 실패, redactor 출력에 대한 잘못된 기대 1개 실패, 합성 알림 반례 1개 실패를 각각 수정·재검증했다. 합성 알림 수정 중 import 위치 오류로 2개가 실패한 실행도 있었으며 import 수정 및 완료 판별 보강 후 위 26개가 전부 통과했다. 실패 테스트 삭제나 skip은 없었다.
- 카드의 정적 `levos-verify` 항목 및 추가 working-tree 버전/공백 검사: 모두 종료코드 0. `origin/main...HEAD`뿐 아니라 미커밋 변경도 검사했다.
- `python3 "$LEVOS_WORKER_QUALITY_TOOL"` 실제 실행: 최종 코드 수정 후 재실행을 포함해 **2회 모두 종료코드 127**, `status=error`, `ok=false`, `FileNotFoundError`. 도구가 실행하려는 `./scripts/check`가 working tree와 `origin/main` 모두에 없다. 따라서 내부 전체 검사는 **미실행**, 전체 품질 성공/신규 위반 0 여부는 **미확인**이다. 코드 실패가 아니라 도구 결손이며 대체 게이트를 만들거나 결과를 성공으로 바꾸지 않았다.
- 자동 기록: `LEVOS_WORKER_QUALITY_RESULT`가 가리키는 `.git/worker-quality.json` 및 `.git/worker-quality.log`. 이 파일은 지정된 도구가 작성했으며 직접 수정하지 않았다.
- 인계 조건: 포크에 맞는 정식 `scripts/check` 및 품질 규칙/감사 문서 접근을 복구한 후 동일 worker 명령을 다시 실행해야 한다. **전체 게이트 성공 전 병합 불가**. 관련 테스트 및 ruff 성공을 전체 게이트 성공으로 대체하지 않는다.
