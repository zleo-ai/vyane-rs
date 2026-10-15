# Vyane

**하나의 Rust CLI로 코딩 에이전트와 모델 API를 실행하세요.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

여러 코딩 에이전트나 모델 제공자를 사용하는 개발자를 위한 도구입니다. 실행 대상에 이름을 붙이고, 하나 또는 여러 대상에 작업을 보내고, 결과와 시도 기록을 함께 보관할 수 있습니다. 코딩 작업에는 Claude Code나 Codex CLI를 사용하고, 텍스트 응답은 모델 API를 직접 호출해 받을 수 있습니다.

> **개발 초기 단계입니다.** 현재 소스 설치만 제공하며 API가 불안정합니다. 일부 기능은 부분 구현 상태이거나 없습니다. 네이티브 세션 재개와 재시작 후 자동 재실행은 미완성입니다. [현재 제약](docs/PROJECT-STATUS.md)을 확인하세요.

## 실행 모습 확인

[터미널 데모 실행](docs/EXAMPLES.md): 설치한 CLI가 단일 작업을 실행하고, 두 대상에 동시에 요청한 뒤, 의도적으로 발생시킨 기본 대상 오류에서 복구합니다. 로컬 HTTP 테스트 응답을 사용하므로 키가 필요 없고 실제 모델을 호출하지 않습니다.

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## 세 가지 활용법

| 원하는 작업 | 명령 | 결과 |
| --- | --- | --- |
| 선택한 에이전트나 모델로 작업 실행 | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | 결과 하나와 로컬 실행 기록. 코딩 하네스는 별도 설치와 설정이 필요합니다. |
| 같은 프롬프트의 답변 비교 | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | 동시 실행 후 개별 결과 반환. 답변 평가는 사용자가 합니다. |
| 호출 실패 시 설정한 대체 대상 시도 | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | 허용된 오류에 대해 순서대로 대체 대상을 시도하고 각 시도를 기록합니다. 세션 인계는 아닙니다. |

## 빠른 시작

**1. 소스에서 설치합니다.** stable Rust/Cargo(최소 Rust 1.88)와 C/C++ 빌드 도구가 필요합니다. CI 플랫폼은 Linux와 macOS이며 Windows 지원은 보장하지 않습니다.

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

예상 버전은 `vyane 0.1.0`입니다. 바이너리는 현재 저장소 안에 설치되므로 PATH의 기존 `vyane`을 덮어쓰지 않습니다. 이 셸을 계속 열어 두세요.

**2. 키 없이 첫 결과를 확인합니다.** 포함된 예제는 라우팅을 미리 보기만 하며 모델 요청을 보내지 않습니다.

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

`offline` 프로필과 JSON의 `"selection_key": "offline"`을 확인하세요. 엔드포인트는 의도적으로 유효하지 않으므로 작업을 보내지 마세요.

**3. 자신의 실행 대상을 연결합니다.** [제공자 설정](docs/QUICKSTART.md#configure-a-real-target)을 마친 뒤 [세 가지 예제](docs/EXAMPLES.md)를 실행하세요. 모델 접근 권한과 자격 증명은 직접 제공해야 하며 실제 호출에는 제공자 요금이 발생할 수 있습니다. 오프라인 미리보기는 실제 제공자의 작동을 검증하지 않습니다.

## 현재 가능한 기능

| 소스에서 사용할 수 있는 기능 | 제약 |
| --- | --- |
| 단일 실행, 다중 대상 요청, 명시적 장애 전환, 스트리밍 | 직접 HTTP 호출은 모델 텍스트를 반환하며 로컬 파일을 첨부하거나 셸 도구를 제공하지 않습니다. |
| Claude Code와 Codex CLI 어댑터 | CLI 하위 프로세스 방식이며 Codex App Server RPC나 Claude Agent SDK 직접 통합은 없습니다. CLI와 접근 권한은 직접 준비하고 샌드박스 적용은 하네스에 위임합니다. |
| 프로필 기반 라우팅과 로컬 실행 기록 | 라우팅은 설정에 따르며 답변 품질을 보장하지 않습니다. |
| CLI, REST, MCP 진입점 | 지원하는 작업은 진입점마다 다릅니다. [CLI 가이드](docs/CLI-GUIDE.md)를 참고하세요. |

상주 실행, 영속 워크플로와 로컬 목표 관리는 계속 개발 중입니다. 네이티브 실행은 매번 새로 시작하며 세션을 유지하지 않습니다. 네이티브 승인·세션 재개와 재시작 후 자동 재실행은 아직 지원하지 않습니다. owner 필드와 루프백 인증은 악의적인 동일 사용자 또는 다중 사용자 접근을 격리하지 않습니다. 이 기능으로 개발하기 전에 [상태와 제한](docs/PROJECT-STATUS.md)을 읽어 주세요.

## 더 알아보기

- [아키텍처와 대상 모델](docs/ARCHITECTURE.md) · [설정 참조](profiles.example.toml) · [CLI, REST, MCP 가이드](docs/CLI-GUIDE.md)
- [기능 매트릭스와 재현 가능한 증거](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [로드맵](docs/ROADMAP.md)
- [변경 이력](docs/CHANGELOG.md) · [버전과 릴리스 정책](docs/RELEASING.md)

<details>
<summary>검증 현황</summary>

[공개 기능 매트릭스](docs/parity/ORIGINAL-VYANE-PARITY.md)는 8개 영역의 53개 기능을 추적합니다. 구현 완료 7개, 부분 구현 23개, 미구현 12개, 의도적 차이 또는 결정 대기 9개, 계획 단계 2개입니다. [민감 정보를 제거한 매니페스트](docs/parity/fixtures/v1/manifest.json)에는 5개 모음의 44개 사례가 있으며 정규화 후 일치 30개, 미해결 차이 14개입니다. 제한된 범위의 검사이며 전체 시스템의 동등성 비율이 아닙니다.

</details>

기존 비공개 Python Vyane 시스템의 기능을 Rust로 공개 구현한 프로젝트입니다. 그대로 대체할 수는 없으며 명령, 설정, 저장된 세션과 워크플로를 각각 검토한 후 이전해야 합니다. 문서화된 이전 절차 없이 상태 디렉터리를 공유하지 마세요. 공개 프로젝트에 기여할 때 비공개 소스 접근은 필요하지 않습니다.

## 기여와 도움 요청

예제를 실행하거나, 재현 가능한 문제를 보고하거나, 번역을 개선하거나, 문서에 기록된 기능 부족을 해결할 수 있습니다. [Issues](https://github.com/zleo-ai/vyane-rs/issues)와 [열린 PR](https://github.com/zleo-ai/vyane-rs/pulls)을 확인하고 [CONTRIBUTING.md](CONTRIBUTING.md)를 읽어 주세요. 소스 리비전과 민감 정보를 제거한 출력을 포함하되 키, 비공개 프롬프트, 개인 경로는 제외하세요. 취약점은 [SECURITY.md](SECURITY.md)에 따라 보고해 주세요.

## 라이선스

[MIT](LICENSE-MIT) 또는 [Apache-2.0](LICENSE-APACHE) 중 선택할 수 있습니다. 기여에도 같은 조건이 적용됩니다.

---

소스 기준: `fe43941` · 워크스페이스: `0.1.0` · 갱신: 2026-10-15. 영어판이 기준입니다. 이 번역은 기계 보조 초안이며 원어민의 검토를 기다리고 있습니다. [번역 범위와 검토 상태](docs/TRANSLATING.md)를 참고하세요. 로드맵의 이정표는 공개된 릴리스를 뜻하지 않습니다.
