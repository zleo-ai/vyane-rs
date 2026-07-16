# Original Vyane parity baseline

状态：可执行基线

比较日期：2026-07-13

原始 Vyane 参考基线：2026-07-11 完成的一次受控私有基线审计。为保护公开边界，本文不披露
私有 commit、源码路径、内部代号或部署标识。

Rust 基线：当前公开集成树

## 1. 这份基线回答什么

这份文档回答的是“`vyane-rs` 与上述固定版本的原始 Vyane 在产品能力上对齐到哪里”，不是
“Rust workspace 是否能编译、测试或发布”。结论先写清楚：

- `vyane-rs` 已形成可独立使用的公开 Rust 执行内核，并在 dispatch、broadcast、failover、
  direct-HTTP transcript session、revisioned session storage、run ledger、声明式 workflow、owner-qualified durable task
  和 resident workflow daemon 上形成了完整闭环。`NativeSessionDomain` strict V2/CAS 数据契约与
  `FsSessionStore` 本地执行期 lease 已落地，但 legacy-unbound 与 domain-bound native harness
  session 都仍会 fail closed，不能把“session control plane”扩写成生产 native continuity。新的
  live authority contract、每次 wire send 的 OpenAI Chat guard 和 allowed-tool registry guard 已增加
  fresh-sessionless permit/store concrete bridge 与有界串行 dark turn driver，但 bridge/driver 都没有
  生产 factory/runtime 接线，也没有 session/checkpoint/trusted-tool 完整 authority；不能把这条局部
  side-effect chain 扩写成 native harness。
- 它**没有**对齐参考系统的整个 AI OS 执行与协作产品面。native harness、AgentRun 生产
  product-host supervision、concrete product operation 与 Process/Remote controller adapter、session-aware recovery、A2A/Channels 接线、广义 worker daemon、goal continuity、Git-backed board/event ledger、
  channel/UI adapter、完整 review/observability 等仍有大量 `partial`、`missing` 或刻意
  `different` 的能力。
- 原仓自身也不是完成态，不能把原仓目录或历史 roadmap 的每个名词都直接变成 Rust 待办。
  原仓仍有 legacy/enforce 双路径、god files、数据模型多事实源、provider/adapter 混层和若干
  仅灰度或草稿能力。

**whole-system capability parity** 是持续演进目标，但不构成复制私有数据、跳过差异决策或执行
crates.io 等外部分发动作的授权。任何单仓 README milestone、测试全绿或包发布都不能替代
这张矩阵。

本文区分两种范围：

- **public-core parity**：公开 Rust 核心及其通用接口在明确范围内对齐；可以明确排除某些
  private-only 部署，但必须把范围写出来。
- **whole-system capability parity**：53 个能力项的用户结果都得到处理；私有实现、身份和配置
  不进入公开仓，但对应 generic contract 与 optional/private adapter 必须可验证。

未加限定的“完全对齐”或 “full parity” 一律指 **whole-system capability parity**，不能靠把
private-only 项从统计中移除而宣称完成。

## 2. 状态、目标层和波次

### 2.1 状态定义

下列状态表示 **Rust 基线自身的实现状态**，不是跨仓行为等价结论。当前 53 项计数为：
`implemented` 7、`partial` 22、`missing` 13、`different` 9、`planned` 2。

| 状态 | 含义 |
| --- | --- |
| `implemented` | Rust 基线已有主要可运行契约，并有本仓 hermetic acceptance 覆盖；仍需矩阵所列跨仓验收才能宣称行为等价。 |
| `partial` | Rust 只覆盖原仓能力的一部分，或只覆盖一个入口/持久化层/执行层。 |
| `missing` | 原仓基线有可运行实现，Rust workspace 与完整 CLI surface 中没有对应能力。 |
| `different` | 两边都有能力，但输入模型、生命周期或产品语义不同；不能用“都有同名命令”视为对齐。 |
| `planned` | Rust 文档明确列为未来工作，但固定基线尚无实现。 |

`different` 不天然是缺陷。若差异是 Rust 产品有意选择，必须用 ADR/范围决策明确接受，并以
等价用户结果或迁移路径验收；否则它仍是 parity blocker。

### 2.2 目标层

| 目标层 | 处理原则 |
| --- | --- |
| `public-core` | 通用执行/协作契约，适合进入公开 Rust 核心。 |
| `optional-adapter` | 通用接口进公开仓，具体平台适配可选装；不得夹带私有账号或路径。 |
| `private-only` | 任何私有实现、部署和身份语境都不进入公开核心；whole-system parity 仍要求公开 generic contract 与可验证的 optional/private adapter 边界。 |
| `decision` | 先决定是追求行为 parity、提供迁移层，还是明确保持 Rust 差异。 |

### 2.3 优先波次

| 波次 | 目标 |
| --- | --- |
| `P0` | 固定契约、消除错误完成声明、建立跨仓 golden/shadow 验证和差异 ADR。 |
| `P1` | 补齐公开执行核心的关键缺口：native harness、workflow/review 语义决策、核心接口 parity。 |
| `P2` | 补齐通用 continuity、collaboration、governance、quality、observability 基座。 |
| `P3` | 按需要抽象 platform-specific 集成；默认做 optional adapter，不污染 core。 |
| `—` | 当前无需新增实现；只维持回归或已决定不作为 parity 目标。 |

## 3. 证据规则

矩阵第五列只概括私有基线审计确认的参考能力或行为，刻意不公开私有 commit、源码路径、
符号名或部署上下文。第六列可以引用本公开仓的路径；对“缺失”的判断以 Rust workspace 成员
`Cargo.toml` 和完整 CLI enum `crates/vyane-cli/src/cli.rs#Command` 为边界，而不是仅凭 README
搜索不到。

每次更新状态必须同时满足：

1. 在受控审计记录中固定私有参考基线，并在本文更新公开 Rust commit 锚点；
2. 私有侧保留可复核审计证据，公开侧给出源码或可执行测试证据，不能只引用 roadmap checkbox；
3. 对 `implemented` 增加本仓 hermetic acceptance；真实账号 smoke 只能作为附加证据；
4. 跨仓 golden/shadow 是宣称行为 parity 的必要条件，但不是把本仓实现状态标为
   `implemented` 的前置条件；
5. 对 `different` 增加明确的接受差异 ADR、迁移说明或行为等价测试；
6. 不把任何 prompt、token、私有 endpoint、个人路径或账号配置复制进公开 fixture。

## 4. Parity 矩阵

### 4.1 Execution

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| EXE-01 | provider / protocol / harness / model 四层目标 | `implemented` | public-core | 四层 target 独立解析，failover leg 各自绑定 model，并支持禁用目标。 | `vyane-rs:crates/vyane-core/src/target.rs`; `vyane-rs:crates/vyane-config/src/resolve.rs` | 建立同一组中性 profile fixtures；两边解析后的四层字段、failover scoped model 和禁用目标结果一致。 | P0 |
| EXE-02 | dispatch / broadcast / failover | `implemented` | public-core | 提供单目标分发、多目标 fan-out 与按错误类别切换 fallback，并保留 attempt trail。 | `vyane-rs:crates/vyane-kernel/src/{dispatch,capability,broadcast}.rs`; `crates/vyane-kernel/tests/acceptance.rs`；kernel 现先分配 execution id、整链准入并保留原始 ordinal，再只执行 admitted targets | 对成功、timeout、auth、quota、不可用 harness、跨 provider failover 建 golden attempt trail；增加 `Write`/`Full` primary rejection、fallback filtering、无 admitted fallback 时原错误保留及 original ordinal fixtures；输出状态和 failover 判定一致，允许已记录的错误分类差异。 | P0 |
| EXE-03 | HTTP protocol clients 与流式事件 | `partial` | public-core | 覆盖 OpenAI Chat/Responses 风格 transport、named direct adapter、流式事件与 provider-specific quirk。 | `vyane-rs:crates/vyane-core/src/{tool_chat,traits,native_authority}.rs`; `crates/vyane-protocol/src/{openai_chat,openai_responses,anthropic_messages,wire,http}.rs`；三种基础协议已有，有界 typed tool-chat contract、8 MiB non-stream body cap 与 OpenAI Chat `complete_turn` 能保真 tool call/result、reasoning、refusal 并限制 envelope/JSON 资源；独立的 authorized OpenAI Chat 路径在每次显式 wire send 前重验 authority，覆盖 cancel/send/body/backoff，并关闭 redirect 与 reqwest 隐式 retry，使 Vyane 显式 loop 独占 attempt 编号。dark `NativeTurnDriver` 可有界串行调用该 authorized trait，但尚无生产 factory/runtime 接线；Anthropic/Responses typed turn、stream tool envelope 及原仓全部 provider quirks/named adapters 仍缺 | 用 fake HTTP servers 扩展 SSE、usage、reasoning、tool/event envelope、retry/error mapping 矩阵到每个协议；每个支持 provider 明确 protocol/quirk 所有者，并在生产 assembler 接线前保持 ordinary 与 authorized typed-turn 边界不可隐式互退。 | P1 |
| EXE-04 | named/custom/A2A provider adapter 生态 | `partial` | optional-adapter | 支持 named adapter、custom provider 与 remote agent 的注册和调用生命周期。 | `vyane-rs:crates/vyane-provider/src/lib.rs`; config 可描述 provider，但无原仓 custom CLI/A2A remote 插件生命周期 | 定义公开 adapter registration contract；至少一个 custom CLI 与一个 remote adapter 通过 config、capability probe、dispatch 和 failover E2E。 | P2 |
| EXE-05 | 智能路由 | `different` | decision | 路由综合 keyword、history、benchmark 与 feedback 等信号；workflow 的 automatic target 现可解析 provider/model/effort，并保留显式 provider、model 与 effort 的优先级。参考路径的 production frontier policy 与完整 model-tier 选择仍是分阶段能力，不能扩写成已完成自动三档模型选择。 | `vyane-rs:crates/vyane-router/src/*`; `crates/vyane-service/src/routing.rs`：deterministic intent/tag/tier/effort，`plan_dispatch` 保留 exact profile identity，在 frontier 关闭时按 profile provenance 过滤或 fail closed。WP-46 增加 closed typed workflow effort，并按 workflow explicit > selected-profile configured > decision-tier default 计算 effective effort；该值写入 canonical `routing.effort`、覆盖整条 failover chain，并由 recorded/detached/daemon replay surface 冻结。保留字段拒绝调用方伪造，普通 `effort` label 不具备 override 权限；`docs/adr/0001-deterministic-routing-core.md` 已接受 deterministic public-core default 与显式 owner-scoped signal provider 边界 | `docs/parity/fixtures/v1` 已固定 9 个 classifier case 与 10 个 automatic-routing case；automatic suite 对净化的 target/effort precedence、真实 template render 与无 eligible target 做 normalized comparison。Rust 缺少 typed automatic explicit-model override；full-chain frontier filtering、direct-leg ambiguity 与 frozen target-chain replay 在参考侧没有同形契约，均诚实保留为 scope/open difference。history/feedback 学习信号与 production model-tier policy 也仍需实现或明确迁移。 | P0 |
| EXE-06 | workflow 编排 | `different` | decision | 提供可编程 workflow frontend，覆盖 agent、parallel、pipeline、budget 与 replay；automatic target 会在 workflow 路径上产生具体 route，但 frontier/model policy 仍有明确的 staged 边界。 | `vyane-rs:crates/vyane-workflow/src/{model,plan,engine,template,journal}.rs`：TOML declarative DAG/fan-out 编译为严格、有界、filesystem-independent 的 `WorkflowPlan` schema v1；compile/prepare/run/resume 共用物化 DAG、typed targets/routes、无损 duration 与 exact plan digest。WP-58 增加 CLI/engine exact-plan replay/fork：terminal source 只读，新的 UUIDv7 journal 复用 dependency-closed、journal-recorded all-success 前缀并记录无正文 provenance；partial fan-out 不复用。plan payload 含 prompt/target/workdir，不是安全 public view；digest 只是 drift checksum，不是认证、provenance 或 authority；manifest 只是 requested pre-resolution summary。plan-only continuation 缺失或不匹配 digest 时 fail closed，source-bearing compatibility API 只能在 exact source hash 后迁移旧 journal。3-finder+1-synth 与 replay 本仓 acceptance 覆盖 JSON roundtrip、split prepare/run、exact resume、新 run id、source 不变与 live suffix。 | typed-plan 与 exact-plan replay 前置条件已关闭；继续实现 dynamic control flow、nested workflow、shared budget、changed-plan call matching、compatibility frontend 与 CLI/REST/MCP plan wire，并用净化的 automatic-route precedence、full-chain frontier guard、resume/replay 跨实现场景验证 migration。现有本仓验收不替代 sanitized cross-implementation fixtures，也不证明 production-complete model-tier policy。schema v1 的 `max_concurrency` 固定为 `u32`；重复 fan-out target 的语义尚未冻结。 | P1 |
| EXE-07 | native self-built harness | `partial` | public-core | 原生 harness 参考能力覆盖 model loop、工具/权限、checkpoint、session continuity 与分阶段 rollout。 | `vyane-rs:crates/vyane-core/src/{native_authority,traits,tool_chat}.rs`; `crates/vyane-agent/src/{model,store,sqlite}.rs`; `crates/vyane-harness/src/native/{tools,permissions,turn_driver}.rs`; `crates/vyane-protocol/src/{openai_chat,http}.rs`; `crates/vyane-service/src/{native_authority,agent_execution}.rs`：已有 bounded typed model envelope、validated tool registry、permission seam、early scope/admission、Linux pinned workdir、strict `NativeSessionDomain` storage 与本地 `SessionExecutionLease`。WP-41/42/43 提供 live authority、fresh-sessionless bridge 与严格串行 dark turn driver。WP-47 新增 fixed-owner、non-`Clone`、one-shot AgentRun execution driver，按 claim→start→permit→pre-effect heartbeat→first poll 推进，并要求 trusted executor 在每个 effect 重验 permit；仅 `Quiesced` proof 授权发起 settle。它仍不是 `Harness`：无 concrete executor、生产 factory/runtime、trusted built-ins、OS sandbox、checkpoint/session commit、approval resume 或 native resume。legacy-unbound/bound state 仍在 `make` 前拒绝，streaming 对任何 session 在 load/probe/`make` 前拒绝；当前 lease 仍是单机 fence，post-model commit 仍 best-effort | 在生产 assembler 中显式组合 fresh-sessionless bridge、turn driver、AgentRun execution seam 与 trusted built-ins，并强制只走 authorized entry；每个 tool 的实际 argument 和外部操作线性化点都须重验。再建立 session-aware authority 组合 exact domain/lease、checkpoint、revision-fenced commit 与 approval store，通过 drift/cancel/retry E2E 后才启用 native resume。非文件系统 store 另补 generation/TTL/stale-holder fencing；随后补 host sandbox、容错 tool-call、事件，以及 hooks、skills/MCP、compaction、task subagent、frozen memory。 | P1 |
| EXE-08 | orchestrate / collaborate / auto-decompose | `missing` | public-core | 提供自动分解、协作执行与多种 convergence pattern。 | Rust CLI 只有 dispatch/broadcast/workflow/review；无对应 service contract | 先收敛成一个通用 plan/run model，避免复制原仓多事实源；覆盖 DAG decomposition、review/consensus/debate 和取消/ledger 关联。 | P2 |
| EXE-09 | durable detached tasks 与 resident workflow execution | `different` | public-core | async dispatch 依赖进程内 task 与状态文件，另有更广义的 resident worker runtime。 | `vyane-rs:crates/vyane-task`; `crates/vyane-cli/src/{api,command,daemon,daemon_workflow,workflow_control,daemon_agent,agent_host,agent_process,agent_spool}.rs`; `crates/vyane-cli/src/task/{mod,proc,spawn,store}.rs`；CLI detached submission 在 task row/process 前完成 capability/session admission，冻结 secret-free plan，并在 Linux 把同一 pinned directory fd 交给 worker 复核。WP-61 另在 daemon 中生产 assembly fresh/sessionless CLI-harness AgentRun：private create-only spool 冻结 prompt/target/capability/workdir，exact Process sidecar 支持 recovery/cancel，completion 经 message store handback；restart 不 replay。Rust 在 metadata/CAS/controller recovery 上更强 | 保留 Rust 强契约；继续验证 client exit、exact cancel、restart no-replay、shutdown drain、payload 不泄漏与 identity drift fail closed。旧 `job.json` compatibility path 不得被描述成所有任务都有新 fd handoff；Process AgentRun 也不得扩写成 native/Remote/session host。 | — |

### 4.2 Continuity

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CON-01 | topic session、runtime binding 与对话续接 | `partial` | public-core | topic session 支持 create/list/history/message/bind/archive，并可绑定 runtime native session。 | `vyane-rs:crates/vyane-core/src/session.rs`; `crates/vyane-ledger/src/session.rs`; `crates/vyane-kernel/src/dispatch.rs` 已有 owner 物理 namespace、strict schema-2 revisioned snapshot、`Absent`/`LegacyUnbound`/`Bound` 状态、`load_snapshot`/`list_snapshots`、store-level CAS reset/fresh-fork/commit、atomic completion update 与 exact owner/session/execution lease；direct-HTTP transcript continuity 可用于 regular dispatch，并由本地 advisory lock 从 continuity read 串行化到 revision-CAS completion。数据契约和 lease 仍不是 native resume authority：fresh-sessionless model/tool bridge 明确拒绝 session-bearing scope，尚未与 session lease/exact domain 组成生产 authority；legacy-unbound 与 bound native state 会在 `make` 前拒绝，streaming 对任何 session 在 load/probe/`make` 前直接拒绝。当前 lease 仅覆盖本地 `FsSessionStore`，post-model persistence 是 best-effort。service/CLI 只提供 owner-local list/inspect/reset-native；没有公开 fork、REST mutation、创建、归档、append message 或 binding | 补创建、归档、append message 与完整的 authenticated session-control API；组合 active permit、live session lease 与 exact domain revalidation 后再启用 native resume；以 forged identity、domain drift、同 session 竞争和跨 harness/direct HTTP 续接 hermetic E2E 验证。非文件系统 store 另需 generation/TTL/stale-holder fencing。 | P2 |
| CON-02 | LogicalSession、runtime sub-session、session mirror | `missing` | optional-adapter | 一个 logical session 可关联多个 runtime sub-session，并可映射外部 thread。 | Rust 无 logical-session/mirror module 或 CLI/API | 先定义 platform-neutral LogicalSession；一个 logical id 可挂多个 runtime session，并以 optional adapter 映射外部 thread。 | P2 |
| CON-03 | workflow journal resume | `different` | decision | prior run 的连续 call-hash prefix 可回放，分叉后转 live 并产生新 run。 | Rust 对同一 journal 重置非成功 step 并继续；source bundle 有 v1 hash/legacy migration；WP-54 让新 journal 记录 exact plan digest，plan-only continuation 必须精确匹配，source-bearing compatibility API 可在 exact source hash 后迁移 planless legacy journal。WP-58 将 `replay/fork` 落为新的 UUIDv7 journal，只接受 terminal exact-plan source，复用 dependency-closed、journal-recorded all-success 前缀，source journal 保持只读；partial fan-out 会 live rerun。CLI 支持 generated/caller-owned new id，并在任何 target journal/live suffix 前 flush 新 id。 | 继续实现 changed-plan call-hash matching、prompt_file/部分成功/重复 replay 的更广 golden 与跨实现 migration；当前 exact-plan replay 不等于参考侧 changed-plan prefix cache，不得据此互称行为等价。 | P1 |
| CON-04 | live pause/resume | `planned` | public-core | pause/resume 只在 adapter call 边界使用进程内 event；server restart 会丢 task reference。 | `vyane-rs:README.md`; `docs/ROADMAP.md` 明确未实现 | 若实施，必须定义安全 checkpoint、进程/HTTP in-flight 语义、daemon restart 行为；不能只切换 metadata flag。 | P2 |
| CON-05 | daemon restart automatic payload replay | `planned` | decision | workflow 没有可靠的 daemon 自动 replay；特定 coding harness 可在 worker crash 后按 native session id 尝试续接。 | Rust restart 将 exact abandoned workflow 标记 `interrupted`，不持久化 source/vars payload；`docs/adr/0002-workflow-frontends-and-resume.md` 明确 daemon restart 不隐含 resume 或 replay | 只有另行建立 encrypted/retained payload policy 与 admission contract 后才可选择 replay；当前继续 fail-closed。 | P2 |
| CON-06 | worker health + session-aware auto-resume | `partial` | optional-adapter | worker health scanner 可识别 controller loss，并在有限条件下尝试 session-aware recovery。 | `vyane-agent` 已提供 durable lease/deadline、heartbeat、two-stage recovery、resume admission 与 active permit。WP-45/47/48 提供 one-shot recovery/execution 与 exact paired `InProcess` backend；WP-51/53 增加 resident loop、completion staging/publication 与 recovery 对账。WP-61 将 fresh/sessionless CLI-harness Process 路径接入 daemon：exact sidecar recovery、active cancel 时 terminate/reap、final completion drain，restart 不 replay。它仍没有 `Remote`、session-aware production authority、automatic resume 或非文件系统 fencing | 仅在 exact native session domain、policy allow、controller gone proof、active permit revalidation、live session lease 和 bounded retry 下增加 session-aware recovery；Process restart 不得隐式 replay fresh input。 | P2 |
| CON-07 | GoalStore、acceptance、quota handoff、pursuit | `partial` | decision | goal lifecycle 包含 acceptance、progress、quota handoff 与自动 pursuit。 | WP-60 新增 `vyane-goal`：owner-scoped 单一 SQLite 真相源、事务型 snapshot/event revision、六态 lifecycle、priority queue、progress、acceptance descriptor，以及本地 `vyane goal` CLI。WP-68 至 WP-72 增加有界本地 verifier、不可变 evidence、manual pursuit、lease-fenced checkpoint 与显式 opt-in daemon pursuit。WP-73 以 schema v5 加入分轴 typed continuity policy 与可见、幂等 quota handoff；WP-74/75 加入严格绑定的一次性 takeover approval/dispatch 与 exact takeover evidence review handback。WP-76 增加 `goal continuity-signal ... quota-reset`：精确绑定当前 quota event 与 primary provider/harness/model，原子保存 typed readiness fact；review 与 reset 任意顺序到达都只在两者齐备时将 `resume_primary` 标为 ready，且不消费 approval、不 dispatch。仍无 primary resume execution、authenticated service/API 或网络/board verifier。 | 保持 quota reader、pursuer、continuity state 与 daemon assembly 分层；后续单独接 primary-resume approval/execution 和 authenticated authority，不能把 ready step 扩写成运行时已恢复。 | P2 |

### 4.3 Collaboration

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| COL-01 | 多目标 fan-out | `implemented` | public-core | CLI/MCP 可对多个目标并发 fan-out，并汇总 partial success。 | `vyane-rs:crates/vyane-kernel/src/broadcast.rs`; CLI/REST/MCP broadcast | 对 partial success、取消、并发上限、各目标独立四层 identity 建跨仓 golden。 | P0 |
| COL-02 | review / consensus / debate collaboration patterns | `partial` | public-core | 通用协作引擎支持 review、consensus、debate 与 convergence round。 | Rust 只有固定 solution-review DAG，没有通用 iterative collaboration engine；`docs/adr/0003-separate-solution-and-change-review.md` 已区分 solution review 与 repository-change review | 定义 pattern-neutral round/convergence contract；review、consensus、debate 各有 fake-target E2E 与 ledger trace。ADR 只固定产品边界，不补足 engine。 | P2 |
| COL-03 | durable local AgentRun queue/inbox | `partial` | public-core | owner-aware durable queue 支持 delayed delivery、claim/read 与 worker inbox。 | `vyane-agent` 已有 owner-scoped FIFO/generation/lease/recovery/resume/topology/cancel/outbox；`vyane-message` 与 broker 分离持有 message/delivery。WP-59 新增同机 `a2a send/inbox/read`。WP-61 将 generic execution/recovery/publication supervisor、WP-55 live spawn authority、private frozen spool、exact Linux Process sidecar 与 message completion handback 生产 assembly 到 authenticated loopback daemon AgentRun submit/status/output/cancel；仅支持 fixed local owner 下 fresh/sessionless CLI harness，restart 不 replay。仍无 `Remote`、native production host、domain-aware resume、principal-derived message API 或 Channels adapter | 保持 fencing/recovery/execution/publication ordering、identity reuse、shutdown 与 race tests；下一步把 fixed local bearer 升级为 principal-derived scope，并接 `Remote`/protocol adapter。remote send→local receipt 不得宣称 distributed exactly-once。 | P2 |
| COL-04 | A2A HTTP v0.3、Agent Card、SSE、push | `missing` | optional-adapter | A2A server/client 覆盖 Agent Card、request/subscribe/cancel 与 push delivery。 | WP-59 只提供同机 SQLite inbox CLI；Rust 仍无 A2A HTTP server/client、Agent Card、SSE 或 push surface | 用协议 fixture 验证 Agent Card、send/get/cancel/sendSubscribe、Bearer owner binding 和 SSRF-safe push；作为可选 crate。 | P3 |
| COL-05 | worker/child topology、tree cancel、worker messaging | `partial` | public-core | worker runtime 支持 child spawn、tree cancel 与 worker-to-worker message。 | `vyane-agent` 已实现 immutable parent、fenced child spawn、bounded topology、children-first tree cancel 与 frozen operation scope。WP-61 的 daemon Process host 消费 tree cancel、exact process stop/recovery 与 message completion handback，并提供 authenticated local API；当前 submission 只创建 root/单 worker，尚无 child spawn product surface、worker-to-worker messaging、`Remote` 或 distinct-principal authority | 将 child topology 与 worker messaging 接入 concrete product operation；保持 prompt/message 不进入 task/AgentRun row；补 nested cancel、remote crash/retry 与 principal scope E2E。 | P2 |
| COL-06 | research campaign / team planning | `missing` | optional-adapter | research campaign 与 team planning 可编排多 worker、阶段和产物。 | Rust 无对应 surface | 先基于通用 collaboration/worker contract 实现可审计 campaign；平台 channel 与 issue-tracker 字段留 adapter。 | P3 |

### 4.4 Ledger

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LED-01 | append-only run ledger、attempt trail、usage/cost、session link | `implemented` | public-core | append-only history/audit 记录 attempt、usage、cost 与 session correlation。 | `vyane-rs:crates/vyane-core/src/run.rs`; `crates/vyane-ledger/src/{jsonl,cost}.rs` | 建 schema mapping 和 golden；验证跨进程 line integrity、锁、process-crash 后 partial-tail 可跳过、prompt digest、attempt order、unknown-price=`None`。当前 JSONL append 不做 fsync，不得宣称 OS crash/断电后的 acknowledged durability。 | P0 |
| LED-02 | durable task lifecycle ledger | `different` | public-core | task 状态分布在 status JSON 与多种 daemon JSON/SQLite store。 | `vyane-rs:crates/vyane-task` 统一 SQLite snapshot+event+CAS/epoch | 接受 Rust 作为 stronger canonical contract；为 CLI/REST/daemon 三 origin 建 lifecycle/event consistency 测试。 | — |
| LED-03 | Git-backed board/event ledger | `missing` | optional-adapter | board workflow 通过 Git-backed 文件、CLI、MCP 与 HTTP mutation 协作。 | Rust 无 board schema/parser/gitops/service | 先做 generic board/event ledger crate；平台文件格式、git remote 与 push 由 optional adapter 处理。 | P3 |
| LED-04 | daemon event/timeline store | `partial` | public-core | daemon event store 支持 timeline 和图形化投影 API。 | `vyane-rs:crates/vyane-ledger/src/event.rs` 已有 owner-scoped append-only stream、monotonic sequence、stable event id、durable/buffered append、bounded cursor/page；`vyane-message` 与 `vyane-agent` 均有事务无正文 outbox。`vyane-broker::{MessageEventProjector,AgentEventProjector}` 都以 bounded append-then-mark 方式 durable append、复用 stable source event id、再独立 ack；AgentRun 投影只映射 bounded worker/run lifecycle metadata，并排除 prompt/target/policy、logical/native session、task/trace 与 raw body。显式 non-`Clone` `ResidentBrokerSupervisor` 可并行常驻轮询两个 projector、delivery lanes 与 maintenance；batch/concurrency/backoff 有界，各 loop failure 隔离，取消会 drain 已开始的有界 cycle。driver 不 spawn task、不建 channel/runtime/第二队列，且尚无 service/CLI/daemon 生产 assembly。`AgentProjectionComponents::open` 封装 raw store；ordinary dispatch 不打开 AgentRun DB 或启动 background work。仍缺 dispatch/workflow producers、subscription、retention/GC 和统一 timeline/trace projection | 定义 trace/span correlation、production resident assembly、subscription、retention 和其余 producer wiring；message/dispatch/workflow/worker 使用可追踪的统一 event projection，并验证 cursor resume、投影重试、backpressure、graceful drain 与 GC。 | P2 |
| LED-05 | approvals/interventions/notifications/artifacts | `missing` | public-core | 独立 typed store 管理 approval、intervention、notification 与 artifact registry。 | Rust 无对应 typed stores | 每类先建立 owner-scoped typed record 与 CAS/idempotency；API mutation 必须 auth/decision-audited，payload 与 secret 分离。 | P2 |
| LED-06 | goal ledger | `different` | decision | goal 使用 append-only truth 与查询索引组合持久化。 | WP-60 的 `vyane-goal` 以一个 SQLite database 同时承载 current query snapshot 与 immutable lifecycle/progress events；每次 mutation 在同一 IMMEDIATE transaction 更新同 revision snapshot/event，foreign-as-absent 与 same-id cross-owner contract 已覆盖。它刻意不复制 JSONL + SQLite 双事实源 | 接受 Rust stronger single-truth contract；后续 migration/projector 必须保持 event immutable、snapshot/event atomic 与 owner predicate，不能再引入并列 writable truth。 | — |

### 4.5 Governance

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GOV-01 | owner-aware records 与 API isolation | `partial` | public-core | user-facing store/API 使用 owner-aware records 和 principal-derived owner context；list/detail/mutation 以 owner 隔离，foreign resource 与 absent 同形，cancel 与 timeline/intervention event 只能归属并作用于请求 principal 的 owner scope。system-global operation 的管理权限是独立 role/capability 问题，不能用 owner 字符串代替。 | Rust stores/records 从 day one 带 owner，并已有 foreign-as-absent 与 cross-owner tests。WP-49 的 `OwnerContextFactory` 冻结 trusted authenticator/resolver，隐藏 `AuthenticatedPrincipal` 构造并拒绝 authenticated `local`；`OwnerScopedService` 将 frozen owner 绑定到 dispatch/stream/run/session/reset。WP-50 将 task snapshot/event/FK/CAS 迁移为 `(owner,id)` 并让 REST artifact 使用 opaque owner/task namespace，same-id cross-owner store contract 已覆盖。内置 REST/CLI/daemon 仍只选择 explicit `local`，bearer 尚不代表 distinct principal，workflow/message/AgentRun/event mutation 也未完成；因此没有关闭 GOV-01 或宣称 multi-user isolation | REST/daemon/MCP authenticated front-end 必须通过 fixed authenticator factory 派生 context，不能从 payload/query 接 owner；将剩余 durable control surface 全部 owner-scope。foreign mutation 不产生 success event；cross-owner/admin 使用独立 typed capability，不得用 owner string 或 nullable owner 隐式授权。 | P2 |
| GOV-02 | clean-env、process group、exact controller recovery | `implemented` | public-core | adapter 与 daemon subprocess manager 负责 clean environment、process group 和 controller recovery。 | `vyane-rs:crates/vyane-core/src/env.rs`; `crates/vyane-harness/src/spawn.rs`; `crates/vyane-cli/src/workflow_control.rs` | 保持 Rust stronger contract；覆盖 env secret scrub、PID/PGID reuse、parent death、TERM→KILL revalidation、residual group fail-closed。 | — |
| GOV-03 | sandbox / filesystem capability gate | `partial` | public-core | coding harness flag、native OS sandbox 与隔离 transport 共同承载 filesystem authority。 | Kernel 已在任何 `make`/HTTP/subprocess 前完成整链 trusted `CapabilityManifest` admission；Linux mutating path 以 open-first fd/identity、descriptor-backed cwd 与 `/proc/self/fd` 抵抗路径替换，非 Linux fail closed。该 pin 不是 OS sandbox，同 UID child、绝对路径与 `Full` 仍不受 host confinement。tool registry 现有 live-authority gate、fresh-sessionless bridge、bounded dark driver；WP-52 又将 paired in-process operation 的 lifetime-bound authority绑定 exact fresh native scope，并在每个 model/tool effect 重验完整原子 predicate。但仍无 concrete operation、production runtime、trusted path-capability built-ins、result handback 或 session-aware authority | 保持 admission/pin/native-scope regression；native harness 再提供 host-enforced sandbox、production authority assembly 与每个 open/publish/spawn 均重验的 path-capability tools。不得把 dark composition、registry gate、`AdapterDelegated` 或 pinned cwd 描述成 same-UID confinement/microVM。 | P1 |
| GOV-04 | DispatchKernel policy、role/persona/manifest | `partial` | public-core | execution policy 组合 role、persona、agent manifest 与 capability manifest。 | Rust 已有 filesystem/isolation capability manifest、整链 admission 与 scoped audit identity，但无完整 role/persona/owner policy approved envelope | 定义 immutable approved execution envelope；policy deny 前不得 spawn；persona/role provenance、capability ceiling 和 audit fields 有 golden。 | P2 |
| GOV-05 | allow/deny/ask permission + approval binding | `partial` | public-core | native permission 层支持 ordered allow/deny/ask 与 approval binding。 | `vyane-rs:crates/vyane-harness/src/native/{permissions,tools,turn_driver}.rs` 已有 ordered allow/ask/deny、default deny、protected deny floor、risky-operation ask、canonical plan hash + call binding；`ToolRegistry::execute_authorized` 只在 allowed call 执行前消费 authority，deny/ask/invalid/unknown/cancelled/expired 保持纯决策。dark driver 只走 authorized model/tool entry，并把 Ask 变成持有 exact plan 的 typed non-replayable stop；possible tool activity 后的失败不再外逃为 failover error。兼容 `execute` 仍不带 authority，driver 没有 production runtime 接线，permission matching/schema guidance 也不是 OS sandbox 或 actual-argument validation，因此 protected-path policy 在下层 sandbox 前仍 deny 每个 `run_bash`。仍无 approval store、one-shot consumption、expiry、drift check 或 resume | 生产 native loop 必须只走 authorized entry；每个 tool 校验 actual arguments；ask 进入 owner-scoped approval store，批准一次性绑定 exact pending call、校验 expiry/drift，并只在同一 active execution 中、每个外部 side effect 重验 permit 后 resume，不得隐式重放已结束/漂移调用。 | P1 |
| GOV-06 | worktree isolation discipline | `missing` | public-core | write-capable workflow 可显式选择 worktree 来隔离并行变更。 | Rust workflow step 只有 workdir/sandbox，无 worktree lifecycle | 为 write/full fan-out 提供 opt-in 隔离，记录 branch/path/controller；clean 自动清理，dirty 可 salvage；是否默认化另立 ADR。 | P2 |
| GOV-07 | project memory governance | `missing` | decision | project-local memory 与 frozen execution memory 有独立生命周期和读取边界。 | Rust 无 memory subsystem；本仓不得复用项目外的全局 memory | 等统一 memory/docs/skills 架构拍板；若实现，只建项目本地规范/索引和 explicit read/write policy，不写项目外全局目录。 | P3 |

### 4.6 Quality

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| QUA-01 | multi-model review pipeline | `different` | decision | repository-change review 覆盖 bounded diff/PR acquisition、specialists、merge、verify 与 durable artifact；对不可信 change content 的 CLI-harness 路径使用最小环境和临时只读工作区，并在 model launch 前验证外部文件、认证材料与网络边界。这些是参考能力，不是可盲信的安全 oracle；平台 probe 只能作为验收证据，不能因环境缺失而静默跳过。 | `vyane-rs:crates/vyane-cli/src/review.rs`：implement→fan-out review→synthesize solution pipeline；它没有 immutable diff bundle、structured finding/verifier/durable change artifact，也没有为 untrusted change-review 定义独立 harness isolation envelope。`crates/vyane-core/src/env.rs` 与 `crates/vyane-harness/src/spawn.rs` 提供通用 clean-env/process-group 基座，但 adapter-delegated read-only flag 不等于已证明的 review confinement；`docs/adr/0003-separate-solution-and-change-review.md` 已接受 solution review 与 repository-change review 为两个产品 | CLI/API 必须明确 product kind；另实现 immutable bounded diff acquisition、structured findings、verifier、revision/exit gate 和 redacted durable artifact。untrusted CLI harness 必须使用同一份 frozen auth/execution plan，并以 non-skippable supported-platform 验收证明 workspace 可读、外部/auth path 不可读、tool network 不可达；managed override、unsupported profile、probe timeout 都必须在 model launch 前 fail closed。不能用现 solution review 关闭 parity。 | P1 |
| QUA-02 | review artifacts、GC、GitHub publication/issue helpers | `missing` | optional-adapter | review findings 可持久化、GC，并发布到代码托管或 issue tracker。 | Rust 无 durable finding artifact/publisher | 定义 platform-neutral finding schema+run artifact；代码托管和 issue-tracker publisher 均为 optional adapter。 | P2 |
| QUA-03 | local/CI verification gates | `implemented` | public-core | local/CI gate 覆盖 test、lint 与 review checks。 | Rust workspace fmt/clippy/test、fake CLI/protocol/daemon acceptance、publish preflight | 维持全部 gate；新增 parity suite 后列为 required check。单仓 test count 不能替代行为 parity。 | P0 |
| QUA-04 | 跨实现 golden/shadow parity harness | `partial` | public-core | 参考基线现包含 automatic workflow routing 的部分行为，但旧 embedded parity bridge/fixture 仍未覆盖当前独立 Rust 实现的完整能力面，也不能把 staged frontier/model policy 冒充为已闭环行为。 | `vyane-rs:docs/parity/fixtures/v1`、`crates/vyane-cli/tests/parity_manifest.rs` 与 `.github/scripts/parity-report.py` 只公开 maintainer-attested、已净化的 classifier/failover/automatic-routing behavior，以公开文件 SHA-256、closed disposition、case digest 和当前 Rust 重算锁定；不发布私有 repository、commit、blob 或 source path provenance。三套精选 fixture 共 25 case：15 normalized exact、10 open difference。automatic suite 的每个输入携带完整中性 candidate config；真实 template render、typed workflow effort 与 closed error reason 会在 Rust 侧重算。Rust candidate/profile mapping、full-chain frontier、direct-leg ambiguity 与 frozen replay 明确是一侧 Rust scope evidence，不冒充 reference equality。离线工具重算 Rust 并验证 stored attestation，不执行私有 reference。 | 继续覆盖可复现 reference-side refresh/export、完整 oracle case、target resolve、production failover trail、run/session schema、workflow migration与更广 shadow coverage。fixture 只使用中性 profile/model id，open difference 只能经 ADR/migration evidence 关闭。 | P0 |

### 4.7 Observability

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| OBS-01 | history、attempt、usage、cost 查询 | `partial` | public-core | history query 支持 stats、cost、source 与 time-window filters。 | Rust `history`/`/v1/runs` + ledger cost，查询维度较少 | 补 time/source/session/label/cost aggregation；schema 不回显 prompt/output；与 ledger golden 对齐。 | P2 |
| OBS-02 | task/workflow status 与日志 | `implemented` | public-core | status file、workflow journal 与 daemon store 提供 task/workflow status 和 log。 | Rust durable task snapshot/events、mode-0600 logs/output、workflow journals | 覆盖 queued→terminal 全状态、controller loss、corrupt artifact、pagination/filter 和 concurrent readers。 | — |
| OBS-03 | live streaming / SSE | `partial` | public-core | status、dispatch、workflow 与 daemon event 可经 SSE 推送。 | Rust dispatch stream CLI+REST SSE，并已有 owner-scoped replayable EventLog storage substrate；EventLog 尚未接 producer 或 subscription，daemon workflow 仍只有 status/cancel | 增加统一 event id/resume token/heartbeat/backpressure contract；dispatch/workflow/worker producers 接同一订阅面，并定义 retention。 | P2 |
| OBS-04 | dashboard / TUI monitor / optional views | `missing` | optional-adapter | dashboard、monitor 与平台 UI 提供运行状态视图。 | Rust 无 UI/TUI | 先提供稳定 read API/event stream；通用 TUI 可公开，平台 UI 留 optional adapter。 | P3 |
| OBS-05 | routing feedback、benchmark、quality analytics | `missing` | public-core | feedback、benchmark 与 route outcomes 可形成持久质量信号。 | Rust router 无持久 feedback/benchmark signal | 定义可匿名化 feedback/benchmark schema；默认 deterministic，只有显式启用时进入 route scoring，并记录贡献信号。 | P2 |
| OBS-06 | health、watchdogs、quota ledger、notifications | `partial` | optional-adapter | daemon health、worker/session watchdog 与 notification store 形成运维信号；新的 upstream quota/balance 能力是对有限 connector 集合的并发 snapshot view，将 absolute/balance/window 信号归一化并隔离单 connector 失败。它当前不是 durable history/ledger，不能用名称把一次快照扩写成可回放账本。 | WP-76 新增 `vyane-quota`：platform-neutral `QuotaConnector`、closed status/window/card/balance/error schema、connector/card identity 校验、有界 connector/concurrency、整次操作 timeout、稳定排序和单 connector 失败隔离。`QuotaHttpReader` 只接受 HTTP(S)，禁 redirect，限制 response body；hermetic tests 使用 fake 与 loopback mock server。仍无具体平台 connector、credential source、持久 quota history/ledger、轮询 watcher、notification store 或 CLI snapshot surface；`vyane-ledger` usage/cost 仍不等于 upstream quota。 | 接入至少一个可选 concrete connector 与 opaque read-only credential reference，并补 polling/retention/notification 的显式治理边界；任何自动动作必须另走 approval。 | P3 |

### 4.8 Interfaces

| ID | 能力 | 状态 | 目标层 | 参考能力/行为（私有基线审计） | Rust 证据/缺口 | 验收条件 | 波次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| INT-01 | CLI | `partial` | public-core | CLI 覆盖 execution、daemon、collaboration、message、session、goal 与 maintenance 等产品面。 | `vyane-rs:crates/vyane-cli/src/cli.rs#Command` 覆盖 core execution/task/workflow/daemon、owner-local session control、WP-59 本地 message `a2a send/inbox/read`，以及 WP-60 本地 goal create/query/lifecycle/progress 全套命令与 stable JSON；仍无完整 collaboration、goal verifier/pursuer 与 maintenance 产品面 | 每个纳入 public 范围的 service 都有 JSON-stable CLI；private-only 命令不要求复制。继续建立 `--help` 和 exit-code snapshots。 | P2 |
| INT-02 | REST API | `partial` | public-core | API 覆盖更广的 daemon/product resources，dispatch/workflow SSE 使用 owner-scoped control。 | Rust `serve` 有 core dispatch/broadcast/tasks/runs/sessions，使用 per-start bearer、loopback Host/Origin enforcement 与 allowlisted views。workflow daemon 有独立 authenticated local workflow API；WP-61 在同一受限 listener 上增加 Linux AgentRun submit/status/output/cancel，deny unknown fields、要求 UUIDv7、只返回 allowlisted lifecycle/output view。两者仍冻结为 fixed local single-user scope；bearer 不是 distinct principal，也不提供 hostile same-UID/multi-user isolation | 定义 API scope/version/auth ADR；把 authenticated request credential 接入 fixed `OwnerContextFactory`，让所有 control surface 使用 principal-derived scope，并补统一 error schema、pagination/idempotency；private resource 走 optional adapter。 | P2 |
| INT-03 | MCP server | `partial` | public-core | 参考 MCP surface 有 19 个工具，覆盖 task control、workflow、board、check 与 collaboration。 | `vyane-rs:crates/vyane-mcp/src/lib.rs` 保留 6 个 base tools，并提供不持有 daemon credential/discovery 的 object-safe `WorkflowControl` port；`vyane mcp` 由 CLI 注入经认证的 resident-daemon adapter 后提供 9 个工具，新增 bounded self-contained workflow submit/status/idempotent-cancel。caller 只能提供 canonical UUIDv7、source bundle 与 vars，不能提供 owner/controller/token/execution cwd；响应只含 caller id、生命周期和 closed failure code。fake-port real-rmcp 与真实 CLI→MCP→authenticated daemon acceptance 均覆盖。route/check 仍是 Rust diagnostics，generic success output 仍有 1 MiB 上限 | 继续接统一 durable task control、collaboration/board 与 principal-derived owner context；旧 execution tools 仍需统一逐字段 input budgets，sessions 仍不等于参考多动作 session tool。 | P1 |
| INT-04 | resident daemon | `different` | decision | 广义 daemon 同时承载 worker、scheduler、event、message、memory、goal 与 channel；workflow HTTP 属于另一 process surface。 | Rust daemon 已持有 authenticated local workflow execution，并以 WP-61 production-assemble generic AgentRun execution/recovery/completion supervisor、fresh/sessionless CLI-harness executor、exact Linux Process sidecar、tree cancel、message handback 与 cooperative admission/drain。启动先 exact recovery，restart 不 replay。broker 仍 unwired，也没有 `Remote`、native production host、distinct-principal wiring、live pause/resume 或 automatic replay。ADR 0004 接受由窄 domain supervisor 组成 authenticated resident host | 继续 production-assemble broker 与 remote/native integrations、统一 principal-derived owner context 和 bounded drain；用 executable acceptance 分别证明每个 domain，不得因已有窄 Process host 或同名 `daemon` 宣称广义 daemon parity。 | P0 |
| INT-05 | A2A server/client | `missing` | optional-adapter | A2A interface 同时提供 server、client、SSE 与 remote adapter。 | WP-59 的本地 queue CLI 不是 protocol adapter；Rust 仍无 A2A server/client interface | 与 COL-04 同一验收，不另造 task truth。 | P3 |
| INT-06 | IM / channel / platform UI adapters | `missing` | optional-adapter | coding-agent channel、IM bot 与平台 dashboard 通过独立 adapter 接入。 | Rust 无 IM/channel/UI adapter | 先抽 platform-neutral channel/session/message API；具体 plugin/bot/UI 独立 crate/repo，凭据与个人路由不进入 core。 | P3 |
| INT-07 | packaging / registry publication | `different` | decision | 参考系统使用 Python packaging；其分发生命周期与 Rust registry 不同。 | Rust 17 crates 已通过本地 package+verify preflight，含 `vyane-goal` 与显式依赖顺序，但没有发布。WP-56 移除 tag-push 触发：只允许从当前 `main` 手动输入既有 release tag；workflow SHA、`origin/main` 与 tag 必须完全一致，且 `crates-io` environment 必须配置 required reviewers 与 prevent-self-review。registry token 只进入最终 publish step | tag 与 token 都不是授权；必须另获明确外部发布授权，由非发起 reviewer 批准 protected environment，且不得改变任何 parity 行状态。 | — |
| INT-08 | config lifecycle / schema compatibility | `partial` | public-core | provider/profile 之外还包含 routing/category/workflow presets、初始化与配置演进体验。 | Rust 有 layered provider/profile TOML、解析与静态 diagnostics，但没有 schema migration、category binding/preset compatibility 或 init/config UI。 | 用中性 fixtures 固定版本化 schema、迁移与 precedence；迁移不得读取或写出 secret，unknown/newer schema fail closed，category/preset 行为须经明确兼容决策。 | P2 |

## 5. 为什么 crates.io 与 parity 无关

`crates.io` 是 Rust 社区的公共包注册表，作用类似 Python 的 PyPI。把 17 个 crate 发布到
crates.io，只能证明：

- crate 名称、版本、license、README 和依赖元数据可被注册表接受；
- workspace path dependency 在发布包里有合法版本；
- 在另行授权并实际执行后，发布工作流能用 registry credential 按依赖顺序上传成功。

它不能证明 native harness、A2A、goal、board、Channels、dashboard 或任何其他功能已经对齐；
也不能证明同名 workflow/review/daemon 具有相同语义。反过来，不发布 crates.io 也不妨碍本地
二进制完成 parity 验收。

因此：

- `docs/ROADMAP.md` 的 “crates.io publish readiness” 只属于发布准备；
- tag 只是运行时会重新验证的发布输入，`CARGO_REGISTRY_TOKEN` 只是 registry credential；两者存在都不构成
  对 crates.io 发布的授权。仓内 workflow 不再由 tag push 触发，只允许从当前 `main` 手动输入
  精确 tag，并要求 protected `crates-io` environment 的非发起 reviewer 批准；
- crates.io publish 是外部、难以撤回的分发动作，必须单独获得明确授权；未获授权时不得创建
  发布 tag 或上传 crate；
- 两者都不是 parity milestone，不能用于关闭本矩阵中的任何 ID。

## 6. 原仓不是完成态

以下内容来自私有基线审计，是参考系统自身的限制，不应机械搬运：

1. 参考系统仍是持续演进的内部实现，不是打磨完成的公开产品。
2. provider registry 仍包含 metadata overlay 与旧 adapter 并存路径，四层 target 的代码边界尚未
   全部收口。
3. 仍存在 legacy/enforce 双路径、较大的聚合模块、多套 task truth 与 store substrate 分叉。
4. async task 的 pause/resume 依赖进程内 task/event；重启后会丢失 task reference。
   这不是可直接照搬的 durable live pause/resume。
5. workflow resume 是显式 prefix-cache replay，不是 daemon 重启后自动 payload replay。
6. native harness 在固定基线虽已有大量实现，但 rollout 仍是 allowlist/default-off；
   “已有代码”不等于已经成为所有 workflow 的默认执行壳。
7. coding-agent channel adapter 虽有实现和 probe，但仍受上游 allowlist、CLI 版本和账号认证
   状态影响。

Rust 可以把这些边界做得更简单或更强；此时应标 `different` 并记录选择，而不是为了表面
一致复刻原仓债务。

## 7. 路线文档冲突与读取优先级

私有基线审计发现若干必须显式处理的路线冲突；公开文档只记录其产品含义，不暴露内部文件名：

- 一份历史架构计划主张先在同仓 shadow，再决定是否独立实现或发布；当前公开 `vyane-rs`
  已独立存在并扩展到 daemon/REST/MCP，因此该历史输入不再是实现 blocker。
- 参考系统的历史 roadmap checkbox 不是当前执行证据。
- native-harness 设计文档与较新的实现/验收存在时间差；状态判断应以受控基线审计和可执行
  验收为准，不能只读“草案”标签。
- 本仓 `docs/ROADMAP.md` 的 v0.1–v0.4 是 **Rust 仓内部 milestone**。`delivered` 表示该
  milestone 的本仓范围完成，不表示原始 Vyane 产品 parity 完成。
- 本仓 README 的 “tracking the private system capability by capability” 是来源说明。矩阵要求
  对全部能力项给出实现、可验证 adapter 或明确差异决策，但仍不允许把私有身份、凭据、
  endpoint、路径或项目数据复制进公开仓。

在新的跨仓战略 ADR 出现前，读取优先级为：

1. 本文档的公开矩阵与受控私有基线审计；
2. 本仓公开 baseline commit 的源码、hermetic tests 与已净化的跨实现 fixtures；
3. 当前架构/范围 ADR；
4. 单仓 roadmap；
5. 历史 roadmap、release note 和草稿。

各 `different` 项的具体产品语义、迁移方式和 private adapter 边界仍须逐项 ADR/验收，不能把
持续推进目标误读成机械复刻参考系统债务。

## 8. 执行顺序与完成判据

### P0：先让“对齐”可证伪

1. v1 manifest 已固定精选 classifier/failover/automatic-routing case 与 open difference；automatic
   workflow route precedence、no-eligible fail-closed、full-chain frontier guard、frozen route replay
   及离线 report 已落地。下一步扩到 EXE-01/02、COL-01、LED-01、完整 oracle case、target resolve、
   production failover trail、run/session schema 和 workflow migration。
2. EXE-05、EXE-06/CON-03、QUA-01、INT-04 的 ADR 已建立；继续实现各 ADR 的 migration 与
   acceptance gate，不能把 accepted decision 当作行为已对齐。
3. 在 README/roadmap 的“delivered”旁保持指向本基线的说明，禁止再把 publish-ready
   或 workspace tests 全绿表述成 full parity。
4. 当前 workspace test 执行 manifest schema/provenance/normalizer 与 Rust behavior 重算；离线
   report 默认先运行该测试，再验证 fixture SHA/case set/disposition，且不要求真实 model key。

### P1：执行核心

1. Early execution scope、整链 capability admission、Linux pinned-workdir handoff、
   `ActiveExecutionPermit` 签发/重验原语、strict revisioned `NativeSessionDomain`
   reset/fresh-fork/commit 数据契约、本地同 session execution lease、permit-plus-native-scope atomic
   validation、per-wire model/allowed-tool abstract authority guard、仅支持 fresh-sessionless
   model/tool effect 的 concrete permit/store bridge，以及 bounded serial dark turn driver 均已落地。
   下一步在生产 assembler 中组合 bridge/driver/trusted built-ins，并把 exact session domain/lease、checkpoint prepare/publish、
   revision-fenced session commit 和 trusted tool 内每次 open/publish/spawn 都纳入同一 live authority；
   通过 domain drift/cancel/retry E2E 后再启用 production resume，并补 host-enforced sandbox
   （EXE-07/GOV-03/GOV-05）。
2. workflow contract 决策与兼容验收（EXE-06/CON-03）：validated explicit effort、
   full-chain frontier guard 与本仓 precedence/replay freeze 验收已落地；下一步是 shared typed
   plan、compatibility frontend 和净化的跨实现 migration fixtures。
3. 安全有界 `vyane_route` 与 static-only `vyane_check` 已接入 MCP；下一步通过 authenticated
   control port 补 workflow/task，并继续保持 route 作为 Rust 扩展、check 不做 live probe 的范围
   （INT-03）。
4. 把 review 的产品语义说清并补 immutable bounded diff、structured findings、
   verifier 与 redacted artifact；untrusted CLI harness 的 least-privilege probe 必须在支持平台
   成为 non-skippable gate，不能用现 solution review 关闭 QUA-01。

### P2：通用 AI OS substrate

按依赖顺序推进：统一 event/typed stores → principal-derived owner auth/policy → logical session/goal → message
queue/worker topology/collaboration → observability/feedback。`vyane-message` 的事务存储、
`vyane-broker` 的 bounded pump/message projector，以及 `vyane-agent` 的 durable queue/topology/recovery
store、bounded append-then-mark AgentEvent projector、显式 owner-bound projection-only service
assembly、non-`Clone` resident broker library driver、paired AgentRun execution/recovery resident library supervisor，及
principal-derived owner service/task truth 已落地。WP-61 已把第一条窄 Process AgentRun 路径接成
authenticated loopback production host：fresh/sessionless CLI harness、frozen spool/snapshot、exact Linux
sidecar/recovery/cancel、message completion handback 与 cooperative shutdown。下一步是 broker production
assembly、`Remote`/native integration、distinct-principal owner context、公开 CLI 与 A2A/Channels adapter。
任何 cancel/intervention/timeline mutation 都必须使用
principal 派生的 owner scope，foreign operation 不得产生 success event。每条线先建稳定 trait/schema，再接 CLI、
REST、MCP，避免把原仓 god files 和多事实源一并移植。

### P3：private 与平台集成

Git-backed board、平台 UI、IM/coding-agent channel、research campaign、watchdog 和 upstream quota
snapshot connector 等只在 generic
contract 稳定后作为 optional/private adapter 接入。发布仓不保存个人配置、账号 token、私有
endpoint、设备路径或项目外全局 memory；但 whole-system capability parity 必须对 generic
contract 和 adapter integration 提供可复现的 fake/hermetic 验证，不能用“private”作为跳过能力的理由。

### “完全对齐”允许使用的最低条件

只有同时满足以下条件，才能在一个明确目标范围内使用“对齐”：

- 该范围内没有未接受的 `partial`、`missing`、`different` 或 `planned`；
- 所有 `different` 都有生效 ADR 和迁移/行为验收；
- P0 parity suite 在固定双 commit 上全绿，且 refresh 后没有未归因 diff；
- private-only 具体实现不进入公开仓，但 generic contract 与 optional/private adapter 边界可验证；
  若明确排除这些项，只能称 **public-core parity**，不能称未限定的“完全对齐”；
- README、roadmap、release notes 使用同一范围措辞；
- crates.io/PyPI/GitHub release 状态只作为分发证据，不作为功能证据。
