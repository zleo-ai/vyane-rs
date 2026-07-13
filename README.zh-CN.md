# Vyane

**A multi-model agent-orchestration kernel, in Rust.**
（一个用 Rust 写的多模型 agent 编排内核。）

[![CI](https://github.com/zleo-ai/vyane-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/zleo-ai/vyane-rs/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#许可协议)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
&nbsp;·&nbsp; [English](README.md)

Vyane 是一个内核——以及内核之上的一个 CLI——用来在**两类目标**之间 dispatch、
broadcast 和 failover 任务：一类是 coding-agent harness（Claude Code、Codex
CLI 等），一类是裸的 HTTP model endpoint。给它一个任务加一个 target，它就把任务
跑起来；给它多个 target，它就并发扇出（broadcast）或按序故障转移（failover）。一次
运行究竟是在带文件系统和工具的 coding CLI 里发生，还是作为一次普通的 HTTP chat
completion，只是 target 上的一个字段——不是另一个工具、另一份配置、另一套心智模型。

真正带来正确性的是**四层 target 模型**——provider ≠ protocol ≠ harness ≠ model，
四者从配置一路到 run ledger 始终是独立字段。于是 **relay 不是 protocol**（它是恰好
讲某种 protocol 的 provider），**coding CLI 不是 provider**（它是仍然需要 provider
的 harness），**一个 model id 只在它所属的 provider 内部有效**。

具体带来四件事：**天然干净的子进程环境**（子 agent 从 scrub 后的基线启动，凭据不泄漏）；
**永不跨 provider 泄漏 model id 的 failover 链**；**整链 capability admission**（`Write`/`Full`
不会静默落到 chat-only target，Linux mutating run 会把准入时打开的目录对象带过 child spawn）；
以及 **append-only JSONL run ledger**
（记录 prompt digest 而非正文、完整 attempt 轨迹、owner-scoped 从第一天起）。

## 起源

`vyane-rs` 是 Vyane 的开源 Rust 实现。Vyane 本体是一套私有的个人 AI-OS 执行底座，自
2026 年初起就在跑真实的多模型开发流水线。现在它在开源世界里被逐能力重建，并持续跟随私有
系统演进——所以这里的设计反映的是日常使用中真正扛住了的东西，而不是凭空的推测。

## 这个仓库是怎么写出来的

由一支被编排调度的 AI coding agent 舰队开发：不同的前沿模型负责写代码、互相对抗性
交叉审查、再修复审查中发现的问题。每一次合并都经过独立的跨模型审查 + `cargo fmt` /
`clippy` / `cargo test` 三道关卡。

## 状态

**仓内 v0.1 到 v0.3 里程碑及 v0.4 实现范围已经交付。外部 crates.io 发布（发布到 Rust
公共包注册表）是延期的分发动作，不是功能里程碑，而且必须另行获得授权；tag 或 registry
token 都不等于这项授权。手动发布 workflow 还要求受保护的 `crates-io` environment 由非发起人
reviewer 批准，并要求输入的 release tag、当前 `main` 与 workflow SHA 精确指向同一个 commit；
registry token 只注入最终 publish step。16 个 crate 的本地 package preflight 已通过，但没有任何 crate
实际发布。这不表示已经完全对齐原始私有 Vyane。** 在当前公开集成基线上，固定双仓
矩阵按 8 个域追踪 53 个能力项：7 个 `implemented`、20 个 `partial`、16 个 `missing`、
8 个刻意不同或待决策、2 个 `planned`。native harness、
连续性、协作、治理、可观测性和接口仍有大量工作。未加限定的“完全对齐”默认指
whole-system capability parity：私有凭据和部署细节不进入本公开仓，但对应 generic contract
与 optional/private adapter 边界仍须可验证。live daemon pause/resume 与重启后自动 replay
只是两个明确的 daemon 限制，不是全部剩余差距。详见
[原始 Vyane 对齐基线](docs/parity/ORIGINAL-VYANE-PARITY.md)。API 仍处于发布前不稳定阶段。

| 能力 | crate | 状态 |
|------|-------|------|
| 核心类型系统（含 process-local workdir pin） | `vyane-core` | [x] 含 non-serializable live native-side-effect authority contract |
| config & profiles | `vyane-config` | [x] |
| OpenAI Chat/Responses + Anthropic Messages | `vyane-protocol` | [x] 基础 client；[~] 有界 typed tool turn 与 per-wire authorized path 目前只覆盖非流式 OpenAI Chat |
| Claude Code + Codex CLI harnesses（含 stdout 事件流） | `vyane-harness` | [x] additive scoped execution 可同时携带 Linux pinned workdir 与 live spawn authority；gated capture/streaming 会在 wrapper spawn 与真实 target release 前重验。尚无生产 AgentRun caller 构造该 authority，而且这仍是 adapter-delegated，不是 host sandbox |
| native permission/tool 执行接缝（尚不是 `Harness` 实现） | `vyane-harness` + `vyane-service` | [~] AgentRun scope atomic validation、per-wire model authorization、allowed-tool registry gate、fresh-sessionless bridge、有界 turn driver、lifetime-bound in-process native-scope composition 与通用 crash-consistent completion handback 已作为 dark components 落地；仍无 concrete product operation 或 production factory/runtime。session-bearing authority、trusted built-ins、OS sandbox、checkpoint/session commit、approval resume、native resume 均未完成 |
| dispatch / broadcast / failover kernel + streaming | `vyane-kernel` | [x] early execution id、整链 trusted capability admission、one-shot prepared dispatch 与 original-ordinal failover evidence |
| append-only run ledger + owner 隔离 session record | `vyane-ledger` | [x] direct-HTTP transcript continuity、strict revisioned V2 snapshot、store-level CAS `Reset` / `ForkFresh` / `Commit` 与 exact 本地文件系统执行期 lease 已具备；CLI/service 仅提供 owner-local list/inspect/reset-native，没有公开 fork、REST mutation、分布式 lease 协议或生产 native resume |
| 可 replay 的 owner-scoped event store | `vyane-ledger` | [~] storage/cursor、有界 message/AgentRun lifecycle 投影、显式 owner-bound projection-only service assembly 及尚未接线的常驻 broker driver 已具备；dispatch/workflow producer、subscription、retention 与统一 timeline 尚未完成 |
| 不含敏感正文的持久化 task metadata | `vyane-task` | [x] schema v2 以 `(owner,id)` 隔离 snapshot/event/CAS，并事务迁移 v1；内置前端仍显式选择 `local` |
| owner-scoped 持久 AgentRun queue、worker topology 与 recovery 真相 | `vyane-agent` | [~] exact lease/deadline、logical/native-session-id 与 policy-digest-fenced resume、active permit/native-scope fencing、有界 tree cancel、无正文 completion receipt/outbox、持久 defer/quarantine 与三循环 in-process resident supervisor 已具备；仍无 concrete product operation、production host、Process/Remote 或公开 execution API |
| owner-scoped 事务型 message/delivery store | `vyane-message` | [~] multi-mailbox strict FIFO、延迟/幂等投递、fenced lease、TTL、ack/nack、无正文 outbox、外部 receipt 对账与隐藏 staged completion publication 已具备 |
| 有界 replay-safe delivery broker + 无正文 EventLog projectors | `vyane-broker` | [~] fake-adapter 契约、复用 stable source event id 的 message/AgentRun lifecycle 投影，以及显式 non-`Clone` `ResidentBrokerSupervisor` library driver 已具备；service/CLI/daemon 生产 assembly、worker/message glue 与 A2A/Channels adapter 仍缺 |
| 声明式 workflow 引擎（DAG + journal/resume） | `vyane-workflow` | [x] |
| 常驻 workflow daemon（带认证的本地 submit/status/cancel） | `vyane-cli` | [x] |
| 后台任务（`--detach` + `task` 命令） | `vyane-cli` | [x] |
| CLI（含 workflow / task / daemon 等命令） | `vyane-cli` | [x] 新增 revision-aware `session list/inspect/reset-native`；旧 `sessions` 保持兼容 |
| 共享服务层 | `vyane-service` | [x] `OwnerContextFactory` 完成 authentication/resolution 并拒绝 authenticated `local`；`OwnerScopedService` 冻结 dispatch/stream/query/session/reset；optional AgentRun components 已含 paired backend、exact message-completion sink 与 execution/recovery/publication resident supervisor，ordinary dispatch 不启动它们 |
| REST API | `vyane-cli` + `axum` | [x] per-start bearer、loopback Host/Origin 校验、拒绝 non-loopback bind、allowlisted view 与 assembly-frozen local service scope；bearer 尚不代表 distinct principal，也不是 hostile same-UID 或多用户隔离 |
| 确定性路由 | `vyane-router` | [x] |
| MCP server | `vyane-mcp` | [x] 6 个工具：dispatch/broadcast/history/sessions，加两个有界 diagnostics——`route` preview 与仅静态 `check`；generic success output 上限为 1 MiB |
| solution-review workflow（implement → fan-out review → synthesize） | `vyane-cli`（review module） | [x] 尚不是原仓结构化 git diff/PR review 产品 |

Capability admission 刻意窄于 sandbox。`ReadOnly` 可用于 chat 或 harness target；`Write`/`Full`
要求现存 workdir 和受信任的内置 Claude/Codex CLI editing manifest，direct HTTP 与未知 adapter
会在构造 executor 前被拒绝。mutating dispatch 当前在非 Linux 平台 fail closed。Pinned descriptor
能防止 workdir 路径被 rename/symlink replacement 重定向，但不能约束同 UID 的恶意 child 或绝对
路径访问。exact `NativeSessionDomain` 存储契约与 store-level CAS transition 已经落地，但它们只是
证据，不是 resume authority。regular dispatch 现在必须取得 exact
`(owner, session_id, execution_id)` lease，先持有 lease 再加载 continuity，并一直持有到 model
执行结束与 revision-CAS completion update。文件系统 store 用 owner-only advisory lock 实现本地
crash-release fence，direct control mutation 也走同一 authority；同 session 竞争运行会在 executor
构造前完成有界等待：前一持有者及时释放时顺序取得 lease，否则返回 `Conflict`，两者都不会重叠。
这不是带 generation/TTL 的分布式 lease 协议，model 之后的 session commit 仍是
best-effort，不能描述成严格持久续接。regular dispatch 仍会在构造 executor 前拒绝 legacy-unbound
或 domain-bound native harness state；streaming dispatch 则更早拒绝任何 session，发生在
session-store load、capability probe 和 executor 构造之前。纯 direct-HTTP transcript 只能通过
regular dispatch 继续。owner-local CLI/service 仅提供 list、inspect 和 revision-checked
reset-native；没有公开 fork、REST mutation 或生产 native resume。

native authority 仍是未完成的 integration seam。独立的 OpenAI Chat typed-turn 路径会在
每次显式 wire send 前即时重验，并在等待 authority、发送、读取 response body 和 retry backoff
期间持续响应 cancellation。共享 HTTP client 不跟随 redirect，也不做隐式 retry，因此一次显式
attempt 对应一次 authority check。`ToolRegistry::execute_authorized` 也只在 allowed call 的 executor
被 poll 前重验；deny/ask/invalid/unknown/cancelled/expired 都保持纯决策，revocation 不会变成
model-facing tool text。`AgentRunModelToolAuthority` 现在提供 fresh、sessionless scope 的 concrete
bridge：它持有 permit/scope，在 Tokio blocking pool 中针对每个 one-based model send 或 tool
operation 重验 AgentRun store，并拒绝 session-bearing scope、checkpoint effect 和 session commit。
它没有被生产 factory 注册，也没有 runtime/native loop 调用，更没有组合 session lease 与 exact
native-session domain。

paired in-process operation 现在可把 lifetime-bound effect authority 绑定到一个 exact fresh、
sessionless `NativeExecutionScope`。bind 会先原子重验 owner/run/generation/lease/deadline/controller
以及 exact target/prompt/policy digest；之后每个 one-based model send/tool operation 都重复完整
native-scope validation。session/resume、checkpoint/session commit、raw store/permit access、Clone/Serde
继续关闭。这只是 authority composition seam，不是 concrete native AgentRun operation 或 result
handback。精确边界见 [WP-52](docs/plan/WP-52.md)。

`NativeTurnDriver` 现在提供独立的有界 dark model/tool loop：默认最多 8 个 model turn，hard cap
为 32；每 turn 最多一个 tool call；初始 advertised tool-name set 必须与 registry-name set 完全
相等；每个 request/response 都会校验，并在 permission/tool future 运行前用 worst-case bounded
tool result 对完整下一轮 transcript 做 preflight。model send 与 allowed tool 只走 authorized entry。
refusal、approval-required、parallel call、tool-choice violation、cancel、timeout、budget exhaustion
均为 typed terminal stop；usage 以 saturating addition 聚合，tool side effect 可能发生后的 model
failure 会转成 redacted non-replayable stop，而不是向外返回可 failover error。invalid JSON argument
只生成静态、不回显原文的 tool result，绝不执行。tool description/schema 只是 non-authoritative
model guidance；每个 `NativeTool` 必须校验实际收到的 argument。

driver outcome 不可序列化且 `Debug` 已脱敏，但 driver 不是 `Harness`，也没有生产 factory/runtime
构造它；trusted built-ins、checkpoint/session-commit consumer、approval resume 与 native resume 仍缺。
另有 `AgentProjectionComponents::open` 提供显式 owner-bound one-shot AgentRun projector 路径，同时
封装 raw store。ordinary dispatch 不会打开该数据库，也不会启动 projection 或其他 resident work。

`vyane-service::AgentRunRecoveryDriver` 是另一条显式 fixed-owner、non-`Clone` 的 one-shot seam。
构造时冻结 owner、注入的 store、options，并限制每种 `ControllerKind` 至多一个 trusted adapter；
`recover_once` 会消费 driver。recovery claim 与最终 confirm 都放到 Tokio blocking pool。单次最多
claim 64 个 ticket、并发 poll 16 个 adapter；单个 adapter timeout hard cap 为 60 秒，durable
operation lease hard cap 为 5 分钟，且 lease 必须严格长于 timeout 加 settlement margin。blocking
claim 开始前就建立 caller-local monotonic 保守窗口，因此 claim latency 会被扣除，custom store 的
wall clock 也不能延长 adapter authority。只有 controller 为空的 ticket，或 adapter 对 exact
controller 明确返回 `Gone`，才会进入 `confirm_controller_gone`；report/error 均不含正文，recovery
ticket 也不会跨过 adapter 边界。

单独看这仍不是常驻 worker-health 或执行循环；WP-51 只组合 paired in-process backend。当前仍没有
Process/Remote controller adapter、concrete product operation、session-aware resume、生产 factory/CLI/daemon assembly、
message handback、live pause/resume 或自动 replay。controller adapter 必须在每次 effect 前重验完整
identity；无法排除 identity reuse 时必须无 effect 地返回 `Unavailable`；timeout、drop 或 settlement
failure 后仍须可安全重复。custom store 的 blocking call 无法被强制取消，adapter timeout 只约束
future polling，不能证明不可中断的外部 effect 已停止。精确边界见
[WP-45](docs/plan/WP-45.md)。

`vyane-service::AgentRunExecutionDriver` 以另一条 fixed-owner、non-`Clone`、消费式 one-shot
路径处理 newly due runs。整次 claim 在 Tokio blocking pool 完成后才进入 item work；单次 hard cap
为 64 runs、16 个并发 poll、5 分钟 lease，heartbeat interval 限于 100ms 到 60s 且必须小于
lease。monotonic base 在 claim 前建立。每个 item 生成彼此独立的 256-bit prospective controller
id 与 fingerprint，严格按 claim → durable start → permit → pre-effect heartbeat → first poll
推进。trusted executor 仍须在每个真实 effect 线性化点重验 permit；driver 也会闭合校验 custom
store 每次 transition 的返回值。

只有证明所有 effect 已停止的 `Quiesced` closed outcome 才授权 driver 发起 terminal settle；单个
item future 独占并推进 receipt。在该证明之前，cancel、timeout、panic、drop、`Unknown` 或
heartbeat failure 不授权新的 settlement，并可能保留 `Starting` 或 `Running` 交给 WP-45
exact-identity recovery。blocking settlement 一旦开始便无法中断，且可能在 waiter 被 drop 后仍完成；
custom store 也可能 mutate-then-error，因此 settlement failure report 只能表示结果不确定。
这仍是未接线的 library seam：没有 concrete executor/controller adapter、生产
assembly、message handback、session-aware resume、live pause/resume 或自动 replay。精确边界见
[WP-47](docs/plan/WP-47.md)。

`InProcessAgentComponents` 为上述 one-shot drivers 提供第一组 concrete pairing：进程内每个
owner 全局只允许一个 live backend，即使 competing assembly 使用不同 store pointer 也保守拒绝；
该 backend 绑定一个 store 与 structured operation，并铸造 paired `InProcess` execution/recovery
drivers。exact id/fingerprint matching、基于 `Notify` 的 cancel/exit
observation，以及 fail-closed 的 4096-entry tombstone 上限，避免 late/reused controller 被误认。
durable confirmation 会回收 exact tombstone；失败/不确定 confirmation 保留它，回收后才发生的 late
registration 仍须在 operation 前再次重验 permit。operation 获得 lifetime-bound、non-`Clone` effect
authority，并须在每个 effect 前消费一次 freshly revalidated permit proof。

`ResidentInProcessAgentSupervisor` 可消费该 exact pairing，形成相互隔离的 execution/recovery/completion-publication
polling loop。poll/backoff 均有界；degraded/error/panic cycle 使用 capped exponential backoff；它不创建
task、channel、runtime、payload queue 或 replay policy，也不会自动 enqueue resume。host cancellation
只阻止新 cycle/中断等待，不会作为 AgentRun cancellation 传给已开始的 pass；当前 pass 使用独立 token
drain。强制 drop 仍放弃该保证，custom blocking store 也使 drain 没有固定 wall-clock 上限。这仍是 dark
library driver：没有 concrete product operation、`Process`/`Remote`、protocol API 或 production host。
通用 handback 契约见 [WP-53](docs/plan/WP-53.md)，原 resident 边界见 [WP-51](docs/plan/WP-51.md)。

service 层也增加 principal-derived owner 的 phase-A boundary。`OwnerContextFactory` 冻结 trusted
authenticator/resolver，隐藏 `AuthenticatedPrincipal` 构造，并拒绝 authenticated principal 进入保留
`local` namespace；`OwnerScopedService` 将 owner 同时冻结到 dispatch、single-target stream、history、
session inspect 与 revision-checked reset。REST 在 router assembly 时把 dispatch/broadcast/run/session/
stream 全部冻结进同一个 local scope，但当前 bearer 尚不代表不同 principal。精确边界见
[WP-49](docs/plan/WP-49.md)。

durable task schema v2 已以 `(owner,id)` 作为 snapshot/event/lease/CAS 真相键，并通过事务迁移验证
row/event count、composite FK、schema 与 event sequence high-water。REST output artifact 使用 opaque
owner/task-qualified segment，并只对已验证的 local UUID task 提供 legacy read fallback。内置 task
control 仍只选择 `local`，旧 detached filesystem scaffold 仍是 local compatibility subsystem；这推进了
storage isolation，但不是 multi-user REST 声明。精确边界见 [WP-50](docs/plan/WP-50.md)。

`vyane-broker::ResidentBrokerSupervisor` 是另一条显式、non-`Clone` 的 library driver。它消费自身
进入 `run` future，并发持有互不重叠且 owner/store-bound 的 delivery lanes、message maintenance、
message projection 与 AgentEvent projection。batch、总 delivery concurrency 与指数 error backoff
均经过校验且有硬上限；单个 lane 的 error/panic 不会停止其他 lane 或 projector。driver 不创建
detached task、channel、runtime 或第二队列：embedding caller 必须提供 Tokio runtime 与 cancellation
token，并 await 该 future。取消只在 cycle 之间观察；已经开始的操作会 drain 当前有界 cycle，
而直接 drop 外层 future 会放弃 graceful-drain 保证。

目前没有 service、CLI 或 daemon 的生产 assembly 构造这个 driver。它不执行或恢复 AgentRun，
不提供 controller/message handback，不实现 A2A/Channels，也不增加 live pause/resume 或自动 replay。
精确边界见 [WP-44](docs/plan/WP-44.md)。

### 协议入口

Vyane 支持三种可互换的前端，全部共享同一个 `vyane-service` 层：

| 协议 | 命令 | 用途 |
|------|------|------|
| **CLI** | `vyane dispatch --target prod "task"` | 交互式 / 脚本化一次性运行 |
| **REST API** | `vyane serve --addr 127.0.0.1:9721` | 任意 HTTP 客户端编程访问 |
| **MCP** | `vyane mcp` | 让其他 agent（Claude、Codex 等）作为工具调用 |

MCP 目前有 6 个工具：dispatch、broadcast、history、sessions、`vyane_route`、`vyane_check`。
generic success payload 有 1 MiB 上限；两个新增 diagnostic 使用更小、更严格的边界。
`vyane_route` 是 Rust 侧确定性预览扩展，不代表与固定参考基线存在同名等价能力；
`vyane_check` 只做静态配置分析，不探测网络 endpoint、不启动 harness，也不验证真实 credential。
所有 success result 共用 generic 输出上限，但旧 execution tool 的输入字段尚未具备 diagnostics
那样统一的逐字段预算。若执行已完成但完整 detail 超过上限，dispatch/broadcast 会返回带 run id 的
有界 receipt，并明确 `operation_status="completed"`、`detail_omitted=true`；调用方不得把它当成未执行
而自动重试。

`vyane serve` 会拒绝非 loopback bind；每次启动都会生成 256-bit bearer capability，并把它原子写入
启动日志所示的私有 `serve.token`（Unix 上为 mode-0600，数据目录为 mode-0700），所有 endpoint
（包括 health）均要求该 bearer。Router 还会
拒绝非 loopback `Host`/`Origin` 与 cross-site browser request，阻断 DNS rebinding。run/session 响应
使用 allowlisted public view，不直接序列化内部 store record。恶意同 UID 进程仍可读取 token，
因此它是本地单用户控制面，不是 hostile same-UID 或多用户安全边界；调用 curl 时应通过 stdin
config 读取 token，避免把 bearer 放进 argv 或环境变量。非 Unix 平台应把 `VYANE_DATA_DIR` 放在
系统管理的用户私有目录；Vyane 不会替调用方给自行选择的共享目录重设平台 ACL。

### 常驻 workflow daemon

workflow daemon 是独立于 `vyane serve` 的本地控制面。daemon 接受 workflow 后，
提交它的 CLI 退出也不会终止这次运行：

```sh
vyane daemon start                         # 后台启动并等待就绪
vyane daemon status --json
vyane workflow submit workflow.toml --var env=dev
vyane workflow status <uuidv7> --json
vyane workflow cancel <uuidv7>
vyane daemon stop
```

`daemon run` 会在前台运行同一个 supervisor。listener 只接受 loopback 地址；所有
endpoint（包括 `/health`）都要求每次启动新生成的 256-bit bearer token。token 与
owner-only daemon descriptor 分文件保存。控制 API 包括 `POST /v1/workflows`、
`GET /v1/workflows/:id` 和 `POST /v1/workflows/:id/cancel`，且不启用宽松 CORS。
这用于防止意外的浏览器和跨进程访问，不是抵御同一 OS 用户下恶意代码的 sandbox。

客户端会先认证记录中的精确 daemon，再读取本地 workflow source。它把 workflow
TOML 与所有声明的 `prompt_file` 作为只存在于请求和 live execution 中的 source
bundle 发送出去；daemon 不会在自己的文件系统上解析这些 source path。语义限制为：
TOML 1 MiB、每个 prompt 4 MiB、bundle 总计 16 MiB、最多 128 个 prompt entry，
每条 bundle path 最多 4,096 bytes。变量最多 128 个，key 最多 256 bytes、单个
value 最多 1 MiB、总计最多 4 MiB。请求还携带 canonical submission working
directory（最多 4,096 bytes）：未设置或相对的 step `workdir` 从这里解析，绝对
`workdir` 保持不变。

客户端默认生成 canonical UUIDv7，并在发送请求前把它输出到 stderr。使用
`--id <uuidv7>` 可对结果做核对或进行幂等重试：只有 daemon workflow scope、
normalized source、working directory 与 variables 全部一致时才返回已有 task；
任何不一致都会冲突，且不会重放旧 payload。task id 与 workflow journal id 完全一致。
daemon 重启时会按精确 controller identity 清理遗留执行，并将废弃的 daemon task
标记为 `interrupted`，不会自动 resume 或 replay。前台 `workflow resume` 仍是显式的、
面向 journal 的命令。

### 智能路由

`vyane route "task text"` 预览路由决策；`vyane dispatch --target auto` 会执行同一
决策，并把 profile/provider/model、tier、effort、score、intent、tag 记录进 ledger。路由器从结构性
信号（changed files、dependency edges、retry count、prompt length、inferred tags）
计算复杂度评分，映射到 economy / mainline / frontier 三档，再按 profile 的
`tier`/`tags`/`stage` 元数据解析偏好：

```
vyane route "write an ADR for the system architecture" --changed-files 20
vyane route "simple task" --tier frontier
vyane dispatch "fix the parser" --target auto
vyane dispatch "review auth" --target auto --no-frontier
```

Workflow step 也支持 `target = "auto"`，并可在 `[step.route]` 中设置
`stage`、`tier`、`tags`、`candidates`、`allow_frontier`、`effort`；选择发生在模板 prompt
渲染完成后。`allow_frontier = false` 会对选中 profile 及其 failover 链整体
fail-closed。名为 `auto` 的普通 profile 可用 `profile:auto` 显式选择；provider
名称若以保留前缀 `profile:` 开头，可用 `target:<provider>/<model>` 消歧。
`routing.provider`、`routing.effort` 等决策标签由 Vyane 保留写入，调用方不能伪造。
`effort` 是 `low`/`medium`/`high`/`xhigh` 的 closed typed 值，只允许用于 deferred
single target；explicit target 或 `fan_out` 携带 route hints 会在 dispatch 前 fail closed。
有效 effort 的优先级为 workflow explicit effort > 选中 profile 配置的 effort > decision tier
默认值，并统一写入整条 failover chain 的 canonical `routing.effort`。recorded、detached、daemon
幂等重试及 journal resume 都冻结这个有效值；非法值不会回显输入，普通 `effort` label 也不能伪造
保留字段。精确边界见 [WP-46](docs/plan/WP-46.md)。`WorkflowPlan` schema v1 现已成为
compile、prepare、run、resume 共用的有界、严格、与源文件路径无关的执行 payload，冻结物化 DAG、
typed target/route、无损 timeout、请求能力摘要、source claim 与 canonical plan checksum。它不是
安全的公开 view；checksum 也不是认证、来源证明或执行授权。plan-only continuation 必须与 journal
中的 digest 精确匹配；只有仍持有 source 的兼容 API 才能在核验 exact source hash 后迁移旧 journal。
精确边界见 [WP-54](docs/plan/WP-54.md)。这只关闭了 shared typed-plan 前置缺口；dynamic control
flow、nested workflow、shared budget、compatibility frontend、replay/fork、CLI/REST/MCP plan wire、
净化的跨实现 fixtures 与 production-complete model-tier policy 仍未完成。
detached 任务会通过一次性的私有 stdin pipe 把不含 secret 的目标快照交给 worker，
不会把快照或请求正文落盘；endpoint、extra passthrough 和 harness env-inject 仅在
该临时快照中使用摘要/结构，不携带原始值。worker 若发现配置漂移会拒绝执行。

### Solution-review workflow

`vyane review` 在已有的 workflow 引擎上跑一个三步 solution-review workflow：
implement → fan-out review → synthesize。它还不是原仓包含结构化 git diff/PR、finding
artifact、verifier 和 publication 的 review 产品。

```
vyane review 'implement a sorting function' \
  --implementer sonnet \
  --reviewers opus,gpt \
  --synthesizer opus
```

## 架构

```
       CLI              REST API           MCP
   vyane dispatch     vyane serve       vyane mcp
        │                  │                  │
        └──────────┬───────┴──────────┬───────┘
                   ▼                  ▼
              vyane-service（共享 facade）
                   │
                   ▼
            ┌─────────────┐
            │   kernel    │  early execution id,
            │  dispatch / │  whole-chain admission,
            │  broadcast  │  attempts + RunRecord
            └──────┬──────┘
      ┌────────────┴────────────┐
      ▼                         ▼
direct-http protocol         cli-wrap harnesses
clients (ChatClient)         (Harness, scrubbed env)
OpenAI Chat / Responses,     Claude Code, Codex CLI, …
Anthropic Messages                    │
      └────────────┬──────────────┘
                   ▼
       append-only JSONL ledger
        (digests, attempt trail)
          + session store
       SQLite task metadata
     (CAS lifecycle, no payload)
  SQLite transactional message store
（message/delivery/receipt/outbox 真相）
        bounded delivery broker
（replay-safe adapter + 无正文 EventLog 投影）
     SQLite AgentRun / worker store
（lease、topology、recovery、tree cancel、无正文 outbox）
```

16 个 crate，kernel 只依赖 `vyane-core` traits。详见
[ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 使用方法

配置是一个 TOML 文件。完整结构见 [`profiles.example.toml`](profiles.example.toml)。

```toml
[providers.anthropic]
base_url      = "https://api.anthropic.com"
api_key_env   = "ANTHROPIC_API_KEY"
auth_style    = "x_api_key"
protocol      = "anthropic_messages"
default_model = "a-capable-anthropic-model"

[profiles.review]
provider = "anthropic"
protocol = "anthropic_messages"
harness  = "none"
model    = "a-capable-anthropic-model"
```

```sh
vyane check                              # 校验配置
vyane dispatch "task" --target review    # 派发
vyane dispatch "task" --target auto      # 自动分档并真实派发
vyane broadcast "task" --targets a,b,c   # 扇出
vyane dispatch "task" --target x --stream  # 流式
vyane route "task"                       # 路由 dry-run
vyane review "task" --implementer ...    # 评审流水线
vyane serve                              # REST API 服务器
vyane mcp                                # MCP server
```

## 设计原则

- **Boring dependencies**：用广为人知的 crate，不花哨。
- **Secrets never serialize**：凭据类型在类型层面不可序列化。
- **声明和 evidence 不是 authority**：capability manifest/scope 可序列化用于 audit；prepared
  plan、pinned directory handle 与 active permit 保持 process-local，并在 provenance drift 时 fail closed。
- **ledger 存 digest 不存 prompt**。
- **子进程环境默认 scrub**。
- **没有隐藏 timeout**。
- **记录带 owner**：多用户隔离从第一天起就有。

## 文档

- [Architecture](docs/ARCHITECTURE.md) — four-layer model、crate map、dispatch 生命周期、streaming、failover 语义。
- [Architecture decisions](docs/adr/README.md) — 已接受的产品差异及仍待完成的验收门槛。
- [Roadmap](docs/ROADMAP.md) — v0.1 ~ v0.4 里程碑。
- [原始 Vyane 对齐基线](docs/parity/ORIGINAL-VYANE-PARITY.md) — 固定双仓能力矩阵与验收门槛。
- [Contributing](CONTRIBUTING.md) — toolchain、检查项、PR 约定、crate map。

## 许可协议

在 [MIT](LICENSE-MIT) 与 [Apache License, Version 2.0](LICENSE-APACHE) 之间任选其一。
