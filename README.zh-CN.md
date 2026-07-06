# Vyane

**A multi-model agent-orchestration kernel, in Rust.**
（一个用 Rust 写的多模型 agent 编排内核。）

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#许可协议)
&nbsp;·&nbsp; [English](README.md)

Vyane 是一个内核——以及内核之上的一个 CLI——用来在**两类目标**之间 dispatch、
broadcast 和 failover 任务：一类是 coding-agent harness（Claude Code、Codex
CLI 等），一类是裸的 HTTP model endpoint。给它一个任务加一个 target，它就把任务
跑起来；给它多个 target，它就并发扇出（broadcast）或按序故障转移（failover）。一次
运行究竟是在带文件系统和工具的 coding CLI 里发生，还是作为一次普通的 HTTP chat
completion，只是 target 上的一个字段——不是另一个工具、另一份配置、另一套心智模型。

真正的差异点是**四层 target 模型**（four-layer target model）。*provider* /
*protocol* / *harness* / *model* 是四个独立的维度，彼此不混淆：provider 是谁提供
endpoint、key 和账单；protocol 是 wire format；harness 是执行外壳（直连 chat 则没有
harness）；model 是实际推理的模型。于是：**relay 不是 protocol**（它是恰好讲某种
protocol 的 provider），**coding CLI 不是 provider**（它是仍然需要一个 provider 的
harness），**一个 model id 只在它所属的那个 provider 内部有效**。这四者从配置一路到
run ledger 始终是四个独立字段——正是这一点，让 Vyane 在那些偷懒工具会悄悄出错的边界
上做对事情。

具体带来三件事：**天然干净的子进程环境**——子 agent 从一份被 scrub 过的基线环境启动，
调用方 session 的凭据和 base-URL override 绝不泄漏进去；**永不跨 provider 泄漏 model
id 的 failover 链**——链上每个元素都被独立、完整地解析，回退项永远用它所运行的那个
provider 名下的 model；以及一个**append-only 的 JSONL run ledger**——记录 prompt 的
*digest*（而非 prompt 正文）、每次 dispatch 的完整 attempt 轨迹，并且**从第一天起就带
owner scope**，让多用户隔离永远不需要事后改 schema。

## 起源

`vyane-rs` 是 Vyane 的开源 Rust 实现。Vyane 本体是一套私有的个人 AI-OS 执行底座，自
2026 年初起就在跑真实的多模型开发流水线。现在它在开源世界里被逐能力重建，并持续跟随私有
系统演进——所以这里的设计反映的是日常使用中真正扛住了的东西，而不是凭空的推测。

## 这个仓库是怎么写出来的

这个仓库由一支被编排调度的 AI coding agent 舰队开发：不同的前沿模型负责写代码、互相对抗性
交叉审查（adversarial cross-review）、再修复审查中发现的问题，整个过程置于人类主导的架构
与整合关卡（integration gate）之下。每一次合并都经过独立的跨模型审查，外加
[CONTRIBUTING.md](CONTRIBUTING.md) 里描述的 `cargo fmt` / `clippy` / `cargo test` 检查
关卡——没有任何改动仅凭写它的那个模型自己说了算就能落地。

## 状态

**v0.1 表层能力端到端完整。API 仍处于发布前不稳定阶段，会不加通知地变动。** 下表每一个
crate 都已实现并有测试覆盖；CLI 今天就能跑真实的 dispatch/broadcast/failover。

| 能力 | crate | 状态 |
|------|-------|------|
| 核心类型系统（four-layer model、traits、errors、env policy） | `vyane-core` | [x] |
| config & profiles | `vyane-config` | [x] |
| OpenAI-Chat + Anthropic-Messages clients（含 Responses 非流式） | `vyane-protocol` | [x] |
| Claude Code + Codex CLI harnesses | `vyane-harness` | [x] |
| dispatch / broadcast / failover kernel | `vyane-kernel` | [x] |
| JSONL ledger & sessions | `vyane-ledger` | [x] |
| CLI（check / dispatch / broadcast / history / sessions） | `vyane-cli` | [x] |
| workflow engine | — | [ ] (v0.2) |
| daemon & async tasks | — | [ ] (v0.2) |
| MCP server | — | [ ] (v0.3) |

## 架构

```
                        caller (CLI / library)
                                 │
                                 ▼
                          ┌─────────────┐
                          │   kernel    │  resolve target chain,
                          │  dispatch / │  attempt loop, failover,
                          │  broadcast  │  assemble RunRecord
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
```

kernel 只依赖 `vyane-core` 里的 traits 和类型；具体的 client、harness、ledger 都藏在这
些 trait 背后，在 CLI 层被组装起来。九个 crate：

| crate | 职责 |
|-------|------|
| `vyane-core` | four-layer target model、capability traits、error taxonomy、env policy——所有其它 crate 共用的词汇 |
| `vyane-config` | TOML config + profiles；把一个 profile（及其 failover 链）解析成 `BoundTarget` |
| `vyane-provider` | provider 注册表；endpoint、auth style、每个 provider 的 env 注入规则 |
| `vyane-protocol` | HTTP 协议的 `ChatClient` 实现（OpenAI Chat / Responses、Anthropic Messages） |
| `vyane-harness` | 包装 coding CLI 的 `Harness` 实现，headless 调用 + 进程组控制 |
| `vyane-kernel` | 编排状态机：dispatch、broadcast、failover 判定、run-record 组装 |
| `vyane-ledger` | JSONL `Ledger` + 文件系统 `SessionStore`，成本估算 |
| `vyane-router` | target 选择 / 路由策略（后续成长为 pluggable routing） |
| `vyane-cli` | 组装者与入口：把各 crate 接线到一个命令行 UI 背后 |

## 使用方法

配置是一个 TOML 文件（用户默认存于对应平台的配置目录——macOS 上是
`~/Library/Application Support/vyane/config.toml`——再与 `.vyane/config.toml` 的项目
override 合并）。完整结构见 [`profiles.example.toml`](profiles.example.toml)；一个
provider 加一个 profile 长这样：

```toml
[providers.anthropic]
base_url      = "https://api.anthropic.com"
api_key_env   = "ANTHROPIC_API_KEY"   # 只存 env var 名；配置里不放 key 本身
auth_style    = "x_api_key"           # bearer | x_api_key
protocol      = "anthropic_messages"
default_model = "a-capable-anthropic-model"

# provider + protocol + harness + model 的具名捆绑 → 一个 BoundTarget。
[profiles.review]
provider = "anthropic"
protocol = "anthropic_messages"
harness  = "none"                     # "none" = 直连 HTTP chat，无 workspace
model    = "a-capable-anthropic-model"
```

然后，在 shell 里：

```sh
# 校验配置、解析每个 profile、探测 harness 二进制和 env var 是否就位。
vyane check

# 把一个任务 dispatch 到一个 target（profile 名）。
vyane dispatch "review this diff" --target myprofile

# 把同一个任务并发 broadcast 到多条 target 链。
vyane broadcast "compare approaches" --targets a,b,c
```

下面是 `vyane check` 在一份含两个 provider、三个 profile 的配置上的真实输出样例（其中一个
profile 所需的 env var 被故意留空，用来展示缺 key 时的告警长什么样）：

```
config files:
  .vyane/config.toml (loaded)
providers:
  anthropic: anthropic_messages default_model=a-capable-anthropic-model
  openai: openai_chat default_model=a-fast-openai-model
profiles:
  builder: anthropic/a-capable-anthropic-model via claude-code (anthropic_messages)
  codex: warning: provider requires environment variable `OPENAI_API_KEY` for its API key, but it is not set
  review: anthropic/a-capable-anthropic-model (anthropic_messages)
harnesses:
  claude-code: available
  codex-cli: available
profile environment:
  builder: ANTHROPIC_API_KEY present
  codex: OPENAI_API_KEY missing
  review: ANTHROPIC_API_KEY present
```

## 设计原则

- **Boring dependencies。** 用广为人知、行为可预期的 crate；能用一个普通文件解决的，不
  上花哨基础设施。
- **Secrets never serialize。** 凭据类型在类型层面就不可序列化；config 只存 env var 的
  *名字*，从不存值。
- **ledger 存 digest，不存 prompt。** run 账目记录 prompt digest（外加可选的短
  preview），而不是 prompt 正文。
- **子进程环境默认 scrub。** harness 从最小基线 env 加一份显式的 per-target 注入集启动；
  继承完整父环境是 opt-in。
- **没有隐藏 timeout。** 长 agentic 运行本就可能跑几个小时；timeout 按任务 opt-in，绝不
  作为静默默认值。
- **记录带 owner。** 每条 run 和 session 从第一天起就带 owner 字段，多用户隔离永远不是
  事后补丁。

## 文档

- [Architecture](docs/ARCHITECTURE.md) —— four-layer model、crate map、dispatch
  生命周期、env policy、failover 与 ledger 语义。
- [Roadmap](docs/ROADMAP.md) —— v0.1、v0.2、v0.3 的里程碑。
- [Contributing](CONTRIBUTING.md) —— toolchain、检查项、PR 约定。

## 许可协议

在 [MIT](LICENSE-MIT) 与 [Apache License, Version 2.0](LICENSE-APACHE) 之间任选其一。
除非你另行声明，你提交并被纳入的任何贡献都按上述双许可授权，不附加其它条款。
