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

具体带来三件事：**天然干净的子进程环境**（子 agent 从 scrub 后的基线启动，凭据不泄漏）；
**永不跨 provider 泄漏 model id 的 failover 链**；以及 **append-only JSONL run ledger**
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

**v0.1 + v0.2 + v0.3 全部完成，v0.4 进行中。API 仍处于发布前不稳定阶段。**

| 能力 | crate | 状态 |
|------|-------|------|
| 核心类型系统 | `vyane-core` | [x] |
| config & profiles | `vyane-config` | [x] |
| OpenAI Chat/Responses + Anthropic Messages | `vyane-protocol` | [x] |
| Claude Code + Codex CLI harnesses | `vyane-harness` | [x] |
| dispatch / broadcast / failover kernel + streaming | `vyane-kernel` | [x] |
| JSONL ledger & sessions | `vyane-ledger` | [x] |
| 声明式 workflow 引擎（DAG + journal/resume） | `vyane-workflow` | [x] |
| 后台任务（`--detach` + `task` 命令） | `vyane-cli` | [x] |
| CLI（11 个子命令） | `vyane-cli` | [x] |
| 共享服务层 | `vyane-service` | [x] |
| 确定性路由 | `vyane-router` | [x] |
| MCP server | `vyane-mcp` | [x] |

### 协议入口

Vyane 支持三种可互换的前端，全部共享同一个 `vyane-service` 层：

| 协议 | 命令 | 用途 |
|------|------|------|
| **CLI** | `vyane dispatch --target prod "task"` | 交互式 / 脚本化一次性运行 |
| **REST API** | `vyane serve --addr 127.0.0.1:9721` | 任意 HTTP 客户端编程访问 |
| **MCP** | `vyane mcp` | 让其他 agent（Claude、Codex 等）作为工具调用 |

### 智能路由

`vyane route "task text"` 展示路由器会选什么目标——不实际 dispatch。路由器从结构性
信号（changed files、dependency edges、retry count、prompt length、inferred tags）
计算复杂度评分，映射到 economy / mainline / frontier 三档，再按 profile 的
`tier`/`tags`/`stage` 元数据解析偏好：

```
vyane route "write an ADR for the system architecture" --changed-files 20
vyane route "simple task" --tier frontier
```

### 评审流水线

`vyane review` 在已有的 workflow 引擎上跑一个三步多模型评审流水线：
implement → fan-out review → synthesize：

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

12 个 crate，kernel 只依赖 `vyane-core` traits。详见
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
- **ledger 存 digest 不存 prompt**。
- **子进程环境默认 scrub**。
- **没有隐藏 timeout**。
- **记录带 owner**：多用户隔离从第一天起就有。

## 文档

- [Architecture](docs/ARCHITECTURE.md) — four-layer model、crate map、dispatch 生命周期、streaming、failover 语义。
- [Roadmap](docs/ROADMAP.md) — v0.1 ~ v0.4 里程碑。
- [Contributing](CONTRIBUTING.md) — toolchain、检查项、PR 约定、crate map。

## 许可协议

在 [MIT](LICENSE-MIT) 与 [Apache License, Version 2.0](LICENSE-APACHE) 之间任选其一。
