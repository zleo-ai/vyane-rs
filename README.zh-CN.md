# Vyane

**用一个 Rust CLI 调用编程代理和模型 API。**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

如果你同时使用多个编程代理或模型服务，Vyane 可以让你给目标起好名字，把任务交给其中一个或多个，并集中保留结果和尝试记录。编程任务可交给 Claude Code 或 Codex CLI，也可以直接调用模型 API 获取文本回答。

> **开发早期。** 目前从源码安装，API 尚不稳定，部分能力仍未完成或缺失。原生会话恢复和重启后自动重放尚未完成。请先了解[当前边界](docs/PROJECT-STATUS.md)。

## 看看它怎么运行

[运行终端演示](docs/EXAMPLES.md)：安装后的 CLI 先执行单个任务，再向两个目标广播，最后在模拟的主目标故障后切换成功。演示使用本地 HTTP 夹具，不需要密钥，也不调用真实模型。

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## 三个用途

| 你想要…… | 使用 | 得到什么 |
| --- | --- | --- |
| 指定代理或模型执行任务 | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | 一份结果和本地运行记录。编程代理需要另行安装和配置。 |
| 比较同一问题的不同回答 | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | 并发执行并分别返回结果，由你判断答案。 |
| 调用失败时尝试配置好的备用目标 | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | 对符合条件的错误按顺序切换，记录每次尝试。这不是会话接力。 |

## 快速开始

**1. 从源码安装。** 需要 stable Rust/Cargo（最低 Rust 1.88）和 C/C++ 构建工具链。CI 覆盖 Linux 和 macOS；目前不声明支持 Windows。

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

预期版本为 `vyane 0.1.0`。二进制文件位于当前仓库内，不会覆盖 PATH 中已有的 `vyane`。请保留这个终端会话。

**2. 不用密钥，先得到一个结果。** 内置示例只预览路由，不发送模型请求。

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

检查是否出现 `offline` 配置，以及 JSON 中的 `"selection_key": "offline"`。示例地址故意无效，不要向它派发任务。

**3. 接入自己的目标。** 按[服务商配置说明](docs/QUICKSTART.md#configure-a-real-target)完成设置，再试[三个示例](docs/EXAMPLES.md)。模型访问权限和凭据由你提供，真实调用可能产生服务商费用。离线预览不验证真实服务是否可用。

## 现在能做什么

| 源码中已有的能力 | 边界 |
| --- | --- |
| 单目标执行、多目标广播、显式故障切换和流式输出 | 直接 HTTP 调用返回模型文本，不会自动附加本地文件，也不提供终端工具。 |
| Claude Code 和 Codex CLI 适配器 | 通过 CLI 子进程接入，未接入 Codex App Server RPC 或直接的 Claude Agent SDK。你需自行提供 CLI 和访问权限；沙箱由所选代理执行。 |
| 按配置路由和本地运行历史 | 路由依据配置选择目标，不保证答案质量。 |
| CLI、REST 和 MCP 入口 | 各入口覆盖的能力不同，见 [CLI 指南](docs/CLI-GUIDE.md)。 |

常驻执行、持久化工作流和本地目标管理仍在演进。原生运行每次都是新运行，没有会话连续性；原生审批恢复、会话恢复和重启后自动重放仍未完成。owner 字段和回环地址认证不能隔离恶意的同用户或多用户访问。使用这些能力构建应用前，请阅读[状态与限制](docs/PROJECT-STATUS.md)。

## 深入了解

- [架构与目标模型](docs/ARCHITECTURE.md) · [配置参考](profiles.example.toml) · [CLI、REST 和 MCP 指南](docs/CLI-GUIDE.md)
- [能力矩阵与可复现证据](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [路线图](docs/ROADMAP.md)
- [更新日志](docs/CHANGELOG.md) · [版本与发布规范](docs/RELEASING.md)

这是原有私有 Python Vyane 系统部分能力的公开 Rust 实现，不能直接替换原系统。命令、配置、已保存的会话和工作流都需要分别核对迁移方式。没有明确迁移方案时，不要共享状态目录。公开贡献不需要访问私有源码。

## 贡献与求助

可以先试一个示例、报告可复现的问题、改进翻译，或补齐已记录的能力缺口。先查看 [Issues](https://github.com/zleo-ai/vyane-rs/issues) 和[进行中的 PR](https://github.com/zleo-ai/vyane-rs/pulls)，再阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。报告中请注明源码版本并提供脱敏输出，不要附上密钥、私密提示词或个人路径。安全漏洞请按 [SECURITY.md](SECURITY.md) 披露。

## 许可证

可任选 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE)。贡献采用相同条款。

---

源码基线：`fe43941` · 工作区版本：`0.1.0` · 更新：2026-10-15。英文版为权威来源；本版为机器辅助翻译，尚待母语人工审校，详见[翻译覆盖与审校状态](docs/TRANSLATING.md)。路线图里程碑不等于已发布版本。
