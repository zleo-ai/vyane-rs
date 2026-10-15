# Vyane

**用一个 Rust CLI 和编排内核运行、协调 AI 模型任务。**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

源代码基线：`b46c194` · 工作区版本：`0.1.0` · 更新日期：2026-10-15。
以英文版为准。本译文为机器辅助草稿，不声称经过母语使用者审校。维护规则见[翻译政策](docs/TRANSLATING.md)。

Vyane 可将任务派发到一个已配置的目标、广播到多个目标，或按显式指定的故障转移链依次尝试。目标可通过 Claude Code 或 Codex CLI 执行适配器运行，也可直接调用 HTTP 模型端点。它面向构建本地智能体工作流的开发者，以及需要显式路由、执行权限、取消操作和运行记录的 Rust 应用开发者。

![抽象终端窗口通过铜色协调中心连接到结果卡片。](docs/assets/vyane-hero.webp)

*AI 生成的概念插图，并非架构图。[资产说明](docs/assets/README.md)。*

## 状态与限制

项目处于早期阶段，需要从源码构建，API 尚不稳定。Cargo 版本为 **0.1.0**；路线图中的 v0.2–v0.4 等标签表示实现里程碑，并非已发布版本。截至 2026-10-15，仓库没有 Git 标签或 GitHub Releases。请使用下方的源码快速上手流程；尚未验证通过包注册表安装的分发方式。[发布检查清单](docs/RELEASING.md)记录了一项当前阻塞：工作区有 18 个 crate，但发布顺序仅列出 17 项，缺少 `vyane-quota`。

| 公开源码已提供的能力 | 边界 |
| --- | --- |
| 单目标派发、多目标广播、显式故障转移、流式输出和本地历史 | 直接 HTTP 目标提供模型响应，不提供文件系统或 shell 工具。 |
| 确定性路由和声明式工作流 | 路由依赖你配置的 profile，并不自动保证结果质量。 |
| CLI、仅监听回环地址的 REST API 和基于标准输入/输出的 MCP | 各入口暴露的功能子集不同；参见 [CLI 指南](docs/CLI-GUIDE.md)。 |
| 持久化任务、本地消息/目标，以及 Linux 常驻 Process/Native 运行 | Native 运行均为全新、无会话的执行；审批后恢复、原生会话恢复和重启后自动重放尚未完成。 |
| 显式权限、经过清理的子进程环境和运行核算 | CLI 沙箱由所选执行适配器负责实施。owner 字段和回环认证无法隔离同一系统用户下的恶意进程，也不提供多用户隔离。 |

CI 覆盖 Linux 和 macOS；部分常驻执行和沙箱功能仅支持 Linux。不声称支持 Windows。模型访问权限、凭据和任何编程智能体 CLI 均需另行准备。调用服务商可能产生费用；构建和下方离线检查不会调用模型。

## 快速上手

安装包含 Cargo 的稳定版 Rust，以及平台所需的 C/C++ 构建工具链。声明的最低 Rust 版本为 1.88。仓库选用 stable 工具链，并包含 rustfmt 和 Clippy。在仓库根目录运行：

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

版本应输出 `vyane 0.1.0`。该命令将程序安装到当前检出目录，不会替换 PATH 中其他位置的 `vyane`。请保持当前 shell 开启，以便继续运行后续命令。若要复现本文档对应的快照，请在安装前检出 `b46c194`；`main` 会持续变化。

### 无需凭据的首次运行

```sh
cat > ./target/offline-config.toml <<'TOML'
[providers.offline]
base_url = "https://example.invalid/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "offline-placeholder"

[profiles.offline]
provider = "offline"
harness = "none"
model = "offline-placeholder"
TOML
"$VYANE" --config ./target/offline-config.toml check
"$VYANE" --config ./target/offline-config.toml route "Explain this function" --json
```

`check` 应报告配置有效，并包含 `offline` profile。`route` 返回的 JSON 中应有 `"selection_key": "offline"` 和 `"model": "offline-placeholder"`，但不会派发任务。这是一个离线配置示例，不是模拟的模型响应，也不是可用的服务商；不要向其故意设置为无效的端点派发任务。

### 配置真实目标

创建 `./target/model-config.toml`，结构如下。将示例端点和模型替换为服务商支持的值；`example.invalid` 特意使用了不可用的示例域名。

```toml
[providers.demo]
base_url = "https://example.invalid/v1"
api_key_env = "VYANE_DEMO_API_KEY"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "YOUR_PROVIDER_MODEL_ID"

[profiles.chat]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_PROVIDER_MODEL_ID"
```

通过你常用的秘密管理工具或 shell 环境设置 `VYANE_DEMO_API_KEY`；切勿将其值写入 TOML 或提交到仓库。然后运行：

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

第一条命令检查配置和凭据是否存在，并不能证明端点接受你的凭据或模型。替换占位值后，第二条命令会发送真实请求。离线快速上手验证不包含真实服务商调用成功的验证。有关编程执行适配器、权限、多个 profile 和故障转移，请参阅[配置示例](profiles.example.toml)和 [CLI 指南](docs/CLI-GUIDE.md)。

## 目标如何工作

![CLI、REST 和 MCP 调用服务层与内核；每个目标绑定 provider、protocol、harness 和 model；执行产生运行记录。](docs/assets/target-flow.svg)

| 字段 | 含义 |
| --- | --- |
| `provider` | 端点、凭据来源和计费身份。 |
| `protocol` | 通信格式：OpenAI Chat、OpenAI Responses 或 Anthropic Messages。 |
| `harness` | 执行适配器，例如 Claude Code 或 Codex CLI；`none` 表示直接 HTTP 调用。 |
| `model` | 服务商命名空间内的模型标识符。 |

一个 profile 将这四个字段绑定在一起。故障转移链中的每个目标会分别解析，因此不会悄悄将某个模型标识符复用于另一家服务商。内核依赖共享 trait；服务层负责组装具体适配器。依赖关系与授权边界的细节见[架构文档](docs/ARCHITECTURE.md)。

## 示例与参考

配置好下列命令中引用的 profile 后，可按这些示例继续使用：

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

广播示例需要另一个名为 `review` 的 profile。提示词按字面文本处理：在提示词中提到文件，不会自动将文件内容附加到直接 HTTP 请求中。[CLI 指南](docs/CLI-GUIDE.md)涵盖工作流、守护进程生命周期、REST 认证、MCP、本地消息和目标。可对各子命令使用 `--help`，查看当前支持的命令接口。

## 与 Python Vyane 的关系

本仓库是对原始私有 Python Vyane 系统中部分能力的公开 Rust 实现，并不能直接替代原系统。迁移前需逐项检查 Python 命令、配置、已存储会话、工作流和运维集成。在没有文档化迁移路径的情况下，不要让两个实现使用同一个状态目录。

[公开能力矩阵](docs/parity/ORIGINAL-VYANE-PARITY.md)汇总了当前状态。矩阵按 8 个域追踪 53 个能力项：7 个 implemented、23 个 partial、12 个 missing、9 个刻意不同或待决策、2 个 planned。其中 implemented 表示已实现，partial 表示部分实现，missing 表示缺失，planned 表示已规划。[脱敏清单](docs/parity/fixtures/v1/manifest.json)包含 5 个测试套件中的 44 个用例：30 个经规范化后匹配，14 个仍有未解决差异。这些是范围有限的行为检查，不能换算成整个系统的对齐百分比。运行以下命令重新计算公开的 Rust 侧证据：

```sh
python3 .github/scripts/parity-report.py --format markdown
```

该命令运行 Rust 固定用例检查，并校验已存储的脱敏证明记录；不会运行私有参考实现。公开贡献者不需要访问私有源码。未完成工作见[路线图](docs/ROADMAP.md)，公开仓库的变化见[变更日志](docs/CHANGELOG.md)。

## 参与贡献与获取帮助

请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)，了解检查项和审查规则。可复现的缺陷报告、更精简的示例、翻译修正，以及针对文档中能力缺口的测试，都是有用的贡献。开始可能重叠的工作前，请搜索 [Issues](https://github.com/zleo-ai/vyane-rs/issues) 和[开放的 PR](https://github.com/zleo-ai/vyane-rs/pulls)。报告中请提供源码修订号、操作系统、Rust 版本、最小复现步骤和脱敏输出。切勿包含密钥、私有提示词或个人文件系统路径。

涉及漏洞时，请先遵循 [SECURITY.md](SECURITY.md)，再发布细节。翻译覆盖范围和审校状态见 [TRANSLATING.md](docs/TRANSLATING.md)；发布和版本维护规则见 [RELEASING.md](docs/RELEASING.md)。

## 许可证

采用 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE) 双许可证，可任选其一。贡献适用相同条款；参见[贡献说明](CONTRIBUTING.md#licensing)。
