# AGENTS.md

> **本仓为公开仓（`zleo-ai/vyane-rs`）——任何私有上下文（私有路径 / 设备名 / 中转账号池 / 看板内部 / 个人信息 / 雇主）不得写入本仓任何文件。** 私密 / 内部上下文一律只留在私有主线仓，不进本仓。

## 项目

`vyane-rs` 是 Vyane 的开源 Rust 实现线：一个多模型 agent 编排内核 + 单一 CLI，把任务的分发 / 广播 / 失败切换，统一到 coding-agent harness（Claude Code、Codex CLI…）与裸 HTTP 模型端点之上。核心是四层 target 模型——provider / protocol / harness / model 四个轴互不混淆。它逐能力重建一个私有个人 AI-OS 执行底座，不是其完整镜像。

## 给在本仓工作的 agent

- **公开安全第一**：提交前自查 diff，不含任何密钥、token、私有主机名 / 路径、账号池、内部看板 ID、真实姓名 / 雇主等。凭据只走环境变量，永不写进源码 / 测试 / 注释 / 提交信息。
- **合并纪律**：每个改动必须过 `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`（见 `CONTRIBUTING.md`）；`unsafe_code` 全仓禁用。
- **审查分离**：写代码的模型不自审自己的代码，合并前过独立的跨模型评审门。
- **面向外部读者**：文档 / 注释里不要把外部看不到的私有源码或路径写成"可验证"步骤。

（贡献规范见 `CONTRIBUTING.md`，架构总览见 `README.md` / `README.zh-CN.md`。）
