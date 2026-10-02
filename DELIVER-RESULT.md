# 交付结果

唯一阻断项已修复；真实入口验收测试已补齐，受沙箱端口限制尚未完成 Anthropic 端到端验证。未执行 Git 提交。本文件与 `DELIVER-FIX.md` 由调用方移走，不纳入提交。

## 改了什么、为什么

- `crates/vyane-protocol/src/wire.rs`：Anthropic 输入总量包含普通输入、缓存创建与缓存读取，缓存子计数只含读取。匹配估算器“子计数包含于父计数”的要求，修正缓存读取大于原始输入时的少计费用，避免创建错误套用读取费率。
- `crates/vyane-protocol/tests/protocol_clients.rs`：更新 HTTP 响应夹具与预期，覆盖读取大于普通输入且同时存在缓存创建。
- `crates/vyane-protocol/src/sse.rs`：加强既有 SSE 解码回归测试，精确断言父计数与缓存读取子计数；生产 SSE 路径仍复用同一转换函数。
- `crates/vyane-cli/tests/cost_acceptance.rs`：新增真实 CLI 子进程 → HTTP 请求 → 账本落盘 → 公共费用估算接口的验收测试，分别验证配置和未配置缓存读取费率的金额。
- `docs/CHANGELOG.md`：在 `Unreleased` 补一条 Anthropic 用量与费用修复记录。

只修独立审查的阻断项；非阻断项的处理说明见 `DELIVER-FIX.md`。没有修改估算器公式、公共类型或配置接口。

官方用量定义确认三类输入应相加。[Anthropic 官方文档](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)。smart-search 的 Jina 提取未返回内容，主动中断后改用网页检索核对官方文档。这里仅核对用量定义，不把本轮估算器的缓存创建费率当作厂商真实账单费率。

## 测试命令与结果

| 命令 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过，退出码 0。 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 全工作区、全部目标通过，退出码 0；包含新增验收测试的编译检查。 |
| `cargo test -p vyane-protocol --lib anthropic_delta_and_usage_normalize` | 修复前红；修复后绿；旧错误口径变异后红；恢复后绿。成功运行 1 条。 |
| `cargo test -p vyane-ledger cost::tests` | 17 条费用测试通过，退出码 0。 |
| `cargo test -p vyane-protocol --lib` | 16 条通过，2 条既有 HTTP 测试因绑定端口被拒绝而失败，退出码 101。 |
| `cargo test -p vyane-protocol --test protocol_clients anthropic_complete_success_parses_outcome_and_request` | 编译成功；测试服务器绑定端口时报 `PermissionDenied / Operation not permitted`，退出码 101，未进入响应断言。**需调用方沙箱外运行**。 |
| `cargo test -p vyane-cli --test cost_acceptance` | 真实 Claude CLI 子进程验收通过；Anthropic 和两条 OpenAI HTTP 验收因绑定端口被拒绝而失败。1 条通过、3 条受阻，退出码 101。**需调用方沙箱外运行**。 |
| `cargo test --workspace` | 默认 `umask 002` 下，既有 agent 数据库测试因临时父目录可被组写而拒绝，5 条通过、3 条失败、1 条忽略；退出码 101，后续目标未执行。 |
| `umask 077; cargo test --workspace` | 使用仅当前用户可写的临时目录权限后，agent／broker 测试通过；到 CLI 单元测试时 312 条通过、24 条因本地端口绑定被拒绝而失败，退出码 101，后续目标未执行。**需调用方沙箱外运行**。 |
| `git diff --check` | 通过。 |

数据库临时目录问题通过本次测试进程的 `umask` 解决，没有更改系统配置或仓库代码。端口受阻不是费用断言失败；不能将受阻测试计为通过。

## 关键不变式与变异证据

不变式：输入父计数含三类输入，缓存子计数只含读取。SSE 夹具普通输入 4、创建 5、读取 30、输出 2，必须解析为输入 39、缓存读取 30、输出 2、无推理计数。

在基线 `5fca395d17bd7041204a43a754274ab43e86dda2` 加入此断言后，测试于用量断言处失败，退出码 101。修复后通过。随后主动恢复旧错误计算，测试再次于同一断言处失败，退出码 101；恢复修复后通过，退出码 0。最终生产文件与变异前修复备份一致，未遗留变异。

真实 Anthropic 验收使用普通输入 9、创建 5、读取 30、输出 4，断言落盘输入 44／缓存读取 30，并独立计算预期金额 `0.000025` 和 `0.000052` 美元。该测试已经写好且编译，但由于沙箱限制没有跑到落盘及金额断言；本轮变异证据来自生产 SSE 解码路径，不冒充端到端证据。

## 没验证的与调用方补跑

以下命令**需调用方沙箱外运行**，允许本地回环监听即可，不需要厂商凭据或付费模型调用：

```sh
umask 077
cargo test -p vyane-protocol --test protocol_clients anthropic_complete_success_parses_outcome_and_request
cargo test -p vyane-cli --test cost_acceptance
cargo test --workspace
```

尚未完成：Anthropic 新增端到端验收、受阻 HTTP 测试、全工作区后续目标、托管 CI、MSRV 1.88 工具链与其他操作系统验证、真实厂商账单比对、修复后的独立复审。没有发布、合并或请求外部付费模型。

测试原始日志位于本次临时目录的 `eos1053-baseline.log`、`eos1053-mutation.log`、`eos1053-restored.log`、`eos1053-protocol-lib.log`、`eos1053-protocol-client.log`、`eos1053-cost-acceptance.log`、`eos1053-ledger-cost.log`、`eos1053-clippy.log`、`eos1053-workspace.log`、`eos1053-workspace-private-umask.log`；关键结论和失败摘要已完整保存在本文件。
