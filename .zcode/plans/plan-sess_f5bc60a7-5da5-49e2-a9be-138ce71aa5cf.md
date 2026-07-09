# vyane-rs v0.3 推进计划：REST API + MCP Server + Router

## 目标

让 vyane-rs 支持 **CLI + REST API + MCP** 三种协议入口，全部建立在同一个共享服务层上。router 随后从 7 行 stub 长出真路由。

## 架构前提（已探查确认）

现有代码已提供完美的复用基础：
- `Runtime`（`vyane-cli/src/app.rs:78`）已封装 `Dispatcher + Ledger + SessionStore` 三件套
- `resolve_target_chain`（`vyane-cli/src/command.rs:1294`）是唯一 selector 解析点（dispatch/worker/workflow 三处复用），但**是 CLI 私有函数**
- `RunRecord` / `SessionRecord` 已 derive `Serialize`，可直接做 API 响应
- `Dispatcher::dispatch` / `broadcast` / `Ledger::query` / `SessionStore::list` 是全部业务入口
- kernel 无 streaming 入口（CLI 手搓的，已知技术债，本批不碰）

## 三个 Work Package

### WP-11：vyane-service 共享服务层（前置，阻塞性最小）

**目的**：把 CLI 里的业务逻辑抽成一个可被 API/MCP/CLI 三者复用的 crate，消除重复。

**改动**：
1. 新建 `crates/vyane-service`，依赖 `vyane-kernel` + `vyane-config` + `vyane-ledger` + `vyane-core`
2. 从 `vyane-cli/src/command.rs` 抽出到 `vyane-service`：
   - `resolve_target_chain` + `parse_provider_model` + `provider_model_config` + `resolve_temp_profile`（selector 解析，目前 CLI 私有）
   - `split_targets`（broadcast 逗号分割）
   - 一个 `DispatchParams { task, target, workdir, sandbox, session, system, timeout, labels }` 结构体 + `VyaneService::dispatch(params) -> Result<DispatchOutcome>` 方法
   - `VyaneService::broadcast(params) -> Vec<Result<DispatchOutcome>>`
   - `VyaneService::history(query) / sessions()` 只读查询
3. `VyaneService` 持有 `Arc<Runtime>`（或直接持有 dispatcher/ledger/sessions 引用），线程安全
4. CLI 的 `command.rs` 改为调用 `vyane-service`（保持现有行为不变，纯重构）
5. 把 `DispatchOutcome` 加 `#[derive(Serialize)]`（目前只有 Debug/Clone），或导出 `RunJson`/`BroadcastJson` 响应结构到 service 层

**验收**：
- `cargo test --workspace` 全绿（含现有 248 测试 + 10 集成套件，行为零回归）
- CLI 的 `vyane dispatch/broadcast/history/sessions` 输出与重构前逐字节一致
- `vyane-service` 有自己的单元测试覆盖 selector 解析（profile name / provider/model / failover chain）

### WP-12：REST API（`vyane serve` 子命令 + axum）

**目的**：暴露 RESTful HTTP API，让外部客户端能驱动 vyane。

**改动**：
1. workspace `Cargo.toml` 新增依赖：`axum = "0.8"`（基于 hyper，与 tokio 1.x 兼容；`tower` 已是传递依赖）
2. CLI 新增 `serve` 子命令（`Command::Serve(ServeArgs)`），含 `--addr`（默认 `127.0.0.1:9721`）、`--config` 复用全局参数
3. API 端点（全部 JSON，字段 snake_case，遵循 `vyane-api-contract` skill 的契约规则）：

| 方法 | 路径 | 功能 | 映射到 |
|---|---|---|---|
| `POST` | `/v1/dispatch` | 单目标派发 | `VyaneService::dispatch` |
| `POST` | `/v1/broadcast` | 多目标并发 | `VyaneService::broadcast` |
| `GET` | `/v1/runs` | 查询账本 | `VyaneService::history`（query 参数：limit/status/provider） |
| `GET` | `/v1/runs/:id` | 单条记录 | `Ledger::query` by run_id |
| `GET` | `/v1/sessions` | 会话列表 | `SessionStore::list` |
| `POST` | `/v1/runs/:id/cancel` | 取消运行 | （v1 有限支持：取消 in-flight dispatch） |
| `GET` | `/v1/health` | 健康检查 | 静态 OK |

4. 请求/响应结构放在 `vyane-service` 或 CLI 的 `api.rs` 模块，`Serialize`/`Deserialize`，用 envelope `{ "items": [...] }` 给列表
5. 错误映射到 HTTP 状态码：`Config/NotFound→400/404`，`Auth/RateLimited→502`（上游），`Timeout→504`，内部错误→500
6. 集成测试：用 axum 的 `oneshot` + `tower::ServiceExt` 测试每个端点（无真实模型调用，用 mock executor factory）

**验收**：
- `cargo test` 覆盖所有端点的 happy path + 错误路径
- `curl` 冒烟测试能跑通 dispatch→history 链路（手动验证，记在 feedback doc）
- `vyane serve --addr 127.0.0.1:0` 能启动并响应 health check

### WP-13：MCP Server（rmcp 官方 SDK，`vyane mcp` 子命令）

**目的**：把 dispatch/broadcast/status 暴露为 MCP 工具，让其他 agent（Claude/Codex 等）能直接调用 vyane。

**改动**：
1. workspace `Cargo.toml` 新增：`rmcp = { version = "0.5", features = ["server", "macros", "transport-io"] }`
2. 新建 `crates/vyane-mcp`，依赖 `vyane-service` + `rmcp`
3. 用 rmcp 的 `#[tool_router]` + `#[tool]` 宏模式实现 MCP 工具：

| 工具名 | 描述 | 参数 | 映射到 |
|---|---|---|---|
| `vyane_dispatch` | Dispatch a task to one target | `task`, `target`, `workdir?`, `sandbox?`, `timeout?`, `system?` | `VyaneService::dispatch` |
| `vyane_broadcast` | Fan out to multiple targets | `task`, `targets[]`, `workdir?`, `sandbox?` | `VyaneService::broadcast` |
| `vyane_history` | Query recent runs | `limit?`, `status?`, `provider?` | `VyaneService::history` |
| `vyane_sessions` | List saved sessions | — | `VyaneService::sessions` |

4. `ServerHandler` impl 提供 `ServerInfo`（name=vyane, capabilities=tools）
5. CLI 新增 `mcp` 子命令（`Command::Mcp`），用 stdio transport 启动 server（`rmcp::transport::stdio()`）
6. 工具返回值用 `Content::json`（把 RunRecord 序列化成 JSON content）
7. 集成测试：用 rmcp client（`TokioChildProcess` 启动自己的 server）测试 tool list + call_tool 往返

**验收**：
- `cargo test` 覆盖 MCP 工具的 list + call
- 能在 Claude Code 的 MCP 配置里注册 `vyane mcp` 并调用 `vyane_dispatch`（手动冒烟，记 feedback）

## 并行执行策略

三个 WP 的依赖关系：**WP-11（service 层）是 WP-12 和 WP-13 的前置**。但 WP-11 本身是纯重构、范围明确、可快速完成。

实际并行打法：
1. **第一波（我直接做）**：WP-11 service 层抽取 — 这是阻塞性前置，我亲自做保证接口冻结质量
2. **第二波（WP-11 合并后并行派发）**：WP-12（REST API）和 WP-13（MCP）可完全并行，因为都只依赖 `vyane-service` 的稳定接口，互不触碰对方的 crate。用多模型流水线外包：spec 由我冻结，实现派给不同模型（如 GLM 做 WP-12、GPT 做 WP-13），交叉评审。
3. **router（WP-14）**：在 API/MCP 合并后单独推进，clean-room 复刻 Python 智能路由 v5（EOS-543 的 tag+复杂度动态分档）。

## 质量门（沿用现有流程，不降标）

每个 WP 必须通过：
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`（新代码带自己的单元+集成测试）
- 跨模型对抗评审（作者≠评审）
- CI ubuntu + macOS 双绿

## 不做的事（明确边界）

- 不碰 kernel streaming 入口（已有技术债注释，单独 WP）
- 不做 daemon 常驻进程（WP-08 的 detached tasks 已覆盖"后台运行"需求；真 daemon 是后续）
- 不做 API 鉴权（v1 绑定 127.0.0.1，本地信任；公网暴露是后续安全 WP）
- router 本批只做到接口冻结，实现是 WP-14

## 交付物

- 3 个新 crate 或模块（vyane-service / API 层 / vyane-mcp）
- CLI 新增 `serve` + `mcp` 子命令
- 完整测试覆盖（单元 + 集成）
- `docs/plan/WP-11.md` / `WP-12.md` / `WP-13.md` 规格文档
- README 更新协议入口表（CLI / REST / MCP 三列）
- 看板 EOS-587 收口 + 新建 v0.3 卡
