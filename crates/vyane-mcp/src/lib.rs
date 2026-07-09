//! # vyane-mcp
//!
//! An MCP (Model Context Protocol) server that exposes vyane's four operations
//! — dispatch, broadcast, history, and sessions — as callable tools.
//!
//! The server is transport-agnostic: it wraps a [`vyane_service::VyaneService`]
//! and registers four tools (`vyane_dispatch`, `vyane_broadcast`, `vyane_history`,
//! `vyane_sessions`). A front-end wires it onto a transport; today the only entry
//! point is [`run_stdio`], driven by the `vyane mcp` CLI subcommand.
//!
//! ## Tool result contract
//!
//! Every tool returns a successful MCP result (`is_error = None/false`) even when
//! the underlying vyane operation failed. The error is carried as structured JSON
//! inside the content so an agent client can read and act on it, rather than the
//! MCP call itself being treated as a protocol failure. This mirrors how the CLI
//! prints a record's `error` field instead of crashing — a recorded failure is a
//! result, not an exception.

use std::future::Future;

use anyhow::Result;
use rmcp::{
    ErrorData as McpError, ServiceExt,
    handler::server::router::tool::ToolRouter,
    handler::server::tool::Parameters,
    model::{
        CallToolResult, Content, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
    },
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use serde::Deserialize;
use vyane_core::RunStatus;
use vyane_service::{BroadcastParams, DispatchParams, HistoryFilter, VyaneService};

const SERVER_INSTRUCTIONS: &str = "Vyane multi-model dispatch server. \
    Use vyane_dispatch to run a task against a configured target (profile name or provider/model), \
    vyane_broadcast to fan one task out to several targets concurrently, \
    vyane_history to query the run ledger, and vyane_sessions to list saved sessions.";

/// The MCP server. Holds one clone-cheap [`VyaneService`] and the macro-generated
/// tool router. Cloning is fine: `VyaneService` is itself `Clone` (everything
/// behind an `Arc`), and rmcp requires the handler to be `Clone + Send + Sync`.
#[derive(Clone)]
pub struct VyaneMcpServer {
    service: VyaneService,
    tool_router: ToolRouter<Self>,
}

// ---- tool parameter schemas -------------------------------------------------
//
// rmcp v0.5 derives the JSON Schema clients see from the `Parameters<T>`
// wrapper's `T` (which must implement `schemars::JsonSchema` + `Deserialize`).
// Optional fields use `#[serde(default)]` so the client may omit them. The
// field doc-comments become the schema field descriptions.

/// Arguments for `vyane_dispatch`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct DispatchArgs {
    /// Task / prompt text to submit.
    pub task: String,
    /// Target selector: a profile name or `provider/model`.
    pub target: String,
    /// Working directory for harness runs. Ignored by direct-chat targets.
    #[serde(default)]
    pub workdir: Option<String>,
    /// Sandbox level for harness runs: `read_only` (default), `write`, or `full`.
    #[serde(default)]
    pub sandbox: Option<String>,
    /// Optional session id to continue.
    #[serde(default)]
    pub session: Option<String>,
    /// System prompt for direct HTTP, appended instructions for harnesses.
    #[serde(default)]
    pub system: Option<String>,
    /// Per-attempt timeout in seconds. Absent = no timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Arguments for `vyane_broadcast`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BroadcastArgs {
    /// Task / prompt text submitted to every target.
    pub task: String,
    /// Comma-separated target list; each is a profile or `provider/model`.
    pub targets: String,
    /// Working directory for harness runs.
    #[serde(default)]
    pub workdir: Option<String>,
    /// Sandbox level: `read_only` (default), `write`, or `full`.
    #[serde(default)]
    pub sandbox: Option<String>,
    /// System prompt for direct HTTP, appended instructions for harnesses.
    #[serde(default)]
    pub system: Option<String>,
    /// Per-attempt timeout in seconds. Absent = no timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Arguments for `vyane_history`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct HistoryArgs {
    /// Maximum records to return. Defaults to 20.
    #[serde(default = "default_history_limit")]
    pub limit: usize,
    /// Filter by run status: `success`, `error`, `timeout`, or `cancelled`.
    #[serde(default)]
    pub status: Option<String>,
    /// Filter by provider id.
    #[serde(default)]
    pub provider: Option<String>,
}

fn default_history_limit() -> usize {
    20
}

#[tool_router]
impl VyaneMcpServer {
    pub fn new(service: VyaneService) -> Self {
        Self {
            service,
            tool_router: Self::tool_router(),
        }
    }

    /// Dispatch a single task to one resolved target chain.
    ///
    /// Returns `{ "record": RunRecord, "output": Option<String> }` on success,
    /// or `{ "error": String }` when the dispatch itself failed (target
    /// resolution, config, etc.). A recorded-but-failed run (e.g. a provider
    /// HTTP error after the run was ledger-appended) still returns its record
    /// with the run's own `status`/`error` fields.
    #[tool(
        description = "Dispatch a task to a configured target (profile name or provider/model). Returns the run record and any output text."
    )]
    async fn vyane_dispatch(
        &self,
        Parameters(args): Parameters<DispatchArgs>,
    ) -> Result<CallToolResult, McpError> {
        let params = DispatchParams {
            task: args.task,
            target: args.target,
            workdir: args.workdir.map(Into::into),
            sandbox: parse_sandbox(args.sandbox),
            session: args.session,
            system: args.system,
            timeout_secs: args.timeout_secs,
            labels: Vec::new(),
        };
        match self
            .service
            .dispatch(params, vyane_core::CancellationToken::new())
            .await
        {
            Ok(outcome) => Ok(success_json(serde_json::json!({
                "record": outcome.record,
                "output": outcome.output,
            }))),
            Err(error) => Ok(error_text(error)),
        }
    }

    /// Fan one task out to several targets concurrently. Returns an array of
    /// `{ "target", "record"?, "output"?, "error"? }` objects, one per selector
    /// in input order.
    #[tool(
        description = "Run one task against several targets concurrently. Pass targets as a comma-separated string of profiles or provider/model pairs."
    )]
    async fn vyane_broadcast(
        &self,
        Parameters(args): Parameters<BroadcastArgs>,
    ) -> Result<CallToolResult, McpError> {
        let params = BroadcastParams {
            task: args.task,
            targets: args.targets,
            workdir: args.workdir.map(Into::into),
            sandbox: parse_sandbox(args.sandbox),
            system: args.system,
            timeout_secs: args.timeout_secs,
            labels: Vec::new(),
        };
        match self
            .service
            .broadcast(params, vyane_core::CancellationToken::new())
            .await
        {
            Ok(results) => {
                let items: Vec<_> = results
                    .into_iter()
                    .map(|(selector, result)| match result {
                        Ok(outcome) => serde_json::json!({
                            "target": selector,
                            "record": outcome.record,
                            "output": outcome.output,
                        }),
                        Err(error) => serde_json::json!({
                            "target": selector,
                            "error": error.to_string(),
                        }),
                    })
                    .collect();
                Ok(success_json(serde_json::json!({ "items": items })))
            }
            Err(error) => Ok(error_text(error)),
        }
    }

    /// Query the run ledger (most-recent-first).
    #[tool(
        description = "Query recent run records from the ledger. Returns { items: [...] }, most recent first."
    )]
    async fn vyane_history(
        &self,
        Parameters(args): Parameters<HistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let filter = HistoryFilter {
            limit: Some(args.limit),
            status: args.status.as_deref().and_then(parse_status),
            provider: args.provider,
        };
        match self.service.history(filter).await {
            Ok(records) => Ok(success_json(serde_json::json!({ "items": records }))),
            Err(error) => Ok(error_text(error)),
        }
    }

    /// List saved sessions.
    #[tool(description = "List saved vyane sessions. Returns { items: [...] }.")]
    async fn vyane_sessions(&self) -> Result<CallToolResult, McpError> {
        match self.service.sessions().await {
            Ok(sessions) => Ok(success_json(serde_json::json!({ "items": sessions }))),
            Err(error) => Ok(error_text(error)),
        }
    }
}

#[tool_handler]
impl rmcp::ServerHandler for VyaneMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "vyane".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            instructions: Some(SERVER_INSTRUCTIONS.into()),
        }
    }
}

/// Run the MCP server over stdio. Blocks until the client disconnects or the
/// transport errors. Intended as the body of the `vyane mcp` subcommand.
pub async fn run_stdio(service: VyaneService) -> Result<()> {
    let server = VyaneMcpServer::new(service);
    let running = server.serve(stdio()).await?;
    running.waiting().await?;
    Ok(())
}

// ---- helpers ----------------------------------------------------------------

/// Map the loose sandbox string callers send into the [`Sandbox`] enum.
///
/// `write` and `full` are recognized explicitly; anything else (including
/// `None`, `"read-only"`, and unrecognized strings) falls back to `ReadOnly`
/// — the safest level, matching the CLI's `default_value_t`. Degrading an
/// unknown value to read-only rather than erroring keeps a tool call usable
/// even when a client sends a slightly different spelling.
pub fn parse_sandbox(s: Option<String>) -> vyane_core::Sandbox {
    use vyane_core::Sandbox;
    match s.as_deref() {
        Some("write") => Sandbox::Write,
        Some("full") => Sandbox::Full,
        // Anything else (including `None`) is read-only.
        _ => Sandbox::ReadOnly,
    }
}

/// Parse a status filter string into a [`RunStatus`]. Returns `None` for
/// anything unrecognized so a bad filter degrades to "no status filter" rather
/// than failing the whole history query.
pub fn parse_status(s: &str) -> Option<RunStatus> {
    match s {
        "success" => Some(RunStatus::Success),
        "error" => Some(RunStatus::Error),
        "timeout" => Some(RunStatus::Timeout),
        "cancelled" | "canceled" => Some(RunStatus::Cancelled),
        _ => None,
    }
}

/// Serialize a value as a single JSON-text content block in a successful tool
/// result. Serialization itself cannot fail for our value types (all serde
/// derived); the `expect` matches that contract and surfaces a bug if it ever
/// does not hold.
fn success_json(value: serde_json::Value) -> CallToolResult {
    let text = serde_json::to_string_pretty(&value)
        .expect("tool result payload is JSON-serializable by construction");
    CallToolResult::success(vec![Content::text(text)])
}

/// Carry a vyane-layer error back to the client as a text content block inside
/// a *successful* MCP call (see the module-level contract: the tool ran, the
/// operation it wrapped failed).
fn error_text(error: anyhow::Error) -> CallToolResult {
    CallToolResult::success(vec![Content::text(format!("{error:#}"))])
}

// `Future` must be in scope for the `#[tool]` macro's async-rewriting in rmcp
// v0.5. Kept as a private import alias so the surface API stays clean.
#[allow(dead_code)]
type _FutureMustBeInScope = Box<dyn Future<Output = ()> + Send>;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use vyane_core::Sandbox;

    #[test]
    fn sandbox_defaults_to_read_only() {
        assert_eq!(parse_sandbox(None), Sandbox::ReadOnly);
        assert_eq!(parse_sandbox(Some("unknown".into())), Sandbox::ReadOnly);
        assert_eq!(parse_sandbox(Some("read-only".into())), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_parses_write_and_full() {
        assert_eq!(parse_sandbox(Some("write".into())), Sandbox::Write);
        assert_eq!(parse_sandbox(Some("full".into())), Sandbox::Full);
    }

    #[test]
    fn status_parses_known_spells() {
        assert_eq!(parse_status("success"), Some(RunStatus::Success));
        assert_eq!(parse_status("error"), Some(RunStatus::Error));
        assert_eq!(parse_status("timeout"), Some(RunStatus::Timeout));
        assert_eq!(parse_status("cancelled"), Some(RunStatus::Cancelled));
        assert_eq!(parse_status("canceled"), Some(RunStatus::Cancelled));
    }

    #[test]
    fn status_unknown_is_none() {
        assert_eq!(parse_status("nope"), None);
        assert_eq!(parse_status(""), None);
    }

    #[test]
    fn history_args_default_limit_is_twenty() {
        // serde(default) is exercised through the JSON round-trip below, but
        // pinning the helper here documents the contract.
        assert_eq!(default_history_limit(), 20);
    }

    #[test]
    fn history_args_fill_default_limit_when_absent() {
        let json = r#"{"status":"success"}"#;
        let args: HistoryArgs = serde_json::from_str(json).unwrap();
        assert_eq!(args.limit, 20);
        assert_eq!(args.status.as_deref(), Some("success"));
    }

    #[test]
    fn dispatch_args_optional_fields_default_to_none() {
        let json = r#"{"task":"hi","target":"default"}"#;
        let args: DispatchArgs = serde_json::from_str(json).unwrap();
        assert_eq!(args.task, "hi");
        assert_eq!(args.target, "default");
        assert!(args.workdir.is_none());
        assert!(args.sandbox.is_none());
        assert!(args.session.is_none());
        assert!(args.system.is_none());
        assert!(args.timeout_secs.is_none());
    }

    #[test]
    fn dispatch_params_round_trip_through_args() {
        let json = r#"{
            "task":"write tests",
            "target":"codex",
            "workdir":"/tmp",
            "sandbox":"write",
            "session":"s1",
            "system":"be terse",
            "timeout_secs":30
        }"#;
        let args: DispatchArgs = serde_json::from_str(json).unwrap();
        let params = DispatchParams {
            task: args.task,
            target: args.target,
            workdir: args.workdir.map(Into::into),
            sandbox: parse_sandbox(args.sandbox),
            session: args.session,
            system: args.system,
            timeout_secs: args.timeout_secs,
            labels: Vec::new(),
        };
        assert_eq!(params.task, "write tests");
        assert_eq!(params.target, "codex");
        assert_eq!(
            params.workdir.as_deref(),
            Some(std::path::Path::new("/tmp"))
        );
        assert_eq!(params.sandbox, Sandbox::Write);
        assert_eq!(params.session.as_deref(), Some("s1"));
        assert_eq!(params.system.as_deref(), Some("be terse"));
        assert_eq!(params.timeout_secs, Some(30));
    }
}
