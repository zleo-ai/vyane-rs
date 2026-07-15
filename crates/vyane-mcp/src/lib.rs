//! # vyane-mcp
//!
//! An MCP (Model Context Protocol) server that exposes vyane's execution,
//! history, session, and static diagnostic operations as callable tools.
//!
//! The server is transport-agnostic: it wraps a [`vyane_service::VyaneService`]
//! and registers six tools (`vyane_dispatch`, `vyane_broadcast`, `vyane_history`,
//! `vyane_sessions`, `vyane_route`, and `vyane_check`). A front-end wires it onto
//! a transport; today the only entry point is [`run_stdio`], driven by the
//! `vyane mcp` CLI subcommand.
//!
//! ## Tool result contract
//!
//! Every tool returns a successful MCP result (`is_error = None/false`) even when
//! the underlying vyane operation failed. Failures are carried in a bounded,
//! structured JSON envelope — `{ "status": "error", "error": { "code",
//! "message" } }` — rather than exposing an `anyhow` chain, provider response,
//! local path, or secret through the MCP boundary. The public error-code set is
//! deliberately closed. Generic successful payloads are capped at 1 MiB;
//! diagnostics use a smaller dedicated budget. If an execution completed but
//! its detailed result is too large, the tool returns a bounded
//! `operation_status=completed` receipt with the run id(s), never a generic
//! limit error that could invite a duplicate retry.
//!
//! Arguments are strict as well: unknown object fields are rejected during
//! deserialization, while semantic errors such as an unsupported sandbox or an
//! out-of-range history limit become the same successful MCP result containing
//! an `invalid_argument` envelope. Validation happens before the service is
//! called.

use std::{collections::BTreeMap, future::Future, pin::Pin, str::FromStr, sync::Arc};

use anyhow::Result;
use rmcp::{
    ErrorData as McpError, ServiceExt,
    handler::server::router::tool::ToolRouter,
    model::{
        CallToolResult, Content, Implementation, JsonObject, ProtocolVersion, ServerCapabilities,
        ServerInfo,
    },
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use vyane_core::{ErrorKind, RunStatus, VyaneError};
use vyane_service::{
    BroadcastParams, DIAGNOSTIC_MAX_OUTPUT_BYTES, DiagnosticError, DiagnosticErrorKind,
    DispatchParams, HistoryFilter, RoutePreviewParams, RunView, VyaneService,
};
use vyane_workflow::{WorkflowRunId, WorkflowSourceBundle, WorkflowSourceEntry};

const SERVER_INSTRUCTIONS: &str = "Vyane multi-model dispatch server. \
    Use vyane_dispatch to run a task against a configured target (profile name, provider/model, or auto), \
    vyane_broadcast to fan one task out to several targets concurrently, \
    vyane_history to query the run ledger, vyane_sessions to list saved sessions, \
    vyane_route to preview deterministic routing, and vyane_check for a static-only redacted config check. \
    A result with operation_status=completed is final even when detail_omitted=true; use its run receipt and never retry it as an execution failure.";

/// The MCP server. Holds one clone-cheap [`VyaneService`] and the macro-generated
/// tool router. Cloning is fine: `VyaneService` is itself `Clone` (everything
/// behind an `Arc`), and rmcp requires the handler to be `Clone + Send + Sync`.
#[derive(Clone)]
pub struct VyaneMcpServer {
    service: VyaneService,
    workflow_control: Option<Arc<dyn WorkflowControl>>,
    tool_router: ToolRouter<Self>,
}

/// Boxed future used by the object-safe workflow control boundary.
pub type WorkflowControlFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Narrow control-plane port supplied by the process that owns workflow
/// authentication and lifecycle. The MCP crate never discovers daemon state
/// or reads a control descriptor or bearer token itself.
pub trait WorkflowControl: Send + Sync {
    fn submit(
        &self,
        request: WorkflowSubmitRequest,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>>;

    fn status(
        &self,
        caller_id: WorkflowRunId,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>>;

    fn cancel(
        &self,
        caller_id: WorkflowRunId,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>>;
}

/// Fully bounded workflow submission passed to the injected control plane.
#[derive(Debug, Clone)]
pub struct WorkflowSubmitRequest {
    pub caller_id: WorkflowRunId,
    pub bundle: WorkflowSourceBundle,
    pub vars: BTreeMap<String, String>,
}

/// Public lifecycle projection. Deliberately excludes ownership, controller,
/// lease, authentication, paths, prompts, and raw error fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkflowView {
    pub caller_id: WorkflowRunId,
    pub state: WorkflowState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<WorkflowFailureCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowFailureCode {
    DispatchFailed,
    SpawnFailed,
    Configuration,
    ControlUnavailable,
    WorkerLost,
    LeaseExpired,
    Cancelled,
    TimedOut,
    Internal,
}

/// Closed error taxonomy. Source messages cannot cross this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowControlError {
    InvalidRequest,
    NotFound,
    Conflict,
    Unavailable,
    OutcomeUnknown,
    Internal,
}

impl std::fmt::Display for WorkflowControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "workflow request is invalid",
            Self::NotFound => "workflow was not found",
            Self::Conflict => "workflow request conflicts with existing state",
            Self::Unavailable => "workflow control is unavailable",
            Self::OutcomeUnknown => "workflow submission outcome is unknown",
            Self::Internal => "workflow operation failed",
        })
    }
}

impl std::error::Error for WorkflowControlError {}

// ---- tool parameter schemas -------------------------------------------------
//
// rmcp v0.5 normally derives the JSON Schema clients see from a
// `Parameters<T>` wrapper. We instead declare those same schemas explicitly
// and receive a raw `JsonObject`, because the stock extractor includes rejected
// string values in its protocol error. Optional fields use `#[serde(default)]`
// so the client may omit them. Field doc-comments become schema descriptions.

/// Arguments for `vyane_dispatch`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DispatchArgs {
    /// Task / prompt text to submit.
    pub task: String,
    /// Target selector: a profile name, `provider/model`, or `auto`.
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
    /// Free-form `key=value` ledger labels. Routing result labels are reserved.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Auto-routing stage hint (for example `implementation` or `review`).
    #[serde(default)]
    pub route_stage: Option<String>,
    /// Auto-routing tier override: `economy`, `mainline`, or `frontier`.
    #[serde(default)]
    pub route_tier: Option<String>,
    /// Auto-routing tags, in priority order.
    #[serde(default)]
    pub route_tags: Vec<String>,
    /// Restrict auto-routing to these profile names.
    #[serde(default)]
    pub route_candidates: Vec<String>,
    /// Hard guard for auto-routing. Set false to forbid frontier profiles and
    /// frontier failover legs.
    #[serde(default)]
    pub allow_frontier: Option<bool>,
}

/// Arguments for `vyane_broadcast`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryArgs {
    /// Maximum records to return. Defaults to 20; valid range is 1..=1000.
    #[serde(default = "default_history_limit")]
    pub limit: usize,
    /// Filter by run status: `success`, `error`, `timeout`, or `cancelled`.
    #[serde(default)]
    pub status: Option<String>,
    /// Filter by provider id.
    #[serde(default)]
    pub provider: Option<String>,
}

/// One bounded route tag or candidate profile name.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct RouteValue(#[schemars(length(min = 1, max = 256))] pub String);

/// Arguments for `vyane_route`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouteArgs {
    /// Task text used only as router input. It is never returned by the tool.
    #[schemars(length(min = 1, max = 65536))]
    pub task: String,
    /// Optional workflow stage hint.
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub stage: Option<String>,
    /// Number of changed files, when known.
    #[serde(default)]
    #[schemars(range(max = 1000000))]
    pub changed_files: Option<usize>,
    /// Number of dependency edges affected, when known.
    #[serde(default)]
    #[schemars(range(max = 1000000))]
    pub dependency_edges: Option<usize>,
    /// Retry count for the current task, when known.
    #[serde(default)]
    #[schemars(range(max = 1000000))]
    pub retry_count: Option<usize>,
    /// Explicit routing tier: `economy`, `mainline`, or `frontier`.
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub tier: Option<String>,
    /// Additional routing tags. Raw tags are not returned by the tool.
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub tags: Vec<RouteValue>,
    /// Restrict routing to these configured profile names.
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub candidates: Vec<RouteValue>,
    /// Whether frontier profiles may be selected. Defaults to true.
    #[serde(default)]
    pub allow_frontier: Option<bool>,
}

/// Strict empty arguments for `vyane_check`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CheckArgs {}

/// One in-memory prompt source included in a workflow submission.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPromptSourceArgs {
    /// Canonical relative `/`-separated path declared by the workflow.
    #[schemars(length(min = 1, max = 4096))]
    pub path: String,
    /// UTF-8 prompt content.
    #[schemars(length(max = 4194304))]
    pub content: String,
}

/// Arguments for `vyane_workflow_submit`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSubmitArgs {
    /// Caller-selected canonical lowercase UUIDv7. Reuse only when retrying the
    /// identical intended submission after an indeterminate outcome.
    #[schemars(length(min = 36, max = 36))]
    pub caller_id: String,
    /// Declarative workflow TOML source.
    #[schemars(length(min = 1, max = 1048576))]
    pub workflow_toml: String,
    /// Prompt files referenced by the workflow.
    #[serde(default)]
    #[schemars(length(max = 128))]
    pub prompt_files: Vec<WorkflowPromptSourceArgs>,
    /// Workflow template variables.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
}

/// Arguments shared by workflow status and cancellation.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkflowIdArgs {
    /// Canonical lowercase UUIDv7 workflow caller id.
    #[schemars(length(min = 36, max = 36))]
    pub caller_id: String,
}

fn default_history_limit() -> usize {
    20
}

impl Default for HistoryArgs {
    fn default() -> Self {
        Self {
            limit: default_history_limit(),
            status: None,
            provider: None,
        }
    }
}

const MAX_HISTORY_LIMIT: usize = 1_000;
const MAX_BROADCAST_TARGETS: usize = 64;
const MAX_BROADCAST_TARGETS_BYTES: usize = 64 * 1024;
const MAX_WORKFLOW_TOML_BYTES: usize = 1024 * 1024;
const MAX_WORKFLOW_PROMPT_BYTES: usize = 4 * 1024 * 1024;
const MAX_WORKFLOW_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const MAX_WORKFLOW_SOURCES: usize = 128;
const MAX_WORKFLOW_SOURCE_PATH_BYTES: usize = 4096;
const MAX_WORKFLOW_VARS: usize = 128;
const MAX_WORKFLOW_VAR_KEY_BYTES: usize = 256;
const MAX_WORKFLOW_VAR_VALUE_BYTES: usize = 1024 * 1024;
const MAX_WORKFLOW_VARS_BYTES: usize = 4 * 1024 * 1024;
/// Hard cap for every non-diagnostic MCP success payload. Diagnostics keep a
/// smaller dedicated budget below. Oversized content is replaced by a stable
/// limit error rather than being copied into one tool result.
const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PublicErrorCode {
    InvalidArgument,
    ConfigInvalid,
    LimitExceeded,
    Cancelled,
    NotFound,
    Conflict,
    Unavailable,
    OutcomeUnknown,
    Internal,
}

/// Safe failure information allowed to cross the MCP boundary.
///
/// Both fields are selected from compile-time constants. In particular, this
/// type never stores the source error's display text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct SafeToolError {
    code: PublicErrorCode,
    message: &'static str,
}

#[derive(Serialize)]
struct ToolItems<T> {
    items: T,
}

#[derive(Serialize)]
struct DispatchToolOutput {
    operation_status: &'static str,
    record: RunView,
    output: Option<String>,
    output_omitted: bool,
    detail_omitted: bool,
}

#[derive(Serialize)]
struct DispatchExecutionReceipt<'a> {
    operation_status: &'static str,
    receipt: RunReceipt<'a>,
    output_omitted: bool,
    detail_omitted: bool,
}

#[derive(Serialize)]
struct RunReceipt<'a> {
    receipt_schema: u32,
    run_id: &'a str,
    run_status: RunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    terminal_error_kind: Option<ErrorKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_chars: Option<u64>,
}

impl<'a> From<&'a RunView> for RunReceipt<'a> {
    fn from(view: &'a RunView) -> Self {
        Self {
            receipt_schema: 1,
            run_id: &view.run_id,
            run_status: view.status,
            terminal_error_kind: view.terminal_error_kind,
            output_chars: view.output_chars,
        }
    }
}

#[derive(Serialize)]
struct BroadcastToolOutput {
    operation_status: &'static str,
    items: Vec<BroadcastToolItem>,
    detail_omitted: bool,
}

#[derive(Serialize)]
struct BroadcastToolItem {
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    record: Option<RunView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<SafeToolError>,
}

#[derive(Serialize)]
struct BroadcastExecutionReceipt<'a> {
    operation_status: &'static str,
    items: Vec<BroadcastReceiptItem<'a>>,
    detail_omitted: bool,
}

#[derive(Serialize)]
struct BroadcastReceiptItem<'a> {
    index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<RunReceipt<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<SafeToolError>,
    output_omitted: bool,
}

#[derive(Serialize)]
struct ToolErrorEnvelope {
    status: &'static str,
    error: SafeToolError,
}

impl SafeToolError {
    const fn invalid_argument(message: &'static str) -> Self {
        Self {
            code: PublicErrorCode::InvalidArgument,
            message,
        }
    }

    const fn from_kind(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Config => Self {
                code: PublicErrorCode::ConfigInvalid,
                message: "vyane configuration is invalid",
            },
            ErrorKind::Cancelled => Self {
                code: PublicErrorCode::Cancelled,
                message: "operation was cancelled",
            },
            ErrorKind::NotFound | ErrorKind::Unsupported => Self {
                code: PublicErrorCode::InvalidArgument,
                message: "request contains an invalid argument",
            },
            _ => Self {
                code: PublicErrorCode::Internal,
                message: "operation failed",
            },
        }
    }

    const fn from_diagnostic_kind(kind: DiagnosticErrorKind) -> Self {
        match kind {
            DiagnosticErrorKind::InvalidInput => Self {
                code: PublicErrorCode::InvalidArgument,
                message: "request contains an invalid argument",
            },
            DiagnosticErrorKind::ConfigInvalid => Self {
                code: PublicErrorCode::ConfigInvalid,
                message: "vyane configuration is invalid",
            },
            DiagnosticErrorKind::BudgetExceeded => Self::diagnostic_limit(),
        }
    }

    const fn diagnostic_limit() -> Self {
        Self {
            code: PublicErrorCode::LimitExceeded,
            message: "diagnostic safety limit exceeded",
        }
    }

    const fn output_limit() -> Self {
        Self {
            code: PublicErrorCode::LimitExceeded,
            message: "tool result safety limit exceeded",
        }
    }

    const fn workflow_control(error: WorkflowControlError) -> Self {
        match error {
            WorkflowControlError::InvalidRequest => Self {
                code: PublicErrorCode::InvalidArgument,
                message: "workflow request is invalid",
            },
            WorkflowControlError::NotFound => Self {
                code: PublicErrorCode::NotFound,
                message: "workflow was not found",
            },
            WorkflowControlError::Conflict => Self {
                code: PublicErrorCode::Conflict,
                message: "workflow request conflicts with existing state",
            },
            WorkflowControlError::Unavailable => Self {
                code: PublicErrorCode::Unavailable,
                message: "workflow control is unavailable",
            },
            WorkflowControlError::OutcomeUnknown => Self {
                code: PublicErrorCode::OutcomeUnknown,
                message: "workflow submission outcome is unknown",
            },
            WorkflowControlError::Internal => Self {
                code: PublicErrorCode::Internal,
                message: "workflow operation failed",
            },
        }
    }
}

#[tool_router]
impl VyaneMcpServer {
    pub fn new(service: VyaneService) -> Self {
        let mut tool_router = Self::tool_router();
        for name in [
            "vyane_workflow_submit",
            "vyane_workflow_status",
            "vyane_workflow_cancel",
        ] {
            tool_router.map.remove(name);
        }
        Self {
            service,
            workflow_control: None,
            tool_router,
        }
    }

    /// Create a server with the three workflow control tools enabled.
    pub fn with_workflow_control(
        service: VyaneService,
        workflow_control: Arc<dyn WorkflowControl>,
    ) -> Self {
        Self {
            service,
            workflow_control: Some(workflow_control),
            tool_router: Self::tool_router(),
        }
    }

    /// Dispatch a single task to one resolved target chain.
    ///
    /// Returns `{ "record": RunView, "output": Option<String> }` on success,
    /// or the module-level safe error envelope when dispatch itself failed.
    /// A recorded-but-failed run still returns an allowlisted view, never the
    /// durable prompt/path/label/error fields. If the completed result exceeds
    /// the tool budget, a bounded receipt preserves the run id and completion
    /// status with `detail_omitted=true`.
    #[tool(
        description = "Dispatch a task to a configured target (profile name, provider/model, or auto). Auto routing accepts stage/tier/tags/candidates and an allow_frontier hard guard. Returns operation_status=completed with the run record and output, or a bounded run receipt with detail_omitted=true if the completed result is too large; never retry a completed receipt.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<DispatchArgs>()
    )]
    async fn vyane_dispatch(
        &self,
        arguments: JsonObject,
        cancel: CancellationToken,
    ) -> Result<CallToolResult, McpError> {
        let args: DispatchArgs = match parse_arguments(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(error_json(error)),
        };
        let sandbox = match parse_sandbox(args.sandbox.as_deref()) {
            Ok(sandbox) => sandbox,
            Err(message) => {
                return Ok(error_json(SafeToolError::invalid_argument(message)));
            }
        };
        let labels = dispatch_labels(&args);
        let params = DispatchParams {
            task: args.task,
            target: args.target,
            workdir: args.workdir.map(Into::into),
            sandbox,
            session: args.session,
            system: args.system,
            timeout_secs: args.timeout_secs,
            labels,
        };
        match self.service.dispatch(params, cancel).await {
            Ok(outcome) => {
                let payload = DispatchToolOutput {
                    operation_status: "completed",
                    record: RunView::from(outcome.record),
                    output: outcome.output,
                    output_omitted: false,
                    detail_omitted: false,
                };
                let fallback = DispatchExecutionReceipt {
                    operation_status: "completed",
                    receipt: RunReceipt::from(&payload.record),
                    output_omitted: payload.output.is_some(),
                    detail_omitted: true,
                };
                Ok(execution_success_json(&payload, &fallback))
            }
            Err(error) => Ok(error_json(classify_service_error(&error))),
        }
    }

    /// Fan one task out to several targets concurrently. Returns an array of
    /// `{ "target", "record"?, "output"?, "error"? }` objects, one per selector
    /// in input order.
    #[tool(
        description = "Run one task against up to 64 targets concurrently. Pass targets as a comma-separated string of profiles or provider/model pairs. Oversized completed results return index-aligned run receipts with operation_status=completed and detail_omitted=true; never retry a completed receipt.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<BroadcastArgs>()
    )]
    async fn vyane_broadcast(
        &self,
        arguments: JsonObject,
        cancel: CancellationToken,
    ) -> Result<CallToolResult, McpError> {
        let args: BroadcastArgs = match parse_arguments(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(error_json(error)),
        };
        if let Err(error) = validate_broadcast_targets(&args.targets) {
            return Ok(error_json(error));
        }
        let sandbox = match parse_sandbox(args.sandbox.as_deref()) {
            Ok(sandbox) => sandbox,
            Err(message) => {
                return Ok(error_json(SafeToolError::invalid_argument(message)));
            }
        };
        let params = BroadcastParams {
            task: args.task,
            targets: args.targets,
            workdir: args.workdir.map(Into::into),
            sandbox,
            system: args.system,
            timeout_secs: args.timeout_secs,
            labels: Vec::new(),
        };
        match self.service.broadcast(params, cancel).await {
            Ok(results) => {
                let items = results
                    .into_iter()
                    .map(|(selector, result)| match result {
                        Ok(outcome) => BroadcastToolItem {
                            target: selector,
                            record: Some(RunView::from(outcome.record)),
                            output: outcome.output,
                            status: None,
                            error: None,
                        },
                        Err(error) => broadcast_error_item(selector, &error),
                    })
                    .collect::<Vec<_>>();
                let payload = BroadcastToolOutput {
                    operation_status: "completed",
                    items,
                    detail_omitted: false,
                };
                let receipt = BroadcastExecutionReceipt {
                    operation_status: "completed",
                    items: payload
                        .items
                        .iter()
                        .enumerate()
                        .map(|(index, item)| BroadcastReceiptItem {
                            index,
                            receipt: item.record.as_ref().map(RunReceipt::from),
                            error: item.error,
                            output_omitted: item.output.is_some(),
                        })
                        .collect(),
                    detail_omitted: true,
                };
                Ok(execution_success_json(&payload, &receipt))
            }
            Err(error) => Ok(error_json(classify_service_error(&error))),
        }
    }

    /// Query the run ledger (most-recent-first).
    #[tool(
        description = "Query recent run records from the ledger. Returns { items: [...] }, most recent first.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<HistoryArgs>()
    )]
    async fn vyane_history(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let args: HistoryArgs = match parse_arguments(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(error_json(error)),
        };
        let filter = match history_filter(args) {
            Ok(filter) => filter,
            Err(error) => return Ok(error_json(error)),
        };
        match self.service.history_views(filter).await {
            Ok(records) => Ok(success_json(&ToolItems { items: records })),
            Err(error) => Ok(error_json(classify_service_error(&error))),
        }
    }

    /// List saved sessions.
    #[tool(
        description = "List saved vyane sessions. Returns { items: [...] }.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<rmcp::model::EmptyObject>()
    )]
    async fn vyane_sessions(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        if !arguments.is_empty() {
            return Ok(error_json(SafeToolError::invalid_argument(
                "arguments do not match the tool schema",
            )));
        }
        match self.service.session_views().await {
            Ok(sessions) => Ok(success_json(&ToolItems { items: sessions })),
            Err(error) => Ok(error_json(classify_service_error(&error))),
        }
    }

    /// Preview deterministic routing without dispatching or echoing the task.
    #[tool(
        description = "Preview deterministic routing without dispatching. Returns an allowlisted profile/provider/model/tier/effort summary and never returns the task or raw routing hints.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<RouteArgs>()
    )]
    async fn vyane_route(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let args: RouteArgs = match parse_arguments(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(error_json(error)),
        };
        let params = RoutePreviewParams {
            task: args.task,
            stage: args.stage,
            changed_files: args.changed_files,
            dependency_edges: args.dependency_edges,
            retry_count: args.retry_count,
            explicit_tier: args.tier,
            extra_tags: args.tags.into_iter().map(|value| value.0).collect(),
            candidate_profiles: args.candidates.into_iter().map(|value| value.0).collect(),
            allow_frontier: args.allow_frontier.unwrap_or(true),
        };
        match self.service.route_preview(params) {
            Ok(preview) => Ok(diagnostic_success_json(&preview)),
            Err(error) => Ok(error_json(classify_service_error(&error))),
        }
    }

    /// Return a static-only redacted configuration check. This does not make
    /// network requests, probe harness binaries, or spawn child processes.
    #[tool(
        description = "Check the already-loaded configuration using static resolution only. Returns redacted readiness summaries; no paths, endpoint URLs, environment names, secrets, or raw errors.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<CheckArgs>()
    )]
    async fn vyane_check(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let _: CheckArgs = match parse_arguments(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(error_json(error)),
        };
        match self.service.check_config() {
            Ok(report) => Ok(diagnostic_success_json(&report)),
            Err(error) => Ok(error_json(classify_service_error(&error))),
        }
    }

    /// Submit one bounded, self-contained workflow source bundle.
    #[tool(
        description = "Submit a bounded workflow source bundle using a caller-selected canonical UUIDv7. The response is a redacted lifecycle view; reuse caller_id only for an identical retry after an outcome_unknown result.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<WorkflowSubmitArgs>()
    )]
    async fn vyane_workflow_submit(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        let args: WorkflowSubmitArgs = match parse_arguments(arguments) {
            Ok(args) => args,
            Err(error) => return Ok(error_json(error)),
        };
        let request = match workflow_submit_request(args) {
            Ok(request) => request,
            Err(error) => return Ok(error_json(error)),
        };
        let Some(control) = &self.workflow_control else {
            return Ok(error_json(SafeToolError::workflow_control(
                WorkflowControlError::Unavailable,
            )));
        };
        match control.submit(request).await {
            Ok(view) => Ok(success_json(&view)),
            Err(error) => Ok(error_json(SafeToolError::workflow_control(error))),
        }
    }

    /// Read one workflow's redacted lifecycle view.
    #[tool(
        description = "Return the redacted lifecycle state for one canonical UUIDv7 workflow caller id.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<WorkflowIdArgs>()
    )]
    async fn vyane_workflow_status(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        let caller_id = match workflow_caller_id(arguments) {
            Ok(caller_id) => caller_id,
            Err(error) => return Ok(error_json(error)),
        };
        let Some(control) = &self.workflow_control else {
            return Ok(error_json(SafeToolError::workflow_control(
                WorkflowControlError::Unavailable,
            )));
        };
        match control.status(caller_id).await {
            Ok(view) => Ok(success_json(&view)),
            Err(error) => Ok(error_json(SafeToolError::workflow_control(error))),
        }
    }

    /// Request cancellation. Repeated requests are idempotent: an already
    /// cancelling or terminal workflow is returned as a successful view.
    #[tool(
        description = "Idempotently request cancellation for one canonical UUIDv7 workflow caller id. Cancelling and terminal states are successful responses.",
        input_schema = rmcp::handler::server::tool::cached_schema_for_type::<WorkflowIdArgs>()
    )]
    async fn vyane_workflow_cancel(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        let caller_id = match workflow_caller_id(arguments) {
            Ok(caller_id) => caller_id,
            Err(error) => return Ok(error_json(error)),
        };
        let Some(control) = &self.workflow_control else {
            return Ok(error_json(SafeToolError::workflow_control(
                WorkflowControlError::Unavailable,
            )));
        };
        match control.cancel(caller_id).await {
            Ok(view) => Ok(success_json(&view)),
            Err(error) => Ok(error_json(SafeToolError::workflow_control(error))),
        }
    }
}

#[tool_handler]
impl rmcp::ServerHandler for VyaneMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
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

/// Run stdio MCP with an injected workflow control plane. Authentication and
/// daemon discovery remain the embedding process's responsibility.
pub async fn run_stdio_with_workflow_control(
    service: VyaneService,
    workflow_control: Arc<dyn WorkflowControl>,
) -> Result<()> {
    let server = VyaneMcpServer::with_workflow_control(service, workflow_control);
    let running = server.serve(stdio()).await?;
    running.waiting().await?;
    Ok(())
}

// ---- helpers ----------------------------------------------------------------

/// Deserialize raw MCP arguments inside the handler so `serde` diagnostics can
/// never cross the protocol boundary. Some diagnostics include the rejected
/// string value, which may itself be a path or secret supplied by a caller.
fn parse_arguments<T>(arguments: JsonObject) -> std::result::Result<T, SafeToolError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(serde_json::Value::Object(arguments))
        .map_err(|_| SafeToolError::invalid_argument("arguments do not match the tool schema"))
}

fn workflow_caller_id(arguments: JsonObject) -> std::result::Result<WorkflowRunId, SafeToolError> {
    let args: WorkflowIdArgs = parse_arguments(arguments)?;
    WorkflowRunId::from_str(&args.caller_id).map_err(|_| {
        SafeToolError::invalid_argument("caller_id must be a canonical lowercase UUIDv7")
    })
}

fn workflow_submit_request(
    args: WorkflowSubmitArgs,
) -> std::result::Result<WorkflowSubmitRequest, SafeToolError> {
    let caller_id = WorkflowRunId::from_str(&args.caller_id).map_err(|_| {
        SafeToolError::invalid_argument("caller_id must be a canonical lowercase UUIDv7")
    })?;
    if args.workflow_toml.is_empty() || args.workflow_toml.len() > MAX_WORKFLOW_TOML_BYTES {
        return Err(SafeToolError::invalid_argument(
            "workflow_toml exceeds the workflow safety limit",
        ));
    }
    if args.prompt_files.len() > MAX_WORKFLOW_SOURCES {
        return Err(SafeToolError::invalid_argument(
            "prompt_files exceed the workflow safety limit",
        ));
    }

    let mut source_bytes = args.workflow_toml.len();
    let mut prompt_files = Vec::with_capacity(args.prompt_files.len());
    for source in args.prompt_files {
        if source.path.is_empty()
            || source.path.len() > MAX_WORKFLOW_SOURCE_PATH_BYTES
            || source.content.len() > MAX_WORKFLOW_PROMPT_BYTES
        {
            return Err(SafeToolError::invalid_argument(
                "prompt_files exceed the workflow safety limit",
            ));
        }
        source_bytes = source_bytes
            .checked_add(source.path.len())
            .and_then(|total| total.checked_add(source.content.len()))
            .ok_or_else(|| {
                SafeToolError::invalid_argument("workflow sources exceed the workflow safety limit")
            })?;
        if source_bytes > MAX_WORKFLOW_SOURCE_BYTES {
            return Err(SafeToolError::invalid_argument(
                "workflow sources exceed the workflow safety limit",
            ));
        }
        let path = source.path.parse().map_err(|_| {
            SafeToolError::invalid_argument("prompt source path must be canonical and relative")
        })?;
        prompt_files.push(WorkflowSourceEntry {
            path,
            content: source.content,
        });
    }

    validate_workflow_vars(&args.vars)?;
    Ok(WorkflowSubmitRequest {
        caller_id,
        bundle: WorkflowSourceBundle {
            workflow_toml: args.workflow_toml,
            prompt_files,
        },
        vars: args.vars,
    })
}

fn validate_workflow_vars(
    vars: &BTreeMap<String, String>,
) -> std::result::Result<(), SafeToolError> {
    if vars.len() > MAX_WORKFLOW_VARS {
        return Err(SafeToolError::invalid_argument(
            "vars exceed the workflow safety limit",
        ));
    }
    let mut total = 0usize;
    for (key, value) in vars {
        if key.is_empty()
            || key.len() > MAX_WORKFLOW_VAR_KEY_BYTES
            || key.contains('\0')
            || value.len() > MAX_WORKFLOW_VAR_VALUE_BYTES
            || value.contains('\0')
        {
            return Err(SafeToolError::invalid_argument(
                "vars exceed the workflow safety limit",
            ));
        }
        total = total
            .checked_add(key.len())
            .and_then(|size| size.checked_add(value.len()))
            .ok_or_else(|| {
                SafeToolError::invalid_argument("vars exceed the workflow safety limit")
            })?;
        if total > MAX_WORKFLOW_VARS_BYTES {
            return Err(SafeToolError::invalid_argument(
                "vars exceed the workflow safety limit",
            ));
        }
    }
    Ok(())
}

/// Parse the MCP sandbox spelling without silently weakening caller intent.
///
/// Omission means read-only. Both public read-only spellings are accepted;
/// every other value is rejected before the service is invoked.
pub fn parse_sandbox(s: Option<&str>) -> Result<vyane_core::Sandbox, &'static str> {
    use vyane_core::Sandbox;
    match s {
        None | Some("read_only" | "read-only") => Ok(Sandbox::ReadOnly),
        Some("write") => Ok(Sandbox::Write),
        Some("full") => Ok(Sandbox::Full),
        Some(_) => Err("sandbox must be one of read_only, read-only, write, or full"),
    }
}

/// Parse an explicit history status. Unknown values are rejected rather than
/// being widened into an unfiltered query.
pub fn parse_status(s: &str) -> Result<RunStatus, &'static str> {
    match s {
        "success" => Ok(RunStatus::Success),
        "error" => Ok(RunStatus::Error),
        "timeout" => Ok(RunStatus::Timeout),
        "cancelled" | "canceled" => Ok(RunStatus::Cancelled),
        _ => Err("status must be one of success, error, timeout, cancelled, or canceled"),
    }
}

fn history_filter(args: HistoryArgs) -> std::result::Result<HistoryFilter, SafeToolError> {
    if !(1..=MAX_HISTORY_LIMIT).contains(&args.limit) {
        return Err(SafeToolError::invalid_argument(
            "limit must be between 1 and 1000",
        ));
    }
    let status = match args.status.as_deref() {
        None => None,
        Some(status) => Some(parse_status(status).map_err(SafeToolError::invalid_argument)?),
    };
    Ok(HistoryFilter {
        limit: Some(args.limit),
        status,
        provider: args.provider,
    })
}

fn validate_broadcast_targets(raw: &str) -> std::result::Result<(), SafeToolError> {
    if raw.len() > MAX_BROADCAST_TARGETS_BYTES {
        return Err(SafeToolError::invalid_argument(
            "targets exceed the MCP safety limit",
        ));
    }
    let count = raw
        .split(',')
        .filter(|selector| !selector.trim().is_empty())
        .take(MAX_BROADCAST_TARGETS + 1)
        .count();
    if count == 0 || count > MAX_BROADCAST_TARGETS {
        return Err(SafeToolError::invalid_argument(
            "targets must contain between 1 and 64 selectors",
        ));
    }
    Ok(())
}

fn dispatch_labels(args: &DispatchArgs) -> Vec<String> {
    let mut labels = args.labels.clone();
    if let Some(stage) = &args.route_stage {
        upsert_label(&mut labels, "routing.stage", stage);
    }
    if let Some(tier) = &args.route_tier {
        upsert_label(&mut labels, "routing.tier", tier);
    }
    if !args.route_tags.is_empty() {
        upsert_label(&mut labels, "routing.tags", &args.route_tags.join(","));
    }
    if !args.route_candidates.is_empty() {
        upsert_label(
            &mut labels,
            "routing.candidates",
            &args.route_candidates.join(","),
        );
    }
    if let Some(allow) = args.allow_frontier {
        upsert_label(
            &mut labels,
            "routing.allow_frontier",
            if allow { "true" } else { "false" },
        );
    }
    labels
}

fn upsert_label(labels: &mut Vec<String>, key: &str, value: &str) {
    labels.retain(|label| label.split_once('=').map(|(seen, _)| seen) != Some(key));
    labels.push(format!("{key}={value}"));
}

/// Serialize a value as a single JSON-text content block in a successful tool
/// result. Serialization itself cannot fail for our value types (all serde
/// derived); the `expect` matches that contract and surfaces a bug if it ever
/// does not hold.
fn success_json<T: Serialize + ?Sized>(value: &T) -> CallToolResult {
    match serialize_json_bounded(value, MAX_TOOL_OUTPUT_BYTES) {
        Some(text) => success_text(text),
        None => error_json(SafeToolError::output_limit()),
    }
}

/// Serialize one diagnostics result under a hard byte budget. The bounded
/// error envelope is intentionally outside this success-payload budget so an
/// overflow can always be reported.
fn diagnostic_success_json<T: Serialize + ?Sized>(value: &T) -> CallToolResult {
    match serialize_json_bounded(value, DIAGNOSTIC_MAX_OUTPUT_BYTES) {
        Some(text) => success_text(text),
        None => error_json(SafeToolError::diagnostic_limit()),
    }
}

/// Carry a safe error envelope in a successful MCP result. This function has no
/// source-error parameter by design, making accidental formatting impossible.
fn error_json(error: SafeToolError) -> CallToolResult {
    let text = serialize_json_bounded(
        &ToolErrorEnvelope {
            status: "error",
            error,
        },
        MAX_TOOL_OUTPUT_BYTES,
    )
    .expect("safe tool error is bounded by construction");
    success_text(text)
}

fn execution_success_json<T: Serialize + ?Sized, R: Serialize + ?Sized>(
    payload: &T,
    receipt: &R,
) -> CallToolResult {
    if let Some(text) = serialize_json_bounded(payload, MAX_TOOL_OUTPUT_BYTES) {
        return success_text(text);
    }
    let text = serialize_json_bounded(receipt, MAX_TOOL_OUTPUT_BYTES)
        .expect("bounded execution receipt fits the tool result budget");
    success_text(text)
}

fn success_text(text: String) -> CallToolResult {
    CallToolResult::success(vec![Content::text(text)])
}

/// Serialize directly into a capped buffer. Returning `None` discards the
/// partial prefix, so an oversized model response is neither duplicated into
/// an unbounded JSON string nor reflected in the limit error.
fn serialize_json_bounded<T: Serialize + ?Sized>(value: &T, limit: usize) -> Option<String> {
    let mut writer = BoundedJsonWriter::new(limit);
    match serde_json::to_writer_pretty(&mut writer, value) {
        Ok(()) => Some(
            String::from_utf8(writer.bytes).expect("serde_json writes UTF-8 bytes by construction"),
        ),
        Err(_) if writer.exceeded => None,
        Err(error) => panic!("tool result payload is JSON-serializable by construction: {error}"),
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl std::io::Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other("bounded JSON output exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Classify by typed domain kind while discarding every source message and
/// context frame. An untyped `anyhow` chain is always the generic internal
/// category.
fn classify_service_error(error: &anyhow::Error) -> SafeToolError {
    if let Some(error) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<DiagnosticError>())
    {
        return SafeToolError::from_diagnostic_kind(error.kind);
    }
    let kind = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<VyaneError>())
        .map(|error| error.kind);
    kind.map_or_else(
        || SafeToolError::from_kind(ErrorKind::Other),
        SafeToolError::from_kind,
    )
}

fn broadcast_error_item(selector: String, error: &anyhow::Error) -> BroadcastToolItem {
    BroadcastToolItem {
        target: selector,
        record: None,
        output: None,
        status: Some("error"),
        error: Some(classify_service_error(error)),
    }
}

// `Future` must be in scope for the `#[tool]` macro's async-rewriting in rmcp
// v0.5. Kept as a private import alias so the surface API stays clean.
#[allow(dead_code)]
type _FutureMustBeInScope = Box<dyn Future<Output = ()> + Send>;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rmcp::model::CallToolRequestParam;
    use vyane_core::Sandbox;
    use vyane_service::StoragePaths;

    fn receipt_run_view(run_id: &str) -> RunView {
        let now = chrono::Utc::now();
        RunView::from(vyane_core::RunRecord {
            run_id: run_id.into(),
            owner: "local".into(),
            started_at: now,
            finished_at: now,
            task_digest: "digest".into(),
            task_preview: None,
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            target: vyane_core::Target {
                provider: vyane_core::ProviderId::new("provider"),
                protocol: vyane_core::Protocol::OpenaiChat,
                harness: None,
                model: vyane_core::ModelId::new("model"),
            },
            transport: vyane_core::AdapterTransport::DirectHttp,
            attempts: Vec::new(),
            status: RunStatus::Success,
            usage: None,
            cost_usd: None,
            session_id: None,
            output_chars: Some(2_000_000),
            error: None,
            labels: Default::default(),
        })
    }

    #[test]
    fn sandbox_defaults_to_read_only() {
        assert_eq!(parse_sandbox(None).unwrap(), Sandbox::ReadOnly);
        assert_eq!(parse_sandbox(Some("read_only")).unwrap(), Sandbox::ReadOnly);
        assert_eq!(parse_sandbox(Some("read-only")).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_parses_write_and_full() {
        assert_eq!(parse_sandbox(Some("write")).unwrap(), Sandbox::Write);
        assert_eq!(parse_sandbox(Some("full")).unwrap(), Sandbox::Full);
        assert!(parse_sandbox(Some("unknown")).is_err());
    }

    #[test]
    fn status_parses_known_spells() {
        assert_eq!(parse_status("success").unwrap(), RunStatus::Success);
        assert_eq!(parse_status("error").unwrap(), RunStatus::Error);
        assert_eq!(parse_status("timeout").unwrap(), RunStatus::Timeout);
        assert_eq!(parse_status("cancelled").unwrap(), RunStatus::Cancelled);
        assert_eq!(parse_status("canceled").unwrap(), RunStatus::Cancelled);
    }

    #[test]
    fn status_unknown_is_rejected() {
        assert!(parse_status("nope").is_err());
        assert!(parse_status("").is_err());
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
    fn strict_argument_structs_reject_unknown_fields() {
        assert!(
            serde_json::from_str::<DispatchArgs>(
                r#"{"task":"hi","target":"default","surprise":true}"#,
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<BroadcastArgs>(
                r#"{"task":"hi","targets":"a,b","surprise":true}"#,
            )
            .is_err()
        );
        assert!(serde_json::from_str::<HistoryArgs>(r#"{"surprise":true}"#).is_err());
        assert!(serde_json::from_str::<RouteArgs>(r#"{"task":"hi","surprise":true}"#).is_err());
        assert!(serde_json::from_str::<CheckArgs>(r#"{"surprise":true}"#).is_err());
    }

    #[test]
    fn history_filter_rejects_unknown_status_and_out_of_range_limits() {
        for limit in [0, 1_001] {
            let error = history_filter(HistoryArgs {
                limit,
                ..HistoryArgs::default()
            })
            .unwrap_err();
            assert_eq!(error.code, PublicErrorCode::InvalidArgument);
            assert_eq!(error.message, "limit must be between 1 and 1000");
        }

        let error = history_filter(HistoryArgs {
            status: Some("unknown".into()),
            ..HistoryArgs::default()
        })
        .unwrap_err();
        assert_eq!(error.code, PublicErrorCode::InvalidArgument);

        let filter = history_filter(HistoryArgs {
            limit: 1_000,
            status: Some("success".into()),
            provider: Some("openai".into()),
        })
        .unwrap();
        assert_eq!(filter.limit, Some(1_000));
        assert_eq!(filter.status, Some(RunStatus::Success));
        assert_eq!(filter.provider.as_deref(), Some("openai"));
    }

    #[test]
    fn broadcast_target_count_and_bytes_are_bounded_before_execution() {
        assert!(validate_broadcast_targets("one").is_ok());
        assert!(validate_broadcast_targets(&vec!["x"; 64].join(",")).is_ok());
        assert!(validate_broadcast_targets("").is_err());
        assert!(validate_broadcast_targets(&vec!["x"; 65].join(",")).is_err());
        assert!(validate_broadcast_targets(&"x".repeat(MAX_BROADCAST_TARGETS_BYTES + 1)).is_err());
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
        assert!(args.labels.is_empty());
        assert!(args.route_stage.is_none());
        assert!(args.route_tier.is_none());
        assert!(args.route_tags.is_empty());
        assert!(args.route_candidates.is_empty());
        assert!(args.allow_frontier.is_none());
    }

    #[test]
    fn route_args_optional_fields_default_to_none_or_empty() {
        let args: RouteArgs = serde_json::from_str(r#"{"task":"review code"}"#).unwrap();
        assert_eq!(args.task, "review code");
        assert!(args.stage.is_none());
        assert!(args.changed_files.is_none());
        assert!(args.dependency_edges.is_none());
        assert!(args.retry_count.is_none());
        assert!(args.tier.is_none());
        assert!(args.tags.is_empty());
        assert!(args.candidates.is_empty());
        assert!(args.allow_frontier.is_none());
        let _: CheckArgs = serde_json::from_str("{}").unwrap();
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
            "timeout_secs":30,
            "labels":["ticket=42","routing.allow_frontier=true"],
            "route_stage":"review",
            "route_tags":["security","rust"],
            "route_candidates":["reviewer"],
            "allow_frontier":false
        }"#;
        let args: DispatchArgs = serde_json::from_str(json).unwrap();
        let labels = dispatch_labels(&args);
        let params = DispatchParams {
            task: args.task,
            target: args.target,
            workdir: args.workdir.map(Into::into),
            sandbox: parse_sandbox(args.sandbox.as_deref()).unwrap(),
            session: args.session,
            system: args.system,
            timeout_secs: args.timeout_secs,
            labels,
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
        assert!(params.labels.iter().any(|label| label == "ticket=42"));
        assert!(
            params
                .labels
                .iter()
                .any(|label| label == "routing.stage=review")
        );
        assert!(
            params
                .labels
                .iter()
                .any(|label| label == "routing.tags=security,rust")
        );
        assert!(
            params
                .labels
                .iter()
                .any(|label| label == "routing.candidates=reviewer")
        );
        let frontier_labels = params
            .labels
            .iter()
            .filter(|label| label.starts_with("routing.allow_frontier="))
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(frontier_labels, vec!["routing.allow_frontier=false"]);
    }

    #[test]
    fn anyhow_canary_is_not_exposed_by_error_envelope() {
        const CANARY: &str = "CANARY_SECRET_VALUE";
        let source = anyhow::anyhow!(CANARY);
        let payload = result_payload(error_json(classify_service_error(&source)));
        assert_eq!(payload["status"], "error");
        assert_eq!(payload["error"]["code"], "internal");
        assert_eq!(payload["error"]["message"], "operation failed");
        assert!(!payload.to_string().contains(CANARY));
        assert!(payload["error"]["message"].as_str().unwrap().len() <= 64);
    }

    #[test]
    fn diagnostics_payload_cap_accepts_exact_limit_and_rejects_one_more_byte() {
        let empty = serde_json::to_string_pretty(&serde_json::json!({ "value": "" })).unwrap();
        let exact_value_bytes = DIAGNOSTIC_MAX_OUTPUT_BYTES - empty.len();
        let exact = serde_json::json!({ "value": "x".repeat(exact_value_bytes) });
        assert_eq!(
            serde_json::to_string_pretty(&exact).unwrap().len(),
            DIAGNOSTIC_MAX_OUTPUT_BYTES
        );
        let exact_result = diagnostic_success_json(&exact);
        assert_eq!(exact_result.is_error, Some(false));
        assert!(result_payload(exact_result).get("value").is_some());

        let overflow_value = format!(
            "CANARY_OVERFLOW{}",
            "x".repeat(exact_value_bytes + 1 - "CANARY_OVERFLOW".len())
        );
        let overflow_payload = serde_json::json!({ "value": overflow_value });
        assert_eq!(
            serde_json::to_string_pretty(&overflow_payload)
                .unwrap()
                .len(),
            DIAGNOSTIC_MAX_OUTPUT_BYTES + 1
        );
        let overflow = diagnostic_success_json(&overflow_payload);
        let wire = serde_json::to_string(&overflow).unwrap();
        assert!(!wire.contains("CANARY_OVERFLOW"));
        assert_eq!(
            result_payload(overflow),
            serde_json::json!({
                "status": "error",
                "error": {
                    "code": "limit_exceeded",
                    "message": "diagnostic safety limit exceeded",
                }
            })
        );
    }

    #[test]
    fn generic_tool_payload_cap_rejects_content_without_echoing_it() {
        let empty = serde_json::to_string_pretty(&serde_json::json!({ "value": "" })).unwrap();
        let exact_value_bytes = MAX_TOOL_OUTPUT_BYTES - empty.len();
        let exact = serde_json::json!({ "value": "x".repeat(exact_value_bytes) });
        assert_eq!(
            serde_json::to_string_pretty(&exact).unwrap().len(),
            MAX_TOOL_OUTPUT_BYTES
        );
        assert!(result_payload(success_json(&exact)).get("value").is_some());

        let overflow_value = format!(
            "CANARY_TOOL_OVERFLOW{}",
            "x".repeat(exact_value_bytes + 1 - "CANARY_TOOL_OVERFLOW".len())
        );
        let overflow_payload = serde_json::json!({ "value": overflow_value });
        let overflow = success_json(&overflow_payload);
        let wire = serde_json::to_string(&overflow).unwrap();
        assert!(!wire.contains("CANARY_TOOL_OVERFLOW"));
        assert_eq!(
            result_payload(overflow),
            serde_json::json!({
                "status": "error",
                "error": {
                    "code": "limit_exceeded",
                    "message": "tool result safety limit exceeded",
                }
            })
        );

        let mut writer = BoundedJsonWriter::new(32);
        assert!(
            serde_json::to_writer_pretty(
                &mut writer,
                &serde_json::json!({ "value": "x".repeat(1024) }),
            )
            .is_err()
        );
        assert!(writer.exceeded);
        assert!(writer.bytes.len() <= 32);
    }

    #[test]
    fn oversized_executed_dispatch_returns_receipt_instead_of_retryable_error() {
        let payload = DispatchToolOutput {
            operation_status: "completed",
            record: receipt_run_view("run-receipt"),
            output: Some(format!(
                "CANARY_EXECUTED_OUTPUT{}",
                "x".repeat(MAX_TOOL_OUTPUT_BYTES)
            )),
            output_omitted: false,
            detail_omitted: false,
        };
        let fallback = DispatchExecutionReceipt {
            operation_status: "completed",
            receipt: RunReceipt::from(&payload.record),
            output_omitted: true,
            detail_omitted: true,
        };
        let result = execution_success_json(&payload, &fallback);
        let wire = serde_json::to_string(&result).unwrap();
        let body = result_payload(result);
        assert_eq!(body["operation_status"], "completed");
        assert_eq!(body["receipt"]["run_id"], "run-receipt");
        assert_eq!(body["receipt"]["run_status"], "success");
        assert_eq!(body["output_omitted"], true);
        assert_eq!(body["detail_omitted"], true);
        assert!(body.get("error").is_none());
        assert!(!wire.contains("CANARY_EXECUTED_OUTPUT"));
        assert!(wire.len() < MAX_TOOL_OUTPUT_BYTES);
    }

    #[test]
    fn oversized_executed_broadcast_preserves_all_bounded_receipts() {
        let items = (0..MAX_BROADCAST_TARGETS)
            .map(|index| BroadcastToolItem {
                target: format!("target-{index}"),
                record: Some(receipt_run_view(&format!("run-{index}"))),
                output: Some(format!(
                    "CANARY_BROADCAST_OUTPUT_{index}_{}",
                    "x".repeat(20 * 1024)
                )),
                status: None,
                error: None,
            })
            .collect::<Vec<_>>();
        let payload = BroadcastToolOutput {
            operation_status: "completed",
            items,
            detail_omitted: false,
        };
        let fallback = BroadcastExecutionReceipt {
            operation_status: "completed",
            items: payload
                .items
                .iter()
                .enumerate()
                .map(|(index, item)| BroadcastReceiptItem {
                    index,
                    receipt: item.record.as_ref().map(RunReceipt::from),
                    error: item.error,
                    output_omitted: item.output.is_some(),
                })
                .collect(),
            detail_omitted: true,
        };
        let result = execution_success_json(&payload, &fallback);
        let wire = serde_json::to_string(&result).unwrap();
        let body = result_payload(result);
        assert_eq!(body["operation_status"], "completed");
        assert_eq!(body["detail_omitted"], true);
        assert_eq!(
            body["items"].as_array().unwrap().len(),
            MAX_BROADCAST_TARGETS
        );
        assert_eq!(body["items"][63]["index"], 63);
        assert_eq!(body["items"][63]["receipt"]["run_id"], "run-63");
        assert_eq!(body["items"][63]["output_omitted"], true);
        assert!(body.get("error").is_none());
        assert!(!wire.contains("CANARY_BROADCAST_OUTPUT"));
        assert!(wire.len() < MAX_TOOL_OUTPUT_BYTES);
    }

    #[test]
    fn typed_error_chain_is_classified_without_exposing_messages() {
        const CANARY: &str = "CANARY_CONFIG_VALUE";
        let error = anyhow::Error::new(VyaneError::config(CANARY)).context("CANARY_CONTEXT_VALUE");
        let payload = result_payload(error_json(classify_service_error(&error)));
        assert_eq!(payload["error"]["code"], "config_invalid");
        assert_eq!(
            payload["error"]["message"],
            "vyane configuration is invalid"
        );
        assert!(!payload.to_string().contains("CANARY"));

        let cancelled = anyhow::Error::new(VyaneError::cancelled());
        let payload = result_payload(error_json(classify_service_error(&cancelled)));
        assert_eq!(payload["error"]["code"], "cancelled");

        let not_found =
            anyhow::Error::new(VyaneError::new(ErrorKind::NotFound, "CANARY_MISSING_VALUE"));
        let payload = result_payload(error_json(classify_service_error(&not_found)));
        assert_eq!(payload["error"]["code"], "invalid_argument");
        assert!(!payload.to_string().contains("CANARY"));
    }

    #[test]
    fn broadcast_item_failure_has_safe_structured_shape() {
        const CANARY: &str = "CANARY_BROADCAST_VALUE";
        let item = serde_json::to_value(broadcast_error_item(
            "missing-target".into(),
            &anyhow::anyhow!(CANARY),
        ))
        .unwrap();
        assert_eq!(item["target"], "missing-target");
        assert_eq!(item["status"], "error");
        assert_eq!(item["error"]["code"], "internal");
        assert_eq!(item["error"]["message"], "operation failed");
        assert!(!item.to_string().contains(CANARY));
        assert!(item.get("record").is_none());
        assert!(item["error"].is_object());
    }

    #[tokio::test]
    async fn real_mcp_handler_redacts_structural_argument_errors_and_keeps_schemas()
    -> anyhow::Result<()> {
        const TYPE_CANARY: &str = "CANARY_TYPE_VALUE";
        const FIELD_CANARY: &str = "CANARY_UNKNOWN_FIELD_TOKEN";
        const PATH_CANARY: &str = "CANARY_CONFIG_PATH.toml";
        const URL_CANARY: &str = "https://CANARY_BASE_URL.invalid/v1";
        const ENV_CANARY: &str = "CANARY_API_KEY_ENV";
        const SECRET_CANARY: &str = "CANARY_SECRET_VALUE";
        const TASK_CANARY: &str = "CANARY_TASK_PROMPT";
        const TAG_CANARY: &str = "CANARY_RAW_ROUTE_TAG";

        let test_root = std::env::temp_dir().join(format!(
            "vyane-mcp-safe-args-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir_all(&test_root)?;
        let config_path = test_root.join(PATH_CANARY);
        std::fs::write(
            &config_path,
            format!(
                r#"
                [providers.safe-provider]
                base_url = "{URL_CANARY}"
                api_key_env = "{ENV_CANARY}"
                auth_style = "bearer"
                protocol = "openai_chat"
                default_model = "safe-model"

                [profiles.safe-profile]
                provider = "safe-provider"
                model = "safe-model"
                tier = "economy"
                tags = ["code"]

                [profiles.bad-codex]
                provider = "safe-provider"
                protocol = "anthropic_messages"
                harness = "codex-cli"
                model = "safe-model"

                [profiles.bad-custom]
                provider = "safe-provider"
                harness = "custom-shell"
                model = "safe-model"
                "#,
            ),
        )?;
        std::fs::write(
            test_root.join("secrets.env"),
            format!("{ENV_CANARY}={SECRET_CANARY}\n"),
        )?;
        let loaded = vyane_service::load_config(Some(&config_path))?;
        let service = VyaneService::from_loaded_with_paths(
            loaded,
            StoragePaths::from_data_dir(test_root.join("data")),
        )?;
        let observer = service.clone();

        let (server_transport, client_transport) = tokio::io::duplex(512 * 1024);
        let server = VyaneMcpServer::new(service);
        let server_handle = tokio::spawn(async move {
            server.serve(server_transport).await?.waiting().await?;
            anyhow::Ok(())
        });
        let client =
            <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;

        let tools = client.list_all_tools().await?;
        assert_eq!(tools.len(), 6);
        for tool in &tools {
            let expected = match tool.name.as_ref() {
                "vyane_dispatch" => rmcp::handler::server::tool::cached_schema_for_type::<
                    rmcp::handler::server::tool::Parameters<DispatchArgs>,
                >(),
                "vyane_broadcast" => rmcp::handler::server::tool::cached_schema_for_type::<
                    rmcp::handler::server::tool::Parameters<BroadcastArgs>,
                >(),
                "vyane_history" => rmcp::handler::server::tool::cached_schema_for_type::<
                    rmcp::handler::server::tool::Parameters<HistoryArgs>,
                >(),
                "vyane_sessions" => rmcp::handler::server::tool::cached_schema_for_type::<
                    rmcp::model::EmptyObject,
                >(),
                "vyane_route" => rmcp::handler::server::tool::cached_schema_for_type::<
                    rmcp::handler::server::tool::Parameters<RouteArgs>,
                >(),
                "vyane_check" => rmcp::handler::server::tool::cached_schema_for_type::<
                    rmcp::handler::server::tool::Parameters<CheckArgs>,
                >(),
                other => panic!("unexpected tool {other}"),
            };
            assert_eq!(tool.input_schema.as_ref(), expected.as_ref());
            assert!(tool.description.is_some());
            if tool.name.as_ref() == "vyane_route" {
                assert_eq!(
                    tool.input_schema.get("additionalProperties"),
                    Some(&serde_json::json!(false))
                );
                let properties = tool.input_schema["properties"].as_object().unwrap();
                assert_eq!(properties["task"]["minLength"], 1);
                assert_eq!(properties["task"]["maxLength"], 65_536);
                assert_eq!(properties["changed_files"]["maximum"], 1_000_000);
                assert_eq!(properties["tags"]["maxItems"], 64);
                assert_eq!(properties["candidates"]["maxItems"], 64);
                assert_eq!(
                    properties["tags"]["items"]["$ref"],
                    "#/definitions/RouteValue"
                );
                assert_eq!(
                    properties["candidates"]["items"]["$ref"],
                    "#/definitions/RouteValue"
                );
                assert_eq!(
                    tool.input_schema["definitions"]["RouteValue"]["maxLength"],
                    256
                );
            }
            if tool.name.as_ref() == "vyane_check" {
                assert_eq!(
                    tool.input_schema.get("additionalProperties"),
                    Some(&serde_json::json!(false))
                );
                assert!(
                    tool.input_schema
                        .get("properties")
                        .is_none_or(|properties| properties
                            .as_object()
                            .is_some_and(JsonObject::is_empty))
                );
            }
        }

        let route_result = client
            .call_tool(CallToolRequestParam {
                name: "vyane_route".into(),
                arguments: Some(
                    serde_json::json!({
                        "task": TASK_CANARY,
                        "tier": "economy",
                        "tags": [TAG_CANARY],
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ),
            })
            .await?;
        assert_eq!(route_result.is_error, Some(false));
        let route_wire = serde_json::to_string(&route_result)?;
        let route_payload = result_payload(route_result);
        assert_eq!(route_payload["profile"], "safe-profile");
        assert_eq!(route_payload["provider"], "safe-provider");
        assert_eq!(route_payload["tier"], "economy");
        for canary in [
            PATH_CANARY,
            URL_CANARY,
            ENV_CANARY,
            SECRET_CANARY,
            TASK_CANARY,
            TAG_CANARY,
        ] {
            assert!(!route_wire.contains(canary), "route leaked {canary}");
        }

        let check_result = client
            .call_tool(CallToolRequestParam {
                name: "vyane_check".into(),
                arguments: Some(JsonObject::new()),
            })
            .await?;
        assert_eq!(check_result.is_error, Some(false));
        let check_wire = serde_json::to_string(&check_result)?;
        let check_payload = result_payload(check_result);
        assert_eq!(check_payload["status"], "partial");
        assert_eq!(check_payload["scope"], "static_config_only");
        for name in ["bad-codex", "bad-custom"] {
            let profile = check_payload["profiles"]
                .as_array()
                .unwrap()
                .iter()
                .find(|profile| profile["name"] == name)
                .unwrap();
            assert_eq!(profile["status"], "unresolvable");
            assert_eq!(profile["issue"]["code"], "target_unsupported");
        }
        for canary in [PATH_CANARY, URL_CANARY, ENV_CANARY, SECRET_CANARY] {
            assert!(!check_wire.contains(canary), "check leaked {canary}");
        }

        let invalid_calls = [
            ("vyane_history", serde_json::json!({ "limit": TYPE_CANARY })),
            (
                "vyane_dispatch",
                serde_json::json!({
                    "task": "hello",
                    "target": "default",
                    (FIELD_CANARY): true,
                }),
            ),
            (
                "vyane_sessions",
                serde_json::json!({ (FIELD_CANARY): TYPE_CANARY }),
            ),
            (
                "vyane_route",
                serde_json::json!({
                    "task": "hello",
                    (FIELD_CANARY): TYPE_CANARY,
                }),
            ),
            (
                "vyane_check",
                serde_json::json!({ (FIELD_CANARY): TYPE_CANARY }),
            ),
        ];
        for (name, arguments) in invalid_calls {
            let result = client
                .call_tool(CallToolRequestParam {
                    name: name.to_string().into(),
                    arguments: Some(arguments.as_object().expect("object").clone()),
                })
                .await?;
            assert_eq!(result.is_error, Some(false));
            let wire = serde_json::to_string(&result)?;
            assert!(!wire.contains(TYPE_CANARY));
            assert!(!wire.contains(FIELD_CANARY));
            assert_eq!(
                result_payload(result),
                serde_json::json!({
                    "status": "error",
                    "error": {
                        "code": "invalid_argument",
                        "message": "arguments do not match the tool schema",
                    }
                })
            );
        }

        let semantic_inputs = [
            serde_json::json!({
                "task": TASK_CANARY,
                "tier": TYPE_CANARY,
            }),
            serde_json::json!({
                "task": TASK_CANARY,
                "changed_files": vyane_service::ROUTE_PREVIEW_MAX_SIGNAL + 1,
            }),
            serde_json::json!({
                "task": TASK_CANARY,
                "tags": vec![TAG_CANARY; vyane_service::ROUTE_PREVIEW_MAX_LIST_ITEMS + 1],
            }),
            serde_json::json!({
                "task": TASK_CANARY,
                "candidates": [TYPE_CANARY],
            }),
            serde_json::json!({
                "task": format!(
                    "{TASK_CANARY}{}",
                    "x".repeat(vyane_service::ROUTE_PREVIEW_MAX_TASK_BYTES)
                ),
            }),
            serde_json::json!({
                "task": format!("{TASK_CANARY}\0"),
            }),
            serde_json::json!({
                "task": TASK_CANARY,
                "stage": format!("review\n{TYPE_CANARY}"),
            }),
        ];
        for arguments in semantic_inputs {
            let semantic_error = client
                .call_tool(CallToolRequestParam {
                    name: "vyane_route".into(),
                    arguments: Some(arguments.as_object().unwrap().clone()),
                })
                .await?;
            let semantic_wire = serde_json::to_string(&semantic_error)?;
            assert!(!semantic_wire.contains(TASK_CANARY));
            assert!(!semantic_wire.contains(TYPE_CANARY));
            assert!(!semantic_wire.contains(TAG_CANARY));
            assert_eq!(
                result_payload(semantic_error),
                serde_json::json!({
                    "status": "error",
                    "error": {
                        "code": "invalid_argument",
                        "message": "request contains an invalid argument",
                    }
                })
            );
        }

        assert!(
            observer
                .history(HistoryFilter {
                    limit: Some(1),
                    ..Default::default()
                })
                .await?
                .is_empty(),
            "diagnostic tools must not append run records"
        );
        assert!(
            observer.sessions().await?.is_empty(),
            "diagnostic tools must not create or update sessions"
        );

        client.cancel().await?;
        server_handle.await??;
        std::fs::remove_dir_all(test_root)?;
        Ok(())
    }

    fn result_payload(result: CallToolResult) -> serde_json::Value {
        let wire = serde_json::to_value(result).unwrap();
        let text = wire["content"][0]["text"].as_str().unwrap();
        serde_json::from_str(text).unwrap()
    }
}
