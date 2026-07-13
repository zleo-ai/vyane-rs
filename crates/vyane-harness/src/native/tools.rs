use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt as _;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use vyane_core::{NativeExecutionAuthority, NativeSideEffect, Result as VyaneResult};

use super::{ApprovalPlan, PermissionEffect, PermissionPolicy};

/// Maximum number of Unicode scalar values returned to a model from one tool.
pub const MAX_TOOL_OUTPUT_CHARS: usize = 30_000;

/// Hard limits applied before a native tool call reaches permissions or code.
pub struct ToolCallLimits;

impl ToolCallLimits {
    pub const ID_BYTES: usize = 256;
    pub const NAME_BYTES: usize = 128;
    pub const ARGUMENT_COUNT: usize = 64;
    pub const ARGUMENT_NAME_BYTES: usize = 256;
    pub const JSON_DEPTH: usize = 16;
    pub const JSON_NODES: usize = 262_144;
    pub const SERIALIZED_ARGUMENT_BYTES: usize = 256 * 1024;
}

/// A provider-neutral tool call produced by a native model turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCallSummary {
    call_id: String,
    tool: String,
    argument_names: Vec<String>,
}

impl ToolCallSummary {
    fn from_call(call: &ToolCall) -> Self {
        Self {
            call_id: bounded_utf8(&call.id, ToolCallLimits::ID_BYTES),
            tool: bounded_utf8(&call.name, ToolCallLimits::NAME_BYTES),
            argument_names: call
                .arguments
                .keys()
                .take(ToolCallLimits::ARGUMENT_COUNT)
                .map(|name| bounded_utf8(name, ToolCallLimits::ARGUMENT_NAME_BYTES))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ToolCallValidationError {
    EmptyId,
    IdTooLarge,
    UnsafeId,
    EmptyName,
    NameTooLarge,
    UnsafeName,
    TooManyArguments,
    ArgumentNameTooLarge,
    JsonTooDeep,
    TooManyJsonNodes,
    ArgumentsTooLarge,
    ArgumentsNotSerializable,
}

impl ToolCallValidationError {
    fn code(&self) -> &'static str {
        match self {
            Self::EmptyId => "empty_call_id",
            Self::IdTooLarge => "call_id_too_large",
            Self::UnsafeId => "unsafe_call_id",
            Self::EmptyName => "empty_tool_name",
            Self::NameTooLarge => "tool_name_too_large",
            Self::UnsafeName => "unsafe_tool_name",
            Self::TooManyArguments => "too_many_arguments",
            Self::ArgumentNameTooLarge => "argument_name_too_large",
            Self::JsonTooDeep => "arguments_too_deep",
            Self::TooManyJsonNodes => "too_many_argument_nodes",
            Self::ArgumentsTooLarge => "arguments_too_large",
            Self::ArgumentsNotSerializable => "arguments_not_serializable",
        }
    }
}

/// Non-secret execution context shared by native tools.
#[derive(Debug, Clone)]
pub struct ToolContext {
    workdir: PathBuf,
    cancellation: CancellationToken,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl ToolContext {
    /// Resolve an existing working directory once so approval hashes and tool
    /// executors bind to the same absolute location.
    pub fn new(workdir: impl Into<PathBuf>) -> Result<Self, ToolContextError> {
        let requested = workdir.into();
        let workdir =
            std::fs::canonicalize(&requested).map_err(|source| ToolContextError::Canonicalize {
                path: requested,
                source,
            })?;
        if !workdir.is_dir() {
            return Err(ToolContextError::NotDirectory(workdir));
        }
        Ok(Self {
            workdir,
            cancellation: CancellationToken::new(),
            timeout: None,
            deadline: None,
        })
    }

    pub fn workdir(&self) -> &std::path::Path {
        &self.workdir
    }

    #[must_use]
    pub fn with_cancellation_token(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation
    }

    fn effective_deadline(&self) -> Option<Instant> {
        let timeout_deadline = self
            .timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        match (timeout_deadline, self.deadline) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ToolContextError {
    #[error("could not resolve native tool workdir `{}`: {source}", path.display())]
    Canonicalize {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("native tool workdir is not a directory: `{}`", .0.display())]
    NotDirectory(PathBuf),
}

/// A recoverable, model-facing tool failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct ToolError {
    message: String,
}

impl ToolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Executable native-harness tool seam.
///
/// The registry may drop the returned future on cancellation or timeout. A tool
/// that owns a subprocess or other external side effect must therefore make
/// future-drop cleanup safe and should also observe
/// [`ToolContext::cancellation_token`] during long operations.
///
/// Permission text matching is not a filesystem capability check. A tool that
/// accepts paths must resolve them against [`ToolContext::workdir`], reject
/// symlink/race escapes, and enforce its own read/write roots immediately
/// before the side effect.
#[async_trait]
pub trait NativeTool: Send + Sync {
    fn name(&self) -> &str;

    async fn execute(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
    ) -> Result<String, ToolError>;
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ToolRegistryError {
    #[error("tool name must not be empty")]
    EmptyName,
    #[error("tool name exceeds {} bytes", ToolCallLimits::NAME_BYTES)]
    NameTooLarge,
    #[error("tool name contains unsafe characters")]
    UnsafeName,
    #[error("tool `{0}` is already registered")]
    Duplicate(String),
}

/// Deterministically ordered collection of executable native tools.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn NativeTool>>,
}

impl std::fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field("tools", &self.tools.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, tool: Arc<dyn NativeTool>) -> Result<(), ToolRegistryError> {
        let name = tool.name().to_string();
        if name.is_empty() {
            return Err(ToolRegistryError::EmptyName);
        }
        if name.len() > ToolCallLimits::NAME_BYTES {
            return Err(ToolRegistryError::NameTooLarge);
        }
        if !is_safe_tool_name(&name) {
            return Err(ToolRegistryError::UnsafeName);
        }
        if self.tools.contains_key(&name) {
            return Err(ToolRegistryError::Duplicate(name));
        }
        self.tools.insert(name, tool);
        Ok(())
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.tools.keys().map(String::as_str)
    }

    /// Validate, permission-check, and invoke one tool call. Denied,
    /// approval-required, invalid, cancelled, and already-expired calls never
    /// reach (or no longer retain) the executor. All outcomes become a bounded,
    /// structured, model-facing result.
    ///
    /// This compatibility entry point does not consume a
    /// [`NativeExecutionAuthority`]. A native model loop must use
    /// [`Self::execute_authorized`] instead.
    pub async fn execute(
        &self,
        call: ToolCall,
        context: &ToolContext,
        policy: &PermissionPolicy,
    ) -> ToolInvocation {
        self.execute_inner(call, context, policy, None).await
    }

    /// Execute one permitted call behind a live native-execution authority.
    ///
    /// Invalid, unknown, denied, approval-required, cancelled, and already
    /// expired calls remain pure decisions and do not consume authority. An
    /// allowed call is revalidated immediately before its executor is polled;
    /// an authority failure escapes as an outer [`vyane_core::VyaneError`] so a future
    /// model loop cannot turn revocation into ordinary model-facing tool text.
    ///
    /// This is the registry dispatch boundary, not authorization for an
    /// arbitrary third-party tool to perform an unbounded number of external
    /// operations. Production native tools remain disabled until each trusted
    /// implementation revalidates at every additional open, publish, or spawn
    /// linearization point.
    pub async fn execute_authorized(
        &self,
        call: ToolCall,
        context: &ToolContext,
        policy: &PermissionPolicy,
        authority: &dyn NativeExecutionAuthority,
        turn: u32,
        ordinal: u32,
    ) -> VyaneResult<ToolInvocation> {
        self.execute_authorized_inner(call, context, policy, authority, turn, ordinal)
            .await
    }

    /// Temporary crate-private observation seam. The callback is synchronous,
    /// so it is intentionally excluded from the public API until a bounded
    /// non-blocking event queue is designed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn execute_observed(
        &self,
        call: ToolCall,
        context: &ToolContext,
        policy: &PermissionPolicy,
        reporter: &ToolEventReporter,
    ) -> ToolInvocation {
        self.execute_inner(call, context, policy, Some(reporter))
            .await
    }

    async fn execute_inner(
        &self,
        call: ToolCall,
        context: &ToolContext,
        policy: &PermissionPolicy,
        reporter: Option<&ToolEventReporter>,
    ) -> ToolInvocation {
        let preparation = self.prepare_invocation(call, context, policy, reporter);
        let invocation = match preparation.outcome {
            PreparedToolOutcome::Complete(invocation) => *invocation,
            PreparedToolOutcome::Allowed { call, deadline } => {
                self.execute_allowed(call, context, deadline).await
            }
        };
        report_post(reporter, preparation.summary, &invocation);
        invocation
    }

    async fn execute_authorized_inner(
        &self,
        call: ToolCall,
        context: &ToolContext,
        policy: &PermissionPolicy,
        authority: &dyn NativeExecutionAuthority,
        turn: u32,
        ordinal: u32,
    ) -> VyaneResult<ToolInvocation> {
        let reporter = None;
        let preparation = self.prepare_invocation(call, context, policy, reporter);
        let invocation = match preparation.outcome {
            PreparedToolOutcome::Complete(invocation) => *invocation,
            PreparedToolOutcome::Allowed { call, deadline } => {
                if let Some((status, output)) = terminal_outcome(context, deadline) {
                    ToolInvocation::new(call, status, output.into(), None)
                } else {
                    let validation =
                        authority.revalidate(NativeSideEffect::ToolOperation { turn, ordinal });
                    let deadline_wait = wait_for_deadline(deadline);
                    tokio::pin!(validation);
                    tokio::pin!(deadline_wait);
                    tokio::select! {
                        biased;
                        _ = context.cancellation_token().cancelled() => {
                            ToolInvocation::new(
                                call,
                                ToolInvocationStatus::Cancelled,
                                "ERROR: tool execution cancelled".into(),
                                None,
                            )
                        }
                        _ = &mut deadline_wait => {
                            ToolInvocation::new(
                                call,
                                ToolInvocationStatus::TimedOut,
                                "ERROR: tool execution timed out".into(),
                                None,
                            )
                        }
                        result = &mut validation => {
                            result?;
                            self.execute_allowed(call, context, deadline).await
                        }
                    }
                }
            }
        };
        report_post(reporter, preparation.summary, &invocation);
        Ok(invocation)
    }

    fn prepare_invocation(
        &self,
        call: ToolCall,
        context: &ToolContext,
        policy: &PermissionPolicy,
        reporter: Option<&ToolEventReporter>,
    ) -> PreparedToolInvocation {
        let summary = ToolCallSummary::from_call(&call);
        report(
            reporter,
            ToolInvocationEvent::PreToolUse {
                call: summary.clone(),
            },
        );
        if let Err(error) = validate_tool_call(&call) {
            let invocation = ToolInvocation::new(
                call,
                ToolInvocationStatus::InvalidCall,
                format!("ERROR: invalid tool call ({})", error.code()),
                None,
            );
            return PreparedToolInvocation::complete(summary, invocation);
        }
        if !self.tools.contains_key(&call.name) {
            let available = if self.tools.is_empty() {
                "(none)".to_string()
            } else {
                self.names().collect::<Vec<_>>().join(", ")
            };
            let output = format!(
                "ERROR: unknown tool `{}`. Available tools: {available}",
                call.name
            );
            let invocation =
                ToolInvocation::new(call, ToolInvocationStatus::UnknownTool, output, None);
            return PreparedToolInvocation::complete(summary, invocation);
        }

        // Do not create approval work after local cancellation or expiry. Keep
        // the same deadline for the policy and execution phases so a relative
        // timeout cannot silently restart.
        let deadline = context.effective_deadline();
        if let Some((status, output)) = terminal_outcome(context, deadline) {
            let invocation = ToolInvocation::new(call, status, output.into(), None);
            return PreparedToolInvocation::complete(summary, invocation);
        }

        let decision = policy.decide(&call, context);
        let outcome = match decision.effect {
            PermissionEffect::Ask => {
                let Some(approval) = decision.approval else {
                    let invocation = ToolInvocation::new(
                        call,
                        ToolInvocationStatus::ToolError,
                        "ERROR: permission policy did not produce an approval plan".into(),
                        None,
                    );
                    return PreparedToolInvocation::complete(summary, invocation);
                };
                report(
                    reporter,
                    ToolInvocationEvent::PermissionRequest {
                        approval: approval.clone(),
                    },
                );
                PreparedToolOutcome::Complete(Box::new(ToolInvocation::new(
                    call,
                    ToolInvocationStatus::ApprovalRequired,
                    format!(
                        "ERROR: tool use requires approval: `{}` (plan {})",
                        approval.tool,
                        &approval.canonical_plan_hash[..12]
                    ),
                    Some(approval),
                )))
            }
            PermissionEffect::Deny => {
                let output = format!(
                    "ERROR: tool use denied by permission policy: `{}`",
                    call.name
                );
                PreparedToolOutcome::Complete(Box::new(ToolInvocation::new(
                    call,
                    ToolInvocationStatus::Denied,
                    output,
                    None,
                )))
            }
            PermissionEffect::Allow => PreparedToolOutcome::Allowed { call, deadline },
        };
        PreparedToolInvocation { summary, outcome }
    }

    async fn execute_allowed(
        &self,
        call: ToolCall,
        context: &ToolContext,
        deadline: Option<Instant>,
    ) -> ToolInvocation {
        // Check again after policy evaluation to close the cancellation/deadline
        // race between the pre-policy gate and executor polling.
        if let Some((status, output)) = terminal_outcome(context, deadline) {
            return ToolInvocation::new(call, status, output.into(), None);
        }
        let Some(tool) = self.tools.get(&call.name) else {
            // Registration is immutable while `&self` is held, so lookup after
            // the pre-permission existence check can only fail if this invariant
            // changes in a future registry implementation. Keep it fail-closed.
            return ToolInvocation::new(
                call,
                ToolInvocationStatus::ToolError,
                "ERROR: registered tool became unavailable".into(),
                None,
            );
        };
        let execution = {
            let execution = AssertUnwindSafe(tool.execute(&call.arguments, context)).catch_unwind();
            let deadline = wait_for_deadline(deadline);
            tokio::pin!(execution);
            tokio::pin!(deadline);
            tokio::select! {
                biased;
                _ = context.cancellation_token().cancelled() => ToolExecution::Cancelled,
                _ = &mut deadline => ToolExecution::TimedOut,
                result = &mut execution => match result {
                    Ok(Ok(output)) => ToolExecution::Completed(output),
                    Ok(Err(error)) => ToolExecution::Failed(error),
                    Err(_) => ToolExecution::Panicked,
                },
            }
        };
        match execution {
            ToolExecution::Completed(output) => {
                ToolInvocation::new(call, ToolInvocationStatus::Executed, output, None)
            }
            ToolExecution::Failed(error) => ToolInvocation::new(
                call,
                ToolInvocationStatus::ToolError,
                format!("ERROR: {error}"),
                None,
            ),
            ToolExecution::Panicked => {
                let output = format!("ERROR: tool `{}` panicked during execution", call.name);
                ToolInvocation::new(call, ToolInvocationStatus::ToolError, output, None)
            }
            ToolExecution::Cancelled => ToolInvocation::new(
                call,
                ToolInvocationStatus::Cancelled,
                "ERROR: tool execution cancelled".into(),
                None,
            ),
            ToolExecution::TimedOut => ToolInvocation::new(
                call,
                ToolInvocationStatus::TimedOut,
                "ERROR: tool execution timed out".into(),
                None,
            ),
        }
    }
}

struct PreparedToolInvocation {
    summary: ToolCallSummary,
    outcome: PreparedToolOutcome,
}

impl PreparedToolInvocation {
    fn complete(summary: ToolCallSummary, invocation: ToolInvocation) -> Self {
        Self {
            summary,
            outcome: PreparedToolOutcome::Complete(Box::new(invocation)),
        }
    }
}

enum PreparedToolOutcome {
    Complete(Box<ToolInvocation>),
    Allowed {
        call: ToolCall,
        deadline: Option<Instant>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolInvocationStatus {
    Executed,
    ApprovalRequired,
    Denied,
    InvalidCall,
    UnknownTool,
    ToolError,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolInvocation {
    pub call: ToolCall,
    pub status: ToolInvocationStatus,
    /// Model-facing output. At most [`MAX_TOOL_OUTPUT_CHARS`] Unicode scalar
    /// values from the tool are retained, followed by a short truncation marker.
    pub output: String,
    /// Character count of the complete, pre-truncation output.
    pub output_chars: usize,
    /// SHA-256 of the complete UTF-8 output, including content not retained in
    /// `output`.
    pub output_sha256: String,
    pub output_truncated: bool,
    pub approval: Option<ApprovalPlan>,
}

impl ToolInvocation {
    fn new(
        call: ToolCall,
        status: ToolInvocationStatus,
        raw_output: String,
        approval: Option<ApprovalPlan>,
    ) -> Self {
        let output_chars = raw_output.chars().count();
        let output_sha256 = sha256_hex(raw_output.as_bytes());
        let output_truncated = output_chars > MAX_TOOL_OUTPUT_CHARS;
        let output = if output_truncated {
            let prefix = raw_output
                .chars()
                .take(MAX_TOOL_OUTPUT_CHARS)
                .collect::<String>();
            format!("{prefix}\n... [truncated, {output_chars} chars total, sha256={output_sha256}]")
        } else {
            raw_output
        };
        Self {
            call,
            status,
            output,
            output_chars,
            output_sha256,
            output_truncated,
            approval,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ToolInvocationEvent {
    PreToolUse {
        call: ToolCallSummary,
    },
    /// Carries the exact canonical plan, including argument values. This event
    /// is sensitive and must only enter an approval store with equivalent
    /// access controls; ordinary lifecycle events are redacted summaries.
    PermissionRequest {
        approval: ApprovalPlan,
    },
    PostToolUse {
        call: ToolCallSummary,
        status: ToolInvocationStatus,
        output_chars: usize,
        output_sha256: String,
        output_truncated: bool,
    },
}

/// Best-effort synchronous observer for crate-internal tests and composition.
/// It remains crate-private because a slow callback can block its caller. A
/// future public reporter must use a bounded non-blocking queue.
#[derive(Clone)]
pub(crate) struct ToolEventReporter {
    callback: Arc<dyn Fn(ToolInvocationEvent) + Send + Sync + 'static>,
}

impl ToolEventReporter {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(callback: impl Fn(ToolInvocationEvent) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    fn report(&self, event: ToolInvocationEvent) {
        if catch_unwind(AssertUnwindSafe(|| (self.callback)(event))).is_err() {
            tracing::warn!("native tool event reporter panicked; ignoring observer failure");
        }
    }
}

impl std::fmt::Debug for ToolEventReporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolEventReporter")
            .finish_non_exhaustive()
    }
}

fn report(reporter: Option<&ToolEventReporter>, event: ToolInvocationEvent) {
    if let Some(reporter) = reporter {
        reporter.report(event);
    }
}

fn report_post(
    reporter: Option<&ToolEventReporter>,
    call: ToolCallSummary,
    invocation: &ToolInvocation,
) {
    report(
        reporter,
        ToolInvocationEvent::PostToolUse {
            call,
            status: invocation.status,
            output_chars: invocation.output_chars,
            output_sha256: invocation.output_sha256.clone(),
            output_truncated: invocation.output_truncated,
        },
    );
}

enum ToolExecution {
    Completed(String),
    Failed(ToolError),
    Panicked,
    Cancelled,
    TimedOut,
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn validate_tool_call(call: &ToolCall) -> Result<(), ToolCallValidationError> {
    if call.id.is_empty() {
        return Err(ToolCallValidationError::EmptyId);
    }
    if call.id.len() > ToolCallLimits::ID_BYTES {
        return Err(ToolCallValidationError::IdTooLarge);
    }
    if !is_safe_call_id(&call.id) {
        return Err(ToolCallValidationError::UnsafeId);
    }
    if call.name.is_empty() {
        return Err(ToolCallValidationError::EmptyName);
    }
    if call.name.len() > ToolCallLimits::NAME_BYTES {
        return Err(ToolCallValidationError::NameTooLarge);
    }
    if !is_safe_tool_name(&call.name) {
        return Err(ToolCallValidationError::UnsafeName);
    }
    if call.arguments.len() > ToolCallLimits::ARGUMENT_COUNT {
        return Err(ToolCallValidationError::TooManyArguments);
    }
    if call
        .arguments
        .keys()
        .any(|name| name.len() > ToolCallLimits::ARGUMENT_NAME_BYTES)
    {
        return Err(ToolCallValidationError::ArgumentNameTooLarge);
    }
    validate_json_shape(&call.arguments)?;
    validate_serialized_size(&call.arguments)
}

fn is_safe_call_id(value: &str) -> bool {
    value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'?')
    })
}

fn is_safe_tool_name(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn terminal_outcome(
    context: &ToolContext,
    deadline: Option<Instant>,
) -> Option<(ToolInvocationStatus, &'static str)> {
    if context.cancellation_token().is_cancelled() {
        return Some((
            ToolInvocationStatus::Cancelled,
            "ERROR: tool execution cancelled",
        ));
    }
    if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
        return Some((
            ToolInvocationStatus::TimedOut,
            "ERROR: tool execution timed out",
        ));
    }
    None
}

fn validate_json_shape(arguments: &BTreeMap<String, Value>) -> Result<(), ToolCallValidationError> {
    let mut stack = arguments
        .values()
        .map(|value| (value, 0_usize))
        .collect::<Vec<_>>();
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > ToolCallLimits::JSON_NODES {
            return Err(ToolCallValidationError::TooManyJsonNodes);
        }
        match value {
            Value::Array(items) => {
                let child_depth = depth.saturating_add(1);
                if child_depth > ToolCallLimits::JSON_DEPTH {
                    return Err(ToolCallValidationError::JsonTooDeep);
                }
                stack.extend(items.iter().map(|item| (item, child_depth)));
            }
            Value::Object(items) => {
                let child_depth = depth.saturating_add(1);
                if child_depth > ToolCallLimits::JSON_DEPTH {
                    return Err(ToolCallValidationError::JsonTooDeep);
                }
                if items
                    .keys()
                    .any(|name| name.len() > ToolCallLimits::ARGUMENT_NAME_BYTES)
                {
                    return Err(ToolCallValidationError::ArgumentNameTooLarge);
                }
                stack.extend(items.values().map(|item| (item, child_depth)));
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn validate_serialized_size(
    arguments: &BTreeMap<String, Value>,
) -> Result<(), ToolCallValidationError> {
    let mut writer = BoundedJsonWriter::new(ToolCallLimits::SERIALIZED_ARGUMENT_BYTES);
    match serde_json::to_writer(&mut writer, arguments) {
        Ok(()) => Ok(()),
        Err(_) if writer.exceeded => Err(ToolCallValidationError::ArgumentsTooLarge),
        Err(_) => Err(ToolCallValidationError::ArgumentsNotSerializable),
    }
}

struct BoundedJsonWriter {
    written: usize,
    limit: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            written: 0,
            limit,
            exceeded: false,
        }
    }
}

impl std::io::Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.written);
        if bytes.len() > remaining {
            self.exceeded = true;
            return Err(std::io::Error::other("native tool arguments exceed limit"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    if max_bytes == 0 {
        return String::new();
    }
    let mut end = max_bytes.saturating_sub('…'.len_utf8());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut bounded = value[..end].to_string();
    if max_bytes >= '…'.len_utf8() {
        bounded.push('…');
    }
    bounded
}

fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::native::{PermissionRule, PermissionRuleError};

    struct CountingTool {
        calls: Arc<AtomicUsize>,
        result: Result<String, ToolError>,
    }

    #[async_trait]
    impl NativeTool for CountingTool {
        fn name(&self) -> &str {
            "write_file"
        }

        async fn execute(
            &self,
            _arguments: &BTreeMap<String, Value>,
            _context: &ToolContext,
        ) -> Result<String, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }
    }

    struct HangingTool {
        entered: Arc<AtomicUsize>,
    }

    struct RecordingAuthority {
        effects: Mutex<Vec<NativeSideEffect>>,
        reject: bool,
        cancel_after_validation: Option<CancellationToken>,
    }

    impl RecordingAuthority {
        fn allowing() -> Self {
            Self {
                effects: Mutex::new(Vec::new()),
                reject: false,
                cancel_after_validation: None,
            }
        }

        fn rejecting() -> Self {
            Self {
                effects: Mutex::new(Vec::new()),
                reject: true,
                cancel_after_validation: None,
            }
        }

        fn cancelling(token: CancellationToken) -> Self {
            Self {
                effects: Mutex::new(Vec::new()),
                reject: false,
                cancel_after_validation: Some(token),
            }
        }

        fn effects(&self) -> Vec<NativeSideEffect> {
            self.effects.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl NativeExecutionAuthority for RecordingAuthority {
        async fn revalidate(&self, effect: NativeSideEffect) -> VyaneResult<()> {
            self.effects.lock().unwrap().push(effect);
            if let Some(token) = &self.cancel_after_validation {
                token.cancel();
            }
            if self.reject {
                return Err(vyane_core::VyaneError::new(
                    vyane_core::ErrorKind::Conflict,
                    "native execution authority is stale or invalid",
                ));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl NativeTool for HangingTool {
        fn name(&self) -> &str {
            "hanging_tool"
        }

        async fn execute(
            &self,
            _arguments: &BTreeMap<String, Value>,
            _context: &ToolContext,
        ) -> Result<String, ToolError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }

    fn registry(calls: Arc<AtomicUsize>, result: Result<String, ToolError>) -> ToolRegistry {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(CountingTool { calls, result }))
            .unwrap();
        registry
    }

    fn call() -> ToolCall {
        ToolCall {
            id: "call-7".into(),
            name: "write_file".into(),
            arguments: BTreeMap::from([
                ("path".into(), Value::String("src/lib.rs".into())),
                ("content".into(), Value::String("replacement".into())),
            ]),
        }
    }

    fn hanging_call() -> ToolCall {
        ToolCall {
            id: "call-hang".into(),
            name: "hanging_tool".into(),
            arguments: BTreeMap::new(),
        }
    }

    fn hanging_registry(entered: Arc<AtomicUsize>) -> ToolRegistry {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(HangingTool { entered }))
            .unwrap();
        registry
    }

    fn context() -> ToolContext {
        ToolContext::new(std::env::current_dir().unwrap()).unwrap()
    }

    fn rule(effect: PermissionEffect) -> Result<PermissionRule, PermissionRuleError> {
        PermissionRule::new("write_*", effect)
    }

    #[tokio::test]
    async fn allow_executes_and_emits_pre_then_post() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("wrote file".into()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });

        let result = registry
            .execute_observed(
                call(),
                &context(),
                &PermissionPolicy::allow_by_default(),
                &reporter,
            )
            .await;

        assert_eq!(result.status, ToolInvocationStatus::Executed);
        assert_eq!(result.output, "wrote file");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = events.lock().unwrap();
        assert!(matches!(events[0], ToolInvocationEvent::PreToolUse { .. }));
        assert!(matches!(
            events[1],
            ToolInvocationEvent::PostToolUse {
                status: ToolInvocationStatus::Executed,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn authorized_allow_revalidates_the_exact_effect_before_execution() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("wrote file".into()));
        let authority = RecordingAuthority::allowing();

        let result = registry
            .execute_authorized(
                call(),
                &context(),
                &PermissionPolicy::allow_by_default(),
                &authority,
                7,
                3,
            )
            .await
            .unwrap();

        assert_eq!(result.status, ToolInvocationStatus::Executed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            authority.effects(),
            vec![NativeSideEffect::ToolOperation {
                turn: 7,
                ordinal: 3
            }]
        );
    }

    #[tokio::test]
    async fn revoked_authority_is_an_outer_error_and_never_reaches_the_tool() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let authority = RecordingAuthority::rejecting();

        let error = registry
            .execute_authorized(
                call(),
                &context(),
                &PermissionPolicy::allow_by_default(),
                &authority,
                1,
                1,
            )
            .await
            .expect_err("revoked authority must stop the native loop");

        assert_eq!(error.kind, vyane_core::ErrorKind::Conflict);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(authority.effects().len(), 1);
        assert!(!error.message.contains("replacement"));
        assert!(!error.message.contains("src/lib.rs"));
    }

    #[tokio::test]
    async fn pure_tool_decisions_do_not_consume_execution_authority() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let authority = RecordingAuthority::allowing();

        let denied = registry
            .execute_authorized(
                call(),
                &context(),
                &PermissionPolicy::default(),
                &authority,
                1,
                1,
            )
            .await
            .unwrap();
        assert_eq!(denied.status, ToolInvocationStatus::Denied);

        let ask =
            PermissionPolicy::allow_by_default().with_rule(rule(PermissionEffect::Ask).unwrap());
        let approval = registry
            .execute_authorized(call(), &context(), &ask, &authority, 1, 2)
            .await
            .unwrap();
        assert_eq!(approval.status, ToolInvocationStatus::ApprovalRequired);

        let invalid = registry
            .execute_authorized(
                ToolCall {
                    id: String::new(),
                    name: "write_file".into(),
                    arguments: BTreeMap::new(),
                },
                &context(),
                &PermissionPolicy::allow_by_default(),
                &authority,
                1,
                3,
            )
            .await
            .unwrap();
        assert_eq!(invalid.status, ToolInvocationStatus::InvalidCall);

        assert!(authority.effects().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancellation_after_revalidation_still_wins_before_tool_polling() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let cancellation = CancellationToken::new();
        let context = context().with_cancellation_token(cancellation.clone());
        let authority = RecordingAuthority::cancelling(cancellation);

        let result = registry
            .execute_authorized(
                call(),
                &context,
                &PermissionPolicy::allow_by_default(),
                &authority,
                2,
                4,
            )
            .await
            .unwrap();

        assert_eq!(result.status, ToolInvocationStatus::Cancelled);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(authority.effects().len(), 1);
    }

    #[tokio::test]
    async fn deny_never_calls_executor_and_still_emits_post() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });
        let policy =
            PermissionPolicy::allow_by_default().with_rule(rule(PermissionEffect::Deny).unwrap());

        let result = registry
            .execute_observed(call(), &context(), &policy, &reporter)
            .await;

        assert_eq!(result.status, ToolInvocationStatus::Denied);
        assert!(result.output.starts_with("ERROR:"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[1],
            ToolInvocationEvent::PostToolUse {
                status: ToolInvocationStatus::Denied,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn default_policy_is_fail_closed_and_never_calls_executor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));

        let result = registry
            .execute(call(), &context(), &PermissionPolicy::default())
            .await;

        assert_eq!(result.status, ToolInvocationStatus::Denied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_calls_are_bounded_and_never_reach_permissions_or_executor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let mut too_many_arguments = BTreeMap::new();
        for index in 0..=ToolCallLimits::ARGUMENT_COUNT {
            too_many_arguments.insert(format!("arg-{index}"), Value::Null);
        }
        let mut deeply_nested = Value::Null;
        for _ in 0..=ToolCallLimits::JSON_DEPTH {
            deeply_nested = Value::Array(vec![deeply_nested]);
        }
        let cases = [
            ToolCall {
                id: String::new(),
                name: "write_file".into(),
                arguments: BTreeMap::new(),
            },
            ToolCall {
                id: "i".repeat(ToolCallLimits::ID_BYTES + 1),
                name: "write_file".into(),
                arguments: BTreeMap::new(),
            },
            ToolCall {
                id: "unsafe id".into(),
                name: "write_file".into(),
                arguments: BTreeMap::new(),
            },
            ToolCall {
                id: "id".into(),
                name: "n".repeat(ToolCallLimits::NAME_BYTES + 1),
                arguments: BTreeMap::new(),
            },
            ToolCall {
                id: "id".into(),
                name: "write\nfile".into(),
                arguments: BTreeMap::new(),
            },
            ToolCall {
                id: "id".into(),
                name: "write_file".into(),
                arguments: too_many_arguments,
            },
            ToolCall {
                id: "id".into(),
                name: "write_file".into(),
                arguments: BTreeMap::from([("nested".into(), deeply_nested)]),
            },
            ToolCall {
                id: "id".into(),
                name: "write_file".into(),
                arguments: BTreeMap::from([(
                    "content".into(),
                    Value::String("x".repeat(ToolCallLimits::SERIALIZED_ARGUMENT_BYTES)),
                )]),
            },
        ];

        for invalid in cases {
            let result = registry
                .execute(invalid, &context(), &PermissionPolicy::allow_by_default())
                .await;
            assert_eq!(result.status, ToolInvocationStatus::InvalidCall);
            assert!(result.output.starts_with("ERROR: invalid tool call"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ask_never_calls_executor_and_emits_bound_plan_between_events() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });
        let policy =
            PermissionPolicy::allow_by_default().with_rule(rule(PermissionEffect::Ask).unwrap());

        let result = registry
            .execute_observed(call(), &context(), &policy, &reporter)
            .await;

        assert_eq!(result.status, ToolInvocationStatus::ApprovalRequired);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let approval = result.approval.as_ref().unwrap();
        assert!(result.output.contains(&approval.canonical_plan_hash[..12]));
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert!(matches!(
            events[1],
            ToolInvocationEvent::PermissionRequest { .. }
        ));
        assert!(matches!(
            events[2],
            ToolInvocationEvent::PostToolUse {
                status: ToolInvocationStatus::ApprovalRequired,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn ordinary_observer_events_do_not_copy_argument_values_or_output() {
        let secret_argument = "argument-secret-that-must-not-enter-events";
        let secret_output = "output-secret-that-must-not-enter-events";
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok(secret_output.into()));
        let rendered = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let rendered = Arc::clone(&rendered);
            move |event| rendered.lock().unwrap().push(format!("{event:?}"))
        });
        let mut call = call();
        call.arguments.insert(
            "sensitive_value".into(),
            Value::String(secret_argument.into()),
        );

        let result = registry
            .execute_observed(
                call,
                &context(),
                &PermissionPolicy::allow_by_default(),
                &reporter,
            )
            .await;
        assert_eq!(result.output, secret_output);
        let events = rendered.lock().unwrap().join("\n");
        assert!(!events.contains(secret_argument));
        assert!(!events.contains(secret_output));
        assert!(events.contains("sensitive_value"));
        assert!(events.contains(&result.output_sha256));
    }

    #[tokio::test]
    async fn tool_output_is_unicode_safe_bounded_and_hashes_the_full_value() {
        let raw = "雪".repeat(MAX_TOOL_OUTPUT_CHARS + 7);
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok(raw.clone()));

        let result = registry
            .execute(call(), &context(), &PermissionPolicy::allow_by_default())
            .await;
        assert_eq!(result.status, ToolInvocationStatus::Executed);
        assert!(
            result
                .output
                .starts_with(&"雪".repeat(MAX_TOOL_OUTPUT_CHARS))
        );
        assert!(result.output.contains("... [truncated,"));
        assert!(result.output.contains(&result.output_sha256));
        assert_eq!(result.output_chars, MAX_TOOL_OUTPUT_CHARS + 7);
        assert!(result.output_truncated);
        assert_eq!(result.output_sha256, sha256_hex(raw.as_bytes()));
        assert!(result.output.is_char_boundary(result.output.len()));
    }

    #[tokio::test]
    async fn cancellation_drops_a_hanging_tool_and_emits_cancelled_post() {
        let entered = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(hanging_registry(Arc::clone(&entered)));
        let cancellation = CancellationToken::new();
        let context = context().with_cancellation_token(cancellation.clone());
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });

        let handle = tokio::spawn(async move {
            registry
                .execute_observed(
                    hanging_call(),
                    &context,
                    &PermissionPolicy::allow_by_default(),
                    &reporter,
                )
                .await
        });
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, ToolInvocationStatus::Cancelled);
        let events = events.lock().unwrap();
        assert!(matches!(
            events.last(),
            Some(ToolInvocationEvent::PostToolUse {
                status: ToolInvocationStatus::Cancelled,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn pre_cancelled_call_never_creates_an_approval_request() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("must not happen".into()));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let context = context().with_cancellation_token(cancellation);
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });
        let policy =
            PermissionPolicy::allow_by_default().with_rule(rule(PermissionEffect::Ask).unwrap());

        let result = registry
            .execute_observed(call(), &context, &policy, &reporter)
            .await;

        assert_eq!(result.status, ToolInvocationStatus::Cancelled);
        assert!(result.approval.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ToolInvocationEvent::PermissionRequest { .. }))
        );
        assert!(matches!(
            events.last(),
            Some(ToolInvocationEvent::PostToolUse {
                status: ToolInvocationStatus::Cancelled,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn timeout_drops_a_hanging_tool_and_emits_timed_out_post() {
        let entered = Arc::new(AtomicUsize::new(0));
        let registry = hanging_registry(Arc::clone(&entered));
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });

        let result = registry
            .execute_observed(
                hanging_call(),
                &context().with_timeout(Duration::from_millis(10)),
                &PermissionPolicy::allow_by_default(),
                &reporter,
            )
            .await;

        assert_eq!(entered.load(Ordering::SeqCst), 1);
        assert_eq!(result.status, ToolInvocationStatus::TimedOut);
        let events = events.lock().unwrap();
        assert!(matches!(
            events.last(),
            Some(ToolInvocationEvent::PostToolUse {
                status: ToolInvocationStatus::TimedOut,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn expired_deadline_wins_before_a_tool_is_polled() {
        let entered = Arc::new(AtomicUsize::new(0));
        let registry = hanging_registry(Arc::clone(&entered));
        let events = Arc::new(Mutex::new(Vec::new()));
        let reporter = ToolEventReporter::new({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event)
        });
        let result = registry
            .execute_observed(
                hanging_call(),
                &context().with_deadline(Instant::now()),
                &PermissionPolicy::new(PermissionEffect::Ask),
                &reporter,
            )
            .await;
        assert_eq!(result.status, ToolInvocationStatus::TimedOut);
        assert!(result.approval.is_none());
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ToolInvocationEvent::PermissionRequest { .. }))
        );
    }

    #[tokio::test]
    async fn tool_and_lookup_failures_become_model_facing_results() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(
            Arc::clone(&calls),
            Err(ToolError::new("path is outside workdir")),
        );
        let failed = registry
            .execute(call(), &context(), &PermissionPolicy::allow_by_default())
            .await;
        assert_eq!(failed.status, ToolInvocationStatus::ToolError);
        assert_eq!(failed.output, "ERROR: path is outside workdir");

        let missing = registry
            .execute(
                ToolCall {
                    id: "missing".into(),
                    name: "does_not_exist".into(),
                    arguments: BTreeMap::new(),
                },
                &context(),
                &PermissionPolicy::new(PermissionEffect::Ask),
            )
            .await;
        assert_eq!(missing.status, ToolInvocationStatus::UnknownTool);
        assert!(missing.approval.is_none());
        assert!(missing.output.contains("write_file"));
    }

    #[tokio::test]
    async fn reporter_panic_does_not_break_execution() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&calls), Ok("ok".into()));
        let reporter = ToolEventReporter::new(|_| panic!("observer failure"));
        let result = registry
            .execute_observed(
                call(),
                &context(),
                &PermissionPolicy::allow_by_default(),
                &reporter,
            )
            .await;
        assert_eq!(result.status, ToolInvocationStatus::Executed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn executor_panic_becomes_a_structured_tool_error() {
        struct PanicTool;

        #[async_trait]
        impl NativeTool for PanicTool {
            fn name(&self) -> &str {
                "panic_tool"
            }

            async fn execute(
                &self,
                _arguments: &BTreeMap<String, Value>,
                _context: &ToolContext,
            ) -> Result<String, ToolError> {
                panic!("must not escape the tool boundary");
            }
        }

        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(PanicTool)).unwrap();
        let result = registry
            .execute(
                ToolCall {
                    id: "panic".into(),
                    name: "panic_tool".into(),
                    arguments: BTreeMap::new(),
                },
                &context(),
                &PermissionPolicy::allow_by_default(),
            )
            .await;
        assert_eq!(result.status, ToolInvocationStatus::ToolError);
        assert_eq!(
            result.output,
            "ERROR: tool `panic_tool` panicked during execution"
        );
    }

    #[test]
    fn context_requires_an_existing_directory_and_canonicalizes_it() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let context = ToolContext::new(nested.join("..").join("nested")).unwrap();
        assert_eq!(context.workdir(), std::fs::canonicalize(nested).unwrap());

        assert!(matches!(
            ToolContext::new(temp.path().join("missing")),
            Err(ToolContextError::Canonicalize { .. })
        ));
    }

    #[test]
    fn duplicate_tool_registration_is_rejected() {
        let calls = Arc::new(AtomicUsize::new(0));
        let tool = || {
            Arc::new(CountingTool {
                calls: Arc::clone(&calls),
                result: Ok("ok".into()),
            }) as Arc<dyn NativeTool>
        };
        let mut registry = ToolRegistry::new();
        registry.register(tool()).unwrap();
        assert_eq!(
            registry.register(tool()),
            Err(ToolRegistryError::Duplicate("write_file".into()))
        );
    }

    #[test]
    fn registry_rejects_unsafe_or_oversized_tool_names() {
        struct NamedTool(String);

        #[async_trait]
        impl NativeTool for NamedTool {
            fn name(&self) -> &str {
                &self.0
            }

            async fn execute(
                &self,
                _arguments: &BTreeMap<String, Value>,
                _context: &ToolContext,
            ) -> Result<String, ToolError> {
                Ok(String::new())
            }
        }

        let mut registry = ToolRegistry::new();
        assert_eq!(
            registry.register(Arc::new(NamedTool("unsafe name".into()))),
            Err(ToolRegistryError::UnsafeName)
        );
        assert_eq!(
            registry.register(Arc::new(NamedTool("unsafe?name".into()))),
            Err(ToolRegistryError::UnsafeName)
        );
        assert_eq!(
            registry.register(Arc::new(NamedTool(
                "n".repeat(ToolCallLimits::NAME_BYTES + 1)
            ))),
            Err(ToolRegistryError::NameTooLarge)
        );
    }
}
