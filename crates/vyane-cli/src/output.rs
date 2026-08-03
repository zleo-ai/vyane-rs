use serde::Serialize;
use vyane_core::{ErrorKind, RunRecord, RunStatus, Sandbox, SessionRecord};
use vyane_harness::native::{NativeTurnStop, ToolInvocationStatus};
use vyane_router::RouteDecision;
use vyane_service::{NativePermissionAxisStatus, PermissionCheck, SessionView};
use vyane_task::TaskRecord;
use vyane_workflow::{WorkflowJournalSummary, WorkflowOutcome, WorkflowRunStatus};

use crate::daemon_workflow::WorkflowTaskView;
use crate::task::store::{StatusFile, TaskListRow, TaskState};

#[derive(Debug, Serialize)]
pub struct RunJson {
    pub record: RunRecord,
    pub output: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BroadcastJson {
    pub target: String,
    pub record: Option<RunRecord>,
    pub output: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct BroadcastRow {
    pub target: String,
    pub record: Option<RunRecord>,
    pub output: Option<String>,
    pub error: Option<String>,
}

pub fn status_name(status: RunStatus) -> &'static str {
    status.as_str()
}

pub fn duration_ms(record: &RunRecord) -> i64 {
    (record.finished_at - record.started_at).num_milliseconds()
}

pub fn target_selector(record: &RunRecord) -> String {
    format!("{}/{}", record.target.provider, record.target.model)
}

pub fn short_run_id(run_id: &str) -> &str {
    run_id.get(..8).unwrap_or(run_id)
}

pub fn first_line(text: Option<&str>) -> String {
    text.and_then(|value| value.lines().find(|line| !line.trim().is_empty()))
        .unwrap_or("")
        .trim()
        .to_string()
}

/// Human one-line history/dispatch run projection.
pub fn format_record_line(record: &RunRecord) -> String {
    let cost = record
        .cost_usd
        .map(|cost| format!(" ${cost:.6}"))
        .unwrap_or_default();
    format!(
        "{} {} {} {} {}ms{}",
        short_run_id(&record.run_id),
        record.started_at.to_rfc3339(),
        target_selector(record),
        status_name(record.status),
        duration_ms(record),
        cost
    )
}

pub fn print_record_line(record: &RunRecord) {
    // Kind-only pure run status on history/record print (WP-438).
    tracing::info!(
        status = record.status.as_str(),
        "{}",
        format_run_status_line(record.status)
    );
    println!("{}", format_record_line(record));
}

/// Human one-line legacy session list projection.
pub fn format_legacy_session_line(record: &SessionRecord) -> String {
    format!(
        "{} {} {} {}",
        record.session_id,
        record.target,
        record.run_count,
        record.updated_at.to_rfc3339()
    )
}

pub fn print_legacy_session_line(record: &SessionRecord) {
    println!("{}", format_legacy_session_line(record));
}

/// Human one-line owner-scoped session view (terminal-safe id/target).
pub fn format_session_view_line(record: &SessionView) -> String {
    let session_id = terminal_safe(&record.session_id);
    let target = terminal_safe(&record.target.to_string());
    format!(
        "{} {} runs={} revision={} native={} native_resume={} updated={}",
        session_id,
        target,
        record.run_count,
        record.session_revision,
        record.native_state.as_str(),
        if record.native_resume_available {
            "available"
        } else {
            "disabled"
        },
        record.updated_at.to_rfc3339(),
    )
}

/// Pure human line for closed [`vyane_service::SessionNativeState`] kinds.
///
/// Live on session list/inspect human path (WP-427).
pub fn format_session_native_state_line(state: vyane_service::SessionNativeState) -> String {
    format!("session native: {}", terminal_safe(state.as_str()))
}

pub fn print_session_view_line(record: &SessionView) {
    // Kind-only pure native state on session list/inspect (WP-427).
    tracing::info!(
        native_session = record.native_state.as_str(),
        "{}",
        format_session_native_state_line(record.native_state)
    );
    println!("{}", format_session_view_line(record));
}

/// Human multi-line `vyane route` projection: resolved profile plus the durable
/// router decision fields operators already see on the non-JSON path.
pub fn format_route_result(profile: &str, decision: &RouteDecision) -> String {
    format!(
        "profile:     {}\nprovider:    {}\nmodel:       {}\ntier:        {}\neffort:      {}\nscore:       {:.3}\ntag:         {}\nintent:      {}\nreason:      {}\n",
        terminal_safe(profile),
        terminal_safe(&decision.provider),
        terminal_safe(&decision.model),
        decision.tier.as_str(),
        decision.effort.as_str(),
        decision.complexity_score,
        terminal_safe(&decision.tag),
        terminal_safe(&decision.intent),
        terminal_safe(&decision.reason),
    )
}

pub fn print_route_result(profile: &str, decision: &RouteDecision) {
    print!("{}", format_route_result(profile, decision));
}

/// Human `vyane check` permissions block from the redacted [`PermissionCheck`]
/// summary (CLI-harness sandbox ceiling + native/canto axis statuses).
pub fn format_permission_check(permissions: &PermissionCheck) -> String {
    format!(
        "permissions:\n  cli-harness: max_sandbox={} ceiling_layers={}\n  native/canto: ceiling_layers={} filesystem_read={} filesystem_write={} command_execution={} command_network={} web_search={} web_fetch={} tool_policy_layers={} tool_policy_rules={}\n",
        sandbox_name(permissions.harness.max_sandbox),
        permissions.harness.ceiling_layers,
        permissions.native.ceiling_layers,
        native_axis_name(permissions.native.filesystem_read),
        native_axis_name(permissions.native.filesystem_write),
        native_axis_name(permissions.native.command_execution),
        native_axis_name(permissions.native.command_network),
        native_axis_name(permissions.native.web_search),
        native_axis_name(permissions.native.web_fetch),
        permissions.native.tool_policy_layers,
        permissions.native.tool_policy_rule_count,
    )
}

pub fn print_permission_check(permissions: &PermissionCheck) {
    // Kind-only pure native axis statuses alongside human permissions block (WP-402).
    for status in [
        permissions.native.filesystem_read,
        permissions.native.filesystem_write,
        permissions.native.command_execution,
        permissions.native.command_network,
        permissions.native.web_search,
        permissions.native.web_fetch,
    ] {
        tracing::info!(
            status = status.as_str(),
            "{}",
            format_native_permission_axis_status_line(status)
        );
    }
    print!("{}", format_permission_check(permissions));
}

/// Human-readable daemon workflow status/submit lines, including the WP-152
/// bounded success projection and durable `failure_code` already present on
/// [`WorkflowTaskView`].
pub fn format_daemon_workflow_view(view: &WorkflowTaskView) -> String {
    let mut out = format!("workflow {} {}\n", view.task.id, view.task.state);
    if let Some(code) = view.task.failure_code {
        out.push_str(&format!("failure {code}\n"));
    }
    if let Some(journal) = view.journal.as_ref() {
        let counts = &journal.steps;
        out.push_str(&format!(
            "journal {} {}: {}/{} ok, {} failed, {} skipped, {} cancelled\n",
            journal.name,
            workflow_status_name(journal.status),
            counts.success,
            counts.success
                + counts.failed
                + counts.skipped
                + counts.cancelled
                + counts.pending
                + counts.running,
            counts.failed,
            counts.skipped,
            counts.cancelled
        ));
    }
    if let Some(output) = view.output.as_ref() {
        out.push_str("output\n");
        out.push_str(output);
        if !output.ends_with('\n') {
            out.push('\n');
        }
    } else if view.output_omitted {
        out.push_str("output omitted\n");
    }
    out
}

pub fn print_daemon_workflow_view(view: &WorkflowTaskView) {
    // Kind-only pure task kinds on daemon workflow status print (WP-434).
    tracing::info!(
        state = view.task.state.as_str(),
        origin = view.task.origin.as_str(),
        kind = view.task.kind.as_str(),
        "{}; {}; {}",
        format_task_state_line(view.task.state),
        format_task_origin_line(view.task.origin),
        format_task_kind_line(view.task.kind)
    );
    if let Some(code) = view.task.failure_code {
        tracing::info!(
            failure_code = code.as_str(),
            "{}",
            format_task_failure_code_line(code)
        );
    }
    // Kind-only pure journal workflow status when present (WP-435).
    if let Some(journal) = view.journal.as_ref() {
        tracing::info!(
            status = journal.status.as_str(),
            "{}",
            format_workflow_run_status_line(journal.status)
        );
    }
    print!("{}", format_daemon_workflow_view(view));
}

/// Human one-line task id + final state (cancel / settle paths).
pub fn format_task_final_state_line(id: &str, state: &str) -> String {
    format!("{} {}", terminal_safe(id), terminal_safe(state))
}

/// Human one-line cancel idempotency: `{id} already {state}` (terminal snapshot
/// already settled; no further control needed).
pub fn format_task_already_state_line(id: &str, state: impl std::fmt::Display) -> String {
    format!("{} already {}", terminal_safe(id), state)
}

/// Human `vyane daemon start` when a live daemon is already owned.
pub fn format_daemon_already_running_line(addr: impl std::fmt::Display) -> String {
    format!("vyane daemon already running at {addr}")
}

/// Human `vyane daemon start` success after readiness.
pub fn format_daemon_started_line(addr: impl std::fmt::Display, pid: i32) -> String {
    format!("vyane daemon started at {addr} (pid {pid})")
}

/// Human `vyane daemon status` success (non-JSON).
pub fn format_daemon_running_line(addr: impl std::fmt::Display, pid: i32) -> String {
    format!("vyane daemon running at {addr} (pid {pid})")
}

/// Human `vyane daemon stop` success.
pub fn format_daemon_stopped_line() -> String {
    "vyane daemon stopped".to_string()
}

/// Human empty `task list` when no detached runs exist.
pub fn format_empty_task_list_line() -> String {
    "no detached runs".to_string()
}

/// Human detached submit success: just the run id (terminal-safe).
pub fn format_run_id_line(run_id: &str) -> String {
    terminal_safe(run_id)
}

/// Redacted session-control error classification shared by JSON and human paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionControlErrorView {
    pub kind_code: &'static str,
    pub message: &'static str,
    /// Process exit status as `u8` (converted at the I/O boundary).
    pub exit_code: u8,
    pub inspect_before_retry: bool,
}

/// Pure mapping from store/service [`ErrorKind`] to operator-visible session
/// control error tokens (no I/O).
#[must_use]
pub fn session_control_error_view(kind: ErrorKind) -> SessionControlErrorView {
    match kind {
        ErrorKind::NotFound => SessionControlErrorView {
            kind_code: kind.as_str(),
            message: "session not found",
            exit_code: 2,
            inspect_before_retry: false,
        },
        ErrorKind::Conflict => SessionControlErrorView {
            kind_code: kind.as_str(),
            message: "session revision changed; inspect the session and retry with its current revision",
            exit_code: 3,
            inspect_before_retry: true,
        },
        ErrorKind::Indeterminate => SessionControlErrorView {
            kind_code: kind.as_str(),
            message: "session reset may have been published; inspect the session before deciding whether to retry",
            exit_code: 4,
            inspect_before_retry: true,
        },
        ErrorKind::Config => SessionControlErrorView {
            // Operator surface token (not ErrorKind::as_str / "config").
            kind_code: "invalid_argument",
            message: "invalid session control request",
            exit_code: 2,
            inspect_before_retry: false,
        },
        ErrorKind::Unsupported => SessionControlErrorView {
            kind_code: kind.as_str(),
            message: "session control is unavailable for this store",
            exit_code: 1,
            inspect_before_retry: false,
        },
        ErrorKind::Io => SessionControlErrorView {
            // Operator surface token (not ErrorKind::as_str / "io").
            kind_code: "storage_error",
            message: "session storage operation failed",
            exit_code: 1,
            inspect_before_retry: false,
        },
        _ => SessionControlErrorView {
            // Operator surface token for all remaining kinds.
            kind_code: "operation_failed",
            message: "session control operation failed",
            exit_code: 1,
            inspect_before_retry: false,
        },
    }
}

/// Human session-control error line (`error: {message}`).
pub fn format_session_control_error_line(view: &SessionControlErrorView) -> String {
    format_error_line(view.message)
}

/// Human workflow engine error prefix line.
///
/// Validation/config failures use `config error:`; other failures use `error:`.
pub fn format_workflow_error_line(is_validation_or_config: bool, message: &str) -> String {
    if is_validation_or_config {
        format_config_error_line(message)
    } else {
        format_error_line(message)
    }
}

/// Human task status/output when the detached run id is missing.
pub fn format_no_such_detached_run_line(id: &str) -> String {
    format!("no such detached run: {}", terminal_safe(id))
}

/// Human task output when no artifact was recorded for the run.
pub fn format_no_output_recorded_line(id: &str) -> String {
    format!("no output recorded for {}", terminal_safe(id))
}

/// Human task status when the id is not a local detached dispatch.
pub fn format_not_local_detached_line(id: &str) -> String {
    format!("{} is not a local detached dispatch", terminal_safe(id))
}

/// Generic human config-error prefix line.
pub fn format_config_error_line(message: &str) -> String {
    format!("config error: {message}")
}

/// Generic human runtime error prefix line.
pub fn format_error_line(message: &str) -> String {
    format!("error: {message}")
}

/// Human detached worker error prefix line.
pub fn format_worker_error_line(message: &str) -> String {
    format!("worker error: {message}")
}

/// Human `vyane serve` rejection when bind address is not loopback.
pub fn format_serve_loopback_only_line() -> String {
    format_config_error_line("vyane serve only accepts loopback listen addresses")
}

/// Human `vyane serve` start banner.
pub fn format_serve_starting_line(addr: impl std::fmt::Display) -> String {
    format!("vyane serve starting on {addr}")
}

/// Human daemon run listen banner.
pub fn format_daemon_listening_line(addr: impl std::fmt::Display) -> String {
    format!("vyane daemon listening on {addr}")
}

/// Human daemon stop/status when no live descriptor is present.
pub fn format_daemon_not_running_line() -> String {
    "vyane daemon is not running".to_string()
}

/// Human daemon stop when the control descriptor was stale and removed.
pub fn format_daemon_not_running_stale_line() -> String {
    "vyane daemon is not running (stale descriptor removed)".to_string()
}

/// Human goal CLI error prefix line.
pub fn format_goal_error_line(message: &str) -> String {
    format!("goal error: {message}")
}

/// Human local A2A CLI error prefix line.
pub fn format_a2a_error_line(message: &str) -> String {
    format!("a2a error: {message}")
}

/// Human A2A inbox note when more rows exist beyond the requested limit.
pub fn format_a2a_more_messages_line() -> String {
    "more messages are available; raise --limit to include them".to_string()
}

/// Human cancel when kill was delivered but the worker never finalized.
pub fn format_kill_delivered_unfinalized_line(id: &str) -> String {
    format!(
        "{}: kill delivered; worker did not finalize",
        terminal_safe(id)
    )
}

/// Human cancel when the id is not a local detached dispatch (frontend-owned).
pub fn format_not_local_detached_cancel_line(id: &str) -> String {
    format!(
        "{} is not a local detached dispatch; cancel it through its owning frontend",
        terminal_safe(id)
    )
}

/// Human cancel when executor ownership changed mid-request.
pub fn format_cancel_ownership_changed_line(id: &str) -> String {
    format!(
        "{}: executor ownership changed while cancellation was requested",
        terminal_safe(id)
    )
}

/// Human cancel interruption diagnostic with current durable state.
pub fn format_cancel_diagnostic_line(
    id: &str,
    diagnostic: &str,
    state: impl std::fmt::Display,
) -> String {
    format!("{}: {}; task is {}", terminal_safe(id), diagnostic, state)
}

/// Human workflow observer: step started.
pub fn format_workflow_step_started_line(step_id: &str) -> String {
    format!("workflow step {}: started", terminal_safe(step_id))
}

/// Human workflow observer: step succeeded with duration.
pub fn format_workflow_step_succeeded_line(step_id: &str, duration_ms: u128) -> String {
    format!(
        "workflow step {}: succeeded in {}ms",
        terminal_safe(step_id),
        duration_ms
    )
}

/// Human workflow observer: step failed with duration and error.
pub fn format_workflow_step_failed_line(step_id: &str, duration_ms: u128, error: &str) -> String {
    format!(
        "workflow step {}: failed in {}ms: {}",
        terminal_safe(step_id),
        duration_ms,
        terminal_safe(error)
    )
}

/// Human workflow observer: step skipped with reason.
pub fn format_workflow_step_skipped_line(step_id: &str, reason: &str) -> String {
    format!(
        "workflow step {}: skipped: {}",
        terminal_safe(step_id),
        terminal_safe(reason)
    )
}

/// Human workflow observer: step cancelled with duration.
pub fn format_workflow_step_cancelled_line(step_id: &str, duration_ms: u128) -> String {
    format!(
        "workflow step {}: cancelled in {}ms",
        terminal_safe(step_id),
        duration_ms
    )
}

/// Human cancel: terminal metadata but process cleanup failed.
pub fn format_task_already_cleanup_failed_line(
    id: &str,
    state: impl std::fmt::Display,
    error: &str,
) -> String {
    format!(
        "{}: task is already {}, but process cleanup failed: {}",
        terminal_safe(id),
        state,
        error
    )
}

/// Human durable cancel: process identity unavailable; refuse control.
pub fn format_process_identity_unavailable_line(
    id: &str,
    phase: &str,
    reason: &str,
    state: impl std::fmt::Display,
) -> String {
    format!(
        "{}: process identity unavailable {} ({}); refusing control; task remains {}",
        terminal_safe(id),
        phase,
        reason,
        state
    )
}

/// Human task status: stale scaffold — worker never published status.
pub fn format_stale_detached_status_line(id: &str, log_path: impl std::fmt::Display) -> String {
    format!(
        "{}: stale — worker never wrote status (spawn or stdin handoff may have failed); see {}",
        terminal_safe(id),
        log_path
    )
}

/// Human worker: durable metadata settlement failed.
pub fn format_worker_metadata_settlement_failed_line(id: &str, error: &str) -> String {
    format!(
        "worker metadata settlement failed for {}: {}",
        terminal_safe(id),
        error
    )
}

/// Human worker: optional output artifact write failed.
pub fn format_output_write_failed_line(path: impl std::fmt::Display, error: &str) -> String {
    format!("write {path}: {error}")
}

/// Human worker: nested harness controller sidecar write failed.
pub fn format_nested_harness_controller_write_failed_line(
    path: impl std::fmt::Display,
    error: &str,
) -> String {
    format!("nested harness controller write failed at {path}: {error}")
}

/// Human worker: nested harness controller sidecar cleanup failed.
pub fn format_nested_harness_controller_cleanup_failed_line(
    path: impl std::fmt::Display,
    error: &str,
) -> String {
    format!("nested harness controller cleanup failed at {path}: {error}")
}

/// Human dispatch stream: target does not support streaming.
pub fn format_stream_unsupported_fallback_line(target: impl std::fmt::Display) -> String {
    format!(
        "notice: {} does not support streaming; falling back to non-streaming",
        terminal_safe(&target.to_string())
    )
}

/// Human dispatch stream: --stream flag not applicable for this request shape.
pub fn format_stream_not_applicable_line() -> String {
    "notice: --stream only applies to a single target with no --session; falling back to non-streaming"
        .to_string()
}

/// Human dispatch stream: tool-use progress line on stderr.
pub fn format_stream_tool_use_line(name: &str, summary: &str) -> String {
    format!(
        "\n[tool] {}: {}",
        terminal_safe(name),
        terminal_safe(summary)
    )
}

/// Human native tool status line using the closed `ToolInvocationStatus` token.
///
/// Pure surface for the native ask/deny/error vocabulary (WP-266/WP-267).
pub fn format_tool_invocation_status_line(status: ToolInvocationStatus) -> String {
    format!("tool status: {}", terminal_safe(status.as_str()))
}

/// Human native turn stop line using the closed `NativeTurnStop` kind token.
///
/// Payloads (assistant text, approval plans) are never included — only
/// `stop.as_str()` (WP-263/WP-268).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_native_turn_stop_line(stop: &NativeTurnStop) -> String {
    format!("native turn stop: {}", terminal_safe(stop.as_str()))
}

/// REST serve operator: task initialization cleanup read failed.
pub fn format_task_init_cleanup_read_failed_line(id: &str, error: &str) -> String {
    format!(
        "task {} initialization cleanup read failed: {}",
        terminal_safe(id),
        error
    )
}

/// REST serve operator: task initialization cleanup failed.
pub fn format_task_init_cleanup_failed_line(id: &str, error: &str) -> String {
    format!(
        "task {} initialization cleanup failed: {}",
        terminal_safe(id),
        error
    )
}

/// REST serve operator: task initialization cleanup remained contended.
pub fn format_task_init_cleanup_contended_line(id: &str) -> String {
    format!(
        "task {} initialization cleanup remained contended",
        terminal_safe(id)
    )
}

/// REST serve operator: duplicate runtime dispatch rejected.
pub fn format_task_duplicate_runtime_dispatch_line(id: &str, epoch: u64) -> String {
    format!(
        "task {} epoch {} rejected duplicate runtime dispatch",
        terminal_safe(id),
        epoch
    )
}

/// REST serve operator: supervised task dispatch failed.
pub fn format_task_dispatch_failed_line(id: &str, error: &str) -> String {
    format!("task {} failed: {}", terminal_safe(id), error)
}

/// REST serve operator: supervised task dispatch future panicked.
pub fn format_task_dispatch_panicked_line(id: &str) -> String {
    format!("task {} dispatch future panicked", terminal_safe(id))
}

/// REST serve operator: task output artifact write failed.
pub fn format_task_output_artifact_failed_line(id: &str, error: &str) -> String {
    format!(
        "task {} output artifact failed: {}",
        terminal_safe(id),
        error
    )
}

/// REST serve operator: metadata settlement retry diagnostic.
pub fn format_task_metadata_settlement_retry_line(id: &str, error: &str) -> String {
    format!(
        "task {} metadata settlement retry: {}",
        terminal_safe(id),
        error
    )
}

/// REST serve operator: task metadata store error (server-side classification).
pub fn format_task_metadata_error_line(error: &str) -> String {
    format!("task metadata error: {error}")
}

/// REST serve operator: dispatch/broadcast service error (server-side).
pub fn format_dispatch_broadcast_error_line(error: &str) -> String {
    format!("dispatch/broadcast error: {error}")
}

/// REST serve operator: goal read service unavailable.
pub fn format_goal_read_unavailable_line() -> String {
    "goal read service unavailable".to_string()
}

/// Pure human line for closed [`vyane_service::GoalReadError`] kind tokens.
pub fn format_goal_read_error_kind_line(error: vyane_service::GoalReadError) -> String {
    format!("goal read: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed continuity-runner construction/run errors.
///
/// No CLI/API host yet; tests lock the token surface (WP-278).
#[allow(dead_code)]
pub fn format_goal_continuity_runner_error_kind_line(
    error: vyane_service::GoalContinuityRunnerError,
) -> String {
    format!("continuity runner: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed continuity-runner authority failures.
///
/// No CLI/API host yet; tests lock the token surface (WP-278).
#[allow(dead_code)]
pub fn format_goal_continuity_authority_error_kind_line(
    error: vyane_service::GoalContinuityRunnerAuthorityError,
) -> String {
    format!("continuity authority: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed goal-observation ingress construction errors.
///
/// No CLI/API host yet; tests lock the token surface (WP-280).
#[allow(dead_code)]
pub fn format_goal_observation_ingress_error_kind_line(
    error: vyane_service::GoalObservationIngressError,
) -> String {
    format!(
        "goal observation ingress: {}",
        terminal_safe(error.as_str())
    )
}

/// Pure human line for closed goal-observation runner construction errors.
///
/// No CLI/API host yet; tests lock the token surface (WP-280).
#[allow(dead_code)]
pub fn format_goal_observation_runner_error_kind_line(
    error: vyane_service::GoalObservationRunnerError,
) -> String {
    format!("goal observation runner: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_service::NativePermissionSetError`] kinds.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_native_permission_set_error_kind_line(
    error: vyane_service::NativePermissionSetError,
) -> String {
    format!("native permission: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed
/// [`vyane_service::AgentMessageCompletionStageError`] kinds.
///
/// Live wire is on Process AgentRun completion staging; CLI pure surface for
/// tests and operator diagnostics (WP-371).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_agent_message_completion_stage_error_kind_line(
    error: vyane_service::AgentMessageCompletionStageError,
) -> String {
    format!("completion stage: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed Process AgentRun [`crate::agent_host::LifecycleObservation`] kinds.
///
/// Live wire is on Process AgentRun dispatch/stop-proof diagnostics; CLI pure
/// surface for tests and operator diagnostics (WP-372).
/// Linux-only: `agent_host` (and Process AgentRun) is not assembled on other OS.
#[cfg(target_os = "linux")]
pub(crate) fn format_lifecycle_observation_line(
    observation: crate::agent_host::LifecycleObservation,
) -> String {
    format!("lifecycle: {}", terminal_safe(observation.as_str()))
}

/// Pure human line for closed [`vyane_harness::native::PermissionEffect`] kinds.
///
/// Live on native AgentRun ApprovalRequired settle (ask surface) (WP-415).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_permission_effect_line(effect: vyane_harness::native::PermissionEffect) -> String {
    format!("permission effect: {}", terminal_safe(effect.as_str()))
}

/// Pure human line for closed [`vyane_core::AuthStyle`] kinds.
///
/// Live on native target freeze when endpoint auth is present (WP-416).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_auth_style_line(style: vyane_core::AuthStyle) -> String {
    format!("auth style: {}", terminal_safe(style.as_str()))
}

/// Pure human line for closed [`vyane_core::WebSearchContextSize`] kinds.
///
/// Live on native web-search tool registration (WP-418).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_web_search_context_size_line(size: vyane_core::WebSearchContextSize) -> String {
    format!("web search context: {}", terminal_safe(size.as_str()))
}

/// Pure human line for closed [`vyane_core::AdapterTransport`] kinds.
///
/// Live on native target freeze (WP-419).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_adapter_transport_line(transport: vyane_core::AdapterTransport) -> String {
    format!("adapter transport: {}", terminal_safe(transport.as_str()))
}

/// Pure human line for closed [`vyane_core::Effort`] kinds.
///
/// Live on native genparams freeze when effort is set (WP-420).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_effort_line(effort: vyane_core::Effort) -> String {
    format!("effort: {}", terminal_safe(effort.as_str()))
}

/// Pure human line for closed [`vyane_harness::native::PermissionRuleError`] kinds.
///
/// Live wire is on native AgentRun permission-policy assembly; CLI pure surface
/// for tests and operator diagnostics (WP-367).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_permission_rule_error_kind_line(
    error: &vyane_harness::native::PermissionRuleError,
) -> String {
    format!("permission rule: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed
/// [`vyane_harness::native::NativeFilesystemPolicyError`] kinds.
///
/// Live wire is on native AgentRun workspace tool registry assembly; CLI pure
/// surface for tests and operator diagnostics (WP-368).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_native_filesystem_policy_error_kind_line(
    error: vyane_harness::native::NativeFilesystemPolicyError,
) -> String {
    format!(
        "native filesystem policy: {}",
        terminal_safe(error.as_str())
    )
}

/// Pure human line for closed [`vyane_service::OwnerContextError`] kinds.
///
/// No multi-principal CLI host yet for all variants; tests lock the token
/// surface and authentication is logged on the service path (WP-287).
#[allow(dead_code)]
pub fn format_owner_context_error_kind_line(error: vyane_service::OwnerContextError) -> String {
    format!("owner context: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_core::ToolChatValidationError`] kinds.
///
/// Live wire is on the native turn driver logs; CLI pure surface for tests
/// and future operator diagnostics (WP-295).
#[allow(dead_code)]
pub fn format_tool_chat_validation_error_kind_line(
    error: &vyane_core::ToolChatValidationError,
) -> String {
    format!("tool chat validation: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_agent::AgentStoreError`] kinds.
///
/// Live wire is on daemon native and Process AgentRun create failures; CLI pure
/// surface for tests and operator diagnostics (WP-299/376/377).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_agent_store_error_kind_line(error: &vyane_agent::AgentStoreError) -> String {
    format!("agent store: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalStoreError`] kinds.
///
/// Live wire is on resident goal daemon store logs (`error.as_str()`); CLI pure
/// surface for tests and operator diagnostics (WP-302).
pub fn format_goal_store_error_kind_line(error: &vyane_goal::GoalStoreError) -> String {
    format!("goal store: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_workflow::WorkflowError`] kinds.
///
/// Live wire is on resident workflow daemon failure logs; CLI pure surface for
/// tests and operator diagnostics (WP-309).
pub fn format_workflow_error_kind_line(error: &vyane_workflow::WorkflowError) -> String {
    format!("workflow: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_task::TaskStoreError`] kinds.
///
/// Live wire is on resident workflow lease/metadata logs; CLI pure surface for
/// tests and operator diagnostics (WP-311).
pub fn format_task_store_error_kind_line(error: &vyane_task::TaskStoreError) -> String {
    format!("task store: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_message::MessageStoreError`] kinds.
///
/// Live wire is on local A2A store open failures; CLI pure surface for tests
/// and operator diagnostics (WP-311/392).
pub fn format_message_store_error_kind_line(error: &vyane_message::MessageStoreError) -> String {
    format!("message store: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_ledger::EventLogError`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-312).
#[allow(dead_code)]
pub fn format_event_log_error_kind_line(error: &vyane_ledger::EventLogError) -> String {
    format!("event log: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_mcp::WorkflowControlError`] kinds.
///
/// Live wire is on MCP submit/control error mapping; CLI pure surface for tests
/// and operator diagnostics (WP-312/381).
pub fn format_workflow_control_error_kind_line(error: vyane_mcp::WorkflowControlError) -> String {
    format!("workflow control: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed command tool registration kinds.
///
/// Live wire is on native AgentRun tool registration; CLI pure surface for
/// tests and operator diagnostics (WP-366).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_register_command_tool_error_kind_line(
    error: &vyane_harness::native::RegisterCommandToolError,
) -> String {
    format!("register run_command: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed web-fetch tool registration kinds.
///
/// Live wire is on native AgentRun tool registration; CLI pure surface for
/// tests and operator diagnostics (WP-313/365).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_register_web_fetch_tool_error_kind_line(
    error: &vyane_harness::native::RegisterWebFetchToolError,
) -> String {
    format!("register web_fetch: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed web-search tool registration kinds.
///
/// Live wire is on native AgentRun tool registration; CLI pure surface for
/// tests and operator diagnostics (WP-313/365).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_register_web_search_tool_error_kind_line(
    error: &vyane_harness::native::RegisterWebSearchToolError,
) -> String {
    format!("register web_search: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed daemon workflow submit error kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-320).
pub(crate) fn format_workflow_submit_error_kind_line(
    error: &crate::daemon_client::WorkflowSubmitError,
) -> String {
    format!("workflow submit: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed daemon workflow control client error kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-320).
pub(crate) fn format_daemon_workflow_control_error_kind_line(
    error: crate::daemon_client::DaemonWorkflowControlError,
) -> String {
    format!("workflow control client: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed workflow step [`vyane_workflow::OnError`] policy.
///
/// Live on review workflow build per-step policy freeze (WP-405).
pub fn format_on_error_policy_line(policy: vyane_workflow::OnError) -> String {
    format!("on_error: {}", terminal_safe(policy.as_str()))
}

/// Pure human line for closed kernel [`vyane_core::ErrorKind`] tokens.
///
/// Live on native AgentRun turn-failure mapping (WP-322/378) and session
/// control failure paths (WP-429).
pub fn format_error_kind_line_token(kind: vyane_core::ErrorKind) -> String {
    format!("error kind: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_agent::RunFailureCode`] kinds.
///
/// Live wire is on AgentRun quiesced failure settlement; CLI pure surface for
/// tests and operator diagnostics (WP-379).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_run_failure_code_line(code: vyane_agent::RunFailureCode) -> String {
    format!("run failure: {}", terminal_safe(code.as_str()))
}

/// Pure human line for closed [`vyane_task::FailureCode`] kinds.
///
/// Live on daemon workflow Failed settle / interrupt (WP-421).
pub fn format_task_failure_code_line(code: vyane_task::FailureCode) -> String {
    format!("task failure: {}", terminal_safe(code.as_str()))
}

/// Pure human line for closed [`vyane_task::TaskOrigin`] kinds.
///
/// Live on daemon workflow task create freeze (WP-425).
pub fn format_task_origin_line(origin: vyane_task::TaskOrigin) -> String {
    format!("task origin: {}", terminal_safe(origin.as_str()))
}

/// Pure human line for closed [`vyane_task::TaskKind`] kinds.
///
/// Live on daemon workflow task create freeze (WP-425).
pub fn format_task_kind_line(kind: vyane_task::TaskKind) -> String {
    format!("task kind: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_agent::ExecutionBackend`] kinds.
///
/// Live on daemon AgentRun submit freeze (process + native) (WP-422).
/// Wire is Linux-only (`daemon_agent` cfg); allow on other OS for clippy.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_execution_backend_line(backend: vyane_agent::ExecutionBackend) -> String {
    format!("execution backend: {}", terminal_safe(backend.as_str()))
}

/// Pure human line for closed [`vyane_agent::RunMode`] kinds.
///
/// Live on daemon AgentRun submit freeze (process + native) (WP-423).
/// Wire is Linux-only (`daemon_agent` cfg); allow on other OS for clippy.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_run_mode_line(mode: vyane_agent::RunMode) -> String {
    format!("run mode: {}", terminal_safe(mode.as_str()))
}

/// Pure human line for closed [`vyane_agent::ControllerKind`] kinds.
///
/// Live on daemon AgentRun cancel dispatch (WP-424).
/// Wire is Linux-only (`daemon_agent` cfg); allow on other OS for clippy.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_controller_kind_line(kind: vyane_agent::ControllerKind) -> String {
    format!("controller kind: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_broker::BrokerError`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-323).
#[allow(dead_code)]
pub fn format_broker_error_kind_line(error: &vyane_broker::BrokerError) -> String {
    format!("broker: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_quota::QuotaValidationError`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-324).
#[allow(dead_code)]
pub fn format_quota_validation_error_kind_line(
    error: &vyane_quota::QuotaValidationError,
) -> String {
    format!("quota validation: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_quota::QuotaRunnerError`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-324).
#[allow(dead_code)]
pub fn format_quota_runner_error_kind_line(error: vyane_quota::QuotaRunnerError) -> String {
    format!("quota runner: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_quota::QuotaTransportError`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-324).
#[allow(dead_code)]
pub fn format_quota_transport_error_kind_line(error: vyane_quota::QuotaTransportError) -> String {
    format!("quota transport: {}", terminal_safe(error.as_str()))
}

/// Pure human line for closed [`vyane_broker::PumpItemStatus`] kinds.
///
/// Live wire is on the resident broker pump degraded-item log (`status.as_str()`);
/// CLI pure surface for tests and operator diagnostics (WP-325).
#[allow(dead_code)]
pub fn format_pump_item_status_line(status: &vyane_broker::PumpItemStatus) -> String {
    format!("pump item: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_core::NativeSideEffect`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-327).
#[allow(dead_code)]
pub fn format_native_side_effect_line(effect: &vyane_core::NativeSideEffect) -> String {
    format!("native effect: {}", terminal_safe(effect.as_str()))
}

/// Pure human line for closed [`vyane_service::InProcessAgentEffect`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-327).
#[allow(dead_code)]
pub fn format_inprocess_agent_effect_line(effect: vyane_service::InProcessAgentEffect) -> String {
    format!("inprocess effect: {}", terminal_safe(effect.as_str()))
}

/// Pure human line for closed [`vyane_service::GoalObservationKind`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-329).
#[allow(dead_code)]
pub fn format_goal_observation_kind_line(kind: &vyane_service::GoalObservationKind) -> String {
    format!("goal observation: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_core::NativeSessionState`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-329).
#[allow(dead_code)]
pub fn format_native_session_state_line(state: &vyane_core::NativeSessionState) -> String {
    format!("native session: {}", terminal_safe(state.as_str()))
}

/// Pure human line for closed [`vyane_message::NackDisposition`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-330).
#[allow(dead_code)]
pub fn format_nack_disposition_line(disposition: &vyane_message::NackDisposition) -> String {
    format!("nack: {}", terminal_safe(disposition.as_str()))
}

/// Pure human line for closed [`vyane_agent::CancelOutcome`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-336).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_cancel_outcome_line(outcome: vyane_agent::CancelOutcome) -> String {
    format!("cancel: {}", terminal_safe(outcome.as_str()))
}

/// Pure human line for closed [`vyane_broker::AdapterOutcome`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-338).
#[allow(dead_code)]
pub fn format_adapter_outcome_line(outcome: &vyane_broker::AdapterOutcome) -> String {
    format!("adapter outcome: {}", terminal_safe(outcome.as_str()))
}

/// Pure human line for closed [`vyane_broker::AdapterFailure`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-338).
#[allow(dead_code)]
pub fn format_adapter_failure_line(failure: &vyane_broker::AdapterFailure) -> String {
    format!("adapter failure: {}", terminal_safe(failure.as_str()))
}

/// Pure human line for closed [`vyane_core::AttemptOutcome`] kinds.
///
/// Live wire is on Process AgentRun quiesced Error settlement; CLI pure surface
/// for tests and operator diagnostics (WP-340/390).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_attempt_outcome_line(outcome: &vyane_core::AttemptOutcome) -> String {
    format!("attempt: {}", terminal_safe(outcome.as_str()))
}

/// Pure human line for closed [`vyane_service::ControllerRecoveryObservation`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-341).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_controller_recovery_observation_line(
    observation: vyane_service::ControllerRecoveryObservation,
) -> String {
    format!(
        "controller recovery: {}",
        terminal_safe(observation.as_str())
    )
}

/// Pure human line for closed [`vyane_service::AgentExecutorOutcome`] kinds.
///
/// Live wire is on Process AgentRun settlement; CLI pure surface for tests and
/// operator diagnostics (WP-343/374).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_agent_executor_outcome_line(outcome: &vyane_service::AgentExecutorOutcome) -> String {
    format!("executor outcome: {}", terminal_safe(outcome.as_str()))
}

/// Pure human line for closed [`vyane_broker::ReplaySafety`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-343).
#[allow(dead_code)]
pub fn format_replay_safety_line(safety: vyane_broker::ReplaySafety) -> String {
    format!("replay safety: {}", terminal_safe(safety.as_str()))
}

/// Pure human line for closed [`vyane_agent::RunSettlement`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-344).
#[allow(dead_code)]
pub fn format_run_settlement_line(settlement: vyane_agent::RunSettlement) -> String {
    format!("run settlement: {}", terminal_safe(settlement.as_str()))
}

/// Pure human line for closed [`vyane_task::TaskSettlement`] kinds.
///
/// Live wire is on daemon workflow worker finish; CLI pure surface for tests
/// and operator diagnostics (WP-344/383).
pub fn format_task_settlement_line(settlement: &vyane_task::TaskSettlement) -> String {
    format!("task settlement: {}", terminal_safe(settlement.as_str()))
}

/// Pure human line for closed [`vyane_service::AgentExecutionSettlement`] kinds.
///
/// Live wire is on Process AgentRun quiesced settlement; CLI pure surface for
/// tests and operator diagnostics (WP-345/374).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_agent_execution_settlement_line(
    settlement: &vyane_service::AgentExecutionSettlement,
) -> String {
    format!(
        "execution settlement: {}",
        terminal_safe(settlement.as_str())
    )
}

/// Pure human line for closed [`vyane_core::NativeSessionTransition`] kinds.
///
/// Live on session reset-native success (WP-428); tokens-only pure surface for
/// tests and operator diagnostics (WP-346).
pub fn format_native_session_transition_line(
    transition: &vyane_core::NativeSessionTransition,
) -> String {
    format!(
        "native session transition: {}",
        terminal_safe(transition.as_str())
    )
}

/// Pure human line for closed [`vyane_service::AgentCompletionSinkObservation`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-347).
#[allow(dead_code)]
pub fn format_agent_completion_sink_observation_line(
    observation: vyane_service::AgentCompletionSinkObservation,
) -> String {
    format!(
        "completion sink observation: {}",
        terminal_safe(observation.as_str())
    )
}

/// Pure human line for closed [`vyane_service::AgentCompletionSinkTransition`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-347).
#[allow(dead_code)]
pub fn format_agent_completion_sink_transition_line(
    transition: vyane_service::AgentCompletionSinkTransition,
) -> String {
    format!(
        "completion sink transition: {}",
        terminal_safe(transition.as_str())
    )
}

/// Pure human line for closed [`vyane_service::RunAttemptOutcomeView`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-349).
#[allow(dead_code)]
pub fn format_run_attempt_outcome_view_line(
    outcome: &vyane_service::RunAttemptOutcomeView,
) -> String {
    format!("run attempt: {}", terminal_safe(outcome.as_str()))
}

/// Pure human line for closed [`vyane_service::AgentRecoveryItemStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-350).
#[allow(dead_code)]
pub fn format_agent_recovery_item_status_line(
    status: vyane_service::AgentRecoveryItemStatus,
) -> String {
    format!("agent recovery item: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::AgentExecutionItemStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-351).
#[allow(dead_code)]
pub fn format_agent_execution_item_status_line(
    status: vyane_service::AgentExecutionItemStatus,
) -> String {
    format!("agent execution item: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::AgentCompletionProjectionStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-352).
#[allow(dead_code)]
pub fn format_agent_completion_projection_status_line(
    status: vyane_service::AgentCompletionProjectionStatus,
) -> String {
    format!(
        "agent completion projection: {}",
        terminal_safe(status.as_str())
    )
}

/// Pure human line for closed [`vyane_agent::RunState`] kinds.
///
/// Live wire is on daemon AgentRun cancel observation of terminal/cancelling
/// state; CLI pure surface for tests and operator diagnostics (WP-353/385).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_run_state_line(state: vyane_agent::RunState) -> String {
    format!("run state: {}", terminal_safe(state.as_str()))
}

/// Pure human line for closed [`vyane_task::TaskState`] kinds.
///
/// Live wire is on daemon workflow cancel observation of terminal/cancelling
/// state; CLI pure surface for tests and operator diagnostics (WP-353/386).
pub fn format_task_state_line(state: vyane_task::TaskState) -> String {
    format!("task state: {}", terminal_safe(state.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-353).
pub fn format_goal_status_line(status: vyane_goal::GoalStatus) -> String {
    format!("goal status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalEventKind`] kinds.
///
/// Live on CLI goal get event rows and progress settlement (WP-407).
pub fn format_goal_event_kind_line(kind: vyane_goal::GoalEventKind) -> String {
    format!("goal event: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_workflow::WorkflowRunStatus`] kinds.
///
/// Live wire is on daemon workflow worker finish; CLI pure surface for tests
/// and operator diagnostics (WP-353/383).
pub fn format_workflow_run_status_line(status: vyane_workflow::WorkflowRunStatus) -> String {
    format!("workflow run status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_message::DeliveryStatus`] kinds.
///
/// Live wire is on local A2A message_view projection; CLI pure surface for
/// tests and operator diagnostics (WP-354/391).
pub fn format_delivery_status_line(status: vyane_message::DeliveryStatus) -> String {
    format!("delivery status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_message::MessagePublicationStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-354).
#[allow(dead_code)]
pub fn format_message_publication_status_line(
    status: vyane_message::MessagePublicationStatus,
) -> String {
    format!("message publication: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_agent::RunCompletionStatus`] kinds.
///
/// Live wire is on daemon AgentRun terminal view assembly; CLI pure surface for
/// tests and operator diagnostics (WP-354/388).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_run_completion_status_line(status: vyane_agent::RunCompletionStatus) -> String {
    format!("run completion: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::PursuitStatus`] kinds.
///
/// Live on daemon goal settle (WP-362) and CLI pursue settle (WP-399).
pub fn format_pursuit_status_line(status: vyane_goal::PursuitStatus) -> String {
    format!("pursuit status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::CriterionStatus`] kinds.
///
/// Live wire is on goal satisfy/verify paths; CLI pure surface for tests and
/// operator diagnostics (WP-355/394).
pub fn format_criterion_status_line(status: vyane_goal::CriterionStatus) -> String {
    format!("criterion status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalContinuityStatus`] kinds.
///
/// Live on CLI continuity-next when durable state is present (WP-401).
pub fn format_goal_continuity_status_line(status: vyane_goal::GoalContinuityStatus) -> String {
    format!("goal continuity status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::TakeoverApprovalStatus`] kinds.
///
/// Live wire is on goal continuity decide; CLI pure surface for tests and
/// operator diagnostics (WP-355/393).
pub fn format_takeover_approval_status_line(status: vyane_goal::TakeoverApprovalStatus) -> String {
    format!("takeover approval: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_workflow::JournalStepStatus`] kinds.
///
/// Live wire is on daemon workflow worker finish last-step observation; CLI
/// pure surface for tests and operator diagnostics (WP-356/389).
pub fn format_journal_step_status_line(status: vyane_workflow::JournalStepStatus) -> String {
    format!("journal step: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_mcp::WorkflowState`] kinds.
///
/// Live wire is on MCP workflow cancel lifecycle observation; CLI pure surface
/// for tests and operator diagnostics (WP-356/387).
pub fn format_workflow_state_line(state: vyane_mcp::WorkflowState) -> String {
    format!("workflow state: {}", terminal_safe(state.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalContinuityStepStatus`] kinds.
///
/// Live on CLI continuity-next for next-ready / projected plan steps (WP-403).
pub fn format_goal_continuity_step_status_line(
    status: vyane_goal::GoalContinuityStepStatus,
) -> String {
    format!("goal continuity step: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_message::MessageEventKind`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-357).
#[allow(dead_code)]
pub fn format_message_event_kind_line(kind: vyane_message::MessageEventKind) -> String {
    format!("message event: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_core::RunStatus`] kinds.
///
/// Live wire is on Process AgentRun non-quiesced terminal status diagnostics;
/// CLI pure surface for tests and operator diagnostics (WP-357/373).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn format_run_status_line(status: vyane_core::RunStatus) -> String {
    format!("run status: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalContinuityMode`] kinds.
///
/// Live on CLI continuity-next when a continuity policy is present (WP-401).
pub fn format_goal_continuity_mode_line(mode: vyane_goal::GoalContinuityMode) -> String {
    format!("goal continuity mode: {}", terminal_safe(mode.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalContinuityNextActionKind`] kinds.
///
/// Live on CLI continuity-next projection (WP-406).
pub fn format_goal_continuity_next_action_kind_line(
    action: vyane_goal::GoalContinuityNextActionKind,
) -> String {
    format!(
        "goal continuity next action: {}",
        terminal_safe(action.as_str())
    )
}

/// Pure human line for closed [`vyane_goal::GoalContinuitySignalKind`] kinds.
///
/// Live on CLI continuity-signal settle (WP-408).
pub fn format_goal_continuity_signal_kind_line(
    kind: vyane_goal::GoalContinuitySignalKind,
) -> String {
    format!("goal continuity signal: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_goal::GoalContinuityOperatorCommand`] kinds.
///
/// Live on CLI continuity-next when a command is projected (WP-409).
pub fn format_goal_continuity_operator_command_line(
    command: vyane_goal::GoalContinuityOperatorCommand,
) -> String {
    format!(
        "goal continuity command: {}",
        terminal_safe(command.as_str())
    )
}

/// Pure human line for closed [`vyane_goal::TakeoverSandbox`] kinds.
///
/// Live on CLI continuity-queue after approval freeze (WP-410).
pub fn format_takeover_sandbox_line(sandbox: vyane_goal::TakeoverSandbox) -> String {
    format!("takeover sandbox: {}", terminal_safe(sandbox.as_str()))
}

/// Pure human line for closed [`vyane_core::Sandbox`] kinds.
///
/// Live on CLI goal pursue runtime freeze (WP-411) and process AgentRun submit
/// freeze (WP-426).
pub fn format_sandbox_line(sandbox: vyane_core::Sandbox) -> String {
    format!("sandbox: {}", terminal_safe(sandbox.as_str()))
}

/// Pure human line for closed [`vyane_goal::TakeoverDecision`] kinds.
///
/// Live wire is on goal continuity decide; CLI pure surface for tests and
/// operator diagnostics (WP-358/393).
pub fn format_takeover_decision_line(decision: vyane_goal::TakeoverDecision) -> String {
    format!("takeover decision: {}", terminal_safe(decision.as_str()))
}

/// Pure human line for closed [`vyane_goal::TakeoverRunStatus`] kinds.
///
/// Live wire is on goal continuity execute finish; CLI pure surface for tests
/// and operator diagnostics (WP-358/395).
pub fn format_takeover_run_status_line(status: vyane_goal::TakeoverRunStatus) -> String {
    format!("takeover run: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::PursuitCheckpointStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-358).
/// Live on goal CLI get when a durable checkpoint is present (WP-398).
pub fn format_pursuit_checkpoint_status_line(
    status: vyane_goal::PursuitCheckpointStatus,
) -> String {
    format!("pursuit checkpoint: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_goal::PursuitSegmentStatus`] kinds.
///
/// Live on shared DispatchGoalRuntime segment settle (WP-404).
pub fn format_pursuit_segment_status_line(status: vyane_goal::PursuitSegmentStatus) -> String {
    format!("pursuit segment: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::GoalObservationSignalKind`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-359).
#[allow(dead_code)]
pub fn format_goal_observation_signal_kind_line(
    kind: vyane_service::GoalObservationSignalKind,
) -> String {
    format!("goal observation signal: {}", terminal_safe(kind.as_str()))
}

/// Pure human line for closed [`vyane_service::GoalObservationStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-359).
#[allow(dead_code)]
pub fn format_goal_observation_status_line(status: vyane_service::GoalObservationStatus) -> String {
    format!(
        "goal observation status: {}",
        terminal_safe(status.as_str())
    )
}

/// Pure human line for closed [`vyane_service::GoalObservationWatchStatus`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-359).
#[allow(dead_code)]
pub fn format_goal_observation_watch_status_line(
    status: vyane_service::GoalObservationWatchStatus,
) -> String {
    format!("goal observation watch: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::GoalObservationWatcherErrorCode`] kinds.
///
/// Tokens-only pure surface for tests and operator diagnostics (WP-359).
#[allow(dead_code)]
pub fn format_goal_observation_watcher_error_code_line(
    code: vyane_service::GoalObservationWatcherErrorCode,
) -> String {
    format!(
        "goal observation watcher error: {}",
        terminal_safe(code.as_str())
    )
}

/// Pure human line for closed [`vyane_service::ConfigCheckStatus`] kinds.
///
/// Live on `vyane check` when static `check_config` succeeds (WP-402).
pub fn format_config_check_status_line(status: vyane_service::ConfigCheckStatus) -> String {
    format!("config check: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::CredentialStatus`] kinds.
///
/// Live on `vyane check` per-provider credential rows (WP-402).
pub fn format_credential_status_line(status: vyane_service::CredentialStatus) -> String {
    format!("credential: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::ProfileCheckStatus`] kinds.
///
/// Live on `vyane check` per-profile rows (WP-402).
pub fn format_profile_check_status_line(status: vyane_service::ProfileCheckStatus) -> String {
    format!("profile check: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::NativePermissionAxisStatus`] kinds.
///
/// Live on `vyane check` permission summary axes (WP-402).
pub fn format_native_permission_axis_status_line(
    status: vyane_service::NativePermissionAxisStatus,
) -> String {
    format!("native permission axis: {}", terminal_safe(status.as_str()))
}

/// Pure human line for closed [`vyane_service::ConfigIssueCode`] kinds.
///
/// Live on `vyane check` static issue codes (WP-402).
pub fn format_config_issue_code_line(code: vyane_service::ConfigIssueCode) -> String {
    format!("config issue: {}", terminal_safe(code.as_str()))
}

/// REST serve operator: goal read worker failed.
pub fn format_goal_read_worker_failed_line(error: &str) -> String {
    format!("goal read worker failed: {error}")
}

/// REST serve operator: task output read failed.
pub fn format_task_output_read_failed_line(id: &str, error: &str) -> String {
    format!("task {} output read failed: {}", terminal_safe(id), error)
}

/// REST serve operator: legacy task output read failed.
pub fn format_task_legacy_output_read_failed_line(id: &str, error: &str) -> String {
    format!(
        "task {} legacy output read failed: {}",
        terminal_safe(id),
        error
    )
}

/// REST serve operator: listening banner with bearer token path.
pub fn format_serve_listening_line(
    addr: impl std::fmt::Display,
    token_path: impl std::fmt::Display,
) -> String {
    format!("vyane serve listening on {addr}; bearer token file: {token_path}")
}

/// REST serve operator: dispatch request label parse error.
pub fn format_dispatch_label_error_line(error: &str) -> String {
    format!("dispatch label error: {error}")
}

/// REST serve operator: stream dispatch label parse error.
pub fn format_stream_dispatch_label_error_line(error: &str) -> String {
    format!("stream dispatch label error: {error}")
}

/// REST serve operator: stream dispatch request construction error.
pub fn format_stream_dispatch_request_error_line(error: &str) -> String {
    format!("stream dispatch request error: {error}")
}

/// REST serve operator: stream route error.
pub fn format_stream_route_error_line(error: &str) -> String {
    format!("stream route error: {error}")
}

/// REST serve operator: dispatch_stream runtime error.
pub fn format_dispatch_stream_error_line(error: &str) -> String {
    format!("dispatch_stream error: {error}")
}

/// REST serve operator: external/async task label parse error.
pub fn format_external_task_label_error_line(error: &str) -> String {
    format!("external task label error: {error}")
}

/// REST serve operator: broadcast label parse error.
pub fn format_broadcast_label_error_line(error: &str) -> String {
    format!("broadcast label error: {error}")
}

/// REST serve operator: broadcast setup error.
pub fn format_broadcast_setup_error_line(error: &str) -> String {
    format!("broadcast setup error: {error}")
}

/// REST serve operator: per-target broadcast error.
pub fn format_broadcast_target_error_line(target: &str, error: &str) -> String {
    format!(
        "broadcast target `{}` error: {}",
        terminal_safe(target),
        error
    )
}

/// REST serve operator: run ledger query error.
pub fn format_run_ledger_query_error_line(error: &str) -> String {
    format!("run ledger query error: {error}")
}

/// REST serve operator: session snapshot query error.
pub fn format_session_snapshot_query_error_line(error: &str) -> String {
    format!("session snapshot query error: {error}")
}

/// REST serve operator: async dispatch label parse error.
pub fn format_async_dispatch_label_error_line(error: &str) -> String {
    format!("async dispatch label error: {error}")
}

/// Human cancel: worker dead + nested harness cleanup failed.
pub fn format_worker_gone_nested_cleanup_failed_line(id: &str, error: &str) -> String {
    format!(
        "{}: worker is gone and nested harness cleanup failed: {}",
        terminal_safe(id),
        error
    )
}

/// Human cancel: worker dead, nested cleanup finished.
pub fn format_worker_gone_nested_cleanup_complete_line(id: &str) -> String {
    format!(
        "{}: worker process is gone (died); nested harness cleanup complete",
        terminal_safe(id)
    )
}

/// Human cancel: identity mismatch + nested cleanup failed.
pub fn format_identity_mismatch_nested_cleanup_failed_line(
    id: &str,
    reason: &str,
    error: &str,
) -> String {
    format!(
        "{}: outer identity mismatch ({}) and nested harness cleanup failed: {}",
        terminal_safe(id),
        reason,
        error
    )
}

/// Human cancel: identity mismatch refuse signal.
pub fn format_identity_mismatch_refuse_signal_line(id: &str, reason: &str) -> String {
    format!(
        "{}: process identity mismatch ({}; pid likely reused); refusing to signal",
        terminal_safe(id),
        reason
    )
}

/// Human cancel: nested harness identity unavailable before SIGKILL.
pub fn format_nested_harness_identity_unavailable_line(id: &str, error: &str) -> String {
    format!(
        "{}: nested harness identity unavailable before SIGKILL: {}",
        terminal_safe(id),
        error
    )
}

/// Human cancel: refuse unsafe SIGKILL when leader exited but group remains.
pub fn format_worker_leader_exited_group_remains_line(id: &str) -> String {
    format!(
        "{}: worker leader exited but its group remains; refusing unsafe SIGKILL escalation",
        terminal_safe(id)
    )
}

/// Human cancel: identity changed before SIGKILL.
pub fn format_identity_changed_before_sigkill_line(id: &str, reason: &str) -> String {
    format!(
        "{}: process identity changed before SIGKILL ({}); refusing escalation",
        terminal_safe(id),
        reason
    )
}

/// Human cancel: not every owned process group finished.
pub fn format_cancel_incomplete_process_groups_line(id: &str) -> String {
    format!(
        "{}: cancellation did not finish every owned process group",
        terminal_safe(id)
    )
}

/// Human `vyane check` section header: providers.
pub fn format_check_providers_header() -> String {
    "providers:".to_string()
}

/// Human `vyane check` section header: profiles.
pub fn format_check_profiles_header() -> String {
    "profiles:".to_string()
}

/// Human `vyane check` section header: harnesses.
pub fn format_check_harnesses_header() -> String {
    "harnesses:".to_string()
}

/// Human `vyane check` section header: profile environment.
pub fn format_check_profile_environment_header() -> String {
    "profile environment:".to_string()
}

/// Human `vyane check` profile body when failover resolution fails.
pub fn format_check_profile_warning_body(message: &str) -> String {
    format!("warning: {message}")
}

/// Human dispatch run failure when there is no output text (no `error:` prefix).
pub fn format_run_failure_line(message: &str) -> String {
    message.to_string()
}

/// Human `vyane check` config-files section: path display + loaded|missing.
pub fn format_check_config_files(entries: &[(String, bool)]) -> String {
    let mut out = String::from("config files:\n");
    for (path, exists) in entries {
        let state = if *exists { "loaded" } else { "missing" };
        out.push_str(&format!("  {} ({state})\n", terminal_safe(path)));
    }
    out
}

/// Pure human line for closed [`vyane_core::Protocol`] kinds.
///
/// Live on `vyane check` per-provider rows (WP-413).
pub fn format_protocol_line(protocol: vyane_core::Protocol) -> String {
    format!("protocol: {}", terminal_safe(protocol.as_str()))
}

/// Human `vyane check` one provider row.
/// Protocol column uses domain `as_str` kind tokens (WP-413).
pub fn format_check_provider_line(
    id: &str,
    protocol: vyane_core::Protocol,
    default_model: Option<&str>,
) -> String {
    format!(
        "  {}: {} default_model={}\n",
        terminal_safe(id),
        protocol.as_str(),
        default_model.unwrap_or("-")
    )
}

/// Human `vyane check` one profile row (resolved chain or warning).
pub fn format_check_profile_line(name: &str, body: &str) -> String {
    format!("  {}: {}\n", terminal_safe(name), terminal_safe(body))
}

/// Pure human line for closed [`vyane_core::HarnessKind`] closed variants.
///
/// Live on `vyane check` harness rows (WP-414). `Other(_)` still uses as_str.
pub fn format_harness_kind_line(kind: &vyane_core::HarnessKind) -> String {
    format!("harness: {}", terminal_safe(kind.as_str()))
}

/// Human `vyane check` one harness availability row.
/// Kind column uses domain `as_str` tokens (WP-414).
pub fn format_check_harness_line(kind: &vyane_core::HarnessKind, available: bool) -> String {
    format!(
        "  {}: {}\n",
        terminal_safe(kind.as_str()),
        if available { "available" } else { "missing" }
    )
}

/// Human `vyane check` one profile environment variable row.
pub fn format_check_profile_env_line(profile: &str, var: &str, present: bool) -> String {
    format!(
        "  {}: {} {}\n",
        terminal_safe(profile),
        terminal_safe(var),
        if present { "present" } else { "missing" }
    )
}

/// Human one-line daemon workflow cancel: workflow id + task state.
pub fn format_workflow_cancel_line(id: &str, state: impl std::fmt::Display) -> String {
    format!("workflow {} {}", terminal_safe(id), state)
}

const fn sandbox_name(sandbox: Sandbox) -> &'static str {
    sandbox.as_str()
}

const fn native_axis_name(status: NativePermissionAxisStatus) -> &'static str {
    status.as_str()
}

fn terminal_safe(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

/// Human `broadcast` result table (success rows and error rows).
pub fn format_broadcast_table(rows: &[BroadcastRow]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<24} {:<10} {:>10} output",
        "target", "status", "duration"
    );
    for row in rows {
        match &row.record {
            Some(record) => {
                let _ = writeln!(
                    out,
                    "{:<24} {:<10} {:>8}ms {}",
                    row.target,
                    status_name(record.status),
                    duration_ms(record),
                    first_line(row.output.as_deref())
                );
            }
            None => {
                let _ = writeln!(
                    out,
                    "{:<24} {:<10} {:>10} {}",
                    row.target,
                    "error",
                    "-",
                    row.error.as_deref().unwrap_or("")
                );
            }
        }
    }
    out
}

pub fn print_broadcast_table(rows: &[BroadcastRow]) {
    // Kind-only pure run status for successful broadcast rows (WP-439).
    for row in rows {
        if let Some(record) = row.record.as_ref() {
            tracing::info!(
                status = record.status.as_str(),
                "{}",
                format_run_status_line(record.status)
            );
        }
    }
    print!("{}", format_broadcast_table(rows));
}

pub fn workflow_status_name(status: WorkflowRunStatus) -> &'static str {
    status.as_str()
}

/// Human multi-line local `workflow run/resume` summary (status, path, steps).
pub fn format_workflow_summary(outcome: &WorkflowOutcome) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "workflow {} {}",
        outcome.wf_run_id,
        workflow_status_name(outcome.status)
    );
    let _ = writeln!(out, "{}", outcome.journal_path.display());
    if let Some(replay) = outcome.journal.replay.as_ref() {
        let _ = writeln!(
            out,
            "replay source={} reused_steps={}",
            replay.source_wf_run_id,
            replay.reused_step_ids.len()
        );
    }
    let _ = writeln!(out, "{:<24} {:<10} runs output", "step", "status");
    for (id, step) in &outcome.journal.steps {
        let output = step
            .output
            .as_deref()
            .or_else(|| {
                step.outputs.as_ref().and_then(|outputs| {
                    outputs
                        .iter()
                        .find(|output| output.ok)
                        .and_then(|output| output.output.as_deref())
                })
            })
            .map(Some)
            .unwrap_or_else(|| step.error.as_deref());
        let _ = writeln!(
            out,
            "{:<24} {:<10} {:>4} {}",
            id,
            step.status.as_str(),
            step.run_ids.len(),
            first_line(output)
        );
    }
    out
}

pub fn print_workflow_summary(outcome: &WorkflowOutcome) {
    // Kind-only pure workflow run status on local summary print (WP-437).
    tracing::info!(
        status = outcome.status.as_str(),
        "{}",
        format_workflow_run_status_line(outcome.status)
    );
    print!("{}", format_workflow_summary(outcome));
}

/// Human `workflow list` table over journal summaries (status + step counts).
pub fn format_workflow_list(rows: &[WorkflowJournalSummary]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<36} {:<24} {:<24} {:<10} steps",
        "id", "started_at", "name", "status"
    );
    for row in rows {
        let counts = &row.steps;
        let total = counts.pending
            + counts.running
            + counts.success
            + counts.failed
            + counts.skipped
            + counts.cancelled;
        let _ = writeln!(
            out,
            "{:<36} {:<24} {:<24} {:<10} {}/{} ok, {} failed, {} skipped, {} cancelled",
            row.id,
            row.started_at.to_rfc3339(),
            row.name,
            workflow_status_name(row.status),
            counts.success,
            total,
            counts.failed,
            counts.skipped,
            counts.cancelled
        );
    }
    out
}

pub fn print_workflow_list(rows: &[WorkflowJournalSummary]) {
    // Kind-only pure workflow run status per list row (WP-436).
    for row in rows {
        tracing::info!(
            status = row.status.as_str(),
            "{}",
            format_workflow_run_status_line(row.status)
        );
    }
    print!("{}", format_workflow_list(rows));
}

/// JSON view of a single detached run's status, plus the derived display state
/// and the recent log tail (matches the human `task status` output).
#[derive(Debug, Serialize)]
pub struct TaskStatusJson<'a> {
    #[serde(flatten)]
    pub status: &'a StatusFile,
    /// The state as displayed: same as `status.state`, except a dead `running`
    /// run reads as `died` (read-side orphan interpretation).
    pub displayed_state: &'a str,
    pub log_tail: &'a [String],
}

/// Stable list projection shared by durable tasks and read-only legacy
/// `status.json` compatibility rows.
#[derive(Debug, Clone, Serialize)]
pub struct TaskRow {
    pub id: String,
    pub state: String,
    pub target: String,
    pub origin: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
}

impl TaskRow {
    pub fn from_record(record: &TaskRecord) -> Self {
        let duration_ms = record
            .started_at
            .zip(record.finished_at)
            .map(|(start, finish)| (finish - start).num_milliseconds());
        Self {
            id: record.id.clone(),
            state: record.state.to_string(),
            target: record.target_key.clone(),
            origin: record.origin.to_string(),
            created_at: record.created_at,
            started_at: record.started_at,
            updated_at: record.updated_at,
            finished_at: record.finished_at,
            duration_ms,
            ledger_run_id: record.ledger_run_id.clone(),
            failure_code: record.failure_code.map(|code| code.to_string()),
        }
    }

    pub fn from_legacy(row: &TaskListRow) -> Self {
        Self {
            id: row.id.clone(),
            state: row.state.as_str().to_string(),
            target: row.target.clone(),
            origin: "legacy_cli_detached".into(),
            created_at: row.started_at,
            started_at: Some(row.started_at),
            updated_at: row.started_at,
            finished_at: row
                .duration_ms
                .map(|milliseconds| row.started_at + chrono::Duration::milliseconds(milliseconds)),
            duration_ms: row.duration_ms,
            ledger_run_id: None,
            failure_code: matches!(row.state, TaskState::Died | TaskState::Stale)
                .then(|| "worker_lost".into()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DurableTaskStatusJson<'a> {
    #[serde(flatten)]
    pub task: &'a TaskRecord,
    pub log_tail: &'a [String],
}

fn task_duration(row_ms: Option<i64>) -> String {
    match row_ms {
        Some(ms) => format!("{ms}ms"),
        None => "-".to_string(),
    }
}

/// Human `task list` table. Surfaces each row's `failure_code` as a trailing
/// `failure` column only when at least one row carries a code — no invented
/// failure column/cells for no-code tables.
pub fn format_task_table(rows: &[TaskRow]) -> String {
    use std::fmt::Write as _;

    let show_failure = rows.iter().any(|row| row.failure_code.is_some());
    let mut out = String::new();
    if show_failure {
        let _ = writeln!(
            out,
            "{:<36} {:<12} {:>12} {:<20} {:<24} failure",
            "id", "state", "duration", "created", "target"
        );
    } else {
        let _ = writeln!(
            out,
            "{:<36} {:<12} {:>12} {:<20} target",
            "id", "state", "duration", "created"
        );
    }
    for row in rows {
        let duration = task_duration(row.duration_ms);
        let created = row.created_at.to_rfc3339();
        if show_failure {
            // Empty string for no-code rows keeps column alignment without
            // inventing a synthetic failure token.
            let failure = row.failure_code.as_deref().unwrap_or("");
            let _ = writeln!(
                out,
                "{:<36} {:<12} {:>12} {:<20} {:<24} {failure}",
                row.id, row.state, duration, created, row.target
            );
        } else {
            let _ = writeln!(
                out,
                "{:<36} {:<12} {:>12} {:<20} {}",
                row.id, row.state, duration, created, row.target
            );
        }
    }
    out
}

pub fn print_task_table(rows: &[TaskRow]) {
    print!("{}", format_task_table(rows));
}

/// Human durable `task status` body. Surfaces durable `failure_code` as
/// `failure: <snake_case>` only when set — no invented failure line when absent.
pub fn format_durable_task_status(task: &TaskRecord, log_tail: &[String]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "id:         {}", task.id);
    let _ = writeln!(out, "state:      {}", task.state);
    let _ = writeln!(out, "origin:     {}", task.origin);
    let _ = writeln!(out, "target:     {}", task.target_key);
    let _ = writeln!(out, "digest:     {}", task.task_digest);
    let _ = writeln!(out, "revision:   {}", task.revision);
    let _ = writeln!(out, "epoch:      {}", task.executor_epoch);
    let _ = writeln!(out, "created_at: {}", task.created_at.to_rfc3339());
    if let Some(started) = task.started_at {
        let _ = writeln!(out, "started_at: {}", started.to_rfc3339());
    }
    if let Some(finished) = task.finished_at {
        let _ = writeln!(out, "finished:   {}", finished.to_rfc3339());
    }
    if let Some(ledger) = &task.ledger_run_id {
        let _ = writeln!(out, "ledger:     {ledger}");
    }
    if let Some(code) = task.failure_code {
        let _ = writeln!(out, "failure:    {code}");
    }
    if log_tail.is_empty() {
        let _ = writeln!(out, "log:        (empty)");
    } else {
        let _ = writeln!(out, "log (last {} lines):", log_tail.len());
        for line in log_tail {
            let _ = writeln!(out, "  {line}");
        }
    }
    out
}

pub fn print_durable_task_status(task: &TaskRecord, log_tail: &[String]) {
    // Kind-only pure task state/origin/kind on durable status print (WP-431/432).
    tracing::info!(
        state = task.state.as_str(),
        origin = task.origin.as_str(),
        kind = task.kind.as_str(),
        "{}; {}; {}",
        format_task_state_line(task.state),
        format_task_origin_line(task.origin),
        format_task_kind_line(task.kind)
    );
    // Kind-only pure failure code only when durable failure is present (WP-433).
    if let Some(code) = task.failure_code {
        tracing::info!(
            failure_code = code.as_str(),
            "{}",
            format_task_failure_code_line(code)
        );
    }
    print!("{}", format_durable_task_status(task, log_tail));
}

/// Human legacy detached `task status` body (status.json + displayed state).
pub fn format_task_status(
    status: &StatusFile,
    displayed: TaskState,
    log_tail: &[String],
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "id:         {}", status.run_id);
    let _ = writeln!(out, "state:      {}", displayed.as_str());
    let _ = writeln!(out, "pid:        {}", status.pid);
    let _ = writeln!(out, "pgid:       {}", status.pgid);
    let _ = writeln!(out, "target:     {}", status.target);
    if let Some(workdir) = &status.workdir {
        let _ = writeln!(out, "workdir:    {workdir}");
    }
    let _ = writeln!(out, "started_at: {}", status.started_at.to_rfc3339());
    if let Some(finished) = status.finished_at {
        let _ = writeln!(out, "finished:   {}", finished.to_rfc3339());
    }
    if let Some(ms) = status.duration_ms() {
        let _ = writeln!(out, "duration:   {ms}ms");
    }
    if let Some(ledger) = &status.ledger_run_id {
        let _ = writeln!(out, "ledger:     {ledger}");
    }
    if let Some(error) = &status.error {
        let _ = writeln!(out, "error:      {error}");
    }
    if log_tail.is_empty() {
        let _ = writeln!(out, "log:        (empty)");
    } else {
        let _ = writeln!(out, "log (last {} lines):", log_tail.len());
        for line in log_tail {
            let _ = writeln!(out, "  {line}");
        }
    }
    out
}

/// Pure human line for closed legacy detached [`TaskState`] kinds.
///
/// Live on detached `task status` print (WP-440). Distinct from durable
/// [`vyane_task::TaskState`] pure lines.
pub fn format_legacy_task_state_line(state: TaskState) -> String {
    format!("legacy task state: {}", terminal_safe(state.as_str()))
}

pub fn print_task_status(status: &StatusFile, displayed: TaskState, log_tail: &[String]) {
    // Kind-only pure legacy task state on detached status print (WP-440).
    tracing::info!(
        state = displayed.as_str(),
        "{}",
        format_legacy_task_state_line(displayed)
    );
    print!("{}", format_task_status(status, displayed, log_tail));
}

#[cfg(test)]
mod tests {
    use vyane_harness::native::{NativeTurnStop, ToolInvocationStatus};

    use super::{
        BroadcastRow, TaskRow, format_a2a_error_line, format_a2a_more_messages_line,
        format_adapter_failure_line, format_adapter_outcome_line, format_adapter_transport_line,
        format_agent_completion_projection_status_line,
        format_agent_completion_sink_observation_line,
        format_agent_completion_sink_transition_line, format_agent_execution_item_status_line,
        format_agent_execution_settlement_line, format_agent_executor_outcome_line,
        format_agent_message_completion_stage_error_kind_line,
        format_agent_recovery_item_status_line, format_agent_store_error_kind_line,
        format_async_dispatch_label_error_line, format_attempt_outcome_line,
        format_auth_style_line, format_broadcast_label_error_line,
        format_broadcast_setup_error_line, format_broadcast_table,
        format_broadcast_target_error_line, format_broker_error_kind_line,
        format_cancel_diagnostic_line, format_cancel_incomplete_process_groups_line,
        format_cancel_outcome_line, format_cancel_ownership_changed_line,
        format_check_config_files, format_check_harness_line, format_check_harnesses_header,
        format_check_profile_env_line, format_check_profile_environment_header,
        format_check_profile_line, format_check_profile_warning_body, format_check_profiles_header,
        format_check_provider_line, format_check_providers_header, format_config_check_status_line,
        format_config_error_line, format_config_issue_code_line, format_controller_kind_line,
        format_controller_recovery_observation_line, format_credential_status_line,
        format_criterion_status_line, format_daemon_already_running_line,
        format_daemon_listening_line, format_daemon_not_running_line,
        format_daemon_not_running_stale_line, format_daemon_running_line,
        format_daemon_started_line, format_daemon_stopped_line,
        format_daemon_workflow_control_error_kind_line, format_daemon_workflow_view,
        format_delivery_status_line, format_dispatch_broadcast_error_line,
        format_dispatch_label_error_line, format_dispatch_stream_error_line,
        format_durable_task_status, format_effort_line, format_empty_task_list_line,
        format_error_kind_line_token, format_error_line, format_event_log_error_kind_line,
        format_execution_backend_line, format_external_task_label_error_line,
        format_goal_continuity_authority_error_kind_line, format_goal_continuity_mode_line,
        format_goal_continuity_next_action_kind_line, format_goal_continuity_operator_command_line,
        format_goal_continuity_runner_error_kind_line, format_goal_continuity_signal_kind_line,
        format_goal_continuity_status_line, format_goal_continuity_step_status_line,
        format_goal_error_line, format_goal_event_kind_line,
        format_goal_observation_ingress_error_kind_line, format_goal_observation_kind_line,
        format_goal_observation_runner_error_kind_line, format_goal_observation_signal_kind_line,
        format_goal_observation_status_line, format_goal_observation_watch_status_line,
        format_goal_observation_watcher_error_code_line, format_goal_read_error_kind_line,
        format_goal_read_unavailable_line, format_goal_read_worker_failed_line,
        format_goal_status_line, format_goal_store_error_kind_line, format_harness_kind_line,
        format_identity_changed_before_sigkill_line,
        format_identity_mismatch_nested_cleanup_failed_line,
        format_identity_mismatch_refuse_signal_line, format_inprocess_agent_effect_line,
        format_journal_step_status_line, format_kill_delivered_unfinalized_line,
        format_legacy_session_line, format_legacy_task_state_line, format_message_event_kind_line,
        format_message_publication_status_line, format_message_store_error_kind_line,
        format_nack_disposition_line, format_native_filesystem_policy_error_kind_line,
        format_native_permission_axis_status_line, format_native_permission_set_error_kind_line,
        format_native_session_state_line, format_native_session_transition_line,
        format_native_side_effect_line, format_native_turn_stop_line,
        format_nested_harness_controller_cleanup_failed_line,
        format_nested_harness_controller_write_failed_line,
        format_nested_harness_identity_unavailable_line, format_no_output_recorded_line,
        format_no_such_detached_run_line, format_not_local_detached_cancel_line,
        format_not_local_detached_line, format_on_error_policy_line,
        format_output_write_failed_line, format_owner_context_error_kind_line,
        format_permission_check, format_permission_effect_line,
        format_permission_rule_error_kind_line, format_process_identity_unavailable_line,
        format_profile_check_status_line, format_protocol_line, format_pump_item_status_line,
        format_pursuit_checkpoint_status_line, format_pursuit_segment_status_line,
        format_pursuit_status_line, format_quota_runner_error_kind_line,
        format_quota_transport_error_kind_line, format_quota_validation_error_kind_line,
        format_record_line, format_register_command_tool_error_kind_line,
        format_register_web_fetch_tool_error_kind_line,
        format_register_web_search_tool_error_kind_line, format_replay_safety_line,
        format_route_result, format_run_attempt_outcome_view_line,
        format_run_completion_status_line, format_run_failure_code_line, format_run_failure_line,
        format_run_id_line, format_run_ledger_query_error_line, format_run_mode_line,
        format_run_settlement_line, format_run_state_line, format_run_status_line,
        format_sandbox_line, format_serve_listening_line, format_serve_loopback_only_line,
        format_serve_starting_line, format_session_control_error_line,
        format_session_native_state_line, format_session_snapshot_query_error_line,
        format_session_view_line, format_stale_detached_status_line,
        format_stream_dispatch_label_error_line, format_stream_dispatch_request_error_line,
        format_stream_not_applicable_line, format_stream_route_error_line,
        format_stream_tool_use_line, format_stream_unsupported_fallback_line,
        format_takeover_approval_status_line, format_takeover_decision_line,
        format_takeover_run_status_line, format_takeover_sandbox_line,
        format_task_already_cleanup_failed_line, format_task_already_state_line,
        format_task_dispatch_failed_line, format_task_dispatch_panicked_line,
        format_task_duplicate_runtime_dispatch_line, format_task_failure_code_line,
        format_task_final_state_line, format_task_init_cleanup_contended_line,
        format_task_init_cleanup_failed_line, format_task_init_cleanup_read_failed_line,
        format_task_kind_line, format_task_legacy_output_read_failed_line,
        format_task_metadata_error_line, format_task_metadata_settlement_retry_line,
        format_task_origin_line, format_task_output_artifact_failed_line,
        format_task_output_read_failed_line, format_task_settlement_line, format_task_state_line,
        format_task_status, format_task_store_error_kind_line, format_task_table,
        format_tool_chat_validation_error_kind_line, format_tool_invocation_status_line,
        format_web_search_context_size_line, format_worker_error_line,
        format_worker_gone_nested_cleanup_complete_line,
        format_worker_gone_nested_cleanup_failed_line,
        format_worker_leader_exited_group_remains_line,
        format_worker_metadata_settlement_failed_line, format_workflow_cancel_line,
        format_workflow_control_error_kind_line, format_workflow_error_kind_line,
        format_workflow_error_line, format_workflow_list, format_workflow_run_status_line,
        format_workflow_state_line, format_workflow_step_cancelled_line,
        format_workflow_step_failed_line, format_workflow_step_skipped_line,
        format_workflow_step_started_line, format_workflow_step_succeeded_line,
        format_workflow_submit_error_kind_line, format_workflow_summary,
        session_control_error_view, short_run_id, status_name, target_selector, terminal_safe,
        workflow_status_name,
    };
    use crate::daemon_workflow::WorkflowTaskView;
    use crate::task::store::{StatusFile, TaskState as LegacyTaskState};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use vyane_core::{
        AdapterTransport, ErrorKind, HarnessKind, ModelId, NativeSessionState, Protocol,
        ProviderId, RunRecord, RunStatus, Sandbox, SessionRecord, SessionSnapshot, Target,
    };
    use vyane_router::{RouteDecision, RouteEffort, RouteTier};
    use vyane_service::{
        HarnessPermissionCheck, NativePermissionAxisStatus, NativePermissionCheck, PermissionCheck,
        SessionView,
    };
    use vyane_task::{FailureCode, TaskKind, TaskOrigin, TaskRecord, TaskState};
    use vyane_workflow::{
        JournalStep, JournalStepStatus, WorkflowJournal, WorkflowJournalSummary, WorkflowOutcome,
        WorkflowReplayProvenance, WorkflowRunId, WorkflowRunStatus, WorkflowStepCounts,
    };

    fn sample_task_row(id: &str, state: &str, failure_code: Option<&str>) -> TaskRow {
        let at = Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).unwrap();
        TaskRow {
            id: id.into(),
            state: state.into(),
            target: "provider/model".into(),
            origin: "cli_detached".into(),
            created_at: at,
            started_at: Some(at),
            updated_at: at,
            finished_at: Some(at),
            duration_ms: Some(42),
            ledger_run_id: None,
            failure_code: failure_code.map(str::to_string),
        }
    }

    fn sample_durable_task(state: TaskState, failure_code: Option<FailureCode>) -> TaskRecord {
        let at = Utc.with_ymd_and_hms(2026, 4, 1, 15, 30, 0).unwrap();
        TaskRecord {
            id: "019aaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa".into(),
            owner: "local".into(),
            kind: TaskKind::Dispatch,
            origin: TaskOrigin::CliDetached,
            state,
            task_digest: "b".repeat(64),
            target_key: "provider/model".into(),
            created_at: at,
            started_at: Some(at),
            updated_at: at,
            finished_at: Some(at),
            revision: 1,
            executor_epoch: 1,
            controller: None,
            lease: None,
            ledger_run_id: None,
            failure_code,
        }
    }

    #[test]
    fn session_control_text_escapes_terminal_control_sequences() {
        let rendered = terminal_safe("session\n\u{1b}[31m\u{202e}");
        assert!(!rendered.contains('\n'));
        assert!(!rendered.contains('\u{1b}'));
        assert!(!rendered.contains('\u{202e}'));
        assert!(rendered.contains("\\n"));
        assert!(rendered.contains("\\u{1b}"));
        assert!(rendered.contains("\\u{202e}"));
    }

    #[test]
    fn human_task_table_prints_snake_case_failure_code_when_present() {
        let rows = [
            sample_task_row("task-failed-1", "failed", Some("worker_lost")),
            sample_task_row("task-timed-out-1", "timed_out", Some("timed_out")),
        ];
        let text = format_task_table(&rows);
        let header = text.lines().next().expect("header line");
        assert!(
            header.split_whitespace().any(|col| col == "failure"),
            "expected failure column when codes present:\n{text}"
        );
        assert!(
            text.contains("worker_lost"),
            "expected worker_lost in human list:\n{text}"
        );
        assert!(
            text.contains("timed_out"),
            "expected timed_out in human list:\n{text}"
        );
        // Stable prior columns remain present.
        for col in ["id", "state", "duration", "created", "target"] {
            assert!(
                header.split_whitespace().any(|c| c == col),
                "missing column {col} in header:\n{text}"
            );
        }
    }

    #[test]
    fn human_task_table_omits_failure_column_when_no_codes() {
        let rows = [
            sample_task_row("task-ok-1", "succeeded", None),
            sample_task_row("task-running-1", "running", None),
        ];
        let text = format_task_table(&rows);
        let header = text.lines().next().expect("header line");
        assert!(
            !header.split_whitespace().any(|col| col == "failure"),
            "must not invent failure column for no-code table:\n{text}"
        );
        for line in text.lines() {
            assert!(
                !line.split_whitespace().any(|cell| {
                    matches!(cell, "worker_lost" | "timed_out" | "cancelled" | "failure")
                }),
                "must not invent failure cells for no-code rows:\n{text}"
            );
        }
        // Prior columns stay present and readable.
        for col in ["id", "state", "duration", "created", "target"] {
            assert!(
                header.split_whitespace().any(|c| c == col),
                "missing column {col} in header:\n{text}"
            );
        }
        assert!(text.contains("task-ok-1"), "{text}");
        assert!(text.contains("succeeded"), "{text}");
    }

    #[test]
    fn human_task_table_mixed_rows_show_code_only_where_present() {
        let rows = [
            sample_task_row("task-ok-2", "succeeded", None),
            sample_task_row("task-cancel-1", "cancelled", Some("cancelled")),
        ];
        let text = format_task_table(&rows);
        assert!(
            text.contains("cancelled"),
            "expected cancelled code in mixed table:\n{text}"
        );
        // Header surfaces failure because at least one row has a code.
        let header = text.lines().next().expect("header");
        assert!(
            header.split_whitespace().any(|col| col == "failure"),
            "expected failure column for mixed table:\n{text}"
        );
        // The no-code data row must not invent a synthetic failure token.
        let ok_line = text
            .lines()
            .find(|line| line.contains("task-ok-2"))
            .expect("ok row");
        assert!(
            !ok_line.split_whitespace().any(|cell| {
                matches!(
                    cell,
                    "worker_lost" | "timed_out" | "cancelled" | "none" | "-"
                )
            }),
            "no-code row must not invent a failure token:\n{ok_line}"
        );
    }

    #[test]
    fn human_durable_task_status_prints_snake_case_failure_code() {
        let task = sample_durable_task(TaskState::Failed, Some(FailureCode::WorkerLost));
        let text = format_durable_task_status(&task, &[]);
        assert!(
            text.contains("failure:    worker_lost\n"),
            "expected durable failure_code line from FailureCode Display:\n{text}"
        );
        assert!(text.contains("state:      failed\n"), "{text}");
        // State precedes failure.
        let state_at = text.find("state:").expect("state");
        let failure_at = text.find("failure:    worker_lost\n").expect("failure");
        assert!(
            state_at < failure_at,
            "state should precede failure:\n{text}"
        );
    }

    #[test]
    fn human_durable_task_status_prints_timed_out_failure_code() {
        let task = sample_durable_task(TaskState::TimedOut, Some(FailureCode::TimedOut));
        let text = format_durable_task_status(&task, &["tail-line".into()]);
        assert!(
            text.contains("failure:    timed_out\n"),
            "expected timed_out failure_code:\n{text}"
        );
        assert!(
            text.contains("  tail-line\n"),
            "log tail must render:\n{text}"
        );
        assert!(!text.contains("log:        (empty)"), "{text}");
    }

    #[test]
    fn human_durable_task_status_omits_failure_line_when_absent() {
        let task = sample_durable_task(TaskState::Succeeded, None);
        let text = format_durable_task_status(&task, &[]);
        assert!(
            !text.lines().any(|line| line.starts_with("failure:")),
            "must not invent failure line when code is None:\n{text}"
        );
        assert!(text.contains("log:        (empty)\n"), "{text}");
        assert!(
            text.contains("id:         019aaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa\n"),
            "{text}"
        );
    }

    fn sample_workflow_summary(
        id: &str,
        name: &str,
        status: WorkflowRunStatus,
        success: usize,
        failed: usize,
        skipped: usize,
        cancelled: usize,
    ) -> WorkflowJournalSummary {
        let at = Utc.with_ymd_and_hms(2026, 5, 1, 10, 0, 0).unwrap();
        WorkflowJournalSummary {
            id: id.parse::<WorkflowRunId>().expect("uuid v7"),
            name: name.into(),
            status,
            started_at: at,
            updated_at: at,
            steps: WorkflowStepCounts {
                pending: 0,
                running: 0,
                success,
                failed,
                skipped,
                cancelled,
            },
        }
    }

    #[test]
    fn human_workflow_list_prints_status_and_step_counts() {
        let rows = [sample_workflow_summary(
            "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e",
            "demo-wf",
            WorkflowRunStatus::CompletedWithFailures,
            2,
            1,
            0,
            0,
        )];
        let text = format_workflow_list(&rows);
        let header = text.lines().next().expect("header");
        for col in ["id", "started_at", "name", "status", "steps"] {
            assert!(
                header.split_whitespace().any(|c| c == col),
                "missing column {col}:\n{text}"
            );
        }
        assert!(
            text.contains(workflow_status_name(
                WorkflowRunStatus::CompletedWithFailures
            )),
            "expected status name from shipped helper:\n{text}"
        );
        assert!(
            text.contains("2/3 ok, 1 failed, 0 skipped, 0 cancelled"),
            "expected step counts from real summary:\n{text}"
        );
        assert!(text.contains("demo-wf"), "{text}");
        assert!(
            text.contains("01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e"),
            "{text}"
        );
    }

    #[test]
    fn human_workflow_list_renders_failed_and_cancelled_rows() {
        let rows = [
            sample_workflow_summary(
                "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e",
                "failed-wf",
                WorkflowRunStatus::Failed,
                0,
                2,
                0,
                0,
            ),
            sample_workflow_summary(
                "01890f3e-7b7d-7cc2-98d2-3f9a2b6c7d8e",
                "cancel-wf",
                WorkflowRunStatus::Cancelled,
                1,
                0,
                0,
                1,
            ),
        ];
        let text = format_workflow_list(&rows);
        assert!(
            text.contains(workflow_status_name(WorkflowRunStatus::Failed)),
            "{text}"
        );
        assert!(
            text.contains(workflow_status_name(WorkflowRunStatus::Cancelled)),
            "{text}"
        );
        assert!(
            text.contains("0/2 ok, 2 failed, 0 skipped, 0 cancelled"),
            "{text}"
        );
        assert!(
            text.contains("1/2 ok, 0 failed, 0 skipped, 1 cancelled"),
            "{text}"
        );
    }

    #[test]
    fn human_workflow_list_empty_table_is_header_only() {
        let text = format_workflow_list(&[]);
        assert_eq!(text.lines().count(), 1, "header only:\n{text}");
        assert!(text.contains("status"), "{text}");
        assert!(!text.contains("failed"), "{text}");
    }

    fn sample_run_record(status: RunStatus, duration_ms: i64) -> RunRecord {
        let start = Utc.with_ymd_and_hms(2026, 6, 1, 12, 0, 0).unwrap();
        let finish = start + chrono::Duration::milliseconds(duration_ms);
        RunRecord {
            run_id: "019bbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb".into(),
            owner: "local".into(),
            started_at: start,
            finished_at: finish,
            task_digest: "d".into(),
            task_preview: None,
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            target: Target {
                provider: ProviderId::new("p"),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new("m"),
            },
            transport: AdapterTransport::DirectHttp,
            attempts: vec![],
            status,
            usage: None,
            cost_usd: None,
            session_id: None,
            output_chars: None,
            error: None,
            labels: Default::default(),
        }
    }

    #[test]
    fn human_broadcast_table_prints_success_and_error_rows() {
        let rows = [
            BroadcastRow {
                target: "primary/model".into(),
                record: Some(sample_run_record(RunStatus::Success, 150)),
                output: Some("hello\nworld".into()),
                error: None,
            },
            BroadcastRow {
                target: "fallback/model".into(),
                record: None,
                output: None,
                error: Some("timeout".into()),
            },
        ];
        let text = format_broadcast_table(&rows);
        let header = text.lines().next().expect("header");
        for col in ["target", "status", "duration", "output"] {
            assert!(
                header.split_whitespace().any(|c| c == col),
                "missing {col}:\n{text}"
            );
        }
        assert!(text.contains("success"), "{text}");
        assert!(text.contains("150ms"), "{text}");
        assert!(text.contains("hello"), "first_line of output:\n{text}");
        assert!(
            !text.contains("world"),
            "only first non-empty line:\n{text}"
        );
        assert!(text.contains("error"), "{text}");
        assert!(text.contains("timeout"), "{text}");
        assert!(text.contains("fallback/model"), "{text}");
    }

    #[test]
    fn human_broadcast_table_empty_is_header_only() {
        let text = format_broadcast_table(&[]);
        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(text.contains("target"), "{text}");
    }

    #[test]
    fn human_record_line_prints_status_target_duration_and_cost() {
        let mut record = sample_run_record(RunStatus::Success, 250);
        record.cost_usd = Some(0.001234);
        let text = format_record_line(&record);
        assert!(
            text.contains(short_run_id(&record.run_id)),
            "short id:\n{text}"
        );
        assert!(
            text.contains(&target_selector(&record)),
            "target selector:\n{text}"
        );
        assert!(
            text.contains(status_name(RunStatus::Success)),
            "status:\n{text}"
        );
        assert!(text.contains("250ms"), "duration:\n{text}");
        assert!(text.contains(" $0.001234"), "cost:\n{text}");
    }

    #[test]
    fn human_record_line_omits_cost_when_absent() {
        let record = sample_run_record(RunStatus::Error, 10);
        let text = format_record_line(&record);
        assert!(text.contains(status_name(RunStatus::Error)), "{text}");
        assert!(text.contains("10ms"), "{text}");
        assert!(!text.contains('$'), "no cost when None:\n{text}");
    }

    fn sample_target() -> Target {
        Target {
            provider: ProviderId::new("p"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("m"),
        }
    }

    fn sample_session_record(session_id: &str, run_count: u64) -> SessionRecord {
        let at = Utc.with_ymd_and_hms(2026, 7, 1, 9, 0, 0).unwrap();
        SessionRecord {
            session_id: session_id.into(),
            owner: "local".into(),
            target: sample_target(),
            native_session_id: None,
            transcript: vec![],
            created_at: at,
            updated_at: at,
            run_count,
        }
    }

    #[test]
    fn human_session_view_line_prints_native_and_resume_flags() {
        let mut record = sample_session_record("sess-1", 4);
        record.session_id = "sess\n\u{1b}[31m".into();
        let view = SessionView::from(SessionSnapshot {
            record,
            session_revision: 7,
            native_session: NativeSessionState::LegacyUnbound {
                native_session_id: "native-x".into(),
            },
        });
        let text = format_session_view_line(&view);
        assert!(text.contains("runs=4"), "{text}");
        assert!(text.contains("revision=7"), "{text}");
        assert!(text.contains("native=legacy_unbound"), "{text}");
        assert!(text.contains("native_resume=disabled"), "{text}");
        assert!(
            text.contains(&terminal_safe("sess\n\u{1b}[31m")),
            "session id must be terminal-safe:\n{text}"
        );
        assert!(
            !text.contains('\n') || text.matches('\n').count() == 0 || !text.contains("\n\u{1b}"),
            "{text}"
        );
        // single line (no raw newline from session id)
        assert_eq!(text.lines().count(), 1, "one line:\n{text}");
    }

    #[test]
    fn human_session_view_line_absent_native_state() {
        let view = SessionView::from(SessionSnapshot {
            record: sample_session_record("plain-sess", 0),
            session_revision: 0,
            native_session: NativeSessionState::Absent,
        });
        let text = format_session_view_line(&view);
        assert!(text.contains("native=absent"), "{text}");
        assert!(text.contains("plain-sess"), "{text}");
        assert!(
            text.contains(&view.target.to_string()) || text.contains("p/"),
            "{text}"
        );
    }

    #[test]
    fn human_legacy_session_line_prints_id_target_runs_updated() {
        let record = sample_session_record("legacy-sess", 3);
        let text = format_legacy_session_line(&record);
        assert!(text.contains("legacy-sess"), "{text}");
        assert!(text.contains("3"), "{text}");
        assert!(text.contains(&record.updated_at.to_rfc3339()), "{text}");
        // Target Display participates in the line.
        assert!(
            text.contains(&record.target.to_string()),
            "target display:\n{text}"
        );
    }

    fn sample_status_file(state: LegacyTaskState, error: Option<&str>) -> StatusFile {
        let start = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
        let finished = start + chrono::Duration::milliseconds(500);
        StatusFile {
            schema: 1,
            run_id: "run-legacy-1".into(),
            pid: 4242,
            pgid: 4242,
            state,
            started_at: start,
            target: "provider/model".into(),
            workdir: Some("/tmp/work".into()),
            finished_at: Some(finished),
            ledger_run_id: Some("ledger-1".into()),
            error: error.map(str::to_string),
        }
    }

    #[test]
    fn human_legacy_task_status_prints_displayed_state_and_error() {
        let status = sample_status_file(LegacyTaskState::Error, Some("boom"));
        let text = format_task_status(&status, LegacyTaskState::Died, &["line-a".into()]);
        assert!(text.contains("id:         run-legacy-1\n"), "{text}");
        assert!(
            text.contains("state:      died\n"),
            "displayed state, not raw file state:\n{text}"
        );
        assert!(text.contains("error:      boom\n"), "{text}");
        assert!(text.contains("  line-a\n"), "{text}");
        assert!(text.contains("duration:   500ms\n"), "{text}");
        assert!(text.contains("workdir:    /tmp/work\n"), "{text}");
    }

    #[test]
    fn human_legacy_task_status_omits_error_when_absent() {
        let status = sample_status_file(LegacyTaskState::Success, None);
        let text = format_task_status(&status, LegacyTaskState::Success, &[]);
        assert!(
            !text.lines().any(|line| line.starts_with("error:")),
            "no error line:\n{text}"
        );
        assert!(text.contains("log:        (empty)\n"), "{text}");
        assert!(text.contains("state:      succeeded\n"), "{text}");
    }

    fn sample_workflow_outcome(status: WorkflowRunStatus, with_replay: bool) -> WorkflowOutcome {
        let at = Utc.with_ymd_and_hms(2026, 8, 2, 12, 0, 0).unwrap();
        let run_id: WorkflowRunId = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e"
            .parse()
            .expect("uuid v7 run id");
        let mut steps = BTreeMap::new();
        steps.insert(
            "finder".into(),
            JournalStep {
                status: JournalStepStatus::Success,
                run_ids: vec!["r1".into()],
                output: Some("found it\nsecond".into()),
                outputs: None,
                error: None,
            },
        );
        steps.insert(
            "synth".into(),
            JournalStep {
                status: JournalStepStatus::Failed,
                run_ids: vec!["r2".into(), "r3".into()],
                output: None,
                outputs: None,
                error: Some("synth boom".into()),
            },
        );
        let source: WorkflowRunId = "01890f3e-7b7d-7cc2-98d2-3f9a2b6c7d8e"
            .parse()
            .expect("uuid v7 source id");
        let replay = if with_replay {
            Some(WorkflowReplayProvenance {
                source_wf_run_id: source,
                source_plan_sha256: "p".repeat(64),
                reused_steps_sha256: "s".repeat(64),
                reused_step_ids: vec!["finder".into()],
            })
        } else {
            None
        };
        WorkflowOutcome {
            wf_run_id: run_id.clone(),
            status,
            journal_path: PathBuf::from("/tmp/wf-journals/demo.json"),
            journal: WorkflowJournal {
                wf_run_id: run_id,
                workflow_name: "demo".into(),
                file_sha256: "f".repeat(64),
                plan_sha256: Some("p".repeat(64)),
                replay,
                vars: BTreeMap::new(),
                started_at: at,
                updated_at: at,
                status,
                steps,
            },
        }
    }

    #[test]
    fn human_workflow_summary_prints_status_steps_and_first_line_output() {
        let outcome = sample_workflow_outcome(WorkflowRunStatus::CompletedWithFailures, false);
        let text = format_workflow_summary(&outcome);
        assert!(
            text.contains(&format!(
                "workflow {} {}",
                outcome.wf_run_id,
                workflow_status_name(WorkflowRunStatus::CompletedWithFailures)
            )),
            "{text}"
        );
        assert!(text.contains("/tmp/wf-journals/demo.json"), "{text}");
        assert!(!text.contains("replay source="), "no replay:\n{text}");
        assert!(text.contains("finder"), "{text}");
        assert!(text.contains("success"), "{text}");
        assert!(text.contains("found it"), "first_line only:\n{text}");
        assert!(!text.contains("second"), "not second line:\n{text}");
        assert!(text.contains("synth"), "{text}");
        assert!(text.contains("failed"), "{text}");
        assert!(text.contains("synth boom"), "{text}");
        assert!(text.contains("   2 "), "two run_ids on synth:\n{text}");
    }

    #[test]
    fn human_workflow_summary_prints_replay_line_when_present() {
        let outcome = sample_workflow_outcome(WorkflowRunStatus::Completed, true);
        let text = format_workflow_summary(&outcome);
        assert!(
            text.contains("replay source=01890f3e-7b7d-7cc2-98d2-3f9a2b6c7d8e reused_steps=1"),
            "{text}"
        );
        assert!(
            text.contains(workflow_status_name(WorkflowRunStatus::Completed)),
            "{text}"
        );
    }

    fn sample_route_decision() -> RouteDecision {
        RouteDecision {
            selection_key: String::new(),
            provider: "openai\n".into(),
            model: "gpt-test\t1".into(),
            effort: RouteEffort::Medium,
            tier: RouteTier::Mainline,
            tag: "frontend\u{1b}[31m".into(),
            intent: "implement".into(),
            complexity_score: 0.4567,
            reason: "tag:frontend preference selected".into(),
        }
    }

    #[test]
    fn human_route_result_prints_profile_and_decision_fields() {
        let decision = sample_route_decision();
        let text = format_route_result("coding\tmain", &decision);
        assert!(text.contains("profile:     coding\\tmain"), "{text}");
        assert!(text.contains("provider:    openai\\n"), "{text}");
        assert!(text.contains("model:       gpt-test\\t1"), "{text}");
        assert!(text.contains("tier:        mainline"), "{text}");
        assert!(text.contains("effort:      medium"), "{text}");
        assert!(text.contains("score:       0.457"), "{text}");
        assert!(text.contains("tag:         frontend"), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("intent:      implement"), "{text}");
        assert!(
            text.contains("reason:      tag:frontend preference selected"),
            "{text}"
        );
    }

    #[test]
    fn human_route_result_uses_tier_and_effort_as_str() {
        let mut decision = sample_route_decision();
        decision.tier = RouteTier::Economy;
        decision.effort = RouteEffort::Low;
        decision.complexity_score = 0.1;
        let text = format_route_result("economy-profile", &decision);
        assert!(text.starts_with("profile:     economy-profile\n"), "{text}");
        assert!(text.contains("tier:        economy"), "{text}");
        assert!(text.contains("effort:      low"), "{text}");
        assert!(text.contains("score:       0.100"), "{text}");
    }

    #[test]
    fn human_permission_check_prints_harness_and_native_axes() {
        let permissions = PermissionCheck {
            harness: HarnessPermissionCheck {
                ceiling_layers: 2,
                max_sandbox: Sandbox::ReadOnly,
            },
            native: NativePermissionCheck {
                ceiling_layers: 1,
                filesystem_read: NativePermissionAxisStatus::Bounded,
                filesystem_write: NativePermissionAxisStatus::Disabled,
                command_execution: NativePermissionAxisStatus::UnrestrictedByConfig,
                command_network: NativePermissionAxisStatus::Bounded,
                web_search: NativePermissionAxisStatus::Disabled,
                web_fetch: NativePermissionAxisStatus::Bounded,
                tool_policy_layers: 1,
                tool_policy_rule_count: 3,
            },
        };
        let text = format_permission_check(&permissions);
        assert!(text.starts_with("permissions:\n"), "{text}");
        assert!(
            text.contains("cli-harness: max_sandbox=read-only ceiling_layers=2"),
            "{text}"
        );
        assert!(text.contains("filesystem_read=bounded"), "{text}");
        assert!(text.contains("filesystem_write=disabled"), "{text}");
        assert!(
            text.contains("command_execution=unrestricted_by_config"),
            "{text}"
        );
        assert!(text.contains("tool_policy_layers=1"), "{text}");
        assert!(text.contains("tool_policy_rules=3"), "{text}");
    }

    #[test]
    fn human_permission_check_uses_full_sandbox_token() {
        let permissions = PermissionCheck {
            harness: HarnessPermissionCheck {
                ceiling_layers: 0,
                max_sandbox: Sandbox::Full,
            },
            native: NativePermissionCheck {
                ceiling_layers: 0,
                filesystem_read: NativePermissionAxisStatus::UnrestrictedByConfig,
                filesystem_write: NativePermissionAxisStatus::UnrestrictedByConfig,
                command_execution: NativePermissionAxisStatus::UnrestrictedByConfig,
                command_network: NativePermissionAxisStatus::UnrestrictedByConfig,
                web_search: NativePermissionAxisStatus::UnrestrictedByConfig,
                web_fetch: NativePermissionAxisStatus::UnrestrictedByConfig,
                tool_policy_layers: 0,
                tool_policy_rule_count: 0,
            },
        };
        let text = format_permission_check(&permissions);
        assert!(text.contains("max_sandbox=full"), "{text}");
        assert!(text.contains("ceiling_layers=0"), "{text}");
    }

    fn sample_workflow_task(state: TaskState) -> TaskRecord {
        TaskRecord {
            id: "01999999-9999-7999-8999-999999999999".to_string(),
            owner: "local".to_string(),
            kind: TaskKind::Workflow,
            origin: TaskOrigin::Daemon,
            state,
            task_digest: "a".repeat(64),
            target_key: "workflow".to_string(),
            created_at: Utc::now(),
            started_at: None,
            updated_at: Utc::now(),
            finished_at: None,
            revision: 0,
            executor_epoch: 0,
            controller: None,
            lease: None,
            ledger_run_id: None,
            failure_code: None,
        }
    }

    #[test]
    fn human_daemon_workflow_view_prints_bounded_success_output() {
        let view = WorkflowTaskView {
            task: sample_workflow_task(TaskState::Succeeded),
            journal: None,
            output: Some("bounded answer".to_string()),
            output_omitted: false,
        };
        let text = format_daemon_workflow_view(&view);
        assert!(
            text.contains("workflow 01999999-9999-7999-8999-999999999999 succeeded"),
            "{text}"
        );
        assert!(
            text.contains("output\nbounded answer\n"),
            "expected WP-152 projection body in human status:\n{text}"
        );
        assert!(!text.contains("output omitted"), "{text}");
        assert!(!text.contains("failure "), "{text}");
    }

    #[test]
    fn human_daemon_workflow_view_prints_output_omitted_without_body() {
        let view = WorkflowTaskView {
            task: sample_workflow_task(TaskState::Succeeded),
            journal: None,
            output: None,
            output_omitted: true,
        };
        let text = format_daemon_workflow_view(&view);
        assert!(text.contains("output omitted\n"), "{text}");
        assert!(!text.contains("output\n"), "{text}");
        assert!(!text.contains("failure "), "{text}");
    }

    #[test]
    fn human_daemon_workflow_view_hides_output_on_non_success() {
        let view = WorkflowTaskView {
            task: sample_workflow_task(TaskState::Running),
            journal: None,
            output: None,
            output_omitted: false,
        };
        let text = format_daemon_workflow_view(&view);
        assert!(text.contains("running"), "{text}");
        assert!(!text.contains("output"), "{text}");
        assert!(!text.contains("failure "), "{text}");
    }

    #[test]
    fn human_daemon_workflow_view_prints_failure_code() {
        let mut task = sample_workflow_task(TaskState::Failed);
        task.failure_code = Some(FailureCode::WorkerLost);
        let view = WorkflowTaskView {
            task,
            journal: None,
            output: None,
            output_omitted: false,
        };
        let text = format_daemon_workflow_view(&view);
        assert!(
            text.contains("workflow 01999999-9999-7999-8999-999999999999 failed"),
            "{text}"
        );
        assert!(
            text.contains("failure worker_lost\n"),
            "expected durable failure_code in human status:\n{text}"
        );
        assert!(!text.contains("output"), "{text}");
    }

    #[test]
    fn human_daemon_workflow_view_prints_timed_out_failure_code() {
        let mut task = sample_workflow_task(TaskState::TimedOut);
        task.failure_code = Some(FailureCode::TimedOut);
        let view = WorkflowTaskView {
            task,
            journal: None,
            output: None,
            output_omitted: false,
        };
        let text = format_daemon_workflow_view(&view);
        assert!(text.contains("timed_out"), "{text}");
        assert!(
            text.contains("failure timed_out\n"),
            "expected timed_out failure_code in human status:\n{text}"
        );
        let state_at = text.find("workflow ").expect("state line");
        let failure_at = text.find("failure timed_out\n").expect("failure line");
        assert!(
            state_at < failure_at,
            "state should precede failure:\n{text}"
        );
    }

    #[test]
    fn human_task_final_state_line_is_terminal_safe() {
        let text = format_task_final_state_line("run-1\n\u{1b}[31m", "cancelled");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("cancelled"), "{text}");
        assert!(text.starts_with("run-1"), "{text}");
    }

    #[test]
    fn human_task_already_state_line_prints_idempotent_cancel_shape() {
        let text = format_task_already_state_line("run-9\n\u{1b}[31m", "succeeded");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains(" already "), "{text}");
        assert!(text.contains("succeeded"), "{text}");
        assert!(text.starts_with("run-9"), "{text}");
    }

    #[test]
    fn human_task_already_state_line_uses_display_state() {
        let text = format_task_already_state_line("t1", TaskState::Cancelled);
        assert_eq!(text, "t1 already cancelled", "{text}");
    }

    #[test]
    fn human_daemon_lifecycle_lines_match_operator_contract() {
        assert_eq!(
            format_daemon_already_running_line("127.0.0.1:7700"),
            "vyane daemon already running at 127.0.0.1:7700"
        );
        assert_eq!(
            format_daemon_started_line("127.0.0.1:7700", 4242),
            "vyane daemon started at 127.0.0.1:7700 (pid 4242)"
        );
        assert_eq!(
            format_daemon_running_line("127.0.0.1:7701", 99),
            "vyane daemon running at 127.0.0.1:7701 (pid 99)"
        );
        assert_eq!(format_daemon_stopped_line(), "vyane daemon stopped");
    }

    #[test]
    fn human_empty_task_list_and_run_id_lines() {
        assert_eq!(format_empty_task_list_line(), "no detached runs");
        let id = format_run_id_line("0199\n\u{1b}[31m");
        assert!(!id.contains('\u{1b}'), "no raw ESC:\n{id}");
        assert!(id.starts_with("0199"), "{id}");
    }

    #[test]
    fn session_control_error_view_maps_exit_codes_and_human_line() {
        let not_found = session_control_error_view(ErrorKind::NotFound);
        assert_eq!(not_found.kind_code, "not_found");
        assert_eq!(not_found.exit_code, 2);
        assert_eq!(
            format_session_control_error_line(&not_found),
            "error: session not found"
        );
        let conflict = session_control_error_view(ErrorKind::Conflict);
        assert_eq!(conflict.exit_code, 3);
        assert!(conflict.inspect_before_retry);
        let indeterminate = session_control_error_view(ErrorKind::Indeterminate);
        assert_eq!(indeterminate.exit_code, 4);
        assert!(indeterminate.inspect_before_retry);
    }

    #[test]
    fn human_workflow_error_line_prefixes_config_vs_runtime() {
        assert_eq!(
            format_workflow_error_line(true, "missing step target"),
            "config error: missing step target"
        );
        assert_eq!(
            format_workflow_error_line(false, "engine interrupted"),
            "error: engine interrupted"
        );
    }

    #[test]
    fn human_detached_task_error_lines_are_terminal_safe() {
        let id = "run\n\u{1b}[31m";
        let missing = format_no_such_detached_run_line(id);
        assert!(missing.starts_with("no such detached run: "), "{missing}");
        assert!(!missing.contains('\u{1b}'), "{missing}");
        let no_out = format_no_output_recorded_line(id);
        assert!(no_out.starts_with("no output recorded for "), "{no_out}");
        assert!(!no_out.contains('\u{1b}'), "{no_out}");
        let not_local = format_not_local_detached_line(id);
        assert!(
            not_local.contains("is not a local detached dispatch"),
            "{not_local}"
        );
        assert!(!not_local.contains('\u{1b}'), "{not_local}");
    }

    #[test]
    fn human_serve_and_config_error_lines_match_contract() {
        assert_eq!(
            format_serve_loopback_only_line(),
            "config error: vyane serve only accepts loopback listen addresses"
        );
        assert_eq!(
            format_serve_starting_line("127.0.0.1:8080"),
            "vyane serve starting on 127.0.0.1:8080"
        );
        assert_eq!(
            format_config_error_line("missing provider"),
            "config error: missing provider"
        );
    }

    #[test]
    fn human_error_and_worker_error_lines_match_contract() {
        assert_eq!(
            format_error_line("dispatch failed"),
            "error: dispatch failed"
        );
        assert_eq!(
            format_worker_error_line("spawn failed"),
            "worker error: spawn failed"
        );
    }

    #[test]
    fn human_daemon_absent_and_goal_error_lines_match_contract() {
        assert_eq!(
            format_daemon_listening_line("127.0.0.1:7700"),
            "vyane daemon listening on 127.0.0.1:7700"
        );
        assert_eq!(
            format_daemon_not_running_line(),
            "vyane daemon is not running"
        );
        assert_eq!(
            format_daemon_not_running_stale_line(),
            "vyane daemon is not running (stale descriptor removed)"
        );
        assert_eq!(
            format_goal_error_line("goal missing"),
            "goal error: goal missing"
        );
    }

    #[test]
    fn human_a2a_and_cancel_diagnostic_lines_match_contract() {
        assert_eq!(
            format_a2a_error_line("mailbox full"),
            "a2a error: mailbox full"
        );
        assert_eq!(
            format_a2a_more_messages_line(),
            "more messages are available; raise --limit to include them"
        );
        let kill = format_kill_delivered_unfinalized_line("run\n\u{1b}");
        assert!(
            kill.contains("kill delivered; worker did not finalize"),
            "{kill}"
        );
        assert!(!kill.contains('\u{1b}'), "{kill}");
        let cancel = format_not_local_detached_cancel_line("x\n");
        assert!(
            cancel.contains("cancel it through its owning frontend"),
            "{cancel}"
        );
        assert!(!cancel.contains('\n') || cancel.contains("\\n"), "{cancel}");
    }

    #[test]
    fn human_cancel_and_workflow_observer_lines_match_contract() {
        assert_eq!(
            format_cancel_ownership_changed_line("t1"),
            "t1: executor ownership changed while cancellation was requested"
        );
        assert_eq!(
            format_cancel_diagnostic_line("t1", "probe failed", TaskState::Running),
            "t1: probe failed; task is running"
        );
        assert_eq!(
            format_workflow_step_started_line("synth"),
            "workflow step synth: started"
        );
        assert_eq!(
            format_workflow_step_succeeded_line("synth", 12),
            "workflow step synth: succeeded in 12ms"
        );
        assert_eq!(
            format_workflow_step_failed_line("synth", 3, "boom"),
            "workflow step synth: failed in 3ms: boom"
        );
        assert_eq!(
            format_workflow_step_skipped_line("synth", "dep failed"),
            "workflow step synth: skipped: dep failed"
        );
        assert_eq!(
            format_workflow_step_cancelled_line("synth", 9),
            "workflow step synth: cancelled in 9ms"
        );
    }

    #[test]
    fn human_legacy_cancel_identity_lines_match_contract() {
        assert_eq!(
            format_task_already_cleanup_failed_line("t", "cancelled", "e"),
            "t: task is already cancelled, but process cleanup failed: e"
        );
        assert_eq!(
            format_worker_gone_nested_cleanup_failed_line("t", "e"),
            "t: worker is gone and nested harness cleanup failed: e"
        );
        assert_eq!(
            format_worker_gone_nested_cleanup_complete_line("t"),
            "t: worker process is gone (died); nested harness cleanup complete"
        );
        assert_eq!(
            format_identity_mismatch_nested_cleanup_failed_line("t", "pg mismatch", "e"),
            "t: outer identity mismatch (pg mismatch) and nested harness cleanup failed: e"
        );
        assert_eq!(
            format_identity_mismatch_refuse_signal_line("t", "process group mismatch"),
            "t: process identity mismatch (process group mismatch; pid likely reused); refusing to signal"
        );
        assert_eq!(
            format_nested_harness_identity_unavailable_line("t", "e"),
            "t: nested harness identity unavailable before SIGKILL: e"
        );
        assert_eq!(
            format_worker_leader_exited_group_remains_line("t"),
            "t: worker leader exited but its group remains; refusing unsafe SIGKILL escalation"
        );
        assert_eq!(
            format_identity_changed_before_sigkill_line("t", "start time mismatch"),
            "t: process identity changed before SIGKILL (start time mismatch); refusing escalation"
        );
        assert_eq!(
            format_cancel_incomplete_process_groups_line("t"),
            "t: cancellation did not finish every owned process group"
        );
        let unsafe_id = "t\nid";
        let refuse = format_identity_mismatch_refuse_signal_line(unsafe_id, "r");
        assert!(!refuse.contains('\n') || refuse.contains("\\n"), "{refuse}");
        assert!(!refuse.contains('\u{1b}'), "{refuse}");
    }

    #[test]
    fn human_durable_cancel_refuse_and_cleanup_lines_match_contract() {
        assert_eq!(
            format_process_identity_unavailable_line(
                "t1",
                "before signal delivery",
                "detached process controller was not recorded",
                "running"
            ),
            "t1: process identity unavailable before signal delivery (detached process controller was not recorded); refusing control; task remains running"
        );
        assert_eq!(
            format_task_already_cleanup_failed_line("t1", "cancelled", "boom"),
            "t1: task is already cancelled, but process cleanup failed: boom"
        );
        let unsafe_id = "t\nid";
        let line =
            format_process_identity_unavailable_line(unsafe_id, "phase", "reason", "running");
        assert!(!line.contains('\n') || line.contains("\\n"), "{line}");
        assert!(!line.contains('\u{1b}'), "{line}");
    }

    #[test]
    fn human_worker_and_stale_detached_lines_match_contract() {
        assert_eq!(
            format_stale_detached_status_line("t1", "/tmp/t1/task.log"),
            "t1: stale — worker never wrote status (spawn or stdin handoff may have failed); see /tmp/t1/task.log"
        );
        assert_eq!(
            format_worker_metadata_settlement_failed_line("t1", "boom"),
            "worker metadata settlement failed for t1: boom"
        );
        assert_eq!(
            format_output_write_failed_line("/tmp/out", "e"),
            "write /tmp/out: e"
        );
        assert_eq!(
            format_nested_harness_controller_write_failed_line("/tmp/hc", "e"),
            "nested harness controller write failed at /tmp/hc: e"
        );
        assert_eq!(
            format_nested_harness_controller_cleanup_failed_line("/tmp/hc", "e"),
            "nested harness controller cleanup failed at /tmp/hc: e"
        );
        let unsafe_id = "t\nid";
        let stale = format_stale_detached_status_line(unsafe_id, "/tmp/log");
        assert!(!stale.contains('\n') || stale.contains("\\n"), "{stale}");
        assert!(!stale.contains('\u{1b}'), "{stale}");
    }

    #[test]
    fn human_stream_notice_and_tool_lines_match_contract() {
        assert_eq!(
            format_stream_unsupported_fallback_line("openai/gpt"),
            "notice: openai/gpt does not support streaming; falling back to non-streaming"
        );
        let unsafe_target = format_stream_unsupported_fallback_line("openai/\ngpt");
        assert!(unsafe_target.contains("openai/\\ngpt"), "{unsafe_target}");
        assert!(!unsafe_target.contains('\u{1b}'), "{unsafe_target}");
        assert_eq!(
            format_stream_not_applicable_line(),
            "notice: --stream only applies to a single target with no --session; falling back to non-streaming"
        );
        assert_eq!(
            format_stream_tool_use_line("bash", "ls"),
            "\n[tool] bash: ls"
        );
        assert_eq!(
            format_tool_invocation_status_line(ToolInvocationStatus::ApprovalRequired),
            "tool status: approval_required"
        );
        assert_eq!(
            format_tool_invocation_status_line(ToolInvocationStatus::TimedOut),
            "tool status: timed_out"
        );
        assert_eq!(
            format_tool_invocation_status_line(ToolInvocationStatus::Denied),
            "tool status: denied"
        );
        assert_eq!(
            format_native_turn_stop_line(&NativeTurnStop::BudgetExhausted),
            "native turn stop: budget_exhausted"
        );
        assert_eq!(
            format_native_turn_stop_line(&NativeTurnStop::ToolChoiceViolation),
            "native turn stop: tool_choice_violation"
        );
        assert_eq!(
            format_native_turn_stop_line(&NativeTurnStop::TimedOut),
            "native turn stop: timed_out"
        );
        // Payloads must never leak into the pure human line or Display.
        let plan = vyane_harness::native::ApprovalPlan {
            schema: 1,
            tool: "run_bash".into(),
            arguments: std::collections::BTreeMap::from([(
                "command".into(),
                serde_json::Value::String("echo secret-should-not-appear".into()),
            )]),
            cwd: "/tmp".into(),
            tool_call_id: "call".into(),
            matched_rule: None,
            canonical_plan_hash: "aa".into(),
            approval_binding_hash: "bb".into(),
        };
        let secret_stop = NativeTurnStop::ApprovalRequired(plan);
        let line = format_native_turn_stop_line(&secret_stop);
        assert_eq!(line, "native turn stop: approval_required");
        assert!(!line.contains("secret-should-not-appear"), "{line}");
        let display = format!("{secret_stop}");
        assert_eq!(display, "approval_required");
        assert!(!display.contains("secret-should-not-appear"), "{display}");
        let tool = format_stream_tool_use_line("n\name", "sum\nmary");
        assert!(!tool.contains('\u{1b}'), "{tool}");
        assert!(tool.contains("\\n"), "{tool}");
    }

    #[test]
    fn rest_task_lifecycle_operator_lines_match_contract() {
        assert_eq!(
            format_task_init_cleanup_read_failed_line("t1", "e"),
            "task t1 initialization cleanup read failed: e"
        );
        assert_eq!(
            format_task_init_cleanup_failed_line("t1", "e"),
            "task t1 initialization cleanup failed: e"
        );
        assert_eq!(
            format_task_init_cleanup_contended_line("t1"),
            "task t1 initialization cleanup remained contended"
        );
        assert_eq!(
            format_task_duplicate_runtime_dispatch_line("t1", 7),
            "task t1 epoch 7 rejected duplicate runtime dispatch"
        );
        assert_eq!(
            format_task_dispatch_failed_line("t1", "boom"),
            "task t1 failed: boom"
        );
        assert_eq!(
            format_task_dispatch_panicked_line("t1"),
            "task t1 dispatch future panicked"
        );
        assert_eq!(
            format_task_output_artifact_failed_line("t1", "e"),
            "task t1 output artifact failed: e"
        );
        let unsafe_id = "t\nid";
        let line = format_task_dispatch_failed_line(unsafe_id, "e");
        assert!(line.contains("t\\nid"), "{line}");
        assert!(!line.contains('\u{1b}'), "{line}");
    }

    #[test]
    fn rest_metadata_goal_serve_operator_lines_match_contract() {
        assert_eq!(
            format_task_metadata_settlement_retry_line("t1", "e"),
            "task t1 metadata settlement retry: e"
        );
        assert_eq!(
            format_task_metadata_error_line("boom"),
            "task metadata error: boom"
        );
        assert_eq!(
            format_dispatch_broadcast_error_line("boom"),
            "dispatch/broadcast error: boom"
        );
        assert_eq!(
            format_goal_read_unavailable_line(),
            "goal read service unavailable"
        );
        assert_eq!(
            format_goal_read_error_kind_line(vyane_service::GoalReadError::ContinuityUnavailable),
            "goal read: continuity_unavailable"
        );
        assert_eq!(
            format_goal_read_error_kind_line(vyane_service::GoalReadError::InvalidGoalId),
            "goal read: invalid_goal_id"
        );
        assert_eq!(
            format_goal_continuity_runner_error_kind_line(
                vyane_service::GoalContinuityRunnerError::InvalidGoalSet
            ),
            "continuity runner: invalid_goal_set"
        );
        assert_eq!(
            format_goal_continuity_authority_error_kind_line(
                vyane_service::GoalContinuityRunnerAuthorityError::AuthenticationFailed
            ),
            "continuity authority: authentication_failed"
        );
        assert_eq!(
            format_goal_continuity_authority_error_kind_line(
                vyane_service::GoalContinuityRunnerAuthorityError::CapabilityMismatch
            ),
            "continuity authority: capability_mismatch"
        );
        assert_eq!(
            format_goal_observation_ingress_error_kind_line(
                vyane_service::GoalObservationIngressError::InvalidSource
            ),
            "goal observation ingress: invalid_source"
        );
        assert_eq!(
            format_goal_observation_runner_error_kind_line(
                vyane_service::GoalObservationRunnerError::InvalidConfiguration
            ),
            "goal observation runner: invalid_configuration"
        );
        assert_eq!(
            format_goal_observation_runner_error_kind_line(
                vyane_service::GoalObservationRunnerError::DuplicateWatcher
            ),
            "goal observation runner: duplicate_watcher"
        );
        assert_eq!(
            format_agent_message_completion_stage_error_kind_line(
                vyane_service::AgentMessageCompletionStageError::InvalidMessage
            ),
            "completion stage: invalid_message"
        );
        assert_eq!(
            format_agent_message_completion_stage_error_kind_line(
                vyane_service::AgentMessageCompletionStageError::SinkUnavailable
            ),
            "completion stage: sink_unavailable"
        );
        assert_eq!(
            format_agent_message_completion_stage_error_kind_line(
                vyane_service::AgentMessageCompletionStageError::RuntimeUnavailable
            ),
            "completion stage: runtime_unavailable"
        );
        assert_eq!(
            format_native_permission_set_error_kind_line(
                vyane_service::NativePermissionSetError::NetworkWithoutCommand
            ),
            "native permission: network_without_command"
        );
        assert_eq!(
            format_native_permission_set_error_kind_line(
                vyane_service::NativePermissionSetError::WriteOutsideSandbox
            ),
            "native permission: write_outside_sandbox"
        );
        assert_eq!(
            format_native_permission_set_error_kind_line(
                vyane_service::NativePermissionSetError::ExceedsCeiling
            ),
            "native permission: exceeds_ceiling"
        );
        assert_eq!(
            format_native_filesystem_policy_error_kind_line(
                vyane_harness::native::NativeFilesystemPolicyError::InvalidReadPolicy
            ),
            "native filesystem policy: invalid_read_policy"
        );
        assert_eq!(
            format_native_filesystem_policy_error_kind_line(
                vyane_harness::native::NativeFilesystemPolicyError::InvalidWritePolicy
            ),
            "native filesystem policy: invalid_write_policy"
        );
        assert_eq!(
            format_native_filesystem_policy_error_kind_line(
                vyane_harness::native::NativeFilesystemPolicyError::Registry
            ),
            "native filesystem policy: registry"
        );
        assert_eq!(
            format_auth_style_line(vyane_core::AuthStyle::Bearer),
            "auth style: bearer"
        );
        assert_eq!(
            format_auth_style_line(vyane_core::AuthStyle::XApiKey),
            "auth style: x_api_key"
        );
        assert_eq!(
            format_web_search_context_size_line(vyane_core::WebSearchContextSize::High),
            "web search context: high"
        );
        assert_eq!(
            format_web_search_context_size_line(vyane_core::WebSearchContextSize::Low),
            "web search context: low"
        );
        assert_eq!(
            format_adapter_transport_line(vyane_core::AdapterTransport::DirectHttp),
            "adapter transport: direct_http"
        );
        assert_eq!(
            format_adapter_transport_line(vyane_core::AdapterTransport::CliWrap),
            "adapter transport: cli_wrap"
        );
        assert_eq!(format_effort_line(vyane_core::Effort::Low), "effort: low");
        assert_eq!(
            format_effort_line(vyane_core::Effort::Medium),
            "effort: medium"
        );
        assert_eq!(format_effort_line(vyane_core::Effort::High), "effort: high");
        assert_eq!(
            format_effort_line(vyane_core::Effort::Xhigh),
            "effort: xhigh"
        );
        assert_eq!(
            format_permission_effect_line(vyane_harness::native::PermissionEffect::Ask),
            "permission effect: ask"
        );
        assert_eq!(
            format_permission_effect_line(vyane_harness::native::PermissionEffect::Deny),
            "permission effect: deny"
        );
        assert_eq!(
            format_permission_effect_line(vyane_harness::native::PermissionEffect::Allow),
            "permission effect: allow"
        );
        assert_eq!(
            format_permission_rule_error_kind_line(
                &vyane_harness::native::PermissionRuleError::EmptyToolPattern
            ),
            "permission rule: empty_tool_pattern"
        );
        assert_eq!(
            format_permission_rule_error_kind_line(
                &vyane_harness::native::PermissionRuleError::FloorRuleMustDeny
            ),
            "permission rule: floor_rule_must_deny"
        );
        assert_eq!(
            format_permission_rule_error_kind_line(
                &vyane_harness::native::PermissionRuleError::PolicyTooLarge
            ),
            "permission rule: policy_too_large"
        );
        assert_eq!(
            format_owner_context_error_kind_line(
                vyane_service::OwnerContextError::AuthenticationFailed
            ),
            "owner context: authentication_failed"
        );
        assert_eq!(
            format_owner_context_error_kind_line(
                vyane_service::OwnerContextError::InvalidPrincipal
            ),
            "owner context: invalid_principal"
        );
        assert_eq!(
            format_owner_context_error_kind_line(vyane_service::OwnerContextError::ReservedOwner),
            "owner context: reserved_owner"
        );
        assert_eq!(
            format_tool_chat_validation_error_kind_line(
                &vyane_core::ToolChatValidationError::MessageWhileToolsPending
            ),
            "tool chat validation: message_while_tools_pending"
        );
        assert_eq!(
            format_tool_chat_validation_error_kind_line(
                &vyane_core::ToolChatValidationError::DuplicateToolCall("secret-id".into())
            ),
            "tool chat validation: duplicate_tool_call"
        );
        assert_eq!(
            format_agent_store_error_kind_line(&vyane_agent::AgentStoreError::NotFound {
                id: "secret-id".into(),
            }),
            "agent store: not_found"
        );
        assert_eq!(
            format_agent_store_error_kind_line(
                &vyane_agent::AgentStoreError::InvalidExecutionPermit {
                    id: "secret-id".into(),
                }
            ),
            "agent store: invalid_execution_permit"
        );
        assert_eq!(
            format_goal_store_error_kind_line(&vyane_goal::GoalStoreError::NotFound {
                id: "secret-goal".into(),
            }),
            "goal store: not_found"
        );
        assert_eq!(
            format_goal_store_error_kind_line(&vyane_goal::GoalStoreError::LeaseHeld {
                id: "secret-goal".into(),
                held_by: "secret-worker".into(),
            }),
            "goal store: lease_held"
        );
        assert_eq!(
            format_workflow_error_kind_line(&vyane_workflow::WorkflowError::InvalidRunId {
                value: "secret-run".into(),
            }),
            "workflow: invalid_run_id"
        );
        assert_eq!(
            format_workflow_error_kind_line(&vyane_workflow::WorkflowError::validation(vec![
                "secret-problem".into()
            ])),
            "workflow: validation"
        );
        assert_eq!(
            format_task_store_error_kind_line(&vyane_task::TaskStoreError::NotFound {
                id: "secret-task".into(),
            }),
            "task store: not_found"
        );
        assert_eq!(
            format_task_store_error_kind_line(&vyane_task::TaskStoreError::LeaseOwnerMismatch {
                id: "secret-task".into(),
                expected: "a".into(),
                actual: "b".into(),
            }),
            "task store: lease_owner_mismatch"
        );
        assert_eq!(
            format_message_store_error_kind_line(&vyane_message::MessageStoreError::NotFound),
            "message store: not_found"
        );
        assert_eq!(
            format_message_store_error_kind_line(
                &vyane_message::MessageStoreError::TransportReceiptConflict {
                    delivery_id: "secret-delivery".into(),
                }
            ),
            "message store: transport_receipt_conflict"
        );
        assert_eq!(
            format_event_log_error_kind_line(&vyane_ledger::EventLogError::CorruptRecord),
            "event log: corrupt_record"
        );
        assert_eq!(
            format_event_log_error_kind_line(&vyane_ledger::EventLogError::InvalidInput(
                "secret".into()
            )),
            "event log: invalid_input"
        );
        assert_eq!(
            format_workflow_control_error_kind_line(vyane_mcp::WorkflowControlError::NotFound),
            "workflow control: not_found"
        );
        assert_eq!(
            format_workflow_control_error_kind_line(
                vyane_mcp::WorkflowControlError::OutcomeUnknown
            ),
            "workflow control: outcome_unknown"
        );
        assert_eq!(
            format_register_command_tool_error_kind_line(
                &vyane_harness::native::RegisterCommandToolError::Command(
                    vyane_harness::native::NativeCommandPolicyError::EmptyAllowlist
                )
            ),
            "register run_command: command_policy"
        );
        assert_eq!(
            format_register_command_tool_error_kind_line(
                &vyane_harness::native::RegisterCommandToolError::Registry
            ),
            "register run_command: registry"
        );
        assert_eq!(
            format_register_web_fetch_tool_error_kind_line(
                &vyane_harness::native::RegisterWebFetchToolError::Policy(
                    vyane_harness::native::NativeWebFetchPolicyError::EmptyAllowlist
                )
            ),
            "register web_fetch: policy"
        );
        assert_eq!(
            format_register_web_fetch_tool_error_kind_line(
                &vyane_harness::native::RegisterWebFetchToolError::Registry(
                    vyane_harness::native::ToolRegistryError::EmptyName
                )
            ),
            "register web_fetch: registry"
        );
        assert_eq!(
            format_register_web_search_tool_error_kind_line(
                &vyane_harness::native::RegisterWebSearchToolError::Policy(
                    vyane_harness::native::NativeWebSearchPolicyError::EmptyAllowlist
                )
            ),
            "register web_search: policy"
        );
        assert_eq!(
            format_register_web_search_tool_error_kind_line(
                &vyane_harness::native::RegisterWebSearchToolError::Registry(
                    vyane_harness::native::ToolRegistryError::EmptyName
                )
            ),
            "register web_search: registry"
        );
        let submit_run: WorkflowRunId = "01900000-0000-7000-8000-000000000001"
            .parse()
            .expect("fixture UUIDv7");
        assert_eq!(
            format_workflow_submit_error_kind_line(
                &crate::daemon_client::WorkflowSubmitError::Rejected {
                    run_id: submit_run,
                    status: 409,
                    code: "conflict",
                }
            ),
            "workflow submit: rejected"
        );
        assert_eq!(
            format_daemon_workflow_control_error_kind_line(
                crate::daemon_client::DaemonWorkflowControlError::Unavailable
            ),
            "workflow control client: unavailable"
        );
        assert_eq!(
            format_on_error_policy_line(vyane_workflow::OnError::Abort),
            "on_error: abort"
        );
        assert_eq!(
            format_on_error_policy_line(vyane_workflow::OnError::Continue),
            "on_error: continue"
        );
        assert_eq!(
            format_error_kind_line_token(ErrorKind::RateLimited),
            "error kind: rate_limited"
        );
        assert_eq!(
            format_error_kind_line_token(ErrorKind::Io),
            "error kind: io"
        );
        assert_eq!(
            format_broker_error_kind_line(&vyane_broker::BrokerError::InvalidConfig(
                "secret-config".into()
            )),
            "broker: invalid_config"
        );
        assert_eq!(
            format_broker_error_kind_line(&vyane_broker::BrokerError::Store(
                vyane_message::MessageStoreError::NotFound
            )),
            "broker: store"
        );
        assert_eq!(
            format_quota_validation_error_kind_line(
                &vyane_quota::QuotaValidationError::InvalidIdentifier { field: "secret" }
            ),
            "quota validation: invalid_identifier"
        );
        assert_eq!(
            format_quota_runner_error_kind_line(vyane_quota::QuotaRunnerError::DuplicateConnector),
            "quota runner: duplicate_connector"
        );
        assert_eq!(
            format_quota_transport_error_kind_line(vyane_quota::QuotaTransportError::BodyTooLarge),
            "quota transport: body_too_large"
        );
        assert_eq!(
            format_pump_item_status_line(&vyane_broker::PumpItemStatus::TimedOut),
            "pump item: timed_out"
        );
        assert_eq!(
            format_pump_item_status_line(&vyane_broker::PumpItemStatus::ReplyEnqueued {
                message_id: "secret-message".into(),
            }),
            "pump item: reply_enqueued"
        );
        assert_eq!(
            format_native_side_effect_line(&vyane_core::NativeSideEffect::ModelSend {
                turn: 3,
                wire_attempt: 1,
            }),
            "native effect: model_send"
        );
        assert_eq!(
            format_native_side_effect_line(&vyane_core::NativeSideEffect::SessionCommit {
                expected_revision: 9,
            }),
            "native effect: session_commit"
        );
        assert_eq!(
            format_inprocess_agent_effect_line(vyane_service::InProcessAgentEffect::ToolOperation),
            "inprocess effect: tool_operation"
        );
        assert_eq!(
            format_goal_observation_kind_line(&vyane_service::GoalObservationKind::QuotaReset),
            "goal observation: quota_reset"
        );
        assert_eq!(
            format_goal_observation_kind_line(
                &vyane_service::GoalObservationKind::ReviewChecksPassed {
                    repository: "secret/repo".into(),
                    pull_request: 1,
                    observation_id: "secret-obs".into(),
                    observation_sequence: 2,
                }
            ),
            "goal observation: review_checks_passed"
        );
        assert_eq!(
            format_native_session_state_line(&vyane_core::NativeSessionState::Absent),
            "native session: absent"
        );
        assert_eq!(
            format_session_native_state_line(vyane_service::SessionNativeState::Absent),
            "session native: absent"
        );
        assert_eq!(
            format_session_native_state_line(vyane_service::SessionNativeState::LegacyUnbound),
            "session native: legacy_unbound"
        );
        assert_eq!(
            format_session_native_state_line(vyane_service::SessionNativeState::Bound),
            "session native: bound"
        );
        assert_eq!(
            format_session_native_state_line(vyane_service::SessionNativeState::Unknown),
            "session native: unknown"
        );
        assert_eq!(
            format_native_session_state_line(&vyane_core::NativeSessionState::LegacyUnbound {
                native_session_id: "secret-session".into(),
            }),
            "native session: legacy_unbound"
        );
        assert_eq!(
            format_nack_disposition_line(&vyane_message::NackDisposition::RetryAfter {
                delay_seconds: 30,
            }),
            "nack: retry_after"
        );
        assert_eq!(
            format_nack_disposition_line(&vyane_message::NackDisposition::Permanent {
                failure_code: "secret-code".into(),
            }),
            "nack: permanent"
        );
        assert_eq!(
            format_cancel_outcome_line(vyane_agent::CancelOutcome::Cancelled),
            "cancel: cancelled"
        );
        assert_eq!(
            format_cancel_outcome_line(vyane_agent::CancelOutcome::ControllerUnavailable),
            "cancel: controller_unavailable"
        );
        assert_eq!(
            format_adapter_outcome_line(&vyane_broker::AdapterOutcome::LocalHandled),
            "adapter outcome: local_handled"
        );
        assert_eq!(
            format_adapter_failure_line(&vyane_broker::AdapterFailure::Permanent {
                failure_code: "secret-code".into(),
            }),
            "adapter failure: permanent"
        );
        assert_eq!(
            format_attempt_outcome_line(&vyane_core::AttemptOutcome::Ok),
            "attempt: ok"
        );
        assert_eq!(
            format_attempt_outcome_line(&vyane_core::AttemptOutcome::Err {
                kind: vyane_core::ErrorKind::Other,
                message: "secret-message".into(),
                failed_over: true,
            }),
            "attempt: err"
        );
        assert_eq!(
            format_controller_recovery_observation_line(
                vyane_service::ControllerRecoveryObservation::Gone
            ),
            "controller recovery: gone"
        );
        assert_eq!(
            format_controller_recovery_observation_line(
                vyane_service::ControllerRecoveryObservation::StillPresent
            ),
            "controller recovery: still_present"
        );
        assert_eq!(
            format_controller_recovery_observation_line(
                vyane_service::ControllerRecoveryObservation::Unavailable
            ),
            "controller recovery: unavailable"
        );
        assert_eq!(
            format_agent_executor_outcome_line(&vyane_service::AgentExecutorOutcome::Unknown),
            "executor outcome: unknown"
        );
        assert_eq!(
            format_replay_safety_line(vyane_broker::ReplaySafety::Idempotent),
            "replay safety: idempotent"
        );
        assert_eq!(
            format_run_settlement_line(vyane_agent::RunSettlement::TimedOut),
            "run settlement: timed_out"
        );
        assert_eq!(
            format_task_settlement_line(&vyane_task::TaskSettlement::Succeeded {
                ledger_run_id: Some("secret-run".into()),
            }),
            "task settlement: succeeded"
        );
        assert_eq!(
            format_agent_execution_settlement_line(
                &vyane_service::AgentExecutionSettlement::TimedOut
            ),
            "execution settlement: timed_out"
        );
        assert_eq!(
            format_native_session_transition_line(&vyane_core::NativeSessionTransition::Reset {
                expected_revision: 3,
            }),
            "native session transition: reset"
        );
        assert_eq!(
            format_agent_completion_sink_observation_line(
                vyane_service::AgentCompletionSinkObservation::Exact
            ),
            "completion sink observation: exact"
        );
        assert_eq!(
            format_agent_completion_sink_transition_line(
                vyane_service::AgentCompletionSinkTransition::Complete
            ),
            "completion sink transition: complete"
        );
        assert_eq!(
            format_run_attempt_outcome_view_line(&vyane_service::RunAttemptOutcomeView::Ok),
            "run attempt: ok"
        );
        assert_eq!(
            format_agent_recovery_item_status_line(
                vyane_service::AgentRecoveryItemStatus::SettlementFailed
            ),
            "agent recovery item: settlement_failed"
        );
        assert_eq!(
            format_agent_execution_item_status_line(
                vyane_service::AgentExecutionItemStatus::SettlementFailed
            ),
            "agent execution item: settlement_failed"
        );
        assert_eq!(
            format_agent_completion_projection_status_line(
                vyane_service::AgentCompletionProjectionStatus::Published
            ),
            "agent completion projection: published"
        );
        assert_eq!(
            format_run_state_line(vyane_agent::RunState::Succeeded),
            "run state: succeeded"
        );
        assert_eq!(
            format_task_state_line(vyane_task::TaskState::Running),
            "task state: running"
        );
        assert_eq!(
            format_legacy_task_state_line(LegacyTaskState::Running),
            "legacy task state: running"
        );
        assert_eq!(
            format_legacy_task_state_line(LegacyTaskState::Died),
            "legacy task state: died"
        );
        assert_eq!(
            format_legacy_task_state_line(LegacyTaskState::Stale),
            "legacy task state: stale"
        );
        assert_eq!(
            format_legacy_task_state_line(LegacyTaskState::Success),
            "legacy task state: succeeded"
        );
        assert_eq!(
            format_goal_status_line(vyane_goal::GoalStatus::InProgress),
            "goal status: in_progress"
        );
        assert_eq!(
            format_goal_event_kind_line(vyane_goal::GoalEventKind::LeaseRenewed),
            "goal event: lease_renewed"
        );
        assert_eq!(
            format_workflow_run_status_line(
                vyane_workflow::WorkflowRunStatus::CompletedWithFailures
            ),
            "workflow run status: completed_with_failures"
        );
        assert_eq!(
            format_delivery_status_line(vyane_message::DeliveryStatus::DeadLettered),
            "delivery status: dead_lettered"
        );
        assert_eq!(
            format_message_publication_status_line(
                vyane_message::MessagePublicationStatus::Published
            ),
            "message publication: published"
        );
        assert_eq!(
            format_run_completion_status_line(vyane_agent::RunCompletionStatus::Prepared),
            "run completion: prepared"
        );
        assert_eq!(
            format_pursuit_status_line(vyane_goal::PursuitStatus::Achieved),
            "pursuit status: achieved"
        );
        assert_eq!(
            format_criterion_status_line(vyane_goal::CriterionStatus::Satisfied),
            "criterion status: satisfied"
        );
        assert_eq!(
            format_goal_continuity_status_line(vyane_goal::GoalContinuityStatus::TakeoverReady),
            "goal continuity status: takeover_ready"
        );
        assert_eq!(
            format_takeover_approval_status_line(vyane_goal::TakeoverApprovalStatus::Pending),
            "takeover approval: pending"
        );
        assert_eq!(
            format_journal_step_status_line(vyane_workflow::JournalStepStatus::Skipped),
            "journal step: skipped"
        );
        assert_eq!(
            format_workflow_state_line(vyane_mcp::WorkflowState::Cancelling),
            "workflow state: cancelling"
        );
        assert_eq!(
            format_goal_continuity_step_status_line(
                vyane_goal::GoalContinuityStepStatus::WaitingForTakeover
            ),
            "goal continuity step: waiting_for_takeover"
        );
        assert_eq!(
            format_message_event_kind_line(vyane_message::MessageEventKind::DeadLettered),
            "message event: dead_lettered"
        );
        assert_eq!(
            format_run_failure_code_line(vyane_agent::RunFailureCode::DispatchFailed),
            "run failure: dispatch_failed"
        );
        assert_eq!(
            format_run_failure_code_line(vyane_agent::RunFailureCode::PolicyDenied),
            "run failure: policy_denied"
        );
        assert_eq!(
            format_run_failure_code_line(vyane_agent::RunFailureCode::TimedOut),
            "run failure: timed_out"
        );
        assert_eq!(
            format_task_failure_code_line(FailureCode::DispatchFailed),
            "task failure: dispatch_failed"
        );
        assert_eq!(
            format_task_failure_code_line(FailureCode::WorkerLost),
            "task failure: worker_lost"
        );
        assert_eq!(
            format_task_failure_code_line(FailureCode::LeaseExpired),
            "task failure: lease_expired"
        );
        assert_eq!(
            format_task_failure_code_line(FailureCode::Configuration),
            "task failure: configuration"
        );
        assert_eq!(
            format_task_failure_code_line(FailureCode::Internal),
            "task failure: internal"
        );
        assert_eq!(
            format_execution_backend_line(vyane_agent::ExecutionBackend::CliHarnessProcess),
            "execution backend: cli_harness_process"
        );
        assert_eq!(
            format_execution_backend_line(vyane_agent::ExecutionBackend::NativeInProcess),
            "execution backend: native_in_process"
        );
        assert_eq!(
            format_execution_backend_line(vyane_agent::ExecutionBackend::Remote),
            "execution backend: remote"
        );
        assert_eq!(
            format_execution_backend_line(vyane_agent::ExecutionBackend::LegacyUnassigned),
            "execution backend: legacy_unassigned"
        );
        assert_eq!(
            format_run_mode_line(vyane_agent::RunMode::Autonomous),
            "run mode: autonomous"
        );
        assert_eq!(
            format_run_mode_line(vyane_agent::RunMode::Interactive),
            "run mode: interactive"
        );
        assert_eq!(
            format_controller_kind_line(vyane_agent::ControllerKind::InProcess),
            "controller kind: in_process"
        );
        assert_eq!(
            format_controller_kind_line(vyane_agent::ControllerKind::Process),
            "controller kind: process"
        );
        assert_eq!(
            format_controller_kind_line(vyane_agent::ControllerKind::Remote),
            "controller kind: remote"
        );
        assert_eq!(
            format_task_origin_line(TaskOrigin::RestAsync),
            "task origin: rest_async"
        );
        assert_eq!(
            format_task_origin_line(TaskOrigin::CliDetached),
            "task origin: cli_detached"
        );
        assert_eq!(
            format_task_origin_line(TaskOrigin::Daemon),
            "task origin: daemon"
        );
        assert_eq!(
            format_task_kind_line(TaskKind::Dispatch),
            "task kind: dispatch"
        );
        assert_eq!(
            format_task_kind_line(TaskKind::Workflow),
            "task kind: workflow"
        );
        assert_eq!(
            format_run_status_line(vyane_core::RunStatus::Timeout),
            "run status: timeout"
        );
        assert_eq!(
            format_goal_continuity_mode_line(vyane_goal::GoalContinuityMode::QuotaHandoff),
            "goal continuity mode: quota_handoff"
        );
        assert_eq!(
            format_goal_continuity_next_action_kind_line(
                vyane_goal::GoalContinuityNextActionKind::DecideApproval
            ),
            "goal continuity next action: decide_approval"
        );
        assert_eq!(
            format_goal_continuity_signal_kind_line(
                vyane_goal::GoalContinuitySignalKind::QuotaReset
            ),
            "goal continuity signal: quota_reset"
        );
        assert_eq!(
            format_goal_continuity_operator_command_line(
                vyane_goal::GoalContinuityOperatorCommand::ContinuityDecide
            ),
            "goal continuity command: continuity_decide"
        );
        assert_eq!(
            format_takeover_decision_line(vyane_goal::TakeoverDecision::Reject),
            "takeover decision: reject"
        );
        assert_eq!(
            format_takeover_sandbox_line(vyane_goal::TakeoverSandbox::ReadOnly),
            "takeover sandbox: read_only"
        );
        assert_eq!(
            format_sandbox_line(vyane_core::Sandbox::ReadOnly),
            "sandbox: read-only"
        );
        assert_eq!(
            format_takeover_run_status_line(vyane_goal::TakeoverRunStatus::Timeout),
            "takeover run: timeout"
        );
        assert_eq!(
            format_pursuit_checkpoint_status_line(vyane_goal::PursuitCheckpointStatus::Paused),
            "pursuit checkpoint: paused"
        );
        assert_eq!(
            format_pursuit_segment_status_line(vyane_goal::PursuitSegmentStatus::Cancelled),
            "pursuit segment: cancelled"
        );
        assert_eq!(
            format_goal_observation_signal_kind_line(
                vyane_service::GoalObservationSignalKind::ReviewChecksFailed
            ),
            "goal observation signal: review_checks_failed"
        );
        assert_eq!(
            format_goal_observation_status_line(vyane_service::GoalObservationStatus::Unchanged),
            "goal observation status: unchanged"
        );
        assert_eq!(
            format_goal_observation_watch_status_line(
                vyane_service::GoalObservationWatchStatus::InvalidBatch
            ),
            "goal observation watch: invalid_batch"
        );
        assert_eq!(
            format_goal_observation_watcher_error_code_line(
                vyane_service::GoalObservationWatcherErrorCode::RateLimited
            ),
            "goal observation watcher error: rate_limited"
        );
        assert_eq!(
            format_config_check_status_line(vyane_service::ConfigCheckStatus::Partial),
            "config check: partial"
        );
        assert_eq!(
            format_credential_status_line(vyane_service::CredentialStatus::Missing),
            "credential: missing"
        );
        assert_eq!(
            format_profile_check_status_line(vyane_service::ProfileCheckStatus::Unresolvable),
            "profile check: unresolvable"
        );
        assert_eq!(
            format_native_permission_axis_status_line(
                vyane_service::NativePermissionAxisStatus::UnrestrictedByConfig
            ),
            "native permission axis: unrestricted_by_config"
        );
        assert_eq!(
            format_config_issue_code_line(vyane_service::ConfigIssueCode::ProfileUnresolvable),
            "config issue: profile_unresolvable"
        );
        assert_eq!(
            format_goal_read_worker_failed_line("e"),
            "goal read worker failed: e"
        );
        assert_eq!(
            format_task_output_read_failed_line("t1", "e"),
            "task t1 output read failed: e"
        );
        assert_eq!(
            format_task_legacy_output_read_failed_line("t1", "e"),
            "task t1 legacy output read failed: e"
        );
        assert_eq!(
            format_serve_listening_line("127.0.0.1:9", "/tmp/token"),
            "vyane serve listening on 127.0.0.1:9; bearer token file: /tmp/token"
        );
        let unsafe_id = "t\nid";
        let line = format_task_output_read_failed_line(unsafe_id, "e");
        assert!(line.contains("t\\nid"), "{line}");
        assert!(!line.contains('\u{1b}'), "{line}");
    }

    #[test]
    fn rest_label_stream_query_operator_lines_match_contract() {
        assert_eq!(
            format_dispatch_label_error_line("e"),
            "dispatch label error: e"
        );
        assert_eq!(
            format_stream_dispatch_label_error_line("e"),
            "stream dispatch label error: e"
        );
        assert_eq!(
            format_stream_dispatch_request_error_line("e"),
            "stream dispatch request error: e"
        );
        assert_eq!(format_stream_route_error_line("e"), "stream route error: e");
        assert_eq!(
            format_dispatch_stream_error_line("e"),
            "dispatch_stream error: e"
        );
        assert_eq!(
            format_external_task_label_error_line("e"),
            "external task label error: e"
        );
        assert_eq!(
            format_broadcast_label_error_line("e"),
            "broadcast label error: e"
        );
        assert_eq!(
            format_broadcast_setup_error_line("e"),
            "broadcast setup error: e"
        );
        assert_eq!(
            format_broadcast_target_error_line("t1", "e"),
            "broadcast target `t1` error: e"
        );
        assert_eq!(
            format_run_ledger_query_error_line("e"),
            "run ledger query error: e"
        );
        assert_eq!(
            format_session_snapshot_query_error_line("e"),
            "session snapshot query error: e"
        );
        assert_eq!(
            format_async_dispatch_label_error_line("e"),
            "async dispatch label error: e"
        );
        let unsafe_target = format_broadcast_target_error_line("t\n1", "e");
        assert!(unsafe_target.contains("t\\n1"), "{unsafe_target}");
        assert!(!unsafe_target.contains('\u{1b}'), "{unsafe_target}");
    }

    #[test]
    fn human_workflow_cancel_line_prints_workflow_prefix() {
        let text = format_workflow_cancel_line("wf-9", TaskState::Cancelled);
        assert_eq!(text, "workflow wf-9 cancelled", "{text}");
    }

    #[test]
    fn human_check_config_files_marks_loaded_and_missing() {
        let text = format_check_config_files(&[
            ("/tmp/a.toml".into(), true),
            ("/tmp/b\n.toml".into(), false),
        ]);
        assert!(text.starts_with("config files:\n"), "{text}");
        assert!(text.contains("loaded"), "{text}");
        assert!(text.contains("missing"), "{text}");
        assert!(
            !text.contains('\n') || text.contains("\\n") || text.lines().count() >= 3,
            "{text}"
        );
    }

    #[test]
    fn human_check_section_headers_match_contract() {
        assert_eq!(format_check_providers_header(), "providers:");
        assert_eq!(format_check_profiles_header(), "profiles:");
        assert_eq!(format_check_harnesses_header(), "harnesses:");
        assert_eq!(
            format_check_profile_environment_header(),
            "profile environment:"
        );
    }

    #[test]
    fn human_check_profile_warning_and_run_failure_match_contract() {
        assert_eq!(
            format_check_profile_warning_body("no eligible target"),
            "warning: no eligible target"
        );
        // Dispatch run-failure stderr intentionally has no `error:` prefix.
        assert_eq!(
            format_run_failure_line("upstream timeout"),
            "upstream timeout"
        );
        assert!(!format_run_failure_line("upstream timeout").starts_with("error:"));
    }

    #[test]
    fn human_check_provider_and_profile_lines_are_terminal_safe() {
        let provider = format_check_provider_line("openai\n", Protocol::OpenaiChat, Some("gpt-x"));
        assert!(provider.contains("openai_chat"), "{provider}");
        assert!(provider.contains("default_model=gpt-x"), "{provider}");
        assert!(!provider.contains('\u{1b}'));
        assert_eq!(
            format_protocol_line(Protocol::AnthropicMessages),
            "protocol: anthropic_messages"
        );
        let profile = format_check_profile_line("coding", "p1 -> p2");
        assert_eq!(profile, "  coding: p1 -> p2\n");
        let harness = format_check_harness_line(&HarnessKind::ClaudeCode, true);
        assert!(harness.contains("claude-code"), "{harness}");
        assert!(harness.contains("available"), "{harness}");
        assert_eq!(
            format_harness_kind_line(&HarnessKind::CodexCli),
            "harness: codex-cli"
        );
        let env = format_check_profile_env_line("coding", "OPENAI_API_KEY", false);
        assert!(env.contains("missing"), "{env}");
    }

    /// Process AgentRun lifecycle pure line is Linux-only (`agent_host` cfg).
    #[cfg(target_os = "linux")]
    #[test]
    fn pure_lifecycle_observation_line_matches_kind_tokens() {
        use super::format_lifecycle_observation_line;
        use crate::agent_host::LifecycleObservation;

        assert_eq!(
            format_lifecycle_observation_line(LifecycleObservation::NeverStarted),
            "lifecycle: never_started"
        );
        assert_eq!(
            format_lifecycle_observation_line(LifecycleObservation::Running),
            "lifecycle: running"
        );
        assert_eq!(
            format_lifecycle_observation_line(LifecycleObservation::Stopped { cycles: 9 }),
            "lifecycle: stopped"
        );
        assert_eq!(
            format_lifecycle_observation_line(LifecycleObservation::Uncertain),
            "lifecycle: uncertain"
        );
    }
}
