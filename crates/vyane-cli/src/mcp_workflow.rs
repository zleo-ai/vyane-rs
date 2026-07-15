//! Authenticated adapter from the public MCP workflow port to the resident daemon.

use std::path::PathBuf;

use vyane_mcp::{
    WorkflowControl, WorkflowControlError, WorkflowControlFuture, WorkflowFailureCode,
    WorkflowState, WorkflowSubmitRequest, WorkflowView,
};
use vyane_task::{FailureCode, TaskRecord, TaskState};
use vyane_workflow::{WorkflowRunId, WorkflowSourceBundle};

use crate::daemon_client::{
    DaemonWorkflowClient, DaemonWorkflowControlError, WorkflowSubmitError, WorkflowTaskView,
};

/// A narrow control adapter. It owns no token or descriptor: every operation
/// re-authenticates the exact resident daemon through the verified local
/// control files. The execution directory is frozen when the MCP server starts
/// and cannot be supplied by an MCP caller.
pub(crate) struct AuthenticatedWorkflowControl {
    execution_cwd: PathBuf,
}

impl AuthenticatedWorkflowControl {
    pub(crate) fn new(execution_cwd: PathBuf) -> Self {
        Self { execution_cwd }
    }
}

impl WorkflowControl for AuthenticatedWorkflowControl {
    fn submit(
        &self,
        request: WorkflowSubmitRequest,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>> {
        let execution_cwd = self.execution_cwd.clone();
        Box::pin(async move {
            validate_submission_policy(&request.bundle)?;
            let client = DaemonWorkflowClient::connect()
                .await
                .map_err(|_| WorkflowControlError::Unavailable)?;
            let caller_id = request.caller_id.clone();
            let view = client
                .submit(
                    &request.caller_id,
                    execution_cwd,
                    request.bundle,
                    request.vars,
                )
                .await
                .map_err(map_submit_error)?;
            map_task_view(&caller_id, view)
        })
    }

    fn status(
        &self,
        caller_id: WorkflowRunId,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>> {
        Box::pin(async move {
            let client = DaemonWorkflowClient::connect()
                .await
                .map_err(|_| WorkflowControlError::Unavailable)?;
            let view = client
                .status_for_control(&caller_id)
                .await
                .map_err(map_control_error)?;
            map_task_view(&caller_id, view)
        })
    }

    fn cancel(
        &self,
        caller_id: WorkflowRunId,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>> {
        Box::pin(async move {
            let client = DaemonWorkflowClient::connect()
                .await
                .map_err(|_| WorkflowControlError::Unavailable)?;
            let task = client
                .cancel_for_control(&caller_id)
                .await
                .map_err(map_control_error)?;
            map_task_record(&caller_id, &task)
        })
    }
}

fn validate_submission_policy(bundle: &WorkflowSourceBundle) -> Result<(), WorkflowControlError> {
    let workflow = bundle
        .materialize()
        .map_err(|_| WorkflowControlError::InvalidRequest)?;
    if workflow
        .steps
        .iter()
        .any(|step| step.workdir.is_some() || step.sandbox != vyane_core::Sandbox::ReadOnly)
    {
        return Err(WorkflowControlError::InvalidRequest);
    }
    Ok(())
}

fn map_submit_error(error: WorkflowSubmitError) -> WorkflowControlError {
    match error {
        WorkflowSubmitError::NotSubmitted { .. } => WorkflowControlError::Unavailable,
        WorkflowSubmitError::OutcomeUnknown { .. } => WorkflowControlError::OutcomeUnknown,
        WorkflowSubmitError::Rejected {
            status: 401 | 403 | 429,
            ..
        } => WorkflowControlError::Unavailable,
        WorkflowSubmitError::Rejected { code, .. } => match code {
            "invalid_request" => WorkflowControlError::InvalidRequest,
            "not_found" => WorkflowControlError::NotFound,
            "conflict" => WorkflowControlError::Conflict,
            _ => WorkflowControlError::Unavailable,
        },
    }
}

fn map_control_error(error: DaemonWorkflowControlError) -> WorkflowControlError {
    match error {
        DaemonWorkflowControlError::InvalidRequest => WorkflowControlError::InvalidRequest,
        DaemonWorkflowControlError::NotFound => WorkflowControlError::NotFound,
        DaemonWorkflowControlError::Conflict => WorkflowControlError::Conflict,
        DaemonWorkflowControlError::Unavailable => WorkflowControlError::Unavailable,
        DaemonWorkflowControlError::Internal => WorkflowControlError::Internal,
    }
}

fn map_task_view(
    caller_id: &WorkflowRunId,
    view: WorkflowTaskView,
) -> Result<WorkflowView, WorkflowControlError> {
    map_task_record(caller_id, &view.task)
}

fn map_task_record(
    caller_id: &WorkflowRunId,
    task: &TaskRecord,
) -> Result<WorkflowView, WorkflowControlError> {
    if task.id != caller_id.as_str() {
        return Err(WorkflowControlError::Internal);
    }
    Ok(WorkflowView {
        caller_id: caller_id.clone(),
        state: match task.state {
            TaskState::Queued => WorkflowState::Queued,
            TaskState::Running => WorkflowState::Running,
            TaskState::Cancelling => WorkflowState::Cancelling,
            TaskState::Succeeded => WorkflowState::Succeeded,
            TaskState::Failed => WorkflowState::Failed,
            TaskState::TimedOut => WorkflowState::TimedOut,
            TaskState::Cancelled => WorkflowState::Cancelled,
            TaskState::Interrupted => WorkflowState::Interrupted,
        },
        failure_code: task.failure_code.map(|code| match code {
            FailureCode::DispatchFailed => WorkflowFailureCode::DispatchFailed,
            FailureCode::SpawnFailed => WorkflowFailureCode::SpawnFailed,
            FailureCode::Configuration => WorkflowFailureCode::Configuration,
            FailureCode::ControlUnavailable => WorkflowFailureCode::ControlUnavailable,
            FailureCode::WorkerLost => WorkflowFailureCode::WorkerLost,
            FailureCode::LeaseExpired => WorkflowFailureCode::LeaseExpired,
            FailureCode::Cancelled => WorkflowFailureCode::Cancelled,
            FailureCode::TimedOut => WorkflowFailureCode::TimedOut,
            FailureCode::Internal => WorkflowFailureCode::Internal,
        }),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use chrono::Utc;
    use vyane_task::{TaskKind, TaskOrigin};

    use super::*;

    fn bundle(extra: &str) -> WorkflowSourceBundle {
        WorkflowSourceBundle {
            workflow_toml: format!(
                r#"[workflow]
name = "policy"

[[step]]
id = "one"
target = "safe"
prompt = "hello"
{extra}
"#
            ),
            prompt_files: Vec::new(),
        }
    }

    #[test]
    fn production_policy_rejects_caller_workdirs_and_elevated_sandbox() {
        assert_eq!(
            validate_submission_policy(&bundle(r#"workdir = "/outside""#)),
            Err(WorkflowControlError::InvalidRequest)
        );
        assert_eq!(
            validate_submission_policy(&bundle(r#"workdir = "../../outside""#)),
            Err(WorkflowControlError::InvalidRequest)
        );
        assert_eq!(
            validate_submission_policy(&bundle(r#"sandbox = "full""#)),
            Err(WorkflowControlError::InvalidRequest)
        );
        assert!(validate_submission_policy(&bundle("")).is_ok());
    }

    #[test]
    fn projection_rejects_id_drift_and_never_exports_durable_authority() {
        let caller_id = WorkflowRunId::generate();
        let task = TaskRecord {
            id: WorkflowRunId::generate().to_string(),
            owner: "private-owner".to_string(),
            kind: TaskKind::Workflow,
            origin: TaskOrigin::Daemon,
            state: TaskState::Running,
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
        };
        assert_eq!(
            map_task_record(&caller_id, &task),
            Err(WorkflowControlError::Internal)
        );
    }

    #[test]
    fn submit_error_mapping_preserves_unknown_outcome_and_auth_drift() {
        let run_id = WorkflowRunId::generate();
        assert_eq!(
            map_submit_error(WorkflowSubmitError::OutcomeUnknown {
                run_id: run_id.clone(),
                status: None,
                reason: "bounded test reason",
            }),
            WorkflowControlError::OutcomeUnknown
        );
        assert_eq!(
            map_submit_error(WorkflowSubmitError::Rejected {
                run_id,
                status: 401,
                code: "http_rejection",
            }),
            WorkflowControlError::Unavailable
        );
    }
}
