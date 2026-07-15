//! Authenticated adapter from the public MCP workflow port to the resident daemon.

use std::path::PathBuf;

use vyane_mcp::{
    WorkflowControl, WorkflowControlError, WorkflowControlFuture, WorkflowFailureCode,
    WorkflowState, WorkflowSubmitRequest, WorkflowView,
};
use vyane_task::{FailureCode, TaskRecord, TaskState};
use vyane_workflow::WorkflowRunId;

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
