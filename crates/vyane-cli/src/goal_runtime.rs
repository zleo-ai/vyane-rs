use std::sync::Arc;

use async_trait::async_trait;
use vyane_core::{CancellationToken, RunStatus, Sandbox};
use vyane_goal::{
    GoalSegmentRuntime, PursuitSegmentRequest, PursuitSegmentResult, PursuitSegmentStatus,
};
use vyane_service::{DispatchParams, VyaneService};

/// Runtime-neutral goal segment adapter shared by the manual CLI and resident daemon.
pub(crate) struct DispatchGoalRuntime {
    service: Arc<VyaneService>,
    target: String,
    sandbox: Sandbox,
}

impl DispatchGoalRuntime {
    pub(crate) fn new(
        service: Arc<VyaneService>,
        target: String,
        sandbox: Sandbox,
    ) -> Self {
        Self {
            service,
            target,
            sandbox,
        }
    }
}

#[async_trait]
impl GoalSegmentRuntime for DispatchGoalRuntime {
    async fn run_segment(
        &self,
        request: PursuitSegmentRequest,
        cancel: CancellationToken,
    ) -> PursuitSegmentResult {
        let outcome = self
            .service
            .dispatch(
                DispatchParams {
                    task: request.prompt,
                    target: self.target.clone(),
                    workdir: Some(request.workdir),
                    sandbox: self.sandbox,
                    session: None,
                    system: None,
                    timeout_secs: Some(request.timeout.as_secs().max(1)),
                    labels: Vec::new(),
                },
                cancel,
            )
            .await;
        match outcome {
            Ok(outcome) => PursuitSegmentResult {
                status: pursuit_segment_status(outcome.record.status),
                run_id: Some(outcome.record.run_id),
            },
            Err(_) => PursuitSegmentResult {
                status: PursuitSegmentStatus::Error,
                run_id: None,
            },
        }
    }
}

pub(crate) const fn pursuit_segment_status(status: RunStatus) -> PursuitSegmentStatus {
    match status {
        RunStatus::Success => PursuitSegmentStatus::Success,
        RunStatus::Timeout => PursuitSegmentStatus::Timeout,
        RunStatus::Cancelled => PursuitSegmentStatus::Cancelled,
        RunStatus::Error => PursuitSegmentStatus::Error,
    }
}
