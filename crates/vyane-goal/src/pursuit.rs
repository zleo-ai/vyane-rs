//! Bounded manual goal pursuit.
//!
//! The pursuer owns the verify -> fresh segment -> reverify loop. Runtime
//! execution is injected; lifecycle truth, verification evidence, criterion
//! satisfaction, progress, pause and completion remain in [`crate::GoalStore`].

use std::path::PathBuf;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::model::validate_worker;
use crate::{
    AcceptanceVerification, AcceptanceVerifier, CriterionStatus, GoalRecord, GoalStatus, GoalStore,
    GoalStoreError, MAX_LEASE_SECONDS, Result,
};

pub const MAX_PURSUIT_SEGMENTS: u16 = 64;
pub const MAX_PURSUIT_FAILURES: u16 = 16;
pub const MAX_PURSUIT_TIMEOUT: Duration = Duration::from_secs(86_400);
pub const MAX_SEGMENT_TIMEOUT: Duration = Duration::from_secs(3_600);

#[derive(Debug, Clone)]
pub struct PursuitConfig {
    pub workdir: PathBuf,
    pub runtime: String,
    pub worker_id: String,
    pub overall_timeout: Duration,
    pub segment_timeout: Duration,
    pub max_segments: u16,
    pub max_failures: u16,
}

impl PursuitConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.workdir.is_absolute() {
            return Err(GoalStoreError::InvalidInput(
                "pursuit workdir must be an existing absolute directory".into(),
            ));
        }
        if self.workdir.to_str().is_none() {
            return Err(GoalStoreError::InvalidInput(
                "pursuit workdir must be valid UTF-8 for durable checkpoints".into(),
            ));
        }
        if !self.workdir.is_dir() {
            return Err(GoalStoreError::InvalidInput(
                "pursuit workdir must be an existing absolute directory".into(),
            ));
        }
        if self.runtime.trim().is_empty() || self.runtime.len() > 256 {
            return Err(GoalStoreError::InvalidInput(
                "pursuit runtime must be between 1 and 256 bytes".into(),
            ));
        }
        validate_worker(&self.worker_id)?;
        if self.overall_timeout.is_zero() || self.overall_timeout > MAX_PURSUIT_TIMEOUT {
            return Err(GoalStoreError::InvalidInput(
                "pursuit overall timeout must be between 1s and 24h".into(),
            ));
        }
        if self.segment_timeout.is_zero() || self.segment_timeout > MAX_SEGMENT_TIMEOUT {
            return Err(GoalStoreError::InvalidInput(
                "pursuit segment timeout must be between 1s and 1h".into(),
            ));
        }
        if self.max_segments == 0 || self.max_segments > MAX_PURSUIT_SEGMENTS {
            return Err(GoalStoreError::InvalidInput(format!(
                "pursuit max segments must be between 1 and {MAX_PURSUIT_SEGMENTS}"
            )));
        }
        if self.max_failures == 0 || self.max_failures > MAX_PURSUIT_FAILURES {
            return Err(GoalStoreError::InvalidInput(format!(
                "pursuit max failures must be between 1 and {MAX_PURSUIT_FAILURES}"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PursuitSegmentRequest {
    pub goal_id: String,
    pub segment_index: u16,
    pub prompt: String,
    pub workdir: PathBuf,
    pub timeout: Duration,
    pub runtime: String,
    pub verification: AcceptanceVerification,
}

/// Runtime-neutral segment result. This intentionally does not reuse a
/// concrete dispatcher's status type so `vyane-goal` remains runtime-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PursuitSegmentStatus {
    Success,
    Error,
    Timeout,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct PursuitSegmentResult {
    pub status: PursuitSegmentStatus,
    pub run_id: Option<String>,
}

#[async_trait]
pub trait GoalSegmentRuntime: Send + Sync {
    /// The runtime must observe `cancel`, finish its cancellation bookkeeping,
    /// and return [`PursuitSegmentStatus::Cancelled`]. The returned future must
    /// also be cancellation-safe when the pursuer drops it at the segment or
    /// overall deadline.
    async fn run_segment(
        &self,
        request: PursuitSegmentRequest,
        cancel: CancellationToken,
    ) -> PursuitSegmentResult;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PursuitStatus {
    Achieved,
    Paused,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PursuitCheckpointStatus {
    Running,
    Paused,
    Achieved,
}

/// Mutable restart checkpoint for one explicit pursuit. Store writes are CAS
/// fenced by both `checkpoint_revision` and the goal's lease generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalPursuitCheckpoint {
    pub owner: String,
    pub goal_id: String,
    pub checkpoint_revision: u64,
    pub goal_revision: u64,
    pub claim_generation: u64,
    pub worker_id: String,
    pub runtime: String,
    pub workdir: PathBuf,
    pub started_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    pub segments_started: u16,
    pub segments_completed: u16,
    pub consecutive_failures: u16,
    pub status: PursuitCheckpointStatus,
    pub last_run_id: Option<String>,
    pub last_verification_id: Option<String>,
}

impl GoalPursuitCheckpoint {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::model::validate_owner(&self.owner)?;
        crate::model::validate_goal_id(&self.goal_id)?;
        validate_worker(&self.worker_id)?;
        if self.runtime.trim().is_empty() || self.runtime.len() > 256 {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint runtime must be between 1 and 256 bytes".into(),
            ));
        }
        if !checkpoint_workdir_is_absolute(&self.workdir) {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint workdir must be absolute".into(),
            ));
        }
        if self.updated_at < self.started_at {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint timestamps are out of order".into(),
            ));
        }
        if self.segments_completed > self.segments_started {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint completed segments exceed started segments".into(),
            ));
        }
        validate_optional_run_id(self.last_run_id.as_deref())?;
        validate_optional_checkpoint_id(
            "pursuit checkpoint verification id",
            self.last_verification_id.as_deref(),
        )
    }
}

fn checkpoint_workdir_is_absolute(path: &std::path::Path) -> bool {
    if path.is_absolute() {
        return true;
    }
    let Some(path) = path.to_str() else {
        return false;
    };
    let bytes = path.as_bytes();
    path.starts_with('/')
        || path.starts_with("\\\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'))
}

fn validate_optional_run_id(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        if value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint run id is invalid".into(),
            ));
        }
    }
    Ok(())
}

fn validate_optional_checkpoint_id(field: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(GoalStoreError::InvalidInput(format!("{field} is invalid")));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PursuitOutcome {
    pub goal_id: String,
    pub status: PursuitStatus,
    pub final_goal_status: GoalStatus,
    pub segments_started: u16,
    pub segments_completed: u16,
    pub consecutive_failures: u16,
    pub summary: String,
    pub reason: String,
    pub last_verification: Option<AcceptanceVerification>,
}

pub struct GoalPursuer<'a, S, R> {
    store: &'a S,
    runtime: &'a R,
    verifier: &'a AcceptanceVerifier,
    config: PursuitConfig,
}

#[derive(Debug, Clone, Copy)]
enum CancellationDisposition {
    Pause,
    PreserveRunning,
}

impl<'a, S, R> GoalPursuer<'a, S, R>
where
    S: GoalStore,
    R: GoalSegmentRuntime,
{
    pub fn new(
        store: &'a S,
        runtime: &'a R,
        verifier: &'a AcceptanceVerifier,
        config: PursuitConfig,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            store,
            runtime,
            verifier,
            config,
        })
    }

    pub async fn pursue(&self, owner: &str, goal_id: &str) -> Result<PursuitOutcome> {
        self.pursue_with_cancel(owner, goal_id, CancellationToken::new())
            .await
    }

    pub async fn pursue_with_cancel(
        &self,
        owner: &str,
        goal_id: &str,
        cancel: CancellationToken,
    ) -> Result<PursuitOutcome> {
        self.pursue_with_cancellation_disposition(
            owner,
            goal_id,
            cancel,
            CancellationDisposition::Pause,
        )
        .await
    }

    /// Cancel in-flight verifier and runtime work, but keep the durable
    /// checkpoint running so a resident supervisor replacement may resume it.
    pub async fn pursue_with_cancel_preserving_checkpoint(
        &self,
        owner: &str,
        goal_id: &str,
        cancel: CancellationToken,
    ) -> Result<PursuitOutcome> {
        self.pursue_with_cancellation_disposition(
            owner,
            goal_id,
            cancel,
            CancellationDisposition::PreserveRunning,
        )
        .await
    }

    async fn pursue_with_cancellation_disposition(
        &self,
        owner: &str,
        goal_id: &str,
        cancel: CancellationToken,
        cancellation: CancellationDisposition,
    ) -> Result<PursuitOutcome> {
        let goal = self.require_running(owner, goal_id)?;
        let now = chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
            .expect("current UTC timestamp is representable");
        let mut checkpoint = self
            .store
            .pursuit_checkpoint(owner, goal_id)?
            .unwrap_or_else(|| GoalPursuitCheckpoint {
                owner: owner.to_string(),
                goal_id: goal_id.to_string(),
                checkpoint_revision: 0,
                goal_revision: goal.revision,
                claim_generation: goal.claim_generation,
                worker_id: self.config.worker_id.clone(),
                runtime: self.config.runtime.clone(),
                workdir: self.config.workdir.clone(),
                started_at: now,
                updated_at: now,
                segments_started: 0,
                segments_completed: 0,
                consecutive_failures: 0,
                status: PursuitCheckpointStatus::Running,
                last_run_id: None,
                last_verification_id: None,
            });
        checkpoint.status = PursuitCheckpointStatus::Running;
        checkpoint.goal_revision = goal.revision;
        checkpoint.claim_generation = goal.claim_generation;
        checkpoint.worker_id.clone_from(&self.config.worker_id);
        checkpoint.runtime.clone_from(&self.config.runtime);
        checkpoint.workdir.clone_from(&self.config.workdir);
        self.record_checkpoint(
            owner,
            goal_id,
            &mut checkpoint,
            "pursuit.started",
            "pursuit started or resumed",
        )?;
        let claim_generation = checkpoint.claim_generation;
        let started = Instant::now();
        let mut previous_verification = None;
        loop {
            let Some(remaining) = self.config.overall_timeout.checked_sub(started.elapsed()) else {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit overall timeout",
                    previous_verification,
                );
            };
            if remaining.is_zero() {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit overall timeout",
                    previous_verification,
                );
            }
            let latest =
                self.store
                    .get(owner, goal_id)?
                    .ok_or_else(|| GoalStoreError::NotFound {
                        id: goal_id.to_string(),
                    })?;
            if let Some(stopped) = self.stopped_for_record(
                &latest,
                checkpoint.segments_started,
                checkpoint.segments_completed,
                checkpoint.consecutive_failures,
                previous_verification.clone(),
                claim_generation,
            ) {
                return Ok(stopped);
            };
            let goal = latest;
            if cancel.is_cancelled() {
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    previous_verification,
                    cancellation,
                );
            }
            self.store.renew_lease(
                owner,
                goal_id,
                &self.config.worker_id,
                remaining
                    .as_secs()
                    .saturating_add(1)
                    .clamp(1, MAX_LEASE_SECONDS),
                Utc::now(),
            )?;
            let verifier = self.verifier.clone();
            let verification_goal = goal.clone();
            let verification_cancel = cancel.child_token();
            let verification = tokio::task::spawn_blocking(move || {
                verifier.verify_with_budget_and_cancel(
                    &verification_goal,
                    remaining,
                    &verification_cancel,
                )
            })
            .await
            .map_err(|_| {
                GoalStoreError::InvalidInput("acceptance verifier worker failed".into())
            })?;
            let Some(verification) = verification else {
                if let Some(stopped) = self.stopped_if_drifted(
                    owner,
                    goal_id,
                    checkpoint.segments_started,
                    checkpoint.segments_completed,
                    checkpoint.consecutive_failures,
                    previous_verification.clone(),
                    claim_generation,
                )? {
                    return Ok(stopped);
                }
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    previous_verification,
                    cancellation,
                );
            };
            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                checkpoint.segments_started,
                checkpoint.segments_completed,
                checkpoint.consecutive_failures,
                previous_verification.clone(),
                claim_generation,
            )? {
                return Ok(stopped);
            }
            if cancel.is_cancelled() {
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    previous_verification,
                    cancellation,
                );
            }
            let verified_at = Utc::now();
            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                checkpoint.segments_started,
                checkpoint.segments_completed,
                checkpoint.consecutive_failures,
                previous_verification.clone(),
                claim_generation,
            )? {
                return Ok(stopped);
            }
            checkpoint.goal_revision = self
                .store
                .get(owner, goal_id)?
                .ok_or_else(|| GoalStoreError::NotFound {
                    id: goal_id.to_string(),
                })?
                .revision;
            let manual_pause = manual_pause_required(&verification);
            let verifier_failed = !verification.results.is_empty()
                && !manual_pause
                && verification
                    .results
                    .iter()
                    .any(|result| result.status == CriterionStatus::Error);
            if verifier_failed && checkpoint.consecutive_failures < self.config.max_failures {
                // Verifier and runtime failures are separate failure events. A
                // round where both fail deliberately consumes two slots, which
                // preserves the established pursuit contract.
                // Persist verifier failures with the verification checkpoint so
                // a restart cannot lose a consumed lifetime failure slot.
                checkpoint.consecutive_failures = checkpoint.consecutive_failures.saturating_add(1);
            }
            checkpoint.updated_at = std::cmp::max(verified_at, checkpoint.updated_at);
            let (_, recorded, _) = self.store.record_pursuit_verification(
                owner,
                goal_id,
                &self.config.worker_id,
                &verification,
                &checkpoint,
                &verification.summary,
                checkpoint.updated_at,
            )?;
            checkpoint = recorded;
            previous_verification = Some(verification.clone());
            let last_verification = previous_verification.clone();

            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                checkpoint.segments_started,
                checkpoint.segments_completed,
                checkpoint.consecutive_failures,
                last_verification.clone(),
                claim_generation,
            )? {
                return Ok(stopped);
            }
            if cancel.is_cancelled() {
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    last_verification,
                    cancellation,
                );
            }
            if verification.all_satisfied {
                checkpoint.consecutive_failures = 0;
                checkpoint.status = PursuitCheckpointStatus::Achieved;
                self.record_checkpoint(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit.achieved",
                    &verification.summary,
                )?;
                let completed =
                    self.store
                        .get(owner, goal_id)?
                        .ok_or_else(|| GoalStoreError::NotFound {
                            id: goal_id.to_string(),
                        })?;
                return Ok(outcome(
                    &completed,
                    PursuitStatus::Achieved,
                    checkpoint.segments_started,
                    checkpoint.segments_completed,
                    0,
                    &verification.summary,
                    "acceptance satisfied",
                    last_verification,
                ));
            }
            if verification.results.is_empty() {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "acceptance criteria required",
                    last_verification,
                );
            }
            if manual_pause {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "manual confirmation required",
                    last_verification,
                );
            }
            if checkpoint.consecutive_failures >= self.config.max_failures {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit max failures reached",
                    last_verification,
                );
            }

            if checkpoint.segments_started >= self.config.max_segments {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit max segments reached",
                    last_verification,
                );
            }
            let remaining = self
                .config
                .overall_timeout
                .saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit overall timeout",
                    last_verification,
                );
            }

            let segment_index = checkpoint.segments_started.saturating_add(1);
            let timeout = std::cmp::min(remaining, self.config.segment_timeout);
            let request = PursuitSegmentRequest {
                goal_id: goal_id.to_string(),
                segment_index,
                prompt: render_prompt(&goal, &verification, segment_index),
                workdir: self.config.workdir.clone(),
                timeout,
                runtime: self.config.runtime.clone(),
                verification,
            };
            if cancel.is_cancelled() {
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    last_verification,
                    cancellation,
                );
            }
            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                checkpoint.segments_started,
                checkpoint.segments_completed,
                checkpoint.consecutive_failures,
                last_verification.clone(),
                claim_generation,
            )? {
                return Ok(stopped);
            }
            checkpoint.segments_started = segment_index;
            // Dispatch is an external side effect. Reserve its failure slot in
            // the durable pre-dispatch checkpoint, then release it only after a
            // successful result is durably observed.
            let failures_before_segment = checkpoint.consecutive_failures;
            checkpoint.consecutive_failures = checkpoint.consecutive_failures.saturating_add(1);
            self.record_checkpoint(
                owner,
                goal_id,
                &mut checkpoint,
                "pursuit.segment.started",
                &format!("segment {segment_index} started"),
            )?;
            let result = match tokio::time::timeout(
                timeout,
                self.runtime.run_segment(request, cancel.child_token()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => PursuitSegmentResult {
                    status: PursuitSegmentStatus::Timeout,
                    run_id: None,
                },
            };

            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                checkpoint.segments_started,
                checkpoint.segments_completed,
                checkpoint.consecutive_failures,
                last_verification.clone(),
                claim_generation,
            )? {
                return Ok(stopped);
            }
            if result.status == PursuitSegmentStatus::Cancelled
                && matches!(cancellation, CancellationDisposition::PreserveRunning)
            {
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    last_verification,
                    cancellation,
                );
            }
            checkpoint.segments_completed = checkpoint.segments_completed.saturating_add(1);
            let valid_run_id = result.run_id.as_deref().filter(|value| {
                !value.is_empty()
                    && value.len() <= 256
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            });
            checkpoint.last_run_id = valid_run_id.map(str::to_string);
            let run_suffix =
                valid_run_id.map_or_else(String::new, |value| format!("; run {value}"));
            let (stage, detail) = match result.status {
                PursuitSegmentStatus::Success => {
                    checkpoint.consecutive_failures = if verifier_failed {
                        failures_before_segment
                    } else {
                        0
                    };
                    (
                        "pursuit.segment.completed",
                        format!("segment {segment_index} completed{run_suffix}"),
                    )
                }
                other => (
                    "pursuit.segment.failed",
                    format!("segment {segment_index} failed: {other:?}{run_suffix}"),
                ),
            };
            self.record_checkpoint(owner, goal_id, &mut checkpoint, stage, &detail)?;
            if result.status == PursuitSegmentStatus::Cancelled {
                return self.finish_cancellation(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    last_verification,
                    cancellation,
                );
            }
            if result.status != PursuitSegmentStatus::Success
                && checkpoint.consecutive_failures >= self.config.max_failures
            {
                return self.pause(
                    owner,
                    goal_id,
                    &mut checkpoint,
                    "pursuit max failures reached",
                    last_verification,
                );
            }
        }
    }

    fn record_checkpoint(
        &self,
        owner: &str,
        goal_id: &str,
        checkpoint: &mut GoalPursuitCheckpoint,
        stage: &str,
        detail: &str,
    ) -> Result<()> {
        let goal = self
            .store
            .get(owner, goal_id)?
            .ok_or_else(|| GoalStoreError::NotFound {
                id: goal_id.to_string(),
            })?;
        if goal.claim_generation != checkpoint.claim_generation {
            return Err(GoalStoreError::CheckpointConflict {
                id: goal_id.to_string(),
            });
        }
        checkpoint.goal_revision = goal.revision;
        checkpoint.updated_at = std::cmp::max(Utc::now(), checkpoint.updated_at);
        let (recorded, _) = self.store.record_pursuit_checkpoint(
            owner,
            goal_id,
            &self.config.worker_id,
            checkpoint,
            stage,
            detail,
            checkpoint.updated_at,
        )?;
        *checkpoint = recorded;
        Ok(())
    }

    fn require_running(&self, owner: &str, goal_id: &str) -> Result<GoalRecord> {
        let goal = self
            .store
            .get(owner, goal_id)?
            .ok_or_else(|| GoalStoreError::NotFound {
                id: goal_id.to_string(),
            })?;
        if goal.status != GoalStatus::InProgress {
            return Err(GoalStoreError::InvalidStatus {
                id: goal_id.to_string(),
                operation: "pursue",
                status: goal.status,
            });
        }
        if !goal.lease_active(Utc::now()) {
            return Err(GoalStoreError::LeaseExpired {
                id: goal_id.to_string(),
            });
        }
        if Some(self.config.worker_id.as_str()) != goal.claimed_by.as_deref() {
            return Err(GoalStoreError::LeaseHeld {
                id: goal_id.to_string(),
                held_by: goal.claimed_by.unwrap_or_else(|| "unknown".into()),
            });
        }
        Ok(goal)
    }

    #[allow(clippy::too_many_arguments)]
    fn stopped_if_drifted(
        &self,
        owner: &str,
        goal_id: &str,
        segments_started: u16,
        segments_completed: u16,
        consecutive_failures: u16,
        verification: Option<AcceptanceVerification>,
        claim_generation: u64,
    ) -> Result<Option<PursuitOutcome>> {
        let latest = self
            .store
            .get(owner, goal_id)?
            .ok_or_else(|| GoalStoreError::NotFound {
                id: goal_id.to_string(),
            })?;
        Ok(self.stopped_for_record(
            &latest,
            segments_started,
            segments_completed,
            consecutive_failures,
            verification,
            claim_generation,
        ))
    }

    fn stopped_for_record(
        &self,
        latest: &GoalRecord,
        segments_started: u16,
        segments_completed: u16,
        consecutive_failures: u16,
        verification: Option<AcceptanceVerification>,
        claim_generation: u64,
    ) -> Option<PursuitOutcome> {
        if latest.status != GoalStatus::InProgress {
            return Some(outcome(
                latest,
                PursuitStatus::Stopped,
                segments_started,
                segments_completed,
                consecutive_failures,
                "goal changed outside pursuit",
                &format!("goal status is {}", latest.status),
                verification,
            ));
        }
        if !latest.lease_active(Utc::now())
            || latest.claimed_by.as_deref() != Some(self.config.worker_id.as_str())
            || latest.claim_generation != claim_generation
        {
            return Some(outcome(
                latest,
                PursuitStatus::Stopped,
                segments_started,
                segments_completed,
                consecutive_failures,
                "goal lease changed outside pursuit",
                "goal lease changed outside pursuit",
                verification,
            ));
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn pause(
        &self,
        owner: &str,
        goal_id: &str,
        checkpoint: &mut GoalPursuitCheckpoint,
        reason: &str,
        verification: Option<AcceptanceVerification>,
    ) -> Result<PursuitOutcome> {
        checkpoint.status = PursuitCheckpointStatus::Paused;
        self.record_checkpoint(owner, goal_id, checkpoint, "pursuit.paused", reason)?;
        let paused = self
            .store
            .get(owner, goal_id)?
            .ok_or_else(|| GoalStoreError::NotFound {
                id: goal_id.to_string(),
            })?;
        let summary = verification
            .as_ref()
            .map_or_else(|| reason.to_string(), |value| value.summary.clone());
        Ok(outcome(
            &paused,
            PursuitStatus::Paused,
            checkpoint.segments_started,
            checkpoint.segments_completed,
            checkpoint.consecutive_failures,
            &summary,
            reason,
            verification,
        ))
    }

    fn finish_cancellation(
        &self,
        owner: &str,
        goal_id: &str,
        checkpoint: &mut GoalPursuitCheckpoint,
        verification: Option<AcceptanceVerification>,
        disposition: CancellationDisposition,
    ) -> Result<PursuitOutcome> {
        if matches!(disposition, CancellationDisposition::Pause) {
            return self.pause(
                owner,
                goal_id,
                checkpoint,
                "pursuit cancelled",
                verification,
            );
        }
        let running = self
            .store
            .get(owner, goal_id)?
            .ok_or_else(|| GoalStoreError::NotFound {
                id: goal_id.to_string(),
            })?;
        let summary = verification.as_ref().map_or_else(
            || "pursuit cancelled".to_string(),
            |value| value.summary.clone(),
        );
        Ok(outcome(
            &running,
            PursuitStatus::Stopped,
            checkpoint.segments_started,
            checkpoint.segments_completed,
            checkpoint.consecutive_failures,
            &summary,
            "pursuit cancelled",
            verification,
        ))
    }
}

fn manual_pause_required(verification: &AcceptanceVerification) -> bool {
    verification
        .results
        .iter()
        .any(|result| result.status == CriterionStatus::ManualRequired)
        && verification.results.iter().all(|result| {
            matches!(
                result.status,
                CriterionStatus::ManualRequired | CriterionStatus::Satisfied
            )
        })
}

fn render_prompt(
    goal: &GoalRecord,
    verification: &AcceptanceVerification,
    segment_index: u16,
) -> String {
    let criteria = goal
        .acceptance_criteria
        .iter()
        .enumerate()
        .map(|(index, criterion)| format!("- [{index}] {}: {}", criterion.kind, criterion.target))
        .collect::<Vec<_>>()
        .join("\n");
    let results = verification
        .results
        .iter()
        .map(|result| {
            format!(
                "- [{}] {:?}: {}",
                result.criterion_index, result.status, result.detail
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Continue goal {}: {}\n\n{}\n\nSegment: {segment_index}\n\nAcceptance criteria:\n{}\n\nLatest verifier result:\n{}\n\nDo one verifiable increment. Report what changed, what you checked, and what remains. Do not mark the goal done; the acceptance verifier runs after this segment.",
        goal.id,
        goal.title,
        goal.description,
        if criteria.is_empty() {
            "- none"
        } else {
            &criteria
        },
        if results.is_empty() {
            &verification.summary
        } else {
            &results
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn outcome(
    goal: &GoalRecord,
    status: PursuitStatus,
    segments_started: u16,
    segments_completed: u16,
    consecutive_failures: u16,
    summary: &str,
    reason: &str,
    last_verification: Option<AcceptanceVerification>,
) -> PursuitOutcome {
    PursuitOutcome {
        goal_id: goal.id.clone(),
        status,
        final_goal_status: goal.status,
        segments_started,
        segments_completed,
        consecutive_failures,
        summary: summary.to_string(),
        reason: reason.to_string(),
        last_verification,
    }
}
