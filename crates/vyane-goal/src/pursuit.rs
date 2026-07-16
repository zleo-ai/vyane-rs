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
        if !self.workdir.is_absolute() || !self.workdir.is_dir() {
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
        let started = Instant::now();
        let mut segments_started = 0_u16;
        let mut segments_completed = 0_u16;
        let mut consecutive_failures = 0_u16;
        let mut previous_verification = None;
        let mut claim_generation = None;
        loop {
            let Some(remaining) = self.config.overall_timeout.checked_sub(started.elapsed()) else {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit overall timeout",
                    previous_verification,
                );
            };
            if remaining.is_zero() {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit overall timeout",
                    previous_verification,
                );
            }
            let goal = if segments_started == 0 {
                let goal = self.require_running(owner, goal_id)?;
                claim_generation = Some(goal.claim_generation);
                goal
            } else {
                let latest =
                    self.store
                        .get(owner, goal_id)?
                        .ok_or_else(|| GoalStoreError::NotFound {
                            id: goal_id.to_string(),
                        })?;
                if let Some(stopped) = self.stopped_for_record(
                    &latest,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    previous_verification.clone(),
                    claim_generation.expect("claim generation initialized"),
                ) {
                    return Ok(stopped);
                }
                latest
            };
            if cancel.is_cancelled() {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit cancelled",
                    previous_verification,
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
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    previous_verification.clone(),
                    claim_generation.expect("claim generation initialized"),
                )? {
                    return Ok(stopped);
                }
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit cancelled",
                    previous_verification,
                );
            };
            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                segments_started,
                segments_completed,
                consecutive_failures,
                previous_verification.clone(),
                claim_generation.expect("claim generation initialized"),
            )? {
                return Ok(stopped);
            }
            if cancel.is_cancelled() {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit cancelled",
                    previous_verification,
                );
            }
            let verified_at = Utc::now();
            self.store.record_verification(
                owner,
                goal_id,
                Some(&self.config.worker_id),
                &verification,
                verified_at,
            )?;
            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                segments_started,
                segments_completed,
                consecutive_failures,
                previous_verification.clone(),
                claim_generation.expect("claim generation initialized"),
            )? {
                return Ok(stopped);
            }
            self.persist_satisfied(owner, &goal, &verification, verified_at)?;
            self.store.progress(
                owner,
                goal_id,
                "acceptance.verify",
                &verification.summary,
                verified_at,
            )?;
            previous_verification = Some(verification.clone());
            let last_verification = previous_verification.clone();

            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                segments_started,
                segments_completed,
                consecutive_failures,
                last_verification.clone(),
                claim_generation.expect("claim generation initialized"),
            )? {
                return Ok(stopped);
            }
            if cancel.is_cancelled() {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit cancelled",
                    last_verification,
                );
            }
            if verification.all_satisfied {
                let completed = self.store.done(
                    owner,
                    goal_id,
                    Some(&self.config.worker_id),
                    Some(&verification.summary),
                    None,
                    Utc::now(),
                )?;
                return Ok(outcome(
                    &completed,
                    PursuitStatus::Achieved,
                    segments_started,
                    segments_completed,
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
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "acceptance criteria required",
                    last_verification,
                );
            }
            if manual_pause_required(&verification) {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "manual confirmation required",
                    last_verification,
                );
            }

            let verifier_failed = verification
                .results
                .iter()
                .any(|result| result.status == CriterionStatus::Error);
            if verifier_failed {
                // Verifier and runtime failures are separate failure events. A
                // round where both fail deliberately consumes two slots, which
                // preserves the established pursuit contract.
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures >= self.config.max_failures {
                    return self.pause(
                        owner,
                        goal_id,
                        segments_started,
                        segments_completed,
                        consecutive_failures,
                        "pursuit max failures reached",
                        last_verification,
                    );
                }
            }
            if segments_started >= self.config.max_segments {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
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
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit overall timeout",
                    last_verification,
                );
            }

            let segment_index = segments_started.saturating_add(1);
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
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit cancelled",
                    last_verification,
                );
            }
            if let Some(stopped) = self.stopped_if_drifted(
                owner,
                goal_id,
                segments_started,
                segments_completed,
                consecutive_failures,
                last_verification.clone(),
                claim_generation.expect("claim generation initialized"),
            )? {
                return Ok(stopped);
            }
            segments_started = segment_index;
            self.store.progress(
                owner,
                goal_id,
                "pursuit.segment.started",
                &format!("segment {segment_index} started"),
                Utc::now(),
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
                segments_started,
                segments_completed,
                consecutive_failures,
                last_verification.clone(),
                claim_generation.expect("claim generation initialized"),
            )? {
                return Ok(stopped);
            }
            segments_completed = segments_completed.saturating_add(1);
            let run_suffix = result
                .run_id
                .as_deref()
                .filter(|value| {
                    !value.is_empty()
                        && value.len() <= 256
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                })
                .map_or_else(String::new, |value| format!("; run {value}"));
            let (stage, detail) = match result.status {
                PursuitSegmentStatus::Success => {
                    if !verifier_failed {
                        consecutive_failures = 0;
                    }
                    (
                        "pursuit.segment.completed",
                        format!("segment {segment_index} completed{run_suffix}"),
                    )
                }
                other => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    (
                        "pursuit.segment.failed",
                        format!("segment {segment_index} failed: {other:?}{run_suffix}"),
                    )
                }
            };
            self.store
                .progress(owner, goal_id, stage, &detail, Utc::now())?;
            if result.status == PursuitSegmentStatus::Cancelled {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit cancelled",
                    last_verification,
                );
            }
            if result.status != PursuitSegmentStatus::Success
                && consecutive_failures >= self.config.max_failures
            {
                return self.pause(
                    owner,
                    goal_id,
                    segments_started,
                    segments_completed,
                    consecutive_failures,
                    "pursuit max failures reached",
                    last_verification,
                );
            }
        }
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

    fn persist_satisfied(
        &self,
        owner: &str,
        goal: &GoalRecord,
        verification: &AcceptanceVerification,
        at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        for result in &verification.results {
            if result.status == CriterionStatus::Satisfied
                && goal
                    .acceptance_criteria
                    .get(result.criterion_index)
                    .is_some_and(|criterion| criterion.satisfied_at.is_none())
            {
                self.store.satisfy_criterion(
                    owner,
                    &goal.id,
                    Some(&self.config.worker_id),
                    result.criterion_index,
                    at,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn pause(
        &self,
        owner: &str,
        goal_id: &str,
        segments_started: u16,
        segments_completed: u16,
        consecutive_failures: u16,
        reason: &str,
        verification: Option<AcceptanceVerification>,
    ) -> Result<PursuitOutcome> {
        self.store
            .progress(owner, goal_id, "pursuit.paused", reason, Utc::now())?;
        let paused = self.store.pause(
            owner,
            goal_id,
            Some(&self.config.worker_id),
            Some(reason),
            Utc::now(),
        )?;
        let summary = verification
            .as_ref()
            .map_or_else(|| reason.to_string(), |value| value.summary.clone());
        Ok(outcome(
            &paused,
            PursuitStatus::Paused,
            segments_started,
            segments_completed,
            consecutive_failures,
            &summary,
            reason,
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
