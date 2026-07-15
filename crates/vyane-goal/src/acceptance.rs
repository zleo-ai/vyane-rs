//! Bounded, model-free acceptance verification.
//!
//! Verification is deliberately separate from goal lifecycle mutation. A
//! caller may persist only the satisfied criteria through the fenced
//! [`crate::GoalStore::satisfy_criterion`] operation; this module never marks
//! a goal complete and never performs network or shell interpretation.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest as _, Sha256};

use crate::{
    AcceptanceCriterion, AcceptanceVerification, CriterionResult, CriterionStatus, GoalRecord,
    Result,
};

pub const MAX_VERIFIER_TIMEOUT: Duration = Duration::from_secs(300);
pub const MAX_OUTPUT_TAIL_BYTES: usize = 4_000;

#[derive(Debug, Clone)]
pub struct AcceptanceVerifier {
    workdir: PathBuf,
    timeout: Duration,
}

impl AcceptanceVerifier {
    pub fn new(workdir: impl Into<PathBuf>, timeout: Duration) -> Result<Self> {
        let workdir = workdir.into();
        let canonical = std::fs::canonicalize(&workdir).map_err(|error| {
            crate::GoalStoreError::InvalidInput(format!(
                "acceptance workdir is unavailable: {error}"
            ))
        })?;
        if !canonical.is_dir() {
            return Err(crate::GoalStoreError::InvalidInput(
                "acceptance workdir must be a directory".into(),
            ));
        }
        if timeout.is_zero() || timeout > MAX_VERIFIER_TIMEOUT {
            return Err(crate::GoalStoreError::InvalidInput(
                "acceptance timeout must be between 1ms and 300s".into(),
            ));
        }
        Ok(Self {
            workdir: canonical,
            timeout,
        })
    }

    #[must_use]
    pub fn verify(&self, record: &GoalRecord) -> AcceptanceVerification {
        let results = record
            .acceptance_criteria
            .iter()
            .enumerate()
            .map(|(index, criterion)| self.verify_criterion(criterion, index))
            .collect::<Vec<_>>();
        let all_satisfied = !results.is_empty()
            && results
                .iter()
                .all(|result| result.status == CriterionStatus::Satisfied);
        let summary = summarize(&results);
        AcceptanceVerification {
            goal_id: record.id.clone(),
            all_satisfied,
            results,
            summary,
        }
    }

    /// Verify all criteria under one shared wall-clock budget. The final
    /// command receives only the remaining budget and every later criterion is
    /// reported as an error without execution once the budget is exhausted.
    #[must_use]
    pub fn verify_with_budget(
        &self,
        record: &GoalRecord,
        budget: Duration,
    ) -> AcceptanceVerification {
        let started = Instant::now();
        let mut results = Vec::with_capacity(record.acceptance_criteria.len());
        for (index, criterion) in record.acceptance_criteria.iter().enumerate() {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                results.push(self.result(
                    index,
                    criterion_key(index, criterion),
                    criterion.kind.trim().to_owned(),
                    criterion.target.trim().to_owned(),
                    CriterionStatus::Error,
                    Vec::new(),
                    None,
                    "verification budget exhausted",
                ));
                continue;
            }
            let verifier = Self {
                workdir: self.workdir.clone(),
                timeout: std::cmp::min(self.timeout, remaining),
            };
            results.push(verifier.verify_criterion(criterion, index));
        }
        let all_satisfied = !results.is_empty()
            && results
                .iter()
                .all(|result| result.status == CriterionStatus::Satisfied);
        let summary = summarize(&results);
        AcceptanceVerification {
            goal_id: record.id.clone(),
            all_satisfied,
            results,
            summary,
        }
    }

    #[must_use]
    pub fn verify_criterion(
        &self,
        criterion: &AcceptanceCriterion,
        index: usize,
    ) -> CriterionResult {
        let kind = criterion.kind.trim().to_owned();
        let target = criterion.target.trim().to_owned();
        let key = criterion_key(index, criterion);
        if kind.is_empty() || target.is_empty() {
            return self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Error,
                Vec::new(),
                None,
                "criterion kind and target must be non-empty",
            );
        }
        if criterion.satisfied_at.is_some() {
            return self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Satisfied,
                Vec::new(),
                None,
                "criterion already satisfied",
            );
        }
        match kind.as_str() {
            "manual-confirm" => self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::ManualRequired,
                Vec::new(),
                None,
                "manual confirmation required",
            ),
            "test-passes" | "custom" => self.verify_command(index, key, kind, target),
            _ => self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Inconclusive,
                Vec::new(),
                None,
                "unsupported acceptance criterion kind",
            ),
        }
    }

    fn verify_command(
        &self,
        index: usize,
        key: String,
        kind: String,
        target: String,
    ) -> CriterionResult {
        #[cfg(not(unix))]
        return self.result(
            index,
            key,
            kind,
            target,
            CriterionStatus::Inconclusive,
            Vec::new(),
            None,
            "command acceptance verification requires Unix process-group support",
        );
        let Some(command) = parse_command(&target) else {
            return self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Error,
                Vec::new(),
                None,
                "command criteria require a cmd: prefix and non-empty argv",
            );
        };
        let started = Instant::now();
        let mut process = Command::new(&command[0]);
        process
            .args(&command[1..])
            .current_dir(&self.workdir)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            process.process_group(0);
        }
        let mut child = match process.spawn() {
            Ok(child) => child,
            Err(error) => {
                return self.result(
                    index,
                    key,
                    kind,
                    target,
                    CriterionStatus::Error,
                    command,
                    None,
                    &format!("failed to start acceptance command: {error}"),
                );
            }
        };
        let stdout = child.stdout.take().map(spawn_tail_reader);
        let stderr = child.stderr.take().map(spawn_tail_reader);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    // The leader has already been reaped here. Do not signal its
                    // cached PGID: that identifier may have been recycled.
                    let stdout = join_tail_bounded(stdout);
                    let stderr = join_tail_bounded(stderr);
                    let status_kind = if status.success() {
                        CriterionStatus::Satisfied
                    } else {
                        CriterionStatus::Unsatisfied
                    };
                    return self.result_with_output(
                        index,
                        key,
                        kind,
                        target,
                        status_kind,
                        command,
                        status.code(),
                        started.elapsed(),
                        stdout,
                        stderr,
                        if status.success() {
                            "acceptance command passed"
                        } else {
                            "acceptance command failed"
                        },
                    );
                }
                Ok(None) if started.elapsed() < self.timeout => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    kill_process_group(child.id());
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = join_tail_bounded(stdout);
                    let _ = join_tail_bounded(stderr);
                    return self.result(
                        index,
                        key,
                        kind,
                        target,
                        CriterionStatus::Error,
                        command,
                        None,
                        "acceptance command timed out",
                    );
                }
                Err(error) => {
                    kill_process_group(child.id());
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = join_tail_bounded(stdout);
                    let _ = join_tail_bounded(stderr);
                    return self.result(
                        index,
                        key,
                        kind,
                        target,
                        CriterionStatus::Error,
                        command,
                        None,
                        &format!("failed to poll acceptance command: {error}"),
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn result(
        &self,
        index: usize,
        key: String,
        kind: String,
        target: String,
        status: CriterionStatus,
        command: Vec<String>,
        exit_code: Option<i32>,
        detail: &str,
    ) -> CriterionResult {
        self.result_with_output(
            index,
            key,
            kind,
            target,
            status,
            command,
            exit_code,
            Duration::ZERO,
            String::new(),
            String::new(),
            detail,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn result_with_output(
        &self,
        criterion_index: usize,
        criterion_key: String,
        kind: String,
        target: String,
        status: CriterionStatus,
        command: Vec<String>,
        exit_code: Option<i32>,
        duration: Duration,
        stdout_tail: String,
        stderr_tail: String,
        detail: &str,
    ) -> CriterionResult {
        CriterionResult {
            criterion_index,
            criterion_key,
            kind,
            target,
            status,
            command,
            cwd: self.workdir.to_string_lossy().into_owned(),
            exit_code,
            duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
            stdout_tail,
            stderr_tail,
            detail: detail.to_owned(),
        }
    }
}

#[must_use]
pub fn criterion_key(index: usize, criterion: &AcceptanceCriterion) -> String {
    let mut digest = Sha256::new();
    digest.update(index.to_string().as_bytes());
    digest.update(b":");
    digest.update(criterion.kind.as_bytes());
    digest.update(b":");
    digest.update(criterion.target.as_bytes());
    digest
        .finalize()
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn parse_command(target: &str) -> Option<Vec<String>> {
    let command = target.strip_prefix("cmd:")?.trim();
    let argv = command
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    (!argv.is_empty() && argv.iter().all(|part| !part.contains('\0'))).then_some(argv)
}

fn spawn_tail_reader(mut reader: impl Read + Send + 'static) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    bytes.extend_from_slice(&chunk[..count]);
                    if bytes.len() > MAX_OUTPUT_TAIL_BYTES {
                        let excess = bytes.len() - MAX_OUTPUT_TAIL_BYTES;
                        bytes.drain(..excess);
                    }
                }
            }
        }
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

fn join_tail_bounded(reader: Option<thread::JoinHandle<String>>) -> String {
    let Some(reader) = reader else {
        return String::new();
    };
    let deadline = Instant::now() + Duration::from_millis(100);
    while !reader.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    if reader.is_finished() {
        reader.join().unwrap_or_default()
    } else {
        // Detach a reader whose pipe is still held by an escaped descendant;
        // the verifier itself remains bounded and the reader exits on EOF.
        String::new()
    }
}

#[cfg(unix)]
fn kill_process_group(pid: u32) {
    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    let Some(pid) = rustix::process::Pid::from_raw(pid) else {
        return;
    };
    let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
}

#[cfg(not(unix))]
fn kill_process_group(_pid: u32) {}

fn summarize(results: &[CriterionResult]) -> String {
    let mut satisfied = 0;
    let mut unsatisfied = 0;
    let mut other = 0;
    for result in results {
        match result.status {
            CriterionStatus::Satisfied => satisfied += 1,
            CriterionStatus::Unsatisfied => unsatisfied += 1,
            _ => other += 1,
        }
    }
    format!(
        "{} criteria: satisfied={satisfied}, unsatisfied={unsatisfied}, other={other}",
        results.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn record(criteria: Vec<AcceptanceCriterion>) -> GoalRecord {
        GoalRecord {
            id: "goal-1".into(),
            owner: "local".into(),
            title: "verify".into(),
            description: String::new(),
            status: crate::GoalStatus::Queued,
            priority: 2,
            parent_goal_id: None,
            acceptance_criteria: criteria,
            continuity_policy: None,
            continuity_state: None,
            created_at: Utc::now(),
            started_at: None,
            updated_at: Utc::now(),
            finished_at: None,
            revision: 0,
            completion_summary: None,
            failure_reason: None,
            pause_reason: None,
            cancel_reason: None,
            claimed_by: None,
            claim_expires_at: None,
            claim_generation: 0,
        }
    }

    #[test]
    fn command_verification_is_bounded_and_scrubs_environment() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let result = verifier.verify(&record(vec![
            AcceptanceCriterion::new("custom", "cmd:sh -c exit 0"),
            AcceptanceCriterion::new("custom", "cmd:env"),
        ]));
        assert!(result.all_satisfied);
        assert_eq!(result.results[0].status, CriterionStatus::Satisfied);
        assert_eq!(result.results[0].exit_code, Some(0));
        assert!(!result.results[1].stdout_tail.contains("HOME="));
        assert!(result.results[1].stdout_tail.contains("PATH="));
    }

    #[test]
    fn unsupported_and_manual_criteria_do_not_auto_satisfy() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let result = verifier.verify(&record(vec![
            AcceptanceCriterion::new("manual-confirm", "operator"),
            AcceptanceCriterion::new("future-kind", "thing"),
        ]));
        assert!(!result.all_satisfied);
        assert_eq!(result.results[0].status, CriterionStatus::ManualRequired);
        assert_eq!(result.results[1].status, CriterionStatus::Inconclusive);
    }

    #[cfg(unix)]
    #[test]
    fn timeout_and_output_are_bounded() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_millis(20)).expect("cwd");
        let timeout = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            "cmd:sleep 5",
        )]));
        assert_eq!(timeout.results[0].status, CriterionStatus::Error);
        assert_eq!(timeout.results[0].detail, "acceptance command timed out");

        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let output = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            "cmd:seq 1 5000",
        )]));
        assert_eq!(output.results[0].status, CriterionStatus::Satisfied);
        assert!(output.results[0].stdout_tail.len() <= MAX_OUTPUT_TAIL_BYTES);
    }

    #[test]
    fn spawn_failure_is_reported_as_error() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let result = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            "cmd:definitely-not-a-real-binary-vyane",
        )]));
        assert_eq!(result.results[0].status, CriterionStatus::Error);
    }

    #[test]
    fn non_zero_command_is_unsatisfied() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let result = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            "cmd:false",
        )]));
        assert_eq!(result.results[0].status, CriterionStatus::Unsatisfied);
        assert_eq!(result.results[0].exit_code, Some(1));
    }

    #[test]
    fn criterion_key_is_deterministic_and_index_sensitive() {
        let criterion = AcceptanceCriterion::new("custom", "cmd:true");
        assert_eq!(criterion_key(0, &criterion), criterion_key(0, &criterion));
        assert_ne!(criterion_key(0, &criterion), criterion_key(1, &criterion));
    }

    #[cfg(unix)]
    #[test]
    fn shared_budget_stops_later_criteria_without_execution() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let result = verifier.verify_with_budget(
            &record(vec![
                AcceptanceCriterion::new("custom", "cmd:sleep 5"),
                AcceptanceCriterion::new("custom", "cmd:true"),
            ]),
            Duration::from_millis(200),
        );
        assert_eq!(result.results[0].detail, "acceptance command timed out");
        assert_eq!(result.results[1].detail, "verification budget exhausted");
    }
}
