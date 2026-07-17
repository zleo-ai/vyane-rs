//! Bounded, model-free acceptance verification.
//!
//! Verification is deliberately separate from goal lifecycle mutation. A
//! caller may persist only the satisfied criteria through the fenced
//! [`crate::GoalStore::satisfy_criterion`] operation; this module never marks
//! a goal complete and never performs network or shell interpretation.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    AcceptanceCriterion, AcceptanceVerification, CriterionResult, CriterionStatus, GoalRecord,
    Result,
};

pub const MAX_VERIFIER_TIMEOUT: Duration = Duration::from_secs(300);
pub const MAX_OUTPUT_TAIL_BYTES: usize = 4_000;
const EXECUTABLE_BUSY_RETRY_DELAY: Duration = Duration::from_millis(20);
const EXECUTABLE_BUSY_MAX_RETRIES: usize = 5;

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
        self.verify_with_budget_and_cancel(record, budget, &CancellationToken::new())
            .expect("a fresh cancellation token cannot be cancelled")
    }

    /// Verify all criteria under one shared wall-clock budget, stopping any
    /// active command process group when `cancel` is triggered. `None` means
    /// cancellation won the race and no partial verification should be
    /// persisted by the caller.
    #[must_use]
    pub fn verify_with_budget_and_cancel(
        &self,
        record: &GoalRecord,
        budget: Duration,
        cancel: &CancellationToken,
    ) -> Option<AcceptanceVerification> {
        let started = Instant::now();
        let mut results = Vec::with_capacity(record.acceptance_criteria.len());
        for (index, criterion) in record.acceptance_criteria.iter().enumerate() {
            if cancel.is_cancelled() {
                return None;
            }
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
            results.push(verifier.verify_criterion_with_cancel(criterion, index, cancel)?);
        }
        let all_satisfied = !results.is_empty()
            && results
                .iter()
                .all(|result| result.status == CriterionStatus::Satisfied);
        let summary = summarize(&results);
        Some(AcceptanceVerification {
            goal_id: record.id.clone(),
            all_satisfied,
            results,
            summary,
        })
    }

    #[must_use]
    pub fn verify_criterion(
        &self,
        criterion: &AcceptanceCriterion,
        index: usize,
    ) -> CriterionResult {
        self.verify_criterion_with_cancel(criterion, index, &CancellationToken::new())
            .expect("a fresh cancellation token cannot be cancelled")
    }

    fn verify_criterion_with_cancel(
        &self,
        criterion: &AcceptanceCriterion,
        index: usize,
        cancel: &CancellationToken,
    ) -> Option<CriterionResult> {
        if cancel.is_cancelled() {
            return None;
        }
        let kind = criterion.kind.trim().to_owned();
        let target = criterion.target.trim().to_owned();
        let key = criterion_key(index, criterion);
        if kind.is_empty() || target.is_empty() {
            return Some(self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Error,
                Vec::new(),
                None,
                "criterion kind and target must be non-empty",
            ));
        }
        if criterion.satisfied_at.is_some() {
            return Some(self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Satisfied,
                Vec::new(),
                None,
                "criterion already satisfied",
            ));
        }
        Some(match kind.as_str() {
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
            "test-passes" | "custom" => {
                return self.verify_command(index, key, kind, target, cancel);
            }
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
        })
    }

    fn verify_command(
        &self,
        index: usize,
        key: String,
        kind: String,
        target: String,
        cancel: &CancellationToken,
    ) -> Option<CriterionResult> {
        #[cfg(not(unix))]
        return Some(self.result(
            index,
            key,
            kind,
            target,
            CriterionStatus::Inconclusive,
            Vec::new(),
            None,
            "command acceptance verification requires Unix process-group support",
        ));
        #[cfg(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))]
        return Some(self.result(
            index,
            key,
            kind,
            target,
            CriterionStatus::Inconclusive,
            Vec::new(),
            None,
            "command acceptance verification requires waitid WNOWAIT support",
        ));
        let Some(command) = parse_command(&target) else {
            return Some(self.result(
                index,
                key,
                kind,
                target,
                CriterionStatus::Error,
                Vec::new(),
                None,
                "command criteria require a cmd: prefix and non-empty argv",
            ));
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
        let deadline = started + self.timeout;
        let mut child = match spawn_with_busy_retry(&mut process, deadline, cancel) {
            Ok(child) => child,
            Err(_) if cancel.is_cancelled() => return None,
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                return Some(self.result(
                    index,
                    key,
                    kind,
                    target,
                    CriterionStatus::Error,
                    command,
                    None,
                    "acceptance command timed out",
                ));
            }
            Err(error) => {
                return Some(self.result(
                    index,
                    key,
                    kind,
                    target,
                    CriterionStatus::Error,
                    command,
                    None,
                    &format!("failed to start acceptance command: {error}"),
                ));
            }
        };
        let stdout = child.stdout.take().map(spawn_tail_reader);
        let stderr = child.stderr.take().map(spawn_tail_reader);
        loop {
            if cancel.is_cancelled() {
                terminate_child_group(&mut child);
                let _ = join_tail_bounded(stdout);
                let _ = join_tail_bounded(stderr);
                return None;
            }
            match child_exited_without_reap(child.id()) {
                Ok(true) => {
                    // waitid(WNOWAIT) leaves the exited leader waitable, so its
                    // PID remains the live process-group identity while every
                    // descendant is stopped. Reap only after group cleanup;
                    // signalling a cached PGID after try_wait would risk PID
                    // reuse and an unrelated process group.
                    kill_process_group(child.id());
                    let status = match child.wait() {
                        Ok(status) => status,
                        Err(error) => {
                            let _ = join_tail_bounded(stdout);
                            let _ = join_tail_bounded(stderr);
                            return Some(self.result(
                                index,
                                key,
                                kind,
                                target,
                                CriterionStatus::Error,
                                command,
                                None,
                                &format!("failed to reap acceptance command: {error}"),
                            ));
                        }
                    };
                    let stdout = join_tail_bounded(stdout);
                    let stderr = join_tail_bounded(stderr);
                    let status_kind = if status.success() {
                        CriterionStatus::Satisfied
                    } else {
                        CriterionStatus::Unsatisfied
                    };
                    return Some(self.result_with_output(
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
                    ));
                }
                Ok(false) if started.elapsed() < self.timeout => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(false) => {
                    terminate_child_group(&mut child);
                    let _ = join_tail_bounded(stdout);
                    let _ = join_tail_bounded(stderr);
                    return Some(self.result(
                        index,
                        key,
                        kind,
                        target,
                        CriterionStatus::Error,
                        command,
                        None,
                        "acceptance command timed out",
                    ));
                }
                Err(error) => {
                    terminate_child_group(&mut child);
                    let _ = join_tail_bounded(stdout);
                    let _ = join_tail_bounded(stderr);
                    return Some(self.result(
                        index,
                        key,
                        kind,
                        target,
                        CriterionStatus::Error,
                        command,
                        None,
                        &format!("failed to poll acceptance command: {error}"),
                    ));
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

fn spawn_with_busy_retry(
    process: &mut Command,
    deadline: Instant,
    cancel: &CancellationToken,
) -> std::io::Result<Child> {
    let mut retries = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "acceptance command cancelled",
            ));
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "acceptance command spawn deadline elapsed",
            ));
        }
        match process.spawn() {
            Ok(child) => return Ok(child),
            Err(error)
                if error.raw_os_error() == Some(26) && retries < EXECUTABLE_BUSY_MAX_RETRIES =>
            {
                retries += 1;
                let remaining = deadline.saturating_duration_since(Instant::now());
                thread::sleep(EXECUTABLE_BUSY_RETRY_DELAY.min(remaining));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(all(
    unix,
    not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
))]
fn child_exited_without_reap(pid: u32) -> std::io::Result<bool> {
    use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};

    let pid = i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| std::io::Error::other("invalid acceptance command pid"))?;
    waitid(
        WaitId::Pid(pid),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
    )
    .map(|status| status.is_some())
    .map_err(std::io::Error::from)
}

#[cfg(any(
    not(unix),
    target_os = "cygwin",
    target_os = "openbsd",
    target_os = "redox"
))]
fn child_exited_without_reap(_pid: u32) -> std::io::Result<bool> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "waitid WNOWAIT is unavailable",
    ))
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

fn terminate_child_group(child: &mut Child) {
    kill_process_group(child.id());
    let _ = child.kill();
    let _ = child.wait();
}

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

    #[cfg(all(
        unix,
        not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
    ))]
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

    #[cfg(all(
        unix,
        not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
    ))]
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

    #[cfg(all(
        unix,
        not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
    ))]
    #[test]
    fn successful_command_does_not_leave_background_descendants() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().expect("command tempdir");
        let script = directory.path().join("spawn-background");
        let escaped_marker = directory.path().join("escaped");
        std::fs::write(
            &script,
            "#!/bin/sh\n(sleep 1; echo escaped > \"$1\") &\nexit 0\n",
        )
        .expect("write background command");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))
            .expect("make background command executable");

        let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1))
            .expect("construct verifier");
        let result = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            format!("cmd:{} {}", script.display(), escaped_marker.display()),
        )]));
        assert_eq!(
            result.results[0].status,
            CriterionStatus::Satisfied,
            "{}",
            result.results[0].detail
        );
        thread::sleep(Duration::from_millis(1_200));
        assert!(
            !escaped_marker.exists(),
            "a successful acceptance command must not escape its process group"
        );
    }

    #[cfg(all(
        unix,
        not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
    ))]
    #[test]
    fn cancellation_stops_command_and_background_descendants() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().expect("command tempdir");
        let script = directory.path().join("cancel-background");
        let started = directory.path().join("started");
        let escaped = directory.path().join("escaped");
        std::fs::write(
            &script,
            "#!/bin/sh\n(sleep 2; : > \"$1\") &\n: > started\nsleep 5\n",
        )
        .expect("write background command");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))
            .expect("make background command executable");
        let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(10))
            .expect("construct verifier");
        let cancel = CancellationToken::new();
        let cancel_worker = cancel.clone();
        let started_worker = started.clone();
        let canceller = thread::spawn(move || {
            while !started_worker.exists() {
                thread::sleep(Duration::from_millis(5));
            }
            cancel_worker.cancel();
        });

        let result = verifier.verify_with_budget_and_cancel(
            &record(vec![AcceptanceCriterion::new(
                "custom",
                format!("cmd:{} {}", script.display(), escaped.display()),
            )]),
            Duration::from_secs(10),
            &cancel,
        );
        canceller.join().expect("join canceller");
        assert!(result.is_none());
        thread::sleep(Duration::from_millis(2_200));
        assert!(
            !escaped.exists(),
            "cancelled acceptance command must not escape its process group"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn command_spawn_retries_transient_executable_busy() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().expect("command tempdir");
        let script = directory.path().join("busy-script");
        std::fs::write(&script, "#!/bin/sh\nexit 0\n").expect("write command");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))
            .expect("make command executable");
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .open(&script)
            .expect("hold command open for writing");
        let release = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(writer);
        });

        let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1))
            .expect("construct verifier");
        let result = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            format!("cmd:{}", script.display()),
        )]));
        release.join().expect("release writer");
        assert_eq!(
            result.results[0].status,
            CriterionStatus::Satisfied,
            "{}",
            result.results[0].detail
        );
    }

    #[cfg(all(
        unix,
        not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
    ))]
    #[test]
    fn spawn_failure_is_reported_as_error() {
        let verifier = AcceptanceVerifier::new(".", Duration::from_secs(1)).expect("cwd");
        let result = verifier.verify(&record(vec![AcceptanceCriterion::new(
            "custom",
            "cmd:definitely-not-a-real-binary-vyane",
        )]));
        assert_eq!(result.results[0].status, CriterionStatus::Error);
    }

    #[cfg(all(
        unix,
        not(any(target_os = "cygwin", target_os = "openbsd", target_os = "redox"))
    ))]
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
