use serde::Serialize;
use vyane_core::{RunRecord, RunStatus, SessionRecord};
use vyane_service::SessionView;
use vyane_task::TaskRecord;
use vyane_workflow::{WorkflowJournalSummary, WorkflowOutcome, WorkflowRunStatus};

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
    match status {
        RunStatus::Success => "success",
        RunStatus::Error => "error",
        RunStatus::Timeout => "timeout",
        RunStatus::Cancelled => "cancelled",
    }
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

pub fn print_record_line(record: &RunRecord) {
    let cost = record
        .cost_usd
        .map(|cost| format!(" ${cost:.6}"))
        .unwrap_or_default();
    println!(
        "{} {} {} {} {}ms{}",
        short_run_id(&record.run_id),
        record.started_at.to_rfc3339(),
        target_selector(record),
        status_name(record.status),
        duration_ms(record),
        cost
    );
}

pub fn print_legacy_session_line(record: &SessionRecord) {
    println!(
        "{} {} {} {}",
        record.session_id,
        record.target,
        record.run_count,
        record.updated_at.to_rfc3339()
    );
}

pub fn print_session_view_line(record: &SessionView) {
    let session_id = terminal_safe(&record.session_id);
    let target = terminal_safe(&record.target.to_string());
    println!(
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
    );
}

fn terminal_safe(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

pub fn print_broadcast_table(rows: &[BroadcastRow]) {
    println!(
        "{:<24} {:<10} {:>10} output",
        "target", "status", "duration"
    );
    for row in rows {
        match &row.record {
            Some(record) => println!(
                "{:<24} {:<10} {:>8}ms {}",
                row.target,
                status_name(record.status),
                duration_ms(record),
                first_line(row.output.as_deref())
            ),
            None => println!(
                "{:<24} {:<10} {:>10} {}",
                row.target,
                "error",
                "-",
                row.error.as_deref().unwrap_or("")
            ),
        }
    }
}

pub fn workflow_status_name(status: WorkflowRunStatus) -> &'static str {
    match status {
        WorkflowRunStatus::Running => "running",
        WorkflowRunStatus::Completed => "completed",
        WorkflowRunStatus::CompletedWithFailures => "completed_with_failures",
        WorkflowRunStatus::Failed => "failed",
        WorkflowRunStatus::Cancelled => "cancelled",
    }
}

pub fn print_workflow_summary(outcome: &WorkflowOutcome) {
    println!(
        "workflow {} {}",
        outcome.wf_run_id,
        workflow_status_name(outcome.status)
    );
    println!("{}", outcome.journal_path.display());
    println!("{:<24} {:<10} runs output", "step", "status");
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
        println!(
            "{:<24} {:<10} {:>4} {}",
            id,
            format!("{:?}", step.status).to_lowercase(),
            step.run_ids.len(),
            first_line(output)
        );
    }
}

pub fn print_workflow_list(rows: &[WorkflowJournalSummary]) {
    println!(
        "{:<36} {:<24} {:<24} {:<10} steps",
        "id", "started_at", "name", "status"
    );
    for row in rows {
        let counts = &row.steps;
        println!(
            "{:<36} {:<24} {:<24} {:<10} {}/{} ok, {} failed, {} skipped, {} cancelled",
            row.id,
            row.started_at.to_rfc3339(),
            row.name,
            workflow_status_name(row.status),
            counts.success,
            counts.pending
                + counts.running
                + counts.success
                + counts.failed
                + counts.skipped
                + counts.cancelled,
            counts.failed,
            counts.skipped,
            counts.cancelled
        );
    }
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

pub fn print_task_table(rows: &[TaskRow]) {
    println!(
        "{:<36} {:<12} {:>12} {:<20} target",
        "id", "state", "duration", "created"
    );
    for row in rows {
        println!(
            "{:<36} {:<12} {:>12} {:<20} {}",
            row.id,
            row.state,
            task_duration(row.duration_ms),
            row.created_at.to_rfc3339(),
            row.target
        );
    }
}

pub fn print_durable_task_status(task: &TaskRecord, log_tail: &[String]) {
    println!("id:         {}", task.id);
    println!("state:      {}", task.state);
    println!("origin:     {}", task.origin);
    println!("target:     {}", task.target_key);
    println!("digest:     {}", task.task_digest);
    println!("revision:   {}", task.revision);
    println!("epoch:      {}", task.executor_epoch);
    println!("created_at: {}", task.created_at.to_rfc3339());
    if let Some(started) = task.started_at {
        println!("started_at: {}", started.to_rfc3339());
    }
    if let Some(finished) = task.finished_at {
        println!("finished:   {}", finished.to_rfc3339());
    }
    if let Some(ledger) = &task.ledger_run_id {
        println!("ledger:     {ledger}");
    }
    if let Some(code) = task.failure_code {
        println!("failure:    {code}");
    }
    if log_tail.is_empty() {
        println!("log:        (empty)");
    } else {
        println!("log (last {} lines):", log_tail.len());
        for line in log_tail {
            println!("  {line}");
        }
    }
}

pub fn print_task_status(status: &StatusFile, displayed: TaskState, log_tail: &[String]) {
    println!("id:         {}", status.run_id);
    println!("state:      {}", displayed.as_str());
    println!("pid:        {}", status.pid);
    println!("pgid:       {}", status.pgid);
    println!("target:     {}", status.target);
    if let Some(workdir) = &status.workdir {
        println!("workdir:    {workdir}");
    }
    println!("started_at: {}", status.started_at.to_rfc3339());
    if let Some(finished) = status.finished_at {
        println!("finished:   {}", finished.to_rfc3339());
    }
    if let Some(ms) = status.duration_ms() {
        println!("duration:   {ms}ms");
    }
    if let Some(ledger) = &status.ledger_run_id {
        println!("ledger:     {ledger}");
    }
    if let Some(error) = &status.error {
        println!("error:      {error}");
    }
    if log_tail.is_empty() {
        println!("log:        (empty)");
    } else {
        println!("log (last {} lines):", log_tail.len());
        for line in log_tail {
            println!("  {line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::terminal_safe;

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
}
