use serde::Serialize;
use vyane_core::{RunRecord, RunStatus, SessionRecord};
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

pub fn print_session_line(record: &SessionRecord) {
    println!(
        "{} {} {} {}",
        record.session_id,
        record.target,
        record.run_count,
        record.updated_at.to_rfc3339()
    );
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

fn task_duration(row_ms: Option<i64>) -> String {
    match row_ms {
        Some(ms) => format!("{ms}ms"),
        None => "-".to_string(),
    }
}

pub fn print_task_table(rows: &[TaskListRow]) {
    println!(
        "{:<36} {:<10} {:>12} {:<20} target",
        "id", "state", "duration", "started"
    );
    for row in rows {
        println!(
            "{:<36} {:<10} {:>12} {:<20} {}",
            row.id,
            row.state.as_str(),
            task_duration(row.duration_ms),
            row.started_at.to_rfc3339(),
            row.target
        );
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
