use serde::Serialize;
use vyane_core::{RunRecord, RunStatus, SessionRecord};

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
