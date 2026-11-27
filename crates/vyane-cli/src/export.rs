//! Read-only `vyane export`.
//!
//! The live `ledger.jsonl` is the only source. Each row is a projection of a
//! [`RunRecord`]: `task_digest` always, and `task_preview` only when that row
//! already stored one. This command does not write the ledger and does not
//! read `history-archive/`.

use std::io::{self, Write as _};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use vyane_core::{Ledger, ProviderId, RunQuery, RunRecord};
use vyane_ledger::JsonlLedger;
use vyane_service::{HistoryStats, HistoryTokenTotal, StoragePaths};

use crate::cli::{ExportArgs, ExportFormat};

const CSV_HEADER: &str = "timestamp,provider,status,duration_seconds,task_digest,task_preview,model,run_id,input_tokens,output_tokens,cost_usd";
const MARKDOWN_TABLE_CAP: usize = 50;
const MARKDOWN_PREVIEW_CHARS: usize = 60;
const LOCAL_OWNER: &str = "local";

pub async fn run(args: ExportArgs) -> Result<ExitCode> {
    let paths = StoragePaths::resolve()?;
    if let Some(output) = &args.output
        && paths_name_same_file(output, &paths.ledger_path)
    {
        bail!("refusing to write the export onto the run ledger");
    }
    let exported_at = Utc::now();
    let records = load_records(&args, &paths.ledger_path, exported_at).await?;
    let content = render_export(args.format, &records, exported_at)?;
    write_export(args.output.as_deref(), &content)?;
    Ok(ExitCode::SUCCESS)
}

async fn load_records(
    args: &ExportArgs,
    ledger_path: &Path,
    exported_at: DateTime<Utc>,
) -> Result<Vec<RunRecord>> {
    if args.limit == 0 {
        return Ok(Vec::new());
    }
    let since = since_from_hours(args.hours, exported_at)?;
    let provider = args
        .provider
        .as_deref()
        .filter(|provider| !provider.is_empty())
        .map(ProviderId::new);
    let ledger = JsonlLedger::new(ledger_path);
    ledger
        .query(RunQuery {
            owner: Some(LOCAL_OWNER.to_owned()),
            provider,
            status: None,
            since,
            until: None,
            limit: Some(args.limit),
        })
        .await
        .context("read run ledger")
}

fn since_from_hours(hours: f64, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    if hours <= 0.0 {
        return Ok(None);
    }
    let millis = (hours * 3_600_000.0).round();
    if !millis.is_finite() || millis >= (i64::MAX as f64) {
        bail!("--hours is too large");
    }
    let millis = millis as i64;
    let Some(offset) = chrono::Duration::try_milliseconds(millis) else {
        bail!("--hours is too large");
    };
    now.checked_sub_signed(offset)
        .map(Some)
        .context("--hours is too large")
}

fn render_export(
    format: ExportFormat,
    records: &[RunRecord],
    exported_at: DateTime<Utc>,
) -> Result<String> {
    let stats = HistoryStats::from_records(records);
    let entries: Vec<ExportEntry> = records.iter().map(ExportEntry::from_record).collect();
    let mut body = match format {
        ExportFormat::Csv => render_csv(&entries),
        ExportFormat::Json => render_json(&entries, &stats, exported_at)?,
        ExportFormat::Md => render_markdown(&entries, &stats, exported_at),
    };
    if !body.ends_with('\n') {
        body.push('\n');
    }
    Ok(body)
}

fn render_csv(entries: &[ExportEntry]) -> String {
    let mut body = String::from(CSV_HEADER);
    body.push('\n');
    for entry in entries {
        let fields = [
            entry.timestamp.clone(),
            entry.provider.clone(),
            entry.status.clone(),
            optional_f64(entry.duration_seconds),
            entry.task_digest.clone(),
            entry.task_preview.clone().unwrap_or_default(),
            entry.model.clone(),
            entry.run_id.clone(),
            optional_u64(entry.input_tokens),
            optional_u64(entry.output_tokens),
            optional_f64(entry.cost_usd),
        ];
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(&csv_field(field));
        }
        body.push('\n');
    }
    body
}

fn render_json(
    entries: &[ExportEntry],
    stats: &HistoryStats,
    exported_at: DateTime<Utc>,
) -> Result<String> {
    let document = ExportDocument {
        exported_at: format_timestamp(exported_at),
        count: entries.len(),
        statistics: stats,
        entries,
    };
    Ok(serde_json::to_string_pretty(&document)?)
}

fn render_markdown(
    entries: &[ExportEntry],
    stats: &HistoryStats,
    exported_at: DateTime<Utc>,
) -> String {
    let stamp = format_timestamp(exported_at);
    let mut lines = vec!["# Vyane Report".to_owned(), String::new()];
    push_summary(&mut lines, stats);
    push_provider_breakdown(&mut lines, stats);
    push_cost_summary(&mut lines, stats);
    push_recent_table(&mut lines, entries);
    lines.push(format!("*Generated at {stamp}*"));
    lines.join("\n")
}

fn push_summary(lines: &mut Vec<String>, stats: &HistoryStats) {
    lines.push("## Summary".to_owned());
    lines.push(String::new());
    lines.push(format!("- **Total dispatches**: {}", stats.counts.total));
    if let Some(rate) = stats.counts.success_rate() {
        lines.push(format!("- **Success rate**: {rate:.1}%"));
    }
    lines.push(String::new());
}

fn push_provider_breakdown(lines: &mut Vec<String>, stats: &HistoryStats) {
    if stats.by_provider.is_empty() {
        return;
    }
    lines.push("## Provider Breakdown".to_owned());
    lines.push(String::new());
    lines.push("| Provider | Calls | Success Rate | Avg Duration |".to_owned());
    lines.push("|----------|-------|-------------|-------------|".to_owned());
    for (provider, counts) in &stats.by_provider {
        let rate = counts
            .success_rate()
            .map_or_else(|| "n/a".to_owned(), |rate| format!("{rate:.1}%"));
        let average = stats
            .by_provider_duration
            .get(provider)
            .and_then(|duration| duration.mean_ms())
            .map_or_else(
                || "n/a".to_owned(),
                |mean_ms| {
                    let seconds = mean_ms / 1000.0;
                    format!("{seconds:.1}s")
                },
            );
        lines.push(format!(
            "| {} | {} | {rate} | {average} |",
            markdown_cell(provider, 80),
            counts.total
        ));
    }
    lines.push(String::new());
}

fn push_cost_summary(lines: &mut Vec<String>, stats: &HistoryStats) {
    let has_tokens = stats.usage.input_tokens.known > 0 || stats.usage.output_tokens.known > 0;
    if stats.cost.known == 0 && !has_tokens {
        return;
    }
    lines.push("## Cost Summary".to_owned());
    lines.push(String::new());
    let cost = match (stats.cost.known_total_usd(), stats.cost.overflowed) {
        (Some(total), _) => format!("- **Known saved cost**: ${total:.4}"),
        (None, true) => "- **Known saved cost**: n/a (sum overflowed)".to_owned(),
        (None, false) => "- **Known saved cost**: n/a".to_owned(),
    };
    lines.push(cost);
    if has_tokens {
        lines.push(format!("- **Total tokens**: {}", token_summary(stats)));
    }
    lines.push(String::new());
}

fn push_recent_table(lines: &mut Vec<String>, entries: &[ExportEntry]) {
    if entries.is_empty() {
        return;
    }
    lines.push("## Recent Dispatches".to_owned());
    lines.push(String::new());
    lines.push("| Time | Provider | Status | Duration | Digest | Preview |".to_owned());
    lines.push("|------|----------|--------|----------|--------|---------|".to_owned());
    for entry in entries.iter().take(MARKDOWN_TABLE_CAP) {
        let duration = entry
            .duration_seconds
            .map_or_else(|| "n/a".to_owned(), |seconds| format!("{seconds:.1}s"));
        lines.push(format!(
            "| {} | {} | {} | {duration} | {} | {} |",
            markdown_cell(&entry.timestamp, 40),
            markdown_cell(&entry.provider, 40),
            markdown_cell(&entry.status, 20),
            markdown_cell(&entry.task_digest, 64),
            markdown_cell(
                entry.task_preview.as_deref().unwrap_or(""),
                MARKDOWN_PREVIEW_CHARS
            ),
        ));
    }
    if entries.len() > MARKDOWN_TABLE_CAP {
        lines.push(String::new());
        lines.push(format!(
            "_Showing {MARKDOWN_TABLE_CAP} of {} exported rows._",
            entries.len()
        ));
    }
    lines.push(String::new());
}

fn token_summary(stats: &HistoryStats) -> String {
    let usage = &stats.usage;
    let mut line = format!(
        "{} in / {} out",
        token_amount(&usage.input_tokens),
        token_amount(&usage.output_tokens)
    );
    if let Some(value) = usage
        .cached_input_tokens
        .total_tokens()
        .filter(|value| *value > 0)
    {
        line.push_str(&format!(" / {} cached_input", grouped(value)));
    }
    if let Some(value) = usage
        .reasoning_tokens
        .total_tokens()
        .filter(|value| *value > 0)
    {
        line.push_str(&format!(" / {} reasoning", grouped(value)));
    }
    line
}

fn token_amount(total: &HistoryTokenTotal) -> String {
    total
        .total_tokens()
        .map_or_else(|| "n/a".to_owned(), grouped)
}

fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut reversed = String::new();
    for (index, ch) in digits.chars().rev().enumerate() {
        if index > 0 && index.is_multiple_of(3) {
            reversed.push(',');
        }
        reversed.push(ch);
    }
    reversed.chars().rev().collect()
}

fn markdown_cell(text: &str, max_chars: usize) -> String {
    let flat: String = text
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    let mut truncated: String = flat.chars().take(max_chars).collect();
    if flat.chars().count() > max_chars {
        truncated.push_str("...");
    }
    truncated.replace('|', "\\|")
}

fn write_export(output: Option<&Path>, content: &str) -> Result<()> {
    match output {
        Some(path) => {
            std::fs::write(path, content)
                .with_context(|| format!("write export to {}", path.display()))?;
            println!("Exported to {}", path.display());
        }
        None => {
            let mut stdout = io::stdout().lock();
            write!(stdout, "{content}")?;
            stdout.flush()?;
        }
    }
    Ok(())
}

fn paths_name_same_file(left: &Path, right: &Path) -> bool {
    if let (Ok(left), Ok(right)) = (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        return left == right;
    }
    normalized_absolute(left) == normalized_absolute(right)
}

fn normalized_absolute(path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(path)
    } else {
        path.to_path_buf()
    };
    let mut normal = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other),
        }
    }
    normal
}

fn format_timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn csv_field(value: &str) -> String {
    let quote = value
        .bytes()
        .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'));
    if quote {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn optional_f64(value: Option<f64>) -> String {
    value.map(|number| number.to_string()).unwrap_or_default()
}

fn optional_u64(value: Option<u64>) -> String {
    value.map(|number| number.to_string()).unwrap_or_default()
}

#[derive(Debug, Serialize)]
struct ExportDocument<'a> {
    exported_at: String,
    count: usize,
    statistics: &'a HistoryStats,
    entries: &'a [ExportEntry],
}

#[derive(Debug, Serialize)]
struct ExportEntry {
    timestamp: String,
    provider: String,
    status: String,
    duration_seconds: Option<f64>,
    task_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_preview: Option<String>,
    model: String,
    run_id: String,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cost_usd: Option<f64>,
}

impl ExportEntry {
    fn from_record(record: &RunRecord) -> Self {
        let usage = record.usage.as_ref();
        Self {
            timestamp: format_timestamp(record.started_at),
            provider: record.target.provider.to_string(),
            status: record.status.as_str().to_owned(),
            duration_seconds: duration_seconds(record),
            task_digest: record.task_digest.clone(),
            task_preview: record.task_preview.clone(),
            model: record.target.model.to_string(),
            run_id: record.run_id.clone(),
            input_tokens: usage.map(|usage| usage.input_tokens),
            output_tokens: usage.map(|usage| usage.output_tokens),
            cost_usd: record.cost_usd.filter(|cost| cost.is_finite()),
        }
    }
}

fn duration_seconds(record: &RunRecord) -> Option<f64> {
    let elapsed = record.finished_at.signed_duration_since(record.started_at);
    if elapsed < chrono::Duration::zero() {
        return None;
    }
    let millis = elapsed.num_milliseconds();
    if millis < 0 {
        return None;
    }
    Some((millis as u64) as f64 / 1000.0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use vyane_core::{AdapterTransport, ModelId, Protocol, RunStatus, Sandbox, Target, Usage};

    fn at(seconds_after_epoch: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds_after_epoch, 0).single().unwrap()
    }

    fn sample(
        run_id: &str,
        provider: &str,
        started: DateTime<Utc>,
        status: RunStatus,
    ) -> RunRecord {
        RunRecord {
            run_id: run_id.into(),
            owner: LOCAL_OWNER.into(),
            started_at: started,
            finished_at: started + chrono::Duration::milliseconds(12_500),
            task_digest: format!("digest-{run_id}"),
            task_preview: None,
            workdir: Some("/tmp/secret-workdir".into()),
            sandbox: Sandbox::ReadOnly,
            target: Target {
                provider: ProviderId::new(provider),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new("model-x"),
            },
            transport: AdapterTransport::DirectHttp,
            attempts: vec![],
            status,
            usage: None,
            cost_usd: None,
            session_id: None,
            output_chars: None,
            error: Some("secret-error-text".into()),
            labels: [("hidden".into(), "not-exported".into())].into(),
        }
    }

    fn assert_no_private_text(body: &str) {
        assert!(!body.contains("secret-workdir"), "{body}");
        assert!(!body.contains("secret-error-text"), "{body}");
        assert!(!body.contains("not-exported"), "{body}");
    }

    #[test]
    fn csv_maps_columns_and_leaves_missing_preview_and_usage_empty() {
        let mut first = sample("abc123", "codex", at(1_700_000_000), RunStatus::Success);
        first.usage = Some(Usage {
            input_tokens: 1000,
            output_tokens: 500,
            ..Usage::default()
        });
        first.cost_usd = Some(0.0123);
        let mut second = sample("def456", "gemini", at(1_700_001_000), RunStatus::Error);
        second.task_preview = Some("legacy preview, \"quoted\"".into());
        second.finished_at = second.started_at - chrono::Duration::seconds(1);

        let csv = render_export(ExportFormat::Csv, &[first, second], at(1_800_000_000)).unwrap();
        let mut lines = csv.lines();
        assert_eq!(lines.next().unwrap(), CSV_HEADER);
        let codex = lines.next().unwrap();
        assert!(codex.contains("codex"), "{codex}");
        assert!(codex.contains("success"), "{codex}");
        assert!(codex.contains("12.5"), "{codex}");
        assert!(codex.contains("digest-abc123"), "{codex}");
        assert!(codex.contains(",1000,500,0.0123"), "{codex}");
        assert!(
            !codex.contains("legacy preview"),
            "a row without task_preview must not gain one: {codex}"
        );
        let gemini = lines.next().unwrap();
        assert!(
            gemini.contains("\"legacy preview, \"\"quoted\"\"\""),
            "{gemini}"
        );
        assert!(gemini.contains("digest-def456"), "{gemini}");
        assert!(
            gemini.split(',').any(|field| field.is_empty()),
            "reversed duration stays empty: {gemini}"
        );
        assert!(lines.next().is_none());
        assert_eq!(gemini.split(',').nth(3).unwrap(), "");
        let headers: Vec<_> = CSV_HEADER.split(',').collect();
        assert!(headers.contains(&"timestamp"));
        assert!(headers.contains(&"duration_seconds"));
        assert!(headers.contains(&"task_digest"));
        assert!(headers.contains(&"task_preview"));
        assert!(headers.contains(&"input_tokens"));
        assert!(headers.contains(&"output_tokens"));
        assert!(!headers.contains(&"task"));
        assert!(!headers.contains(&"source"));
        assert_no_private_text(&csv);
    }

    #[test]
    fn json_document_has_count_entries_and_statistics() {
        let mut record = sample("abc123", "codex", at(1_700_000_000), RunStatus::Success);
        record.usage = Some(Usage {
            input_tokens: 1000,
            output_tokens: 500,
            cached_input_tokens: Some(7),
            reasoning_tokens: Some(3),
        });
        record.cost_usd = Some(0.0123);
        record.task_preview = Some("kept preview".into());
        let plain = sample("def456", "gemini", at(1_700_001_000), RunStatus::Error);

        let json = render_export(ExportFormat::Json, &[record, plain], at(1_800_000_000)).unwrap();
        let body: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(body["exported_at"], "2027-01-15T08:00:00Z");
        assert_eq!(body["count"], 2);
        assert_eq!(body["statistics"]["total"], 2);
        assert_eq!(body["statistics"]["by_provider"]["codex"]["success"], 1);
        assert_eq!(body["entries"][0]["provider"], "codex");
        assert_eq!(body["entries"][0]["status"], "success");
        assert_eq!(body["entries"][0]["task_digest"], "digest-abc123");
        assert_eq!(body["entries"][0]["task_preview"], "kept preview");
        assert_eq!(body["entries"][0]["input_tokens"], 1000);
        assert_eq!(body["entries"][0]["output_tokens"], 500);
        assert_eq!(body["entries"][0]["duration_seconds"], 12.5);
        assert_eq!(body["entries"][0]["model"], "model-x");
        assert_eq!(body["entries"][0]["run_id"], "abc123");
        assert_eq!(body["entries"][0]["timestamp"], "2023-11-14T22:13:20Z");
        assert!(body["entries"][0].get("task").is_none());
        assert!(body["entries"][0].get("source").is_none());
        assert!(body["entries"][1].get("task_preview").is_none());
        assert!(body["entries"][1]["input_tokens"].is_null());
        assert_no_private_text(&json);
    }

    #[test]
    fn empty_documents_are_valid() {
        let when = at(1_800_000_000);
        let csv = render_export(ExportFormat::Csv, &[], when).unwrap();
        assert_eq!(format!("{CSV_HEADER}\n"), csv);

        let json = render_export(ExportFormat::Json, &[], when).unwrap();
        let body: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(body["count"], 0);
        assert_eq!(body["entries"], serde_json::json!([]));
        assert_eq!(body["statistics"]["total"], 0);
        assert!(body["exported_at"].is_string());

        let markdown = render_export(ExportFormat::Md, &[], when).unwrap();
        assert!(markdown.contains("# Vyane Report"), "{markdown}");
        assert!(markdown.contains("- **Total dispatches**: 0"), "{markdown}");
        assert!(!markdown.contains("## Recent Dispatches"), "{markdown}");
        assert!(!markdown.contains("## Cost Summary"), "{markdown}");
        assert!(
            markdown.contains("*Generated at 2027-01-15T08:00:00Z*"),
            "{markdown}"
        );
    }

    #[test]
    fn markdown_caps_the_table_and_reports_saved_cost() {
        let mut records = Vec::new();
        for index in 0..60 {
            let mut record = sample(
                &format!("run-{index:02}"),
                "codex",
                at(1_700_000_000 + index),
                RunStatus::Success,
            );
            record.usage = Some(Usage {
                input_tokens: 100,
                output_tokens: 40,
                ..Usage::default()
            });
            record.cost_usd = Some(0.01);
            if index == 0 {
                record.task_preview = Some(format!("preview|line\n{}", "x".repeat(80)));
            }
            records.push(record);
        }
        let markdown = render_export(ExportFormat::Md, &records, at(1_800_000_000)).unwrap();
        assert!(markdown.contains("# Vyane Report"), "{markdown}");
        assert!(markdown.contains("## Summary"), "{markdown}");
        assert!(
            markdown.contains("- **Total dispatches**: 60"),
            "{markdown}"
        );
        assert!(markdown.contains("## Provider Breakdown"), "{markdown}");
        assert!(markdown.contains("## Cost Summary"), "{markdown}");
        assert!(markdown.contains("$0.6000"), "{markdown}");
        assert!(markdown.contains("6,000 in / 2,400 out"), "{markdown}");
        let recent = markdown.split("## Recent Dispatches").nth(1).unwrap();
        let rows = recent
            .lines()
            .filter(|line| line.starts_with("| 2023-"))
            .count();
        assert_eq!(rows, MARKDOWN_TABLE_CAP);
        assert!(
            recent.contains("_Showing 50 of 60 exported rows._"),
            "{recent}"
        );
        assert!(recent.contains("preview\\|line"), "{recent}");
        assert!(!recent.contains(&"x".repeat(80)), "{recent}");
        assert_no_private_text(&markdown);
    }

    #[test]
    fn hour_window_is_zero_or_a_finite_lookback() {
        let now = at(1_700_000_000);
        assert_eq!(since_from_hours(0.0, now).unwrap(), None);
        assert_eq!(
            since_from_hours(24.0, now).unwrap(),
            Some(now - chrono::Duration::hours(24))
        );
        assert_eq!(
            since_from_hours(0.5, now).unwrap(),
            Some(now - chrono::Duration::minutes(30))
        );
        assert!(since_from_hours(1.0e18, now).is_err());
    }
}
