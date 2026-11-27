//! Read-only `vyane export` against a temp ledger.

#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::Output;

use assert_cmd::Command;
use chrono::{DateTime, Duration, Utc};
use predicates::prelude::*;
use serde_json::{Value, json};
use vyane_core::{
    AdapterTransport, ModelId, Protocol, ProviderId, RunRecord, RunStatus, Sandbox, Target, Usage,
};

const CSV_HEADER: &str = "timestamp,provider,status,duration_seconds,task_digest,task_preview,model,run_id,input_tokens,output_tokens,cost_usd";

fn vyane(data: &Path) -> Command {
    let mut command = Command::cargo_bin("vyane").unwrap();
    command.env("VYANE_DATA_DIR", data).env_remove("RUST_LOG");
    command
}

fn output_of(data: &Path, args: &[&str]) -> Output {
    vyane(data).args(args).output().unwrap()
}

fn success_stdout(data: &Path, args: &[&str]) -> String {
    let output = output_of(data, args);
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn record(
    run_id: &str,
    owner: &str,
    provider: &str,
    started_at: DateTime<Utc>,
    preview: Option<&str>,
    usage: Option<Usage>,
    cost_usd: Option<f64>,
) -> RunRecord {
    RunRecord {
        run_id: run_id.into(),
        owner: owner.into(),
        started_at,
        finished_at: started_at + Duration::seconds(10),
        task_digest: format!("digest-{run_id}"),
        task_preview: preview.map(str::to_owned),
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
        status: RunStatus::Success,
        usage,
        cost_usd,
        session_id: None,
        output_chars: None,
        error: Some("secret-error-text".into()),
        labels: [("hidden".into(), "not-exported".into())].into(),
    }
}

fn write_ledger(dir: &Path, records: &[RunRecord]) -> String {
    let mut body = String::new();
    for record in records {
        body.push_str(&serde_json::to_string(record).unwrap());
        body.push('\n');
    }
    std::fs::write(dir.join("ledger.jsonl"), &body).unwrap();
    body
}

fn assert_ledger_unchanged(dir: &Path, before: &str) {
    assert_eq!(
        std::fs::read_to_string(dir.join("ledger.jsonl")).unwrap(),
        before
    );
}

fn assert_public_projection(body: &str) {
    assert!(!body.contains("secret-workdir"), "{body}");
    assert!(!body.contains("secret-error-text"), "{body}");
    assert!(!body.contains("not-exported"), "{body}");
    assert!(!body.contains("foreign preview"), "{body}");
    assert!(body.contains("digest-newrun01"), "{body}");
}

#[test]
fn export_writes_three_formats_without_changing_the_ledger_or_history() {
    let root = common::private_tempdir();
    let now = Utc::now();
    let records = vec![
        record(
            "oldrun01",
            "local",
            "gemini",
            now - Duration::hours(48),
            None,
            None,
            None,
        ),
        record(
            "otherrun",
            "someone",
            "codex",
            now - Duration::hours(1),
            Some("foreign preview"),
            None,
            None,
        ),
        record(
            "midrun01",
            "local",
            "codex",
            now - Duration::hours(3),
            None,
            None,
            None,
        ),
        record(
            "newrun01",
            "local",
            "codex",
            now - Duration::hours(2),
            Some("legacy preview"),
            Some(Usage {
                input_tokens: 1000,
                output_tokens: 500,
                ..Usage::default()
            }),
            Some(0.25),
        ),
    ];
    let before = write_ledger(root.path(), &records);
    let history_before = success_stdout(root.path(), &["history"]);

    let csv = success_stdout(root.path(), &["export"]);
    let mut csv_lines = csv.lines();
    assert_eq!(csv_lines.next().unwrap(), CSV_HEADER);
    let first = csv_lines.next().unwrap();
    assert!(first.contains("newrun01"), "{first}");
    assert!(first.contains("legacy preview"), "{first}");
    assert!(first.contains("1000"), "{first}");
    assert!(first.contains("500"), "{first}");
    let second = csv_lines.next().unwrap();
    assert!(second.contains("midrun01"), "{second}");
    assert!(!second.contains("legacy preview"), "{second}");
    let third = csv_lines.next().unwrap();
    assert!(third.contains("oldrun01"), "{third}");
    assert!(third.contains("gemini"), "{third}");
    assert!(csv_lines.next().is_none(), "{csv}");
    assert_public_projection(&csv);

    let json_text = success_stdout(root.path(), &["export", "--format", "json"]);
    let json: Value = serde_json::from_str(&json_text).unwrap();
    assert!(json["exported_at"].is_string());
    assert_eq!(json["count"], 3);
    assert_eq!(json["entries"].as_array().unwrap().len(), 3);
    assert_eq!(json["statistics"]["total"], 3);
    assert_eq!(json["entries"][0]["run_id"], "newrun01");
    assert_eq!(json["entries"][0]["task_preview"], "legacy preview");
    assert_eq!(json["entries"][0]["task_digest"], "digest-newrun01");
    assert_eq!(json["entries"][0]["provider"], "codex");
    assert_eq!(json["entries"][0]["model"], "model-x");
    assert_eq!(json["entries"][0]["input_tokens"], 1000);
    assert_eq!(json["entries"][0]["output_tokens"], 500);
    assert!(json["entries"][1].get("task_preview").is_none());
    assert!(json["entries"][1]["input_tokens"].is_null());
    assert!(json.get("source").is_none());
    assert_public_projection(&json_text);

    let markdown = success_stdout(root.path(), &["export", "-f", "md"]);
    assert!(markdown.starts_with("# Vyane Report\n"), "{markdown}");
    assert!(markdown.contains("## Summary"), "{markdown}");
    assert!(markdown.contains("- **Total dispatches**: 3"), "{markdown}");
    assert!(markdown.contains("## Cost Summary"), "{markdown}");
    assert!(markdown.contains("## Recent Dispatches"), "{markdown}");
    assert!(markdown.contains("legacy preview"), "{markdown}");
    assert!(markdown.contains("*Generated at "), "{markdown}");
    assert_public_projection(&markdown);

    let recent = success_stdout(root.path(), &["export", "--hours", "24", "--format", "csv"]);
    assert!(recent.contains("newrun01"), "{recent}");
    assert!(recent.contains("midrun01"), "{recent}");
    assert!(!recent.contains("oldrun01"), "{recent}");
    assert!(!recent.contains("otherrun"), "{recent}");

    let gemini = success_stdout(
        root.path(),
        &["export", "--provider", "gemini", "-f", "json"],
    );
    let gemini: Value = serde_json::from_str(&gemini).unwrap();
    assert_eq!(gemini["count"], 1);
    assert_eq!(gemini["entries"][0]["run_id"], "oldrun01");

    let limited = success_stdout(root.path(), &["export", "-n", "1"]);
    assert_eq!(limited.lines().count(), 2, "{limited}");
    assert!(limited.lines().nth(1).unwrap().contains("newrun01"));

    let empty = success_stdout(root.path(), &["export", "--limit", "0", "-f", "json"]);
    let empty: Value = serde_json::from_str(&empty).unwrap();
    assert_eq!(empty["count"], 0);
    assert_eq!(empty["entries"], json!([]));

    assert_ledger_unchanged(root.path(), &before);
    assert_eq!(success_stdout(root.path(), &["history"]), history_before);
}

#[test]
fn export_output_flag_writes_a_file_and_leaves_the_ledger_unchanged() {
    let root = common::private_tempdir();
    let now = Utc::now();
    let before = write_ledger(
        root.path(),
        &[record(
            "newrun01",
            "local",
            "codex",
            now - Duration::minutes(5),
            None,
            None,
            None,
        )],
    );
    for (format, name) in [
        ("csv", "report.csv"),
        ("json", "report.json"),
        ("md", "report.md"),
    ] {
        let path = root.path().join(name);
        let output = output_of(
            root.path(),
            &[
                "export",
                "--format",
                format,
                "--output",
                path.to_str().unwrap(),
            ],
        );
        assert!(output.status.success(), "{format}: {output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(stdout, format!("Exported to {}\n", path.display()));
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("newrun01"), "{format}: {body}");
        assert!(!body.contains("secret-workdir"), "{body}");
        assert!(!stdout.contains("newrun01"), "{stdout}");
    }
    assert_ledger_unchanged(root.path(), &before);
}

#[test]
fn export_empty_ledger_is_a_valid_document_and_creates_no_ledger() {
    let root = common::private_tempdir();
    let csv = success_stdout(root.path(), &["export"]);
    assert_eq!(csv, format!("{CSV_HEADER}\n"));

    let json: Value =
        serde_json::from_str(&success_stdout(root.path(), &["export", "-f", "json"])).unwrap();
    assert!(json["exported_at"].is_string());
    assert_eq!(json["count"], 0);
    assert_eq!(json["entries"], json!([]));
    assert_eq!(json["statistics"]["total"], 0);

    let markdown = success_stdout(root.path(), &["export", "-f", "md"]);
    assert!(markdown.contains("# Vyane Report"), "{markdown}");
    assert!(markdown.contains("- **Total dispatches**: 0"), "{markdown}");
    assert!(!markdown.contains("## Recent Dispatches"), "{markdown}");

    let out = root.path().join("empty.csv");
    let written = output_of(root.path(), &["export", "-o", out.to_str().unwrap()]);
    assert!(written.status.success(), "{written:?}");
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        format!("{CSV_HEADER}\n")
    );
    assert!(!root.path().join("ledger.jsonl").exists());
}

#[test]
fn export_limit_zero_does_not_open_the_ledger() {
    let root = common::private_tempdir();
    let blocked = root.path().join("ledger.jsonl");
    std::fs::create_dir(&blocked).unwrap();
    let csv = success_stdout(root.path(), &["export", "-n", "0"]);
    assert_eq!(csv, format!("{CSV_HEADER}\n"));
    assert!(blocked.is_dir());
}

#[test]
fn export_rejects_an_unknown_format_and_does_not_touch_the_ledger() {
    let root = common::private_tempdir();
    let before = write_ledger(
        root.path(),
        &[record(
            "newrun01",
            "local",
            "codex",
            Utc::now(),
            None,
            None,
            None,
        )],
    );
    vyane(root.path())
        .args(["export", "--format", "xml"])
        .assert()
        .code(2)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("invalid value"));
    vyane(root.path())
        .args(["export", "-f", "markdown"])
        .assert()
        .code(2)
        .stdout(predicate::str::is_empty());
    vyane(root.path())
        .args(["export", "--hours", "-1"])
        .assert()
        .code(2);
    assert_ledger_unchanged(root.path(), &before);
}

#[test]
fn export_refuses_to_overwrite_the_ledger() {
    let root = common::private_tempdir();
    let before = write_ledger(
        root.path(),
        &[record(
            "newrun01",
            "local",
            "codex",
            Utc::now(),
            Some("legacy preview"),
            None,
            None,
        )],
    );
    let ledger = root.path().join("ledger.jsonl");
    vyane(root.path())
        .args(["export", "-o", ledger.to_str().unwrap()])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "refusing to write the export onto the run ledger",
        ));
    assert_ledger_unchanged(root.path(), &before);
}
