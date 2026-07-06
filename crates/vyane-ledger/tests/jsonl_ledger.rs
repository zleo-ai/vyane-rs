//! Acceptance tests for [`vyane_ledger::JsonlLedger`].
//!
//! These exercise the WP-05 acceptance list against real temp files:
//! concurrent-append integrity, corrupt-line tolerance, and every query filter.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::time::Duration;

use chrono::Utc;
use tempfile::TempDir;
use vyane_core::{
    AdapterTransport, Attempt, AttemptOutcome, Ledger, ModelId, Protocol, ProviderId, RunQuery,
    RunRecord, RunStatus, Sandbox, Target, Usage,
};
use vyane_ledger::JsonlLedger;

/// A builder for varied records so tests can pin specific fields per filter.
fn record(run_id: &str) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        owner: "local".into(),
        started_at: Utc::now(),
        finished_at: Utc::now(),
        task_digest: "0123456789abcdef".into(),
        task_preview: None,
        workdir: None,
        sandbox: Sandbox::ReadOnly,
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o-mini"),
        },
        transport: AdapterTransport::DirectHttp,
        attempts: Vec::new(),
        status: RunStatus::Success,
        usage: Some(Usage {
            input_tokens: 10,
            output_tokens: 5,
            ..Default::default()
        }),
        cost_usd: None,
        session_id: None,
        output_chars: Some(2),
        error: None,
        labels: Default::default(),
    }
}

fn attempt() -> Attempt {
    Attempt {
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o-mini"),
        },
        transport: AdapterTransport::DirectHttp,
        started_at: Utc::now(),
        duration_ms: 42,
        outcome: AttemptOutcome::Ok,
    }
}

#[tokio::test]
async fn append_then_query_roundtrips_in_reverse() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("runs.jsonl"));

    // Append oldest → newest. The query must hand them back newest → oldest.
    for i in 0..5 {
        let mut rec = record(&format!("run-{i}"));
        rec.started_at = Utc::now();
        // Spread timestamps so order is unambiguous; insert with a tiny gap.
        tokio::time::sleep(Duration::from_millis(2)).await;
        ledger.append(&rec).await.unwrap();
    }

    let got = ledger.query(RunQuery::default()).await.unwrap();
    assert_eq!(got.len(), 5);
    // Most-recent-first: the last appended is returned first.
    assert_eq!(got[0].run_id, "run-4");
    assert_eq!(got[4].run_id, "run-0");
    // Every record survived a full serialize/deserialize round-trip.
    assert_eq!(got[2].target.model.as_str(), "gpt-4o-mini");
    assert_eq!(got[2].attempts.len(), 0);
    assert_eq!(ledger.skipped_lines(), 0, "no corrupt lines were present");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_append_keeps_every_line_intact() {
    // Two tasks each append 100 records to the SAME ledger concurrently. After,
    // the file must hold exactly 200 valid JSONL lines — no loss, no
    // interleaved/corrupt bytes. The advisory lock serializes the writes.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("concurrent.jsonl");
    let ledger_a = JsonlLedger::new(path.clone());
    let ledger_b = JsonlLedger::new(path.clone());

    let make_task = |ledger: JsonlLedger, tag: &'static str| async move {
        for i in 0..100u32 {
            let mut rec = record(&format!("{tag}-{i}"));
            rec.task_preview = Some(format!("concurrent {tag} {i}"));
            rec.attempts = vec![attempt()];
            ledger.append(&rec).await.unwrap();
        }
    };

    let (ra, rb) = tokio::join!(make_task(ledger_a, "a"), make_task(ledger_b, "b"));
    let _ = (ra, rb);

    // The ledger on disk is the source of truth: count lines and parse each.
    let bytes = std::fs::read(&path).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let lines: Vec<&str> = text.split('\n').filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 200, "exactly 200 records, no loss");

    // Every line must parse — no interleaved corruption.
    let mut ids = HashSet::new();
    for line in &lines {
        let rec: RunRecord = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!("corrupt line after concurrent append: {e}\n  line: {line}")
        });
        assert!(
            ids.insert(rec.run_id.clone()),
            "duplicate run_id: {}",
            rec.run_id
        );
    }
    assert_eq!(ids.len(), 200);

    // And the query surface agrees: 200 records, none skipped.
    let ledger = JsonlLedger::new(&path);
    let got = ledger.query(RunQuery::default()).await.unwrap();
    assert_eq!(got.len(), 200);
    assert_eq!(ledger.skipped_lines(), 0);
}

#[tokio::test]
async fn corrupt_lines_are_skipped_and_counted() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mixed.jsonl");

    // Seed the file with valid records interleaved with garbage lines.
    let good_a = serde_json::to_string(&record("good-a")).unwrap();
    let good_b = serde_json::to_string(&record("good-b")).unwrap();
    let content = format!("{good_a}\nthis is not json\n{{\"run_id\":\"broken\",\n{good_b}\n   \n",);
    std::fs::write(&path, content).unwrap();

    let ledger = JsonlLedger::new(&path);
    let got = ledger.query(RunQuery::default()).await.unwrap();

    // Only the two valid records survive, in most-recent-first order.
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].run_id, "good-b");
    assert_eq!(got[1].run_id, "good-a");
    // Two non-empty lines failed to parse (the garbage and the truncated JSON);
    // the blank line is ignored, not counted.
    assert!(
        ledger.skipped_lines() >= 1,
        "corrupt lines must be counted, got {}",
        ledger.skipped_lines()
    );
}

#[tokio::test]
async fn query_filters_by_owner() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("owners.jsonl"));

    let mut alice = record("a1");
    alice.owner = "alice".into();
    let mut bob = record("b1");
    bob.owner = "bob".into();
    ledger.append(&alice).await.unwrap();
    ledger.append(&bob).await.unwrap();

    let q = RunQuery {
        owner: Some("alice".into()),
        ..Default::default()
    };
    let got = ledger.query(q).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].run_id, "a1");
}

#[tokio::test]
async fn query_filters_by_provider() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("providers.jsonl"));

    let mut openai = record("o1");
    openai.target.provider = ProviderId::new("openai");
    let mut anthropic = record("a1");
    anthropic.target.provider = ProviderId::new("anthropic");
    ledger.append(&openai).await.unwrap();
    ledger.append(&anthropic).await.unwrap();

    let q = RunQuery {
        provider: Some(ProviderId::new("anthropic")),
        ..Default::default()
    };
    let got = ledger.query(q).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].run_id, "a1");
}

#[tokio::test]
async fn query_filters_by_status() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("status.jsonl"));

    let mut ok = record("ok1");
    ok.status = RunStatus::Success;
    let mut err = record("err1");
    err.status = RunStatus::Error;
    ledger.append(&ok).await.unwrap();
    ledger.append(&err).await.unwrap();

    let q = RunQuery {
        status: Some(RunStatus::Error),
        ..Default::default()
    };
    let got = ledger.query(q).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].run_id, "err1");
}

#[tokio::test]
async fn query_filters_by_since() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("since.jsonl"));

    let old_time = Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cutoff = Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut old = record("old");
    old.started_at = old_time;
    let mut recent = record("recent");
    recent.started_at = Utc::now();
    ledger.append(&old).await.unwrap();
    ledger.append(&recent).await.unwrap();

    let q = RunQuery {
        since: Some(cutoff),
        ..Default::default()
    };
    let got = ledger.query(q).await.unwrap();
    assert_eq!(
        got.len(),
        1,
        "only the run started at/after the cutoff survives"
    );
    assert_eq!(got[0].run_id, "recent");
}

#[tokio::test]
async fn query_limit_caps_results_most_recent_first() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("limit.jsonl"));

    for i in 0..10 {
        let mut rec = record(&format!("r{i}"));
        rec.started_at = Utc::now();
        tokio::time::sleep(Duration::from_millis(2)).await;
        ledger.append(&rec).await.unwrap();
    }

    let q = RunQuery {
        limit: Some(3),
        ..Default::default()
    };
    let got = ledger.query(q).await.unwrap();
    assert_eq!(got.len(), 3);
    // The three most recent, in order.
    assert_eq!(got[0].run_id, "r9");
    assert_eq!(got[1].run_id, "r8");
    assert_eq!(got[2].run_id, "r7");
}

#[tokio::test]
async fn missing_file_queries_as_empty() {
    // A ledger that has never been written to is an empty ledger, not an error.
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("never.jsonl"));
    let got = ledger.query(RunQuery::default()).await.unwrap();
    assert!(got.is_empty());
    assert_eq!(ledger.skipped_lines(), 0);
}

#[tokio::test]
async fn filters_combine() {
    let dir = TempDir::new().unwrap();
    let ledger = JsonlLedger::new(dir.path().join("combo.jsonl"));

    let mut a = record("match");
    a.owner = "alice".into();
    a.target.provider = ProviderId::new("openai");
    a.status = RunStatus::Success;

    let mut b = record("wrong-owner");
    b.owner = "bob".into();

    let mut c = record("wrong-provider");
    c.target.provider = ProviderId::new("anthropic");

    let mut d = record("wrong-status");
    d.status = RunStatus::Timeout;

    for rec in [&a, &b, &c, &d] {
        ledger.append(rec).await.unwrap();
    }

    let q = RunQuery {
        owner: Some("alice".into()),
        provider: Some(ProviderId::new("openai")),
        status: Some(RunStatus::Success),
        ..Default::default()
    };
    let got = ledger.query(q).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].run_id, "match");
}
