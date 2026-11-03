//! `vyane feedback` and `vyane history --outcomes` against real recorded runs.
//!
//! Runs are produced by dispatching to a local wiremock endpoint, so the
//! ledger rows are the kernel's own records rather than hand-written fixtures.
//! The final test pins ADR 0001: recorded ratings never change a route.

#![allow(clippy::unwrap_used)]

mod common;

use std::fs;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY_ENV: &str = "VYANE_FEEDBACK_TEST_KEY";

fn vyane(data: &Path, config: &Path) -> Command {
    let mut command = Command::cargo_bin("vyane").unwrap();
    command
        .env(KEY_ENV, "sk-test")
        .env("VYANE_DATA_DIR", data)
        .arg("--config")
        .arg(config);
    command
}

fn config_for(server: &MockServer) -> String {
    format!(
        r#"
        [providers.alpha]
        base_url = "{uri}"
        api_key_env = "{KEY_ENV}"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "alpha-model"

        [providers.beta]
        base_url = "{uri}"
        api_key_env = "{KEY_ENV}"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "beta-model"

        [profiles.fast]
        provider = "alpha"
        protocol = "openai_chat"
        harness = "none"
        model = "alpha-model"
        tier = "economy"
        tags = ["code"]

        [profiles.deep]
        provider = "beta"
        protocol = "openai_chat"
        harness = "none"
        model = "beta-model"
        tier = "frontier"
        tags = ["code"]
        "#,
        uri = server.uri()
    )
}

async fn mock_chat(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl-feedback",
            "model": "fixture-model",
            "choices": [{
                "message": { "role": "assistant", "content": "fixture answer" },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
        })))
        .mount(server)
        .await;
}

struct Fixture {
    _server: MockServer,
    _config_dir: tempfile::TempDir,
    data: tempfile::TempDir,
    config: std::path::PathBuf,
}

impl Fixture {
    async fn start() -> Self {
        let server = MockServer::start().await;
        mock_chat(&server).await;
        let config_dir = common::private_tempdir();
        let config = config_dir.path().join("config.toml");
        fs::write(&config, config_for(&server)).unwrap();
        Self {
            _server: server,
            _config_dir: config_dir,
            data: common::private_tempdir(),
            config,
        }
    }

    fn cmd(&self) -> Command {
        vyane(self.data.path(), &self.config)
    }

    /// Dispatch one task and return the recorded run id.
    fn dispatch(&self, target: &str) -> String {
        let output = self
            .cmd()
            .args(["dispatch", "rate me", "--target", target, "--json"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let body: Value = serde_json::from_slice(&output).unwrap();
        body["record"]["run_id"].as_str().unwrap().to_owned()
    }

    fn json(&self, args: &[&str]) -> Value {
        let output = self
            .cmd()
            .args(args)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        serde_json::from_slice(&output).unwrap()
    }

    fn append_ledger_line(&self, line: &Value) {
        let path = self.data.path().join("ledger.jsonl");
        let mut text = fs::read_to_string(&path).unwrap_or_default();
        text.push_str(&line.to_string());
        text.push('\n');
        fs::write(path, text).unwrap();
    }
}

fn foreign_run(run_id: &str) -> Value {
    json!({
        "run_id": run_id, "owner": "someone-else",
        "started_at": "2026-01-01T00:00:00Z", "finished_at": "2026-01-01T00:00:01Z",
        "task_digest": "d", "sandbox": "read-only",
        "target": {"provider": "alpha", "protocol": "openai_chat", "model": "alpha-model"},
        "transport": "direct_http", "attempts": [], "status": "success"
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feedback_rate_records_validates_and_hides_absent_runs() {
    let fixture = Fixture::start().await;
    let run_id = fixture.dispatch("fast");

    let view = fixture.json(&["feedback", "rate", &run_id, "4", "--json"]);
    assert_eq!(view["run_id"], run_id.as_str());
    assert_eq!(view["rating"], 4);
    assert_eq!(view["source"], "human");
    assert_eq!(view["view_schema"], 1);
    assert!(view.get("owner").is_none());
    assert!(
        view["feedback_id"]
            .as_str()
            .is_some_and(|id| id.len() == 36)
    );

    let auto = fixture.json(&[
        "feedback",
        "rate",
        &run_id,
        "2",
        "--source",
        "auto:tests",
        "--json",
    ]);
    assert_eq!(auto["source"], "auto:tests");

    fixture
        .cmd()
        .args(["feedback", "rate", &run_id, "4"])
        .assert()
        .success()
        .stdout(predicate::str::contains(run_id.as_str()))
        .stdout(predicate::str::contains("rating 4"));

    for rating in ["9", "0", "four", "4.5", "256"] {
        fixture
            .cmd()
            .args(["feedback", "rate", &run_id, rating])
            .assert()
            .code(2)
            .stderr(predicate::str::contains(
                "rating must be an integer from 1 to 5",
            ));
    }
    for source in ["robot", "auto:", "auto:Tests", "auto:SOURCE_CANARY"] {
        fixture
            .cmd()
            .args(["feedback", "rate", &run_id, "3", "--source", source])
            .assert()
            .code(2)
            .stderr(predicate::str::contains("SOURCE_CANARY").not());
    }
    fixture
        .cmd()
        .args(["feedback", "rate", &run_id])
        .assert()
        .code(2);
    fixture
        .cmd()
        .args(["feedback", "rate", "--last", &run_id, "3"])
        .assert()
        .code(2);

    let absent = fixture
        .cmd()
        .args([
            "feedback",
            "rate",
            "0190f5d6-0000-7000-8000-000000000000",
            "4",
            "--json",
        ])
        .assert()
        .code(1)
        .get_output()
        .stderr
        .clone();
    let absent: Value = serde_json::from_slice(&absent).unwrap();
    assert_eq!(
        absent,
        json!({"status": "error", "error": {"kind": "not_found", "message": "run not found"}})
    );

    // Three valid appends; every rejected call wrote nothing.
    let lines = fs::read_to_string(fixture.data.path().join("feedback.jsonl")).unwrap();
    assert_eq!(lines.lines().count(), 3);
    let list = fixture.json(&["feedback", "list", "--json"]);
    // Folded: one human and one auto rating for the run.
    assert_eq!(list["items"].as_array().unwrap().len(), 2);
    assert_eq!(list["items"][0]["source"], "human");
    assert_eq!(list["items"][0]["rating"], 4);
    assert_eq!(list["skipped_lines"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feedback_treats_another_owners_run_and_ratings_as_absent() {
    let fixture = Fixture::start().await;
    let own = fixture.dispatch("fast");
    fixture.append_ledger_line(&foreign_run("run-foreign"));
    let foreign_feedback = json!({
        "schema": 1, "feedback_id": "0190f5d6-0000-7000-8000-000000000001",
        "owner": "someone-else", "run_id": "run-foreign", "rating": 1,
        "source": "human", "recorded_at": "2026-01-01T00:00:00Z"
    });
    fs::write(
        fixture.data.path().join("feedback.jsonl"),
        format!("{foreign_feedback}\n"),
    )
    .unwrap();

    let mut errors = Vec::new();
    for run_id in ["run-foreign", "run-absent"] {
        let stderr = fixture
            .cmd()
            .args(["feedback", "rate", run_id, "5", "--json"])
            .assert()
            .code(1)
            .get_output()
            .stderr
            .clone();
        errors.push(String::from_utf8(stderr).unwrap());
    }
    assert_eq!(
        errors[0], errors[1],
        "foreign and absent runs look identical"
    );

    fixture
        .cmd()
        .args(["feedback", "rate", &own, "3"])
        .assert()
        .success();
    let list = fixture.json(&["feedback", "list", "--json"]);
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["run_id"], own.as_str());
    assert!(!list.to_string().contains("run-foreign"));
    let filtered = fixture.json(&["feedback", "list", "--run", "run-foreign", "--json"]);
    assert_eq!(filtered["items"], json!([]));

    let outcomes = fixture.json(&["history", "--outcomes", "--group-by", "provider", "--json"]);
    assert_eq!(outcomes["total_runs"], 1);
    assert_eq!(outcomes["groups"][0]["rating_count"], 1);
    assert_eq!(outcomes["groups"][0]["average_rating"], 3.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feedback_rate_last_targets_the_most_recent_run() {
    let fixture = Fixture::start().await;
    fixture
        .cmd()
        .args(["feedback", "rate", "--last", "5", "--json"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("no runs recorded"));

    let _older = fixture.dispatch("fast");
    let newest = fixture.dispatch("deep");
    let view = fixture.json(&["feedback", "rate", "--last", "5", "--json"]);
    assert_eq!(view["run_id"], newest.as_str());
    assert_eq!(view["rating"], 5);
    fixture
        .cmd()
        .args(["feedback", "rate", "--last", "4"])
        .assert()
        .success()
        .stdout(predicate::str::contains(newest.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_outcomes_groups_real_runs_and_rejects_conflicting_modes() {
    let fixture = Fixture::start().await;
    let fast_a = fixture.dispatch("fast");
    let fast_b = fixture.dispatch("fast");
    let deep = fixture.dispatch("deep");
    for (run_id, rating, source) in [
        (&fast_a, "5", "human"),
        (&fast_b, "2", "human"),
        (&deep, "1", "auto:lint"),
    ] {
        fixture
            .cmd()
            .args(["feedback", "rate", run_id, rating, "--source", source])
            .assert()
            .success();
    }

    let report = fixture.json(&[
        "history",
        "--outcomes",
        "--group-by",
        "provider,model",
        "--json",
    ]);
    // Snapshot of the stable fields; timings are measured, so only their
    // sample counts are pinned.
    let mut stable = report.clone();
    for group in stable["groups"].as_array_mut().unwrap() {
        let duration = group["duration"].as_object_mut().unwrap();
        duration.remove("total_ms");
        duration.remove("mean_ms");
    }
    let token = |known: u64, total: Option<u64>| json!({"known": known, "unknown": 0, "total_tokens": total, "overflowed": false});
    let detail = |count: u64| json!({"known": 0, "unknown": count, "total_tokens": null, "overflowed": false});
    let group = |provider: &str, model: &str, runs: u64, ratings: u64, average: Value| {
        json!({
            "key": {"provider": provider, "model": model},
            "total": runs, "success": runs, "error": 0, "timeout": 0, "cancelled": 0,
            "success_rate": 100.0,
            "failover_count": 0, "failover_rate": 0.0,
            "duration": {"samples": runs, "invalid": 0},
            "cost": {"known": 0, "unknown": runs, "invalid": 0, "known_total_usd": null, "overflowed": false},
            "usage": {
                "input_tokens": token(runs, Some(3 * runs)),
                "output_tokens": token(runs, Some(2 * runs)),
                "reasoning_tokens": detail(runs),
                "cached_input_tokens": detail(runs)
            },
            "rating_count": ratings, "average_rating": average
        })
    };
    assert_eq!(
        stable,
        json!({
            "group_by": ["provider", "model"],
            "include_auto_ratings": false,
            "total_runs": 3,
            "groups": [
                group("alpha", "alpha-model", 2, 2, json!(3.5)),
                group("beta", "beta-model", 1, 0, Value::Null),
            ],
            "feedback_skipped_lines": 0
        })
    );

    let with_auto = fixture.json(&[
        "history",
        "--outcomes",
        "--group-by",
        "provider,model",
        "--include-auto-ratings",
        "--json",
    ]);
    assert_eq!(with_auto["include_auto_ratings"], true);
    assert_eq!(with_auto["groups"][1]["rating_count"], 1);
    assert_eq!(with_auto["groups"][1]["average_rating"], 1.0);

    let defaults = fixture.json(&["history", "--outcomes", "--json"]);
    assert_eq!(
        defaults["group_by"],
        json!(["provider", "harness", "model", "effort"])
    );
    assert_eq!(
        defaults["groups"][0]["key"],
        json!({"provider": "alpha", "harness": "direct", "model": "alpha-model", "effort": "unknown"})
    );
    let filtered = fixture.json(&["history", "--outcomes", "--provider", "beta", "--json"]);
    assert_eq!(filtered["total_runs"], 1);

    fixture
        .cmd()
        .args(["history", "--outcomes", "--group-by", "provider"])
        .assert()
        .success()
        .stdout(predicate::str::contains("PROVIDER\tTOTAL\tSUCCESS RATE"))
        .stdout(predicate::str::contains("alpha\t2\t100%"));

    for args in [
        vec!["history", "--outcomes", "--stats"],
        vec!["history", "--outcomes", "--limit", "5"],
        vec!["history", "--outcomes", "--session", "s"],
        vec!["history", "--outcomes", "--group-by", "vendor"],
        vec!["history", "--outcomes", "--group-by", "provider,provider"],
        vec![
            "history",
            "--outcomes",
            "--group-by",
            "provider,protocol,harness,model,effort",
        ],
        vec!["history", "--group-by", "provider"],
        vec!["history", "--include-auto-ratings"],
        vec![
            "history",
            "--outcomes",
            "--since",
            "2026-01-02T00:00:00Z",
            "--until",
            "2026-01-01T00:00:00Z",
        ],
    ] {
        fixture.cmd().args(&args).assert().code(2);
    }
    // Rejected groupings are reported with a fixed message, never echoed.
    fixture
        .cmd()
        .args(["history", "--outcomes", "--group-by", "GROUP_CANARY"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("group_by accepts provider"))
        .stderr(predicate::str::contains("GROUP_CANARY").not());
    // So are rejected statuses, in every history mode; `canceled` is the
    // same alias REST and MCP accept.
    for mode in [&["--outcomes"][..], &["--stats"][..], &[][..]] {
        fixture
            .cmd()
            .arg("history")
            .args(mode)
            .args(["--status", "STATUS_CANARY"])
            .assert()
            .code(2)
            .stderr(predicate::str::contains(
                "status must be one of success, error, timeout, or cancelled",
            ))
            .stderr(predicate::str::contains("STATUS_CANARY").not());
    }
    let canceled = fixture.json(&["history", "--outcomes", "--status", "canceled", "--json"]);
    assert_eq!(canceled["total_runs"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ratings_never_change_a_route_decision() {
    let fixture = Fixture::start().await;
    let route = |task: &str| {
        fixture
            .cmd()
            .args(["route", task, "--json"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone()
    };
    let tasks = [
        "fix a typo in the readme",
        "design a distributed consensus protocol with formal proofs",
    ];
    let before: Vec<Vec<u8>> = tasks.iter().map(|task| route(task)).collect();

    // Rate every run of both profiles as badly as possible, many times over.
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(fixture.dispatch("fast"));
        runs.push(fixture.dispatch("deep"));
    }
    for run_id in &runs {
        for source in ["human", "auto:bench"] {
            for _ in 0..4 {
                fixture
                    .cmd()
                    .args(["feedback", "rate", run_id, "1", "--source", source])
                    .assert()
                    .success();
            }
        }
    }
    let report = fixture.json(&["history", "--outcomes", "--group-by", "provider", "--json"]);
    assert!(
        report["groups"]
            .as_array()
            .unwrap()
            .iter()
            .all(|group| group["average_rating"] == 1.0)
    );

    let after: Vec<Vec<u8>> = tasks.iter().map(|task| route(task)).collect();
    assert_eq!(before, after, "ratings must not influence routing");
}
