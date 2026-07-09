//! Integration tests for the vyane-mcp tool layer.
//!
//! The macro-generated rmcp handler is awkward to drive directly in-process
//! (it needs a full transport), so these tests focus on the pieces a tool call
//! exercises between argument deserialization and the service boundary: the
//! schema structs, the sandbox/status parsers, and the JSON shape a tool
//! returns. That is exactly the surface that can regress silently.

#![allow(clippy::unwrap_used)]

use vyane_core::{RunStatus, Sandbox};
use vyane_mcp::{BroadcastArgs, DispatchArgs, HistoryArgs, parse_sandbox, parse_status};

#[test]
fn dispatch_args_schema_round_trip() {
    // A client always sends at least task + target; optionals may be omitted.
    let minimal = r#"{"task":"hello","target":"default"}"#;
    let parsed: DispatchArgs = serde_json::from_str(minimal).unwrap();
    assert_eq!(parsed.task, "hello");
    assert_eq!(parsed.target, "default");
    assert!(parsed.workdir.is_none());
    assert!(parsed.timeout_secs.is_none());

    // A fully-populated call survives the round-trip with every field intact.
    let full = r#"{
        "task": "ship it",
        "target": "openai/gpt-4o",
        "workdir": "/repo",
        "sandbox": "write",
        "session": "abc",
        "system": "be terse",
        "timeout_secs": 60
    }"#;
    let parsed: DispatchArgs = serde_json::from_str(full).unwrap();
    assert_eq!(parsed.task, "ship it");
    assert_eq!(parsed.target, "openai/gpt-4o");
    assert_eq!(parsed.workdir.as_deref(), Some("/repo"));
    assert_eq!(parsed.sandbox.as_deref(), Some("write"));
    assert_eq!(parsed.session.as_deref(), Some("abc"));
    assert_eq!(parsed.system.as_deref(), Some("be terse"));
    assert_eq!(parsed.timeout_secs, Some(60));
}

#[test]
fn broadcast_args_schema_round_trip() {
    let json = r#"{
        "task": "review",
        "targets": "codex,claude",
        "sandbox": "full"
    }"#;
    let parsed: BroadcastArgs = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.task, "review");
    assert_eq!(parsed.targets, "codex,claude");
    assert_eq!(parsed.sandbox.as_deref(), Some("full"));
    assert!(parsed.workdir.is_none());
}

#[test]
fn history_args_default_limit_applied() {
    // Omitting `limit` must yield the documented default of 20.
    let json = r#"{"provider":"openai"}"#;
    let parsed: HistoryArgs = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.limit, 20);
    assert_eq!(parsed.provider.as_deref(), Some("openai"));
    assert!(parsed.status.is_none());
}

#[test]
fn history_args_explicit_limit_respected() {
    let json = r#"{"limit": 3, "status": "error"}"#;
    let parsed: HistoryArgs = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.limit, 3);
    assert_eq!(parsed.status.as_deref(), Some("error"));
}

#[test]
fn sandbox_parser_matches_serde_representation() {
    // `write` and `full` are the only non-default spell; everything else,
    // including nonsense and `None`, is read-only.
    assert_eq!(parse_sandbox(Some("write".into())), Sandbox::Write);
    assert_eq!(parse_sandbox(Some("full".into())), Sandbox::Full);
    assert_eq!(parse_sandbox(None), Sandbox::ReadOnly);
    assert_eq!(parse_sandbox(Some("read-only".into())), Sandbox::ReadOnly);
    assert_eq!(parse_sandbox(Some("garbage".into())), Sandbox::ReadOnly);
}

#[test]
fn status_parser_handles_known_and_canceled_spelling() {
    assert_eq!(parse_status("success"), Some(RunStatus::Success));
    assert_eq!(parse_status("error"), Some(RunStatus::Error));
    assert_eq!(parse_status("timeout"), Some(RunStatus::Timeout));
    assert_eq!(parse_status("cancelled"), Some(RunStatus::Cancelled));
    assert_eq!(parse_status("canceled"), Some(RunStatus::Cancelled));
    assert_eq!(parse_status("bogus"), None);
    assert_eq!(parse_status(""), None);
}

// ---- serialization shape tests ----------------------------------------------
//
// The tools return `RunRecord` and `SessionRecord` serialized into JSON via the
// `success_json` helper. These tests pin the exact shapes so a client parsing
// the result is not broken by a silent type change.

#[test]
fn run_record_serializes_into_dispatch_result_shape() {
    use chrono::Utc;
    use vyane_core::{AdapterTransport, Attempt, AttemptOutcome, RunRecord, Target, Usage};
    use vyane_core::{ModelId, Protocol, ProviderId};

    let record = RunRecord {
        run_id: "0198c0de-0000-7000-8000-000000000000".into(),
        owner: "local".into(),
        started_at: Utc::now(),
        finished_at: Utc::now(),
        task_digest: "abcd1234abcd1234".into(),
        task_preview: Some("hello".into()),
        workdir: None,
        sandbox: Sandbox::ReadOnly,
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o"),
        },
        transport: AdapterTransport::DirectHttp,
        attempts: vec![Attempt {
            target: Target {
                provider: ProviderId::new("openai"),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new("gpt-4o"),
            },
            transport: AdapterTransport::DirectHttp,
            started_at: Utc::now(),
            duration_ms: 42,
            outcome: AttemptOutcome::Ok,
        }],
        status: RunStatus::Success,
        usage: Some(Usage {
            input_tokens: 10,
            output_tokens: 5,
            ..Default::default()
        }),
        cost_usd: None,
        session_id: None,
        output_chars: Some(5),
        error: None,
        labels: Default::default(),
    };

    // This is the exact object `vyane_dispatch` returns on success.
    let payload = serde_json::json!({
        "record": record,
        "output": Some("hello".to_string()),
    });
    let text = serde_json::to_string(&payload).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["record"]["status"], "success");
    assert_eq!(parsed["record"]["sandbox"], "read-only");
    assert_eq!(parsed["output"], "hello");
    // A round-trip back into the typed struct proves the shape is lossless.
    let again: RunRecord = serde_json::from_value(parsed["record"].clone()).unwrap();
    assert_eq!(again.run_id, record.run_id);
}

#[test]
fn session_record_serializes_into_items_array_shape() {
    use chrono::Utc;
    use vyane_core::{ModelId, Protocol, ProviderId, SessionRecord, Target};

    let session = SessionRecord {
        session_id: "s1".into(),
        owner: "local".into(),
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o"),
        },
        native_session_id: None,
        transcript: Vec::new(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        run_count: 3,
    };

    // `vyane_sessions` wraps records in { "items": [...] }.
    let payload = serde_json::json!({ "items": [session] });
    let text = serde_json::to_string(&payload).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["items"][0]["session_id"], "s1");
    assert_eq!(parsed["items"][0]["run_count"], 3);
}
