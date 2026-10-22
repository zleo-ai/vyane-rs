//! Integration tests for the vyane-mcp tool layer.
//!
//! The macro-generated rmcp handler is awkward to drive directly in-process
//! (it needs a full transport), so these tests focus on the pieces a tool call
//! exercises between argument deserialization and the service boundary: the
//! schema structs, the sandbox/status parsers, and the JSON shape a tool
//! returns. That is exactly the surface that can regress silently.

#![allow(clippy::unwrap_used)]

use rmcp::{
    ServiceExt as _,
    model::{CallToolRequestParams, CallToolResult},
};
use vyane_core::{RunStatus, Sandbox};
use vyane_mcp::{
    BroadcastArgs, CheckArgs, DispatchArgs, HistoryArgs, RouteArgs, VyaneMcpServer, parse_sandbox,
    parse_status,
};

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
fn route_and_check_args_have_strict_stable_shapes() {
    let route: RouteArgs = serde_json::from_value(serde_json::json!({
        "task": "review the patch",
        "stage": "review",
        "changed_files": 12,
        "dependency_edges": 4,
        "retry_count": 1,
        "tier": "mainline",
        "tags": ["rust", "security"],
        "candidates": ["reviewer"],
        "allow_frontier": false,
    }))
    .unwrap();
    assert_eq!(route.task, "review the patch");
    assert_eq!(route.stage.as_deref(), Some("review"));
    assert_eq!(route.changed_files, Some(12));
    assert_eq!(route.dependency_edges, Some(4));
    assert_eq!(route.retry_count, Some(1));
    assert_eq!(route.tier.as_deref(), Some("mainline"));
    assert_eq!(
        route
            .tags
            .iter()
            .map(|value| value.0.as_str())
            .collect::<Vec<_>>(),
        ["rust", "security"]
    );
    assert_eq!(
        route
            .candidates
            .iter()
            .map(|value| value.0.as_str())
            .collect::<Vec<_>>(),
        ["reviewer"]
    );
    assert_eq!(route.allow_frontier, Some(false));

    let _: CheckArgs = serde_json::from_str("{}").unwrap();
    assert!(serde_json::from_str::<CheckArgs>(r#"{"path":"CANARY_PATH_VALUE"}"#).is_err());
}

#[test]
fn sandbox_parser_matches_serde_representation() {
    assert_eq!(parse_sandbox(Some("write")).unwrap(), Sandbox::Write);
    assert_eq!(parse_sandbox(Some("full")).unwrap(), Sandbox::Full);
    assert_eq!(parse_sandbox(None).unwrap(), Sandbox::ReadOnly);
    assert_eq!(parse_sandbox(Some("read_only")).unwrap(), Sandbox::ReadOnly);
    assert_eq!(parse_sandbox(Some("read-only")).unwrap(), Sandbox::ReadOnly);
    assert!(parse_sandbox(Some("garbage")).is_err());
}

#[test]
fn status_parser_handles_known_and_canceled_spelling() {
    assert_eq!(parse_status("success").unwrap(), RunStatus::Success);
    assert_eq!(parse_status("error").unwrap(), RunStatus::Error);
    assert_eq!(parse_status("timeout").unwrap(), RunStatus::Timeout);
    assert_eq!(parse_status("cancelled").unwrap(), RunStatus::Cancelled);
    assert_eq!(parse_status("canceled").unwrap(), RunStatus::Cancelled);
    assert!(parse_status("bogus").is_err());
    assert!(parse_status("").is_err());
}

#[test]
fn all_argument_objects_reject_unknown_fields() {
    assert!(
        serde_json::from_str::<DispatchArgs>(r#"{"task":"hello","target":"default","unknown":1}"#)
            .is_err()
    );
    assert!(
        serde_json::from_str::<BroadcastArgs>(r#"{"task":"hello","targets":"a,b","unknown":1}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<HistoryArgs>(r#"{"unknown":1}"#).is_err());
    assert!(serde_json::from_str::<RouteArgs>(r#"{"task":"hello","unknown":1}"#).is_err());
    assert!(serde_json::from_str::<CheckArgs>(r#"{"unknown":1}"#).is_err());
}

#[test]
fn history_boundary_values_deserialize_for_semantic_validation() {
    for limit in [0, 1, 1_000, 1_001] {
        let args: HistoryArgs =
            serde_json::from_value(serde_json::json!({ "limit": limit })).unwrap();
        assert_eq!(args.limit, limit);
    }
}

// ---- serialization shape tests ----------------------------------------------
//
// Generic MCP boundaries return allowlisted views rather than durable records.
// These tests pin both the useful fields and the omission of sensitive ones.

#[test]
fn run_view_serializes_into_dispatch_result_shape_without_durable_secrets() {
    use chrono::Utc;
    use vyane_core::{AdapterTransport, Attempt, AttemptOutcome, RunRecord, Target, Usage};
    use vyane_core::{ModelId, Protocol, ProviderId};

    let record = RunRecord {
        run_id: "0198c0de-0000-7000-8000-000000000000".into(),
        owner: "local".into(),
        started_at: Utc::now(),
        finished_at: Utc::now(),
        task_digest: "abcd1234abcd1234".into(),
        task_preview: Some("CANARY_PROMPT".into()),
        workdir: Some("/CANARY_WORKDIR".into()),
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
        session_id: Some("CANARY_SESSION".into()),
        output_chars: Some(5),
        error: Some("CANARY_ERROR".into()),
        labels: std::collections::BTreeMap::from([("CANARY_LABEL".into(), "CANARY_VALUE".into())]),
    };

    let run_id = record.run_id.clone();
    let payload = serde_json::json!({
        "record": vyane_service::RunView::from(record),
        "output": Some("hello".to_string()),
    });
    let text = serde_json::to_string(&payload).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["record"]["status"], "success");
    assert_eq!(parsed["record"]["sandbox"], "read-only");
    assert_eq!(parsed["record"]["session_attached"], true);
    assert_eq!(parsed["output"], "hello");
    assert_eq!(parsed["record"]["run_id"], run_id);
    for canary in [
        "CANARY_PROMPT",
        "CANARY_WORKDIR",
        "CANARY_SESSION",
        "CANARY_ERROR",
        "CANARY_LABEL",
        "CANARY_VALUE",
    ] {
        assert!(!text.contains(canary), "leaked {canary}");
    }
}

#[test]
fn session_view_serializes_into_items_array_shape_without_continuity_authority() {
    use chrono::Utc;
    use vyane_core::{
        ChatMessage, ModelId, NativeSessionState, Protocol, ProviderId, SessionRecord,
        SessionSnapshot, Target,
    };

    let session = SessionRecord {
        session_id: "s1".into(),
        owner: "local".into(),
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o"),
        },
        native_session_id: Some("CANARY_NATIVE_ID".into()),
        transcript: vec![ChatMessage::user("CANARY_TRANSCRIPT_BODY")],
        created_at: Utc::now(),
        updated_at: Utc::now(),
        run_count: 3,
    };

    let view = vyane_service::SessionView::from(SessionSnapshot {
        record: session,
        session_revision: 5,
        native_session: NativeSessionState::LegacyUnbound {
            native_session_id: "CANARY_NATIVE_ID".into(),
        },
    });
    let payload = serde_json::json!({ "items": [view] });
    let text = serde_json::to_string(&payload).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["items"][0]["session_id"], "s1");
    assert_eq!(parsed["items"][0]["run_count"], 3);
    assert_eq!(parsed["items"][0]["session_revision"], 5);
    assert_eq!(parsed["items"][0]["native_state"], "legacy_unbound");
    assert_eq!(parsed["items"][0]["native_resume_available"], false);
    assert!(!text.contains("CANARY_NATIVE_ID"));
    assert!(!text.contains("CANARY_TRANSCRIPT_BODY"));
}

#[tokio::test]
async fn history_stats_mcp_success_rates_use_filtered_counts_and_unknown_empty_samples()
-> anyhow::Result<()> {
    const HISTORY: &str = include_str!("fixtures/history_stats.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, expected) in [
        (serde_json::json!({"stats":true}), serde_json::json!(25.0)),
        (
            serde_json::json!({"stats":true,"until":"2026-01-01T11:00:00Z"}),
            serde_json::json!(25.0),
        ),
        (
            serde_json::json!({"stats":true,"provider":"q","status":"success"}),
            serde_json::json!(100.0),
        ),
        (
            serde_json::json!({"stats":true,"status":"error"}),
            serde_json::json!(0.0),
        ),
        (
            serde_json::json!({"stats":true,"since":"2026-01-02T00:00:00Z"}),
            serde_json::Value::Null,
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        let body = result_payload(result);
        assert_eq!(
            body.get("success_rate"),
            Some(&expected),
            "arguments: {arguments}"
        );
        if arguments == serde_json::json!({"stats":true}) {
            assert_eq!(body["by_provider"]["p"]["success_rate"], 0.0);
            assert_eq!(body["by_provider"]["q"]["success_rate"], 100.0);
        }
    }
    client.cancel().await?;
    server_handle.await??;
    assert_eq!(std::fs::read_to_string(ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn history_stats_mcp_counts_all_filtered_runs_without_a_list_limit() -> anyhow::Result<()> {
    const HISTORY: &str = include_str!("fixtures/history_stats.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, total) in [
        (serde_json::json!({"stats":true}), 120),
        (
            serde_json::json!({"stats":true,"since":"2026-01-01T14:00:00+02:00"}),
            80,
        ),
        (
            serde_json::json!({"stats":true,"provider":"p","status":"error","since":"2026-01-01T12:00:00Z"}),
            20,
        ),
        (
            serde_json::json!({"stats":true,"since":"2026-01-02T00:00:00Z"}),
            0,
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        let body = result_payload(result);
        assert_eq!(body["total"], total, "arguments: {arguments}");
        assert!(body["by_provider"].get("foreign-provider-canary").is_none());
        assert!(!body.to_string().contains("STATS_PRIVATE_DATA_CANARY"));
        if total == 120 {
            assert_eq!(body["success"], 30);
            assert_eq!(body["error"], 30);
            assert_eq!(body["timeout"], 30);
            assert_eq!(body["cancelled"], 30);
        }
    }
    for arguments in [
        serde_json::json!({"stats":true,"limit":1}),
        serde_json::json!({"stats":true,"status":"unknown"}),
        serde_json::json!({"stats":true,"since":"yesterday"}),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_eq!(result_payload(result)["error"]["code"], "invalid_argument");
    }
    let result = client
        .call_tool(
            CallToolRequestParams::new("vyane_history")
                .with_arguments(serde_json::json!({}).as_object().unwrap().clone()),
        )
        .await?;
    assert_eq!(
        result_payload(result)["items"].as_array().unwrap().len(),
        20
    );
    client.cancel().await?;
    server_handle.await??;
    assert_eq!(std::fs::read_to_string(ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn history_until_mcp_bounds_lists_and_statistics_and_rejects_invalid_windows()
-> anyhow::Result<()> {
    const HISTORY: &str = include_str!("fixtures/history_since.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, expected) in [
        (
            serde_json::json!({"until":"2026-01-01T12:00:00Z"}),
            vec!["late-old", "boundary", "before"],
        ),
        (
            serde_json::json!({"since":"2026-01-01T12:00:00Z","until":"2026-01-01T14:00:00+02:00"}),
            vec!["boundary"],
        ),
        (
            serde_json::json!({"until":"2026-01-01T14:00:00.001+02:00"}),
            vec!["late-old", "after", "boundary", "before"],
        ),
        (
            serde_json::json!({"since":"2026-01-01T11:00:00Z","until":"2026-01-01T12:00:00Z","limit":1}),
            vec!["boundary"],
        ),
        (
            serde_json::json!({"until":"2026-01-01T14:00:00Z","provider":"p","status":"error"}),
            vec!["error"],
        ),
        (serde_json::json!({"until":"2026-01-01T09:00:00Z"}), vec![]),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        let body = result_payload(result);
        assert!(
            body["items"].is_array(),
            "arguments: {arguments}, body: {body}"
        );
        let ids: Vec<_> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["run_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, expected, "arguments: {arguments}");
    }
    for (arguments, total) in [
        (
            serde_json::json!({"stats":true,"until":"2026-01-01T12:00:00Z"}),
            3,
        ),
        (
            serde_json::json!({"stats":true,"since":"2026-01-01T12:00:00Z","until":"2026-01-01T12:00:00Z"}),
            1,
        ),
        (
            serde_json::json!({"stats":true,"until":"2026-01-01T14:00:00Z","provider":"p","status":"error"}),
            1,
        ),
        (
            serde_json::json!({"stats":true,"until":"2026-01-01T09:00:00Z"}),
            0,
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        assert_eq!(
            result_payload(result)["total"],
            total,
            "arguments: {arguments}"
        );
    }
    for arguments in [
        serde_json::json!({"until":"yesterday"}),
        serde_json::json!({"until":"2026-01-01"}),
        serde_json::json!({"until":"2026-01-01T12:00:00"}),
        serde_json::json!({"until":"2026-02-30T12:00:00Z"}),
        serde_json::json!({"since":"2026-01-02T00:00:00Z","until":"2026-01-01T00:00:00Z"}),
        serde_json::json!({"stats":true,"since":"2026-01-02T00:00:00Z","until":"2026-01-01T00:00:00Z"}),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_eq!(result_payload(result)["error"]["code"], "invalid_argument");
    }
    client.cancel().await?;
    server_handle.await??;
    assert_eq!(std::fs::read_to_string(ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn history_since_mcp_filters_before_limit_and_rejects_invalid_dates() -> anyhow::Result<()> {
    const HISTORY: &str = include_str!("fixtures/history_since.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, expected) in [
        (
            serde_json::json!({}),
            vec![
                "late-old",
                "error",
                "other-provider",
                "after",
                "boundary",
                "before",
            ],
        ),
        (
            serde_json::json!({"since": "2026-01-01T12:00:00Z"}),
            vec!["error", "other-provider", "after", "boundary"],
        ),
        (
            serde_json::json!({"since": "2026-01-01T14:00:00+02:00", "provider": "p", "status": "success"}),
            vec!["after", "boundary"],
        ),
        (
            serde_json::json!({"since": "2026-01-01T12:00:00Z", "provider": "p", "status": "success", "limit": 1}),
            vec!["after"],
        ),
        (serde_json::json!({"since": "2026-01-02T00:00:00Z"}), vec![]),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        let body = result_payload(result);
        let ids: Vec<_> = body["items"]
            .as_array()
            .expect("history items")
            .iter()
            .map(|record| record["run_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, expected, "arguments: {arguments}");
    }
    for since in [
        "yesterday",
        "2026-01-01",
        "2026-01-01T12:00:00",
        "2026-02-30T12:00:00Z",
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history").with_arguments(
                    serde_json::json!({"since": since})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await?;
        let body = result_payload(result);
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(!body.to_string().contains(since));
    }
    client.cancel().await?;
    server_handle.await??;
    assert_eq!(std::fs::read_to_string(ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn diagnostics_tools_work_over_real_rmcp_duplex_and_redact_canaries() -> anyhow::Result<()> {
    const PATH_CANARY: &str = "CANARY_WIRE_CONFIG.toml";
    const URL_CANARY: &str = "https://CANARY_WIRE_URL.invalid/v1";
    const ENV_CANARY: &str = "CANARY_WIRE_ENV";
    const SECRET_CANARY: &str = "CANARY_WIRE_SECRET";
    const TASK_CANARY: &str = "CANARY_WIRE_TASK";

    // Exclusive directory: pid + clock-nanos collides when tests in this
    // binary start in the same realtime tick (macOS CI is microsecond-quantized).
    let root = tempfile::Builder::new()
        .prefix("vyane-mcp-wire-")
        .tempdir()?;
    let config_path = root.path().join(PATH_CANARY);
    std::fs::write(
        &config_path,
        format!(
            r#"
            [providers.safe]
            base_url = "{URL_CANARY}"
            api_key_env = "{ENV_CANARY}"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "safe-model"

            [profiles.safe]
            provider = "safe"
            model = "safe-model"
            tier = "economy"
            "#,
        ),
    )?;
    std::fs::write(
        root.path().join("secrets.env"),
        format!("{ENV_CANARY}={SECRET_CANARY}\n"),
    )?;
    let loaded = vyane_service::load_config(Some(&config_path))?;
    let service = vyane_service::VyaneService::from_loaded_with_paths(
        loaded,
        vyane_service::StoragePaths::from_data_dir(root.path().join("data")),
    )?;
    let now = chrono::Utc::now();
    let target = vyane_core::Target {
        provider: vyane_core::ProviderId::new("safe"),
        protocol: vyane_core::Protocol::OpenaiChat,
        harness: None,
        model: vyane_core::ModelId::new("safe-model"),
    };
    service
        .runtime()
        .ledger
        .append(&vyane_core::RunRecord {
            run_id: "wire-run".into(),
            owner: "local".into(),
            started_at: now,
            finished_at: now,
            task_digest: "CANARY_WIRE_DIGEST".into(),
            task_preview: Some("CANARY_WIRE_PROMPT".into()),
            workdir: Some("/CANARY_WIRE_WORKDIR".into()),
            sandbox: Sandbox::ReadOnly,
            target: target.clone(),
            transport: vyane_core::AdapterTransport::DirectHttp,
            attempts: vec![vyane_core::Attempt {
                target: target.clone(),
                transport: vyane_core::AdapterTransport::DirectHttp,
                started_at: now,
                duration_ms: 1,
                outcome: vyane_core::AttemptOutcome::Err {
                    kind: vyane_core::ErrorKind::Protocol,
                    message: "CANARY_WIRE_ATTEMPT_ERROR".into(),
                    failed_over: false,
                },
            }],
            status: RunStatus::Error,
            usage: None,
            cost_usd: None,
            session_id: Some("CANARY_WIRE_SESSION_ID".into()),
            output_chars: None,
            error: Some("CANARY_WIRE_TERMINAL_ERROR".into()),
            labels: std::collections::BTreeMap::from([(
                "CANARY_WIRE_LABEL".into(),
                "CANARY_WIRE_VALUE".into(),
            )]),
        })
        .await?;
    service
        .runtime()
        .sessions
        .save(
            "local",
            &vyane_core::SessionRecord {
                session_id: "wire-visible-session".into(),
                owner: "local".into(),
                target,
                native_session_id: Some("CANARY_WIRE_NATIVE_ID".into()),
                transcript: vec![vyane_core::ChatMessage::user("CANARY_WIRE_TRANSCRIPT_BODY")],
                created_at: now,
                updated_at: now,
                run_count: 1,
            },
        )
        .await?;
    let bound_target = vyane_core::Target {
        provider: vyane_core::ProviderId::new("safe"),
        protocol: vyane_core::Protocol::AnthropicMessages,
        harness: Some(vyane_core::HarnessKind::ClaudeCode),
        model: vyane_core::ModelId::new("safe-model"),
    };
    let bound_binding = vyane_core::NativeSessionBinding {
        native_session_id: "CANARY_WIRE_BOUND_NATIVE_ID".into(),
        domain: vyane_core::NativeSessionDomain {
            runtime: "CANARY_WIRE_RUNTIME".into(),
            harness: vyane_core::HarnessKind::ClaudeCode,
            provider: bound_target.provider.clone(),
            protocol: bound_target.protocol,
            model: bound_target.model.clone(),
            endpoint_routing_digest: "a".repeat(64),
            canonical_workdir: "/CANARY_WIRE_BOUND_WORKDIR".into(),
            workdir_identity: vyane_core::WorkdirIdentity {
                device: 123,
                inode: 456,
            },
            checkpoint_namespace: "CANARY_WIRE_CHECKPOINT".into(),
            checkpoint_schema: 1,
            account_scope_digest: "b".repeat(64),
            runtime_scope_digest: "c".repeat(64),
        },
    };
    service
        .runtime()
        .sessions
        .apply_native_transition(
            "local",
            "wire-bound-session",
            &vyane_core::NativeSessionTransition::Commit {
                expected_revision: 0,
                update: vyane_core::SessionUpdate {
                    owner: "local".into(),
                    session_id: "wire-bound-session".into(),
                    target: bound_target,
                    native_session_id: None,
                    transcript_delta: Vec::new(),
                    occurred_at: now,
                },
                binding: bound_binding,
            },
        )
        .await?;
    let observer = service.clone();

    let (server_transport, client_transport) = tokio::io::duplex(256 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;

    let mut tool_names = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    tool_names.sort();
    assert_eq!(
        tool_names,
        [
            "vyane_broadcast",
            "vyane_check",
            "vyane_dispatch",
            "vyane_history",
            "vyane_route",
            "vyane_sessions",
            "vyane_workflow_plan",
        ]
    );

    let route = client
        .call_tool(
            CallToolRequestParams::new("vyane_route").with_arguments(
                serde_json::json!({
                    "task": TASK_CANARY,
                    "tier": "economy",
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await?;
    assert_eq!(route.is_error, Some(false));
    let route_wire = serde_json::to_string(&route)?;
    assert_eq!(result_payload(route)["profile"], "safe");
    assert!(!route_wire.contains(TASK_CANARY));

    let check = client
        .call_tool(CallToolRequestParams::new("vyane_check").with_arguments(Default::default()))
        .await?;
    assert_eq!(check.is_error, Some(false));
    let check_wire = serde_json::to_string(&check)?;
    let check_payload = result_payload(check);
    assert_eq!(check_payload["status"], "valid");
    assert_eq!(check_payload["scope"], "static_config_only");
    for canary in [PATH_CANARY, URL_CANARY, ENV_CANARY, SECRET_CANARY] {
        assert!(!check_wire.contains(canary), "leaked {canary}");
    }

    let history = client
        .call_tool(
            CallToolRequestParams::new("vyane_history").with_arguments(
                serde_json::json!({ "limit": 5 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    let history_wire = serde_json::to_string(&history)?;
    assert_eq!(result_payload(history)["items"][0]["run_id"], "wire-run");

    let sessions = client
        .call_tool(CallToolRequestParams::new("vyane_sessions").with_arguments(Default::default()))
        .await?;
    let sessions_wire = serde_json::to_string(&sessions)?;
    let sessions_payload = result_payload(sessions);
    assert_eq!(sessions_payload["items"].as_array().unwrap().len(), 2);
    assert!(
        sessions_payload["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["session_id"] == "wire-visible-session"
                    && item["native_state"] == "legacy_unbound"
            })
    );
    assert!(
        sessions_payload["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["session_id"] == "wire-bound-session" && item["native_state"] == "bound"
            })
    );
    for canary in [
        "CANARY_WIRE_DIGEST",
        "CANARY_WIRE_PROMPT",
        "CANARY_WIRE_WORKDIR",
        "CANARY_WIRE_ATTEMPT_ERROR",
        "CANARY_WIRE_SESSION_ID",
        "CANARY_WIRE_TERMINAL_ERROR",
        "CANARY_WIRE_LABEL",
        "CANARY_WIRE_VALUE",
        "CANARY_WIRE_NATIVE_ID",
        "CANARY_WIRE_TRANSCRIPT_BODY",
        "CANARY_WIRE_BOUND_NATIVE_ID",
        "CANARY_WIRE_RUNTIME",
        "CANARY_WIRE_BOUND_WORKDIR",
        "CANARY_WIRE_CHECKPOINT",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    ] {
        assert!(!history_wire.contains(canary), "history leaked {canary}");
        assert!(!sessions_wire.contains(canary), "sessions leaked {canary}");
    }

    let invalid = client
        .call_tool(
            CallToolRequestParams::new("vyane_route").with_arguments(
                serde_json::json!({
                    "task": TASK_CANARY,
                    "tier": SECRET_CANARY,
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await?;
    let invalid_wire = serde_json::to_string(&invalid)?;
    assert!(!invalid_wire.contains(TASK_CANARY));
    assert!(!invalid_wire.contains(SECRET_CANARY));
    assert_eq!(result_payload(invalid)["error"]["code"], "invalid_argument");

    assert_eq!(
        observer
            .history(vyane_service::HistoryFilter {
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .len(),
        1
    );
    assert_eq!(observer.sessions().await?.len(), 2);

    client.cancel().await?;
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn diagnostics_config_row_overflow_is_a_static_wire_error() -> anyhow::Result<()> {
    const ID_CANARY: &str = "CANARY_OVERFLOW_PROVIDER";
    // Exclusive directory: pid + clock-nanos collides when tests in this
    // binary start in the same realtime tick (macOS CI is microsecond-quantized).
    let root = tempfile::Builder::new()
        .prefix("vyane-mcp-budget-")
        .tempdir()?;
    let config_path = root.path().join("budget.toml");
    let mut config = String::new();
    for index in 0..=vyane_service::DIAGNOSTIC_MAX_CONFIG_ITEMS {
        config.push_str(&format!(
            r#"
            [providers."{ID_CANARY}-{index}"]
            base_url = "https://example.invalid"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "model"
            "#,
        ));
    }
    std::fs::write(&config_path, config)?;
    let loaded = vyane_service::load_config(Some(&config_path))?;
    let service = vyane_service::VyaneService::from_loaded_with_paths(
        loaded,
        vyane_service::StoragePaths::from_data_dir(root.path().join("data")),
    )?;
    let observer = service.clone();

    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;

    let result = client
        .call_tool(CallToolRequestParams::new("vyane_check").with_arguments(Default::default()))
        .await?;
    let wire = serde_json::to_string(&result)?;
    assert!(!wire.contains(ID_CANARY));
    assert_eq!(
        result_payload(result),
        serde_json::json!({
            "status": "error",
            "error": {
                "code": "limit_exceeded",
                "message": "diagnostic safety limit exceeded",
            }
        })
    );
    assert!(
        observer
            .history(vyane_service::HistoryFilter {
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .is_empty()
    );
    assert!(observer.sessions().await?.is_empty());

    client.cancel().await?;
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn diagnostics_invalid_endpoint_is_a_redacted_static_wire_error() -> anyhow::Result<()> {
    const URL_CANARY: &str = "CANARY_ENDPOINT_VALUE";
    // Exclusive directory: pid + clock-nanos collides when tests in this
    // binary start in the same realtime tick (macOS CI is microsecond-quantized).
    let root = tempfile::Builder::new()
        .prefix("vyane-mcp-endpoint-")
        .tempdir()?;
    let config_path = root.path().join("endpoint.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
            [providers.bad]
            base_url = "file:///{URL_CANARY}/socket"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "model"

            [profiles.bad]
            provider = "bad"
            model = "model"
            "#,
        ),
    )?;
    let loaded = vyane_service::load_config(Some(&config_path))?;
    let service = vyane_service::VyaneService::from_loaded_with_paths(
        loaded,
        vyane_service::StoragePaths::from_data_dir(root.path().join("data")),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server_handle = tokio::spawn(async move {
        VyaneMcpServer::new(service)
            .serve(server_transport)
            .await?
            .waiting()
            .await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;

    let result = client
        .call_tool(CallToolRequestParams::new("vyane_check").with_arguments(Default::default()))
        .await?;
    let wire = serde_json::to_string(&result)?;
    assert!(!wire.contains(URL_CANARY));
    assert_eq!(
        result_payload(result),
        serde_json::json!({
            "status": "error",
            "error": {
                "code": "config_invalid",
                "message": "vyane configuration is invalid",
            }
        })
    );

    client.cancel().await?;
    server_handle.await??;
    Ok(())
}

fn result_payload(result: CallToolResult) -> serde_json::Value {
    let wire = serde_json::to_value(result).unwrap();
    let text = wire["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn history_duration_mcp_projects_owned_filtered_record_timings() -> anyhow::Result<()> {
    const HISTORY: &str = include_str!("fixtures/history_duration.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, total, expected) in [
        (
            serde_json::json!({"stats":true}),
            35,
            serde_json::json!({"samples":34,"invalid":1,"total_ms":7000,"mean_ms":205.9}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"q"}),
            2,
            serde_json::json!({"samples":1,"invalid":1,"total_ms":2750,"mean_ms":2750.0}),
        ),
        (
            serde_json::json!({"stats":true,"status":"cancelled"}),
            1,
            serde_json::json!({"samples":0,"invalid":1,"total_ms":null,"mean_ms":null}),
        ),
        (
            serde_json::json!({"stats":true,"since":"2026-01-01T13:00:00+02:00","until":"2026-01-01T14:00:00Z","provider":"p"}),
            2,
            serde_json::json!({"samples":2,"invalid":0,"total_ms":0,"mean_ms":0.0}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"absent"}),
            0,
            serde_json::json!({"samples":0,"invalid":0,"total_ms":null,"mean_ms":null}),
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        let body = result_payload(result);
        assert_eq!(body["total"], total, "{arguments}");
        assert_eq!(body["duration"], expected, "{arguments}");
        assert!(!body.to_string().contains("DURATION_PRIVATE_CANARY"));
        assert!(body.get("session_id").is_none() && body.get("labels").is_none());
    }
    let result = client
        .call_tool(
            CallToolRequestParams::new("vyane_history").with_arguments(
                serde_json::json!({"stats":true,"session":"duration-session"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    assert_eq!(result_payload(result)["error"]["code"], "invalid_argument");
    client.cancel().await?;
    server_handle.await??;
    assert_eq!(std::fs::read_to_string(&ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn provider_duration_mcp_projects_complete_owned_final_provider_groups() -> anyhow::Result<()>
{
    const HISTORY: &str = include_str!("fixtures/history_duration.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, expected) in [
        (
            serde_json::json!({"stats":true}),
            serde_json::json!({"p":{"samples":33,"invalid":0,"total_ms":4250,"mean_ms":128.8},"q":{"samples":1,"invalid":1,"total_ms":2750,"mean_ms":2750.0}}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"p"}),
            serde_json::json!({"p":{"samples":33,"invalid":0,"total_ms":4250,"mean_ms":128.8}}),
        ),
        (
            serde_json::json!({"stats":true,"status":"cancelled"}),
            serde_json::json!({"q":{"samples":0,"invalid":1,"total_ms":null,"mean_ms":null}}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"p","since":"2026-01-01T13:00:00+02:00","until":"2026-01-01T14:00:00Z"}),
            serde_json::json!({"p":{"samples":2,"invalid":0,"total_ms":0,"mean_ms":0.0}}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"absent"}),
            serde_json::json!({}),
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        let body = result_payload(result);
        assert_eq!(body["by_provider_duration"], expected, "{arguments}");
        assert!(!body.to_string().contains("DURATION_PRIVATE_CANARY"));
        assert_eq!(
            body["by_provider"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            body["by_provider_duration"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>()
        );
    }
    client.cancel().await?;
    handle.await??;
    assert_eq!(std::fs::read_to_string(&ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn history_cost_mcp_projects_owned_filtered_saved_values() -> anyhow::Result<()> {
    const HISTORY: &str = include_str!("../../vyane-service/tests/fixtures/history_cost.jsonl");
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let handle = tokio::spawn(async move {
        VyaneMcpServer::new(service)
            .serve(server_transport)
            .await?
            .waiting()
            .await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, expected) in [
        (
            serde_json::json!({"stats":true}),
            serde_json::json!({"known":123,"unknown":1,"invalid":1,"known_total_usd":62.0,"overflowed":false}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"q"}),
            serde_json::json!({"known":1,"unknown":1,"invalid":1,"known_total_usd":0.75,"overflowed":false}),
        ),
        (
            serde_json::json!({"stats":true,"status":"timeout"}),
            serde_json::json!({"known":0,"unknown":1,"invalid":0,"known_total_usd":null,"overflowed":false}),
        ),
        (
            serde_json::json!({"stats":true,"since":"2026-01-01T13:00:00+02:00","until":"2026-01-01T13:00:00Z"}),
            serde_json::json!({"known":1,"unknown":1,"invalid":1,"known_total_usd":1.25,"overflowed":false}),
        ),
        (
            serde_json::json!({"stats":true,"provider":"absent"}),
            serde_json::json!({"known":0,"unknown":0,"invalid":0,"known_total_usd":null,"overflowed":false}),
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        let body = result_payload(result);
        assert_eq!(body["cost"], expected, "{arguments}");
        assert!(!body.to_string().contains("COST_PRIVATE_CANARY"));
    }
    client.cancel().await?;
    handle.await??;
    assert_eq!(std::fs::read_to_string(&ledger)?, HISTORY);
    Ok(())
}

#[tokio::test]
async fn history_usage_mcp_counts_all_owned_filtered_samples() -> anyhow::Result<()> {
    const HISTORY: &str = include_str!("../../vyane-service/tests/fixtures/history_usage.jsonl");
    const EXPECTED: &str =
        include_str!("../../vyane-service/tests/fixtures/history_usage_expected.json");
    let expected: serde_json::Value = serde_json::from_str(EXPECTED)?;
    let root = tempfile::tempdir()?;
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY)?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server = VyaneMcpServer::new(service);
    let handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    for (arguments, case, total) in [
        (serde_json::json!({"stats":true}), "all", 126),
        (serde_json::json!({"stats":true,"provider":"q"}), "q", 2),
        (
            serde_json::json!({"stats":true,"status":"error"}),
            "zero",
            1,
        ),
        (
            serde_json::json!({"stats":true,"provider":"p","since":"2026-01-01T13:00:00+02:00","until":"2026-01-01T14:00:00Z"}),
            "window",
            2,
        ),
        (
            serde_json::json!({"stats":true,"provider":"absent"}),
            "empty",
            0,
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("vyane_history")
                    .with_arguments(arguments.as_object().unwrap().clone()),
            )
            .await?;
        assert_ne!(result.is_error, Some(true));
        let body = result_payload(result);
        assert_eq!(body["total"], total);
        assert_eq!(body["usage"], expected[case], "{arguments}");
        assert!(!body.to_string().contains("USAGE_PRIVATE_CANARY"));
        assert!(body.get("session_id").is_none() && body.get("labels").is_none());
    }
    client.cancel().await?;
    handle.await??;
    assert_eq!(std::fs::read_to_string(ledger)?, HISTORY);
    Ok(())
}

/// A caller-supplied owner is not an authority. The tool must reject it before
/// any store read: the session read path chmods that owner's directory to
/// `0o700` before listing, and a history read of a mode-`0o000` ledger fails
/// as `internal` rather than as a schema error.
#[tokio::test]
async fn tool_call_with_wrong_owner_is_rejected_without_reading_other_owner_data()
-> anyhow::Result<()> {
    const FOREIGN_OWNER: &str = "another-owner";
    const FOREIGN_RUN: &str = "foreign-run-canary";
    const FOREIGN_PREVIEW: &str = "FOREIGN_OWNER_PREVIEW_CANARY";
    const FOREIGN_SESSION: &str = "foreign-session-canary";
    const FOREIGN_TRANSCRIPT: &str = "FOREIGN_OWNER_TRANSCRIPT_CANARY";
    const LOCAL_RUN: &str = "local-run";
    const LOCAL_SESSION: &str = "local-session";

    let root = tempfile::Builder::new()
        .prefix("vyane-mcp-owner-fence-")
        .tempdir()?;
    let service = vyane_service::VyaneService::from_local_storage(
        vyane_service::StoragePaths::from_data_dir(root.path()),
    )?;
    let now = chrono::Utc::now();
    let target = vyane_core::Target {
        provider: vyane_core::ProviderId::new("p"),
        protocol: vyane_core::Protocol::OpenaiChat,
        harness: None,
        model: vyane_core::ModelId::new("m"),
    };
    for (owner, run_id, preview) in [
        ("local", LOCAL_RUN, None),
        (
            FOREIGN_OWNER,
            FOREIGN_RUN,
            Some(FOREIGN_PREVIEW.to_string()),
        ),
    ] {
        service
            .runtime()
            .ledger
            .append(&vyane_core::RunRecord {
                run_id: run_id.into(),
                owner: owner.into(),
                started_at: now,
                finished_at: now,
                task_digest: "d".into(),
                task_preview: preview,
                workdir: None,
                sandbox: Sandbox::ReadOnly,
                target: target.clone(),
                transport: vyane_core::AdapterTransport::DirectHttp,
                attempts: Vec::new(),
                status: RunStatus::Success,
                usage: None,
                cost_usd: None,
                session_id: None,
                output_chars: None,
                error: None,
                labels: std::collections::BTreeMap::new(),
            })
            .await?;
    }
    for (owner, session_id, transcript) in [
        ("local", LOCAL_SESSION, "local transcript"),
        (FOREIGN_OWNER, FOREIGN_SESSION, FOREIGN_TRANSCRIPT),
    ] {
        service
            .runtime()
            .sessions
            .save(
                owner,
                &vyane_core::SessionRecord {
                    session_id: session_id.into(),
                    owner: owner.into(),
                    target: target.clone(),
                    native_session_id: None,
                    transcript: vec![vyane_core::ChatMessage::user(transcript)],
                    created_at: now,
                    updated_at: now,
                    run_count: 1,
                },
            )
            .await?;
    }

    let ledger = service.storage_paths().ledger_path.clone();
    let foreign_session_dir =
        foreign_session_dir(&service.storage_paths().sessions_dir, FOREIGN_SESSION)?;
    let stored = service
        .runtime()
        .sessions
        .list_snapshots(FOREIGN_OWNER)
        .await?;
    assert!(
        stored
            .iter()
            .any(|snapshot| snapshot.record.session_id == FOREIGN_SESSION)
    );
    let observer = service.clone();

    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let server_handle = tokio::spawn(async move {
        VyaneMcpServer::new(service)
            .serve(server_transport)
            .await?
            .waiting()
            .await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;

    let owned = call_json(&client, "vyane_history", serde_json::json!({"limit": 20})).await?;
    let ids: Vec<_> = owned["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["run_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![LOCAL_RUN]);
    assert!(!owned.to_string().contains(FOREIGN_RUN));
    assert!(!owned.to_string().contains(FOREIGN_PREVIEW));

    let sessions = call_json(&client, "vyane_sessions", serde_json::json!({})).await?;
    assert_eq!(sessions["items"].as_array().unwrap().len(), 1);
    assert_eq!(sessions["items"][0]["session_id"], LOCAL_SESSION);
    assert!(!sessions.to_string().contains(FOREIGN_SESSION));
    assert!(!sessions.to_string().contains(FOREIGN_TRANSCRIPT));

    let _restore_ledger = RestoreMode::new(&ledger, 0o600);
    let _restore_sessions = RestoreMode::new(&foreign_session_dir, 0o700);
    seal_private(&ledger)?;
    seal_private(&foreign_session_dir)?;

    for arguments in [
        serde_json::json!({"owner": FOREIGN_OWNER}),
        serde_json::json!({"owner": FOREIGN_OWNER, "limit": 20}),
        serde_json::json!({"stats": true, "owner": FOREIGN_OWNER}),
    ] {
        let rejected = call_json(&client, "vyane_history", arguments).await?;
        assert_eq!(
            rejected,
            serde_json::json!({
                "status": "error",
                "error": {
                    "code": "invalid_argument",
                    "message": "arguments do not match the tool schema",
                }
            })
        );
        let wire = rejected.to_string();
        assert!(!wire.contains(FOREIGN_OWNER));
        assert!(!wire.contains(FOREIGN_RUN));
        assert!(!wire.contains(FOREIGN_PREVIEW));
    }
    let rejected_sessions = call_json(
        &client,
        "vyane_sessions",
        serde_json::json!({"owner": FOREIGN_OWNER}),
    )
    .await?;
    assert_eq!(
        rejected_sessions,
        serde_json::json!({
            "status": "error",
            "error": {
                "code": "invalid_argument",
                "message": "arguments do not match the tool schema",
            }
        })
    );
    let wire = rejected_sessions.to_string();
    assert!(!wire.contains(FOREIGN_OWNER));
    assert!(!wire.contains(FOREIGN_SESSION));
    assert!(!wire.contains(FOREIGN_TRANSCRIPT));

    assert!(
        still_sealed(&ledger),
        "wrong-owner history must not open the other owner's ledger"
    );
    assert!(
        still_sealed(&foreign_session_dir),
        "wrong-owner sessions must not enter the other owner's session directory"
    );

    let attempted = call_json(&client, "vyane_history", serde_json::json!({"limit": 1})).await?;
    if sealed_reads_fail() {
        assert_eq!(attempted["error"]["code"], "internal");
        assert!(!attempted.to_string().contains(FOREIGN_RUN));
        assert!(!attempted.to_string().contains(FOREIGN_PREVIEW));
        assert!(still_sealed(&ledger));
    }

    let revealed = observer
        .runtime()
        .sessions
        .list_snapshots(FOREIGN_OWNER)
        .await?;
    assert!(
        revealed
            .iter()
            .any(|snapshot| snapshot.record.session_id == FOREIGN_SESSION),
        "the other owner's session must still be readable by its own store path"
    );
    if sealed_reads_fail() {
        assert!(
            !still_sealed(&foreign_session_dir),
            "the session read path is what unseals the owner directory"
        );
    }

    client.cancel().await?;
    server_handle.await??;
    Ok(())
}

async fn call_json(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    name: &str,
    arguments: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let result = client
        .call_tool(
            CallToolRequestParams::new(name.to_owned())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await?;
    assert_eq!(result.is_error, Some(false));
    Ok(result_payload(result))
}

fn foreign_session_dir(
    root: &std::path::Path,
    session_id: &str,
) -> anyhow::Result<std::path::PathBuf> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(&path)? {
            let file = file?.path();
            if file.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            if std::fs::read_to_string(&file)?.contains(session_id) {
                return Ok(path);
            }
        }
    }
    anyhow::bail!("foreign session directory was not created");
}

fn seal_private(path: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn still_sealed(path: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|metadata| metadata.permissions().mode() & 0o777 == 0)
            .unwrap_or(false)
            && std::fs::read(path).is_err()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

fn sealed_reads_fail() -> bool {
    cfg!(unix)
}

struct RestoreMode {
    path: std::path::PathBuf,
    mode: u32,
}

impl RestoreMode {
    fn new(path: &std::path::Path, mode: u32) -> Self {
        Self {
            path: path.to_path_buf(),
            mode,
        }
    }
}

impl Drop for RestoreMode {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(self.mode));
        }
    }
}
