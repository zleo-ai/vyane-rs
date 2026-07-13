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
    model::{CallToolRequestParam, CallToolResult},
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
async fn diagnostics_tools_work_over_real_rmcp_duplex_and_redact_canaries() -> anyhow::Result<()> {
    const PATH_CANARY: &str = "CANARY_WIRE_CONFIG.toml";
    const URL_CANARY: &str = "https://CANARY_WIRE_URL.invalid/v1";
    const ENV_CANARY: &str = "CANARY_WIRE_ENV";
    const SECRET_CANARY: &str = "CANARY_WIRE_SECRET";
    const TASK_CANARY: &str = "CANARY_WIRE_TASK";

    let root = std::env::temp_dir().join(format!(
        "vyane-mcp-wire-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    let config_path = root.join(PATH_CANARY);
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
        root.join("secrets.env"),
        format!("{ENV_CANARY}={SECRET_CANARY}\n"),
    )?;
    let loaded = vyane_service::load_config(Some(&config_path))?;
    let service = vyane_service::VyaneService::from_loaded_with_paths(
        loaded,
        vyane_service::StoragePaths::from_data_dir(root.join("data")),
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
        ]
    );

    let route = client
        .call_tool(CallToolRequestParam {
            name: "vyane_route".into(),
            arguments: Some(
                serde_json::json!({
                    "task": TASK_CANARY,
                    "tier": "economy",
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        })
        .await?;
    assert_eq!(route.is_error, Some(false));
    let route_wire = serde_json::to_string(&route)?;
    assert_eq!(result_payload(route)["profile"], "safe");
    assert!(!route_wire.contains(TASK_CANARY));

    let check = client
        .call_tool(CallToolRequestParam {
            name: "vyane_check".into(),
            arguments: Some(Default::default()),
        })
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
        .call_tool(CallToolRequestParam {
            name: "vyane_history".into(),
            arguments: Some(
                serde_json::json!({ "limit": 5 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        })
        .await?;
    let history_wire = serde_json::to_string(&history)?;
    assert_eq!(result_payload(history)["items"][0]["run_id"], "wire-run");

    let sessions = client
        .call_tool(CallToolRequestParam {
            name: "vyane_sessions".into(),
            arguments: Some(Default::default()),
        })
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
        .call_tool(CallToolRequestParam {
            name: "vyane_route".into(),
            arguments: Some(
                serde_json::json!({
                    "task": TASK_CANARY,
                    "tier": SECRET_CANARY,
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        })
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
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn diagnostics_config_row_overflow_is_a_static_wire_error() -> anyhow::Result<()> {
    const ID_CANARY: &str = "CANARY_OVERFLOW_PROVIDER";
    let root = std::env::temp_dir().join(format!(
        "vyane-mcp-budget-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    let config_path = root.join("budget.toml");
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
        vyane_service::StoragePaths::from_data_dir(root.join("data")),
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
        .call_tool(CallToolRequestParam {
            name: "vyane_check".into(),
            arguments: Some(Default::default()),
        })
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
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn diagnostics_invalid_endpoint_is_a_redacted_static_wire_error() -> anyhow::Result<()> {
    const URL_CANARY: &str = "CANARY_ENDPOINT_VALUE";
    let root = std::env::temp_dir().join(format!(
        "vyane-mcp-endpoint-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    let config_path = root.join("endpoint.toml");
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
        vyane_service::StoragePaths::from_data_dir(root.join("data")),
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
        .call_tool(CallToolRequestParam {
            name: "vyane_check".into(),
            arguments: Some(Default::default()),
        })
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
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn result_payload(result: CallToolResult) -> serde_json::Value {
    let wire = serde_json::to_value(result).unwrap();
    let text = wire["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}
