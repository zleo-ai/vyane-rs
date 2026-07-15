#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use rmcp::{
    ServiceExt as _,
    model::{CallToolRequestParam, CallToolResult},
};
use vyane_mcp::{
    VyaneMcpServer, WorkflowControl, WorkflowControlError, WorkflowControlFuture,
    WorkflowFailureCode, WorkflowState, WorkflowSubmitRequest, WorkflowView,
};
use vyane_service::{StoragePaths, VyaneService};
use vyane_workflow::WorkflowRunId;

#[derive(Default)]
struct FakeWorkflowControl {
    submissions: Mutex<Vec<WorkflowSubmitRequest>>,
    statuses: Mutex<Vec<WorkflowRunId>>,
    cancellations: Mutex<Vec<WorkflowRunId>>,
}

impl FakeWorkflowControl {
    fn view(caller_id: WorkflowRunId, state: WorkflowState) -> WorkflowView {
        WorkflowView {
            caller_id,
            state,
            failure_code: None,
        }
    }
}

impl WorkflowControl for FakeWorkflowControl {
    fn submit(
        &self,
        request: WorkflowSubmitRequest,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>> {
        Box::pin(async move {
            let caller_id = request.caller_id.clone();
            self.submissions.lock().unwrap().push(request);
            Ok(Self::view(caller_id, WorkflowState::Queued))
        })
    }

    fn status(
        &self,
        caller_id: WorkflowRunId,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>> {
        Box::pin(async move {
            self.statuses.lock().unwrap().push(caller_id.clone());
            Ok(WorkflowView {
                caller_id,
                state: WorkflowState::Failed,
                failure_code: Some(WorkflowFailureCode::DispatchFailed),
            })
        })
    }

    fn cancel(
        &self,
        caller_id: WorkflowRunId,
    ) -> WorkflowControlFuture<'_, Result<WorkflowView, WorkflowControlError>> {
        Box::pin(async move {
            self.cancellations.lock().unwrap().push(caller_id.clone());
            // Returning an already-terminal view is the idempotent cancel
            // contract, not a conflict.
            Ok(Self::view(caller_id, WorkflowState::Cancelled))
        })
    }
}

#[tokio::test]
async fn optional_workflow_port_preserves_six_tool_default_and_enables_strict_control_tools()
-> anyhow::Result<()> {
    let (service, root) = test_service()?;
    let default_tools = list_tools(VyaneMcpServer::new(service.clone())).await?;
    assert_eq!(default_tools.len(), 6);
    assert!(!default_tools.iter().any(|name| name.contains("workflow")));

    let fake = Arc::new(FakeWorkflowControl::default());
    let server = VyaneMcpServer::with_workflow_control(service, fake.clone());
    let (server_transport, client_transport) = tokio::io::duplex(512 * 1024);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;

    let names = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 9);
    for name in [
        "vyane_workflow_submit",
        "vyane_workflow_status",
        "vyane_workflow_cancel",
    ] {
        assert!(names.iter().any(|candidate| candidate == name));
    }

    let caller_id = WorkflowRunId::generate().to_string();
    let submit = call(
        &client,
        "vyane_workflow_submit",
        serde_json::json!({
            "caller_id": caller_id,
            "workflow_toml": "[workflow]\nname = \"safe\"\n[[steps]]\nid = \"one\"\ntask = \"hello\"\ntarget = \"default\"",
            "vars": { "mode": "review" }
        }),
    )
    .await?;
    assert_eq!(submit["caller_id"], caller_id);
    assert_eq!(submit["state"], "queued");
    {
        let requests = fake.submissions.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].caller_id.as_str(), caller_id);
        assert_eq!(requests[0].vars["mode"], "review");
    }

    let status = call(
        &client,
        "vyane_workflow_status",
        serde_json::json!({ "caller_id": caller_id }),
    )
    .await?;
    assert_eq!(status["state"], "failed");
    assert_eq!(status["failure_code"], "dispatch_failed");

    for _ in 0..2 {
        let cancelled = call(
            &client,
            "vyane_workflow_cancel",
            serde_json::json!({ "caller_id": caller_id }),
        )
        .await?;
        assert_eq!(cancelled["state"], "cancelled");
    }
    assert_eq!(fake.cancellations.lock().unwrap().len(), 2);

    const CANARY: &str = "DO_NOT_REFLECT_THIS_VALUE";
    for forbidden in ["owner", "controller", "token"] {
        let mut arguments = serde_json::json!({ "caller_id": caller_id });
        arguments
            .as_object_mut()
            .unwrap()
            .insert(forbidden.into(), CANARY.into());
        let invalid = call(&client, "vyane_workflow_status", arguments).await?;
        assert_eq!(invalid["status"], "error");
        assert_eq!(invalid["error"]["code"], "invalid_argument");
        assert!(!invalid.to_string().contains(CANARY));
    }

    let invalid_id = call(
        &client,
        "vyane_workflow_status",
        serde_json::json!({ "caller_id": CANARY }),
    )
    .await?;
    assert_eq!(invalid_id["error"]["code"], "invalid_argument");
    assert!(!invalid_id.to_string().contains(CANARY));

    client.cancel().await?;
    server_handle.await??;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

async fn list_tools(server: VyaneMcpServer) -> anyhow::Result<Vec<String>> {
    let (server_transport, client_transport) = tokio::io::duplex(128 * 1024);
    let handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), client_transport).await?;
    let names = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect();
    client.cancel().await?;
    handle.await??;
    Ok(names)
}

async fn call(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    name: &str,
    arguments: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let result = client
        .call_tool(CallToolRequestParam {
            name: name.to_owned().into(),
            arguments: Some(arguments.as_object().unwrap().clone()),
        })
        .await?;
    Ok(result_payload(result))
}

fn result_payload(result: CallToolResult) -> serde_json::Value {
    let wire = serde_json::to_value(result).unwrap();
    serde_json::from_str(wire["content"][0]["text"].as_str().unwrap()).unwrap()
}

fn test_service() -> anyhow::Result<(VyaneService, std::path::PathBuf)> {
    let root = std::env::temp_dir().join(format!(
        "vyane-mcp-workflow-port-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    let config = root.join("config.toml");
    std::fs::write(
        &config,
        r#"
        [providers.safe]
        base_url = "https://example.invalid/v1"
        api_key_env = "VYANE_TEST_UNUSED_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "safe"

        [profiles.default]
        provider = "safe"
        model = "safe"
        "#,
    )?;
    let loaded = vyane_service::load_config(Some(&config))?;
    let service = VyaneService::from_loaded_with_paths(
        loaded,
        StoragePaths::from_data_dir(root.join("data")),
    )?;
    Ok((service, root))
}
