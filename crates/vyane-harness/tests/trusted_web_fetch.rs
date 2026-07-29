#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use vyane_core::{
    AuthorizedWebFetchClient, CancellationToken, NativeExecutionAuthority, NativeSideEffect,
    Result, WebFetchOutcome, WebFetchRequest, WebFetchRoute,
};
use vyane_harness::native::{
    NativeWebFetchPolicy, PermissionPolicy, ToolCall, ToolContext, ToolInvocationStatus,
    ToolRegistry, register_web_fetch_tool, web_fetch_permission_policy,
};

struct FetchClient {
    outcome: WebFetchOutcome,
    requests: Mutex<Vec<WebFetchRequest>>,
}

#[async_trait]
impl AuthorizedWebFetchClient for FetchClient {
    async fn fetch_authorized(
        &self,
        req: WebFetchRequest,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
        _cancel: &CancellationToken,
    ) -> Result<WebFetchOutcome> {
        authority.revalidate(effect).await?;
        self.requests.lock().unwrap().push(req);
        Ok(self.outcome.clone())
    }
}

#[derive(Default)]
struct RecordingAuthority(Mutex<Vec<NativeSideEffect>>);

#[async_trait]
impl NativeExecutionAuthority for RecordingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> Result<()> {
        self.0.lock().unwrap().push(effect);
        Ok(())
    }
}

fn policy() -> NativeWebFetchPolicy {
    NativeWebFetchPolicy {
        allow_domains: vec!["example.com".into()],
        route: WebFetchRoute::Direct,
        max_fetches: 2,
        max_response_bytes: 4096,
        max_redirects: 2,
    }
}

fn call(id: &str, url: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "web_fetch".into(),
        arguments: BTreeMap::from([("url".into(), Value::String(url.into()))]),
    }
}

#[tokio::test]
async fn admitted_fetch_forwards_closed_bounds_and_marks_content_untrusted() {
    let client = Arc::new(FetchClient {
        outcome: WebFetchOutcome {
            final_url: "https://docs.example.com/reference".into(),
            content_type: "text/html".into(),
            text: "<p>reference</p>".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_fetch_tool(
        &mut registry,
        policy(),
        Arc::clone(&client) as Arc<dyn AuthorizedWebFetchClient>,
    )
    .unwrap();
    let permissions = web_fetch_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();
    let authority = RecordingAuthority::default();

    let result = registry
        .execute_authorized(
            call("fetch-1", "https://docs.example.com/reference"),
            &context,
            &permissions,
            &authority,
            3,
            2,
        )
        .await
        .unwrap();

    assert_eq!(result.status, ToolInvocationStatus::Executed);
    assert!(result.output.contains("Untrusted web content:"));
    assert!(result.output.contains("<p>reference</p>"));
    let requests = client.requests.lock().unwrap();
    assert_eq!(requests[0].allowed_domains, ["example.com"]);
    assert_eq!(requests[0].route, WebFetchRoute::Direct);
    assert_eq!(requests[0].max_response_bytes, 4096);
    assert_eq!(requests[0].max_redirects, 2);
    assert_eq!(
        authority.0.lock().unwrap().as_slice(),
        &[
            NativeSideEffect::ToolOperation {
                turn: 3,
                ordinal: 2
            },
            NativeSideEffect::ToolOperation {
                turn: 3,
                ordinal: 2
            }
        ]
    );
}

#[tokio::test]
async fn off_policy_url_is_rejected_before_the_fetch_client() {
    let client = Arc::new(FetchClient {
        outcome: WebFetchOutcome {
            final_url: "https://example.com".into(),
            content_type: "text/plain".into(),
            text: "unused".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_fetch_tool(
        &mut registry,
        policy(),
        Arc::clone(&client) as Arc<dyn AuthorizedWebFetchClient>,
    )
    .unwrap();
    let permissions = web_fetch_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();

    let result = registry
        .execute_authorized(
            call("fetch-1", "https://outside.example.net"),
            &context,
            &permissions,
            &RecordingAuthority::default(),
            1,
            1,
        )
        .await
        .unwrap();

    assert_eq!(result.status, ToolInvocationStatus::ToolError);
    assert!(client.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn per_run_fetch_limit_is_enforced_before_a_third_request() {
    let client = Arc::new(FetchClient {
        outcome: WebFetchOutcome {
            final_url: "https://example.com/page".into(),
            content_type: "text/plain".into(),
            text: "ok".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_fetch_tool(
        &mut registry,
        policy(),
        Arc::clone(&client) as Arc<dyn AuthorizedWebFetchClient>,
    )
    .unwrap();
    let permissions = web_fetch_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();
    let authority = RecordingAuthority::default();

    for ordinal in 1..=3 {
        let result = registry
            .execute_authorized(
                call(&format!("fetch-{ordinal}"), "https://example.com/page"),
                &context,
                &permissions,
                &authority,
                1,
                ordinal,
            )
            .await
            .unwrap();
        assert_eq!(
            result.status,
            if ordinal < 3 {
                ToolInvocationStatus::Executed
            } else {
                ToolInvocationStatus::ToolError
            }
        );
    }
    assert_eq!(client.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn off_policy_final_url_aborts_the_native_operation() {
    let client = Arc::new(FetchClient {
        outcome: WebFetchOutcome {
            final_url: "https://outside.example.net/page".into(),
            content_type: "text/plain".into(),
            text: "off-policy".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_fetch_tool(
        &mut registry,
        policy(),
        client as Arc<dyn AuthorizedWebFetchClient>,
    )
    .unwrap();
    let permissions = web_fetch_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();

    let error = registry
        .execute_authorized(
            call("fetch-1", "https://example.com/page"),
            &context,
            &permissions,
            &RecordingAuthority::default(),
            1,
            1,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, vyane_core::ErrorKind::Protocol);
}
