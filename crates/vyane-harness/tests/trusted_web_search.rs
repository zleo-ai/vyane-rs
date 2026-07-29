#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use vyane_core::{
    AuthorizedWebSearchClient, CancellationToken, GenParams, ModelId, NativeExecutionAuthority,
    NativeSideEffect, Protocol, Result, WebSearchContextSize, WebSearchOutcome, WebSearchRequest,
    WebSearchSource,
};
use vyane_harness::native::{
    NativeWebSearchPolicy, PermissionPolicy, ToolCall, ToolContext, ToolInvocationStatus,
    ToolRegistry, register_web_search_tool, web_search_permission_policy,
};

struct SearchClient {
    outcome: WebSearchOutcome,
    requests: Mutex<Vec<WebSearchRequest>>,
}

#[async_trait]
impl AuthorizedWebSearchClient for SearchClient {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiResponses
    }

    async fn search_authorized(
        &self,
        req: WebSearchRequest,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
        _cancel: &CancellationToken,
    ) -> Result<WebSearchOutcome> {
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

fn policy() -> NativeWebSearchPolicy {
    NativeWebSearchPolicy {
        allow_domains: Some(vec!["example.com".into()]),
        max_searches: 3,
        search_context_size: WebSearchContextSize::Low,
    }
}

fn call(domains: serde_json::Value) -> ToolCall {
    ToolCall {
        id: "search-1".into(),
        name: "web_search".into(),
        arguments: BTreeMap::from([
            ("query".into(), json!("find the reference")),
            ("allowed_domains".into(), domains),
        ]),
    }
}

#[tokio::test]
async fn model_may_narrow_domains_and_every_search_attempt_is_authorized() {
    let client = Arc::new(SearchClient {
        outcome: WebSearchOutcome {
            text: "A cited answer.".into(),
            sources: vec![WebSearchSource {
                url: "https://docs.example.com/reference".into(),
                title: Some("Reference".into()),
            }],
            usage: None,
            model_echo: None,
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_search_tool(
        &mut registry,
        policy(),
        Arc::clone(&client) as Arc<dyn AuthorizedWebSearchClient>,
        ModelId::new("search-model"),
        GenParams::default(),
    )
    .unwrap();
    let permissions = web_search_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();
    let authority = RecordingAuthority::default();

    let result = registry
        .execute_authorized(
            call(json!(["docs.example.com"])),
            &context,
            &permissions,
            &authority,
            2,
            1,
        )
        .await
        .unwrap();

    assert_eq!(result.status, ToolInvocationStatus::Executed);
    assert!(result.output.contains("A cited answer."));
    assert!(result.output.contains("https://docs.example.com/reference"));
    let requests = client.requests.lock().unwrap();
    assert_eq!(
        requests[0].allowed_domains.as_deref(),
        Some(["docs.example.com".to_string()].as_slice())
    );
    assert_eq!(requests[0].max_searches, 3);
    assert_eq!(
        authority.0.lock().unwrap().as_slice(),
        &[
            NativeSideEffect::ToolOperation {
                turn: 2,
                ordinal: 1
            },
            NativeSideEffect::ToolOperation {
                turn: 2,
                ordinal: 1
            },
            NativeSideEffect::ToolOperation {
                turn: 2,
                ordinal: 1
            }
        ]
    );
}

#[tokio::test]
async fn widening_is_a_tool_error_before_any_search_request() {
    let client = Arc::new(SearchClient {
        outcome: WebSearchOutcome {
            text: "unused".into(),
            sources: Vec::new(),
            usage: None,
            model_echo: None,
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_search_tool(
        &mut registry,
        policy(),
        Arc::clone(&client) as Arc<dyn AuthorizedWebSearchClient>,
        ModelId::new("search-model"),
        GenParams::default(),
    )
    .unwrap();
    let permissions = web_search_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();

    let result = registry
        .execute_authorized(
            call(json!(["outside.example.org"])),
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
async fn provider_source_outside_policy_aborts_the_native_operation() {
    let client = Arc::new(SearchClient {
        outcome: WebSearchOutcome {
            text: "off-policy".into(),
            sources: vec![WebSearchSource {
                url: "https://outside.example.org/result".into(),
                title: None,
            }],
            usage: None,
            model_echo: None,
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut registry = ToolRegistry::new();
    register_web_search_tool(
        &mut registry,
        policy(),
        client as Arc<dyn AuthorizedWebSearchClient>,
        ModelId::new("search-model"),
        GenParams::default(),
    )
    .unwrap();
    let permissions = web_search_permission_policy(PermissionPolicy::deny_by_default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = ToolContext::new(directory.path()).unwrap();

    let error = registry
        .execute_authorized(
            call(json!(["example.com"])),
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
