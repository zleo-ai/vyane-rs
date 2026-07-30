#![allow(clippy::unwrap_used)]

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use vyane_core::{
    AuthMaterial, AuthStyle, AuthorizedWebSearchClient, CancellationToken, Endpoint, GenParams,
    ModelId, NativeExecutionAuthority, NativeSideEffect, Result, Secret, WebSearchContextSize,
    WebSearchRequest,
};
use vyane_protocol::{ClientOptions, OpenAiResponsesClient, RetryConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Default)]
struct RecordingAuthority(Mutex<Vec<NativeSideEffect>>);

#[async_trait]
impl NativeExecutionAuthority for RecordingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> Result<()> {
        self.0.lock().unwrap().push(effect);
        Ok(())
    }
}

struct DenySecondAuthority(Mutex<usize>);

#[async_trait]
impl NativeExecutionAuthority for DenySecondAuthority {
    async fn revalidate(&self, _effect: NativeSideEffect) -> Result<()> {
        let mut calls = self.0.lock().unwrap();
        *calls += 1;
        if *calls == 2 {
            return Err(vyane_core::VyaneError::new(
                vyane_core::ErrorKind::Conflict,
                "revoked",
            ));
        }
        Ok(())
    }
}

fn client(server: &MockServer) -> OpenAiResponsesClient {
    client_with_attempts(server, 1)
}

fn client_with_attempts(server: &MockServer, max_attempts: u32) -> OpenAiResponsesClient {
    OpenAiResponsesClient::with_options(
        Endpoint {
            base_url: server.uri(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("search-test-secret"),
            }),
        },
        ClientOptions {
            retry: RetryConfig::new(max_attempts).without_sleep(),
            request_timeout: Some(Duration::from_secs(30)),
        },
    )
    .unwrap()
}

fn request() -> WebSearchRequest {
    WebSearchRequest {
        model: ModelId::new("search-model"),
        query: "current Rust release notes".into(),
        allowed_domains: Some(vec!["rust-lang.org".into()]),
        max_searches: 3,
        context_size: WebSearchContextSize::High,
        params: GenParams::default(),
    }
}

// Authorized web-search fixtures drive MockServer plus cancel/retry paths.
// Independent Tokio workers keep the mock schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_is_bounded_and_response_preserves_cited_sources() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "search-model",
            "output": [
                {
                    "type": "web_search_call",
                    "action": {
                        "type": "search",
                        "sources": [
                            {"type": "url", "url": "https://www.rust-lang.org/learn"}
                        ]
                    }
                },
                {
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "Rust search answer.",
                        "annotations": [{
                            "type": "url_citation",
                            "url": "https://www.rust-lang.org/learn",
                            "title": "Learn Rust"
                        }]
                    }]
                }
            ],
            "usage": {"input_tokens": 11, "output_tokens": 7}
        })))
        .mount(&server)
        .await;

    let authority = RecordingAuthority::default();
    let effect = NativeSideEffect::ToolOperation {
        turn: 2,
        ordinal: 1,
    };
    let outcome = client(&server)
        .search_authorized(request(), &authority, effect, &CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.text, "Rust search answer.");
    assert_eq!(outcome.sources.len(), 1);
    assert_eq!(outcome.sources[0].title.as_deref(), Some("Learn Rust"));
    assert_eq!(authority.0.lock().unwrap().as_slice(), &[effect]);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer search-test-secret"
    );
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["model"], "search-model");
    assert_eq!(body["input"], "current Rust release notes");
    assert_eq!(body["store"], false);
    assert_eq!(body["tool_choice"], "required");
    assert_eq!(body["max_tool_calls"], 3);
    assert_eq!(body["tools"][0]["type"], "web_search");
    assert_eq!(body["tools"][0]["search_context_size"], "high");
    assert_eq!(
        body["tools"][0]["filters"]["allowed_domains"],
        json!(["rust-lang.org"])
    );
    assert_eq!(body["include"], json!(["web_search_call.action.sources"]));
}

// Authorized web-search fixtures drive MockServer plus cancel/retry paths.
// Independent Tokio workers keep the mock schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_or_oversized_sources_fail_as_protocol_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "output": [{
                "type": "web_search_call",
                "action": {"sources": [{"url": "x".repeat(9000)}]}
            }]
        })))
        .mount(&server)
        .await;

    let error = client(&server)
        .search_authorized(
            request(),
            &RecordingAuthority::default(),
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, vyane_core::ErrorKind::Protocol);
}

// Authorized web-search fixtures drive MockServer plus cancel/retry paths.
// Independent Tokio workers keep the mock schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_before_retry_prevents_the_next_search_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let error = client_with_attempts(&server, 2)
        .search_authorized(
            request(),
            &DenySecondAuthority(Mutex::new(0)),
            NativeSideEffect::ToolOperation {
                turn: 3,
                ordinal: 2,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind, vyane_core::ErrorKind::Conflict);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

// Authorized web-search fixtures drive MockServer plus cancel/retry paths.
// Independent Tokio workers keep the mock schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_cancelled_search_sends_nothing() {
    let server = MockServer::start().await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = client(&server)
        .search_authorized(
            request(),
            &RecordingAuthority::default(),
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1,
            },
            &cancel,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, vyane_core::ErrorKind::Cancelled);
    assert!(server.received_requests().await.unwrap().is_empty());
}
