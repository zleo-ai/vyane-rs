#![allow(clippy::unwrap_used)]

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use vyane_core::{
    AuthMaterial, AuthStyle, AuthorizedToolChatClient, CancellationToken, ChatClient, Endpoint,
    ErrorKind, GenParams, ModelId, NativeExecutionAuthority, NativeSideEffect, Result, Secret,
    ToolChatMessage, ToolChatRequest, ToolChoice, ToolDefinition, VyaneError,
};
use vyane_protocol::{AnthropicMessagesClient, ClientOptions, RetryConfig};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AUTHORITY_DENIAL: &str = "native execution authority was revoked";

struct RecordingAuthority {
    effects: Mutex<Vec<NativeSideEffect>>,
    deny_on_call: Option<usize>,
}

impl Default for RecordingAuthority {
    fn default() -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
            deny_on_call: None,
        }
    }
}

impl RecordingAuthority {
    fn denying_on(call: usize) -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
            deny_on_call: Some(call),
        }
    }

    fn effects(&self) -> Vec<NativeSideEffect> {
        self.effects.lock().unwrap().clone()
    }
}

#[async_trait]
impl NativeExecutionAuthority for RecordingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> Result<()> {
        let call = {
            let mut effects = self.effects.lock().unwrap();
            effects.push(effect);
            effects.len()
        };
        if self.deny_on_call == Some(call) {
            return Err(VyaneError::new(ErrorKind::Conflict, AUTHORITY_DENIAL));
        }
        Ok(())
    }
}

fn endpoint(base_url: String) -> Endpoint {
    Endpoint {
        base_url,
        auth: Some(AuthMaterial {
            style: AuthStyle::XApiKey,
            secret: Secret::new("test-only-secret"),
        }),
    }
}

fn client(server: &MockServer, max_attempts: u32) -> AnthropicMessagesClient {
    AnthropicMessagesClient::with_options(
        endpoint(server.uri()),
        ClientOptions {
            retry: RetryConfig::new(max_attempts).without_sleep(),
            request_timeout: Some(Duration::from_secs(60)),
        },
    )
    .unwrap()
}

fn request() -> ToolChatRequest {
    ToolChatRequest {
        model: ModelId::new("model-example"),
        messages: vec![ToolChatMessage::user("inspect the workspace")],
        tools: vec![ToolDefinition {
            name: "read_file".into(),
            description: "Read one workspace file".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }),
        }],
        tool_choice: ToolChoice::Auto,
        params: GenParams::default(),
    }
}

fn success_response() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "model": "model-example",
        "role": "assistant",
        "content": [
            {"type": "text", "text": "done"},
            {
                "type": "tool_use",
                "id": "call-1",
                "name": "read_file",
                "input": {"path": "README.md"}
            }
        ],
        "stop_reason": "tool_use"
    }))
}

// MockServer-backed authorized Anthropic fixtures use two workers so
// wiremock and the client path stay independently schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_wire_retry_is_revalidated_before_it_is_sent() {
    let server = MockServer::start().await;
    let responses = std::sync::Arc::new(AtomicUsize::new(0));
    let responses_for_mock = std::sync::Arc::clone(&responses);
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "test-only-secret"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(move |_: &wiremock::Request| {
            if responses_for_mock.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(503).set_body_string("transient")
            } else {
                success_response()
            }
        })
        .mount(&server)
        .await;

    let authority = RecordingAuthority::default();
    let outcome = client(&server, 2)
        .complete_turn_authorized(request(), 7, &authority, &CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.assistant.text, "done");
    assert_eq!(outcome.assistant.tool_calls.len(), 1);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(
        authority.effects(),
        vec![
            NativeSideEffect::ModelSend {
                turn: 7,
                wire_attempt: 1,
            },
            NativeSideEffect::ModelSend {
                turn: 7,
                wire_attempt: 2,
            },
        ]
    );
}

// MockServer-backed authorized Anthropic fixtures use two workers so
// wiremock and the client path stay independently schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_complete_turn_does_not_consume_authority() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(success_response())
        .mount(&server)
        .await;

    let authority = RecordingAuthority::denying_on(1);
    let outcome = client(&server, 1).complete_turn(request()).await.unwrap();

    assert_eq!(outcome.assistant.tool_calls[0].id, "call-1");
    assert!(authority.effects().is_empty());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

// MockServer-backed authorized Anthropic fixtures use two workers so
// wiremock and the client path stay independently schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_before_send_prevents_http_and_does_not_use_ordinary_turn() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(success_response())
        .mount(&server)
        .await;

    let authority = RecordingAuthority::denying_on(1);
    let error = client(&server, 3)
        .complete_turn_authorized(request(), 11, &authority, &CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(error.message, AUTHORITY_DENIAL);
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(
        authority.effects(),
        vec![NativeSideEffect::ModelSend {
            turn: 11,
            wire_attempt: 1,
        }]
    );
}

// MockServer-backed authorized Anthropic fixtures use two workers so
// wiremock and the client path stay independently schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_typed_request_fails_before_authority_or_http() {
    let server = MockServer::start().await;
    let authority = RecordingAuthority::default();
    let mut invalid = request();
    invalid.messages.clear();

    let error = client(&server, 3)
        .complete_turn_authorized(invalid, 1, &authority, &CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Config);
    assert!(authority.effects().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}

// MockServer-backed authorized Anthropic fixtures use two workers so
// wiremock and the client path stay independently schedulable under suite load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_cancelled_turn_neither_authorizes_nor_sends() {
    let server = MockServer::start().await;
    let authority = RecordingAuthority::default();
    let cancel = CancellationToken::new();
    cancel.cancel();

    let error = client(&server, 3)
        .complete_turn_authorized(request(), 1, &authority, &cancel)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(authority.effects().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}
