#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Notify;
use vyane_core::{
    AuthMaterial, AuthStyle, AuthorizedToolChatClient, CancellationToken, Endpoint, ErrorKind,
    GenParams, ModelId, NativeExecutionAuthority, NativeSideEffect, Result, Secret,
    ToolChatMessage, ToolChatRequest, ToolChoice, ToolDefinition, VyaneError,
};
use vyane_protocol::{ClientOptions, OpenAiChatClient, RetryConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AUTHORITY_DENIAL: &str = "native execution authority was revoked";

struct RecordingAuthority {
    effects: Mutex<Vec<NativeSideEffect>>,
    deny_on_call: Option<usize>,
    denial_kind: ErrorKind,
}

impl Default for RecordingAuthority {
    fn default() -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
            deny_on_call: None,
            denial_kind: ErrorKind::Conflict,
        }
    }
}

impl RecordingAuthority {
    fn denying_on(call: usize) -> Self {
        Self::denying_on_with_kind(call, ErrorKind::Conflict)
    }

    fn denying_on_with_kind(call: usize, denial_kind: ErrorKind) -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
            deny_on_call: Some(call),
            denial_kind,
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
            return Err(VyaneError::new(self.denial_kind, AUTHORITY_DENIAL));
        }
        Ok(())
    }
}

struct CancellingAuthority(CancellationToken);

#[async_trait]
impl NativeExecutionAuthority for CancellingAuthority {
    async fn revalidate(&self, _effect: NativeSideEffect) -> Result<()> {
        self.0.cancel();
        Ok(())
    }
}

struct BlockingAuthority {
    entered: Arc<Notify>,
}

#[async_trait]
impl NativeExecutionAuthority for BlockingAuthority {
    async fn revalidate(&self, _effect: NativeSideEffect) -> Result<()> {
        self.entered.notify_one();
        std::future::pending().await
    }
}

fn endpoint(base_url: String, secret: &str) -> Endpoint {
    Endpoint {
        base_url,
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new(secret),
        }),
    }
}

fn client(server: &MockServer, max_attempts: u32, retry: Option<RetryConfig>) -> OpenAiChatClient {
    OpenAiChatClient::with_options(
        endpoint(server.uri(), "test-only-secret"),
        ClientOptions {
            retry: retry.unwrap_or_else(|| RetryConfig::new(max_attempts).without_sleep()),
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
        "choices": [{
            "message": {"role": "assistant", "content": "done"},
            "finish_reason": "stop"
        }]
    }))
}

#[tokio::test]
async fn every_wire_retry_is_revalidated_before_it_is_sent() {
    let server = MockServer::start().await;
    let responses = Arc::new(AtomicUsize::new(0));
    let responses_for_mock = Arc::clone(&responses);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
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
    let outcome = client(&server, 2, None)
        .complete_turn_authorized(request(), 7, &authority, &CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.assistant.text, "done");
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

#[tokio::test]
async fn redirects_never_create_an_unvalidated_physical_request() {
    let redirect_target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(success_response())
        .mount(&redirect_target)
        .await;

    let origin = MockServer::start().await;
    let location = format!("{}/v1/chat/completions", redirect_target.uri());
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(307).insert_header("location", location.as_str()))
        .mount(&origin)
        .await;

    let authority = RecordingAuthority::default();
    let error = client(&origin, 1, None)
        .complete_turn_authorized(request(), 8, &authority, &CancellationToken::new())
        .await
        .expect_err("redirects must remain terminal responses");

    assert_eq!(error.kind, ErrorKind::Protocol);
    assert_eq!(origin.received_requests().await.unwrap().len(), 1);
    assert!(
        redirect_target
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        authority.effects(),
        vec![NativeSideEffect::ModelSend {
            turn: 8,
            wire_attempt: 1,
        }]
    );
}

#[tokio::test]
async fn revocation_before_a_retry_prevents_the_second_http_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503).set_body_string("retry me"))
        .mount(&server)
        .await;

    let authority = RecordingAuthority::denying_on(2);
    let error = client(&server, 3, None)
        .complete_turn_authorized(request(), 11, &authority, &CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(error.message, AUTHORITY_DENIAL);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert_eq!(
        authority.effects(),
        vec![
            NativeSideEffect::ModelSend {
                turn: 11,
                wire_attempt: 1,
            },
            NativeSideEffect::ModelSend {
                turn: 11,
                wire_attempt: 2,
            },
        ]
    );
}

#[tokio::test]
async fn retryable_authority_error_is_returned_without_http_or_internal_retry() {
    let server = MockServer::start().await;
    let authority = RecordingAuthority::denying_on_with_kind(1, ErrorKind::Transport);

    let error = client(&server, 3, None)
        .complete_turn_authorized(request(), 13, &authority, &CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Transport);
    assert_eq!(error.message, AUTHORITY_DENIAL);
    assert_eq!(
        authority.effects(),
        vec![NativeSideEffect::ModelSend {
            turn: 13,
            wire_attempt: 1,
        }]
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn pre_cancelled_turn_neither_authorizes_nor_sends() {
    let server = MockServer::start().await;
    let authority = RecordingAuthority::default();
    let cancel = CancellationToken::new();
    cancel.cancel();

    let error = client(&server, 3, None)
        .complete_turn_authorized(request(), 1, &authority, &cancel)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(authority.effects().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_during_revalidation_is_observed_before_send() {
    let server = MockServer::start().await;
    let cancel = CancellationToken::new();
    let authority = CancellingAuthority(cancel.clone());

    let error = client(&server, 3, None)
        .complete_turn_authorized(request(), 1, &authority, &cancel)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_interrupts_blocked_revalidation_before_send() {
    let server = MockServer::start().await;
    let entered = Arc::new(Notify::new());
    let authority = Arc::new(BlockingAuthority {
        entered: Arc::clone(&entered),
    });
    let cancel = CancellationToken::new();
    let client = client(&server, 3, None);
    let task_authority = Arc::clone(&authority);
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        client
            .complete_turn_authorized(request(), 2, task_authority.as_ref(), &task_cancel)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("authority revalidation was not entered");

    cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("blocked revalidation ignored cancellation")
        .unwrap()
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn invalid_typed_request_fails_before_authority_or_http() {
    let server = MockServer::start().await;
    let authority = RecordingAuthority::default();
    let mut invalid = request();
    invalid.messages.clear();

    let error = client(&server, 3, None)
        .complete_turn_authorized(invalid, 1, &authority, &CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Config);
    assert!(authority.effects().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_interrupts_an_in_flight_send() {
    let server = MockServer::start().await;
    let request_seen = Arc::new(Notify::new());
    let request_seen_by_mock = Arc::clone(&request_seen);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(move |_: &wiremock::Request| {
            request_seen_by_mock.notify_one();
            success_response().set_delay(Duration::from_secs(30))
        })
        .mount(&server)
        .await;

    let authority = Arc::new(RecordingAuthority::default());
    let cancel = CancellationToken::new();
    let client = client(&server, 1, None);
    let task_authority = Arc::clone(&authority);
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        client
            .complete_turn_authorized(request(), 3, task_authority.as_ref(), &task_cancel)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), request_seen.notified())
        .await
        .expect("the guarded HTTP request was not observed");

    cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("in-flight send ignored cancellation")
        .unwrap()
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert_eq!(
        authority.effects(),
        vec![NativeSideEffect::ModelSend {
            turn: 3,
            wire_attempt: 1,
        }]
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn cancellation_interrupts_retry_backoff() {
    let server = MockServer::start().await;
    let request_seen = Arc::new(Notify::new());
    let request_seen_by_mock = Arc::clone(&request_seen);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(move |_: &wiremock::Request| {
            request_seen_by_mock.notify_one();
            ResponseTemplate::new(503).set_body_string("retry later")
        })
        .mount(&server)
        .await;

    let retry = RetryConfig::new(2).with_sleeper(|_| std::future::pending());
    let authority = Arc::new(RecordingAuthority::default());
    let cancel = CancellationToken::new();
    let client = client(&server, 2, Some(retry));
    let task_authority = Arc::clone(&authority);
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        client
            .complete_turn_authorized(request(), 4, task_authority.as_ref(), &task_cancel)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), request_seen.notified())
        .await
        .expect("the first HTTP request was not observed");
    while server.received_requests().await.unwrap().is_empty() {
        tokio::task::yield_now().await;
    }

    cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("retry backoff ignored cancellation")
        .unwrap()
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert_eq!(authority.effects().len(), 1);
}

#[tokio::test]
async fn terminal_http_errors_do_not_echo_auth_or_response_secrets() {
    const AUTH_SECRET: &str = "CANARY_AUTH_SECRET";
    const BODY_SECRET: &str = "CANARY_RESPONSE_BODY";

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503).set_body_string(BODY_SECRET))
        .mount(&server)
        .await;
    let client = OpenAiChatClient::with_options(
        endpoint(server.uri(), AUTH_SECRET),
        ClientOptions {
            retry: RetryConfig::new(1).without_sleep(),
            request_timeout: Some(Duration::from_secs(10)),
        },
    )
    .unwrap();

    let error = client
        .complete_turn_authorized(
            request(),
            1,
            &RecordingAuthority::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    let rendered = format!("{error:?} {error}");

    assert_eq!(error.kind, ErrorKind::Protocol);
    assert!(!rendered.contains(AUTH_SECRET));
    assert!(!rendered.contains(BODY_SECRET));
}
