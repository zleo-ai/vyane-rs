use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use serde_json::{Value, json};
use vyane_core::{
    AuthMaterial, AuthStyle, ChatClient, ChatMessage, ChatRequest, Effort, Endpoint, ErrorKind,
    GenParams, ModelId, Secret, StreamEvent, Usage,
};
use vyane_protocol::{
    AnthropicMessagesClient, ClientOptions, OpenAiChatClient, OpenAiResponsesClient, RetryConfig,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn bearer_endpoint(base_url: String) -> Endpoint {
    endpoint(base_url, AuthStyle::Bearer)
}

fn x_api_key_endpoint(base_url: String) -> Endpoint {
    endpoint(base_url, AuthStyle::XApiKey)
}

fn endpoint(base_url: String, style: AuthStyle) -> Endpoint {
    Endpoint {
        base_url,
        auth: Some(AuthMaterial {
            style,
            secret: Secret::new("sk-test"),
        }),
    }
}

fn request() -> ChatRequest {
    ChatRequest {
        model: ModelId::new("model-example"),
        messages: vec![
            ChatMessage::system("system prompt"),
            ChatMessage::user("hello"),
        ],
        params: GenParams {
            temperature: Some(0.2),
            top_p: Some(0.9),
            max_output_tokens: Some(32),
            effort: Some(Effort::Low),
            extra: serde_json::Map::new(),
        },
    }
}

fn client_options(max_attempts: u32) -> ClientOptions {
    ClientOptions {
        retry: RetryConfig::new(max_attempts).without_sleep(),
        request_timeout: Some(Duration::from_secs(10)),
    }
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn openai_chat_complete_success_parses_outcome_and_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer sk-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl-example",
            "model": "model-echo",
            "choices": [{
                "message": { "role": "assistant", "content": "answer" },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 7,
                "completion_tokens": 5,
                "prompt_tokens_details": { "cached_tokens": 2 },
                "completion_tokens_details": { "reasoning_tokens": 1 }
            }
        })))
        .mount(&server)
        .await;

    let client =
        OpenAiChatClient::with_options(bearer_endpoint(server.uri()), client_options(1)).unwrap();
    let outcome = client.complete(request()).await.unwrap();

    assert_eq!(outcome.text, "answer");
    assert_eq!(outcome.model_echo.as_deref(), Some("model-echo"));
    assert_eq!(outcome.finish_reason.as_deref(), Some("stop"));
    assert_eq!(
        outcome.usage,
        Some(Usage {
            input_tokens: 7,
            output_tokens: 5,
            reasoning_tokens: Some(1),
            cached_input_tokens: Some(2),
        })
    );

    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["model"], "model-example");
    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(body["messages"][0]["content"], "system prompt");
    assert_eq!(body["max_tokens"], 32);
    assert_eq!(body["reasoning_effort"], "low");
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn anthropic_complete_success_parses_outcome_and_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "sk-test"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg-example",
            "model": "anthropic-echo",
            "content": [{ "type": "text", "text": "hello from anthropic" }],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 9,
                "output_tokens": 4,
                "cache_read_input_tokens": 3
            }
        })))
        .mount(&server)
        .await;

    let client =
        AnthropicMessagesClient::with_options(x_api_key_endpoint(server.uri()), client_options(1))
            .unwrap();
    let mut req = request();
    req.params.max_output_tokens = None;
    let outcome = client.complete(req).await.unwrap();

    assert_eq!(outcome.text, "hello from anthropic");
    assert_eq!(outcome.model_echo.as_deref(), Some("anthropic-echo"));
    assert_eq!(outcome.finish_reason.as_deref(), Some("end_turn"));
    assert_eq!(
        outcome.usage,
        Some(Usage {
            input_tokens: 9,
            output_tokens: 4,
            reasoning_tokens: None,
            cached_input_tokens: Some(3),
        })
    );

    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["system"], "system prompt");
    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["messages"][0]["content"], "hello");
    assert_eq!(body["max_tokens"], 8192);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn anthropic_complete_with_bearer_auth_sends_version_header() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("authorization", "Bearer sk-test"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg-example",
            "model": "anthropic-echo",
            "content": [{ "type": "text", "text": "bearer answer" }],
            "stop_reason": "end_turn"
        })))
        .mount(&server)
        .await;

    let client =
        AnthropicMessagesClient::with_options(bearer_endpoint(server.uri()), client_options(1))
            .unwrap();
    let outcome = client.complete(request()).await.unwrap();

    assert_eq!(outcome.text, "bearer answer");
    assert_eq!(outcome.model_echo.as_deref(), Some("anthropic-echo"));
    assert_eq!(outcome.finish_reason.as_deref(), Some("end_turn"));
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn openai_responses_complete_success_parses_outcome() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .and(header("authorization", "Bearer sk-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "resp-example",
            "model": "responses-echo",
            "output_text": "responses answer",
            "usage": {
                "input_tokens": 8,
                "output_tokens": 6,
                "input_tokens_details": { "cached_tokens": 1 },
                "output_tokens_details": { "reasoning_tokens": 2 }
            }
        })))
        .mount(&server)
        .await;

    let client =
        OpenAiResponsesClient::with_options(bearer_endpoint(server.uri()), client_options(1))
            .unwrap();
    let outcome = client.complete(request()).await.unwrap();

    assert_eq!(outcome.text, "responses answer");
    assert_eq!(outcome.model_echo.as_deref(), Some("responses-echo"));
    assert_eq!(
        outcome.usage,
        Some(Usage {
            input_tokens: 8,
            output_tokens: 6,
            reasoning_tokens: Some(2),
            cached_input_tokens: Some(1),
        })
    );

    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["model"], "model-example");
    assert!(body.get("messages").is_none());
    assert!(body.get("instructions").is_none());
    assert_eq!(body["input"][0]["role"], "system");
    assert_eq!(body["input"][0]["content"], "system prompt");
    assert_eq!(body["input"][1]["role"], "user");
    assert_eq!(body["input"][1]["content"], "hello");
    assert_eq!(body["max_output_tokens"], 32);
    assert_eq!(body["reasoning"]["effort"], "low");
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn auth_errors_do_not_retry() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(401).set_body_string("bad key"))
        .mount(&server)
        .await;

    let client =
        OpenAiChatClient::with_options(bearer_endpoint(server.uri()), client_options(3)).unwrap();
    let error = client.complete(request()).await.unwrap_err();

    assert_eq!(error.kind, ErrorKind::Auth);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn rate_limit_retries_with_retry_after_then_surfaces_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "1")
                .set_body_string("rate limited"),
        )
        .mount(&server)
        .await;

    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let sleeps_for_retry = Arc::clone(&sleeps);
    let retry = RetryConfig::new(2).with_sleeper(move |delay| {
        let sleeps = Arc::clone(&sleeps_for_retry);
        async move {
            sleeps.lock().unwrap().push(delay);
        }
    });
    let client = OpenAiChatClient::with_options(
        bearer_endpoint(server.uri()),
        ClientOptions {
            retry,
            request_timeout: None,
        },
    )
    .unwrap();
    let error = client.complete(request()).await.unwrap_err();

    assert_eq!(error.kind, ErrorKind::RateLimited);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(sleeps.lock().unwrap().as_slice(), &[Duration::from_secs(1)]);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn server_errors_retry_then_surface_protocol() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;

    let client =
        OpenAiChatClient::with_options(bearer_endpoint(server.uri()), client_options(2)).unwrap();
    let error = client.complete(request()).await.unwrap_err();

    assert_eq!(error.kind, ErrorKind::Protocol);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn openai_chat_stream_normalizes_events() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n",
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;

    let client =
        OpenAiChatClient::with_options(bearer_endpoint(server.uri()), client_options(1)).unwrap();
    let events = client
        .stream(request())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    let events = events.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "hi"));
    assert!(matches!(
        &events[1],
        StreamEvent::Usage(Usage {
            input_tokens: 3,
            output_tokens: 2,
            ..
        })
    ));
    assert!(matches!(
        &events[2],
        StreamEvent::Done {
            finish_reason: Some(reason)
        } if reason == "stop"
    ));

    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(body["stream"], true);
    assert_eq!(body["stream_options"]["include_usage"], true);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn anthropic_stream_normalizes_events() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reason\"}}\n\n",
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"answer\"}}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":6}}\n\n",
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;

    let client =
        AnthropicMessagesClient::with_options(x_api_key_endpoint(server.uri()), client_options(1))
            .unwrap();
    let events = client
        .stream(request())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    let events = events.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(events.len(), 4);
    assert!(matches!(
        &events[0],
        StreamEvent::ReasoningDelta(text) if text == "reason"
    ));
    assert!(matches!(&events[1], StreamEvent::Delta(text) if text == "answer"));
    assert!(matches!(
        &events[2],
        StreamEvent::Usage(Usage {
            input_tokens: 5,
            output_tokens: 6,
            ..
        })
    ));
    assert!(matches!(
        &events[3],
        StreamEvent::Done {
            finish_reason: Some(reason)
        } if reason == "end_turn"
    ));
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn malformed_json_maps_to_protocol_for_each_complete_protocol() {
    for path_name in ["/v1/chat/completions", "/v1/messages", "/v1/responses"] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(path_name))
            .respond_with(ResponseTemplate::new(200).set_body_string("{not-json"))
            .mount(&server)
            .await;

        let error = match path_name {
            "/v1/chat/completions" => {
                OpenAiChatClient::with_options(bearer_endpoint(server.uri()), client_options(1))
                    .unwrap()
                    .complete(request())
                    .await
                    .unwrap_err()
            }
            "/v1/messages" => AnthropicMessagesClient::with_options(
                x_api_key_endpoint(server.uri()),
                client_options(1),
            )
            .unwrap()
            .complete(request())
            .await
            .unwrap_err(),
            "/v1/responses" => OpenAiResponsesClient::with_options(
                bearer_endpoint(server.uri()),
                client_options(1),
            )
            .unwrap()
            .complete(request())
            .await
            .unwrap_err(),
            _ => unreachable!(),
        };

        assert_eq!(error.kind, ErrorKind::Protocol);
    }
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn malformed_stream_json_maps_to_protocol() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw("data: {not-json}\n\n", "text/event-stream"),
        )
        .mount(&server)
        .await;

    let client =
        OpenAiChatClient::with_options(bearer_endpoint(server.uri()), client_options(1)).unwrap();
    let mut stream = client.stream(request()).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();

    assert_eq!(error.kind, ErrorKind::Protocol);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn responses_streaming_is_unsupported() {
    let server = MockServer::start().await;
    let client =
        OpenAiResponsesClient::with_options(bearer_endpoint(server.uri()), client_options(1))
            .unwrap();

    let error = match client.stream(request()).await {
        Ok(_) => panic!("Responses streaming unexpectedly succeeded"),
        Err(error) => error,
    };

    assert_eq!(error.kind, ErrorKind::Unsupported);
}
