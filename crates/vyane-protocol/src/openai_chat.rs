use async_trait::async_trait;
use futures::stream::BoxStream;
use reqwest::RequestBuilder;
use vyane_core::{ChatClient, ChatOutcome, ChatRequest, Endpoint, Protocol, Result, StreamEvent};

use crate::http::{ClientOptions, HttpClient};
use crate::sse::{StreamProtocol, response_to_stream};
use crate::wire;

const PATH: &str = "/v1/chat/completions";

#[derive(Debug, Clone)]
pub struct OpenAiChatClient {
    http: HttpClient,
}

impl OpenAiChatClient {
    pub fn new(endpoint: Endpoint) -> Result<Self> {
        Self::with_options(endpoint, ClientOptions::default())
    }

    pub fn with_options(endpoint: Endpoint, options: ClientOptions) -> Result<Self> {
        Ok(Self {
            http: HttpClient::new(endpoint, options)?,
        })
    }
}

#[async_trait]
impl ChatClient for OpenAiChatClient {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiChat
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome> {
        let body = wire::openai_chat::Request::from(&req);
        let response: wire::openai_chat::Response =
            self.http.post_json(PATH, body, |request| request).await?;
        response.try_into()
    }

    async fn stream(&self, req: ChatRequest) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let mut body = wire::openai_chat::Request::from(&req);
        body.extra
            .entry("stream".to_string())
            .or_insert(serde_json::Value::Bool(true));
        body.extra
            .entry("stream_options".to_string())
            .or_insert(serde_json::json!({ "include_usage": true }));
        let response = self.http.post_stream(PATH, body, accept_sse).await?;
        Ok(response_to_stream(response, StreamProtocol::OpenAiChat))
    }
}

fn accept_sse(request: RequestBuilder) -> RequestBuilder {
    request.header("accept", "text/event-stream")
}
