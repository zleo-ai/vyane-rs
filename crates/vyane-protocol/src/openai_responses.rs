use async_trait::async_trait;
use futures::stream::BoxStream;
use vyane_core::{ChatClient, ChatOutcome, ChatRequest, Endpoint, Protocol, Result, StreamEvent};

use crate::http::{ClientOptions, HttpClient};
use crate::sse::{StreamProtocol, response_to_stream};
use crate::wire;

const PATH: &str = "/v1/responses";

#[derive(Debug, Clone)]
pub struct OpenAiResponsesClient {
    http: HttpClient,
}

impl OpenAiResponsesClient {
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
impl ChatClient for OpenAiResponsesClient {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiResponses
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome> {
        let body = wire::openai_responses::Request::from(&req);
        let response: wire::openai_responses::Response =
            self.http.post_json(PATH, body, |request| request).await?;
        response.try_into()
    }

    async fn stream(&self, req: ChatRequest) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let mut body = wire::openai_responses::Request::from(&req);
        body.extra
            .entry("stream".to_string())
            .or_insert(serde_json::Value::Bool(true));
        let response = self.http.post_stream(PATH, body, |request| request).await?;
        Ok(response_to_stream(
            response,
            StreamProtocol::OpenAiResponses,
        ))
    }
}
