use async_trait::async_trait;
use vyane_core::{ChatClient, ChatOutcome, ChatRequest, Endpoint, Protocol, Result};

use crate::http::{ClientOptions, HttpClient};
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
}
