use async_trait::async_trait;
use futures::stream::BoxStream;
use reqwest::RequestBuilder;
use vyane_core::{
    AuthorizedToolChatClient, CancellationToken, ChatClient, ChatOutcome, ChatRequest, Endpoint,
    ErrorKind, NativeExecutionAuthority, Protocol, Result, StreamEvent, ToolChatOutcome,
    ToolChatRequest, VyaneError,
};

use crate::http::{ClientOptions, HttpClient};
use crate::sse::{StreamProtocol, response_to_stream};
use crate::wire;

const PATH: &str = "/v1/messages";

/// Default `max_tokens` sent to Anthropic Messages when
/// `GenParams.max_output_tokens` is unset. Anthropic requires this field on
/// the wire.
pub const DEFAULT_MAX_TOKENS: u32 = wire::anthropic::DEFAULT_MAX_TOKENS;

/// Anthropic Messages requires `max_tokens`; when callers leave
/// `GenParams.max_output_tokens` unset, requests use
/// [`DEFAULT_MAX_TOKENS`].
#[derive(Debug, Clone)]
pub struct AnthropicMessagesClient {
    http: HttpClient,
}

impl AnthropicMessagesClient {
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
impl ChatClient for AnthropicMessagesClient {
    fn protocol(&self) -> Protocol {
        Protocol::AnthropicMessages
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome> {
        let body = wire::anthropic::Request::from(&req);
        let response: wire::anthropic::Response = self
            .http
            .post_json(PATH, body, |request| self.decorate(request))
            .await?;
        response.try_into()
    }

    async fn complete_turn(&self, req: ToolChatRequest) -> Result<ToolChatOutcome> {
        req.validate().map_err(|error| {
            VyaneError::new(
                ErrorKind::Config,
                format!("invalid typed tool-chat request: {error}"),
            )
        })?;
        let body = wire::anthropic_tool_chat::Request::try_from(&req)?;
        let response: wire::anthropic_tool_chat::Response = self
            .http
            .post_json(PATH, body, |request| self.decorate(request))
            .await?;
        response.try_into()
    }

    async fn stream(&self, req: ChatRequest) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let mut body = wire::anthropic::Request::from(&req);
        body.extra
            .entry("stream".to_string())
            .or_insert(serde_json::Value::Bool(true));
        let response = self
            .http
            .post_stream(PATH, body, |request| self.decorate(request))
            .await?;
        Ok(response_to_stream(response, StreamProtocol::Anthropic))
    }
}

#[async_trait]
impl AuthorizedToolChatClient for AnthropicMessagesClient {
    fn protocol(&self) -> Protocol {
        Protocol::AnthropicMessages
    }

    async fn complete_turn_authorized(
        &self,
        req: ToolChatRequest,
        turn: u32,
        authority: &dyn NativeExecutionAuthority,
        cancel: &CancellationToken,
    ) -> Result<ToolChatOutcome> {
        req.validate().map_err(|error| {
            VyaneError::new(
                ErrorKind::Config,
                format!("invalid typed tool-chat request: {error}"),
            )
        })?;
        let body = wire::anthropic_tool_chat::Request::try_from(&req)?;
        let response: wire::anthropic_tool_chat::Response = self
            .http
            .post_json_authorized(
                PATH,
                body,
                |request| self.decorate(request),
                turn,
                authority,
                cancel,
            )
            .await?;
        response.try_into()
    }
}

impl AnthropicMessagesClient {
    fn decorate(&self, request: RequestBuilder) -> RequestBuilder {
        request
            .header("accept", "application/json")
            .header("anthropic-version", wire::anthropic::VERSION)
    }
}
