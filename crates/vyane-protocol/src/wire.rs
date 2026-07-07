use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use vyane_core::{
    ChatMessage, ChatOutcome, ChatRequest, Effort, ErrorKind, Result, Role, Usage, VyaneError,
};

pub(crate) mod openai_chat {
    use super::*;

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Request {
        pub(crate) model: String,
        pub(crate) messages: Vec<Message>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) temperature: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) top_p: Option<f32>,
        #[serde(flatten)]
        pub(crate) extra: Map<String, Value>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Message {
        pub(crate) role: &'static str,
        pub(crate) content: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct Response {
        pub(crate) model: Option<String>,
        #[serde(default)]
        pub(crate) choices: Vec<Choice>,
        pub(crate) usage: Option<UsageResponse>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct Choice {
        pub(crate) message: Option<AssistantMessage>,
        pub(crate) finish_reason: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct AssistantMessage {
        pub(crate) content: Option<Content>,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(untagged)]
    pub(crate) enum Content {
        Text(String),
        Parts(Vec<ContentPart>),
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct ContentPart {
        #[serde(rename = "type")]
        pub(crate) kind: Option<String>,
        pub(crate) text: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct UsageResponse {
        pub(crate) prompt_tokens: Option<u64>,
        pub(crate) completion_tokens: Option<u64>,
        pub(crate) total_tokens: Option<u64>,
        pub(crate) prompt_tokens_details: Option<TokenDetails>,
        pub(crate) completion_tokens_details: Option<TokenDetails>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct TokenDetails {
        pub(crate) cached_tokens: Option<u64>,
        pub(crate) reasoning_tokens: Option<u64>,
    }

    impl From<&ChatRequest> for Request {
        fn from(req: &ChatRequest) -> Self {
            let mut extra = req.params.extra.clone();
            if let Some(max) = req.params.max_output_tokens {
                extra.entry("max_tokens".to_string()).or_insert(json!(max));
            }
            if let Some(effort) = req.params.effort {
                extra
                    .entry("reasoning_effort".to_string())
                    .or_insert(json!(effort.as_str()));
            }

            Self {
                model: req.model.as_str().to_string(),
                messages: req.messages.iter().map(Message::from).collect(),
                temperature: req.params.temperature,
                top_p: req.params.top_p,
                extra,
            }
        }
    }

    impl From<&ChatMessage> for Message {
        fn from(message: &ChatMessage) -> Self {
            Self {
                role: role_name(message.role),
                content: message.content.clone(),
            }
        }
    }

    impl TryFrom<Response> for ChatOutcome {
        type Error = VyaneError;

        fn try_from(response: Response) -> Result<Self> {
            let choice = response.choices.into_iter().next().ok_or_else(|| {
                VyaneError::new(ErrorKind::Protocol, "OpenAI chat response had no choices")
            })?;
            let text = choice
                .message
                .and_then(|message| message.content)
                .map(content_text)
                .unwrap_or_default();

            Ok(Self {
                text,
                usage: response.usage.map(usage_from_response),
                model_echo: response.model,
                finish_reason: choice.finish_reason,
            })
        }
    }

    fn content_text(content: Content) -> String {
        match content {
            Content::Text(text) => text,
            Content::Parts(parts) => parts
                .into_iter()
                .filter(|part| part.kind.as_deref().unwrap_or("text") == "text")
                .filter_map(|part| part.text)
                .collect::<Vec<_>>()
                .join(""),
        }
    }

    pub(crate) fn usage_from_response(usage: UsageResponse) -> Usage {
        Usage {
            input_tokens: usage.prompt_tokens.unwrap_or(0),
            output_tokens: usage
                .completion_tokens
                .or_else(|| {
                    usage
                        .total_tokens
                        .zip(usage.prompt_tokens)
                        .map(|(total, input)| total.saturating_sub(input))
                })
                .unwrap_or(0),
            reasoning_tokens: usage
                .completion_tokens_details
                .and_then(|details| details.reasoning_tokens),
            cached_input_tokens: usage
                .prompt_tokens_details
                .and_then(|details| details.cached_tokens),
        }
    }
}

pub(crate) mod anthropic {
    use super::*;

    pub(crate) const DEFAULT_MAX_TOKENS: u32 = 8192;
    pub(crate) const VERSION: &str = "2023-06-01";

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Request {
        pub(crate) model: String,
        pub(crate) messages: Vec<Message>,
        pub(crate) max_tokens: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) system: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) temperature: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) top_p: Option<f32>,
        #[serde(flatten)]
        pub(crate) extra: Map<String, Value>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Message {
        pub(crate) role: &'static str,
        pub(crate) content: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct Response {
        pub(crate) model: Option<String>,
        #[serde(default)]
        pub(crate) content: Vec<ContentBlock>,
        pub(crate) stop_reason: Option<String>,
        pub(crate) usage: Option<UsageResponse>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct ContentBlock {
        #[serde(rename = "type")]
        pub(crate) kind: String,
        pub(crate) text: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct UsageResponse {
        pub(crate) input_tokens: Option<u64>,
        pub(crate) output_tokens: Option<u64>,
        pub(crate) cache_read_input_tokens: Option<u64>,
        pub(crate) cache_creation_input_tokens: Option<u64>,
    }

    impl From<&ChatRequest> for Request {
        fn from(req: &ChatRequest) -> Self {
            let mut extra = req.params.extra.clone();
            if let Some(effort) = req.params.effort {
                extra.entry("thinking".to_string()).or_insert(
                    json!({ "type": "enabled", "budget_tokens": effort_budget(effort) }),
                );
            }

            let system = req
                .messages
                .iter()
                .find(|message| message.role == Role::System)
                .map(|message| message.content.clone());
            let messages = req
                .messages
                .iter()
                .filter(|message| message.role != Role::System)
                .map(Message::from)
                .collect();

            Self {
                model: req.model.as_str().to_string(),
                messages,
                max_tokens: req.params.max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
                system,
                temperature: req.params.temperature,
                top_p: req.params.top_p,
                extra,
            }
        }
    }

    impl From<&ChatMessage> for Message {
        fn from(message: &ChatMessage) -> Self {
            let role = match message.role {
                Role::Assistant => "assistant",
                Role::System | Role::User => "user",
            };
            Self {
                role,
                content: message.content.clone(),
            }
        }
    }

    impl TryFrom<Response> for ChatOutcome {
        type Error = VyaneError;

        fn try_from(response: Response) -> Result<Self> {
            let text = response
                .content
                .into_iter()
                .filter(|block| block.kind == "text")
                .filter_map(|block| block.text)
                .collect::<Vec<_>>()
                .join("");

            Ok(Self {
                text,
                usage: response.usage.map(usage_from_response),
                model_echo: response.model,
                finish_reason: response.stop_reason,
            })
        }
    }

    pub(crate) fn usage_from_response(usage: UsageResponse) -> Usage {
        let cached = usage
            .cache_read_input_tokens
            .unwrap_or(0)
            .saturating_add(usage.cache_creation_input_tokens.unwrap_or(0));
        Usage {
            input_tokens: usage.input_tokens.unwrap_or(0),
            output_tokens: usage.output_tokens.unwrap_or(0),
            reasoning_tokens: None,
            cached_input_tokens: (cached > 0).then_some(cached),
        }
    }

    fn effort_budget(effort: Effort) -> u32 {
        match effort {
            Effort::Low => 1024,
            Effort::Medium => 4096,
            Effort::High => 8192,
            Effort::Xhigh => 16384,
        }
    }
}

pub(crate) mod openai_responses {
    use super::*;

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Request {
        pub(crate) model: String,
        pub(crate) input: Vec<InputMessage>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) temperature: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) top_p: Option<f32>,
        #[serde(flatten)]
        pub(crate) extra: Map<String, Value>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct InputMessage {
        pub(crate) role: &'static str,
        pub(crate) content: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct Response {
        pub(crate) model: Option<String>,
        #[serde(default)]
        pub(crate) output: Vec<OutputItem>,
        #[serde(default)]
        pub(crate) output_text: Option<String>,
        pub(crate) usage: Option<UsageResponse>,
        pub(crate) incomplete_details: Option<IncompleteDetails>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct OutputItem {
        #[serde(rename = "type")]
        pub(crate) kind: Option<String>,
        #[serde(default)]
        pub(crate) content: Vec<OutputContent>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct OutputContent {
        #[serde(rename = "type")]
        pub(crate) kind: Option<String>,
        pub(crate) text: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct IncompleteDetails {
        pub(crate) reason: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct UsageResponse {
        pub(crate) input_tokens: Option<u64>,
        pub(crate) output_tokens: Option<u64>,
        pub(crate) input_tokens_details: Option<TokenDetails>,
        pub(crate) output_tokens_details: Option<TokenDetails>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct TokenDetails {
        pub(crate) cached_tokens: Option<u64>,
        pub(crate) reasoning_tokens: Option<u64>,
    }

    impl From<&ChatRequest> for Request {
        fn from(req: &ChatRequest) -> Self {
            let mut extra = req.params.extra.clone();
            if let Some(max) = req.params.max_output_tokens {
                extra
                    .entry("max_output_tokens".to_string())
                    .or_insert(json!(max));
            }
            if let Some(effort) = req.params.effort {
                extra
                    .entry("reasoning".to_string())
                    .or_insert(json!({ "effort": effort.as_str() }));
            }

            Self {
                model: req.model.as_str().to_string(),
                input: req.messages.iter().map(InputMessage::from).collect(),
                temperature: req.params.temperature,
                top_p: req.params.top_p,
                extra,
            }
        }
    }

    impl From<&ChatMessage> for InputMessage {
        fn from(message: &ChatMessage) -> Self {
            Self {
                role: role_name(message.role),
                content: message.content.clone(),
            }
        }
    }

    impl TryFrom<Response> for ChatOutcome {
        type Error = VyaneError;

        fn try_from(response: Response) -> Result<Self> {
            let text = response.output_text.unwrap_or_else(|| {
                response
                    .output
                    .into_iter()
                    .filter(|item| item.kind.as_deref().unwrap_or("message") == "message")
                    .flat_map(|item| item.content)
                    .filter(|content| {
                        matches!(
                            content.kind.as_deref(),
                            Some("output_text") | Some("text") | None
                        )
                    })
                    .filter_map(|content| content.text)
                    .collect::<Vec<_>>()
                    .join("")
            });
            let finish_reason = response
                .incomplete_details
                .and_then(|details| details.reason);

            Ok(Self {
                text,
                usage: response.usage.map(usage_from_response),
                model_echo: response.model,
                finish_reason,
            })
        }
    }

    pub(crate) fn usage_from_response(usage: UsageResponse) -> Usage {
        Usage {
            input_tokens: usage.input_tokens.unwrap_or(0),
            output_tokens: usage.output_tokens.unwrap_or(0),
            reasoning_tokens: usage
                .output_tokens_details
                .and_then(|details| details.reasoning_tokens),
            cached_input_tokens: usage
                .input_tokens_details
                .and_then(|details| details.cached_tokens),
        }
    }
}

pub(crate) fn role_name(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}
