use std::collections::BTreeMap;
use std::marker::PhantomData;

use serde::de::{self, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use vyane_core::{
    AssistantContentPart, AssistantToolTurn, ChatMessage, ChatOutcome, ChatRequest, Effort,
    ErrorKind, ModelToolCall, Result, Role, ToolCallArguments, ToolChatLimits, ToolChatMessage,
    ToolChatOutcome, ToolChatRequest, ToolChoice, Usage, VyaneError,
};

const MAX_OPENAI_CHOICES: usize = 8;

fn deserialize_bounded_sequence<'de, D, T>(
    deserializer: D,
    max: usize,
    field: &'static str,
) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedSequenceVisitor<T> {
        max: usize,
        field: &'static str,
        marker: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for BoundedSequenceVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "at most {} {}", self.max, self.field)
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence.size_hint().is_some_and(|hint| hint > self.max) {
                return Err(de::Error::custom(format_args!(
                    "{} exceeds {} entries",
                    self.field, self.max
                )));
            }
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(self.max));
            loop {
                if values.len() == self.max {
                    return match sequence.next_element::<IgnoredAny>()? {
                        Some(_) => Err(de::Error::custom(format_args!(
                            "{} exceeds {} entries",
                            self.field, self.max
                        ))),
                        None => Ok(values),
                    };
                }
                match sequence.next_element::<T>()? {
                    Some(value) => values.push(value),
                    None => return Ok(values),
                }
            }
        }
    }

    deserializer.deserialize_seq(BoundedSequenceVisitor {
        max,
        field,
        marker: PhantomData,
    })
}

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
        #[serde(default, deserialize_with = "deserialize_choices")]
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
        pub(crate) refusal: Option<String>,
    }

    #[derive(Debug, Clone)]
    pub(crate) enum Content {
        Text(String),
        Parts(BoundedContentParts),
    }

    impl<'de> Deserialize<'de> for Content {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct ContentVisitor;

            impl<'de> Visitor<'de> for ContentVisitor {
                type Value = Content;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("OpenAI text or a bounded content-part array")
                }

                fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(Content::Text(value.to_string()))
                }

                fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(Content::Text(value))
                }

                fn visit_seq<A>(self, sequence: A) -> std::result::Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    BoundedContentParts::from_sequence(sequence).map(Content::Parts)
                }
            }

            deserializer.deserialize_any(ContentVisitor)
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct BoundedContentParts(pub(crate) Vec<ContentPart>);

    impl BoundedContentParts {
        fn from_sequence<'de, A>(mut sequence: A) -> std::result::Result<Self, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|hint| hint > ToolChatLimits::CONTENT_PARTS)
            {
                return Err(de::Error::custom("OpenAI content parts exceed the limit"));
            }
            let mut parts = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(ToolChatLimits::CONTENT_PARTS),
            );
            loop {
                if parts.len() == ToolChatLimits::CONTENT_PARTS {
                    return match sequence.next_element::<IgnoredAny>()? {
                        Some(_) => Err(de::Error::custom("OpenAI content parts exceed the limit")),
                        None => Ok(Self(parts)),
                    };
                }
                match sequence.next_element::<ContentPart>()? {
                    Some(part) => parts.push(part),
                    None => return Ok(Self(parts)),
                }
            }
        }
    }

    impl<'de> Deserialize<'de> for BoundedContentParts {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserialize_bounded_sequence(
                deserializer,
                ToolChatLimits::CONTENT_PARTS,
                "OpenAI content parts",
            )
            .map(Self)
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct ContentPart {
        #[serde(rename = "type")]
        pub(crate) kind: Option<String>,
        pub(crate) text: Option<String>,
        pub(crate) refusal: Option<String>,
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
            if response.choices.len() > MAX_OPENAI_CHOICES {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    "OpenAI chat response exceeded the choice limit",
                ));
            }
            let choice = response.choices.into_iter().next().ok_or_else(|| {
                VyaneError::new(ErrorKind::Protocol, "OpenAI chat response had no choices")
            })?;
            let text = match choice.message {
                Some(message) => {
                    if message.refusal.is_some() {
                        return Err(legacy_refusal_error());
                    }
                    message
                        .content
                        .map(content_text)
                        .transpose()?
                        .unwrap_or_default()
                }
                None => String::new(),
            };

            Ok(Self {
                text,
                usage: response.usage.map(usage_from_response),
                model_echo: response.model,
                finish_reason: choice.finish_reason,
            })
        }
    }

    fn content_text(content: Content) -> Result<String> {
        match content {
            Content::Text(text) => Ok(text),
            Content::Parts(parts) => {
                if parts
                    .0
                    .iter()
                    .any(|part| part.refusal.is_some() || part.kind.as_deref() == Some("refusal"))
                {
                    return Err(legacy_refusal_error());
                }
                Ok(parts
                    .0
                    .into_iter()
                    .filter(|part| part.kind.as_deref().unwrap_or("text") == "text")
                    .filter_map(|part| part.text)
                    .collect::<Vec<_>>()
                    .join(""))
            }
        }
    }

    fn legacy_refusal_error() -> VyaneError {
        VyaneError::new(
            ErrorKind::Protocol,
            "OpenAI chat response contained a refusal that legacy text cannot represent",
        )
    }

    fn deserialize_choices<'de, D>(deserializer: D) -> std::result::Result<Vec<Choice>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_bounded_sequence(deserializer, MAX_OPENAI_CHOICES, "OpenAI response choices")
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

pub(crate) mod openai_tool_chat {
    use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

    use super::*;

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Request {
        pub(crate) model: String,
        pub(crate) messages: Vec<Message>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) tools: Vec<Tool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) tool_choice: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) temperature: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) top_p: Option<f32>,
        #[serde(flatten)]
        pub(crate) extra: Map<String, Value>,
    }

    #[derive(Debug, Clone, Serialize)]
    #[serde(untagged)]
    pub(crate) enum Message {
        Text(TextMessage),
        Assistant(AssistantMessageRequest),
        ToolResult(ToolResultRequest),
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct TextMessage {
        role: &'static str,
        content: String,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct AssistantMessageRequest {
        role: &'static str,
        content: Option<AssistantContentRequest>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reasoning_content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        refusal: Option<String>,
        tool_calls: Vec<ToolCallRequest>,
    }

    #[derive(Debug, Clone, Serialize)]
    #[serde(untagged)]
    pub(crate) enum AssistantContentRequest {
        Text(String),
        Parts(Vec<AssistantContentPartRequest>),
    }

    #[derive(Debug, Clone, Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub(crate) enum AssistantContentPartRequest {
        Text { text: String },
        Refusal { refusal: String },
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct ToolResultRequest {
        role: &'static str,
        tool_call_id: String,
        content: String,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct ToolCallRequest {
        id: String,
        #[serde(rename = "type")]
        kind: &'static str,
        function: FunctionCallRequest,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct FunctionCallRequest {
        name: String,
        arguments: String,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct Tool {
        #[serde(rename = "type")]
        kind: &'static str,
        function: FunctionDefinition,
    }

    #[derive(Debug, Clone, Serialize)]
    pub(crate) struct FunctionDefinition {
        name: String,
        description: String,
        parameters: Value,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct Response {
        pub(crate) model: Option<String>,
        #[serde(default, deserialize_with = "deserialize_choices")]
        pub(crate) choices: Vec<Choice>,
        pub(crate) usage: Option<openai_chat::UsageResponse>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct Choice {
        pub(crate) message: Option<AssistantMessageResponse>,
        pub(crate) finish_reason: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct AssistantMessageResponse {
        pub(crate) role: Option<String>,
        pub(crate) content: Option<openai_chat::Content>,
        pub(crate) reasoning_content: Option<String>,
        pub(crate) refusal: Option<String>,
        #[serde(default, deserialize_with = "deserialize_tool_calls")]
        pub(crate) tool_calls: Vec<ToolCallResponse>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct ToolCallResponse {
        pub(crate) id: Option<String>,
        #[serde(rename = "type")]
        pub(crate) kind: Option<String>,
        pub(crate) function: Option<FunctionCallResponse>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub(crate) struct FunctionCallResponse {
        pub(crate) name: Option<String>,
        pub(crate) arguments: Option<BoundedArgumentString>,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct BoundedArgumentString(pub(crate) String);

    impl<'de> Deserialize<'de> for BoundedArgumentString {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct ArgumentStringVisitor;

            impl Visitor<'_> for ArgumentStringVisitor {
                type Value = BoundedArgumentString;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("a bounded JSON argument string")
                }

                fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    if value.len() > ToolChatLimits::ARGUMENT_BYTES {
                        return Err(E::custom("tool arguments exceed the byte limit"));
                    }
                    Ok(BoundedArgumentString(value.to_string()))
                }

                fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    if value.len() > ToolChatLimits::ARGUMENT_BYTES {
                        return Err(E::custom("tool arguments exceed the byte limit"));
                    }
                    Ok(BoundedArgumentString(value))
                }
            }

            deserializer.deserialize_string(ArgumentStringVisitor)
        }
    }

    impl TryFrom<&ToolChatRequest> for Request {
        type Error = VyaneError;

        fn try_from(req: &ToolChatRequest) -> Result<Self> {
            const RESERVED: &[&str] = &[
                "model",
                "messages",
                "tools",
                "tool_choice",
                "stream",
                "stream_options",
                "temperature",
                "top_p",
                "max_tokens",
                "max_completion_tokens",
                "reasoning_effort",
            ];
            if RESERVED
                .iter()
                .any(|field| req.params.extra.contains_key(*field))
            {
                return Err(VyaneError::new(
                    ErrorKind::Config,
                    "typed OpenAI tool-chat params.extra contains a reserved field",
                ));
            }
            let mut extra = req.params.extra.clone();
            if let Some(max) = req.params.max_output_tokens {
                extra.entry("max_tokens".to_string()).or_insert(json!(max));
            }
            if let Some(effort) = req.params.effort {
                extra
                    .entry("reasoning_effort".to_string())
                    .or_insert(json!(effort.as_str()));
            }

            let messages = req
                .messages
                .iter()
                .map(message_from)
                .collect::<Result<Vec<_>>>()?;
            let tools = req
                .tools
                .iter()
                .map(|tool| Tool {
                    kind: "function",
                    function: FunctionDefinition {
                        name: tool.name.clone(),
                        description: tool.description.clone(),
                        parameters: tool.input_schema.clone(),
                    },
                })
                .collect::<Vec<_>>();
            let tool_choice = if tools.is_empty() {
                None
            } else {
                Some(match &req.tool_choice {
                    ToolChoice::Auto => json!("auto"),
                    ToolChoice::None => json!("none"),
                    ToolChoice::Required => json!("required"),
                    ToolChoice::Named(name) => {
                        json!({"type": "function", "function": {"name": name}})
                    }
                })
            };

            Ok(Self {
                model: req.model.as_str().to_string(),
                messages,
                tools,
                tool_choice,
                temperature: req.params.temperature,
                top_p: req.params.top_p,
                extra,
            })
        }
    }

    impl TryFrom<Response> for ToolChatOutcome {
        type Error = VyaneError;

        fn try_from(response: Response) -> Result<Self> {
            if response.choices.len() > MAX_OPENAI_CHOICES {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    "OpenAI tool-chat response exceeded the choice limit",
                ));
            }
            let choice = response.choices.into_iter().next().ok_or_else(|| {
                VyaneError::new(
                    ErrorKind::Protocol,
                    "OpenAI tool-chat response had no choices",
                )
            })?;
            let message = choice.message.ok_or_else(|| {
                VyaneError::new(
                    ErrorKind::Protocol,
                    "OpenAI tool-chat response choice had no assistant message",
                )
            })?;
            if message
                .role
                .as_deref()
                .is_some_and(|role| role != "assistant")
            {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    "OpenAI tool-chat response had a non-assistant message role",
                ));
            }
            if message.tool_calls.len() > ToolChatLimits::CALLS_PER_TURN {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    "OpenAI tool-chat response exceeded the tool-call limit",
                ));
            }
            let (text, content_parts) = match message.content {
                Some(content) => assistant_content_from(content)?,
                None => (String::new(), Vec::new()),
            };
            let mut argument_budget = StrictJsonBudget::default();
            let tool_calls = message
                .tool_calls
                .into_iter()
                .map(|call| tool_call_from(call, &mut argument_budget))
                .collect::<Result<Vec<_>>>()?;
            let outcome = ToolChatOutcome {
                assistant: AssistantToolTurn {
                    text,
                    content_parts,
                    reasoning: message.reasoning_content,
                    refusal: message.refusal,
                    tool_calls,
                },
                usage: response.usage.map(openai_chat::usage_from_response),
                model_echo: response.model,
                finish_reason: choice.finish_reason,
            };
            outcome.validate().map_err(|error| {
                VyaneError::new(
                    ErrorKind::Protocol,
                    format!("invalid OpenAI tool-chat response: {error}"),
                )
            })?;
            Ok(outcome)
        }
    }

    fn message_from(message: &ToolChatMessage) -> Result<Message> {
        match message {
            ToolChatMessage::Text(message) => Ok(Message::Text(TextMessage {
                role: role_name(message.role),
                content: message.content.clone(),
            })),
            ToolChatMessage::Assistant(turn) => Ok(Message::Assistant(AssistantMessageRequest {
                role: "assistant",
                content: if !turn.content_parts.is_empty() {
                    Some(AssistantContentRequest::Parts(
                        turn.content_parts
                            .iter()
                            .map(|part| match part {
                                AssistantContentPart::Text { text } => {
                                    AssistantContentPartRequest::Text { text: text.clone() }
                                }
                                AssistantContentPart::Refusal { refusal } => {
                                    AssistantContentPartRequest::Refusal {
                                        refusal: refusal.clone(),
                                    }
                                }
                            })
                            .collect(),
                    ))
                } else if turn.text.is_empty()
                    && (!turn.tool_calls.is_empty() || turn.refusal.is_some())
                {
                    None
                } else {
                    Some(AssistantContentRequest::Text(turn.text.clone()))
                },
                reasoning_content: turn.reasoning.clone(),
                refusal: turn.refusal.clone(),
                tool_calls: turn
                    .tool_calls
                    .iter()
                    .map(tool_call_request_from)
                    .collect::<Result<Vec<_>>>()?,
            })),
            ToolChatMessage::ToolResult(result) => Ok(Message::ToolResult(ToolResultRequest {
                role: "tool",
                tool_call_id: result.tool_call_id.clone(),
                content: result.content.clone(),
            })),
        }
    }

    fn tool_call_request_from(call: &ModelToolCall) -> Result<ToolCallRequest> {
        let arguments = match &call.arguments {
            ToolCallArguments::Object(arguments) => {
                serde_json::to_string(arguments).map_err(|e| {
                    VyaneError::with_source(
                        ErrorKind::Config,
                        "could not serialize typed tool-call arguments",
                        e,
                    )
                })?
            }
            ToolCallArguments::InvalidJson { raw } => raw.clone(),
        };
        Ok(ToolCallRequest {
            id: call.id.clone(),
            kind: "function",
            function: FunctionCallRequest {
                name: call.name.clone(),
                arguments,
            },
        })
    }

    fn deserialize_choices<'de, D>(deserializer: D) -> std::result::Result<Vec<Choice>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_bounded_sequence(
            deserializer,
            MAX_OPENAI_CHOICES,
            "OpenAI tool-chat response choices",
        )
    }

    fn deserialize_tool_calls<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Vec<ToolCallResponse>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_bounded_sequence(
            deserializer,
            ToolChatLimits::CALLS_PER_TURN,
            "OpenAI assistant tool calls",
        )
    }

    fn tool_call_from(
        call: ToolCallResponse,
        budget: &mut StrictJsonBudget,
    ) -> Result<ModelToolCall> {
        if call.kind.as_deref().is_some_and(|kind| kind != "function") {
            return Err(VyaneError::new(
                ErrorKind::Protocol,
                "OpenAI tool-chat response contained an unsupported tool-call type",
            ));
        }
        let function = call.function.unwrap_or(FunctionCallResponse {
            name: None,
            arguments: None,
        });
        let arguments = match function.arguments {
            Some(BoundedArgumentString(raw)) => match parse_object_arguments(&raw, budget) {
                Ok(arguments) => ToolCallArguments::Object(arguments),
                Err(ArgumentParseError::Invalid) => ToolCallArguments::InvalidJson { raw },
                Err(ArgumentParseError::Limit) => {
                    return Err(VyaneError::new(
                        ErrorKind::Protocol,
                        "OpenAI tool-chat arguments exceeded structural limits",
                    ));
                }
            },
            None => ToolCallArguments::InvalidJson { raw: String::new() },
        };
        Ok(ModelToolCall {
            id: call.id.unwrap_or_default(),
            name: function.name.unwrap_or_default(),
            arguments,
        })
    }

    fn assistant_content_from(
        content: openai_chat::Content,
    ) -> Result<(String, Vec<AssistantContentPart>)> {
        match content {
            openai_chat::Content::Text(text) => {
                if text.len() > ToolChatLimits::CONTENT_BYTES {
                    return Err(VyaneError::new(
                        ErrorKind::Protocol,
                        "OpenAI assistant text exceeded the content limit",
                    ));
                }
                Ok((text, Vec::new()))
            }
            openai_chat::Content::Parts(parts) => {
                if parts.0.len() > ToolChatLimits::CONTENT_PARTS {
                    return Err(VyaneError::new(
                        ErrorKind::Protocol,
                        "OpenAI assistant content exceeded the part limit",
                    ));
                }
                let mut text = String::new();
                let mut content_parts = Vec::with_capacity(parts.0.len());
                for part in parts.0 {
                    let kind = part.kind.as_deref().unwrap_or_else(|| {
                        if part.refusal.is_some() {
                            "refusal"
                        } else {
                            "text"
                        }
                    });
                    match kind {
                        "text" if part.refusal.is_none() => {
                            let value = part.text.unwrap_or_default();
                            if value.len()
                                > ToolChatLimits::CONTENT_BYTES.saturating_sub(text.len())
                            {
                                return Err(VyaneError::new(
                                    ErrorKind::Protocol,
                                    "OpenAI assistant text exceeded the content limit",
                                ));
                            }
                            text.push_str(&value);
                            content_parts.push(AssistantContentPart::Text { text: value });
                        }
                        "refusal" if part.text.is_none() => {
                            let refusal = part.refusal.ok_or_else(|| {
                                VyaneError::new(
                                    ErrorKind::Protocol,
                                    "OpenAI refusal content part had no refusal text",
                                )
                            })?;
                            if refusal.len() > ToolChatLimits::CONTENT_BYTES {
                                return Err(VyaneError::new(
                                    ErrorKind::Protocol,
                                    "OpenAI refusal content exceeded the content limit",
                                ));
                            }
                            content_parts.push(AssistantContentPart::Refusal { refusal });
                        }
                        _ => {
                            return Err(VyaneError::new(
                                ErrorKind::Protocol,
                                "OpenAI assistant content contained an unsupported part",
                            ));
                        }
                    }
                }
                Ok((text, content_parts))
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ArgumentParseError {
        Invalid,
        Limit,
    }

    /// Shared resource budget across every arguments object in one response.
    #[derive(Default)]
    struct StrictJsonBudget {
        nodes: usize,
        limit_hit: bool,
    }

    /// Parse model-supplied arguments without accepting duplicate keys and
    /// without constructing nodes beyond the response-wide resource budget.
    fn parse_object_arguments(
        raw: &str,
        budget: &mut StrictJsonBudget,
    ) -> std::result::Result<BTreeMap<String, Value>, ArgumentParseError> {
        if raw.len() > ToolChatLimits::ARGUMENT_BYTES {
            return Err(ArgumentParseError::Limit);
        }
        let mut deserializer = serde_json::Deserializer::from_str(raw);
        let parsed = StrictJsonSeed { budget, depth: 0 }.deserialize(&mut deserializer);
        let value = match parsed {
            Ok(StrictJson(value)) => value,
            Err(_) if budget.limit_hit => return Err(ArgumentParseError::Limit),
            Err(_) => return Err(ArgumentParseError::Invalid),
        };
        if deserializer.end().is_err() {
            return Err(ArgumentParseError::Invalid);
        }
        match value {
            Value::Object(values) => Ok(values.into_iter().collect()),
            _ => Err(ArgumentParseError::Invalid),
        }
    }

    struct StrictJson(Value);

    struct StrictJsonSeed<'a> {
        budget: &'a mut StrictJsonBudget,
        depth: usize,
    }

    impl<'de> DeserializeSeed<'de> for StrictJsonSeed<'_> {
        type Value = StrictJson;

        fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            if self.depth > ToolChatLimits::JSON_DEPTH
                || self.budget.nodes >= ToolChatLimits::JSON_NODES
            {
                self.budget.limit_hit = true;
                return Err(de::Error::custom("tool arguments exceed structural limits"));
            }
            self.budget.nodes = self.budget.nodes.saturating_add(1);
            deserializer.deserialize_any(StrictJsonVisitor {
                budget: self.budget,
                depth: self.depth,
            })
        }
    }

    struct StrictJsonVisitor<'a> {
        budget: &'a mut StrictJsonBudget,
        depth: usize,
    }

    impl<'de> Visitor<'de> for StrictJsonVisitor<'_> {
        type Value = StrictJson;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON value without duplicate object keys")
        }

        fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
            Ok(StrictJson(Value::Bool(value)))
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
            Ok(StrictJson(Value::Number(value.into())))
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
            Ok(StrictJson(Value::Number(value.into())))
        }

        fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            serde_json::Number::from_f64(value)
                .map(Value::Number)
                .map(StrictJson)
                .ok_or_else(|| E::custom("non-finite JSON number"))
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_string(value.to_string())
        }

        fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
            Ok(StrictJson(Value::String(value)))
        }

        fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
            Ok(StrictJson(Value::Null))
        }

        fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
            Ok(StrictJson(Value::Null))
        }

        fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            StrictJsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
            }
            .deserialize(deserializer)
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let remaining = ToolChatLimits::JSON_NODES.saturating_sub(self.budget.nodes);
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(remaining));
            while let Some(StrictJson(value)) = sequence.next_element_seed(StrictJsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
            })? {
                values.push(value);
            }
            Ok(StrictJson(Value::Array(values)))
        }

        fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut values = Map::new();
            while let Some(key) = map.next_key::<String>()? {
                if values.contains_key(&key) {
                    return Err(de::Error::custom("duplicate JSON object key"));
                }
                let StrictJson(value) = map.next_value_seed(StrictJsonSeed {
                    budget: self.budget,
                    depth: self.depth.saturating_add(1),
                })?;
                values.insert(key, value);
            }
            Ok(StrictJson(Value::Object(values)))
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
