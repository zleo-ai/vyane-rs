//! Protocol-neutral, typed tool-calling conversation envelopes.
//!
//! These types intentionally live beside, rather than inside, the existing
//! text-only chat vocabulary. A tool call returned by a model is untrusted
//! protocol data: malformed JSON arguments remain represented as
//! [`ToolCallArguments::InvalidJson`] and can never be mistaken for executable
//! arguments by downstream code.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{ChatMessage, ChatOutcome, ChatRequest, GenParams, ModelId, Role, Usage};

/// Hard limits for one typed tool-chat request or response.
pub struct ToolChatLimits;

impl ToolChatLimits {
    pub const MESSAGES: usize = 512;
    pub const TOOLS: usize = 128;
    pub const CALLS_PER_TURN: usize = 64;
    pub const CALL_ID_BYTES: usize = 256;
    pub const TOOL_NAME_BYTES: usize = 128;
    pub const DESCRIPTION_BYTES: usize = 16 * 1024;
    pub const CONTENT_BYTES: usize = 1024 * 1024;
    pub const CONTENT_PARTS: usize = 1024;
    pub const ARGUMENT_BYTES: usize = 256 * 1024;
    pub const ARGUMENT_COUNT: usize = 64;
    pub const ARGUMENT_NAME_BYTES: usize = 256;
    pub const SCHEMA_BYTES: usize = 256 * 1024;
    pub const JSON_DEPTH: usize = 16;
    pub const JSON_NODES: usize = 262_144;
    pub const ENVELOPE_BYTES: usize = 8 * 1024 * 1024;
    pub const MODEL_BYTES: usize = 512;
}

/// A provider-neutral function definition advertised to a model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// Tool-call arguments as received from the provider.
///
/// Only [`Object`](Self::Object) can later be normalized into an executable
/// native tool call. Invalid or non-object JSON is kept verbatim so a caller
/// can return a deterministic error tool-result without silently repairing the
/// model output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallArguments {
    Object(BTreeMap<String, Value>),
    InvalidJson { raw: String },
}

/// One untrusted tool call produced by a model turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub arguments: ToolCallArguments,
}

/// One ordered assistant content block returned by a provider.
///
/// Keeping refusal blocks distinct prevents a refusal-only response from being
/// collapsed into an empty successful text response and allows exact replay of
/// OpenAI-compatible content arrays.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantContentPart {
    Text { text: String },
    Refusal { refusal: String },
}

/// One assistant turn, optionally requesting one or more tools.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AssistantToolTurn {
    #[serde(default)]
    pub text: String,
    /// Exact ordered content blocks when the provider returned an array.
    /// `text` must equal the concatenation of all text blocks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_parts: Vec<AssistantContentPart>,
    /// Optional OpenAI-compatible reasoning content that must be replayed
    /// losslessly when present. It is never flattened into legacy text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// OpenAI-compatible top-level assistant refusal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    #[serde(default)]
    pub tool_calls: Vec<ModelToolCall>,
}

/// One result paired to an exact assistant tool-call id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultMessage {
    pub tool_call_id: String,
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
}

/// A typed conversation item. Tool results are not flattened into user text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChatMessage {
    Text(ChatMessage),
    Assistant(AssistantToolTurn),
    ToolResult(ToolResultMessage),
}

impl ToolChatMessage {
    pub fn text(message: ChatMessage) -> Self {
        Self::Text(message)
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self::Text(ChatMessage::system(content))
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::Text(ChatMessage::user(content))
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::Text(ChatMessage::assistant(content))
    }
}

impl AssistantToolTurn {
    /// Whether this turn carries any structured refusal signal.
    pub fn has_refusal(&self) -> bool {
        self.refusal.is_some()
            || self
                .content_parts
                .iter()
                .any(|part| matches!(part, AssistantContentPart::Refusal { .. }))
    }
}

/// Provider-neutral tool selection.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
    Named(String),
}

/// One non-streaming typed tool-chat request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolChatRequest {
    pub model: ModelId,
    pub messages: Vec<ToolChatMessage>,
    #[serde(default)]
    pub tools: Vec<ToolDefinition>,
    #[serde(default)]
    pub tool_choice: ToolChoice,
    pub params: GenParams,
}

/// The normalized product of one non-streaming model turn.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolChatOutcome {
    pub assistant: AssistantToolTurn,
    pub usage: Option<Usage>,
    pub model_echo: Option<String>,
    pub finish_reason: Option<String>,
}

impl From<ChatOutcome> for ToolChatOutcome {
    fn from(outcome: ChatOutcome) -> Self {
        Self {
            assistant: AssistantToolTurn {
                text: outcome.text,
                content_parts: Vec::new(),
                reasoning: None,
                refusal: None,
                tool_calls: Vec::new(),
            },
            usage: outcome.usage,
            model_echo: outcome.model_echo,
            finish_reason: outcome.finish_reason,
        }
    }
}

/// Deterministic validation failures for typed conversations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ToolChatValidationError {
    #[error("tool-chat conversation must contain at least one message")]
    EmptyConversation,
    #[error("tool-chat conversation exceeds {} messages", ToolChatLimits::MESSAGES)]
    TooManyMessages,
    #[error("tool-chat request exceeds {} tool definitions", ToolChatLimits::TOOLS)]
    TooManyTools,
    #[error("assistant turn exceeds {} tool calls", ToolChatLimits::CALLS_PER_TURN)]
    TooManyToolCalls,
    #[error(
        "assistant turn exceeds {} content parts",
        ToolChatLimits::CONTENT_PARTS
    )]
    TooManyContentParts,
    #[error("assistant text does not match its ordered text content parts")]
    ContentTextMismatch,
    #[error("{field} is empty, oversized, or contains unsafe characters")]
    InvalidIdentifier { field: &'static str },
    #[error("{field} exceeds its byte limit")]
    TextTooLarge { field: &'static str },
    #[error("tool call id `{0}` is duplicated")]
    DuplicateToolCall(String),
    #[error("tool result for call id `{0}` is duplicated")]
    DuplicateToolResult(String),
    #[error("tool result references unknown or inactive call id `{0}`")]
    OrphanToolResult(String),
    #[error("conversation contains a non-result message before pending tool calls were settled")]
    MessageWhileToolsPending,
    #[error("conversation ends with {0} unresolved tool call(s)")]
    UnresolvedToolCalls(usize),
    #[error("tool definition name `{0}` is duplicated")]
    DuplicateToolDefinition(String),
    #[error("tool input schema must be a JSON object")]
    SchemaNotObject,
    #[error("tool choice requires at least one tool definition")]
    ToolChoiceWithoutTools,
    #[error("named tool choice `{0}` is not present in the tool definitions")]
    UnknownNamedTool(String),
    #[error("JSON value exceeds the maximum depth")]
    JsonTooDeep,
    #[error("JSON value exceeds the maximum node count")]
    TooManyJsonNodes,
    #[error("JSON object contains an oversized key")]
    JsonKeyTooLarge,
    #[error("{field} exceeds its serialized byte limit")]
    JsonTooLarge { field: &'static str },
    #[error("{field} could not be serialized")]
    NotSerializable { field: &'static str },
    #[error("tool-chat envelope exceeds {} bytes", ToolChatLimits::ENVELOPE_BYTES)]
    EnvelopeTooLarge,
}

impl ToolChatRequest {
    /// Validate limits and exact tool-call/result pairing at a model-request
    /// boundary. An unresolved assistant call is never a valid next request.
    pub fn validate(&self) -> Result<(), ToolChatValidationError> {
        let mut json_budget = JsonBudget::default();
        validate_model(&self.model)?;
        validate_params(&self.params, &mut json_budget)?;
        validate_conversation_with_budget(&self.messages, &mut json_budget)?;
        validate_tools(&self.tools, &self.tool_choice, &mut json_budget)?;
        bounded_serialized_size(self, ToolChatLimits::ENVELOPE_BYTES, "tool-chat request")
            .map(|_| ())
    }

    /// Convert a validated request to the legacy text request when that
    /// conversion is lossless. `Ok(None)` means typed tool semantics are
    /// present and a text-only client must return `Unsupported`.
    pub fn try_into_text_request(self) -> Result<Option<ChatRequest>, ToolChatValidationError> {
        self.validate()?;
        if !self.tools.is_empty() {
            return Ok(None);
        }

        let mut messages = Vec::with_capacity(self.messages.len());
        for message in self.messages {
            match message {
                ToolChatMessage::Text(message) => messages.push(message),
                ToolChatMessage::Assistant(turn)
                    if turn.tool_calls.is_empty()
                        && turn.content_parts.is_empty()
                        && turn.reasoning.is_none()
                        && turn.refusal.is_none() =>
                {
                    messages.push(ChatMessage {
                        role: Role::Assistant,
                        content: turn.text,
                    });
                }
                ToolChatMessage::Assistant(_) | ToolChatMessage::ToolResult(_) => return Ok(None),
            }
        }
        Ok(Some(ChatRequest {
            model: self.model,
            messages,
            params: self.params,
        }))
    }
}

impl ToolChatOutcome {
    /// Validate a provider response before any tool execution decision sees it.
    pub fn validate(&self) -> Result<(), ToolChatValidationError> {
        let mut json_budget = JsonBudget::default();
        validate_assistant_turn(&self.assistant, &mut json_budget)?;
        bounded_serialized_size(self, ToolChatLimits::ENVELOPE_BYTES, "tool-chat outcome")
            .map(|_| ())
    }
}

/// Validate one complete conversation boundary.
pub fn validate_conversation(messages: &[ToolChatMessage]) -> Result<(), ToolChatValidationError> {
    validate_conversation_with_budget(messages, &mut JsonBudget::default())
}

fn validate_conversation_with_budget(
    messages: &[ToolChatMessage],
    json_budget: &mut JsonBudget,
) -> Result<(), ToolChatValidationError> {
    if messages.is_empty() {
        return Err(ToolChatValidationError::EmptyConversation);
    }
    if messages.len() > ToolChatLimits::MESSAGES {
        return Err(ToolChatValidationError::TooManyMessages);
    }

    let mut all_calls = BTreeSet::new();
    let mut pending = BTreeSet::new();
    let mut results = BTreeSet::new();

    for message in messages {
        match message {
            ToolChatMessage::ToolResult(result) => {
                validate_call_id(&result.tool_call_id)?;
                validate_text(
                    "tool result",
                    &result.content,
                    ToolChatLimits::CONTENT_BYTES,
                )?;
                if results.contains(&result.tool_call_id) {
                    return Err(ToolChatValidationError::DuplicateToolResult(
                        result.tool_call_id.clone(),
                    ));
                }
                if !pending.remove(&result.tool_call_id) {
                    return Err(ToolChatValidationError::OrphanToolResult(
                        result.tool_call_id.clone(),
                    ));
                }
                results.insert(result.tool_call_id.clone());
            }
            ToolChatMessage::Text(message) => {
                if !pending.is_empty() {
                    return Err(ToolChatValidationError::MessageWhileToolsPending);
                }
                validate_text(
                    "message content",
                    &message.content,
                    ToolChatLimits::CONTENT_BYTES,
                )?;
            }
            ToolChatMessage::Assistant(turn) => {
                if !pending.is_empty() {
                    return Err(ToolChatValidationError::MessageWhileToolsPending);
                }
                validate_assistant_turn(turn, json_budget)?;
                for call in &turn.tool_calls {
                    if !all_calls.insert(call.id.clone()) {
                        return Err(ToolChatValidationError::DuplicateToolCall(call.id.clone()));
                    }
                    pending.insert(call.id.clone());
                }
            }
        }
    }

    if pending.is_empty() {
        Ok(())
    } else {
        Err(ToolChatValidationError::UnresolvedToolCalls(pending.len()))
    }
}

fn validate_assistant_turn(
    turn: &AssistantToolTurn,
    json_budget: &mut JsonBudget,
) -> Result<(), ToolChatValidationError> {
    validate_text("assistant text", &turn.text, ToolChatLimits::CONTENT_BYTES)?;
    if turn.content_parts.len() > ToolChatLimits::CONTENT_PARTS {
        return Err(ToolChatValidationError::TooManyContentParts);
    }
    let mut text_offset = 0usize;
    for part in &turn.content_parts {
        match part {
            AssistantContentPart::Text { text } => {
                validate_text("assistant text part", text, ToolChatLimits::CONTENT_BYTES)?;
                if !turn.text[text_offset..].starts_with(text) {
                    return Err(ToolChatValidationError::ContentTextMismatch);
                }
                text_offset = text_offset.saturating_add(text.len());
            }
            AssistantContentPart::Refusal { refusal } => {
                validate_text(
                    "assistant refusal part",
                    refusal,
                    ToolChatLimits::CONTENT_BYTES,
                )?;
            }
        }
    }
    if !turn.content_parts.is_empty() && text_offset != turn.text.len() {
        return Err(ToolChatValidationError::ContentTextMismatch);
    }
    if let Some(reasoning) = turn.reasoning.as_deref() {
        validate_text(
            "assistant reasoning",
            reasoning,
            ToolChatLimits::CONTENT_BYTES,
        )?;
    }
    if let Some(refusal) = turn.refusal.as_deref() {
        validate_text("assistant refusal", refusal, ToolChatLimits::CONTENT_BYTES)?;
    }
    if turn.tool_calls.len() > ToolChatLimits::CALLS_PER_TURN {
        return Err(ToolChatValidationError::TooManyToolCalls);
    }
    let mut ids = BTreeSet::new();
    for call in &turn.tool_calls {
        validate_call_id(&call.id)?;
        validate_tool_name(&call.name)?;
        if !ids.insert(call.id.clone()) {
            return Err(ToolChatValidationError::DuplicateToolCall(call.id.clone()));
        }
        validate_arguments(&call.arguments, json_budget)?;
    }
    Ok(())
}

fn validate_tools(
    tools: &[ToolDefinition],
    choice: &ToolChoice,
    json_budget: &mut JsonBudget,
) -> Result<(), ToolChatValidationError> {
    if tools.len() > ToolChatLimits::TOOLS {
        return Err(ToolChatValidationError::TooManyTools);
    }
    let mut names = BTreeSet::new();
    for tool in tools {
        validate_tool_name(&tool.name)?;
        validate_text(
            "tool description",
            &tool.description,
            ToolChatLimits::DESCRIPTION_BYTES,
        )?;
        if !names.insert(tool.name.clone()) {
            return Err(ToolChatValidationError::DuplicateToolDefinition(
                tool.name.clone(),
            ));
        }
        if !tool.input_schema.is_object() {
            return Err(ToolChatValidationError::SchemaNotObject);
        }
        validate_json_shape(&tool.input_schema, json_budget)?;
        bounded_serialized_size(
            &tool.input_schema,
            ToolChatLimits::SCHEMA_BYTES,
            "tool input schema",
        )?;
    }

    match choice {
        ToolChoice::Required if tools.is_empty() => {
            Err(ToolChatValidationError::ToolChoiceWithoutTools)
        }
        ToolChoice::Named(name) => {
            validate_tool_name(name)?;
            if names.contains(name) {
                Ok(())
            } else if tools.is_empty() {
                Err(ToolChatValidationError::ToolChoiceWithoutTools)
            } else {
                Err(ToolChatValidationError::UnknownNamedTool(name.clone()))
            }
        }
        ToolChoice::Auto | ToolChoice::None | ToolChoice::Required => Ok(()),
    }
}

fn validate_arguments(
    arguments: &ToolCallArguments,
    json_budget: &mut JsonBudget,
) -> Result<(), ToolChatValidationError> {
    match arguments {
        ToolCallArguments::InvalidJson { raw } => validate_text(
            "invalid tool arguments",
            raw,
            ToolChatLimits::ARGUMENT_BYTES,
        ),
        ToolCallArguments::Object(arguments) => {
            if arguments.len() > ToolChatLimits::ARGUMENT_COUNT {
                return Err(ToolChatValidationError::TooManyJsonNodes);
            }
            if arguments
                .keys()
                .any(|key| key.len() > ToolChatLimits::ARGUMENT_NAME_BYTES)
            {
                return Err(ToolChatValidationError::JsonKeyTooLarge);
            }
            for value in arguments.values() {
                validate_json_shape(value, json_budget)?;
            }
            bounded_serialized_size(arguments, ToolChatLimits::ARGUMENT_BYTES, "tool arguments")?;
            Ok(())
        }
    }
}

fn validate_params(
    params: &GenParams,
    json_budget: &mut JsonBudget,
) -> Result<(), ToolChatValidationError> {
    if params
        .extra
        .keys()
        .any(|key| key.len() > ToolChatLimits::ARGUMENT_NAME_BYTES)
    {
        return Err(ToolChatValidationError::JsonKeyTooLarge);
    }
    for value in params.extra.values() {
        validate_json_shape(value, json_budget)?;
    }
    Ok(())
}

#[derive(Default)]
struct JsonBudget {
    nodes: usize,
}

fn validate_json_shape(
    root: &Value,
    budget: &mut JsonBudget,
) -> Result<(), ToolChatValidationError> {
    let mut stack = vec![(root, 0usize)];
    while let Some((value, depth)) = stack.pop() {
        budget.nodes = budget.nodes.saturating_add(1);
        if budget.nodes > ToolChatLimits::JSON_NODES {
            return Err(ToolChatValidationError::TooManyJsonNodes);
        }
        match value {
            Value::Array(values) => {
                let next = depth.saturating_add(1);
                if next > ToolChatLimits::JSON_DEPTH {
                    return Err(ToolChatValidationError::JsonTooDeep);
                }
                stack.extend(values.iter().map(|value| (value, next)));
            }
            Value::Object(values) => {
                let next = depth.saturating_add(1);
                if next > ToolChatLimits::JSON_DEPTH {
                    return Err(ToolChatValidationError::JsonTooDeep);
                }
                if values
                    .keys()
                    .any(|key| key.len() > ToolChatLimits::ARGUMENT_NAME_BYTES)
                {
                    return Err(ToolChatValidationError::JsonKeyTooLarge);
                }
                stack.extend(values.values().map(|value| (value, next)));
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn validate_model(model: &ModelId) -> Result<(), ToolChatValidationError> {
    validate_identifier("model", model.as_str(), ToolChatLimits::MODEL_BYTES, |_| {
        true
    })
}

fn validate_call_id(id: &str) -> Result<(), ToolChatValidationError> {
    validate_identifier("tool call id", id, ToolChatLimits::CALL_ID_BYTES, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'?')
    })
}

fn validate_tool_name(name: &str) -> Result<(), ToolChatValidationError> {
    validate_identifier("tool name", name, ToolChatLimits::TOOL_NAME_BYTES, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')
    })
}

fn validate_identifier(
    field: &'static str,
    value: &str,
    max: usize,
    allowed: impl Fn(u8) -> bool,
) -> Result<(), ToolChatValidationError> {
    if value.is_empty() || value.len() > max || value.contains('\0') || !value.bytes().all(allowed)
    {
        return Err(ToolChatValidationError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ToolChatValidationError> {
    if value.len() > max {
        Err(ToolChatValidationError::TextTooLarge { field })
    } else {
        Ok(())
    }
}

fn bounded_serialized_size(
    value: &impl Serialize,
    limit: usize,
    field: &'static str,
) -> Result<usize, ToolChatValidationError> {
    let mut writer = BoundedWriter::new(limit);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.written),
        Err(_) if writer.exceeded => {
            if limit == ToolChatLimits::ENVELOPE_BYTES {
                Err(ToolChatValidationError::EnvelopeTooLarge)
            } else {
                Err(ToolChatValidationError::JsonTooLarge { field })
            }
        }
        Err(_) => Err(ToolChatValidationError::NotSerializable { field }),
    }
}

struct BoundedWriter {
    written: usize,
    limit: usize,
    exceeded: bool,
}

impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            written: 0,
            limit,
            exceeded: false,
        }
    }
}

impl io::Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.written) {
            self.exceeded = true;
            return Err(io::Error::other("typed tool-chat envelope exceeds limit"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::{ChatClient, ErrorKind, Protocol, Result};

    fn call(id: &str) -> ModelToolCall {
        ModelToolCall {
            id: id.into(),
            name: "read_file".into(),
            arguments: ToolCallArguments::Object(BTreeMap::from([(
                "path".into(),
                Value::String("README.md".into()),
            )])),
        }
    }

    fn request(messages: Vec<ToolChatMessage>) -> ToolChatRequest {
        ToolChatRequest {
            model: ModelId::new("model"),
            messages,
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            params: GenParams::default(),
        }
    }

    #[test]
    fn complete_tool_turn_is_a_valid_boundary() {
        let request = request(vec![
            ToolChatMessage::user("inspect"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: String::new(),
                content_parts: Vec::new(),
                reasoning: None,
                refusal: None,
                tool_calls: vec![call("call-1"), call("call-2")],
            }),
            ToolChatMessage::ToolResult(ToolResultMessage {
                tool_call_id: "call-1".into(),
                content: "one".into(),
                is_error: false,
            }),
            ToolChatMessage::ToolResult(ToolResultMessage {
                tool_call_id: "call-2".into(),
                content: "two".into(),
                is_error: false,
            }),
        ]);
        request.validate().unwrap();
    }

    #[test]
    fn unresolved_or_orphaned_results_are_rejected() {
        let unresolved = request(vec![
            ToolChatMessage::user("inspect"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: String::new(),
                content_parts: Vec::new(),
                reasoning: None,
                refusal: None,
                tool_calls: vec![call("call-1")],
            }),
        ]);
        assert_eq!(
            unresolved.validate().unwrap_err(),
            ToolChatValidationError::UnresolvedToolCalls(1)
        );

        let orphan = request(vec![
            ToolChatMessage::user("inspect"),
            ToolChatMessage::ToolResult(ToolResultMessage {
                tool_call_id: "call-1".into(),
                content: "orphan".into(),
                is_error: true,
            }),
        ]);
        assert!(matches!(
            orphan.validate(),
            Err(ToolChatValidationError::OrphanToolResult(id)) if id == "call-1"
        ));
    }

    #[test]
    fn duplicate_call_and_result_ids_are_rejected() {
        let duplicate_call = request(vec![
            ToolChatMessage::user("inspect"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: String::new(),
                content_parts: Vec::new(),
                reasoning: None,
                refusal: None,
                tool_calls: vec![call("same"), call("same")],
            }),
        ]);
        assert!(matches!(
            duplicate_call.validate(),
            Err(ToolChatValidationError::DuplicateToolCall(id)) if id == "same"
        ));

        let duplicate_result = request(vec![
            ToolChatMessage::user("inspect"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: String::new(),
                content_parts: Vec::new(),
                reasoning: None,
                refusal: None,
                tool_calls: vec![call("same")],
            }),
            ToolChatMessage::ToolResult(ToolResultMessage {
                tool_call_id: "same".into(),
                content: "first".into(),
                is_error: false,
            }),
            ToolChatMessage::ToolResult(ToolResultMessage {
                tool_call_id: "same".into(),
                content: "second".into(),
                is_error: false,
            }),
        ]);
        assert!(matches!(
            duplicate_result.validate(),
            Err(ToolChatValidationError::DuplicateToolResult(id)) if id == "same"
        ));
    }

    #[test]
    fn missing_or_oversized_call_ids_are_rejected() {
        for id in [String::new(), "x".repeat(ToolChatLimits::CALL_ID_BYTES + 1)] {
            let outcome = ToolChatOutcome {
                assistant: AssistantToolTurn {
                    text: String::new(),
                    content_parts: Vec::new(),
                    reasoning: None,
                    refusal: None,
                    tool_calls: vec![call(&id)],
                },
                ..ToolChatOutcome::default()
            };
            assert!(matches!(
                outcome.validate(),
                Err(ToolChatValidationError::InvalidIdentifier {
                    field: "tool call id"
                })
            ));
        }
    }

    #[test]
    fn only_lossless_text_requests_downgrade() {
        let text = request(vec![
            ToolChatMessage::system("system"),
            ToolChatMessage::user("hello"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: "answer".into(),
                content_parts: Vec::new(),
                reasoning: None,
                refusal: None,
                tool_calls: Vec::new(),
            }),
        ]);
        let legacy = text.try_into_text_request().unwrap().unwrap();
        assert_eq!(legacy.messages.len(), 3);
        assert_eq!(legacy.messages[2], ChatMessage::assistant("answer"));

        let mut typed = request(vec![ToolChatMessage::user("hello")]);
        typed.tools.push(ToolDefinition {
            name: "read_file".into(),
            description: "read".into(),
            input_schema: serde_json::json!({"type": "object"}),
        });
        assert!(typed.try_into_text_request().unwrap().is_none());
    }

    #[test]
    fn invalid_json_arguments_remain_non_executable_data() {
        let arguments = ToolCallArguments::InvalidJson {
            raw: "{not-json".into(),
        };
        assert!(matches!(arguments, ToolCallArguments::InvalidJson { .. }));
        validate_arguments(&arguments, &mut JsonBudget::default()).unwrap();
    }

    struct TextOnlyClient {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ChatClient for TextOnlyClient {
        fn protocol(&self) -> Protocol {
            Protocol::OpenaiChat
        }

        async fn complete(&self, _req: ChatRequest) -> Result<ChatOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ChatOutcome {
                text: "legacy answer".into(),
                ..ChatOutcome::default()
            })
        }
    }

    #[test]
    fn text_only_client_default_delegates_only_lossless_requests() {
        let client = TextOnlyClient {
            calls: AtomicUsize::new(0),
        };
        let outcome = futures::executor::block_on(
            client.complete_turn(request(vec![ToolChatMessage::user("hello")])),
        )
        .unwrap();
        assert_eq!(outcome.assistant.text, "legacy answer");
        assert_eq!(client.calls.load(Ordering::SeqCst), 1);

        let mut typed = request(vec![ToolChatMessage::user("hello")]);
        typed.tools.push(ToolDefinition {
            name: "read_file".into(),
            description: "read".into(),
            input_schema: serde_json::json!({"type": "object"}),
        });
        let error = futures::executor::block_on(client.complete_turn(typed)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(client.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reasoning_cannot_be_silently_flattened_into_legacy_text() {
        let client = TextOnlyClient {
            calls: AtomicUsize::new(0),
        };
        let req = request(vec![
            ToolChatMessage::user("hello"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: "answer".into(),
                content_parts: Vec::new(),
                reasoning: Some("private chain state".into()),
                refusal: None,
                tool_calls: Vec::new(),
            }),
        ]);
        let error = futures::executor::block_on(client.complete_turn(req)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(client.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn refusal_cannot_be_silently_flattened_into_legacy_text() {
        let client = TextOnlyClient {
            calls: AtomicUsize::new(0),
        };
        let req = request(vec![
            ToolChatMessage::user("hello"),
            ToolChatMessage::Assistant(AssistantToolTurn {
                text: String::new(),
                content_parts: vec![AssistantContentPart::Refusal {
                    refusal: "cannot comply".into(),
                }],
                reasoning: None,
                refusal: Some("policy refusal".into()),
                tool_calls: Vec::new(),
            }),
        ]);
        let error = futures::executor::block_on(client.complete_turn(req)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(client.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn assistant_text_must_match_ordered_text_parts() {
        let outcome = ToolChatOutcome {
            assistant: AssistantToolTurn {
                text: "first second".into(),
                content_parts: vec![
                    AssistantContentPart::Text {
                        text: "first ".into(),
                    },
                    AssistantContentPart::Refusal {
                        refusal: "policy".into(),
                    },
                    AssistantContentPart::Text {
                        text: "different".into(),
                    },
                ],
                reasoning: None,
                refusal: None,
                tool_calls: Vec::new(),
            },
            ..ToolChatOutcome::default()
        };

        assert_eq!(
            outcome.validate().unwrap_err(),
            ToolChatValidationError::ContentTextMismatch
        );
    }

    #[test]
    fn json_node_budget_is_shared_across_all_tool_schemas() {
        let mut req = request(vec![ToolChatMessage::user("hello")]);
        req.tools = (0..6)
            .map(|index| ToolDefinition {
                name: format!("tool_{index}"),
                description: "bounded".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "enum": vec![Value::Null; 50_000]
                }),
            })
            .collect();

        assert_eq!(
            req.validate().unwrap_err(),
            ToolChatValidationError::TooManyJsonNodes
        );
    }
}
