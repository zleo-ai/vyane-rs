//! A bounded, provider-neutral model/tool turn driver.
//!
//! This is intentionally a dark execution seam rather than a
//! [`vyane_core::Harness`] implementation. It does not assemble sessions,
//! checkpoints, built-in tools, a host sandbox, or approval replay. Every
//! model send and every allowed registry dispatch is routed through the
//! narrower authorized capabilities supplied by the caller.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;
use vyane_core::{
    AssistantContentPart, AuthorizedToolChatClient, ErrorKind, NativeExecutionAuthority,
    ToolCallArguments, ToolChatMessage, ToolChatRequest, ToolChoice, ToolResultMessage, Usage,
    VyaneError,
};

use super::{
    ApprovalPlan, MAX_TOOL_OUTPUT_CHARS, PermissionEffect, PermissionPolicy, ToolCall, ToolContext,
    ToolInvocationStatus, ToolRegistry,
};

/// Default number of logical model turns in one native run.
pub const DEFAULT_NATIVE_MODEL_TURNS: u32 = 8;

/// Absolute logical-turn ceiling for the first bounded native driver.
pub const MAX_NATIVE_MODEL_TURNS: u32 = 32;

const INVALID_JSON_TOOL_RESULT: &str = "ERROR: tool arguments were not valid JSON";
const SAFE_RESULT_HEADROOM_CHARS: usize = 512;

/// Validated native-loop limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeTurnLimits {
    max_model_turns: u32,
}

impl NativeTurnLimits {
    pub fn new(max_model_turns: u32) -> Result<Self, NativeTurnLimitError> {
        if max_model_turns == 0 {
            return Err(NativeTurnLimitError::Zero);
        }
        if max_model_turns > MAX_NATIVE_MODEL_TURNS {
            return Err(NativeTurnLimitError::AboveHardMaximum);
        }
        Ok(Self { max_model_turns })
    }

    pub fn max_model_turns(self) -> u32 {
        self.max_model_turns
    }
}

impl Default for NativeTurnLimits {
    fn default() -> Self {
        Self {
            max_model_turns: DEFAULT_NATIVE_MODEL_TURNS,
        }
    }
}

/// Invalid bounds for a native model loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NativeTurnLimitError {
    #[error("native model turn limit must be at least one")]
    Zero,
    #[error("native model turn limit exceeds the hard maximum")]
    AboveHardMaximum,
}

/// Final assistant material retained by a completed or refused run.
///
/// Reasoning and tool calls are deliberately absent. Callers can retrieve the
/// user-facing answer/refusal, while `Debug` reveals only shape metadata.
#[derive(Clone, PartialEq, Eq)]
pub struct NativeAssistantReply {
    text: String,
    content_parts: Vec<AssistantContentPart>,
    refusal: Option<String>,
}

impl NativeAssistantReply {
    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn content_parts(&self) -> &[AssistantContentPart] {
        &self.content_parts
    }

    pub fn refusal(&self) -> Option<&str> {
        self.refusal.as_deref()
    }
}

impl fmt::Debug for NativeAssistantReply {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAssistantReply")
            .field("has_text", &!self.text.is_empty())
            .field("content_parts", &self.content_parts.len())
            .field("has_refusal", &self.refusal.is_some())
            .finish()
    }
}

/// Non-replayable terminal state from the bounded driver.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NativeTurnStop {
    Completed(NativeAssistantReply),
    Refused(NativeAssistantReply),
    ApprovalRequired(ApprovalPlan),
    BudgetExhausted,
    UnsupportedParallelCalls,
    ToolChoiceViolation,
    Cancelled,
    TimedOut,
    /// A model request failed after a tool might already have crossed its
    /// side-effect boundary. This typed stop is never failover-eligible.
    AbortedAfterToolActivity {
        kind: ErrorKind,
    },
}

impl NativeTurnStop {
    pub fn assistant_reply(&self) -> Option<&NativeAssistantReply> {
        match self {
            Self::Completed(reply) | Self::Refused(reply) => Some(reply),
            _ => None,
        }
    }

    pub fn approval_plan(&self) -> Option<&ApprovalPlan> {
        match self {
            Self::ApprovalRequired(plan) => Some(plan),
            _ => None,
        }
    }
}

impl fmt::Debug for NativeTurnStop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(_) => formatter.write_str("Completed(<redacted>)"),
            Self::Refused(_) => formatter.write_str("Refused(<redacted>)"),
            Self::ApprovalRequired(_) => formatter.write_str("ApprovalRequired(<redacted>)"),
            Self::BudgetExhausted => formatter.write_str("BudgetExhausted"),
            Self::UnsupportedParallelCalls => formatter.write_str("UnsupportedParallelCalls"),
            Self::ToolChoiceViolation => formatter.write_str("ToolChoiceViolation"),
            Self::Cancelled => formatter.write_str("Cancelled"),
            Self::TimedOut => formatter.write_str("TimedOut"),
            Self::AbortedAfterToolActivity { kind } => formatter
                .debug_struct("AbortedAfterToolActivity")
                .field("kind", kind)
                .finish(),
        }
    }
}

/// Bounded result metadata. No prompt, reasoning, conversation transcript, or
/// tool output is retained. An approval stop necessarily owns its exact
/// argument/workdir-bound [`ApprovalPlan`], but custom `Debug` redacts it.
#[derive(Clone, PartialEq, Eq)]
pub struct NativeTurnOutcome {
    pub stop: NativeTurnStop,
    pub usage: Option<Usage>,
    pub model_turns: u32,
    pub tool_side_effects_possible: bool,
}

impl fmt::Debug for NativeTurnOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTurnOutcome")
            .field("stop", &self.stop)
            .field("usage", &self.usage)
            .field("model_turns", &self.model_turns)
            .field(
                "tool_side_effects_possible",
                &self.tool_side_effects_possible,
            )
            .finish()
    }
}

/// Strictly serial provider-neutral model/tool loop.
pub struct NativeTurnDriver {
    client: Arc<dyn AuthorizedToolChatClient>,
    registry: ToolRegistry,
    policy: PermissionPolicy,
    limits: NativeTurnLimits,
}

impl NativeTurnDriver {
    pub fn new(
        client: Arc<dyn AuthorizedToolChatClient>,
        registry: ToolRegistry,
        policy: PermissionPolicy,
    ) -> Self {
        Self::with_limits(client, registry, policy, NativeTurnLimits::default())
    }

    pub fn with_limits(
        client: Arc<dyn AuthorizedToolChatClient>,
        registry: ToolRegistry,
        policy: PermissionPolicy,
        limits: NativeTurnLimits,
    ) -> Self {
        Self {
            client,
            registry,
            policy,
            limits,
        }
    }

    pub fn limits(&self) -> NativeTurnLimits {
        self.limits
    }

    /// Run one owned typed conversation to a terminal, non-replayable stop.
    ///
    /// Failover-eligible errors returned before any possible tool side effect
    /// retain their ordinary classification; cancellation and timeout become
    /// typed terminal stops. Once an allowed, known tool may have been polled,
    /// later model errors are collapsed into
    /// [`NativeTurnStop::AbortedAfterToolActivity`].
    pub async fn run(
        &self,
        mut request: ToolChatRequest,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
    ) -> vyane_core::Result<NativeTurnOutcome> {
        request.validate().map_err(|_| invalid_initial_request())?;
        if !advertised_tool_names_match_registry(&request, &self.registry) {
            return Err(VyaneError::new(
                ErrorKind::Config,
                "native tool definitions do not match the executable registry",
            ));
        }

        let mut state = RunState::new(initial_tool_activity_possible(&request));
        for turn in 1..=self.limits.max_model_turns() {
            if context.cancellation_token().is_cancelled() {
                return Ok(state.finish(NativeTurnStop::Cancelled));
            }
            if request.validate().is_err() {
                return state.invalid_request_boundary();
            }

            state.model_turns = turn;
            let response = self
                .client
                .complete_turn_authorized(
                    request.clone(),
                    turn,
                    authority,
                    context.cancellation_token(),
                )
                .await;
            let response = match response {
                Ok(response) => response,
                Err(error) => return state.model_error(error),
            };
            if context.cancellation_token().is_cancelled() {
                return Ok(state.finish(NativeTurnStop::Cancelled));
            }
            if response.validate().is_err() {
                return state.invalid_model_response();
            }
            add_usage_saturating(&mut state.usage, response.usage.as_ref());

            let assistant = response.assistant;
            if assistant.has_refusal() {
                return Ok(state.finish(NativeTurnStop::Refused(reply_from(assistant))));
            }
            if assistant.tool_calls.len() > 1 {
                return Ok(state.finish(NativeTurnStop::UnsupportedParallelCalls));
            }
            if !tool_choice_allows(&request.tool_choice, assistant.tool_calls.first()) {
                return Ok(state.finish(NativeTurnStop::ToolChoiceViolation));
            }
            let Some(model_call) = assistant.tool_calls.first().cloned() else {
                return Ok(state.finish(NativeTurnStop::Completed(reply_from(assistant))));
            };

            // Before permissions or a tool future can be polled, prove that a
            // worst-case bounded result for this exact assistant call can be
            // represented in the next request. This catches duplicate call
            // ids and conversation/envelope exhaustion before side effects.
            let placeholder = worst_case_tool_result(&model_call.id);
            if next_request_with_result(&request, &assistant, placeholder)
                .and_then(|next| next.validate().map(|()| next).map_err(|_| ()))
                .is_err()
            {
                return state.invalid_model_response();
            }

            let (result, stop) = match model_call.arguments.clone() {
                ToolCallArguments::InvalidJson { .. } => (
                    ToolResultMessage {
                        tool_call_id: model_call.id.clone(),
                        content: INVALID_JSON_TOOL_RESULT.to_string(),
                        is_error: true,
                    },
                    None,
                ),
                ToolCallArguments::Object(arguments) => {
                    let call = ToolCall {
                        id: model_call.id.clone(),
                        name: model_call.name.clone(),
                        arguments,
                    };
                    let known_allowed = self.registry.names().any(|name| name == call.name)
                        && self.policy.decide(&call, context).effect == PermissionEffect::Allow;
                    if known_allowed {
                        // Set this before entering the authorized registry. A
                        // dropped/panicked caller cannot later assume the tool
                        // was pure merely because no result was observed.
                        state.tool_side_effects_possible = true;
                    }
                    let invocation = match self
                        .registry
                        .execute_authorized(call, context, &self.policy, authority, turn, 1)
                        .await
                    {
                        Ok(invocation) => invocation,
                        Err(error) if state.tool_side_effects_possible => {
                            return Ok(state.finish(NativeTurnStop::AbortedAfterToolActivity {
                                kind: error.kind,
                            }));
                        }
                        Err(error) => return Err(error),
                    };
                    let stop = match invocation.status {
                        ToolInvocationStatus::ApprovalRequired => {
                            let Some(approval) = invocation.approval else {
                                return state.invalid_model_response();
                            };
                            Some(NativeTurnStop::ApprovalRequired(approval))
                        }
                        ToolInvocationStatus::Cancelled => Some(NativeTurnStop::Cancelled),
                        ToolInvocationStatus::TimedOut => Some(NativeTurnStop::TimedOut),
                        ToolInvocationStatus::Executed
                        | ToolInvocationStatus::Denied
                        | ToolInvocationStatus::InvalidCall
                        | ToolInvocationStatus::UnknownTool
                        | ToolInvocationStatus::ToolError => None,
                    };
                    let result = ToolResultMessage {
                        tool_call_id: invocation.call.id,
                        content: invocation.output,
                        is_error: invocation.status != ToolInvocationStatus::Executed,
                    };
                    (result, stop)
                }
            };

            if let Some(stop) = stop {
                return Ok(state.finish(stop));
            }
            request.messages.push(ToolChatMessage::Assistant(assistant));
            request.messages.push(ToolChatMessage::ToolResult(result));
            if turn == self.limits.max_model_turns() {
                return Ok(state.finish(NativeTurnStop::BudgetExhausted));
            }
        }

        // The validated limit is non-zero and the loop returns at its final
        // iteration. Keep the fallback fail-closed if that invariant changes.
        Ok(state.finish(NativeTurnStop::BudgetExhausted))
    }
}

impl fmt::Debug for NativeTurnDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTurnDriver")
            .field("protocol", &self.client.protocol())
            .field("registered_tools", &self.registry.names().count())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

struct RunState {
    usage: Option<Usage>,
    model_turns: u32,
    tool_side_effects_possible: bool,
}

impl RunState {
    fn new(tool_side_effects_possible: bool) -> Self {
        Self {
            usage: None,
            model_turns: 0,
            tool_side_effects_possible,
        }
    }

    fn finish(&self, stop: NativeTurnStop) -> NativeTurnOutcome {
        NativeTurnOutcome {
            stop,
            usage: self.usage,
            model_turns: self.model_turns,
            tool_side_effects_possible: self.tool_side_effects_possible,
        }
    }

    fn model_error(&self, error: VyaneError) -> vyane_core::Result<NativeTurnOutcome> {
        if self.tool_side_effects_possible {
            return Ok(self.finish(NativeTurnStop::AbortedAfterToolActivity { kind: error.kind }));
        }
        match error.kind {
            ErrorKind::Cancelled => Ok(self.finish(NativeTurnStop::Cancelled)),
            ErrorKind::Timeout => Ok(self.finish(NativeTurnStop::TimedOut)),
            _ => Err(error),
        }
    }

    fn invalid_request_boundary(&self) -> vyane_core::Result<NativeTurnOutcome> {
        if self.tool_side_effects_possible {
            Ok(self.finish(NativeTurnStop::AbortedAfterToolActivity {
                kind: ErrorKind::Protocol,
            }))
        } else {
            Err(VyaneError::new(
                ErrorKind::Protocol,
                "native conversation exceeded a validated boundary",
            ))
        }
    }

    fn invalid_model_response(&self) -> vyane_core::Result<NativeTurnOutcome> {
        if self.tool_side_effects_possible {
            Ok(self.finish(NativeTurnStop::AbortedAfterToolActivity {
                kind: ErrorKind::Protocol,
            }))
        } else {
            Err(VyaneError::new(
                ErrorKind::Protocol,
                "native model response failed validation",
            ))
        }
    }
}

fn invalid_initial_request() -> VyaneError {
    VyaneError::new(
        ErrorKind::Config,
        "native tool-chat request failed validation",
    )
}

fn initial_tool_activity_possible(request: &ToolChatRequest) -> bool {
    request.messages.iter().any(|message| match message {
        ToolChatMessage::Assistant(turn) => !turn.tool_calls.is_empty(),
        ToolChatMessage::ToolResult(_) => true,
        ToolChatMessage::Text(_) => false,
    })
}

// Definitions and schemas guide the model; they are not execution authority.
// A model can ignore them, so each NativeTool remains responsible for exact
// semantic argument validation. This boundary binds the complete advertised
// identity set to executable registry identities before the first send.
fn advertised_tool_names_match_registry(
    request: &ToolChatRequest,
    registry: &ToolRegistry,
) -> bool {
    let advertised = request
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    let executable = registry.names().collect::<BTreeSet<_>>();
    advertised == executable
}

fn tool_choice_allows(choice: &ToolChoice, call: Option<&vyane_core::ModelToolCall>) -> bool {
    match choice {
        ToolChoice::Auto => true,
        ToolChoice::None => call.is_none(),
        ToolChoice::Required => call.is_some(),
        ToolChoice::Named(name) => call.is_some_and(|call| call.name == *name),
    }
}

fn reply_from(assistant: vyane_core::AssistantToolTurn) -> NativeAssistantReply {
    NativeAssistantReply {
        text: assistant.text,
        content_parts: assistant.content_parts,
        refusal: assistant.refusal,
    }
}

fn worst_case_tool_result(tool_call_id: &str) -> ToolResultMessage {
    // NUL is escaped as six JSON bytes by serde_json, making this a
    // conservative envelope-size probe for any retained Unicode scalar.
    let content = "\0".repeat(MAX_TOOL_OUTPUT_CHARS + SAFE_RESULT_HEADROOM_CHARS);
    ToolResultMessage {
        tool_call_id: tool_call_id.to_string(),
        content,
        is_error: true,
    }
}

fn next_request_with_result(
    request: &ToolChatRequest,
    assistant: &vyane_core::AssistantToolTurn,
    result: ToolResultMessage,
) -> Result<ToolChatRequest, ()> {
    let mut next = request.clone();
    next.messages.try_reserve(2).map_err(|_| ())?;
    next.messages
        .push(ToolChatMessage::Assistant(assistant.clone()));
    next.messages.push(ToolChatMessage::ToolResult(result));
    Ok(next)
}

fn add_usage_saturating(total: &mut Option<Usage>, next: Option<&Usage>) {
    let Some(next) = next else {
        return;
    };
    let total = total.get_or_insert_with(Usage::default);
    total.input_tokens = total.input_tokens.saturating_add(next.input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(next.output_tokens);
    if let Some(reasoning) = next.reasoning_tokens {
        total.reasoning_tokens = Some(
            total
                .reasoning_tokens
                .unwrap_or_default()
                .saturating_add(reasoning),
        );
    }
    if let Some(cached) = next.cached_input_tokens {
        total.cached_input_tokens = Some(
            total
                .cached_input_tokens
                .unwrap_or_default()
                .saturating_add(cached),
        );
    }
}
