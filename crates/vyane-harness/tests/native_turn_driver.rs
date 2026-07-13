use std::collections::{BTreeMap, VecDeque};
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use static_assertions::assert_not_impl_any;
use tempfile::TempDir;
use vyane_core::{
    AssistantContentPart, AssistantToolTurn, AuthorizedToolChatClient, ErrorKind, GenParams,
    ModelId, ModelToolCall, NativeExecutionAuthority, NativeSideEffect, Protocol,
    ToolCallArguments, ToolChatMessage, ToolChatOutcome, ToolChatRequest, ToolChoice,
    ToolDefinition, Usage, VyaneError,
};
use vyane_harness::native::{
    DEFAULT_NATIVE_MODEL_TURNS, MAX_NATIVE_MODEL_TURNS, NativeTool, NativeTurnDriver,
    NativeTurnLimitError, NativeTurnLimits, NativeTurnOutcome, NativeTurnStop, PermissionEffect,
    PermissionPolicy, PermissionRule, ToolContext, ToolError, ToolRegistry,
};

assert_not_impl_any!(NativeTurnOutcome: serde::Serialize, serde::de::DeserializeOwned);

#[allow(clippy::large_enum_variant)]
enum ScriptStep {
    Outcome(ToolChatOutcome),
    Error(ErrorKind),
}

struct ScriptedClient {
    script: Mutex<VecDeque<ScriptStep>>,
    requests: Mutex<Vec<ToolChatRequest>>,
    logical_turns: Mutex<Vec<u32>>,
}

impl ScriptedClient {
    fn new(script: impl IntoIterator<Item = ScriptStep>) -> Self {
        Self {
            script: Mutex::new(script.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            logical_turns: Mutex::new(Vec::new()),
        }
    }

    fn send_count(&self) -> usize {
        self.requests.lock().expect("request lock").len()
    }
}

#[async_trait]
impl AuthorizedToolChatClient for ScriptedClient {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiChat
    }

    async fn complete_turn_authorized(
        &self,
        req: ToolChatRequest,
        turn: u32,
        authority: &dyn NativeExecutionAuthority,
        cancel: &vyane_core::CancellationToken,
    ) -> vyane_core::Result<ToolChatOutcome> {
        if cancel.is_cancelled() {
            return Err(VyaneError::cancelled());
        }
        authority
            .revalidate(NativeSideEffect::ModelSend {
                turn,
                wire_attempt: 1,
            })
            .await?;
        if cancel.is_cancelled() {
            return Err(VyaneError::cancelled());
        }
        self.logical_turns.lock().expect("turn lock").push(turn);
        self.requests.lock().expect("request lock").push(req);
        match self.script.lock().expect("script lock").pop_front() {
            Some(ScriptStep::Outcome(outcome)) => Ok(outcome),
            Some(ScriptStep::Error(kind)) => {
                Err(VyaneError::new(kind, "provider detail must stay private"))
            }
            None => Err(VyaneError::new(
                ErrorKind::Protocol,
                "test script exhausted",
            )),
        }
    }
}

#[derive(Default)]
struct RecordingAuthority {
    effects: Mutex<Vec<NativeSideEffect>>,
    reject: Mutex<Option<(NativeSideEffect, ErrorKind)>>,
}

impl RecordingAuthority {
    fn rejecting(effect: NativeSideEffect, kind: ErrorKind) -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
            reject: Mutex::new(Some((effect, kind))),
        }
    }

    fn effects(&self) -> Vec<NativeSideEffect> {
        self.effects.lock().expect("effects lock").clone()
    }
}

#[async_trait]
impl NativeExecutionAuthority for RecordingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> vyane_core::Result<()> {
        self.effects.lock().expect("effects lock").push(effect);
        if self
            .reject
            .lock()
            .expect("reject lock")
            .as_ref()
            .is_some_and(|(rejected, _)| *rejected == effect)
        {
            let kind = self
                .reject
                .lock()
                .expect("reject lock")
                .as_ref()
                .map(|(_, kind)| *kind)
                .expect("checked rejection");
            return Err(VyaneError::new(kind, "revoked private execution"));
        }
        Ok(())
    }
}

struct TestTool {
    name: &'static str,
    calls: Arc<AtomicUsize>,
    output: String,
}

#[async_trait]
impl NativeTool for TestTool {
    fn name(&self) -> &str {
        self.name
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, serde_json::Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.output.clone())
    }
}

struct HangingTool {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl NativeTool for HangingTool {
    fn name(&self) -> &str {
        "echo"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, serde_json::Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        pending().await
    }
}

fn registry(calls: Arc<AtomicUsize>, output: &str) -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(TestTool {
            name: "echo",
            calls,
            output: output.to_string(),
        }))
        .expect("register tool");
    registry
}

fn hanging_registry(calls: Arc<AtomicUsize>) -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(HangingTool { calls }))
        .expect("register tool");
    registry
}

fn request(choice: ToolChoice) -> ToolChatRequest {
    ToolChatRequest {
        model: ModelId::new("test-model"),
        messages: vec![ToolChatMessage::user("private prompt canary")],
        tools: vec![definition("echo")],
        tool_choice: choice,
        params: GenParams::default(),
    }
}

fn definition(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.to_string(),
        description: "test tool".to_string(),
        input_schema: json!({"type": "object"}),
    }
}

fn call(id: &str, name: &str) -> ModelToolCall {
    ModelToolCall {
        id: id.to_string(),
        name: name.to_string(),
        arguments: ToolCallArguments::Object(BTreeMap::from([(
            "value".to_string(),
            json!("private argument canary"),
        )])),
    }
}

fn call_outcome(id: &str, name: &str, usage: Option<Usage>) -> ToolChatOutcome {
    ToolChatOutcome {
        assistant: AssistantToolTurn {
            tool_calls: vec![call(id, name)],
            ..AssistantToolTurn::default()
        },
        usage,
        ..ToolChatOutcome::default()
    }
}

fn text_outcome(text: &str, usage: Option<Usage>) -> ToolChatOutcome {
    ToolChatOutcome {
        assistant: AssistantToolTurn {
            text: text.to_string(),
            reasoning: Some("private reasoning canary".to_string()),
            ..AssistantToolTurn::default()
        },
        usage,
        ..ToolChatOutcome::default()
    }
}

fn context() -> (TempDir, ToolContext) {
    let directory = tempfile::tempdir().expect("tempdir");
    let context = ToolContext::new(directory.path()).expect("tool context");
    (directory, context)
}

#[test]
fn turn_limits_are_validated_and_default_is_bounded() {
    assert_eq!(
        NativeTurnLimits::default().max_model_turns(),
        DEFAULT_NATIVE_MODEL_TURNS
    );
    assert_eq!(NativeTurnLimits::new(0), Err(NativeTurnLimitError::Zero));
    assert_eq!(
        NativeTurnLimits::new(MAX_NATIVE_MODEL_TURNS + 1),
        Err(NativeTurnLimitError::AboveHardMaximum)
    );
    assert_eq!(
        NativeTurnLimits::new(MAX_NATIVE_MODEL_TURNS)
            .expect("hard maximum")
            .max_model_turns(),
        MAX_NATIVE_MODEL_TURNS
    );
}

#[tokio::test]
async fn completed_reply_is_retrievable_but_debug_is_redacted() {
    let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(text_outcome(
        "private answer canary",
        Some(Usage {
            input_tokens: 2,
            output_tokens: 3,
            reasoning_tokens: Some(5),
            cached_input_tokens: Some(7),
        }),
    ))]));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::new(AtomicUsize::new(0)), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let (_directory, context) = crate::context();
    let outcome = driver
        .run(
            request(ToolChoice::Auto),
            &context,
            &RecordingAuthority::default(),
        )
        .await
        .expect("completed run");

    let reply = outcome.stop.assistant_reply().expect("assistant reply");
    assert_eq!(reply.text(), "private answer canary");
    let debug = format!("{outcome:?}");
    for canary in [
        "private prompt canary",
        "private answer canary",
        "private reasoning canary",
    ] {
        assert!(!debug.contains(canary));
    }
    assert_eq!(outcome.model_turns, 1);
    assert!(!outcome.tool_side_effects_possible);
}

#[tokio::test]
async fn model_and_tool_effects_are_one_based_serial_and_usage_saturates() {
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let first_usage = Usage {
        input_tokens: u64::MAX,
        output_tokens: 4,
        reasoning_tokens: Some(u64::MAX),
        cached_input_tokens: None,
    };
    let second_usage = Usage {
        input_tokens: 9,
        output_tokens: u64::MAX,
        reasoning_tokens: Some(2),
        cached_input_tokens: Some(u64::MAX),
    };
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", Some(first_usage))),
        ScriptStep::Outcome(text_outcome("done", Some(second_usage))),
    ]));
    let authority = RecordingAuthority::default();
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&tool_calls), "tool result"),
        PermissionPolicy::allow_by_default(),
    );
    let (_directory, context) = crate::context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("run");

    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        authority.effects(),
        vec![
            NativeSideEffect::ModelSend {
                turn: 1,
                wire_attempt: 1,
            },
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1,
            },
            NativeSideEffect::ModelSend {
                turn: 2,
                wire_attempt: 1,
            },
        ]
    );
    assert_eq!(outcome.model_turns, 2);
    assert!(outcome.tool_side_effects_possible);
    assert_eq!(
        outcome.usage,
        Some(Usage {
            input_tokens: u64::MAX,
            output_tokens: u64::MAX,
            reasoning_tokens: Some(u64::MAX),
            cached_input_tokens: Some(u64::MAX),
        })
    );
}

#[tokio::test]
async fn invalid_json_returns_static_non_echo_result_and_continues() {
    let raw = "private invalid-json canary {";
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(ToolChatOutcome {
            assistant: AssistantToolTurn {
                tool_calls: vec![ModelToolCall {
                    id: "call-1".to_string(),
                    name: "echo".to_string(),
                    arguments: ToolCallArguments::InvalidJson {
                        raw: raw.to_string(),
                    },
                }],
                ..AssistantToolTurn::default()
            },
            ..ToolChatOutcome::default()
        }),
        ScriptStep::Outcome(text_outcome("done", None)),
    ]));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::clone(&tool_calls), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("run");

    assert!(matches!(outcome.stop, NativeTurnStop::Completed(_)));
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert!(!outcome.tool_side_effects_possible);
    let requests = client.requests.lock().expect("request lock");
    let ToolChatMessage::ToolResult(result) = &requests[1].messages[2] else {
        panic!("second request should contain typed tool result");
    };
    assert_eq!(result.content, "ERROR: tool arguments were not valid JSON");
    assert!(!result.content.contains(raw));
    assert!(
        authority
            .effects()
            .iter()
            .all(|effect| !matches!(effect, NativeSideEffect::ToolOperation { .. }))
    );
}

#[tokio::test]
async fn denied_and_unknown_calls_are_model_facing_and_may_continue() {
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
        ScriptStep::Outcome(call_outcome("call-2", "hallucinated", None)),
        ScriptStep::Outcome(text_outcome("done", None)),
    ]));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&tool_calls), "unused"),
        PermissionPolicy::deny_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("run");

    assert!(matches!(outcome.stop, NativeTurnStop::Completed(_)));
    assert_eq!(outcome.model_turns, 3);
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert!(!outcome.tool_side_effects_possible);
    assert_eq!(
        authority
            .effects()
            .iter()
            .filter(|effect| matches!(effect, NativeSideEffect::ToolOperation { .. }))
            .count(),
        0
    );
}

#[tokio::test]
async fn ask_stops_without_tool_poll_or_replay_and_keeps_plan_out_of_debug() {
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
        ScriptStep::Outcome(text_outcome("must not send", None)),
    ]));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let policy = PermissionPolicy::deny_by_default()
        .with_rule(PermissionRule::new("echo", PermissionEffect::Ask).expect("permission rule"));
    let driver = NativeTurnDriver::new(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::clone(&tool_calls), "unused"),
        policy,
    );
    let authority = RecordingAuthority::default();
    let (directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("approval stop");

    let plan = outcome.stop.approval_plan().expect("approval plan");
    assert_eq!(plan.tool_call_id, "call-1");
    assert_eq!(plan.arguments["value"], json!("private argument canary"));
    assert_eq!(client.send_count(), 1);
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert!(!outcome.tool_side_effects_possible);
    let debug = format!("{outcome:?}");
    assert!(!debug.contains("private argument canary"));
    assert!(!debug.contains(directory.path().to_string_lossy().as_ref()));
}

#[tokio::test]
async fn refusal_with_a_tool_call_stops_before_permissions_or_tool() {
    let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(
        ToolChatOutcome {
            assistant: AssistantToolTurn {
                refusal: Some("private refusal canary".to_string()),
                content_parts: vec![AssistantContentPart::Refusal {
                    refusal: "private refusal block".to_string(),
                }],
                tool_calls: vec![call("call-1", "echo")],
                ..AssistantToolTurn::default()
            },
            ..ToolChatOutcome::default()
        },
    )]));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&tool_calls), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Required), &context, &authority)
        .await
        .expect("refused stop");

    assert!(matches!(outcome.stop, NativeTurnStop::Refused(_)));
    assert_eq!(
        outcome
            .stop
            .assistant_reply()
            .and_then(|reply| reply.refusal()),
        Some("private refusal canary")
    );
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert!(!format!("{outcome:?}").contains("private refusal canary"));
}

#[tokio::test]
async fn parallel_calls_stop_before_any_tool() {
    let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(
        ToolChatOutcome {
            assistant: AssistantToolTurn {
                tool_calls: vec![call("call-1", "echo"), call("call-2", "echo")],
                ..AssistantToolTurn::default()
            },
            ..ToolChatOutcome::default()
        },
    )]));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&tool_calls), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("parallel stop");

    assert!(matches!(
        outcome.stop,
        NativeTurnStop::UnsupportedParallelCalls
    ));
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert!(!outcome.tool_side_effects_possible);
}

#[tokio::test]
async fn tool_choice_none_named_and_required_are_enforced() {
    let cases = [
        (ToolChoice::None, call_outcome("call-1", "echo", None)),
        (ToolChoice::Required, text_outcome("no call", None)),
        (
            ToolChoice::Named("echo".to_string()),
            call_outcome("call-1", "other", None),
        ),
    ];

    for (choice, response) in cases {
        let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(response)]));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let driver = NativeTurnDriver::new(
            client,
            registry(Arc::clone(&tool_calls), "unused"),
            PermissionPolicy::allow_by_default(),
        );
        let authority = RecordingAuthority::default();
        let (_directory, context) = context();

        let outcome = driver
            .run(request(choice), &context, &authority)
            .await
            .expect("choice stop");
        assert!(matches!(outcome.stop, NativeTurnStop::ToolChoiceViolation));
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn definition_registry_mismatch_fails_before_model_send() {
    let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(text_outcome(
        "unused", None,
    ))]));
    let driver = NativeTurnDriver::new(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::new(AtomicUsize::new(0)), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();
    let mut mismatched = request(ToolChoice::Auto);
    mismatched.tools = vec![definition("different")];

    let error = driver
        .run(mismatched, &context, &authority)
        .await
        .expect_err("definition mismatch");

    assert_eq!(error.kind, ErrorKind::Config);
    assert_eq!(client.send_count(), 0);
    assert!(authority.effects().is_empty());
}

#[tokio::test]
async fn cancellation_and_timeout_stop_without_another_send() {
    let pre_cancel_client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(text_outcome(
        "unused", None,
    ))]));
    let driver = NativeTurnDriver::new(
        Arc::clone(&pre_cancel_client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::new(AtomicUsize::new(0)), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let (_directory, context) = context();
    context.cancellation_token().cancel();
    let outcome = driver
        .run(
            request(ToolChoice::Auto),
            &context,
            &RecordingAuthority::default(),
        )
        .await
        .expect("cancel stop");
    assert!(matches!(outcome.stop, NativeTurnStop::Cancelled));
    assert_eq!(pre_cancel_client.send_count(), 0);

    let timeout_client = Arc::new(ScriptedClient::new([ScriptStep::Error(ErrorKind::Timeout)]));
    let driver = NativeTurnDriver::new(
        Arc::clone(&timeout_client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::new(AtomicUsize::new(0)), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let (_directory, context) = crate::context();
    let outcome = driver
        .run(
            request(ToolChoice::Auto),
            &context,
            &RecordingAuthority::default(),
        )
        .await
        .expect("timeout stop");
    assert!(matches!(outcome.stop, NativeTurnStop::TimedOut));
    assert_eq!(timeout_client.send_count(), 1);
}

#[tokio::test]
async fn tool_timeout_stops_without_poll_or_second_send() {
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
        ScriptStep::Outcome(text_outcome("must not send", None)),
    ]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        hanging_registry(Arc::clone(&calls)),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let directory = tempfile::tempdir().expect("tempdir");
    let context = ToolContext::new(directory.path())
        .expect("context")
        .with_timeout(Duration::ZERO);

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("timeout stop");

    assert!(matches!(outcome.stop, NativeTurnStop::TimedOut));
    assert_eq!(client.send_count(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(outcome.tool_side_effects_possible);
}

#[tokio::test]
async fn cancellation_during_a_hanging_tool_stops_without_second_send() {
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
        ScriptStep::Outcome(text_outcome("must not send", None)),
    ]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        hanging_registry(Arc::clone(&calls)),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();
    let cancel = context.cancellation_token().clone();
    let observed_calls = Arc::clone(&calls);
    let canceller = tokio::spawn(async move {
        while observed_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancel.cancel();
    });

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("cancel stop");
    canceller.await.expect("canceller task");

    assert!(matches!(outcome.stop, NativeTurnStop::Cancelled));
    assert_eq!(client.send_count(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(outcome.tool_side_effects_possible);
    assert_eq!(
        authority
            .effects()
            .iter()
            .filter(|effect| matches!(effect, NativeSideEffect::ToolOperation { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn budget_exhaustion_is_a_typed_non_failover_stop() {
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
        ScriptStep::Outcome(text_outcome("must not send", None)),
    ]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::with_limits(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::clone(&calls), "private tool output canary"),
        PermissionPolicy::allow_by_default(),
        NativeTurnLimits::new(1).expect("one turn"),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("budget stop");

    assert!(matches!(outcome.stop, NativeTurnStop::BudgetExhausted));
    assert_eq!(client.send_count(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(outcome.tool_side_effects_possible);
    assert!(!format!("{outcome:?}").contains("private tool output canary"));
}

#[tokio::test]
async fn post_tool_model_errors_are_non_failover_typed_stops() {
    for kind in [
        ErrorKind::Transport,
        ErrorKind::Protocol,
        ErrorKind::Timeout,
    ] {
        let client = Arc::new(ScriptedClient::new([
            ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
            ScriptStep::Error(kind),
        ]));
        let calls = Arc::new(AtomicUsize::new(0));
        let driver = NativeTurnDriver::new(
            client,
            registry(Arc::clone(&calls), "tool result"),
            PermissionPolicy::allow_by_default(),
        );
        let authority = RecordingAuthority::default();
        let (_directory, context) = context();

        let outcome = driver
            .run(request(ToolChoice::Auto), &context, &authority)
            .await
            .expect("post-tool error must be typed");

        assert!(matches!(
            outcome.stop,
            NativeTurnStop::AbortedAfterToolActivity { kind: actual } if actual == kind
        ));
        assert!(outcome.tool_side_effects_possible);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!format!("{outcome:?}").contains("provider detail"));
    }
}

#[tokio::test]
async fn pre_tool_transport_error_remains_failover_eligible() {
    let client = Arc::new(ScriptedClient::new([ScriptStep::Error(
        ErrorKind::Transport,
    )]));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::new(AtomicUsize::new(0)), "unused"),
        PermissionPolicy::allow_by_default(),
    );
    let (_directory, context) = context();

    let error = driver
        .run(
            request(ToolChoice::Auto),
            &context,
            &RecordingAuthority::default(),
        )
        .await
        .expect_err("pre-tool transport should escape");

    assert_eq!(error.kind, ErrorKind::Transport);
    assert!(error.failover_eligible());
}

#[tokio::test]
async fn authority_revocation_prevents_physical_send_or_tool_poll() {
    let second_send = NativeSideEffect::ModelSend {
        turn: 2,
        wire_attempt: 1,
    };
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("call-1", "echo", None)),
        ScriptStep::Outcome(text_outcome("must not send", None)),
    ]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        Arc::clone(&client) as Arc<dyn AuthorizedToolChatClient>,
        registry(Arc::clone(&calls), "tool result"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::rejecting(second_send, ErrorKind::Conflict);
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("post-tool revocation is typed");

    assert!(matches!(
        outcome.stop,
        NativeTurnStop::AbortedAfterToolActivity {
            kind: ErrorKind::Conflict
        }
    ));
    assert_eq!(client.send_count(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let tool_effect = NativeSideEffect::ToolOperation {
        turn: 1,
        ordinal: 1,
    };
    let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(call_outcome(
        "call-1", "echo", None,
    ))]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&calls), "tool result"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::rejecting(tool_effect, ErrorKind::Conflict);
    let (_directory, context) = crate::context();
    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("tool revocation is typed");
    assert!(matches!(
        outcome.stop,
        NativeTurnStop::AbortedAfterToolActivity {
            kind: ErrorKind::Conflict
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_call_id_is_rejected_before_a_second_tool_poll() {
    let client = Arc::new(ScriptedClient::new([
        ScriptStep::Outcome(call_outcome("same-id", "echo", None)),
        ScriptStep::Outcome(call_outcome("same-id", "echo", None)),
    ]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&calls), "tool result"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();

    let outcome = driver
        .run(request(ToolChoice::Auto), &context, &authority)
        .await
        .expect("duplicate after tool must not fail over");

    assert!(matches!(
        outcome.stop,
        NativeTurnStop::AbortedAfterToolActivity {
            kind: ErrorKind::Protocol
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        authority
            .effects()
            .iter()
            .filter(|effect| matches!(effect, NativeSideEffect::ToolOperation { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn message_budget_is_preflighted_before_tool_poll() {
    let client = Arc::new(ScriptedClient::new([ScriptStep::Outcome(call_outcome(
        "call-1", "echo", None,
    ))]));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = NativeTurnDriver::new(
        client,
        registry(Arc::clone(&calls), "tool result"),
        PermissionPolicy::allow_by_default(),
    );
    let authority = RecordingAuthority::default();
    let (_directory, context) = context();
    let mut full = request(ToolChoice::Auto);
    full.messages = (0..512)
        .map(|_| ToolChatMessage::user("bounded message"))
        .collect();
    full.validate().expect("initial request at message limit");

    let error = driver
        .run(full, &context, &authority)
        .await
        .expect_err("next transcript cannot fit");

    assert_eq!(error.kind, ErrorKind::Protocol);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        authority
            .effects()
            .iter()
            .all(|effect| !matches!(effect, NativeSideEffect::ToolOperation { .. }))
    );
}
