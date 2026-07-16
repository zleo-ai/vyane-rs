//! Dark, fresh-only native AgentRun operation.
//!
//! The operation resolves all body-bearing input from the private spool only
//! after the durable run is active.  It supports exactly one direct OpenAI
//! Chat target, no tools, no sessions, and no restart replay.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use vyane_agent::{ControllerKind, ControllerRef, NativeExecutionScope, RunFailureCode};
use vyane_core::{
    AdapterTransport, AuthStyle, ChatMessage, ErrorKind, PinnedWorkdir, Protocol, ToolChatMessage,
    ToolChatRequest, ToolChoice,
};
use vyane_harness::native::{
    NativeTurnDriver, NativeTurnLimits, NativeTurnStop, PermissionPolicy, ToolContext, ToolRegistry,
};
use vyane_message::{
    EndpointKind, EndpointRef, IdempotencyKey, MessageDirection, NewDelivery, NewMessage,
};
use vyane_protocol::endpoint_routing_digest;
use vyane_service::{
    AgentExecutionIdentity, AgentExecutionSettlement, AgentExecutorOutcome,
    InProcessAgentOperation, InProcessAgentOperationContext, InProcessEffectAuthority,
    MESSAGE_COMPLETION_PRODUCER, MessageComponents, OwnerScopedService, authorized_native_client,
    message_run_completion,
};

use crate::native_agent_spool::{
    NativeAgentInput, NativeAgentInputSpool, NativeAgentPolicy, NativeAgentSpoolError,
    NativeAuthStyleSnapshot, NativeGenParamsSnapshot, NativeProtocolSnapshot, NativeTargetSnapshot,
};

const OPERATION_NAME: &str = "fresh-native-chat-v1";
const COMPLETION_DOMAIN: &[u8] = b"vyane.native-agent.completion.v1\0";
const MAX_PENDING_CLEANUPS: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
struct PendingNativeInput {
    fingerprint: String,
    run_id: String,
    worker_id: String,
    target_key: String,
    prompt_digest: String,
    policy_digest: String,
    timeout_seconds: u64,
}

impl PendingNativeInput {
    fn from_identity(
        controller: &ControllerRef,
        identity: &AgentExecutionIdentity,
    ) -> Option<Self> {
        Some(Self {
            fingerprint: controller.fingerprint.clone()?,
            run_id: identity.run_id().to_owned(),
            worker_id: identity.worker_id().to_owned(),
            target_key: identity.target_key().to_owned(),
            prompt_digest: identity.prompt_digest().to_owned(),
            policy_digest: identity.policy_digest().to_owned(),
            timeout_seconds: identity.timeout_seconds(),
        })
    }

    fn matches_controller(&self, controller: &ControllerRef) -> bool {
        controller.kind == ControllerKind::InProcess
            && controller.fingerprint.as_deref() == Some(self.fingerprint.as_str())
    }

    fn matches_input(&self, input: &NativeAgentInput) -> bool {
        input.run_id == self.run_id
            && input.worker_id == self.worker_id
            && input.policy.target_selector == self.target_key
            && input.prompt_sha256 == self.prompt_digest
            && input.policy_sha256 == self.policy_digest
            && input.policy.timeout_seconds == self.timeout_seconds
    }
}

/// Owner-bound native operation for the first intentionally dark production
/// seam. Construction performs no model, spool, or store effects.
pub(crate) struct FreshNativeAgentOperation {
    owner: String,
    service: OwnerScopedService,
    spool: NativeAgentInputSpool,
    messages: MessageComponents,
    pending_cleanup: Arc<Mutex<BTreeMap<String, PendingNativeInput>>>,
}

impl FreshNativeAgentOperation {
    pub(crate) fn new(
        owner: impl Into<String>,
        service: OwnerScopedService,
        spool: NativeAgentInputSpool,
        messages: MessageComponents,
    ) -> Self {
        Self {
            owner: owner.into(),
            service,
            spool,
            messages,
            pending_cleanup: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn track_pending(&self, controller: &ControllerRef, identity: &AgentExecutionIdentity) -> bool {
        let Some(pending) = PendingNativeInput::from_identity(controller, identity) else {
            return false;
        };
        let Ok(mut tracked) = self.pending_cleanup.lock() else {
            return false;
        };
        if let Some(existing) = tracked.get(&controller.id) {
            return existing == &pending;
        }
        if tracked.len() >= MAX_PENDING_CLEANUPS {
            return false;
        }
        tracked.insert(controller.id.clone(), pending);
        true
    }

    fn forget_pending(&self, controller: &ControllerRef) {
        forget_pending(&self.pending_cleanup, controller);
    }

    fn cleanup_confirmed_gone(&self, controller: &ControllerRef) {
        let pending = self
            .pending_cleanup
            .lock()
            .ok()
            .and_then(|tracked| tracked.get(&controller.id).cloned());
        let Some(pending) = pending.filter(|pending| pending.matches_controller(controller)) else {
            return;
        };
        let _ = match self.spool.read(&pending.run_id, &pending.worker_id) {
            Ok(input) if pending.matches_input(&input) => self.spool.remove_exact(&input).is_ok(),
            Err(NativeAgentSpoolError::NotFound) => true,
            Ok(_) | Err(_) => false,
        };
        // Durable confirmation consumes the exact recovery tombstone even when
        // private-file cleanup is unavailable, so there is no safe retry path.
        // Release the bounded in-memory slot; uncertain spool content remains
        // owner-private and is never replayed or removed by a mismatched run.
        self.forget_pending(controller);
    }

    fn exact_input(&self, identity: &AgentExecutionIdentity) -> Option<NativeAgentInput> {
        let input = self
            .spool
            .read(identity.run_id(), identity.worker_id())
            .ok()?;
        (input.owner == self.owner
            && input.run_id == identity.run_id()
            && input.worker_id == identity.worker_id()
            && input.policy.target_selector == identity.target_key()
            && input.prompt_sha256 == identity.prompt_digest()
            && input.policy_sha256 == identity.policy_digest()
            && input.policy.timeout_seconds == identity.timeout_seconds())
        .then_some(input)
    }

    fn exact_target(&self, input: &NativeAgentInput) -> Option<vyane_core::BoundTarget> {
        let resolved = self.service.resolve(&input.policy.target_selector).ok()?;
        if resolved.selector != input.policy.target_selector || resolved.chain.len() != 1 {
            return None;
        }
        let bound = resolved.chain.into_iter().next()?;
        target_matches(&input.policy.target, &bound).then_some(bound)
    }

    fn remove_quiesced(&self, controller: &ControllerRef, input: &NativeAgentInput) -> bool {
        if self.spool.remove_exact(input).is_err() {
            return false;
        }
        self.forget_pending(controller);
        true
    }

    fn quiesced_failure(
        &self,
        controller: &ControllerRef,
        input: &NativeAgentInput,
        code: RunFailureCode,
    ) -> AgentExecutorOutcome {
        if !self.remove_quiesced(controller, input) {
            return AgentExecutorOutcome::Unknown;
        }
        if code == RunFailureCode::TimedOut {
            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::TimedOut)
        } else {
            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed { code })
        }
    }
}

/// Freeze one public native submission into the private, fresh-only input
/// shape. This deliberately accepts only a single direct OpenAI Chat target;
/// callers must provide an already pinned workdir and its identity.
pub(crate) fn native_input_for_submission(
    owner: &str,
    run_id: &str,
    worker_id: &str,
    details: NativeSubmissionDetails<'_>,
) -> Result<NativeAgentInput, NativeAgentSpoolError> {
    let NativeSubmissionDetails {
        prompt,
        selector,
        bound,
        workdir,
        system,
        timeout_seconds,
    } = details;
    if bound.target.protocol != Protocol::OpenaiChat
        || bound.target.harness.is_some()
        || bound.transport != AdapterTransport::DirectHttp
        || bound.endpoint.is_none()
    {
        return Err(NativeAgentSpoolError::BindingMismatch);
    }
    let endpoint = bound
        .endpoint
        .as_ref()
        .ok_or(NativeAgentSpoolError::BindingMismatch)?;
    let auth_style = endpoint.auth.as_ref().map(|auth| match auth.style {
        AuthStyle::Bearer => NativeAuthStyleSnapshot::Bearer,
        AuthStyle::XApiKey => NativeAuthStyleSnapshot::XApiKey,
    });
    let routing_digest = endpoint_routing_digest(&endpoint.base_url)
        .map_err(|_| NativeAgentSpoolError::BindingMismatch)?;
    NativeAgentInput::fresh(
        owner,
        run_id,
        worker_id,
        prompt,
        NativeAgentPolicy {
            target_selector: selector.to_owned(),
            target: NativeTargetSnapshot {
                provider: bound.target.provider.as_str().to_owned(),
                protocol: NativeProtocolSnapshot::OpenaiChat,
                model: bound.target.model.as_str().to_owned(),
                auth_style,
                routing_digest,
                params: params_snapshot(&bound.params),
            },
            canonical_workdir: workdir.canonical_path().to_path_buf(),
            workdir_identity: workdir.identity().clone(),
            system,
            timeout_seconds,
            max_model_turns: 2,
        },
    )
}

pub(crate) struct NativeSubmissionDetails<'a> {
    pub(crate) prompt: String,
    pub(crate) selector: &'a str,
    pub(crate) bound: &'a vyane_core::BoundTarget,
    pub(crate) workdir: &'a PinnedWorkdir,
    pub(crate) system: Option<String>,
    pub(crate) timeout_seconds: u64,
}

#[async_trait]
impl InProcessAgentOperation for FreshNativeAgentOperation {
    fn name(&self) -> &str {
        OPERATION_NAME
    }

    fn owner(&self) -> &str {
        &self.owner
    }

    fn admit(&self, identity: &AgentExecutionIdentity, controller: &ControllerRef) -> bool {
        controller.kind == ControllerKind::InProcess
            && controller.fingerprint.is_some()
            && !identity.run_id().is_empty()
            && !identity.worker_id().is_empty()
            && !identity.target_key().is_empty()
    }

    fn confirmed_gone(&self, controller: &ControllerRef) {
        self.cleanup_confirmed_gone(controller);
    }

    async fn execute(
        &self,
        context: InProcessAgentOperationContext,
        identity: AgentExecutionIdentity,
        authority: InProcessEffectAuthority<'_>,
    ) -> AgentExecutorOutcome {
        let controller = context.controller().clone();
        if !self.track_pending(&controller, &identity) {
            return AgentExecutorOutcome::Unknown;
        }
        let Some(input) = self.exact_input(&identity) else {
            return AgentExecutorOutcome::Unknown;
        };
        let Some(bound) = self.exact_target(&input) else {
            return AgentExecutorOutcome::Unknown;
        };
        let Ok(workdir) = PinnedWorkdir::open(&input.policy.canonical_workdir) else {
            return AgentExecutorOutcome::Unknown;
        };
        if workdir.canonical_path() != input.policy.canonical_workdir
            || workdir.identity() != &input.policy.workdir_identity
        {
            return AgentExecutorOutcome::Unknown;
        }

        let Ok(scope) = NativeExecutionScope::fresh(
            identity.target_key(),
            &input.prompt_sha256,
            &input.policy_sha256,
            None,
        ) else {
            return AgentExecutorOutcome::Unknown;
        };
        let Ok(native_authority) = authority.bind_fresh_native_scope(scope).await else {
            return AgentExecutorOutcome::Unknown;
        };
        let Ok(client) = authorized_native_client(&bound) else {
            return AgentExecutorOutcome::Unknown;
        };
        let Ok(limits) = NativeTurnLimits::new(input.policy.max_model_turns) else {
            return AgentExecutorOutcome::Unknown;
        };
        let Ok(tool_context) = ToolContext::new(&input.policy.canonical_workdir) else {
            return AgentExecutorOutcome::Unknown;
        };
        if tool_context.workdir() != workdir.canonical_path() {
            return AgentExecutorOutcome::Unknown;
        }
        let tool_context = tool_context
            .with_cancellation_token(context.cancellation().clone())
            .with_timeout(Duration::from_secs(input.policy.timeout_seconds))
            .with_deadline(context.deadline());
        let request = native_request(&input, &bound);
        let driver = NativeTurnDriver::with_limits(
            client,
            ToolRegistry::new(),
            PermissionPolicy::deny_by_default(),
            limits,
        );
        let turn = tokio::time::timeout_at(
            context.deadline(),
            driver.run(request, &tool_context, &native_authority),
        )
        .await;
        drop(native_authority);
        drop(workdir);

        let reply = match turn {
            Ok(Ok(outcome)) => match outcome.stop {
                NativeTurnStop::Completed(reply) => reply,
                NativeTurnStop::Cancelled => {
                    // The operation knows its futures are quiesced, but only
                    // the durable cancellation path may settle cancellation.
                    // Retain the spool and leave reconciliation in charge.
                    return AgentExecutorOutcome::Unknown;
                }
                NativeTurnStop::TimedOut => {
                    return self.quiesced_failure(&controller, &input, RunFailureCode::TimedOut);
                }
                NativeTurnStop::ApprovalRequired(_) | NativeTurnStop::ToolChoiceViolation => {
                    return self.quiesced_failure(
                        &controller,
                        &input,
                        RunFailureCode::PolicyDenied,
                    );
                }
                NativeTurnStop::Refused(_)
                | NativeTurnStop::BudgetExhausted
                | NativeTurnStop::UnsupportedParallelCalls
                | NativeTurnStop::AbortedAfterToolActivity { .. } => {
                    return self.quiesced_failure(&controller, &input, RunFailureCode::Internal);
                }
                _ => {
                    return self.quiesced_failure(&controller, &input, RunFailureCode::Internal);
                }
            },
            Ok(Err(error)) => {
                if error.kind == ErrorKind::Cancelled {
                    return AgentExecutorOutcome::Unknown;
                }
                return self.quiesced_failure(&controller, &input, failure_code(error.kind));
            }
            Err(_) => {
                return self.quiesced_failure(&controller, &input, RunFailureCode::TimedOut);
            }
        };

        let key = completion_key(identity.run_id(), identity.generation());
        let message = completion_message(&input, &key, reply.text().to_owned());
        let Ok(completion) = message_run_completion(key.clone(), &message) else {
            return self.quiesced_failure(&controller, &input, RunFailureCode::Internal);
        };
        let Ok(prepared) = authority.prepare_completion(completion).await else {
            return AgentExecutorOutcome::Unknown;
        };
        let cleanup_spool = self.spool.clone();
        let cleanup_pending = Arc::clone(&self.pending_cleanup);
        let cleanup_controller = controller.clone();
        let cleanup_input = input.clone();
        let Ok(staged) = self
            .messages
            .stage_completion_with_cleanup(prepared, message, move || {
                if cleanup_spool.remove_exact(&cleanup_input).is_err() {
                    return false;
                }
                forget_pending(&cleanup_pending, &cleanup_controller);
                true
            })
            .await
        else {
            return AgentExecutorOutcome::Unknown;
        };
        AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::CompletionStaged(staged))
    }
}

fn forget_pending(
    pending_cleanup: &Mutex<BTreeMap<String, PendingNativeInput>>,
    controller: &ControllerRef,
) {
    let Ok(mut tracked) = pending_cleanup.lock() else {
        return;
    };
    if tracked
        .get(&controller.id)
        .is_some_and(|pending| pending.matches_controller(controller))
    {
        tracked.remove(&controller.id);
    }
}

fn target_matches(snapshot: &NativeTargetSnapshot, bound: &vyane_core::BoundTarget) -> bool {
    let protocol = match bound.target.protocol {
        Protocol::OpenaiChat => NativeProtocolSnapshot::OpenaiChat,
        _ => return false,
    };
    let routing_digest = bound
        .endpoint
        .as_ref()
        .and_then(|endpoint| endpoint_routing_digest(&endpoint.base_url).ok());
    let auth_style = bound
        .endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.auth.as_ref())
        .map(|auth| match auth.style {
            AuthStyle::Bearer => NativeAuthStyleSnapshot::Bearer,
            AuthStyle::XApiKey => NativeAuthStyleSnapshot::XApiKey,
        });
    snapshot.provider == bound.target.provider.as_str()
        && snapshot.protocol == protocol
        && snapshot.model == bound.target.model.as_str()
        && bound.target.harness.is_none()
        && bound.transport == AdapterTransport::DirectHttp
        && snapshot.auth_style == auth_style
        && routing_digest.as_deref() == Some(snapshot.routing_digest.as_str())
        && snapshot.params == params_snapshot(&bound.params)
}

fn params_snapshot(params: &vyane_core::GenParams) -> NativeGenParamsSnapshot {
    let extra_digest = (!params.extra.is_empty()).then(|| {
        let encoded = serde_json::to_string(&params.extra).unwrap_or_default();
        format!("{:x}", Sha256::digest(encoded.as_bytes()))
    });
    NativeGenParamsSnapshot {
        temperature: params.temperature,
        top_p: params.top_p,
        max_output_tokens: params.max_output_tokens,
        effort: params.effort,
        extra_digest,
    }
}

fn native_request(input: &NativeAgentInput, bound: &vyane_core::BoundTarget) -> ToolChatRequest {
    let mut messages = Vec::with_capacity(2);
    if let Some(system) = &input.policy.system {
        messages.push(ToolChatMessage::Text(ChatMessage::system(system.clone())));
    }
    messages.push(ToolChatMessage::Text(ChatMessage::user(
        input.prompt.clone(),
    )));
    ToolChatRequest {
        model: bound.target.model.clone(),
        messages,
        tools: Vec::new(),
        tool_choice: ToolChoice::None,
        params: bound.params.clone(),
    }
}

fn completion_key(run_id: &str, generation: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(COMPLETION_DOMAIN);
    digest.update((run_id.len() as u64).to_be_bytes());
    digest.update(run_id.as_bytes());
    digest.update(generation.to_be_bytes());
    format!("{:x}", digest.finalize())
}

fn completion_message(input: &NativeAgentInput, key: &str, body: String) -> NewMessage {
    NewMessage {
        conversation_id: format!("agent-run-{}", input.run_id),
        session_id: None,
        direction: MessageDirection::Internal,
        kind: "agent_run_completion".into(),
        sender: EndpointRef {
            kind: EndpointKind::Agent,
            id: "resident-native-agent".into(),
        },
        body,
        payload: serde_json::json!({"status": "completed"}),
        reply_to: None,
        trace_id: None,
        correlation_id: Some(input.run_id.clone()),
        idempotency: IdempotencyKey {
            producer: MESSAGE_COMPLETION_PRODUCER.into(),
            key: key.into(),
        },
        deliveries: vec![NewDelivery {
            route: "local".into(),
            target: EndpointRef {
                kind: EndpointKind::User,
                id: "local-requester".into(),
            },
            available_at: None,
            expires_at: None,
            max_attempts: 3,
        }],
    }
}

const fn failure_code(kind: ErrorKind) -> RunFailureCode {
    match kind {
        ErrorKind::Cancelled => RunFailureCode::Internal,
        ErrorKind::Timeout => RunFailureCode::TimedOut,
        ErrorKind::Auth => RunFailureCode::PolicyDenied,
        ErrorKind::Transport | ErrorKind::Protocol | ErrorKind::RateLimited => {
            RunFailureCode::DispatchFailed
        }
        _ => RunFailureCode::Internal,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, OnceLock};

    use chrono::Utc;
    use tempfile::TempDir;
    use vyane_agent::{
        AgentStore, ExecutionBackend, NewAgentRun, NewWorker, RunMode, RunState, SqliteAgentStore,
    };
    use vyane_config::{ProfilePatch, ResolvedConfig};
    use vyane_core::{AuthStyle, CancellationToken, ModelId, PinnedWorkdir};
    use vyane_provider::{Provider, ProviderRegistry};
    use vyane_service::{
        AgentCompletionPublisherOptions, AgentExecutionItemStatus, AgentExecutionOptions,
        InProcessAgentComponents, LoadedConfig, OwnerContext, StoragePaths, VyaneService,
    };
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::native_agent_spool::{NativeAgentPolicy, NativeAgentSpoolError};

    const OWNER: &str = "local";
    const PROFILE: &str = "native-test";
    const RUN_ID: &str = "run-native-e2e";
    const WORKER_ID: &str = "worker-native-e2e";

    fn assembly_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn service(root: &TempDir, server: &MockServer) -> (OwnerScopedService, StoragePaths) {
        let mut providers = ProviderRegistry::new();
        providers.insert(
            "mock-provider",
            Provider {
                base_url: server.uri(),
                api_key_env: Some("VYANE_NATIVE_TEST_KEY".into()),
                auth_style: AuthStyle::Bearer,
                protocol: Protocol::OpenaiChat,
                default_model: Some(ModelId::new("mock-model")),
                extra: serde_json::Map::new(),
                env_inject: BTreeMap::new(),
            },
        );
        let mut profiles = BTreeMap::new();
        profiles.insert(
            PROFILE.into(),
            ProfilePatch {
                provider: Some("mock-provider".into()),
                protocol: Some(Protocol::OpenaiChat),
                harness: None,
                model: Some(ModelId::new("mock-model")),
                ..ProfilePatch::default()
            },
        );
        let paths = StoragePaths::from_data_dir(root.path().join("data"));
        let service = VyaneService::from_loaded_with_paths(
            LoadedConfig {
                config: ResolvedConfig {
                    providers,
                    profiles,
                },
                files: Vec::new(),
                secrets: BTreeMap::from([(
                    "VYANE_NATIVE_TEST_KEY".into(),
                    "test-only-credential".into(),
                )]),
            },
            paths.clone(),
        )
        .unwrap();
        (service.scope(OwnerContext::single_user_local()), paths)
    }

    fn policy(service: &OwnerScopedService, workdir: &PinnedWorkdir) -> NativeAgentPolicy {
        let bound = service.resolve(PROFILE).unwrap().chain.remove(0);
        NativeAgentPolicy {
            target_selector: PROFILE.into(),
            target: NativeTargetSnapshot {
                provider: bound.target.provider.as_str().into(),
                protocol: NativeProtocolSnapshot::OpenaiChat,
                model: bound.target.model.as_str().into(),
                auth_style: bound
                    .endpoint
                    .as_ref()
                    .and_then(|endpoint| endpoint.auth.as_ref())
                    .map(|auth| match auth.style {
                        AuthStyle::Bearer => NativeAuthStyleSnapshot::Bearer,
                        AuthStyle::XApiKey => NativeAuthStyleSnapshot::XApiKey,
                    }),
                routing_digest: endpoint_routing_digest(&bound.endpoint.as_ref().unwrap().base_url)
                    .unwrap(),
                params: params_snapshot(&bound.params),
            },
            canonical_workdir: workdir.canonical_path().to_path_buf(),
            workdir_identity: workdir.identity().clone(),
            system: Some("Answer concisely.".into()),
            timeout_seconds: 30,
            max_model_turns: 2,
        }
    }

    fn input(service: &OwnerScopedService, workdir: &PinnedWorkdir) -> NativeAgentInput {
        NativeAgentInput::fresh(
            OWNER,
            RUN_ID,
            WORKER_ID,
            "Return the test answer.",
            policy(service, workdir),
        )
        .unwrap()
    }

    fn create_run(store: &SqliteAgentStore, input: &NativeAgentInput, timeout_seconds: u64) {
        store
            .create_root(
                OWNER,
                &NewWorker {
                    id: WORKER_ID.into(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: RUN_ID.into(),
                    worker_id: WORKER_ID.into(),
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    execution_backend: ExecutionBackend::NativeInProcess,
                    mode: RunMode::Autonomous,
                    target_key: PROFILE.into(),
                    prompt_digest: input.prompt_sha256.clone(),
                    policy_digest: input.policy_sha256.clone(),
                    available_at: Utc::now(),
                    timeout_seconds,
                    max_resume_attempts: 0,
                },
            )
            .unwrap();
    }

    fn only_input_path(root: &Path) -> PathBuf {
        let owner_root = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap();
        let paths = std::fs::read_dir(owner_root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|value| value == "json"))
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 1);
        paths.into_iter().next().unwrap()
    }

    fn execution_components(
        service: OwnerScopedService,
        spool: NativeAgentInputSpool,
        messages: MessageComponents,
        store: Arc<SqliteAgentStore>,
    ) -> InProcessAgentComponents {
        let agent_store: Arc<dyn AgentStore> = store;
        InProcessAgentComponents::new(
            OWNER,
            agent_store,
            Arc::new(FreshNativeAgentOperation::new(
                OWNER, service, spool, messages,
            )),
        )
        .unwrap()
    }

    async fn execute_once(
        components: InProcessAgentComponents,
        cancellation: CancellationToken,
    ) -> vyane_service::AgentExecutionReport {
        components
            .execution_driver(
                "native-test-lease",
                AgentExecutionOptions {
                    batch_limit: 1,
                    max_in_flight: 1,
                    ..AgentExecutionOptions::default()
                },
            )
            .unwrap()
            .execute_once(cancellation)
            .await
            .unwrap()
    }

    #[derive(Clone, Copy)]
    enum PreWireCase {
        PromptDigest,
        PolicyDigest,
        TargetSnapshot,
        AuthStyleDrift,
        TimeoutDrift,
        CancelledBeforeClaim,
    }

    async fn assert_pre_wire_rejection(case: PreWireCase) {
        let server = MockServer::start().await;
        let root = tempfile::tempdir().unwrap();
        let workdir_path = root.path().join("workspace");
        std::fs::create_dir(&workdir_path).unwrap();
        let workdir = PinnedWorkdir::open(&workdir_path).unwrap();
        let (service, paths) = service(&root, &server);
        let spool = NativeAgentInputSpool::open(root.path().join("native-input"), OWNER).unwrap();
        let mut frozen_policy = policy(&service, &workdir);
        if matches!(case, PreWireCase::TargetSnapshot) {
            frozen_policy.target.routing_digest = "f".repeat(64);
        }
        if matches!(case, PreWireCase::AuthStyleDrift) {
            frozen_policy.target.auth_style = Some(NativeAuthStyleSnapshot::XApiKey);
        }
        let input = NativeAgentInput::fresh(
            OWNER,
            RUN_ID,
            WORKER_ID,
            "Return the test answer.",
            frozen_policy,
        )
        .unwrap();
        spool.create(&input).unwrap();
        let messages = MessageComponents::open(&paths, OWNER).unwrap();
        let sqlite_store =
            Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        let prompt_digest = if matches!(case, PreWireCase::PromptDigest) {
            "a".repeat(64)
        } else {
            input.prompt_sha256.clone()
        };
        let policy_digest = if matches!(case, PreWireCase::PolicyDigest) {
            "b".repeat(64)
        } else {
            input.policy_sha256.clone()
        };
        let mut durable_input = input.clone();
        durable_input.prompt_sha256 = prompt_digest;
        durable_input.policy_sha256 = policy_digest;
        create_run(
            sqlite_store.as_ref(),
            &durable_input,
            if matches!(case, PreWireCase::TimeoutDrift) {
                31
            } else {
                30
            },
        );
        let store: Arc<dyn AgentStore> = sqlite_store.clone();
        let components = InProcessAgentComponents::new(
            OWNER,
            Arc::clone(&store),
            Arc::new(FreshNativeAgentOperation::new(
                OWNER,
                service,
                spool.clone(),
                messages,
            )),
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        if matches!(case, PreWireCase::CancelledBeforeClaim) {
            cancellation.cancel();
        }
        let report = execute_once(components, cancellation).await;
        if matches!(case, PreWireCase::CancelledBeforeClaim) {
            assert_eq!(report.claimed, 0);
            assert!(report.cancelled_before_claim);
        } else {
            assert_eq!(report.claimed, 1);
            assert_eq!(
                report.items,
                vec![AgentExecutionItemStatus::ControllerUnknown]
            );
        }
        assert!(spool.read(RUN_ID, WORKER_ID).is_ok());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn dark_native_run_validates_scope_then_commits_and_publishes_once() {
        let _assembly = assembly_test_lock().lock().await;
        assert_pre_wire_rejection(PreWireCase::PromptDigest).await;
        assert_pre_wire_rejection(PreWireCase::PolicyDigest).await;
        assert_pre_wire_rejection(PreWireCase::TargetSnapshot).await;
        assert_pre_wire_rejection(PreWireCase::AuthStyleDrift).await;
        assert_pre_wire_rejection(PreWireCase::TimeoutDrift).await;
        assert_pre_wire_rejection(PreWireCase::CancelledBeforeClaim).await;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "mock-model",
                "choices": [{
                    "message": {"role": "assistant", "content": "native answer"},
                    "finish_reason": "stop"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let root = tempfile::tempdir().unwrap();
        let workdir_path = root.path().join("workspace");
        std::fs::create_dir(&workdir_path).unwrap();
        let workdir = PinnedWorkdir::open(&workdir_path).unwrap();
        let (service, paths) = service(&root, &server);
        let spool = NativeAgentInputSpool::open(root.path().join("native-input"), OWNER).unwrap();
        let input = input(&service, &workdir);
        let bound = service.resolve(PROFILE).unwrap().chain.remove(0);
        let request = native_request(&input, &bound);
        assert!(request.tools.is_empty());
        assert_eq!(request.tool_choice, ToolChoice::None);
        spool.create(&input).unwrap();

        let messages = MessageComponents::open(&paths, OWNER).unwrap();
        let sqlite_store =
            Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        create_run(sqlite_store.as_ref(), &input, 30);
        let store: Arc<dyn AgentStore> = sqlite_store.clone();
        let operation = Arc::new(FreshNativeAgentOperation::new(
            OWNER,
            service,
            spool.clone(),
            messages.clone(),
        ));
        let components = InProcessAgentComponents::new_with_completion_sinks(
            OWNER,
            Arc::clone(&store),
            operation,
            vec![messages.completion_sink()],
        )
        .unwrap();

        let report = components
            .execution_driver(
                "native-test-lease",
                AgentExecutionOptions {
                    batch_limit: 1,
                    max_in_flight: 1,
                    ..AgentExecutionOptions::default()
                },
            )
            .unwrap()
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.claimed, 1);
        assert_eq!(report.items, vec![AgentExecutionItemStatus::Settled]);
        let succeeded = sqlite_store.get_run(OWNER, RUN_ID).unwrap().unwrap();
        assert_eq!(succeeded.state, RunState::Succeeded);
        let public_status = format!("{report:?} {succeeded:?}");
        for body in [
            "Return the test answer.",
            "Answer concisely.",
            "test-only-credential",
            "native answer",
        ] {
            assert!(!public_status.contains(body));
        }
        assert!(matches!(
            spool.read(RUN_ID, WORKER_ID),
            Err(NativeAgentSpoolError::NotFound)
        ));

        let publisher = components
            .completion_publisher(
                "native-test-projector",
                AgentCompletionPublisherOptions::default(),
            )
            .unwrap();
        let first = publisher.project_once().await.unwrap();
        assert_eq!(first.acknowledged, first.scanned);
        assert_eq!(
            messages
                .published_completion_body(Arc::clone(&store), RUN_ID)
                .await
                .unwrap()
                .as_deref(),
            Some("native answer")
        );
        let second = publisher.project_once().await.unwrap();
        assert_eq!(second.scanned, 0);

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let request: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(request["model"], "mock-model");
        assert_eq!(request["messages"][1]["content"], "Return the test answer.");
        assert_eq!(
            completion_message(&input, "completion-key", "body".into()).conversation_id,
            format!("agent-run-{RUN_ID}")
        );
    }

    #[tokio::test]
    async fn malformed_private_input_is_body_free_and_zero_wire() {
        let _assembly = assembly_test_lock().lock().await;
        let server = MockServer::start().await;
        let root = tempfile::tempdir().unwrap();
        let workdir_path = root.path().join("workspace");
        std::fs::create_dir(&workdir_path).unwrap();
        let workdir = PinnedWorkdir::open(&workdir_path).unwrap();
        let (service, paths) = service(&root, &server);
        let spool_root = root.path().join("native-input");
        let spool = NativeAgentInputSpool::open(&spool_root, OWNER).unwrap();
        let input = input(&service, &workdir);
        spool.create(&input).unwrap();
        std::fs::write(
            only_input_path(&spool_root),
            br#"{"prompt":"TEST_MALFORMED_BODY_CANARY""#,
        )
        .unwrap();

        let messages = MessageComponents::open(&paths, OWNER).unwrap();
        let store = Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        create_run(store.as_ref(), &input, 30);
        let report = execute_once(
            execution_components(service, spool.clone(), messages, Arc::clone(&store)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.claimed, 1);
        assert_eq!(
            report.items,
            vec![AgentExecutionItemStatus::ControllerUnknown]
        );
        assert_eq!(
            store.get_run(OWNER, RUN_ID).unwrap().unwrap().state,
            RunState::Running
        );
        assert_eq!(
            spool.read(RUN_ID, WORKER_ID),
            Err(NativeAgentSpoolError::CorruptInput)
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        assert!(!format!("{report:?}").contains("TEST_MALFORMED_BODY_CANARY"));
    }

    #[tokio::test]
    async fn durable_timeout_bounds_wire_and_retains_recovery_input() {
        let _assembly = assembly_test_lock().lock().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(3))
                    .set_body_json(serde_json::json!({
                        "model": "mock-model",
                        "choices": [{
                            "message": {"role": "assistant", "content": "late answer"},
                            "finish_reason": "stop"
                        }]
                    })),
            )
            .mount(&server)
            .await;

        let root = tempfile::tempdir().unwrap();
        let workdir_path = root.path().join("workspace");
        std::fs::create_dir(&workdir_path).unwrap();
        let workdir = PinnedWorkdir::open(&workdir_path).unwrap();
        let (service, paths) = service(&root, &server);
        let spool = NativeAgentInputSpool::open(root.path().join("native-input"), OWNER).unwrap();
        let mut frozen_policy = policy(&service, &workdir);
        frozen_policy.timeout_seconds = 1;
        let input = NativeAgentInput::fresh(
            OWNER,
            RUN_ID,
            WORKER_ID,
            "Return before the durable deadline.",
            frozen_policy,
        )
        .unwrap();
        spool.create(&input).unwrap();
        let messages = MessageComponents::open(&paths, OWNER).unwrap();
        let store = Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        create_run(store.as_ref(), &input, 1);

        let report = tokio::time::timeout(
            Duration::from_secs(2),
            execute_once(
                execution_components(service, spool.clone(), messages, Arc::clone(&store)),
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(report.claimed, 1);
        assert_eq!(report.items, vec![AgentExecutionItemStatus::TimedOut]);
        assert_eq!(
            store.get_run(OWNER, RUN_ID).unwrap().unwrap().state,
            RunState::Running
        );
        assert_eq!(spool.read(RUN_ID, WORKER_ID).unwrap(), input);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_after_send_leaves_durable_reconciliation_in_charge() {
        let _assembly = assembly_test_lock().lock().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(3))
                    .set_body_json(serde_json::json!({
                        "model": "mock-model",
                        "choices": [{
                            "message": {"role": "assistant", "content": "late answer"},
                            "finish_reason": "stop"
                        }]
                    })),
            )
            .mount(&server)
            .await;

        let root = tempfile::tempdir().unwrap();
        let workdir_path = root.path().join("workspace");
        std::fs::create_dir(&workdir_path).unwrap();
        let workdir = PinnedWorkdir::open(&workdir_path).unwrap();
        let (service, paths) = service(&root, &server);
        let spool = NativeAgentInputSpool::open(root.path().join("native-input"), OWNER).unwrap();
        let input = input(&service, &workdir);
        spool.create(&input).unwrap();
        let messages = MessageComponents::open(&paths, OWNER).unwrap();
        let store = Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        create_run(store.as_ref(), &input, 30);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(execute_once(
            execution_components(service, spool.clone(), messages, Arc::clone(&store)),
            task_cancellation,
        ));

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !server.received_requests().await.unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        let report = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.claimed, 1);
        assert_eq!(report.items, vec![AgentExecutionItemStatus::Cancelled]);
        assert_eq!(
            store.get_run(OWNER, RUN_ID).unwrap().unwrap().state,
            RunState::Running
        );
        assert_eq!(spool.read(RUN_ID, WORKER_ID).unwrap(), input);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unavailable_completion_store_never_reports_success_or_drops_input() {
        let _assembly = assembly_test_lock().lock().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "mock-model",
                "choices": [{
                    "message": {"role": "assistant", "content": "unstaged answer"},
                    "finish_reason": "stop"
                }]
            })))
            .mount(&server)
            .await;

        let root = tempfile::tempdir().unwrap();
        let workdir_path = root.path().join("workspace");
        std::fs::create_dir(&workdir_path).unwrap();
        let workdir = PinnedWorkdir::open(&workdir_path).unwrap();
        let (service, paths) = service(&root, &server);
        let spool = NativeAgentInputSpool::open(root.path().join("native-input"), OWNER).unwrap();
        let input = input(&service, &workdir);
        spool.create(&input).unwrap();
        let messages = MessageComponents::open(&paths, OWNER).unwrap();
        std::fs::remove_file(paths.message_db_path()).unwrap();
        let store = Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        create_run(store.as_ref(), &input, 30);
        let replacement_policy = policy(&service, &workdir);
        let operation = Arc::new(FreshNativeAgentOperation::new(
            OWNER,
            service,
            spool.clone(),
            messages,
        ));
        let agent_store: Arc<dyn AgentStore> = store.clone();
        let components = InProcessAgentComponents::new(
            OWNER,
            agent_store,
            operation.clone() as Arc<dyn InProcessAgentOperation>,
        )
        .unwrap();
        let report = execute_once(components, CancellationToken::new()).await;
        assert_eq!(report.claimed, 1);
        assert_eq!(
            report.items,
            vec![AgentExecutionItemStatus::ControllerUnknown]
        );
        assert_eq!(
            store.get_run(OWNER, RUN_ID).unwrap().unwrap().state,
            RunState::Running
        );
        assert_eq!(spool.read(RUN_ID, WORKER_ID).unwrap(), input);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let status = format!("{report:?}");
        for body in [
            "Return the test answer.",
            "unstaged answer",
            "test-only-credential",
        ] {
            assert!(!status.contains(body));
        }

        // Durable recovery consumes its tombstone even if the owner-private
        // spool entry was concurrently replaced. The mismatch must stay on
        // disk, but the one-shot in-memory cleanup slot cannot leak forever.
        let replacement = NativeAgentInput::fresh(
            OWNER,
            RUN_ID,
            WORKER_ID,
            "replacement body",
            replacement_policy,
        )
        .unwrap();
        std::fs::write(
            only_input_path(&root.path().join("native-input")),
            serde_json::to_vec(&replacement).unwrap(),
        )
        .unwrap();
        let controller = store
            .get_run(OWNER, RUN_ID)
            .unwrap()
            .unwrap()
            .controller
            .unwrap();
        operation.confirmed_gone(&controller);
        assert!(operation.pending_cleanup.lock().unwrap().is_empty());
        assert_eq!(spool.read(RUN_ID, WORKER_ID).unwrap(), replacement);
    }
}
