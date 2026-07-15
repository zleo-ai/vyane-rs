//! Production Linux Process AgentRun execution and resident assembly.
//!
//! Prompt bodies live only in the private one-shot spool. Durable AgentRun
//! truth contains hashes and controller references; subprocess identity lives
//! only in the exact process sidecar.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest as _, Sha256};
use vyane_agent::{
    ActiveExecutionPermit, AgentStore, ControllerKind, ControllerRef, ExecutionPermitSnapshot,
    RunFailureCode,
};
use vyane_core::{
    AttemptOutcome, ErrorKind, HarnessLifecycleEvent, HarnessLifecycleReporter,
    HarnessSpawnAuthority, RunStatus, Sandbox, VyaneError,
};
use vyane_message::{
    EndpointKind, EndpointRef, IdempotencyKey, MAX_BODY_BYTES, MessageDirection, NewDelivery,
    NewMessage,
};
use vyane_service::{
    AgentExecutionContext, AgentExecutionIdentity, AgentExecutionSettlement, AgentExecutorOutcome,
    AgentRunExecutor, DispatchParams, MESSAGE_COMPLETION_PRODUCER, MessageComponents,
    OwnerScopedService,
};

use crate::agent_process::ProcessControllerStore;
use crate::agent_spool::{AgentInputSpool, AgentSpoolInput, AgentSpoolSandbox};
use crate::task::store::TargetSnapshot;

const COMPLETION_DOMAIN: &[u8] = b"vyane.process-agent-completion.v1\0";

pub(crate) struct ProcessAgentRunExecutor {
    owner: Arc<str>,
    store: Arc<dyn AgentStore>,
    service: OwnerScopedService,
    spool: AgentInputSpool,
    sidecars: ProcessControllerStore,
    messages: MessageComponents,
}

impl ProcessAgentRunExecutor {
    pub(crate) fn new(
        owner: impl Into<Arc<str>>,
        store: Arc<dyn AgentStore>,
        service: OwnerScopedService,
        spool: AgentInputSpool,
        sidecars: ProcessControllerStore,
        messages: MessageComponents,
    ) -> Self {
        Self {
            owner: owner.into(),
            store,
            service,
            spool,
            sidecars,
            messages,
        }
    }

    fn exact_snapshot(
        owner: &str,
        permit: &ActiveExecutionPermit,
        input: &AgentSpoolInput,
        snapshot: &ExecutionPermitSnapshot,
    ) -> bool {
        snapshot.owner() == owner
            && snapshot.run_id() == input.run_id
            && snapshot.worker_id() == input.worker_id
            && snapshot.generation() == permit.generation()
            && snapshot.lease_owner() == permit.lease_owner()
            && snapshot.target_key() == input.policy.target
            && snapshot.prompt_digest() == input.prompt_sha256
            && snapshot.policy_digest() == input.policy_sha256
            && permit.owner() == owner
            && permit.run_id() == input.run_id
            && permit.worker_id() == input.worker_id
            && permit.policy_digest() == input.policy_sha256
    }

    fn validate_permit(
        store: &Arc<dyn AgentStore>,
        owner: &str,
        permit: &ActiveExecutionPermit,
        input: &AgentSpoolInput,
    ) -> bool {
        store
            .validate_execution_permit(owner, permit, &input.policy_sha256)
            .is_ok_and(|snapshot| Self::exact_snapshot(owner, permit, input, &snapshot))
    }

    fn remove_failed_input(&self, input: &AgentSpoolInput) -> bool {
        self.spool.remove(&input.run_id, &input.worker_id).is_ok()
    }
}

impl std::fmt::Debug for ProcessAgentRunExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessAgentRunExecutor")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AgentRunExecutor for ProcessAgentRunExecutor {
    fn kind(&self) -> ControllerKind {
        ControllerKind::Process
    }

    fn admit_controller(
        &self,
        identity: &AgentExecutionIdentity,
        controller: &ControllerRef,
    ) -> bool {
        let controller_id = controller.id.strip_prefix("vyane-exec-v1:");
        controller.kind == ControllerKind::Process
            && controller
                .fingerprint
                .as_ref()
                .is_some_and(|value| value.len() == 64 && value.bytes().all(is_lower_hex))
            && controller_id
                .is_some_and(|value| value.len() == 64 && value.bytes().all(is_lower_hex))
            && !identity.run_id().is_empty()
            && !identity.worker_id().is_empty()
            && !identity.target_key().is_empty()
    }

    fn reserve_controller(
        &self,
        identity: &AgentExecutionIdentity,
        controller: &ControllerRef,
    ) -> bool {
        self.sidecars
            .reserve(
                controller,
                identity.run_id(),
                identity.worker_id(),
                identity.generation(),
            )
            .is_ok()
    }

    fn confirmed_controller_gone(&self, controller: &ControllerRef) {
        let _ = self.sidecars.remove(controller);
    }

    async fn execute(
        &self,
        context: AgentExecutionContext,
        identity: AgentExecutionIdentity,
        permit: ActiveExecutionPermit,
    ) -> AgentExecutorOutcome {
        let input = match self.spool.read(identity.run_id(), identity.worker_id()) {
            Ok(input) => input,
            Err(_) => return AgentExecutorOutcome::Unknown,
        };
        if input.owner.as_str() != self.owner.as_ref()
            || input.run_id != identity.run_id()
            || input.worker_id != identity.worker_id()
            || input.policy.target != identity.target_key()
            || input.policy.config.is_some()
            || !Self::validate_permit(&self.store, &self.owner, &permit, &input)
        {
            return AgentExecutorOutcome::Unknown;
        }

        let remaining = context
            .deadline()
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining <= Duration::from_secs(1) {
            return if self.remove_failed_input(&input) {
                AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::TimedOut)
            } else {
                AgentExecutorOutcome::Unknown
            };
        }
        let timeout_secs = remaining.as_secs().saturating_sub(1).max(1);
        let params = DispatchParams {
            task: input.prompt.clone(),
            target: input.policy.target.clone(),
            workdir: input.policy.workdir.clone(),
            sandbox: match input.policy.sandbox {
                AgentSpoolSandbox::ReadOnly => Sandbox::ReadOnly,
                AgentSpoolSandbox::Write => Sandbox::Write,
                AgentSpoolSandbox::Full => Sandbox::Full,
            },
            session: None,
            system: input.policy.system.clone(),
            timeout_secs: Some(timeout_secs),
            labels: input.policy.labels.clone(),
        };
        let prepared = match self.service.prepare_harness_dispatch(params) {
            Ok(prepared) => prepared,
            Err(_) => {
                return if self.remove_failed_input(&input) {
                    AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                        code: RunFailureCode::PolicyDenied,
                    })
                } else {
                    AgentExecutorOutcome::Unknown
                };
            }
        };
        let actual_targets = prepared
            .resolved_chain()
            .iter()
            .map(|bound| TargetSnapshot::from_bound(bound, &self.service.config().config))
            .collect::<anyhow::Result<Vec<_>>>();
        if !actual_targets.is_ok_and(|actual| actual == input.policy.target_snapshot)
            || prepared.capability_snapshot() != &input.policy.capability_plan
        {
            return if self.remove_failed_input(&input) {
                AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                    code: RunFailureCode::PolicyDenied,
                })
            } else {
                AgentExecutorOutcome::Unknown
            };
        }

        let lifecycle = Arc::new(ProcessLifecycle::new(
            self.sidecars.clone(),
            context.controller().clone(),
        ));
        let lifecycle_for_reporter = Arc::clone(&lifecycle);
        let reporter =
            HarnessLifecycleReporter::new(move |event| lifecycle_for_reporter.report(event));
        let permit = Arc::new(permit);
        let store = Arc::clone(&self.store);
        let owner = Arc::clone(&self.owner);
        let expected = Arc::new(input.clone());
        let authority_permit = Arc::clone(&permit);
        let authority_expected = Arc::clone(&expected);
        let authority = HarnessSpawnAuthority::new(move || {
            Self::validate_permit(&store, &owner, &authority_permit, &authority_expected)
        });
        let dispatched = self
            .service
            .execute_prepared_harness_authorized(
                prepared,
                authority,
                reporter,
                context.cancellation().clone(),
            )
            .await;
        drop(expected);

        let permit = match Arc::try_unwrap(permit) {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!("Process AgentRun retained execution authority after dispatch");
                return AgentExecutorOutcome::Unknown;
            }
        };
        let state = lifecycle.observation();
        let outcome = match dispatched {
            Err(error) if state.quiesced_or_never_started() => AgentExecutionSettlement::Failed {
                code: if error
                    .downcast_ref::<VyaneError>()
                    .is_some_and(|error| error.kind == ErrorKind::SpawnFailed)
                {
                    RunFailureCode::SpawnFailed
                } else {
                    RunFailureCode::DispatchFailed
                },
            },
            Err(_) => {
                tracing::warn!(?state, "Process AgentRun dispatch ended without quiescence");
                return AgentExecutorOutcome::Unknown;
            }
            Ok(outcome) => match outcome.record.status {
                RunStatus::Success if state.quiesced_after_start() => {
                    let Some(output) = outcome.output else {
                        return if self.remove_failed_input(&input) {
                            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                                code: RunFailureCode::Internal,
                            })
                        } else {
                            AgentExecutorOutcome::Unknown
                        };
                    };
                    if output.len() > MAX_BODY_BYTES || output.contains('\0') {
                        return if self.remove_failed_input(&input) {
                            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                                code: RunFailureCode::Internal,
                            })
                        } else {
                            AgentExecutorOutcome::Unknown
                        };
                    }
                    let key = completion_key(&input.run_id, permit.generation());
                    let message = completion_message(&input, &key, output);
                    let staged = self
                        .messages
                        .prepare_and_stage_completion(
                            Arc::clone(&self.store),
                            permit,
                            key.clone(),
                            message,
                        )
                        .await;
                    let Ok(staged) = staged else {
                        tracing::warn!(
                            ?staged,
                            "Process AgentRun completion staging was unavailable"
                        );
                        return AgentExecutorOutcome::Unknown;
                    };
                    let _ = self.spool.remove(&input.run_id, &input.worker_id);
                    return AgentExecutorOutcome::Quiesced(
                        AgentExecutionSettlement::CompletionStaged(staged),
                    );
                }
                RunStatus::Success => {
                    tracing::warn!(?state, "successful Process AgentRun lacked stop proof");
                    return AgentExecutorOutcome::Unknown;
                }
                RunStatus::Timeout if state.quiesced_or_never_started() => {
                    AgentExecutionSettlement::TimedOut
                }
                RunStatus::Error if state.quiesced_or_never_started() => {
                    let spawn_failed = outcome.record.attempts.last().is_some_and(|attempt| {
                        matches!(
                            attempt.outcome,
                            AttemptOutcome::Err {
                                kind: ErrorKind::SpawnFailed,
                                ..
                            }
                        )
                    });
                    AgentExecutionSettlement::Failed {
                        code: if spawn_failed {
                            RunFailureCode::SpawnFailed
                        } else {
                            RunFailureCode::DispatchFailed
                        },
                    }
                }
                RunStatus::Cancelled | RunStatus::Timeout | RunStatus::Error => {
                    return AgentExecutorOutcome::Unknown;
                }
            },
        };
        if self.remove_failed_input(&input) {
            AgentExecutorOutcome::Quiesced(outcome)
        } else {
            AgentExecutorOutcome::Unknown
        }
    }
}

const fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

#[derive(Debug, Clone, Copy)]
enum LifecycleObservation {
    NeverStarted,
    Running,
    Stopped { cycles: u32 },
    Uncertain,
}

impl LifecycleObservation {
    fn quiesced_or_never_started(self) -> bool {
        matches!(self, Self::NeverStarted | Self::Stopped { .. })
    }

    fn quiesced_after_start(self) -> bool {
        matches!(self, Self::Stopped { cycles } if cycles > 0)
    }
}

struct ProcessLifecycle {
    sidecars: ProcessControllerStore,
    controller: ControllerRef,
    state: Mutex<LifecycleState>,
}

#[derive(Debug, Clone, Copy)]
enum LifecycleState {
    NeverStarted,
    Running { pid: u32, pgid: i32, cycles: u32 },
    Stopped { cycles: u32 },
    Uncertain,
}

impl ProcessLifecycle {
    fn new(sidecars: ProcessControllerStore, controller: ControllerRef) -> Self {
        Self {
            sidecars,
            controller,
            state: Mutex::new(LifecycleState::NeverStarted),
        }
    }

    fn report(&self, event: HarnessLifecycleEvent) -> vyane_core::Result<()> {
        let mut state = self.state.lock().map_err(|_| lifecycle_error())?;
        match event {
            HarnessLifecycleEvent::Started { pid, pgid } => {
                let cycles = match *state {
                    LifecycleState::NeverStarted => 0,
                    LifecycleState::Stopped { cycles } => cycles,
                    LifecycleState::Running { .. } | LifecycleState::Uncertain => {
                        *state = LifecycleState::Uncertain;
                        return Err(lifecycle_error());
                    }
                };
                if self
                    .sidecars
                    .record_started(&self.controller, pid, pgid, Utc::now())
                    .is_err()
                {
                    *state = LifecycleState::Uncertain;
                    return Err(lifecycle_error());
                }
                *state = LifecycleState::Running { pid, pgid, cycles };
                Ok(())
            }
            HarnessLifecycleEvent::Stopped {
                pid,
                pgid,
                group_empty,
            } => {
                let LifecycleState::Running {
                    pid: expected_pid,
                    pgid: expected_pgid,
                    cycles,
                } = *state
                else {
                    *state = LifecycleState::Uncertain;
                    return Err(lifecycle_error());
                };
                if pid != expected_pid || pgid != expected_pgid || !group_empty {
                    *state = LifecycleState::Uncertain;
                    return Err(lifecycle_error());
                }
                if self
                    .sidecars
                    .record_stopped(&self.controller, pid, pgid, true)
                    .is_err()
                {
                    *state = LifecycleState::Uncertain;
                    return Err(lifecycle_error());
                }
                *state = LifecycleState::Stopped {
                    cycles: cycles.saturating_add(1),
                };
                Ok(())
            }
        }
    }

    fn observation(&self) -> LifecycleObservation {
        match self.state.lock().map(|state| *state) {
            Ok(LifecycleState::NeverStarted) => LifecycleObservation::NeverStarted,
            Ok(LifecycleState::Running { .. }) => LifecycleObservation::Running,
            Ok(LifecycleState::Stopped { cycles }) => LifecycleObservation::Stopped { cycles },
            Ok(LifecycleState::Uncertain) | Err(_) => LifecycleObservation::Uncertain,
        }
    }
}

fn lifecycle_error() -> VyaneError {
    VyaneError::new(
        ErrorKind::Io,
        "AgentRun process lifecycle evidence is unavailable",
    )
}

fn completion_key(run_id: &str, generation: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(COMPLETION_DOMAIN);
    digest.update((run_id.len() as u64).to_be_bytes());
    digest.update(run_id.as_bytes());
    digest.update(generation.to_be_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn completion_message(input: &AgentSpoolInput, key: &str, output: String) -> NewMessage {
    NewMessage {
        conversation_id: format!("agent-run-{}", input.run_id),
        session_id: None,
        direction: MessageDirection::Internal,
        kind: "agent_run_completion".into(),
        sender: EndpointRef {
            kind: EndpointKind::Agent,
            id: "resident-process-agent".into(),
        },
        body: output,
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
