//! Authenticated loopback AgentRun submission and control for the Linux host.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use uuid::Uuid;
use vyane_agent::{
    AgentRunRecord, AgentStore, CancelOutcome, CancelRequest, ControllerKind, ExecutionBackend,
    NewAgentRun, NewWorker, RunCompletionStatus, RunFailureCode, RunMode, RunState,
    SqliteAgentStore,
};
use vyane_core::{CancellationToken, PinnedWorkdir, Sandbox};
use vyane_service::{
    AgentControllerAdapter, AgentExecutionOptions, AgentRecoveryOptions, AgentRunExecutor,
    AgentRunRecoveryDriver, AgentSupervisorOptions, InProcessAgentComponents, MessageComponents,
    OwnerContext, ResidentAgentExecutionLane, ResidentAgentHost, ResidentAgentHostBackend,
    VyaneService,
};

use crate::agent_host::ProcessAgentRunExecutor;
use crate::agent_process::{
    BoundProcessController, ProcessAgentControllerAdapter, ProcessControllerStore,
};
use crate::agent_spool::{
    AgentInputSpool, AgentSpoolCreate, AgentSpoolInput, AgentSpoolPolicy, AgentSpoolSandbox,
};
use crate::daemon::DaemonHttpState;
use crate::native_agent::{
    FreshNativeAgentOperation, NativeSubmissionDetails, native_input_for_submission,
};
use crate::native_agent_spool::{NativeAgentInput, NativeAgentInputSpool, NativeAgentSpoolCreate};
use crate::task::LOCAL_TASK_OWNER;
use crate::task::store::TargetSnapshot;

const INPUT_DIR: &str = "agent-inputs";
const NATIVE_INPUT_DIR: &str = "native-agent-inputs";
const CONTROLLER_DIR: &str = "agent-controllers";
const DEFAULT_TIMEOUT_SECONDS: u64 = 10 * 60;
const CANCEL_LEASE_SECONDS: u64 = 30;
const CANCEL_CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const WORKER_DOMAIN: &[u8] = b"vyane.daemon-agent.worker.v1\0";
const CANCEL_DOMAIN: &[u8] = b"vyane.daemon-agent.cancel.v1\0";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentRunSubmitRequest {
    pub(crate) run_id: Uuid,
    pub(crate) task: String,
    pub(crate) target: String,
    #[serde(default)]
    pub(crate) sandbox: AgentSpoolSandbox,
    #[serde(default)]
    pub(crate) workdir: Option<PathBuf>,
    #[serde(default)]
    pub(crate) system: Option<String>,
    #[serde(default)]
    pub(crate) timeout_seconds: Option<u64>,
    #[serde(default)]
    pub(crate) labels: Vec<String>,
    #[serde(default)]
    pub(crate) execution_backend: AgentExecutionBackend,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentExecutionBackend {
    #[default]
    CliHarnessProcess,
    NativeInProcess,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AgentRunView {
    pub(crate) run_id: String,
    pub(crate) worker_id: String,
    pub(crate) state: RunState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_code: Option<RunFailureCode>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completion_status: Option<RunCompletionStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct AgentRunOutputView {
    run_id: String,
    output: String,
}

#[derive(Clone)]
pub(crate) struct DaemonAgentHost {
    service: Arc<VyaneService>,
    store: Arc<dyn AgentStore>,
    spool: AgentInputSpool,
    native_spool: NativeAgentInputSpool,
    messages: MessageComponents,
    controller: Arc<ProcessAgentControllerAdapter>,
    submissions: Arc<SubmissionGate>,
    cancel_gate: Arc<Mutex<()>>,
    cancel_owner: Arc<str>,
}

#[derive(Debug)]
struct SubmissionGate {
    accepting: AtomicBool,
    in_flight: AtomicUsize,
    drained: Notify,
}

impl SubmissionGate {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            in_flight: AtomicUsize::new(0),
            drained: Notify::new(),
        }
    }

    fn try_enter(self: &Arc<Self>) -> Option<SubmissionPermit> {
        if !self.accepting.load(Ordering::SeqCst) {
            return None;
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.accepting.load(Ordering::SeqCst) {
            Some(SubmissionPermit {
                gate: Arc::clone(self),
            })
        } else {
            self.leave();
            None
        }
    }

    fn close(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    async fn drain(&self) {
        loop {
            let notified = self.drained.notified();
            if self.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn leave(&self) {
        let previous = self.in_flight.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "submission permit count underflow");
        if previous == 1 {
            self.drained.notify_waiters();
        }
    }
}

#[derive(Debug)]
struct SubmissionPermit {
    gate: Arc<SubmissionGate>,
}

impl Drop for SubmissionPermit {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

impl std::fmt::Debug for DaemonAgentHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonAgentHost")
            .finish_non_exhaustive()
    }
}

impl DaemonAgentHost {
    pub(crate) async fn open(
        service: Arc<VyaneService>,
        instance_id: &str,
    ) -> Result<(Self, ResidentAgentHost)> {
        let instance_key = opaque_digest(WORKER_DOMAIN, instance_id);
        let instance_key = &instance_key[..32];
        let paths = service.storage_paths().clone();
        let database = paths.agent_metadata_db_path();
        let store: Arc<dyn AgentStore> = Arc::new(
            tokio::task::spawn_blocking(move || SqliteAgentStore::open(database))
                .await
                .context("join AgentRun store opener")??,
        );
        let spool = AgentInputSpool::open(paths.data_dir.join(INPUT_DIR), LOCAL_TASK_OWNER)
            .context("open AgentRun input spool")?;
        let sidecars =
            ProcessControllerStore::open(paths.data_dir.join(CONTROLLER_DIR), LOCAL_TASK_OWNER)
                .context("open AgentRun process controllers")?;
        let messages = MessageComponents::open(&paths, LOCAL_TASK_OWNER)
            .context("open AgentRun completion messages")?;
        let scoped = service.scope(OwnerContext::single_user_local());
        let process_executor: Arc<dyn AgentRunExecutor> = Arc::new(ProcessAgentRunExecutor::new(
            Arc::<str>::from(LOCAL_TASK_OWNER),
            Arc::clone(&store),
            scoped,
            spool.clone(),
            sidecars.clone(),
            messages.clone(),
        ));
        let controller = Arc::new(ProcessAgentControllerAdapter::new(sidecars.clone()));
        let process_adapter: Arc<dyn AgentControllerAdapter> = controller.clone();
        let native_spool =
            NativeAgentInputSpool::open(paths.data_dir.join(NATIVE_INPUT_DIR), LOCAL_TASK_OWNER)
                .context("open native AgentRun input spool")?;
        let native_components = InProcessAgentComponents::new_with_completion_sinks(
            LOCAL_TASK_OWNER,
            Arc::clone(&store),
            Arc::new(FreshNativeAgentOperation::new(
                LOCAL_TASK_OWNER,
                service.scope(OwnerContext::single_user_local()),
                native_spool.clone(),
                messages.clone(),
            )),
            Vec::new(),
        )
        .context("construct native AgentRun lane")?;
        let native_backend = native_components.into_resident_backend();
        let (native_owner, native_store, native_executor, native_adapters, _) =
            native_backend.into_parts();
        if native_owner != LOCAL_TASK_OWNER || !Arc::ptr_eq(&native_store, &store) {
            return Err(anyhow::anyhow!("native AgentRun lane owner/store mismatch"));
        }
        let completion_sinks = vec![messages.completion_sink()];
        let mut adapters = vec![process_adapter];
        adapters.extend(native_adapters);
        let all_completion_sinks = completion_sinks.clone();
        AgentRunRecoveryDriver::new_with_completion_sinks(
            LOCAL_TASK_OWNER,
            Arc::clone(&store),
            format!("agent-startup-{instance_key}"),
            AgentRecoveryOptions::default(),
            adapters.clone(),
            all_completion_sinks.clone(),
        )
        .context("construct startup AgentRun recovery")?
        .recover_once(CancellationToken::new())
        .await
        .context("run startup AgentRun recovery")?;
        cleanup_terminal_process_sidecars(&store, &sidecars, &controller)
            .await
            .context("clean terminal AgentRun process controllers")?;
        let supervisor = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                LOCAL_TASK_OWNER,
                Arc::clone(&store),
                adapters,
                all_completion_sinks,
            ),
            vec![
                ResidentAgentExecutionLane::new(
                    process_executor,
                    format!("agent-exec-process-{instance_key}"),
                    AgentExecutionOptions::default(),
                ),
                ResidentAgentExecutionLane::new(
                    native_executor,
                    format!("agent-exec-native-{instance_key}"),
                    AgentExecutionOptions::default(),
                ),
            ],
            format!("agent-recovery-{instance_key}"),
            AgentRecoveryOptions::default(),
            AgentSupervisorOptions::default(),
        )
        .context("construct resident AgentRun supervisor")?;
        Ok((
            Self {
                service,
                store,
                spool,
                native_spool,
                messages,
                controller,
                submissions: Arc::new(SubmissionGate::new()),
                cancel_gate: Arc::new(Mutex::new(())),
                cancel_owner: Arc::from(format!("agent-cancel-{instance_key}")),
            },
            supervisor,
        ))
    }

    pub(crate) fn begin_shutdown(&self) {
        self.submissions.close();
    }

    pub(crate) async fn drain_submissions(&self) {
        self.submissions.drain().await;
    }

    async fn submit(&self, request: AgentRunSubmitRequest) -> Result<AgentRunView, AgentApiError> {
        let permit = self
            .submissions
            .try_enter()
            .ok_or_else(AgentApiError::unavailable)?;
        // Run the admitted initializer in its own task. If an HTTP connection
        // is force-closed during graceful shutdown, dropping the handler must
        // not detach a still-running blocking database write from the gate.
        let host = self.clone();
        tokio::spawn(async move { host.submit_admitted(request, permit).await })
            .await
            .map_err(|_| AgentApiError::unavailable())?
    }

    async fn submit_admitted(
        &self,
        request: AgentRunSubmitRequest,
        _permit: SubmissionPermit,
    ) -> Result<AgentRunView, AgentApiError> {
        if request.run_id.get_version_num() != 7 {
            return Err(AgentApiError::bad_request());
        }
        let timeout_seconds = request.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS);
        if matches!(
            request.execution_backend,
            AgentExecutionBackend::NativeInProcess
        ) {
            return self.submit_native_admitted(request, timeout_seconds).await;
        }
        let sandbox = match request.sandbox {
            AgentSpoolSandbox::ReadOnly => Sandbox::ReadOnly,
            AgentSpoolSandbox::Write => Sandbox::Write,
            AgentSpoolSandbox::Full => Sandbox::Full,
        };
        let requested_workdir = freeze_requested_workdir(sandbox, request.workdir)
            .map_err(|_| AgentApiError::bad_request())?;
        let scoped = self.service.scope(OwnerContext::single_user_local());
        let prepared = scoped
            .prepare_harness_dispatch(vyane_service::DispatchParams {
                task: request.task.clone(),
                target: request.target.clone(),
                workdir: requested_workdir.clone(),
                sandbox,
                session: None,
                system: request.system.clone(),
                timeout_secs: Some(timeout_seconds),
                labels: request.labels.clone(),
            })
            .map_err(|_| AgentApiError::bad_request())?;
        let target_snapshot = prepared
            .resolved_chain()
            .iter()
            .map(|target| TargetSnapshot::from_bound(target, &scoped.config().config))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|_| AgentApiError::bad_request())?;
        let frozen_workdir = match request.sandbox {
            AgentSpoolSandbox::ReadOnly => requested_workdir,
            AgentSpoolSandbox::Write | AgentSpoolSandbox::Full => {
                prepared.capability_snapshot().canonical_workdir.clone()
            }
        };
        let policy = AgentSpoolPolicy {
            target: request.target,
            sandbox: request.sandbox,
            workdir: frozen_workdir,
            system: request.system,
            timeout_seconds: Some(timeout_seconds),
            labels: request.labels,
            config: None,
            target_snapshot,
            capability_plan: prepared.capability_snapshot().clone(),
        };
        drop(prepared);
        let run_id = request.run_id.to_string();
        let worker_id = worker_id(&run_id);
        let input = AgentSpoolInput::fresh(
            LOCAL_TASK_OWNER,
            run_id.clone(),
            worker_id.clone(),
            request.task,
            policy,
        )
        .map_err(|_| AgentApiError::bad_request())?;
        let spool_create = self
            .spool
            .create(&input)
            .map_err(|_| AgentApiError::unavailable())?;
        let now = Utc::now();
        let worker = NewWorker {
            id: worker_id.clone(),
            logical_session_id: None,
        };
        let run = NewAgentRun {
            id: run_id.clone(),
            worker_id,
            task_id: None,
            trace_id: None,
            parent_run_id: None,
            execution_backend: ExecutionBackend::CliHarnessProcess,
            mode: RunMode::Autonomous,
            target_key: input.policy.target.clone(),
            prompt_digest: input.prompt_sha256.clone(),
            policy_digest: input.policy_sha256.clone(),
            available_at: now,
            timeout_seconds,
            max_resume_attempts: 0,
        };
        let store = Arc::clone(&self.store);
        let created =
            tokio::task::spawn_blocking(move || store.create_root(LOCAL_TASK_OWNER, &worker, &run))
                .await
                .map_err(|_| AgentApiError::unavailable())?;
        match created {
            Ok((_, record)) => self.view(record).await,
            Err(_) => {
                let store = Arc::clone(&self.store);
                let lookup = run_id.clone();
                let existing =
                    tokio::task::spawn_blocking(move || store.get_run(LOCAL_TASK_OWNER, &lookup))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .flatten();
                if let Some(existing) = existing.filter(|existing| exact_retry(existing, &input)) {
                    self.view(existing).await
                } else {
                    if spool_create == AgentSpoolCreate::Created {
                        let _ = self.spool.remove(&run_id, &input.worker_id);
                    }
                    Err(AgentApiError::conflict())
                }
            }
        }
    }

    async fn submit_native_admitted(
        &self,
        request: AgentRunSubmitRequest,
        timeout_seconds: u64,
    ) -> Result<AgentRunView, AgentApiError> {
        if request.sandbox != AgentSpoolSandbox::ReadOnly {
            return Err(AgentApiError::bad_request());
        }
        let workdir = request
            .workdir
            .ok_or_else(AgentApiError::bad_request)
            .and_then(|path| PinnedWorkdir::open(path).map_err(|_| AgentApiError::bad_request()))?;
        let scoped = self.service.scope(OwnerContext::single_user_local());
        let chain = scoped
            .resolve(&request.target)
            .map_err(|_| AgentApiError::bad_request())?;
        if chain.chain.len() != 1 {
            return Err(AgentApiError::bad_request());
        }
        let run_id = request.run_id.to_string();
        let worker_id = worker_id(&run_id);
        let input = native_input_for_submission(
            LOCAL_TASK_OWNER,
            &run_id,
            &worker_id,
            NativeSubmissionDetails {
                prompt: request.task,
                selector: &request.target,
                bound: &chain.chain[0],
                workdir: &workdir,
                system: request.system,
                timeout_seconds,
            },
        )
        .map_err(|_| AgentApiError::bad_request())?;
        let spool_create = self
            .native_spool
            .create(&input)
            .map_err(|_| AgentApiError::unavailable())?;
        let run = NewAgentRun {
            id: run_id.clone(),
            worker_id: worker_id.clone(),
            task_id: None,
            trace_id: None,
            parent_run_id: None,
            execution_backend: ExecutionBackend::NativeInProcess,
            mode: RunMode::Autonomous,
            target_key: input.policy.target_selector.clone(),
            prompt_digest: input.prompt_sha256.clone(),
            policy_digest: input.policy_sha256.clone(),
            available_at: Utc::now(),
            timeout_seconds,
            max_resume_attempts: 0,
        };
        let worker = NewWorker {
            id: worker_id,
            logical_session_id: None,
        };
        let store = Arc::clone(&self.store);
        let created =
            tokio::task::spawn_blocking(move || store.create_root(LOCAL_TASK_OWNER, &worker, &run))
                .await
                .map_err(|_| AgentApiError::unavailable())?;
        match created {
            Ok((_, record)) => self.view(record).await,
            Err(_) => {
                let store = Arc::clone(&self.store);
                let lookup = run_id.clone();
                let existing =
                    tokio::task::spawn_blocking(move || store.get_run(LOCAL_TASK_OWNER, &lookup))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .flatten();
                if let Some(existing) = existing
                    .filter(|existing| exact_native_retry(existing, &input, &self.native_spool))
                {
                    self.view(existing).await
                } else {
                    if spool_create == NativeAgentSpoolCreate::Created {
                        let _ = self.native_spool.remove_exact(&input);
                    }
                    Err(AgentApiError::conflict())
                }
            }
        }
    }

    async fn get(&self, run_id: String) -> Result<AgentRunView, AgentApiError> {
        let record = self.get_record(run_id).await?;
        self.view(record).await
    }

    async fn output(&self, run_id: String) -> Result<AgentRunOutputView, AgentApiError> {
        let record = self.get_record(run_id.clone()).await?;
        if record.state != RunState::Succeeded {
            return Err(AgentApiError::not_found());
        }
        let output = self
            .messages
            .published_completion_body(Arc::clone(&self.store), run_id.clone())
            .await
            .map_err(|_| AgentApiError::unavailable())?
            .ok_or_else(AgentApiError::not_found)?;
        Ok(AgentRunOutputView { run_id, output })
    }

    async fn cancel(&self, run_id: String) -> Result<AgentRunView, AgentApiError> {
        let permit = self
            .submissions
            .try_enter()
            .ok_or_else(AgentApiError::unavailable)?;
        // Keep cancellation alive if the HTTP response path disappears and
        // include it in shutdown draining. The per-host cancellation mutex
        // makes a retry wait instead of issuing an empty-ticket retry against
        // the first exact control operation.
        let host = self.clone();
        tokio::spawn(async move { host.cancel_admitted(run_id, permit).await })
            .await
            .map_err(|_| AgentApiError::unavailable())?
    }

    async fn cancel_admitted(
        &self,
        run_id: String,
        _permit: SubmissionPermit,
    ) -> Result<AgentRunView, AgentApiError> {
        let _cancel_guard = self.cancel_gate.lock().await;
        let record = self.get_record(run_id.clone()).await?;
        if record.state.is_terminal() {
            return self.view(record).await;
        }
        if record.state == RunState::Cancelling {
            // A detached request or startup recovery owns the durable control
            // ticket. Raw ticket authority is deliberately not recoverable
            // from the store, so observing the durable state is the safe
            // idempotent response.
            return self.view(record).await;
        }
        let request = CancelRequest {
            operation_id: cancel_operation_id(&run_id, &self.cancel_owner),
            lease_owner: self.cancel_owner.to_string(),
            lease_seconds: CANCEL_LEASE_SECONDS,
            retry_tickets: Vec::new(),
        };
        let store = Arc::clone(&self.store);
        let worker_id = record.worker_id.clone();
        let plan = tokio::task::spawn_blocking(move || {
            store.request_cancel_tree(LOCAL_TASK_OWNER, &worker_id, &request)
        })
        .await
        .map_err(|_| AgentApiError::unavailable())?
        .map_err(|_| AgentApiError::conflict())?;
        let mut root = None;
        for ticket in plan.tickets {
            let (outcome, confirmed_gone) = match ticket.controller.clone() {
                None => (CancelOutcome::Cancelled, None),
                Some(controller) if controller.kind == ControllerKind::Process => {
                    match self
                        .controller
                        .stop_exact(Instant::now() + CANCEL_CONTROL_TIMEOUT, controller.clone())
                        .await
                    {
                        vyane_service::ControllerRecoveryObservation::Gone => {
                            (CancelOutcome::Cancelled, Some(controller))
                        }
                        vyane_service::ControllerRecoveryObservation::StillPresent
                        | vyane_service::ControllerRecoveryObservation::Unavailable => {
                            (CancelOutcome::ControllerUnavailable, None)
                        }
                    }
                }
                Some(_) => (CancelOutcome::ControllerUnavailable, None),
            };
            let store = Arc::clone(&self.store);
            let settled = tokio::task::spawn_blocking(move || {
                store.settle_cancel(LOCAL_TASK_OWNER, &ticket, outcome)
            })
            .await
            .map_err(|_| AgentApiError::unavailable())?
            .map_err(|_| AgentApiError::unavailable())?;
            if let Some(controller) = confirmed_gone {
                let adapter = self.controller.clone();
                // Durable cancellation now owns terminal truth, so the exact
                // no-longer-live controller evidence can be retired.
                let _ =
                    tokio::task::spawn_blocking(move || adapter.confirmed_gone(&controller)).await;
            }
            if settled.id == run_id {
                root = Some(settled);
            }
        }
        let root = match root {
            Some(root) => root,
            None => self.get_record(run_id).await?,
        };
        self.view(root).await
    }

    async fn get_record(&self, run_id: String) -> Result<AgentRunRecord, AgentApiError> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.get_run(LOCAL_TASK_OWNER, &run_id))
            .await
            .map_err(|_| AgentApiError::unavailable())?
            .map_err(|_| AgentApiError::unavailable())?
            .ok_or_else(AgentApiError::not_found)
    }

    async fn view(&self, record: AgentRunRecord) -> Result<AgentRunView, AgentApiError> {
        // Terminal metadata and completion publication are self-contained.
        // Removing the bound prompt here repairs residue left by an exact
        // retry or an earlier interrupted cleanup whenever the run is next
        // observed, without sweeping unverifiable spool files.
        if record.state.is_terminal() {
            self.spool
                .remove(&record.id, &record.worker_id)
                .map_err(|_| AgentApiError::unavailable())?;
        }
        let store = Arc::clone(&self.store);
        let run_id = record.id.clone();
        let completion_status = tokio::task::spawn_blocking(move || {
            store
                .get_completion(LOCAL_TASK_OWNER, &run_id)
                .map(|record| record.map(|record| record.status))
        })
        .await
        .map_err(|_| AgentApiError::unavailable())?
        .map_err(|_| AgentApiError::unavailable())?;
        Ok(AgentRunView {
            run_id: record.id,
            worker_id: record.worker_id,
            state: record.state,
            failure_code: record.failure_code,
            created_at: record.created_at,
            updated_at: record.updated_at,
            completion_status,
        })
    }
}

async fn cleanup_terminal_process_sidecars(
    store: &Arc<dyn AgentStore>,
    sidecars: &ProcessControllerStore,
    controller: &Arc<ProcessAgentControllerAdapter>,
) -> Result<()> {
    let sidecars_for_scan = sidecars.clone();
    let bindings = tokio::task::spawn_blocking(move || sidecars_for_scan.bound_controllers())
        .await
        .context("join AgentRun controller scan")??;
    for binding in bindings {
        let store = Arc::clone(store);
        let run_id = binding.run_id.clone();
        let record = tokio::task::spawn_blocking(move || store.get_run(LOCAL_TASK_OWNER, &run_id))
            .await
            .context("join AgentRun controller lookup")??;
        if !record
            .as_ref()
            .is_some_and(|record| terminal_binding_matches(record, &binding))
        {
            continue;
        }
        let controller = Arc::clone(controller);
        tokio::task::spawn_blocking(move || {
            controller.remove_terminal_residue(&binding.controller);
        })
        .await
        .context("join AgentRun controller cleanup")?;
    }
    Ok(())
}

fn terminal_binding_matches(record: &AgentRunRecord, binding: &BoundProcessController) -> bool {
    record.state.is_terminal()
        && record.controller.is_none()
        && record.id == binding.run_id
        && record.worker_id == binding.worker_id
        && record.worker_generation == binding.worker_generation
}

pub(crate) fn routes() -> Router<DaemonHttpState> {
    Router::new()
        .route("/v1/agent-runs", post(submit_agent_run))
        .route("/v1/agent-runs/{id}", get(agent_run_status))
        .route("/v1/agent-runs/{id}/output", get(agent_run_output))
        .route("/v1/agent-runs/{id}/cancel", post(cancel_agent_run))
}

async fn submit_agent_run(
    State(state): State<DaemonHttpState>,
    Json(request): Json<AgentRunSubmitRequest>,
) -> Result<(StatusCode, Json<AgentRunView>), AgentApiError> {
    let view = state
        .agents
        .as_ref()
        .ok_or_else(AgentApiError::unavailable)?
        .submit(request)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(view)))
}

async fn agent_run_status(
    State(state): State<DaemonHttpState>,
    Path(id): Path<String>,
) -> Result<Json<AgentRunView>, AgentApiError> {
    Ok(Json(
        state
            .agents
            .as_ref()
            .ok_or_else(AgentApiError::unavailable)?
            .get(id)
            .await?,
    ))
}

async fn agent_run_output(
    State(state): State<DaemonHttpState>,
    Path(id): Path<String>,
) -> Result<Json<AgentRunOutputView>, AgentApiError> {
    Ok(Json(
        state
            .agents
            .as_ref()
            .ok_or_else(AgentApiError::unavailable)?
            .output(id)
            .await?,
    ))
}

async fn cancel_agent_run(
    State(state): State<DaemonHttpState>,
    Path(id): Path<String>,
) -> Result<Json<AgentRunView>, AgentApiError> {
    Ok(Json(
        state
            .agents
            .as_ref()
            .ok_or_else(AgentApiError::unavailable)?
            .cancel(id)
            .await?,
    ))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AgentApiError {
    status: StatusCode,
    message: &'static str,
}

impl AgentApiError {
    fn bad_request() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: "invalid AgentRun request",
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: "AgentRun not found",
        }
    }

    fn conflict() -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: "AgentRun request conflicts with durable state",
        }
    }

    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "AgentRun service unavailable",
        }
    }
}

impl IntoResponse for AgentApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}

fn exact_retry(record: &AgentRunRecord, input: &AgentSpoolInput) -> bool {
    exact_retry_backend(record.execution_backend)
        && record.owner == input.owner
        && record.id == input.run_id
        && record.worker_id == input.worker_id
        && record.parent_run_id.is_none()
        && record.target_key == input.policy.target
        && record.prompt_digest == input.prompt_sha256
        && record.policy_digest == input.policy_sha256
        && record.timeout_seconds == input.policy.timeout_seconds.unwrap_or_default()
        && record.max_resume_attempts == 0
}

fn exact_native_retry(
    record: &AgentRunRecord,
    input: &NativeAgentInput,
    spool: &NativeAgentInputSpool,
) -> bool {
    record.execution_backend == ExecutionBackend::NativeInProcess
        && record.owner == input.owner
        && record.id == input.run_id
        && record.worker_id == input.worker_id
        && record.parent_run_id.is_none()
        && record.target_key == input.policy.target_selector
        && record.prompt_digest == input.prompt_sha256
        && record.policy_digest == input.policy_sha256
        && record.timeout_seconds == input.policy.timeout_seconds
        && record.max_resume_attempts == 0
        && spool
            .read(&input.run_id, &input.worker_id)
            .is_ok_and(|stored| stored == *input)
}

const fn exact_retry_backend(backend: ExecutionBackend) -> bool {
    matches!(backend, ExecutionBackend::CliHarnessProcess)
}

fn worker_id(run_id: &str) -> String {
    format!("worker-{}", opaque_digest(WORKER_DOMAIN, run_id))
}

fn cancel_operation_id(run_id: &str, lease_owner: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(CANCEL_DOMAIN);
    for value in [run_id, lease_owner] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    format!("cancel-{}", hex_digest(digest.finalize()))
}

fn opaque_digest(domain: &[u8], value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
    hex_digest(digest.finalize())
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn freeze_requested_workdir(
    sandbox: Sandbox,
    workdir: Option<PathBuf>,
) -> std::io::Result<Option<PathBuf>> {
    match (sandbox, workdir) {
        (Sandbox::ReadOnly, Some(workdir)) => std::fs::canonicalize(workdir).map(Some),
        (_, workdir) => Ok(workdir),
    }
}

pub(crate) async fn run_supervisor(supervisor: ResidentAgentHost, cancel: CancellationToken) {
    let _ = supervisor.run(cancel).await;
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{
        SubmissionGate, cancel_operation_id, exact_retry_backend, freeze_requested_workdir,
    };
    use vyane_agent::ExecutionBackend;
    use vyane_core::Sandbox;

    #[tokio::test]
    async fn closed_submission_gate_rejects_new_work() {
        let gate = Arc::new(SubmissionGate::new());
        gate.close();

        assert!(gate.try_enter().is_none());
        gate.drain().await;
    }

    #[tokio::test]
    async fn submission_gate_drains_every_admitted_initializer() {
        let gate = Arc::new(SubmissionGate::new());
        let first = gate.try_enter().expect("first submission is admitted");
        let second = gate.try_enter().expect("second submission is admitted");
        gate.close();
        assert!(gate.try_enter().is_none());

        let draining = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.drain().await })
        };
        tokio::task::yield_now().await;
        assert!(!draining.is_finished());

        drop(first);
        tokio::task::yield_now().await;
        assert!(!draining.is_finished());

        drop(second);
        tokio::time::timeout(Duration::from_secs(1), draining)
            .await
            .expect("drain observes the final initializer")
            .expect("drain task completes");
    }

    #[tokio::test]
    async fn detached_initializer_keeps_submission_gate_busy() {
        let gate = Arc::new(SubmissionGate::new());
        let permit = gate.try_enter().expect("submission is admitted");
        let (release, admitted) = tokio::sync::oneshot::channel();
        let initializer = tokio::spawn(async move {
            let _permit = permit;
            let _ = admitted.await;
        });
        drop(initializer);
        gate.close();

        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.drain())
                .await
                .is_err()
        );
        let _ = release.send(());
        tokio::time::timeout(Duration::from_secs(1), gate.drain())
            .await
            .expect("detached initializer releases its permit");
    }

    #[test]
    fn read_only_workdir_is_frozen_to_its_canonical_path() {
        let directory = tempfile::tempdir().expect("create workdir fixture");
        let actual = directory.path().join("actual");
        let alias = directory.path().join("alias");
        std::fs::create_dir(&actual).expect("create canonical workdir");
        symlink(&actual, &alias).expect("create workdir alias");

        assert_eq!(
            freeze_requested_workdir(Sandbox::ReadOnly, Some(alias))
                .expect("freeze read-only workdir"),
            Some(actual)
        );
        assert_eq!(
            freeze_requested_workdir(Sandbox::ReadOnly, None).expect("preserve absent workdir"),
            None
        );
        let untouched = PathBuf::from("relative-write-workdir");
        assert_eq!(
            freeze_requested_workdir(Sandbox::Write, Some(untouched.clone()))
                .expect("defer write workdir pinning"),
            Some(untouched)
        );
    }

    #[test]
    fn cancel_operation_identity_is_framed_and_daemon_scoped() {
        assert_eq!(
            cancel_operation_id("run-a", "owner-a"),
            cancel_operation_id("run-a", "owner-a")
        );
        assert_ne!(
            cancel_operation_id("run-a", "owner-a"),
            cancel_operation_id("run-a", "owner-b")
        );
        assert_ne!(
            cancel_operation_id("ab", "c"),
            cancel_operation_id("a", "bc")
        );
    }

    #[test]
    fn exact_retry_accepts_only_the_process_backend_owned_by_this_host() {
        assert!(exact_retry_backend(ExecutionBackend::CliHarnessProcess));
        for backend in [
            ExecutionBackend::NativeInProcess,
            ExecutionBackend::Remote,
            ExecutionBackend::LegacyUnassigned,
        ] {
            assert!(!exact_retry_backend(backend));
        }
    }
}
