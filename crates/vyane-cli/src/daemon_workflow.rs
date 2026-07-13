//! Durable daemon ownership for workflow tasks.

use std::collections::BTreeMap;
use std::path::{Component, Path as FsPath, PathBuf};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;
use vyane_core::CancellationToken;
use vyane_service::VyaneService;
use vyane_task::{
    ControllerRef, FailureCode, Lease, NewTask, SqliteTaskStore, TaskKind, TaskOrigin, TaskQuery,
    TaskRecord, TaskSettlement, TaskState, TaskStore, TaskStoreError,
};
use vyane_workflow::{
    Workflow, WorkflowEngine, WorkflowError, WorkflowJournalSummary, WorkflowOutcome,
    WorkflowResult, WorkflowRunId, WorkflowRunStatus, WorkflowSourceBundle, read_journal,
    validate_workflow,
};

use crate::command::CliWorkflowResolver;
use crate::daemon::DaemonHttpState;
use crate::task::LOCAL_TASK_OWNER;
use crate::workflow_control::WorkflowHarnessControl;

const LEASE_DURATION_SECS: i64 = 30;
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const CANCEL_CONTROLLER_GRACE: Duration = Duration::from_secs(7);
const INITIALIZER_DRAIN_BUDGET: Duration = Duration::from_secs(11);
const BLOCKING_OPERATION_DRAIN_BUDGET: Duration = Duration::from_secs(6);
const METADATA_PHASE_BUDGET: Duration = Duration::from_secs(6);
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(12);
const CONTROLLER_CLEANUP_BUDGET: Duration = Duration::from_secs(6);
const ABORTED_WORKER_DRAIN_BUDGET: Duration = Duration::from_secs(5);
const SHUTDOWN_POLL: Duration = Duration::from_millis(20);
const API_ERROR_LIMIT: usize = 512;
const EXECUTION_CWD_MAX_BYTES: usize = 4_096;
const WORKFLOW_VARS_MAX_ENTRIES: usize = 128;
const WORKFLOW_VAR_KEY_MAX_BYTES: usize = 256;
const WORKFLOW_VAR_VALUE_MAX_BYTES: usize = 1024 * 1024;
pub(crate) const WORKFLOW_VARS_MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const SUBMISSION_DIGEST_DOMAIN: &[u8] = b"vyane.workflow.daemon-submission\0v1\0";

type WatcherKey = (String, u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowSubmitRequest {
    pub(crate) run_id: WorkflowRunId,
    pub(crate) execution_cwd: PathBuf,
    pub(crate) bundle: WorkflowSourceBundle,
    #[serde(default)]
    pub(crate) vars: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowTaskView {
    pub(crate) task: TaskRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) journal: Option<WorkflowJournalSummary>,
}

#[derive(Clone)]
struct LiveWorkflow {
    epoch: u64,
    cancel: CancellationToken,
    abort: tokio::task::AbortHandle,
    control: WorkflowHarnessControl,
}

#[derive(Debug, Clone)]
enum CompletionAction {
    Settle(TaskSettlement),
    Interrupt(FailureCode),
}

#[derive(Debug)]
enum SubmissionCreate {
    Created(TaskRecord),
    Existing(TaskRecord),
}

#[derive(Debug, Default)]
struct InitializerControl {
    abort: Mutex<Option<tokio::task::AbortHandle>>,
}

#[cfg(test)]
#[derive(Clone)]
struct AfterLiveInsertHook {
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

#[derive(Clone)]
pub(crate) struct DaemonWorkflowSupervisor {
    service: Arc<VyaneService>,
    resolver: Arc<CliWorkflowResolver>,
    store: Arc<SqliteTaskStore>,
    instance_id: Arc<str>,
    live: Arc<DashMap<String, LiveWorkflow>>,
    initializing_cancels: Arc<DashMap<String, CancellationToken>>,
    finished: Arc<Notify>,
    accepting: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
    initializers: Arc<AtomicUsize>,
    initializer_sequence: Arc<AtomicU64>,
    initializer_controls: Arc<DashMap<u64, Arc<InitializerControl>>>,
    initializer_finished: Arc<Notify>,
    blocking_operations: Arc<AtomicUsize>,
    blocking_operation_finished: Arc<Notify>,
    watchers: Arc<Mutex<BTreeMap<WatcherKey, tokio::task::JoinHandle<()>>>>,
    controller_cleanup_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    #[cfg(test)]
    after_live_insert_hook: Arc<Mutex<Option<AfterLiveInsertHook>>>,
}

impl DaemonWorkflowSupervisor {
    pub(crate) async fn open(service: Arc<VyaneService>, instance_id: String) -> Result<Self> {
        let database = service.storage_paths().task_metadata_db_path();
        let store = tokio::task::spawn_blocking(move || SqliteTaskStore::open(database))
            .await
            .context("join daemon task-store opener")??;
        let resolver = Arc::new(CliWorkflowResolver::new(service.config().clone()));
        Ok(Self {
            service,
            resolver,
            store: Arc::new(store),
            instance_id: Arc::from(instance_id),
            live: Arc::new(DashMap::new()),
            initializing_cancels: Arc::new(DashMap::new()),
            finished: Arc::new(Notify::new()),
            accepting: Arc::new(AtomicBool::new(true)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            initializers: Arc::new(AtomicUsize::new(0)),
            initializer_sequence: Arc::new(AtomicU64::new(0)),
            initializer_controls: Arc::new(DashMap::new()),
            initializer_finished: Arc::new(Notify::new()),
            blocking_operations: Arc::new(AtomicUsize::new(0)),
            blocking_operation_finished: Arc::new(Notify::new()),
            watchers: Arc::new(Mutex::new(BTreeMap::new())),
            controller_cleanup_tasks: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            after_live_insert_hook: Arc::new(Mutex::new(None)),
        })
    }

    fn matches_scope(record: &TaskRecord) -> bool {
        record.matches_scope(LOCAL_TASK_OWNER, TaskKind::Workflow, TaskOrigin::Daemon)
    }

    /// Close workflow admission before the HTTP server starts waiting for
    /// in-flight requests. This is synchronous and idempotent so signal
    /// handling can establish the shutdown gate without waiting on storage.
    pub(crate) fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.shutting_down.store(true, Ordering::Release);
        for token in self.initializing_cancels.iter() {
            token.cancel();
        }
    }

    async fn call<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&SqliteTaskStore) -> vyane_task::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        if self
            .blocking_operations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .is_err()
        {
            bail!("daemon workflow blocking-operation count overflow");
        }
        let permit = BlockingOperationPermit {
            count: Arc::clone(&self.blocking_operations),
            finished: Arc::clone(&self.blocking_operation_finished),
        };
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&store)
        })
        .await
        .context("join daemon task-store operation")?
        .map_err(anyhow::Error::from)
    }

    async fn idempotent_existing(
        &self,
        id: &str,
        submission_digest: &str,
    ) -> Result<Option<TaskRecord>> {
        match self.get(id).await? {
            None => Ok(None),
            Some(record)
                if Self::matches_scope(&record)
                    && record.task_digest == submission_digest
                    && record.target_key == "workflow" =>
            {
                Ok(Some(record))
            }
            Some(_) => Err(TaskStoreError::AlreadyExists { id: id.to_string() }.into()),
        }
    }

    async fn create_submission(&self, task: NewTask) -> Result<SubmissionCreate> {
        let id = task.id.clone();
        let digest = task.task_digest.clone();
        match self
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
        {
            Ok(created) => Ok(SubmissionCreate::Created(created)),
            Err(error)
                if error
                    .downcast_ref::<TaskStoreError>()
                    .is_some_and(|error| matches!(error, TaskStoreError::AlreadyExists { .. })) =>
            {
                match self.idempotent_existing(&id, &digest).await? {
                    Some(existing) => Ok(SubmissionCreate::Existing(existing)),
                    None => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn recover_interrupted(&self) -> Result<usize> {
        let mut cursor = None;
        let mut recovered = 0;
        loop {
            let query = TaskQuery {
                kinds: vec![TaskKind::Workflow],
                origins: vec![TaskOrigin::Daemon],
                states: vec![TaskState::Queued, TaskState::Running, TaskState::Cancelling],
                limit: 1_000,
                cursor,
            };
            let page = self
                .call(move |store| store.list(LOCAL_TASK_OWNER, &query))
                .await?;
            for record in page.items {
                match WorkflowRunId::from_str(&record.id) {
                    Ok(run_id) => {
                        let data_dir = self.service.storage_paths().data_dir.clone();
                        let opened = tokio::task::spawn_blocking(move || {
                            WorkflowHarnessControl::new(&run_id, &data_dir)
                        })
                        .await;
                        match opened {
                            Ok(Ok(control)) => match control.cleanup_all().await {
                                Ok(report) if !report.all_resolved() => tracing::warn!(
                                    task_id = %record.id,
                                    report = ?report,
                                    "workflow recovery left fail-closed controller entries"
                                ),
                                Ok(_) => {}
                                Err(error) => tracing::warn!(
                                    task_id = %record.id,
                                    error = %error,
                                    "workflow recovery controller cleanup failed; continuing interruption"
                                ),
                            },
                            Ok(Err(error)) => tracing::warn!(
                                task_id = %record.id,
                                error = %error,
                                "workflow recovery controller set could not be opened; continuing interruption"
                            ),
                            Err(error) => tracing::warn!(
                                task_id = %record.id,
                                error = %error,
                                "workflow recovery controller opener failed to join; continuing interruption"
                            ),
                        }
                    }
                    Err(error) => tracing::warn!(
                        task_id = %record.id,
                        error = %error,
                        "workflow recovery found a non-canonical task id; skipping controller path"
                    ),
                }
                if self.interrupt_abandoned(&record.id).await? {
                    recovered += 1;
                }
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        Ok(recovered)
    }

    pub(crate) async fn submit(&self, request: WorkflowSubmitRequest) -> Result<TaskRecord> {
        let permit = self.begin_initialization()?;
        let control = Arc::clone(&permit.control);
        let supervisor = self.clone();
        let initializer = tokio::spawn(async move {
            let _permit = permit;
            supervisor.initialize(request).await
        });
        *control
            .abort
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(initializer.abort_handle());
        initializer
            .await
            .context("join daemon workflow initializer")?
    }

    fn begin_initialization(&self) -> Result<InitializationPermit> {
        if !self.accepting.load(Ordering::Acquire) {
            bail!("daemon workflow admission is closed");
        }
        if self
            .initializers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .is_err()
        {
            bail!("daemon workflow initializer count overflow");
        }
        let id = match self.initializer_sequence.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| current.checked_add(1),
        ) {
            Ok(id) => id,
            Err(_) => {
                self.initializers.fetch_sub(1, Ordering::AcqRel);
                self.initializer_finished.notify_one();
                bail!("daemon workflow initializer sequence overflow");
            }
        };
        let control = Arc::new(InitializerControl::default());
        self.initializer_controls.insert(id, Arc::clone(&control));
        let permit = InitializationPermit {
            id,
            control,
            controls: Arc::clone(&self.initializer_controls),
            count: Arc::clone(&self.initializers),
            finished: Arc::clone(&self.initializer_finished),
        };
        if !self.accepting.load(Ordering::Acquire) {
            drop(permit);
            bail!("daemon workflow admission is closed");
        }
        Ok(permit)
    }

    async fn initialize(&self, request: WorkflowSubmitRequest) -> Result<TaskRecord> {
        let WorkflowSubmitRequest {
            run_id,
            execution_cwd,
            bundle,
            vars,
        } = request;
        // The wire bundle is already bounded. Keeping materialization inside
        // the tracked initializer avoids detaching untracked blocking work if
        // shutdown has to abort this task.
        let mut workflow = bundle.materialize()?;
        validate_workflow_vars(&vars)?;
        validate_execution_cwd_wire(&execution_cwd)?;
        let submission_digest = submission_digest(&workflow, &vars, &execution_cwd);

        // An authenticated retry is allowed to recover its prior id even if
        // target configuration has drifted since the first submission. Exact
        // scope + complete submission digest is required; nothing is replayed.
        if let Some(existing) = self
            .idempotent_existing(run_id.as_str(), &submission_digest)
            .await?
        {
            return Ok(existing);
        }

        validate_new_execution_cwd(&execution_cwd)?;
        rebase_workdirs(&mut workflow, &execution_cwd)?;
        validate_workflow(&workflow, &vars, self.resolver.as_ref())?;
        if !self.accepting.load(Ordering::Acquire) {
            bail!("daemon workflow admission is closed");
        }

        let task = NewTask {
            id: run_id.to_string(),
            kind: TaskKind::Workflow,
            origin: TaskOrigin::Daemon,
            task_digest: submission_digest,
            target_key: "workflow".into(),
            created_at: chrono::Utc::now(),
        };
        let created = match self.create_submission(task).await? {
            SubmissionCreate::Created(created) => created,
            SubmissionCreate::Existing(existing) => return Ok(existing),
        };
        let cancel = CancellationToken::new();
        self.initializing_cancels
            .insert(created.id.clone(), cancel.clone());
        let _initializing_cancel = InitializingCancelGuard {
            id: created.id.clone(),
            tokens: Arc::clone(&self.initializing_cancels),
        };
        if !self.accepting.load(Ordering::Acquire) {
            return match self.interrupt_abandoned(&created.id).await {
                Ok(_) => Err(anyhow::anyhow!(
                    "daemon workflow admission closed during initialization"
                )),
                Err(metadata_error) => Err(anyhow::anyhow!(
                    "daemon workflow admission closed during initialization; metadata interruption also failed: {metadata_error:#}"
                )),
            };
        }

        let control = match WorkflowHarnessControl::new(
            &run_id,
            &self.service.storage_paths().data_dir,
        ) {
            Ok(control) => control,
            Err(error) => {
                let metadata_result = self
                    .settle_record(
                        &created,
                        TaskSettlement::Failed {
                            code: FailureCode::Internal,
                            ledger_run_id: None,
                        },
                    )
                    .await;
                return match metadata_result {
                        Ok(()) => Err(error),
                        Err(metadata_error) => Err(error.context(format!(
                            "workflow controller initialization metadata settlement also failed: {metadata_error:#}"
                        ))),
                    };
            }
        };
        let now = chrono::Utc::now();
        let controller = ControllerRef::InProcess {
            instance_id: self.instance_id.to_string(),
        };
        let lease = Lease {
            owner: self.instance_id.to_string(),
            expires_at: now + chrono::Duration::seconds(LEASE_DURATION_SECS),
        };
        let task_id = created.id.clone();
        let created_revision = created.revision;
        let created_epoch = created.executor_epoch;
        let attached = match self
            .call(move |store| {
                store.attach_controller(
                    LOCAL_TASK_OWNER,
                    &task_id,
                    created_revision,
                    created_epoch,
                    controller,
                    Some(lease),
                    now,
                )
            })
            .await
        {
            Ok(attached) => attached,
            Err(error) => {
                if let Some(current) = self.get(&created.id).await? {
                    if Self::matches_scope(&current) && current.state.is_terminal() {
                        return Ok(current);
                    }
                    if Self::matches_scope(&current) {
                        let metadata_result = match current.state {
                            TaskState::Queued => {
                                self.settle_record(
                                    &current,
                                    TaskSettlement::Failed {
                                        code: FailureCode::Internal,
                                        ledger_run_id: None,
                                    },
                                )
                                .await
                            }
                            TaskState::Running | TaskState::Cancelling => self
                                .interrupt_record(&current, FailureCode::Internal)
                                .await
                                .map(|_| ()),
                            _ => Ok(()),
                        };
                        if let Err(metadata_error) = metadata_result {
                            return Err(error.context(format!(
                                "attach failure metadata cleanup also failed: {metadata_error:#}"
                            )));
                        }
                    }
                }
                return Err(error);
            }
        };

        let engine = WorkflowEngine::new(
            Arc::new(self.service.runtime().dispatcher.clone()),
            self.resolver.clone(),
            self.service.storage_paths().workflows_dir.clone(),
        )
        .with_harness_lifecycle_reporter(control.reporter());

        // The task row is already attached, but the workflow future does not
        // exist yet. Make the initial v1 journal durable before spawning it so
        // a crash can leave at most an interrupted, non-replayable run.
        if let Err(error) = engine.prepare_run_with_id(run_id.clone(), &workflow, vars) {
            let metadata_result = self
                .settle_current(
                    &attached.id,
                    attached.executor_epoch,
                    TaskSettlement::Failed {
                        code: FailureCode::Internal,
                        ledger_run_id: None,
                    },
                )
                .await;
            return match metadata_result {
                Ok(()) => Err(error.into()),
                Err(metadata_error) => Err(anyhow::anyhow!(
                    "initialize workflow journal: {error}; metadata settlement also failed: {metadata_error:#}"
                )),
            };
        }

        let worker_run_id = run_id.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                start_rx.await.map_err(|_| {
                    vyane_workflow::WorkflowError::validation(vec![
                        "daemon workflow start gate closed".into(),
                    ])
                })?;
                engine.run_prepared(worker_run_id, &workflow, cancel).await
            }
        });
        let abort = worker.abort_handle();
        let live = LiveWorkflow {
            epoch: attached.executor_epoch,
            cancel: cancel.clone(),
            abort: abort.clone(),
            control: control.clone(),
        };
        match self.live.entry(attached.id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(live);
            }
            Entry::Occupied(_) => {
                abort.abort();
                let _ = worker.await;
                let metadata_result = self
                    .interrupt_current(&attached.id, attached.executor_epoch, FailureCode::Internal)
                    .await;
                return match metadata_result {
                    Ok(()) => Err(anyhow::anyhow!(
                        "duplicate live workflow id {}",
                        attached.id
                    )),
                    Err(metadata_error) => Err(anyhow::anyhow!(
                        "duplicate live workflow id {}; metadata interruption also failed: {metadata_error:#}",
                        attached.id
                    )),
                };
            }
        }
        // From the instant the live generation becomes visible, one guard must
        // own its rollback. In particular, shutdown may abort this initializer
        // at the authoritative store read below. Moving this guard into the
        // watcher later transfers (rather than briefly drops) that ownership.
        let completion = LiveCompletionGuard {
            id: attached.id.clone(),
            epoch: attached.executor_epoch,
            live: Arc::clone(&self.live),
            watchers: Arc::clone(&self.watchers),
            finished: Arc::clone(&self.finished),
        };
        #[cfg(test)]
        self.pause_after_live_insert_for_test().await;

        // Cancellation can commit after attach but before the live token is
        // published. Re-read after insertion to close that gap. Any later
        // cancellation observes and signals this exact-epoch token directly.
        let authoritative = self.get(&attached.id).await;
        let current = match authoritative {
            Err(error) => {
                abort.abort();
                let _ = worker.await;
                self.live.remove_if(&attached.id, |_, live| {
                    live.epoch == attached.executor_epoch
                });
                let metadata_result = self
                    .interrupt_current(&attached.id, attached.executor_epoch, FailureCode::Internal)
                    .await;
                let context = match metadata_result {
                    Ok(()) => format!(
                        "re-read workflow {} after live-controller publication",
                        attached.id
                    ),
                    Err(metadata_error) => format!(
                        "re-read workflow {} after live-controller publication; metadata interruption also failed: {metadata_error:#}",
                        attached.id
                    ),
                };
                return Err(error.context(context));
            }
            Ok(Some(current)) if Self::matches_scope(&current) && current.state.is_terminal() => {
                abort.abort();
                let _ = worker.await;
                self.live.remove_if(&attached.id, |_, live| {
                    live.epoch == attached.executor_epoch
                });
                return Ok(current);
            }
            Ok(Some(current))
                if Self::matches_scope(&current)
                    && matches!(current.state, TaskState::Running | TaskState::Cancelling)
                    && current.executor_epoch == attached.executor_epoch
                    && self.owns_controller(&current) =>
            {
                current
            }
            _ => {
                abort.abort();
                let _ = worker.await;
                self.live.remove_if(&attached.id, |_, live| {
                    live.epoch == attached.executor_epoch
                });
                bail!(
                    "workflow {} lost exact daemon ownership during initialization",
                    attached.id
                );
            }
        };
        if current.state == TaskState::Cancelling {
            cancel.cancel();
        }
        let watcher = self.clone();
        let watcher_id = attached.id.clone();
        let watcher_epoch = attached.executor_epoch;
        let (watcher_start_tx, watcher_start_rx) = tokio::sync::oneshot::channel::<()>();
        let watcher_task = tokio::spawn(async move {
            let _completion = completion;
            if watcher_start_rx.await.is_err() {
                return;
            }
            watcher
                .watch_worker(watcher_id, watcher_epoch, cancel, worker)
                .await;
        });
        self.watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((attached.id.clone(), attached.executor_epoch), watcher_task);
        if watcher_start_tx.send(()).is_err() || start_tx.send(()).is_err() {
            abort.abort();
        }
        Ok(current)
    }

    #[cfg(test)]
    async fn pause_after_live_insert_for_test(&self) {
        let hook = self
            .after_live_insert_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook.reached.notify_one();
            hook.release.notified().await;
        }
    }

    async fn watch_worker(
        &self,
        id: String,
        epoch: u64,
        cancel: CancellationToken,
        mut worker: tokio::task::JoinHandle<vyane_workflow::WorkflowResult<WorkflowOutcome>>,
    ) {
        let mut renew = tokio::time::interval(LEASE_RENEW_INTERVAL);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renew.tick().await;
        let mut forced_code = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                _ = renew.tick() => {
                    match self.renew_lease(&id, epoch).await {
                        Ok(true) => {}
                        Ok(false) => {
                            forced_code = Some(FailureCode::WorkerLost);
                            cancel.cancel();
                            worker.abort();
                            break worker.await;
                        }
                        Err(error) => {
                            tracing::error!(task_id = %id, error = %error, "workflow lease renewal failed");
                            forced_code = Some(FailureCode::LeaseExpired);
                            cancel.cancel();
                            worker.abort();
                            break worker.await;
                        }
                    }
                }
            }
        };

        let completion = if let Some(code) = forced_code {
            if let Some(live) = self.live.get(&id).map(|entry| entry.clone()) {
                match live.control.cancel_all().await {
                    Ok(report) if !report.all_resolved() => {
                        tracing::warn!(task_id = %id, report = ?report, "lease loss retained unresolved workflow controllers");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(task_id = %id, error = %error, "lease-loss controller cleanup failed");
                    }
                }
            }
            CompletionAction::Settle(TaskSettlement::Failed {
                code,
                ledger_run_id: None,
            })
        } else {
            self.finish_worker(&id, result)
        };
        self.persist_completion(&id, epoch, completion).await;
    }

    async fn renew_lease(&self, id: &str, epoch: u64) -> Result<bool> {
        for _ in 0..8 {
            let Some(record) = self.get(id).await? else {
                return Ok(false);
            };
            if !Self::matches_scope(&record) || record.state.is_terminal() {
                return Ok(false);
            }
            if record.executor_epoch != epoch || !self.owns_controller(&record) {
                return Ok(false);
            }
            let task_id = id.to_string();
            let owner = self.instance_id.to_string();
            let now = chrono::Utc::now();
            let result = self
                .call(move |store| {
                    store.renew_lease(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        record.executor_epoch,
                        &owner,
                        now + chrono::Duration::seconds(LEASE_DURATION_SECS),
                        now,
                    )
                })
                .await;
            match result {
                Ok(_) => return Ok(true),
                Err(error)
                    if error
                        .downcast_ref::<TaskStoreError>()
                        .is_some_and(|error| matches!(error, TaskStoreError::Conflict { .. })) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        bail!("workflow lease renewal remained contended for {id}")
    }

    fn finish_worker(
        &self,
        id: &str,
        result: std::result::Result<
            vyane_workflow::WorkflowResult<WorkflowOutcome>,
            tokio::task::JoinError,
        >,
    ) -> CompletionAction {
        match result {
            Ok(Ok(outcome)) => {
                let settlement = match outcome.status {
                    WorkflowRunStatus::Completed => TaskSettlement::Succeeded {
                        ledger_run_id: None,
                    },
                    WorkflowRunStatus::Cancelled => TaskSettlement::Cancelled {
                        ledger_run_id: None,
                    },
                    WorkflowRunStatus::CompletedWithFailures | WorkflowRunStatus::Failed => {
                        TaskSettlement::Failed {
                            code: FailureCode::DispatchFailed,
                            ledger_run_id: None,
                        }
                    }
                    WorkflowRunStatus::Running => TaskSettlement::Failed {
                        code: FailureCode::Internal,
                        ledger_run_id: None,
                    },
                };
                CompletionAction::Settle(settlement)
            }
            Ok(Err(error)) => {
                let code = if error.is_validation_or_config() {
                    FailureCode::Configuration
                } else {
                    FailureCode::Internal
                };
                tracing::error!(task_id = %id, error = %error, "daemon workflow failed");
                CompletionAction::Settle(TaskSettlement::Failed {
                    code,
                    ledger_run_id: None,
                })
            }
            Err(error) => {
                tracing::error!(task_id = %id, error = %error, "daemon workflow task join failed");
                if self.shutting_down.load(Ordering::Acquire) {
                    CompletionAction::Interrupt(FailureCode::WorkerLost)
                } else {
                    CompletionAction::Settle(TaskSettlement::Failed {
                        code: FailureCode::Internal,
                        ledger_run_id: None,
                    })
                }
            }
        }
    }

    /// Keep the exact generation supervised while durable completion is
    /// temporarily unavailable. Shutdown can abort and await this watcher; a
    /// normal run never drops its cancellation/control handles before the row
    /// is terminal or ownership has changed.
    async fn persist_completion(&self, id: &str, epoch: u64, action: CompletionAction) {
        let mut delay = Duration::from_millis(25);
        loop {
            let result = match &action {
                CompletionAction::Settle(settlement) => {
                    self.settle_current(id, epoch, settlement.clone()).await
                }
                CompletionAction::Interrupt(code) => self.interrupt_current(id, epoch, *code).await,
            };
            match result {
                Ok(()) => return,
                Err(error) => {
                    tracing::error!(task_id = %id, error = %error, "workflow metadata completion retry");
                }
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(Duration::from_secs(1));
        }
    }

    fn owns_controller(&self, record: &TaskRecord) -> bool {
        matches!(
            &record.controller,
            Some(ControllerRef::InProcess { instance_id })
                if instance_id == self.instance_id.as_ref()
        )
    }

    async fn settle_current(&self, id: &str, epoch: u64, settlement: TaskSettlement) -> Result<()> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(());
            };
            if !Self::matches_scope(&record) || record.state.is_terminal() {
                return Ok(());
            }
            if record.executor_epoch != epoch || !self.owns_controller(&record) {
                return Ok(());
            }
            let task_id = id.to_string();
            let settlement = settlement.clone();
            let result = self
                .call(move |store| {
                    store.settle(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        record.executor_epoch,
                        settlement,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(_) => return Ok(()),
                Err(error)
                    if error
                        .downcast_ref::<TaskStoreError>()
                        .is_some_and(|error| matches!(error, TaskStoreError::Conflict { .. })) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        bail!("workflow settlement remained contended for {id}")
    }

    async fn settle_record(&self, record: &TaskRecord, settlement: TaskSettlement) -> Result<()> {
        let task_id = record.id.clone();
        let revision = record.revision;
        let epoch = record.executor_epoch;
        self.call(move |store| {
            store.settle(
                LOCAL_TASK_OWNER,
                &task_id,
                revision,
                epoch,
                settlement,
                chrono::Utc::now(),
            )
        })
        .await
        .map(|_| ())
    }

    async fn interrupt_record(&self, record: &TaskRecord, code: FailureCode) -> Result<bool> {
        if !Self::matches_scope(record) || record.state.is_terminal() {
            return Ok(false);
        }
        let task_id = record.id.clone();
        let revision = record.revision;
        let epoch = record.executor_epoch;
        let result = self
            .call(move |store| {
                store.interrupt(
                    LOCAL_TASK_OWNER,
                    &task_id,
                    revision,
                    epoch,
                    code,
                    chrono::Utc::now(),
                )
            })
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error)
                if error.downcast_ref::<TaskStoreError>().is_some_and(|error| {
                    matches!(
                        error,
                        TaskStoreError::Conflict { .. } | TaskStoreError::InvalidState { .. }
                    )
                }) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    async fn interrupt_abandoned(&self, id: &str) -> Result<bool> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(false);
            };
            if !Self::matches_scope(&record) || record.state.is_terminal() {
                return Ok(false);
            }
            if self
                .interrupt_record(&record, FailureCode::WorkerLost)
                .await?
            {
                return Ok(true);
            }
        }
        bail!("workflow recovery interruption remained contended for {id}")
    }

    async fn interrupt_current(&self, id: &str, epoch: u64, code: FailureCode) -> Result<()> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(());
            };
            if !Self::matches_scope(&record) || record.state.is_terminal() {
                return Ok(());
            }
            if record.executor_epoch != epoch || !self.owns_controller(&record) {
                return Ok(());
            }
            if self.interrupt_record(&record, code).await? {
                return Ok(());
            }
        }
        bail!("workflow interruption remained contended for {id}")
    }

    pub(crate) async fn get(&self, id: &str) -> Result<Option<TaskRecord>> {
        let task_id = id.to_string();
        self.call(move |store| store.get(LOCAL_TASK_OWNER, &task_id))
            .await
    }

    pub(crate) async fn view(&self, run_id: &WorkflowRunId) -> Result<Option<WorkflowTaskView>> {
        let Some(task) = self.get(run_id.as_str()).await? else {
            return Ok(None);
        };
        if !Self::matches_scope(&task) {
            return Ok(None);
        }
        let journal_dir = self.service.storage_paths().workflows_dir.clone();
        let id = run_id.to_string();
        let journal = tokio::task::spawn_blocking(move || read_journal(&journal_dir, &id))
            .await
            .context("join workflow journal reader")?;
        let journal = match journal {
            Ok(journal) => Some(WorkflowJournalSummary::from(&journal)),
            Err(vyane_workflow::WorkflowError::ReadJournal { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Some(WorkflowTaskView { task, journal }))
    }

    pub(crate) async fn cancel(&self, run_id: &WorkflowRunId) -> Result<Option<TaskRecord>> {
        let id = run_id.to_string();
        for _ in 0..16 {
            let Some(record) = self.get(&id).await? else {
                return Ok(None);
            };
            if !Self::matches_scope(&record) {
                return Ok(None);
            }
            if record.state.is_terminal() {
                self.cleanup_terminal_controller(run_id).await;
                return Ok(Some(record));
            }
            if matches!(record.state, TaskState::Running | TaskState::Cancelling) {
                if !self.owns_controller(&record) {
                    bail!("workflow {id} is not controlled by this daemon generation");
                }
                let has_live = self
                    .live
                    .get(&id)
                    .is_some_and(|live| live.epoch == record.executor_epoch);
                if !has_live && !self.initializing_cancels.contains_key(&id) {
                    bail!("workflow {id} has no exact live or initializing controller");
                }
            }
            if record.state == TaskState::Cancelling {
                self.signal_live_cancel(&record);
                return Ok(Some(record));
            }
            let task_id = id.clone();
            let revision = record.revision;
            let epoch = record.executor_epoch;
            let result = self
                .call(move |store| {
                    store.request_cancel(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        revision,
                        epoch,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(cancelled) => {
                    self.signal_live_cancel(&cancelled);
                    return Ok(Some(cancelled));
                }
                Err(error)
                    if error
                        .downcast_ref::<TaskStoreError>()
                        .is_some_and(|error| matches!(error, TaskStoreError::Conflict { .. })) =>
                {
                    continue;
                }
                Err(error)
                    if error.downcast_ref::<TaskStoreError>().is_some_and(|error| {
                        matches!(error, TaskStoreError::InvalidState { .. })
                    }) =>
                {
                    match self.get(&id).await? {
                        None => return Ok(None),
                        Some(current)
                            if Self::matches_scope(&current) && current.state.is_terminal() =>
                        {
                            return Ok(Some(current));
                        }
                        Some(current)
                            if Self::matches_scope(&current)
                                && current.state == TaskState::Cancelling
                                && current.executor_epoch == epoch
                                && self.owns_controller(&current) =>
                        {
                            self.signal_live_cancel(&current);
                            return Ok(Some(current));
                        }
                        Some(_) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        bail!("workflow cancellation remained contended for {id}")
    }

    fn signal_live_cancel(&self, record: &TaskRecord) {
        if let Some(token) = self.initializing_cancels.get(&record.id) {
            token.cancel();
        }
        let Some(live) = self.live.get(&record.id).map(|entry| entry.clone()) else {
            return;
        };
        if live.epoch != record.executor_epoch || !self.owns_controller(record) {
            return;
        }
        live.cancel.cancel();
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let live_map = Arc::clone(&self.live);
        let id = record.id.clone();
        let epoch = record.executor_epoch;
        let control = live.control.clone();
        let cleanup = tokio::spawn(async move {
            tokio::time::sleep(CANCEL_CONTROLLER_GRACE).await;
            if live_map.get(&id).is_some_and(|entry| entry.epoch == epoch) {
                if let Err(error) = control.cancel_all().await {
                    tracing::warn!(task_id = %id, error = %error, "workflow controller cleanup failed after cancel grace");
                }
            }
        });
        let mut tasks = self
            .controller_cleanup_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.push(cleanup);
    }

    async fn cleanup_terminal_controller(&self, run_id: &WorkflowRunId) {
        match WorkflowHarnessControl::new(run_id, &self.service.storage_paths().data_dir) {
            Ok(control) => match control.cancel_all().await {
                Ok(report) if !report.all_resolved() => {
                    tracing::warn!(task_id = %run_id, report = ?report, "terminal workflow retained unresolved controllers");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(task_id = %run_id, error = %error, "terminal workflow controller cleanup failed")
                }
            },
            Err(error) => {
                tracing::warn!(task_id = %run_id, error = %error, "open terminal workflow controllers failed")
            }
        }
    }

    pub(crate) async fn shutdown_and_drain(&self) -> Result<()> {
        self.begin_shutdown();
        let mut first_error = None;

        if !self
            .wait_initializers_until(tokio::time::Instant::now() + INITIALIZER_DRAIN_BUDGET)
            .await
        {
            tracing::warn!("workflow initializers exceeded shutdown drain budget; aborting them");
            self.abort_initializers();
            self.wait_initializers().await;
        }
        let cleanup_tasks = {
            let mut tasks = self
                .controller_cleanup_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *tasks)
        };
        for task in &cleanup_tasks {
            task.abort();
        }
        let _ = futures::future::join_all(cleanup_tasks).await;

        match tokio::time::timeout(METADATA_PHASE_BUDGET, self.request_shutdown_cancellation())
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => first_error = Some(error),
            Err(_) => {
                first_error = Some(anyhow::anyhow!(
                    "timed out requesting durable workflow cancellation"
                ));
            }
        }
        for live in self.live.iter() {
            live.cancel.cancel();
        }

        self.wait_live_until(tokio::time::Instant::now() + SHUTDOWN_DRAIN_BUDGET)
            .await;

        let forced = self
            .live
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        if !forced.is_empty() {
            let cleanup = futures::future::join_all(
                forced
                    .iter()
                    .map(|(id, live)| async move { (id, live.control.cancel_all().await) }),
            );
            match tokio::time::timeout(CONTROLLER_CLEANUP_BUDGET, cleanup).await {
                Ok(results) => {
                    for (id, result) in results {
                        match result {
                            Ok(report) if !report.all_resolved() => tracing::warn!(
                                task_id = %id,
                                report = ?report,
                                "shutdown retained fail-closed workflow controllers"
                            ),
                            Ok(_) => {}
                            Err(error) => tracing::warn!(
                                task_id = %id,
                                error = %error,
                                "shutdown workflow controller cleanup failed"
                            ),
                        }
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(anyhow::anyhow!(
                            "timed out cleaning workflow harness controllers"
                        ));
                    }
                }
            }

            // The first exact interruption pass races normal settlement by
            // CAS; whichever terminal transition commits first is authoritative.
            match tokio::time::timeout(METADATA_PHASE_BUDGET, self.interrupt_owned_active()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(_) if first_error.is_none() => {
                    first_error = Some(anyhow::anyhow!(
                        "timed out interrupting active workflows before forced abort"
                    ));
                }
                _ => {}
            }
            for (_, live) in &forced {
                live.abort.abort();
            }
            self.wait_live_until(tokio::time::Instant::now() + ABORTED_WORKER_DRAIN_BUDGET)
                .await;
        }

        // Any watcher still present is now supervising an already-aborted
        // worker or retrying metadata. Abort and await those exact tasks before
        // the supervisor lock can be released.
        let remaining_keys = self
            .live
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().epoch))
            .collect::<Vec<_>>();
        let mut watcher_handles = {
            let mut watchers = self
                .watchers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            remaining_keys
                .iter()
                .filter_map(|key| watchers.remove(key))
                .collect::<Vec<_>>()
        };
        for watcher in &watcher_handles {
            watcher.abort();
        }
        let _ = futures::future::join_all(watcher_handles.drain(..)).await;

        // Aborted async tasks may have been awaiting a non-cancellable
        // spawn_blocking SQLite call. SQLite's busy timeout bounds the normal
        // wait; we nevertheless preserve the ownership lock until every such
        // closure has actually returned.
        if !self
            .wait_blocking_operations_until(
                tokio::time::Instant::now() + BLOCKING_OPERATION_DRAIN_BUDGET,
            )
            .await
        {
            tracing::error!("workflow SQLite operations exceeded their bounded busy timeout");
            self.wait_blocking_operations().await;
        }

        // No initializer, worker, watcher, or old blocking write can now race
        // this final exact pass.
        let final_pass_timed_out = match tokio::time::timeout(
            METADATA_PHASE_BUDGET,
            self.interrupt_owned_active(),
        )
        .await
        {
            Ok(Ok(())) => false,
            Ok(Err(error)) if first_error.is_none() => {
                first_error = Some(error);
                false
            }
            Err(_) if first_error.is_none() => {
                first_error = Some(anyhow::anyhow!(
                    "timed out during final workflow interruption pass"
                ));
                true
            }
            Err(_) => true,
            Ok(Err(_)) => false,
        };

        // A timed-out final pass may have dropped an awaiter for an already
        // running SQLite closure. Quiesce it and repeat once without dropping
        // the future; otherwise that late write could cross lock release.
        if final_pass_timed_out {
            self.wait_blocking_operations().await;
            if let Err(error) = self.interrupt_owned_active().await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn request_shutdown_cancellation(&self) -> Result<()> {
        for record in self.owned_active_records().await? {
            self.request_shutdown_cancel(&record.id).await?;
        }
        Ok(())
    }

    async fn request_shutdown_cancel(&self, id: &str) -> Result<()> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(());
            };
            if !Self::matches_scope(&record) || record.state.is_terminal() {
                return Ok(());
            }
            if matches!(record.state, TaskState::Running | TaskState::Cancelling)
                && !self.owns_controller(&record)
            {
                return Ok(());
            }
            if record.state == TaskState::Cancelling {
                self.signal_live_cancel(&record);
                return Ok(());
            }
            let task_id = record.id.clone();
            let revision = record.revision;
            let epoch = record.executor_epoch;
            match self
                .call(move |store| {
                    store.request_cancel(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        revision,
                        epoch,
                        chrono::Utc::now(),
                    )
                })
                .await
            {
                Ok(cancelled) => {
                    self.signal_live_cancel(&cancelled);
                    return Ok(());
                }
                Err(error)
                    if error.downcast_ref::<TaskStoreError>().is_some_and(|error| {
                        matches!(
                            error,
                            TaskStoreError::Conflict { .. } | TaskStoreError::InvalidState { .. }
                        )
                    }) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("workflow shutdown cancellation remained contended for {id}")
    }

    async fn interrupt_owned_active(&self) -> Result<()> {
        for record in self.owned_active_records().await? {
            let mut resolved = false;
            for _ in 0..16 {
                let Some(current) = self.get(&record.id).await? else {
                    resolved = true;
                    break;
                };
                if !Self::matches_scope(&current) || current.state.is_terminal() {
                    resolved = true;
                    break;
                }
                if matches!(current.state, TaskState::Running | TaskState::Cancelling)
                    && !self.owns_controller(&current)
                {
                    bail!(
                        "workflow {} changed to a foreign controller during shutdown",
                        current.id
                    );
                }
                if self
                    .interrupt_record(&current, FailureCode::WorkerLost)
                    .await?
                {
                    resolved = true;
                    break;
                }
            }
            if !resolved {
                bail!(
                    "workflow shutdown interruption remained contended for {}",
                    record.id
                );
            }
        }
        Ok(())
    }

    async fn owned_active_records(&self) -> Result<Vec<TaskRecord>> {
        let mut cursor = None;
        let mut records = Vec::new();
        loop {
            let query = TaskQuery {
                kinds: vec![TaskKind::Workflow],
                origins: vec![TaskOrigin::Daemon],
                states: vec![TaskState::Queued, TaskState::Running, TaskState::Cancelling],
                limit: 1_000,
                cursor,
            };
            let page = self
                .call(move |store| store.list(LOCAL_TASK_OWNER, &query))
                .await?;
            records.extend(page.items.into_iter().filter(Self::matches_scope));
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        Ok(records)
    }

    async fn wait_live_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.finished.notified();
            if self.live.is_empty() {
                return true;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let _ = tokio::time::timeout(deadline.saturating_duration_since(now), notified).await;
        }
    }

    fn abort_initializers(&self) {
        for control in self.initializer_controls.iter() {
            if let Some(abort) = control
                .abort
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                abort.abort();
            }
        }
    }

    async fn wait_initializers_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.initializer_finished.notified();
            if self.initializers.load(Ordering::Acquire) == 0 {
                return true;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let _ = tokio::time::timeout(deadline.saturating_duration_since(now), notified).await;
        }
    }

    async fn wait_initializers(&self) {
        while self.initializers.load(Ordering::Acquire) != 0 {
            self.abort_initializers();
            let _ = tokio::time::timeout(SHUTDOWN_POLL, self.initializer_finished.notified()).await;
        }
    }

    async fn wait_blocking_operations_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.blocking_operation_finished.notified();
            if self.blocking_operations.load(Ordering::Acquire) == 0 {
                return true;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let _ = tokio::time::timeout(deadline.saturating_duration_since(now), notified).await;
        }
    }

    async fn wait_blocking_operations(&self) {
        while self.blocking_operations.load(Ordering::Acquire) != 0 {
            self.blocking_operation_finished.notified().await;
        }
    }
}

fn validate_workflow_vars(vars: &BTreeMap<String, String>) -> WorkflowResult<()> {
    if vars.len() > WORKFLOW_VARS_MAX_ENTRIES {
        return Err(WorkflowError::validation(vec![format!(
            "workflow vars exceed the {WORKFLOW_VARS_MAX_ENTRIES}-entry limit"
        )]));
    }
    let mut total = 0usize;
    for (index, (key, value)) in vars.iter().enumerate() {
        if key.is_empty() || key.len() > WORKFLOW_VAR_KEY_MAX_BYTES || key.contains('\0') {
            return Err(WorkflowError::validation(vec![format!(
                "workflow var {} has an invalid key",
                index + 1
            )]));
        }
        if value.len() > WORKFLOW_VAR_VALUE_MAX_BYTES || value.contains('\0') {
            return Err(WorkflowError::validation(vec![format!(
                "workflow var {} has an invalid value",
                index + 1
            )]));
        }
        total = total
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or_else(|| WorkflowError::validation(vec!["workflow vars size overflow".into()]))?;
        if total > WORKFLOW_VARS_MAX_TOTAL_BYTES {
            return Err(WorkflowError::validation(vec![format!(
                "workflow vars exceed the {WORKFLOW_VARS_MAX_TOTAL_BYTES}-byte total limit"
            )]));
        }
    }
    Ok(())
}

fn validate_execution_cwd_wire(execution_cwd: &FsPath) -> WorkflowResult<()> {
    let Some(value) = execution_cwd.to_str() else {
        return Err(WorkflowError::validation(vec![
            "workflow execution cwd must be UTF-8".into(),
        ]));
    };
    if value.is_empty()
        || value.len() > EXECUTION_CWD_MAX_BYTES
        || value.contains('\0')
        || !execution_cwd.is_absolute()
        || execution_cwd
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(WorkflowError::validation(vec![
            "workflow execution cwd must be a bounded canonical absolute path".into(),
        ]));
    }
    Ok(())
}

fn validate_new_execution_cwd(execution_cwd: &FsPath) -> WorkflowResult<()> {
    let canonical = std::fs::canonicalize(execution_cwd).map_err(|_| {
        WorkflowError::validation(vec![
            "workflow execution cwd is unavailable or not a directory".into(),
        ])
    })?;
    let is_directory = std::fs::metadata(execution_cwd)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);
    if canonical != execution_cwd || !is_directory {
        return Err(WorkflowError::validation(vec![
            "workflow execution cwd must be a bounded canonical absolute path".into(),
        ]));
    }
    Ok(())
}

fn rebase_workdirs(workflow: &mut Workflow, execution_cwd: &FsPath) -> WorkflowResult<()> {
    for (index, step) in workflow.steps.iter_mut().enumerate() {
        let Some(workdir) = step.workdir.as_ref() else {
            step.workdir = Some(execution_cwd.to_path_buf());
            continue;
        };
        let Some(value) = workdir.to_str() else {
            return Err(WorkflowError::validation(vec![format!(
                "workflow step {} has a non-UTF-8 workdir",
                index + 1
            )]));
        };
        if value.contains('\0') || value.len() > EXECUTION_CWD_MAX_BYTES {
            return Err(WorkflowError::validation(vec![format!(
                "workflow step {} has an invalid workdir",
                index + 1
            )]));
        }
        if workdir.is_relative() {
            step.workdir = Some(execution_cwd.join(workdir));
        }
    }
    Ok(())
}

fn submission_digest(
    workflow: &Workflow,
    vars: &BTreeMap<String, String>,
    execution_cwd: &FsPath,
) -> String {
    let mut hash = Sha256::new();
    hash.update(SUBMISSION_DIGEST_DOMAIN);
    digest_field(&mut hash, workflow.file_sha256.as_bytes());
    digest_field(&mut hash, execution_cwd.as_os_str().as_encoded_bytes());
    hash.update((vars.len() as u64).to_be_bytes());
    for (key, value) in vars {
        digest_field(&mut hash, key.as_bytes());
        digest_field(&mut hash, value.as_bytes());
    }
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn digest_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

struct InitializationPermit {
    id: u64,
    control: Arc<InitializerControl>,
    controls: Arc<DashMap<u64, Arc<InitializerControl>>>,
    count: Arc<AtomicUsize>,
    finished: Arc<Notify>,
}

impl Drop for InitializationPermit {
    fn drop(&mut self) {
        self.controls
            .remove_if(&self.id, |_, current| Arc::ptr_eq(current, &self.control));
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.finished.notify_one();
    }
}

struct BlockingOperationPermit {
    count: Arc<AtomicUsize>,
    finished: Arc<Notify>,
}

impl Drop for BlockingOperationPermit {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.finished.notify_one();
    }
}

struct InitializingCancelGuard {
    id: String,
    tokens: Arc<DashMap<String, CancellationToken>>,
}

impl Drop for InitializingCancelGuard {
    fn drop(&mut self) {
        self.tokens.remove(&self.id);
    }
}

struct LiveCompletionGuard {
    id: String,
    epoch: u64,
    live: Arc<DashMap<String, LiveWorkflow>>,
    watchers: Arc<Mutex<BTreeMap<WatcherKey, tokio::task::JoinHandle<()>>>>,
    finished: Arc<Notify>,
}

impl Drop for LiveCompletionGuard {
    fn drop(&mut self) {
        if let Some((_, live)) = self
            .live
            .remove_if(&self.id, |_, live| live.epoch == self.epoch)
        {
            // A watcher can be aborted while still owning its nested worker.
            // Abort the worker before publishing completion of the live entry.
            live.abort.abort();
        }
        self.watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(self.id.clone(), self.epoch));
        self.finished.notify_one();
    }
}

pub(crate) fn routes() -> Router<DaemonHttpState> {
    Router::new()
        .route("/v1/workflows", post(submit_workflow))
        .route("/v1/workflows/{id}", get(workflow_status))
        .route("/v1/workflows/{id}/cancel", post(cancel_workflow))
}

async fn submit_workflow(
    State(state): State<DaemonHttpState>,
    Json(request): Json<WorkflowSubmitRequest>,
) -> std::result::Result<(StatusCode, Json<WorkflowTaskView>), DaemonWorkflowApiError> {
    let task = state
        .workflows
        .submit(request)
        .await
        .map_err(DaemonWorkflowApiError::from_anyhow)?;
    let run_id = WorkflowRunId::from_str(&task.id)
        .map_err(|error| DaemonWorkflowApiError::internal(error.to_string()))?;
    let view = state
        .workflows
        .view(&run_id)
        .await
        .map_err(DaemonWorkflowApiError::from_anyhow)?
        .unwrap_or(WorkflowTaskView {
            task,
            journal: None,
        });
    Ok((StatusCode::ACCEPTED, Json(view)))
}

async fn workflow_status(
    State(state): State<DaemonHttpState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<WorkflowTaskView>, DaemonWorkflowApiError> {
    let run_id = parse_run_id(&id)?;
    state
        .workflows
        .view(&run_id)
        .await
        .map_err(DaemonWorkflowApiError::from_anyhow)?
        .map(Json)
        .ok_or_else(DaemonWorkflowApiError::not_found)
}

async fn cancel_workflow(
    State(state): State<DaemonHttpState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<TaskRecord>, DaemonWorkflowApiError> {
    let run_id = parse_run_id(&id)?;
    state
        .workflows
        .cancel(&run_id)
        .await
        .map_err(DaemonWorkflowApiError::from_anyhow)?
        .map(Json)
        .ok_or_else(DaemonWorkflowApiError::not_found)
}

fn parse_run_id(value: &str) -> std::result::Result<WorkflowRunId, DaemonWorkflowApiError> {
    WorkflowRunId::from_str(value)
        .map_err(|_| DaemonWorkflowApiError::bad_request("invalid workflow run id"))
}

#[derive(Debug, Serialize)]
struct DaemonWorkflowErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct DaemonWorkflowApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl DaemonWorkflowApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "workflow task was not found",
        )
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    fn from_anyhow(error: anyhow::Error) -> Self {
        if error
            .downcast_ref::<vyane_workflow::WorkflowError>()
            .is_some_and(vyane_workflow::WorkflowError::is_validation_or_config)
        {
            return Self::bad_request(error.to_string());
        }
        if error
            .downcast_ref::<TaskStoreError>()
            .is_some_and(|error| matches!(error, TaskStoreError::AlreadyExists { .. }))
        {
            return Self::new(
                StatusCode::CONFLICT,
                "conflict",
                "workflow task already exists",
            );
        }
        tracing::error!(error = %error, "daemon workflow API failed");
        Self::internal("daemon workflow operation failed")
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        let message = message.into();
        let bounded = message.chars().take(API_ERROR_LIMIT).collect();
        Self {
            status,
            code,
            message: bounded,
        }
    }
}

impl IntoResponse for DaemonWorkflowApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(DaemonWorkflowErrorBody {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use vyane_service::StoragePaths;

    use super::*;

    async fn test_supervisor(data_dir: &Path) -> DaemonWorkflowSupervisor {
        let service = VyaneService::from_loaded_with_paths(
            vyane_service::LoadedConfig {
                config: vyane_config::ResolvedConfig::default(),
                files: Vec::new(),
                secrets: BTreeMap::new(),
            },
            StoragePaths::from_data_dir(data_dir),
        )
        .unwrap();
        DaemonWorkflowSupervisor::open(Arc::new(service), "daemon:test-workflow-supervisor".into())
            .await
            .unwrap()
    }

    async fn configured_test_supervisor(data_dir: &Path) -> DaemonWorkflowSupervisor {
        let config_path = data_dir.join("daemon-workflow-test-config.toml");
        std::fs::write(
            &config_path,
            r#"
            [providers.test]
            base_url = "http://127.0.0.1:9"
            api_key_env = "VYANE_DAEMON_WORKFLOW_TEST_KEY"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "test-model"

            [profiles.worker]
            provider = "test"
            protocol = "openai_chat"
            harness = "none"
            model = "test-model"
            "#,
        )
        .unwrap();
        let mut layers = vyane_config::ConfigLayers::new();
        layers.merge_file(&config_path).unwrap();
        let service = VyaneService::from_loaded_with_paths(
            vyane_service::LoadedConfig {
                config: layers.into(),
                files: Vec::new(),
                secrets: BTreeMap::from([(
                    "VYANE_DAEMON_WORKFLOW_TEST_KEY".into(),
                    "test-secret".into(),
                )]),
            },
            StoragePaths::from_data_dir(data_dir),
        )
        .unwrap();
        DaemonWorkflowSupervisor::open(Arc::new(service), "daemon:test-workflow-supervisor".into())
            .await
            .unwrap()
    }

    fn bundle(name: &str) -> WorkflowSourceBundle {
        WorkflowSourceBundle {
            workflow_toml: format!(
                r#"[workflow]
name = "{name}"

[[step]]
id = "only"
target = "missing-current-config"
prompt = "run"
"#
            ),
            prompt_files: Vec::new(),
        }
    }

    fn configured_bundle(name: &str) -> WorkflowSourceBundle {
        let mut bundle = bundle(name);
        bundle.workflow_toml = bundle
            .workflow_toml
            .replace("missing-current-config", "worker");
        bundle
    }

    fn new_task(id: &WorkflowRunId, _owner: &str, digest: &str) -> NewTask {
        NewTask {
            id: id.to_string(),
            kind: TaskKind::Workflow,
            origin: TaskOrigin::Daemon,
            task_digest: digest.into(),
            target_key: "workflow".into(),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn execution_cwd_rebases_missing_and_relative_workdirs_but_preserves_absolute() {
        let directory = tempfile::tempdir().unwrap();
        let daemon_cwd = directory.path().join("daemon-start");
        let submit_cwd = directory.path().join("client-submit");
        let absolute = directory.path().join("absolute-workdir");
        for path in [&daemon_cwd, &submit_cwd, &absolute] {
            std::fs::create_dir(path).unwrap();
        }
        let submit_cwd = std::fs::canonicalize(submit_cwd).unwrap();
        validate_execution_cwd_wire(&submit_cwd).unwrap();
        validate_new_execution_cwd(&submit_cwd).unwrap();
        let mut workflow = bundle("cwd-rebase").materialize().unwrap();
        let relative_step = workflow.steps[0].clone();
        let mut absolute_step = workflow.steps[0].clone();
        workflow.steps[0].workdir = None;
        workflow
            .steps
            .push(crate::daemon_workflow::tests::with_workdir(
                relative_step,
                PathBuf::from("nested/project"),
            ));
        absolute_step.workdir = Some(absolute.clone());
        workflow.steps.push(absolute_step);

        rebase_workdirs(&mut workflow, &submit_cwd).unwrap();

        assert_eq!(
            workflow.steps[0].workdir.as_deref(),
            Some(submit_cwd.as_path())
        );
        assert_eq!(
            workflow.steps[1].workdir.as_deref(),
            Some(submit_cwd.join("nested/project").as_path())
        );
        assert_eq!(
            workflow.steps[2].workdir.as_deref(),
            Some(absolute.as_path())
        );
        assert!(workflow.steps.iter().all(|step| {
            step.workdir
                .as_deref()
                .is_some_and(|workdir| !workdir.starts_with(&daemon_cwd))
        }));
    }

    fn with_workdir(
        mut step: vyane_workflow::WorkflowStep,
        workdir: PathBuf,
    ) -> vyane_workflow::WorkflowStep {
        step.workdir = Some(workdir);
        step
    }

    #[test]
    fn submission_digest_covers_source_ordered_vars_and_execution_cwd() {
        let directory = tempfile::tempdir().unwrap();
        let first_cwd = directory.path().join("first");
        let second_cwd = directory.path().join("second");
        std::fs::create_dir(&first_cwd).unwrap();
        std::fs::create_dir(&second_cwd).unwrap();
        let first_cwd = std::fs::canonicalize(first_cwd).unwrap();
        let second_cwd = std::fs::canonicalize(second_cwd).unwrap();
        let first = bundle("first").materialize().unwrap();
        let second = bundle("second").materialize().unwrap();
        let mut vars = BTreeMap::from([("key".into(), "one".into())]);
        let baseline = submission_digest(&first, &vars, &first_cwd);

        assert_ne!(baseline, submission_digest(&second, &vars, &first_cwd));
        vars.insert("key".into(), "two".into());
        assert_ne!(baseline, submission_digest(&first, &vars, &first_cwd));
        vars.insert("key".into(), "one".into());
        assert_ne!(baseline, submission_digest(&first, &vars, &second_cwd));
        assert_eq!(baseline.len(), 64);
    }

    #[test]
    fn workflow_var_limits_reject_values_without_echoing_them() {
        let secret = "WORKFLOW_VAR_SECRET_MUST_NOT_BE_ECHOED";
        let vars = BTreeMap::from([("key".into(), format!("{secret}\0"))]);

        let error = validate_workflow_vars(&vars).unwrap_err().to_string();

        assert!(!error.contains(secret));
        assert!(error.contains("invalid value"));
    }

    #[test]
    fn semantic_request_failures_map_to_bounded_400_and_conflicts_stay_generic() {
        let secret = "WORKFLOW_VAR_SECRET_MUST_NOT_BE_ECHOED";
        let vars = BTreeMap::from([("key".into(), format!("{secret}\0"))]);
        let validation = validate_workflow_vars(&vars).unwrap_err();

        let bad_request = DaemonWorkflowApiError::from_anyhow(validation.into());
        let conflict = DaemonWorkflowApiError::from_anyhow(
            TaskStoreError::AlreadyExists {
                id: "caller-controlled-id".into(),
            }
            .into(),
        );

        assert_eq!(bad_request.status, StatusCode::BAD_REQUEST);
        assert!(!bad_request.message.contains(secret));
        assert!(bad_request.message.len() <= API_ERROR_LIMIT);
        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert_eq!(conflict.message, "workflow task already exists");
        assert!(!conflict.message.contains("caller-controlled-id"));
    }

    #[tokio::test]
    async fn excessive_concurrency_is_rejected_before_durable_creation() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let request = WorkflowSubmitRequest {
            run_id: run_id.clone(),
            execution_cwd: std::fs::canonicalize(directory.path()).unwrap(),
            bundle: WorkflowSourceBundle {
                workflow_toml: format!(
                    r#"[workflow]
name = "excessive-concurrency"
max_concurrency = {}

[[step]]
id = "only"
target = "missing-current-config"
prompt = "run"
"#,
                    tokio::sync::Semaphore::MAX_PERMITS + 1
                ),
                prompt_files: Vec::new(),
            },
            vars: BTreeMap::new(),
        };

        let error = supervisor.submit(request).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("max_concurrency must not exceed")
        );
        assert!(supervisor.get(run_id.as_str()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn invalid_route_effort_is_rejected_before_durable_creation_without_echo() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let canary = "EFFORT_VALUE_MUST_NOT_BE_ECHOED";
        let request = WorkflowSubmitRequest {
            run_id: run_id.clone(),
            execution_cwd: std::fs::canonicalize(directory.path()).unwrap(),
            bundle: WorkflowSourceBundle {
                workflow_toml: format!(
                    r#"[workflow]
name = "invalid-route-effort"

[[step]]
id = "only"
target = "auto"
prompt = "run"
[step.route]
effort = "{canary}"
"#
                ),
                prompt_files: Vec::new(),
            },
            vars: BTreeMap::new(),
        };

        let error = supervisor.submit(request).await.unwrap_err().to_string();

        assert!(!error.contains(canary));
        assert!(supervisor.get(run_id.as_str()).await.unwrap().is_none());
        assert!(supervisor.live.is_empty());
        assert!(
            vyane_workflow::list_journals(&supervisor.service.storage_paths().workflows_dir)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_identical_creates_have_one_creator_and_one_idempotent_reader() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let digest = "b".repeat(64);
        let first = supervisor.create_submission(new_task(&run_id, LOCAL_TASK_OWNER, &digest));
        let second = supervisor.create_submission(new_task(&run_id, LOCAL_TASK_OWNER, &digest));

        let (first, second) = tokio::join!(first, second);
        let results = [first.unwrap(), second.unwrap()];

        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, SubmissionCreate::Created(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, SubmissionCreate::Existing(_)))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn idempotency_rejects_digest_or_scope_mismatch_as_generic_already_exists() {
        for mismatch in ["digest", "scope"] {
            let directory = tempfile::tempdir().unwrap();
            let supervisor = test_supervisor(directory.path()).await;
            let run_id = WorkflowRunId::generate();
            let stored_digest = "c".repeat(64);
            let requested_digest = if mismatch == "digest" {
                "d".repeat(64)
            } else {
                stored_digest.clone()
            };
            let owner = if mismatch == "scope" {
                "foreign-owner"
            } else {
                LOCAL_TASK_OWNER
            };
            let task = new_task(&run_id, owner, &stored_digest);
            supervisor
                .call(move |store| store.create(owner, task))
                .await
                .unwrap();

            let existing = supervisor
                .idempotent_existing(run_id.as_str(), &requested_digest)
                .await;
            if mismatch == "scope" {
                assert!(existing.unwrap().is_none());
            } else {
                let error = existing.unwrap_err();
                assert!(matches!(
                    error.downcast_ref::<TaskStoreError>(),
                    Some(TaskStoreError::AlreadyExists { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn exact_retry_returns_before_current_target_configuration_validation() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let execution_cwd = std::fs::canonicalize(directory.path()).unwrap();
        let bundle = bundle("config-drift-idempotency");
        let workflow = bundle.materialize().unwrap();
        let vars = BTreeMap::new();
        let digest = submission_digest(&workflow, &vars, &execution_cwd);
        let task = new_task(&run_id, LOCAL_TASK_OWNER, &digest);
        let stored = supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let request = WorkflowSubmitRequest {
            run_id,
            execution_cwd,
            bundle,
            vars,
        };

        let retried = supervisor.initialize(request).await.unwrap();

        assert_eq!(retried, stored);
        assert!(supervisor.live.is_empty());
    }

    #[tokio::test]
    async fn corrupt_controller_entry_does_not_block_recovery_interruption() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let task = new_task(&run_id, LOCAL_TASK_OWNER, &"e".repeat(64));
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let controller_dir = directory
            .path()
            .join("workflow-controllers")
            .join(run_id.as_str());
        std::fs::create_dir_all(&controller_dir).unwrap();
        std::fs::write(
            controller_dir.join("controller-123-corrupt.json"),
            b"not-json",
        )
        .unwrap();

        let recovered = supervisor.recover_interrupted().await.unwrap();
        let record = supervisor.get(run_id.as_str()).await.unwrap().unwrap();

        assert_eq!(recovered, 1);
        assert_eq!(record.state, TaskState::Interrupted);
        assert_eq!(record.failure_code, Some(FailureCode::WorkerLost));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_cancel_signals_the_registered_initializer_generation() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let id = run_id.to_string();
        let task = NewTask {
            id: id.clone(),
            kind: TaskKind::Workflow,
            origin: TaskOrigin::Daemon,
            task_digest: "a".repeat(64),
            target_key: "workflow".into(),
            created_at: chrono::Utc::now(),
        };
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let token = CancellationToken::new();
        supervisor
            .initializing_cancels
            .insert(id.clone(), token.clone());

        let cancelled = supervisor.cancel(&run_id).await.unwrap().unwrap();

        assert_eq!(cancelled.state, TaskState::Cancelled);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn begin_shutdown_is_idempotent_closes_admission_and_cancels_initializers() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let token = CancellationToken::new();
        supervisor
            .initializing_cancels
            .insert("initializing".into(), token.clone());

        supervisor.begin_shutdown();
        supervisor.begin_shutdown();

        assert!(token.is_cancelled());
        assert!(!supervisor.accepting.load(Ordering::Acquire));
        assert!(supervisor.shutting_down.load(Ordering::Acquire));
        assert!(supervisor.begin_initialization().is_err());

        let request = WorkflowSubmitRequest {
            run_id: WorkflowRunId::generate(),
            execution_cwd: std::fs::canonicalize(directory.path()).unwrap(),
            bundle: bundle("rejected-after-shutdown"),
            vars: BTreeMap::new(),
        };
        let error = supervisor.submit(request).await.unwrap_err();
        assert!(error.to_string().contains("admission is closed"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborting_initializer_after_live_publication_rolls_back_before_drain() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = configured_test_supervisor(directory.path()).await;
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *supervisor
            .after_live_insert_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(AfterLiveInsertHook {
            reached: Arc::clone(&reached),
            release,
        });
        let run_id = WorkflowRunId::generate();
        let request = WorkflowSubmitRequest {
            run_id: run_id.clone(),
            execution_cwd: std::fs::canonicalize(directory.path()).unwrap(),
            bundle: configured_bundle("abort-after-live-publication"),
            vars: BTreeMap::new(),
        };
        let submission = tokio::spawn({
            let supervisor = supervisor.clone();
            async move { supervisor.submit(request).await }
        });

        tokio::time::timeout(Duration::from_secs(2), reached.notified())
            .await
            .expect("initializer should publish its live generation");
        assert!(supervisor.live.contains_key(run_id.as_str()));

        supervisor.begin_shutdown();
        supervisor.abort_initializers();
        tokio::time::timeout(Duration::from_secs(2), supervisor.wait_initializers())
            .await
            .expect("aborted initializer should release its permit");
        let submission_error = tokio::time::timeout(Duration::from_secs(2), submission)
            .await
            .expect("submission waiter should observe initializer abort")
            .unwrap()
            .unwrap_err();
        assert!(
            submission_error
                .to_string()
                .contains("join daemon workflow initializer")
        );
        assert!(supervisor.live.is_empty());
        assert!(supervisor.watchers.lock().unwrap().is_empty());

        tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown_and_drain())
            .await
            .expect("shutdown must not consume the live-worker drain budgets")
            .unwrap();
        assert!(supervisor.live.is_empty());
        assert!(supervisor.watchers.lock().unwrap().is_empty());
        assert_eq!(supervisor.initializers.load(Ordering::Acquire), 0);
        assert_eq!(supervisor.blocking_operations.load(Ordering::Acquire), 0);
        assert!(
            supervisor
                .get(run_id.as_str())
                .await
                .unwrap()
                .is_some_and(|record| record.state.is_terminal())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drops_the_live_generation_and_stops_its_worker_and_watcher() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = test_supervisor(directory.path()).await;
        let run_id = WorkflowRunId::generate();
        let id = run_id.to_string();
        let epoch = 1;
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(std::future::pending::<()>());
        let worker_abort = worker.abort_handle();
        let control =
            WorkflowHarnessControl::new(&run_id, &supervisor.service.storage_paths().data_dir)
                .unwrap();
        supervisor.live.insert(
            id.clone(),
            LiveWorkflow {
                epoch,
                cancel: cancel.clone(),
                abort: worker_abort,
                control,
            },
        );
        let watcher_finished = Arc::new(AtomicBool::new(false));
        let finished = Arc::clone(&watcher_finished);
        let guard = LiveCompletionGuard {
            id: id.clone(),
            epoch,
            live: Arc::clone(&supervisor.live),
            watchers: Arc::clone(&supervisor.watchers),
            finished: Arc::clone(&supervisor.finished),
        };
        let watcher = tokio::spawn(async move {
            let _guard = guard;
            cancel.cancelled().await;
            finished.store(true, Ordering::Release);
        });
        supervisor
            .watchers
            .lock()
            .unwrap()
            .insert((id, epoch), watcher);

        supervisor.shutdown_and_drain().await.unwrap();

        assert!(supervisor.live.is_empty());
        assert!(supervisor.watchers.lock().unwrap().is_empty());
        assert!(watcher_finished.load(Ordering::Acquire));
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(supervisor.initializers.load(Ordering::Acquire), 0);
        assert_eq!(supervisor.blocking_operations.load(Ordering::Acquire), 0);
    }
}
