//! The REST API layer: thin axum routing on top of [`VyaneService`].
//!
//! Every handler constructs the same `DispatchParams` / `BroadcastParams` /
//! `HistoryFilter` the CLI does and hands them to one shared service, so dispatch
//! semantics are identical regardless of whether a request arrives over the
//! command line or over HTTP. The service is loaded once at startup and shared
//! across requests via axum `State` (it is `Clone`-cheap — everything is behind
//! an `Arc`).
//!
//! JSON fields are snake_case throughout, matching the kernel's own
//! `Serialize`/`Deserialize` derives. The one wire-format wrinkle is `sandbox`:
//! the request body accepts the snake-case spellings (`read_only`, `write`,
//! `full`) that read naturally in JSON, while the kernel's `Sandbox` enum
//! serializes *back* as kebab-case (`read-only`) — that is the form already
//! pinned by `RunRecord` in the ledger, so the response preserves it.

use std::net::{IpAddr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRef, Path, Query, RawQuery, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{MethodFilter, get, on, post},
};
use futures::{FutureExt as _, stream::Stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, mpsc, oneshot};
use vyane_core::{CancellationToken, RunStatus, Sandbox};
use vyane_kernel::{DispatchOutcome, StreamDispatchEvent};
use vyane_service::{
    BroadcastParams, DispatchParams, GoalNextActionView, GoalReadError, GoalReadService,
    HistoryFilter, OwnerContext, OwnerScopedService, RunView, SessionView, VyaneService,
    parse_labels,
};
use vyane_task::{
    ControllerRef, FailureCode, NewTask, SqliteTaskStore, TaskKind, TaskOrigin, TaskQuery,
    TaskRecord, TaskSettlement, TaskState, TaskStore, TaskStoreError,
};

use crate::supervisor::{acquire_task_supervisor_lock, shutdown_signal};
use crate::task::{LOCAL_TASK_OWNER, is_local_dispatch};

fn is_local_rest_dispatch(record: &TaskRecord) -> bool {
    is_local_dispatch(record, TaskOrigin::RestAsync)
}

/// Shared service state. Durable task snapshots live only in SQLite; the
/// supervisor keeps only cancellation tokens and opaque runtime supervision
/// handles in memory.
#[derive(Clone)]
pub struct ApiState {
    service: Arc<OwnerScopedService>,
    tasks: TaskSupervisor,
    goals: Arc<GoalReadService>,
}

#[derive(Clone)]
struct ApiBearerToken(Arc<str>);

struct ApiTokenPublication {
    path: PathBuf,
    token: String,
}

impl Drop for ApiTokenPublication {
    fn drop(&mut self) {
        let metadata = std::fs::symlink_metadata(&self.path).ok();
        let matches = metadata.is_some_and(|metadata| {
            metadata.file_type().is_file()
                && metadata.len() <= 128
                && std::fs::read_to_string(&self.path)
                    .ok()
                    .is_some_and(|current| {
                        crate::daemon::bearer_tokens_equal(current.trim_end(), &self.token)
                    })
        });
        if matches {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Secret-free durable task metadata returned by the REST API.
///
/// Prompts, model output, and raw errors are deliberately absent. A caller can
/// correlate a completed task with the run ledger through `ledger_run_id`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskEntry {
    pub id: String,
    pub task_digest: String,
    pub target: String,
    pub state: TaskState,
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<FailureCode>,
}

#[derive(Debug, Serialize)]
pub struct TaskOutput {
    pub output: String,
}

impl From<TaskRecord> for TaskEntry {
    fn from(record: TaskRecord) -> Self {
        Self {
            id: record.id,
            task_digest: record.task_digest,
            target: record.target_key,
            state: record.state,
            created_at: record.created_at,
            started_at: record.started_at,
            updated_at: record.updated_at,
            finished_at: record.finished_at,
            revision: record.revision,
            ledger_run_id: record.ledger_run_id,
            failure_code: record.failure_code,
        }
    }
}

#[derive(Debug)]
enum TaskCallError {
    Store(TaskStoreError),
    Join(tokio::task::JoinError),
    Contended { id: String },
    ControlUnavailable { id: String },
    ShuttingDown,
}

impl std::fmt::Display for TaskCallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "{error}"),
            Self::Join(error) => write!(formatter, "task metadata worker failed: {error}"),
            Self::Contended { id } => {
                write!(
                    formatter,
                    "task `{id}` kept changing during metadata update"
                )
            }
            Self::ControlUnavailable { id } => {
                write!(formatter, "task `{id}` is owned by another server instance")
            }
            Self::ShuttingDown => formatter.write_str("task supervisor is shutting down"),
        }
    }
}

impl std::error::Error for TaskCallError {}

/// REST task control adapter. SQLite is the only metadata source of truth.
/// Process-local state is limited to exact-epoch cancellation tokens and opaque
/// task handles used to prove dispatch futures have stopped before lock release.
#[derive(Clone)]
struct TaskSupervisor {
    store: Arc<dyn TaskStore>,
    live_tokens: Arc<dashmap::DashMap<(String, u64), CancellationToken>>,
    live_dispatches: Arc<dashmap::DashMap<(String, u64), RuntimeDispatch>>,
    dispatch_finished: Arc<Notify>,
    instance_id: Arc<str>,
    artifacts_root: Option<Arc<PathBuf>>,
    initialization_state: Arc<AtomicUsize>,
    initialization_finished: Arc<Notify>,
}

struct RuntimeDispatch {
    runtime_id: uuid::Uuid,
    task_id: Option<tokio::task::Id>,
    abort: Option<tokio::task::AbortHandle>,
    abort_requested: bool,
}

const INITIALIZATION_SHUTDOWN_BIT: usize = 1 << (usize::BITS - 1);

struct InitializationPermit {
    state: Arc<AtomicUsize>,
    finished: Arc<Notify>,
}

impl Drop for InitializationPermit {
    fn drop(&mut self) {
        self.state.fetch_sub(1, Ordering::AcqRel);
        self.finished.notify_one();
    }
}

struct DurableTaskMetadata {
    task_digest: String,
    target_key: String,
}

const SETTLEMENT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const SETTLEMENT_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

impl FromRef<ApiState> for TaskSupervisor {
    fn from_ref(state: &ApiState) -> Self {
        state.tasks.clone()
    }
}

impl TaskSupervisor {
    async fn open(path: impl AsRef<FsPath>) -> std::result::Result<Self, TaskCallError> {
        let path = path.as_ref().to_path_buf();
        let artifacts_root = path
            .parent()
            .unwrap_or_else(|| FsPath::new("."))
            .join("tasks");
        let store = tokio::task::spawn_blocking(move || SqliteTaskStore::open(path))
            .await
            .map_err(TaskCallError::Join)?
            .map_err(TaskCallError::Store)?;
        Ok(Self::from_store_with_artifacts(
            Arc::new(store),
            artifacts_root,
        ))
    }

    fn from_store(store: Arc<dyn TaskStore>) -> Self {
        Self {
            store,
            live_tokens: Arc::new(dashmap::DashMap::new()),
            live_dispatches: Arc::new(dashmap::DashMap::new()),
            dispatch_finished: Arc::new(Notify::new()),
            instance_id: Arc::from(format!("rest:{}", uuid::Uuid::now_v7())),
            artifacts_root: None,
            initialization_state: Arc::new(AtomicUsize::new(0)),
            initialization_finished: Arc::new(Notify::new()),
        }
    }

    fn from_store_with_artifacts(store: Arc<dyn TaskStore>, artifacts_root: PathBuf) -> Self {
        let mut supervisor = Self::from_store(store);
        supervisor.artifacts_root = Some(Arc::new(artifacts_root));
        supervisor
    }

    fn output_path_for(&self, owner: &str, id: &str) -> Option<PathBuf> {
        self.artifacts_root.as_ref().map(|root| {
            root.join(artifact_segment("owner", owner))
                .join(artifact_segment("task", id))
                .join("output.txt")
        })
    }

    fn output_path(&self, id: &str) -> Option<PathBuf> {
        self.output_path_for(LOCAL_TASK_OWNER, id)
    }

    fn legacy_local_output_path(&self, id: &str) -> Option<PathBuf> {
        uuid::Uuid::parse_str(id).ok()?;
        self.artifacts_root
            .as_ref()
            .map(|root| root.join(id).join("output.txt"))
    }

    async fn call<T, F>(&self, operation: F) -> std::result::Result<T, TaskCallError>
    where
        T: Send + 'static,
        F: FnOnce(&dyn TaskStore) -> vyane_task::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || operation(store.as_ref()))
            .await
            .map_err(TaskCallError::Join)?
            .map_err(TaskCallError::Store)
    }

    async fn get(&self, id: &str) -> std::result::Result<Option<TaskRecord>, TaskCallError> {
        let id = id.to_owned();
        self.call(move |store| store.get(LOCAL_TASK_OWNER, &id))
            .await
    }

    async fn list_rest_tasks(&self) -> std::result::Result<Vec<TaskRecord>, TaskCallError> {
        self.list_rest_tasks_with_page_limit(1_000).await
    }

    async fn list_rest_tasks_with_page_limit(
        &self,
        page_limit: usize,
    ) -> std::result::Result<Vec<TaskRecord>, TaskCallError> {
        let mut cursor = None;
        let mut records = Vec::new();
        loop {
            let query = TaskQuery {
                kinds: vec![TaskKind::Dispatch],
                origins: vec![TaskOrigin::RestAsync],
                limit: page_limit,
                cursor,
                ..TaskQuery::default()
            };
            let page = self
                .call(move |store| store.list(LOCAL_TASK_OWNER, &query))
                .await?;
            records.extend(page.items.into_iter().filter(is_local_rest_dispatch));
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(records)
    }

    /// Interrupt every REST task that could have been left behind by a crash.
    /// Queued is included because create is durable before controller attach.
    async fn recover_interrupted(&self) -> std::result::Result<usize, TaskCallError> {
        self.recover_interrupted_with_page_limit(1_000).await
    }

    async fn recover_interrupted_with_page_limit(
        &self,
        page_limit: usize,
    ) -> std::result::Result<usize, TaskCallError> {
        let mut recovered = 0;
        let mut cursor = None;
        loop {
            let query = TaskQuery {
                kinds: vec![TaskKind::Dispatch],
                origins: vec![TaskOrigin::RestAsync],
                states: vec![TaskState::Queued, TaskState::Running, TaskState::Cancelling],
                limit: page_limit,
                cursor,
            };
            let page = self
                .call(move |store| store.list(LOCAL_TASK_OWNER, &query))
                .await?;
            for record in page.items.into_iter().filter(is_local_rest_dispatch) {
                if self.interrupt_if_active(&record.id).await? {
                    recovered += 1;
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(recovered)
    }

    async fn interrupt_if_active(&self, id: &str) -> std::result::Result<bool, TaskCallError> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(false);
            };
            if record.state.is_terminal() || !is_local_rest_dispatch(&record) {
                return Ok(false);
            }
            let task_id = record.id.clone();
            let result = self
                .call(move |store| {
                    store.interrupt(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        record.executor_epoch,
                        FailureCode::WorkerLost,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(_) => return Ok(true),
                Err(TaskCallError::Store(TaskStoreError::Conflict { .. })) => continue,
                Err(error @ TaskCallError::Store(TaskStoreError::InvalidState { .. })) => {
                    let current = self.get(id).await?;
                    if current.is_none_or(|record| record.state.is_terminal()) {
                        return Ok(false);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(TaskCallError::Contended { id: id.into() })
    }

    async fn submit(
        &self,
        service: Arc<OwnerScopedService>,
        params: DispatchParams,
        metadata: DurableTaskMetadata,
    ) -> std::result::Result<TaskRecord, TaskCallError> {
        let id = uuid::Uuid::now_v7().to_string();
        let initialization = self.begin_initialization()?;
        let supervisor = self.clone();
        tokio::spawn(async move {
            let _initialization = initialization;
            supervisor
                .initialize_and_spawn(id, service, params, metadata)
                .await
        })
        .await
        .map_err(TaskCallError::Join)?
    }

    fn begin_initialization(&self) -> std::result::Result<InitializationPermit, TaskCallError> {
        let mut current = self.initialization_state.load(Ordering::Acquire);
        loop {
            if current & INITIALIZATION_SHUTDOWN_BIT != 0 {
                return Err(TaskCallError::ShuttingDown);
            }
            if current == INITIALIZATION_SHUTDOWN_BIT - 1 {
                return Err(TaskCallError::Store(TaskStoreError::CorruptData(
                    "REST task initializer count overflow".into(),
                )));
            }
            match self.initialization_state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(InitializationPermit {
                        state: Arc::clone(&self.initialization_state),
                        finished: Arc::clone(&self.initialization_finished),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.initialization_state.load(Ordering::Acquire) & INITIALIZATION_SHUTDOWN_BIT != 0
    }

    fn initializer_count(&self) -> usize {
        self.initialization_state.load(Ordering::Acquire) & !INITIALIZATION_SHUTDOWN_BIT
    }

    /// Initialization runs in an owned task so an HTTP disconnect cannot drop
    /// the future between durable create, controller attach, and dispatch spawn.
    async fn initialize_and_spawn(
        &self,
        id: String,
        service: Arc<OwnerScopedService>,
        params: DispatchParams,
        metadata: DurableTaskMetadata,
    ) -> std::result::Result<TaskRecord, TaskCallError> {
        if self.is_shutting_down() {
            return Err(TaskCallError::ShuttingDown);
        }
        let task = NewTask {
            id: id.clone(),
            kind: TaskKind::Dispatch,
            origin: TaskOrigin::RestAsync,
            task_digest: metadata.task_digest,
            target_key: metadata.target_key,
            created_at: chrono::Utc::now(),
        };
        let created = self
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await?;
        let created_revision = created.revision;
        let created_epoch = created.executor_epoch;
        if self.is_shutting_down() {
            self.interrupt_failed_initialization(
                &id,
                created_revision,
                created_epoch,
                created_epoch,
            )
            .await;
            return Err(TaskCallError::ShuttingDown);
        }
        let Some(epoch) = created.executor_epoch.checked_add(1) else {
            self.interrupt_failed_initialization(
                &id,
                created_revision,
                created_epoch,
                created_epoch,
            )
            .await;
            return Err(TaskCallError::Store(TaskStoreError::CorruptData(
                "executor epoch overflow before REST attach".into(),
            )));
        };
        let cancel = CancellationToken::new();
        self.live_tokens.insert((id.clone(), epoch), cancel.clone());
        let controller = ControllerRef::InProcess {
            instance_id: self.instance_id.to_string(),
        };
        let attached_id = id.clone();
        let attached = match self
            .call(move |store| {
                store.attach_controller(
                    LOCAL_TASK_OWNER,
                    &attached_id,
                    created.revision,
                    created.executor_epoch,
                    controller,
                    None,
                    chrono::Utc::now(),
                )
            })
            .await
        {
            Ok(attached) => attached,
            Err(error) => {
                self.live_tokens.remove(&(id.clone(), epoch));
                self.interrupt_failed_initialization(&id, created_revision, created_epoch, epoch)
                    .await;
                return Err(error);
            }
        };
        if attached.executor_epoch != epoch {
            self.live_tokens.remove(&(id.clone(), epoch));
            self.interrupt_failed_initialization(&id, created_revision, created_epoch, epoch)
                .await;
            return Err(TaskCallError::Contended { id });
        }
        if self.is_shutting_down() {
            cancel.cancel();
            self.live_tokens.remove(&(id.clone(), epoch));
            self.interrupt_failed_initialization(&id, created_revision, created_epoch, epoch)
                .await;
            return Err(TaskCallError::ShuttingDown);
        }

        let output_path = self.output_path(&id);
        let spawned = self.spawn_supervised_dispatch(id.clone(), epoch, output_path, async move {
            service.dispatch(params, cancel).await
        });
        if !spawned {
            return Err(TaskCallError::Contended { id });
        }

        Ok(attached)
    }

    /// Best-effort cleanup for the create-to-attach handoff. A failed attach
    /// may have committed before returning an error, so cleanup accepts either
    /// the exact snapshot this initializer created or the exact epoch attached
    /// to this server instance. It never borrows a revision from a foreign or
    /// newer controller.
    async fn interrupt_failed_initialization(
        &self,
        id: &str,
        created_revision: u64,
        created_epoch: u64,
        attached_epoch: u64,
    ) {
        for _ in 0..16 {
            let current = match self.get(id).await {
                Ok(Some(record)) => record,
                Ok(None) => return,
                Err(error) => {
                    eprintln!("task {id} initialization cleanup read failed: {error}");
                    return;
                }
            };
            if current.state.is_terminal() {
                return;
            }
            let exact_created = current.state == TaskState::Queued
                && current.revision == created_revision
                && current.executor_epoch == created_epoch
                && current.controller.is_none();
            let exact_attached = matches!(
                &current.controller,
                Some(ControllerRef::InProcess { instance_id })
                    if instance_id == self.instance_id.as_ref()
            ) && current.executor_epoch == attached_epoch
                && matches!(current.state, TaskState::Running | TaskState::Cancelling);
            if !exact_created && !exact_attached {
                return;
            }

            let task_id = current.id.clone();
            let result = self
                .call(move |store| {
                    store.interrupt(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        current.revision,
                        current.executor_epoch,
                        FailureCode::ControlUnavailable,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(_) => return,
                Err(TaskCallError::Store(TaskStoreError::Conflict { .. })) => continue,
                Err(TaskCallError::Store(TaskStoreError::InvalidState { .. })) => continue,
                Err(error) => {
                    eprintln!("task {id} initialization cleanup failed: {error}");
                    return;
                }
            }
        }
        eprintln!("task {id} initialization cleanup remained contended");
    }

    fn spawn_supervised_dispatch<F>(
        &self,
        id: String,
        epoch: u64,
        output_path: Option<PathBuf>,
        dispatch: F,
    ) -> bool
    where
        F: std::future::Future<Output = anyhow::Result<DispatchOutcome>> + Send + 'static,
    {
        let key = (id.clone(), epoch);
        let runtime_id = uuid::Uuid::now_v7();
        match self.live_dispatches.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(RuntimeDispatch {
                    runtime_id,
                    task_id: None,
                    abort: None,
                    abort_requested: false,
                });
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                eprintln!(
                    "task {} epoch {} rejected duplicate runtime dispatch",
                    key.0, key.1
                );
                return false;
            }
        }

        let dispatch_id = id.clone();
        let supervisor = self.clone();
        let (start_tx, start_rx) = oneshot::channel();
        // Catch the dispatch panic in the same Tokio task we track. A nested
        // `tokio::spawn` would detach its child when this outer task is aborted,
        // allowing provider work to outlive the supervisor lock.
        let worker = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return;
            }
            let (settlement, output) = match AssertUnwindSafe(dispatch).catch_unwind().await {
                Ok(result) => {
                    if let Err(error) = &result {
                        // Raw errors are diagnostic output only; never durable task data.
                        eprintln!("task {dispatch_id} failed: {error:#}");
                    }
                    let output = result
                        .as_ref()
                        .ok()
                        .filter(|outcome| outcome.record.status == RunStatus::Success)
                        .and_then(|outcome| outcome.output.clone());
                    (settlement_from_dispatch_result(result), output)
                }
                Err(_) => {
                    eprintln!("task {dispatch_id} dispatch future panicked");
                    (
                        TaskSettlement::Failed {
                            code: FailureCode::Internal,
                            ledger_run_id: None,
                        },
                        None,
                    )
                }
            };
            if let (Some(path), Some(output)) = (output_path, output) {
                if let Err(error) = write_private_task_output(path, output).await {
                    eprintln!("task {dispatch_id} output artifact failed: {error:#}");
                }
            }
            supervisor
                .settle_with_retry(&dispatch_id, epoch, settlement)
                .await;
        });

        let task_id = worker.id();
        let abort = worker.abort_handle();
        let abort_immediately = {
            let mut runtime = self.live_dispatches.get_mut(&key);
            match runtime.as_deref_mut() {
                Some(runtime) if runtime.runtime_id == runtime_id => {
                    runtime.task_id = Some(task_id);
                    runtime.abort = Some(abort.clone());
                    Some(runtime.abort_requested)
                }
                _ => None,
            }
        };
        let Some(abort_immediately) = abort_immediately else {
            worker.abort();
            self.remove_runtime_reservation(&key, runtime_id);
            return false;
        };
        if abort_immediately {
            abort.abort();
        }

        let cleanup = self.clone();
        let token_key = key.clone();
        tokio::spawn(async move {
            let _ = worker.await;
            let removed_current = match cleanup.live_dispatches.entry(key) {
                dashmap::mapref::entry::Entry::Occupied(entry)
                    if entry.get().runtime_id == runtime_id
                        && entry.get().task_id == Some(task_id) =>
                {
                    entry.remove();
                    true
                }
                _ => false,
            };
            if removed_current {
                cleanup.live_tokens.remove(&token_key);
                cleanup.dispatch_finished.notify_one();
            }
        });
        let _ = start_tx.send(());
        true
    }

    fn remove_runtime_reservation(&self, key: &(String, u64), runtime_id: uuid::Uuid) {
        let removed = match self.live_dispatches.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry)
                if entry.get().runtime_id == runtime_id =>
            {
                entry.remove();
                true
            }
            _ => false,
        };
        if removed {
            self.dispatch_finished.notify_one();
        }
    }

    /// Re-read the authoritative revision immediately before settlement. A
    /// stale worker must never borrow the revision of a newer executor epoch.
    async fn settle_current(
        &self,
        id: &str,
        worker_epoch: u64,
        settlement: TaskSettlement,
    ) -> std::result::Result<Option<TaskRecord>, TaskCallError> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(None);
            };
            if record.state.is_terminal() || record.executor_epoch != worker_epoch {
                return Ok(Some(record));
            }
            let task_id = record.id.clone();
            let settlement = settlement.clone();
            let result = self
                .call(move |store| {
                    store.settle(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        worker_epoch,
                        settlement,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(settled) => return Ok(Some(settled)),
                Err(TaskCallError::Store(TaskStoreError::Conflict { .. })) => continue,
                Err(error @ TaskCallError::Store(TaskStoreError::InvalidState { .. })) => {
                    let current = self.get(id).await?;
                    if current.as_ref().is_none_or(|record| {
                        record.state.is_terminal() || record.executor_epoch != worker_epoch
                    }) {
                        return Ok(current);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(TaskCallError::Contended { id: id.into() })
    }

    /// Keep the exact executor epoch supervised while durable settlement is
    /// temporarily unavailable. Backoff is bounded, but retries continue until
    /// settlement succeeds (or shutdown aborts and drops this tracked future).
    async fn settle_with_retry(&self, id: &str, worker_epoch: u64, settlement: TaskSettlement) {
        let mut delay = SETTLEMENT_RETRY_INITIAL_DELAY;
        loop {
            match self
                .settle_current(id, worker_epoch, settlement.clone())
                .await
            {
                Ok(_) => return,
                Err(error) => {
                    eprintln!("task {id} metadata settlement retry: {error}");
                }
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(SETTLEMENT_RETRY_MAX_DELAY);
        }
    }

    async fn cancel(&self, id: &str) -> std::result::Result<Option<TaskRecord>, TaskCallError> {
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                return Ok(None);
            };
            if !is_local_rest_dispatch(&record) {
                return Ok(None);
            }
            if record.state.is_terminal() {
                return Ok(Some(record));
            }
            let token_epoch = match record.state {
                // Queued has no controller yet. It is always safe to cancel by
                // CAS; a pre-registered initializer token is optional.
                TaskState::Queued => record.executor_epoch.checked_add(1),
                TaskState::Running | TaskState::Cancelling => match &record.controller {
                    Some(ControllerRef::InProcess { instance_id })
                        if instance_id == self.instance_id.as_ref() =>
                    {
                        Some(record.executor_epoch)
                    }
                    _ => return Err(TaskCallError::ControlUnavailable { id: id.into() }),
                },
                _ => return Ok(Some(record)),
            };
            let token_key = token_epoch.map(|epoch| (record.id.clone(), epoch));
            if record.state != TaskState::Queued
                && token_key
                    .as_ref()
                    .is_none_or(|key| !self.live_tokens.contains_key(key))
            {
                return Err(TaskCallError::ControlUnavailable { id: id.into() });
            }
            if record.state == TaskState::Cancelling {
                if let Some(key) = &token_key {
                    if let Some(token) = self.live_tokens.get(key) {
                        token.cancel();
                    }
                }
                return Ok(Some(record));
            }

            let task_id = record.id.clone();
            let result = self
                .call(move |store| {
                    store.request_cancel(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        record.executor_epoch,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(cancelling) => {
                    if let Some(key) = &token_key {
                        if let Some(token) = self.live_tokens.get(key) {
                            token.cancel();
                        }
                    }
                    return Ok(Some(cancelling));
                }
                Err(TaskCallError::Store(TaskStoreError::Conflict { .. })) => continue,
                Err(error @ TaskCallError::Store(TaskStoreError::InvalidState { .. })) => {
                    let current = self.get(id).await?;
                    match current {
                        None => return Ok(None),
                        Some(record) if record.state.is_terminal() => return Ok(Some(record)),
                        Some(record)
                            if record.state == TaskState::Cancelling
                                && Some(record.executor_epoch) == token_epoch
                                && matches!(
                                    &record.controller,
                                    Some(ControllerRef::InProcess { instance_id })
                                        if instance_id == self.instance_id.as_ref()
                                ) =>
                        {
                            if let Some(key) = &token_key {
                                if let Some(token) = self.live_tokens.get(key) {
                                    token.cancel();
                                }
                            }
                            return Ok(Some(record));
                        }
                        Some(_) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(TaskCallError::Contended { id: id.into() })
    }

    /// Stop new initialization, durably request cancellation for every active
    /// REST task owned by this server, signal its exact epoch token, and wait a
    /// bounded interval for normal settlement. Tasks that do not drain before
    /// the deadline are interrupted only after an exact ownership re-check.
    async fn shutdown_and_drain(&self, budget: Duration) -> std::result::Result<(), TaskCallError> {
        self.initialization_state
            .fetch_or(INITIALIZATION_SHUTDOWN_BIT, Ordering::AcqRel);
        let deadline = tokio::time::Instant::now() + budget;
        let mut first_metadata_error = None;

        loop {
            let owned = match self.owned_active_task_keys().await {
                Ok(owned) => owned,
                Err(error) => {
                    first_metadata_error = Some(error);
                    break;
                }
            };
            for (id, epoch) in &owned {
                if let Err(error) = self.request_shutdown_cancel(id, *epoch).await {
                    if first_metadata_error.is_none() {
                        first_metadata_error = Some(error);
                    }
                }
            }
            if first_metadata_error.is_some() {
                break;
            }
            if let Err(error) = self.prune_inactive_live_tokens().await {
                first_metadata_error = Some(error);
                break;
            }

            let remaining = match self.owned_active_task_keys().await {
                Ok(remaining) => remaining,
                Err(error) => {
                    first_metadata_error = Some(error);
                    break;
                }
            };
            if self.initializer_count() == 0
                && remaining.is_empty()
                && self.live_tokens.is_empty()
                && self.live_dispatches.is_empty()
            {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }

            tokio::time::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(10)),
            )
            .await;
        }

        // This is a finally path: no metadata failure may release the server
        // lock while an initializer or dispatch future can still run. Close the
        // spawn race, signal every process-local token, make a best-effort exact
        // interruption pass, then abort and await every opaque runtime handle.
        self.wait_for_initializers().await;
        self.cancel_all_live_tokens();
        match self.owned_active_task_keys().await {
            Ok(remaining) => {
                for (id, epoch) in remaining {
                    if let Err(error) = self.interrupt_shutdown_task(&id, epoch).await {
                        if first_metadata_error.is_none() {
                            first_metadata_error = Some(error);
                        }
                    }
                }
            }
            Err(error) => {
                if first_metadata_error.is_none() {
                    first_metadata_error = Some(error);
                }
            }
        }
        self.abort_live_dispatches();
        self.wait_for_dispatches_dropped().await;
        self.live_tokens.clear();

        match first_metadata_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn wait_for_initializers(&self) {
        loop {
            if self.initializer_count() == 0 {
                return;
            }
            self.initialization_finished.notified().await;
        }
    }

    fn abort_live_dispatches(&self) {
        // Abort in place; do not clone a snapshot of opaque runtime handles.
        for mut dispatch in self.live_dispatches.iter_mut() {
            if let Some(abort) = &dispatch.abort {
                abort.abort();
            } else {
                dispatch.abort_requested = true;
            }
        }
    }

    fn cancel_all_live_tokens(&self) {
        for token in self.live_tokens.iter() {
            token.cancel();
        }
    }

    async fn wait_for_dispatches_dropped(&self) {
        loop {
            if self.live_dispatches.is_empty() {
                return;
            }
            // Watchers use notify_one, which retains a permit if completion
            // races this wait between the emptiness check and registration.
            self.dispatch_finished.notified().await;
        }
    }

    async fn owned_active_task_keys(
        &self,
    ) -> std::result::Result<Vec<(String, u64)>, TaskCallError> {
        let mut cursor = None;
        let mut owned = Vec::new();
        loop {
            let query = TaskQuery {
                kinds: vec![TaskKind::Dispatch],
                origins: vec![TaskOrigin::RestAsync],
                states: vec![TaskState::Queued, TaskState::Running, TaskState::Cancelling],
                limit: 1_000,
                cursor,
            };
            let page = self
                .call(move |store| store.list(LOCAL_TASK_OWNER, &query))
                .await?;
            for record in page.items.into_iter().filter(is_local_rest_dispatch) {
                let key = match record.state {
                    TaskState::Queued => record
                        .executor_epoch
                        .checked_add(1)
                        .map(|epoch| (record.id.clone(), epoch))
                        .filter(|key| self.live_tokens.contains_key(key)),
                    TaskState::Running | TaskState::Cancelling
                        if matches!(
                            &record.controller,
                            Some(ControllerRef::InProcess { instance_id })
                                if instance_id == self.instance_id.as_ref()
                        ) =>
                    {
                        Some((record.id.clone(), record.executor_epoch))
                    }
                    _ => None,
                };
                if let Some(key) = key {
                    owned.push(key);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(owned)
    }

    async fn request_shutdown_cancel(
        &self,
        id: &str,
        owned_epoch: u64,
    ) -> std::result::Result<(), TaskCallError> {
        let token_key = (id.to_owned(), owned_epoch);
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                self.live_tokens.remove(&token_key);
                return Ok(());
            };
            if record.state.is_terminal() {
                self.live_tokens.remove(&token_key);
                return Ok(());
            }
            if !self.shutdown_owns_record(&record, owned_epoch) {
                self.live_tokens.remove(&token_key);
                return Ok(());
            }
            if record.state == TaskState::Cancelling {
                if let Some(token) = self.live_tokens.get(&token_key) {
                    token.cancel();
                }
                return Ok(());
            }

            let task_id = record.id.clone();
            let result = self
                .call(move |store| {
                    store.request_cancel(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        record.executor_epoch,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(_) => {
                    if let Some(token) = self.live_tokens.get(&token_key) {
                        token.cancel();
                    }
                    return Ok(());
                }
                Err(TaskCallError::Store(TaskStoreError::Conflict { .. }))
                | Err(TaskCallError::Store(TaskStoreError::InvalidState { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(TaskCallError::Contended { id: id.into() })
    }

    async fn interrupt_shutdown_task(
        &self,
        id: &str,
        owned_epoch: u64,
    ) -> std::result::Result<(), TaskCallError> {
        let token_key = (id.to_owned(), owned_epoch);
        for _ in 0..16 {
            let Some(record) = self.get(id).await? else {
                self.live_tokens.remove(&token_key);
                return Ok(());
            };
            if record.state.is_terminal() {
                self.live_tokens.remove(&token_key);
                return Ok(());
            }
            if !self.shutdown_owns_record(&record, owned_epoch) {
                self.live_tokens.remove(&token_key);
                return Ok(());
            }
            if let Some(token) = self.live_tokens.get(&token_key) {
                token.cancel();
            }
            let task_id = record.id.clone();
            let result = self
                .call(move |store| {
                    store.interrupt(
                        LOCAL_TASK_OWNER,
                        &task_id,
                        record.revision,
                        record.executor_epoch,
                        FailureCode::ControlUnavailable,
                        chrono::Utc::now(),
                    )
                })
                .await;
            match result {
                Ok(_) => {
                    self.live_tokens.remove(&token_key);
                    return Ok(());
                }
                Err(TaskCallError::Store(TaskStoreError::Conflict { .. }))
                | Err(TaskCallError::Store(TaskStoreError::InvalidState { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(TaskCallError::Contended { id: id.into() })
    }

    fn shutdown_owns_record(&self, record: &TaskRecord, owned_epoch: u64) -> bool {
        if !is_local_rest_dispatch(record) {
            return false;
        }
        match record.state {
            TaskState::Queued => {
                record.executor_epoch.checked_add(1) == Some(owned_epoch)
                    && self
                        .live_tokens
                        .contains_key(&(record.id.clone(), owned_epoch))
            }
            TaskState::Running | TaskState::Cancelling => {
                record.executor_epoch == owned_epoch
                    && matches!(
                        &record.controller,
                        Some(ControllerRef::InProcess { instance_id })
                            if instance_id == self.instance_id.as_ref()
                    )
            }
            _ => false,
        }
    }

    async fn prune_inactive_live_tokens(&self) -> std::result::Result<(), TaskCallError> {
        let keys = self
            .live_tokens
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in keys {
            let keep = self
                .get(&key.0)
                .await?
                .is_some_and(|record| self.shutdown_owns_record(&record, key.1));
            if !keep {
                self.live_tokens.remove(&key);
            }
        }
        Ok(())
    }
}

fn artifact_segment(domain: &str, value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vyane-task-artifact-v1\0");
    digest.update(domain.as_bytes());
    digest.update(b"\0");
    digest.update(value.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn settlement_from_dispatch_result(result: anyhow::Result<DispatchOutcome>) -> TaskSettlement {
    let Ok(outcome) = result else {
        return TaskSettlement::Failed {
            code: FailureCode::DispatchFailed,
            ledger_run_id: None,
        };
    };
    let ledger_run_id = (!outcome.record.run_id.is_empty()).then_some(outcome.record.run_id);
    match outcome.record.status {
        RunStatus::Success => TaskSettlement::Succeeded { ledger_run_id },
        RunStatus::Error => TaskSettlement::Failed {
            code: FailureCode::DispatchFailed,
            ledger_run_id,
        },
        RunStatus::Timeout => TaskSettlement::TimedOut { ledger_run_id },
        RunStatus::Cancelled => TaskSettlement::Cancelled { ledger_run_id },
    }
}

async fn write_private_task_output(path: PathBuf, output: String) -> Result<()> {
    tokio::task::spawn_blocking(move || write_private_task_output_sync(&path, output.as_bytes()))
        .await
        .map_err(|error| anyhow::anyhow!("output artifact worker failed: {error}"))?
}

fn write_private_task_output_sync(path: &FsPath, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("task output path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|error| anyhow::anyhow!("create task output dir {}: {error}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| anyhow::anyhow!("chmod task output dir {}: {error}", parent.display()),
        )?;
    }

    let temp = parent.join(format!(
        ".output.txt.tmp.{}.{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .map_err(|error| anyhow::anyhow!("create task output {}: {error}", temp.display()))?;
    file.write_all(bytes)
        .map_err(|error| anyhow::anyhow!("write task output {}: {error}", temp.display()))?;
    file.sync_all()
        .map_err(|error| anyhow::anyhow!("sync task output {}: {error}", temp.display()))?;
    drop(file);
    std::fs::rename(&temp, path).map_err(|error| {
        anyhow::anyhow!(
            "publish task output {} -> {}: {error}",
            temp.display(),
            path.display()
        )
    })?;
    Ok(())
}

async fn read_task_output(path: PathBuf) -> Result<Option<String>> {
    tokio::task::spawn_blocking(move || match std::fs::read_to_string(&path) {
        Ok(output) => Ok(Some(output)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(anyhow::anyhow!(
            "read task output {}: {error}",
            path.display()
        )),
    })
    .await
    .map_err(|error| anyhow::anyhow!("output artifact reader failed: {error}"))?
}

/// Body for `POST /v1/dispatch`. Field names mirror [`DispatchParams`] minus the
/// fields the server owns (cancellation, runtime config).
#[derive(Debug, Deserialize)]
pub struct DispatchRequest {
    pub task: String,
    /// Profile name or `provider/model`.
    pub target: String,
    #[serde(default)]
    pub workdir: Option<String>,
    /// `read_only` | `write` | `full`. Defaults to `read_only`.
    #[serde(default)]
    pub sandbox: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Each entry is a `key=value` label, matching `--label` on the CLI.
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

/// Body for `POST /v1/broadcast`. Like [`DispatchRequest`] but `targets` is a
/// single comma-separated string, matching the CLI's `--targets` flag.
#[derive(Debug, Deserialize)]
pub struct BroadcastRequest {
    pub task: String,
    /// Comma-separated list; each element is a profile or `provider/model`.
    pub targets: String,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub sandbox: Option<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

/// Query params for `GET /v1/runs`.
#[derive(Debug, Default, Deserialize)]
pub struct RunsQuery {
    /// Max records to return. `None` defaults to 100. `0` is rejected as 400.
    #[serde(default)]
    pub limit: Option<usize>,
    /// `success` | `error` | `timeout` | `cancelled`.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
}

/// Default and max record limits for `GET /v1/runs`.
const DEFAULT_RUN_LIMIT: usize = 100;
const MAX_RUN_LIMIT: usize = 10_000;

/// One redacted row in a `{"items":[...]}` envelope. The local CLI may expose
/// richer owner-requested output, while the generic HTTP boundary never
/// serializes durable prompt/path/label/error fields.
#[derive(Debug, Serialize)]
pub struct BroadcastItem {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<RunView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DispatchResponse {
    pub record: RunView,
    pub output: Option<String>,
}

impl From<DispatchOutcome> for DispatchResponse {
    fn from(outcome: DispatchOutcome) -> Self {
        Self {
            record: RunView::from(outcome.record),
            output: outcome.output,
        }
    }
}

#[derive(Debug, Serialize)]
struct ItemsEnvelope<T: Serialize> {
    items: Vec<T>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct GoalNextActionResponse {
    next_action: GoalNextActionView,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// An API error that maps onto an HTTP status code. The conversion below keeps
/// the mapping in one place: config/resolution errors are caller faults (400),
/// everything else is a server fault (500).
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn from_task_store(error: TaskCallError) -> Self {
        if let TaskCallError::ControlUnavailable { id } = &error {
            return Self {
                status: StatusCode::CONFLICT,
                message: format!("task {id} is controlled by another server instance"),
            };
        }
        if matches!(error, TaskCallError::ShuttingDown) {
            return Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "task supervisor is shutting down".into(),
            };
        }
        eprintln!("task metadata error: {error}");
        Self::internal("task metadata unavailable")
    }

    /// Classify a service error: config/resolution/label-parsing failures are
    /// caller faults (400), everything else is a server fault (500).
    ///
    /// The error chain is logged server-side (stderr) for debugging; only a
    /// generic message reaches the client to avoid leaking internal paths,
    /// endpoint URLs, or secret-resolution details.
    fn from_service_error(e: anyhow::Error) -> Self {
        let msg = e.to_string();
        let display = format!("{e:#}");
        eprintln!("dispatch/broadcast error: {display}");
        if is_caller_fault(&msg) {
            Self::bad_request("invalid dispatch request")
        } else {
            Self::internal("internal error")
        }
    }
}

/// Heuristic: a resolution/config error message mentions profiles, providers,
/// endpoints, labels, or "not found" — all caller-input problems. A genuine
/// server fault (transport, auth upstream, spawn) does not.
fn is_caller_fault(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("profile")
        || lower.contains("provider")
        || lower.contains("endpoint")
        || lower.contains("label")
        || lower.contains("not found")
        || lower.contains("no such")
        || lower.contains("missing")
        || lower.contains("invalid")
        || lower.contains("targets must")
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

/// Maximum request body size (16 MiB). Large enough for substantial task/system
/// prompts, small enough to prevent a single client from OOMing the process.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const REST_SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(10);

async fn require_api_bearer(
    State(token): State<ApiBearerToken>,
    request: Request,
    next: Next,
) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| crate::daemon::bearer_tokens_equal(provided, token.0.as_ref()));
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

async fn require_loopback_authority(request: Request, next: Next) -> Response {
    let header_authority = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    let uri_authority = request.uri().authority().map(|value| value.as_str());
    let authority_present = header_authority.is_some() || uri_authority.is_some();
    let authority_allowed = authority_present
        && header_authority.is_none_or(authority_is_loopback)
        && uri_authority.is_none_or(authority_is_loopback);
    let origin_allowed = request
        .headers()
        .get(header::ORIGIN)
        .is_none_or(|value| value.to_str().ok().is_some_and(origin_is_loopback));
    let fetch_site_allowed = request.headers().get("sec-fetch-site").is_none_or(|value| {
        value
            .to_str()
            .ok()
            .is_some_and(|value| !value.eq_ignore_ascii_case("cross-site"))
    });
    if !authority_allowed || !origin_allowed || !fetch_site_allowed {
        return StatusCode::FORBIDDEN.into_response();
    }
    next.run(request).await
}

fn authority_is_loopback(raw: &str) -> bool {
    if raw.contains('@') {
        return false;
    }
    if let Some(bracketed) = raw.strip_prefix('[') {
        let Some(close) = bracketed.find(']') else {
            return false;
        };
        let host = &bracketed[..close];
        let suffix = &bracketed[close + 1..];
        let port_valid = suffix.is_empty()
            || suffix
                .strip_prefix(':')
                .is_some_and(|port| !port.is_empty() && port.parse::<u16>().is_ok());
        return port_valid
            && host
                .parse::<std::net::Ipv6Addr>()
                .is_ok_and(|address| address.is_loopback());
    }
    if raw.contains('[') || raw.contains(']') {
        return false;
    }
    let Ok(authority) = raw.parse::<axum::http::uri::Authority>() else {
        return false;
    };
    let host = authority.host();
    let suffix = &raw[host.len()..];
    let port_valid = suffix.is_empty()
        || suffix
            .strip_prefix(':')
            .is_some_and(|port| !port.is_empty() && port.parse::<u16>().is_ok());
    if !port_valid {
        return false;
    }
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn origin_is_loopback(raw: &str) -> bool {
    let Ok(uri) = raw.parse::<axum::http::Uri>() else {
        return false;
    };
    matches!(uri.scheme_str(), Some("http" | "https"))
        && uri
            .authority()
            .is_some_and(|authority| authority_is_loopback(authority.as_str()))
        && matches!(uri.path(), "" | "/")
        && uri.query().is_none()
}

async fn goal_continuity_next(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    if query.is_some() {
        return no_store(
            ApiError::bad_request("goal continuity-next does not accept query parameters")
                .into_response(),
        );
    }
    if !body.is_empty() {
        return no_store(
            ApiError::bad_request("goal continuity-next does not accept a request body")
                .into_response(),
        );
    }

    let goals = Arc::clone(&state.goals);
    let result = tokio::task::spawn_blocking(move || goals.continuity_next(&id)).await;
    let response = match result {
        Ok(Ok(next_action)) => Json(GoalNextActionResponse { next_action }).into_response(),
        Ok(Err(GoalReadError::InvalidGoalId)) => {
            ApiError::bad_request("invalid goal id").into_response()
        }
        Ok(Err(GoalReadError::NotFound)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: "goal not found".into(),
            }),
        )
            .into_response(),
        Ok(Err(GoalReadError::ContinuityUnavailable)) => (
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "goal continuity is unavailable".into(),
            }),
        )
            .into_response(),
        Ok(Err(GoalReadError::Unavailable)) => {
            eprintln!("goal read service unavailable");
            ApiError::internal("goal read unavailable").into_response()
        }
        Err(error) => {
            eprintln!("goal read worker failed: {error}");
            ApiError::internal("goal read unavailable").into_response()
        }
    };
    no_store(response)
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn goal_read_method_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(header::ALLOW, HeaderValue::from_static("GET"))],
    )
        .into_response()
}

async fn no_store_goal_read_responses(request: Request, next: Next) -> Response {
    let path = request.uri().path();
    let is_goal_read = path
        .strip_prefix("/v1/goals/")
        .and_then(|suffix| suffix.strip_suffix("/continuity-next"))
        .is_some_and(|goal_id| !goal_id.is_empty() && !goal_id.contains('/'));
    let response = next.run(request).await;
    if is_goal_read {
        no_store(response)
    } else {
        response
    }
}

fn router_from_parts(
    service: VyaneService,
    tasks: TaskSupervisor,
    bearer_token: &str,
) -> Result<Router> {
    let goals = service.goal_reader(OwnerContext::single_user_local())?;
    let state = ApiState {
        service: Arc::new(service.scope(OwnerContext::single_user_local())),
        tasks,
        goals: Arc::new(goals),
    };

    Ok(Router::new()
        .route("/v1/health", get(health))
        .route("/v1/dispatch", post(dispatch))
        .route("/v1/dispatch/stream", post(dispatch_stream))
        .route("/v1/broadcast", post(broadcast))
        .route("/v1/tasks", post(submit_task).get(list_tasks))
        .route("/v1/tasks/{id}", get(get_task))
        .route("/v1/tasks/{id}/output", get(get_task_output))
        .route("/v1/tasks/{id}/cancel", post(cancel_task))
        .route("/v1/runs", get(runs))
        .route("/v1/sessions", get(sessions))
        .route(
            "/v1/goals/{id}/continuity-next",
            on(MethodFilter::GET, goal_continuity_next)
                .on(MethodFilter::HEAD, goal_read_method_not_allowed)
                .fallback(goal_read_method_not_allowed),
        )
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
        .layer(middleware::from_fn(require_loopback_authority))
        .layer(middleware::from_fn_with_state(
            ApiBearerToken(Arc::from(bearer_token)),
            require_api_bearer,
        ))
        .layer(middleware::from_fn(no_store_goal_read_responses)))
}

/// Build the bearer-authenticated axum router against this service's durable
/// task database. Embedders supply one 256-bit lowercase-hex capability and
/// must still keep the listener loopback-only. The bundled [`run_server`] path
/// enforces loopback before binding.
#[allow(dead_code)] // Public embedding entrypoint; the CLI server owns recovery separately.
pub async fn build_router(service: VyaneService, bearer_token: &str) -> Result<Router> {
    let task_db_path = service.storage_paths().task_metadata_db_path();
    build_router_with_task_db(service, task_db_path, bearer_token).await
}

/// Build the same authenticated router using an explicit task metadata
/// database path.
///
/// Tests and embedders should prefer this over mutating `VYANE_DATA_DIR`.
#[allow(dead_code)] // Public embedding/test entrypoint; not called by the binary server path.
pub async fn build_router_with_task_db(
    service: VyaneService,
    task_db_path: impl AsRef<FsPath>,
    bearer_token: &str,
) -> Result<Router> {
    crate::daemon::validate_bearer_token(bearer_token)?;
    let tasks = TaskSupervisor::open(task_db_path)
        .await
        .map_err(|error| anyhow::anyhow!("open task metadata database: {error}"))?;
    router_from_parts(service, tasks, bearer_token)
}

/// Run the API server until interrupted. The caller loads the service and hands
/// it in; this function owns the listener and graceful shutdown.
pub async fn run_server(service: VyaneService, addr: SocketAddr) -> Result<()> {
    if !addr.ip().is_loopback() {
        anyhow::bail!("vyane serve only accepts loopback listen addresses");
    }
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("bind {addr}: {e}"))?;
    // Binding is intentionally the first server-side operation. If the address
    // is unavailable, this process must not recover or otherwise mutate tasks.
    // A per-data-dir advisory lock also prevents a second server on a different
    // address from interrupting the first server's live in-process tasks.
    let _supervisor_lock = acquire_task_supervisor_lock(
        &service
            .storage_paths()
            .data_dir
            .join("task-supervisor.lock"),
    )?;
    let token = crate::daemon::generate_bearer_token()?;
    let token_path = service.storage_paths().data_dir.join("serve.token");
    let _token_publication = ApiTokenPublication {
        path: token_path.clone(),
        token: token.clone(),
    };
    // Arm exact-generation cleanup before publication: the atomic helper can
    // report a late directory-sync error after rename has made the token
    // visible. Returning through `?` must still remove that generation.
    crate::daemon::write_private_atomic(&token_path, token.as_bytes())?;
    let task_db_path = service.storage_paths().task_metadata_db_path();
    let tasks = TaskSupervisor::open(task_db_path)
        .await
        .map_err(|error| anyhow::anyhow!("open task metadata database: {error}"))?;
    let recovered = tasks
        .recover_interrupted()
        .await
        .map_err(|error| anyhow::anyhow!("recover interrupted REST tasks: {error}"))?;
    if recovered > 0 {
        tracing::warn!(recovered, "marked abandoned REST tasks interrupted");
    }
    let router = router_from_parts(service, tasks.clone(), &token)?;
    eprintln!(
        "vyane serve listening on {addr}; bearer token file: {}",
        token_path.display()
    );
    let serve_result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    // The supervisor lock is deliberately still held while in-process work is
    // cancelled and drained. A replacement server therefore cannot recover or
    // claim this database while the old server's workers are still settling.
    let drain_result = tasks.shutdown_and_drain(REST_SHUTDOWN_DRAIN_BUDGET).await;
    serve_result.map_err(|e| anyhow::anyhow!("serve {addr}: {e}"))?;
    drain_result.map_err(|error| anyhow::anyhow!("drain REST tasks during shutdown: {error}"))?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn dispatch(
    State(state): State<ApiState>,
    Json(req): Json<DispatchRequest>,
) -> Result<Json<DispatchResponse>, ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    // Validate label shape up front (mirrors the CLI's input-phase check) so a
    // malformed `key=value` is rejected before any dispatch work begins.
    let _ = parse_labels(labels.clone()).map_err(|error| {
        eprintln!("dispatch label error: {error:#}");
        ApiError::bad_request("invalid dispatch request")
    })?;

    let params = DispatchParams {
        task: req.task,
        target: req.target,
        workdir: req.workdir.map(PathBuf::from),
        sandbox,
        session: req.session,
        system: req.system,
        timeout_secs: req.timeout_secs,
        labels,
    };

    // v1: a fresh, never-cancelled token. Timeout-to-cancel wiring is a future
    // concern; the kernel already enforces `timeout_secs` per attempt.
    let cancel = CancellationToken::new();
    let outcome = state
        .service
        .dispatch(params, cancel)
        .await
        .map_err(ApiError::from_service_error)?;
    Ok(Json(DispatchResponse::from(outcome)))
}

/// SSE event type sent over the streaming endpoint.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SsePayload {
    Delta {
        text: String,
    },
    ReasoningDelta {
        text: String,
    },
    ToolUse {
        name: String,
        summary: String,
    },
    Finished {
        record: Box<RunView>,
        output: Option<String>,
    },
    Unsupported,
}

async fn send_sse_terminal(tx: &mpsc::Sender<SsePayload>, payload: SsePayload) {
    // Terminal delivery is reliable while the receiver remains connected. A
    // bounded channel still caps memory; backpressure delays only this producer
    // task instead of silently converting a completed stream into bare EOF.
    let _ = tx.send(payload).await;
}

/// `POST /v1/dispatch/stream` — dispatch a task and stream deltas as
/// Server-Sent Events. Each event's `data` field is a JSON object with a
/// `type` discriminator: `delta`, `reasoning_delta`, `tool_use`, `finished`,
/// or `unsupported`.
///
/// Works for one direct-HTTP or CLI-harness target, with no failover or session.
/// When the selected adapter declines streaming, an `unsupported` event is sent
/// and the caller should retry with the non-streaming `/v1/dispatch` endpoint.
async fn dispatch_stream(
    State(state): State<ApiState>,
    Json(req): Json<DispatchRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    let _ = parse_labels(labels.clone()).map_err(|error| {
        eprintln!("stream dispatch label error: {error:#}");
        ApiError::bad_request("invalid dispatch request")
    })?;

    let has_session = req.session.is_some();
    if has_session {
        return Err(ApiError::bad_request(
            "streaming does not support sessions; use /v1/dispatch instead",
        ));
    }
    let selector = req.target.clone();
    let stream_params = DispatchParams {
        task: req.task.clone(),
        target: req.target.clone(),
        workdir: req.workdir.clone().map(PathBuf::from),
        sandbox,
        session: None,
        system: req.system.clone(),
        timeout_secs: req.timeout_secs,
        labels: labels.clone(),
    };
    let mut task = vyane_service::build_task_spec(
        req.task,
        req.workdir.map(PathBuf::from),
        sandbox,
        req.system,
        req.timeout_secs,
        labels,
    )
    .map_err(|error| {
        eprintln!("stream dispatch request error: {error:#}");
        ApiError::bad_request("invalid dispatch request")
    })?;
    validate_external_task_labels(&task)?;
    let plan = state
        .service
        .plan_dispatch(&selector, &mut task)
        .map_err(|error| {
            eprintln!("stream route error: {error:#}");
            ApiError::bad_request("invalid dispatch request")
        })?;
    let bound = streamable_bound(&plan.chain, false)?;

    let cancel = CancellationToken::new();
    let service = Arc::clone(&state.service);

    // Bridge the callback-based dispatch_stream to an SSE stream via a channel.
    let (tx, mut rx) = mpsc::channel::<SsePayload>(64);

    tokio::spawn(async move {
        let tx = tx;
        let event_tx = tx.clone();
        let outcome = service
            .dispatch_stream(stream_params, cancel, move |event| {
                let payload = match event {
                    StreamDispatchEvent::Delta(text) => SsePayload::Delta { text },
                    StreamDispatchEvent::ReasoningDelta(text) => {
                        SsePayload::ReasoningDelta { text }
                    }
                    StreamDispatchEvent::ToolUse { name, summary } => {
                        SsePayload::ToolUse { name, summary }
                    }
                };
                // Best-effort send: if the client disconnected (receiver dropped),
                // the error is silently ignored — the dispatch continues and the
                // RunRecord is still ledger-appended by the kernel.
                let _ = event_tx.try_send(payload);
            })
            .await;

        let final_payload = match outcome {
            Ok(None) => SsePayload::Unsupported,
            Ok(Some(outcome)) => SsePayload::Finished {
                record: Box::new(RunView::from(outcome.record)),
                output: outcome.output,
            },
            Err(e) => {
                eprintln!("dispatch_stream error: {e:#}");
                SsePayload::Finished {
                    record: Box::new(RunView::from(vyane_core::RunRecord {
                        run_id: String::new(),
                        owner: "local".into(),
                        started_at: chrono::Utc::now(),
                        finished_at: chrono::Utc::now(),
                        task_digest: String::new(),
                        task_preview: None,
                        workdir: None,
                        sandbox: task.sandbox,
                        target: bound.target.clone(),
                        transport: bound.transport,
                        attempts: vec![],
                        status: RunStatus::Error,
                        usage: None,
                        cost_usd: None,
                        session_id: None,
                        output_chars: None,
                        error: Some(e.to_string()),
                        labels: task.labels.clone(),
                    })),
                    output: None,
                }
            }
        };
        // Unlike observational deltas, the terminal payload is part of the API
        // contract. Wait for queue capacity so a slow connected client still
        // receives the RunRecord (or the explicit unsupported marker).
        send_sse_terminal(&tx, final_payload).await;
    });

    // Convert the receiver into a futures::Stream of SSE Events.
    let stream = async_stream::stream! {
        while let Some(payload) = rx.recv().await {
            let json = serde_json::to_string(&payload).unwrap_or_else(|_| {
                r#"{"type":"delta","text":"(serialization error)"}"#.to_string()
            });
            yield Ok::<Event, std::convert::Infallible>(Event::default().data(json));
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn validate_external_task_labels(task: &vyane_core::TaskSpec) -> Result<(), ApiError> {
    vyane_service::validate_user_routing_labels(&task.labels).map_err(|error| {
        eprintln!("external task label error: {error:#}");
        ApiError::bad_request("invalid dispatch request")
    })
}

/// Validate the transport-independent streaming constraints shared by the SSE
/// endpoint: exactly one executable target, and no session continuity.
fn streamable_bound(
    chain: &[vyane_core::BoundTarget],
    has_session: bool,
) -> Result<vyane_core::BoundTarget, ApiError> {
    if has_session {
        return Err(ApiError::bad_request(
            "streaming does not support sessions; use /v1/dispatch instead",
        ));
    }

    match chain {
        [bound]
            if matches!(
                bound.transport,
                vyane_core::AdapterTransport::DirectHttp | vyane_core::AdapterTransport::CliWrap
            ) =>
        {
            Ok(bound.clone())
        }
        _ => Err(ApiError::bad_request(
            "streaming requires a single target (no failover)",
        )),
    }
}

async fn broadcast(
    State(state): State<ApiState>,
    Json(req): Json<BroadcastRequest>,
) -> Result<Json<ItemsEnvelope<BroadcastItem>>, ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    let _ = parse_labels(labels.clone()).map_err(|error| {
        eprintln!("broadcast label error: {error:#}");
        ApiError::bad_request("invalid broadcast request")
    })?;

    let params = BroadcastParams {
        task: req.task,
        targets: req.targets,
        workdir: req.workdir.map(PathBuf::from),
        sandbox,
        system: req.system,
        timeout_secs: req.timeout_secs,
        labels,
    };

    let cancel = CancellationToken::new();
    let results = state.service.broadcast(params, cancel).await.map_err(|e| {
        // Only caller-fault errors (bad targets list, bad labels, bad task
        // spec) reach here — per-target resolution errors are already in
        // the per-item results.
        eprintln!("broadcast setup error: {e:#}");
        ApiError::bad_request("invalid broadcast request")
    })?;

    let items = results
        .into_iter()
        .map(|(target, result)| match result {
            Ok(outcome) => BroadcastItem {
                target,
                record: Some(RunView::from(outcome.record)),
                output: outcome.output,
                error: None,
            },
            Err(e) => {
                // Per-target error: log the full chain server-side, surface a
                // concise message to the client.
                eprintln!("broadcast target `{target}` error: {e:#}");
                BroadcastItem {
                    target,
                    record: None,
                    output: None,
                    error: Some("target execution failed".into()),
                }
            }
        })
        .collect();

    Ok(Json(ItemsEnvelope { items }))
}

async fn runs(
    State(state): State<ApiState>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<ItemsEnvelope<RunView>>, ApiError> {
    let status = match query.status.as_deref() {
        Some(s) => Some(parse_run_status(s)?),
        None => None,
    };

    let limit = match query.limit {
        Some(0) => {
            return Err(ApiError::bad_request(
                "limit must be greater than 0 (omit for default, or use a positive number)",
            ));
        }
        Some(n) if n > MAX_RUN_LIMIT => {
            return Err(ApiError::bad_request(format!(
                "limit {n} exceeds maximum of {MAX_RUN_LIMIT}"
            )));
        }
        Some(n) => Some(n),
        None => Some(DEFAULT_RUN_LIMIT),
    };

    let filter = HistoryFilter {
        limit,
        status,
        provider: query.provider,
    };

    let records = state.service.history_views(filter).await.map_err(|error| {
        eprintln!("run ledger query error: {error:#}");
        ApiError::internal("run ledger unavailable")
    })?;
    Ok(Json(ItemsEnvelope { items: records }))
}

async fn sessions(
    State(state): State<ApiState>,
) -> Result<Json<ItemsEnvelope<SessionView>>, ApiError> {
    let records = state.service.session_views().await.map_err(|error| {
        eprintln!("session snapshot query error: {error:#}");
        ApiError::internal("session storage unavailable")
    })?;
    Ok(Json(ItemsEnvelope { items: records }))
}

/// `POST /v1/tasks` — submit a dispatch asynchronously. Returns the task id
/// immediately; the dispatch runs in a spawned task. Poll with
/// `GET /v1/tasks/:id`.
async fn submit_task(
    State(state): State<ApiState>,
    Json(req): Json<DispatchRequest>,
) -> Result<(StatusCode, Json<TaskEntry>), ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    let _ = parse_labels(labels.clone()).map_err(|error| {
        eprintln!("async dispatch label error: {error:#}");
        ApiError::bad_request("invalid dispatch request")
    })?;

    let params = DispatchParams {
        task: req.task,
        target: req.target,
        workdir: req.workdir.map(PathBuf::from),
        sandbox,
        session: req.session,
        system: req.system,
        timeout_secs: req.timeout_secs,
        labels,
    };
    // Resolve and validate before any caller-controlled target string enters
    // durable metadata. This also turns `auto` into the selected profile key.
    let mut task = state
        .service
        .task_from_dispatch(params.clone())
        .map_err(ApiError::from_service_error)?;
    vyane_service::validate_user_routing_labels(&task.labels)
        .map_err(ApiError::from_service_error)?;
    let plan = state
        .service
        .plan_dispatch(&params.target, &mut task)
        .map_err(ApiError::from_service_error)?;
    let metadata = DurableTaskMetadata {
        task_digest: vyane_kernel::task_digest(&task.prompt),
        target_key: plan.selector,
    };
    let record = state
        .tasks
        .submit(Arc::clone(&state.service), params, metadata)
        .await
        .map_err(ApiError::from_task_store)?;
    Ok((StatusCode::ACCEPTED, Json(record.into())))
}

/// `GET /v1/tasks` — list durable REST task metadata.
async fn list_tasks(
    State(tasks): State<TaskSupervisor>,
) -> Result<Json<ItemsEnvelope<TaskEntry>>, ApiError> {
    let items = tasks
        .list_rest_tasks()
        .await
        .map_err(ApiError::from_task_store)?
        .into_iter()
        .map(TaskEntry::from)
        .collect();
    Ok(Json(ItemsEnvelope { items }))
}

/// `GET /v1/tasks/:id` — get one task's durable metadata.
async fn get_task(
    State(tasks): State<TaskSupervisor>,
    Path(id): Path<String>,
) -> Result<Json<TaskEntry>, ApiError> {
    match tasks.get(&id).await.map_err(ApiError::from_task_store)? {
        Some(record) if is_local_rest_dispatch(&record) => Ok(Json(record.into())),
        None => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} not found"),
        }),
        Some(_) => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} not found"),
        }),
    }
}

/// `GET /v1/tasks/:id/output` — read the mode-0600 result artifact for a
/// successful REST task. Output is deliberately separate from SQLite metadata.
async fn get_task_output(
    State(tasks): State<TaskSupervisor>,
    Path(id): Path<String>,
) -> Result<Json<TaskOutput>, ApiError> {
    let record = tasks
        .get(&id)
        .await
        .map_err(ApiError::from_task_store)?
        .filter(is_local_rest_dispatch)
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} not found"),
        })?;
    if !record.state.is_terminal() {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: format!("task {id} is still {}", record.state),
        });
    }
    if record.state != TaskState::Succeeded {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} has no successful output"),
        });
    }
    let path = tasks
        .output_path(&id)
        .ok_or_else(|| ApiError::internal("task output storage is unavailable for this server"))?;
    let mut output = read_task_output(path).await.map_err(|error| {
        eprintln!("task {id} output read failed: {error:#}");
        ApiError::internal("task output unavailable")
    })?;
    // Pre-owner-namespace REST artifacts were always local and used generated
    // UUID task ids. Restrict the compatibility read to that exact shape; all
    // new writes use opaque owner- and task-qualified path segments.
    if output.is_none() {
        if let Some(legacy) = tasks.legacy_local_output_path(&id) {
            output = read_task_output(legacy).await.map_err(|error| {
                eprintln!("task {id} legacy output read failed: {error:#}");
                ApiError::internal("task output unavailable")
            })?;
        }
    }
    match output {
        Some(output) => Ok(Json(TaskOutput { output })),
        None => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} produced no output artifact"),
        }),
    }
}

/// `POST /v1/tasks/:id/cancel` — durably request cancellation, then signal the
/// exact `(task_id, executor_epoch)` token. Terminal tasks are idempotent no-ops.
async fn cancel_task(
    State(tasks): State<TaskSupervisor>,
    Path(id): Path<String>,
) -> Result<Json<TaskEntry>, ApiError> {
    match tasks.cancel(&id).await.map_err(ApiError::from_task_store)? {
        Some(record) if is_local_rest_dispatch(&record) => Ok(Json(record.into())),
        None => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} not found"),
        }),
        Some(_) => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("task {id} not found"),
        }),
    }
}

/// Parse a sandbox string from a request body. Accepts the snake-case spellings
/// (`read_only`, `write`, `full`) that read naturally in JSON; `read-only` is
/// also accepted so the value round-trips with the kernel's own serialization.
fn parse_sandbox(raw: Option<&str>) -> Result<Sandbox, ApiError> {
    match raw {
        None | Some("read_only") | Some("read-only") => Ok(Sandbox::ReadOnly),
        Some("write") => Ok(Sandbox::Write),
        Some("full") => Ok(Sandbox::Full),
        Some(other) => Err(ApiError::bad_request(format!(
            "unknown sandbox `{other}` (expected read_only, write, or full)"
        ))),
    }
}

/// Parse a run-status filter string. Matches the `snake_case` serialization of
/// [`RunStatus`].
fn parse_run_status(raw: &str) -> Result<RunStatus, ApiError> {
    match raw {
        "success" => Ok(RunStatus::Success),
        "error" => Ok(RunStatus::Error),
        "timeout" => Ok(RunStatus::Timeout),
        "cancelled" => Ok(RunStatus::Cancelled),
        _ => Err(ApiError::bad_request(format!(
            "unknown status `{raw}` (expected success, error, timeout, or cancelled)"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tower::ServiceExt;
    use vyane_config::ProfilePatch;
    use vyane_core::{AuthStyle, ModelId, Protocol, RunQuery};
    use vyane_goal::{
        GoalContinuityMode, GoalContinuityPolicy, GoalExecutionTarget, GoalQuotaEvent, GoalStore,
        NewGoal, SqliteGoalStore, apply_quota_handoff_events,
    };
    use vyane_provider::{Provider, ProviderRegistry};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const API_TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    struct DropFlag(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    struct FlakySettleStore {
        inner: SqliteTaskStore,
        failures_remaining: AtomicUsize,
        settle_calls: AtomicUsize,
    }

    impl FlakySettleStore {
        fn new(inner: SqliteTaskStore, failures: usize) -> Self {
            Self {
                inner,
                failures_remaining: AtomicUsize::new(failures),
                settle_calls: AtomicUsize::new(0),
            }
        }
    }

    impl TaskStore for FlakySettleStore {
        fn create(&self, owner: &str, task: NewTask) -> vyane_task::Result<TaskRecord> {
            self.inner.create(owner, task)
        }

        fn get(&self, owner: &str, id: &str) -> vyane_task::Result<Option<TaskRecord>> {
            self.inner.get(owner, id)
        }

        fn list(&self, owner: &str, query: &TaskQuery) -> vyane_task::Result<vyane_task::TaskPage> {
            self.inner.list(owner, query)
        }

        fn events(&self, owner: &str, id: &str) -> vyane_task::Result<Vec<vyane_task::TaskEvent>> {
            self.inner.events(owner, id)
        }

        fn attach_controller(
            &self,
            owner: &str,
            id: &str,
            expected_revision: u64,
            expected_executor_epoch: u64,
            controller: ControllerRef,
            lease: Option<vyane_task::Lease>,
            at: chrono::DateTime<chrono::Utc>,
        ) -> vyane_task::Result<TaskRecord> {
            self.inner.attach_controller(
                owner,
                id,
                expected_revision,
                expected_executor_epoch,
                controller,
                lease,
                at,
            )
        }

        fn request_cancel(
            &self,
            owner: &str,
            id: &str,
            expected_revision: u64,
            expected_executor_epoch: u64,
            at: chrono::DateTime<chrono::Utc>,
        ) -> vyane_task::Result<TaskRecord> {
            self.inner
                .request_cancel(owner, id, expected_revision, expected_executor_epoch, at)
        }

        fn settle(
            &self,
            owner: &str,
            id: &str,
            expected_revision: u64,
            expected_executor_epoch: u64,
            settlement: TaskSettlement,
            at: chrono::DateTime<chrono::Utc>,
        ) -> vyane_task::Result<TaskRecord> {
            self.settle_calls.fetch_add(1, Ordering::AcqRel);
            if self
                .failures_remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    if remaining > 0 {
                        Some(remaining - 1)
                    } else {
                        None
                    }
                })
                .is_ok()
            {
                return Err(TaskStoreError::CorruptData(
                    "injected temporary settlement failure".into(),
                ));
            }
            self.inner.settle(
                owner,
                id,
                expected_revision,
                expected_executor_epoch,
                settlement,
                at,
            )
        }

        fn interrupt(
            &self,
            owner: &str,
            id: &str,
            expected_revision: u64,
            expected_executor_epoch: u64,
            code: FailureCode,
            at: chrono::DateTime<chrono::Utc>,
        ) -> vyane_task::Result<TaskRecord> {
            self.inner.interrupt(
                owner,
                id,
                expected_revision,
                expected_executor_epoch,
                code,
                at,
            )
        }

        fn claim_expired(
            &self,
            owner: &str,
            id: &str,
            expected_revision: u64,
            expected_executor_epoch: u64,
            controller: ControllerRef,
            lease: vyane_task::Lease,
            now: chrono::DateTime<chrono::Utc>,
        ) -> vyane_task::Result<TaskRecord> {
            self.inner.claim_expired(
                owner,
                id,
                expected_revision,
                expected_executor_epoch,
                controller,
                lease,
                now,
            )
        }

        fn renew_lease(
            &self,
            owner: &str,
            id: &str,
            expected_revision: u64,
            expected_executor_epoch: u64,
            lease_owner: &str,
            expires_at: chrono::DateTime<chrono::Utc>,
            now: chrono::DateTime<chrono::Utc>,
        ) -> vyane_task::Result<TaskRecord> {
            self.inner.renew_lease(
                owner,
                id,
                expected_revision,
                expected_executor_epoch,
                lease_owner,
                expires_at,
                now,
            )
        }
    }

    fn stream_target(transport: vyane_core::AdapterTransport) -> vyane_core::BoundTarget {
        let harness = match transport {
            vyane_core::AdapterTransport::CliWrap => Some(vyane_core::HarnessKind::ClaudeCode),
            _ => None,
        };
        vyane_core::BoundTarget {
            target: vyane_core::Target {
                provider: vyane_core::ProviderId::new("test"),
                protocol: vyane_core::Protocol::AnthropicMessages,
                harness,
                model: vyane_core::ModelId::new("model"),
            },
            transport,
            endpoint: None,
            params: vyane_core::GenParams::default(),
        }
    }

    async fn temp_supervisor() -> (tempfile::TempDir, TaskSupervisor) {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("rest-task-metadata.sqlite3");
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        (directory, supervisor)
    }

    fn test_service(data_dir: &FsPath) -> VyaneService {
        VyaneService::from_loaded_with_paths(
            vyane_service::LoadedConfig {
                config: vyane_config::ResolvedConfig::default(),
                files: Vec::new(),
                secrets: std::collections::BTreeMap::new(),
            },
            vyane_service::StoragePaths::from_data_dir(data_dir),
        )
        .unwrap()
    }

    fn test_state(service: VyaneService, tasks: TaskSupervisor) -> ApiState {
        let goals = service
            .goal_reader(OwnerContext::single_user_local())
            .unwrap();
        ApiState {
            service: Arc::new(service.scope(OwnerContext::single_user_local())),
            tasks,
            goals: Arc::new(goals),
        }
    }

    fn goal_request(uri: &str, token: Option<&str>) -> axum::http::Request<axum::body::Body> {
        let mut request = axum::http::Request::get(uri).header("host", "127.0.0.1:9721");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        request.body(axum::body::Body::empty()).unwrap()
    }

    async fn response_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn create_continuity_goal(data_dir: &FsPath, owner: &str, id: &str) -> SqliteGoalStore {
        let store = SqliteGoalStore::open(data_dir.join("goals.sqlite3")).unwrap();
        let target = |role: &str| GoalExecutionTarget {
            provider: "provider".into(),
            protocol: "openai_chat".into(),
            harness: "harness".into(),
            model: "model".into(),
            profile: None,
            role: role.into(),
        };
        let mut goal = NewGoal::new("REST continuity projection", chrono::Utc::now());
        goal.id = Some(id.into());
        goal.continuity_policy = Some(GoalContinuityPolicy {
            mode: GoalContinuityMode::QuotaHandoff,
            primary: target("primary"),
            takeover: vec![target("takeover")],
            reviewer: Some(target("reviewer")),
            resume_primary_after_reset: true,
            require_review_before_resume: true,
            wait_for_review_checks_before_resume: true,
        });
        store.create(owner, goal).unwrap();
        store.start(owner, id, chrono::Utc::now()).unwrap();
        apply_quota_handoff_events(
            &store,
            owner,
            &[GoalQuotaEvent {
                event_id: format!("quota-{id}"),
                goal_id: Some(id.into()),
                provider: "provider".into(),
                harness: "harness".into(),
                model: "model".into(),
                session_id: None,
                observed_at: chrono::Utc::now(),
                estimated_reset_at: None,
            }],
            chrono::Utc::now(),
        )
        .unwrap();
        store
    }

    fn direct_http_test_service(data_dir: &FsPath, base_url: String) -> VyaneService {
        let mut providers = ProviderRegistry::new();
        providers.insert(
            "test-provider",
            Provider {
                base_url,
                api_key_env: None,
                auth_style: AuthStyle::Bearer,
                protocol: Protocol::OpenaiChat,
                default_model: Some(ModelId::new("test-model")),
                extra: Default::default(),
                env_inject: Default::default(),
            },
        );
        VyaneService::from_loaded_with_paths(
            vyane_service::LoadedConfig {
                config: vyane_config::ResolvedConfig {
                    providers,
                    profiles: std::collections::BTreeMap::from([(
                        "test".into(),
                        ProfilePatch {
                            provider: Some("test-provider".into()),
                            protocol: Some(Protocol::OpenaiChat),
                            harness: Some("none".into()),
                            model: Some(ModelId::new("test-model")),
                            ..Default::default()
                        },
                    )]),
                },
                files: Vec::new(),
                secrets: std::collections::BTreeMap::new(),
            },
            vyane_service::StoragePaths::from_data_dir(data_dir),
        )
        .unwrap()
    }

    fn dispatch_request(session: Option<&str>) -> DispatchRequest {
        DispatchRequest {
            task: "scope test".into(),
            target: "test".into(),
            workdir: None,
            sandbox: None,
            session: session.map(str::to_owned),
            system: None,
            timeout_secs: None,
            labels: None,
        }
    }

    fn new_task(id: &str, origin: TaskOrigin) -> NewTask {
        NewTask {
            id: id.into(),
            kind: TaskKind::Dispatch,
            origin,
            task_digest: vyane_kernel::task_digest("secret prompt"),
            target_key: "test/model".into(),
            created_at: chrono::Utc::now(),
        }
    }

    async fn create_attached(supervisor: &TaskSupervisor, id: &str) -> TaskRecord {
        let task = new_task(id, TaskOrigin::RestAsync);
        let created = supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let id = id.to_owned();
        let instance_id = supervisor.instance_id.to_string();
        supervisor
            .call(move |store| {
                store.attach_controller(
                    LOCAL_TASK_OWNER,
                    &id,
                    created.revision,
                    created.executor_epoch,
                    ControllerRef::InProcess { instance_id },
                    None,
                    chrono::Utc::now(),
                )
            })
            .await
            .unwrap()
    }

    fn outcome_with_status(status: RunStatus) -> DispatchOutcome {
        let error = match status {
            RunStatus::Success => None,
            RunStatus::Error => Some("dispatch failed".into()),
            RunStatus::Timeout => Some("dispatch timed out".into()),
            RunStatus::Cancelled => Some("dispatch cancelled".into()),
        };
        DispatchOutcome {
            record: vyane_core::RunRecord {
                run_id: "run-1".into(),
                owner: "local".into(),
                started_at: chrono::Utc::now(),
                finished_at: chrono::Utc::now(),
                task_digest: "deadbeef".into(),
                task_preview: None,
                workdir: None,
                sandbox: Sandbox::ReadOnly,
                target: vyane_core::Target {
                    provider: vyane_core::ProviderId::new("test"),
                    protocol: vyane_core::Protocol::OpenaiChat,
                    harness: None,
                    model: vyane_core::ModelId::new("model"),
                },
                transport: vyane_core::AdapterTransport::DirectHttp,
                attempts: vec![],
                status,
                usage: None,
                cost_usd: None,
                session_id: None,
                output_chars: None,
                error,
                labels: Default::default(),
            },
            output: (status == RunStatus::Success).then(|| "done".into()),
        }
    }

    // --- Request deserialization ------------------------------------------

    #[test]
    fn dispatch_request_minimal() {
        let json = r#"{"task":"say hi","target":"openai/gpt-4"}"#;
        let req: DispatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.task, "say hi");
        assert_eq!(req.target, "openai/gpt-4");
        assert_eq!(req.workdir, None);
        assert_eq!(req.sandbox, None);
        assert_eq!(req.session, None);
        assert_eq!(req.system, None);
        assert_eq!(req.timeout_secs, None);
        assert_eq!(req.labels, None);
    }

    #[test]
    fn dispatch_request_full() {
        let json = r#"{
            "task":"do the thing",
            "target":"prod",
            "workdir":"/tmp/work",
            "sandbox":"write",
            "session":"s1",
            "system":"be terse",
            "timeout_secs":30,
            "labels":["env=prod","team=ops"]
        }"#;
        let req: DispatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.task, "do the thing");
        assert_eq!(req.target, "prod");
        assert_eq!(req.workdir.as_deref(), Some("/tmp/work"));
        assert_eq!(req.sandbox.as_deref(), Some("write"));
        assert_eq!(req.session.as_deref(), Some("s1"));
        assert_eq!(req.system.as_deref(), Some("be terse"));
        assert_eq!(req.timeout_secs, Some(30));
        assert_eq!(
            req.labels.unwrap(),
            vec!["env=prod".to_string(), "team=ops".to_string()]
        );
    }

    #[test]
    fn broadcast_request_minimal() {
        let json = r#"{"task":"hi","targets":"a,b,c"}"#;
        let req: BroadcastRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.task, "hi");
        assert_eq!(req.targets, "a,b,c");
        assert_eq!(req.sandbox, None);
    }

    #[test]
    fn dispatch_request_requires_task() {
        let json = r#"{"target":"x"}"#;
        assert!(serde_json::from_str::<DispatchRequest>(json).is_err());
    }

    #[test]
    fn dispatch_request_requires_target() {
        let json = r#"{"task":"x"}"#;
        assert!(serde_json::from_str::<DispatchRequest>(json).is_err());
    }

    #[test]
    fn loopback_authority_and_origin_validation_rejects_rebinding_hosts() {
        for allowed in [
            "localhost",
            "localhost:9721",
            "127.0.0.1",
            "127.42.0.9:9721",
            "[::1]",
            "[::1]:9721",
        ] {
            assert!(authority_is_loopback(allowed), "rejected {allowed}");
        }
        for rejected in [
            "attacker.invalid:9721",
            "127.0.0.1.attacker.invalid",
            "0.0.0.0:9721",
            "[::]:9721",
            "localhost.attacker.invalid",
            "localhost:",
            "localhost:evil",
            "localhost:99999",
            "[localhost]",
            "[localhost]:9721",
            "[::1]attacker.invalid",
            "[::1]attacker.invalid:9721",
            "[::1]:99999",
            "user@127.0.0.1:9721",
        ] {
            assert!(!authority_is_loopback(rejected), "accepted {rejected}");
        }
        assert!(origin_is_loopback("http://127.0.0.1:9721"));
        assert!(origin_is_loopback("https://localhost"));
        assert!(!origin_is_loopback("http://attacker.invalid:9721"));
        assert!(!origin_is_loopback("http://[::1]attacker.invalid/"));
        assert!(!origin_is_loopback("null"));
    }

    #[tokio::test]
    async fn full_rest_router_requires_bearer_and_loopback_browser_authority() {
        let (_directory, supervisor) = temp_supervisor().await;
        let service_directory = tempfile::tempdir().unwrap();
        let app = router_from_parts(
            test_service(service_directory.path()),
            supervisor,
            API_TOKEN,
        )
        .unwrap();

        let missing_token = app
            .clone()
            .oneshot(
                axum::http::Request::get("/v1/health")
                    .header("host", "127.0.0.1:9721")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        for (host, origin, fetch_site) in [
            ("attacker.invalid:9721", None, None),
            ("127.0.0.1:9721", Some("http://attacker.invalid:9721"), None),
            ("127.0.0.1:9721", None, Some("cross-site")),
        ] {
            let mut request = axum::http::Request::get("/v1/health")
                .header("host", host)
                .header("authorization", format!("Bearer {API_TOKEN}"));
            if let Some(origin) = origin {
                request = request.header("origin", origin);
            }
            if let Some(fetch_site) = fetch_site {
                request = request.header("sec-fetch-site", fetch_site);
            }
            let response = app
                .clone()
                .oneshot(request.body(axum::body::Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }

        let allowed = app
            .oneshot(
                axum::http::Request::get("/v1/health")
                    .header("host", "localhost:9721")
                    .header("origin", "http://localhost:9721")
                    .header("authorization", format!("Bearer {API_TOKEN}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn goal_continuity_next_requires_exact_bearer_and_rejects_query_or_write_routes() {
        let data = tempfile::tempdir().unwrap();
        let (_task_directory, tasks) = temp_supervisor().await;
        let app = router_from_parts(test_service(data.path()), tasks, API_TOKEN).unwrap();
        let route = "/v1/goals/missing/continuity-next";

        let missing = app
            .clone()
            .oneshot(goal_request(route, None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let wrong = "b".repeat(64);
        let wrong = app
            .clone()
            .oneshot(goal_request(route, Some(&wrong)))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

        let authenticated = app
            .clone()
            .oneshot(goal_request(route, Some(API_TOKEN)))
            .await
            .unwrap();
        assert_eq!(authenticated.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            authenticated.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );

        assert_eq!(
            missing.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(
            wrong.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );

        let forbidden = app
            .clone()
            .oneshot(
                axum::http::Request::get(route)
                    .header("host", "example.invalid")
                    .header("authorization", format!("Bearer {API_TOKEN}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            forbidden.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );

        let query = app
            .clone()
            .oneshot(goal_request(
                "/v1/goals/missing/continuity-next?owner=foreign",
                Some(API_TOKEN),
            ))
            .await
            .unwrap();
        assert_eq!(query.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(query).await,
            serde_json::json!({
                "error": "goal continuity-next does not accept query parameters"
            })
        );

        let invalid = app
            .clone()
            .oneshot(goal_request(
                "/v1/goals/%20/continuity-next",
                Some(API_TOKEN),
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(invalid).await,
            serde_json::json!({ "error": "invalid goal id" })
        );

        for body in ["opaque", r#"{"owner":"foreign"}"#] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::get(route)
                        .header("host", "127.0.0.1:9721")
                        .header("authorization", format!("Bearer {API_TOKEN}"))
                        .body(axum::body::Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store"
            );
            assert_eq!(
                response_json(response).await,
                serde_json::json!({
                    "error": "goal continuity-next does not accept a request body"
                })
            );
        }

        let head = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(axum::http::Method::HEAD)
                    .uri(route)
                    .header("host", "127.0.0.1:9721")
                    .header("authorization", format!("Bearer {API_TOKEN}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            head.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(head.headers().get(header::ALLOW).unwrap(), "GET");

        let write = app
            .oneshot(
                axum::http::Request::post(route)
                    .header("host", "127.0.0.1:9721")
                    .header("authorization", format!("Bearer {API_TOKEN}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(write.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            write.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(write.headers().get(header::ALLOW).unwrap(), "GET");
    }

    #[tokio::test]
    async fn goal_continuity_next_is_stable_no_store_and_has_no_side_effects() {
        let data = tempfile::tempdir().unwrap();
        let store = create_continuity_goal(data.path(), LOCAL_TASK_OWNER, "rest-projection");
        let before = store
            .get(LOCAL_TASK_OWNER, "rest-projection")
            .unwrap()
            .unwrap();
        let events_before = store.events(LOCAL_TASK_OWNER, "rest-projection").unwrap();
        let approvals_before = store
            .list_takeover_approvals(LOCAL_TASK_OWNER, Some("rest-projection"))
            .unwrap();
        assert!(approvals_before.is_empty());

        let (_task_directory, tasks) = temp_supervisor().await;
        let app = router_from_parts(test_service(data.path()), tasks, API_TOKEN).unwrap();
        let route = "/v1/goals/rest-projection/continuity-next";
        let first = app
            .clone()
            .oneshot(goal_request(route, Some(API_TOKEN)))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            first.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let first = response_json(first).await;
        let second = app
            .oneshot(goal_request(route, Some(API_TOKEN)))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second = response_json(second).await;

        assert_eq!(first, second);
        assert_eq!(first["next_action"]["view_schema"], 1);
        assert_eq!(first["next_action"]["goal_id"], "rest-projection");
        assert_eq!(first["next_action"]["action"], "queue_approval");
        assert_eq!(first["next_action"]["command"], "continuity_queue");
        assert_eq!(first["next_action"]["reason_code"], "approval_required");
        let next_action = first["next_action"].as_object().unwrap();
        for forbidden in ["owner", "workdir", "db", "reason"] {
            assert!(!next_action.contains_key(forbidden), "leaked {forbidden}");
        }

        assert_eq!(
            store
                .get(LOCAL_TASK_OWNER, "rest-projection")
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            store.events(LOCAL_TASK_OWNER, "rest-projection").unwrap(),
            events_before
        );
        assert_eq!(
            store
                .list_takeover_approvals(LOCAL_TASK_OWNER, Some("rest-projection"))
                .unwrap(),
            approvals_before
        );
    }

    #[tokio::test]
    async fn goal_continuity_next_maps_absent_and_unavailable_without_internal_details() {
        let data = tempfile::tempdir().unwrap();
        let store = SqliteGoalStore::open(data.path().join("goals.sqlite3")).unwrap();
        let mut goal = NewGoal::new("No continuity", chrono::Utc::now());
        goal.id = Some("plain".into());
        store.create(LOCAL_TASK_OWNER, goal).unwrap();
        create_continuity_goal(data.path(), "foreign", "foreign-only");

        let (_task_directory, tasks) = temp_supervisor().await;
        let app = router_from_parts(test_service(data.path()), tasks, API_TOKEN).unwrap();
        let missing = app
            .clone()
            .oneshot(goal_request(
                "/v1/goals/absent/continuity-next",
                Some(API_TOKEN),
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(missing).await,
            serde_json::json!({ "error": "goal not found" })
        );

        let absent = app
            .clone()
            .oneshot(goal_request(
                "/v1/goals/absent/continuity-next",
                Some(API_TOKEN),
            ))
            .await
            .unwrap();
        let foreign = app
            .clone()
            .oneshot(goal_request(
                "/v1/goals/foreign-only/continuity-next",
                Some(API_TOKEN),
            ))
            .await
            .unwrap();
        assert_eq!(foreign.status(), absent.status());
        assert_eq!(foreign.headers(), absent.headers());
        assert_eq!(response_json(foreign).await, response_json(absent).await);

        let unavailable = app
            .oneshot(goal_request(
                "/v1/goals/plain/continuity-next",
                Some(API_TOKEN),
            ))
            .await
            .unwrap();
        assert_eq!(unavailable.status(), StatusCode::CONFLICT);
        assert_eq!(
            unavailable.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(
            response_json(unavailable).await,
            serde_json::json!({ "error": "goal continuity is unavailable" })
        );
    }

    #[tokio::test]
    async fn rest_dispatch_broadcast_history_and_sessions_share_the_frozen_local_scope() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "chatcmpl-rest-scope",
                "model": "test-model",
                "choices": [{
                    "message": { "role": "assistant", "content": "ok" },
                    "finish_reason": "stop"
                }]
            })))
            .mount(&server)
            .await;

        let service_directory = tempfile::tempdir().unwrap();
        let service = direct_http_test_service(service_directory.path(), server.uri());
        let (_task_directory, tasks) = temp_supervisor().await;
        let state = test_state(service.clone(), tasks);

        let Json(dispatched) = dispatch(
            State(state.clone()),
            Json(dispatch_request(Some("rest-scope-session"))),
        )
        .await
        .unwrap();
        assert_eq!(dispatched.output.as_deref(), Some("ok"));
        let Json(broadcasted) = broadcast(
            State(state.clone()),
            Json(BroadcastRequest {
                task: "broadcast scope test".into(),
                targets: "test,test".into(),
                workdir: None,
                sandbox: None,
                system: None,
                timeout_secs: None,
                labels: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(broadcasted.items.len(), 2);

        let Json(run_views) = runs(State(state.clone()), Query(RunsQuery::default()))
            .await
            .unwrap();
        assert_eq!(run_views.items.len(), 3);
        let Json(session_views) = sessions(State(state)).await.unwrap();
        assert_eq!(session_views.items.len(), 1);
        assert_eq!(session_views.items[0].session_id, "rest-scope-session");

        let local_runs = service
            .runtime()
            .ledger
            .query(RunQuery {
                owner: Some(LOCAL_TASK_OWNER.into()),
                ..RunQuery::default()
            })
            .await
            .unwrap();
        let foreign_runs = service
            .runtime()
            .ledger
            .query(RunQuery {
                owner: Some("foreign".into()),
                ..RunQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(local_runs.len(), 3);
        assert!(foreign_runs.is_empty());
    }

    #[tokio::test]
    async fn rest_sse_uses_the_same_frozen_local_scope() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n",
                ),
                "text/event-stream",
            ))
            .mount(&server)
            .await;

        let service_directory = tempfile::tempdir().unwrap();
        let service = direct_http_test_service(service_directory.path(), server.uri());
        let (_task_directory, tasks) = temp_supervisor().await;
        let state = test_state(service.clone(), tasks);

        let response = dispatch_stream(State(state), Json(dispatch_request(None)))
            .await
            .unwrap()
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("\"type\":\"delta\""));
        assert!(body.contains("\"type\":\"finished\""));

        let local_runs = service
            .runtime()
            .ledger
            .query(RunQuery {
                owner: Some(LOCAL_TASK_OWNER.into()),
                ..RunQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(local_runs.len(), 1);
        assert_eq!(local_runs[0].owner, LOCAL_TASK_OWNER);
    }

    // --- Streaming target validation --------------------------------------

    #[test]
    fn sse_streaming_accepts_single_direct_http_target() {
        let target = stream_target(vyane_core::AdapterTransport::DirectHttp);
        let accepted = streamable_bound(std::slice::from_ref(&target), false).unwrap();
        assert_eq!(accepted.transport, vyane_core::AdapterTransport::DirectHttp);
    }

    #[test]
    fn sse_streaming_accepts_single_harness_target() {
        let target = stream_target(vyane_core::AdapterTransport::CliWrap);
        let accepted = streamable_bound(std::slice::from_ref(&target), false).unwrap();
        assert_eq!(accepted.transport, vyane_core::AdapterTransport::CliWrap);
    }

    #[test]
    fn sse_streaming_rejects_failover_chain() {
        let target = stream_target(vyane_core::AdapterTransport::DirectHttp);
        let err = streamable_bound(&[target.clone(), target], false).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("single target"));
    }

    #[test]
    fn sse_streaming_rejects_session() {
        let target = stream_target(vyane_core::AdapterTransport::CliWrap);
        let err = streamable_bound(&[target], true).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("does not support sessions"));
    }

    #[test]
    fn sse_rejects_forged_routing_decision_labels_before_planning() {
        let mut forged = vyane_core::TaskSpec::new("test");
        forged
            .labels
            .insert("routing.provider".into(), "pretend".into());
        let error = validate_external_task_labels(&forged).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.message, "invalid dispatch request");

        let mut input = vyane_core::TaskSpec::new("test");
        input
            .labels
            .insert("routing.allow_frontier".into(), "false".into());
        validate_external_task_labels(&input).unwrap();
    }

    #[tokio::test]
    async fn sse_terminal_waits_for_capacity_instead_of_being_dropped() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(SsePayload::Delta {
            text: "full".into(),
        })
        .await
        .unwrap();

        let terminal = tokio::spawn(async move {
            send_sse_terminal(&tx, SsePayload::Unsupported).await;
        });
        tokio::task::yield_now().await;
        assert!(
            !terminal.is_finished(),
            "terminal send must wait while the bounded queue is full"
        );

        assert!(matches!(rx.recv().await, Some(SsePayload::Delta { .. })));
        terminal.await.unwrap();
        assert!(matches!(rx.recv().await, Some(SsePayload::Unsupported)));
    }

    // --- Sandbox parsing --------------------------------------------------

    #[test]
    fn sandbox_default_is_read_only() {
        assert_eq!(parse_sandbox(None).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_snake_case_read_only() {
        assert_eq!(parse_sandbox(Some("read_only")).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_kebab_case_read_only() {
        assert_eq!(parse_sandbox(Some("read-only")).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_write() {
        assert_eq!(parse_sandbox(Some("write")).unwrap(), Sandbox::Write);
    }

    #[test]
    fn sandbox_full() {
        assert_eq!(parse_sandbox(Some("full")).unwrap(), Sandbox::Full);
    }

    #[test]
    fn sandbox_unknown_errors() {
        let err = parse_sandbox(Some("danger")).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("danger"));
    }

    // --- Run status parsing -----------------------------------------------

    #[test]
    fn run_status_all_variants() {
        assert_eq!(parse_run_status("success").unwrap(), RunStatus::Success);
        assert_eq!(parse_run_status("error").unwrap(), RunStatus::Error);
        assert_eq!(parse_run_status("timeout").unwrap(), RunStatus::Timeout);
        assert_eq!(parse_run_status("cancelled").unwrap(), RunStatus::Cancelled);
    }

    #[test]
    fn run_status_unknown_errors() {
        let err = parse_run_status("pending").unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("pending"));
    }

    // --- Error response formatting ----------------------------------------

    #[test]
    fn error_body_serializes() {
        let body = ErrorBody {
            error: "bad target".into(),
        };
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(json, r#"{"error":"bad target"}"#);
    }

    #[test]
    fn api_error_bad_request_status() {
        let err = ApiError::bad_request("nope");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.message, "nope");
    }

    #[test]
    fn api_error_internal_status() {
        let err = ApiError::internal("boom");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.message, "boom");
    }

    // --- Envelope ---------------------------------------------------------

    #[test]
    fn items_envelope_serializes_empty() {
        let env = ItemsEnvelope {
            items: Vec::<BroadcastItem>::new(),
        };
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(json, r#"{"items":[]}"#);
    }

    #[test]
    fn health_response_serializes() {
        let json = serde_json::to_string(&HealthResponse { status: "ok" }).unwrap();
        assert_eq!(json, r#"{"status":"ok"}"#);
    }

    // --- Durable REST tasks -----------------------------------------------

    #[test]
    fn run_record_status_is_authoritative_for_task_settlement() {
        for (run_status, expected) in [
            (RunStatus::Success, TaskState::Succeeded),
            (RunStatus::Error, TaskState::Failed),
            (RunStatus::Timeout, TaskState::TimedOut),
            (RunStatus::Cancelled, TaskState::Cancelled),
        ] {
            let settlement = settlement_from_dispatch_result(Ok(outcome_with_status(run_status)));
            let actual = match settlement {
                TaskSettlement::Succeeded { ledger_run_id } => {
                    assert_eq!(ledger_run_id.as_deref(), Some("run-1"));
                    TaskState::Succeeded
                }
                TaskSettlement::Failed {
                    code,
                    ledger_run_id,
                } => {
                    assert_eq!(code, FailureCode::DispatchFailed);
                    assert_eq!(ledger_run_id.as_deref(), Some("run-1"));
                    TaskState::Failed
                }
                TaskSettlement::TimedOut { ledger_run_id } => {
                    assert_eq!(ledger_run_id.as_deref(), Some("run-1"));
                    TaskState::TimedOut
                }
                TaskSettlement::Cancelled { ledger_run_id } => {
                    assert_eq!(ledger_run_id.as_deref(), Some("run-1"));
                    TaskState::Cancelled
                }
            };
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn cancel_then_success_settlement_rereads_revision_and_uses_run_status() {
        let (_directory, supervisor) = temp_supervisor().await;
        let attached = create_attached(&supervisor, "race").await;
        let cancel = CancellationToken::new();
        supervisor.live_tokens.insert(
            (attached.id.clone(), attached.executor_epoch),
            cancel.clone(),
        );

        let cancelling = supervisor.cancel("race").await.unwrap().unwrap();
        assert_eq!(cancelling.state, TaskState::Cancelling);
        assert!(cancel.is_cancelled());

        // Completion won at the kernel boundary despite the late cancel. The
        // settle path must use the post-cancel revision it just re-read.
        let settlement =
            settlement_from_dispatch_result(Ok(outcome_with_status(RunStatus::Success)));
        let settled = supervisor
            .settle_current("race", attached.executor_epoch, settlement)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.state, TaskState::Succeeded);
        assert_eq!(settled.ledger_run_id.as_deref(), Some("run-1"));
    }

    #[tokio::test]
    async fn cancel_after_terminal_is_noop_and_does_not_signal_token() {
        let (_directory, supervisor) = temp_supervisor().await;
        let attached = create_attached(&supervisor, "done").await;
        let settled = supervisor
            .settle_current(
                "done",
                attached.executor_epoch,
                TaskSettlement::Succeeded {
                    ledger_run_id: Some("run-done".into()),
                },
            )
            .await
            .unwrap()
            .unwrap();
        let cancel = CancellationToken::new();
        supervisor
            .live_tokens
            .insert((settled.id.clone(), settled.executor_epoch), cancel.clone());

        let response = supervisor.cancel("done").await.unwrap().unwrap();
        assert_eq!(response, settled);
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_route_signals_exact_epoch_token_and_unknown_is_404() {
        let (_directory, supervisor) = temp_supervisor().await;
        let attached = create_attached(&supervisor, "route-task").await;
        let stale = CancellationToken::new();
        let current = CancellationToken::new();
        supervisor.live_tokens.insert(
            (attached.id.clone(), attached.executor_epoch - 1),
            stale.clone(),
        );
        supervisor.live_tokens.insert(
            (attached.id.clone(), attached.executor_epoch),
            current.clone(),
        );
        let app = Router::new()
            .route("/v1/tasks/{id}/cancel", post(cancel_task))
            .with_state(supervisor.clone());

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/v1/tasks/route-task/cancel")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(current.is_cancelled());
        assert!(!stale.is_cancelled());
        assert_eq!(
            supervisor.get("route-task").await.unwrap().unwrap().state,
            TaskState::Cancelling
        );

        let missing = app
            .oneshot(
                axum::http::Request::post("/v1/tasks/missing/cancel")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn another_supervisor_cannot_cancel_a_running_task_it_does_not_own() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("shared-rest-tasks.sqlite3");
        let owner = TaskSupervisor::open(&database).await.unwrap();
        let foreign = TaskSupervisor::open(&database).await.unwrap();
        let attached = create_attached(&owner, "foreign-cancel").await;
        let owner_token = CancellationToken::new();
        owner.live_tokens.insert(
            (attached.id.clone(), attached.executor_epoch),
            owner_token.clone(),
        );

        let error = foreign.cancel(&attached.id).await.unwrap_err();
        assert!(matches!(error, TaskCallError::ControlUnavailable { .. }));
        let current = foreign.get(&attached.id).await.unwrap().unwrap();
        assert_eq!(current.state, TaskState::Running);
        assert_eq!(current.revision, attached.revision);
        assert!(!owner_token.is_cancelled());
    }

    #[tokio::test]
    async fn queued_task_without_a_registered_token_is_cancellable() {
        let (_directory, supervisor) = temp_supervisor().await;
        let task = new_task("queued-without-token", TaskOrigin::RestAsync);
        let created = supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();

        let cancelled = supervisor.cancel(&created.id).await.unwrap().unwrap();
        assert_eq!(cancelled.state, TaskState::Cancelled);
        assert_eq!(cancelled.failure_code, Some(FailureCode::Cancelled));

        let id = created.id.clone();
        let late_attach = supervisor
            .call(move |store| {
                store.attach_controller(
                    LOCAL_TASK_OWNER,
                    &id,
                    created.revision,
                    created.executor_epoch,
                    ControllerRef::InProcess {
                        instance_id: "rest:late".into(),
                    },
                    None,
                    chrono::Utc::now(),
                )
            })
            .await;
        assert!(matches!(
            late_attach,
            Err(TaskCallError::Store(TaskStoreError::Conflict { .. }))
                | Err(TaskCallError::Store(TaskStoreError::InvalidState { .. }))
        ));
    }

    #[tokio::test]
    async fn queued_cancel_signals_a_pre_registered_initializer_token() {
        let (_directory, supervisor) = temp_supervisor().await;
        let task = new_task("queued-with-token", TaskOrigin::RestAsync);
        let created = supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let next_epoch = created.executor_epoch.checked_add(1).unwrap();
        let initializer_token = CancellationToken::new();
        supervisor
            .live_tokens
            .insert((created.id.clone(), next_epoch), initializer_token.clone());

        let cancelled = supervisor.cancel(&created.id).await.unwrap().unwrap();
        assert_eq!(cancelled.state, TaskState::Cancelled);
        assert!(initializer_token.is_cancelled());
    }

    #[tokio::test]
    async fn failed_initialization_cleanup_never_interrupts_a_foreign_controller() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("initialization-cleanup.sqlite3");
        let initializer = TaskSupervisor::open(&database).await.unwrap();
        let foreign = TaskSupervisor::open(&database).await.unwrap();
        let task = new_task("foreign-initializer", TaskOrigin::RestAsync);
        let created = initializer
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let id = created.id.clone();
        let foreign_instance = foreign.instance_id.to_string();
        let attached = foreign
            .call(move |store| {
                store.attach_controller(
                    LOCAL_TASK_OWNER,
                    &id,
                    created.revision,
                    created.executor_epoch,
                    ControllerRef::InProcess {
                        instance_id: foreign_instance,
                    },
                    None,
                    chrono::Utc::now(),
                )
            })
            .await
            .unwrap();

        initializer
            .interrupt_failed_initialization(
                &attached.id,
                created.revision,
                created.executor_epoch,
                attached.executor_epoch,
            )
            .await;
        assert_eq!(
            initializer.get(&attached.id).await.unwrap().unwrap(),
            attached
        );
    }

    #[tokio::test]
    async fn shutdown_drains_cooperative_background_work_before_lock_release() {
        let (directory, supervisor) = temp_supervisor().await;
        let lock_path = directory.path().join("task-supervisor.lock");
        let supervisor_lock = acquire_task_supervisor_lock(&lock_path).unwrap();
        let attached = create_attached(&supervisor, "shutdown-cooperative").await;
        let token = CancellationToken::new();
        supervisor.live_tokens.insert(
            (attached.id.clone(), attached.executor_epoch),
            token.clone(),
        );
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_finished = Arc::clone(&finished);
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            None,
            async move {
                token.cancelled().await;
                worker_finished.store(true, Ordering::Release);
                Ok(outcome_with_status(RunStatus::Cancelled))
            },
        ));

        supervisor
            .shutdown_and_drain(Duration::from_secs(1))
            .await
            .unwrap();

        assert!(finished.load(Ordering::Acquire));
        assert!(supervisor.live_tokens.is_empty());
        assert!(supervisor.live_dispatches.is_empty());
        let terminal = supervisor
            .get("shutdown-cooperative")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.state, TaskState::Cancelled);
        assert!(acquire_task_supervisor_lock(&lock_path).is_err());
        drop(supervisor_lock);
        drop(acquire_task_supervisor_lock(&lock_path).unwrap());
    }

    #[tokio::test]
    async fn shutdown_timeout_interrupts_only_exact_owned_epoch() {
        let (directory, supervisor) = temp_supervisor().await;
        let lock_path = directory.path().join("task-supervisor.lock");
        let supervisor_lock = acquire_task_supervisor_lock(&lock_path).unwrap();
        let attached = create_attached(&supervisor, "shutdown-timeout").await;
        let token = CancellationToken::new();
        supervisor.live_tokens.insert(
            (attached.id.clone(), attached.executor_epoch),
            token.clone(),
        );
        let future_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&future_dropped));
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            None,
            async move {
                let _drop_flag = drop_flag;
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(outcome_with_status(RunStatus::Success))
            },
        ));

        supervisor
            .shutdown_and_drain(Duration::from_millis(5))
            .await
            .unwrap();

        assert!(token.is_cancelled());
        assert!(future_dropped.load(Ordering::Acquire));
        assert!(supervisor.live_tokens.is_empty());
        assert!(supervisor.live_dispatches.is_empty());
        let interrupted = supervisor.get("shutdown-timeout").await.unwrap().unwrap();
        assert_eq!(interrupted.state, TaskState::Interrupted);
        assert_eq!(
            interrupted.failure_code,
            Some(FailureCode::ControlUnavailable)
        );
        assert!(acquire_task_supervisor_lock(&lock_path).is_err());
        drop(supervisor_lock);
        drop(acquire_task_supervisor_lock(&lock_path).unwrap());
    }

    #[tokio::test]
    async fn shutdown_store_error_still_aborts_and_drops_pending_dispatch_before_return() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("shutdown-corrupt.sqlite3");
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        let lock_path = directory.path().join("task-supervisor.lock");
        let supervisor_lock = acquire_task_supervisor_lock(&lock_path).unwrap();
        let attached = create_attached(&supervisor, "shutdown-corrupt").await;
        let key = (attached.id.clone(), attached.executor_epoch);
        let token = CancellationToken::new();
        supervisor.live_tokens.insert(key.clone(), token.clone());
        let future_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&future_dropped));
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id,
            attached.executor_epoch,
            None,
            async move {
                let _drop_flag = drop_flag;
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(outcome_with_status(RunStatus::Success))
            },
        ));

        for suffix in ["-wal", "-shm"] {
            let mut sidecar = database.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(sidecar));
        }
        std::fs::remove_file(&database).unwrap();
        std::fs::write(&database, b"not a sqlite database").unwrap();

        let error = supervisor
            .shutdown_and_drain(Duration::from_millis(5))
            .await
            .unwrap_err();

        assert!(matches!(error, TaskCallError::Store(_)));
        assert!(token.is_cancelled());
        assert!(future_dropped.load(Ordering::Acquire));
        assert!(supervisor.live_dispatches.is_empty());
        assert!(supervisor.live_tokens.is_empty());
        assert!(acquire_task_supervisor_lock(&lock_path).is_err());
        drop(supervisor_lock);
        drop(acquire_task_supervisor_lock(&lock_path).unwrap());
    }

    #[tokio::test]
    async fn duplicate_exact_epoch_dispatch_is_atomically_rejected() {
        let (_directory, supervisor) = temp_supervisor().await;
        let attached = create_attached(&supervisor, "shutdown-replacement").await;
        let key = (attached.id.clone(), attached.executor_epoch);
        let token = CancellationToken::new();
        supervisor.live_tokens.insert(key.clone(), token);

        let first_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_drop_flag = DropFlag(Arc::clone(&first_dropped));
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            None,
            async move {
                let _drop_flag = first_drop_flag;
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(outcome_with_status(RunStatus::Success))
            },
        ));

        let second_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_drop_flag = DropFlag(Arc::clone(&second_dropped));
        assert!(!supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            None,
            async move {
                let _drop_flag = second_drop_flag;
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(outcome_with_status(RunStatus::Success))
            },
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !second_dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!first_dropped.load(Ordering::Acquire));
        // The duplicate future is dropped without ever replacing the current
        // exact-epoch handle or creating a token ownership gap.
        assert!(supervisor.live_dispatches.contains_key(&key));
        assert!(supervisor.live_tokens.contains_key(&key));

        supervisor
            .shutdown_and_drain(Duration::from_millis(5))
            .await
            .unwrap();
        assert!(first_dropped.load(Ordering::Acquire));
        assert!(supervisor.live_dispatches.is_empty());
        assert!(supervisor.live_tokens.is_empty());
    }

    #[tokio::test]
    async fn shutdown_is_terminal_noop_and_does_not_touch_foreign_or_cli_tasks() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("shutdown-scope.sqlite3");
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        let foreign = TaskSupervisor::open(&database).await.unwrap();

        let terminal = create_attached(&supervisor, "shutdown-terminal").await;
        let terminal = supervisor
            .settle_current(
                &terminal.id,
                terminal.executor_epoch,
                TaskSettlement::Succeeded {
                    ledger_run_id: Some("run-shutdown-terminal".into()),
                },
            )
            .await
            .unwrap()
            .unwrap();
        let stale_terminal_token = CancellationToken::new();
        supervisor.live_tokens.insert(
            (terminal.id.clone(), terminal.executor_epoch),
            stale_terminal_token.clone(),
        );
        let terminal_id = terminal.id.clone();
        let terminal_events_before = supervisor
            .call(move |store| store.events(LOCAL_TASK_OWNER, &terminal_id))
            .await
            .unwrap();

        let foreign_running = create_attached(&foreign, "shutdown-foreign").await;
        let detached = new_task("shutdown-cli", TaskOrigin::CliDetached);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, detached))
            .await
            .unwrap();

        supervisor
            .shutdown_and_drain(Duration::from_millis(5))
            .await
            .unwrap();

        assert!(!stale_terminal_token.is_cancelled());
        assert_eq!(
            supervisor.get(&terminal.id).await.unwrap().unwrap(),
            terminal
        );
        let terminal_id = terminal.id.clone();
        assert_eq!(
            supervisor
                .call(move |store| store.events(LOCAL_TASK_OWNER, &terminal_id))
                .await
                .unwrap(),
            terminal_events_before
        );
        assert_eq!(
            supervisor.get(&foreign_running.id).await.unwrap().unwrap(),
            foreign_running
        );
        assert_eq!(
            supervisor.get("shutdown-cli").await.unwrap().unwrap().state,
            TaskState::Queued
        );
    }

    #[tokio::test]
    async fn invalid_target_is_rejected_before_task_metadata_is_created() {
        let (_directory, supervisor) = temp_supervisor().await;
        let service_directory = tempfile::tempdir().unwrap();
        let app = router_from_parts(
            test_service(service_directory.path()),
            supervisor.clone(),
            API_TOKEN,
        )
        .unwrap();
        let response = app
            .oneshot(
                axum::http::Request::post("/v1/tasks")
                    .header("content-type", "application/json")
                    .header("host", "127.0.0.1:9721")
                    .header("authorization", format!("Bearer {API_TOKEN}"))
                    .body(axum::body::Body::from(
                        r#"{"task":"private input","target":"missing-profile"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(supervisor.list_rest_tasks().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn output_route_reads_separate_artifact_only_after_success() {
        let (_directory, supervisor) = temp_supervisor().await;
        let attached = create_attached(&supervisor, "output-route").await;
        let path = supervisor.output_path(&attached.id).unwrap();
        write_private_task_output(path, "route result".into())
            .await
            .unwrap();
        supervisor
            .settle_current(
                &attached.id,
                attached.executor_epoch,
                TaskSettlement::Succeeded {
                    ledger_run_id: Some("run-output-route".into()),
                },
            )
            .await
            .unwrap();
        let app = Router::new()
            .route("/v1/tasks/{id}/output", get(get_task_output))
            .with_state(supervisor);

        let response = app
            .oneshot(
                axum::http::Request::get("/v1/tasks/output-route/output")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["output"], "route result");
    }

    #[tokio::test]
    async fn startup_recovery_interrupts_queued_running_and_cancelling_without_replay() {
        let (_directory, supervisor) = temp_supervisor().await;
        let queued = new_task("queued-crash-window", TaskOrigin::RestAsync);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, queued))
            .await
            .unwrap();
        create_attached(&supervisor, "running-crash-window").await;
        let cancelling = create_attached(&supervisor, "cancelling-crash-window").await;
        supervisor.live_tokens.insert(
            (cancelling.id.clone(), cancelling.executor_epoch),
            CancellationToken::new(),
        );
        supervisor.cancel("cancelling-crash-window").await.unwrap();
        // Recovery runs in a fresh process after the old in-memory token map
        // has disappeared.
        supervisor.live_tokens.clear();

        assert_eq!(supervisor.recover_interrupted().await.unwrap(), 3);
        for id in [
            "queued-crash-window",
            "running-crash-window",
            "cancelling-crash-window",
        ] {
            let record = supervisor.get(id).await.unwrap().unwrap();
            assert_eq!(record.state, TaskState::Interrupted);
            assert_eq!(record.failure_code, Some(FailureCode::WorkerLost));
        }
        assert!(supervisor.live_tokens.is_empty());
    }

    #[tokio::test]
    async fn dispatch_panic_is_classified_internal_and_cleans_live_token() {
        let (_directory, supervisor) = temp_supervisor().await;
        let attached = create_attached(&supervisor, "panic").await;
        let key = (attached.id.clone(), attached.executor_epoch);
        supervisor
            .live_tokens
            .insert(key.clone(), CancellationToken::new());
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            None,
            async move {
                panic!("provider secret panic text");
                #[allow(unreachable_code)]
                Ok(outcome_with_status(RunStatus::Success))
            },
        ));

        let settled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let record = supervisor.get("panic").await.unwrap().unwrap();
                if record.state.is_terminal() && !supervisor.live_tokens.contains_key(&key) {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(settled.state, TaskState::Failed);
        assert_eq!(settled.failure_code, Some(FailureCode::Internal));
        assert!(settled.ledger_run_id.is_none());
        assert!(!supervisor.live_tokens.contains_key(&key));
    }

    #[tokio::test]
    async fn completed_dispatch_keeps_runtime_control_until_exact_settlement_retry_succeeds() {
        let directory = tempfile::tempdir().unwrap();
        let inner = SqliteTaskStore::open(directory.path().join("flaky-settle.sqlite3")).unwrap();
        let store = Arc::new(FlakySettleStore::new(inner, 2));
        let supervisor = TaskSupervisor::from_store(store.clone());
        let attached = create_attached(&supervisor, "flaky-settle").await;
        let key = (attached.id.clone(), attached.executor_epoch);
        supervisor
            .live_tokens
            .insert(key.clone(), CancellationToken::new());
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            None,
            async move { Ok(outcome_with_status(RunStatus::Success)) },
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while store.settle_calls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            supervisor.get(&attached.id).await.unwrap().unwrap().state,
            TaskState::Running
        );
        assert!(supervisor.live_dispatches.contains_key(&key));
        assert!(supervisor.live_tokens.contains_key(&key));

        let settled = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let record = supervisor.get(&attached.id).await.unwrap().unwrap();
                if record.state.is_terminal()
                    && !supervisor.live_dispatches.contains_key(&key)
                    && !supervisor.live_tokens.contains_key(&key)
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(settled.state, TaskState::Succeeded);
        assert_eq!(settled.executor_epoch, attached.executor_epoch);
        assert!(store.settle_calls.load(Ordering::Acquire) >= 3);
    }

    #[tokio::test]
    async fn supervised_dispatch_persists_output_only_as_separate_artifact() {
        const OUTPUT: &str = "REST-OUTPUT-CANARY-outside-task-database";
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("tasks.sqlite3");
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        let attached = create_attached(&supervisor, "output-artifact").await;
        let key = (attached.id.clone(), attached.executor_epoch);
        supervisor
            .live_tokens
            .insert(key.clone(), CancellationToken::new());
        let output_path = supervisor.output_path(&attached.id).unwrap();
        let mut outcome = outcome_with_status(RunStatus::Success);
        outcome.output = Some(OUTPUT.into());
        assert!(supervisor.spawn_supervised_dispatch(
            attached.id.clone(),
            attached.executor_epoch,
            Some(output_path.clone()),
            async move { Ok(outcome) },
        ));

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let terminal = supervisor
                    .get("output-artifact")
                    .await
                    .unwrap()
                    .is_some_and(|record| record.state.is_terminal());
                if terminal
                    && !supervisor.live_dispatches.contains_key(&key)
                    && !supervisor.live_tokens.contains_key(&key)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(
            read_task_output(output_path).await.unwrap().as_deref(),
            Some(OUTPUT)
        );
        let database_bytes = std::fs::read(database).unwrap();
        assert!(
            !database_bytes
                .windows(OUTPUT.len())
                .any(|window| window == OUTPUT.as_bytes()),
            "output content must never enter SQLite task metadata"
        );
        assert!(!supervisor.live_dispatches.contains_key(&key));
        assert!(!supervisor.live_tokens.contains_key(&key));
    }

    #[tokio::test]
    async fn router_construction_does_not_recover_existing_tasks() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("explicit-rest.sqlite3");
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        let queued = new_task("embedded-queued", TaskOrigin::RestAsync);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, queued))
            .await
            .unwrap();

        let service = test_service(directory.path());
        let _router = build_router_with_task_db(service, &database, API_TOKEN)
            .await
            .unwrap();

        let verifier = TaskSupervisor::open(&database).await.unwrap();
        assert_eq!(
            verifier
                .get("embedded-queued")
                .await
                .unwrap()
                .unwrap()
                .state,
            TaskState::Queued
        );
    }

    #[tokio::test]
    async fn bind_failure_happens_before_task_database_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let service = test_service(directory.path());
        let database = service.storage_paths().task_metadata_db_path();
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        let queued = new_task("bind-failed", TaskOrigin::RestAsync);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, queued))
            .await
            .unwrap();
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = occupied.local_addr().unwrap();

        assert!(run_server(service, address).await.is_err());
        assert_eq!(
            supervisor.get("bind-failed").await.unwrap().unwrap().state,
            TaskState::Queued
        );
    }

    #[tokio::test]
    async fn non_loopback_listen_is_rejected_before_task_database_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let service = test_service(directory.path());
        let database = service.storage_paths().task_metadata_db_path();
        let supervisor = TaskSupervisor::open(&database).await.unwrap();
        let queued = new_task("non-loopback-refused", TaskOrigin::RestAsync);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, queued))
            .await
            .unwrap();

        let address: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let error = run_server(service, address).await.unwrap_err();
        assert!(error.to_string().contains("only accepts loopback"));
        assert_eq!(
            supervisor
                .get("non-loopback-refused")
                .await
                .unwrap()
                .unwrap()
                .state,
            TaskState::Queued
        );
    }

    #[test]
    fn api_token_publication_removes_only_its_exact_generation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("serve.token");
        crate::daemon::write_private_atomic(&path, API_TOKEN.as_bytes()).unwrap();
        drop(ApiTokenPublication {
            path: path.clone(),
            token: API_TOKEN.into(),
        });
        assert!(!path.exists());

        crate::daemon::write_private_atomic(&path, API_TOKEN.as_bytes()).unwrap();
        let newer = "b".repeat(64);
        let publication = ApiTokenPublication {
            path: path.clone(),
            token: API_TOKEN.into(),
        };
        crate::daemon::write_private_atomic(&path, newer.as_bytes()).unwrap();
        drop(publication);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), newer);
    }

    #[tokio::test]
    async fn run_and_session_routes_return_only_allowlisted_views() {
        let directory = tempfile::tempdir().unwrap();
        let service = test_service(directory.path());
        let mut run = outcome_with_status(RunStatus::Error).record;
        run.owner = "local".into();
        run.task_digest = "CANARY_TASK_DIGEST".into();
        run.task_preview = Some("CANARY_PROMPT".into());
        run.workdir = Some("/CANARY_WORKDIR".into());
        run.session_id = Some("CANARY_SESSION_ID".into());
        run.error = Some("CANARY_TERMINAL_ERROR".into());
        run.labels
            .insert("CANARY_LABEL".into(), "CANARY_VALUE".into());
        run.attempts.push(vyane_core::Attempt {
            target: run.target.clone(),
            transport: run.transport,
            started_at: run.started_at,
            duration_ms: 1,
            outcome: vyane_core::AttemptOutcome::Err {
                kind: vyane_core::ErrorKind::Protocol,
                message: "CANARY_ATTEMPT_ERROR".into(),
                failed_over: false,
            },
        });
        service.runtime().ledger.append(&run).await.unwrap();

        let session = vyane_core::SessionRecord {
            session_id: "visible-session".into(),
            owner: "local".into(),
            target: run.target.clone(),
            native_session_id: Some("CANARY_NATIVE_ID".into()),
            transcript: vec![vyane_core::ChatMessage::user("CANARY_TRANSCRIPT_BODY")],
            created_at: run.started_at,
            updated_at: run.finished_at,
            run_count: 2,
        };
        service
            .runtime()
            .sessions
            .save("local", &session)
            .await
            .unwrap();

        let (_task_directory, supervisor) = temp_supervisor().await;
        let app = router_from_parts(service, supervisor, API_TOKEN).unwrap();
        for route in ["/v1/runs", "/v1/sessions"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::get(route)
                        .header("host", "localhost:9721")
                        .header("authorization", format!("Bearer {API_TOKEN}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let text = String::from_utf8(body.to_vec()).unwrap();
            if route == "/v1/runs" {
                assert!(text.contains("run-1"));
                assert!(text.contains("terminal_error_kind"));
            } else {
                assert!(text.contains("visible-session"));
                assert!(text.contains("native_resume_available"));
            }
            for canary in [
                "CANARY_TASK_DIGEST",
                "CANARY_PROMPT",
                "CANARY_WORKDIR",
                "CANARY_SESSION_ID",
                "CANARY_TERMINAL_ERROR",
                "CANARY_ATTEMPT_ERROR",
                "CANARY_LABEL",
                "CANARY_VALUE",
                "CANARY_NATIVE_ID",
                "CANARY_TRANSCRIPT_BODY",
            ] {
                assert!(!text.contains(canary), "{route} leaked {canary}");
            }
        }
    }

    #[tokio::test]
    async fn task_output_artifact_roundtrips_with_private_permissions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks/task-1/output.txt");
        write_private_task_output(path.clone(), "private result".into())
            .await
            .unwrap();
        assert_eq!(
            read_task_output(path.clone()).await.unwrap().as_deref(),
            Some("private result")
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn task_output_artifacts_are_owner_and_task_qualified() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn TaskStore> =
            Arc::new(SqliteTaskStore::open(directory.path().join("tasks.sqlite")).unwrap());
        let root = directory.path().join("artifacts");
        let supervisor = TaskSupervisor::from_store_with_artifacts(store, root.clone());
        let alpha = supervisor.output_path_for("alpha", "shared").unwrap();
        let beta = supervisor.output_path_for("beta", "shared").unwrap();
        let other_task = supervisor.output_path_for("alpha", "other").unwrap();
        let hostile = supervisor.output_path_for("../owner", "../task").unwrap();
        assert_ne!(alpha, beta);
        assert_ne!(alpha, other_task);
        for path in [&alpha, &beta, &other_task, &hostile] {
            assert!(path.starts_with(&root));
            let rendered = path.to_string_lossy();
            assert!(!rendered.contains("alpha"));
            assert!(!rendered.contains("beta"));
            assert!(!rendered.contains("shared"));
        }
        let legacy_id = uuid::Uuid::now_v7().to_string();
        assert_eq!(
            supervisor.legacy_local_output_path(&legacy_id),
            Some(root.join(&legacy_id).join("output.txt"))
        );
        assert!(supervisor.legacy_local_output_path("../escape").is_none());
    }

    #[tokio::test]
    async fn rest_recovery_and_listing_do_not_claim_detached_tasks() {
        let (_directory, supervisor) = temp_supervisor().await;
        let detached = new_task("detached", TaskOrigin::CliDetached);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, detached))
            .await
            .unwrap();

        assert_eq!(supervisor.recover_interrupted().await.unwrap(), 0);
        assert_eq!(
            supervisor.get("detached").await.unwrap().unwrap().state,
            TaskState::Queued
        );
        assert!(supervisor.list_rest_tasks().await.unwrap().is_empty());
        assert!(supervisor.cancel("detached").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rest_scope_rejects_same_origin_workflows_and_foreign_owners_everywhere() {
        let (_directory, supervisor) = temp_supervisor().await;
        let mut workflow = new_task("rest-scope-workflow", TaskOrigin::RestAsync);
        workflow.kind = TaskKind::Workflow;
        let foreign = new_task("rest-scope-foreign", TaskOrigin::RestAsync);
        supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, workflow))
            .await
            .unwrap();
        supervisor
            .call(move |store| store.create("other-owner", foreign))
            .await
            .unwrap();

        let workflow_token = CancellationToken::new();
        let foreign_token = CancellationToken::new();
        supervisor
            .live_tokens
            .insert(("rest-scope-workflow".into(), 1), workflow_token.clone());
        supervisor
            .live_tokens
            .insert(("rest-scope-foreign".into(), 1), foreign_token.clone());

        for id in ["rest-scope-workflow", "rest-scope-foreign"] {
            write_private_task_output(
                supervisor.output_path(id).unwrap(),
                format!("private output for {id}"),
            )
            .await
            .unwrap();
        }

        let app = Router::new()
            .route("/v1/tasks", get(list_tasks))
            .route("/v1/tasks/{id}", get(get_task))
            .route("/v1/tasks/{id}/output", get(get_task_output))
            .route("/v1/tasks/{id}/cancel", post(cancel_task))
            .with_state(supervisor.clone());

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/v1/tasks")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["items"], serde_json::json!([]));

        for id in ["rest-scope-workflow", "rest-scope-foreign"] {
            for path in [format!("/v1/tasks/{id}"), format!("/v1/tasks/{id}/output")] {
                let response = app
                    .clone()
                    .oneshot(
                        axum::http::Request::get(path)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::NOT_FOUND);
            }

            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::post(format!("/v1/tasks/{id}/cancel"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            if id == "rest-scope-workflow" {
                assert_eq!(
                    supervisor.get(id).await.unwrap().unwrap().state,
                    TaskState::Queued
                );
            } else {
                assert!(supervisor.get(id).await.unwrap().is_none());
            }
        }

        assert_eq!(supervisor.recover_interrupted().await.unwrap(), 0);
        supervisor
            .shutdown_and_drain(Duration::from_millis(5))
            .await
            .unwrap();
        for id in ["rest-scope-workflow", "rest-scope-foreign"] {
            if id == "rest-scope-workflow" {
                assert_eq!(
                    supervisor.get(id).await.unwrap().unwrap().state,
                    TaskState::Queued
                );
            } else {
                assert!(supervisor.get(id).await.unwrap().is_none());
            }
        }
        assert!(!workflow_token.is_cancelled());
        assert!(!foreign_token.is_cancelled());
    }

    #[tokio::test]
    async fn listing_and_recovery_follow_every_sqlite_page_without_crossing_scope() {
        let (_directory, supervisor) = temp_supervisor().await;
        let created_at = chrono::Utc::now();
        supervisor
            .call(move |store| {
                // The test page size is two; three matching rows prove that
                // both listing and mutation follow the cursor. A same-origin
                // workflow row remains outside the exact REST dispatch scope.
                for index in 0..3_u8 {
                    store.create(
                        LOCAL_TASK_OWNER,
                        NewTask {
                            id: format!("z-rest-{index:02}"),
                            kind: TaskKind::Dispatch,
                            origin: TaskOrigin::RestAsync,
                            task_digest: vyane_kernel::task_digest("paged REST"),
                            target_key: "test/model".into(),
                            created_at,
                        },
                    )?;
                }
                store.create(
                    LOCAL_TASK_OWNER,
                    NewTask {
                        id: "a-same-origin-workflow".into(),
                        kind: TaskKind::Workflow,
                        origin: TaskOrigin::RestAsync,
                        task_digest: vyane_kernel::task_digest("foreign workflow"),
                        target_key: "test/model".into(),
                        created_at,
                    },
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let listed = supervisor.list_rest_tasks_with_page_limit(2).await.unwrap();
        assert_eq!(listed.len(), 3);
        assert_eq!(listed.last().unwrap().id, "z-rest-00");
        assert_eq!(
            supervisor
                .recover_interrupted_with_page_limit(2)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            supervisor.get("z-rest-00").await.unwrap().unwrap().state,
            TaskState::Interrupted
        );
        assert_eq!(
            supervisor
                .get("a-same-origin-workflow")
                .await
                .unwrap()
                .unwrap()
                .state,
            TaskState::Queued
        );
    }

    #[tokio::test]
    async fn task_entry_exposes_only_bounded_durable_metadata() {
        let (_directory, supervisor) = temp_supervisor().await;
        let task = new_task("wire", TaskOrigin::RestAsync);
        let record = supervisor
            .call(move |store| store.create(LOCAL_TASK_OWNER, task))
            .await
            .unwrap();
        let json = serde_json::to_value(TaskEntry::from(record)).unwrap();

        assert_eq!(json["id"], "wire");
        assert_eq!(json["state"], "queued");
        assert_eq!(json["target"], "test/model");
        assert_eq!(
            json["task_digest"],
            vyane_kernel::task_digest("secret prompt")
        );
        assert!(json.get("revision").is_some());
        for forbidden in ["task", "prompt", "output", "outcome", "error", "controller"] {
            assert!(json.get(forbidden).is_none(), "leaked field {forbidden}");
        }
        assert!(!json.to_string().contains("secret prompt"));
    }

    // --- SSE payload serialization ----------------------------------------

    #[test]
    fn sse_delta_serializes() {
        let payload = SsePayload::Delta {
            text: "hello".into(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(json, r#"{"type":"delta","text":"hello"}"#);
    }

    #[test]
    fn sse_tool_use_serializes() {
        let payload = SsePayload::ToolUse {
            name: "Read".into(),
            summary: r#"{"path":"src/lib.rs"}"#.into(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tool_use","name":"Read","summary":"{\"path\":\"src/lib.rs\"}"}"#
        );
    }

    #[test]
    fn sse_unsupported_serializes() {
        let payload = SsePayload::Unsupported;
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(json, r#"{"type":"unsupported"}"#);
    }

    // --- Error classification ---------------------------------------------

    #[test]
    fn caller_fault_detection() {
        assert!(is_caller_fault("profile not found"));
        assert!(is_caller_fault("provider openai missing"));
        assert!(is_caller_fault("invalid target"));
        assert!(is_caller_fault("endpoint has no key"));
        assert!(!is_caller_fault("connection refused"));
        assert!(!is_caller_fault("internal panic"));
    }
}
