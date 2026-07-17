//! The high-level service facade.
//!
//! [`VyaneService`] composes loaded config, the assembled runtime, selector
//! resolution, safe diagnostics, and owner-local query/control projections.
//! Execution front-ends share dispatch/broadcast semantics; generic REST/MCP
//! boundaries use allowlisted run/session views, while source-compatible local
//! embedding methods can still read the richer durable records explicitly.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use vyane_broker::ResidentBrokerSupervisor;
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, AttemptOutcome, BoundTarget, CancellationToken, ErrorKind,
    HarnessLifecycleReporter, HarnessSpawnAuthority, NativeSessionState, NativeSessionTransition,
    ProviderId, RunQuery, RunRecord, RunStatus, Sandbox, SessionRef, SessionSnapshot, Target,
    TaskSpec, Usage,
};
use vyane_kernel::{DispatchOutcome, PreparedDispatch, StreamDispatchEvent};

use crate::agent::AgentProjectionComponents;
use crate::config::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::diagnostics::{
    ConfigCheckReport, RoutePreview, RoutePreviewParams, check_config, route_preview,
};
use crate::message::MessageComponents;
use crate::owner::OwnerContext;
use crate::routing::{DispatchPlan, plan_dispatch};
use crate::selector::{resolve_target_chain, split_targets};
use crate::task::{build_task_spec, validate_user_routing_labels};

pub const RUN_VIEW_SCHEMA: u32 = 1;
pub const SESSION_VIEW_SCHEMA: u32 = 1;

/// Parameters for a single-target dispatch. Maps 1:1 to the CLI's `DispatchArgs`
/// (minus CLI-specific flags like `--detach`/`--stream`/`--json`).
#[derive(Debug, Clone)]
pub struct DispatchParams {
    pub task: String,
    /// Profile name or `provider/model`.
    pub target: String,
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    pub session: Option<String>,
    pub system: Option<String>,
    pub timeout_secs: Option<u64>,
    pub labels: Vec<String>,
}

/// One side-effect-free, fresh CLI-harness dispatch frozen for an immediate
/// authorized execution. The resolved chain is retained alongside the exact
/// kernel plan so trusted hosts can compare private submission evidence
/// without planning twice.
pub struct PreparedHarnessDispatch {
    task: TaskSpec,
    resolved_chain: Vec<BoundTarget>,
    prepared: PreparedDispatch,
}

impl PreparedHarnessDispatch {
    #[must_use]
    pub fn resolved_chain(&self) -> &[BoundTarget] {
        &self.resolved_chain
    }

    #[must_use]
    pub fn capability_snapshot(&self) -> &vyane_kernel::CapabilityPlanSnapshot {
        self.prepared.capability_snapshot()
    }
}

impl std::fmt::Debug for PreparedHarnessDispatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedHarnessDispatch")
            .field("targets", &self.resolved_chain.len())
            .finish_non_exhaustive()
    }
}

/// Parameters for a multi-target broadcast.
#[derive(Debug, Clone)]
pub struct BroadcastParams {
    pub task: String,
    /// Raw comma-separated list; each element is a profile or `provider/model`.
    pub targets: String,
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    pub system: Option<String>,
    pub timeout_secs: Option<u64>,
    pub labels: Vec<String>,
}

/// Read-only history filter.
#[derive(Debug, Clone, Default)]
pub struct HistoryFilter {
    pub limit: Option<usize>,
    pub status: Option<RunStatus>,
    pub provider: Option<String>,
}

/// One resolved target + its raw selector, returned by [`VyaneService::resolve`].
/// Kept as a pair so the broadcast path can label output rows by selector.
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub selector: String,
    pub chain: Vec<BoundTarget>,
}

/// Message-free outcome for one public run attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "result")]
#[non_exhaustive]
pub enum RunAttemptOutcomeView {
    Ok,
    Err {
        kind: ErrorKind,
        message: &'static str,
        failed_over: bool,
    },
}

/// Allowlisted attempt metadata. Provider/harness error text is intentionally
/// omitted because it may contain endpoints, local paths, or response bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct RunAttemptView {
    pub target: Target,
    pub transport: AdapterTransport,
    pub started_at: DateTime<Utc>,
    pub duration_ms: u64,
    pub outcome: RunAttemptOutcomeView,
}

/// Redacted run record for generic REST and MCP boundaries.
///
/// The durable ledger remains richer. This projection excludes owner,
/// prompt preview, workdir, free-form labels, raw attempt messages, terminal
/// error text, task digest, and the caller-chosen session id.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct RunView {
    pub view_schema: u32,
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    /// Compatibility placeholder for clients that deserialize the historical
    /// `RunRecord` shape. The durable prompt digest is never exposed here.
    pub task_digest: &'static str,
    pub sandbox: Sandbox,
    pub target: Target,
    pub transport: AdapterTransport,
    pub attempts: Vec<RunAttemptView>,
    pub status: RunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    pub session_attached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_chars: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_error_kind: Option<ErrorKind>,
}

impl From<RunRecord> for RunView {
    fn from(record: RunRecord) -> Self {
        let mut last_attempt_error_kind = None;
        let attempts = record
            .attempts
            .into_iter()
            .map(|attempt| {
                let outcome = match attempt.outcome {
                    AttemptOutcome::Ok => RunAttemptOutcomeView::Ok,
                    AttemptOutcome::Err {
                        kind, failed_over, ..
                    } => {
                        last_attempt_error_kind = Some(kind);
                        RunAttemptOutcomeView::Err {
                            kind,
                            message: "attempt failed",
                            failed_over,
                        }
                    }
                };
                RunAttemptView {
                    target: attempt.target,
                    transport: attempt.transport,
                    started_at: attempt.started_at,
                    duration_ms: attempt.duration_ms,
                    outcome,
                }
            })
            .collect();
        let terminal_error_kind = match record.status {
            RunStatus::Success => None,
            RunStatus::Timeout => Some(last_attempt_error_kind.unwrap_or(ErrorKind::Timeout)),
            RunStatus::Cancelled => Some(last_attempt_error_kind.unwrap_or(ErrorKind::Cancelled)),
            RunStatus::Error => Some(last_attempt_error_kind.unwrap_or(ErrorKind::Other)),
        };
        Self {
            view_schema: RUN_VIEW_SCHEMA,
            run_id: record.run_id,
            started_at: record.started_at,
            finished_at: record.finished_at,
            task_digest: "redacted",
            sandbox: record.sandbox,
            target: record.target,
            transport: record.transport,
            attempts,
            status: record.status,
            usage: record.usage,
            cost_usd: record.cost_usd,
            session_attached: record.session_id.is_some(),
            output_chars: record.output_chars,
            terminal_error_kind,
        }
    }
}

/// Allowlisted native-session state exposed by read-only front-ends.
///
/// The actual binding contains a runtime-native id, canonical workdir identity,
/// and scope digests. Those fields are execution authority and must not cross a
/// generic CLI, REST, or MCP listing boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionNativeState {
    Absent,
    LegacyUnbound,
    Bound,
    /// Forward-compatible projection for a state introduced by a newer core.
    Unknown,
}

impl SessionNativeState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::LegacyUnbound => "legacy_unbound",
            Self::Bound => "bound",
            Self::Unknown => "unknown",
        }
    }
}

/// Redacted, owner-scoped view of one persisted continuity session.
///
/// This deliberately omits the owner, transcript bodies, native runtime id,
/// canonical workdir, object identity, and endpoint/account/runtime digests.
/// Native resume is currently disabled even for a domain-bound session; the
/// explicit flag prevents callers from treating `bound` as authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SessionView {
    pub view_schema: u32,
    pub session_id: String,
    pub target: Target,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub run_count: u64,
    pub transcript_messages: u64,
    pub session_revision: u64,
    pub native_state: SessionNativeState,
    pub native_resume_available: bool,
}

impl From<SessionSnapshot> for SessionView {
    fn from(snapshot: SessionSnapshot) -> Self {
        let native_state = match snapshot.native_session {
            NativeSessionState::Absent => SessionNativeState::Absent,
            NativeSessionState::LegacyUnbound { .. } => SessionNativeState::LegacyUnbound,
            NativeSessionState::Bound { .. } => SessionNativeState::Bound,
            _ => SessionNativeState::Unknown,
        };
        let transcript_messages =
            u64::try_from(snapshot.record.transcript.len()).unwrap_or(u64::MAX);
        Self {
            view_schema: SESSION_VIEW_SCHEMA,
            session_id: snapshot.record.session_id,
            target: snapshot.record.target,
            created_at: snapshot.record.created_at,
            updated_at: snapshot.record.updated_at,
            run_count: snapshot.record.run_count,
            transcript_messages,
            session_revision: snapshot.session_revision,
            native_state,
            // A persisted domain is evidence, not execution permission. Keep
            // this false until an ActiveExecutionPermit consumer and exact
            // domain checks guard every native side effect.
            native_resume_available: false,
        }
    }
}

/// The shared service: holds a loaded config and a live runtime.
///
/// Clone-cheap (everything is behind an `Arc`). Legacy methods retain an
/// explicit trusted single-user `"local"` compatibility scope; authenticated
/// embedders freeze a resolved owner with [`Self::scope`]. Protocol-level
/// authentication and multi-user REST wiring remain separate concerns.
#[derive(Clone)]
pub struct VyaneService {
    loaded: Arc<LoadedConfig>,
    runtime: Arc<Runtime>,
    storage_paths: Arc<StoragePaths>,
}

/// Clone-cheap service facade with a durable owner frozen by value.
///
/// This is a service-layer authority boundary, not an HTTP authentication
/// implementation. Administrative cross-owner operations require a separate
/// typed capability and are intentionally outside this facade.
#[derive(Clone)]
pub struct OwnerScopedService {
    service: VyaneService,
    owner: Arc<str>,
    dispatcher: vyane_kernel::Dispatcher,
}

impl VyaneService {
    /// Load config from the default layers (or a single override path) and
    /// assemble the runtime against the resolved storage paths.
    pub fn load(config_override: Option<&std::path::Path>) -> Result<Self> {
        let loaded = load_config(config_override)?;
        Self::from_loaded(loaded)
    }

    /// Assemble from an already-loaded config.
    pub fn from_loaded(loaded: LoadedConfig) -> Result<Self> {
        let paths = StoragePaths::resolve()?;
        Self::from_loaded_with_paths(loaded, paths)
    }

    /// Assemble from an already-loaded config and explicit storage paths.
    ///
    /// Keeping this constructor free of environment lookup makes service and
    /// REST tests hermetic, and lets an embedding application choose one data
    /// root without changing process-global state.
    pub fn from_loaded_with_paths(loaded: LoadedConfig, paths: StoragePaths) -> Result<Self> {
        let runtime = Runtime::new(loaded.config.clone(), paths.clone())?;
        Ok(Self {
            loaded: Arc::new(loaded),
            runtime: Arc::new(runtime),
            storage_paths: Arc::new(paths),
        })
    }

    /// Construct the local owner control surface without loading model,
    /// credential, or project configuration. Session inspection/reset depends
    /// only on the storage root and must remain usable when execution config is
    /// broken.
    pub fn from_local_storage(paths: StoragePaths) -> Result<Self> {
        Self::from_loaded_with_paths(
            LoadedConfig {
                config: ResolvedConfig::default(),
                files: Vec::new(),
                secrets: BTreeMap::new(),
            },
            paths,
        )
    }

    /// Expose the loaded config (front-ends that need provider/profile metadata,
    /// like `vyane check`, read from this).
    pub fn config(&self) -> &LoadedConfig {
        &self.loaded
    }

    /// Expose the assembled runtime (the CLI's detached-worker path and the
    /// streaming path still need direct access to the dispatcher/ledger).
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Paths backing this service instance.
    #[must_use]
    pub fn storage_paths(&self) -> &StoragePaths {
        &self.storage_paths
    }

    /// Open the opt-in goal read surface with one previously authenticated
    /// durable owner frozen into it.
    ///
    /// Ordinary service construction deliberately does not open or migrate the
    /// goal database. Protocol hosts must opt in after establishing their
    /// owner authority.
    pub fn goal_reader(
        &self,
        context: OwnerContext,
    ) -> std::result::Result<crate::GoalReadService, crate::GoalReadError> {
        crate::GoalReadService::open(self.storage_paths(), context.owner())
    }

    /// Open the opt-in goal observation mutation port with one previously
    /// authenticated durable owner frozen into it. Source identity is bound
    /// separately before any typed fact can be recorded.
    pub fn goal_observation_ingress(
        &self,
        context: OwnerContext,
    ) -> std::result::Result<crate::GoalObservationIngress, crate::GoalObservationIngressError>
    {
        crate::GoalObservationIngress::open(self.storage_paths(), context.owner())
    }

    /// Assemble an opt-in, one-shot continuity runner from purpose-separated
    /// authenticated authority and explicitly supplied queue/execution ports.
    pub fn goal_continuity_runner(
        &self,
        authority: crate::GoalContinuityRunnerAuthority,
        queue: Option<Arc<dyn crate::GoalContinuityQueuePort>>,
        execute: Option<Arc<dyn crate::GoalContinuityExecutionPort>>,
        options: crate::GoalContinuityRunnerOptions,
    ) -> std::result::Result<crate::GoalContinuityRunner, crate::GoalContinuityRunnerError> {
        let crate::GoalContinuityRunnerAuthority {
            read,
            queue: queue_context,
            execute: execute_context,
        } = authority;
        let reader = self
            .goal_reader(read)
            .map_err(|_| crate::GoalContinuityRunnerError::Unavailable)?;
        let reader = crate::goal_continuity_runner::BlockingGoalProjectionReader::new(
            reader,
            options.max_concurrency,
        )?;
        crate::GoalContinuityRunner::assemble(
            Arc::new(reader),
            queue_context,
            queue,
            execute_context,
            execute,
            options,
        )
    }

    /// Assemble the owner-bound resident broker/projector loops for a daemon.
    /// Component construction stays behind the service boundary so a
    /// frontend cannot accidentally derive a second owner or storage scope.
    pub fn resident_broker(&self, owner: impl Into<String>) -> Result<ResidentBrokerSupervisor> {
        let messages = MessageComponents::open(&self.storage_paths, owner)?;
        let agents = AgentProjectionComponents::open(
            &self.storage_paths,
            messages.broker().scope().owner(),
        )?;
        messages.resident_broker_default(&agents)
    }

    /// Freeze a previously authenticated owner authority into a service
    /// facade. The owner-bound dispatcher is constructed synchronously, before
    /// any returned async operation can be spawned or polled elsewhere.
    #[must_use]
    pub fn scope(&self, context: OwnerContext) -> OwnerScopedService {
        let owner = context.owner();
        let dispatcher = self
            .runtime
            .dispatcher
            .clone()
            .with_owner(owner.to_string());
        OwnerScopedService {
            service: self.clone(),
            owner,
            dispatcher,
        }
    }

    /// Resolve a selector into a failover chain without dispatching. Useful for
    /// config validation (`vyane check`) and dry-run API calls.
    pub fn resolve(&self, selector: &str) -> Result<ResolvedTarget> {
        let chain = resolve_target_chain(&self.loaded, selector)?;
        Ok(ResolvedTarget {
            selector: selector.to_string(),
            chain,
        })
    }

    /// Produce a deterministic, redacted route preview without dispatching,
    /// probing a provider, inspecting harness availability, or spawning.
    pub fn route_preview(&self, params: RoutePreviewParams) -> Result<RoutePreview> {
        route_preview(&self.loaded, params)
    }

    /// Inspect the already-loaded configuration using static resolution only.
    /// The returned allowlisted DTO contains no paths, endpoints, environment
    /// names, credential values, or raw resolver diagnostics.
    pub fn check_config(&self) -> Result<ConfigCheckReport> {
        check_config(&self.loaded)
    }

    /// Dispatch a single task to a resolved chain, producing one recorded run.
    ///
    /// The caller supplies the cancellation token so front-ends can wire their
    /// own cancellation (ctrl-c, HTTP shutdown, MCP transport close).
    pub async fn dispatch(
        &self,
        params: DispatchParams,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        self.scope(OwnerContext::single_user_local())
            .dispatch(params, cancel)
            .await
    }

    /// Dispatch one local, fresh CLI-harness task under live spawn authority.
    ///
    /// Direct-HTTP targets and logical sessions are rejected before executor
    /// construction. Protocol hosts with authenticated owners must use the
    /// corresponding [`OwnerScopedService`] method.
    pub async fn dispatch_harness_authorized(
        &self,
        params: DispatchParams,
        spawn_authority: HarnessSpawnAuthority,
        lifecycle_reporter: HarnessLifecycleReporter,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        self.scope(OwnerContext::single_user_local())
            .dispatch_harness_authorized(params, spawn_authority, lifecycle_reporter, cancel)
            .await
    }

    /// Stream one local single-target dispatch.
    ///
    /// Authenticated protocol hosts must use the corresponding method on
    /// [`OwnerScopedService`] so they never obtain the runtime's default-local
    /// dispatcher.
    pub async fn dispatch_stream<F>(
        &self,
        params: DispatchParams,
        cancel: CancellationToken,
        on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        self.scope(OwnerContext::single_user_local())
            .dispatch_stream(params, cancel, on_event)
            .await
    }

    async fn dispatch_stream_with<F>(
        &self,
        dispatcher: &vyane_kernel::Dispatcher,
        params: DispatchParams,
        cancel: CancellationToken,
        on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        if params.session.is_some() {
            anyhow::bail!("streaming does not support sessions");
        }
        let selector = params.target.clone();
        let mut task = self.task_from_dispatch(params)?;
        validate_user_routing_labels(&task.labels)?;
        let plan = self.plan_dispatch(&selector, &mut task)?;
        let [bound] = plan.chain.as_slice() else {
            anyhow::bail!("streaming requires exactly one resolved target");
        };
        dispatcher
            .dispatch_stream(&task, bound, cancel, on_event)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn dispatch_with(
        &self,
        dispatcher: &vyane_kernel::Dispatcher,
        params: DispatchParams,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        let selector = params.target.clone();
        let mut task = self.task_from_dispatch(params)?;
        validate_user_routing_labels(&task.labels)?;
        let plan = self.plan_dispatch(&selector, &mut task)?;
        dispatcher
            .dispatch(&task, plan.chain, cancel)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn dispatch_harness_authorized_with(
        &self,
        dispatcher: &vyane_kernel::Dispatcher,
        params: DispatchParams,
        spawn_authority: HarnessSpawnAuthority,
        lifecycle_reporter: HarnessLifecycleReporter,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        let prepared = self.prepare_harness_dispatch_with(dispatcher, params)?;
        self.execute_prepared_harness_authorized_with(
            dispatcher,
            prepared,
            spawn_authority,
            lifecycle_reporter,
            cancel,
        )
        .await
    }

    fn prepare_harness_dispatch_with(
        &self,
        dispatcher: &vyane_kernel::Dispatcher,
        params: DispatchParams,
    ) -> Result<PreparedHarnessDispatch> {
        if params.session.is_some() {
            anyhow::bail!("authorized harness dispatch supports only fresh sessionless execution");
        }
        let selector = params.target.clone();
        let mut task = self.task_from_dispatch(params)?;
        validate_user_routing_labels(&task.labels)?;
        let plan = self.plan_dispatch(&selector, &mut task)?;
        if plan
            .chain
            .iter()
            .any(|target| target.transport != AdapterTransport::CliWrap)
        {
            return Err(anyhow::Error::from(vyane_core::VyaneError::unsupported(
                "authorized harness dispatch supports only CLI harness targets",
            )));
        }
        let resolved_chain = plan.chain.clone();
        let prepared = dispatcher.prepare(&task, plan.chain)?;
        Ok(PreparedHarnessDispatch {
            task,
            resolved_chain,
            prepared,
        })
    }

    async fn execute_prepared_harness_authorized_with(
        &self,
        dispatcher: &vyane_kernel::Dispatcher,
        prepared: PreparedHarnessDispatch,
        spawn_authority: HarnessSpawnAuthority,
        lifecycle_reporter: HarnessLifecycleReporter,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        dispatcher
            .dispatch_prepared_harness_authorized(
                &prepared.task,
                prepared.prepared,
                spawn_authority,
                lifecycle_reporter,
                cancel,
            )
            .await
            .map_err(anyhow::Error::from)
    }

    /// Turn an explicit selector or `auto` into a concrete dispatch chain.
    /// Auto-routing also records the canonical decision fields as task labels
    /// so the ledger explains why a model was selected.
    pub fn plan_dispatch(&self, selector: &str, task: &mut TaskSpec) -> Result<DispatchPlan> {
        plan_dispatch(&self.loaded, selector, task)
    }

    /// Fan out one task across multiple targets concurrently.
    ///
    /// Each comma-separated target is resolved into its own chain, then all
    /// chains are dispatched under the kernel's concurrency semaphore. Results
    /// are returned in input order, paired with their raw selector.
    ///
    /// A target that fails to **resolve** (unknown profile, missing provider)
    /// becomes a per-target `Err` in the result vector — it does NOT abort the
    /// whole broadcast. This matches the kernel's own partial-failure contract
    /// for `Dispatcher::broadcast`: the good targets still run, the bad ones
    /// surface their resolution error in their slot. Only a failure in task-spec
    /// construction or target-list parsing (caller-fault input) aborts early.
    pub async fn broadcast(
        &self,
        params: BroadcastParams,
        cancel: CancellationToken,
    ) -> Result<Vec<(String, anyhow::Result<DispatchOutcome>)>> {
        self.scope(OwnerContext::single_user_local())
            .broadcast(params, cancel)
            .await
    }

    async fn broadcast_with(
        &self,
        dispatcher: &vyane_kernel::Dispatcher,
        params: BroadcastParams,
        cancel: CancellationToken,
    ) -> Result<Vec<(String, anyhow::Result<DispatchOutcome>)>> {
        let targets = split_targets(&params.targets)?;
        let task = build_task_spec(
            params.task,
            params.workdir,
            params.sandbox,
            params.system,
            params.timeout_secs,
            params.labels,
        )?;
        validate_user_routing_labels(&task.labels)?;

        // Resolve each target independently: a resolution failure on one
        // target is a per-target error, not a broadcast-wide abort.
        let mut chains: Vec<Option<Vec<BoundTarget>>> = Vec::with_capacity(targets.len());
        let mut resolve_errors: Vec<Option<anyhow::Error>> = Vec::with_capacity(targets.len());
        for target in &targets {
            match resolve_target_chain(&self.loaded, target) {
                Ok(chain) => {
                    chains.push(Some(chain));
                    resolve_errors.push(None);
                }
                Err(e) => {
                    chains.push(None);
                    resolve_errors.push(Some(e));
                }
            }
        }

        // Only dispatch the targets that resolved; pad the results to match the
        // full target list so zip alignment is preserved.
        let resolved_indices: Vec<usize> = chains
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.as_ref().map(|_| i))
            .collect();
        let resolved_chains: Vec<Vec<BoundTarget>> = chains.into_iter().flatten().collect();

        let dispatch_results = if resolved_chains.is_empty() {
            Vec::new()
        } else {
            dispatcher.broadcast(&task, resolved_chains, cancel).await
        };

        // Reassemble: resolved targets get their dispatch result, unresolved
        // targets get their resolution error — both in input order.
        let mut merged = Vec::with_capacity(targets.len());
        let mut dispatch_iter = dispatch_results.into_iter();
        for (i, selector) in targets.into_iter().enumerate() {
            if resolved_indices.contains(&i) {
                let result = dispatch_iter
                    .next()
                    .map(|r| r.map_err(anyhow::Error::from))
                    .unwrap_or_else(|| Err(anyhow::anyhow!("missing dispatch result")));
                merged.push((selector, result));
            } else {
                let err = resolve_errors[i]
                    .take()
                    .unwrap_or_else(|| anyhow::anyhow!("resolution failed"));
                merged.push((selector, Err(err)));
            }
        }

        Ok(merged)
    }

    /// Query the run ledger (read-only).
    pub async fn history(&self, filter: HistoryFilter) -> Result<Vec<RunRecord>> {
        self.scope(OwnerContext::single_user_local())
            .history(filter)
            .await
    }

    async fn history_with(&self, owner: &str, filter: HistoryFilter) -> Result<Vec<RunRecord>> {
        self.runtime
            .ledger
            .query(RunQuery {
                owner: Some(owner.to_string()),
                provider: filter.provider.map(ProviderId::new),
                status: filter.status,
                since: None,
                limit: filter.limit,
            })
            .await
            .context("query ledger")
    }

    /// Query the run ledger through the allowlisted projection used by generic
    /// protocol front-ends.
    pub async fn history_views(&self, filter: HistoryFilter) -> Result<Vec<RunView>> {
        self.scope(OwnerContext::single_user_local())
            .history_views(filter)
            .await
    }

    /// List legacy session records for source compatibility with local
    /// embedders. Generic protocol front-ends must use [`Self::session_views`].
    pub async fn sessions(&self) -> Result<Vec<vyane_core::SessionRecord>> {
        self.scope(OwnerContext::single_user_local())
            .sessions()
            .await
    }

    /// List saved sessions as redacted, revision-aware views.
    pub async fn session_views(&self) -> Result<Vec<SessionView>> {
        self.scope(OwnerContext::single_user_local())
            .session_views()
            .await
    }

    async fn session_views_with(&self, owner: &str) -> Result<Vec<SessionView>> {
        let snapshots = self
            .runtime
            .sessions
            .list_snapshots(owner)
            .await
            .context("list session snapshots")?;
        Ok(snapshots.into_iter().map(SessionView::from).collect())
    }

    /// Inspect one local session without exposing transcript or native binding
    /// authority.
    pub async fn session(&self, session_id: &str) -> vyane_core::Result<Option<SessionView>> {
        self.scope(OwnerContext::single_user_local())
            .session(session_id)
            .await
    }

    async fn session_with(
        &self,
        owner: &str,
        session_id: &str,
    ) -> vyane_core::Result<Option<SessionView>> {
        Ok(self
            .runtime
            .sessions
            .load_snapshot(owner, session_id)
            .await?
            .map(SessionView::from))
    }

    /// Reset native continuity with an explicit compare-and-swap revision.
    ///
    /// This remains the only native-session mutation safe to expose while the
    /// trusted fresh-run producer and active-permit/native-domain consumers are
    /// unfinished. It never accepts owner, target, transcript, native id, or
    /// domain data from the caller, and the store fences it against a live
    /// session execution lease.
    pub async fn reset_native_session(
        &self,
        session_id: &str,
        expected_revision: u64,
    ) -> vyane_core::Result<SessionView> {
        self.scope(OwnerContext::single_user_local())
            .reset_native_session(session_id, expected_revision)
            .await
    }

    async fn reset_native_session_with(
        &self,
        owner: &str,
        session_id: &str,
        expected_revision: u64,
    ) -> vyane_core::Result<SessionView> {
        self.runtime
            .sessions
            .apply_native_transition(
                owner,
                session_id,
                &NativeSessionTransition::Reset { expected_revision },
            )
            .await
            .map(SessionView::from)
    }

    /// Build a TaskSpec from dispatch params (used by the detached-worker path,
    /// which needs the spec serialized to a job file before re-exec).
    pub fn task_from_dispatch(&self, params: DispatchParams) -> Result<TaskSpec> {
        let mut task = build_task_spec(
            params.task,
            params.workdir,
            params.sandbox,
            params.system,
            params.timeout_secs,
            params.labels,
        )?;
        if let Some(session) = params.session {
            task.session = Some(SessionRef::new(session));
        }
        Ok(task)
    }
}

impl OwnerScopedService {
    /// Loaded configuration shared across owner scopes.
    #[must_use]
    pub fn config(&self) -> &LoadedConfig {
        self.service.config()
    }

    /// Storage paths shared across owner scopes.
    #[must_use]
    pub fn storage_paths(&self) -> &StoragePaths {
        self.service.storage_paths()
    }

    /// Resolve a selector without performing an owner-sensitive operation.
    pub fn resolve(&self, selector: &str) -> Result<ResolvedTarget> {
        self.service.resolve(selector)
    }

    /// Produce a static, redacted route preview.
    pub fn route_preview(&self, params: RoutePreviewParams) -> Result<RoutePreview> {
        self.service.route_preview(params)
    }

    /// Inspect static configuration.
    pub fn check_config(&self) -> Result<ConfigCheckReport> {
        self.service.check_config()
    }

    /// Plan a dispatch while retaining this facade's frozen owner authority.
    pub fn plan_dispatch(&self, selector: &str, task: &mut TaskSpec) -> Result<DispatchPlan> {
        self.service.plan_dispatch(selector, task)
    }

    /// Build a task without reading owner authority from its payload or labels.
    pub fn task_from_dispatch(&self, params: DispatchParams) -> Result<TaskSpec> {
        self.service.task_from_dispatch(params)
    }

    /// Dispatch under the owner frozen when this facade was created.
    pub async fn dispatch(
        &self,
        params: DispatchParams,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        self.service
            .dispatch_with(&self.dispatcher, params, cancel)
            .await
    }

    /// Dispatch one fresh CLI-harness task under the frozen owner and live
    /// subprocess-spawn authority. Runtime callbacks remain process-local and
    /// are not copied into durable task or run metadata.
    pub async fn dispatch_harness_authorized(
        &self,
        params: DispatchParams,
        spawn_authority: HarnessSpawnAuthority,
        lifecycle_reporter: HarnessLifecycleReporter,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        self.service
            .dispatch_harness_authorized_with(
                &self.dispatcher,
                params,
                spawn_authority,
                lifecycle_reporter,
                cancel,
            )
            .await
    }

    /// Freeze one fresh CLI-only dispatch without constructing an executor or
    /// touching a model/harness. The returned plan must be consumed by the
    /// matching `execute_prepared_harness_authorized` method on this facade.
    pub fn prepare_harness_dispatch(
        &self,
        params: DispatchParams,
    ) -> Result<PreparedHarnessDispatch> {
        self.service
            .prepare_harness_dispatch_with(&self.dispatcher, params)
    }

    /// Consume a plan produced by this owner-scoped facade under live process
    /// spawn and lifecycle authority.
    pub async fn execute_prepared_harness_authorized(
        &self,
        prepared: PreparedHarnessDispatch,
        spawn_authority: HarnessSpawnAuthority,
        lifecycle_reporter: HarnessLifecycleReporter,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        self.service
            .execute_prepared_harness_authorized_with(
                &self.dispatcher,
                prepared,
                spawn_authority,
                lifecycle_reporter,
                cancel,
            )
            .await
    }

    /// Stream one single-target dispatch under the frozen owner.
    pub async fn dispatch_stream<F>(
        &self,
        params: DispatchParams,
        cancel: CancellationToken,
        on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        self.service
            .dispatch_stream_with(&self.dispatcher, params, cancel, on_event)
            .await
    }

    /// Broadcast under the owner frozen when this facade was created.
    pub async fn broadcast(
        &self,
        params: BroadcastParams,
        cancel: CancellationToken,
    ) -> Result<Vec<(String, anyhow::Result<DispatchOutcome>)>> {
        self.service
            .broadcast_with(&self.dispatcher, params, cancel)
            .await
    }

    /// Query only this owner's run records.
    pub async fn history(&self, filter: HistoryFilter) -> Result<Vec<RunRecord>> {
        self.service.history_with(&self.owner, filter).await
    }

    /// Query only this owner's allowlisted run views.
    pub async fn history_views(&self, filter: HistoryFilter) -> Result<Vec<RunView>> {
        Ok(self
            .history(filter)
            .await?
            .into_iter()
            .map(RunView::from)
            .collect())
    }

    /// List only this owner's legacy session records.
    pub async fn sessions(&self) -> Result<Vec<vyane_core::SessionRecord>> {
        self.service
            .runtime
            .sessions
            .list(&self.owner)
            .await
            .context("list sessions")
    }

    /// List only this owner's redacted session views.
    pub async fn session_views(&self) -> Result<Vec<SessionView>> {
        self.service.session_views_with(&self.owner).await
    }

    /// Inspect one session in this owner namespace. A foreign session with the
    /// same identifier is indistinguishable from an absent session.
    pub async fn session(&self, session_id: &str) -> vyane_core::Result<Option<SessionView>> {
        self.service.session_with(&self.owner, session_id).await
    }

    /// Reset native continuity only in this owner namespace.
    pub async fn reset_native_session(
        &self,
        session_id: &str,
        expected_revision: u64,
    ) -> vyane_core::Result<SessionView> {
        self.service
            .reset_native_session_with(&self.owner, session_id, expected_revision)
            .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use tempfile::TempDir;
    use vyane_config::{ProfilePatch, ResolvedConfig};
    use vyane_core::{
        AdapterTransport, Attempt, ChatClient, ChatMessage, ChatOutcome, ChatRequest, HarnessKind,
        Ledger, ModelId, NativeSessionBinding, NativeSessionDomain, Protocol, ProviderId, Role,
        RunRecord, SessionRecord, SessionStore, WorkdirIdentity,
    };
    use vyane_kernel::{Dispatcher, Executor, ExecutorFactory};
    use vyane_ledger::{FsSessionStore, JsonlLedger};
    use vyane_provider::{Provider, ProviderRegistry};

    use super::*;

    fn target() -> Target {
        Target {
            provider: ProviderId::new("provider"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("model"),
        }
    }

    fn service(directory: &TempDir) -> VyaneService {
        VyaneService::from_loaded_with_paths(
            LoadedConfig {
                config: ResolvedConfig::default(),
                files: Vec::new(),
                secrets: BTreeMap::new(),
            },
            StoragePaths::from_data_dir(directory.path()),
        )
        .unwrap()
    }

    struct PrincipalResolver;

    struct PrincipalAuthenticator;

    impl crate::PrincipalAuthenticator for PrincipalAuthenticator {
        fn authenticate(&self, credential: &[u8]) -> Result<String> {
            Ok(std::str::from_utf8(credential)?.to_string())
        }
    }

    impl crate::PrincipalOwnerResolver for PrincipalResolver {
        fn resolve_owner(&self, principal: &crate::AuthenticatedPrincipal) -> Result<String> {
            Ok(format!("tenant-{}", principal.subject()))
        }
    }

    fn owner_context(subject: &str) -> OwnerContext {
        crate::OwnerContextFactory::new(
            Arc::new(PrincipalAuthenticator),
            Arc::new(PrincipalResolver),
        )
        .authenticate(subject.as_bytes())
        .unwrap()
    }

    struct SuccessfulChat;

    #[async_trait]
    impl ChatClient for SuccessfulChat {
        fn protocol(&self) -> Protocol {
            Protocol::OpenaiChat
        }

        async fn complete(&self, _: ChatRequest) -> vyane_core::Result<ChatOutcome> {
            Ok(ChatOutcome {
                text: "ok".into(),
                ..Default::default()
            })
        }
    }

    struct SuccessfulFactory;

    impl ExecutorFactory for SuccessfulFactory {
        fn make(&self, _: &BoundTarget) -> vyane_core::Result<Executor> {
            Ok(Executor::Chat(Arc::new(SuccessfulChat)))
        }
    }

    fn executable_service(directory: &TempDir) -> VyaneService {
        let paths = StoragePaths::from_data_dir(directory.path());
        std::fs::create_dir_all(&paths.sessions_dir).unwrap();
        let ledger: Arc<dyn Ledger> = Arc::new(JsonlLedger::new(&paths.ledger_path));
        let sessions: Arc<dyn SessionStore> = Arc::new(FsSessionStore::new(&paths.sessions_dir));
        let dispatcher = Dispatcher::new(
            Arc::new(SuccessfulFactory),
            Arc::clone(&ledger),
            Arc::clone(&sessions),
        );
        let mut providers = ProviderRegistry::new();
        providers.insert(
            "provider",
            Provider {
                base_url: "https://example.invalid".into(),
                api_key_env: None,
                auth_style: vyane_core::AuthStyle::Bearer,
                protocol: Protocol::OpenaiChat,
                default_model: Some(ModelId::new("model")),
                extra: Default::default(),
                env_inject: Default::default(),
            },
        );
        let loaded = LoadedConfig {
            config: ResolvedConfig {
                providers,
                profiles: BTreeMap::from([(
                    "test".into(),
                    ProfilePatch {
                        provider: Some("provider".into()),
                        protocol: Some(Protocol::OpenaiChat),
                        harness: Some("none".into()),
                        model: Some(ModelId::new("model")),
                        ..Default::default()
                    },
                )]),
            },
            files: Vec::new(),
            secrets: BTreeMap::new(),
        };
        VyaneService {
            loaded: Arc::new(loaded),
            runtime: Arc::new(Runtime {
                dispatcher,
                ledger,
                sessions,
            }),
            storage_paths: Arc::new(paths),
        }
    }

    fn dispatch_params(labels: Vec<String>) -> DispatchParams {
        DispatchParams {
            task: "task".into(),
            target: "test".into(),
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            session: None,
            system: None,
            timeout_secs: None,
            labels,
        }
    }

    fn record(owner: &str, session_id: &str) -> SessionRecord {
        let now = Utc::now();
        SessionRecord {
            session_id: session_id.into(),
            owner: owner.into(),
            target: target(),
            native_session_id: Some("CANARY_NATIVE_ID".into()),
            transcript: vec![ChatMessage {
                role: Role::User,
                content: "CANARY_TRANSCRIPT_BODY".into(),
            }],
            created_at: now,
            updated_at: now,
            run_count: 4,
        }
    }

    #[test]
    fn bound_session_view_omits_every_native_authority_field() {
        let mut record = record("local", "session");
        record.native_session_id = None;
        let domain = NativeSessionDomain {
            runtime: "CANARY_RUNTIME".into(),
            harness: HarnessKind::ClaudeCode,
            provider: ProviderId::new("provider"),
            protocol: Protocol::AnthropicMessages,
            model: ModelId::new("model"),
            endpoint_routing_digest: "CANARY_ENDPOINT_DIGEST".into(),
            canonical_workdir: "/CANARY_WORKDIR".into(),
            workdir_identity: WorkdirIdentity {
                device: 123,
                inode: 456,
            },
            checkpoint_namespace: "CANARY_CHECKPOINT".into(),
            checkpoint_schema: 1,
            account_scope_digest: "CANARY_ACCOUNT_DIGEST".into(),
            runtime_scope_digest: "CANARY_RUNTIME_DIGEST".into(),
        };
        let view = SessionView::from(SessionSnapshot {
            record,
            session_revision: 7,
            native_session: NativeSessionState::Bound {
                binding: Box::new(NativeSessionBinding {
                    native_session_id: "CANARY_BOUND_ID".into(),
                    domain,
                }),
            },
        });

        assert_eq!(view.native_state, SessionNativeState::Bound);
        assert!(!view.native_resume_available);
        assert_eq!(view.session_revision, 7);
        assert_eq!(view.transcript_messages, 1);
        let wire = serde_json::to_string(&view).unwrap();
        for canary in [
            "CANARY_TRANSCRIPT_BODY",
            "CANARY_BOUND_ID",
            "CANARY_RUNTIME",
            "CANARY_ENDPOINT_DIGEST",
            "CANARY_WORKDIR",
            "CANARY_CHECKPOINT",
            "CANARY_ACCOUNT_DIGEST",
        ] {
            assert!(!wire.contains(canary), "leaked {canary}");
        }
        let compatible: SessionRecord = serde_json::from_str(&wire).unwrap();
        assert_eq!(compatible.session_id, "session");
        assert!(compatible.native_session_id.is_none());
        assert!(compatible.transcript.is_empty());
    }

    #[test]
    fn run_view_omits_prompt_path_labels_messages_and_session_id() {
        let now = Utc::now();
        let record = RunRecord {
            run_id: "run".into(),
            owner: "CANARY_OWNER".into(),
            started_at: now,
            finished_at: now,
            task_digest: "CANARY_TASK_DIGEST".into(),
            task_preview: Some("CANARY_PROMPT".into()),
            workdir: Some("/CANARY_WORKDIR".into()),
            sandbox: Sandbox::ReadOnly,
            target: target(),
            transport: AdapterTransport::DirectHttp,
            attempts: vec![Attempt {
                target: target(),
                transport: AdapterTransport::DirectHttp,
                started_at: now,
                duration_ms: 1,
                outcome: AttemptOutcome::Err {
                    kind: ErrorKind::Protocol,
                    message: "CANARY_ATTEMPT_ERROR".into(),
                    failed_over: false,
                },
            }],
            status: RunStatus::Error,
            usage: None,
            cost_usd: None,
            session_id: Some("CANARY_SESSION_ID".into()),
            output_chars: None,
            error: Some("CANARY_TERMINAL_ERROR".into()),
            labels: BTreeMap::from([("CANARY_LABEL".into(), "CANARY_VALUE".into())]),
        };

        let view = RunView::from(record);
        assert!(view.session_attached);
        assert_eq!(view.terminal_error_kind, Some(ErrorKind::Protocol));
        let wire = serde_json::to_string(&view).unwrap();
        for canary in [
            "CANARY_OWNER",
            "CANARY_TASK_DIGEST",
            "CANARY_PROMPT",
            "CANARY_WORKDIR",
            "CANARY_ATTEMPT_ERROR",
            "CANARY_SESSION_ID",
            "CANARY_TERMINAL_ERROR",
            "CANARY_LABEL",
            "CANARY_VALUE",
        ] {
            assert!(!wire.contains(canary), "leaked {canary}");
        }
        let mut compatible: RunRecord = serde_json::from_str(&wire).unwrap();
        assert_eq!(compatible.task_digest, "redacted");
        let AttemptOutcome::Err { message, .. } = &compatible.attempts[0].outcome else {
            panic!("expected compatible error attempt");
        };
        assert_eq!(message, "attempt failed");

        compatible.status = RunStatus::Success;
        compatible.error = None;
        compatible.attempts[0].outcome = AttemptOutcome::Ok;
        let success_wire = serde_json::to_string(&RunView::from(compatible)).unwrap();
        let success: RunRecord = serde_json::from_str(&success_wire).unwrap();
        assert_eq!(success.status, RunStatus::Success);
        assert!(matches!(success.attempts[0].outcome, AttemptOutcome::Ok));
    }

    #[tokio::test]
    async fn service_lists_inspects_and_resets_only_the_local_owner() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        service
            .runtime()
            .sessions
            .save("local", &record("local", "shared"))
            .await
            .unwrap();
        service
            .runtime()
            .sessions
            .save("other", &record("other", "foreign-only"))
            .await
            .unwrap();

        let listed = service.session_views().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id, "shared");
        assert_eq!(listed[0].native_state, SessionNativeState::LegacyUnbound);
        assert!(!listed[0].native_resume_available);
        let revision = listed[0].session_revision;

        assert!(service.session("foreign-only").await.unwrap().is_none());
        let foreign_reset = service
            .reset_native_session("foreign-only", 0)
            .await
            .unwrap_err();
        assert_eq!(foreign_reset.kind, ErrorKind::NotFound);
        let foreign = service
            .runtime()
            .sessions
            .load_snapshot("other", "foreign-only")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            foreign.native_session,
            NativeSessionState::LegacyUnbound { .. }
        ));
        let reset = service
            .reset_native_session("shared", revision)
            .await
            .unwrap();
        assert_eq!(reset.native_state, SessionNativeState::Absent);
        assert_eq!(reset.session_revision, revision + 1);
        assert_eq!(reset.run_count, 4);
        assert_eq!(reset.transcript_messages, 1);

        let conflict = service
            .reset_native_session("shared", revision)
            .await
            .unwrap_err();
        assert_eq!(conflict.kind, ErrorKind::Conflict);

        service
            .runtime()
            .sessions
            .save("local", &record("local", "race"))
            .await
            .unwrap();
        let race_revision = service
            .session("race")
            .await
            .unwrap()
            .unwrap()
            .session_revision;
        let first = service.clone();
        let second = service.clone();
        let (left, right) = tokio::join!(
            first.reset_native_session("race", race_revision),
            second.reset_native_session("race", race_revision),
        );
        let outcomes = [left, right];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter_map(|result| result.as_ref().err())
                .filter(|error| error.kind == ErrorKind::Conflict)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn scoped_dispatch_and_broadcast_persist_only_the_frozen_owner() {
        let directory = TempDir::new().unwrap();
        let service = executable_service(&directory);
        let owner_a = service.scope(owner_context("a"));
        let owner_b = service.scope(owner_context("b"));

        // Payload labels are data, never owner authority. Creating the future
        // after the scope and moving it to another task preserves owner A.
        let spawned = owner_a.clone();
        tokio::spawn(async move {
            spawned
                .dispatch(
                    dispatch_params(vec!["owner=tenant-b".into()]),
                    CancellationToken::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap();

        owner_b
            .dispatch(dispatch_params(Vec::new()), CancellationToken::new())
            .await
            .unwrap();
        owner_a
            .broadcast(
                BroadcastParams {
                    task: "broadcast".into(),
                    targets: "test,test".into(),
                    workdir: None,
                    sandbox: Sandbox::ReadOnly,
                    system: None,
                    timeout_secs: None,
                    labels: vec!["owner=tenant-b".into()],
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();

        let a_runs = owner_a.history(HistoryFilter::default()).await.unwrap();
        let b_runs = owner_b.history(HistoryFilter::default()).await.unwrap();
        assert_eq!(a_runs.len(), 3);
        assert!(a_runs.iter().all(|run| run.owner == "tenant-a"));
        assert_eq!(b_runs.len(), 1);
        assert!(b_runs.iter().all(|run| run.owner == "tenant-b"));
        assert_eq!(
            a_runs
                .iter()
                .filter(|run| run.labels.get("owner").is_some_and(|v| v == "tenant-b"))
                .count(),
            3
        );

        let mut same_a = a_runs[0].clone();
        same_a.run_id = "shared-run".into();
        same_a.owner = "tenant-a".into();
        let mut same_b = same_a.clone();
        same_b.owner = "tenant-b".into();
        service.runtime().ledger.append(&same_a).await.unwrap();
        service.runtime().ledger.append(&same_b).await.unwrap();
        let a_shared = owner_a
            .history(HistoryFilter::default())
            .await
            .unwrap()
            .into_iter()
            .filter(|run| run.run_id == "shared-run")
            .collect::<Vec<_>>();
        let b_shared = owner_b
            .history(HistoryFilter::default())
            .await
            .unwrap()
            .into_iter()
            .filter(|run| run.run_id == "shared-run")
            .collect::<Vec<_>>();
        assert_eq!(a_shared.len(), 1);
        assert_eq!(a_shared[0].owner, "tenant-a");
        assert_eq!(b_shared.len(), 1);
        assert_eq!(b_shared[0].owner, "tenant-b");
    }

    #[tokio::test]
    async fn authorized_service_dispatch_rejects_http_and_sessions_without_a_run() {
        let directory = TempDir::new().unwrap();
        let service = executable_service(&directory);
        let scoped = service.scope(owner_context("authorized"));
        let authority = || HarnessSpawnAuthority::new(|| true);
        let reporter = || HarnessLifecycleReporter::new(|_| Ok(()));

        let error = scoped
            .dispatch_harness_authorized(
                dispatch_params(Vec::new()),
                authority(),
                reporter(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        let typed = error
            .downcast_ref::<vyane_core::VyaneError>()
            .expect("kernel rejection retains its typed error");
        assert_eq!(typed.kind, ErrorKind::Unsupported);

        let mut session = dispatch_params(Vec::new());
        session.session = Some("existing".into());
        let error = scoped
            .dispatch_harness_authorized(session, authority(), reporter(), CancellationToken::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fresh sessionless execution"));
        assert!(
            scoped
                .history(HistoryFilter::default())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn scoped_sessions_isolate_same_id_and_reset_cannot_cross_owner() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let owner_a = service.scope(owner_context("a"));
        let owner_b = service.scope(owner_context("b"));
        service
            .runtime()
            .sessions
            .save("tenant-a", &record("tenant-a", "shared"))
            .await
            .unwrap();
        service
            .runtime()
            .sessions
            .save("tenant-b", &record("tenant-b", "shared"))
            .await
            .unwrap();

        let a = owner_a.session("shared").await.unwrap().unwrap();
        let b = owner_b.session("shared").await.unwrap().unwrap();
        assert_eq!(owner_a.session_views().await.unwrap().len(), 1);
        assert_eq!(owner_b.sessions().await.unwrap().len(), 1);

        owner_a
            .reset_native_session("shared", a.session_revision)
            .await
            .unwrap();
        let b_after = owner_b.session("shared").await.unwrap().unwrap();
        assert_eq!(b_after.session_revision, b.session_revision);
        assert_eq!(b_after.native_state, SessionNativeState::LegacyUnbound);

        // A resource that exists only in B is absent from A, including reset.
        service
            .runtime()
            .sessions
            .save("tenant-b", &record("tenant-b", "foreign-only"))
            .await
            .unwrap();
        assert!(owner_a.session("foreign-only").await.unwrap().is_none());
        let error = owner_a
            .reset_native_session("foreign-only", 0)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::NotFound);
    }
}
