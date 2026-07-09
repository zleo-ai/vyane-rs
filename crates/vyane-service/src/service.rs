//! The high-level service facade.
//!
//! [`VyaneService`] composes a loaded config, the assembled runtime, and the
//! selector-resolution logic into the four operations a front-end needs:
//! dispatch, broadcast, history, and sessions. Every front-end (CLI, REST,
//! MCP) constructs one of these and calls the same methods, so dispatch
//! semantics are identical regardless of the protocol entry point.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use vyane_core::{
    BoundTarget, CancellationToken, ProviderId, RunQuery, RunStatus, Sandbox, SessionRef, TaskSpec,
};
use vyane_kernel::DispatchOutcome;

use crate::config::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::selector::{resolve_target_chain, split_targets};
use crate::task::build_task_spec;

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

/// The shared service: holds a loaded config and a live runtime.
///
/// Clone-cheap (everything is behind an `Arc`). The owner scope is fixed to
/// `"local"` for now — multi-user owner override is a future concern, tracked
/// separately (the kernel already accepts an owner on `Dispatcher::with_owner`).
#[derive(Clone)]
pub struct VyaneService {
    loaded: Arc<LoadedConfig>,
    runtime: Arc<Runtime>,
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
        let runtime = Runtime::new(loaded.config.clone(), paths)?;
        Ok(Self {
            loaded: Arc::new(loaded),
            runtime: Arc::new(runtime),
        })
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

    /// Resolve a selector into a failover chain without dispatching. Useful for
    /// config validation (`vyane check`) and dry-run API calls.
    pub fn resolve(&self, selector: &str) -> Result<ResolvedTarget> {
        let chain = resolve_target_chain(&self.loaded, selector)?;
        Ok(ResolvedTarget {
            selector: selector.to_string(),
            chain,
        })
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
        let chain = resolve_target_chain(&self.loaded, &params.target)?;
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
        self.runtime
            .dispatcher
            .dispatch(&task, chain, cancel)
            .await
            .map_err(anyhow::Error::from)
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
        let targets = split_targets(&params.targets)?;
        let task = build_task_spec(
            params.task,
            params.workdir,
            params.sandbox,
            params.system,
            params.timeout_secs,
            params.labels,
        )?;

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
            self.runtime
                .dispatcher
                .broadcast(&task, resolved_chains, cancel)
                .await
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
    pub async fn history(&self, filter: HistoryFilter) -> Result<Vec<vyane_core::RunRecord>> {
        self.runtime
            .ledger
            .query(RunQuery {
                owner: Some("local".to_string()),
                provider: filter.provider.map(ProviderId::new),
                status: filter.status,
                since: None,
                limit: filter.limit,
            })
            .await
            .context("query ledger")
    }

    /// List saved sessions (read-only).
    pub async fn sessions(&self) -> Result<Vec<vyane_core::SessionRecord>> {
        self.runtime
            .sessions
            .list(Some("local"))
            .await
            .context("list sessions")
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
