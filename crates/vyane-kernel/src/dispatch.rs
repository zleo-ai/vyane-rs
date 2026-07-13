//! The single-task dispatch state machine.
//!
//! `dispatch` walks a resolved chain of targets, executing one attempt each,
//! and always ends by producing exactly one [`RunRecord`] — appended to the
//! ledger and reflected in the session store — whether the run succeeded,
//! failed, timed out, or was cancelled. Failover between targets is gated by
//! [`vyane_core::ErrorKind::failover_eligible`]; the kernel never re-implements
//! that rule, it only calls it.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use serde::Serialize;
use vyane_core::{
    AdapterTransport, Attempt, AttemptOutcome, BoundTarget, CancellationToken, ChatClient,
    ChatMessage, ChatRequest, EnvPolicy, ErrorKind, Harness, HarnessExecutionContext, HarnessJob,
    Ledger, NativeSessionState, PinnedWorkdir, Result, RunRecord, RunStatus, SessionExecutionLease,
    SessionStore, SessionUpdate, Target, TaskSpec, Usage, VyaneError,
};

use crate::digest::task_digest;
use crate::executor::{Executor, ExecutorFactory};
use crate::{
    AttemptScope, CapabilityAdmissionDecision, CapabilityAdmissionError,
    CapabilityAdmissionEvidence, CapabilityManifest, CapabilityPlanSnapshot,
    CapabilityRejectionReason, CapabilityTargetSnapshot, ExecutionScope, FilesystemCapability,
    IsolationStrength,
};

/// Owner scope used when the caller does not name one.
const DEFAULT_OWNER: &str = "local";

/// Separator that turns `TaskSpec.system` into appended harness instructions.
///
/// Per `vyane-core`'s `task.rs`, `system` is a *system prompt* for direct chat
/// but *appended instructions* for a harness (a CLI harness has no separate
/// system-message channel). The kernel therefore folds it onto the end of the
/// prompt for harness runs using exactly this shape; the constant is pinned by
/// a test so the wire format cannot drift silently.
const HARNESS_SYSTEM_HEADING: &str = "\n\n## Additional instructions\n\n";

/// The orchestration kernel: turns a task plus a resolved target chain into a
/// recorded run.
///
/// Holds its collaborators purely as `vyane-core` trait objects, so the kernel
/// stays free of any concrete adapter, HTTP client, or ledger implementation.
/// The [`ExecutorFactory`] is the seam through which concrete (or, in tests,
/// mock) executors are injected.
#[derive(Clone)]
pub struct Dispatcher {
    factory: Arc<dyn ExecutorFactory>,
    ledger: Arc<dyn Ledger>,
    sessions: Arc<dyn SessionStore>,
    owner: String,
    /// Process-local authority shared only by clones of this dispatcher.
    ///
    /// This deliberately has no serializable representation.  A prepared
    /// plan is executable only by the dispatcher instance (or one of its
    /// clones) that admitted it; constructing another dispatcher around the
    /// same factory does not inherit this authority.
    identity: Arc<DispatcherIdentity>,
}

/// Non-zero-sized so each `Arc` allocation has a distinct stable address.
struct DispatcherIdentity {
    _marker: u8,
}

/// Result of a completed dispatch: the persisted run record plus successful
/// answer text when the run produced one.
#[derive(Debug, Clone, Serialize)]
pub struct DispatchOutcome {
    pub record: RunRecord,
    pub output: Option<String>,
}

/// A live event during a streaming dispatch. The callback receives these as
/// the stream progresses; the method returns the final [`DispatchOutcome`]
/// once the run completes. The prepared probe API may return `None` when the
/// client declines streaming so its caller can reuse the same prepared value.
#[derive(Debug, Clone)]
pub enum StreamDispatchEvent {
    /// A fragment of the answer text.
    Delta(String),
    /// A fragment of reasoning/thinking output.
    ReasoningDelta(String),
    /// A coding harness invoked a tool. Tool events are observational only;
    /// the final answer still comes from the harness outcome.
    ToolUse { name: String, summary: String },
}

/// The successful product of a single attempt, before it becomes an `Attempt`.
struct AttemptOk {
    text: String,
    usage: Option<Usage>,
    /// Native session id reported by a harness, if any. It is persisted as
    /// legacy evidence today; in-place continuation remains fail-closed until
    /// an exact `NativeSessionDomain` is stored with it.
    native_session_id: Option<String>,
    /// For a direct-chat win, the (user, assistant) pair to append to the
    /// stored transcript so the next run replays it. `None` for harness wins —
    /// the CLI owns its own history and Vyane must not fabricate a transcript
    /// for it.
    transcript_delta: Option<(ChatMessage, ChatMessage)>,
}

/// Session continuity context loaded once, before the attempt loop.
///
/// The logical (Vyane) session id is only the store key. Native resume and
/// transcript replay both need the *stored* [`SessionRecord`], so it is loaded
/// up front and its two continuity carriers are threaded into the attempt.
struct SessionContext {
    /// Exclusive live authority acquired before the continuity read and held
    /// through the final update. `None` only when the task names no session.
    execution_lease: Option<Box<dyn SessionExecutionLease>>,
    /// Revision loaded under the lease and required by the final CAS update.
    session_revision: u64,
    /// Revision-aware native state. Both legacy-unbound and exact bound state
    /// remain fail-closed for harness chains until the native consumer
    /// revalidates an active execution permit and exact domain before each
    /// side effect. Never infer a domain from the current target.
    native_session: NativeSessionState,
    /// Prior transcript to replay for direct-chat continuity, in stored order,
    /// inserted after any `TaskSpec.system` message and before the current user
    /// message. Empty for a new or pure-harness session.
    transcript: Vec<ChatMessage>,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            execution_lease: None,
            session_revision: 0,
            native_session: NativeSessionState::Absent,
            transcript: Vec::new(),
        }
    }
}

/// One target that survived whole-chain capability admission.
struct AdmittedTarget {
    bound: BoundTarget,
    scope: AttemptScope,
}

/// Side-effect-free admission result for one future dispatch.
///
/// Fields are private deliberately: serializable evidence can be inspected or
/// persisted, but callers cannot assemble an executable plan from evidence.
/// The process-local pinned directory handle is retained here from admission
/// through child spawn.
pub struct PreparedDispatch {
    execution_scope: ExecutionScope,
    admitted: Vec<AdmittedTarget>,
    snapshot: CapabilityPlanSnapshot,
    pinned_workdir: Option<PinnedWorkdir>,
    dispatcher_identity: Arc<DispatcherIdentity>,
    prepared_owner: String,
    lifecycle: AtomicU8,
}

const PREPARED_FRESH: u8 = 0;
const PREPARED_PROBING: u8 = 1;
const PREPARED_FALLBACK_READY: u8 = 2;
const PREPARED_CONSUMED: u8 = 3;

impl PreparedDispatch {
    pub fn execution_id(&self) -> &str {
        &self.execution_scope.execution_id
    }

    pub fn capability_snapshot(&self) -> &CapabilityPlanSnapshot {
        &self.snapshot
    }

    /// Borrow the process-local pinned directory for an immediate child
    /// handoff. Audit snapshots intentionally expose no equivalent handle.
    pub fn pinned_workdir(&self) -> Option<&PinnedWorkdir> {
        self.pinned_workdir.as_ref()
    }

    /// Verify a frozen parent-side admission snapshot before execution.
    pub fn verify_capability_snapshot(&self, expected: &CapabilityPlanSnapshot) -> Result<()> {
        if &self.snapshot == expected {
            Ok(())
        } else {
            Err(VyaneError::config(
                "capability plan changed after admission; refusing detached execution",
            ))
        }
    }

    fn begin_stream_probe(&self) -> Result<()> {
        self.lifecycle
            .compare_exchange(
                PREPARED_FRESH,
                PREPARED_PROBING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| VyaneError::config("prepared dispatch was already used"))
    }

    fn finish_stream_probe(&self, fallback_ready: bool) {
        self.lifecycle.store(
            if fallback_ready {
                PREPARED_FALLBACK_READY
            } else {
                PREPARED_CONSUMED
            },
            Ordering::Release,
        );
    }

    fn consume_for_dispatch(&self) -> Result<()> {
        loop {
            let state = self.lifecycle.load(Ordering::Acquire);
            if !matches!(state, PREPARED_FRESH | PREPARED_FALLBACK_READY) {
                return Err(VyaneError::config("prepared dispatch was already used"));
            }
            if self
                .lifecycle
                .compare_exchange(
                    state,
                    PREPARED_CONSUMED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}

fn prepare_dispatch_inner(
    factory: &dyn ExecutorFactory,
    owner: &str,
    task: &TaskSpec,
    chain: Vec<BoundTarget>,
    supplied_pinned_workdir: Option<PinnedWorkdir>,
    dispatcher_identity: Arc<DispatcherIdentity>,
) -> Result<PreparedDispatch> {
    let initial_scope = ExecutionScope::allocate(owner, task.sandbox);
    if chain.is_empty() {
        return Err(VyaneError::config(
            "dispatch received an empty target chain; resolution must supply at least one target",
        ));
    }

    // Inspect every manifest up front. A factory implementation must keep
    // this method side-effect-free; actual construction belongs to
    // make_scoped and happens only after this complete pass.
    let manifests: Vec<CapabilityManifest> = chain
        .iter()
        .map(|bound| factory.capability_manifest(bound))
        .collect();

    let (canonical_workdir, pinned_workdir, workdir_rejection) = match task.sandbox {
        vyane_core::Sandbox::ReadOnly if supplied_pinned_workdir.is_some() => {
            return Err(VyaneError::config(
                "read-only dispatch cannot consume an inherited mutating workdir",
            ));
        }
        vyane_core::Sandbox::ReadOnly => (None, None, None),
        vyane_core::Sandbox::Write | vyane_core::Sandbox::Full => match supplied_pinned_workdir {
            Some(pinned) => (
                Some(pinned.canonical_path().to_path_buf()),
                Some(pinned),
                None,
            ),
            None => canonical_mutating_workdir(task.workdir.as_deref()),
        },
    };
    let workdir_identity = pinned_workdir
        .as_ref()
        .map(|pinned: &PinnedWorkdir| pinned.identity().clone());
    let execution_scope =
        initial_scope.with_workdir(canonical_workdir.clone(), workdir_identity.clone());

    let evidences: Vec<CapabilityAdmissionEvidence> = chain
        .iter()
        .zip(manifests)
        .enumerate()
        .map(|(ordinal, (bound, manifest))| {
            let decision = admission_decision(task.sandbox, &manifest, workdir_rejection);
            CapabilityAdmissionEvidence {
                execution_id: execution_scope.execution_id.clone(),
                original_chain_ordinal: ordinal,
                target: bound.target.clone(),
                requested_sandbox: task.sandbox,
                canonical_workdir: canonical_workdir.clone(),
                workdir_identity: workdir_identity.clone(),
                manifest,
                decision,
            }
        })
        .collect();

    if matches!(
        evidences.first().map(|e| &e.decision),
        Some(CapabilityAdmissionDecision::Rejected(_))
    ) {
        return Err(CapabilityAdmissionError {
            // The caller rejects an empty chain before admission.
            evidence: evidences[0].clone(),
        }
        .into_vyane_error());
    }

    let snapshot = CapabilityPlanSnapshot {
        requested_sandbox: task.sandbox,
        requires_inherited_workdir: pinned_workdir.is_some(),
        canonical_workdir: canonical_workdir.clone(),
        workdir_identity,
        targets: evidences
            .iter()
            .map(|evidence| CapabilityTargetSnapshot {
                original_chain_ordinal: evidence.original_chain_ordinal,
                target: evidence.target.clone(),
                manifest: evidence.manifest.clone(),
                decision: evidence.decision.clone(),
            })
            .collect(),
    };

    let admitted = chain
        .into_iter()
        .zip(evidences)
        .filter_map(|(bound, admission)| match admission.decision {
            CapabilityAdmissionDecision::Admitted => Some(AdmittedTarget {
                bound,
                scope: AttemptScope {
                    execution: execution_scope.clone(),
                    admission,
                },
            }),
            CapabilityAdmissionDecision::Rejected(reason) => {
                tracing::debug!(
                    execution_id = %execution_scope.execution_id,
                    original_chain_ordinal = admission.original_chain_ordinal,
                    target = %admission.target,
                    reason = %reason,
                    "filtered capability-ineligible fallback before execution"
                );
                None
            }
        })
        .collect();

    Ok(PreparedDispatch {
        execution_scope,
        admitted,
        snapshot,
        pinned_workdir,
        dispatcher_identity,
        prepared_owner: owner.to_string(),
        lifecycle: AtomicU8::new(PREPARED_FRESH),
    })
}

fn canonical_mutating_workdir(
    requested: Option<&Path>,
) -> (
    Option<PathBuf>,
    Option<PinnedWorkdir>,
    Option<CapabilityRejectionReason>,
) {
    let Some(requested) = requested else {
        return (None, None, Some(CapabilityRejectionReason::MissingWorkdir));
    };
    match PinnedWorkdir::open(requested) {
        Ok(pinned) => (
            Some(pinned.canonical_path().to_path_buf()),
            Some(pinned),
            None,
        ),
        Err(_) => (
            None,
            None,
            Some(CapabilityRejectionReason::WorkdirPinningUnavailable),
        ),
    }
}

fn admission_decision(
    sandbox: vyane_core::Sandbox,
    manifest: &CapabilityManifest,
    workdir_rejection: Option<CapabilityRejectionReason>,
) -> CapabilityAdmissionDecision {
    if sandbox == vyane_core::Sandbox::ReadOnly {
        return CapabilityAdmissionDecision::Admitted;
    }
    if let Some(reason) = workdir_rejection {
        return CapabilityAdmissionDecision::Rejected(reason);
    }
    if manifest.filesystem != FilesystemCapability::CallerWorkdirEditing {
        return CapabilityAdmissionDecision::Rejected(
            CapabilityRejectionReason::LocalEditingUnavailable,
        );
    }
    if manifest.isolation == IsolationStrength::None {
        return CapabilityAdmissionDecision::Rejected(
            CapabilityRejectionReason::IsolationUnavailable,
        );
    }
    CapabilityAdmissionDecision::Admitted
}

impl Dispatcher {
    /// Construct a dispatcher with the default `"local"` owner scope.
    pub fn new(
        factory: Arc<dyn ExecutorFactory>,
        ledger: Arc<dyn Ledger>,
        sessions: Arc<dyn SessionStore>,
    ) -> Self {
        Self {
            factory,
            ledger,
            sessions,
            owner: DEFAULT_OWNER.to_string(),
            identity: Arc::new(DispatcherIdentity { _marker: 0 }),
        }
    }

    /// Override the owner scope written onto records (default `"local"`).
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// Resolve capability admission and pin a mutating workdir without
    /// constructing an executor, issuing HTTP, or spawning a subprocess.
    pub fn prepare(&self, task: &TaskSpec, chain: Vec<BoundTarget>) -> Result<PreparedDispatch> {
        prepare_dispatch_inner(
            self.factory.as_ref(),
            &self.owner,
            task,
            chain,
            None,
            Arc::clone(&self.identity),
        )
    }

    /// Worker-side preparation using the exact directory descriptor inherited
    /// from its detached parent.
    pub fn prepare_with_pinned_workdir(
        &self,
        task: &TaskSpec,
        chain: Vec<BoundTarget>,
        pinned_workdir: PinnedWorkdir,
    ) -> Result<PreparedDispatch> {
        prepare_dispatch_inner(
            self.factory.as_ref(),
            &self.owner,
            task,
            chain,
            Some(pinned_workdir),
            Arc::clone(&self.identity),
        )
    }

    /// Reject a plan admitted by another dispatcher or owner before touching
    /// session storage, constructing an executor, or invoking a model.
    fn validate_prepared_provenance(&self, prepared: &PreparedDispatch) -> Result<()> {
        if Arc::ptr_eq(&self.identity, &prepared.dispatcher_identity)
            && self.owner == prepared.prepared_owner
        {
            Ok(())
        } else {
            Err(VyaneError::config(
                "prepared dispatch provenance does not match this dispatcher",
            ))
        }
    }

    /// Validate session continuity constraints without constructing an
    /// executor. Detached parents call this before writing task metadata or
    /// spawning a worker; normal dispatch repeats it in the execution process.
    pub async fn validate_session_admission(
        &self,
        task: &TaskSpec,
        prepared: &PreparedDispatch,
    ) -> Result<()> {
        self.validate_prepared_provenance(prepared)?;
        let session_ctx = self.load_session_context_for_admission(task).await?;
        validate_native_resume(prepared, &session_ctx)
    }

    /// Run `task` against `chain`, honouring `cancel`, and produce one recorded
    /// run.
    ///
    /// Walks the chain in order: each target is built into an [`Executor`] via
    /// the factory and given exactly one attempt. On an attempt error the
    /// kernel advances to the next target **only when**
    /// [`ErrorKind::failover_eligible`] holds for that error **and** a target
    /// remains; otherwise it stops. Every attempt — including the last, failed
    /// one — is recorded. Regardless of outcome, exactly one [`RunRecord`] is
    /// appended to the ledger and (when the task names a session) the session
    /// store is updated, before the record is returned.
    ///
    /// Cancellation is checked before building each executor and between
    /// failover attempts, so a token that is already cancelled yields a
    /// `Cancelled` run deterministically without ever calling the factory.
    ///
    /// The `Result::Err` arm is reserved for pre-execution failures that make a
    /// truthful run impossible: an empty chain or failure to load a requested
    /// session without silently discarding continuity. A model attempt that
    /// simply *failed* is still `Ok` — it comes back as a record whose `status`
    /// is `Error`/`Timeout`/`Cancelled`. Persistence failures *after* a
    /// completed run remain best-effort and never demote a finished run.
    pub async fn dispatch(
        &self,
        task: &TaskSpec,
        chain: Vec<BoundTarget>,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        let prepared = self.prepare(task, chain)?;
        self.dispatch_prepared(task, prepared, cancel).await
    }

    /// Execute a previously admitted plan.
    ///
    /// This is the fallback seam for streaming and the worker-side seam for a
    /// detached snapshot recheck.  Reusing the same value preserves one
    /// execution id and the same pinned directory object.
    pub async fn dispatch_prepared(
        &self,
        task: &TaskSpec,
        prepared: PreparedDispatch,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        self.validate_prepared_provenance(&prepared)?;
        if task.sandbox != prepared.execution_scope.requested_sandbox {
            return Err(VyaneError::config(
                "prepared dispatch sandbox does not match the execution task",
            ));
        }
        let session_ctx = self
            .load_session_context(task, prepared.execution_id())
            .await?;
        validate_native_resume(&prepared, &session_ctx)?;
        prepared.consume_for_dispatch()?;
        let PreparedDispatch {
            execution_scope,
            admitted: chain,
            pinned_workdir,
            ..
        } = prepared;
        let mut execution_task = task.clone();
        if let Some(workdir) = execution_scope.canonical_workdir.as_ref() {
            execution_task.workdir = Some(workdir.clone());
        }
        let task = &execution_task;

        // Session continuity was loaded once, up front. A stored native id is
        // currently a fail-closed signal unless/until it has an exact domain;
        // a direct-chat transcript remains replayable. An identity, migration,
        // or I/O error fails before model execution rather than silently
        // discarding continuity.
        let started_at = execution_scope.started_at;
        let total = chain.len();

        let mut attempts: Vec<Attempt> = Vec::with_capacity(total);
        let mut usage_total: Option<Usage> = None;
        // The outcome carried out of the loop: Ok holds the winning attempt's
        // product; Err holds the terminal error that stopped the chain.
        let mut final_outcome: std::result::Result<AttemptOk, VyaneError> =
            Err(VyaneError::new(ErrorKind::Other, "no attempt executed"));
        // Identity/transport of the last attempt drives the record's headline.
        let mut last_target: Option<Target> = None;
        let mut last_transport: AdapterTransport = AdapterTransport::DirectHttp;

        for (index, admitted) in chain.iter().enumerate() {
            let bound = &admitted.bound;
            let has_next = index + 1 < total;
            last_target = Some(bound.target.clone());
            last_transport = bound.transport;

            // Cancellation is checked *before* the factory builds anything, so a
            // pre-cancelled (or between-attempts cancelled) dispatch produces a
            // Cancelled run with no factory side effects. Cancelled is not
            // failover-eligible, so this always aborts the chain.
            if cancel.is_cancelled() {
                attempts.push(Attempt {
                    target: bound.target.clone(),
                    transport: bound.transport,
                    started_at: Utc::now(),
                    duration_ms: 0,
                    outcome: AttemptOutcome::Err {
                        kind: ErrorKind::Cancelled,
                        message: "cancelled by caller".to_string(),
                        failed_over: false,
                    },
                });
                final_outcome = Err(VyaneError::cancelled());
                break;
            }

            let attempt_start_wall = Utc::now();
            let attempt_start = Instant::now();

            // Attempt = build the executor, then drive one call. A factory
            // failure is a real attempt failure (e.g. a missing harness
            // binary) and is gated for failover like any execution error.
            let outcome = match self.factory.make_scoped(bound, &admitted.scope) {
                Ok(executor) => {
                    self.run_attempt(
                        executor,
                        task,
                        bound,
                        &session_ctx,
                        pinned_workdir.as_ref(),
                        &cancel,
                    )
                    .await
                }
                Err(e) => Err(e),
            };

            let duration_ms = attempt_start.elapsed().as_millis() as u64;

            match outcome {
                Ok(ok) => {
                    if let Some(u) = ok.usage.as_ref() {
                        usage_total.get_or_insert_with(Usage::default).add(u);
                    }
                    attempts.push(Attempt {
                        target: bound.target.clone(),
                        transport: bound.transport,
                        started_at: attempt_start_wall,
                        duration_ms,
                        outcome: AttemptOutcome::Ok,
                    });
                    final_outcome = Ok(ok);
                    break;
                }
                Err(err) => {
                    // Failover gate: advance only when this error kind is
                    // eligible AND a target remains. Never re-derive the
                    // eligibility rule — defer to core.
                    let failed_over = err.kind.failover_eligible() && has_next;
                    attempts.push(Attempt {
                        target: bound.target.clone(),
                        transport: bound.transport,
                        started_at: attempt_start_wall,
                        duration_ms,
                        outcome: AttemptOutcome::Err {
                            kind: err.kind,
                            message: err.message.clone(),
                            failed_over,
                        },
                    });
                    final_outcome = Err(err);
                    if !failed_over {
                        break;
                    }
                    // else: loop to the next target.
                }
            }
        }

        // `last_target` is always Some here: the chain is non-empty and every
        // path through the loop body sets it before it can break or finish.
        let target = last_target.unwrap_or_else(|| {
            debug_assert!(false, "non-empty chain must set last_target");
            fallback_target()
        });

        let finished_at = Utc::now();

        let (status, session_id_from_run, transcript_delta, output, output_chars, error_msg) =
            match &final_outcome {
                Ok(ok) => (
                    RunStatus::Success,
                    ok.native_session_id.clone(),
                    ok.transcript_delta.clone(),
                    Some(ok.text.clone()),
                    Some(ok.text.chars().count() as u64),
                    None,
                ),
                Err(err) => (
                    status_for_error(err.kind),
                    None,
                    None,
                    None,
                    None,
                    Some(err.to_string()),
                ),
            };

        // The logical session this run belongs to, if the caller named one.
        let session_id = task.session.as_ref().map(|s| s.as_str().to_string());

        let workdir = task
            .workdir
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned());

        let record = RunRecord {
            run_id: execution_scope.execution_id.clone(),
            owner: self.owner.clone(),
            started_at,
            finished_at,
            task_digest: task_digest(&task.prompt),
            // Prompt text is never copied into the default run ledger. The
            // optional field remains in the schema only for old records and
            // explicit future opt-in policies.
            task_preview: None,
            workdir,
            sandbox: task.sandbox,
            target,
            transport: last_transport,
            attempts,
            status,
            usage: usage_total,
            cost_usd: None,
            session_id: session_id.clone(),
            output_chars,
            error: error_msg,
            labels: task.labels.clone(),
        };

        // Persistence after a completed run is **best-effort** (architect
        // decision): the model call already happened, so a ledger or session
        // store error must not convert a successful run into a caller-visible
        // failure. We emit a `tracing::warn` and return the record normally.
        // Pre-execution configuration/session failures use `Result::Err`; once
        // an external model call completed, the truthful run is returned.
        if let Err(e) = self.ledger.append(&record).await {
            tracing::warn!(
                run_id = %record.run_id,
                error = %e,
                "ledger append failed after run completed; returning run anyway"
            );
        }

        // Session continuity is likewise best-effort. Only runs that name a
        // session touch the store.
        if let Some(sid) = session_id.as_deref() {
            if let Err(e) = self
                .update_session(
                    sid,
                    &session_ctx,
                    &record,
                    session_id_from_run.as_deref(),
                    transcript_delta,
                )
                .await
            {
                tracing::warn!(
                    session_id = sid,
                    error = %e,
                    "session store update failed after run was recorded"
                );
            }
        }

        Ok(DispatchOutcome { record, output })
    }

    /// Stream a single HTTP or harness attempt, calling `on_event` for each
    /// delta as it arrives and returning the assembled, ledger-appended
    /// [`DispatchOutcome`] when done.
    ///
    /// Returns `Ok(None)` when the selected adapter declines streaming. This
    /// compatibility entrypoint deliberately uses the legacy unscoped factory
    /// seam for the probe, so no factory observes an execution id that the
    /// caller cannot reuse. New in-process callers should use
    /// [`Self::dispatch_stream_prepared`] and retain its prepared value.
    ///
    /// Scoped to a single target with no session — the same constraint the
    /// CLI's previous hand-rolled streaming path used. Both direct HTTP and
    /// harness targets are supported when their adapter implements streaming.
    pub async fn dispatch_stream<F>(
        &self,
        task: &TaskSpec,
        bound: &BoundTarget,
        cancel: CancellationToken,
        on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        let prepared = self.prepare(task, vec![bound.clone()])?;
        self.dispatch_stream_prepared_inner(task, &prepared, cancel, on_event, false)
            .await
    }

    /// Probe streaming against an already prepared single-target dispatch.
    ///
    /// If the adapter returns `Unsupported`, callers may pass the same
    /// `PreparedDispatch` to [`Self::dispatch_prepared`].  Both factory calls
    /// then observe the same execution id and pinned workdir.
    pub async fn dispatch_stream_prepared<F>(
        &self,
        task: &TaskSpec,
        prepared: &PreparedDispatch,
        cancel: CancellationToken,
        on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        self.dispatch_stream_prepared_inner(task, prepared, cancel, on_event, true)
            .await
    }

    async fn dispatch_stream_prepared_inner<F>(
        &self,
        task: &TaskSpec,
        prepared: &PreparedDispatch,
        cancel: CancellationToken,
        on_event: F,
        scoped_factory: bool,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        self.validate_prepared_provenance(prepared)?;
        if task.sandbox != prepared.execution_scope.requested_sandbox {
            return Err(VyaneError::config(
                "prepared stream sandbox does not match the execution task",
            ));
        }
        if task.session.is_some() {
            return Err(VyaneError::new(
                ErrorKind::Unsupported,
                "stream dispatch does not support sessions; use non-streaming dispatch for continuity",
            ));
        }
        let [admitted] = prepared.admitted.as_slice() else {
            return Err(VyaneError::config(
                "stream dispatch requires exactly one admitted target",
            ));
        };
        prepared.begin_stream_probe()?;
        let execution_scope = &prepared.execution_scope;
        let bound = &admitted.bound;
        let mut execution_task = task.clone();
        if let Some(workdir) = execution_scope.canonical_workdir.as_ref() {
            execution_task.workdir = Some(workdir.clone());
        }
        let task = &execution_task;

        // Match `dispatch`'s pre-attempt cancellation contract: an already
        // cancelled token produces a record without constructing an executor.
        // Besides avoiding needless work, this keeps factory side effects out
        // of a run that never started.
        if cancel.is_cancelled() {
            let attempt_started_at = Utc::now();
            let attempt_start = Instant::now();
            let error = VyaneError::cancelled();
            let record = self
                .assemble_stream_record(
                    execution_scope,
                    task,
                    bound,
                    attempt_started_at,
                    attempt_start,
                    Err(&error),
                )
                .await;
            prepared.finish_stream_probe(false);
            return Ok(Some(DispatchOutcome {
                record,
                output: None,
            }));
        }

        // Build the executor via the same factory seam `dispatch` uses.
        let built = if scoped_factory {
            self.factory.make_scoped(bound, &admitted.scope)
        } else {
            self.factory.make(bound)
        };
        let executor = match built {
            Ok(executor) => executor,
            Err(error) => {
                prepared.finish_stream_probe(false);
                return Err(error);
            }
        };
        let result = match executor {
            Executor::Chat(client) => {
                // --- HTTP streaming path ---
                self.dispatch_stream_http(execution_scope, task, bound, cancel, client, on_event)
                    .await
            }
            Executor::Agent(harness) => {
                // --- Harness streaming path (WP-36 step 5) ---
                self.dispatch_stream_harness(prepared, task, admitted, cancel, harness, on_event)
                    .await
            }
        };
        prepared.finish_stream_probe(matches!(&result, Ok(None)));
        result
    }

    /// HTTP streaming path: calls ChatClient::stream, races against timeout/cancel.
    async fn dispatch_stream_http<F>(
        &self,
        execution_scope: &ExecutionScope,
        task: &TaskSpec,
        bound: &BoundTarget,
        cancel: CancellationToken,
        client: Arc<dyn ChatClient>,
        mut on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        use futures::StreamExt as _;
        use vyane_core::StreamEvent;

        // Pre-cancel check.
        if cancel.is_cancelled() {
            let attempt_started_at = Utc::now();
            let attempt_start = Instant::now();
            let record = self
                .assemble_stream_record(
                    execution_scope,
                    task,
                    bound,
                    attempt_started_at,
                    attempt_start,
                    Err(&VyaneError::cancelled()),
                )
                .await;
            return Ok(Some(DispatchOutcome {
                record,
                output: None,
            }));
        }

        // Assemble the request: system (if any) + user. No transcript replay
        // (streaming does not support sessions — same as the old CLI path).
        let mut messages = Vec::new();
        if let Some(system) = task.system.as_ref() {
            messages.push(ChatMessage::system(system.clone()));
        }
        messages.push(ChatMessage::user(task.prompt.clone()));
        let req = ChatRequest {
            model: bound.target.model.clone(),
            messages,
            params: bound.params.clone(),
        };

        let attempt_started_at = Utc::now();
        let attempt_start = Instant::now();

        // Call client.stream — may itself return Unsupported before any HTTP call.
        let mut stream = match client.stream(req).await {
            Ok(s) => s,
            Err(e) if e.kind == ErrorKind::Unsupported => return Ok(None),
            Err(e) => {
                let record = self
                    .assemble_stream_record(
                        execution_scope,
                        task,
                        bound,
                        attempt_started_at,
                        attempt_start,
                        Err(&e),
                    )
                    .await;
                return Ok(Some(DispatchOutcome {
                    record,
                    output: None,
                }));
            }
        };

        // Consume the event stream, accumulating text and usage, calling
        // `on_event` for each delta. The loop is raced against timeout +
        // cancellation via the same biased select pattern `drive` uses.
        let mut text = String::new();
        let mut usage: Option<Usage> = None;
        let mut stream_error: Option<VyaneError> = None;

        let timeout = task.timeout;
        let cancel_for_select = cancel.clone();

        let event_loop = async {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(StreamEvent::Delta(delta)) => {
                        text.push_str(&delta);
                        on_event(StreamDispatchEvent::Delta(delta));
                    }
                    Ok(StreamEvent::ReasoningDelta(delta)) => {
                        on_event(StreamDispatchEvent::ReasoningDelta(delta));
                    }
                    Ok(StreamEvent::Usage(u)) => {
                        usage.get_or_insert_with(Usage::default).add(&u);
                    }
                    Ok(StreamEvent::Done { .. }) => break,
                    Err(e) => {
                        stream_error = Some(e);
                        break;
                    }
                }
            }
        };

        tokio::pin!(event_loop);

        match timeout {
            Some(d) => {
                tokio::select! {
                    biased;
                    _ = cancel_for_select.cancelled() => {
                        let e = VyaneError::cancelled();
                        let record = self.assemble_stream_record(
                            execution_scope, task, bound, attempt_started_at, attempt_start, Err(&e),
                        ).await;
                        return Ok(Some(DispatchOutcome { record, output: None }));
                    }
                    _ = tokio::time::sleep(d) => {
                        let e = VyaneError::new(
                            ErrorKind::Timeout,
                            format!("attempt exceeded timeout of {}ms", d.as_millis()),
                        );
                        let record = self.assemble_stream_record(
                            execution_scope, task, bound, attempt_started_at, attempt_start, Err(&e),
                        ).await;
                        return Ok(Some(DispatchOutcome { record, output: None }));
                    }
                    _ = &mut event_loop => {}
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = cancel_for_select.cancelled() => {
                        let e = VyaneError::cancelled();
                        let record = self.assemble_stream_record(
                            execution_scope, task, bound, attempt_started_at, attempt_start, Err(&e),
                        ).await;
                        return Ok(Some(DispatchOutcome { record, output: None }));
                    }
                    _ = &mut event_loop => {}
                }
            }
        }

        // Event loop completed (or broke on error / Done).
        let result: std::result::Result<(String, Option<Usage>), VyaneError> = match stream_error {
            Some(e) => Err(e),
            None => Ok((text.clone(), usage)),
        };
        let result_ref: std::result::Result<&(String, Option<Usage>), &VyaneError> = match &result {
            Ok(v) => Ok(v),
            Err(e) => Err(e),
        };
        let record = self
            .assemble_stream_record(
                execution_scope,
                task,
                bound,
                attempt_started_at,
                attempt_start,
                result_ref,
            )
            .await;
        let output = if record.status == RunStatus::Success {
            Some(text)
        } else {
            None
        };
        Ok(Some(DispatchOutcome { record, output }))
    }

    /// Harness streaming path: calls Harness::run_stream, converting
    /// HarnessStreamEvent to StreamDispatchEvent for the caller's callback.
    async fn dispatch_stream_harness<F>(
        &self,
        prepared: &PreparedDispatch,
        task: &TaskSpec,
        admitted: &AdmittedTarget,
        cancel: CancellationToken,
        harness: Arc<dyn Harness>,
        mut on_event: F,
    ) -> Result<Option<DispatchOutcome>>
    where
        F: FnMut(StreamDispatchEvent) + Send,
    {
        let execution_scope = &prepared.execution_scope;
        let bound = &admitted.bound;
        // Compose the harness prompt (system folded in, same as dispatch).
        let prompt = compose_harness_prompt(&task.prompt, task.system.as_deref());
        let job = HarnessJob {
            prompt,
            model: bound.target.model.clone(),
            protocol: bound.target.protocol,
            endpoint: bound.endpoint.clone(),
            params: bound.params.clone(),
            workdir: task.workdir.clone(),
            sandbox: task.sandbox,
            resume: None, // streaming path does not support session resume
            env: EnvPolicy::scrubbed(),
            timeout: task.timeout,
            harness_lifecycle_reporter: task.harness_lifecycle_reporter.clone(),
        };

        let attempt_started_at = Utc::now();
        let attempt_start = Instant::now();

        // `Harness::run_stream` owns a `'static` callback because concrete
        // harnesses may invoke it from a spawned stdout-drain task. The public
        // dispatcher callback deliberately does not require `'static`: callers
        // should be able to borrow local output buffers. Bridge the two with an
        // unbounded channel, and invoke the user's callback only on this task.
        // Unlike the former `try_lock` bridge, every queued delta is delivered.
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let context = prepared
            .pinned_workdir
            .clone()
            .map(HarnessExecutionContext::with_pinned_workdir)
            .unwrap_or_default();
        let harness_run = harness.run_stream_scoped(
            job,
            context,
            cancel,
            Box::new(move |event| match event {
                vyane_core::HarnessStreamEvent::Delta(text) => {
                    let _ = event_tx.send(StreamDispatchEvent::Delta(text));
                }
                vyane_core::HarnessStreamEvent::ToolUse { name, summary } => {
                    let _ = event_tx.send(StreamDispatchEvent::ToolUse { name, summary });
                }
            }),
        );
        tokio::pin!(harness_run);

        let mut channel_open = true;
        let outcome = loop {
            tokio::select! {
                biased;
                event = event_rx.recv(), if channel_open => {
                    match event {
                        Some(event) => on_event(event),
                        None => channel_open = false,
                    }
                }
                result = &mut harness_run => break result,
            }
        };

        // A harness can enqueue its final delta and return in the same poll.
        // Drain anything already queued before assembling and returning the
        // final outcome so callback delivery remains lossless and ordered.
        while let Ok(event) = event_rx.try_recv() {
            on_event(event);
        }

        match outcome {
            Ok(outcome) => {
                let text = outcome.text.clone();
                let usage = outcome.usage;
                let result: (String, Option<Usage>) = (text.clone(), usage);
                let result_ref: std::result::Result<&(String, Option<Usage>), &VyaneError> =
                    Ok(&result);
                let record = self
                    .assemble_stream_record(
                        execution_scope,
                        task,
                        bound,
                        attempt_started_at,
                        attempt_start,
                        result_ref,
                    )
                    .await;
                let output = if record.status == RunStatus::Success {
                    Some(text)
                } else {
                    None
                };
                Ok(Some(DispatchOutcome { record, output }))
            }
            Err(e) if e.kind == ErrorKind::Unsupported => {
                // Harness doesn't support streaming → fall back to dispatch.
                Ok(None)
            }
            Err(e) => {
                let result_ref: std::result::Result<&(String, Option<Usage>), &VyaneError> =
                    Err(&e);
                let record = self
                    .assemble_stream_record(
                        execution_scope,
                        task,
                        bound,
                        attempt_started_at,
                        attempt_start,
                        result_ref,
                    )
                    .await;
                Ok(Some(DispatchOutcome {
                    record,
                    output: None,
                }))
            }
        }
    }

    /// Assemble a single-attempt [`RunRecord`] for a streaming run and append
    /// it to the ledger (best-effort). This is the kernel-owned replacement
    /// for the CLI's former hand-rolled `build_stream_record` — same fields,
    /// same status mapping, same best-effort append rule, but no duplication.
    async fn assemble_stream_record(
        &self,
        execution_scope: &ExecutionScope,
        task: &TaskSpec,
        bound: &BoundTarget,
        attempt_started_at: chrono::DateTime<Utc>,
        attempt_start: Instant,
        result: std::result::Result<&(String, Option<Usage>), &VyaneError>,
    ) -> RunRecord {
        let now = Utc::now();
        let duration_ms = attempt_start.elapsed().as_millis() as u64;

        let (status, usage, output_chars, error_msg, outcome) = match result {
            Ok((text, usage)) => (
                RunStatus::Success,
                *usage,
                Some(text.chars().count() as u64),
                None,
                AttemptOutcome::Ok,
            ),
            Err(e) => {
                let kind = e.kind;
                (
                    status_for_error(kind),
                    None,
                    None,
                    Some(e.to_string()),
                    AttemptOutcome::Err {
                        kind,
                        message: e.to_string(),
                        failed_over: false,
                    },
                )
            }
        };

        let record = RunRecord {
            run_id: execution_scope.execution_id.clone(),
            owner: self.owner.clone(),
            started_at: execution_scope.started_at,
            finished_at: now,
            task_digest: task_digest(&task.prompt),
            task_preview: None,
            workdir: task
                .workdir
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            sandbox: task.sandbox,
            target: bound.target.clone(),
            transport: bound.transport,
            attempts: vec![Attempt {
                target: bound.target.clone(),
                transport: bound.transport,
                started_at: attempt_started_at,
                duration_ms,
                outcome,
            }],
            status,
            usage,
            cost_usd: None,
            session_id: task.session.as_ref().map(|s| s.as_str().to_string()),
            output_chars,
            error: error_msg,
            labels: task.labels.clone(),
        };

        if let Err(e) = self.ledger.append(&record).await {
            tracing::warn!(
                run_id = %record.run_id,
                error = %e,
                "ledger append failed after streaming run completed; returning run anyway"
            );
        }

        record
    }

    /// Load the session continuity context for `task`, if it names a session.
    ///
    /// Acquires exclusive `(owner, session_id)` execution authority before
    /// reading revision-aware native state and the replayable transcript. The
    /// live lease remains in the returned context until dispatch finishes its
    /// final session update (or exits early), preventing two model calls from
    /// branching off the same prior context.
    /// Neither a legacy id nor a domain binding alone authorizes execution:
    /// harness continuation remains disabled until the active-permit consumer
    /// lands. A missing session yields an empty context; storage or integrity
    /// errors fail closed before executing a model call.
    async fn load_session_context(
        &self,
        task: &TaskSpec,
        execution_id: &str,
    ) -> Result<SessionContext> {
        let Some(session_ref) = task.session.as_ref() else {
            return Ok(SessionContext::default());
        };
        let sid = session_ref.as_str();
        let lease = self
            .sessions
            .acquire_execution_lease(&self.owner, sid, execution_id)
            .await?;
        validate_session_lease_identity(lease.as_ref(), &self.owner, sid, execution_id)?;
        lease.revalidate().await?;
        match lease.load_snapshot().await {
            Ok(Some(snapshot)) => {
                validate_session_snapshot_identity(&snapshot, &self.owner, sid)?;
                Ok(SessionContext {
                    execution_lease: Some(lease),
                    session_revision: snapshot.session_revision,
                    native_session: snapshot.native_session,
                    transcript: snapshot.record.transcript,
                })
            }
            Ok(None) => Ok(SessionContext {
                execution_lease: Some(lease),
                ..SessionContext::default()
            }),
            Err(error) => Err(error),
        }
    }

    /// Read-only detached-parent admission. It never acquires or serializes
    /// execution authority; the process that will actually call the model must
    /// acquire the lease again and reload the snapshot immediately before
    /// execution.
    async fn load_session_context_for_admission(&self, task: &TaskSpec) -> Result<SessionContext> {
        let Some(session_ref) = task.session.as_ref() else {
            return Ok(SessionContext::default());
        };
        match self
            .sessions
            .load_snapshot(&self.owner, session_ref.as_str())
            .await?
        {
            Some(snapshot) => {
                validate_session_snapshot_identity(&snapshot, &self.owner, session_ref.as_str())?;
                Ok(SessionContext {
                    native_session: snapshot.native_session,
                    transcript: snapshot.record.transcript,
                    ..SessionContext::default()
                })
            }
            None => Ok(SessionContext::default()),
        }
    }

    /// Execute exactly one attempt against a built executor, enforcing the
    /// task timeout and cancellation uniformly across both transports.
    async fn run_attempt(
        &self,
        executor: Executor,
        task: &TaskSpec,
        bound: &BoundTarget,
        session_ctx: &SessionContext,
        pinned_workdir: Option<&PinnedWorkdir>,
        cancel: &CancellationToken,
    ) -> std::result::Result<AttemptOk, VyaneError> {
        match executor {
            Executor::Chat(client) => {
                // Direct-chat continuity: assemble messages as system (if any)
                // → replayed transcript → current user message. The system
                // prompt comes from `TaskSpec.system`; the transcript is the
                // prior turns loaded from the stored session record.
                let mut messages: Vec<ChatMessage> = Vec::new();
                if let Some(system) = task.system.as_ref() {
                    messages.push(ChatMessage::system(system.clone()));
                }
                messages.extend(session_ctx.transcript.iter().cloned());
                let user_message = ChatMessage::user(task.prompt.clone());
                messages.push(user_message.clone());

                let req = ChatRequest {
                    model: bound.target.model.clone(),
                    messages,
                    params: bound.params.clone(),
                };
                let continued = task.session.is_some();
                let fut = async move {
                    let out = client.complete(req).await?;
                    // On success, remember the (user, assistant) pair so the
                    // caller can append it to the stored transcript — but only
                    // for runs that belong to a session.
                    let transcript_delta = if continued {
                        Some((user_message, ChatMessage::assistant(out.text.clone())))
                    } else {
                        None
                    };
                    Ok(AttemptOk {
                        text: out.text,
                        usage: out.usage,
                        native_session_id: None,
                        transcript_delta,
                    })
                };
                drive(fut, task.timeout, cancel).await
            }
            Executor::Agent(harness) => {
                // The kernel carries no credentials, so it hands the harness a
                // scrubbed env policy; concrete injection (auth, base URL,
                // model overrides) is the assembler/adapter's responsibility,
                // not the kernel's.
                //
                // `TaskSpec.system` is *appended instructions* for a harness
                // (not a separate system channel), so it is folded onto the end
                // of the prompt. The resume field is never the logical id.
                // Current admission allows it only when empty; a future
                // domain-bound path may pass the exact native id after
                // validating its runtime domain.
                let prompt = compose_harness_prompt(&task.prompt, task.system.as_deref());
                let job = HarnessJob {
                    prompt,
                    model: bound.target.model.clone(),
                    protocol: bound.target.protocol,
                    endpoint: bound.endpoint.clone(),
                    params: bound.params.clone(),
                    workdir: task.workdir.clone(),
                    sandbox: task.sandbox,
                    // Any stored native state is rejected before executor
                    // construction. Fresh harness runs therefore never receive
                    // an inferred or stale resume id through this path.
                    resume: None,
                    env: EnvPolicy::scrubbed(),
                    timeout: task.timeout,
                    harness_lifecycle_reporter: task.harness_lifecycle_reporter.clone(),
                };
                // Harnesses own their process tree and the Harness contract
                // requires them to honour both this token and `job.timeout`.
                // Await the harness directly: racing it in `drive` would drop
                // the process-owning future before it can reap/kill the group.
                let child = cancel.clone();
                let context = pinned_workdir
                    .cloned()
                    .map(HarnessExecutionContext::with_pinned_workdir)
                    .unwrap_or_default();
                let out = harness.run_scoped(job, context, child).await?;
                Ok(AttemptOk {
                    text: out.text,
                    usage: out.usage,
                    native_session_id: out.native_session_id,
                    // Harness history is CLI-owned; the kernel never
                    // fabricates a transcript for it.
                    transcript_delta: None,
                })
            }
        }
    }

    /// Load-or-create the session record, fold in this run, and persist it.
    async fn update_session(
        &self,
        session_id: &str,
        session_ctx: &SessionContext,
        record: &RunRecord,
        native_session_id: Option<&str>,
        transcript_delta: Option<(ChatMessage, ChatMessage)>,
    ) -> Result<()> {
        let transcript_delta = transcript_delta
            .map(|(user, assistant)| vec![user, assistant])
            .unwrap_or_default();
        let lease = session_ctx.execution_lease.as_ref().ok_or_else(|| {
            VyaneError::config("session update requires its live execution-period lease")
        })?;
        if lease.owner() != self.owner
            || lease.session_id() != session_id
            || lease.execution_id() != record.run_id
        {
            return Err(VyaneError::config(
                "session execution lease identity does not match the completed run",
            ));
        }
        lease.revalidate().await?;
        lease
            .apply_update(
                session_ctx.session_revision,
                &SessionUpdate {
                    owner: self.owner.clone(),
                    session_id: session_id.to_string(),
                    target: record.target.clone(),
                    native_session_id: native_session_id.map(str::to_string),
                    transcript_delta,
                    occurred_at: Utc::now(),
                },
            )
            .await
            .map(|_| ())
    }
}

fn validate_session_lease_identity(
    lease: &dyn SessionExecutionLease,
    owner: &str,
    session_id: &str,
    execution_id: &str,
) -> Result<()> {
    if lease.owner() != owner
        || lease.session_id() != session_id
        || lease.execution_id() != execution_id
    {
        return Err(VyaneError::config(
            "session store returned an execution lease for another identity",
        ));
    }
    Ok(())
}

fn validate_session_snapshot_identity(
    snapshot: &vyane_core::SessionSnapshot,
    owner: &str,
    session_id: &str,
) -> Result<()> {
    if snapshot.record.owner != owner || snapshot.record.session_id != session_id {
        return Err(VyaneError::config(
            "session store returned a snapshot for another identity",
        ));
    }
    Ok(())
}

fn validate_native_resume(prepared: &PreparedDispatch, session_ctx: &SessionContext) -> Result<()> {
    let has_harness_target = prepared.admitted.iter().any(|admitted| {
        admitted.bound.transport == AdapterTransport::CliWrap
            || admitted.bound.target.harness.is_some()
    });
    if has_harness_target {
        match &session_ctx.native_session {
            NativeSessionState::Absent => {}
            NativeSessionState::LegacyUnbound { .. } => {
                return Err(VyaneError::new(
                    ErrorKind::Unsupported,
                    "native-session resume requires an exact NativeSessionDomain; legacy unbound resume is refused",
                ));
            }
            NativeSessionState::Bound { .. } => {
                return Err(VyaneError::new(
                    ErrorKind::Unsupported,
                    "domain-bound native-session resume remains disabled until an active execution permit consumer and exact domain revalidation are enforced",
                ));
            }
            _ => {
                return Err(VyaneError::new(
                    ErrorKind::Unsupported,
                    "native-session state is not supported by this execution kernel",
                ));
            }
        }
    }
    Ok(())
}

/// Compose the prompt handed to a harness from the task prompt and, when set,
/// `TaskSpec.system` appended as instructions.
///
/// The shape is fixed — `prompt + "\n\n## Additional instructions\n\n" +
/// system` — because a harness has no separate system-message channel; the
/// system text has to ride along inside the single prompt string. When there is
/// no system text the prompt is passed through unchanged. Pinned by a test so
/// the format cannot drift.
fn compose_harness_prompt(prompt: &str, system: Option<&str>) -> String {
    match system {
        Some(system) => format!("{prompt}{HARNESS_SYSTEM_HEADING}{system}"),
        None => prompt.to_string(),
    }
}

/// Drive one attempt future under a timeout and a cancellation token.
///
/// Cancellation is checked with a biased select so an already-cancelled token
/// wins deterministically; the timeout maps to an [`ErrorKind::Timeout`] error
/// so it is failover-eligible, while cancellation maps to
/// [`ErrorKind::Cancelled`] so it aborts the chain.
async fn drive<F>(
    fut: F,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
) -> std::result::Result<AttemptOk, VyaneError>
where
    F: Future<Output = std::result::Result<AttemptOk, VyaneError>>,
{
    if cancel.is_cancelled() {
        return Err(VyaneError::cancelled());
    }

    let timed = async {
        match timeout {
            Some(d) => match tokio::time::timeout(d, fut).await {
                Ok(r) => r,
                Err(_elapsed) => Err(VyaneError::new(
                    ErrorKind::Timeout,
                    format!("attempt exceeded timeout of {}ms", d.as_millis()),
                )),
            },
            None => fut.await,
        }
    };

    tokio::pin!(timed);
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(VyaneError::cancelled()),
        r = &mut timed => r,
    }
}

/// Map a terminal error kind onto a run status. `Timeout`/`Cancelled` keep
/// their identity; every other failing kind is a generic `Error`.
fn status_for_error(kind: ErrorKind) -> RunStatus {
    match kind {
        ErrorKind::Timeout => RunStatus::Timeout,
        ErrorKind::Cancelled => RunStatus::Cancelled,
        _ => RunStatus::Error,
    }
}

/// Only reached in the impossible "empty chain slipped past the guard" case,
/// behind a `debug_assert`. Kept minimal and credential-free.
fn fallback_target() -> Target {
    use vyane_core::{ModelId, Protocol, ProviderId};
    Target {
        provider: ProviderId::new("unknown"),
        protocol: Protocol::OpenaiChat,
        harness: None,
        model: ModelId::new("unknown"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn harness_prompt_appends_system_in_fixed_shape() {
        // Pin the exact wire shape the harness receives: prompt, the heading,
        // then the system text — nothing else, in this order.
        let composed = compose_harness_prompt("do the thing", Some("be terse"));
        assert_eq!(
            composed,
            "do the thing\n\n## Additional instructions\n\nbe terse"
        );
    }

    #[test]
    fn harness_prompt_without_system_is_passthrough() {
        assert_eq!(compose_harness_prompt("just this", None), "just this");
    }
}
