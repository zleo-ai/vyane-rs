//! The single-task dispatch state machine.
//!
//! `dispatch` walks a resolved chain of targets, executing one attempt each,
//! and always ends by producing exactly one [`RunRecord`] — appended to the
//! ledger and reflected in the session store — whether the run succeeded,
//! failed, timed out, or was cancelled. Failover between targets is gated by
//! [`vyane_core::ErrorKind::failover_eligible`]; the kernel never re-implements
//! that rule, it only calls it.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use vyane_core::{
    AdapterTransport, Attempt, AttemptOutcome, BoundTarget, CancellationToken, ChatMessage,
    ChatRequest, EnvPolicy, ErrorKind, HarnessJob, Ledger, Result, RunRecord, RunStatus,
    SessionRecord, SessionStore, Target, TaskSpec, Usage, VyaneError,
};

use crate::digest::task_digest;
use crate::executor::{Executor, ExecutorFactory};

/// Owner scope used when the caller does not name one.
const DEFAULT_OWNER: &str = "local";

/// Number of leading characters of the prompt kept as a human-scannable
/// preview on the run record (see `RunRecord::task_preview`).
const TASK_PREVIEW_CHARS: usize = 120;

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
}

/// The successful product of a single attempt, before it becomes an `Attempt`.
struct AttemptOk {
    text: String,
    usage: Option<Usage>,
    /// Native session id reported by a harness, if any.
    native_session_id: Option<String>,
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
        }
    }

    /// Override the owner scope written onto records (default `"local"`).
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
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
    /// The `Result::Err` arm is reserved for kernel-level failures that make a
    /// record impossible or meaningless: an empty chain (a caller/resolution
    /// bug — there is no target to record against) and a ledger-append I/O
    /// failure (the run cannot be persisted). A run that simply *failed* is
    /// still `Ok` — it comes back as a record whose `status` is
    /// `Error`/`Timeout`/`Cancelled`.
    pub async fn dispatch(
        &self,
        task: &TaskSpec,
        chain: Vec<BoundTarget>,
        cancel: CancellationToken,
    ) -> Result<RunRecord> {
        if chain.is_empty() {
            return Err(VyaneError::config(
                "dispatch received an empty target chain; resolution must supply at least one target",
            ));
        }

        let started_at = Utc::now();
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

        for (index, bound) in chain.iter().enumerate() {
            let has_next = index + 1 < total;
            last_target = Some(bound.target.clone());
            last_transport = bound.transport;

            let attempt_start_wall = Utc::now();
            let attempt_start = Instant::now();

            // Attempt = build the executor, then drive one call. A factory
            // failure is a real attempt failure (e.g. a missing harness
            // binary) and is gated for failover like any execution error.
            let outcome = match self.factory.make(bound) {
                Ok(executor) => self.run_attempt(executor, task, bound, &cancel).await,
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

        let (status, session_id_from_run, output_chars, error_msg) = match &final_outcome {
            Ok(ok) => (
                RunStatus::Success,
                ok.native_session_id.clone(),
                Some(ok.text.chars().count() as u64),
                None,
            ),
            Err(err) => (
                status_for_error(err.kind),
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
            run_id: uuid::Uuid::now_v7().to_string(),
            owner: self.owner.clone(),
            started_at,
            finished_at,
            task_digest: task_digest(&task.prompt),
            task_preview: Some(task_preview(&task.prompt)),
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

        // Ledger append is the run's source of truth and happens on success and
        // failure alike. A real append failure is infrastructure-level and
        // surfaces as Err — the run could not be persisted.
        self.ledger.append(&record).await?;

        // Session continuity is best-effort: the run is already durably
        // recorded, so a session-store hiccup must not discard it. Only runs
        // that name a session touch the store.
        if let Some(sid) = session_id.as_deref() {
            if let Err(e) = self
                .update_session(sid, &record, session_id_from_run.as_deref())
                .await
            {
                tracing::warn!(
                    session_id = sid,
                    error = %e,
                    "session store update failed after run was recorded"
                );
            }
        }

        Ok(record)
    }

    /// Execute exactly one attempt against a built executor, enforcing the
    /// task timeout and cancellation uniformly across both transports.
    async fn run_attempt(
        &self,
        executor: Executor,
        task: &TaskSpec,
        bound: &BoundTarget,
        cancel: &CancellationToken,
    ) -> std::result::Result<AttemptOk, VyaneError> {
        match executor {
            Executor::Chat(client) => {
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
                let fut = async move {
                    let out = client.complete(req).await?;
                    Ok(AttemptOk {
                        text: out.text,
                        usage: out.usage,
                        native_session_id: None,
                    })
                };
                drive(fut, task.timeout, cancel).await
            }
            Executor::Agent(harness) => {
                // The kernel carries no credentials, so it hands the harness a
                // scrubbed env policy; concrete injection (auth, base URL,
                // model overrides) is the assembler/adapter's responsibility,
                // not the kernel's.
                let job = HarnessJob {
                    prompt: task.prompt.clone(),
                    model: bound.target.model.clone(),
                    endpoint: bound.endpoint.clone(),
                    params: bound.params.clone(),
                    workdir: task.workdir.clone(),
                    sandbox: task.sandbox,
                    resume: task.session.as_ref().map(|s| s.as_str().to_string()),
                    env: EnvPolicy::scrubbed(),
                    timeout: task.timeout,
                };
                // Forward the token so the harness can kill its process group;
                // `drive` additionally enforces the kernel-level timeout and
                // top-level cancellation for uniform behaviour with chat.
                let child = cancel.clone();
                let fut = async move {
                    let out = harness.run(job, child).await?;
                    Ok(AttemptOk {
                        text: out.text,
                        usage: out.usage,
                        native_session_id: out.native_session_id,
                    })
                };
                drive(fut, task.timeout, cancel).await
            }
        }
    }

    /// Load-or-create the session record, fold in this run, and persist it.
    async fn update_session(
        &self,
        session_id: &str,
        record: &RunRecord,
        native_session_id: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now();
        let mut session = match self.sessions.load(session_id).await? {
            Some(existing) => existing,
            None => SessionRecord {
                session_id: session_id.to_string(),
                owner: self.owner.clone(),
                target: record.target.clone(),
                native_session_id: None,
                transcript: Vec::new(),
                created_at: now,
                updated_at: now,
                run_count: 0,
            },
        };

        session.target = record.target.clone();
        session.updated_at = now;
        session.run_count = session.run_count.saturating_add(1);

        // Native harness continuity: when the run produced a native session id
        // (only harness/`CliWrap` runs do), store it so the next run resumes
        // the CLI's own session; the transcript stays empty because the CLI
        // owns that history. Direct-chat transcript growth is left to the
        // assembler's session integration — the kernel does not have the
        // assistant reply threaded back here as a message, and inventing one
        // would corrupt the replay. Keyed on the id (not the transport enum) so
        // it stays exhaustive as new transports are added.
        if let Some(native) = native_session_id {
            session.native_session_id = Some(native.to_string());
        }

        self.sessions.save(&session).await
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

/// First [`TASK_PREVIEW_CHARS`] characters of the prompt, on a char boundary.
fn task_preview(prompt: &str) -> String {
    prompt.chars().take(TASK_PREVIEW_CHARS).collect()
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
