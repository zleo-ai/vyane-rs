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
}

/// Result of a completed dispatch: the persisted run record plus successful
/// answer text when the run produced one.
#[derive(Debug, Clone)]
pub struct DispatchOutcome {
    pub record: RunRecord,
    pub output: Option<String>,
}

/// The successful product of a single attempt, before it becomes an `Attempt`.
struct AttemptOk {
    text: String,
    usage: Option<Usage>,
    /// Native session id reported by a harness, if any. Drives native
    /// (CLI-owned) session continuity on the next run.
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
#[derive(Default)]
struct SessionContext {
    /// Native session id to resume for harness runs (`None` if the session is
    /// new or purely a transcript session). Never the logical id.
    native_session_id: Option<String>,
    /// Prior transcript to replay for direct-chat continuity, in stored order,
    /// inserted after any `TaskSpec.system` message and before the current user
    /// message. Empty for a new or pure-harness session.
    transcript: Vec<ChatMessage>,
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
    /// Cancellation is checked before building each executor and between
    /// failover attempts, so a token that is already cancelled yields a
    /// `Cancelled` run deterministically without ever calling the factory.
    ///
    /// The `Result::Err` arm is reserved for one kernel-level failure that
    /// makes a record impossible: an empty chain (a caller/resolution bug —
    /// there is no target to record against). A run that simply *failed* is
    /// still `Ok` — it comes back as a record whose `status` is
    /// `Error`/`Timeout`/`Cancelled`. Persistence failures *after* a completed
    /// run (ledger append, session save) are best-effort and never demote a
    /// finished run to a caller-visible error (see below).
    pub async fn dispatch(
        &self,
        task: &TaskSpec,
        chain: Vec<BoundTarget>,
        cancel: CancellationToken,
    ) -> Result<DispatchOutcome> {
        if chain.is_empty() {
            return Err(VyaneError::config(
                "dispatch received an empty target chain; resolution must supply at least one target",
            ));
        }

        // Load session continuity once, up front: native resume id and any
        // transcript to replay both come from the stored record, keyed by the
        // logical id. A load error is not fatal — treat it as a fresh session
        // and note it, so a flaky store never blocks the run.
        let session_ctx = self.load_session_context(task).await;

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
            let outcome = match self.factory.make(bound) {
                Ok(executor) => {
                    self.run_attempt(executor, task, bound, &session_ctx, &cancel)
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

        // Persistence after a completed run is **best-effort** (architect
        // decision): the model call already happened, so a ledger or session
        // store error must not convert a successful run into a caller-visible
        // failure. We emit a `tracing::warn` and return the record normally.
        // The `Result::Err` arm of `dispatch` stays reserved for the empty
        // chain, i.e. failures that make a record impossible in the first place.
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

    /// Load the session continuity context for `task`, if it names a session.
    ///
    /// Returns native-resume id + replayable transcript from the *stored*
    /// record. A missing session yields an empty context (fresh session); a
    /// load error also yields an empty context but is logged — a flaky store
    /// degrades to "start fresh" rather than failing the whole dispatch.
    async fn load_session_context(&self, task: &TaskSpec) -> SessionContext {
        let Some(session_ref) = task.session.as_ref() else {
            return SessionContext::default();
        };
        let sid = session_ref.as_str();
        match self.sessions.load(sid).await {
            Ok(Some(record)) => SessionContext {
                native_session_id: record.native_session_id.clone(),
                transcript: record.transcript.clone(),
            },
            Ok(None) => SessionContext::default(),
            Err(e) => {
                tracing::warn!(
                    session_id = sid,
                    error = %e,
                    "session load failed; continuing as a fresh session"
                );
                SessionContext::default()
            }
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
                // of the prompt. Native session continuity resumes the CLI's
                // own session via `native_session_id` from the stored record —
                // never the logical id, which is only the store key.
                let prompt = compose_harness_prompt(&task.prompt, task.system.as_deref());
                let job = HarnessJob {
                    prompt,
                    model: bound.target.model.clone(),
                    protocol: bound.target.protocol,
                    endpoint: bound.endpoint.clone(),
                    params: bound.params.clone(),
                    workdir: task.workdir.clone(),
                    sandbox: task.sandbox,
                    resume: session_ctx.native_session_id.clone(),
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
                        // Harness history is CLI-owned; the kernel never
                        // fabricates a transcript for it.
                        transcript_delta: None,
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
        transcript_delta: Option<(ChatMessage, ChatMessage)>,
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
        // the CLI's own session. Harness runs skip transcript replay entirely
        // — the CLI owns that history — so they update the native id + run
        // count and nothing else.
        if let Some(native) = native_session_id {
            session.native_session_id = Some(native.to_string());
        }

        // Direct-chat continuity: append this run's (user, assistant) pair to
        // the stored transcript so the next run replays it. Harness runs carry
        // no delta, so this is skipped for them.
        if let Some((user, assistant)) = transcript_delta {
            session.transcript.push(user);
            session.transcript.push(assistant);
        }

        self.sessions.save(&session).await
    }
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
