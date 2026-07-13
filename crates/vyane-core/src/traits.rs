//! The capability traits the kernel composes.
//!
//! * [`ChatClient`] — speaks one wire protocol over HTTP (no workspace).
//! * [`AuthorizedToolChatClient`] — performs guarded native-loop model turns.
//! * [`Harness`] — spawns a CLI execution shell as a subprocess.
//! * [`Ledger`] — append-only run accounting.
//! * [`SessionStore`] — session persistence.
//!
//! All traits are object-safe: the kernel holds them as trait objects and
//! composes targets at runtime from configuration.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;

use crate::chat::{ChatOutcome, ChatRequest, StreamEvent};
use crate::env::EnvPolicy;
use crate::error::Result;
use crate::native_authority::NativeExecutionAuthority;
use crate::run::{RunQuery, RunRecord, Usage};
use crate::session::{NativeSessionTransition, SessionRecord, SessionSnapshot, SessionUpdate};
use crate::target::{Endpoint, HarnessKind, ModelId, Protocol, Sandbox};
use crate::task::{GenParams, HarnessLifecycleReporter, HarnessSpawnAuthority};
use crate::tool_chat::{ToolChatOutcome, ToolChatRequest};
use crate::workdir::PinnedWorkdir;

/// A client for one wire protocol against one endpoint.
#[async_trait]
pub trait ChatClient: Send + Sync {
    fn protocol(&self) -> Protocol;

    /// Perform a non-streaming completion.
    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome>;

    /// Perform one typed, non-streaming tool-chat turn.
    ///
    /// Text-only clients remain source-compatible: the default implementation
    /// delegates to [`Self::complete`] only when the request contains no tool
    /// definitions and every message can be represented losslessly by the
    /// legacy text envelope. It never flattens tool calls/results into text.
    async fn complete_turn(&self, req: ToolChatRequest) -> Result<ToolChatOutcome> {
        let protocol = self.protocol();
        match req.try_into_text_request() {
            Ok(Some(req)) => self.complete(req).await.map(ToolChatOutcome::from),
            Ok(None) => Err(crate::error::VyaneError::unsupported(format!(
                "{protocol} client does not support typed tool chat"
            ))),
            Err(error) => Err(crate::error::VyaneError::config(format!(
                "invalid typed tool-chat request: {error}"
            ))),
        }
    }

    /// Perform a streaming completion. Default: unsupported.
    ///
    /// Implementations must remain live even when a target emits no
    /// reasoning deltas at all — completion is signalled by
    /// [`StreamEvent::Done`], never by reasoning traffic.
    async fn stream(&self, req: ChatRequest) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let _ = req;
        Err(crate::error::VyaneError::unsupported(format!(
            "{} client does not support streaming",
            self.protocol()
        )))
    }
}

/// A typed tool-chat client whose model sends cannot bypass live authority.
///
/// This is deliberately independent from [`ChatClient`]. Native-loop callers
/// receive this narrower capability and therefore have no unguarded
/// `complete_turn` fallback available on the same trait object. Implementors
/// must revalidate `authority` immediately before every physical wire attempt,
/// including retries, and must honor `cancel` throughout the request.
#[async_trait]
pub trait AuthorizedToolChatClient: Send + Sync {
    fn protocol(&self) -> Protocol;

    async fn complete_turn_authorized(
        &self,
        req: ToolChatRequest,
        turn: u32,
        authority: &dyn NativeExecutionAuthority,
        cancel: &CancellationToken,
    ) -> Result<ToolChatOutcome>;
}

/// Everything a harness needs to execute one job.
#[derive(Debug, Clone)]
pub struct HarnessJob {
    /// The task / prompt text (already assembled by the kernel).
    pub prompt: String,
    pub model: ModelId,
    /// wire protocol of the endpoint, letting harnesses pick a matching wire_api for custom endpoints
    pub protocol: Protocol,
    /// Endpoint override. `None` = the harness authenticates natively.
    pub endpoint: Option<Endpoint>,
    pub params: GenParams,
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    /// Native session id to resume, if continuing a session.
    pub resume: Option<String>,
    /// Environment policy; harnesses must build the child environment
    /// exclusively through [`EnvPolicy::build`].
    pub env: EnvPolicy,
    /// `None` = run until completion.
    pub timeout: Option<Duration>,
    /// Optional process-local observer for the independently-grouped harness
    /// child. The kernel copies this from the originating task; harnesses must
    /// not persist it or pass it into the child environment.
    pub harness_lifecycle_reporter: Option<HarnessLifecycleReporter>,
}

/// Additive process-local execution context for a harness invocation.
///
/// Keeping this separate from [`HarnessJob`] preserves source compatibility
/// for downstream job literals and prevents a serializable-looking job from
/// carrying an executable directory handle.
#[derive(Debug, Clone, Default)]
pub struct HarnessExecutionContext {
    pinned_workdir: Option<PinnedWorkdir>,
    spawn_authority: Option<HarnessSpawnAuthority>,
}

impl HarnessExecutionContext {
    pub fn with_pinned_workdir(pinned_workdir: PinnedWorkdir) -> Self {
        Self {
            pinned_workdir: Some(pinned_workdir),
            spawn_authority: None,
        }
    }

    /// Construct a context carrying only a live subprocess-spawn authority.
    pub fn with_spawn_authority(spawn_authority: HarnessSpawnAuthority) -> Self {
        Self {
            pinned_workdir: None,
            spawn_authority: Some(spawn_authority),
        }
    }

    /// Construct a context carrying both a stable workdir and live spawn
    /// authority without exposing either through the serializable job.
    pub fn with_pinned_workdir_and_spawn_authority(
        pinned_workdir: PinnedWorkdir,
        spawn_authority: HarnessSpawnAuthority,
    ) -> Self {
        Self {
            pinned_workdir: Some(pinned_workdir),
            spawn_authority: Some(spawn_authority),
        }
    }

    pub fn pinned_workdir(&self) -> Option<&PinnedWorkdir> {
        self.pinned_workdir.as_ref()
    }

    pub fn spawn_authority(&self) -> Option<&HarnessSpawnAuthority> {
        self.spawn_authority.as_ref()
    }
}

/// Result of a harness run.
#[derive(Debug, Clone)]
pub struct HarnessOutcome {
    /// The final answer text extracted from the harness output.
    pub text: String,
    /// Native session id for future resumes, when the harness reports one.
    pub native_session_id: Option<String>,
    pub usage: Option<Usage>,
    pub exit_code: i32,
    pub duration: Duration,
}

/// A live event during a streaming harness run. Implementations that override
/// [`Harness::run_stream`] emit these as the CLI's stdout arrives, so the
/// caller can display incremental output to the user.
#[derive(Debug, Clone)]
pub enum HarnessStreamEvent {
    /// A fragment of the answer text (from the CLI's stdout).
    Delta(String),
    /// A tool-use notification — the agent invoked a tool (file edit, command,
    /// etc.). Optional: implementations may emit these for observability.
    ToolUse { name: String, summary: String },
}

/// An execution shell that runs jobs as a subprocess.
#[async_trait]
pub trait Harness: Send + Sync {
    fn kind(&self) -> HarnessKind;

    /// Whether the harness binary is present and runnable on this machine.
    async fn available(&self) -> bool;

    /// Run a job to completion.
    ///
    /// Implementations must honour `cancel` promptly (kill the child process
    /// group — a bare child kill leaves grandchildren running), honour
    /// `job.timeout` when set, and classify failures onto
    /// [`crate::ErrorKind`] faithfully.
    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome>;

    /// Run with process-local execution context. Existing harnesses remain
    /// source-compatible, but the default fails closed when the context carries
    /// a pinned workdir that the implementation does not understand.
    async fn run_scoped(
        &self,
        job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: CancellationToken,
    ) -> Result<HarnessOutcome> {
        if context.pinned_workdir().is_some() {
            return Err(crate::error::VyaneError::unsupported(format!(
                "{} harness does not implement pinned scoped execution",
                self.kind()
            )));
        }
        if context.spawn_authority().is_some() {
            return Err(crate::error::VyaneError::unsupported(format!(
                "{} harness does not implement live-authority scoped execution",
                self.kind()
            )));
        }
        self.run(job, cancel).await
    }

    /// Run a job with streaming output. Default: unsupported.
    ///
    /// Implementations that support streaming should call `on_event` for each
    /// text fragment as it arrives, then return the final [`HarnessOutcome`].
    /// The returned outcome is identical in shape to what [`run`](Self::run)
    /// would produce — streaming only changes *when* intermediate output is
    /// observed, not the final result.
    ///
    /// When this method returns [`crate::ErrorKind::Unsupported`], the caller
    /// should fall back to [`run`](Self::run).
    async fn run_stream(
        &self,
        job: HarnessJob,
        cancel: CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        let _ = (job, cancel, on_event);
        Err(crate::error::VyaneError::unsupported(format!(
            "{} does not support streaming",
            self.kind()
        )))
    }

    /// Streaming counterpart to [`Self::run_scoped`].
    async fn run_stream_scoped(
        &self,
        job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        if context.pinned_workdir().is_some() {
            return Err(crate::error::VyaneError::unsupported(format!(
                "{} harness does not implement pinned scoped streaming",
                self.kind()
            )));
        }
        if context.spawn_authority().is_some() {
            return Err(crate::error::VyaneError::unsupported(format!(
                "{} harness does not implement live-authority scoped streaming",
                self.kind()
            )));
        }
        self.run_stream(job, cancel, on_event).await
    }
}

/// Append-only run accounting.
#[async_trait]
pub trait Ledger: Send + Sync {
    async fn append(&self, record: &RunRecord) -> Result<()>;
    async fn query(&self, query: RunQuery) -> Result<Vec<RunRecord>>;
}

/// Live execution ownership for one logical session.
///
/// A session execution lease is deliberately a live trait object rather than
/// a serializable token. The store that grants it remains the authority for
/// all reads and mutations performed through it, and dropping the object ends
/// the execution period. Callers must acquire the lease before loading
/// continuity state and retain it through the final session commit.
#[async_trait]
pub trait SessionExecutionLease: Send + Sync {
    /// Exact owner namespace protected by this lease.
    fn owner(&self) -> &str;
    /// Exact logical session protected by this lease.
    fn session_id(&self) -> &str;
    /// Kernel execution identity bound when the lease was acquired.
    fn execution_id(&self) -> &str;
    /// Revalidate the live lease without exposing implementation credentials.
    /// A local store may use an OS-owned lock whose descriptor lifetime is the
    /// fence. Distributed implementations need their own bounded renewal and
    /// stale-holder fencing protocol; this interface does not provide one.
    async fn revalidate(&self) -> Result<()>;
    /// Load continuity while the execution lease is held.
    async fn load_snapshot(&self) -> Result<Option<SessionSnapshot>>;
    /// Apply one completed transcript/legacy update with the revision observed
    /// after acquisition. A live lease is single-commit: retries after any
    /// commit attempt must reload through a new execution.
    async fn apply_update(
        &self,
        expected_revision: u64,
        update: &SessionUpdate,
    ) -> Result<SessionSnapshot>;
    /// Apply one revision-fenced native transition while the lease is held.
    async fn apply_native_transition(
        &self,
        transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot>;
}

/// Session persistence.
#[async_trait]
pub trait SessionStore: Send + Sync {
    /// Acquire exclusive execution ownership of one `(owner, session_id)`.
    ///
    /// The returned object must remain live from the first continuity read
    /// through the final session mutation. Implementations must release the
    /// lease on drop and make stale ownership recoverable after process
    /// failure. A competing execution or control mutation returns
    /// [`ErrorKind::Conflict`](crate::ErrorKind::Conflict) instead of waiting
    /// without bound.
    ///
    /// Legacy/custom stores compile unchanged but fail closed for session
    /// execution until they implement this contract.
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        let _ = (owner, session_id, execution_id);
        Err(crate::error::VyaneError::unsupported(
            "session store does not support execution-period leases",
        ))
    }
    /// Load one session from an explicit owner namespace.
    ///
    /// Session ids are only unique within an owner. Implementations must not
    /// use `session_id` as a global key or return a record whose embedded
    /// owner/session identity differs from the requested pair.
    async fn load(&self, owner: &str, session_id: &str) -> Result<Option<SessionRecord>>;
    /// Load revisioned native-session binding state when the implementation
    /// supports it.
    ///
    /// The default preserves source compatibility for existing stores and
    /// classifies any legacy `SessionRecord.native_session_id` as unbound at
    /// revision zero. It never infers a native domain from the current target.
    async fn load_snapshot(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Option<SessionSnapshot>> {
        Ok(self
            .load(owner, session_id)
            .await?
            .map(SessionSnapshot::from_legacy_record))
    }
    /// Save inside an explicit owner authority and reject a mismatched embedded
    /// owner rather than trusting caller-controlled record data.
    async fn save(&self, owner: &str, record: &SessionRecord) -> Result<()>;
    /// Atomically load, apply, and persist one completed-run update.
    async fn apply_update(&self, owner: &str, update: &SessionUpdate) -> Result<SessionRecord>;
    /// Apply one compare-and-swap native-session transition.
    ///
    /// Legacy/custom stores remain source-compatible but fail closed until
    /// they implement an atomic domain-aware persistence contract.
    async fn apply_native_transition(
        &self,
        owner: &str,
        session_id: &str,
        transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot> {
        let _ = (owner, session_id, transition);
        Err(crate::error::VyaneError::unsupported(
            "session store does not support atomic native-session transitions",
        ))
    }
    /// List revision-aware session snapshots for one owner.
    ///
    /// The default projects legacy stores at revision zero. Domain-aware
    /// stores should override this so callers can distinguish `Bound`,
    /// `LegacyUnbound`, and `Absent` without treating the legacy record shape
    /// as authority.
    async fn list_snapshots(&self, owner: &str) -> Result<Vec<SessionSnapshot>> {
        Ok(self
            .list(owner)
            .await?
            .into_iter()
            .map(SessionSnapshot::from_legacy_record)
            .collect())
    }
    /// List exactly one owner namespace. Cross-owner enumeration belongs on an
    /// explicit administrative interface, not this runtime trait.
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::{ModelId, NativeSideEffect, ToolChatMessage, ToolChoice, VyaneError};

    #[derive(Default)]
    struct RecordingAuthority {
        effects: Mutex<Vec<NativeSideEffect>>,
    }

    #[async_trait]
    impl NativeExecutionAuthority for RecordingAuthority {
        async fn revalidate(&self, effect: NativeSideEffect) -> Result<()> {
            self.effects
                .lock()
                .expect("recording authority lock")
                .push(effect);
            Ok(())
        }
    }

    struct GuardedClient;

    #[async_trait]
    impl AuthorizedToolChatClient for GuardedClient {
        fn protocol(&self) -> Protocol {
            Protocol::OpenaiResponses
        }

        async fn complete_turn_authorized(
            &self,
            req: ToolChatRequest,
            turn: u32,
            authority: &dyn NativeExecutionAuthority,
            cancel: &CancellationToken,
        ) -> Result<ToolChatOutcome> {
            if cancel.is_cancelled() {
                return Err(VyaneError::cancelled());
            }
            authority
                .revalidate(NativeSideEffect::ModelSend {
                    turn,
                    wire_attempt: 1,
                })
                .await?;
            Ok(ToolChatOutcome {
                assistant: crate::AssistantToolTurn {
                    text: req.model.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            })
        }
    }

    #[test]
    fn authorized_tool_chat_client_is_object_safe_and_dispatches_authority() {
        let client: Box<dyn AuthorizedToolChatClient> = Box::new(GuardedClient);
        let recorder = Arc::new(RecordingAuthority::default());
        let authority: Arc<dyn NativeExecutionAuthority> = Arc::clone(&recorder) as Arc<_>;
        let cancel = CancellationToken::new();
        let request = ToolChatRequest {
            model: ModelId::new("public-test-model"),
            messages: vec![ToolChatMessage::user(
                "request body stays outside authority",
            )],
            tools: Vec::new(),
            tool_choice: ToolChoice::None,
            params: Default::default(),
        };

        let outcome = futures::executor::block_on(client.complete_turn_authorized(
            request,
            9,
            authority.as_ref(),
            &cancel,
        ))
        .expect("guarded completion");

        assert_eq!(client.protocol(), Protocol::OpenaiResponses);
        assert_eq!(outcome.assistant.text, "public-test-model");
        assert_eq!(
            recorder
                .effects
                .lock()
                .expect("recording authority lock")
                .as_slice(),
            &[NativeSideEffect::ModelSend {
                turn: 9,
                wire_attempt: 1,
            }]
        );
    }

    struct LegacyHarness {
        called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Harness for LegacyHarness {
        fn kind(&self) -> HarnessKind {
            HarnessKind::Other("legacy-test".into())
        }

        async fn available(&self) -> bool {
            true
        }

        async fn run(
            &self,
            _job: HarnessJob,
            _cancel: CancellationToken,
        ) -> Result<HarnessOutcome> {
            self.called.store(true, Ordering::SeqCst);
            Ok(HarnessOutcome {
                text: String::new(),
                native_session_id: None,
                usage: None,
                exit_code: 0,
                duration: Duration::ZERO,
            })
        }
    }

    #[test]
    fn legacy_harness_rejects_spawn_authority_without_delegating() {
        let called = Arc::new(AtomicBool::new(false));
        let harness = LegacyHarness {
            called: Arc::clone(&called),
        };
        let job = HarnessJob {
            prompt: String::new(),
            model: ModelId::new("test"),
            protocol: Protocol::OpenaiChat,
            endpoint: None,
            params: Default::default(),
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            resume: None,
            env: EnvPolicy::scrubbed(),
            timeout: None,
            harness_lifecycle_reporter: None,
        };
        let context =
            HarnessExecutionContext::with_spawn_authority(HarnessSpawnAuthority::new(|| true));
        let error =
            futures::executor::block_on(harness.run_scoped(job, context, CancellationToken::new()))
                .expect_err("legacy harness must fail closed");
        assert_eq!(error.kind, crate::ErrorKind::Unsupported);
        assert!(!called.load(Ordering::SeqCst));
    }
}
