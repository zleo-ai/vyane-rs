//! Acceptance tests for the dispatch kernel, run entirely against mock
//! `ChatClient` / `Harness` / `Ledger` / `SessionStore` / `ExecutorFactory` —
//! no network, no subprocesses. Each test maps to a bullet in WP-04's
//! "Acceptance (mechanically checkable)" list.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::pending;
use vyane_core::{
    AdapterTransport, AttemptOutcome, BoundTarget, CancellationToken, ChatClient, ChatMessage,
    ChatOutcome, ChatRequest, Endpoint, ErrorKind, GenParams, Harness, HarnessExecutionContext,
    HarnessJob, HarnessKind, HarnessLifecycleEvent, HarnessLifecycleReporter, HarnessOutcome,
    HarnessSpawnAuthority, HarnessStreamEvent, Ledger, ModelId, NativeSessionBinding,
    NativeSessionDomain, NativeSessionState, NativeSessionTransition, Protocol, ProviderId, Result,
    Role, RunQuery, RunRecord, RunStatus, Sandbox, SessionExecutionLease, SessionRecord,
    SessionRef, SessionSnapshot, SessionStore, SessionUpdate, Target, TaskSpec, Usage, VyaneError,
    WorkdirIdentity,
};
use vyane_kernel::{
    AttemptScope, CapabilityAdmissionDecision, CapabilityAdmissionError,
    CapabilityAdmissionEvidence, CapabilityManifest, Dispatcher, Executor, ExecutorFactory,
    FilesystemCapability, IsolationStrength,
};
use vyane_ledger::FsSessionStore;

// ---------------------------------------------------------------------------
// Shared probe: concurrency high-water mark + captured requests/jobs
// ---------------------------------------------------------------------------

/// Instrumentation shared by every mock executor a factory builds. It records
/// the maximum number of attempts in flight at once (to prove the broadcast
/// semaphore truly bounds concurrency) and captures the `ChatRequest` /
/// `HarnessJob` each attempt saw (to prove transcript replay, native-id resume,
/// and harness prompt composition).
#[derive(Default)]
struct Probe {
    active_now: AtomicUsize,
    active_max: AtomicUsize,
    chat_requests: Mutex<Vec<ChatRequest>>,
    harness_jobs: Mutex<Vec<HarnessJob>>,
    scoped_contexts: Mutex<Vec<(bool, Option<std::path::PathBuf>)>>,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Mark one attempt as entered; updates the high-water mark. The returned
    /// guard decrements on drop, so overlap is measured for real.
    fn enter(self: &Arc<Self>) -> ActiveGuard {
        let now = self.active_now.fetch_add(1, Ordering::SeqCst) + 1;
        self.active_max.fetch_max(now, Ordering::SeqCst);
        ActiveGuard {
            probe: Arc::clone(self),
        }
    }

    fn max_concurrent(&self) -> usize {
        self.active_max.load(Ordering::SeqCst)
    }

    fn chat_requests(&self) -> Vec<ChatRequest> {
        self.chat_requests.lock().unwrap().clone()
    }

    fn harness_jobs(&self) -> Vec<HarnessJob> {
        self.harness_jobs.lock().unwrap().clone()
    }

    #[cfg(target_os = "linux")]
    fn scoped_contexts(&self) -> Vec<(bool, Option<std::path::PathBuf>)> {
        self.scoped_contexts.lock().unwrap().clone()
    }
}

struct ActiveGuard {
    probe: Arc<Probe>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.probe.active_now.fetch_sub(1, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Behaviour scripting
// ---------------------------------------------------------------------------

/// What a single mock attempt should do when executed.
#[derive(Clone)]
enum Behaviour {
    /// Succeed, echoing this answer text and (optionally) usage / native id.
    Succeed {
        text: String,
        usage: Option<Usage>,
        native_session_id: Option<String>,
    },
    /// Fail with this error kind and message.
    Fail { kind: ErrorKind, message: String },
    /// Never complete on its own — used to test cancellation/timeout, where
    /// the kernel's own `drive` layer must terminate the attempt.
    Hang,
    /// Sleep for `delay` (on the tokio clock, so `tokio::time::pause`/`advance`
    /// drives it), then behave as `then`. Used to force out-of-order broadcast
    /// completion and to exercise the real kernel timeout.
    Delay {
        delay: Duration,
        then: Box<Behaviour>,
    },
    /// Wait at a deterministic barrier until the test releases the attempt.
    Block {
        entered: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Notify>,
        then: Box<Behaviour>,
    },
}

impl Behaviour {
    fn succeed(text: &str) -> Self {
        Behaviour::Succeed {
            text: text.to_string(),
            usage: None,
            native_session_id: None,
        }
    }
    fn succeed_with_usage(text: &str, usage: Usage) -> Self {
        Behaviour::Succeed {
            text: text.to_string(),
            usage: Some(usage),
            native_session_id: None,
        }
    }
    fn succeed_harness(text: &str, native: &str) -> Self {
        Behaviour::Succeed {
            text: text.to_string(),
            usage: None,
            native_session_id: Some(native.to_string()),
        }
    }
    fn fail(kind: ErrorKind) -> Self {
        Behaviour::Fail {
            kind,
            message: format!("mock {kind:?}"),
        }
    }
    fn delayed_succeed(delay: Duration, text: &str) -> Self {
        Behaviour::Delay {
            delay,
            then: Box::new(Behaviour::succeed(text)),
        }
    }
}

// ---------------------------------------------------------------------------
// Mock ChatClient / Harness
// ---------------------------------------------------------------------------

/// Walk any leading `Delay` layers (sleeping on the tokio clock), returning the
/// terminal non-delay behaviour to enact. The caller already holds the active
/// guard across the whole attempt, so the sleep is counted as in-flight.
async fn settle(behaviour: &Behaviour) -> &Behaviour {
    let mut current = behaviour;
    loop {
        match current {
            Behaviour::Delay { delay, then } => {
                tokio::time::sleep(*delay).await;
                current = then;
            }
            Behaviour::Block {
                entered,
                release,
                then,
            } => {
                entered.wait().await;
                release.notified().await;
                current = then;
            }
            _ => return current,
        }
    }
}

struct MockChat {
    behaviour: Behaviour,
    probe: Arc<Probe>,
}

#[async_trait]
impl ChatClient for MockChat {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiChat
    }
    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome> {
        self.probe.chat_requests.lock().unwrap().push(req);
        let _guard = self.probe.enter();
        match settle(&self.behaviour).await {
            Behaviour::Succeed { text, usage, .. } => Ok(ChatOutcome {
                text: text.clone(),
                usage: *usage,
                model_echo: None,
                finish_reason: None,
            }),
            Behaviour::Fail { kind, message } => Err(VyaneError::new(*kind, message.clone())),
            Behaviour::Hang => {
                // Never resolves; the kernel's cancel/timeout path terminates it.
                pending::<()>().await;
                unreachable!("hang behaviour must be cancelled by the kernel")
            }
            Behaviour::Delay { .. } | Behaviour::Block { .. } => {
                unreachable!("settle strips all waiting layers")
            }
        }
    }
}

struct MockHarnessImpl {
    behaviour: Behaviour,
    probe: Arc<Probe>,
}

#[async_trait]
impl Harness for MockHarnessImpl {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }
    async fn available(&self) -> bool {
        true
    }
    async fn run(&self, job: HarnessJob, _cancel: CancellationToken) -> Result<HarnessOutcome> {
        self.probe.harness_jobs.lock().unwrap().push(job);
        let _guard = self.probe.enter();
        match settle(&self.behaviour).await {
            Behaviour::Succeed {
                text,
                usage,
                native_session_id,
            } => Ok(HarnessOutcome {
                text: text.clone(),
                native_session_id: native_session_id.clone(),
                usage: *usage,
                exit_code: 0,
                duration: std::time::Duration::from_millis(1),
            }),
            Behaviour::Fail { kind, message } => Err(VyaneError::new(*kind, message.clone())),
            Behaviour::Hang => {
                pending::<()>().await;
                unreachable!("hang behaviour must be cancelled by the kernel")
            }
            Behaviour::Delay { .. } | Behaviour::Block { .. } => {
                unreachable!("settle strips all waiting layers")
            }
        }
    }

    async fn run_scoped(
        &self,
        job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: CancellationToken,
    ) -> Result<HarnessOutcome> {
        self.probe.scoped_contexts.lock().unwrap().push((
            context
                .spawn_authority()
                .is_some_and(HarnessSpawnAuthority::revalidate),
            context
                .pinned_workdir()
                .map(|workdir| workdir.canonical_path().to_path_buf()),
        ));
        if let Some(reporter) = job.harness_lifecycle_reporter.as_ref() {
            reporter.report(HarnessLifecycleEvent::Started { pid: 41, pgid: 41 })?;
        }
        self.run(job, cancel).await
    }
}

// ---------------------------------------------------------------------------
// Mock ExecutorFactory — routes on transport, scripts behaviour per target
// ---------------------------------------------------------------------------

/// Maps a target key to the behaviour its attempt should exhibit. The key is
/// the target's model id, which every test makes unique per target so behaviour
/// is unambiguous.
struct MockFactory {
    behaviours: HashMap<String, Behaviour>,
    /// If a factory should refuse to build an executor for a given key (e.g. a
    /// missing harness binary), the error kind is recorded here.
    build_errors: HashMap<String, ErrorKind>,
    /// Shared instrumentation handed to every built mock (concurrency
    /// high-water mark + captured requests/jobs). Also lets a test detect
    /// whether the factory was ever called (pre-cancelled determinism).
    probe: Arc<Probe>,
    /// Count of `make` calls, to assert the factory is never touched when a
    /// dispatch is cancelled before any attempt.
    make_calls: Arc<AtomicUsize>,
    /// Audit scopes observed by `make_scoped`, in construction order.
    scopes: Arc<Mutex<Vec<AttemptScope>>>,
    /// When `make` is asked to build this model, it cancels the paired token
    /// (synchronously, before returning) and reports a `SpawnFailed` build
    /// error. This lets a test cancel *between* failover attempts with no
    /// await-point race: attempt 1 fails over, then the loop's pre-make guard
    /// catches the cancellation before attempt 2.
    cancel_on: Option<(String, CancellationToken)>,
}

impl MockFactory {
    fn new() -> Self {
        Self {
            behaviours: HashMap::new(),
            build_errors: HashMap::new(),
            probe: Probe::new(),
            make_calls: Arc::new(AtomicUsize::new(0)),
            scopes: Arc::new(Mutex::new(Vec::new())),
            cancel_on: None,
        }
    }
    fn on(mut self, model: &str, behaviour: Behaviour) -> Self {
        self.behaviours.insert(model.to_string(), behaviour);
        self
    }
    fn build_error(mut self, model: &str, kind: ErrorKind) -> Self {
        self.build_errors.insert(model.to_string(), kind);
        self
    }
    /// Cancel `token` (and abort with a `SpawnFailed` build error) when asked to
    /// build `model`. See the field docs for why this is race-free.
    fn cancel_on_make(mut self, model: &str, token: CancellationToken) -> Self {
        self.cancel_on = Some((model.to_string(), token));
        self
    }
    /// Handle to the shared probe, cloned before the factory is consumed by
    /// `into_arc`.
    fn probe(&self) -> Arc<Probe> {
        Arc::clone(&self.probe)
    }
    /// Handle to the `make` call counter.
    fn make_calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.make_calls)
    }
    fn scopes(&self) -> Arc<Mutex<Vec<AttemptScope>>> {
        Arc::clone(&self.scopes)
    }
    fn into_arc(self) -> Arc<dyn ExecutorFactory> {
        Arc::new(self)
    }
}

impl ExecutorFactory for MockFactory {
    fn capability_manifest(&self, target: &BoundTarget) -> CapabilityManifest {
        match target.transport {
            AdapterTransport::CliWrap => {
                CapabilityManifest::local_workdir_editing(IsolationStrength::AdapterDelegated)
            }
            AdapterTransport::DirectHttp => CapabilityManifest::chat_only(),
            _ => CapabilityManifest::chat_only(),
        }
    }

    fn make(&self, target: &BoundTarget) -> Result<Executor> {
        self.make_calls.fetch_add(1, Ordering::SeqCst);
        let key = target.target.model.as_str();
        if let Some((model, token)) = self.cancel_on.as_ref() {
            if model == key {
                // Cancel synchronously, then fail this attempt over: the next
                // loop iteration's pre-make guard sees the cancellation.
                token.cancel();
                return Err(VyaneError::new(
                    ErrorKind::SpawnFailed,
                    format!("mock cancel-on-make for {key}"),
                ));
            }
        }
        if let Some(kind) = self.build_errors.get(key) {
            return Err(VyaneError::new(
                *kind,
                format!("mock build error for {key}"),
            ));
        }
        let behaviour = self
            .behaviours
            .get(key)
            .cloned()
            .unwrap_or_else(|| Behaviour::succeed("default"));
        let probe = Arc::clone(&self.probe);
        match target.transport {
            AdapterTransport::DirectHttp => {
                Ok(Executor::Chat(Arc::new(MockChat { behaviour, probe })))
            }
            AdapterTransport::CliWrap => Ok(Executor::Agent(Arc::new(MockHarnessImpl {
                behaviour,
                probe,
            }))),
            // `AdapterTransport` is non-exhaustive; treat any future transport
            // as direct chat for the purposes of these mocks.
            _ => Ok(Executor::Chat(Arc::new(MockChat { behaviour, probe }))),
        }
    }

    fn make_scoped(&self, target: &BoundTarget, scope: &AttemptScope) -> Result<Executor> {
        self.scopes.lock().unwrap().push(scope.clone());
        self.make(target)
    }
}

/// A harness that makes cancellation cleanup observable. The dispatcher must
/// keep polling `run` after signalling cancellation; dropping the future would
/// leave a real harness's child process group unreaped.
struct CancellationCleanupHarness {
    started: Arc<tokio::sync::Barrier>,
    cleaned: Arc<AtomicBool>,
}

#[async_trait]
impl Harness for CancellationCleanupHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }

    async fn available(&self) -> bool {
        true
    }

    async fn run(&self, _job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        self.started.wait().await;
        cancel.cancelled().await;
        self.cleaned.store(true, Ordering::SeqCst);
        Err(VyaneError::cancelled())
    }
}

struct CancellationCleanupFactory {
    started: Arc<tokio::sync::Barrier>,
    cleaned: Arc<AtomicBool>,
}

impl ExecutorFactory for CancellationCleanupFactory {
    fn make(&self, _target: &BoundTarget) -> Result<Executor> {
        Ok(Executor::Agent(Arc::new(CancellationCleanupHarness {
            started: Arc::clone(&self.started),
            cleaned: Arc::clone(&self.cleaned),
        })))
    }
}

// ---------------------------------------------------------------------------
// Mock Ledger / SessionStore
// ---------------------------------------------------------------------------

/// Source-compatible custom stores must opt into the new execution-period
/// contract. These test stores delegate through a live object so kernel tests
/// exercise the same read/update API shape without pretending to test the
/// filesystem lock implementation (covered in `vyane-ledger`).
struct DelegatingExecutionLease<S> {
    store: S,
    owner: String,
    session_id: String,
    execution_id: String,
}

#[async_trait]
impl<S> SessionExecutionLease for DelegatingExecutionLease<S>
where
    S: SessionStore + Clone + Send + Sync + 'static,
{
    fn owner(&self) -> &str {
        &self.owner
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn execution_id(&self) -> &str {
        &self.execution_id
    }

    async fn revalidate(&self) -> Result<()> {
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<Option<SessionSnapshot>> {
        self.store
            .load_snapshot(&self.owner, &self.session_id)
            .await
    }

    async fn apply_update(
        &self,
        expected_revision: u64,
        update: &SessionUpdate,
    ) -> Result<SessionSnapshot> {
        if update.owner != self.owner || update.session_id != self.session_id {
            return Err(VyaneError::config("test lease identity mismatch"));
        }
        let record = self.store.apply_update(&self.owner, update).await?;
        let native_session = record.native_session_id.as_ref().map_or(
            NativeSessionState::Absent,
            |native_session_id| NativeSessionState::LegacyUnbound {
                native_session_id: native_session_id.clone(),
            },
        );
        Ok(SessionSnapshot {
            record,
            session_revision: expected_revision.saturating_add(1),
            native_session,
        })
    }

    async fn apply_native_transition(
        &self,
        transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot> {
        self.store
            .apply_native_transition(&self.owner, &self.session_id, transition)
            .await
    }
}

fn delegating_execution_lease<S>(
    store: S,
    owner: &str,
    session_id: &str,
    execution_id: &str,
) -> Box<dyn SessionExecutionLease>
where
    S: SessionStore + Clone + Send + Sync + 'static,
{
    Box::new(DelegatingExecutionLease {
        store,
        owner: owner.to_string(),
        session_id: session_id.to_string(),
        execution_id: execution_id.to_string(),
    })
}

#[derive(Clone, Default)]
struct MockLedger {
    records: Arc<Mutex<Vec<RunRecord>>>,
}

impl MockLedger {
    fn new() -> Self {
        Self::default()
    }
    fn records(&self) -> Vec<RunRecord> {
        self.records.lock().unwrap().clone()
    }
    fn append_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait]
impl Ledger for MockLedger {
    async fn append(&self, record: &RunRecord) -> Result<()> {
        self.records.lock().unwrap().push(record.clone());
        Ok(())
    }
    async fn query(&self, _query: RunQuery) -> Result<Vec<RunRecord>> {
        Ok(self.records.lock().unwrap().clone())
    }
}

#[derive(Clone, Default)]
struct MockSessions {
    store: Arc<Mutex<HashMap<(String, String), SessionRecord>>>,
    load_calls: Arc<AtomicUsize>,
}

impl MockSessions {
    fn new() -> Self {
        Self::default()
    }
    fn get(&self, id: &str) -> Option<SessionRecord> {
        self.get_for("local", id)
    }
    fn get_for(&self, owner: &str, id: &str) -> Option<SessionRecord> {
        self.store
            .lock()
            .unwrap()
            .get(&(owner.to_string(), id.to_string()))
            .cloned()
    }
    fn load_calls(&self) -> usize {
        self.load_calls.load(Ordering::SeqCst)
    }
    /// Pre-populate a session record (e.g. an existing transcript or native id
    /// to resume) so the dispatcher loads it as continuity context.
    fn seed(&self, record: SessionRecord) {
        self.store
            .lock()
            .unwrap()
            .insert((record.owner.clone(), record.session_id.clone()), record);
    }
}

#[async_trait]
impl SessionStore for MockSessions {
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        Ok(delegating_execution_lease(
            self.clone(),
            owner,
            session_id,
            execution_id,
        ))
    }

    async fn load(&self, owner: &str, session_id: &str) -> Result<Option<SessionRecord>> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .store
            .lock()
            .unwrap()
            .get(&(owner.to_string(), session_id.to_string()))
            .cloned())
    }
    async fn save(&self, owner: &str, record: &SessionRecord) -> Result<()> {
        assert_eq!(owner, record.owner);
        self.store.lock().unwrap().insert(
            (record.owner.clone(), record.session_id.clone()),
            record.clone(),
        );
        Ok(())
    }
    async fn apply_update(&self, owner: &str, update: &SessionUpdate) -> Result<SessionRecord> {
        assert_eq!(owner, update.owner);
        let key = (owner.to_string(), update.session_id.clone());
        let mut store = self.store.lock().unwrap();
        let record = update.apply_to(store.remove(&key));
        store.insert(key, record.clone());
        Ok(record)
    }
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .values()
            .filter(|record| record.owner == owner)
            .cloned()
            .collect())
    }
}

#[derive(Clone)]
struct BoundSnapshotSessions {
    snapshot: SessionSnapshot,
    legacy_load_calls: Arc<AtomicUsize>,
    snapshot_load_calls: Arc<AtomicUsize>,
}

impl BoundSnapshotSessions {
    fn new(session_id: &str) -> Self {
        let mut record = seed_record(session_id, None, Vec::new());
        record.target = Target {
            provider: ProviderId::new("anthropic"),
            protocol: Protocol::AnthropicMessages,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new("agent-model"),
        };
        let domain = NativeSessionDomain {
            runtime: "claude-code-v1".into(),
            harness: HarnessKind::ClaudeCode,
            provider: record.target.provider.clone(),
            protocol: record.target.protocol,
            model: record.target.model.clone(),
            endpoint_routing_digest: "1".repeat(64),
            canonical_workdir: "/workspace".into(),
            workdir_identity: WorkdirIdentity {
                device: 1,
                inode: 2,
            },
            checkpoint_namespace: "claude-code-v1".into(),
            checkpoint_schema: 1,
            account_scope_digest: "2".repeat(64),
            runtime_scope_digest: "3".repeat(64),
        };
        Self {
            snapshot: SessionSnapshot {
                record,
                session_revision: 7,
                native_session: NativeSessionState::Bound {
                    binding: Box::new(NativeSessionBinding {
                        native_session_id: "native-bound".into(),
                        domain,
                    }),
                },
            },
            legacy_load_calls: Arc::new(AtomicUsize::new(0)),
            snapshot_load_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl SessionStore for BoundSnapshotSessions {
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        Ok(delegating_execution_lease(
            self.clone(),
            owner,
            session_id,
            execution_id,
        ))
    }

    async fn load(&self, _owner: &str, _session_id: &str) -> Result<Option<SessionRecord>> {
        self.legacy_load_calls.fetch_add(1, Ordering::SeqCst);
        Err(VyaneError::unsupported(
            "legacy load must not be used for a bound snapshot",
        ))
    }

    async fn load_snapshot(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Option<SessionSnapshot>> {
        self.snapshot_load_calls.fetch_add(1, Ordering::SeqCst);
        if owner == self.snapshot.record.owner && session_id == self.snapshot.record.session_id {
            Ok(Some(self.snapshot.clone()))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, _owner: &str, _record: &SessionRecord) -> Result<()> {
        Err(VyaneError::unsupported("test store is read-only"))
    }

    async fn apply_update(&self, _owner: &str, update: &SessionUpdate) -> Result<SessionRecord> {
        Ok(update.apply_to(Some(self.snapshot.record.clone())))
    }

    async fn list(&self, _owner: &str) -> Result<Vec<SessionRecord>> {
        Ok(vec![self.snapshot.record.clone()])
    }
}

/// A ledger whose `append` always fails, to prove persistence is best-effort:
/// a completed run must still come back `Ok` even when the ledger errors.
#[derive(Clone, Default)]
struct ErroringLedger {
    append_calls: Arc<AtomicUsize>,
}

impl ErroringLedger {
    fn new() -> Self {
        Self::default()
    }
    fn append_calls(&self) -> usize {
        self.append_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Ledger for ErroringLedger {
    async fn append(&self, _record: &RunRecord) -> Result<()> {
        self.append_calls.fetch_add(1, Ordering::SeqCst);
        Err(VyaneError::new(ErrorKind::Io, "mock ledger append failure"))
    }
    async fn query(&self, _query: RunQuery) -> Result<Vec<RunRecord>> {
        Ok(Vec::new())
    }
}

/// A session store whose `save` always fails (loads succeed as empty), to prove
/// a session-persistence error also never demotes a completed run.
#[derive(Clone, Default)]
struct ErroringSessions {
    save_calls: Arc<AtomicUsize>,
}

impl ErroringSessions {
    fn new() -> Self {
        Self::default()
    }
    fn save_calls(&self) -> usize {
        self.save_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SessionStore for ErroringSessions {
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        Ok(delegating_execution_lease(
            self.clone(),
            owner,
            session_id,
            execution_id,
        ))
    }

    async fn load(&self, _owner: &str, _session_id: &str) -> Result<Option<SessionRecord>> {
        Ok(None)
    }
    async fn save(&self, _owner: &str, _record: &SessionRecord) -> Result<()> {
        self.save_calls.fetch_add(1, Ordering::SeqCst);
        Err(VyaneError::new(ErrorKind::Io, "mock session save failure"))
    }
    async fn apply_update(&self, _owner: &str, _update: &SessionUpdate) -> Result<SessionRecord> {
        self.save_calls.fetch_add(1, Ordering::SeqCst);
        Err(VyaneError::new(ErrorKind::Io, "mock session save failure"))
    }
    async fn list(&self, _owner: &str) -> Result<Vec<SessionRecord>> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Default)]
struct LoadErrorSessions;

#[async_trait]
impl SessionStore for LoadErrorSessions {
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        Ok(delegating_execution_lease(
            self.clone(),
            owner,
            session_id,
            execution_id,
        ))
    }

    async fn load(&self, _owner: &str, _session_id: &str) -> Result<Option<SessionRecord>> {
        Err(VyaneError::new(
            ErrorKind::Config,
            "session requires migration",
        ))
    }

    async fn save(&self, _owner: &str, _record: &SessionRecord) -> Result<()> {
        Ok(())
    }

    async fn apply_update(&self, _owner: &str, update: &SessionUpdate) -> Result<SessionRecord> {
        Ok(update.apply_to(None))
    }

    async fn list(&self, _owner: &str) -> Result<Vec<SessionRecord>> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct ForgedAuthoritySessions {
    forge_lease_identity: bool,
    snapshot: SessionSnapshot,
    lease_load_calls: Arc<AtomicUsize>,
}

struct ForgedAuthorityLease {
    owner: String,
    session_id: String,
    execution_id: String,
    snapshot: SessionSnapshot,
    load_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SessionExecutionLease for ForgedAuthorityLease {
    fn owner(&self) -> &str {
        &self.owner
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn execution_id(&self) -> &str {
        &self.execution_id
    }

    async fn revalidate(&self) -> Result<()> {
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<Option<SessionSnapshot>> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.snapshot.clone()))
    }

    async fn apply_update(
        &self,
        _expected_revision: u64,
        _update: &SessionUpdate,
    ) -> Result<SessionSnapshot> {
        Err(VyaneError::unsupported(
            "forged authority fixture is read-only",
        ))
    }

    async fn apply_native_transition(
        &self,
        _transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot> {
        Err(VyaneError::unsupported(
            "forged authority fixture is read-only",
        ))
    }
}

#[async_trait]
impl SessionStore for ForgedAuthoritySessions {
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        let (owner, session_id, execution_id) = if self.forge_lease_identity {
            ("foreign-owner", "foreign-session", "foreign-execution")
        } else {
            (owner, session_id, execution_id)
        };
        Ok(Box::new(ForgedAuthorityLease {
            owner: owner.to_string(),
            session_id: session_id.to_string(),
            execution_id: execution_id.to_string(),
            snapshot: self.snapshot.clone(),
            load_calls: Arc::clone(&self.lease_load_calls),
        }))
    }

    async fn load(&self, _owner: &str, _session_id: &str) -> Result<Option<SessionRecord>> {
        Err(VyaneError::unsupported(
            "forged authority fixture has no direct reads",
        ))
    }

    async fn save(&self, _owner: &str, _record: &SessionRecord) -> Result<()> {
        Err(VyaneError::unsupported(
            "forged authority fixture is read-only",
        ))
    }

    async fn apply_update(&self, _owner: &str, _update: &SessionUpdate) -> Result<SessionRecord> {
        Err(VyaneError::unsupported(
            "forged authority fixture is read-only",
        ))
    }

    async fn list(&self, _owner: &str) -> Result<Vec<SessionRecord>> {
        Ok(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// A direct-HTTP `BoundTarget` with a given provider + model.
fn http_target(provider: &str, model: &str) -> BoundTarget {
    BoundTarget {
        target: Target {
            provider: ProviderId::new(provider),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new(model),
        },
        transport: AdapterTransport::DirectHttp,
        endpoint: Some(Endpoint {
            base_url: "https://api.example.com/v1".to_string(),
            auth: None,
        }),
        params: GenParams::default(),
    }
}

/// A CLI-harness `BoundTarget` with a given provider + model.
fn cli_target(provider: &str, model: &str) -> BoundTarget {
    BoundTarget {
        target: Target {
            provider: ProviderId::new(provider),
            protocol: Protocol::AnthropicMessages,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new(model),
        },
        transport: AdapterTransport::CliWrap,
        endpoint: None,
        params: GenParams::default(),
    }
}

fn dispatcher(
    factory: Arc<dyn ExecutorFactory>,
    ledger: MockLedger,
    sessions: MockSessions,
) -> Dispatcher {
    Dispatcher::new(factory, Arc::new(ledger), Arc::new(sessions))
}

/// A `SessionRecord` for seeding continuity state: a given native id and
/// transcript, against a placeholder target.
fn seed_record(
    session_id: &str,
    native_session_id: Option<&str>,
    transcript: Vec<ChatMessage>,
) -> SessionRecord {
    let now = chrono::Utc::now();
    SessionRecord {
        session_id: session_id.to_string(),
        owner: "local".to_string(),
        target: Target {
            provider: ProviderId::new("seed-provider"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("seed-model"),
        },
        native_session_id: native_session_id.map(str::to_string),
        transcript,
        created_at: now,
        updated_at: now,
        run_count: 3,
    }
}

// ===========================================================================
// Acceptance: early execution identity + whole-chain capability admission
// ===========================================================================

#[tokio::test]
async fn direct_http_mutating_sandboxes_are_rejected_before_make() {
    for sandbox in [Sandbox::Write, Sandbox::Full] {
        let workdir = tempfile::tempdir().unwrap();
        let factory = MockFactory::new();
        let make_calls = factory.make_calls();
        let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
        let task = TaskSpec::new("edit")
            .with_sandbox(sandbox)
            .with_workdir(workdir.path());

        let error = d
            .dispatch(
                &task,
                vec![http_target("remote", "chat-only")],
                CancellationToken::new(),
            )
            .await
            .expect_err("direct HTTP must not be admitted for filesystem editing");

        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(make_calls.load(Ordering::SeqCst), 0);
        let typed = error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<CapabilityAdmissionError>())
            .expect("pre-execution rejection keeps its typed source");
        #[cfg(target_os = "linux")]
        let expected_reason = vyane_kernel::CapabilityRejectionReason::LocalEditingUnavailable;
        #[cfg(not(target_os = "linux"))]
        let expected_reason = vyane_kernel::CapabilityRejectionReason::WorkdirPinningUnavailable;
        assert_eq!(
            typed.evidence.decision,
            CapabilityAdmissionDecision::Rejected(expected_reason)
        );
    }
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn authorized_prepared_harness_dispatch_carries_live_context_and_pinned_workdir() {
    let workdir = tempfile::tempdir().unwrap();
    let canonical = std::fs::canonicalize(workdir.path()).unwrap();
    let factory = MockFactory::new().on("authorized", Behaviour::succeed("done"));
    let probe = factory.probe();
    let dispatcher = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(workdir.path());
    let prepared = dispatcher
        .prepare(&task, vec![cli_target("local", "authorized")])
        .unwrap();
    let lifecycle_seen = Arc::new(AtomicBool::new(false));
    let reporter_seen = Arc::clone(&lifecycle_seen);

    let outcome = dispatcher
        .dispatch_prepared_harness_authorized(
            &task,
            prepared,
            HarnessSpawnAuthority::new(|| true),
            HarnessLifecycleReporter::new(move |event| {
                if matches!(event, HarnessLifecycleEvent::Started { .. }) {
                    reporter_seen.store(true, Ordering::SeqCst);
                }
                Ok(())
            }),
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(outcome.output.as_deref(), Some("done"));
    assert!(lifecycle_seen.load(Ordering::SeqCst));
    assert_eq!(probe.scoped_contexts(), vec![(true, Some(canonical))]);
    let jobs = probe.harness_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].resume, None);
    assert_eq!(jobs[0].workdir.as_deref(), Some(workdir.path()));
    assert!(jobs[0].harness_lifecycle_reporter.is_some());
}

#[tokio::test]
async fn authorized_prepared_harness_dispatch_rejects_http_and_sessions_before_make() {
    let factory = MockFactory::new().on("http", Behaviour::succeed("must-not-run"));
    let make_calls = factory.make_calls();
    let sessions = MockSessions::new();
    let dispatcher = dispatcher(factory.into_arc(), MockLedger::new(), sessions.clone());
    let task = TaskSpec::new("fresh only");
    let prepared = dispatcher
        .prepare(&task, vec![http_target("remote", "http")])
        .unwrap();
    let error = dispatcher
        .dispatch_prepared_harness_authorized(
            &task,
            prepared,
            HarnessSpawnAuthority::new(|| true),
            HarnessLifecycleReporter::new(|_| Ok(())),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);

    let mixed = dispatcher
        .prepare(
            &task,
            vec![
                cli_target("local", "authorized-primary"),
                http_target("remote", "http-fallback"),
            ],
        )
        .unwrap();
    let error = dispatcher
        .dispatch_prepared_harness_authorized(
            &task,
            mixed,
            HarnessSpawnAuthority::new(|| true),
            HarnessLifecycleReporter::new(|_| Ok(())),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);

    let mut session_task = TaskSpec::new("no continuation");
    session_task.session = Some(SessionRef::new("existing"));
    let prepared = dispatcher
        .prepare(
            &session_task,
            vec![cli_target("local", "authorized-session")],
        )
        .unwrap();
    let error = dispatcher
        .dispatch_prepared_harness_authorized(
            &session_task,
            prepared,
            HarnessSpawnAuthority::new(|| true),
            HarnessLifecycleReporter::new(|_| Ok(())),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(sessions.load_calls(), 0);
}

#[tokio::test]
async fn prepared_dispatch_rejects_another_factory_before_any_side_effect() {
    let admitting_factory = MockFactory::new().on("bound", Behaviour::succeed("admitted"));
    let admitting = dispatcher(
        admitting_factory.into_arc(),
        MockLedger::new(),
        MockSessions::new(),
    );
    let executing_factory = MockFactory::new().on("bound", Behaviour::succeed("wrong"));
    let make_calls = executing_factory.make_calls();
    let probe = executing_factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let executing = dispatcher(
        executing_factory.into_arc(),
        ledger.clone(),
        sessions.clone(),
    );
    let mut task = TaskSpec::new("provenance");
    task.session = Some(SessionRef::new("session-a"));
    let prepared = admitting
        .prepare(&task, vec![http_target("remote", "bound")])
        .unwrap();

    let error = executing
        .dispatch_prepared(&task, prepared, CancellationToken::new())
        .await
        .expect_err("another dispatcher must not consume the prepared plan");

    assert_eq!(error.kind, ErrorKind::Config);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(sessions.load_calls(), 0);
    assert_eq!(ledger.append_count(), 0);
    assert!(probe.chat_requests.lock().unwrap().is_empty());
    assert!(probe.harness_jobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn prepared_stream_rejects_owner_swap_before_any_side_effect() {
    let factory = MockFactory::new().on("bound", Behaviour::succeed("wrong"));
    let make_calls = factory.make_calls();
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let admitting = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());
    let executing = admitting.clone().with_owner("other-owner");
    let task = TaskSpec::new("provenance");
    let prepared = admitting
        .prepare(&task, vec![http_target("remote", "bound")])
        .unwrap();
    let event_calls = Arc::new(AtomicUsize::new(0));
    let event_calls_for_callback = Arc::clone(&event_calls);

    let error = executing
        .dispatch_stream_prepared(&task, &prepared, CancellationToken::new(), move |_| {
            event_calls_for_callback.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect_err("an owner-swapped clone must not consume the prepared plan");

    assert_eq!(error.kind, ErrorKind::Config);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(sessions.load_calls(), 0);
    assert_eq!(ledger.append_count(), 0);
    assert_eq!(event_calls.load(Ordering::SeqCst), 0);
    assert!(probe.chat_requests.lock().unwrap().is_empty());
    assert!(probe.harness_jobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn prepared_session_validation_rejects_factory_and_owner_swap_before_load() {
    let admitting = dispatcher(
        MockFactory::new().into_arc(),
        MockLedger::new(),
        MockSessions::new(),
    );
    let executing_factory = MockFactory::new();
    let make_calls = executing_factory.make_calls();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let executing = dispatcher(
        executing_factory.into_arc(),
        ledger.clone(),
        sessions.clone(),
    )
    .with_owner("other-owner");
    let mut task = TaskSpec::new("provenance");
    task.session = Some(SessionRef::new("session-a"));
    let prepared = admitting
        .prepare(&task, vec![http_target("remote", "bound")])
        .unwrap();

    let error = executing
        .validate_session_admission(&task, &prepared)
        .await
        .expect_err("foreign provenance must fail before session lookup");

    assert_eq!(error.kind, ErrorKind::Config);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(sessions.load_calls(), 0);
    assert_eq!(ledger.append_count(), 0);
}

#[tokio::test]
async fn same_owner_dispatcher_clone_can_consume_prepared_plan() {
    let factory = MockFactory::new().on("bound", Behaviour::succeed("ok"));
    let make_calls = factory.make_calls();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let admitting = dispatcher(factory.into_arc(), ledger.clone(), sessions);
    let executing = admitting.clone();
    let task = TaskSpec::new("provenance");
    let prepared = admitting
        .prepare(&task, vec![http_target("remote", "bound")])
        .unwrap();

    let outcome = executing
        .dispatch_prepared(&task, prepared, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.output.as_deref(), Some("ok"));
    assert_eq!(make_calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.append_count(), 1);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn legacy_harness_default_fails_closed_when_given_a_pinned_context() {
    struct LegacyHarness {
        runs: Arc<AtomicUsize>,
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
            self.runs.fetch_add(1, Ordering::SeqCst);
            unreachable!("default run_scoped must reject before legacy run")
        }
    }
    struct LegacyFactory {
        runs: Arc<AtomicUsize>,
    }
    impl ExecutorFactory for LegacyFactory {
        fn capability_manifest(&self, _target: &BoundTarget) -> CapabilityManifest {
            CapabilityManifest::local_workdir_editing(IsolationStrength::AdapterDelegated)
        }
        fn make(&self, _target: &BoundTarget) -> Result<Executor> {
            Ok(Executor::Agent(Arc::new(LegacyHarness {
                runs: Arc::clone(&self.runs),
            })))
        }
    }

    let runs = Arc::new(AtomicUsize::new(0));
    let d = dispatcher(
        Arc::new(LegacyFactory {
            runs: Arc::clone(&runs),
        }),
        MockLedger::new(),
        MockSessions::new(),
    );
    let workdir = tempfile::tempdir().unwrap();
    let outcome = d
        .dispatch(
            &TaskSpec::new("edit")
                .with_sandbox(Sandbox::Write)
                .with_workdir(workdir.path()),
            vec![cli_target("legacy", "model")],
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(outcome.record.status, RunStatus::Error);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(
        outcome
            .record
            .error
            .as_deref()
            .unwrap()
            .contains("does not implement pinned scoped execution")
    );
}

#[tokio::test]
async fn read_only_direct_http_remains_compatible() {
    let factory = MockFactory::new()
        .on("chat", Behaviour::succeed("read-only answer"))
        .into_arc();
    let d = dispatcher(factory, MockLedger::new(), MockSessions::new());

    let outcome = d
        .dispatch(
            &TaskSpec::new("inspect"),
            vec![http_target("remote", "chat")],
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(outcome.record.status, RunStatus::Success);
    assert_eq!(outcome.output.as_deref(), Some("read-only answer"));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn trusted_local_cli_write_uses_the_canonical_workdir() {
    let workdir = tempfile::tempdir().unwrap();
    let canonical = std::fs::canonicalize(workdir.path()).unwrap();
    let factory = MockFactory::new().on(
        "local-editor",
        Behaviour::succeed_harness("edited", "native-1"),
    );
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger, MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(workdir.path());

    let outcome = d
        .dispatch(
            &task,
            vec![cli_target("local", "local-editor")],
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(outcome.record.status, RunStatus::Success);
    assert_eq!(outcome.record.workdir.as_deref(), canonical.to_str());
    assert_eq!(
        probe.harness_jobs()[0].workdir.as_deref(),
        Some(canonical.as_path())
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn filtered_chat_only_fallback_preserves_primary_error_semantics() {
    let workdir = tempfile::tempdir().unwrap();
    let factory = MockFactory::new().on("local-primary", Behaviour::fail(ErrorKind::RateLimited));
    let make_calls = factory.make_calls();
    let scopes = factory.scopes();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(workdir.path());

    let outcome = d
        .dispatch(
            &task,
            vec![
                cli_target("local", "local-primary"),
                http_target("remote", "filtered-fallback"),
            ],
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(make_calls.load(Ordering::SeqCst), 1);
    assert_eq!(scopes.lock().unwrap().len(), 1);
    assert_eq!(outcome.record.status, RunStatus::Error);
    assert_eq!(outcome.record.attempts.len(), 1);
    match &outcome.record.attempts[0].outcome {
        AttemptOutcome::Err {
            kind,
            message,
            failed_over,
        } => {
            assert_eq!(*kind, ErrorKind::RateLimited);
            assert!(message.contains("RateLimited"));
            assert!(
                !failed_over,
                "filtered fallback is not an attempted failover"
            );
        }
        AttemptOutcome::Ok => panic!("primary should fail"),
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn scoped_make_sees_final_execution_id_and_original_chain_ordinals() {
    let workdir = tempfile::tempdir().unwrap();
    let factory = MockFactory::new()
        .on("first", Behaviour::fail(ErrorKind::RateLimited))
        .on("third", Behaviour::succeed_harness("winner", "native-3"));
    let scopes = factory.scopes();
    let make_calls = factory.make_calls();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(workdir.path());

    let outcome = d
        .dispatch(
            &task,
            vec![
                cli_target("local", "first"),
                http_target("remote", "filtered"),
                cli_target("local", "third"),
            ],
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(make_calls.load(Ordering::SeqCst), 2);
    let scopes = scopes.lock().unwrap();
    assert_eq!(
        scopes
            .iter()
            .map(AttemptScope::original_chain_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert!(
        scopes
            .iter()
            .all(|scope| scope.execution.execution_id == outcome.record.run_id)
    );
    assert_eq!(outcome.record.status, RunStatus::Success);
    match &outcome.record.attempts[0].outcome {
        AttemptOutcome::Err { failed_over, .. } => assert!(*failed_over),
        AttemptOutcome::Ok => panic!("first target should fail over"),
    }
}

#[test]
fn capability_evidence_is_serializable_audit_data_without_authority_fields() {
    let manifest = CapabilityManifest::local_workdir_editing(IsolationStrength::AdapterDelegated);
    let evidence = CapabilityAdmissionEvidence {
        execution_id: "01900000-0000-7000-8000-000000000000".to_string(),
        original_chain_ordinal: 7,
        target: cli_target("local", "editor").target,
        requested_sandbox: Sandbox::Write,
        canonical_workdir: Some(std::path::PathBuf::from("/workspace")),
        workdir_identity: None,
        manifest: manifest.clone(),
        decision: CapabilityAdmissionDecision::Admitted,
    };

    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let evidence_json = serde_json::to_string(&evidence).unwrap();
    assert_eq!(
        manifest.filesystem,
        FilesystemCapability::CallerWorkdirEditing
    );
    for json in [manifest_json, evidence_json] {
        assert!(!json.contains("token"));
        assert!(!json.contains("authority"));
        assert!(!json.contains("credential"));
    }
}

#[tokio::test]
async fn streaming_mutating_direct_target_is_gated_before_make() {
    let workdir = tempfile::tempdir().unwrap();
    let factory = MockFactory::new();
    let make_calls = factory.make_calls();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(workdir.path());

    let error = d
        .dispatch_stream(
            &task,
            &http_target("remote", "chat-only"),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .expect_err("stream dispatch must capability-gate before make");

    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn prepared_stream_fallback_reuses_one_factory_observed_execution_id() {
    let factory = MockFactory::new().on("chat", Behaviour::succeed("fallback answer"));
    let scopes = factory.scopes();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("inspect");
    let prepared = d
        .prepare(&task, vec![http_target("remote", "chat")])
        .unwrap();
    let execution_id = prepared.execution_id().to_string();

    let streamed = d
        .dispatch_stream_prepared(&task, &prepared, CancellationToken::new(), |_| {})
        .await
        .unwrap();
    assert!(streamed.is_none(), "mock client declines streaming");

    let outcome = d
        .dispatch_prepared(&task, prepared, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.record.run_id, execution_id);
    assert_eq!(outcome.output.as_deref(), Some("fallback answer"));
    let scopes = scopes.lock().unwrap();
    assert_eq!(scopes.len(), 2, "stream probe and fallback each build once");
    assert!(
        scopes
            .iter()
            .all(|scope| scope.execution.execution_id == execution_id)
    );
}

#[tokio::test]
async fn compatibility_stream_api_returns_none_without_exposing_scope() {
    let factory = MockFactory::new().on("chat", Behaviour::succeed("compat fallback"));
    let scopes = factory.scopes();
    let make_calls = factory.make_calls();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());
    let task = TaskSpec::new("inspect");

    let outcome = d
        .dispatch_stream(
            &task,
            &http_target("remote", "chat"),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();

    assert!(outcome.is_none());
    let scopes = scopes.lock().unwrap();
    assert!(scopes.is_empty(), "legacy probe must not expose its scope");
    assert_eq!(make_calls.load(Ordering::SeqCst), 1);
    assert!(ledger.records().is_empty());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn mutating_legacy_native_resume_is_rejected_before_make() {
    let sessions = MockSessions::new();
    sessions.seed(seed_record("legacy-native", Some("native-old"), Vec::new()));
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("must not run", "native-new"),
    );
    let make_calls = factory.make_calls();
    let d = Dispatcher::new(
        factory.into_arc(),
        Arc::new(MockLedger::new()),
        Arc::new(sessions),
    );

    for (workdir, target) in [
        (
            tempfile::tempdir().unwrap(),
            cli_target("anthropic", "agent-model"),
        ),
        (
            tempfile::tempdir().unwrap(),
            cli_target("another-provider", "another-model"),
        ),
    ] {
        let mut task = TaskSpec::new("edit")
            .with_sandbox(Sandbox::Write)
            .with_workdir(workdir.path());
        task.session = Some(SessionRef::new("legacy-native"));
        let error = d
            .dispatch(&task, vec![target], CancellationToken::new())
            .await
            .expect_err("legacy native resume must fail closed for every workdir");
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert!(error.message.contains("NativeSessionDomain"));
    }
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn domain_bound_native_resume_stays_disabled_before_make() {
    let sessions = BoundSnapshotSessions::new("bound-native");
    let legacy_load_calls = Arc::clone(&sessions.legacy_load_calls);
    let snapshot_load_calls = Arc::clone(&sessions.snapshot_load_calls);
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("must not run", "native-new"),
    );
    let make_calls = factory.make_calls();
    let d = Dispatcher::new(
        factory.into_arc(),
        Arc::new(MockLedger::new()),
        Arc::new(sessions),
    );
    let mut task = TaskSpec::new("continue safely");
    task.session = Some(SessionRef::new("bound-native"));

    let error = d
        .dispatch(
            &task,
            vec![cli_target("anthropic", "agent-model")],
            CancellationToken::new(),
        )
        .await
        .expect_err("binding alone must not enable native resume");

    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert!(error.message.contains("active execution permit"));
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(legacy_load_calls.load(Ordering::SeqCst), 0);
    assert_eq!(snapshot_load_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_session_store_defaults_are_source_compatible_and_fail_closed() {
    let sessions = MockSessions::new();
    sessions.seed(seed_record(
        "legacy-default",
        Some("native-old"),
        Vec::new(),
    ));
    let snapshot = sessions
        .load_snapshot("local", "legacy-default")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.session_revision, 0);
    assert!(matches!(
        snapshot.native_session,
        NativeSessionState::LegacyUnbound { native_session_id }
            if native_session_id == "native-old"
    ));
    let listed = sessions.list_snapshots("local").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_revision, 0);
    assert!(matches!(
        &listed[0].native_session,
        NativeSessionState::LegacyUnbound { native_session_id }
            if native_session_id == "native-old"
    ));
    let error = sessions
        .apply_native_transition(
            "local",
            "legacy-default",
            &vyane_core::NativeSessionTransition::Reset {
                expected_revision: 0,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unsupported);

    #[derive(Clone)]
    struct LegacyOnlyStore(MockSessions);

    #[async_trait]
    impl SessionStore for LegacyOnlyStore {
        async fn load(&self, owner: &str, session_id: &str) -> Result<Option<SessionRecord>> {
            self.0.load(owner, session_id).await
        }

        async fn save(&self, owner: &str, record: &SessionRecord) -> Result<()> {
            self.0.save(owner, record).await
        }

        async fn apply_update(&self, owner: &str, update: &SessionUpdate) -> Result<SessionRecord> {
            self.0.apply_update(owner, update).await
        }

        async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>> {
            self.0.list(owner).await
        }
    }

    let legacy = LegacyOnlyStore(sessions);
    let lease_error = legacy
        .acquire_execution_lease("local", "legacy-default", "execution")
        .await
        .err()
        .expect("legacy store must not fabricate execution authority");
    assert_eq!(lease_error.kind, ErrorKind::Unsupported);
}

#[cfg(target_os = "linux")]
#[test]
fn frozen_capability_snapshot_detects_replaced_workdir_identity() {
    let root = tempfile::tempdir().unwrap();
    let requested = root.path().join("work");
    let admitted = root.path().join("admitted-work");
    std::fs::create_dir(&requested).unwrap();
    let factory = MockFactory::new();
    let make_calls = factory.make_calls();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(&requested);

    let parent = d
        .prepare(&task, vec![cli_target("local", "editor")])
        .unwrap();
    let frozen = parent.capability_snapshot().clone();
    std::fs::rename(&requested, &admitted).unwrap();
    std::fs::create_dir(&requested).unwrap();

    let worker = d
        .prepare(&task, vec![cli_target("local", "editor")])
        .unwrap();
    assert_ne!(
        frozen.workdir_identity,
        worker.capability_snapshot().workdir_identity
    );
    let error = worker
        .verify_capability_snapshot(&frozen)
        .expect_err("replacement directory must invalidate the frozen plan");
    assert_eq!(error.kind, ErrorKind::Config);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
}

#[cfg(target_os = "linux")]
#[test]
fn inherited_pin_revalidates_frozen_plan_without_reopening_replaced_path() {
    let root = tempfile::tempdir().unwrap();
    let requested = root.path().join("work");
    let moved = root.path().join("moved-work");
    std::fs::create_dir(&requested).unwrap();
    let factory = MockFactory::new();
    let make_calls = factory.make_calls();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), MockSessions::new());
    let task = TaskSpec::new("edit")
        .with_sandbox(Sandbox::Write)
        .with_workdir(&requested);
    let parent = d
        .prepare(&task, vec![cli_target("local", "editor")])
        .unwrap();
    let frozen = parent.capability_snapshot().clone();
    let parent_pin = parent.pinned_workdir().unwrap();
    let inherited = vyane_core::PinnedWorkdir::from_open_file(
        parent_pin.canonical_path().to_path_buf(),
        parent_pin.handle().try_clone().unwrap(),
        parent_pin.identity(),
    )
    .unwrap();

    std::fs::rename(&requested, &moved).unwrap();
    std::fs::create_dir(&requested).unwrap();
    let worker = d
        .prepare_with_pinned_workdir(&task, vec![cli_target("local", "editor")], inherited)
        .unwrap();
    worker.verify_capability_snapshot(&frozen).unwrap();
    assert_eq!(
        worker.capability_snapshot().workdir_identity,
        frozen.workdir_identity
    );
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
}

// ===========================================================================
// Acceptance: failover gating — mirror the eligibility table
// ===========================================================================

#[tokio::test]
async fn rate_limited_first_target_fails_over_to_second() {
    let factory = MockFactory::new()
        .on("model-a", Behaviour::fail(ErrorKind::RateLimited))
        .on("model-b", Behaviour::succeed("from b"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("hi");
    let chain = vec![
        http_target("prov-a", "model-a"),
        http_target("prov-b", "model-b"),
    ];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Success);
    assert_eq!(rec.record.attempts.len(), 2, "both targets attempted");
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            kind: ErrorKind::RateLimited,
            ..
        }
    ));
    assert!(matches!(rec.record.attempts[1].outcome, AttemptOutcome::Ok));
    assert_eq!(rec.record.target.model.as_str(), "model-b");
    assert_eq!(rec.output.as_deref(), Some("from b"));
    assert_eq!(ledger.append_count(), 1);
}

#[tokio::test]
async fn config_first_target_aborts_without_second_attempt() {
    let factory = MockFactory::new()
        .on("model-a", Behaviour::fail(ErrorKind::Config))
        .on("model-b", Behaviour::succeed("should never run"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("hi");
    let chain = vec![
        http_target("prov-a", "model-a"),
        http_target("prov-b", "model-b"),
    ];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Error);
    assert_eq!(
        rec.record.attempts.len(),
        1,
        "config error must not fail over"
    );
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: false,
            kind: ErrorKind::Config,
            ..
        }
    ));
    assert_eq!(rec.record.target.model.as_str(), "model-a");
    assert_eq!(rec.output, None);
    assert_eq!(ledger.append_count(), 1);
}

#[tokio::test]
async fn cancelled_first_target_aborts_without_second_attempt() {
    // A `Cancelled` classified error on the first target must abort even
    // though a second target remains (Cancelled is not failover-eligible).
    let factory = MockFactory::new()
        .on("model-a", Behaviour::fail(ErrorKind::Cancelled))
        .on("model-b", Behaviour::succeed("should never run"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("hi");
    let chain = vec![
        http_target("prov-a", "model-a"),
        http_target("prov-b", "model-b"),
    ];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Cancelled);
    assert_eq!(rec.record.attempts.len(), 1);
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: false,
            kind: ErrorKind::Cancelled,
            ..
        }
    ));
    assert_eq!(ledger.append_count(), 1);
}

/// Exhaustively mirror `ErrorKind::failover_eligible`: for a two-target chain,
/// a first-target failure of each kind either fails over (2 attempts, success)
/// or aborts (1 attempt), exactly as the core table dictates.
#[tokio::test]
async fn failover_table_is_mirrored_for_every_error_kind() {
    let all_kinds = [
        ErrorKind::Config,
        ErrorKind::Auth,
        ErrorKind::RateLimited,
        ErrorKind::Timeout,
        ErrorKind::Transport,
        ErrorKind::Protocol,
        ErrorKind::SpawnFailed,
        ErrorKind::HarnessFailed,
        ErrorKind::Cancelled,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::Conflict,
        ErrorKind::Io,
        ErrorKind::Indeterminate,
        ErrorKind::Other,
    ];

    for kind in all_kinds {
        let factory = MockFactory::new()
            .on("first", Behaviour::fail(kind))
            .on("second", Behaviour::succeed("recovered"))
            .into_arc();
        let ledger = MockLedger::new();
        let d = dispatcher(factory, ledger.clone(), MockSessions::new());

        let task = TaskSpec::new("hi");
        let chain = vec![http_target("p1", "first"), http_target("p2", "second")];
        let rec = d
            .dispatch(&task, chain, CancellationToken::new())
            .await
            .unwrap();

        // The kernel must defer to core, never re-derive: compare against the
        // authoritative predicate directly.
        if kind.failover_eligible() {
            assert_eq!(rec.record.attempts.len(), 2, "{kind:?} should fail over");
            assert_eq!(
                rec.record.status,
                RunStatus::Success,
                "{kind:?} recovers on second"
            );
            assert!(
                matches!(
                    rec.record.attempts[0].outcome,
                    AttemptOutcome::Err {
                        failed_over: true,
                        ..
                    }
                ),
                "{kind:?} first attempt should be marked failed_over"
            );
        } else {
            assert_eq!(rec.record.attempts.len(), 1, "{kind:?} should abort");
            assert_ne!(
                rec.record.status,
                RunStatus::Success,
                "{kind:?} must not reach second"
            );
            assert!(
                matches!(
                    rec.record.attempts[0].outcome,
                    AttemptOutcome::Err {
                        failed_over: false,
                        ..
                    }
                ),
                "{kind:?} first attempt should not be marked failed_over"
            );
        }
        assert_eq!(ledger.append_count(), 1, "{kind:?}: exactly one record");
    }
}

// ===========================================================================
// Acceptance: model non-leakage across providers
// ===========================================================================

#[tokio::test]
async fn model_ids_never_leak_across_provider_boundary() {
    // First target fails over so BOTH attempts are recorded, letting us check
    // each attempt pins its own (provider, model) pair.
    let factory = MockFactory::new()
        .on("model-alpha", Behaviour::fail(ErrorKind::Transport))
        .on("model-beta", Behaviour::succeed("ok"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("hi");
    let chain = vec![
        http_target("provider-one", "model-alpha"),
        http_target("provider-two", "model-beta"),
    ];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.attempts.len(), 2);
    // Each attempt's model is paired only with its own provider — no id crossed
    // the boundary.
    assert_eq!(
        rec.record.attempts[0].target.provider.as_str(),
        "provider-one"
    );
    assert_eq!(rec.record.attempts[0].target.model.as_str(), "model-alpha");
    assert_eq!(
        rec.record.attempts[1].target.provider.as_str(),
        "provider-two"
    );
    assert_eq!(rec.record.attempts[1].target.model.as_str(), "model-beta");
    // The specific wrong pairings must never appear.
    for a in &rec.record.attempts {
        let paired_alpha_with_two = a.target.provider.as_str() == "provider-two"
            && a.target.model.as_str() == "model-alpha";
        let paired_beta_with_one =
            a.target.provider.as_str() == "provider-one" && a.target.model.as_str() == "model-beta";
        assert!(
            !paired_alpha_with_two && !paired_beta_with_one,
            "model id crossed provider boundary"
        );
    }
}

// ===========================================================================
// Acceptance: attempt-trail completeness
// ===========================================================================

#[tokio::test]
async fn fail_over_then_succeed_records_full_ordered_trail() {
    let factory = MockFactory::new()
        .on("m1", Behaviour::fail(ErrorKind::Auth))
        .on("m2", Behaviour::succeed("second wins"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("hi");
    let first = http_target("p1", "m1");
    let second = http_target("p2", "m2");
    let rec = d
        .dispatch(
            &task,
            vec![first.clone(), second.clone()],
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(rec.record.attempts.len(), 2);
    // Order preserved: first the failing attempt, then the succeeding one.
    assert_eq!(rec.record.attempts[0].target.model.as_str(), "m1");
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            ..
        }
    ));
    assert_eq!(rec.record.attempts[1].target.model.as_str(), "m2");
    assert!(matches!(rec.record.attempts[1].outcome, AttemptOutcome::Ok));
    // The record's headline target is the last (successful) attempt's.
    assert_eq!(rec.record.target, second.target);
    assert_eq!(rec.record.transport, second.transport);
    assert_eq!(rec.record.status, RunStatus::Success);
}

// ===========================================================================
// Acceptance: broadcast ordering + partial failure
// ===========================================================================

#[tokio::test]
async fn broadcast_preserves_input_order_with_partial_failure() {
    // Five chains: even indices succeed, odd indices fail (non-eligible Config
    // so they terminate as Error without recovery). Different models per chain
    // let us assert the returned order matches the input order exactly.
    let factory = MockFactory::new()
        .on("chain0", Behaviour::succeed("r0"))
        .on("chain1", Behaviour::fail(ErrorKind::Config))
        .on("chain2", Behaviour::succeed("r2"))
        .on("chain3", Behaviour::fail(ErrorKind::Config))
        .on("chain4", Behaviour::succeed("r4"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("fan out");
    let chains = vec![
        vec![http_target("p", "chain0")],
        vec![http_target("p", "chain1")],
        vec![http_target("p", "chain2")],
        vec![http_target("p", "chain3")],
        vec![http_target("p", "chain4")],
    ];
    let results = d.broadcast(&task, chains, CancellationToken::new()).await;

    assert_eq!(results.len(), 5);
    let expected_ok = [true, false, true, false, true];
    for (i, res) in results.iter().enumerate() {
        let rec = res.as_ref().expect("each chain produces its own RunRecord");
        // Position i must correspond to input chain i (its unique model).
        assert_eq!(
            rec.record.target.model.as_str(),
            format!("chain{i}"),
            "order preserved at {i}"
        );
        let expected_status = if expected_ok[i] {
            RunStatus::Success
        } else {
            RunStatus::Error
        };
        assert_eq!(rec.record.status, expected_status, "chain {i} status");
        let expected_output = expected_ok[i].then(|| format!("r{i}"));
        assert_eq!(rec.output, expected_output, "chain {i} output");
    }
    // Each chain wrote exactly one record.
    assert_eq!(ledger.append_count(), 5);
    // Every produced record has a distinct run id.
    let recs = ledger.records();
    let mut ids: Vec<_> = recs.iter().map(|r| r.run_id.clone()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 5, "run ids are unique per record");
}

#[tokio::test]
async fn broadcast_bounded_concurrency_still_orders_and_completes_all() {
    use std::num::NonZeroUsize;
    // Many chains through a width-1 semaphore: fully serialized, yet the
    // results must still be positionally aligned with the input.
    let mut factory = MockFactory::new();
    let n = 12;
    for i in 0..n {
        factory = factory.on(&format!("c{i}"), Behaviour::succeed(&format!("r{i}")));
    }
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("serial fan out");
    let chains: Vec<_> = (0..n)
        .map(|i| vec![http_target("p", &format!("c{i}"))])
        .collect();
    let results = d
        .broadcast_with_concurrency(
            &task,
            chains,
            CancellationToken::new(),
            NonZeroUsize::new(1).unwrap(),
        )
        .await;

    assert_eq!(results.len(), n);
    for (i, res) in results.iter().enumerate() {
        let rec = res.as_ref().unwrap();
        assert_eq!(rec.record.target.model.as_str(), format!("c{i}"));
        assert_eq!(rec.record.status, RunStatus::Success);
    }
    assert_eq!(ledger.append_count(), n);
}

// ===========================================================================
// Acceptance: cancellation
// ===========================================================================

#[tokio::test]
async fn cancelling_mid_attempt_yields_cancelled_and_still_appends() {
    // The single target hangs forever; the kernel's cancellation path must
    // terminate the attempt, mark the run Cancelled, and still append.
    let factory = MockFactory::new()
        .on("hang-model", Behaviour::Hang)
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let cancel = CancellationToken::new();
    let task = TaskSpec::new("will be cancelled");
    let chain = vec![http_target("p", "hang-model")];

    let cancel_child = cancel.clone();
    let handle = tokio::spawn(async move { d.dispatch(&task, chain, cancel_child).await });

    // Yield so the dispatch task begins the attempt, then cancel. Whether the
    // cancel lands before or after the select is polled, the outcome is the
    // same (the kernel checks is_cancelled() up front and races the token).
    tokio::task::yield_now().await;
    cancel.cancel();

    let rec = handle.await.unwrap().unwrap();
    assert_eq!(rec.record.status, RunStatus::Cancelled);
    assert_eq!(rec.record.attempts.len(), 1);
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Cancelled,
            failed_over: false,
            ..
        }
    ));
    assert_eq!(ledger.append_count(), 1, "cancelled run is still recorded");
}

#[tokio::test]
async fn harness_cancellation_waits_for_process_owner_cleanup() {
    let started = Arc::new(tokio::sync::Barrier::new(2));
    let cleaned = Arc::new(AtomicBool::new(false));
    let factory: Arc<dyn ExecutorFactory> = Arc::new(CancellationCleanupFactory {
        started: Arc::clone(&started),
        cleaned: Arc::clone(&cleaned),
    });
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());
    let cancel = CancellationToken::new();
    let child_cancel = cancel.clone();

    let handle = tokio::spawn(async move {
        d.dispatch(
            &TaskSpec::new("cancel harness"),
            vec![cli_target("anthropic", "cleanup-aware")],
            child_cancel,
        )
        .await
    });

    // Do not cancel until Harness::run owns the synthetic process lifecycle.
    started.wait().await;
    cancel.cancel();

    let outcome = handle.await.unwrap().unwrap();
    assert_eq!(outcome.record.status, RunStatus::Cancelled);
    assert!(
        cleaned.load(Ordering::SeqCst),
        "dispatcher returned before the harness completed cancellation cleanup"
    );
    assert_eq!(ledger.append_count(), 1);
}

#[tokio::test]
async fn already_cancelled_token_produces_cancelled_run() {
    // Pre-cancelled token: the attempt must not even reach a would-succeed
    // executor; the run is Cancelled and recorded.
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed("should not be returned"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let cancel = CancellationToken::new();
    cancel.cancel();
    let task = TaskSpec::new("pre-cancelled");
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], cancel)
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Cancelled);
    assert_eq!(ledger.append_count(), 1);
}

// ===========================================================================
// Acceptance: ledger-write-on-failure
// ===========================================================================

#[tokio::test]
async fn all_targets_fail_still_appends_exactly_one_error_record() {
    // Every target in the chain fails with an eligible kind, so the kernel
    // exhausts the chain; the terminal status is Error and exactly one record
    // is appended.
    let factory = MockFactory::new()
        .on("m1", Behaviour::fail(ErrorKind::Transport))
        .on("m2", Behaviour::fail(ErrorKind::RateLimited))
        .on("m3", Behaviour::fail(ErrorKind::Protocol))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("all fail");
    let chain = vec![
        http_target("p1", "m1"),
        http_target("p2", "m2"),
        http_target("p3", "m3"),
    ];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Error);
    assert_eq!(rec.record.attempts.len(), 3, "every target attempted");
    // The first two failed over; the last did not (chain exhausted).
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            ..
        }
    ));
    assert!(matches!(
        rec.record.attempts[1].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            ..
        }
    ));
    assert!(matches!(
        rec.record.attempts[2].outcome,
        AttemptOutcome::Err {
            failed_over: false,
            ..
        }
    ));
    // Headline target is the last attempted one.
    assert_eq!(rec.record.target.model.as_str(), "m3");
    assert!(rec.record.error.is_some(), "terminal error message present");
    assert_eq!(
        ledger.append_count(),
        1,
        "exactly one ledger append on total failure"
    );
}

// ===========================================================================
// Supporting behaviour beyond the bullet list (record assembly correctness)
// ===========================================================================

#[tokio::test]
async fn successful_run_records_digest_usage_and_appends() {
    let usage = Usage {
        input_tokens: 10,
        output_tokens: 4,
        ..Default::default()
    };
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed_with_usage("hello there", usage))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("greet");
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Success);
    // Digest is SHA-256("greet") first 16 hex chars — deterministic, not body.
    assert_eq!(rec.record.task_digest, vyane_kernel::task_digest("greet"));
    assert_ne!(rec.record.task_digest, "greet");
    assert_eq!(rec.record.task_digest.len(), 16);
    assert_eq!(rec.record.usage, Some(usage));
    assert_eq!(
        rec.record.output_chars,
        Some("hello there".chars().count() as u64)
    );
    assert_eq!(rec.output.as_deref(), Some("hello there"));
    assert_eq!(rec.record.owner, "local");
    assert_eq!(rec.record.attempts.len(), 1);
    assert_eq!(ledger.append_count(), 1, "success is recorded too");
}

#[tokio::test]
async fn winning_attempt_usage_is_folded_in_via_usage_add() {
    // Within the frozen trait interface a *failed* attempt cannot report usage
    // (the `Err` arm carries no `ChatOutcome`/`HarnessOutcome`), so in a single
    // dispatch only the winning attempt contributes usage. The kernel still
    // routes that through `Usage::add` into a fresh accumulator, which this
    // asserts field-by-field: reasoning/cached stay `None` (not zeroed), and
    // the base counters match exactly — the behaviour `Usage::add` guarantees.
    let u2 = Usage {
        input_tokens: 5,
        output_tokens: 2,
        ..Default::default()
    };
    let factory = MockFactory::new()
        .on("m1", Behaviour::fail(ErrorKind::Timeout))
        .on("m2", Behaviour::succeed_with_usage("done", u2))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("agg");
    let chain = vec![http_target("p1", "m1"), http_target("p2", "m2")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    let got = rec.record.usage.expect("winning attempt usage present");
    assert_eq!(got.input_tokens, 5);
    assert_eq!(got.output_tokens, 2);
    assert_eq!(got.reasoning_tokens, None);
    assert_eq!(got.cached_input_tokens, None);
}

#[tokio::test]
async fn attempt_without_reported_usage_leaves_record_usage_none() {
    // A success that reports no usage must leave `RunRecord.usage` as None,
    // never a zeroed `Usage` — the accumulator is only created on first report.
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed("no usage"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("x");
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rec.record.usage, None);
}

#[tokio::test]
async fn harness_run_updates_session_native_id_and_run_count() {
    let factory = MockFactory::new()
        .on(
            "agent-model",
            Behaviour::succeed_harness("agent answer", "native-xyz"),
        )
        .into_arc();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let d = dispatcher(factory, ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("do work");
    task.session = Some(vyane_core::SessionRef::new("sess-1"));
    let chain = vec![cli_target("anthropic", "agent-model")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Success);
    assert_eq!(rec.record.transport, AdapterTransport::CliWrap);
    assert_eq!(rec.record.session_id.as_deref(), Some("sess-1"));
    assert_eq!(rec.output.as_deref(), Some("agent answer"));

    // Session was created and updated with the harness native id + run_count.
    let saved = sessions.get("sess-1").expect("session persisted");
    assert_eq!(saved.native_session_id.as_deref(), Some("native-xyz"));
    assert_eq!(saved.run_count, 1);
    assert_eq!(saved.owner, "local");
}

#[tokio::test]
async fn session_run_count_increments_across_runs() {
    let factory = MockFactory::new()
        .on("chat-model", Behaviour::succeed("a"))
        .into_arc();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let d = dispatcher(factory, ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("work");
    task.session = Some(vyane_core::SessionRef::new("sess-run"));
    let chain = || vec![http_target("openai", "chat-model")];

    d.dispatch(&task, chain(), CancellationToken::new())
        .await
        .unwrap();
    d.dispatch(&task, chain(), CancellationToken::new())
        .await
        .unwrap();

    let saved = sessions.get("sess-run").unwrap();
    assert_eq!(saved.run_count, 2, "run_count bumps each run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_session_lease_spans_model_execution_and_commit() {
    let directory = tempfile::tempdir().unwrap();
    let sessions = Arc::new(FsSessionStore::new(directory.path().join("sessions")));
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Notify::new());
    let factory = MockFactory::new().on(
        "slow-model",
        Behaviour::Block {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            then: Box::new(Behaviour::succeed("serialized answer")),
        },
    );
    let make_calls = factory.make_calls();
    let dispatcher = Dispatcher::new(
        factory.into_arc(),
        Arc::new(MockLedger::new()),
        sessions.clone(),
    );
    let mut task = TaskSpec::new("continue once");
    task.session = Some(SessionRef::new("serialized"));

    let first_dispatcher = dispatcher.clone();
    let first_task = task.clone();
    let first = tokio::spawn(async move {
        first_dispatcher
            .dispatch(
                &first_task,
                vec![http_target("provider", "slow-model")],
                CancellationToken::new(),
            )
            .await
    });
    entered.wait().await;

    let conflict = dispatcher
        .dispatch(
            &task,
            vec![http_target("provider", "slow-model")],
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.kind, ErrorKind::Conflict);
    assert_eq!(
        make_calls.load(Ordering::SeqCst),
        1,
        "the competing execution must fail before factory construction"
    );

    release.notify_waiters();
    let outcome = first.await.unwrap().unwrap();
    assert_eq!(outcome.record.status, RunStatus::Success);

    let post = sessions
        .acquire_execution_lease("local", "serialized", "post-check")
        .await
        .unwrap();
    let snapshot = post.load_snapshot().await.unwrap().unwrap();
    assert_eq!(snapshot.record.run_count, 1);
    assert_eq!(snapshot.record.transcript.len(), 2);
}

#[tokio::test]
async fn no_session_ref_means_no_session_write() {
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed("x"))
        .into_arc();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let d = dispatcher(factory, ledger.clone(), sessions.clone());

    let task = TaskSpec::new("no session"); // session = None
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.session_id, None);
    assert!(
        sessions.list("local").await.unwrap().is_empty(),
        "no session record written"
    );
}

#[tokio::test]
async fn factory_build_error_is_treated_as_failover_eligible_attempt() {
    // A factory that cannot build the first executor (e.g. missing harness
    // binary → SpawnFailed) must be recorded as a failed attempt and, being
    // eligible, fail over to the next target.
    let factory = MockFactory::new()
        .build_error("m1", ErrorKind::SpawnFailed)
        .on("m2", Behaviour::succeed("recovered"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("hi");
    let chain = vec![cli_target("p1", "m1"), http_target("p2", "m2")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.attempts.len(), 2);
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::SpawnFailed,
            failed_over: true,
            ..
        }
    ));
    assert_eq!(rec.record.status, RunStatus::Success);
    assert_eq!(rec.record.target.model.as_str(), "m2");
}

#[tokio::test]
async fn labels_are_copied_but_prompt_preview_is_omitted() {
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed("ok"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let mut task = TaskSpec::new("a rather long prompt body for preview checking");
    task.labels.insert("ticket".into(), "ISSUE-4".into());
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(
        rec.record.labels.get("ticket").map(String::as_str),
        Some("ISSUE-4")
    );
    assert!(rec.record.task_preview.is_none());
}

#[tokio::test]
async fn empty_chain_is_a_kernel_error_not_a_record() {
    let factory = MockFactory::new().into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("nothing to run");
    let err = d
        .dispatch(&task, vec![], CancellationToken::new())
        .await
        .expect_err("empty chain must be an error, not a fabricated record");
    assert_eq!(err.kind, ErrorKind::Config);
    assert_eq!(
        ledger.append_count(),
        0,
        "no record for an unrunnable dispatch"
    );
}

// ===========================================================================
// Finding 5: strengthened concurrency — delayed mocks, out-of-order
// completion, and a real semaphore high-water probe
// ===========================================================================

#[tokio::test(start_paused = true)]
async fn broadcast_returns_input_order_despite_out_of_order_completion() {
    // Chains finish in the *reverse* of input order: chain 0 is the slowest,
    // chain 4 the fastest. If the kernel returned completion order the result
    // would be reversed; input-order alignment is proven by each position's
    // model matching its input index even though later chains finished first.
    let n = 5usize;
    let mut factory = MockFactory::new();
    for i in 0..n {
        // Later indices get shorter delays → they complete earlier.
        let delay = Duration::from_millis(((n - i) * 100) as u64);
        factory = factory.on(
            &format!("c{i}"),
            Behaviour::delayed_succeed(delay, &format!("r{i}")),
        );
    }
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("fan out, out of order");
    let chains: Vec<_> = (0..n)
        .map(|i| vec![http_target("p", &format!("c{i}"))])
        .collect();

    // With the clock paused, drive the whole broadcast to completion by
    // auto-advancing time as the sleeps register.
    let results = d.broadcast(&task, chains, CancellationToken::new()).await;

    assert_eq!(results.len(), n);
    for (i, res) in results.iter().enumerate() {
        let rec = res.as_ref().expect("each chain produced a record");
        assert_eq!(
            rec.record.target.model.as_str(),
            format!("c{i}"),
            "position {i} must map to input chain {i} regardless of finish order"
        );
        assert_eq!(rec.record.status, RunStatus::Success);
    }
}

#[tokio::test(start_paused = true)]
async fn broadcast_semaphore_bounds_active_dispatches() {
    use std::num::NonZeroUsize;
    // 10 chains, each sleeping, through a width-3 semaphore. The shared probe's
    // high-water mark proves no more than 3 attempts were ever in flight at
    // once — the semaphore genuinely bounds concurrency, it isn't just a label.
    let n = 10usize;
    let width = 3usize;
    let mut factory = MockFactory::new();
    for i in 0..n {
        factory = factory.on(
            &format!("c{i}"),
            Behaviour::delayed_succeed(Duration::from_millis(100), &format!("r{i}")),
        );
    }
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("bounded fan out");
    let chains: Vec<_> = (0..n)
        .map(|i| vec![http_target("p", &format!("c{i}"))])
        .collect();
    let results = d
        .broadcast_with_concurrency(
            &task,
            chains,
            CancellationToken::new(),
            NonZeroUsize::new(width).unwrap(),
        )
        .await;

    assert_eq!(results.len(), n);
    for res in &results {
        assert_eq!(res.as_ref().unwrap().record.status, RunStatus::Success);
    }
    let peak = probe.max_concurrent();
    assert!(
        peak <= width,
        "at most {width} attempts may run at once, observed peak {peak}"
    );
    assert!(
        peak >= 2,
        "with 10 sleeping chains and width 3, real overlap is expected (peak {peak})"
    );
}

#[tokio::test(start_paused = true)]
async fn attempt_timeout_fires_on_the_tokio_clock() {
    // A hanging attempt under a task timeout must terminate as Timeout once the
    // clock advances past the deadline — proving the kernel's own `drive`
    // timeout works, not just adapter-side timeouts. `start_paused` + the
    // runtime's auto-advance move virtual time to the timer deadline.
    let factory = MockFactory::new().on("slow", Behaviour::Hang).into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("times out").with_timeout(Duration::from_secs(30));
    let rec = d
        .dispatch(
            &task,
            vec![http_target("p", "slow")],
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Timeout);
    assert_eq!(rec.output, None);
    assert_eq!(rec.record.attempts.len(), 1);
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Timeout,
            ..
        }
    ));
    assert_eq!(
        ledger.append_count(),
        1,
        "a timed-out run is still recorded"
    );
}

#[tokio::test(start_paused = true)]
async fn timeout_is_failover_eligible_and_recovers_on_next_target() {
    // The first target hangs and trips the timeout (eligible), so the chain
    // fails over to a fast second target and succeeds.
    let factory = MockFactory::new()
        .on("slow", Behaviour::Hang)
        .on("fast", Behaviour::succeed("recovered"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("timeout then recover").with_timeout(Duration::from_secs(5));
    let chain = vec![http_target("p1", "slow"), http_target("p2", "fast")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Success);
    assert_eq!(rec.record.attempts.len(), 2);
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Timeout,
            failed_over: true,
            ..
        }
    ));
    assert_eq!(rec.record.target.model.as_str(), "fast");
}

// ===========================================================================
// Finding 1: unbound native harness sessions fail closed
// ===========================================================================

#[tokio::test]
async fn harness_legacy_native_resume_is_rejected_even_for_read_only() {
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("must not run", "native-2"),
    );
    let make_calls = factory.make_calls();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    sessions.seed(seed_record("logical-sess", Some("native-abc"), Vec::new()));
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("keep going");
    task.session = Some(SessionRef::new("logical-sess"));
    let chain = vec![cli_target("anthropic", "agent-model")];
    let error = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .expect_err("unbound native sessions must not resume in a harness");
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert!(error.message.contains("NativeSessionDomain"));
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert!(ledger.records().is_empty());
}

#[tokio::test]
async fn native_resume_rejects_when_any_admitted_fallback_is_a_harness() {
    let factory = MockFactory::new()
        .on("chat-model", Behaviour::succeed("must not run"))
        .on(
            "agent-model",
            Behaviour::succeed_harness("must not run", "native-new"),
        );
    let make_calls = factory.make_calls();
    let sessions = MockSessions::new();
    sessions.seed(seed_record("hybrid", Some("legacy-native"), Vec::new()));
    let d = dispatcher(factory.into_arc(), MockLedger::new(), sessions);
    let mut task = TaskSpec::new("inspect");
    task.session = Some(SessionRef::new("hybrid"));

    let error = d
        .dispatch(
            &task,
            vec![
                http_target("openai", "chat-model"),
                cli_target("anthropic", "agent-model"),
            ],
            CancellationToken::new(),
        )
        .await
        .expect_err("an admitted harness fallback makes unbound native resume unsafe");
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn harness_resume_is_none_when_session_has_no_native_id() {
    // A named session that has never produced a native id (e.g. only a
    // transcript, or brand new) must resume with `None`, not the logical id.
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("first agent turn", "native-first"),
    );
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    // Seed a session with no native id.
    sessions.seed(seed_record("sess-no-native", None, Vec::new()));
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("start agent work");
    task.session = Some(SessionRef::new("sess-no-native"));
    let chain = vec![cli_target("anthropic", "agent-model")];
    d.dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    let jobs = probe.harness_jobs();
    assert_eq!(jobs[0].resume, None, "no stored native id → resume is None");
}

// ===========================================================================
// Finding 2: direct-chat transcript replay + persistence
// ===========================================================================

#[tokio::test]
async fn direct_chat_replays_transcript_then_current_user_message() {
    // A session with an existing transcript [user q1, assistant a1]; the next
    // chat run with a system prompt must send exactly:
    //   system, user q1, assistant a1, user q2
    let factory = MockFactory::new().on("chat-model", Behaviour::succeed("a2"));
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    sessions.seed(seed_record(
        "chat-sess",
        Some("native-history-is-ignored-for-direct-http"),
        vec![ChatMessage::user("q1"), ChatMessage::assistant("a1")],
    ));
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("q2");
    task.system = Some("you are terse".to_string());
    task.session = Some(SessionRef::new("chat-sess"));
    let chain = vec![http_target("openai", "chat-model")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rec.record.status, RunStatus::Success);

    let reqs = probe.chat_requests();
    assert_eq!(reqs.len(), 1);
    let msgs = &reqs[0].messages;
    assert_eq!(msgs.len(), 4, "system + 2 history + current user");
    assert_eq!(msgs[0].role, Role::System);
    assert_eq!(msgs[0].content, "you are terse");
    assert_eq!(msgs[1].role, Role::User);
    assert_eq!(msgs[1].content, "q1");
    assert_eq!(msgs[2].role, Role::Assistant);
    assert_eq!(msgs[2].content, "a1");
    assert_eq!(msgs[3].role, Role::User);
    assert_eq!(msgs[3].content, "q2");
    assert_eq!(
        sessions
            .get("chat-sess")
            .unwrap()
            .native_session_id
            .as_deref(),
        Some("native-history-is-ignored-for-direct-http")
    );
}

#[tokio::test]
async fn dispatcher_never_loads_or_overwrites_another_owners_session() {
    let factory = MockFactory::new().on("chat-model", Behaviour::succeed("alice-answer"));
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let mut bob = seed_record(
        "shared",
        None,
        vec![
            ChatMessage::user("bob-private-question"),
            ChatMessage::assistant("bob-private-answer"),
        ],
    );
    bob.owner = "bob".into();
    sessions.seed(bob.clone());
    let d = dispatcher(factory.into_arc(), ledger, sessions.clone()).with_owner("alice");

    let mut task = TaskSpec::new("alice-question");
    task.session = Some(SessionRef::new("shared"));
    d.dispatch(
        &task,
        vec![http_target("openai", "chat-model")],
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let requests = probe.chat_requests();
    assert_eq!(requests[0].messages.len(), 1);
    assert_eq!(requests[0].messages[0].content, "alice-question");
    let saved_alice = sessions.get_for("alice", "shared").unwrap();
    assert_eq!(saved_alice.owner, "alice");
    assert_eq!(saved_alice.transcript.len(), 2);
    let saved_bob = sessions.get_for("bob", "shared").unwrap();
    assert_eq!(saved_bob.transcript, bob.transcript);
    assert_eq!(saved_bob.run_count, bob.run_count);
}

#[tokio::test]
async fn session_integrity_error_fails_before_any_model_execution() {
    let factory = MockFactory::new().on("chat-model", Behaviour::succeed("must-not-run"));
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let d = Dispatcher::new(
        factory.into_arc(),
        Arc::new(ledger.clone()),
        Arc::new(LoadErrorSessions),
    );
    let mut task = TaskSpec::new("question");
    task.session = Some(SessionRef::new("legacy"));

    let error = d
        .dispatch(
            &task,
            vec![http_target("openai", "chat-model")],
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Config);
    assert!(probe.chat_requests().is_empty());
    assert!(ledger.records().is_empty());
}

#[tokio::test]
async fn forged_lease_or_snapshot_identity_never_reaches_a_model() {
    for forge_lease_identity in [true, false] {
        let factory = MockFactory::new().on("chat-model", Behaviour::succeed("must-not-run"));
        let make_calls = factory.make_calls();
        let probe = factory.probe();
        let mut foreign_record = seed_record(
            "foreign-session",
            None,
            vec![ChatMessage::user("cross-session-canary")],
        );
        foreign_record.owner = "foreign-owner".into();
        let lease_load_calls = Arc::new(AtomicUsize::new(0));
        let sessions = ForgedAuthoritySessions {
            forge_lease_identity,
            snapshot: SessionSnapshot {
                record: foreign_record,
                session_revision: 9,
                native_session: NativeSessionState::Absent,
            },
            lease_load_calls: Arc::clone(&lease_load_calls),
        };
        let dispatcher = Dispatcher::new(
            factory.into_arc(),
            Arc::new(MockLedger::new()),
            Arc::new(sessions),
        );
        let mut task = TaskSpec::new("local question");
        task.session = Some(SessionRef::new("local-session"));

        let error = dispatcher
            .dispatch(
                &task,
                vec![http_target("openai", "chat-model")],
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Config);
        assert_eq!(make_calls.load(Ordering::SeqCst), 0);
        assert!(probe.chat_requests().is_empty());
        assert_eq!(
            lease_load_calls.load(Ordering::SeqCst),
            usize::from(!forge_lease_identity),
            "a forged lease is rejected before any continuity read"
        );
    }
}

#[tokio::test]
async fn direct_chat_appends_user_and_assistant_to_transcript_on_success() {
    // After a successful chat run, the session transcript must grow by the
    // (user, assistant) pair and run_count must bump.
    let factory = MockFactory::new().on("chat-model", Behaviour::succeed("answer-2"));
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    sessions.seed(seed_record(
        "grow-sess",
        None,
        vec![
            ChatMessage::user("prev-q"),
            ChatMessage::assistant("prev-a"),
        ],
    ));
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("new-q");
    task.session = Some(SessionRef::new("grow-sess"));
    let chain = vec![http_target("openai", "chat-model")];
    d.dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    let saved = sessions.get("grow-sess").unwrap();
    let t = &saved.transcript;
    assert_eq!(t.len(), 4, "prior pair + this run's pair");
    assert_eq!((t[0].role, t[0].content.as_str()), (Role::User, "prev-q"));
    assert_eq!(
        (t[1].role, t[1].content.as_str()),
        (Role::Assistant, "prev-a")
    );
    assert_eq!((t[2].role, t[2].content.as_str()), (Role::User, "new-q"));
    assert_eq!(
        (t[3].role, t[3].content.as_str()),
        (Role::Assistant, "answer-2")
    );
    // run_count was seeded at 3; one run bumps it to 4.
    assert_eq!(saved.run_count, 4);
}

#[tokio::test]
async fn direct_chat_starts_transcript_when_session_is_new() {
    // No pre-seeded record: the first chat turn creates the session with a
    // fresh transcript of exactly [user, assistant].
    let factory = MockFactory::new().on("chat-model", Behaviour::succeed("hello-back"));
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("hello");
    task.session = Some(SessionRef::new("brand-new"));
    let chain = vec![http_target("openai", "chat-model")];
    d.dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    // First turn replays nothing, just sends the user message.
    let reqs = probe.chat_requests();
    assert_eq!(reqs[0].messages.len(), 1);
    assert_eq!(reqs[0].messages[0].role, Role::User);

    let saved = sessions.get("brand-new").unwrap();
    assert_eq!(saved.transcript.len(), 2);
    assert_eq!(
        (
            saved.transcript[0].role,
            saved.transcript[0].content.as_str()
        ),
        (Role::User, "hello")
    );
    assert_eq!(
        (
            saved.transcript[1].role,
            saved.transcript[1].content.as_str()
        ),
        (Role::Assistant, "hello-back")
    );
    assert_eq!(saved.run_count, 1);
}

#[tokio::test]
async fn harness_run_does_not_grow_transcript() {
    // Harness continuity is native-id based; a harness run through a session
    // must leave the transcript empty (the CLI owns its history) while still
    // updating native id + run_count.
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("agent said", "native-h"),
    );
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("agent job");
    task.session = Some(SessionRef::new("harness-sess"));
    let chain = vec![cli_target("anthropic", "agent-model")];
    d.dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    let saved = sessions.get("harness-sess").unwrap();
    assert!(
        saved.transcript.is_empty(),
        "harness runs never fabricate a transcript"
    );
    assert_eq!(saved.native_session_id.as_deref(), Some("native-h"));
    assert_eq!(saved.run_count, 1);
}

#[tokio::test]
async fn failed_chat_run_does_not_grow_transcript() {
    // A failing chat run must not append anything to the transcript — only a
    // successful turn is recorded as history.
    let factory = MockFactory::new().on("chat-model", Behaviour::fail(ErrorKind::Config));
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    sessions.seed(seed_record(
        "no-grow-on-fail",
        None,
        vec![ChatMessage::user("q0"), ChatMessage::assistant("a0")],
    ));
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("will fail");
    task.session = Some(SessionRef::new("no-grow-on-fail"));
    let chain = vec![http_target("openai", "chat-model")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rec.record.status, RunStatus::Error);

    let saved = sessions.get("no-grow-on-fail").unwrap();
    assert_eq!(
        saved.transcript.len(),
        2,
        "transcript unchanged after a failed turn"
    );
}

// ===========================================================================
// Finding 3: TaskSpec.system is appended as harness instructions
// ===========================================================================

#[tokio::test]
async fn harness_prompt_appends_system_instructions() {
    // The composed harness prompt must be exactly
    //   "<prompt>\n\n## Additional instructions\n\n<system>"
    let factory = MockFactory::new().on("agent-model", Behaviour::succeed("done"));
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let mut task = TaskSpec::new("do the migration");
    task.system = Some("prefer small commits".to_string());
    let chain = vec![cli_target("anthropic", "agent-model")];
    d.dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    let jobs = probe.harness_jobs();
    assert_eq!(
        jobs[0].prompt,
        "do the migration\n\n## Additional instructions\n\nprefer small commits"
    );
}

#[tokio::test]
async fn harness_prompt_without_system_is_unchanged() {
    let factory = MockFactory::new().on("agent-model", Behaviour::succeed("done"));
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("just the prompt"); // system = None
    let chain = vec![cli_target("anthropic", "agent-model")];
    d.dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();

    let jobs = probe.harness_jobs();
    assert_eq!(jobs[0].prompt, "just the prompt");
}

// ===========================================================================
// Finding 4: cancellation checked before make and between failover attempts
// ===========================================================================

#[tokio::test]
async fn pre_cancelled_dispatch_never_touches_the_factory() {
    // A token cancelled before dispatch must yield Cancelled with no factory
    // side effects: `make` is never called, and no chat/harness attempt runs.
    let factory = MockFactory::new().on("m", Behaviour::succeed("must not run"));
    let probe = factory.probe();
    let make_calls = factory.make_calls();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let cancel = CancellationToken::new();
    cancel.cancel();
    let task = TaskSpec::new("pre-cancelled");
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], cancel)
        .await
        .unwrap();

    assert_eq!(rec.record.status, RunStatus::Cancelled);
    assert_eq!(
        rec.record.attempts.len(),
        1,
        "one recorded (cancelled) attempt"
    );
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Cancelled,
            failed_over: false,
            ..
        }
    ));
    assert_eq!(
        make_calls.load(Ordering::SeqCst),
        0,
        "factory must not be called for a pre-cancelled dispatch"
    );
    assert!(
        probe.chat_requests().is_empty() && probe.harness_jobs().is_empty(),
        "no executor attempt may run"
    );
    assert_eq!(
        ledger.append_count(),
        1,
        "the cancelled run is still recorded"
    );
}

#[tokio::test]
async fn cancellation_between_attempts_stops_before_next_factory_make() {
    // Attempt 1 fails over (SpawnFailed) and, in doing so, cancels the token.
    // The loop's pre-make guard must then catch the cancellation before
    // building the second target: status Cancelled, and the factory is asked to
    // build only the first target (make_calls == 1).
    let cancel = CancellationToken::new();
    let factory = MockFactory::new()
        .cancel_on_make("first", cancel.clone())
        .on("second", Behaviour::succeed("must not run"));
    let make_calls = factory.make_calls();
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let d = dispatcher(factory.into_arc(), ledger.clone(), MockSessions::new());

    let task = TaskSpec::new("cancel mid-chain");
    let chain = vec![http_target("p1", "first"), http_target("p2", "second")];
    let rec = d.dispatch(&task, chain, cancel).await.unwrap();

    assert_eq!(rec.record.status, RunStatus::Cancelled);
    assert_eq!(
        rec.record.attempts.len(),
        2,
        "failed-over first + cancelled second"
    );
    assert!(matches!(
        rec.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::SpawnFailed,
            failed_over: true,
            ..
        }
    ));
    assert!(matches!(
        rec.record.attempts[1].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Cancelled,
            failed_over: false,
            ..
        }
    ));
    assert_eq!(
        make_calls.load(Ordering::SeqCst),
        1,
        "second target's factory make must never be called"
    );
    assert!(
        probe.chat_requests().is_empty(),
        "the second (would-succeed) attempt must never execute"
    );
    assert_eq!(ledger.append_count(), 1);
}

// ===========================================================================
// Architect decision: persistence after a completed run is best-effort — a
// ledger or session-store error must not demote a successful run to Err.
// ===========================================================================

#[tokio::test]
async fn ledger_append_failure_does_not_fail_a_completed_run() {
    // The model call succeeds; the ledger append errors. The run must still
    // come back Ok with status Success — the append failure is only logged.
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed("all good"))
        .into_arc();
    let ledger = ErroringLedger::new();
    let sessions = MockSessions::new();
    let d = Dispatcher::new(factory, Arc::new(ledger.clone()), Arc::new(sessions));

    let task = TaskSpec::new("succeed then fail to persist");
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .expect("a completed run must survive a ledger append failure");

    assert_eq!(rec.record.status, RunStatus::Success);
    assert_eq!(
        rec.record.output_chars,
        Some("all good".chars().count() as u64)
    );
    assert_eq!(
        ledger.append_calls(),
        1,
        "the append was attempted exactly once"
    );
}

#[tokio::test]
async fn session_save_failure_does_not_fail_a_completed_run() {
    // The model call succeeds; the session store's save errors. The run must
    // still return Ok/Success — session persistence is best-effort.
    let factory = MockFactory::new()
        .on("agent-model", Behaviour::succeed_harness("ok", "native-x"))
        .into_arc();
    let ledger = MockLedger::new();
    let sessions = ErroringSessions::new();
    let d = Dispatcher::new(
        factory,
        Arc::new(ledger.clone()),
        Arc::new(sessions.clone()),
    );

    let mut task = TaskSpec::new("succeed then fail to save session");
    task.session = Some(SessionRef::new("sess-save-fail"));
    let chain = vec![cli_target("anthropic", "agent-model")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .expect("a completed run must survive a session save failure");

    assert_eq!(rec.record.status, RunStatus::Success);
    assert_eq!(rec.record.session_id.as_deref(), Some("sess-save-fail"));
    assert_eq!(ledger.append_count(), 1, "the run was still recorded");
    assert!(
        sessions.save_calls() >= 1,
        "a session save was attempted (and failed, but did not propagate)"
    );
}

#[tokio::test]
async fn ledger_failure_on_a_failed_run_still_returns_the_error_record() {
    // Even when the run itself failed *and* the ledger append fails, dispatch
    // still returns Ok with the Error record — the append error never masks the
    // run's own terminal status.
    let factory = MockFactory::new()
        .on("m", Behaviour::fail(ErrorKind::Config))
        .into_arc();
    let ledger = ErroringLedger::new();
    let sessions = MockSessions::new();
    let d = Dispatcher::new(factory, Arc::new(ledger.clone()), Arc::new(sessions));

    let task = TaskSpec::new("fail then fail to persist");
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .expect("dispatch returns the record even when append fails");

    assert_eq!(rec.record.status, RunStatus::Error);
    assert!(rec.record.error.is_some());
    assert_eq!(ledger.append_calls(), 1);
}

// ---------------------------------------------------------------------------
// dispatch_stream tests (WP-18 kernel streaming API)
// ---------------------------------------------------------------------------

/// A streaming mock ChatClient that emits a vector of StreamEvents.
struct StreamingChat {
    events: Vec<vyane_core::StreamEvent>,
    probe: Arc<Probe>,
}

#[async_trait]
impl ChatClient for StreamingChat {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiChat
    }

    async fn complete(&self, _req: ChatRequest) -> Result<ChatOutcome> {
        unreachable!("streaming tests should call stream(), not complete()")
    }

    async fn stream(
        &self,
        req: ChatRequest,
    ) -> Result<futures::stream::BoxStream<'static, Result<vyane_core::StreamEvent>>> {
        self.probe.chat_requests.lock().unwrap().push(req);
        let _guard = self.probe.enter();
        let events = self.events.clone();
        let stream = async_stream::stream! {
            for event in events {
                yield Ok(event);
            }
        };
        Ok(Box::pin(stream))
    }
}

/// Factory variant that produces streaming chat clients.
struct StreamingFactory {
    events: Vec<vyane_core::StreamEvent>,
    scopes: Arc<Mutex<Vec<AttemptScope>>>,
}

impl ExecutorFactory for StreamingFactory {
    fn make(&self, _target: &BoundTarget) -> Result<Executor> {
        Ok(Executor::Chat(Arc::new(StreamingChat {
            events: self.events.clone(),
            probe: Probe::new(),
        })))
    }

    fn make_scoped(&self, target: &BoundTarget, scope: &AttemptScope) -> Result<Executor> {
        self.scopes.lock().unwrap().push(scope.clone());
        self.make(target)
    }
}

/// Final result scripted for a streaming harness after it emits its events.
#[derive(Clone)]
enum StreamingHarnessTerminal {
    Succeed { text: String, usage: Option<Usage> },
    Fail { kind: ErrorKind, message: String },
}

impl StreamingHarnessTerminal {
    fn succeed(text: &str, usage: Option<Usage>) -> Self {
        Self::Succeed {
            text: text.to_string(),
            usage,
        }
    }

    fn fail(kind: ErrorKind) -> Self {
        Self::Fail {
            kind,
            message: format!("mock streaming harness {kind:?}"),
        }
    }
}

/// Harness mock whose callback is `'static`, while the dispatcher callback in
/// the tests below deliberately borrows a local vector. Yielding after each
/// event exercises the kernel's mpsc bridge rather than only its final drain.
struct StreamingHarness {
    events: Vec<HarnessStreamEvent>,
    terminal: StreamingHarnessTerminal,
    jobs: Arc<Mutex<Vec<HarnessJob>>>,
}

#[async_trait]
impl Harness for StreamingHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }

    async fn available(&self) -> bool {
        true
    }

    async fn run(&self, _job: HarnessJob, _cancel: CancellationToken) -> Result<HarnessOutcome> {
        unreachable!("streaming harness tests should call run_stream(), not run()")
    }

    async fn run_stream(
        &self,
        job: HarnessJob,
        cancel: CancellationToken,
        mut on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        self.jobs.lock().unwrap().push(job);

        if cancel.is_cancelled() {
            return Err(VyaneError::cancelled());
        }
        for event in self.events.iter().cloned() {
            on_event(event);
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                return Err(VyaneError::cancelled());
            }
        }

        match &self.terminal {
            StreamingHarnessTerminal::Succeed { text, usage } => Ok(HarnessOutcome {
                text: text.clone(),
                native_session_id: Some("native-stream-session".to_string()),
                usage: *usage,
                exit_code: 0,
                duration: Duration::from_millis(1),
            }),
            StreamingHarnessTerminal::Fail { kind, message } => {
                Err(VyaneError::new(*kind, message.clone()))
            }
        }
    }
}

/// Factory for the streaming harness mock, with observable make/job counts.
struct StreamingHarnessFactory {
    events: Vec<HarnessStreamEvent>,
    terminal: StreamingHarnessTerminal,
    jobs: Arc<Mutex<Vec<HarnessJob>>>,
    make_calls: Arc<AtomicUsize>,
}

impl StreamingHarnessFactory {
    fn new(events: Vec<HarnessStreamEvent>, terminal: StreamingHarnessTerminal) -> Self {
        Self {
            events,
            terminal,
            jobs: Arc::new(Mutex::new(Vec::new())),
            make_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ExecutorFactory for StreamingHarnessFactory {
    fn make(&self, _target: &BoundTarget) -> Result<Executor> {
        self.make_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Executor::Agent(Arc::new(StreamingHarness {
            events: self.events.clone(),
            terminal: self.terminal.clone(),
            jobs: Arc::clone(&self.jobs),
        })))
    }
}

#[tokio::test]
async fn compatibility_stream_rejects_legacy_session_before_load_probe_or_make() {
    let sessions = MockSessions::new();
    sessions.seed(seed_record("legacy-stream", Some("native-old"), Vec::new()));
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("must not run", "native-new"),
    );
    let make_calls = factory.make_calls();
    let d = dispatcher(factory.into_arc(), MockLedger::new(), sessions.clone());
    let mut task = TaskSpec::new("stream");
    task.session = Some(SessionRef::new("legacy-stream"));

    let error = d
        .dispatch_stream(
            &task,
            &cli_target("anthropic", "agent-model"),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert!(error.message.contains("does not support sessions"));
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(sessions.load_calls(), 0);
}

#[tokio::test]
async fn prepared_stream_rejects_bound_session_before_load_probe_or_make() {
    let sessions = BoundSnapshotSessions::new("bound-stream");
    let snapshot_load_calls = Arc::clone(&sessions.snapshot_load_calls);
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("must not run", "native-new"),
    );
    let make_calls = factory.make_calls();
    let d = Dispatcher::new(
        factory.into_arc(),
        Arc::new(MockLedger::new()),
        Arc::new(sessions),
    );
    let mut task = TaskSpec::new("stream");
    task.session = Some(SessionRef::new("bound-stream"));
    let prepared = d
        .prepare(&task, vec![cli_target("anthropic", "agent-model")])
        .unwrap();

    let error = d
        .dispatch_stream_prepared(&task, &prepared, CancellationToken::new(), |_| {})
        .await
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert!(error.message.contains("does not support sessions"));
    assert_eq!(make_calls.load(Ordering::SeqCst), 0);
    assert_eq!(snapshot_load_calls.load(Ordering::SeqCst), 0);
}

/// `dispatch_stream` with a successful stream produces a Success RunRecord
/// and delivers all delta events through the callback.
#[tokio::test]
async fn dispatch_stream_success_delivers_deltas_and_records() {
    let scopes = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(StreamingFactory {
        events: vec![
            vyane_core::StreamEvent::Delta("Hello ".into()),
            vyane_core::StreamEvent::Delta("world".into()),
            vyane_core::StreamEvent::Usage(Usage {
                input_tokens: 5,
                output_tokens: 2,
                reasoning_tokens: None,
                cached_input_tokens: None,
            }),
            vyane_core::StreamEvent::Done {
                finish_reason: Some("stop".into()),
            },
        ],
        scopes: Arc::clone(&scopes),
    });
    let ledger = MockLedger::default();
    let sessions = MockSessions::new();
    let d = Dispatcher::new(factory, Arc::new(ledger), Arc::new(sessions));

    let task = TaskSpec::new("say hello");
    let prepared = d
        .prepare(&task, vec![http_target("test", "streaming-model")])
        .unwrap();
    let mut collected = Vec::new();
    let outcome = d
        .dispatch_stream_prepared(&task, &prepared, CancellationToken::new(), |event| {
            if let vyane_kernel::StreamDispatchEvent::Delta(text) = event {
                collected.push(text);
            }
        })
        .await
        .expect("dispatch_stream succeeds");

    let outcome = outcome.expect("streaming was supported");
    assert_eq!(outcome.record.status, RunStatus::Success);
    assert_eq!(
        scopes.lock().unwrap()[0].execution.execution_id,
        outcome.record.run_id
    );
    assert_eq!(outcome.output.as_deref(), Some("Hello world"));
    assert_eq!(collected, vec!["Hello ".to_string(), "world".to_string()]);
}

/// Harness streaming forwards every text and tool event in order and records
/// the harness's final outcome.
#[tokio::test]
async fn dispatch_stream_harness_success_delivers_all_deltas_and_records() {
    let usage = Usage {
        input_tokens: 11,
        output_tokens: 7,
        reasoning_tokens: Some(3),
        cached_input_tokens: None,
    };
    let factory = Arc::new(StreamingHarnessFactory::new(
        vec![
            HarnessStreamEvent::Delta("first ".to_string()),
            HarnessStreamEvent::ToolUse {
                name: "Edit".to_string(),
                summary: "src/lib.rs".to_string(),
            },
            HarnessStreamEvent::Delta("second".to_string()),
        ],
        StreamingHarnessTerminal::succeed("first second", Some(usage)),
    ));
    let jobs = Arc::clone(&factory.jobs);
    let make_calls = Arc::clone(&factory.make_calls);
    let ledger = MockLedger::new();
    let d = Dispatcher::new(
        factory,
        Arc::new(ledger.clone()),
        Arc::new(MockSessions::new()),
    );

    let mut task = TaskSpec::new("use the harness");
    task.system = Some("be careful".to_string());
    let target = cli_target("anthropic", "streaming-agent");
    let mut collected = Vec::new();
    let outcome = d
        .dispatch_stream(
            &task,
            &target,
            CancellationToken::new(),
            |event| match event {
                vyane_kernel::StreamDispatchEvent::Delta(text) => {
                    collected.push(format!("delta:{text}"));
                }
                vyane_kernel::StreamDispatchEvent::ToolUse { name, summary } => {
                    collected.push(format!("tool:{name}:{summary}"));
                }
                vyane_kernel::StreamDispatchEvent::ReasoningDelta(_) => {}
            },
        )
        .await
        .expect("harness streaming succeeds")
        .expect("harness supports streaming");

    assert_eq!(
        collected,
        vec![
            "delta:first ".to_string(),
            "tool:Edit:src/lib.rs".to_string(),
            "delta:second".to_string(),
        ]
    );
    assert_eq!(outcome.output.as_deref(), Some("first second"));
    assert_eq!(outcome.record.status, RunStatus::Success);
    assert_eq!(outcome.record.transport, AdapterTransport::CliWrap);
    assert_eq!(outcome.record.output_chars, Some(12));
    assert_eq!(outcome.record.usage.as_ref().unwrap().input_tokens, 11);
    assert_eq!(outcome.record.usage.as_ref().unwrap().output_tokens, 7);
    assert_eq!(outcome.record.attempts.len(), 1);
    assert!(matches!(
        outcome.record.attempts[0].outcome,
        AttemptOutcome::Ok
    ));
    assert_eq!(ledger.append_count(), 1);
    assert_eq!(make_calls.load(Ordering::SeqCst), 1);

    let jobs = jobs.lock().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].prompt,
        "use the harness\n\n## Additional instructions\n\nbe careful"
    );
    assert!(
        jobs[0].resume.is_none(),
        "streaming does not resume sessions"
    );
}

/// The prepared probe seam reports Unsupported without appending; its caller
/// retains the prepared value and can reuse it for fallback.
#[tokio::test]
async fn dispatch_stream_harness_unsupported_returns_none_without_record() {
    let factory = Arc::new(StreamingHarnessFactory::new(
        Vec::new(),
        StreamingHarnessTerminal::fail(ErrorKind::Unsupported),
    ));
    let make_calls = Arc::clone(&factory.make_calls);
    let ledger = MockLedger::new();
    let d = Dispatcher::new(
        factory,
        Arc::new(ledger.clone()),
        Arc::new(MockSessions::new()),
    );

    let task = TaskSpec::new("fallback");
    let prepared = d
        .prepare(&task, vec![cli_target("anthropic", "unsupported-agent")])
        .unwrap();
    let outcome = d
        .dispatch_stream_prepared(&task, &prepared, CancellationToken::new(), |_| {})
        .await
        .expect("Unsupported is not a kernel error");

    assert!(outcome.is_none());
    assert_eq!(make_calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.append_count(), 0);
}

/// Cancellation returned by a streaming harness is represented as a normal
/// Cancelled RunRecord, and deltas emitted before cancellation are not lost.
#[tokio::test]
async fn dispatch_stream_harness_cancelled_result_is_recorded() {
    let factory = Arc::new(StreamingHarnessFactory::new(
        vec![HarnessStreamEvent::Delta("partial".to_string())],
        StreamingHarnessTerminal::fail(ErrorKind::Cancelled),
    ));
    let ledger = MockLedger::new();
    let d = Dispatcher::new(
        factory,
        Arc::new(ledger.clone()),
        Arc::new(MockSessions::new()),
    );

    let mut collected = Vec::new();
    let outcome = d
        .dispatch_stream(
            &TaskSpec::new("cancel me"),
            &cli_target("anthropic", "cancelled-agent"),
            CancellationToken::new(),
            |event| {
                if let vyane_kernel::StreamDispatchEvent::Delta(text) = event {
                    collected.push(text);
                }
            },
        )
        .await
        .expect("cancelled harness run returns its record")
        .expect("Cancelled is not Unsupported");

    assert_eq!(collected, vec!["partial".to_string()]);
    assert_eq!(outcome.record.status, RunStatus::Cancelled);
    assert!(outcome.output.is_none());
    assert!(matches!(
        outcome.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Cancelled,
            failed_over: false,
            ..
        }
    ));
    assert_eq!(ledger.append_count(), 1);
}

/// Non-cancellation harness failures are likewise assembled into an Error
/// record with the original error kind and no successful output.
#[tokio::test]
async fn dispatch_stream_harness_error_is_recorded() {
    let factory = Arc::new(StreamingHarnessFactory::new(
        Vec::new(),
        StreamingHarnessTerminal::fail(ErrorKind::Protocol),
    ));
    let ledger = MockLedger::new();
    let d = Dispatcher::new(
        factory,
        Arc::new(ledger.clone()),
        Arc::new(MockSessions::new()),
    );

    let outcome = d
        .dispatch_stream(
            &TaskSpec::new("fail"),
            &cli_target("anthropic", "broken-agent"),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .expect("harness error returns its record")
        .expect("Protocol is not Unsupported");

    assert_eq!(outcome.record.status, RunStatus::Error);
    assert!(outcome.output.is_none());
    assert!(
        outcome
            .record
            .error
            .as_deref()
            .unwrap()
            .contains("Protocol")
    );
    assert!(matches!(
        outcome.record.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Protocol,
            failed_over: false,
            ..
        }
    ));
    assert_eq!(ledger.append_count(), 1);
}

/// `dispatch_stream_prepared` returns Ok(None) when the ChatClient returns
/// ErrorKind::Unsupported from stream().
#[tokio::test]
async fn dispatch_stream_unsupported_returns_none() {
    struct UnsupportedChat;
    #[async_trait]
    impl ChatClient for UnsupportedChat {
        fn protocol(&self) -> Protocol {
            Protocol::OpenaiChat
        }
        async fn complete(&self, _req: ChatRequest) -> Result<ChatOutcome> {
            unreachable!()
        }
        async fn stream(
            &self,
            _req: ChatRequest,
        ) -> Result<futures::stream::BoxStream<'static, Result<vyane_core::StreamEvent>>> {
            Err(VyaneError::unsupported("no streaming"))
        }
    }

    struct UnsupportedFactory;
    impl ExecutorFactory for UnsupportedFactory {
        fn make(&self, _target: &BoundTarget) -> Result<Executor> {
            Ok(Executor::Chat(Arc::new(UnsupportedChat)))
        }
    }

    let d = dispatcher(
        Arc::new(UnsupportedFactory),
        MockLedger::default(),
        MockSessions::new(),
    );

    let task = TaskSpec::new("test");
    let prepared = d.prepare(&task, vec![http_target("p", "m")]).unwrap();
    let outcome = d
        .dispatch_stream_prepared(&task, &prepared, CancellationToken::new(), |_| {})
        .await
        .expect("no kernel error");

    assert!(outcome.is_none(), "Unsupported → Ok(None)");
}

/// `dispatch_stream` with a pre-cancelled token produces a Cancelled record
/// without calling the client.
#[tokio::test]
async fn dispatch_stream_pre_cancelled_produces_cancelled_record() {
    let factory = MockFactory::new().on("m", Behaviour::succeed("must not run"));
    let make_calls = factory.make_calls();
    let probe = factory.probe();
    let ledger = MockLedger::default();
    let d = Dispatcher::new(
        factory.into_arc(),
        Arc::new(ledger.clone()),
        Arc::new(MockSessions::new()),
    );

    let cancel = CancellationToken::new();
    cancel.cancel();

    let outcome = d
        .dispatch_stream(
            &TaskSpec::new("test"),
            &http_target("p", "m"),
            cancel,
            |_| {},
        )
        .await
        .expect("no kernel error");

    let outcome = outcome.expect("pre-cancelled still produces a record");
    assert_eq!(outcome.record.status, RunStatus::Cancelled);
    assert_eq!(
        make_calls.load(Ordering::SeqCst),
        0,
        "pre-cancelled streaming must not call factory.make"
    );
    assert!(
        probe.chat_requests().is_empty() && probe.harness_jobs().is_empty(),
        "no executor may run"
    );
    assert_eq!(ledger.append_count(), 1);
}

/// `dispatch_stream` with a mid-stream error produces an Error record.
#[tokio::test]
async fn dispatch_stream_mid_stream_error_produces_error_record() {
    struct ErrorChat;
    #[async_trait]
    impl ChatClient for ErrorChat {
        fn protocol(&self) -> Protocol {
            Protocol::OpenaiChat
        }
        async fn complete(&self, _req: ChatRequest) -> Result<ChatOutcome> {
            unreachable!()
        }
        async fn stream(
            &self,
            _req: ChatRequest,
        ) -> Result<futures::stream::BoxStream<'static, Result<vyane_core::StreamEvent>>> {
            let stream = async_stream::stream! {
                yield Ok(vyane_core::StreamEvent::Delta("partial".into()));
                yield Err(VyaneError::new(ErrorKind::Protocol, "stream broke"));
            };
            Ok(Box::pin(stream))
        }
    }

    struct ErrorFactory;
    impl ExecutorFactory for ErrorFactory {
        fn make(&self, _target: &BoundTarget) -> Result<Executor> {
            Ok(Executor::Chat(Arc::new(ErrorChat)))
        }
    }

    let d = dispatcher(
        Arc::new(ErrorFactory),
        MockLedger::default(),
        MockSessions::new(),
    );

    let outcome = d
        .dispatch_stream(
            &TaskSpec::new("test"),
            &http_target("p", "m"),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .expect("no kernel error");

    let outcome = outcome.expect("error stream still produces a record");
    assert_eq!(outcome.record.status, RunStatus::Error);
    assert!(outcome.record.error.is_some());
}
