//! Acceptance tests for the dispatch kernel, run entirely against mock
//! `ChatClient` / `Harness` / `Ledger` / `SessionStore` / `ExecutorFactory` —
//! no network, no subprocesses. Each test maps to a bullet in WP-04's
//! "Acceptance (mechanically checkable)" list.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::pending;
use vyane_core::{
    AdapterTransport, AttemptOutcome, BoundTarget, CancellationToken, ChatClient, ChatMessage,
    ChatOutcome, ChatRequest, Endpoint, ErrorKind, GenParams, Harness, HarnessJob, HarnessKind,
    HarnessOutcome, Ledger, ModelId, Protocol, ProviderId, Result, Role, RunQuery, RunRecord,
    RunStatus, SessionRecord, SessionRef, SessionStore, Target, TaskSpec, Usage, VyaneError,
};
use vyane_kernel::{Dispatcher, Executor, ExecutorFactory};

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
    while let Behaviour::Delay { delay, then } = current {
        tokio::time::sleep(*delay).await;
        current = then;
    }
    current
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
            Behaviour::Delay { .. } => unreachable!("settle strips all Delay layers"),
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
            Behaviour::Delay { .. } => unreachable!("settle strips all Delay layers"),
        }
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
    fn into_arc(self) -> Arc<dyn ExecutorFactory> {
        Arc::new(self)
    }
}

impl ExecutorFactory for MockFactory {
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
}

// ---------------------------------------------------------------------------
// Mock Ledger / SessionStore
// ---------------------------------------------------------------------------

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
    store: Arc<Mutex<HashMap<String, SessionRecord>>>,
}

impl MockSessions {
    fn new() -> Self {
        Self::default()
    }
    fn get(&self, id: &str) -> Option<SessionRecord> {
        self.store.lock().unwrap().get(id).cloned()
    }
    /// Pre-populate a session record (e.g. an existing transcript or native id
    /// to resume) so the dispatcher loads it as continuity context.
    fn seed(&self, record: SessionRecord) {
        self.store
            .lock()
            .unwrap()
            .insert(record.session_id.clone(), record);
    }
}

#[async_trait]
impl SessionStore for MockSessions {
    async fn load(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        Ok(self.store.lock().unwrap().get(session_id).cloned())
    }
    async fn save(&self, record: &SessionRecord) -> Result<()> {
        self.store
            .lock()
            .unwrap()
            .insert(record.session_id.clone(), record.clone());
        Ok(())
    }
    async fn list(&self, _owner: Option<&str>) -> Result<Vec<SessionRecord>> {
        Ok(self.store.lock().unwrap().values().cloned().collect())
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
    async fn load(&self, _session_id: &str) -> Result<Option<SessionRecord>> {
        Ok(None)
    }
    async fn save(&self, _record: &SessionRecord) -> Result<()> {
        self.save_calls.fetch_add(1, Ordering::SeqCst);
        Err(VyaneError::new(ErrorKind::Io, "mock session save failure"))
    }
    async fn list(&self, _owner: Option<&str>) -> Result<Vec<SessionRecord>> {
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

    assert_eq!(rec.status, RunStatus::Success);
    assert_eq!(rec.attempts.len(), 2, "both targets attempted");
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            kind: ErrorKind::RateLimited,
            ..
        }
    ));
    assert!(matches!(rec.attempts[1].outcome, AttemptOutcome::Ok));
    assert_eq!(rec.target.model.as_str(), "model-b");
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

    assert_eq!(rec.status, RunStatus::Error);
    assert_eq!(rec.attempts.len(), 1, "config error must not fail over");
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: false,
            kind: ErrorKind::Config,
            ..
        }
    ));
    assert_eq!(rec.target.model.as_str(), "model-a");
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

    assert_eq!(rec.status, RunStatus::Cancelled);
    assert_eq!(rec.attempts.len(), 1);
    assert!(matches!(
        rec.attempts[0].outcome,
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
        ErrorKind::Io,
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
            assert_eq!(rec.attempts.len(), 2, "{kind:?} should fail over");
            assert_eq!(
                rec.status,
                RunStatus::Success,
                "{kind:?} recovers on second"
            );
            assert!(
                matches!(
                    rec.attempts[0].outcome,
                    AttemptOutcome::Err {
                        failed_over: true,
                        ..
                    }
                ),
                "{kind:?} first attempt should be marked failed_over"
            );
        } else {
            assert_eq!(rec.attempts.len(), 1, "{kind:?} should abort");
            assert_ne!(
                rec.status,
                RunStatus::Success,
                "{kind:?} must not reach second"
            );
            assert!(
                matches!(
                    rec.attempts[0].outcome,
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

    assert_eq!(rec.attempts.len(), 2);
    // Each attempt's model is paired only with its own provider — no id crossed
    // the boundary.
    assert_eq!(rec.attempts[0].target.provider.as_str(), "provider-one");
    assert_eq!(rec.attempts[0].target.model.as_str(), "model-alpha");
    assert_eq!(rec.attempts[1].target.provider.as_str(), "provider-two");
    assert_eq!(rec.attempts[1].target.model.as_str(), "model-beta");
    // The specific wrong pairings must never appear.
    for a in &rec.attempts {
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

    assert_eq!(rec.attempts.len(), 2);
    // Order preserved: first the failing attempt, then the succeeding one.
    assert_eq!(rec.attempts[0].target.model.as_str(), "m1");
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            ..
        }
    ));
    assert_eq!(rec.attempts[1].target.model.as_str(), "m2");
    assert!(matches!(rec.attempts[1].outcome, AttemptOutcome::Ok));
    // The record's headline target is the last (successful) attempt's.
    assert_eq!(rec.target, second.target);
    assert_eq!(rec.transport, second.transport);
    assert_eq!(rec.status, RunStatus::Success);
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
            rec.target.model.as_str(),
            format!("chain{i}"),
            "order preserved at {i}"
        );
        let expected_status = if expected_ok[i] {
            RunStatus::Success
        } else {
            RunStatus::Error
        };
        assert_eq!(rec.status, expected_status, "chain {i} status");
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
        assert_eq!(rec.target.model.as_str(), format!("c{i}"));
        assert_eq!(rec.status, RunStatus::Success);
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
    assert_eq!(rec.status, RunStatus::Cancelled);
    assert_eq!(rec.attempts.len(), 1);
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Cancelled,
            failed_over: false,
            ..
        }
    ));
    assert_eq!(ledger.append_count(), 1, "cancelled run is still recorded");
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

    assert_eq!(rec.status, RunStatus::Cancelled);
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

    assert_eq!(rec.status, RunStatus::Error);
    assert_eq!(rec.attempts.len(), 3, "every target attempted");
    // The first two failed over; the last did not (chain exhausted).
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            ..
        }
    ));
    assert!(matches!(
        rec.attempts[1].outcome,
        AttemptOutcome::Err {
            failed_over: true,
            ..
        }
    ));
    assert!(matches!(
        rec.attempts[2].outcome,
        AttemptOutcome::Err {
            failed_over: false,
            ..
        }
    ));
    // Headline target is the last attempted one.
    assert_eq!(rec.target.model.as_str(), "m3");
    assert!(rec.error.is_some(), "terminal error message present");
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

    assert_eq!(rec.status, RunStatus::Success);
    // Digest is SHA-256("greet") first 16 hex chars — deterministic, not body.
    assert_eq!(rec.task_digest, vyane_kernel::task_digest("greet"));
    assert_ne!(rec.task_digest, "greet");
    assert_eq!(rec.task_digest.len(), 16);
    assert_eq!(rec.usage, Some(usage));
    assert_eq!(rec.output_chars, Some("hello there".chars().count() as u64));
    assert_eq!(rec.owner, "local");
    assert_eq!(rec.attempts.len(), 1);
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

    let got = rec.usage.expect("winning attempt usage present");
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
    assert_eq!(rec.usage, None);
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

    assert_eq!(rec.status, RunStatus::Success);
    assert_eq!(rec.transport, AdapterTransport::CliWrap);
    assert_eq!(rec.session_id.as_deref(), Some("sess-1"));

    // Session was created and updated with the harness native id + run_count.
    let saved = sessions.get("sess-1").expect("session persisted");
    assert_eq!(saved.native_session_id.as_deref(), Some("native-xyz"));
    assert_eq!(saved.run_count, 1);
    assert_eq!(saved.owner, "local");
}

#[tokio::test]
async fn session_run_count_increments_across_runs() {
    let factory = MockFactory::new()
        .on("agent-model", Behaviour::succeed_harness("a", "native-1"))
        .into_arc();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    let d = dispatcher(factory, ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("work");
    task.session = Some(vyane_core::SessionRef::new("sess-run"));
    let chain = || vec![cli_target("anthropic", "agent-model")];

    d.dispatch(&task, chain(), CancellationToken::new())
        .await
        .unwrap();
    d.dispatch(&task, chain(), CancellationToken::new())
        .await
        .unwrap();

    let saved = sessions.get("sess-run").unwrap();
    assert_eq!(saved.run_count, 2, "run_count bumps each run");
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

    assert_eq!(rec.session_id, None);
    assert!(
        sessions.list(None).await.unwrap().is_empty(),
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

    assert_eq!(rec.attempts.len(), 2);
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::SpawnFailed,
            failed_over: true,
            ..
        }
    ));
    assert_eq!(rec.status, RunStatus::Success);
    assert_eq!(rec.target.model.as_str(), "m2");
}

#[tokio::test]
async fn labels_and_preview_are_copied_onto_the_record() {
    let factory = MockFactory::new()
        .on("m", Behaviour::succeed("ok"))
        .into_arc();
    let ledger = MockLedger::new();
    let d = dispatcher(factory, ledger.clone(), MockSessions::new());

    let mut task = TaskSpec::new("a rather long prompt body for preview checking");
    task.labels.insert("ticket".into(), "EOS-4".into());
    let rec = d
        .dispatch(&task, vec![http_target("p", "m")], CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(rec.labels.get("ticket").map(String::as_str), Some("EOS-4"));
    assert_eq!(
        rec.task_preview.as_deref(),
        Some("a rather long prompt body for preview checking")
    );
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
            rec.target.model.as_str(),
            format!("c{i}"),
            "position {i} must map to input chain {i} regardless of finish order"
        );
        assert_eq!(rec.status, RunStatus::Success);
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
        assert_eq!(res.as_ref().unwrap().status, RunStatus::Success);
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

    assert_eq!(rec.status, RunStatus::Timeout);
    assert_eq!(rec.attempts.len(), 1);
    assert!(matches!(
        rec.attempts[0].outcome,
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

    assert_eq!(rec.status, RunStatus::Success);
    assert_eq!(rec.attempts.len(), 2);
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::Timeout,
            failed_over: true,
            ..
        }
    ));
    assert_eq!(rec.target.model.as_str(), "fast");
}

// ===========================================================================
// Finding 1: harness resume uses the native session id, never the logical id
// ===========================================================================

#[tokio::test]
async fn harness_resume_passes_native_session_id_not_logical_id() {
    // A session already carries a native id from a prior harness run. The next
    // harness dispatch must resume with that native id — never the logical
    // (store-key) id.
    let factory = MockFactory::new().on(
        "agent-model",
        Behaviour::succeed_harness("continued", "native-2"),
    );
    let probe = factory.probe();
    let ledger = MockLedger::new();
    let sessions = MockSessions::new();
    sessions.seed(seed_record("logical-sess", Some("native-abc"), Vec::new()));
    let d = dispatcher(factory.into_arc(), ledger.clone(), sessions.clone());

    let mut task = TaskSpec::new("keep going");
    task.session = Some(SessionRef::new("logical-sess"));
    let chain = vec![cli_target("anthropic", "agent-model")];
    let rec = d
        .dispatch(&task, chain, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rec.status, RunStatus::Success);

    let jobs = probe.harness_jobs();
    assert_eq!(jobs.len(), 1, "exactly one harness attempt");
    assert_eq!(
        jobs[0].resume.as_deref(),
        Some("native-abc"),
        "resume must be the stored native id"
    );
    assert_ne!(
        jobs[0].resume.as_deref(),
        Some("logical-sess"),
        "the logical id must never be used as the native resume token"
    );

    // And the run then advances the stored native id to the one it reported.
    let saved = sessions.get("logical-sess").unwrap();
    assert_eq!(saved.native_session_id.as_deref(), Some("native-2"));
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
        None,
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
    assert_eq!(rec.status, RunStatus::Success);

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
    assert_eq!(rec.status, RunStatus::Error);

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

    assert_eq!(rec.status, RunStatus::Cancelled);
    assert_eq!(rec.attempts.len(), 1, "one recorded (cancelled) attempt");
    assert!(matches!(
        rec.attempts[0].outcome,
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

    assert_eq!(rec.status, RunStatus::Cancelled);
    assert_eq!(
        rec.attempts.len(),
        2,
        "failed-over first + cancelled second"
    );
    assert!(matches!(
        rec.attempts[0].outcome,
        AttemptOutcome::Err {
            kind: ErrorKind::SpawnFailed,
            failed_over: true,
            ..
        }
    ));
    assert!(matches!(
        rec.attempts[1].outcome,
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

    assert_eq!(rec.status, RunStatus::Success);
    assert_eq!(rec.output_chars, Some("all good".chars().count() as u64));
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

    assert_eq!(rec.status, RunStatus::Success);
    assert_eq!(rec.session_id.as_deref(), Some("sess-save-fail"));
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

    assert_eq!(rec.status, RunStatus::Error);
    assert!(rec.error.is_some());
    assert_eq!(ledger.append_calls(), 1);
}
