//! Acceptance tests for the dispatch kernel, run entirely against mock
//! `ChatClient` / `Harness` / `Ledger` / `SessionStore` / `ExecutorFactory` —
//! no network, no subprocesses. Each test maps to a bullet in WP-04's
//! "Acceptance (mechanically checkable)" list.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::future::pending;
use vyane_core::{
    AdapterTransport, AttemptOutcome, BoundTarget, CancellationToken, ChatClient, ChatOutcome,
    ChatRequest, Endpoint, ErrorKind, GenParams, Harness, HarnessJob, HarnessKind, HarnessOutcome,
    Ledger, ModelId, Protocol, ProviderId, Result, RunQuery, RunRecord, RunStatus, SessionRecord,
    SessionStore, Target, TaskSpec, Usage, VyaneError,
};
use vyane_kernel::{Dispatcher, Executor, ExecutorFactory};

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
}

// ---------------------------------------------------------------------------
// Mock ChatClient / Harness
// ---------------------------------------------------------------------------

struct MockChat {
    behaviour: Behaviour,
}

#[async_trait]
impl ChatClient for MockChat {
    fn protocol(&self) -> Protocol {
        Protocol::OpenaiChat
    }
    async fn complete(&self, _req: ChatRequest) -> Result<ChatOutcome> {
        match &self.behaviour {
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
        }
    }
}

struct MockHarnessImpl {
    behaviour: Behaviour,
}

#[async_trait]
impl Harness for MockHarnessImpl {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }
    async fn available(&self) -> bool {
        true
    }
    async fn run(&self, _job: HarnessJob, _cancel: CancellationToken) -> Result<HarnessOutcome> {
        match &self.behaviour {
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
}

impl MockFactory {
    fn new() -> Self {
        Self {
            behaviours: HashMap::new(),
            build_errors: HashMap::new(),
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
    fn into_arc(self) -> Arc<dyn ExecutorFactory> {
        Arc::new(self)
    }
}

impl ExecutorFactory for MockFactory {
    fn make(&self, target: &BoundTarget) -> Result<Executor> {
        let key = target.target.model.as_str();
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
        match target.transport {
            AdapterTransport::DirectHttp => Ok(Executor::Chat(Arc::new(MockChat { behaviour }))),
            AdapterTransport::CliWrap => {
                Ok(Executor::Agent(Arc::new(MockHarnessImpl { behaviour })))
            }
            // `AdapterTransport` is non-exhaustive; treat any future transport
            // as direct chat for the purposes of these mocks.
            _ => Ok(Executor::Chat(Arc::new(MockChat { behaviour }))),
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
