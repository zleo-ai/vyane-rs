#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::pending;
use tempfile::TempDir;
use vyane_core::{
    AdapterTransport, BoundTarget, CancellationToken, ChatClient, ChatOutcome, ChatRequest,
    Endpoint, ErrorKind, GenParams, Harness, HarnessJob, HarnessKind, HarnessOutcome, Ledger,
    ModelId, Protocol, ProviderId, Result, RunQuery, RunRecord, SessionRecord, SessionStore,
    SessionUpdate, Target, VyaneError,
};
use vyane_kernel::{Dispatcher, Executor, ExecutorFactory};
use vyane_workflow::{
    JournalStepStatus, JournalTargetOutput, TargetResolver, Workflow, WorkflowEngine, WorkflowPlan,
    WorkflowRunId, WorkflowRunStatus, render_template,
};

#[derive(Default)]
struct Probe {
    active_now: AtomicUsize,
    active_max: AtomicUsize,
    prompts: Mutex<Vec<String>>,
    calls: Mutex<HashMap<String, usize>>,
}

impl Probe {
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

    fn prompts(&self) -> Vec<String> {
        self.prompts.lock().unwrap().clone()
    }

    fn call_count(&self, model: &str) -> usize {
        self.calls.lock().unwrap().get(model).copied().unwrap_or(0)
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

#[derive(Clone)]
enum Behaviour {
    Succeed(String),
    Fail(ErrorKind),
    Panic,
    Delay {
        delay: Duration,
        then: Box<Behaviour>,
    },
    Hang,
}

impl Behaviour {
    fn delayed(delay: Duration, text: &str) -> Self {
        Behaviour::Delay {
            delay,
            then: Box::new(Behaviour::Succeed(text.to_string())),
        }
    }
}

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
        self.probe.prompts.lock().unwrap().push(
            req.messages
                .last()
                .map(|message| message.content.clone())
                .unwrap_or_default(),
        );
        let _guard = self.probe.enter();
        match settle(&self.behaviour).await {
            Behaviour::Succeed(text) => Ok(ChatOutcome {
                text: text.clone(),
                usage: None,
                model_echo: None,
                finish_reason: None,
            }),
            Behaviour::Fail(kind) => Err(VyaneError::new(*kind, format!("mock {kind:?}"))),
            Behaviour::Panic => panic!("mock step panic"),
            Behaviour::Hang => {
                pending::<()>().await;
                unreachable!("cancel should end hanging mock")
            }
            Behaviour::Delay { .. } => unreachable!("settle removes delays"),
        }
    }
}

struct MockHarness;

#[async_trait]
impl Harness for MockHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }

    async fn available(&self) -> bool {
        true
    }

    async fn run(&self, _job: HarnessJob, _cancel: CancellationToken) -> Result<HarnessOutcome> {
        unreachable!("workflow tests only use direct chat mocks")
    }
}

struct MockFactory {
    behaviours: HashMap<String, Behaviour>,
    probe: Arc<Probe>,
}

impl MockFactory {
    fn new() -> Self {
        Self {
            behaviours: HashMap::new(),
            probe: Arc::new(Probe::default()),
        }
    }

    fn on(mut self, model: &str, behaviour: Behaviour) -> Self {
        self.behaviours.insert(model.to_string(), behaviour);
        self
    }

    fn probe(&self) -> Arc<Probe> {
        Arc::clone(&self.probe)
    }

    fn into_arc(self) -> Arc<dyn ExecutorFactory> {
        Arc::new(self)
    }
}

impl ExecutorFactory for MockFactory {
    fn make(&self, target: &BoundTarget) -> Result<Executor> {
        let model = target.target.model.as_str().to_string();
        *self
            .probe
            .calls
            .lock()
            .unwrap()
            .entry(model.clone())
            .or_default() += 1;
        let behaviour = self
            .behaviours
            .get(&model)
            .cloned()
            .unwrap_or_else(|| Behaviour::Succeed(format!("{model}-out")));
        match target.transport {
            AdapterTransport::DirectHttp => Ok(Executor::Chat(Arc::new(MockChat {
                behaviour,
                probe: Arc::clone(&self.probe),
            }))),
            AdapterTransport::CliWrap => Ok(Executor::Agent(Arc::new(MockHarness))),
            _ => Ok(Executor::Chat(Arc::new(MockChat {
                behaviour,
                probe: Arc::clone(&self.probe),
            }))),
        }
    }
}

#[derive(Clone, Default)]
struct MockLedger {
    records: Arc<Mutex<Vec<RunRecord>>>,
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
struct MockSessions;

#[async_trait]
impl SessionStore for MockSessions {
    async fn load(&self, _owner: &str, _session_id: &str) -> Result<Option<SessionRecord>> {
        Ok(None)
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

#[derive(Default)]
struct MockResolver {
    targets: HashMap<String, Vec<BoundTarget>>,
}

impl MockResolver {
    fn with_targets(names: &[&str]) -> Self {
        let mut resolver = Self::default();
        for name in names {
            resolver
                .targets
                .insert((*name).to_string(), vec![http_target("p", name)]);
        }
        resolver
    }
}

impl TargetResolver for MockResolver {
    fn resolve(&self, target: &str) -> Result<Vec<BoundTarget>> {
        self.targets.get(target).cloned().ok_or_else(|| {
            VyaneError::new(ErrorKind::NotFound, format!("unknown target `{target}`"))
        })
    }
}

struct CountingResolver {
    calls: Arc<AtomicUsize>,
    inner: MockResolver,
}

impl TargetResolver for CountingResolver {
    fn resolve(&self, target: &str) -> Result<Vec<BoundTarget>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.resolve(target)
    }
}

struct DeferredResolver;

impl TargetResolver for DeferredResolver {
    fn resolve(&self, target: &str) -> Result<Vec<BoundTarget>> {
        Ok(vec![http_target("deferred", target)])
    }

    fn resolve_for_validation(&self, target: &str) -> Result<Option<Vec<BoundTarget>>> {
        if target == "auto" {
            Ok(None)
        } else {
            self.resolve(target).map(Some)
        }
    }
}

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
            base_url: "https://example.test/v1".to_string(),
            auth: None,
        }),
        params: GenParams::default(),
    }
}

fn dispatcher(factory: Arc<dyn ExecutorFactory>) -> Arc<Dispatcher> {
    Arc::new(Dispatcher::new(
        factory,
        Arc::new(MockLedger::default()),
        Arc::new(MockSessions),
    ))
}

fn write_workflow(dir: &TempDir, text: &str) -> std::path::PathBuf {
    let path = dir.path().join("workflow.toml");
    std::fs::write(&path, text).unwrap();
    path
}

fn workflow_from(dir: &TempDir, text: &str) -> Workflow {
    Workflow::from_path(write_workflow(dir, text)).unwrap()
}

fn workflow_engine(
    factory: MockFactory,
    resolver: MockResolver,
    journal_dir: &Path,
) -> (WorkflowEngine, Arc<Probe>) {
    let probe = factory.probe();
    (
        WorkflowEngine::new(
            dispatcher(factory.into_arc()),
            Arc::new(resolver),
            journal_dir.into(),
        ),
        probe,
    )
}

#[test]
fn validation_collects_all_requested_problem_types() {
    let dir = TempDir::new().unwrap();
    let mut wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "bad"

        [[step]]
        id = "a"
        needs = ["c"]
        target = "ok"
        fan_out = ["ok"]
        prompt = "{{vars.missing}}"

        [[step]]
        id = "b"
        needs = ["a"]
        target = "ok"
        prompt = "{{steps.d.output}}"

        [[step]]
        id = "c"
        needs = ["b"]
        target = "missing-target"
        prompt = "c"

        [[step]]
        id = "d"
        target = "ok"
        prompt = "d"
        "#,
    );
    wf.max_concurrency = tokio::sync::Semaphore::MAX_PERMITS + 1;
    let resolver = MockResolver::with_targets(&["ok"]);
    let err = vyane_workflow::validate_workflow(&wf, &BTreeMap::new(), &resolver).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("cycle"));
    assert!(text.contains("max_concurrency must not exceed"));
    assert!(text.contains("unknown variable `missing`"));
    assert!(text.contains("exactly one of `target` or `fan_out`, not both"));
    assert!(text.contains("could not be resolved"));
    assert!(text.contains("not in its transitive needs"));
}

#[test]
fn template_ancestor_budget_rejects_long_chain_before_target_resolution() {
    let dir = TempDir::new().unwrap();
    let mut source = String::from("[workflow]\nname = \"closure-budget\"\n");
    for index in 0..400 {
        source.push_str("\n[[step]]\n");
        source.push_str(&format!("id = \"n{index:03}\"\n"));
        if index > 0 {
            source.push_str(&format!("needs = [\"n{:03}\"]\n", index - 1));
        }
        source.push_str("target = \"ok\"\nprompt = \"run\"\n");
    }
    let workflow = workflow_from(&dir, &source);
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = CountingResolver {
        calls: Arc::clone(&calls),
        inner: MockResolver::with_targets(&["ok"]),
    };

    let error =
        vyane_workflow::validate_workflow(&workflow, &BTreeMap::new(), &resolver).unwrap_err();
    assert!(error.to_string().contains("template ancestor relations"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let plan = workflow.compile_plan().unwrap();
    let error = vyane_workflow::validate_plan(&plan, &BTreeMap::new(), &resolver).unwrap_err();
    assert!(error.to_string().contains("template ancestor relations"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn route_hints_fail_closed_for_explicit_and_fan_out_targets() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "route-hint-scope"

        [[step]]
        id = "explicit"
        target = "one"
        prompt = "one"
        [step.route]
        effort = "high"

        [[step]]
        id = "fan"
        fan_out = ["one", "missing"]
        prompt = "fan"
        [step.route]
        tier = "mainline"
        "#,
    );
    let resolver = MockResolver::with_targets(&["one"]);

    let error = vyane_workflow::validate_workflow(&wf, &BTreeMap::new(), &resolver)
        .unwrap_err()
        .to_string();

    assert!(error.contains("route hints on a non-deferred target"));
    assert!(error.contains("route hints on fan_out targets"));
    assert!(error.contains("fan_out target `missing` could not be resolved"));
}

#[test]
fn templating_substitutes_outputs_vars_workflow_and_escape() {
    let mut steps = BTreeMap::new();
    steps.insert(
        "draft".to_string(),
        vyane_workflow::JournalStep {
            status: JournalStepStatus::Success,
            run_ids: vec![],
            output: Some("draft text".to_string()),
            outputs: None,
            error: None,
        },
    );
    steps.insert(
        "fan".to_string(),
        vyane_workflow::JournalStep {
            status: JournalStepStatus::Success,
            run_ids: vec![],
            output: None,
            outputs: Some(vec![
                JournalTargetOutput {
                    target: "a".to_string(),
                    ok: true,
                    output: Some("A".to_string()),
                },
                JournalTargetOutput {
                    target: "b".to_string(),
                    ok: false,
                    output: None,
                },
                JournalTargetOutput {
                    target: "c".to_string(),
                    ok: true,
                    output: Some("C".to_string()),
                },
            ]),
            error: None,
        },
    );
    let vars = BTreeMap::from([("topic".to_string(), "rust".to_string())]);
    let rendered = render_template(
        "{{workflow.name}} {{vars.topic}} {{steps.draft.output}}\n{{steps.fan.outputs}}{{{{",
        "wf",
        &vars,
        &steps,
    )
    .unwrap();
    assert!(rendered.contains("wf rust draft text"));
    assert!(rendered.contains("## a\nA\n"));
    assert!(rendered.contains("## c\nC\n"));
    assert!(!rendered.contains("## b"));
    assert!(rendered.ends_with("{{"));
}

#[tokio::test]
async fn diamond_dag_runs_middle_steps_concurrently_and_d_sees_outputs() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "diamond"
        max_concurrency = 3

        [[step]]
        id = "a"
        target = "a"
        prompt = "a"

        [[step]]
        id = "b"
        needs = ["a"]
        target = "b"
        prompt = "b sees {{steps.a.output}}"

        [[step]]
        id = "c"
        needs = ["a"]
        target = "c"
        prompt = "c sees {{steps.a.output}}"

        [[step]]
        id = "d"
        needs = ["b", "c"]
        target = "d"
        prompt = "d sees {{steps.b.output}} and {{steps.c.output}}"
        "#,
    );
    let factory = MockFactory::new()
        .on("a", Behaviour::Succeed("A".to_string()))
        .on("b", Behaviour::delayed(Duration::from_millis(80), "B"))
        .on("c", Behaviour::delayed(Duration::from_millis(80), "C"))
        .on("d", Behaviour::Succeed("D".to_string()));
    let (engine, probe) = workflow_engine(
        factory,
        MockResolver::with_targets(&["a", "b", "c", "d"]),
        &dir.path().join("journals"),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::Completed);
    assert!(
        probe.max_concurrent() >= 2,
        "b and c should overlap under the DAG scheduler"
    );
    let prompts = probe.prompts();
    assert!(prompts.iter().any(|prompt| prompt == "d sees B and C"));
}

#[tokio::test]
async fn typed_plan_executes_three_finders_then_one_synthesizer() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "typed-plan-3-plus-1"
        max_concurrency = 3

        [[step]]
        id = "finders"
        fan_out = ["finder-a", "finder-b", "finder-c"]
        prompt = "find evidence"

        [[step]]
        id = "synth"
        needs = ["finders"]
        target = "synth"
        prompt = "combine {{steps.finders.outputs}}"
        "#,
    );
    let compiled = wf.compile_plan().unwrap();
    let plan = WorkflowPlan::from_json(&compiled.to_canonical_json().unwrap()).unwrap();
    assert_eq!(plan, compiled);
    let factory = MockFactory::new()
        .on(
            "finder-a",
            Behaviour::delayed(Duration::from_millis(40), "A"),
        )
        .on(
            "finder-b",
            Behaviour::delayed(Duration::from_millis(40), "B"),
        )
        .on(
            "finder-c",
            Behaviour::delayed(Duration::from_millis(40), "C"),
        )
        .on("synth", Behaviour::Succeed("S".to_string()));
    let (engine, probe) = workflow_engine(
        factory,
        MockResolver::with_targets(&["finder-a", "finder-b", "finder-c", "synth"]),
        &dir.path().join("journals"),
    );

    let run_id = WorkflowRunId::generate();
    engine
        .prepare_plan_with_id(run_id.clone(), &plan, BTreeMap::new())
        .unwrap();
    let outcome = engine
        .run_prepared_plan(run_id.clone(), &plan, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::Completed);
    assert_eq!(
        outcome.journal.plan_sha256.as_deref(),
        Some(plan.plan_sha256.as_str())
    );
    assert!(probe.max_concurrent() >= 3);
    assert_eq!(probe.call_count("finder-a"), 1);
    assert_eq!(probe.call_count("finder-b"), 1);
    assert_eq!(probe.call_count("finder-c"), 1);
    assert_eq!(probe.call_count("synth"), 1);
    let prompts = probe.prompts();
    let synth = prompts
        .iter()
        .find(|prompt| prompt.starts_with("combine "))
        .unwrap();
    assert!(synth.contains("## finder-a\nA"));
    assert!(synth.contains("## finder-b\nB"));
    assert!(synth.contains("## finder-c\nC"));

    let resumed = engine
        .resume_plan(run_id.as_str(), &plan, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(resumed.journal.plan_sha256, Some(plan.plan_sha256.clone()));
    assert_eq!(probe.call_count("synth"), 1, "successful steps are reused");
}

#[tokio::test]
async fn plan_only_continuations_require_digest_before_resolution_or_dispatch() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "plan-binding"

        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let plan = wf.compile_plan().unwrap();
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let factory = factory_ok();
    let probe = factory.probe();
    let engine = WorkflowEngine::new(
        dispatcher(factory.into_arc()),
        Arc::new(CountingResolver {
            calls: Arc::clone(&resolver_calls),
            inner: MockResolver::with_targets(&["ok"]),
        }),
        journal_dir.clone(),
    );

    for resume in [false, true] {
        let run_id = WorkflowRunId::generate();
        engine
            .prepare_plan_with_id(run_id.clone(), &plan, BTreeMap::new())
            .unwrap();
        resolver_calls.store(0, Ordering::SeqCst);
        let path = journal_dir.join(format!("{run_id}.json"));
        let mut journal: vyane_workflow::WorkflowJournal =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        journal.plan_sha256 = None;
        std::fs::write(&path, serde_json::to_vec_pretty(&journal).unwrap()).unwrap();

        let error = if resume {
            engine
                .resume_plan(run_id.as_str(), &plan, CancellationToken::new())
                .await
                .unwrap_err()
        } else {
            engine
                .run_prepared_plan(run_id, &plan, CancellationToken::new())
                .await
                .unwrap_err()
        };
        assert!(error.to_string().contains("not bound to a plan digest"));
        assert_eq!(resolver_calls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.call_count("ok"), 0);
    }
}

#[tokio::test]
async fn workflow_prepared_compatibility_migrates_missing_plan_digest_after_source_check() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "compat-binding"
        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let expected_digest = wf.compile_plan().unwrap().plan_sha256;
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        &journal_dir,
    );
    let run_id = WorkflowRunId::generate();
    engine
        .prepare_run_with_id(run_id.clone(), &wf, BTreeMap::new())
        .unwrap();
    let path = journal_dir.join(format!("{run_id}.json"));
    let mut journal: vyane_workflow::WorkflowJournal =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    journal.plan_sha256 = None;
    std::fs::write(&path, serde_json::to_vec_pretty(&journal).unwrap()).unwrap();

    let outcome = engine
        .run_prepared(run_id, &wf, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.journal.plan_sha256, Some(expected_digest));
    assert_eq!(probe.call_count("ok"), 1);
}

#[tokio::test]
async fn programmatic_workflow_split_continuations_use_derived_source_digest() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let base = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "programmatic-split"
        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        &journal_dir,
    );

    for (index, file_sha256) in ["", "not-a-source-sha"].into_iter().enumerate() {
        let mut workflow = base.clone();
        workflow.file_path = std::path::PathBuf::new();
        workflow.file_sha256 = file_sha256.into();
        workflow.legacy_file_sha256 = None;
        let plan = workflow.compile_plan().unwrap();
        assert_ne!(plan.source_sha256, workflow.file_sha256);

        let run_id = WorkflowRunId::generate();
        engine
            .prepare_run_with_id(run_id.clone(), &workflow, BTreeMap::new())
            .unwrap();
        let first = engine
            .run_prepared(run_id.clone(), &workflow, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(first.journal.file_sha256, plan.source_sha256);
        assert_eq!(first.journal.plan_sha256, Some(plan.plan_sha256.clone()));

        let resumed = engine
            .resume(run_id.as_str(), &workflow, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(resumed.journal.file_sha256, plan.source_sha256);
        assert_eq!(
            probe.call_count("ok"),
            index + 1,
            "resume must reuse the successful step"
        );
    }
}

#[tokio::test]
async fn replay_creates_new_run_and_reuses_only_dependency_closed_successful_prefix() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "exact-replay"

        [[step]]
        id = "a"
        target = "a"
        prompt = "a"

        [[step]]
        id = "b"
        needs = ["a"]
        target = "b"
        prompt = "b {{steps.a.output}}"
        on_error = "continue"

        [[step]]
        id = "c"
        needs = ["b"]
        target = "c"
        prompt = "c"
        "#,
    );
    let source_factory = MockFactory::new()
        .on("a", Behaviour::Succeed("A".into()))
        .on("b", Behaviour::Fail(ErrorKind::Protocol))
        .on("c", Behaviour::Succeed("C".into()));
    let (source_engine, source_probe) = workflow_engine(
        source_factory,
        MockResolver::with_targets(&["a", "b", "c"]),
        &journal_dir,
    );
    let source = source_engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(source.status, WorkflowRunStatus::CompletedWithFailures);
    assert_eq!(source_probe.call_count("a"), 1);
    assert_eq!(source_probe.call_count("b"), 1);
    assert_eq!(source_probe.call_count("c"), 0);
    let source_bytes = std::fs::read(&source.journal_path).unwrap();

    let replay_factory = MockFactory::new()
        .on("b", Behaviour::Succeed("B".into()))
        .on("c", Behaviour::Succeed("C".into()));
    let (replay_engine, replay_probe) = workflow_engine(
        replay_factory,
        MockResolver::with_targets(&["a", "b", "c"]),
        &journal_dir,
    );
    let replay = replay_engine
        .replay(source.wf_run_id.as_str(), &wf, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(replay.status, WorkflowRunStatus::Completed);
    assert_ne!(replay.wf_run_id, source.wf_run_id);
    assert_eq!(std::fs::read(&source.journal_path).unwrap(), source_bytes);
    assert_eq!(replay_probe.call_count("a"), 0);
    assert_eq!(replay_probe.call_count("b"), 1);
    assert_eq!(replay_probe.call_count("c"), 1);
    assert_eq!(replay.journal.steps["a"].output.as_deref(), Some("A"));
    let provenance = replay.journal.replay.as_ref().unwrap();
    assert_eq!(provenance.source_wf_run_id, source.wf_run_id);
    assert_eq!(provenance.reused_step_ids, ["a"]);
    assert_eq!(provenance.reused_steps_sha256.len(), 64);
}

#[tokio::test]
async fn replay_rejects_digest_drift_and_existing_identity_before_resolution_or_dispatch() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "replay-gates"
        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let (source_engine, _) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        &journal_dir,
    );
    let source = source_engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    let source_bytes = std::fs::read(&source.journal_path).unwrap();

    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let factory = factory_ok();
    let probe = factory.probe();
    let engine = WorkflowEngine::new(
        dispatcher(factory.into_arc()),
        Arc::new(CountingResolver {
            calls: Arc::clone(&resolver_calls),
            inner: MockResolver::with_targets(&["ok"]),
        }),
        journal_dir.clone(),
    );
    let mut changed = wf.compile_plan().unwrap();
    changed.plan_sha256 = "0".repeat(64);
    let error = engine
        .replay_plan(
            source.wf_run_id.as_str(),
            &changed,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("digest does not match"));
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.call_count("ok"), 0);

    let valid_plan = wf.compile_plan().unwrap();
    let mut rebound_source: vyane_workflow::WorkflowJournal =
        serde_json::from_slice(&source_bytes).unwrap();
    rebound_source.plan_sha256 = Some("f".repeat(64));
    std::fs::write(
        &source.journal_path,
        serde_json::to_vec_pretty(&rebound_source).unwrap(),
    )
    .unwrap();
    let error = engine
        .replay_plan(
            source.wf_run_id.as_str(),
            &valid_plan,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("workflow plan digest changed for continuation")
    );
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.call_count("ok"), 0);
    std::fs::write(&source.journal_path, &source_bytes).unwrap();

    let error = engine
        .replay_with_id(
            source.wf_run_id.clone(),
            source.wf_run_id.as_str(),
            &wf,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("new run identity"));
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&source.journal_path).unwrap(), source_bytes);
}

#[tokio::test]
async fn replay_reruns_a_fan_out_step_when_any_recorded_target_failed() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "fan-out-replay"
        [[step]]
        id = "fan"
        fan_out = ["good", "bad"]
        prompt = "fan"
        "#,
    );
    let source_factory = MockFactory::new()
        .on("good", Behaviour::Succeed("G".into()))
        .on("bad", Behaviour::Fail(ErrorKind::Protocol));
    let (source_engine, _) = workflow_engine(
        source_factory,
        MockResolver::with_targets(&["good", "bad"]),
        &journal_dir,
    );
    let source = source_engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(source.status, WorkflowRunStatus::Completed);
    assert!(
        source.journal.steps["fan"]
            .outputs
            .as_ref()
            .unwrap()
            .iter()
            .any(|output| !output.ok)
    );

    let replay_factory = MockFactory::new()
        .on("good", Behaviour::Succeed("G2".into()))
        .on("bad", Behaviour::Succeed("B2".into()));
    let (replay_engine, replay_probe) = workflow_engine(
        replay_factory,
        MockResolver::with_targets(&["good", "bad"]),
        &journal_dir,
    );
    let replay = replay_engine
        .replay(source.wf_run_id.as_str(), &wf, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(replay.status, WorkflowRunStatus::Completed);
    assert_eq!(replay_probe.call_count("good"), 1);
    assert_eq!(replay_probe.call_count("bad"), 1);
    assert!(
        replay
            .journal
            .replay
            .as_ref()
            .unwrap()
            .reused_step_ids
            .is_empty()
    );
}

#[tokio::test]
async fn workflow_run_preserves_prepare_and_execute_validation_phases() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "single-validation"
        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let factory = factory_ok();
    let engine = WorkflowEngine::new(
        dispatcher(factory.into_arc()),
        Arc::new(CountingResolver {
            calls: Arc::clone(&resolver_calls),
            inner: MockResolver::with_targets(&["ok"]),
        }),
        dir.path().join("journals"),
    );

    engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn tampered_plan_is_rejected_before_resolution_or_dispatch() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "tamper"
        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let plan = wf.compile_plan().unwrap();
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let factory = factory_ok();
    let probe = factory.probe();
    let engine = WorkflowEngine::new(
        dispatcher(factory.into_arc()),
        Arc::new(CountingResolver {
            calls: Arc::clone(&resolver_calls),
            inner: MockResolver::with_targets(&["ok"]),
        }),
        journal_dir,
    );
    let run_id = WorkflowRunId::generate();
    engine
        .prepare_plan_with_id(run_id.clone(), &plan, BTreeMap::new())
        .unwrap();
    resolver_calls.store(0, Ordering::SeqCst);
    let mut tampered = plan;
    tampered.steps[0].prompt_template = "changed".into();

    let error = engine
        .run_prepared_plan(run_id, &tampered, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("digest does not match"));
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.call_count("ok"), 0);
}

#[tokio::test]
async fn on_error_abort_stops_new_scheduling() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "abort"
        max_concurrency = 1

        [[step]]
        id = "a"
        target = "a"
        prompt = "a"

        [[step]]
        id = "b"
        target = "b"
        prompt = "b"
        "#,
    );
    let factory = MockFactory::new()
        .on("a", Behaviour::Fail(ErrorKind::Config))
        .on("b", Behaviour::Succeed("B".to_string()));
    let (engine, probe) = workflow_engine(
        factory,
        MockResolver::with_targets(&["a", "b"]),
        &dir.path().join("journals"),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::Failed);
    assert_eq!(outcome.journal.steps["a"].status, JournalStepStatus::Failed);
    assert_eq!(
        outcome.journal.steps["b"].status,
        JournalStepStatus::Skipped
    );
    assert_eq!(probe.call_count("b"), 0);
}

#[tokio::test]
async fn on_error_continue_skips_dependents_and_runs_independent_branch() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "continue"
        max_concurrency = 2

        [[step]]
        id = "a"
        target = "a"
        prompt = "a"
        on_error = "continue"

        [[step]]
        id = "b"
        needs = ["a"]
        target = "b"
        prompt = "b"

        [[step]]
        id = "c"
        target = "c"
        prompt = "c"
        "#,
    );
    let factory = MockFactory::new()
        .on("a", Behaviour::Fail(ErrorKind::Config))
        .on("b", Behaviour::Succeed("B".to_string()))
        .on("c", Behaviour::Succeed("C".to_string()));
    let (engine, probe) = workflow_engine(
        factory,
        MockResolver::with_targets(&["a", "b", "c"]),
        &dir.path().join("journals"),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::CompletedWithFailures);
    assert_eq!(outcome.journal.steps["a"].status, JournalStepStatus::Failed);
    assert_eq!(
        outcome.journal.steps["b"].status,
        JournalStepStatus::Skipped
    );
    assert_eq!(
        outcome.journal.steps["c"].status,
        JournalStepStatus::Success
    );
    assert_eq!(probe.call_count("b"), 0);
    assert_eq!(probe.call_count("c"), 1);
}

#[tokio::test]
async fn fan_out_partial_failure_succeeds_and_records_only_success_outputs_for_templates() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "fan"

        [[step]]
        id = "fan"
        fan_out = ["a", "b", "c"]
        prompt = "fan"

        [[step]]
        id = "after"
        needs = ["fan"]
        target = "after"
        prompt = "{{steps.fan.outputs}}"
        "#,
    );
    let factory = MockFactory::new()
        .on("a", Behaviour::Succeed("A".to_string()))
        .on("b", Behaviour::Fail(ErrorKind::Config))
        .on("c", Behaviour::Succeed("C".to_string()))
        .on("after", Behaviour::Succeed("after".to_string()));
    let (engine, probe) = workflow_engine(
        factory,
        MockResolver::with_targets(&["a", "b", "c", "after"]),
        &dir.path().join("journals"),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::Completed);
    let outputs = outcome.journal.steps["fan"].outputs.as_ref().unwrap();
    assert_eq!(outputs.iter().filter(|output| output.ok).count(), 2);
    assert_eq!(outputs.iter().filter(|output| !output.ok).count(), 1);
    let after_prompt = probe
        .prompts()
        .into_iter()
        .find(|prompt| prompt.contains("## a"))
        .unwrap();
    assert!(after_prompt.contains("## a\nA"));
    assert!(after_prompt.contains("## c\nC"));
    assert!(!after_prompt.contains("## b"));
}

#[tokio::test]
async fn cancellation_mid_flight_marks_journal_cancelled() {
    let dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "cancel"

        [[step]]
        id = "a"
        target = "a"
        prompt = "a"

        [[step]]
        id = "b"
        needs = ["a"]
        target = "b"
        prompt = "b"
        "#,
    );
    let (engine, _probe) = workflow_engine(
        MockFactory::new().on("a", Behaviour::Hang),
        MockResolver::with_targets(&["a", "b"]),
        &dir.path().join("journals"),
    );
    let cancel = CancellationToken::new();
    let child = cancel.clone();
    let handle = tokio::spawn(async move { engine.run(&wf, BTreeMap::new(), child).await });
    tokio::task::yield_now().await;
    cancel.cancel();
    let outcome = handle.await.unwrap().unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::Cancelled);
    assert_eq!(
        outcome.journal.steps["a"].status,
        JournalStepStatus::Cancelled
    );
    assert_eq!(
        outcome.journal.steps["b"].status,
        JournalStepStatus::Cancelled
    );
}

/// A panic inside a step must propagate to the workflow caller instead of being
/// converted through a detached JoinHandle into a fake `<join-error>` step. The
/// old path could leave the real step in `running`, exhaust the futures set, and
/// then incorrectly persist the whole workflow as `completed`.
#[tokio::test]
async fn step_panic_is_never_misreported_as_completed() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "panic"

        [[step]]
        id = "boom"
        target = "boom"
        prompt = "boom"
        "#,
    );
    let (engine, _probe) = workflow_engine(
        MockFactory::new().on("boom", Behaviour::Panic),
        MockResolver::with_targets(&["boom"]),
        &journal_dir,
    );

    let handle = tokio::spawn(async move {
        engine
            .run(&wf, BTreeMap::new(), CancellationToken::new())
            .await
    });
    let error = handle.await.expect_err("step panic must propagate");
    assert!(error.is_panic());

    let journals = vyane_workflow::list_journals(&journal_dir).unwrap();
    assert_eq!(journals.len(), 1);
    assert_eq!(journals[0].status, WorkflowRunStatus::Running);
}

#[tokio::test]
async fn resume_skips_successes_reruns_failed_and_refuses_changed_hash() {
    let dir = TempDir::new().unwrap();
    let path = write_workflow(
        &dir,
        r#"
        [workflow]
        name = "resume"

        [[step]]
        id = "a"
        target = "a"
        prompt = "a"

        [[step]]
        id = "b"
        needs = ["a"]
        target = "b"
        prompt = "{{steps.a.output}}"
        on_error = "continue"

        [[step]]
        id = "c"
        needs = ["b"]
        target = "c"
        prompt = "c"
        "#,
    );
    let wf = Workflow::from_path(&path).unwrap();
    let journal_dir = dir.path().join("journals");

    let factory = MockFactory::new()
        .on("a", Behaviour::Succeed("A".to_string()))
        .on("b", Behaviour::Fail(ErrorKind::Config))
        .on("c", Behaviour::Succeed("C".to_string()));
    let (engine, probe) = workflow_engine(
        factory,
        MockResolver::with_targets(&["a", "b", "c"]),
        &journal_dir,
    );
    let first = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first.status, WorkflowRunStatus::CompletedWithFailures);
    assert_eq!(probe.call_count("a"), 1);
    assert_eq!(probe.call_count("b"), 1);

    let factory2 = MockFactory::new()
        .on("a", Behaviour::Succeed("A2".to_string()))
        .on("b", Behaviour::Succeed("B".to_string()))
        .on("c", Behaviour::Succeed("C".to_string()));
    let (engine2, probe2) = workflow_engine(
        factory2,
        MockResolver::with_targets(&["a", "b", "c"]),
        &journal_dir,
    );
    let resumed = engine2
        .resume(&first.wf_run_id, &wf, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(resumed.status, WorkflowRunStatus::Completed);
    assert_eq!(probe2.call_count("a"), 0, "successful step reused");
    assert_eq!(probe2.call_count("b"), 1);
    assert_eq!(probe2.call_count("c"), 1);
    assert_eq!(
        resumed.journal.steps["a"].output.as_deref(),
        Some("A"),
        "resume reused recorded output"
    );

    std::fs::write(
        &path,
        std::fs::read_to_string(&path).unwrap() + "\n# changed\n",
    )
    .unwrap();
    let changed = Workflow::from_path(&path).unwrap();
    let err = engine2
        .resume(&first.wf_run_id, &changed, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("workflow file hash changed"));
}

#[tokio::test]
async fn prepared_journal_rejects_route_effort_drift_before_dispatch() {
    let dir = TempDir::new().unwrap();
    let path = write_workflow(
        &dir,
        r#"
        [workflow]
        name = "effort-replay-freeze"

        [[step]]
        id = "only"
        target = "auto"
        prompt = "run"
        [step.route]
        effort = "high"
        "#,
    );
    let original = Workflow::from_path(&path).unwrap();
    let journal_dir = dir.path().join("journals");
    let factory = MockFactory::new();
    let probe = factory.probe();
    let engine = WorkflowEngine::new(
        dispatcher(factory.into_arc()),
        Arc::new(DeferredResolver),
        journal_dir,
    );
    let run_id = WorkflowRunId::generate();
    engine
        .prepare_run_with_id(run_id.clone(), &original, BTreeMap::new())
        .unwrap();

    let changed_source = std::fs::read_to_string(&path)
        .unwrap()
        .replace("effort = \"high\"", "effort = \"low\"");
    std::fs::write(&path, changed_source).unwrap();
    let changed = Workflow::from_path(&path).unwrap();
    let error = engine
        .run_prepared(run_id, &changed, CancellationToken::new())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("workflow file hash changed"));
    assert_eq!(probe.call_count("auto"), 0);
}

#[tokio::test]
async fn resume_accepts_an_exact_legacy_hash_once_and_migrates_it_to_v1() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "legacy-migration"

        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let legacy = wf
        .legacy_file_sha256
        .clone()
        .expect("filesystem workflows carry their exact legacy hash");
    assert_ne!(legacy, wf.file_sha256);
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        &journal_dir,
    );
    let run_id = WorkflowRunId::generate();
    engine
        .prepare_run_with_id(run_id.clone(), &wf, BTreeMap::new())
        .unwrap();
    let path = journal_dir.join(format!("{run_id}.json"));
    let mut journal: vyane_workflow::WorkflowJournal =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    journal.file_sha256 = legacy;
    journal.plan_sha256 = None;
    std::fs::write(&path, serde_json::to_vec_pretty(&journal).unwrap()).unwrap();

    let outcome = engine
        .resume(run_id.as_str(), &wf, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.status, WorkflowRunStatus::Completed);
    assert_eq!(probe.call_count("ok"), 1);
    let migrated = vyane_workflow::read_journal(&journal_dir, run_id.as_str()).unwrap();
    assert_eq!(migrated.file_sha256, wf.file_sha256);
    assert_eq!(
        migrated.plan_sha256,
        Some(wf.compile_plan().unwrap().plan_sha256)
    );
}

#[tokio::test]
async fn caller_supplied_run_id_is_the_persisted_journal_identity() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "caller-id"

        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        &journal_dir,
    );
    let run_id: WorkflowRunId = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e".parse().unwrap();

    let outcome = engine
        .run_with_id(
            run_id.clone(),
            &wf,
            BTreeMap::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(outcome.wf_run_id, run_id);
    assert_eq!(outcome.journal.wf_run_id, outcome.wf_run_id);
    assert_eq!(
        outcome
            .journal_path
            .file_name()
            .and_then(|name| name.to_str()),
        Some("01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e.json")
    );
    let persisted = vyane_workflow::read_journal(&journal_dir, outcome.wf_run_id.as_str()).unwrap();
    assert_eq!(persisted.wf_run_id, outcome.wf_run_id);

    let before = std::fs::read(&outcome.journal_path).unwrap();
    let error = engine
        .run_with_id(run_id, &wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        vyane_workflow::WorkflowError::JournalAlreadyExists { .. }
    ));
    assert_eq!(probe.call_count("ok"), 1);
    assert_eq!(std::fs::read(&outcome.journal_path).unwrap(), before);
}

#[tokio::test]
async fn resume_rejects_traversal_before_reading_a_journal_path() {
    let dir = TempDir::new().unwrap();
    let journal_dir = dir.path().join("journals");
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "resume-id"

        [[step]]
        id = "only"
        target = "ok"
        prompt = "run"
        "#,
    );
    let (engine, _) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        &journal_dir,
    );

    let error = engine
        .resume("../outside", &wf, CancellationToken::new())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        vyane_workflow::WorkflowError::InvalidRunId { .. }
    ));
    assert!(!journal_dir.exists());
}

/// Helper: a MockFactory that returns "OK" for any model.
fn factory_ok() -> MockFactory {
    MockFactory::new().on("ok", Behaviour::Succeed("OK".into()))
}

/// A single-step workflow runs to completion and records its output.
#[tokio::test]
async fn single_step_workflow_completes() {
    let dir = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "single"

        [[step]]
        id = "only"
        target = "ok"
        prompt = "just do it"
        "#,
    );
    let (engine, _probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        journal_dir.path(),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.status, WorkflowRunStatus::Completed);
    assert!(outcome.journal.steps.contains_key("only"));
    assert_eq!(outcome.journal.steps["only"].output.as_deref(), Some("OK"));
}

/// A linear two-step workflow passes output via template substitution.
#[tokio::test]
async fn linear_two_step_passes_output_via_template() {
    let dir = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "linear"

        [[step]]
        id = "first"
        target = "ok"
        prompt = "step one"

        [[step]]
        id = "second"
        needs = ["first"]
        target = "ok"
        prompt = "got: {{steps.first.output}}"
        "#,
    );
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        journal_dir.path(),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.status, WorkflowRunStatus::Completed);

    let prompts = probe.prompts();
    assert!(prompts.len() >= 2);
    assert!(
        prompts.iter().any(|p| p.contains("got: OK")),
        "second step received first output"
    );
}

/// With max_concurrency = 1, parallel-capable steps run strictly sequentially.
#[tokio::test]
async fn max_concurrency_1_runs_strictly_sequential() {
    let dir = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "serial"
        max_concurrency = 1

        [[step]]
        id = "a"
        target = "ok"
        prompt = "a"

        [[step]]
        id = "b"
        target = "ok"
        prompt = "b"

        [[step]]
        id = "c"
        target = "ok"
        prompt = "c"
        "#,
    );
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        journal_dir.path(),
    );

    let outcome = engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.status, WorkflowRunStatus::Completed);
    assert_eq!(probe.max_concurrent(), 1, "no two steps ran concurrently");
}

/// Workflow variables are substituted into prompts.
#[tokio::test]
async fn workflow_variables_substitute_in_prompts() {
    let dir = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "vars-test"

        [[step]]
        id = "step1"
        target = "ok"
        prompt = "do {{vars.action}} on {{vars.target}}"
        "#,
    );
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        journal_dir.path(),
    );

    let mut vars = BTreeMap::new();
    vars.insert("action".into(), "deploy".into());
    vars.insert("target".into(), "production".into());

    engine
        .run(&wf, vars, CancellationToken::new())
        .await
        .unwrap();

    let prompts = probe.prompts();
    assert!(
        prompts
            .iter()
            .any(|p| p.contains("deploy") && p.contains("production")),
        "variables were substituted: {:?}",
        prompts
    );
}

/// `{{workflow.name}}` placeholder resolves to the workflow's name.
#[tokio::test]
async fn workflow_name_placeholder_resolves() {
    let dir = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();
    let wf = workflow_from(
        &dir,
        r#"
        [workflow]
        name = "my-pipeline"

        [[step]]
        id = "s"
        target = "ok"
        prompt = "running {{workflow.name}}"
        "#,
    );
    let (engine, probe) = workflow_engine(
        factory_ok(),
        MockResolver::with_targets(&["ok"]),
        journal_dir.path(),
    );

    engine
        .run(&wf, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();

    let prompts = probe.prompts();
    assert!(
        prompts.iter().any(|p| p.contains("running my-pipeline")),
        "workflow name was substituted: {:?}",
        prompts
    );
}
