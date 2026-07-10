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
    Target, VyaneError,
};
use vyane_kernel::{Dispatcher, Executor, ExecutorFactory};
use vyane_workflow::{
    JournalStepStatus, JournalTargetOutput, TargetResolver, Workflow, WorkflowEngine,
    WorkflowRunStatus, render_template,
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
    async fn load(&self, _session_id: &str) -> Result<Option<SessionRecord>> {
        Ok(None)
    }

    async fn save(&self, _record: &SessionRecord) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _owner: Option<&str>) -> Result<Vec<SessionRecord>> {
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
    let wf = workflow_from(
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
    let resolver = MockResolver::with_targets(&["ok"]);
    let err = vyane_workflow::validate_workflow(&wf, &BTreeMap::new(), &resolver).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("cycle"));
    assert!(text.contains("unknown variable `missing`"));
    assert!(text.contains("exactly one of `target` or `fan_out`, not both"));
    assert!(text.contains("could not be resolved"));
    assert!(text.contains("not in its transitive needs"));
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
