use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use vyane_core::{CancellationToken, HarnessLifecycleReporter, RunStatus, TaskSpec};
use vyane_kernel::Dispatcher;

use crate::error::{WorkflowError, WorkflowResult};
use crate::journal::{
    JournalStep, JournalStepStatus, JournalTargetOutput, WorkflowJournal, WorkflowRunId,
    journal_path, read_journal, write_journal_atomic, write_journal_create_atomic,
};
use crate::model::{OnError, Workflow, WorkflowOutcome, WorkflowRunStatus};
use crate::plan::{WorkflowPlan, WorkflowPlanStep};
use crate::template::render_template_inner;
use crate::validate::{ResolvedStepTargets, TargetResolver, ValidatedWorkflow, validate_plan};

#[derive(Debug, Clone)]
pub enum StepEvent {
    Started {
        step_id: String,
    },
    Succeeded {
        step_id: String,
        duration: Duration,
    },
    Failed {
        step_id: String,
        duration: Duration,
        error: String,
    },
    Skipped {
        step_id: String,
        reason: String,
    },
    Cancelled {
        step_id: String,
        duration: Duration,
    },
}

pub trait WorkflowObserver: Send + Sync {
    fn on_event(&self, event: StepEvent);
}

#[derive(Clone)]
pub struct WorkflowEngine {
    dispatcher: Arc<Dispatcher>,
    resolver: Arc<dyn TargetResolver>,
    journal_dir: PathBuf,
    observer: Option<Arc<dyn WorkflowObserver>>,
    harness_lifecycle_reporter: Option<HarnessLifecycleReporter>,
}

impl WorkflowEngine {
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        resolver: Arc<dyn TargetResolver>,
        journal_dir: PathBuf,
    ) -> Self {
        Self {
            dispatcher,
            resolver,
            journal_dir,
            observer: None,
            harness_lifecycle_reporter: None,
        }
    }

    pub fn with_observer(mut self, observer: Arc<dyn WorkflowObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Publish every CLI-harness sentinel owned by this workflow before its
    /// start gate is released. Direct-HTTP steps carry the reporter harmlessly
    /// but never invoke it.
    pub fn with_harness_lifecycle_reporter(mut self, reporter: HarnessLifecycleReporter) -> Self {
        self.harness_lifecycle_reporter = Some(reporter);
        self
    }

    pub async fn run(
        &self,
        wf: &Workflow,
        vars: BTreeMap<String, String>,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let plan = wf.compile_plan()?;
        self.run_plan(&plan, vars, cancel).await
    }

    pub async fn run_plan(
        &self,
        plan: &WorkflowPlan,
        vars: BTreeMap<String, String>,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        self.run_plan_with_id(WorkflowRunId::generate(), plan, vars, cancel)
            .await
    }

    /// Starts a workflow with an identity allocated by the caller.
    ///
    /// The supplied identity is also the journal identity, allowing a durable
    /// task owner to allocate one UUIDv7 and use it end-to-end.
    pub async fn run_with_id(
        &self,
        wf_run_id: WorkflowRunId,
        wf: &Workflow,
        vars: BTreeMap<String, String>,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let plan = wf.compile_plan()?;
        self.run_plan_with_id(wf_run_id, &plan, vars, cancel).await
    }

    pub async fn run_plan_with_id(
        &self,
        wf_run_id: WorkflowRunId,
        plan: &WorkflowPlan,
        vars: BTreeMap<String, String>,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        self.prepare_plan_with_id(wf_run_id.clone(), plan, vars)?;
        self.run_prepared_plan(wf_run_id, plan, cancel).await
    }

    /// Validate a caller-owned workflow and durably create its initial journal.
    ///
    /// Resident supervisors use this split phase to make the journal durable
    /// before publishing and releasing a tracked execution future. Calling it
    /// twice for the same id remains an error: prepared runs are never replayed
    /// implicitly.
    pub fn prepare_run_with_id(
        &self,
        wf_run_id: WorkflowRunId,
        wf: &Workflow,
        vars: BTreeMap<String, String>,
    ) -> WorkflowResult<()> {
        let plan = wf.compile_plan()?;
        self.prepare_plan_with_id(wf_run_id, &plan, vars)
    }

    pub fn prepare_plan_with_id(
        &self,
        wf_run_id: WorkflowRunId,
        plan: &WorkflowPlan,
        vars: BTreeMap<String, String>,
    ) -> WorkflowResult<()> {
        validate_plan(plan, &vars, self.resolver.as_ref())?;
        let mut journal = WorkflowJournal::new_with_plan(wf_run_id, plan, vars);
        write_journal_create_atomic(&self.journal_dir, &mut journal)
    }

    /// Execute a pristine journal created by [`Self::prepare_run_with_id`].
    ///
    /// This is deliberately not a resume API. Exact identity, v1 source hash,
    /// and pristine step state are revalidated before any target dispatch.
    pub async fn run_prepared(
        &self,
        wf_run_id: WorkflowRunId,
        wf: &Workflow,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let plan = wf.compile_plan()?;
        let mut journal = read_journal(&self.journal_dir, wf_run_id.as_str())?;
        validate_and_migrate_resume_hash(&mut journal, &plan.source_sha256, wf)?;
        migrate_or_validate_plan_binding(&mut journal, &plan)?;
        self.run_prepared_plan_from_journal(journal, &plan, cancel)
            .await
    }

    pub async fn run_prepared_plan(
        &self,
        wf_run_id: WorkflowRunId,
        plan: &WorkflowPlan,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let journal = read_journal(&self.journal_dir, wf_run_id.as_str())?;
        validate_journal_plan_binding(&journal, plan)?;
        self.run_prepared_plan_from_journal(journal, plan, cancel)
            .await
    }

    async fn run_prepared_plan_from_journal(
        &self,
        journal: WorkflowJournal,
        plan: &WorkflowPlan,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let wf_run_id = &journal.wf_run_id;
        let expected_steps = plan
            .steps
            .iter()
            .map(|step| step.id.as_str())
            .collect::<BTreeSet<_>>();
        let pristine = journal.status == WorkflowRunStatus::Running
            && journal.steps.len() == expected_steps.len()
            && journal.steps.iter().all(|(id, step)| {
                expected_steps.contains(id.as_str())
                    && step.status == JournalStepStatus::Pending
                    && step.run_ids.is_empty()
                    && step.output.is_none()
                    && step.outputs.is_none()
                    && step.error.is_none()
            });
        if !pristine {
            return Err(WorkflowError::validation(vec![format!(
                "prepared workflow journal `{wf_run_id}` is not pristine"
            )]));
        }
        let validated = validate_plan(plan, &journal.vars, self.resolver.as_ref())?;
        self.execute(plan, &validated, journal, cancel).await
    }

    pub async fn resume(
        &self,
        wf_run_id: &str,
        wf: &Workflow,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let mut journal = read_journal(&self.journal_dir, wf_run_id)?;
        let plan = wf.compile_plan()?;
        validate_and_migrate_resume_hash(&mut journal, &plan.source_sha256, wf)?;
        // Legacy journals may predate plan digests. Once a source-identical
        // plan is compiled, bind it atomically with the resume rewrite.
        migrate_or_validate_plan_binding(&mut journal, &plan)?;
        self.resume_plan_from_journal(journal, &plan, cancel).await
    }

    pub async fn resume_plan(
        &self,
        wf_run_id: &str,
        plan: &WorkflowPlan,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let journal = read_journal(&self.journal_dir, wf_run_id)?;
        validate_journal_plan_binding(&journal, plan)?;
        self.resume_plan_from_journal(journal, plan, cancel).await
    }

    async fn resume_plan_from_journal(
        &self,
        mut journal: WorkflowJournal,
        plan: &WorkflowPlan,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let validated = validate_plan(plan, &journal.vars, self.resolver.as_ref())?;
        journal.status = WorkflowRunStatus::Running;
        for step in journal.steps.values_mut() {
            step.reset_for_rerun();
        }
        for step in &plan.steps {
            journal
                .steps
                .entry(step.id.clone())
                .or_insert_with(JournalStep::pending);
        }
        journal.plan_sha256 = Some(plan.plan_sha256.clone());
        write_journal_atomic(&self.journal_dir, &mut journal)?;
        self.execute(plan, &validated, journal, cancel).await
    }

    async fn execute(
        &self,
        wf: &WorkflowPlan,
        validated: &ValidatedWorkflow,
        mut journal: WorkflowJournal,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let step_by_id = wf
            .steps
            .iter()
            .map(|step| (step.id.clone(), step))
            .collect::<BTreeMap<_, _>>();
        let order_index = validated
            .topo_order
            .iter()
            .enumerate()
            .map(|(index, id)| (id.clone(), index))
            .collect::<BTreeMap<_, _>>();
        let mut completed = journal
            .steps
            .iter()
            .filter_map(|(id, step)| {
                (step.status == JournalStepStatus::Success).then_some(id.clone())
            })
            .collect::<BTreeSet<_>>();
        let mut terminal = journal
            .steps
            .iter()
            .filter_map(|(id, step)| step.status.is_terminal().then_some(id.clone()))
            .collect::<BTreeSet<_>>();
        let mut running = BTreeSet::<String>::new();
        let mut aborting = false;
        let mut had_failures = journal.steps.values().any(|step| {
            matches!(
                step.status,
                JournalStepStatus::Failed
                    | JournalStepStatus::Skipped
                    | JournalStepStatus::Cancelled
            )
        });
        let semaphore = Arc::new(Semaphore::new(wf.max_concurrency.max(1) as usize));
        let mut futures = FuturesUnordered::new();

        loop {
            if cancel.is_cancelled() && !aborting {
                aborting = true;
                had_failures = true;
                self.cancel_pending(&mut journal, &terminal, &running, "cancelled by caller")?;
            }

            if !aborting {
                let ready = ready_steps(validated, &completed, &terminal, &running, &order_index);
                let available_permits = semaphore.available_permits();
                for step_id in ready.into_iter().take(available_permits) {
                    if cancel.is_cancelled() {
                        aborting = true;
                        had_failures = true;
                        self.cancel_pending(
                            &mut journal,
                            &terminal,
                            &running,
                            "cancelled by caller",
                        )?;
                        break;
                    }
                    let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => break,
                    };
                    let step = step_by_id
                        .get(&step_id)
                        .expect("validated step id should exist");
                    let resolved = validated
                        .resolved_targets
                        .get(&step_id)
                        .expect("validated step target should exist")
                        .clone();
                    let prompt = render_template_inner(
                        &step.prompt_template,
                        &wf.name,
                        &journal.vars,
                        &journal.steps,
                    )?;
                    if let Some(state) = journal.steps.get_mut(&step_id) {
                        state.status = JournalStepStatus::Running;
                        state.error = None;
                    }
                    write_journal_atomic(&self.journal_dir, &mut journal)?;
                    self.emit(StepEvent::Started {
                        step_id: step_id.clone(),
                    });
                    running.insert(step_id.clone());

                    let dispatcher = Arc::clone(&self.dispatcher);
                    let resolver = Arc::clone(&self.resolver);
                    let cancel = cancel.clone();
                    let task = task_for_step(step, prompt, self.harness_lifecycle_reporter.clone());
                    futures.push(async move {
                        let _permit = permit;
                        let started = std::time::Instant::now();
                        let result =
                            run_step_dispatch(dispatcher, resolver, task, resolved, cancel).await;
                        FinishedStep {
                            step_id,
                            duration: started.elapsed(),
                            result,
                        }
                    });
                }
            }

            if running.is_empty() {
                break;
            }

            let Some(finished) = futures.next().await else {
                break;
            };
            running.remove(&finished.step_id);

            match finished.result {
                StepDispatchResult::Succeeded {
                    run_ids,
                    output,
                    outputs,
                } => {
                    completed.insert(finished.step_id.clone());
                    terminal.insert(finished.step_id.clone());
                    if let Some(state) = journal.steps.get_mut(&finished.step_id) {
                        state.status = JournalStepStatus::Success;
                        state.run_ids = run_ids;
                        state.output = output;
                        state.outputs = outputs;
                        state.error = None;
                    }
                    write_journal_atomic(&self.journal_dir, &mut journal)?;
                    self.emit(StepEvent::Succeeded {
                        step_id: finished.step_id,
                        duration: finished.duration,
                    });
                }
                StepDispatchResult::Failed {
                    run_ids,
                    output,
                    outputs,
                    error,
                    cancelled,
                } => {
                    had_failures = true;
                    terminal.insert(finished.step_id.clone());
                    if cancelled {
                        aborting = true;
                    }
                    if let Some(state) = journal.steps.get_mut(&finished.step_id) {
                        state.status = if cancelled {
                            JournalStepStatus::Cancelled
                        } else {
                            JournalStepStatus::Failed
                        };
                        state.run_ids = run_ids;
                        state.output = output;
                        state.outputs = outputs;
                        state.error = Some(error.clone());
                    }
                    let on_error = step_by_id
                        .get(&finished.step_id)
                        .map(|step| step.on_error)
                        .unwrap_or(OnError::Abort);
                    if cancelled || on_error == OnError::Abort {
                        aborting = true;
                        self.mark_unscheduled_terminal(
                            &mut journal,
                            &terminal,
                            &running,
                            if cancelled {
                                JournalStepStatus::Cancelled
                            } else {
                                JournalStepStatus::Skipped
                            },
                            if cancelled {
                                "cancelled by caller"
                            } else {
                                "workflow aborted after failed step"
                            },
                        )?;
                    } else {
                        let skipped = skip_dependents(
                            &finished.step_id,
                            validated,
                            &completed,
                            &mut terminal,
                            &running,
                        );
                        for skipped_id in skipped {
                            if let Some(state) = journal.steps.get_mut(&skipped_id) {
                                state.status = JournalStepStatus::Skipped;
                                state.error =
                                    Some(format!("dependency `{}` failed", finished.step_id));
                            }
                            terminal.insert(skipped_id.clone());
                            self.emit(StepEvent::Skipped {
                                step_id: skipped_id,
                                reason: format!("dependency `{}` failed", finished.step_id),
                            });
                        }
                    }
                    write_journal_atomic(&self.journal_dir, &mut journal)?;
                    if cancelled {
                        self.emit(StepEvent::Cancelled {
                            step_id: finished.step_id,
                            duration: finished.duration,
                        });
                    } else {
                        self.emit(StepEvent::Failed {
                            step_id: finished.step_id,
                            duration: finished.duration,
                            error,
                        });
                    }
                }
            }
        }

        if cancel.is_cancelled() {
            self.mark_unscheduled_terminal(
                &mut journal,
                &terminal,
                &running,
                JournalStepStatus::Cancelled,
                "cancelled by caller",
            )?;
            journal.status = WorkflowRunStatus::Cancelled;
        } else if journal
            .steps
            .values()
            .any(|step| step.status == JournalStepStatus::Cancelled)
        {
            journal.status = WorkflowRunStatus::Cancelled;
        } else if aborting
            && journal
                .steps
                .values()
                .any(|step| step.status == JournalStepStatus::Failed)
        {
            journal.status = WorkflowRunStatus::Failed;
        } else if had_failures
            || journal.steps.values().any(|step| {
                matches!(
                    step.status,
                    JournalStepStatus::Failed | JournalStepStatus::Skipped
                )
            })
        {
            journal.status = WorkflowRunStatus::CompletedWithFailures;
        } else {
            journal.status = WorkflowRunStatus::Completed;
        }

        write_journal_atomic(&self.journal_dir, &mut journal)?;
        Ok(WorkflowOutcome {
            wf_run_id: journal.wf_run_id.clone(),
            status: journal.status,
            journal_path: journal_path(&self.journal_dir, &journal.wf_run_id),
            journal,
        })
    }

    fn emit(&self, event: StepEvent) {
        if let Some(observer) = self.observer.as_ref() {
            observer.on_event(event);
        }
    }

    fn cancel_pending(
        &self,
        journal: &mut WorkflowJournal,
        terminal: &BTreeSet<String>,
        running: &BTreeSet<String>,
        reason: &str,
    ) -> WorkflowResult<()> {
        self.mark_unscheduled_terminal(
            journal,
            terminal,
            running,
            JournalStepStatus::Cancelled,
            reason,
        )
    }

    fn mark_unscheduled_terminal(
        &self,
        journal: &mut WorkflowJournal,
        terminal: &BTreeSet<String>,
        running: &BTreeSet<String>,
        status: JournalStepStatus,
        reason: &str,
    ) -> WorkflowResult<()> {
        for (id, state) in &mut journal.steps {
            if terminal.contains(id)
                || running.contains(id)
                || state.status == JournalStepStatus::Success
            {
                continue;
            }
            state.status = status;
            state.error = Some(reason.to_string());
            match status {
                JournalStepStatus::Skipped => self.emit(StepEvent::Skipped {
                    step_id: id.clone(),
                    reason: reason.to_string(),
                }),
                JournalStepStatus::Cancelled => self.emit(StepEvent::Cancelled {
                    step_id: id.clone(),
                    duration: Duration::ZERO,
                }),
                JournalStepStatus::Pending
                | JournalStepStatus::Running
                | JournalStepStatus::Success
                | JournalStepStatus::Failed => {}
            }
        }
        Ok(())
    }
}

fn validate_and_migrate_resume_hash(
    journal: &mut WorkflowJournal,
    expected_source_sha256: &str,
    workflow: &Workflow,
) -> WorkflowResult<()> {
    let matches_v1 = journal.file_sha256 == expected_source_sha256;
    let matches_legacy = workflow
        .legacy_file_sha256
        .as_deref()
        .is_some_and(|legacy| journal.file_sha256 == legacy);
    if !matches_v1 && !matches_legacy {
        return Err(WorkflowError::WorkflowHashChanged {
            expected: journal.file_sha256.clone(),
            actual: expected_source_sha256.to_string(),
        });
    }
    // A legacy hash is accepted only after exact comparison. The caller writes
    // this mutation with the first atomic resume rewrite; new journals always
    // start on v1.
    journal.file_sha256 = expected_source_sha256.to_string();
    Ok(())
}

fn validate_journal_plan_binding(
    journal: &WorkflowJournal,
    plan: &WorkflowPlan,
) -> WorkflowResult<()> {
    plan.verify()?;
    if journal.file_sha256 != plan.source_sha256 {
        return Err(WorkflowError::WorkflowHashChanged {
            expected: journal.file_sha256.clone(),
            actual: plan.source_sha256.clone(),
        });
    }
    let Some(expected) = journal.plan_sha256.as_deref() else {
        return Err(WorkflowError::validation(vec![
            "workflow journal is not bound to a plan digest".into(),
        ]));
    };
    if expected != plan.plan_sha256 {
        return Err(WorkflowError::validation(vec![
            "workflow plan digest changed for continuation".into(),
        ]));
    }
    Ok(())
}

fn migrate_or_validate_plan_binding(
    journal: &mut WorkflowJournal,
    plan: &WorkflowPlan,
) -> WorkflowResult<()> {
    plan.verify()?;
    match journal.plan_sha256.as_deref() {
        Some(expected) if expected != plan.plan_sha256 => Err(WorkflowError::validation(vec![
            "workflow plan digest changed for continuation".into(),
        ])),
        Some(_) => Ok(()),
        None => {
            journal.plan_sha256 = Some(plan.plan_sha256.clone());
            Ok(())
        }
    }
}

struct FinishedStep {
    step_id: String,
    duration: Duration,
    result: StepDispatchResult,
}

enum StepDispatchResult {
    Succeeded {
        run_ids: Vec<String>,
        output: Option<String>,
        outputs: Option<Vec<JournalTargetOutput>>,
    },
    Failed {
        run_ids: Vec<String>,
        output: Option<String>,
        outputs: Option<Vec<JournalTargetOutput>>,
        error: String,
        cancelled: bool,
    },
}

async fn run_step_dispatch(
    dispatcher: Arc<Dispatcher>,
    resolver: Arc<dyn TargetResolver>,
    mut task: TaskSpec,
    resolved: ResolvedStepTargets,
    cancel: CancellationToken,
) -> StepDispatchResult {
    match resolved {
        ResolvedStepTargets::Single { target, chain } => {
            let chain = match chain {
                Some(chain) => chain,
                None => match resolver.resolve_for_task(&target, &mut task) {
                    Ok(chain) if !chain.is_empty() => chain,
                    Ok(_) => {
                        return StepDispatchResult::Failed {
                            run_ids: Vec::new(),
                            output: None,
                            outputs: None,
                            error: format!("deferred target `{target}` resolved to an empty chain"),
                            cancelled: false,
                        };
                    }
                    Err(error) => {
                        return StepDispatchResult::Failed {
                            run_ids: Vec::new(),
                            output: None,
                            outputs: None,
                            error: error.to_string(),
                            cancelled: error.kind == vyane_core::ErrorKind::Cancelled,
                        };
                    }
                },
            };
            match dispatcher.dispatch(&task, chain, cancel).await {
                Ok(outcome) if outcome.record.status == RunStatus::Success => {
                    StepDispatchResult::Succeeded {
                        run_ids: vec![outcome.record.run_id],
                        output: outcome.output,
                        outputs: None,
                    }
                }
                Ok(outcome) => StepDispatchResult::Failed {
                    run_ids: vec![outcome.record.run_id],
                    output: outcome.output,
                    outputs: None,
                    cancelled: outcome.record.status == RunStatus::Cancelled,
                    error: outcome.record.error.unwrap_or_else(|| {
                        format!("dispatch ended with {:?}", outcome.record.status)
                    }),
                },
                Err(error) => StepDispatchResult::Failed {
                    run_ids: Vec::new(),
                    output: None,
                    outputs: None,
                    error: error.to_string(),
                    cancelled: error.kind == vyane_core::ErrorKind::Cancelled,
                },
            }
        }
        ResolvedStepTargets::FanOut { targets, chains } => {
            let results = dispatcher.broadcast(&task, chains, cancel).await;
            let mut run_ids = Vec::new();
            let mut outputs = Vec::with_capacity(results.len());
            let mut success_count = 0usize;
            let mut errors = Vec::new();
            let mut cancelled = false;
            for (target, result) in targets.into_iter().zip(results) {
                match result {
                    Ok(outcome) => {
                        run_ids.push(outcome.record.run_id.clone());
                        let ok = outcome.record.status == RunStatus::Success;
                        if ok {
                            success_count += 1;
                        }
                        if outcome.record.status == RunStatus::Cancelled {
                            cancelled = true;
                        }
                        if !ok {
                            errors.push(format!(
                                "{target}: {}",
                                outcome.record.error.as_deref().unwrap_or("dispatch failed")
                            ));
                        }
                        outputs.push(JournalTargetOutput {
                            target,
                            ok,
                            output: ok.then_some(outcome.output).flatten(),
                        });
                    }
                    Err(error) => {
                        if error.kind == vyane_core::ErrorKind::Cancelled {
                            cancelled = true;
                        }
                        errors.push(format!("{target}: {error}"));
                        outputs.push(JournalTargetOutput {
                            target,
                            ok: false,
                            output: None,
                        });
                    }
                }
            }
            if success_count > 0 {
                StepDispatchResult::Succeeded {
                    run_ids,
                    output: None,
                    outputs: Some(outputs),
                }
            } else {
                StepDispatchResult::Failed {
                    run_ids,
                    output: None,
                    outputs: Some(outputs),
                    error: if errors.is_empty() {
                        "fan_out produced no successful targets".to_string()
                    } else {
                        errors.join("; ")
                    },
                    cancelled,
                }
            }
        }
    }
}

fn task_for_step(
    step: &WorkflowPlanStep,
    prompt: String,
    harness_lifecycle_reporter: Option<HarnessLifecycleReporter>,
) -> TaskSpec {
    let mut task = TaskSpec::new(prompt).with_sandbox(step.sandbox);
    task.system = step.system.clone();
    task.workdir = step.workdir.as_ref().map(PathBuf::from);
    task.timeout = step.timeout.and_then(|timeout| timeout.to_duration().ok());
    task.harness_lifecycle_reporter = harness_lifecycle_reporter;
    task.labels
        .insert("workflow.step".to_string(), step.id.clone());
    step.route.apply_to_labels(&mut task.labels);
    task
}

fn ready_steps(
    validated: &ValidatedWorkflow,
    completed: &BTreeSet<String>,
    terminal: &BTreeSet<String>,
    running: &BTreeSet<String>,
    order_index: &BTreeMap<String, usize>,
) -> Vec<String> {
    let mut ready = Vec::new();
    for id in &validated.topo_order {
        if terminal.contains(id) || running.contains(id) {
            continue;
        }
        let needs_done = validated
            .dependencies
            .get(id)
            .map(|needs| needs.iter().all(|need| completed.contains(need)))
            .unwrap_or(true);
        if needs_done {
            ready.push(id.clone());
        }
    }
    ready.sort_by_key(|id| order_index.get(id).copied().unwrap_or(usize::MAX));
    ready
}

fn skip_dependents(
    failed_id: &str,
    validated: &ValidatedWorkflow,
    completed: &BTreeSet<String>,
    terminal: &mut BTreeSet<String>,
    running: &BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut queue = VecDeque::new();
    if let Some(children) = validated.dependents.get(failed_id) {
        for child in children {
            queue.push_back(child.clone());
        }
    }
    while let Some(id) = queue.pop_front() {
        if completed.contains(&id) || running.contains(&id) || !terminal.insert(id.clone()) {
            continue;
        }
        out.push(id.clone());
        if let Some(children) = validated.dependents.get(&id) {
            for child in children {
                queue.push_back(child.clone());
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn workflow(v1: &str, legacy: Option<&str>) -> Workflow {
        Workflow {
            name: "hash-migration".into(),
            description: None,
            max_concurrency: 1,
            steps: Vec::new(),
            file_path: PathBuf::from("workflow.toml"),
            legacy_file_sha256: legacy.map(str::to_string),
            file_sha256: v1.into(),
        }
    }

    #[test]
    fn exact_legacy_resume_hash_is_migrated_to_v1() {
        let workflow = workflow("v1-hash", Some("legacy-hash"));
        let directory = tempfile::tempdir().unwrap();
        let run_id = WorkflowRunId::generate();
        let mut journal = WorkflowJournal::new_with_id(run_id.clone(), &workflow, BTreeMap::new());
        journal.file_sha256 = "legacy-hash".into();
        write_journal_create_atomic(directory.path(), &mut journal).unwrap();

        let mut journal = read_journal(directory.path(), run_id.as_str()).unwrap();

        validate_and_migrate_resume_hash(&mut journal, "v1-hash", &workflow).unwrap();
        write_journal_atomic(directory.path(), &mut journal).unwrap();
        let persisted = read_journal(directory.path(), run_id.as_str()).unwrap();

        assert_eq!(journal.file_sha256, "v1-hash");
        assert_eq!(persisted.file_sha256, "v1-hash");
    }

    #[test]
    fn resume_hash_never_accepts_an_unrelated_or_missing_legacy_hash() {
        for legacy in [Some("different-legacy"), None] {
            let workflow = workflow("v1-hash", legacy);
            let mut journal =
                WorkflowJournal::new_with_id(WorkflowRunId::generate(), &workflow, BTreeMap::new());
            journal.file_sha256 = "old-journal-hash".into();

            let error =
                validate_and_migrate_resume_hash(&mut journal, "v1-hash", &workflow).unwrap_err();

            assert!(matches!(error, WorkflowError::WorkflowHashChanged { .. }));
            assert_eq!(journal.file_sha256, "old-journal-hash");
        }
    }
}
