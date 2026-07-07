use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use vyane_core::{CancellationToken, RunStatus, TaskSpec};
use vyane_kernel::Dispatcher;

use crate::error::{WorkflowError, WorkflowResult};
use crate::journal::{
    JournalStep, JournalStepStatus, JournalTargetOutput, WorkflowJournal, journal_path,
    read_journal, write_journal_atomic,
};
use crate::model::{OnError, Workflow, WorkflowOutcome, WorkflowRunStatus};
use crate::template::render_template_inner;
use crate::validate::{ResolvedStepTargets, TargetResolver, ValidatedWorkflow, validate_workflow};

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
        }
    }

    pub fn with_observer(mut self, observer: Arc<dyn WorkflowObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub async fn run(
        &self,
        wf: &Workflow,
        vars: BTreeMap<String, String>,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let validated = validate_workflow(wf, &vars, self.resolver.as_ref())?;
        let mut journal = WorkflowJournal::new(wf, vars);
        write_journal_atomic(&self.journal_dir, &mut journal)?;
        self.execute(wf, &validated, journal, cancel).await
    }

    pub async fn resume(
        &self,
        wf_run_id: &str,
        wf: &Workflow,
        cancel: CancellationToken,
    ) -> WorkflowResult<WorkflowOutcome> {
        let mut journal = read_journal(&self.journal_dir, wf_run_id)?;
        if journal.file_sha256 != wf.file_sha256 {
            return Err(WorkflowError::WorkflowHashChanged {
                expected: journal.file_sha256,
                actual: wf.file_sha256.clone(),
            });
        }
        let validated = validate_workflow(wf, &journal.vars, self.resolver.as_ref())?;
        journal.status = WorkflowRunStatus::Running;
        for step in journal.steps.values_mut() {
            step.reset_for_rerun();
        }
        for step in &wf.steps {
            journal
                .steps
                .entry(step.id.clone())
                .or_insert_with(JournalStep::pending);
        }
        write_journal_atomic(&self.journal_dir, &mut journal)?;
        self.execute(wf, &validated, journal, cancel).await
    }

    async fn execute(
        &self,
        wf: &Workflow,
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
        let semaphore = Arc::new(Semaphore::new(wf.max_concurrency.max(1)));
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
                        step.prompt_template.as_deref().unwrap_or_default(),
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
                    let cancel = cancel.clone();
                    let task = task_for_step(step, prompt);
                    futures.push(tokio::spawn(async move {
                        let _permit = permit;
                        let started = std::time::Instant::now();
                        let result = run_step_dispatch(dispatcher, task, resolved, cancel).await;
                        FinishedStep {
                            step_id,
                            duration: started.elapsed(),
                            result,
                        }
                    }));
                }
            }

            if running.is_empty() {
                break;
            }

            let Some(joined) = futures.next().await else {
                break;
            };
            let finished = match joined {
                Ok(finished) => finished,
                Err(error) => FinishedStep {
                    step_id: "<join-error>".to_string(),
                    duration: Duration::ZERO,
                    result: StepDispatchResult::Failed {
                        run_ids: Vec::new(),
                        output: None,
                        outputs: None,
                        error: format!("workflow step task join failed: {error}"),
                        cancelled: false,
                    },
                },
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
    task: TaskSpec,
    resolved: ResolvedStepTargets,
    cancel: CancellationToken,
) -> StepDispatchResult {
    match resolved {
        ResolvedStepTargets::Single { chain, .. } => {
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

fn task_for_step(step: &crate::model::WorkflowStep, prompt: String) -> TaskSpec {
    let mut task = TaskSpec::new(prompt).with_sandbox(step.sandbox);
    task.system = step.system.clone();
    task.workdir = step.workdir.clone();
    task.timeout = step.timeout;
    task.labels
        .insert("workflow.step".to_string(), step.id.clone());
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
