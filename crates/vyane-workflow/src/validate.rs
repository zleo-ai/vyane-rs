use std::collections::{BTreeMap, BTreeSet, VecDeque};

use vyane_core::{BoundTarget, Result as VyaneResult, TaskSpec};

use crate::error::{WorkflowError, WorkflowResult};
use crate::model::{StepTargets, Workflow, WorkflowRouteHints};
use crate::plan::WorkflowPlan;
use crate::template::validate_template;

pub const MAX_TEMPLATE_ANCESTOR_RELATIONS: usize = 65_536;

pub trait TargetResolver: Send + Sync {
    fn resolve(&self, target: &str) -> VyaneResult<Vec<BoundTarget>>;

    /// Validate and, when possible, pre-resolve a selector. Returning `None`
    /// declares a deferred selector whose concrete target depends on the
    /// rendered task and will be resolved immediately before dispatch.
    fn resolve_for_validation(&self, target: &str) -> VyaneResult<Option<Vec<BoundTarget>>> {
        self.resolve(target).map(Some)
    }

    /// Resolve a deferred selector against the rendered task. Implementations
    /// may attach decision metadata to `task.labels`.
    fn resolve_for_task(
        &self,
        target: &str,
        _task: &mut TaskSpec,
    ) -> VyaneResult<Vec<BoundTarget>> {
        self.resolve(target)
    }

    /// Validate a selector that intentionally deferred concrete resolution.
    /// Implementations can check candidate names, policy guards, and config
    /// viability without committing to the prompt-dependent final route.
    fn validate_deferred(&self, _target: &str, _route: &WorkflowRouteHints) -> VyaneResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ResolvedStepTargets {
    Single {
        target: String,
        /// `None` means the resolver deferred selection until the rendered
        /// task is available at execution time.
        chain: Option<Vec<BoundTarget>>,
    },
    FanOut {
        targets: Vec<String>,
        chains: Vec<Vec<BoundTarget>>,
    },
}

impl ResolvedStepTargets {
    pub fn is_fan_out(&self) -> bool {
        matches!(self, ResolvedStepTargets::FanOut { .. })
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedWorkflow {
    pub topo_order: Vec<String>,
    pub dependents: BTreeMap<String, Vec<String>>,
    pub dependencies: BTreeMap<String, Vec<String>>,
    pub resolved_targets: BTreeMap<String, ResolvedStepTargets>,
}

pub fn validate_workflow(
    wf: &Workflow,
    vars: &BTreeMap<String, String>,
    resolver: &dyn TargetResolver,
) -> WorkflowResult<ValidatedWorkflow> {
    let mut problems = Vec::new();
    if wf.name.trim().is_empty() {
        problems.push("[workflow].name is required".to_string());
    }
    if wf.steps.is_empty() {
        problems.push("workflow must contain at least one [[step]]".to_string());
    }
    if wf.max_concurrency > tokio::sync::Semaphore::MAX_PERMITS {
        problems.push(format!(
            "[workflow].max_concurrency must not exceed {}",
            tokio::sync::Semaphore::MAX_PERMITS
        ));
    }

    let mut ids = BTreeMap::<String, usize>::new();
    let mut duplicate_ids = BTreeSet::new();
    for step in &wf.steps {
        if step.id.trim().is_empty() {
            problems.push(format!(
                "step at index {} is missing required `id`",
                step.index
            ));
            continue;
        }
        if ids.insert(step.id.clone(), step.index).is_some() {
            duplicate_ids.insert(step.id.clone());
        }
    }
    for id in &duplicate_ids {
        problems.push(format!("duplicate step id `{id}`"));
    }

    let unique_ids: BTreeSet<String> = ids.keys().cloned().collect();
    let mut dependencies = BTreeMap::<String, Vec<String>>::new();
    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    for id in &unique_ids {
        dependencies.insert(id.clone(), Vec::new());
        dependents.insert(id.clone(), Vec::new());
    }

    for step in wf.steps.iter().filter(|step| unique_ids.contains(&step.id)) {
        let mut seen_needs = BTreeSet::new();
        for need in &step.needs {
            if !seen_needs.insert(need.clone()) {
                problems.push(format!("step `{}` repeats dependency `{need}`", step.id));
            }
            if !unique_ids.contains(need) {
                problems.push(format!("step `{}` needs unknown step `{need}`", step.id));
                continue;
            }
            dependencies
                .entry(step.id.clone())
                .or_default()
                .push(need.clone());
            dependents
                .entry(need.clone())
                .or_default()
                .push(step.id.clone());
        }
    }

    let topo_order = topo_sort(&unique_ids, &dependencies, &mut problems);
    let Some(ancestors) = transitive_needs(&topo_order, &dependencies, &mut problems) else {
        return Err(WorkflowError::validation(problems));
    };

    for step in &wf.steps {
        match &step.targets {
            StepTargets::Single(target) if target.trim().is_empty() => problems.push(format!(
                "step `{}` has an empty `target`",
                display_step_id(step)
            )),
            StepTargets::FanOut(targets) if targets.is_empty() => problems.push(format!(
                "step `{}` has an empty `fan_out` list",
                display_step_id(step)
            )),
            StepTargets::FanOut(targets) => {
                for target in targets {
                    if target.trim().is_empty() {
                        problems.push(format!(
                            "step `{}` has an empty target in `fan_out`",
                            display_step_id(step)
                        ));
                    }
                }
            }
            StepTargets::Both { .. } => problems.push(format!(
                "step `{}` must set exactly one of `target` or `fan_out`, not both",
                display_step_id(step)
            )),
            StepTargets::Missing => problems.push(format!(
                "step `{}` must set exactly one of `target` or `fan_out`",
                display_step_id(step)
            )),
            StepTargets::Single(_) => {}
        }

        match (&step.prompt, &step.prompt_file) {
            (Some(_), Some(_)) => problems.push(format!(
                "step `{}` must set exactly one of `prompt` or `prompt_file`, not both",
                display_step_id(step)
            )),
            (None, None) => problems.push(format!(
                "step `{}` must set exactly one of `prompt` or `prompt_file`",
                display_step_id(step)
            )),
            (Some(_), None) | (None, Some(_)) => {}
        }
    }

    let mut resolved_targets = BTreeMap::new();
    for step in wf.steps.iter().filter(|step| unique_ids.contains(&step.id)) {
        match &step.targets {
            StepTargets::Single(target) if !target.trim().is_empty() => {
                match resolver.resolve_for_validation(target) {
                    Ok(Some(chain)) if !chain.is_empty() => {
                        if step.route.is_empty() {
                            resolved_targets.insert(
                                step.id.clone(),
                                ResolvedStepTargets::Single {
                                    target: target.clone(),
                                    chain: Some(chain),
                                },
                            );
                        } else {
                            problems.push(format!(
                                "step `{}` has route hints on a non-deferred target; route hints require a deferred single target",
                                step.id
                            ));
                        }
                    }
                    Ok(Some(_)) => {
                        if !step.route.is_empty() {
                            problems.push(format!(
                                "step `{}` has route hints on a non-deferred target; route hints require a deferred single target",
                                step.id
                            ));
                        }
                        problems.push(format!(
                            "step `{}` target `{target}` resolved to an empty chain",
                            step.id
                        ));
                    }
                    Ok(None) => match resolver.validate_deferred(target, &step.route) {
                        Ok(()) => {
                            resolved_targets.insert(
                                step.id.clone(),
                                ResolvedStepTargets::Single {
                                    target: target.clone(),
                                    chain: None,
                                },
                            );
                        }
                        Err(error) => problems.push(format!(
                            "step `{}` deferred target `{target}` is invalid: {}",
                            step.id, error.message
                        )),
                    },
                    Err(error) => problems.push(format!(
                        "step `{}` target `{target}` could not be resolved: {}",
                        step.id, error.message
                    )),
                }
            }
            StepTargets::FanOut(targets) if !targets.is_empty() => {
                let route_hints_valid = if step.route.is_empty() {
                    true
                } else {
                    problems.push(format!(
                        "step `{}` has route hints on fan_out targets; route hints require a deferred single target",
                        step.id
                    ));
                    false
                };
                let mut chains = Vec::with_capacity(targets.len());
                let mut ok = true;
                for target in targets {
                    if target.trim().is_empty() {
                        ok = false;
                        continue;
                    }
                    match resolver.resolve_for_validation(target) {
                        Ok(Some(chain)) if !chain.is_empty() => chains.push(chain),
                        Ok(Some(_)) => {
                            ok = false;
                            problems.push(format!(
                                "step `{}` fan_out target `{target}` resolved to an empty chain",
                                step.id
                            ));
                        }
                        Ok(None) => {
                            ok = false;
                            problems.push(format!(
                                "step `{}` fan_out target `{target}` requires deferred routing, which is only supported for a single target",
                                step.id
                            ));
                        }
                        Err(error) => {
                            ok = false;
                            problems.push(format!(
                                "step `{}` fan_out target `{target}` could not be resolved: {}",
                                step.id, error.message
                            ));
                        }
                    }
                }
                if ok && route_hints_valid {
                    resolved_targets.insert(
                        step.id.clone(),
                        ResolvedStepTargets::FanOut {
                            targets: targets.clone(),
                            chains,
                        },
                    );
                }
            }
            StepTargets::Single(_)
            | StepTargets::FanOut(_)
            | StepTargets::Both { .. }
            | StepTargets::Missing => {}
        }
    }

    for step in wf.steps.iter().filter(|step| unique_ids.contains(&step.id)) {
        if let Some(template) = step.prompt_template.as_deref() {
            let available = ancestors.get(&step.id).cloned().unwrap_or_default();
            problems.extend(validate_template(
                &step.id,
                template,
                &wf.name,
                vars,
                &available,
                &|id| {
                    resolved_targets
                        .get(id)
                        .map(ResolvedStepTargets::is_fan_out)
                },
            ));
        }
    }

    if !problems.is_empty() {
        return Err(WorkflowError::validation(problems));
    }

    Ok(ValidatedWorkflow {
        topo_order,
        dependents,
        dependencies,
        resolved_targets,
    })
}

/// Validate the portable execution contract. The compatibility frontend uses
/// the same graph/target validator, but the engine's primary API accepts only
/// the already-materialized plan.
pub fn validate_plan(
    plan: &WorkflowPlan,
    vars: &BTreeMap<String, String>,
    resolver: &dyn TargetResolver,
) -> WorkflowResult<ValidatedWorkflow> {
    plan.verify()?;
    validate_workflow(&plan.as_validation_workflow(), vars, resolver)
}

fn topo_sort(
    ids: &BTreeSet<String>,
    dependencies: &BTreeMap<String, Vec<String>>,
    problems: &mut Vec<String>,
) -> Vec<String> {
    let mut indegree = BTreeMap::<String, usize>::new();
    let mut outgoing = BTreeMap::<String, Vec<String>>::new();
    for id in ids {
        indegree.insert(id.clone(), 0);
        outgoing.insert(id.clone(), Vec::new());
    }
    for (step, needs) in dependencies {
        for need in needs {
            if !ids.contains(step) || !ids.contains(need) {
                continue;
            }
            *indegree.entry(step.clone()).or_default() += 1;
            outgoing.entry(need.clone()).or_default().push(step.clone());
        }
    }

    let mut queue = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
        .collect::<VecDeque<_>>();
    let mut order = Vec::with_capacity(ids.len());
    while let Some(id) = queue.pop_front() {
        order.push(id.clone());
        if let Some(children) = outgoing.get(&id) {
            for child in children {
                let Some(degree) = indegree.get_mut(child) else {
                    continue;
                };
                *degree = degree.saturating_sub(1);
                if *degree == 0 {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    if order.len() != ids.len() {
        let cycle_nodes = indegree
            .into_iter()
            .filter_map(|(id, degree)| (degree > 0).then_some(id))
            .collect::<Vec<_>>();
        problems.push(format!(
            "workflow dependency cycle detected involving: {}",
            cycle_nodes.join(", ")
        ));
    }
    order
}

fn transitive_needs(
    topo_order: &[String],
    dependencies: &BTreeMap<String, Vec<String>>,
    problems: &mut Vec<String>,
) -> Option<BTreeMap<String, BTreeSet<String>>> {
    let mut out = BTreeMap::<String, BTreeSet<String>>::new();
    let mut relation_count = 0usize;
    for id in topo_order {
        let mut ancestors = BTreeSet::new();
        for need in dependencies.get(id).into_iter().flatten() {
            if ancestors.insert(need.clone()) {
                if relation_count == MAX_TEMPLATE_ANCESTOR_RELATIONS {
                    problems.push(format!(
                        "workflow template ancestor relations exceed {MAX_TEMPLATE_ANCESTOR_RELATIONS}"
                    ));
                    return None;
                }
                relation_count += 1;
            }
            if let Some(inherited) = out.get(need) {
                for ancestor in inherited {
                    if ancestors.insert(ancestor.clone()) {
                        if relation_count == MAX_TEMPLATE_ANCESTOR_RELATIONS {
                            problems.push(format!(
                                "workflow template ancestor relations exceed {MAX_TEMPLATE_ANCESTOR_RELATIONS}"
                            ));
                            return None;
                        }
                        relation_count += 1;
                    }
                }
            }
        }
        out.insert(id.clone(), ancestors);
    }
    Some(out)
}

fn display_step_id(step: &crate::model::WorkflowStep) -> String {
    if step.id.is_empty() {
        format!("#{}", step.index)
    } else {
        step.id.clone()
    }
}
