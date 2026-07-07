use std::collections::{BTreeMap, BTreeSet, VecDeque};

use vyane_core::{BoundTarget, Result as VyaneResult};

use crate::error::{WorkflowError, WorkflowResult};
use crate::model::{StepTargets, Workflow};
use crate::template::validate_template;

pub trait TargetResolver: Send + Sync {
    fn resolve(&self, target: &str) -> VyaneResult<Vec<BoundTarget>>;
}

#[derive(Debug, Clone)]
pub enum ResolvedStepTargets {
    Single {
        target: String,
        chain: Vec<BoundTarget>,
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
    let ancestors = transitive_needs(&unique_ids, &dependencies);

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
                match resolver.resolve(target) {
                    Ok(chain) if !chain.is_empty() => {
                        resolved_targets.insert(
                            step.id.clone(),
                            ResolvedStepTargets::Single {
                                target: target.clone(),
                                chain,
                            },
                        );
                    }
                    Ok(_) => problems.push(format!(
                        "step `{}` target `{target}` resolved to an empty chain",
                        step.id
                    )),
                    Err(error) => problems.push(format!(
                        "step `{}` target `{target}` could not be resolved: {}",
                        step.id, error.message
                    )),
                }
            }
            StepTargets::FanOut(targets) if !targets.is_empty() => {
                let mut chains = Vec::with_capacity(targets.len());
                let mut ok = true;
                for target in targets {
                    if target.trim().is_empty() {
                        ok = false;
                        continue;
                    }
                    match resolver.resolve(target) {
                        Ok(chain) if !chain.is_empty() => chains.push(chain),
                        Ok(_) => {
                            ok = false;
                            problems.push(format!(
                                "step `{}` fan_out target `{target}` resolved to an empty chain",
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
                if ok {
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
    ids: &BTreeSet<String>,
    dependencies: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out = BTreeMap::new();
    for id in ids {
        let mut seen = BTreeSet::new();
        collect_needs(id, dependencies, &mut seen, &mut BTreeSet::new());
        out.insert(id.clone(), seen);
    }
    out
}

fn collect_needs(
    id: &str,
    dependencies: &BTreeMap<String, Vec<String>>,
    out: &mut BTreeSet<String>,
    stack: &mut BTreeSet<String>,
) {
    if !stack.insert(id.to_string()) {
        return;
    }
    if let Some(needs) = dependencies.get(id) {
        for need in needs {
            if out.insert(need.clone()) {
                collect_needs(need, dependencies, out, stack);
            }
        }
    }
    stack.remove(id);
}

fn display_step_id(step: &crate::model::WorkflowStep) -> String {
    if step.id.is_empty() {
        format!("#{}", step.index)
    } else {
        step.id.clone()
    }
}
