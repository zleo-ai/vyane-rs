use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use vyane_core::{Effort, Sandbox};

use crate::error::{WorkflowError, WorkflowResult};
use crate::model::{OnError, StepTargets, Workflow, WorkflowRouteHints};

pub const WORKFLOW_PLAN_SCHEMA_V1: u32 = 1;
pub const WORKFLOW_PLAN_MAX_STEPS: usize = 4_096;
pub const WORKFLOW_PLAN_MAX_NAME_BYTES: usize = 1_024;
pub const WORKFLOW_PLAN_MAX_DESCRIPTION_BYTES: usize = 64 * 1_024;
pub const WORKFLOW_PLAN_MAX_STEP_ID_BYTES: usize = 1_024;
pub const WORKFLOW_PLAN_MAX_NEEDS: usize = 1_024;
pub const WORKFLOW_PLAN_MAX_DEPENDENCY_EDGES: usize = 16_384;
pub const WORKFLOW_PLAN_MAX_TARGETS: usize = 256;
pub const WORKFLOW_PLAN_MAX_TOTAL_TARGETS: usize = 16_384;
pub const WORKFLOW_PLAN_MAX_ROUTE_VALUES: usize = 256;
pub const WORKFLOW_PLAN_MAX_TOTAL_ROUTE_VALUES: usize = 16_384;
pub const WORKFLOW_PLAN_MAX_TARGET_BYTES: usize = 4_096;
pub const WORKFLOW_PLAN_MAX_PROMPT_BYTES: usize = 4 * 1_024 * 1_024;
pub const WORKFLOW_PLAN_MAX_SYSTEM_BYTES: usize = 1_024 * 1_024;
pub const WORKFLOW_PLAN_MAX_WORKDIR_BYTES: usize = 4_096;
pub const WORKFLOW_PLAN_MAX_TOTAL_BYTES: usize = 16 * 1_024 * 1_024;
pub const WORKFLOW_PLAN_MAX_WIRE_BYTES: usize = 32 * 1_024 * 1_024;

const PLAN_DIGEST_DOMAIN: &[u8] = b"vyane.workflow.plan\0v1\0";
const PROGRAMMATIC_SOURCE_DIGEST_DOMAIN: &[u8] = b"vyane.workflow.programmatic-source\0v1\0";

/// A filesystem-independent, fully materialized workflow execution plan.
///
/// The source file path is deliberately absent. `source_sha256` identifies
/// collected source material or derived programmatic source semantics, while
/// `plan_sha256` is a deterministic corruption/drift checksum, not an
/// authentication mechanism.
/// This is an execution payload, not a safe public view: prompts, system text,
/// targets, and workdirs can contain sensitive data. Absolute workdirs remain
/// representable for compatibility with existing materialized workflows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WorkflowPlan {
    pub schema_version: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub max_concurrency: u32,
    pub source_sha256: String,
    pub capability_manifest: WorkflowCapabilityManifest,
    pub steps: Vec<WorkflowPlanStep>,
    pub plan_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WorkflowPlanStep {
    pub order: u32,
    pub id: String,
    #[serde(default)]
    pub needs: Vec<String>,
    pub targets: WorkflowPlanTargets,
    pub prompt_template: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    pub sandbox: Sandbox,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<WorkflowPlanDuration>,
    pub on_error: OnError,
    #[serde(default)]
    pub route: WorkflowPlanRouteHints,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum WorkflowPlanTargets {
    Single { target: String },
    FanOut { targets: Vec<String> },
}

impl WorkflowPlanTargets {
    pub fn target_names(&self) -> Vec<&str> {
        match self {
            Self::Single { target } => vec![target],
            Self::FanOut { targets } => targets.iter().map(String::as_str).collect(),
        }
    }
}

impl WorkflowPlanRouteHints {
    pub fn is_empty(&self) -> bool {
        self.stage.is_none()
            && self.tier.is_none()
            && self.tags.is_empty()
            && self.candidates.is_empty()
            && self.allow_frontier.is_none()
            && self.effort.is_none()
    }

    pub(crate) fn apply_to_labels(&self, labels: &mut std::collections::BTreeMap<String, String>) {
        WorkflowRouteHints::from(self).apply_to_labels(labels);
    }
}

impl From<&WorkflowRouteHints> for WorkflowPlanRouteHints {
    fn from(route: &WorkflowRouteHints) -> Self {
        Self {
            stage: route.stage.clone(),
            tier: route.tier.clone(),
            tags: route.tags.clone(),
            candidates: route.candidates.clone(),
            allow_frontier: route.allow_frontier,
            effort: route.effort,
        }
    }
}

impl From<&WorkflowPlanRouteHints> for WorkflowRouteHints {
    fn from(route: &WorkflowPlanRouteHints) -> Self {
        Self {
            stage: route.stage.clone(),
            tier: route.tier.clone(),
            tags: route.tags.clone(),
            candidates: route.candidates.clone(),
            allow_frontier: route.allow_frontier,
            effort: route.effort,
        }
    }
}

/// Requested pre-resolution execution summary frozen into a plan.
///
/// This is descriptive and digest-bound, but is not an authorization decision
/// and does not prove what a resolver or harness will ultimately do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WorkflowCapabilityManifest {
    pub maximum_sandbox: Sandbox,
    pub uses_explicit_workdir: bool,
    pub uses_fan_out: bool,
    pub has_route_hints: bool,
    pub maximum_timeout: Option<WorkflowPlanDuration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WorkflowPlanRouteHints {
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub candidates: Vec<String>,
    #[serde(default)]
    pub allow_frontier: Option<bool>,
    #[serde(default)]
    pub effort: Option<Effort>,
}

/// Lossless wire representation of [`Duration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WorkflowPlanDuration {
    pub secs: u64,
    pub nanos: u32,
}

impl WorkflowPlanDuration {
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        }
    }

    pub fn to_duration(self) -> WorkflowResult<Duration> {
        if self.nanos >= 1_000_000_000 {
            return Err(WorkflowError::InvalidWorkflowPlan {
                reason: "workflow plan duration nanos must be less than 1000000000".into(),
            });
        }
        Ok(Duration::new(self.secs, self.nanos))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowPlanFeature {
    DeclarativeDag,
    DynamicControlFlow,
    NestedWorkflow,
    SharedBudget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum WorkflowPlanFeatureSupport {
    Supported,
    Unsupported {
        feature: WorkflowPlanFeature,
        reason: String,
    },
}

impl WorkflowPlan {
    pub fn compile(workflow: &Workflow) -> WorkflowResult<Self> {
        let mut problems = Vec::new();
        let mut steps = Vec::with_capacity(workflow.steps.len());
        for (order, step) in workflow.steps.iter().enumerate() {
            let order = u32::try_from(order).map_err(|_| WorkflowError::InvalidWorkflowPlan {
                reason: "workflow step order exceeds u32".into(),
            })?;
            let targets = match &step.targets {
                StepTargets::Single(target) => WorkflowPlanTargets::Single {
                    target: target.clone(),
                },
                StepTargets::FanOut(targets) => WorkflowPlanTargets::FanOut {
                    targets: targets.clone(),
                },
                StepTargets::Both { .. } => {
                    problems.push(format!(
                        "step `{}` cannot compile: exactly one of target or fan_out is required",
                        step.id
                    ));
                    continue;
                }
                StepTargets::Missing => {
                    problems.push(format!(
                        "step `{}` cannot compile: target or fan_out is required",
                        step.id
                    ));
                    continue;
                }
            };
            let Some(prompt_template) = step.prompt_template.clone() else {
                problems.push(format!(
                    "step `{}` cannot compile: materialized prompt template is missing",
                    step.id
                ));
                continue;
            };
            if !matches!(
                (&step.prompt, &step.prompt_file),
                (Some(_), None) | (None, Some(_))
            ) {
                problems.push(format!(
                    "step `{}` cannot compile: exactly one of prompt or prompt_file is required",
                    step.id
                ));
                continue;
            }
            let workdir = match step.workdir.as_ref() {
                Some(path) => match path.to_str() {
                    Some(path) => Some(path.to_string()),
                    None => {
                        problems.push(format!(
                            "step `{}` cannot compile: workdir is not valid UTF-8",
                            step.id
                        ));
                        continue;
                    }
                },
                None => None,
            };
            steps.push(WorkflowPlanStep {
                order,
                id: step.id.clone(),
                needs: step.needs.clone(),
                targets,
                prompt_template,
                system: step.system.clone(),
                workdir,
                sandbox: step.sandbox,
                timeout: step.timeout.map(WorkflowPlanDuration::from_duration),
                on_error: step.on_error,
                route: WorkflowPlanRouteHints::from(&step.route),
            });
        }
        if !problems.is_empty() {
            return Err(WorkflowError::validation(problems));
        }

        let max_concurrency = u32::try_from(workflow.max_concurrency.max(1)).map_err(|_| {
            WorkflowError::InvalidWorkflowPlan {
                reason: "workflow max_concurrency exceeds u32".into(),
            }
        })?;
        let capability_manifest = WorkflowCapabilityManifest::from_steps(&steps);
        let source_sha256 = if is_sha256(&workflow.file_sha256) {
            workflow.file_sha256.clone()
        } else {
            programmatic_source_digest(
                &workflow.name,
                &workflow.description,
                max_concurrency,
                &steps,
            )?
        };
        let mut plan = Self {
            schema_version: WORKFLOW_PLAN_SCHEMA_V1,
            name: workflow.name.clone(),
            description: workflow.description.clone(),
            max_concurrency,
            source_sha256,
            capability_manifest,
            steps,
            plan_sha256: String::new(),
        };
        plan.plan_sha256 = plan.compute_digest()?;
        plan.verify()?;
        Ok(plan)
    }

    pub fn from_json(bytes: &[u8]) -> WorkflowResult<Self> {
        if bytes.len() > WORKFLOW_PLAN_MAX_WIRE_BYTES {
            return Err(WorkflowError::InvalidWorkflowPlan {
                reason: format!(
                    "workflow plan wire payload exceeds {WORKFLOW_PLAN_MAX_WIRE_BYTES} bytes"
                ),
            });
        }
        let plan: Self =
            serde_json::from_slice(bytes).map_err(|source| WorkflowError::InvalidWorkflowPlan {
                reason: source.to_string(),
            })?;
        plan.verify()?;
        Ok(plan)
    }

    pub fn to_canonical_json(&self) -> WorkflowResult<Vec<u8>> {
        self.verify()?;
        serde_json::to_vec(self).map_err(|source| WorkflowError::InvalidWorkflowPlan {
            reason: source.to_string(),
        })
    }

    pub fn verify(&self) -> WorkflowResult<()> {
        let mut problems = Vec::new();
        if self.schema_version != WORKFLOW_PLAN_SCHEMA_V1 {
            problems.push(format!(
                "unsupported workflow plan schema version {}; expected {}",
                self.schema_version, WORKFLOW_PLAN_SCHEMA_V1
            ));
        }
        validate_text(
            "workflow name",
            &self.name,
            1,
            WORKFLOW_PLAN_MAX_NAME_BYTES,
            &mut problems,
        );
        if let Some(description) = self.description.as_deref() {
            validate_text(
                "workflow description",
                description,
                0,
                WORKFLOW_PLAN_MAX_DESCRIPTION_BYTES,
                &mut problems,
            );
        }
        if self.max_concurrency == 0
            || usize::try_from(self.max_concurrency)
                .map_or(true, |value| value > tokio::sync::Semaphore::MAX_PERMITS)
        {
            problems.push(format!(
                "workflow plan max_concurrency must be between 1 and {}",
                tokio::sync::Semaphore::MAX_PERMITS
            ));
        }
        if self.steps.is_empty() || self.steps.len() > WORKFLOW_PLAN_MAX_STEPS {
            problems.push(format!(
                "workflow plan step count must be between 1 and {WORKFLOW_PLAN_MAX_STEPS}"
            ));
        }
        if !is_sha256(&self.source_sha256) {
            problems.push("workflow plan source_sha256 must be lowercase SHA-256 hex".into());
        }

        let mut ids = BTreeSet::new();
        let mut total_bytes = self
            .name
            .len()
            .saturating_add(self.description.as_deref().map_or(0, str::len))
            .saturating_add(self.source_sha256.len())
            .saturating_add(self.plan_sha256.len());
        let mut dependency_edges = 0usize;
        let mut total_targets = 0usize;
        let mut total_route_values = 0usize;
        for (order, step) in self.steps.iter().enumerate() {
            total_bytes = total_bytes
                .saturating_add(step.id.len())
                .saturating_add(step.prompt_template.len())
                .saturating_add(step.system.as_deref().map_or(0, str::len))
                .saturating_add(step.workdir.as_deref().map_or(0, str::len));
            if usize::try_from(step.order).ok() != Some(order) {
                problems.push(format!(
                    "workflow plan step `{}` has order {}, expected {order}",
                    step.id, step.order
                ));
            }
            validate_text(
                "workflow step id",
                &step.id,
                1,
                WORKFLOW_PLAN_MAX_STEP_ID_BYTES,
                &mut problems,
            );
            if !ids.insert(step.id.as_str()) {
                problems.push(format!("workflow plan repeats step id `{}`", step.id));
            }
            if step.needs.len() > WORKFLOW_PLAN_MAX_NEEDS {
                problems.push(format!(
                    "workflow plan step `{}` exceeds dependency limit {WORKFLOW_PLAN_MAX_NEEDS}",
                    step.id
                ));
            }
            dependency_edges = dependency_edges.saturating_add(step.needs.len());
            for need in &step.needs {
                total_bytes = total_bytes.saturating_add(need.len());
                validate_text(
                    "workflow dependency id",
                    need,
                    1,
                    WORKFLOW_PLAN_MAX_STEP_ID_BYTES,
                    &mut problems,
                );
            }
            let target_names = step.targets.target_names();
            total_targets = total_targets.saturating_add(target_names.len());
            for target in target_names {
                total_bytes = total_bytes.saturating_add(target.len());
                validate_text(
                    "workflow target",
                    target,
                    1,
                    WORKFLOW_PLAN_MAX_TARGET_BYTES,
                    &mut problems,
                );
            }
            if matches!(&step.targets, WorkflowPlanTargets::FanOut { targets } if targets.is_empty() || targets.len() > WORKFLOW_PLAN_MAX_TARGETS)
            {
                problems.push(format!(
                    "workflow plan step `{}` fan_out count must be between 1 and {WORKFLOW_PLAN_MAX_TARGETS}",
                    step.id
                ));
            }
            if let WorkflowPlanTargets::FanOut { .. } = &step.targets {
                if !step.route.is_empty() {
                    problems.push(format!(
                        "workflow plan step `{}` cannot apply route hints to fan_out",
                        step.id
                    ));
                }
            }
            validate_text(
                "workflow prompt template",
                &step.prompt_template,
                0,
                WORKFLOW_PLAN_MAX_PROMPT_BYTES,
                &mut problems,
            );
            if let Some(system) = step.system.as_deref() {
                validate_text(
                    "workflow system prompt",
                    system,
                    0,
                    WORKFLOW_PLAN_MAX_SYSTEM_BYTES,
                    &mut problems,
                );
            }
            if let Some(workdir) = step.workdir.as_deref() {
                validate_text(
                    "workflow workdir",
                    workdir,
                    1,
                    WORKFLOW_PLAN_MAX_WORKDIR_BYTES,
                    &mut problems,
                );
                if workdir.contains('\0') {
                    problems.push(format!(
                        "workflow plan step `{}` workdir contains NUL",
                        step.id
                    ));
                }
            }
            if step
                .timeout
                .is_some_and(|timeout| timeout.to_duration().is_err())
            {
                problems.push(format!(
                    "workflow plan step `{}` timeout nanos must be less than 1000000000",
                    step.id
                ));
            }
            total_route_values = total_route_values
                .saturating_add(step.route.tags.len())
                .saturating_add(step.route.candidates.len())
                .saturating_add(usize::from(step.route.stage.is_some()))
                .saturating_add(usize::from(step.route.tier.is_some()));
            total_bytes = total_bytes
                .saturating_add(step.route.stage.as_deref().map_or(0, str::len))
                .saturating_add(step.route.tier.as_deref().map_or(0, str::len))
                .saturating_add(step.route.tags.iter().map(String::len).sum::<usize>())
                .saturating_add(step.route.candidates.iter().map(String::len).sum::<usize>());
            validate_route(&step.id, &step.route, &mut problems);
        }
        if dependency_edges > WORKFLOW_PLAN_MAX_DEPENDENCY_EDGES {
            problems.push(format!(
                "workflow plan exceeds global dependency edge limit {WORKFLOW_PLAN_MAX_DEPENDENCY_EDGES}"
            ));
        }
        if total_targets > WORKFLOW_PLAN_MAX_TOTAL_TARGETS {
            problems.push(format!(
                "workflow plan exceeds global target limit {WORKFLOW_PLAN_MAX_TOTAL_TARGETS}"
            ));
        }
        if total_route_values > WORKFLOW_PLAN_MAX_TOTAL_ROUTE_VALUES {
            problems.push(format!(
                "workflow plan exceeds global route value limit {WORKFLOW_PLAN_MAX_TOTAL_ROUTE_VALUES}"
            ));
        }
        validate_graph(&self.steps, &ids, &mut problems);
        if total_bytes > WORKFLOW_PLAN_MAX_TOTAL_BYTES {
            problems.push(format!(
                "workflow plan string content exceeds {WORKFLOW_PLAN_MAX_TOTAL_BYTES} bytes"
            ));
        }
        let expected_manifest = WorkflowCapabilityManifest::from_steps(&self.steps);
        if self.capability_manifest != expected_manifest {
            problems.push("workflow plan capability manifest does not match its steps".into());
        }
        if problems.is_empty() {
            let expected_digest = self.compute_digest()?;
            if self.plan_sha256 != expected_digest {
                problems.push("workflow plan digest does not match canonical plan content".into());
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(WorkflowError::validation(problems))
        }
    }

    /// Report whether a feature is statically representable in schema v1.
    ///
    /// This is not frontend compatibility negotiation and does not authorize
    /// execution against any particular resolver or harness.
    pub fn feature_support(feature: WorkflowPlanFeature) -> WorkflowPlanFeatureSupport {
        match feature {
            WorkflowPlanFeature::DeclarativeDag => WorkflowPlanFeatureSupport::Supported,
            WorkflowPlanFeature::DynamicControlFlow => WorkflowPlanFeatureSupport::Unsupported {
                feature,
                reason: "dynamic control flow cannot be represented by WorkflowPlan v1".into(),
            },
            WorkflowPlanFeature::NestedWorkflow => WorkflowPlanFeatureSupport::Unsupported {
                feature,
                reason: "nested workflow calls cannot be represented by WorkflowPlan v1".into(),
            },
            WorkflowPlanFeature::SharedBudget => WorkflowPlanFeatureSupport::Unsupported {
                feature,
                reason: "shared run budgets are not enforced by WorkflowPlan v1".into(),
            },
        }
    }

    fn compute_digest(&self) -> WorkflowResult<String> {
        #[derive(Serialize)]
        struct DigestView<'a> {
            schema_version: u32,
            name: &'a str,
            description: &'a Option<String>,
            max_concurrency: u32,
            source_sha256: &'a str,
            capability_manifest: &'a WorkflowCapabilityManifest,
            steps: &'a [WorkflowPlanStep],
        }
        let bytes = serde_json::to_vec(&DigestView {
            schema_version: self.schema_version,
            name: &self.name,
            description: &self.description,
            max_concurrency: self.max_concurrency,
            source_sha256: &self.source_sha256,
            capability_manifest: &self.capability_manifest,
            steps: &self.steps,
        })
        .map_err(|source| WorkflowError::InvalidWorkflowPlan {
            reason: source.to_string(),
        })?;
        let mut hash = Sha256::new();
        hash.update(PLAN_DIGEST_DOMAIN);
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        Ok(hex(hash.finalize()))
    }

    pub(crate) fn as_validation_workflow(&self) -> Workflow {
        Workflow {
            name: self.name.clone(),
            description: self.description.clone(),
            max_concurrency: self.max_concurrency as usize,
            steps: self
                .steps
                .iter()
                .map(|step| crate::model::WorkflowStep {
                    index: step.order as usize,
                    id: step.id.clone(),
                    needs: step.needs.clone(),
                    targets: match &step.targets {
                        WorkflowPlanTargets::Single { target } => {
                            StepTargets::Single(target.clone())
                        }
                        WorkflowPlanTargets::FanOut { targets } => {
                            StepTargets::FanOut(targets.clone())
                        }
                    },
                    prompt: Some(step.prompt_template.clone()),
                    prompt_file: None,
                    prompt_template: Some(step.prompt_template.clone()),
                    system: step.system.clone(),
                    workdir: step.workdir.as_ref().map(PathBuf::from),
                    sandbox: step.sandbox,
                    timeout: step.timeout.and_then(|timeout| timeout.to_duration().ok()),
                    on_error: step.on_error,
                    route: WorkflowRouteHints::from(&step.route),
                })
                .collect(),
            file_path: PathBuf::new(),
            legacy_file_sha256: None,
            file_sha256: self.source_sha256.clone(),
        }
    }
}

impl WorkflowCapabilityManifest {
    fn from_steps(steps: &[WorkflowPlanStep]) -> Self {
        Self {
            maximum_sandbox: steps
                .iter()
                .map(|step| step.sandbox)
                .max_by_key(|sandbox| sandbox_rank(*sandbox))
                .unwrap_or(Sandbox::ReadOnly),
            uses_explicit_workdir: steps.iter().any(|step| step.workdir.is_some()),
            uses_fan_out: steps
                .iter()
                .any(|step| matches!(step.targets, WorkflowPlanTargets::FanOut { .. })),
            has_route_hints: steps.iter().any(|step| !step.route.is_empty()),
            maximum_timeout: steps.iter().filter_map(|step| step.timeout).max(),
        }
    }
}

fn sandbox_rank(sandbox: Sandbox) -> u8 {
    match sandbox {
        Sandbox::ReadOnly => 0,
        Sandbox::Write => 1,
        Sandbox::Full => 2,
    }
}

fn validate_text(label: &str, value: &str, min: usize, max: usize, problems: &mut Vec<String>) {
    let len = value.len();
    if len < min || len > max {
        problems.push(format!(
            "{label} byte length must be between {min} and {max}"
        ));
    }
}

fn validate_route(step_id: &str, route: &WorkflowPlanRouteHints, problems: &mut Vec<String>) {
    for (label, value) in [
        ("stage", route.stage.as_deref()),
        ("tier", route.tier.as_deref()),
    ] {
        if value
            .is_some_and(|value| value.is_empty() || value.len() > WORKFLOW_PLAN_MAX_TARGET_BYTES)
        {
            problems.push(format!(
                "workflow plan step `{step_id}` route {label} is empty or oversized"
            ));
        }
    }
    if route.tags.len() > WORKFLOW_PLAN_MAX_ROUTE_VALUES
        || route.candidates.len() > WORKFLOW_PLAN_MAX_ROUTE_VALUES
    {
        problems.push(format!(
            "workflow plan step `{step_id}` route list exceeds {WORKFLOW_PLAN_MAX_ROUTE_VALUES} entries"
        ));
    }
    for value in route.tags.iter().chain(&route.candidates) {
        if value.is_empty() || value.len() > WORKFLOW_PLAN_MAX_TARGET_BYTES {
            problems.push(format!(
                "workflow plan step `{step_id}` route value is empty or oversized"
            ));
        }
    }
}

fn validate_graph(steps: &[WorkflowPlanStep], ids: &BTreeSet<&str>, problems: &mut Vec<String>) {
    let mut indegree = std::collections::BTreeMap::<&str, usize>::new();
    let mut children = std::collections::BTreeMap::<&str, Vec<&str>>::new();
    for id in ids {
        indegree.insert(id, 0);
        children.insert(id, Vec::new());
    }
    for step in steps {
        let mut unique_needs = BTreeSet::new();
        for need in &step.needs {
            if !unique_needs.insert(need.as_str()) {
                problems.push(format!(
                    "workflow plan step `{}` repeats dependency `{need}`",
                    step.id
                ));
                continue;
            }
            if need == &step.id {
                problems.push(format!(
                    "workflow plan step `{}` cannot depend on itself",
                    step.id
                ));
                continue;
            }
            if !ids.contains(need.as_str()) {
                problems.push(format!(
                    "workflow plan step `{}` needs unknown step `{need}`",
                    step.id
                ));
                continue;
            }
            *indegree.entry(&step.id).or_default() += 1;
            children
                .entry(need.as_str())
                .or_default()
                .push(step.id.as_str());
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect::<Vec<_>>();
    let mut visited = 0;
    while let Some(id) = ready.pop() {
        visited += 1;
        if let Some(next) = children.get(id) {
            for child in next {
                if let Some(degree) = indegree.get_mut(child) {
                    *degree = degree.saturating_sub(1);
                    if *degree == 0 {
                        ready.push(child);
                    }
                }
            }
        }
    }
    if visited != ids.len() {
        problems.push("workflow plan dependency graph contains a cycle".into());
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn programmatic_source_digest(
    name: &str,
    description: &Option<String>,
    max_concurrency: u32,
    steps: &[WorkflowPlanStep],
) -> WorkflowResult<String> {
    #[derive(Serialize)]
    struct ProgrammaticSource<'a> {
        name: &'a str,
        description: &'a Option<String>,
        max_concurrency: u32,
        steps: &'a [WorkflowPlanStep],
    }
    let bytes = serde_json::to_vec(&ProgrammaticSource {
        name,
        description,
        max_concurrency,
        steps,
    })
    .map_err(|source| WorkflowError::InvalidWorkflowPlan {
        reason: source.to_string(),
    })?;
    let mut hash = Sha256::new();
    hash.update(PROGRAMMATIC_SOURCE_DIGEST_DOMAIN);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(hex(hash.finalize()))
}

fn hex(digest: impl AsRef<[u8]>) -> String {
    let mut out = String::with_capacity(64);
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::WorkflowSourceBundle;

    fn workflow(source: &str) -> Workflow {
        WorkflowSourceBundle {
            workflow_toml: source.into(),
            prompt_files: Vec::new(),
        }
        .materialize()
        .unwrap()
    }

    fn source(system: &str) -> String {
        format!(
            r#"
[workflow]
name = "plan-contract"
description = "portable"
max_concurrency = 3

[[step]]
id = "find"
fan_out = ["one", "two", "three"]
prompt = "find {{{{vars.topic}}}}"
system = "{system}"
workdir = "workspace"
sandbox = "write"
timeout_secs = 30

[[step]]
id = "synth"
needs = ["find"]
target = "auto"
prompt = "{{{{steps.find.outputs}}}}"
[step.route]
tier = "mainline"
effort = "high"
"#
        )
    }

    #[test]
    fn compile_is_stable_source_materialized_and_digest_bound() {
        let first = workflow(&source("stable")).compile_plan().unwrap();
        let second = workflow(&source("stable")).compile_plan().unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.to_canonical_json().unwrap(),
            second.to_canonical_json().unwrap()
        );
        assert_eq!(first.plan_sha256.len(), 64);
        let json = String::from_utf8(first.to_canonical_json().unwrap()).unwrap();
        let expected = r#"{"schema_version":1,"name":"plan-contract","description":"portable","max_concurrency":3,"source_sha256":"d50f2bfb5fc0692ca02bf3b1c399f03d2250b18332cfc0ea68355778a36f036c","capability_manifest":{"maximum_sandbox":"write","uses_explicit_workdir":true,"uses_fan_out":true,"has_route_hints":true,"maximum_timeout":{"secs":30,"nanos":0}},"steps":[{"order":0,"id":"find","needs":[],"targets":{"kind":"fan_out","targets":["one","two","three"]},"prompt_template":"find {{vars.topic}}","system":"stable","workdir":"workspace","sandbox":"write","timeout":{"secs":30,"nanos":0},"on_error":"abort","route":{"stage":null,"tier":null,"tags":[],"candidates":[],"allow_frontier":null,"effort":null}},{"order":1,"id":"synth","needs":["find"],"targets":{"kind":"single","target":"auto"},"prompt_template":"{{steps.find.outputs}}","sandbox":"read-only","on_error":"abort","route":{"stage":null,"tier":"mainline","tags":[],"candidates":[],"allow_frontier":null,"effort":"high"}}],"plan_sha256":"8925ad33d5f6ce89286affb01c79fccf0e29e6a1ce0f3a80ef73bfef235bb5d3"}"#;
        assert_eq!(json, expected);
        assert_eq!(
            first.plan_sha256,
            "8925ad33d5f6ce89286affb01c79fccf0e29e6a1ce0f3a80ef73bfef235bb5d3"
        );
        assert!(!json.contains("file_path"));
        assert!(!json.contains("workflow-source-bundle"));

        let drifted = workflow(&source("changed")).compile_plan().unwrap();
        assert_ne!(first.source_sha256, drifted.source_sha256);
        assert_ne!(first.plan_sha256, drifted.plan_sha256);
    }

    #[test]
    fn strict_wire_rejects_unknown_schema_field_and_digest_drift() {
        let plan = workflow(&source("stable")).compile_plan().unwrap();
        let mut value = serde_json::to_value(&plan).unwrap();
        value["schema_version"] = serde_json::json!(99);
        let error = WorkflowPlan::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported workflow plan schema")
        );

        let mut value = serde_json::to_value(&plan).unwrap();
        value["unknown"] = serde_json::json!(true);
        let error = WorkflowPlan::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(error.to_string().contains("unknown field"));

        let mut value = serde_json::to_value(&plan).unwrap();
        value["steps"][0]["unknown"] = serde_json::json!(true);
        let error = WorkflowPlan::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(error.to_string().contains("unknown field"));

        let mut value = serde_json::to_value(&plan).unwrap();
        value["steps"][1]["route"]["unknown"] = serde_json::json!(true);
        let error = WorkflowPlan::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(error.to_string().contains("unknown field"));

        let mut value = serde_json::to_value(&plan).unwrap();
        value["name"] = serde_json::json!("tampered");
        let error = WorkflowPlan::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(error.to_string().contains("digest does not match"));
    }

    #[test]
    fn wire_size_is_rejected_before_deserialization() {
        let oversized = vec![b' '; WORKFLOW_PLAN_MAX_WIRE_BYTES + 1];
        let error = WorkflowPlan::from_json(&oversized).unwrap_err();
        assert!(error.to_string().contains("wire payload exceeds"));
    }

    #[test]
    fn subsecond_timeouts_are_preserved_exactly() {
        let mut half = workflow(&source("stable"));
        half.steps[0].timeout = Some(Duration::from_millis(500));
        let half_plan = half.compile_plan().unwrap();
        assert_eq!(
            half_plan.steps[0].timeout,
            Some(WorkflowPlanDuration {
                secs: 0,
                nanos: 500_000_000
            })
        );
        assert_eq!(
            half_plan.as_validation_workflow().steps[0].timeout,
            Some(Duration::from_millis(500))
        );

        let mut one_and_half = workflow(&source("stable"));
        one_and_half.steps[0].timeout = Some(Duration::from_millis(1_500));
        let plan = one_and_half.compile_plan().unwrap();
        assert_eq!(
            plan.steps[0].timeout,
            Some(WorkflowPlanDuration {
                secs: 1,
                nanos: 500_000_000
            })
        );
        assert_eq!(
            plan.as_validation_workflow().steps[0].timeout,
            Some(Duration::from_millis(1_500))
        );

        for duration in [
            Duration::from_secs(8 * 24 * 60 * 60),
            Duration::new(u64::MAX, 999_999_999),
        ] {
            let mut workflow = workflow(&source("stable"));
            workflow.steps[0].timeout = Some(duration);
            let plan = workflow.compile_plan().unwrap();
            assert_eq!(
                plan.steps[0].timeout.unwrap().to_duration().unwrap(),
                duration
            );
            let decoded = WorkflowPlan::from_json(&plan.to_canonical_json().unwrap()).unwrap();
            assert_eq!(decoded.steps[0].timeout, plan.steps[0].timeout);
        }

        let mut invalid = workflow(&source("stable")).compile_plan().unwrap();
        invalid.steps[0].timeout = Some(WorkflowPlanDuration {
            secs: 0,
            nanos: 1_000_000_000,
        });
        assert!(invalid.verify().unwrap_err().to_string().contains("nanos"));
    }

    #[test]
    fn programmatic_zero_concurrency_keeps_legacy_minimum_one() {
        let mut workflow = workflow(&source("stable"));
        workflow.max_concurrency = 0;
        assert_eq!(workflow.compile_plan().unwrap().max_concurrency, 1);
    }

    #[test]
    fn plan_total_budget_preserves_two_large_legacy_prompts() {
        assert_eq!(WORKFLOW_PLAN_MAX_TOTAL_BYTES, 16 * 1_024 * 1_024);
        let mut workflow = workflow(&source("stable"));
        workflow.file_sha256.clear();
        workflow.legacy_file_sha256 = None;
        let prompt = "x".repeat(3 * 1_024 * 1_024);
        for step in &mut workflow.steps {
            step.prompt = Some(prompt.clone());
            step.prompt_file = None;
            step.prompt_template = Some(prompt.clone());
        }

        let plan = workflow.compile_plan().unwrap();
        assert_eq!(plan.steps.len(), 2);
        assert!(plan.to_canonical_json().unwrap().len() > 6 * 1_024 * 1_024);
    }

    #[test]
    fn aggregate_vector_budgets_reject_amplification() {
        let base = workflow(&source("stable")).compile_plan().unwrap();

        let mut edges = base.clone();
        edges.steps = (0..17)
            .map(|index| {
                let mut step = base.steps[0].clone();
                step.order = index;
                step.id = format!("edge-{index}");
                step.needs = vec!["edge-0".into(); WORKFLOW_PLAN_MAX_NEEDS];
                step
            })
            .collect();
        assert!(
            edges
                .verify()
                .unwrap_err()
                .to_string()
                .contains("global dependency edge limit")
        );

        let mut targets = base.clone();
        targets.steps = (0..65)
            .map(|index| {
                let mut step = base.steps[0].clone();
                step.order = index;
                step.id = format!("target-{index}");
                step.needs.clear();
                step.targets = WorkflowPlanTargets::FanOut {
                    targets: vec!["x".into(); WORKFLOW_PLAN_MAX_TARGETS],
                };
                step
            })
            .collect();
        assert!(
            targets
                .verify()
                .unwrap_err()
                .to_string()
                .contains("global target limit")
        );

        let mut routes = base;
        let route_template = routes.steps[1].clone();
        routes.steps = (0..65)
            .map(|index| {
                let mut step = route_template.clone();
                step.order = index;
                step.id = format!("route-{index}");
                step.needs.clear();
                step.route.tags = vec!["x".into(); WORKFLOW_PLAN_MAX_ROUTE_VALUES];
                step
            })
            .collect();
        assert!(
            routes
                .verify()
                .unwrap_err()
                .to_string()
                .contains("global route value limit")
        );
    }

    #[test]
    fn capability_manifest_is_derived_and_tamper_evident() {
        let plan = workflow(&source("stable")).compile_plan().unwrap();
        assert_eq!(plan.capability_manifest.maximum_sandbox, Sandbox::Write);
        assert!(plan.capability_manifest.uses_explicit_workdir);
        assert!(plan.capability_manifest.uses_fan_out);
        assert!(plan.capability_manifest.has_route_hints);
        assert_eq!(
            plan.capability_manifest.maximum_timeout,
            Some(WorkflowPlanDuration { secs: 30, nanos: 0 })
        );

        let mut tampered = plan;
        tampered.capability_manifest.maximum_sandbox = Sandbox::ReadOnly;
        let error = tampered.verify().unwrap_err();
        assert!(error.to_string().contains("capability manifest"));
    }

    #[test]
    fn unsupported_frontend_features_are_explicit() {
        assert_eq!(
            WorkflowPlan::feature_support(WorkflowPlanFeature::DeclarativeDag),
            WorkflowPlanFeatureSupport::Supported
        );
        for feature in [
            WorkflowPlanFeature::DynamicControlFlow,
            WorkflowPlanFeature::NestedWorkflow,
            WorkflowPlanFeature::SharedBudget,
        ] {
            match WorkflowPlan::feature_support(feature) {
                WorkflowPlanFeatureSupport::Unsupported {
                    feature: reported,
                    reason,
                } => {
                    assert_eq!(reported, feature);
                    assert!(!reason.is_empty());
                }
                WorkflowPlanFeatureSupport::Supported => panic!("feature must be unsupported"),
            }
        }
    }
}
