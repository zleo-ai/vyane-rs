//! Declarative workflow execution for Vyane.
//!
//! A workflow is a TOML-described DAG of kernel dispatches. This crate owns the
//! workflow file format, validation, templating, scheduling, journal, and
//! resume logic. It deliberately does not depend on `vyane-config`: target
//! resolution is injected by the CLI or tests through [`TargetResolver`].

mod engine;
mod error;
mod journal;
mod model;
mod plan;
mod source;
mod template;
mod validate;

pub use engine::{StepEvent, WorkflowEngine, WorkflowObserver};
pub use error::{ValidationReport, WorkflowError, WorkflowResult};
pub use journal::{
    JournalStep, JournalStepStatus, JournalTargetOutput, WorkflowJournal, WorkflowJournalSummary,
    WorkflowRunId, WorkflowRunIdError, list_journals, read_journal,
};
pub use model::{
    OnError, StepTargets, Workflow, WorkflowOutcome, WorkflowRouteHints, WorkflowRunStatus,
    WorkflowStep,
};
pub use plan::{
    WORKFLOW_PLAN_MAX_DEPENDENCY_EDGES, WORKFLOW_PLAN_MAX_DESCRIPTION_BYTES,
    WORKFLOW_PLAN_MAX_NAME_BYTES, WORKFLOW_PLAN_MAX_NEEDS, WORKFLOW_PLAN_MAX_PROMPT_BYTES,
    WORKFLOW_PLAN_MAX_ROUTE_VALUES, WORKFLOW_PLAN_MAX_STEP_ID_BYTES, WORKFLOW_PLAN_MAX_STEPS,
    WORKFLOW_PLAN_MAX_SYSTEM_BYTES, WORKFLOW_PLAN_MAX_TARGET_BYTES, WORKFLOW_PLAN_MAX_TARGETS,
    WORKFLOW_PLAN_MAX_TOTAL_BYTES, WORKFLOW_PLAN_MAX_TOTAL_ROUTE_VALUES,
    WORKFLOW_PLAN_MAX_TOTAL_TARGETS, WORKFLOW_PLAN_MAX_WIRE_BYTES, WORKFLOW_PLAN_MAX_WORKDIR_BYTES,
    WORKFLOW_PLAN_SCHEMA_V1, WorkflowCapabilityManifest, WorkflowPlan, WorkflowPlanDuration,
    WorkflowPlanFeature, WorkflowPlanFeatureSupport, WorkflowPlanRouteHints, WorkflowPlanStep,
    WorkflowPlanTargets,
};
pub use source::{
    WORKFLOW_SOURCE_MAX_ENTRIES, WORKFLOW_SOURCE_MAX_PATH_BYTES, WORKFLOW_SOURCE_MAX_PROMPT_BYTES,
    WORKFLOW_SOURCE_MAX_TOML_BYTES, WORKFLOW_SOURCE_MAX_TOTAL_BYTES, WorkflowSourceBundle,
    WorkflowSourceEntry, WorkflowSourcePath, WorkflowSourcePathError,
};
pub use template::{RenderedStepOutputs, render_template};
pub use validate::{
    MAX_TEMPLATE_ANCESTOR_RELATIONS, TargetResolver, ValidatedWorkflow, validate_plan,
    validate_workflow,
};
