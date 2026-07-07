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
mod template;
mod validate;

pub use engine::{StepEvent, WorkflowEngine, WorkflowObserver};
pub use error::{ValidationReport, WorkflowError, WorkflowResult};
pub use journal::{
    JournalStep, JournalStepStatus, JournalTargetOutput, WorkflowJournal, WorkflowJournalSummary,
    list_journals, read_journal,
};
pub use model::{OnError, StepTargets, Workflow, WorkflowOutcome, WorkflowRunStatus, WorkflowStep};
pub use template::{RenderedStepOutputs, render_template};
pub use validate::{TargetResolver, ValidatedWorkflow, validate_workflow};
