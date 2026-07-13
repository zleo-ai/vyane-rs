use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use vyane_core::{Effort, Sandbox};

use crate::journal::{WorkflowJournal, WorkflowRunId};

#[derive(Debug, Clone)]
pub struct Workflow {
    pub name: String,
    pub description: Option<String>,
    pub max_concurrency: usize,
    pub steps: Vec<WorkflowStep>,
    pub file_path: PathBuf,
    /// Hash produced by the pre-source-bundle algorithm. New journals never
    /// write this value; it is retained only so `resume` can migrate existing
    /// journals to the versioned source-bundle hash without weakening content
    /// comparison.
    pub legacy_file_sha256: Option<String>,
    pub file_sha256: String,
}

#[derive(Debug, Clone)]
pub struct WorkflowStep {
    pub index: usize,
    pub id: String,
    pub needs: Vec<String>,
    pub targets: StepTargets,
    pub prompt: Option<String>,
    pub prompt_file: Option<PathBuf>,
    pub prompt_template: Option<String>,
    pub system: Option<String>,
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    pub timeout: Option<Duration>,
    pub on_error: OnError,
    pub route: WorkflowRouteHints,
}

/// Optional routing hints used when a resolver supports deferred/automatic
/// target selection. They are translated into canonical `routing.*` task
/// labels immediately before a step is dispatched.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRouteHints {
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
    /// Explicit reasoning effort for a deferred route. This is a closed,
    /// protocol-neutral value; adapters translate it to their own wire shape.
    #[serde(default)]
    pub effort: Option<Effort>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepTargets {
    Single(String),
    FanOut(Vec<String>),
    Both {
        target: String,
        fan_out: Vec<String>,
    },
    Missing,
}

impl StepTargets {
    pub(crate) fn from_raw(target: Option<String>, fan_out: Option<Vec<String>>) -> Self {
        match (target, fan_out) {
            (Some(target), Some(fan_out)) => StepTargets::Both { target, fan_out },
            (Some(target), None) => StepTargets::Single(target),
            (None, Some(fan_out)) => StepTargets::FanOut(fan_out),
            (None, None) => StepTargets::Missing,
        }
    }

    pub fn target_names(&self) -> Vec<&str> {
        match self {
            StepTargets::Single(target) => vec![target.as_str()],
            StepTargets::FanOut(targets) => targets.iter().map(String::as_str).collect(),
            StepTargets::Both { target, fan_out } => {
                let mut out = vec![target.as_str()];
                out.extend(fan_out.iter().map(String::as_str));
                out
            }
            StepTargets::Missing => Vec::new(),
        }
    }

    pub fn is_valid(&self) -> bool {
        matches!(self, StepTargets::Single(_) | StepTargets::FanOut(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    #[default]
    Abort,
    Continue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRunStatus {
    Running,
    Completed,
    CompletedWithFailures,
    Failed,
    Cancelled,
}

impl WorkflowRunStatus {
    pub fn is_success(self) -> bool {
        self == WorkflowRunStatus::Completed
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowOutcome {
    pub wf_run_id: WorkflowRunId,
    pub status: WorkflowRunStatus,
    pub journal_path: PathBuf,
    pub journal: WorkflowJournal,
}

impl WorkflowRouteHints {
    /// Whether this step carries no deferred-routing inputs.
    pub fn is_empty(&self) -> bool {
        self.stage.is_none()
            && self.tier.is_none()
            && self.tags.is_empty()
            && self.candidates.is_empty()
            && self.allow_frontier.is_none()
            && self.effort.is_none()
    }

    /// Add normalized routing inputs to a task without exposing workflow
    /// internals to resolver implementations.
    pub fn apply_to_labels(&self, labels: &mut std::collections::BTreeMap<String, String>) {
        if let Some(stage) = self.stage.as_ref() {
            labels.insert("routing.stage".into(), stage.clone());
        }
        if let Some(tier) = self.tier.as_ref() {
            labels.insert("routing.tier".into(), tier.clone());
        }
        if !self.tags.is_empty() {
            labels.insert("routing.tags".into(), self.tags.join(","));
        }
        if !self.candidates.is_empty() {
            labels.insert("routing.candidates".into(), self.candidates.join(","));
        }
        if let Some(allow) = self.allow_frontier {
            labels.insert("routing.allow_frontier".into(), allow.to_string());
        }
        if let Some(effort) = self.effort {
            labels.insert("routing.effort".into(), effort.as_str().to_string());
        }
    }
}

impl Workflow {
    /// Compile the materialized frontend model into the filesystem-independent
    /// source-materialized execution
    /// contract consumed by the workflow engine.
    pub fn compile_plan(&self) -> crate::WorkflowResult<crate::WorkflowPlan> {
        crate::WorkflowPlan::compile(self)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn prompt_file_content_participates_in_resume_hash() {
        let dir = TempDir::new().unwrap();
        let workflow_path = dir.path().join("workflow.toml");
        let prompt_path = dir.path().join("prompt.txt");
        let workflow = br#"
[workflow]
name = "hash-test"

[[step]]
id = "one"
target = "test"
prompt_file = "prompt.txt"
"#;
        std::fs::write(&workflow_path, workflow).unwrap();
        std::fs::write(&prompt_path, "first prompt").unwrap();
        let first = Workflow::from_path(&workflow_path).unwrap();

        std::fs::write(&prompt_path, "second prompt").unwrap();
        let second = Workflow::from_path(&workflow_path).unwrap();

        assert_ne!(first.file_sha256, second.file_sha256);
        assert_eq!(
            first.steps[0].prompt_template.as_deref(),
            Some("first prompt")
        );
        assert_eq!(
            second.steps[0].prompt_template.as_deref(),
            Some("second prompt")
        );
    }
}
