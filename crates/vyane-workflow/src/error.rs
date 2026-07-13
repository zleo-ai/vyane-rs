use std::path::PathBuf;

use thiserror::Error;

pub type WorkflowResult<T> = std::result::Result<T, WorkflowError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationReport {
    pub problems: Vec<String>,
}

impl ValidationReport {
    pub fn new(problems: Vec<String>) -> Self {
        Self { problems }
    }

    pub fn is_empty(&self) -> bool {
        self.problems.is_empty()
    }
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "workflow validation failed with {} problem(s):",
            self.problems.len()
        )?;
        for problem in &self.problems {
            writeln!(f, "- {problem}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("failed to read workflow file {path}: {source}")]
    ReadWorkflow {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse workflow file {path}: invalid TOML")]
    ParseWorkflow { path: PathBuf },
    #[error("invalid workflow plan: {reason}")]
    InvalidWorkflowPlan { reason: String },
    #[error("failed to read prompt file {path}: {source}")]
    ReadPrompt {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("workflow TOML at {path} exceeds the {limit}-byte limit (observed {actual} bytes)")]
    WorkflowSourceTooLarge {
        path: PathBuf,
        limit: usize,
        actual: u64,
    },
    #[error("workflow prompt `{path}` exceeds the {limit}-byte limit (observed {actual} bytes)")]
    WorkflowPromptTooLarge {
        path: String,
        limit: usize,
        actual: u64,
    },
    #[error("workflow source bundle exceeds the {limit}-byte limit (observed {actual} bytes)")]
    WorkflowSourceBundleTooLarge { limit: usize, actual: usize },
    #[error("workflow source bundle has {actual} entries; limit is {limit}")]
    WorkflowSourceTooManyEntries { limit: usize, actual: usize },
    #[error(
        "step {step} has an invalid prompt_file path; expected canonical UTF-8 relative components"
    )]
    InvalidWorkflowPromptPath { step: usize },
    #[error("workflow prompt `{path}` resolves outside the workflow directory")]
    WorkflowPromptPathEscape { path: String },
    #[error("workflow prompt `{path}` is not a regular file")]
    WorkflowPromptNotRegular { path: String },
    #[error("workflow source bundle contains duplicate prompt entry `{path}`")]
    DuplicateWorkflowPromptEntry { path: String },
    #[error("workflow source bundle is missing declared prompt entry `{path}`")]
    MissingWorkflowPromptEntry { path: String },
    #[error("workflow source bundle contains undeclared prompt entry `{path}`")]
    ExtraWorkflowPromptEntry { path: String },
    #[error("{0}")]
    Validation(ValidationReport),
    #[error("failed to write workflow journal {path}: {source}")]
    WriteJournal {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read workflow journal {path}: {source}")]
    ReadJournal {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse workflow journal {path}: {source}")]
    ParseJournal {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid workflow run ID `{value}`: expected a canonical lowercase hyphenated UUIDv7")]
    InvalidRunId { value: String },
    #[error(
        "workflow journal ID mismatch in {path}: requested {requested}, journal contains {actual}"
    )]
    JournalIdMismatch {
        path: PathBuf,
        requested: String,
        actual: String,
    },
    #[error(
        "invalid workflow journal filename {path}: expected `<canonical lowercase hyphenated UUIDv7>.json`"
    )]
    InvalidJournalFileName { path: PathBuf },
    #[error("workflow journal already exists for run {wf_run_id} at {path}")]
    JournalAlreadyExists { path: PathBuf, wf_run_id: String },
    #[error(
        "workflow file hash changed for resume: journal has {expected}, current file is {actual}"
    )]
    WorkflowHashChanged { expected: String, actual: String },
}

impl WorkflowError {
    pub fn validation(problems: Vec<String>) -> Self {
        WorkflowError::Validation(ValidationReport::new(problems))
    }

    pub fn is_validation_or_config(&self) -> bool {
        matches!(
            self,
            WorkflowError::Validation(_)
                | WorkflowError::ParseWorkflow { .. }
                | WorkflowError::InvalidWorkflowPlan { .. }
                | WorkflowError::InvalidRunId { .. }
                | WorkflowError::WorkflowSourceTooLarge { .. }
                | WorkflowError::WorkflowPromptTooLarge { .. }
                | WorkflowError::WorkflowSourceBundleTooLarge { .. }
                | WorkflowError::WorkflowSourceTooManyEntries { .. }
                | WorkflowError::InvalidWorkflowPromptPath { .. }
                | WorkflowError::WorkflowPromptPathEscape { .. }
                | WorkflowError::WorkflowPromptNotRegular { .. }
                | WorkflowError::DuplicateWorkflowPromptEntry { .. }
                | WorkflowError::MissingWorkflowPromptEntry { .. }
                | WorkflowError::ExtraWorkflowPromptEntry { .. }
                | WorkflowError::WorkflowHashChanged { .. }
        )
    }
}
