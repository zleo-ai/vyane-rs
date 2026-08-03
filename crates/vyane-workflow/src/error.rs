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

    /// Stable snake_case *kind* token; paths, run ids, and IO payloads stay out.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadWorkflow { .. } => "read_workflow",
            Self::ParseWorkflow { .. } => "parse_workflow",
            Self::InvalidWorkflowPlan { .. } => "invalid_workflow_plan",
            Self::ReadPrompt { .. } => "read_prompt",
            Self::WorkflowSourceTooLarge { .. } => "workflow_source_too_large",
            Self::WorkflowPromptTooLarge { .. } => "workflow_prompt_too_large",
            Self::WorkflowSourceBundleTooLarge { .. } => "workflow_source_bundle_too_large",
            Self::WorkflowSourceTooManyEntries { .. } => "workflow_source_too_many_entries",
            Self::InvalidWorkflowPromptPath { .. } => "invalid_workflow_prompt_path",
            Self::WorkflowPromptPathEscape { .. } => "workflow_prompt_path_escape",
            Self::WorkflowPromptNotRegular { .. } => "workflow_prompt_not_regular",
            Self::DuplicateWorkflowPromptEntry { .. } => "duplicate_workflow_prompt_entry",
            Self::MissingWorkflowPromptEntry { .. } => "missing_workflow_prompt_entry",
            Self::ExtraWorkflowPromptEntry { .. } => "extra_workflow_prompt_entry",
            Self::Validation(_) => "validation",
            Self::WriteJournal { .. } => "write_journal",
            Self::ReadJournal { .. } => "read_journal",
            Self::ParseJournal { .. } => "parse_journal",
            Self::InvalidRunId { .. } => "invalid_run_id",
            Self::JournalIdMismatch { .. } => "journal_id_mismatch",
            Self::InvalidJournalFileName { .. } => "invalid_journal_file_name",
            Self::JournalAlreadyExists { .. } => "journal_already_exists",
            Self::WorkflowHashChanged { .. } => "workflow_hash_changed",
        }
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

#[cfg(test)]
mod tests {
    use super::WorkflowError;
    use std::path::PathBuf;

    #[test]
    fn workflow_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(
            WorkflowError::ReadWorkflow {
                path: PathBuf::from("/secret/path.toml"),
                source: std::io::Error::other("secret-io"),
            }
            .as_str(),
            "read_workflow"
        );
        assert_eq!(
            WorkflowError::ParseWorkflow {
                path: PathBuf::from("/secret/path.toml")
            }
            .as_str(),
            "parse_workflow"
        );
        assert_eq!(
            WorkflowError::InvalidWorkflowPlan {
                reason: "secret-reason".into()
            }
            .as_str(),
            "invalid_workflow_plan"
        );
        assert_eq!(
            WorkflowError::ReadPrompt {
                path: PathBuf::from("/secret/prompt.md"),
                source: std::io::Error::other("secret-io"),
            }
            .as_str(),
            "read_prompt"
        );
        assert_eq!(
            WorkflowError::WorkflowSourceTooLarge {
                path: PathBuf::from("/secret/path.toml"),
                limit: 1,
                actual: 2,
            }
            .as_str(),
            "workflow_source_too_large"
        );
        assert_eq!(
            WorkflowError::WorkflowPromptTooLarge {
                path: "secret.md".into(),
                limit: 1,
                actual: 2,
            }
            .as_str(),
            "workflow_prompt_too_large"
        );
        assert_eq!(
            WorkflowError::WorkflowSourceBundleTooLarge {
                limit: 1,
                actual: 2
            }
            .as_str(),
            "workflow_source_bundle_too_large"
        );
        assert_eq!(
            WorkflowError::WorkflowSourceTooManyEntries {
                limit: 1,
                actual: 2
            }
            .as_str(),
            "workflow_source_too_many_entries"
        );
        assert_eq!(
            WorkflowError::InvalidWorkflowPromptPath { step: 3 }.as_str(),
            "invalid_workflow_prompt_path"
        );
        assert_eq!(
            WorkflowError::WorkflowPromptPathEscape {
                path: "../secret".into()
            }
            .as_str(),
            "workflow_prompt_path_escape"
        );
        assert_eq!(
            WorkflowError::WorkflowPromptNotRegular {
                path: "secret.md".into()
            }
            .as_str(),
            "workflow_prompt_not_regular"
        );
        assert_eq!(
            WorkflowError::DuplicateWorkflowPromptEntry {
                path: "secret.md".into()
            }
            .as_str(),
            "duplicate_workflow_prompt_entry"
        );
        assert_eq!(
            WorkflowError::MissingWorkflowPromptEntry {
                path: "secret.md".into()
            }
            .as_str(),
            "missing_workflow_prompt_entry"
        );
        assert_eq!(
            WorkflowError::ExtraWorkflowPromptEntry {
                path: "secret.md".into()
            }
            .as_str(),
            "extra_workflow_prompt_entry"
        );
        assert_eq!(
            WorkflowError::validation(vec!["secret-problem".into()]).as_str(),
            "validation"
        );
        assert_eq!(
            WorkflowError::WriteJournal {
                path: PathBuf::from("/secret/j.json"),
                source: std::io::Error::other("secret-io"),
            }
            .as_str(),
            "write_journal"
        );
        assert_eq!(
            WorkflowError::ReadJournal {
                path: PathBuf::from("/secret/j.json"),
                source: std::io::Error::other("secret-io"),
            }
            .as_str(),
            "read_journal"
        );
        assert_eq!(
            WorkflowError::ParseJournal {
                path: PathBuf::from("/secret/j.json"),
                source: serde_json::from_str::<()>("not-json").expect_err("parse"),
            }
            .as_str(),
            "parse_journal"
        );
        assert_eq!(
            WorkflowError::InvalidRunId {
                value: "secret-run".into()
            }
            .as_str(),
            "invalid_run_id"
        );
        assert_eq!(
            WorkflowError::JournalIdMismatch {
                path: PathBuf::from("/secret/j.json"),
                requested: "req".into(),
                actual: "act".into(),
            }
            .as_str(),
            "journal_id_mismatch"
        );
        assert_eq!(
            WorkflowError::InvalidJournalFileName {
                path: PathBuf::from("/secret/j.json")
            }
            .as_str(),
            "invalid_journal_file_name"
        );
        assert_eq!(
            WorkflowError::JournalAlreadyExists {
                path: PathBuf::from("/secret/j.json"),
                wf_run_id: "secret-run".into(),
            }
            .as_str(),
            "journal_already_exists"
        );
        assert_eq!(
            WorkflowError::WorkflowHashChanged {
                expected: "a".into(),
                actual: "b".into(),
            }
            .as_str(),
            "workflow_hash_changed"
        );
        assert!(
            !WorkflowError::InvalidWorkflowPlan {
                reason: "secret-reason".into()
            }
            .as_str()
            .contains("secret")
        );
    }
}
