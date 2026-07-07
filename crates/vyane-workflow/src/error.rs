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
    #[error("failed to parse workflow file {path}: {source}")]
    ParseWorkflow {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to read prompt file {path}: {source}")]
    ReadPrompt {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
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
            WorkflowError::Validation(_) | WorkflowError::WorkflowHashChanged { .. }
        )
    }
}
