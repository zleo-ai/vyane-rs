use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vyane_core::Sandbox;

use crate::error::{WorkflowError, WorkflowResult};
use crate::journal::WorkflowJournal;

#[derive(Debug, Clone)]
pub struct Workflow {
    pub name: String,
    pub description: Option<String>,
    pub max_concurrency: usize,
    pub steps: Vec<WorkflowStep>,
    pub file_path: PathBuf,
    pub file_sha256: String,
}

impl Workflow {
    pub fn from_path(path: impl AsRef<Path>) -> WorkflowResult<Self> {
        let path = path.as_ref().to_path_buf();
        let bytes = std::fs::read(&path).map_err(|source| WorkflowError::ReadWorkflow {
            path: path.clone(),
            source,
        })?;
        Self::from_bytes(path, &bytes)
    }

    pub fn from_bytes(path: PathBuf, bytes: &[u8]) -> WorkflowResult<Self> {
        let text = std::str::from_utf8(bytes).map_err(|source| WorkflowError::ReadWorkflow {
            path: path.clone(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
        })?;
        let raw: RawRoot = toml::from_str(text).map_err(|source| WorkflowError::ParseWorkflow {
            path: path.clone(),
            source,
        })?;
        let file_sha256 = sha256_hex(bytes);
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let section = raw.workflow.unwrap_or_default();
        let max_concurrency = section.max_concurrency.unwrap_or(4).max(1);
        let mut steps = Vec::with_capacity(raw.steps.len());

        for (index, raw_step) in raw.steps.into_iter().enumerate() {
            let prompt_template = match (&raw_step.prompt, &raw_step.prompt_file) {
                (Some(prompt), _) => Some(prompt.clone()),
                (None, Some(rel)) => {
                    let full = base_dir.join(rel);
                    Some(
                        std::fs::read_to_string(&full)
                            .map_err(|source| WorkflowError::ReadPrompt { path: full, source })?,
                    )
                }
                (None, None) => None,
            };
            steps.push(WorkflowStep {
                index,
                id: raw_step.id.unwrap_or_default(),
                needs: raw_step.needs,
                targets: StepTargets::from_raw(raw_step.target, raw_step.fan_out),
                prompt: raw_step.prompt,
                prompt_file: raw_step.prompt_file,
                prompt_template,
                system: raw_step.system,
                workdir: raw_step.workdir,
                sandbox: raw_step.sandbox.unwrap_or_default(),
                timeout: raw_step.timeout_secs.map(Duration::from_secs),
                on_error: raw_step.on_error.unwrap_or_default(),
            });
        }

        Ok(Self {
            name: section.name.unwrap_or_default(),
            description: section.description,
            max_concurrency,
            steps,
            file_path: path,
            file_sha256,
        })
    }
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
    fn from_raw(target: Option<String>, fan_out: Option<Vec<String>>) -> Self {
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
    pub wf_run_id: String,
    pub status: WorkflowRunStatus,
    pub journal_path: PathBuf,
    pub journal: WorkflowJournal,
}

#[derive(Debug, Default, Deserialize)]
struct RawRoot {
    #[serde(default)]
    workflow: Option<RawWorkflowSection>,
    #[serde(default, rename = "step")]
    steps: Vec<RawStep>,
}

#[derive(Debug, Default, Deserialize)]
struct RawWorkflowSection {
    name: Option<String>,
    description: Option<String>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct RawStep {
    id: Option<String>,
    #[serde(default)]
    needs: Vec<String>,
    target: Option<String>,
    fan_out: Option<Vec<String>>,
    prompt: Option<String>,
    prompt_file: Option<PathBuf>,
    system: Option<String>,
    workdir: Option<PathBuf>,
    sandbox: Option<Sandbox>,
    timeout_secs: Option<u64>,
    on_error: Option<OnError>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
