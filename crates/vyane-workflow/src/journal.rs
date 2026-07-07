use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{WorkflowError, WorkflowResult};
use crate::model::{Workflow, WorkflowRunStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalStepStatus {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
    Cancelled,
}

impl JournalStepStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JournalStepStatus::Success
                | JournalStepStatus::Failed
                | JournalStepStatus::Skipped
                | JournalStepStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalTargetOutput {
    pub target: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalStep {
    pub status: JournalStepStatus,
    #[serde(default)]
    pub run_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Vec<JournalTargetOutput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JournalStep {
    pub fn pending() -> Self {
        Self {
            status: JournalStepStatus::Pending,
            run_ids: Vec::new(),
            output: None,
            outputs: None,
            error: None,
        }
    }

    pub fn reset_for_rerun(&mut self) {
        if self.status == JournalStepStatus::Success {
            return;
        }
        *self = JournalStep::pending();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowJournal {
    pub wf_run_id: String,
    pub workflow_name: String,
    pub file_sha256: String,
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: WorkflowRunStatus,
    pub steps: BTreeMap<String, JournalStep>,
}

impl WorkflowJournal {
    pub fn new(wf: &Workflow, vars: BTreeMap<String, String>) -> Self {
        let now = Utc::now();
        let steps = wf
            .steps
            .iter()
            .map(|step| (step.id.clone(), JournalStep::pending()))
            .collect();
        Self {
            wf_run_id: Uuid::now_v7().to_string(),
            workflow_name: wf.name.clone(),
            file_sha256: wf.file_sha256.clone(),
            vars,
            started_at: now,
            updated_at: now,
            status: WorkflowRunStatus::Running,
            steps,
        }
    }

    pub fn counts(&self) -> WorkflowStepCounts {
        let mut counts = WorkflowStepCounts::default();
        for step in self.steps.values() {
            match step.status {
                JournalStepStatus::Pending => counts.pending += 1,
                JournalStepStatus::Running => counts.running += 1,
                JournalStepStatus::Success => counts.success += 1,
                JournalStepStatus::Failed => counts.failed += 1,
                JournalStepStatus::Skipped => counts.skipped += 1,
                JournalStepStatus::Cancelled => counts.cancelled += 1,
            }
        }
        counts
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkflowStepCounts {
    pub pending: usize,
    pub running: usize,
    pub success: usize,
    pub failed: usize,
    pub skipped: usize,
    pub cancelled: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowJournalSummary {
    pub id: String,
    pub name: String,
    pub status: WorkflowRunStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub steps: WorkflowStepCounts,
}

impl From<&WorkflowJournal> for WorkflowJournalSummary {
    fn from(journal: &WorkflowJournal) -> Self {
        Self {
            id: journal.wf_run_id.clone(),
            name: journal.workflow_name.clone(),
            status: journal.status,
            started_at: journal.started_at,
            updated_at: journal.updated_at,
            steps: journal.counts(),
        }
    }
}

pub fn journal_path(journal_dir: &Path, wf_run_id: &str) -> PathBuf {
    journal_dir.join(format!("{wf_run_id}.json"))
}

pub fn write_journal_atomic(
    journal_dir: &Path,
    journal: &mut WorkflowJournal,
) -> WorkflowResult<()> {
    std::fs::create_dir_all(journal_dir).map_err(|source| WorkflowError::WriteJournal {
        path: journal_dir.to_path_buf(),
        source,
    })?;
    journal.updated_at = Utc::now();
    let path = journal_path(journal_dir, &journal.wf_run_id);
    let tmp_path = journal_dir.join(format!(".{}.{}.tmp", journal.wf_run_id, Uuid::now_v7()));
    let bytes =
        serde_json::to_vec_pretty(journal).map_err(|source| WorkflowError::ParseJournal {
            path: path.clone(),
            source,
        })?;
    std::fs::write(&tmp_path, bytes).map_err(|source| WorkflowError::WriteJournal {
        path: tmp_path.clone(),
        source,
    })?;
    std::fs::rename(&tmp_path, &path).map_err(|source| WorkflowError::WriteJournal {
        path: path.clone(),
        source,
    })?;
    Ok(())
}

pub fn read_journal(journal_dir: &Path, wf_run_id: &str) -> WorkflowResult<WorkflowJournal> {
    let path = journal_path(journal_dir, wf_run_id);
    let bytes = std::fs::read(&path).map_err(|source| WorkflowError::ReadJournal {
        path: path.clone(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| WorkflowError::ParseJournal { path, source })
}

pub fn list_journals(journal_dir: &Path) -> WorkflowResult<Vec<WorkflowJournalSummary>> {
    let entries = match std::fs::read_dir(journal_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(WorkflowError::ReadJournal {
                path: journal_dir.to_path_buf(),
                source,
            });
        }
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| WorkflowError::ReadJournal {
            path: journal_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|source| WorkflowError::ReadJournal {
            path: path.clone(),
            source,
        })?;
        let journal: WorkflowJournal = serde_json::from_slice(&bytes)
            .map_err(|source| WorkflowError::ParseJournal { path, source })?;
        out.push(WorkflowJournalSummary::from(&journal));
    }
    out.sort_by(|a, b| {
        b.started_at
            .cmp(&a.started_at)
            .then_with(|| b.id.cmp(&a.id))
    });
    Ok(out)
}
