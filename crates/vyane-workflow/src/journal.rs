use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::{Uuid, Variant};

use crate::error::{WorkflowError, WorkflowResult};
use crate::model::{Workflow, WorkflowRunStatus};
use crate::plan::WorkflowPlan;

const RUN_ID_EXPECTATION: &str = "a canonical lowercase hyphenated UUIDv7";

/// Stable identity for one workflow run and its journal.
///
/// Values can only be constructed from canonical lowercase hyphenated UUIDv7
/// strings. Keeping the inner string private makes it safe to use as a journal
/// filename component.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct WorkflowRunId(String);

impl WorkflowRunId {
    pub fn generate() -> Self {
        Self(Uuid::now_v7().hyphenated().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkflowRunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for WorkflowRunId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for WorkflowRunId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl FromStr for WorkflowRunId {
    type Err = WorkflowRunIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = Uuid::parse_str(value).map_err(|_| WorkflowRunIdError {
            value: value.to_string(),
        })?;
        let canonical = parsed.hyphenated().to_string();
        if parsed.get_version_num() != 7
            || parsed.get_variant() != Variant::RFC4122
            || canonical != value
        {
            return Err(WorkflowRunIdError {
                value: value.to_string(),
            });
        }
        Ok(Self(canonical))
    }
}

impl TryFrom<String> for WorkflowRunId {
    type Error = WorkflowRunIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<&str> for WorkflowRunId {
    type Error = WorkflowRunIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl<'de> Deserialize<'de> for WorkflowRunId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRunIdError {
    value: String,
}

impl fmt::Display for WorkflowRunIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid workflow run ID `{}`: expected {RUN_ID_EXPECTATION}",
            self.value
        )
    }
}

impl std::error::Error for WorkflowRunIdError {}

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
    pub wf_run_id: WorkflowRunId,
    pub workflow_name: String,
    pub file_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<WorkflowReplayProvenance>,
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: WorkflowRunStatus,
    pub steps: BTreeMap<String, JournalStep>,
}

/// Body-free lineage describing a new-run replay from one prior journal.
///
/// `reused_steps_sha256` is a drift checksum over copied journal data, not an
/// authenticity or external-provenance proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowReplayProvenance {
    pub source_wf_run_id: WorkflowRunId,
    pub source_plan_sha256: String,
    pub reused_steps_sha256: String,
    pub reused_step_ids: Vec<String>,
}

impl WorkflowJournal {
    pub fn new(wf: &Workflow, vars: BTreeMap<String, String>) -> Self {
        Self::new_with_id(WorkflowRunId::generate(), wf, vars)
    }

    pub fn new_with_id(
        wf_run_id: WorkflowRunId,
        wf: &Workflow,
        vars: BTreeMap<String, String>,
    ) -> Self {
        let now = Utc::now();
        let steps = wf
            .steps
            .iter()
            .map(|step| (step.id.clone(), JournalStep::pending()))
            .collect();
        Self {
            wf_run_id,
            workflow_name: wf.name.clone(),
            file_sha256: wf.file_sha256.clone(),
            plan_sha256: wf.compile_plan().ok().map(|plan| plan.plan_sha256),
            replay: None,
            vars,
            started_at: now,
            updated_at: now,
            status: WorkflowRunStatus::Running,
            steps,
        }
    }

    pub fn new_with_plan(
        wf_run_id: WorkflowRunId,
        plan: &WorkflowPlan,
        vars: BTreeMap<String, String>,
    ) -> Self {
        let now = Utc::now();
        let steps = plan
            .steps
            .iter()
            .map(|step| (step.id.clone(), JournalStep::pending()))
            .collect();
        Self {
            wf_run_id,
            workflow_name: plan.name.clone(),
            file_sha256: plan.source_sha256.clone(),
            plan_sha256: Some(plan.plan_sha256.clone()),
            replay: None,
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
    pub id: WorkflowRunId,
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

pub fn journal_path(journal_dir: &Path, wf_run_id: &WorkflowRunId) -> PathBuf {
    journal_dir.join(format!("{wf_run_id}.json"))
}

pub fn write_journal_atomic(
    journal_dir: &Path,
    journal: &mut WorkflowJournal,
) -> WorkflowResult<()> {
    write_journal(journal_dir, journal, JournalPublish::Replace)
}

pub fn write_journal_create_atomic(
    journal_dir: &Path,
    journal: &mut WorkflowJournal,
) -> WorkflowResult<()> {
    write_journal(journal_dir, journal, JournalPublish::CreateOnly)
}

#[derive(Clone, Copy)]
enum JournalPublish {
    CreateOnly,
    Replace,
}

fn write_journal(
    journal_dir: &Path,
    journal: &mut WorkflowJournal,
    publish: JournalPublish,
) -> WorkflowResult<()> {
    secure_journal_directory(journal_dir).map_err(|source| WorkflowError::WriteJournal {
        path: journal_dir.to_path_buf(),
        source,
    })?;
    journal.updated_at = Utc::now();
    let path = journal_path(journal_dir, &journal.wf_run_id);
    let bytes =
        serde_json::to_vec_pretty(journal).map_err(|source| WorkflowError::ParseJournal {
            path: path.clone(),
            source,
        })?;

    let (mut file, temp) = open_unique_temp(journal_dir, &journal.wf_run_id).map_err(|source| {
        WorkflowError::WriteJournal {
            path: journal_dir.to_path_buf(),
            source,
        }
    })?;
    let tmp_path = temp.path().to_path_buf();
    let write_result = file.write_all(&bytes).and_then(|()| file.sync_all());
    drop(file);
    write_result.map_err(|source| WorkflowError::WriteJournal {
        path: tmp_path,
        source,
    })?;
    match publish {
        JournalPublish::CreateOnly => {
            temp.publish_new(&path).map_err(|source| {
                if source.kind() == std::io::ErrorKind::AlreadyExists {
                    WorkflowError::JournalAlreadyExists {
                        path: path.clone(),
                        wf_run_id: journal.wf_run_id.to_string(),
                    }
                } else {
                    WorkflowError::WriteJournal {
                        path: path.clone(),
                        source,
                    }
                }
            })?;
        }
        JournalPublish::Replace => {
            temp.replace(&path)
                .map_err(|source| WorkflowError::WriteJournal {
                    path: path.clone(),
                    source,
                })?;
        }
    }
    sync_journal_directory(journal_dir).map_err(|source| WorkflowError::WriteJournal {
        path: journal_dir.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn secure_journal_directory(journal_dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(journal_dir)?;
    #[cfg(unix)]
    std::fs::set_permissions(journal_dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(unix)]
fn sync_journal_directory(journal_dir: &Path) -> std::io::Result<()> {
    File::open(journal_dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_journal_directory(_journal_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

fn open_unique_temp(
    journal_dir: &Path,
    wf_run_id: &WorkflowRunId,
) -> std::io::Result<(File, TempPath)> {
    for _ in 0..16 {
        let path = journal_dir.join(format!(".{wf_run_id}.{}.tmp", Uuid::now_v7()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);

        match options.open(&path) {
            Ok(file) => {
                let temp = TempPath::new(path);
                #[cfg(unix)]
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
                return Ok((file, temp));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique workflow journal temporary file",
    ))
}

struct TempPath {
    path: PathBuf,
    committed: bool,
}

impl TempPath {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn replace(mut self, destination: &Path) -> std::io::Result<()> {
        std::fs::rename(self.path(), destination)?;
        self.committed = true;
        Ok(())
    }

    fn publish_new(mut self, destination: &Path) -> std::io::Result<()> {
        std::fs::hard_link(self.path(), destination)?;
        // The hard link is the atomic, create-only commit point. Failure to
        // unlink the private temporary name must not turn a known-successful
        // publication into an ambiguous error that a caller might retry. Drop
        // makes one more best-effort unlink attempt when the first one fails.
        if std::fs::remove_file(self.path()).is_ok() {
            self.committed = true;
        }
        Ok(())
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn parse_run_id(value: &str) -> WorkflowResult<WorkflowRunId> {
    value
        .parse()
        .map_err(|_: WorkflowRunIdError| WorkflowError::InvalidRunId {
            value: value.to_string(),
        })
}

fn read_journal_for_id(
    journal_dir: &Path,
    wf_run_id: &WorkflowRunId,
) -> WorkflowResult<WorkflowJournal> {
    let path = journal_path(journal_dir, wf_run_id);
    read_journal_at_path(&path, wf_run_id)
}

fn read_journal_at_path(path: &Path, wf_run_id: &WorkflowRunId) -> WorkflowResult<WorkflowJournal> {
    let bytes = std::fs::read(path).map_err(|source| WorkflowError::ReadJournal {
        path: path.to_path_buf(),
        source,
    })?;
    let journal: WorkflowJournal =
        serde_json::from_slice(&bytes).map_err(|source| WorkflowError::ParseJournal {
            path: path.to_path_buf(),
            source,
        })?;
    if &journal.wf_run_id != wf_run_id {
        return Err(WorkflowError::JournalIdMismatch {
            path: path.to_path_buf(),
            requested: wf_run_id.to_string(),
            actual: journal.wf_run_id.to_string(),
        });
    }
    Ok(journal)
}

pub fn read_journal(journal_dir: &Path, wf_run_id: &str) -> WorkflowResult<WorkflowJournal> {
    let wf_run_id = parse_run_id(wf_run_id)?;
    read_journal_for_id(journal_dir, &wf_run_id)
}

fn run_id_from_journal_filename(path: &Path) -> WorkflowResult<WorkflowRunId> {
    let value = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".json"))
        .ok_or_else(|| WorkflowError::InvalidJournalFileName {
            path: path.to_path_buf(),
        })?;
    value.parse().map_err(
        |_: WorkflowRunIdError| WorkflowError::InvalidJournalFileName {
            path: path.to_path_buf(),
        },
    )
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
        let wf_run_id = run_id_from_journal_filename(&path)?;
        let journal = read_journal_at_path(&path, &wf_run_id)?;
        out.push(WorkflowJournalSummary::from(&journal));
    }
    out.sort_by(|a, b| {
        b.started_at
            .cmp(&a.started_at)
            .then_with(|| b.id.cmp(&a.id))
    });
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const ID_A: &str = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e";
    const ID_B: &str = "01890f3e-7b7d-7cc2-98d2-3f9a2b6c7d8e";

    fn workflow() -> Workflow {
        Workflow {
            name: "journal-test".to_string(),
            description: None,
            max_concurrency: 1,
            steps: Vec::new(),
            file_path: PathBuf::from("workflow.toml"),
            legacy_file_sha256: None,
            file_sha256: "test-hash".to_string(),
        }
    }

    fn journal(id: &str) -> WorkflowJournal {
        WorkflowJournal::new_with_id(id.parse().unwrap(), &workflow(), BTreeMap::new())
    }

    #[test]
    fn workflow_run_id_accepts_only_canonical_uuid_v7() {
        let id: WorkflowRunId = ID_A.parse().unwrap();
        assert_eq!(id.as_str(), ID_A);
        let generated = WorkflowRunId::generate();
        assert_eq!(
            generated.as_str().parse::<WorkflowRunId>().unwrap(),
            generated
        );

        for invalid in [
            "../../outside",
            "550e8400-e29b-41d4-a716-446655440000",
            "01890F3E-7B7C-7CC2-98D2-3F9A2B6C7D8E",
            "01890f3e7b7c7cc298d23f9a2b6c7d8e",
            "01890f3e-7b7c-7cc2-78d2-3f9a2b6c7d8e",
        ] {
            assert!(invalid.parse::<WorkflowRunId>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn workflow_run_id_deserialization_revalidates_the_value() {
        let valid: WorkflowRunId = serde_json::from_str(&format!("\"{ID_A}\"")).unwrap();
        assert_eq!(valid.as_str(), ID_A);

        let invalid =
            serde_json::from_str::<WorkflowRunId>("\"01890F3E-7B7C-7CC2-98D2-3F9A2B6C7D8E\"");
        assert!(invalid.is_err());
    }

    #[test]
    fn read_rejects_traversal_and_noncanonical_ids_before_path_access() {
        let dir = TempDir::new().unwrap();
        for invalid in [
            "../outside",
            "550e8400-e29b-41d4-a716-446655440000",
            "01890F3E-7B7C-7CC2-98D2-3F9A2B6C7D8E",
        ] {
            let error = read_journal(dir.path(), invalid).unwrap_err();
            assert!(matches!(error, WorkflowError::InvalidRunId { .. }));
        }
    }

    #[test]
    fn read_rejects_a_valid_journal_stored_under_a_different_id() {
        let dir = TempDir::new().unwrap();
        let requested: WorkflowRunId = ID_A.parse().unwrap();
        let stored = journal(ID_B);
        std::fs::write(
            journal_path(dir.path(), &requested),
            serde_json::to_vec_pretty(&stored).unwrap(),
        )
        .unwrap();

        let error = read_journal(dir.path(), requested.as_str()).unwrap_err();
        assert!(matches!(
            error,
            WorkflowError::JournalIdMismatch {
                requested: ref expected,
                actual: ref found,
                ..
            } if expected == ID_A && found == ID_B
        ));

        let list_error = list_journals(dir.path()).unwrap_err();
        assert!(matches!(
            list_error,
            WorkflowError::JournalIdMismatch {
                requested: ref expected,
                actual: ref found,
                ..
            } if expected == ID_A && found == ID_B
        ));
    }

    #[test]
    fn list_rejects_a_noncanonical_journal_filename() {
        let dir = TempDir::new().unwrap();
        let stored = journal(ID_A);
        let path = dir.path().join("01890F3E-7B7C-7CC2-98D2-3F9A2B6C7D8E.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&stored).unwrap()).unwrap();

        let error = list_journals(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            WorkflowError::InvalidJournalFileName { path: ref invalid } if invalid == &path
        ));
    }

    #[test]
    fn create_only_publish_never_overwrites_an_existing_journal() {
        let dir = TempDir::new().unwrap();
        let journal_dir = dir.path().join("journals");
        let mut first = journal(ID_A);
        first.workflow_name = "first".to_string();
        write_journal_create_atomic(&journal_dir, &mut first).unwrap();
        let path = journal_path(&journal_dir, &first.wf_run_id);
        let before = std::fs::read(&path).unwrap();

        let mut second = journal(ID_A);
        second.workflow_name = "second".to_string();
        let error = write_journal_create_atomic(&journal_dir, &mut second).unwrap_err();
        assert!(matches!(
            error,
            WorkflowError::JournalAlreadyExists {
                wf_run_id: ref id,
                path: ref existing,
            } if id == ID_A && existing == &path
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!std::fs::read_dir(&journal_dir).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));

        write_journal_atomic(&journal_dir, &mut second).unwrap();
        assert_eq!(
            read_journal(&journal_dir, ID_A).unwrap().workflow_name,
            "second"
        );
    }

    #[test]
    fn concurrent_create_only_publish_has_exactly_one_winner() {
        let dir = TempDir::new().unwrap();
        let journal_dir = dir.path().join("journals");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut first = journal(ID_A);
        first.workflow_name = "first".to_string();
        let mut second = journal(ID_A);
        second.workflow_name = "second".to_string();

        let (first_result, second_result) = std::thread::scope(|scope| {
            let first_dir = journal_dir.clone();
            let first_barrier = std::sync::Arc::clone(&barrier);
            let first_handle = scope.spawn(move || {
                first_barrier.wait();
                write_journal_create_atomic(&first_dir, &mut first)
            });
            let second_dir = journal_dir.clone();
            let second_handle = scope.spawn(move || {
                barrier.wait();
                write_journal_create_atomic(&second_dir, &mut second)
            });
            (first_handle.join().unwrap(), second_handle.join().unwrap())
        });

        assert_eq!(
            usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
            1
        );
        for error in [first_result, second_result]
            .into_iter()
            .filter_map(Result::err)
        {
            assert!(matches!(error, WorkflowError::JournalAlreadyExists { .. }));
        }
        let stored = read_journal(&journal_dir, ID_A).unwrap();
        assert!(matches!(stored.workflow_name.as_str(), "first" | "second"));
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_sets_private_directory_and_file_permissions() {
        let dir = TempDir::new().unwrap();
        let journal_dir = dir.path().join("journals");
        let mut journal = journal(ID_A);

        write_journal_create_atomic(&journal_dir, &mut journal).unwrap();

        let directory_mode = std::fs::metadata(&journal_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let file_mode = std::fs::metadata(journal_path(&journal_dir, &journal.wf_run_id))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[test]
    fn failed_atomic_rename_cleans_up_the_temporary_file() {
        let dir = TempDir::new().unwrap();
        let journal_dir = dir.path().join("journals");
        let mut journal = journal(ID_A);
        std::fs::create_dir_all(journal_path(&journal_dir, &journal.wf_run_id)).unwrap();

        let error = write_journal_atomic(&journal_dir, &mut journal).unwrap_err();
        assert!(matches!(error, WorkflowError::WriteJournal { .. }));
        let has_temp = std::fs::read_dir(&journal_dir).unwrap().any(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            name.starts_with('.') && name.ends_with(".tmp")
        });
        assert!(!has_temp);
    }
}
