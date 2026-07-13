use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use vyane_core::Sandbox;

use crate::error::{WorkflowError, WorkflowResult};
use crate::model::{OnError, StepTargets, Workflow, WorkflowRouteHints, WorkflowStep};

pub const WORKFLOW_SOURCE_MAX_TOML_BYTES: usize = 1024 * 1024;
pub const WORKFLOW_SOURCE_MAX_PROMPT_BYTES: usize = 4 * 1024 * 1024;
pub const WORKFLOW_SOURCE_MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
pub const WORKFLOW_SOURCE_MAX_ENTRIES: usize = 128;
pub const WORKFLOW_SOURCE_MAX_PATH_BYTES: usize = 4096;

const BUNDLE_DISPLAY_PATH: &str = "(workflow-source-bundle)";
const SOURCE_HASH_DOMAIN: &[u8] = b"vyane.workflow.source-bundle\0v1\0";

/// A canonical, portable path for one prompt source in a workflow bundle.
///
/// The wire form is always a UTF-8 relative path separated by `/`. Values are
/// rejected rather than normalized so the client and daemon hash the exact
/// same path spelling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct WorkflowSourcePath(String);

impl WorkflowSourcePath {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl fmt::Display for WorkflowSourcePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for WorkflowSourcePath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for WorkflowSourcePath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl FromStr for WorkflowSourcePath {
    type Err = WorkflowSourcePathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_source_path(value)?;
        Ok(Self(value.to_string()))
    }
}

impl TryFrom<String> for WorkflowSourcePath {
    type Error = WorkflowSourcePathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<&str> for WorkflowSourcePath {
    type Error = WorkflowSourcePathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<&Path> for WorkflowSourcePath {
    type Error = WorkflowSourcePathError;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        value.to_str().ok_or(WorkflowSourcePathError)?.parse()
    }
}

impl<'de> Deserialize<'de> for WorkflowSourcePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowSourcePathError;

impl fmt::Display for WorkflowSourcePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expected canonical UTF-8 relative path components")
    }
}

impl std::error::Error for WorkflowSourcePathError {}

fn validate_source_path(value: &str) -> Result<(), WorkflowSourcePathError> {
    let bytes = value.as_bytes();
    if value.is_empty()
        || bytes.len() > WORKFLOW_SOURCE_MAX_PATH_BYTES
        || value.contains('\0')
        || value.contains('\\')
        || value.starts_with('/')
        || value.ends_with('/')
        || (bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
        || value
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(WorkflowSourcePathError);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSourceEntry {
    pub path: WorkflowSourcePath,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSourceBundle {
    pub workflow_toml: String,
    #[serde(default)]
    pub prompt_files: Vec<WorkflowSourceEntry>,
}

impl WorkflowSourceBundle {
    /// Collect a bounded workflow source bundle from the client filesystem.
    pub fn from_path(path: impl AsRef<Path>) -> WorkflowResult<Self> {
        let path = path.as_ref().to_path_buf();
        let bytes = read_workflow_limited(&path)?;
        Self::collect_from_bytes(&path, &bytes)
    }

    /// Parse and materialize this bundle without consulting the filesystem.
    pub fn materialize(&self) -> WorkflowResult<Workflow> {
        self.materialize_at(PathBuf::from(BUNDLE_DISPLAY_PATH))
    }

    /// Alias for [`Self::materialize`], suitable for server request handling.
    pub fn parse(&self) -> WorkflowResult<Workflow> {
        self.materialize()
    }

    fn collect_from_bytes(path: &Path, bytes: &[u8]) -> WorkflowResult<Self> {
        enforce_workflow_size(path, bytes.len() as u64)?;
        let workflow_toml =
            String::from_utf8(bytes.to_vec()).map_err(|source| WorkflowError::ReadWorkflow {
                path: path.to_path_buf(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
            })?;
        let raw = parse_raw(&workflow_toml, path)?;
        let declared = declared_prompt_paths(&raw)?;
        let mut prompt_files = Vec::with_capacity(declared.len());
        let mut total = workflow_toml.len();

        if !declared.is_empty() {
            let base_dir = workflow_base_dir(path);
            let canonical_base =
                std::fs::canonicalize(base_dir).map_err(|source| WorkflowError::ReadWorkflow {
                    path: path.to_path_buf(),
                    source,
                })?;
            for source_path in declared {
                let requested = base_dir.join(source_path.as_path());
                let canonical = std::fs::canonicalize(&requested).map_err(|source| {
                    WorkflowError::ReadPrompt {
                        path: requested.clone(),
                        source,
                    }
                })?;
                if !canonical.starts_with(&canonical_base) {
                    return Err(WorkflowError::WorkflowPromptPathEscape {
                        path: source_path.to_string(),
                    });
                }
                let metadata =
                    std::fs::metadata(&canonical).map_err(|source| WorkflowError::ReadPrompt {
                        path: requested.clone(),
                        source,
                    })?;
                if !metadata.is_file() {
                    return Err(WorkflowError::WorkflowPromptNotRegular {
                        path: source_path.to_string(),
                    });
                }
                let prompt_bytes = read_prompt_limited(
                    &canonical,
                    &requested,
                    source_path.as_str(),
                    metadata.len(),
                )?;
                total = total
                    .saturating_add(source_path.as_str().len())
                    .saturating_add(prompt_bytes.len());
                enforce_total_size(total)?;
                let content = String::from_utf8(prompt_bytes).map_err(|source| {
                    WorkflowError::ReadPrompt {
                        path: requested,
                        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
                    }
                })?;
                prompt_files.push(WorkflowSourceEntry {
                    path: source_path,
                    content,
                });
            }
        }

        let bundle = Self {
            workflow_toml,
            prompt_files,
        };
        bundle.validate_limits(path)?;
        Ok(bundle)
    }

    fn validate_limits(&self, display_path: &Path) -> WorkflowResult<()> {
        enforce_workflow_size(display_path, self.workflow_toml.len() as u64)?;
        if self.prompt_files.len() > WORKFLOW_SOURCE_MAX_ENTRIES {
            return Err(WorkflowError::WorkflowSourceTooManyEntries {
                limit: WORKFLOW_SOURCE_MAX_ENTRIES,
                actual: self.prompt_files.len(),
            });
        }
        let mut total = self.workflow_toml.len();
        for entry in &self.prompt_files {
            let prompt_len = entry.content.len();
            if prompt_len > WORKFLOW_SOURCE_MAX_PROMPT_BYTES {
                return Err(WorkflowError::WorkflowPromptTooLarge {
                    path: entry.path.to_string(),
                    limit: WORKFLOW_SOURCE_MAX_PROMPT_BYTES,
                    actual: prompt_len as u64,
                });
            }
            total = total
                .saturating_add(entry.path.as_str().len())
                .saturating_add(prompt_len);
            enforce_total_size(total)?;
        }
        Ok(())
    }

    fn materialize_at(&self, display_path: PathBuf) -> WorkflowResult<Workflow> {
        self.validate_limits(&display_path)?;
        let raw = parse_raw(&self.workflow_toml, &display_path)?;
        let declared = declared_prompt_paths(&raw)?;
        let mut entries = BTreeMap::<WorkflowSourcePath, &str>::new();
        for entry in &self.prompt_files {
            if entries
                .insert(entry.path.clone(), entry.content.as_str())
                .is_some()
            {
                return Err(WorkflowError::DuplicateWorkflowPromptEntry {
                    path: entry.path.to_string(),
                });
            }
        }
        for path in &declared {
            if !entries.contains_key(path) {
                return Err(WorkflowError::MissingWorkflowPromptEntry {
                    path: path.to_string(),
                });
            }
        }
        for path in entries.keys() {
            if !declared.contains(path) {
                return Err(WorkflowError::ExtraWorkflowPromptEntry {
                    path: path.to_string(),
                });
            }
        }

        let legacy_file_sha256 = legacy_source_hash(&self.workflow_toml, &raw, &entries)?;
        let file_sha256 = source_hash(&self.workflow_toml, &entries);
        build_workflow(display_path, raw, &entries, legacy_file_sha256, file_sha256)
    }
}

impl Workflow {
    pub fn from_path(path: impl AsRef<Path>) -> WorkflowResult<Self> {
        let path = path.as_ref().to_path_buf();
        WorkflowSourceBundle::from_path(&path)?.materialize_at(path)
    }

    /// Parse workflow bytes using the existing local-file semantics.
    ///
    /// This compatibility entry point collects declared prompt files relative
    /// to `path`. Server-side request handling must use
    /// [`WorkflowSourceBundle::materialize`] instead.
    pub fn from_bytes(path: PathBuf, bytes: &[u8]) -> WorkflowResult<Self> {
        WorkflowSourceBundle::collect_from_bytes(&path, bytes)?.materialize_at(path)
    }

    /// Materialize a previously collected source bundle without filesystem IO.
    pub fn from_source_bundle(bundle: &WorkflowSourceBundle) -> WorkflowResult<Self> {
        bundle.materialize()
    }
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
    prompt_file: Option<String>,
    system: Option<String>,
    workdir: Option<PathBuf>,
    sandbox: Option<Sandbox>,
    timeout_secs: Option<u64>,
    on_error: Option<OnError>,
    route: Option<WorkflowRouteHints>,
}

fn parse_raw(source: &str, display_path: &Path) -> WorkflowResult<RawRoot> {
    toml::from_str(source).map_err(|_| WorkflowError::ParseWorkflow {
        path: display_path.to_path_buf(),
    })
}

fn declared_prompt_paths(raw: &RawRoot) -> WorkflowResult<BTreeSet<WorkflowSourcePath>> {
    let mut declared = BTreeSet::new();
    for (index, step) in raw.steps.iter().enumerate() {
        if step.prompt.is_some() && step.prompt_file.is_some() {
            // Reject before the client collector opens any file. Besides being
            // invalid workflow syntax, reading the ignored `prompt_file`
            // would unnecessarily place unrelated local content in a bundle.
            return Err(WorkflowError::validation(vec![format!(
                "step {} must set exactly one of `prompt` or `prompt_file`, not both",
                index + 1
            )]));
        }
        if let Some(path) = step.prompt_file.as_deref() {
            let path = path.parse().map_err(|_: WorkflowSourcePathError| {
                WorkflowError::InvalidWorkflowPromptPath { step: index + 1 }
            })?;
            declared.insert(path);
        }
    }
    if declared.len() > WORKFLOW_SOURCE_MAX_ENTRIES {
        return Err(WorkflowError::WorkflowSourceTooManyEntries {
            limit: WORKFLOW_SOURCE_MAX_ENTRIES,
            actual: declared.len(),
        });
    }
    Ok(declared)
}

fn build_workflow(
    file_path: PathBuf,
    raw: RawRoot,
    entries: &BTreeMap<WorkflowSourcePath, &str>,
    legacy_file_sha256: String,
    file_sha256: String,
) -> WorkflowResult<Workflow> {
    let section = raw.workflow.unwrap_or_default();
    let max_concurrency = section.max_concurrency.unwrap_or(4).max(1);
    let mut steps = Vec::with_capacity(raw.steps.len());
    for (index, raw_step) in raw.steps.into_iter().enumerate() {
        let prompt_file = raw_step
            .prompt_file
            .as_deref()
            .map(str::parse::<WorkflowSourcePath>)
            .transpose()
            .map_err(|_| WorkflowError::InvalidWorkflowPromptPath { step: index + 1 })?;
        let prompt_template = match (&raw_step.prompt, prompt_file.as_ref()) {
            (Some(prompt), _) => Some(prompt.clone()),
            (None, Some(path)) => entries.get(path).map(|content| (*content).to_string()),
            (None, None) => None,
        };
        steps.push(WorkflowStep {
            index,
            id: raw_step.id.unwrap_or_default(),
            needs: raw_step.needs,
            targets: StepTargets::from_raw(raw_step.target, raw_step.fan_out),
            prompt: raw_step.prompt,
            prompt_file: prompt_file.map(|path| PathBuf::from(path.as_str())),
            prompt_template,
            system: raw_step.system,
            workdir: raw_step.workdir,
            sandbox: raw_step.sandbox.unwrap_or_default(),
            timeout: raw_step.timeout_secs.map(Duration::from_secs),
            on_error: raw_step.on_error.unwrap_or_default(),
            route: raw_step.route.unwrap_or_default(),
        });
    }
    Ok(Workflow {
        name: section.name.unwrap_or_default(),
        description: section.description,
        max_concurrency,
        steps,
        file_path,
        legacy_file_sha256: Some(legacy_file_sha256),
        file_sha256,
    })
}

/// Reproduce the hash written by Vyane before source bundles were introduced.
///
/// This deliberately preserves the old step-order framing for migration only.
/// It is never used for a new journal identity.
fn legacy_source_hash(
    source: &str,
    raw: &RawRoot,
    entries: &BTreeMap<WorkflowSourcePath, &str>,
) -> WorkflowResult<String> {
    let mut hash = Sha256::new();
    hash.update(source.as_bytes());
    for (index, step) in raw.steps.iter().enumerate() {
        if step.prompt.is_some() {
            continue;
        }
        let Some(path) = step.prompt_file.as_deref() else {
            continue;
        };
        let canonical = path
            .parse::<WorkflowSourcePath>()
            .map_err(|_| WorkflowError::InvalidWorkflowPromptPath { step: index + 1 })?;
        let content =
            entries
                .get(&canonical)
                .ok_or_else(|| WorkflowError::MissingWorkflowPromptEntry {
                    path: canonical.to_string(),
                })?;
        hash.update(b"\0prompt-file\0");
        hash.update(path.as_bytes());
        hash.update(b"\0");
        hash.update(content.as_bytes());
    }
    Ok(digest_hex(hash.finalize()))
}

fn source_hash(source: &str, entries: &BTreeMap<WorkflowSourcePath, &str>) -> String {
    let mut hash = Sha256::new();
    hash.update(SOURCE_HASH_DOMAIN);
    hash_field(&mut hash, source.as_bytes());
    for (path, content) in entries {
        hash_field(&mut hash, path.as_str().as_bytes());
        hash_field(&mut hash, content.as_bytes());
    }
    digest_hex(hash.finalize())
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn digest_hex(digest: impl AsRef<[u8]>) -> String {
    let digest = digest.as_ref();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn workflow_base_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn read_workflow_limited(path: &Path) -> WorkflowResult<Vec<u8>> {
    let file = File::open(path).map_err(|source| WorkflowError::ReadWorkflow {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = file
        .metadata()
        .map_err(|source| WorkflowError::ReadWorkflow {
            path: path.to_path_buf(),
            source,
        })?;
    enforce_workflow_size(path, metadata.len())?;
    let bytes = read_at_most(file, WORKFLOW_SOURCE_MAX_TOML_BYTES).map_err(|source| {
        WorkflowError::ReadWorkflow {
            path: path.to_path_buf(),
            source,
        }
    })?;
    enforce_workflow_size(path, bytes.len() as u64)?;
    Ok(bytes)
}

fn read_prompt_limited(
    canonical_path: &Path,
    display_path: &Path,
    source_path: &str,
    metadata_len: u64,
) -> WorkflowResult<Vec<u8>> {
    enforce_prompt_size(source_path, metadata_len)?;
    let file = File::open(canonical_path).map_err(|source| WorkflowError::ReadPrompt {
        path: display_path.to_path_buf(),
        source,
    })?;
    let bytes = read_at_most(file, WORKFLOW_SOURCE_MAX_PROMPT_BYTES).map_err(|source| {
        WorkflowError::ReadPrompt {
            path: display_path.to_path_buf(),
            source,
        }
    })?;
    enforce_prompt_size(source_path, bytes.len() as u64)?;
    Ok(bytes)
}

fn read_at_most(file: File, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    file.take((limit + 1) as u64).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn enforce_workflow_size(path: &Path, actual: u64) -> WorkflowResult<()> {
    if actual > WORKFLOW_SOURCE_MAX_TOML_BYTES as u64 {
        return Err(WorkflowError::WorkflowSourceTooLarge {
            path: path.to_path_buf(),
            limit: WORKFLOW_SOURCE_MAX_TOML_BYTES,
            actual,
        });
    }
    Ok(())
}

fn enforce_prompt_size(path: &str, actual: u64) -> WorkflowResult<()> {
    if actual > WORKFLOW_SOURCE_MAX_PROMPT_BYTES as u64 {
        return Err(WorkflowError::WorkflowPromptTooLarge {
            path: path.to_string(),
            limit: WORKFLOW_SOURCE_MAX_PROMPT_BYTES,
            actual,
        });
    }
    Ok(())
}

fn enforce_total_size(actual: usize) -> WorkflowResult<()> {
    if actual > WORKFLOW_SOURCE_MAX_TOTAL_BYTES {
        return Err(WorkflowError::WorkflowSourceBundleTooLarge {
            limit: WORKFLOW_SOURCE_MAX_TOTAL_BYTES,
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use vyane_core::Effort;

    fn workflow_with_prompt_files(paths: &[&str]) -> String {
        let mut source = String::from("[workflow]\nname = \"bundle-test\"\n");
        for (index, path) in paths.iter().enumerate() {
            use std::fmt::Write as _;
            write!(
                source,
                "\n[[step]]\nid = \"step-{index}\"\ntarget = \"test\"\nprompt_file = \"{path}\"\n"
            )
            .unwrap();
        }
        source
    }

    fn entry(path: &str, content: &str) -> WorkflowSourceEntry {
        WorkflowSourceEntry {
            path: path.parse().unwrap(),
            content: content.to_string(),
        }
    }

    #[test]
    fn source_path_rejects_noncanonical_and_cross_platform_escape_forms() {
        for invalid in [
            "",
            "../secret",
            "dir/../secret",
            "./prompt.txt",
            "dir/./prompt.txt",
            "dir//prompt.txt",
            "/absolute/prompt.txt",
            "C:/prompt.txt",
            "C:\\prompt.txt",
            "dir\\prompt.txt",
            "prompt.txt/",
            "nul\0prompt.txt",
        ] {
            assert!(
                invalid.parse::<WorkflowSourcePath>().is_err(),
                "accepted {invalid:?}"
            );
        }
        assert_eq!(
            "prompts/review.txt"
                .parse::<WorkflowSourcePath>()
                .unwrap()
                .as_str(),
            "prompts/review.txt"
        );
    }

    #[cfg(unix)]
    #[test]
    fn source_path_rejects_non_utf8_os_paths() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(OsString::from_vec(vec![b'p', 0xff, b't']));
        assert!(WorkflowSourcePath::try_from(path.as_path()).is_err());
    }

    #[test]
    fn bundle_round_trip_is_filesystem_independent_and_hash_stable() {
        let dir = TempDir::new().unwrap();
        let workflow_path = dir.path().join("workflow.toml");
        let prompts = dir.path().join("prompts");
        std::fs::create_dir(&prompts).unwrap();
        let source = workflow_with_prompt_files(&["prompts/z.txt", "prompts/a.txt"]);
        std::fs::write(&workflow_path, &source).unwrap();
        std::fs::write(prompts.join("z.txt"), "Z prompt").unwrap();
        std::fs::write(prompts.join("a.txt"), "A prompt").unwrap();

        let bundle = WorkflowSourceBundle::from_path(&workflow_path).unwrap();
        assert_eq!(bundle.prompt_files[0].path.as_str(), "prompts/a.txt");
        assert_eq!(bundle.prompt_files[1].path.as_str(), "prompts/z.txt");
        let local = Workflow::from_path(&workflow_path).unwrap();
        let from_bytes = Workflow::from_bytes(workflow_path.clone(), source.as_bytes()).unwrap();
        assert_eq!(from_bytes.file_sha256, local.file_sha256);
        let wire = serde_json::to_vec(&bundle).unwrap();
        let decoded: WorkflowSourceBundle = serde_json::from_slice(&wire).unwrap();
        let materialized = decoded.materialize().unwrap();
        assert_eq!(local.file_sha256, materialized.file_sha256);
        assert_eq!(
            materialized.steps[0].prompt_template.as_deref(),
            Some("Z prompt")
        );

        let mut reordered = decoded.clone();
        reordered.prompt_files.reverse();
        assert_eq!(
            reordered.materialize().unwrap().file_sha256,
            materialized.file_sha256
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
        let after_delete = decoded.materialize().unwrap();
        assert_eq!(after_delete.file_sha256, materialized.file_sha256);
        assert_eq!(
            after_delete.steps[1].prompt_template.as_deref(),
            Some("A prompt")
        );
    }

    #[test]
    fn route_effort_is_typed_and_frozen_by_bundle_and_source_hash() {
        let source = r#"
[workflow]
name = "effort-freeze"

[[step]]
id = "only"
target = "auto"
prompt = "run"
[step.route]
effort = "high"
"#;
        let bundle = WorkflowSourceBundle {
            workflow_toml: source.into(),
            prompt_files: Vec::new(),
        };
        let wire = serde_json::to_vec(&bundle).unwrap();
        let decoded: WorkflowSourceBundle = serde_json::from_slice(&wire).unwrap();
        let materialized = decoded.materialize().unwrap();

        assert_eq!(materialized.steps[0].route.effort, Some(Effort::High));
        let mut labels = BTreeMap::new();
        materialized.steps[0].route.apply_to_labels(&mut labels);
        assert_eq!(
            labels.get("routing.effort").map(String::as_str),
            Some("high")
        );

        let changed = WorkflowSourceBundle {
            workflow_toml: source.replace("effort = \"high\"", "effort = \"low\""),
            prompt_files: Vec::new(),
        }
        .materialize()
        .unwrap();
        assert_ne!(materialized.file_sha256, changed.file_sha256);
    }

    #[test]
    fn invalid_route_effort_does_not_echo_the_value() {
        let canary = "EFFORT_VALUE_MUST_NOT_BE_ECHOED";
        let bundle = WorkflowSourceBundle {
            workflow_toml: format!(
                r#"
[workflow]
name = "invalid-effort"

[[step]]
id = "only"
target = "auto"
prompt = "run"
[step.route]
effort = "{canary}"
"#
            ),
            prompt_files: Vec::new(),
        };

        let error = bundle.materialize().unwrap_err().to_string();

        assert!(!error.contains(canary));
        assert!(matches!(
            bundle.materialize().unwrap_err(),
            WorkflowError::ParseWorkflow { .. }
        ));
    }

    #[test]
    fn legacy_toml_frontend_ignores_unknown_fields() {
        let bundle = WorkflowSourceBundle {
            workflow_toml: r#"
legacy_root = true
[workflow]
name = "compatible"
legacy_workflow = "ignored"

[[step]]
id = "only"
target = "auto"
prompt = "run"
legacy_step = 7
[step.route]
effort = "high"
legacy_route = "ignored"
"#
            .into(),
            prompt_files: Vec::new(),
        };

        let workflow = bundle.materialize().unwrap();
        assert_eq!(workflow.name, "compatible");
        assert_eq!(workflow.steps[0].route.effort, Some(Effort::High));
    }

    #[test]
    fn bundle_rejects_missing_extra_and_duplicate_entries() {
        let source = workflow_with_prompt_files(&["prompt.txt", "prompt.txt"]);
        let valid = WorkflowSourceBundle {
            workflow_toml: source,
            prompt_files: vec![entry("prompt.txt", "prompt")],
        };
        valid.materialize().unwrap();

        let mut missing = valid.clone();
        missing.prompt_files.clear();
        assert!(matches!(
            missing.materialize().unwrap_err(),
            WorkflowError::MissingWorkflowPromptEntry { .. }
        ));

        let mut extra = valid.clone();
        extra.prompt_files.push(entry("extra.txt", "extra"));
        assert!(matches!(
            extra.materialize().unwrap_err(),
            WorkflowError::ExtraWorkflowPromptEntry { .. }
        ));

        let mut duplicate = valid;
        duplicate.prompt_files.push(entry("prompt.txt", "second"));
        assert!(matches!(
            duplicate.materialize().unwrap_err(),
            WorkflowError::DuplicateWorkflowPromptEntry { .. }
        ));
    }

    #[test]
    fn client_rejects_ambiguous_prompt_before_opening_declared_file() {
        let dir = TempDir::new().unwrap();
        let workflow_path = dir.path().join("workflow.toml");
        std::fs::write(
            &workflow_path,
            r#"
[[step]]
id = "ambiguous"
target = "test"
prompt = "inline"
prompt_file = "must-not-be-opened.txt"
"#,
        )
        .unwrap();

        let error = WorkflowSourceBundle::from_path(&workflow_path).unwrap_err();
        assert!(matches!(error, WorkflowError::Validation(_)));
        assert!(!error.to_string().contains("failed to read prompt file"));
    }

    #[test]
    fn normalized_source_hash_has_a_stable_golden_value() {
        let bundle = WorkflowSourceBundle {
            workflow_toml: workflow_with_prompt_files(&["prompt.txt"]),
            prompt_files: vec![entry("prompt.txt", "prompt")],
        };
        let workflow = bundle.materialize().unwrap();

        assert_eq!(
            workflow.file_sha256,
            "77b45aae80d6ee84d5a4c7d5d4dd319f1e2f49629162a34b641ef052872828dc"
        );
        assert_eq!(
            workflow.legacy_file_sha256.as_deref(),
            Some("7910b3f6563b19626abca9dd333544440fb928c7389ac0198de10e0ce70200de")
        );
    }

    #[test]
    fn malformed_paths_and_errors_never_echo_prompt_content() {
        let traversal = WorkflowSourceBundle {
            workflow_toml: workflow_with_prompt_files(&["../secret.txt"]),
            prompt_files: Vec::new(),
        };
        assert!(matches!(
            traversal.materialize().unwrap_err(),
            WorkflowError::InvalidWorkflowPromptPath { .. }
        ));

        let secret = "PROMPT_SECRET_MUST_NOT_BE_ECHOED";
        let malformed = WorkflowSourceBundle {
            workflow_toml: format!("prompt = \"{secret}\"\ninvalid = ["),
            prompt_files: Vec::new(),
        };
        let error = malformed.materialize().unwrap_err().to_string();
        assert!(!error.contains(secret));

        let serde_error = serde_json::from_value::<WorkflowSourceBundle>(serde_json::json!({
            "workflow_toml": "",
            "prompt_files": [{"path": "../secret", "content": secret}]
        }))
        .unwrap_err()
        .to_string();
        assert!(!serde_error.contains(secret));
    }

    #[cfg(unix)]
    #[test]
    fn client_collection_rejects_symlink_escape() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("workflow");
        std::fs::create_dir(&base).unwrap();
        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, "outside prompt").unwrap();
        std::os::unix::fs::symlink(&outside, base.join("prompt.txt")).unwrap();
        let workflow_path = base.join("workflow.toml");
        std::fs::write(&workflow_path, workflow_with_prompt_files(&["prompt.txt"])).unwrap();

        let error = WorkflowSourceBundle::from_path(&workflow_path).unwrap_err();
        assert!(matches!(
            error,
            WorkflowError::WorkflowPromptPathEscape { .. }
        ));
    }

    #[test]
    fn bundle_enforces_each_fixed_size_and_entry_limit() {
        let oversized_workflow = WorkflowSourceBundle {
            workflow_toml: "x".repeat(WORKFLOW_SOURCE_MAX_TOML_BYTES + 1),
            prompt_files: Vec::new(),
        };
        assert!(matches!(
            oversized_workflow.materialize().unwrap_err(),
            WorkflowError::WorkflowSourceTooLarge { .. }
        ));

        let oversized_prompt = WorkflowSourceBundle {
            workflow_toml: workflow_with_prompt_files(&["prompt.txt"]),
            prompt_files: vec![WorkflowSourceEntry {
                path: "prompt.txt".parse().unwrap(),
                content: "x".repeat(WORKFLOW_SOURCE_MAX_PROMPT_BYTES + 1),
            }],
        };
        assert!(matches!(
            oversized_prompt.materialize().unwrap_err(),
            WorkflowError::WorkflowPromptTooLarge { .. }
        ));

        let paths = ["a.txt", "b.txt", "c.txt", "d.txt"];
        let total_oversized = WorkflowSourceBundle {
            workflow_toml: workflow_with_prompt_files(&paths),
            prompt_files: paths
                .into_iter()
                .map(|path| WorkflowSourceEntry {
                    path: path.parse().unwrap(),
                    content: "x".repeat(WORKFLOW_SOURCE_MAX_PROMPT_BYTES),
                })
                .collect(),
        };
        assert!(matches!(
            total_oversized.materialize().unwrap_err(),
            WorkflowError::WorkflowSourceBundleTooLarge { .. }
        ));

        let too_many = WorkflowSourceBundle {
            workflow_toml: String::new(),
            prompt_files: (0..=WORKFLOW_SOURCE_MAX_ENTRIES)
                .map(|index| entry(&format!("prompt-{index}.txt"), ""))
                .collect(),
        };
        assert!(matches!(
            too_many.materialize().unwrap_err(),
            WorkflowError::WorkflowSourceTooManyEntries { .. }
        ));
    }

    #[test]
    fn client_collection_rejects_oversized_prompt_before_materialization() {
        let dir = TempDir::new().unwrap();
        let workflow_path = dir.path().join("workflow.toml");
        std::fs::write(&workflow_path, workflow_with_prompt_files(&["prompt.txt"])).unwrap();
        std::fs::write(
            dir.path().join("prompt.txt"),
            vec![b'x'; WORKFLOW_SOURCE_MAX_PROMPT_BYTES + 1],
        )
        .unwrap();

        assert!(matches!(
            WorkflowSourceBundle::from_path(&workflow_path).unwrap_err(),
            WorkflowError::WorkflowPromptTooLarge { .. }
        ));
    }

    #[test]
    fn client_collection_rejects_oversized_workflow_before_parsing() {
        let dir = TempDir::new().unwrap();
        let workflow_path = dir.path().join("workflow.toml");
        std::fs::write(
            &workflow_path,
            vec![b'x'; WORKFLOW_SOURCE_MAX_TOML_BYTES + 1],
        )
        .unwrap();

        assert!(matches!(
            WorkflowSourceBundle::from_path(&workflow_path).unwrap_err(),
            WorkflowError::WorkflowSourceTooLarge { .. }
        ));
    }
}
