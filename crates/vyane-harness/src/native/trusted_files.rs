//! Linux-first trusted native filesystem tools.
//!
//! Model-provided paths are resolved component-by-component beneath the exact
//! [`PinnedWorkdir`] held by [`ToolContext`]. Every descriptor open uses
//! `openat2` with beneath/no-mount/no-symlink resolution, and every open is
//! preceded by a live authority revalidation. Canonical path strings are never
//! reopened as execution authority.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Component, Path};
use std::sync::Arc;

use async_trait::async_trait;
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use vyane_core::{
    NativeExecutionAuthority, NativeSideEffect, PinnedWorkdir, Result as VyaneResult,
    ToolDefinition,
};

use super::{
    NativeTool, PermissionEffect, PermissionPolicy, PermissionRule, PermissionRuleError,
    ToolContext, ToolError, ToolRegistry, ToolRegistryError,
};

const MAX_READ_BYTES: usize = 1024 * 1024;
const MAX_SEARCH_FILES: usize = 10_000;
const MAX_SEARCH_ENTRIES: usize = 20_000;
const MAX_SEARCH_DEPTH: usize = 32;
const DEFAULT_SEARCH_RESULTS: usize = 100;
const MAX_SEARCH_RESULTS: usize = 200;
const MAX_QUERY_CHARS: usize = 1024;
const MAX_EXCLUDED_PATTERNS: usize = 128;
const MAX_EXCLUDED_PATTERN_BYTES: usize = 4096;
const MAX_EXCLUDED_TOTAL_BYTES: usize = 64 * 1024;

/// Configurable read boundary inside an already admitted workspace.
///
/// The default intentionally allows every confined workspace path, matching
/// Canto's read-only workspace authority. Projects may narrow that authority
/// with workspace-relative glob patterns. Exclusions never widen the pinned
/// workdir boundary and apply equally to direct reads and recursive search.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReadPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
}

impl NativeReadPolicy {
    pub fn workspace() -> Self {
        Self::default()
    }

    pub fn excluding(exclude: Vec<String>) -> Self {
        Self { exclude }
    }

    pub fn validate(&self) -> Result<(), NativeReadPolicyError> {
        CompiledReadPolicy::new(self).map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeReadPolicyError {
    #[error("native read policy contains too many excluded path patterns")]
    TooManyPatterns,
    #[error("native read policy contains an invalid excluded path pattern")]
    InvalidPattern,
}

/// Deterministic definitions advertised by the production read-only native lane.
pub fn read_only_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_file".into(),
            description: "Read one UTF-8 text file beneath the admitted workspace.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative file path"
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        },
        ToolDefinition {
            name: "search_files".into(),
            description:
                "Search UTF-8 workspace files for a literal string in deterministic path order."
                    .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Non-empty literal string"
                    },
                    "path": {
                        "type": "string",
                        "description": "Optional workspace-relative directory",
                        "default": "."
                    },
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_SEARCH_RESULTS,
                        "default": DEFAULT_SEARCH_RESULTS
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        },
    ]
}

/// Construct the exact executable registry matching
/// [`read_only_tool_definitions`].
pub fn read_only_tool_registry() -> Result<ToolRegistry, ToolRegistryError> {
    let mut registry = ToolRegistry::new();
    let policy = Arc::new(
        CompiledReadPolicy::new(&NativeReadPolicy::workspace())
            .map_err(|_| ToolRegistryError::UnsafeName)?,
    );
    registry.register(Arc::new(ReadFileTool {
        policy: Arc::clone(&policy),
    }))?;
    registry.register(Arc::new(SearchFilesTool { policy }))?;
    Ok(registry)
}

/// Construct the exact executable registry with a frozen workspace read
/// policy. Policy validation occurs before any model turn begins.
pub fn read_only_tool_registry_with_policy(
    policy: NativeReadPolicy,
) -> Result<ToolRegistry, NativeReadPolicyError> {
    let policy = Arc::new(CompiledReadPolicy::new(&policy)?);
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(ReadFileTool {
            policy: Arc::clone(&policy),
        }))
        .map_err(|_| NativeReadPolicyError::InvalidPattern)?;
    registry
        .register(Arc::new(SearchFilesTool { policy }))
        .map_err(|_| NativeReadPolicyError::InvalidPattern)?;
    Ok(registry)
}

/// Construct the deny-by-default policy for the exact read-only registry.
pub fn read_only_permission_policy() -> Result<PermissionPolicy, PermissionRuleError> {
    Ok(PermissionPolicy::deny_by_default()
        .with_rule(PermissionRule::new("read_file", PermissionEffect::Allow)?)
        .with_rule(PermissionRule::new(
            "search_files",
            PermissionEffect::Allow,
        )?))
}

struct CompiledReadPolicy {
    excluded: GlobSet,
}

impl CompiledReadPolicy {
    fn new(policy: &NativeReadPolicy) -> Result<Self, NativeReadPolicyError> {
        if policy.exclude.len() > MAX_EXCLUDED_PATTERNS {
            return Err(NativeReadPolicyError::TooManyPatterns);
        }
        let mut builder = GlobSetBuilder::new();
        let mut total_bytes = 0usize;
        for raw in &policy.exclude {
            total_bytes = total_bytes.saturating_add(raw.len());
            let pattern = raw.replace('\\', "/");
            if raw.is_empty()
                || raw.len() > MAX_EXCLUDED_PATTERN_BYTES
                || total_bytes > MAX_EXCLUDED_TOTAL_BYTES
                || raw.contains('\0')
                || pattern.starts_with('/')
                || Path::new(&pattern)
                    .components()
                    .any(|component| matches!(component, Component::ParentDir))
            {
                return Err(NativeReadPolicyError::InvalidPattern);
            }
            add_exclusion_glob(&mut builder, &pattern)?;
            if !pattern.ends_with("/**") {
                add_exclusion_glob(&mut builder, &format!("{pattern}/**"))?;
            }
        }
        let excluded = builder
            .build()
            .map_err(|_| NativeReadPolicyError::InvalidPattern)?;
        Ok(Self { excluded })
    }

    fn allows(&self, components: &[OsString]) -> bool {
        !self.excluded.is_match(display_components(components))
    }
}

fn add_exclusion_glob(
    builder: &mut GlobSetBuilder,
    pattern: &str,
) -> Result<(), NativeReadPolicyError> {
    let glob = GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
        .map_err(|_| NativeReadPolicyError::InvalidPattern)?;
    builder.add(glob);
    Ok(())
}

struct ReadFileTool {
    policy: Arc<CompiledReadPolicy>,
}

#[async_trait]
impl NativeTool for ReadFileTool {
    fn name(&self) -> &str {
        "read_file"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "trusted read_file requires live native authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        if let Err(error) = reject_unknown_arguments(arguments, &["path"]) {
            return Ok(Err(error));
        }
        let path = match required_string(arguments, "path") {
            Ok(path) => path,
            Err(error) => return Ok(Err(error)),
        };
        let Some(pinned) = context.pinned_workdir() else {
            return Ok(Err(ToolError::new(
                "trusted filesystem tools require a pinned Linux workdir",
            )));
        };
        let components = match checked_components(path, false) {
            Ok(components) if self.policy.allows(&components) => components,
            Ok(_) => {
                return Ok(Err(ToolError::new(
                    "workspace read policy denied this path",
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        let mut file = match open_regular_components(pinned, &components, authority, effect).await?
        {
            Ok(file) => file,
            Err(error) => return Ok(Err(error)),
        };
        Ok(read_utf8_bounded(&mut file))
    }
}

struct SearchFilesTool {
    policy: Arc<CompiledReadPolicy>,
}

#[async_trait]
impl NativeTool for SearchFilesTool {
    fn name(&self) -> &str {
        "search_files"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "trusted search_files requires live native authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        if let Err(error) = reject_unknown_arguments(arguments, &["query", "path", "max_results"]) {
            return Ok(Err(error));
        }
        let query = match required_string(arguments, "query") {
            Ok(query) if !query.is_empty() && query.chars().count() <= MAX_QUERY_CHARS => query,
            Ok(_) => {
                return Ok(Err(ToolError::new(
                    "query must contain between 1 and 1024 characters",
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        let path = match optional_string(arguments, "path", ".") {
            Ok(path) => path,
            Err(error) => return Ok(Err(error)),
        };
        let max_results = match optional_usize(
            arguments,
            "max_results",
            DEFAULT_SEARCH_RESULTS,
            MAX_SEARCH_RESULTS,
        ) {
            Ok(limit) => limit,
            Err(error) => return Ok(Err(error)),
        };
        let Some(pinned) = context.pinned_workdir() else {
            return Ok(Err(ToolError::new(
                "trusted filesystem tools require a pinned Linux workdir",
            )));
        };
        let root = match checked_components(path, true) {
            Ok(root) if self.policy.allows(&root) => root,
            Ok(_) => {
                return Ok(Err(ToolError::new(
                    "workspace read policy denied this path",
                )));
            }
            Err(error) => return Ok(Err(error)),
        };

        let mut pending = vec![(root, 0usize)];
        let mut visited_files = 0usize;
        let mut visited_entries = 0usize;
        let mut matches = Vec::new();
        while let Some((directory, depth)) = pending.pop() {
            if context.cancellation_token().is_cancelled() {
                return Ok(Err(ToolError::new("search cancelled")));
            }
            let dir = match open_directory_components(pinned, &directory, authority, effect).await?
            {
                Ok(dir) => dir,
                Err(error) => return Ok(Err(error)),
            };
            authority.revalidate(effect).await?;
            let remaining_entries = MAX_SEARCH_ENTRIES - visited_entries;
            let (mut entries, observed_entries) = match directory_entries(&dir, remaining_entries) {
                Ok(entries) => entries,
                Err(error) => return Ok(Err(error)),
            };
            visited_entries += observed_entries;
            entries.sort();

            let mut child_directories = Vec::new();
            for entry in entries {
                let relative = join_components(&directory, &entry);
                if !self.policy.allows(&relative) {
                    continue;
                }
                match open_entry(pinned, &relative, authority, effect).await? {
                    Ok(OpenedEntry::Directory) if depth < MAX_SEARCH_DEPTH => {
                        child_directories.push(relative);
                    }
                    Ok(OpenedEntry::Regular(mut file)) => {
                        visited_files += 1;
                        if visited_files > MAX_SEARCH_FILES {
                            return Ok(Err(ToolError::new(
                                "search exceeded the workspace file limit",
                            )));
                        }
                        let text = match read_utf8_bounded(&mut file) {
                            Ok(text) => text,
                            Err(_) => continue,
                        };
                        let display = display_components(&relative);
                        for (index, line) in text.lines().enumerate() {
                            if line.contains(query) {
                                matches.push(format!("{display}:{}:{line}", index + 1));
                                if matches.len() == max_results {
                                    return Ok(Ok(matches.join("\n")));
                                }
                            }
                        }
                    }
                    Ok(OpenedEntry::Other) | Err(_) => {}
                    Ok(OpenedEntry::Directory) => {}
                }
            }
            child_directories.reverse();
            pending.extend(
                child_directories
                    .into_iter()
                    .map(|child| (child, depth + 1)),
            );
        }

        Ok(Ok(if matches.is_empty() {
            "No matches found.".into()
        } else {
            matches.join("\n")
        }))
    }
}

fn reject_unknown_arguments(
    arguments: &BTreeMap<String, Value>,
    allowed: &[&str],
) -> Result<(), ToolError> {
    if arguments
        .keys()
        .any(|name| !allowed.contains(&name.as_str()))
    {
        return Err(ToolError::new("tool call contains an unknown argument"));
    }
    Ok(())
}

fn required_string<'a>(
    arguments: &'a BTreeMap<String, Value>,
    name: &str,
) -> Result<&'a str, ToolError> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.contains('\0'))
        .ok_or_else(|| ToolError::new(format!("argument `{name}` must be a string without NUL")))
}

fn optional_string<'a>(
    arguments: &'a BTreeMap<String, Value>,
    name: &str,
    default: &'a str,
) -> Result<&'a str, ToolError> {
    match arguments.get(name) {
        None => Ok(default),
        Some(value) => value
            .as_str()
            .filter(|value| !value.contains('\0'))
            .ok_or_else(|| {
                ToolError::new(format!("argument `{name}` must be a string without NUL"))
            }),
    }
}

fn optional_usize(
    arguments: &BTreeMap<String, Value>,
    name: &str,
    default: usize,
    maximum: usize,
) -> Result<usize, ToolError> {
    match arguments.get(name) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| (1..=maximum).contains(value))
            .ok_or_else(|| {
                ToolError::new(format!(
                    "argument `{name}` must be an integer between 1 and {maximum}"
                ))
            }),
    }
}

fn checked_components(path: &str, allow_root: bool) -> Result<Vec<OsString>, ToolError> {
    let path = Path::new(path);
    if path.is_absolute() {
        return Err(ToolError::new("absolute paths are not allowed"));
    }
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                components.push(value.to_os_string());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ToolError::new("path traversal is not allowed"));
            }
        }
    }
    if components.is_empty() && !allow_root {
        return Err(ToolError::new("file path must not be empty"));
    }
    Ok(components)
}

#[cfg(target_os = "linux")]
async fn open_regular_components(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    Ok(
        match open_entry(pinned, components, authority, effect).await? {
            Ok(OpenedEntry::Regular(file)) => Ok(file),
            Ok(OpenedEntry::Directory | OpenedEntry::Other) => {
                Err(ToolError::new("requested path is not a regular file"))
            }
            Err(error) => Err(error),
        },
    )
}

#[cfg(not(target_os = "linux"))]
async fn open_regular_components(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

// Non-Linux builds keep the closed return shape so the public tool registry can
// fail at runtime without conditional API types; only the Linux implementation
// constructs these variants.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
enum OpenedEntry {
    Directory,
    Regular(File),
    Other,
}

#[cfg(target_os = "linux")]
async fn open_entry(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<OpenedEntry, ToolError>> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::MetadataExt as _;

    use rustix::fs::{Mode, OFlags, openat2};

    let (name, parents) = match components.split_last() {
        Some(parts) => parts,
        None => return Ok(Err(ToolError::new("file path must not be empty"))),
    };
    let directory = match open_directory_components(pinned, parents, authority, effect).await? {
        Ok(directory) => directory,
        Err(error) => return Ok(Err(error)),
    };
    authority.revalidate(effect).await?;
    let fd = match openat2(
        &directory,
        name,
        OFlags::PATH | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
        confined_resolution(),
    ) {
        Ok(fd) => fd,
        Err(_) => {
            return Ok(Err(ToolError::new(
                "could not open requested workspace entry",
            )));
        }
    };
    let file = File::from(fd);
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            return Ok(Err(ToolError::new(
                "could not inspect requested workspace entry",
            )));
        }
    };
    if metadata.dev() != pinned.identity().device {
        return Ok(Err(ToolError::new(
            "requested workspace entry crosses a filesystem boundary",
        )));
    }
    Ok(Ok(if metadata.is_file() {
        // O_PATH cannot read file content. Revalidate at the actual read-open
        // boundary, then reopen this exact stable object through procfs rather
        // than resolving the model-provided pathname again.
        authority.revalidate(effect).await?;
        let proc_path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
        let readable = match File::open(proc_path) {
            Ok(readable) => readable,
            Err(_) => {
                return Ok(Err(ToolError::new(
                    "could not open requested workspace file for reading",
                )));
            }
        };
        OpenedEntry::Regular(readable)
    } else if metadata.is_dir() {
        OpenedEntry::Directory
    } else {
        OpenedEntry::Other
    }))
}

#[cfg(not(target_os = "linux"))]
async fn open_entry(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<OpenedEntry, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

#[cfg(target_os = "linux")]
async fn open_directory_components(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    use std::os::unix::fs::MetadataExt as _;

    use rustix::fs::{Mode, OFlags, openat2};

    let mut directory = match pinned.handle().try_clone() {
        Ok(directory) => directory,
        Err(_) => {
            return Ok(Err(ToolError::new(
                "could not duplicate pinned workspace handle",
            )));
        }
    };
    for component in components {
        authority.revalidate(effect).await?;
        let fd = match openat2(
            &directory,
            component,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
            confined_resolution(),
        ) {
            Ok(fd) => fd,
            Err(_) => {
                return Ok(Err(ToolError::new(
                    "could not open requested workspace directory",
                )));
            }
        };
        directory = File::from(fd);
        let metadata = match directory.metadata() {
            Ok(metadata) => metadata,
            Err(_) => {
                return Ok(Err(ToolError::new(
                    "could not inspect requested workspace directory",
                )));
            }
        };
        if metadata.dev() != pinned.identity().device {
            return Ok(Err(ToolError::new(
                "requested workspace directory crosses a filesystem boundary",
            )));
        }
    }
    Ok(Ok(directory))
}

#[cfg(target_os = "linux")]
fn confined_resolution() -> rustix::fs::ResolveFlags {
    use rustix::fs::ResolveFlags;

    ResolveFlags::BENEATH
        | ResolveFlags::NO_XDEV
        | ResolveFlags::NO_MAGICLINKS
        | ResolveFlags::NO_SYMLINKS
}

#[cfg(not(target_os = "linux"))]
async fn open_directory_components(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

/*
 * Directory enumeration deliberately remains a separate operation from
 * descriptor acquisition. The caller revalidates immediately before invoking
 * it, after the exact directory object has been opened.
 */
#[cfg(target_os = "linux")]
fn directory_entries(
    directory: &File,
    remaining: usize,
) -> Result<(Vec<OsString>, usize), ToolError> {
    use std::os::fd::AsRawFd as _;

    let path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    let entries = std::fs::read_dir(path)
        .map_err(|_| ToolError::new("could not enumerate requested workspace directory"))?;
    let mut names = Vec::new();
    let mut observed = 0usize;
    for entry in entries {
        if observed == remaining {
            return Err(ToolError::new("search exceeded the workspace entry limit"));
        }
        observed += 1;
        let Ok(entry) = entry else {
            continue;
        };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let name = entry.file_name();
        if !file_type.is_symlink() && name.to_str().is_some() {
            names.push(name);
        }
    }
    Ok((names, observed))
}

#[cfg(not(target_os = "linux"))]
fn directory_entries(
    _directory: &File,
    _remaining: usize,
) -> Result<(Vec<OsString>, usize), ToolError> {
    Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    ))
}

fn join_components(parent: &[OsString], child: &OsString) -> Vec<OsString> {
    let mut joined = parent.to_vec();
    joined.push(child.clone());
    joined
}

fn display_components(components: &[OsString]) -> String {
    components
        .iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn read_utf8_bounded(file: &mut File) -> Result<String, ToolError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ToolError::new("could not read requested workspace file"))?;
    let mut bytes = Vec::new();
    file.take((MAX_READ_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ToolError::new("could not read requested workspace file"))?;
    if bytes.len() > MAX_READ_BYTES {
        return Err(ToolError::new("workspace file exceeds the read limit"));
    }
    String::from_utf8(bytes).map_err(|_| ToolError::new("workspace file is not UTF-8 text"))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::directory_entries;

    #[test]
    fn directory_enumeration_enforces_the_raw_entry_budget() {
        let root = tempfile::tempdir().expect("root");
        std::fs::write(root.path().join("one"), "one").expect("one");
        std::fs::write(root.path().join("two"), "two").expect("two");
        let directory = std::fs::File::open(root.path()).expect("directory");

        let error = directory_entries(&directory, 1).expect_err("entry budget");
        assert_eq!(
            error.to_string(),
            "search exceeded the workspace entry limit"
        );
    }
}
