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
#[cfg(target_os = "linux")]
use std::io::Write as _;
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

#[cfg(target_os = "linux")]
use super::{EditError, EditRequest, compute_edit_bounded};
use super::{
    MAX_TOOL_OUTPUT_CHARS, NativeTool, PermissionEffect, PermissionPolicy, PermissionRule,
    PermissionRuleError, ToolContext, ToolError, ToolRegistry, ToolRegistryError,
};

const MAX_READ_BYTES: usize = 1024 * 1024;
const MAX_WRITE_BYTES: usize = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_EDIT_MATCHES: usize = 10_000;
const MAX_SEARCH_FILES: usize = 10_000;
const MAX_SEARCH_ENTRIES: usize = 20_000;
const MAX_SEARCH_DEPTH: usize = 32;
const MAX_PATH_COMPONENTS: usize = 32;
const DEFAULT_SEARCH_RESULTS: usize = 100;
const MAX_SEARCH_RESULTS: usize = 200;
const MAX_QUERY_CHARS: usize = 1024;
const MAX_EXCLUDED_PATTERNS: usize = 128;
const MAX_EXCLUDED_PATTERN_BYTES: usize = 4096;
const MAX_EXCLUDED_TOTAL_BYTES: usize = 64 * 1024;
const SEARCH_OUTPUT_LIMIT_MARKER: &str = "\n... [search output limit reached]";

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
        CompiledPathPolicy::new(self).map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeReadPolicyError {
    #[error("native read policy contains too many excluded path patterns")]
    TooManyPatterns,
    #[error("native read policy contains an invalid excluded path pattern")]
    InvalidPattern,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeReadHostError {
    #[error("native read tools require Linux openat2 confinement")]
    Unsupported,
}

/// Explicit, independently configurable write boundary inside the admitted
/// workspace. Merely granting read access never constructs this policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWritePolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
}

impl NativeWritePolicy {
    pub fn workspace() -> Self {
        Self::default()
    }

    pub fn excluding(exclude: Vec<String>) -> Self {
        Self { exclude }
    }

    pub fn validate(&self) -> Result<(), NativeWritePolicyError> {
        CompiledPathPolicy::from_exclusions(&self.exclude)
            .map(|_| ())
            .map_err(NativeWritePolicyError::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeWritePolicyError {
    #[error("native write policy contains too many excluded path patterns")]
    TooManyPatterns,
    #[error("native write policy contains an invalid excluded path pattern")]
    InvalidPattern,
}

impl From<NativeReadPolicyError> for NativeWritePolicyError {
    fn from(error: NativeReadPolicyError) -> Self {
        match error {
            NativeReadPolicyError::TooManyPatterns => Self::TooManyPatterns,
            NativeReadPolicyError::InvalidPattern => Self::InvalidPattern,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeFilesystemPolicyError {
    #[error("native filesystem read policy is invalid")]
    InvalidReadPolicy,
    #[error("native filesystem write policy is invalid")]
    InvalidWritePolicy,
    #[error("native filesystem tool registry could not be assembled")]
    Registry,
}

/// Prove that the admitted workdir supports the exact `openat2` confinement
/// used by the production read tools. Native submission calls this before any
/// model request so an unsupported kernel or seccomp profile fails closed.
#[cfg(target_os = "linux")]
pub fn validate_read_only_host(pinned: &PinnedWorkdir) -> Result<(), NativeReadHostError> {
    use std::os::unix::fs::MetadataExt as _;

    use rustix::fs::{Mode, OFlags, openat2};

    let fd = openat2(
        pinned.handle(),
        ".",
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
        confined_resolution(),
    )
    .map_err(|_| NativeReadHostError::Unsupported)?;
    let directory = File::from(fd);
    let metadata = directory
        .metadata()
        .map_err(|_| NativeReadHostError::Unsupported)?;
    if !metadata.is_dir() || metadata.dev() != pinned.identity().device {
        return Err(NativeReadHostError::Unsupported);
    }
    let proc_path = proc_fd_path(&directory);
    let reopened = File::open(&proc_path).map_err(|_| NativeReadHostError::Unsupported)?;
    let reopened_metadata = reopened
        .metadata()
        .map_err(|_| NativeReadHostError::Unsupported)?;
    if !reopened_metadata.is_dir() || reopened_metadata.dev() != pinned.identity().device {
        return Err(NativeReadHostError::Unsupported);
    }
    std::fs::read_dir(proc_path).map_err(|_| NativeReadHostError::Unsupported)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn validate_read_only_host(_pinned: &PinnedWorkdir) -> Result<(), NativeReadHostError> {
    Err(NativeReadHostError::Unsupported)
}

/// Deterministic definitions advertised by the production read-only native lane.
pub fn read_only_tool_definitions() -> Vec<ToolDefinition> {
    workspace_tool_definitions(false)
}

/// Deterministic definitions for the exact configured workspace capability.
pub fn workspace_tool_definitions(write_enabled: bool) -> Vec<ToolDefinition> {
    let mut definitions = vec![
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
    ];
    if write_enabled {
        definitions.extend([
            ToolDefinition {
                name: "write_file".into(),
                description: "Create one new UTF-8 text file beneath the admitted workspace."
                    .into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Workspace-relative new file path"
                        },
                        "content": {
                            "type": "string",
                            "description": "Complete UTF-8 file content"
                        }
                    },
                    "required": ["path", "content"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "edit_file".into(),
                description: "Apply one guarded text replacement to an existing workspace file."
                    .into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Workspace-relative existing file path"
                        },
                        "old_string": {
                            "type": "string",
                            "description": "Text to replace"
                        },
                        "new_string": {
                            "type": "string",
                            "description": "Replacement text"
                        },
                        "replace_all": {
                            "type": "boolean",
                            "default": false
                        }
                    },
                    "required": ["path", "old_string", "new_string"],
                    "additionalProperties": false
                }),
            },
        ]);
    }
    definitions
}

/// Construct the exact executable registry matching
/// [`read_only_tool_definitions`].
pub fn read_only_tool_registry() -> Result<ToolRegistry, ToolRegistryError> {
    let mut registry = ToolRegistry::new();
    let policy = Arc::new(
        CompiledPathPolicy::new(&NativeReadPolicy::workspace())
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
    let policy = Arc::new(CompiledPathPolicy::new(&policy)?);
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

/// Construct the exact executable workspace registry. Write tools exist only
/// when an explicit write policy is supplied.
pub fn workspace_tool_registry_with_policy(
    read_policy: NativeReadPolicy,
    write_policy: Option<NativeWritePolicy>,
) -> Result<ToolRegistry, NativeFilesystemPolicyError> {
    let read_policy = Arc::new(
        CompiledPathPolicy::new(&read_policy)
            .map_err(|_| NativeFilesystemPolicyError::InvalidReadPolicy)?,
    );
    let write_policy = write_policy
        .map(|policy| {
            CompiledPathPolicy::from_exclusions(&policy.exclude)
                .map(Arc::new)
                .map_err(|_| NativeFilesystemPolicyError::InvalidWritePolicy)
        })
        .transpose()?;
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(ReadFileTool {
            policy: Arc::clone(&read_policy),
        }))
        .map_err(|_| NativeFilesystemPolicyError::Registry)?;
    registry
        .register(Arc::new(SearchFilesTool {
            policy: Arc::clone(&read_policy),
        }))
        .map_err(|_| NativeFilesystemPolicyError::Registry)?;
    if let Some(write_policy) = write_policy {
        registry
            .register(Arc::new(WriteFileTool {
                policy: Arc::clone(&write_policy),
            }))
            .map_err(|_| NativeFilesystemPolicyError::Registry)?;
        registry
            .register(Arc::new(EditFileTool {
                read_policy,
                write_policy,
            }))
            .map_err(|_| NativeFilesystemPolicyError::Registry)?;
    }
    Ok(registry)
}

/// Construct the deny-by-default policy for the exact read-only registry.
pub fn read_only_permission_policy() -> Result<PermissionPolicy, PermissionRuleError> {
    workspace_permission_policy(false)
}

/// Construct the deny-by-default policy for the exact configured workspace
/// registry.
pub fn workspace_permission_policy(
    write_enabled: bool,
) -> Result<PermissionPolicy, PermissionRuleError> {
    let mut policy = PermissionPolicy::deny_by_default()
        .with_rule(PermissionRule::new("read_file", PermissionEffect::Allow)?)
        .with_rule(PermissionRule::new(
            "search_files",
            PermissionEffect::Allow,
        )?);
    if write_enabled {
        policy = policy
            .with_rule(PermissionRule::new("write_file", PermissionEffect::Allow)?)
            .with_rule(PermissionRule::new("edit_file", PermissionEffect::Allow)?);
    }
    Ok(policy)
}

struct CompiledPathPolicy {
    excluded: GlobSet,
}

impl CompiledPathPolicy {
    fn new(policy: &NativeReadPolicy) -> Result<Self, NativeReadPolicyError> {
        Self::from_exclusions(&policy.exclude)
    }

    fn from_exclusions(exclude: &[String]) -> Result<Self, NativeReadPolicyError> {
        if exclude.len() > MAX_EXCLUDED_PATTERNS {
            return Err(NativeReadPolicyError::TooManyPatterns);
        }
        let mut builder = GlobSetBuilder::new();
        let mut total_bytes = 0usize;
        for raw in exclude {
            total_bytes = total_bytes.saturating_add(raw.len());
            let pattern = raw.replace('\\', "/");
            if raw.is_empty()
                || raw.len() > MAX_EXCLUDED_PATTERN_BYTES
                || total_bytes > MAX_EXCLUDED_TOTAL_BYTES
                || raw.contains('\0')
                || raw.contains('\\')
                || pattern.starts_with('/')
                || pattern.ends_with('/')
                || pattern.contains("//")
                || pattern
                    .split('/')
                    .any(|component| component == "." || component == "..")
                || Path::new(&pattern)
                    .components()
                    .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
            {
                return Err(NativeReadPolicyError::InvalidPattern);
            }
            add_exclusion_glob(&mut builder, &pattern)?;
            if let Some(directory) = pattern.strip_suffix("/**") {
                if !directory.is_empty() {
                    add_exclusion_glob(&mut builder, directory)?;
                }
            } else {
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
    policy: Arc<CompiledPathPolicy>,
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
        let file =
            match open_regular_components(pinned, &components, context, authority, effect).await? {
                Ok(file) => file,
                Err(error) => return Ok(Err(error)),
            };
        Ok(read_utf8_bounded(file, context).await)
    }
}

struct SearchFilesTool {
    policy: Arc<CompiledPathPolicy>,
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
        let mut regular_paths = Vec::new();
        let mut visited_files = 0usize;
        let mut visited_entries = 0usize;
        while let Some((directory, depth)) = pending.pop() {
            if context.cancellation_token().is_cancelled() {
                return Ok(Err(ToolError::new("search cancelled")));
            }
            let dir =
                match open_directory_components(pinned, &directory, context, authority, effect)
                    .await?
                {
                    Ok(dir) => dir,
                    Err(error) => return Ok(Err(error)),
                };
            authority.revalidate(effect).await?;
            let remaining_entries = MAX_SEARCH_ENTRIES - visited_entries;
            let (mut entries, observed_entries) =
                match directory_entries(dir, remaining_entries, context).await {
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
                if relative.len() > MAX_PATH_COMPONENTS {
                    continue;
                }
                match classify_entry(pinned, &relative, context, authority, effect).await? {
                    Ok(EntryKind::Directory) if depth < MAX_SEARCH_DEPTH => {
                        child_directories.push(relative);
                    }
                    Ok(EntryKind::Regular) => {
                        visited_files += 1;
                        if visited_files > MAX_SEARCH_FILES {
                            return Ok(Err(ToolError::new(
                                "search exceeded the workspace file limit",
                            )));
                        }
                        regular_paths.push(relative);
                    }
                    Ok(EntryKind::Other) | Err(_) => {}
                    Ok(EntryKind::Directory) => {}
                }
            }
            child_directories.reverse();
            pending.extend(
                child_directories
                    .into_iter()
                    .map(|child| (child, depth + 1)),
            );
        }

        regular_paths.sort();
        let mut output = String::new();
        let mut match_count = 0usize;
        for relative in regular_paths {
            if context.cancellation_token().is_cancelled() {
                return Ok(Err(ToolError::new("search cancelled")));
            }
            let file = match open_regular_components(pinned, &relative, context, authority, effect)
                .await?
            {
                Ok(file) => file,
                Err(_) => continue,
            };
            let text = match read_utf8_bounded(file, context).await {
                Ok(text) => text,
                Err(_) => continue,
            };
            let display = display_components(&relative);
            for (index, line) in text.lines().enumerate() {
                if line.contains(query) {
                    match_count += 1;
                    if !append_search_match(&mut output, &display, index + 1, line)
                        || match_count == max_results
                    {
                        return Ok(Ok(output));
                    }
                }
            }
        }

        Ok(Ok(if output.is_empty() {
            "No matches found.".into()
        } else {
            output
        }))
    }
}

struct WriteFileTool {
    policy: Arc<CompiledPathPolicy>,
}

#[async_trait]
impl NativeTool for WriteFileTool {
    fn name(&self) -> &str {
        "write_file"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "trusted write_file requires live native authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        if let Err(error) = reject_unknown_arguments(arguments, &["path", "content"]) {
            return Ok(Err(error));
        }
        let path = match required_string(arguments, "path") {
            Ok(path) => path,
            Err(error) => return Ok(Err(error)),
        };
        let content = match required_string(arguments, "content") {
            Ok(content) if content.len() <= MAX_WRITE_BYTES => content,
            Ok(_) => return Ok(Err(ToolError::new("file content exceeds the write limit"))),
            Err(error) => return Ok(Err(error)),
        };
        let components = match checked_components(path, false) {
            Ok(components) if self.policy.allows(&components) => components,
            Ok(_) => {
                return Ok(Err(ToolError::new(
                    "workspace write policy denied this path",
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        let Some(pinned) = context.pinned_workdir() else {
            return Ok(Err(ToolError::new(
                "trusted filesystem tools require a pinned Linux workdir",
            )));
        };
        write_new_file(
            pinned,
            &components,
            content.as_bytes(),
            context,
            authority,
            effect,
        )
        .await
    }
}

struct EditFileTool {
    read_policy: Arc<CompiledPathPolicy>,
    write_policy: Arc<CompiledPathPolicy>,
}

#[async_trait]
impl NativeTool for EditFileTool {
    fn name(&self) -> &str {
        "edit_file"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "trusted edit_file requires live native authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        if let Err(error) = reject_unknown_arguments(
            arguments,
            &["path", "old_string", "new_string", "replace_all"],
        ) {
            return Ok(Err(error));
        }
        let path = match required_string(arguments, "path") {
            Ok(path) => path,
            Err(error) => return Ok(Err(error)),
        };
        let old_string = match required_string(arguments, "old_string") {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let new_string = match required_string(arguments, "new_string") {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let replace_all = match optional_bool(arguments, "replace_all", false) {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let components = match checked_components(path, false) {
            Ok(components)
                if self.read_policy.allows(&components)
                    && self.write_policy.allows(&components) =>
            {
                components
            }
            Ok(_) => {
                return Ok(Err(ToolError::new(
                    "workspace read or write policy denied this path",
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        let Some(pinned) = context.pinned_workdir() else {
            return Ok(Err(ToolError::new(
                "trusted filesystem tools require a pinned Linux workdir",
            )));
        };
        edit_existing_file(
            pinned,
            &components,
            old_string,
            new_string,
            replace_all,
            context,
            authority,
            effect,
        )
        .await
    }
}

fn append_search_match(output: &mut String, path: &str, line_number: usize, line: &str) -> bool {
    let separator = if output.is_empty() { "" } else { "\n" };
    let prefix = format!("{separator}{path}:{line_number}:");
    let current_chars = output.chars().count();
    let candidate_chars = prefix.chars().count().saturating_add(line.chars().count());
    if current_chars.saturating_add(candidate_chars) <= MAX_TOOL_OUTPUT_CHARS {
        output.push_str(&prefix);
        output.push_str(line);
        return true;
    }

    let content_limit =
        MAX_TOOL_OUTPUT_CHARS.saturating_sub(SEARCH_OUTPUT_LIMIT_MARKER.chars().count());
    truncate_to_chars(output, content_limit);
    let remaining = content_limit.saturating_sub(output.chars().count());
    append_up_to_chars(output, &prefix, remaining);
    let remaining = content_limit.saturating_sub(output.chars().count());
    append_up_to_chars(output, line, remaining);
    output.push_str(SEARCH_OUTPUT_LIMIT_MARKER);
    false
}

fn truncate_to_chars(value: &mut String, limit: usize) {
    if let Some((byte_index, _)) = value.char_indices().nth(limit) {
        value.truncate(byte_index);
    }
}

fn append_up_to_chars(output: &mut String, value: &str, limit: usize) {
    if value.chars().count() <= limit {
        output.push_str(value);
    } else {
        output.extend(value.chars().take(limit));
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

fn optional_bool(
    arguments: &BTreeMap<String, Value>,
    name: &str,
    default: bool,
) -> Result<bool, ToolError> {
    match arguments.get(name) {
        None => Ok(default),
        Some(value) => value
            .as_bool()
            .ok_or_else(|| ToolError::new(format!("argument `{name}` must be a boolean"))),
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
    if components.len() > MAX_PATH_COMPONENTS {
        return Err(ToolError::new("path exceeds the component limit"));
    }
    Ok(components)
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileSnapshot {
    device: u64,
    inode: u64,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[cfg(target_os = "linux")]
impl FileSnapshot {
    fn from_file(file: &File) -> Result<Self, ToolError> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = file
            .metadata()
            .map_err(|_| ToolError::new("could not inspect workspace file for editing"))?;
        if !metadata.is_file() {
            return Err(ToolError::new("requested edit path is not a regular file"));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.size(),
            mode: metadata.mode() & 0o7777,
            uid: metadata.uid(),
            gid: metadata.gid(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }

    fn preservation(self) -> FilePreservation {
        FilePreservation {
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
        }
    }
}

#[cfg(target_os = "linux")]
fn ensure_no_extended_attributes(file: &File) -> Result<(), ToolError> {
    let mut probe = [0u8; 1];
    match rustix::fs::flistxattr(file, &mut probe) {
        Ok(0) => Ok(()),
        Ok(_) | Err(rustix::io::Errno::RANGE) => Err(ToolError::new(
            "workspace file security metadata cannot be preserved",
        )),
        Err(_) => Err(ToolError::new(
            "could not inspect workspace file security metadata",
        )),
    }
}

#[cfg(target_os = "linux")]
async fn ensure_no_extended_attributes_async(
    file: &File,
    context: &ToolContext,
) -> Result<(), ToolError> {
    let file = file
        .try_clone()
        .map_err(|_| ToolError::new("could not duplicate workspace file"))?;
    tracked_blocking(context, move || ensure_no_extended_attributes(&file))
        .await
        .map_err(|_| ToolError::new("could not inspect workspace file security metadata"))?
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FilePreservation {
    mode: u32,
    uid: u32,
    gid: u32,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectorySnapshot {
    device: u64,
    inode: u64,
}

#[cfg(target_os = "linux")]
impl DirectorySnapshot {
    fn from_file(file: &File) -> Result<Self, ToolError> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = file
            .metadata()
            .map_err(|_| ToolError::new("could not inspect workspace directory"))?;
        if !metadata.is_dir() {
            return Err(ToolError::new("workspace parent is not a directory"));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[cfg(target_os = "linux")]
struct StagedFile {
    parent: File,
    file: File,
    name: OsString,
    require_no_xattrs: bool,
    published: bool,
}

#[cfg(target_os = "linux")]
impl StagedFile {
    fn publish_new(mut self, target: &OsString) -> Result<(), ToolError> {
        use rustix::fs::{RenameFlags, renameat_with};

        self.validate_named_inode()?;
        match renameat_with(
            &self.parent,
            &self.name,
            &self.parent,
            target,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {
                self.published = true;
                Ok(())
            }
            Err(rustix::io::Errno::EXIST) => Err(ToolError::new("workspace file already exists")),
            Err(_) => Err(ToolError::new("could not publish workspace file")),
        }
    }

    fn publish_replacement(mut self, target: &OsString) -> Result<(), ToolError> {
        self.validate_named_inode()?;
        rustix::fs::renameat(&self.parent, &self.name, &self.parent, target)
            .map_err(|_| ToolError::new("could not publish workspace edit"))?;
        self.published = true;
        Ok(())
    }

    fn validate_named_inode(&self) -> Result<(), ToolError> {
        use std::os::unix::fs::MetadataExt as _;

        use rustix::fs::{Mode, OFlags, openat2};

        let descriptor = openat2(
            &self.parent,
            &self.name,
            OFlags::PATH | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
            confined_resolution(),
        )
        .map_err(|_| ToolError::new("staged workspace file changed before publication"))?;
        let named = File::from(descriptor);
        let held_metadata = self
            .file
            .metadata()
            .map_err(|_| ToolError::new("could not inspect staged workspace file"))?;
        let named_metadata = named
            .metadata()
            .map_err(|_| ToolError::new("could not inspect staged workspace file"))?;
        if !held_metadata.is_file()
            || !named_metadata.is_file()
            || held_metadata.dev() != named_metadata.dev()
            || held_metadata.ino() != named_metadata.ino()
        {
            return Err(ToolError::new(
                "staged workspace file changed before publication",
            ));
        }
        if self.require_no_xattrs {
            ensure_no_extended_attributes(&self.file)?;
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = rustix::fs::unlinkat(&self.parent, &self.name, rustix::fs::AtFlags::empty());
        }
    }
}

#[cfg(target_os = "linux")]
async fn write_new_file(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    content: &[u8],
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<String, ToolError>> {
    let Some((name, parents)) = components.split_last() else {
        return Ok(Err(ToolError::new("file path must not be empty")));
    };
    let parent =
        match open_directory_components(pinned, parents, context, authority, effect).await? {
            Ok(parent) => parent,
            Err(error) => return Ok(Err(error)),
        };
    let parent_snapshot = match directory_snapshot(&parent, context).await {
        Ok(snapshot) => snapshot,
        Err(error) => return Ok(Err(error)),
    };
    authority.revalidate(effect).await?;
    let staged = match stage_content(parent, name, content.to_vec(), None, context).await {
        Ok(staged) => staged,
        Err(error) => return Ok(Err(error)),
    };
    if context.cancellation_token().is_cancelled() {
        return Ok(Err(ToolError::new("file write cancelled")));
    }
    match reopen_unchanged_parent(pinned, parents, parent_snapshot, context, authority, effect)
        .await?
    {
        Ok(_) => {}
        Err(error) => return Ok(Err(error)),
    }
    authority.revalidate(effect).await?;
    if context.cancellation_token().is_cancelled() {
        return Ok(Err(ToolError::new("file write cancelled")));
    }
    let byte_count = content.len();
    let display = display_components(components);
    Ok(staged
        .publish_new(name)
        .map(|()| format!("Created {display} ({byte_count} bytes).")))
}

#[cfg(not(target_os = "linux"))]
async fn write_new_file(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _content: &[u8],
    _context: &ToolContext,
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<String, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
async fn edit_existing_file(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    old_string: &str,
    new_string: &str,
    replace_all: bool,
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<String, ToolError>> {
    let Some((name, parents)) = components.split_last() else {
        return Ok(Err(ToolError::new("file path must not be empty")));
    };
    let parent =
        match open_directory_components(pinned, parents, context, authority, effect).await? {
            Ok(parent) => parent,
            Err(error) => return Ok(Err(error)),
        };
    let parent_snapshot = match directory_snapshot(&parent, context).await {
        Ok(snapshot) => snapshot,
        Err(error) => return Ok(Err(error)),
    };
    let (original, original_snapshot) =
        match read_regular_from_parent(&parent, name, context, authority, effect).await? {
            Ok(source) => source,
            Err(error) => return Ok(Err(error)),
        };
    let edit_content = original.clone();
    let old_string = old_string.to_owned();
    let new_string = new_string.to_owned();
    let outcome = match tracked_blocking(context, move || {
        compute_edit_bounded(
            &EditRequest {
                content: &edit_content,
                old_string: &old_string,
                new_string: &new_string,
                replace_all,
            },
            MAX_WRITE_BYTES,
            MAX_EDIT_MATCHES,
        )
    })
    .await
    {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(EditError::OutputTooLarge { .. })) => {
            return Ok(Err(ToolError::new("edited file exceeds the write limit")));
        }
        Ok(Err(EditError::TooManyMatches { .. })) => {
            return Ok(Err(ToolError::new(
                "edit exceeds the workspace match limit",
            )));
        }
        Ok(Err(error)) => return Ok(Err(ToolError::new(error.to_string()))),
        Err(_) => return Ok(Err(ToolError::new("could not compute workspace edit"))),
    };

    authority.revalidate(effect).await?;
    let staging_parent = match parent.try_clone() {
        Ok(parent) => parent,
        Err(_) => {
            return Ok(Err(ToolError::new(
                "could not duplicate workspace directory",
            )));
        }
    };
    let staged = match stage_content(
        staging_parent,
        name,
        outcome.new_content.as_bytes().to_vec(),
        Some(original_snapshot.preservation()),
        context,
    )
    .await
    {
        Ok(staged) => staged,
        Err(error) => return Ok(Err(error)),
    };
    if context.cancellation_token().is_cancelled() {
        return Ok(Err(ToolError::new("file edit cancelled")));
    }

    let current_parent =
        match reopen_unchanged_parent(pinned, parents, parent_snapshot, context, authority, effect)
            .await?
        {
            Ok(parent) => parent,
            Err(error) => return Ok(Err(error)),
        };
    let current =
        match read_regular_from_parent(&current_parent, name, context, authority, effect).await? {
            Ok(current) => current,
            Err(_) => {
                return Ok(Err(ToolError::new(
                    "workspace file changed before edit publication",
                )));
            }
        };
    if current.1 != original_snapshot || current.0 != original {
        return Ok(Err(ToolError::new(
            "workspace file changed before edit publication",
        )));
    }
    authority.revalidate(effect).await?;
    if context.cancellation_token().is_cancelled() {
        return Ok(Err(ToolError::new("file edit cancelled")));
    }
    let display = display_components(components);
    let replacements = outcome.replacements;
    Ok(staged
        .publish_replacement(name)
        .map(|()| format!("Updated {display} ({replacements} replacements).")))
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::too_many_arguments)]
async fn edit_existing_file(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _old_string: &str,
    _new_string: &str,
    _replace_all: bool,
    _context: &ToolContext,
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<String, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

#[cfg(target_os = "linux")]
async fn read_regular_from_parent(
    parent: &File,
    name: &OsString,
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<(String, FileSnapshot), ToolError>> {
    use std::os::unix::fs::MetadataExt as _;

    authority.revalidate(effect).await?;
    let expected_device = match parent.metadata() {
        Ok(metadata) => metadata.dev(),
        Err(_) => return Ok(Err(ToolError::new("could not inspect workspace directory"))),
    };
    let inspected_parent = match parent.try_clone() {
        Ok(parent) => parent,
        Err(_) => {
            return Ok(Err(ToolError::new(
                "could not duplicate workspace directory",
            )));
        }
    };
    let inspected_name = name.clone();
    let source = match tracked_blocking(context, move || {
        inspect_entry(inspected_parent, &inspected_name, expected_device)
    })
    .await
    {
        Ok(Ok(InspectedEntry::Regular(source))) => source,
        Ok(Ok(_)) => {
            return Ok(Err(ToolError::new(
                "requested edit path is not a regular file",
            )));
        }
        Ok(Err(error)) => return Ok(Err(error)),
        Err(_) => return Ok(Err(ToolError::new("could not inspect workspace file"))),
    };
    let before = match FileSnapshot::from_file(&source) {
        Ok(snapshot) => snapshot,
        Err(error) => return Ok(Err(error)),
    };
    authority.revalidate(effect).await?;
    let read_source = match source.try_clone() {
        Ok(source) => source,
        Err(_) => {
            return Ok(Err(ToolError::new("could not duplicate workspace file")));
        }
    };
    let readable = match tracked_blocking(context, move || reopen_for_read(&read_source)).await {
        Ok(Ok(readable)) => readable,
        Ok(Err(())) | Err(_) => {
            return Ok(Err(ToolError::new(
                "could not open workspace file for editing",
            )));
        }
    };
    if let Err(error) = ensure_no_extended_attributes_async(&readable, context).await {
        return Ok(Err(error));
    }
    let content = match read_utf8_bounded(readable, context).await {
        Ok(content) => content,
        Err(error) => return Ok(Err(error)),
    };
    let after = match FileSnapshot::from_file(&source) {
        Ok(snapshot) => snapshot,
        Err(error) => return Ok(Err(error)),
    };
    if before != after {
        return Ok(Err(ToolError::new(
            "workspace file changed while it was being read",
        )));
    }
    Ok(Ok((content, before)))
}

#[cfg(target_os = "linux")]
async fn stage_content(
    parent: File,
    target: &OsString,
    content: Vec<u8>,
    preservation: Option<FilePreservation>,
    context: &ToolContext,
) -> Result<StagedFile, ToolError> {
    let target = target.clone();
    tracked_blocking(context, move || {
        stage_content_blocking(parent, &target, &content, preservation)
    })
    .await
    .map_err(|_| ToolError::new("could not stage workspace file"))?
}

#[cfg(target_os = "linux")]
fn stage_content_blocking(
    parent: File,
    target: &OsString,
    content: &[u8],
    preservation: Option<FilePreservation>,
) -> Result<StagedFile, ToolError> {
    use rustix::fs::{Mode, OFlags, openat2};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    for _ in 0..16 {
        let name = OsString::from(format!(".vyane-write-{}.tmp", uuid::Uuid::now_v7()));
        if &name == target {
            continue;
        }
        let descriptor = match openat2(
            &parent,
            &name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::from_bits_retain(if preservation.is_some() { 0o600 } else { 0o666 }),
            confined_resolution(),
        ) {
            Ok(descriptor) => descriptor,
            Err(rustix::io::Errno::EXIST) => continue,
            Err(_) => return Err(ToolError::new("could not stage workspace file")),
        };
        let file = File::from(descriptor);
        let mut staged = StagedFile {
            parent,
            file,
            name,
            require_no_xattrs: preservation.is_some(),
            published: false,
        };
        staged
            .file
            .write_all(content)
            .map_err(|_| ToolError::new("could not stage workspace file"))?;
        if let Some(preservation) = preservation {
            ensure_no_extended_attributes(&staged.file)?;
            let metadata = staged
                .file
                .metadata()
                .map_err(|_| ToolError::new("could not inspect staged workspace file"))?;
            if metadata.uid() != preservation.uid || metadata.gid() != preservation.gid {
                return Err(ToolError::new(
                    "could not preserve workspace file ownership",
                ));
            }
            staged
                .file
                .set_permissions(std::fs::Permissions::from_mode(preservation.mode))
                .map_err(|_| ToolError::new("could not stage workspace file"))?;
        }
        staged
            .file
            .sync_all()
            .map_err(|_| ToolError::new("could not stage workspace file"))?;
        return Ok(staged);
    }
    Err(ToolError::new(
        "could not allocate a temporary workspace file",
    ))
}

#[cfg(target_os = "linux")]
async fn reopen_unchanged_parent(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    expected: DirectorySnapshot,
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    let parent =
        match open_directory_components(pinned, components, context, authority, effect).await? {
            Ok(parent) => parent,
            Err(_) => {
                return Ok(Err(ToolError::new(
                    "workspace parent changed before publication",
                )));
            }
        };
    match directory_snapshot(&parent, context).await {
        Ok(observed) if observed == expected => Ok(Ok(parent)),
        Ok(_) | Err(_) => Ok(Err(ToolError::new(
            "workspace parent changed before publication",
        ))),
    }
}

#[cfg(target_os = "linux")]
async fn directory_snapshot(
    directory: &File,
    context: &ToolContext,
) -> Result<DirectorySnapshot, ToolError> {
    let directory = directory
        .try_clone()
        .map_err(|_| ToolError::new("could not duplicate workspace directory"))?;
    tracked_blocking(context, move || DirectorySnapshot::from_file(&directory))
        .await
        .map_err(|_| ToolError::new("could not inspect workspace directory"))?
}

#[cfg(target_os = "linux")]
async fn open_regular_components(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    let inspected = match inspect_components(pinned, components, context, authority, effect).await?
    {
        Ok(inspected) => inspected,
        Err(error) => return Ok(Err(error)),
    };
    let InspectedEntry::Regular(file) = inspected else {
        return Ok(Err(ToolError::new("requested path is not a regular file")));
    };
    authority.revalidate(effect).await?;
    Ok(
        match tracked_blocking(context, move || reopen_for_read(&file)).await {
            Ok(Ok(readable)) => Ok(readable),
            Ok(Err(())) | Err(_) => Err(ToolError::new(
                "could not open requested workspace file for reading",
            )),
        },
    )
}

#[cfg(not(target_os = "linux"))]
async fn open_regular_components(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _context: &ToolContext,
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Copy)]
enum EntryKind {
    Directory,
    Regular,
    Other,
}

#[cfg(target_os = "linux")]
async fn classify_entry(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<EntryKind, ToolError>> {
    Ok(
        match inspect_components(pinned, components, context, authority, effect).await? {
            Ok(InspectedEntry::Directory) => Ok(EntryKind::Directory),
            Ok(InspectedEntry::Regular(_)) => Ok(EntryKind::Regular),
            Ok(InspectedEntry::Other) => Ok(EntryKind::Other),
            Err(error) => Err(error),
        },
    )
}

#[cfg(target_os = "linux")]
async fn inspect_components(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<InspectedEntry, ToolError>> {
    let (name, parents) = match components.split_last() {
        Some(parts) => parts,
        None => return Ok(Err(ToolError::new("file path must not be empty"))),
    };
    let directory =
        match open_directory_components(pinned, parents, context, authority, effect).await? {
            Ok(directory) => directory,
            Err(error) => return Ok(Err(error)),
        };
    authority.revalidate(effect).await?;
    let name = name.clone();
    let expected_device = pinned.identity().device;
    let inspected = match tracked_blocking(context, move || {
        inspect_entry(directory, &name, expected_device)
    })
    .await
    {
        Ok(Ok(inspected)) => inspected,
        Ok(Err(error)) => return Ok(Err(error)),
        Err(_) => {
            return Ok(Err(ToolError::new(
                "could not inspect requested workspace entry",
            )));
        }
    };
    Ok(Ok(inspected))
}

#[cfg(target_os = "linux")]
enum InspectedEntry {
    Directory,
    Regular(File),
    Other,
}

#[cfg(target_os = "linux")]
fn inspect_entry(
    directory: File,
    name: &OsString,
    expected_device: u64,
) -> Result<InspectedEntry, ToolError> {
    use std::os::unix::fs::MetadataExt as _;

    use rustix::fs::{Mode, OFlags, openat2};

    let fd = openat2(
        &directory,
        name,
        OFlags::PATH | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
        confined_resolution(),
    )
    .map_err(|_| ToolError::new("could not open requested workspace entry"))?;
    let file = File::from(fd);
    let metadata = file
        .metadata()
        .map_err(|_| ToolError::new("could not inspect requested workspace entry"))?;
    if metadata.dev() != expected_device {
        return Err(ToolError::new(
            "requested workspace entry crosses a filesystem boundary",
        ));
    }
    Ok(if metadata.is_file() {
        InspectedEntry::Regular(file)
    } else if metadata.is_dir() {
        InspectedEntry::Directory
    } else {
        InspectedEntry::Other
    })
}

#[cfg(target_os = "linux")]
fn reopen_for_read(file: &File) -> Result<File, ()> {
    File::open(proc_fd_path(file)).map_err(|_| ())
}

#[cfg(target_os = "linux")]
fn proc_fd_path(file: &File) -> PathBuf {
    use std::os::fd::AsRawFd as _;

    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

#[cfg(not(target_os = "linux"))]
async fn classify_entry(
    _pinned: &PinnedWorkdir,
    _components: &[OsString],
    _context: &ToolContext,
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
) -> VyaneResult<Result<EntryKind, ToolError>> {
    Ok(Err(ToolError::new(
        "trusted filesystem tools are supported only on Linux",
    )))
}

#[cfg(target_os = "linux")]
async fn open_directory_components(
    pinned: &PinnedWorkdir,
    components: &[OsString],
    context: &ToolContext,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<Result<File, ToolError>> {
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
        let component = component.clone();
        let expected_device = pinned.identity().device;
        directory = match tracked_blocking(context, move || {
            open_directory_component(directory, &component, expected_device)
        })
        .await
        {
            Ok(Ok(directory)) => directory,
            Ok(Err(error)) => return Ok(Err(error)),
            Err(_) => {
                return Ok(Err(ToolError::new(
                    "could not open requested workspace directory",
                )));
            }
        };
    }
    Ok(Ok(directory))
}

#[cfg(target_os = "linux")]
fn open_directory_component(
    directory: File,
    component: &OsString,
    expected_device: u64,
) -> Result<File, ToolError> {
    use std::os::unix::fs::MetadataExt as _;

    use rustix::fs::{Mode, OFlags, openat2};

    let fd = openat2(
        &directory,
        component,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
        confined_resolution(),
    )
    .map_err(|_| ToolError::new("could not open requested workspace directory"))?;
    let directory = File::from(fd);
    let metadata = directory
        .metadata()
        .map_err(|_| ToolError::new("could not inspect requested workspace directory"))?;
    if metadata.dev() != expected_device {
        return Err(ToolError::new(
            "requested workspace directory crosses a filesystem boundary",
        ));
    }
    Ok(directory)
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
    _context: &ToolContext,
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
async fn directory_entries(
    directory: File,
    remaining: usize,
    context: &ToolContext,
) -> Result<(Vec<OsString>, usize), ToolError> {
    tracked_blocking(context, move || {
        directory_entries_blocking(&directory, remaining)
    })
    .await
    .map_err(|_| ToolError::new("could not enumerate requested workspace directory"))?
}

#[cfg(target_os = "linux")]
fn directory_entries_blocking(
    directory: &File,
    remaining: usize,
) -> Result<(Vec<OsString>, usize), ToolError> {
    let entries = std::fs::read_dir(proc_fd_path(directory))
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
async fn directory_entries(
    _directory: File,
    _remaining: usize,
    _context: &ToolContext,
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

async fn tracked_blocking<T, F>(
    context: &ToolContext,
    operation: F,
) -> Result<T, tokio::task::JoinError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let activity = context.begin_blocking_activity();
    tokio::task::spawn_blocking(move || {
        let _activity = activity;
        operation()
    })
    .await
}

async fn read_utf8_bounded(mut file: File, context: &ToolContext) -> Result<String, ToolError> {
    tracked_blocking(context, move || read_utf8_bounded_blocking(&mut file))
        .await
        .map_err(|_| ToolError::new("could not read requested workspace file"))?
}

fn read_utf8_bounded_blocking(file: &mut File) -> Result<String, ToolError> {
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
    use super::{
        CompiledPathPolicy, FilePreservation, MAX_PATH_COMPONENTS, MAX_TOOL_OUTPUT_CHARS,
        NativeReadPolicy, SEARCH_OUTPUT_LIMIT_MARKER, append_search_match, checked_components,
        directory_entries, stage_content_blocking,
    };
    use crate::native::ToolContext;
    use std::ffi::OsString;
    use std::fs::File;
    use std::os::unix::fs::MetadataExt as _;

    #[tokio::test]
    async fn directory_enumeration_enforces_the_raw_entry_budget() {
        let root = tempfile::tempdir().expect("root");
        std::fs::write(root.path().join("one"), "one").expect("one");
        std::fs::write(root.path().join("two"), "two").expect("two");
        let directory = std::fs::File::open(root.path()).expect("directory");
        let context = ToolContext::new(root.path()).expect("context");

        let error = directory_entries(directory, 1, &context)
            .await
            .expect_err("entry budget");
        assert_eq!(
            error.to_string(),
            "search exceeded the workspace entry limit"
        );
    }

    #[test]
    fn trailing_globstar_excludes_the_named_directory_itself() {
        let policy =
            CompiledPathPolicy::new(&NativeReadPolicy::excluding(vec!["private/**".into()]))
                .expect("policy");
        assert!(!policy.allows(&[std::ffi::OsString::from("private")]));
        assert!(!policy.allows(&[
            std::ffi::OsString::from("private"),
            std::ffi::OsString::from("nested"),
        ]));
        assert!(policy.allows(&[std::ffi::OsString::from("public")]));
    }

    #[test]
    fn current_directory_components_are_rejected_in_exclusions() {
        let policy = NativeReadPolicy::excluding(vec!["./private/**".into()]);
        assert!(CompiledPathPolicy::new(&policy).is_err());
    }

    #[test]
    fn search_output_is_bounded_while_accumulating_matches() {
        let mut output = String::new();
        let line = "雪".repeat(MAX_TOOL_OUTPUT_CHARS);

        assert!(!append_search_match(&mut output, "large.txt", 1, &line));
        assert_eq!(output.chars().count(), MAX_TOOL_OUTPUT_CHARS);
        assert!(output.ends_with(SEARCH_OUTPUT_LIMIT_MARKER));
    }

    #[test]
    fn model_paths_have_a_fixed_component_limit() {
        let accepted = std::iter::repeat_n("a", MAX_PATH_COMPONENTS)
            .collect::<Vec<_>>()
            .join("/");
        let rejected = format!("{accepted}/b");

        assert_eq!(
            checked_components(&accepted, false)
                .expect("bounded path")
                .len(),
            MAX_PATH_COMPONENTS
        );
        assert_eq!(
            checked_components(&rejected, false)
                .expect_err("over-deep path")
                .to_string(),
            "path exceeds the component limit"
        );
    }

    #[test]
    fn staged_edit_rejects_ownership_drift_before_restoring_special_bits() {
        let root = tempfile::tempdir().expect("root");
        let parent = File::open(root.path()).expect("parent");
        let metadata = parent.metadata().expect("metadata");
        let preservation = FilePreservation {
            mode: 0o4755,
            uid: metadata.uid().wrapping_add(1),
            gid: metadata.gid(),
        };

        let result = stage_content_blocking(
            parent,
            &OsString::from("target"),
            b"replacement",
            Some(preservation),
        );

        assert!(result.is_err());
        assert!(
            std::fs::read_dir(root.path())
                .expect("entries")
                .all(|entry| !entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".vyane-write-"))
        );
    }
}
