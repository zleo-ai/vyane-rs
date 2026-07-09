//! Task-spec construction: turning front-end parameters into a [`TaskSpec`].
//!
//! Shared by every front-end so label parsing, timeout mapping, and sandbox
//! assignment stay identical across CLI, REST, and MCP.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use vyane_core::{Sandbox, TaskSpec};

/// Build a [`TaskSpec`] from the parameters every front-end exposes.
///
/// `labels` are raw `key=value` strings parsed here so a malformed label fails
/// before dispatch (not deep inside the kernel).
pub fn build_task_spec(
    prompt: String,
    workdir: Option<PathBuf>,
    sandbox: Sandbox,
    system: Option<String>,
    timeout_secs: Option<u64>,
    labels: Vec<String>,
) -> Result<TaskSpec> {
    let mut task = TaskSpec::new(prompt).with_sandbox(sandbox);
    task.workdir = workdir;
    task.system = system;
    task.timeout = timeout_secs.map(Duration::from_secs);
    task.labels = parse_labels(labels)?;
    Ok(task)
}

pub fn parse_labels(raw: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    for label in raw {
        let (key, value) = label
            .split_once('=')
            .ok_or_else(|| anyhow!("label `{label}` must be in key=value form"))?;
        if key.is_empty() {
            bail!("label `{label}` has an empty key");
        }
        labels.insert(key.to_string(), value.to_string());
    }
    Ok(labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_parsing() {
        let labels = parse_labels(vec!["env=prod".into(), "team=ops".into()]).unwrap();
        assert_eq!(labels.get("env").unwrap(), "prod");
        assert_eq!(labels.get("team").unwrap(), "ops");
    }

    #[test]
    fn label_missing_equals_errors() {
        assert!(parse_labels(vec!["bad".into()]).is_err());
    }

    #[test]
    fn label_empty_key_errors() {
        assert!(parse_labels(vec!["=value".into()]).is_err());
    }
}
