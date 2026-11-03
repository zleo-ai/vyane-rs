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

/// Reject labels that impersonate router output. Routing inputs use a small,
/// explicit allow-list; decision fields are written only by `plan_dispatch`.
pub fn validate_user_routing_labels(labels: &BTreeMap<String, String>) -> Result<()> {
    const INPUT_KEYS: &[&str] = &[
        "routing.stage",
        "routing.tier",
        "routing.tags",
        "routing.candidates",
        "routing.allow_frontier",
    ];
    for key in labels.keys().filter(|key| key.starts_with("routing.")) {
        if !INPUT_KEYS.contains(&key.as_str()) {
            bail!(
                "label `{key}` is reserved for Vyane routing decisions; use stage/tier/tags/candidates/allow_frontier inputs instead"
            );
        }
    }
    Ok(())
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

    #[test]
    fn routing_decision_labels_are_reserved() {
        let labels = parse_labels(vec![
            "routing.stage=review".into(),
            "routing.allow_frontier=false".into(),
        ])
        .unwrap();
        validate_user_routing_labels(&labels).unwrap();

        let forged = parse_labels(vec!["routing.provider=foreign".into()]).unwrap();
        assert!(validate_user_routing_labels(&forged).is_err());
        let forged_effort = parse_labels(vec!["routing.effort=xhigh".into()]).unwrap();
        assert!(validate_user_routing_labels(&forged_effort).is_err());
        let future_reserved = parse_labels(vec!["routing.unknown=value".into()]).unwrap();
        assert!(validate_user_routing_labels(&future_reserved).is_err());
    }
}

/// Red-team reproductions; these fail on the current implementation.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod redteam_tests {
    use super::*;

    /// The same class #283 fixed for workflow variable keys: dispatch label
    /// keys accept control characters (newline, ESC, NUL) on every entry
    /// (CLI `--label`, REST `labels`, MCP `labels`), and they land verbatim
    /// in the run ledger.
    #[test]
    fn redteam_label_keys_reject_control_characters() {
        for raw in ["team\n=ops", "te\u{1b}[2Jam=ops", "te\0am=ops", "team\t=ops"] {
            assert!(
                parse_labels(vec![raw.into()]).is_err(),
                "label key with a control character was accepted: {raw:?}"
            );
        }
    }

    /// A malformed label is echoed whole in the error, which the REST
    /// handlers print to the daemon's stderr (`format_dispatch_label_error_line`).
    /// That echoes caller-supplied bytes (including newlines/ANSI) into the
    /// operator log and repeats a value the caller may have pasted by mistake.
    #[test]
    fn redteam_label_errors_do_not_echo_input() {
        const CANARY: &str = "LABEL_CANARY_MUST_NOT_BE_ECHOED";
        for raw in [CANARY.to_string(), format!("={CANARY}")] {
            let error = format!("{:#}", parse_labels(vec![raw]).unwrap_err());
            assert!(!error.contains(CANARY), "{error}");
        }
    }
}
