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

/// Most labels one dispatch may carry, routing inputs included.
pub const MAX_LABELS: usize = 128;
/// Longest label key, in UTF-8 bytes.
pub const MAX_LABEL_KEY_BYTES: usize = 256;
/// Longest label value, in UTF-8 bytes. Large enough for a joined routing
/// list (256 values of up to 4 KiB each, the workflow plan budget).
pub const MAX_LABEL_VALUE_BYTES: usize = 2 * 1024 * 1024;
/// Largest combined size of every raw `key=value` label, in UTF-8 bytes.
pub const MAX_LABELS_TOTAL_BYTES: usize = 4 * 1024 * 1024;

/// Parse raw `key=value` labels, the single rule shared by CLI `--label`,
/// REST `labels` and MCP `labels`.
///
/// * the first `=` splits key from value; the value may contain `=`;
/// * the key follows the shared identifier rule from `vyane-core`
///   (non-blank, at most [`MAX_LABEL_KEY_BYTES`], no surrounding whitespace,
///   no control characters); an internal space is allowed;
/// * the value may be empty and may keep surrounding spaces, but it is a
///   single line of ledger metadata: no control character at all (newline,
///   tab and NUL included), at most [`MAX_LABEL_VALUE_BYTES`];
/// * at most [`MAX_LABELS`] labels and [`MAX_LABELS_TOTAL_BYTES`] in total;
///   a later duplicate key replaces an earlier one.
///
/// Errors name the 1-based label position and the reason, never the label:
/// front-ends print them to operator logs, and a malformed label may be a
/// value the caller pasted by mistake.
pub fn parse_labels(raw: Vec<String>) -> Result<BTreeMap<String, String>> {
    if raw.len() > MAX_LABELS {
        bail!("at most {MAX_LABELS} labels are allowed");
    }
    if raw.iter().map(String::len).sum::<usize>() > MAX_LABELS_TOTAL_BYTES {
        bail!("labels must total at most {MAX_LABELS_TOTAL_BYTES} bytes");
    }
    let mut labels = BTreeMap::new();
    for (index, label) in raw.into_iter().enumerate() {
        let position = index + 1;
        let (key, value) = label
            .split_once('=')
            .ok_or_else(|| anyhow!("label {position} must be in key=value form"))?;
        if let Some(problem) = vyane_core::identifier_problem(key, MAX_LABEL_KEY_BYTES) {
            bail!("label {position} key {problem}");
        }
        if value.len() > MAX_LABEL_VALUE_BYTES {
            bail!("label {position} value must be at most {MAX_LABEL_VALUE_BYTES} bytes");
        }
        if value.chars().any(char::is_control) {
            bail!("label {position} value must not contain control characters");
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
    if labels
        .keys()
        .any(|key| key.starts_with("routing.") && !INPUT_KEYS.contains(&key.as_str()))
    {
        bail!(
            "a `routing.*` label key is reserved for Vyane routing decisions; use stage/tier/tags/candidates/allow_frontier inputs instead"
        );
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

    /// Label keys and values land verbatim in the run ledger and in operator
    /// logs, so a control character (newline, ESC, NUL, tab) is rejected on
    /// every entry point that shares this parser.
    #[test]
    fn label_keys_reject_control_characters() {
        for raw in [
            "team\n=ops",
            "te\u{1b}[2Jam=ops",
            "te\0am=ops",
            "team\t=ops",
        ] {
            assert!(
                parse_labels(vec![raw.into()]).is_err(),
                "label key with a control character was accepted: {raw:?}"
            );
        }
    }

    #[test]
    fn label_keys_reject_surrounding_whitespace_but_allow_inner_spaces() {
        for raw in [" team=ops", "team =ops", " =ops"] {
            assert!(parse_labels(vec![raw.into()]).is_err(), "{raw:?}");
        }
        let labels = parse_labels(vec!["my team= ops = blue ".into()]).unwrap();
        assert_eq!(labels.get("my team").unwrap(), " ops = blue ");
    }

    #[test]
    fn label_values_are_single_line_without_control_characters() {
        for raw in [
            "team=o\nps",
            "team=ops\r",
            "team=o\0ps",
            "team=o\tps",
            "team=\u{1b}[2J",
        ] {
            assert!(parse_labels(vec![raw.into()]).is_err(), "{raw:?}");
        }
        let labels = parse_labels(vec!["empty=".into()]).unwrap();
        assert_eq!(labels.get("empty").unwrap(), "");
    }

    #[test]
    fn labels_are_bounded_in_count_and_size() {
        let at_limit = (0..MAX_LABELS).map(|index| format!("k{index}=v")).collect();
        assert_eq!(parse_labels(at_limit).unwrap().len(), MAX_LABELS);
        let too_many = (0..=MAX_LABELS)
            .map(|index| format!("k{index}=v"))
            .collect();
        assert!(parse_labels(too_many).is_err());

        let longest_key = format!("{}=v", "k".repeat(MAX_LABEL_KEY_BYTES));
        assert!(parse_labels(vec![longest_key]).is_ok());
        let long_key = format!("{}=v", "k".repeat(MAX_LABEL_KEY_BYTES + 1));
        assert!(parse_labels(vec![long_key]).is_err());

        let longest_value = format!("k={}", "v".repeat(MAX_LABEL_VALUE_BYTES));
        assert!(parse_labels(vec![longest_value]).is_ok());
        let long_value = format!("k={}", "v".repeat(MAX_LABEL_VALUE_BYTES + 1));
        assert!(parse_labels(vec![long_value]).is_err());

        let half = "v".repeat(MAX_LABEL_VALUE_BYTES - 2);
        let oversized_total = vec![
            format!("a={half}"),
            format!("b={half}"),
            format!("c={half}"),
        ];
        assert!(parse_labels(oversized_total).is_err());
    }

    /// Front-ends print label errors to operator logs (the REST handlers write
    /// them to the daemon's stderr), so an error names only the position and
    /// the reason, never caller-supplied bytes.
    #[test]
    fn label_errors_do_not_echo_input() {
        const CANARY: &str = "LABEL_CANARY_MUST_NOT_BE_ECHOED";
        for raw in [
            CANARY.to_string(),
            format!("={CANARY}"),
            format!("{CANARY}\n=value"),
            format!("key={CANARY}\n"),
            format!(" {CANARY}=value"),
        ] {
            let error = format!("{:#}", parse_labels(vec!["ok=1".into(), raw]).unwrap_err());
            assert!(error.starts_with("label 2 "), "{error}");
            assert!(!error.contains(CANARY), "{error}");
        }
        let reserved = parse_labels(vec![format!("routing.{CANARY}=x")]).unwrap();
        let error = format!("{:#}", validate_user_routing_labels(&reserved).unwrap_err());
        assert!(error.contains("reserved for Vyane routing decisions"));
        assert!(!error.contains(CANARY), "{error}");
    }
}
