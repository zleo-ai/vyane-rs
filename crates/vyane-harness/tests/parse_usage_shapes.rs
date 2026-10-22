//! Codex usage JSON shapes the parser already reads, beyond
//! `turn.completed.usage` and `token_count.info.total_token_usage`.
//!
//! Cost and `duration_ms` are not part of these shapes: the harness does not
//! parse them into the outcome. Duration on the outcome is the measured run
//! time, and reported USD is not priced here.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use vyane_core::Usage;
use vyane_core::env::EnvPolicy;
use vyane_core::target::{ModelId, Protocol, Sandbox};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessJob};
use vyane_harness::CodexCliHarness;

fn base_job(prompt: &str) -> HarnessJob {
    HarnessJob {
        prompt: prompt.to_string(),
        model: ModelId::new(""),
        protocol: Protocol::OpenaiResponses,
        endpoint: None,
        params: GenParams::default(),
        workdir: None,
        sandbox: Sandbox::ReadOnly,
        resume: None,
        env: EnvPolicy::scrubbed(),
        timeout: None,
        harness_lifecycle_reporter: None,
    }
}

fn write_script(dir: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, body).unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).unwrap();
    path
}

fn codex_script(events: &[&str]) -> String {
    let mut body = r#"#!/bin/sh
set -eu
out=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "-o" ] || [ "$prev" = "--output-last-message" ]; then
    out="$arg"
  fi
  prev="$arg"
done
if [ -n "$out" ]; then
  printf '%s\n' 'codex final' > "$out"
fi
"#
    .to_string();
    for event in events {
        assert!(!event.contains('\''), "fixture must stay single-quote free");
        body.push_str(&format!("printf '%s\\n' '{event}'\n"));
    }
    body
}

async fn assert_codex_usage(name: &str, events: &[&str], session: &str, usage: Usage) {
    let dir = TempDir::new().unwrap();
    let bin = write_script(&dir, name, &codex_script(events));
    let outcome = CodexCliHarness::with_binary(bin.to_string_lossy())
        .run(base_job("usage shape"), CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.text, "codex final");
    assert_eq!(outcome.native_session_id.as_deref(), Some(session));
    assert_eq!(outcome.exit_code, 0);
    assert_eq!(outcome.usage, Some(usage));
}

fn usage(input: u64, output: u64, cached: u64) -> Usage {
    Usage {
        input_tokens: input,
        output_tokens: output,
        reasoning_tokens: None,
        cached_input_tokens: Some(cached),
    }
}

#[tokio::test]
async fn token_count_prefers_total_usage_over_last_usage() {
    assert_codex_usage(
        "fake-codex-total-over-last",
        &[
            r#"{"type":"thread.started","thread_id":"th-total"}"#,
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":12,"output_tokens":40,"reasoning_output_tokens":7,"total_tokens":140},"last_token_usage":{"input_tokens":8,"cached_input_tokens":4,"output_tokens":9,"reasoning_output_tokens":3,"total_tokens":17}}}}"#,
        ],
        "th-total",
        usage(100, 40, 12),
    )
    .await;
}

#[tokio::test]
async fn token_count_reads_last_token_usage_when_total_is_absent() {
    assert_codex_usage(
        "fake-codex-last-usage",
        &[
            r#"{"type":"thread.started","thread_id":"th-last"}"#,
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":8,"output_tokens":9,"cached_input_tokens":4}}}}"#,
        ],
        "th-last",
        usage(8, 9, 4),
    )
    .await;
}

#[tokio::test]
async fn token_count_reads_info_usage_when_named_totals_are_absent() {
    assert_codex_usage(
        "fake-codex-info-usage",
        &[
            r#"{"type":"thread.started","thread_id":"th-info"}"#,
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"usage":{"input_tokens":21,"output_tokens":22,"cached_input_tokens":2}}}}"#,
        ],
        "th-info",
        usage(21, 22, 2),
    )
    .await;
}

#[tokio::test]
async fn other_events_read_top_level_usage() {
    assert_codex_usage(
        "fake-codex-top-usage",
        &[
            r#"{"type":"item.completed","thread_id":"th-top","item":{"type":"agent_message","text":"ignored"},"usage":{"input_tokens":30,"output_tokens":31,"cached_input_tokens":6}}"#,
        ],
        "th-top",
        usage(30, 31, 6),
    )
    .await;
}
