//! Native executable coverage shared by Windows and Unix; no shell or login.
#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use vyane_core::env::EnvPolicy;
use vyane_core::target::{ModelId, Protocol, Sandbox, Secret};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessJob};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};

fn captured_args(path: &Path) -> Vec<String> {
    let bytes = std::fs::read(path).unwrap();
    let mut bytes = bytes.as_slice();
    let mut args = Vec::new();
    while !bytes.is_empty() {
        let length = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        args.push(String::from_utf8(bytes[8..8 + length].to_vec()).unwrap());
        bytes = &bytes[8 + length..];
    }
    args
}

#[tokio::test]
async fn native_executable_handles_unicode_spaces_crlf_and_literal_prompt() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("fixture space 中文");
    std::fs::create_dir(&directory).unwrap();
    let binary = directory.join(format!("fake-cli{}", std::env::consts::EXE_SUFFIX));
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/native_cli.rs");
    let mut compiler = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()));
    compiler
        .arg(source)
        .arg("--edition=2024")
        .arg("-o")
        .arg(&binary);
    if let Some(linker) = std::env::var_os("CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER") {
        compiler
            .arg("-C")
            .arg(format!("linker={}", linker.to_string_lossy()));
    }
    let output = compiler.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = "--literal & echo injected | 中文\r\nsecond line";
    for codex in [false, true] {
        let harness: Box<dyn Harness> = if codex {
            Box::new(CodexCliHarness::with_binary(binary.to_string_lossy()))
        } else {
            Box::new(ClaudeCodeHarness::with_binary_and_parent_env_for_tests(
                binary.to_string_lossy(),
                Vec::new(),
            ))
        };
        for stream in [false, true] {
            let mut env = EnvPolicy::scrubbed();
            env.inject.insert(
                "VYANE_FIXTURE_DIR".into(),
                Secret::new(directory.to_string_lossy()),
            );
            let job = HarnessJob {
                prompt: prompt.into(),
                model: ModelId::new("fixture-model"),
                protocol: Protocol::OpenaiResponses,
                endpoint: None,
                params: GenParams::default(),
                workdir: Some(directory.clone()),
                sandbox: Sandbox::ReadOnly,
                resume: None,
                env,
                timeout: Some(Duration::from_secs(10)),
                harness_lifecycle_reporter: None,
            };
            let outcome = if stream {
                harness
                    .run_stream(job, CancellationToken::new(), Box::new(|_| {}))
                    .await
            } else {
                harness.run(job, CancellationToken::new()).await
            }
            .unwrap();
            assert_eq!(outcome.text.trim(), "fixture final");
            assert_eq!(
                outcome.native_session_id.as_deref(),
                Some("fixture-session")
            );
            let args = captured_args(&directory.join("argv.bin"));
            assert_eq!(args.last().map(String::as_str), Some(prompt));
            assert_eq!(args[args.len() - 2], "--");
            let cwd = std::fs::read_to_string(directory.join("cwd.txt")).unwrap();
            assert_eq!(
                std::fs::canonicalize(cwd).unwrap(),
                std::fs::canonicalize(&directory).unwrap()
            );
        }
    }
}
