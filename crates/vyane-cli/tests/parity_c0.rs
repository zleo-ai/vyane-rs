#![allow(clippy::unwrap_used)]

#[path = "parity_c0/cli_error.rs"]
mod cli_error;
#[path = "parity_c0/effort.rs"]
mod effort;
#[path = "parity_c0/selector.rs"]
mod selector;
#[path = "parity_c0/session.rs"]
mod session;
#[path = "parity_c0/usage.rs"]
mod usage;
#[path = "parity_c0/workflow.rs"]
mod workflow;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

const RUST_TEST: &str = "crates/vyane-cli/tests/parity_c0.rs::c0_corpus_replays_offline";
const FORBIDDEN_SNIPPETS: &[&str] = &[
    ".sqlite",
    ".config/vyane",
    ".config\\vyane",
    "sk-",
    "api_key",
    "AKIA",
    "github_pat_",
    "BEGIN PRIVATE",
    "C:/Users/",
    "C:\\Users\\",
    "/Users/",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct C0Manifest {
    schema_version: u32,
    normalization_version: String,
    reference: Reference,
    suites: Vec<SuiteManifest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    snapshot: String,
    disclosure: String,
    python_expectation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SuiteManifest {
    id: String,
    fixture_sha256: String,
    scope: String,
    normalized_fixture: String,
    cases: Vec<CaseManifest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseManifest {
    id: String,
    disposition: String,
    blocker: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema_version: u32,
    suite: String,
    normalization: BTreeMap<String, String>,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    input: Value,
    oracle_locator: Locator,
    // Kept so deny_unknown_fields still requires the authored raw oracle.
    #[allow(dead_code)]
    oracle_raw_output: Value,
    normalized_oracle_output: Value,
    rust_output: Value,
    disposition: String,
    blocker: Option<String>,
    rust_test: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Locator {
    #[serde(rename = "fn")]
    function: String,
    args: BTreeMap<String, Value>,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize repo root")
}

fn worktree_temp_root() -> PathBuf {
    let root = repo_root().join(".tmp-c0-parity");
    fs::create_dir_all(&root).expect("create worktree temp root");
    let canonical = root.canonicalize().expect("canonicalize temp root");
    assert!(
        canonical.starts_with(repo_root()),
        "temp root must stay inside the worktree"
    );
    assert!(canonical.ends_with(".tmp-c0-parity"));
    canonical
}

fn reject_private_text(label: &str, text: &str) {
    for snippet in FORBIDDEN_SNIPPETS {
        assert!(
            !text.contains(snippet),
            "{label} contains forbidden snippet {snippet}"
        );
    }
}

fn sha256_file(path: &Path) -> String {
    let bytes = fs::read(path).expect("read fixture");
    format!("{:x}", Sha256::digest(bytes))
}

struct TempRootGuard(PathBuf);

impl Drop for TempRootGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn replay(suite: &str, input: &Value, temp: &Path) -> Value {
    match suite {
        "cli_error" => cli_error::replay(input, temp),
        "selector" => selector::replay(input, temp),
        "effort" => effort::replay(input, temp),
        "usage" => usage::replay(input),
        "session" => session::replay(input, temp),
        "workflow" => workflow::replay(input, temp),
        other => panic!("unknown c0 suite {other}"),
    }
}

fn assert_disposition(case: &Case) {
    match case.disposition.as_str() {
        "aligned" => {
            assert!(case.blocker.is_none(), "{}", case.id);
            assert_eq!(
                case.normalized_oracle_output, case.rust_output,
                "{}",
                case.id
            );
        }
        "intentional_retain" => {
            assert_ne!(
                case.normalized_oracle_output, case.rust_output,
                "{}",
                case.id
            );
            assert!(
                case.blocker
                    .as_deref()
                    .is_some_and(|blocker| blocker.starts_with("RETAIN ")),
                "{}",
                case.id
            );
        }
        "pending" => {
            assert_ne!(
                case.normalized_oracle_output, case.rust_output,
                "{}",
                case.id
            );
            assert!(
                case.blocker
                    .as_deref()
                    .is_some_and(|blocker| blocker.starts_with("PENDING ")),
                "{}",
                case.id
            );
        }
        other => panic!("unknown disposition {other} for {}", case.id),
    }
}

#[test]
fn c0_corpus_replays_offline() {
    let root = repo_root();
    let manifest_path = root.join("docs/parity/fixtures/v1/c0-manifest.json");
    let manifest_text = fs::read_to_string(&manifest_path).expect("read c0 manifest");
    reject_private_text("c0 manifest", &manifest_text);
    let manifest: C0Manifest = serde_json::from_str(&manifest_text).expect("parse c0 manifest");
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.normalization_version, "vyane-c0-contract-v1");
    assert_eq!(manifest.reference.snapshot, "c0-contract-2026-11-05");
    assert_eq!(manifest.reference.disclosure, "sanitized_behavior_only");
    assert_eq!(
        manifest.reference.python_expectation,
        "authored_contract_pin"
    );

    let expected_suites = [
        "cli_error",
        "selector",
        "effort",
        "usage",
        "session",
        "workflow",
    ];
    assert_eq!(
        manifest
            .suites
            .iter()
            .map(|suite| suite.id.as_str())
            .collect::<Vec<_>>(),
        expected_suites
    );

    let temp_root = worktree_temp_root();
    let _temp_root_guard = TempRootGuard(temp_root.clone());
    let temp = tempfile::tempdir_in(&temp_root).expect("worktree tempdir");
    let mut seen_ids = BTreeSet::new();
    for suite in &manifest.suites {
        assert!(!suite.scope.is_empty(), "{}", suite.id);
        let relative = Path::new(&suite.normalized_fixture);
        assert!(
            relative
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
            "{}",
            suite.id
        );
        let fixture_path = root.join(relative);
        assert_eq!(
            fixture_path.parent().and_then(|path| path.file_name()),
            Some(std::ffi::OsStr::new("v1"))
        );
        assert_eq!(
            sha256_file(&fixture_path),
            suite.fixture_sha256,
            "{}",
            suite.id
        );
        let fixture_text = fs::read_to_string(&fixture_path).expect("read fixture");
        reject_private_text(&suite.id, &fixture_text);
        let fixture: Fixture = serde_json::from_str(&fixture_text).expect("parse fixture");
        assert_eq!(fixture.schema_version, 1);
        assert_eq!(fixture.suite, suite.id);
        assert!(!fixture.normalization.is_empty(), "{}", suite.id);

        let manifest_ids = suite
            .cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>();
        let fixture_ids = fixture
            .cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(manifest_ids, fixture_ids, "{}", suite.id);
        assert_eq!(manifest_ids.len(), suite.cases.len(), "{}", suite.id);
        let manifest_by_id = suite
            .cases
            .iter()
            .map(|case| (case.id.as_str(), case))
            .collect::<BTreeMap<_, _>>();

        for case in &fixture.cases {
            assert!(seen_ids.insert(case.id.clone()), "duplicate {}", case.id);
            assert_eq!(case.oracle_locator.function, "authored_contract_pin");
            assert!(case.oracle_locator.args.is_empty(), "{}", case.id);
            assert_eq!(case.rust_test, RUST_TEST, "{}", case.id);
            let manifest_case = manifest_by_id[case.id.as_str()];
            assert_eq!(manifest_case.disposition, case.disposition, "{}", case.id);
            assert_eq!(
                manifest_case.blocker.as_deref(),
                case.blocker.as_deref(),
                "{}",
                case.id
            );
            assert_disposition(case);
            let case_dir = temp.path().join(case.id.replace(['.', ':'], "_"));
            fs::create_dir_all(&case_dir).expect("case temp");
            let actual = replay(&suite.id, &case.input, &case_dir);
            let rendered = serde_json::to_string(&actual).expect("render actual");
            reject_private_text(&case.id, &rendered);
            assert!(
                !rendered.contains(case_dir.to_string_lossy().as_ref()),
                "{} echoed its temp path",
                case.id
            );
            assert_eq!(
                actual, case.rust_output,
                "rust output drifted for {}",
                case.id
            );
        }
    }

    drop(temp);
    let _ = fs::remove_dir_all(&temp_root);
}
