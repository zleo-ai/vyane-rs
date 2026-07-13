#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use vyane_config::{ProfilePatch, ResolvedConfig};
use vyane_core::{ErrorKind, ModelId, Protocol};
use vyane_router::classify_intent;
use vyane_service::{RouteParams, route_task};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    normalization_version: String,
    reference: Reference,
    suites: Vec<SuiteManifest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    snapshot: String,
    disclosure: ReferenceDisclosure,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReferenceDisclosure {
    SanitizedBehaviorOnly,
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
    disposition: Disposition,
    blocker: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Disposition {
    Exact,
    NormalizedExact,
    OpenDifference,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoldenSuite<C> {
    schema_version: u32,
    suite: String,
    normalization: BTreeMap<String, String>,
    cases: Vec<C>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct RoutingArgs {
    task: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingLocator {
    #[serde(rename = "fn")]
    function: String,
    args: RoutingArgs,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RoutingOracleOutput {
    primary: String,
    confidence: f64,
    secondary: Option<String>,
    signals: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RoutingOutput {
    primary: String,
    confidence_millis: i64,
    secondary: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingCase {
    id: String,
    oracle_locator: RoutingLocator,
    oracle_raw_output: RoutingOracleOutput,
    normalized_oracle_output: RoutingOutput,
    rust_input: RoutingArgs,
    rust_output: RoutingOutput,
    disposition: Disposition,
    blocker: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FailoverArgs {
    status: String,
    error: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailoverLocator {
    #[serde(rename = "fn")]
    function: String,
    args: FailoverArgs,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReasonOutput {
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RustFailoverInput {
    error_kind: ErrorKind,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailoverCase {
    id: String,
    oracle_locator: FailoverLocator,
    oracle_raw_output: String,
    normalized_oracle_output: ReasonOutput,
    rust_input: RustFailoverInput,
    rust_output: ReasonOutput,
    rust_failover_eligible: bool,
    disposition: Disposition,
    blocker: Option<String>,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn fixture_root() -> PathBuf {
    repo_root().join("docs/parity/fixtures/v1")
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read parity fixture {}: {error}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("parse parity fixture {}: {error}", path.display()))
}

fn load_manifest() -> Manifest {
    read_json(&fixture_root().join("manifest.json"))
}

fn load_suite<C: DeserializeOwned>(suite: &SuiteManifest) -> GoldenSuite<C> {
    read_json(&repo_root().join(&suite.normalized_fixture))
}

fn public_fixture_sha256(suite: &SuiteManifest) -> String {
    let bytes = fs::read(repo_root().join(&suite.normalized_fixture))
        .unwrap_or_else(|error| panic!("read public parity fixture: {error}"));
    format!("{:x}", Sha256::digest(bytes))
}

trait GoldenCaseContract {
    fn id(&self) -> &str;
    fn disposition(&self) -> Disposition;
    fn blocker(&self) -> Option<&str>;
    fn oracle_digest(&self) -> String;
    fn oracle_raw_value(&self) -> Value;
    fn normalized_oracle_value(&self) -> Value;
    fn rust_value(&self) -> Value;
}

impl GoldenCaseContract for RoutingCase {
    fn id(&self) -> &str {
        &self.id
    }

    fn disposition(&self) -> Disposition {
        self.disposition
    }

    fn blocker(&self) -> Option<&str> {
        self.blocker.as_deref()
    }

    fn oracle_digest(&self) -> String {
        oracle_case_digest(
            &self.oracle_locator.function,
            &self.oracle_locator.args,
            &self.oracle_raw_output,
        )
    }

    fn oracle_raw_value(&self) -> Value {
        serde_json::to_value(&self.oracle_raw_output).expect("serialize routing oracle output")
    }

    fn normalized_oracle_value(&self) -> Value {
        serde_json::to_value(&self.normalized_oracle_output)
            .expect("serialize normalized routing output")
    }

    fn rust_value(&self) -> Value {
        serde_json::to_value(&self.rust_output).expect("serialize Rust routing output")
    }
}

impl GoldenCaseContract for FailoverCase {
    fn id(&self) -> &str {
        &self.id
    }

    fn disposition(&self) -> Disposition {
        self.disposition
    }

    fn blocker(&self) -> Option<&str> {
        self.blocker.as_deref()
    }

    fn oracle_digest(&self) -> String {
        oracle_case_digest(
            &self.oracle_locator.function,
            &self.oracle_locator.args,
            &self.oracle_raw_output,
        )
    }

    fn oracle_raw_value(&self) -> Value {
        serde_json::to_value(&self.oracle_raw_output).expect("serialize failover oracle output")
    }

    fn normalized_oracle_value(&self) -> Value {
        serde_json::to_value(&self.normalized_oracle_output)
            .expect("serialize normalized failover output")
    }

    fn rust_value(&self) -> Value {
        serde_json::to_value(&self.rust_output).expect("serialize Rust failover output")
    }
}

fn oracle_case_digest(function: &str, args: &impl Serialize, expected: &impl Serialize) -> String {
    let canonical = json!([
        function,
        serde_json::to_value(args).expect("serialize oracle args"),
        serde_json::to_value(expected).expect("serialize oracle expected output"),
    ]);
    let bytes = serde_json::to_vec(&canonical).expect("serialize canonical oracle case");
    format!("{:x}", Sha256::digest(bytes))
}

fn assert_repo_relative(path: &str) {
    let path = Path::new(path);
    assert!(!path.is_absolute(), "provenance path must be relative");
    assert!(
        path.components()
            .all(|part| matches!(part, Component::Normal(_))),
        "provenance path must not contain traversal or platform prefixes: {}",
        path.display()
    );
}

fn assert_sha256(value: &str) {
    assert_eq!(value.len(), 64, "SHA-256 must contain 64 hex digits");
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "SHA-256 must be lowercase hexadecimal: {value}"
    );
}

fn expected_public_fixtures() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        (
            "routing",
            "e7e026785b0fc9637a936ef8e08f008fa881cd69a5911b5073aba15763e9d572",
        ),
        (
            "failover",
            "22fb2e054535adc23753528f1446586d0ea09eb1177ca94a74d9f1dbdfcac355",
        ),
    ])
}

fn expected_scope(suite: &str) -> &'static str {
    match suite {
        "routing" => {
            "classify_intent primary, confidence and secondary plus vyane-service primary-intent exposure; stateful history, benchmark and feedback routing are excluded"
        }
        "failover" => {
            "classify_failover_reason taxonomy only; Rust failover_eligible is pinned as one-sided regression data and is not an oracle-equivalence claim; production attempt behavior is outside this suite and requires separate EXE-02 cross-repository acceptance evidence"
        }
        _ => panic!("unexpected parity suite `{suite}`"),
    }
}

fn expected_case_digests(suite: &str) -> BTreeMap<&'static str, &'static str> {
    let cases = match suite {
        "routing" => vec![
            (
                "routing.classify_intent.implement",
                "cc391317896c7afc1d7d915199f38e4f2ba72d89981ed39407e9f3f6fb59b509",
            ),
            (
                "routing.classify_intent.function",
                "d3a95659587ef011e7a5579815949e79aba2306083024a33f548e5103725ffec",
            ),
            (
                "routing.classify_intent.unit-test",
                "3d07272c2903167073cbae886383368c4caeb5fbf57d8c73b38d8be8fffa17d1",
            ),
            (
                "routing.classify_intent.implement-and-review",
                "989e70eec52391ec808e663922eb455adad7a654d88aacfd3466e6b268987186",
            ),
            (
                "routing.classify_intent.review-audit-implement",
                "45717e753bb6bffcfc3b8c093b5efa1452032ab2482a23fe33415be535f403b5",
            ),
            (
                "routing.classify_intent.debug-refactor",
                "82f99f566b3ad773ce54c2423ff1b2386b21f60b70a1edde927e3deb91f91cbe",
            ),
            (
                "routing.classify_intent.test-api-endpoint",
                "821007968e8e8026bc1eefde12be9f28da6a23d3c08c2acf0ce3eeab4c87d834",
            ),
            (
                "routing.classify_intent.root-cause",
                "6c59a543852f7bf61b2c3e37d1e872101ecff92752106c4e0b6bd00256e6901c",
            ),
            (
                "routing.classify_intent.bare-pr-confidence",
                "c1751234811e78d3f189ee45618dbf7a59da5b12ec07c4594fbeaf755d363d4e",
            ),
        ],
        "failover" => vec![
            (
                "failover.reason.timeout",
                "f85a62ba51beefacfa99b40bd616161b38243ce286d18249668be4040cdfcf36",
            ),
            (
                "failover.reason.rate-limit",
                "e446077554531a9129545bbc95d023db2854ef44b7e82f8dfa8e5766db351c57",
            ),
            (
                "failover.reason.runtime-unavailable",
                "0490be755594f93b81d43d7a07d28bd39c91c8e25fc9dd48a77bc96b3a3e82f6",
            ),
            (
                "failover.reason.execution-error",
                "fda556a0338e2b31c1cb8c68a6676ac404498ab306309d181d4a93d0f384448b",
            ),
            (
                "failover.reason.policy-blocked",
                "e11764e2b7109f9b5e71eaaa573397bbbc48cb7c62c679cdbee8161a57f0756b",
            ),
            (
                "failover.reason.quota-exhausted",
                "dfe8e4e263e257bbad3e5e38c7d6ef23f524a5f07a2c2a9c5ee244d8f4bcf9c7",
            ),
        ],
        _ => panic!("unexpected parity suite `{suite}`"),
    };
    cases.into_iter().collect()
}

fn validate_fixture<C>(suite: &SuiteManifest, fixture: GoldenSuite<C>)
where
    C: GoldenCaseContract,
{
    assert_eq!(fixture.schema_version, 1);
    assert_eq!(fixture.suite, suite.id);
    assert!(!fixture.normalization.is_empty());

    let manifest_cases = suite
        .cases
        .iter()
        .map(|case| (case.id.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        manifest_cases.len(),
        suite.cases.len(),
        "manifest contains duplicate case ids"
    );
    let fixture_cases = fixture
        .cases
        .iter()
        .map(|case| (case.id(), case))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        fixture_cases.len(),
        fixture.cases.len(),
        "fixture contains duplicate case ids"
    );

    let expected_digests = expected_case_digests(&suite.id);
    assert_eq!(
        manifest_cases.keys().copied().collect::<BTreeSet<_>>(),
        expected_digests.keys().copied().collect(),
        "manifest case set drifted; coverage changes require an explicit test update"
    );
    assert_eq!(
        fixture_cases.keys().copied().collect::<BTreeSet<_>>(),
        expected_digests.keys().copied().collect(),
        "fixture case set drifted; coverage changes require an explicit test update"
    );

    for (id, expected_digest) in expected_digests {
        let manifest_case = manifest_cases[id];
        let fixture_case = fixture_cases[id];
        assert_eq!(manifest_case.disposition, fixture_case.disposition());
        assert_eq!(manifest_case.blocker.as_deref(), fixture_case.blocker());
        assert_eq!(
            fixture_case.oracle_digest(),
            expected_digest,
            "vendored oracle locator/output drifted for `{id}`"
        );
        assert_disposition_truth(fixture_case);
    }
}

#[test]
fn manifest_schema_public_fixture_integrity_and_dispositions_are_closed() {
    let manifest = load_manifest();
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.normalization_version, "vyane-cross-repo-v1");
    assert_eq!(manifest.reference.snapshot, "behavioral-baseline-v1");
    assert_eq!(
        manifest.reference.disclosure,
        ReferenceDisclosure::SanitizedBehaviorOnly
    );

    let raw_manifest = fs::read_to_string(fixture_root().join("manifest.json")).unwrap();
    for forbidden_key in ["\"repository\"", "\"commit\"", "\"git_blob\"", "\"source\""] {
        assert!(
            !raw_manifest.contains(forbidden_key),
            "public parity manifest contains private provenance key {forbidden_key}"
        );
    }

    let expected = expected_public_fixtures();
    let mut suite_ids = BTreeSet::new();
    for suite in &manifest.suites {
        assert!(suite_ids.insert(suite.id.as_str()), "duplicate suite id");
        let expected_sha256 = expected
            .get(suite.id.as_str())
            .unwrap_or_else(|| panic!("unexpected parity suite `{}`", suite.id));
        assert_eq!(&suite.fixture_sha256, expected_sha256);
        assert_eq!(suite.scope, expected_scope(&suite.id));
        assert_repo_relative(&suite.normalized_fixture);
        assert_sha256(&suite.fixture_sha256);
        assert_eq!(public_fixture_sha256(suite), suite.fixture_sha256);
        assert!(
            suite
                .normalized_fixture
                .starts_with("docs/parity/fixtures/v1/"),
            "normalized fixtures must remain in the versioned parity directory"
        );

        match suite.id.as_str() {
            "routing" => validate_fixture(suite, load_suite::<RoutingCase>(suite)),
            "failover" => validate_fixture(suite, load_suite::<FailoverCase>(suite)),
            _ => panic!("unexpected parity suite `{}`", suite.id),
        }
    }

    assert_eq!(suite_ids, expected.keys().copied().collect());
}

fn hermetic_routing_config() -> ResolvedConfig {
    ResolvedConfig {
        providers: Default::default(),
        profiles: BTreeMap::from([(
            "parity-default".to_string(),
            ProfilePatch {
                provider: Some("parity-provider".to_string()),
                protocol: Some(Protocol::OpenaiChat),
                harness: Some("none".to_string()),
                model: Some(ModelId::new("parity-model")),
                tier: Some("economy".to_string()),
                ..Default::default()
            },
        )]),
    }
}

#[test]
fn routing_cases_recompute_current_rust_output() {
    let manifest = load_manifest();
    let suite_manifest = manifest
        .suites
        .iter()
        .find(|suite| suite.id == "routing")
        .expect("routing suite");
    let fixture = load_suite::<RoutingCase>(suite_manifest);
    let config = hermetic_routing_config();

    for case in fixture.cases {
        assert_eq!(case.oracle_locator.function, "classify_intent");
        assert_eq!(
            case.oracle_locator.args, case.rust_input,
            "routing inputs must be shared"
        );
        let task = &case.rust_input.task;

        let classified = classify_intent(task);
        let actual = RoutingOutput {
            primary: classified.primary.as_str().replace('_', "-"),
            confidence_millis: (classified.confidence * 1000.0).round() as i64,
            secondary: classified
                .secondary
                .map(|intent| intent.as_str().replace('_', "-")),
        };
        assert_eq!(
            actual, case.rust_output,
            "pinned Rust routing output drifted for `{}`",
            case.id
        );

        let routed = route_task(
            &config,
            RouteParams {
                task: task.to_string(),
                ..Default::default()
            },
        )
        .unwrap_or_else(|error| panic!("route parity case `{}`: {error}", case.id));
        assert_eq!(
            routed
                .decision
                .intent
                .to_ascii_lowercase()
                .replace('_', "-"),
            actual.primary,
            "service routing intent drifted from the core classifier for `{}`",
            case.id
        );

        let normalized = RoutingOutput {
            primary: case.oracle_raw_output.primary.replace('_', "-"),
            confidence_millis: (case.oracle_raw_output.confidence * 1000.0).round() as i64,
            secondary: case
                .oracle_raw_output
                .secondary
                .map(|value| value.replace('_', "-")),
        };
        assert_eq!(
            case.normalized_oracle_output, normalized,
            "oracle routing normalization drifted for `{}`",
            case.id
        );
    }
}

fn normalize_error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::RateLimited => "rate_limit",
        ErrorKind::Timeout => "timeout",
        ErrorKind::SpawnFailed => "runtime_unavailable",
        ErrorKind::HarnessFailed => "runtime_crash",
        ErrorKind::Other => "execution_error",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::Config => "config",
        ErrorKind::Auth => "auth",
        ErrorKind::Transport => "transport",
        ErrorKind::Protocol => "protocol",
        ErrorKind::Cancelled => "cancelled",
        ErrorKind::NotFound => "not_found",
        ErrorKind::Conflict => "conflict",
        ErrorKind::Io => "io",
        ErrorKind::Indeterminate => "indeterminate",
        _ => "unknown",
    }
}

#[test]
fn failover_taxonomy_recomputes_reason_and_pins_rust_gate_without_parity_claim() {
    let manifest = load_manifest();
    let suite_manifest = manifest
        .suites
        .iter()
        .find(|suite| suite.id == "failover")
        .expect("failover suite");
    let fixture = load_suite::<FailoverCase>(suite_manifest);

    for case in fixture.cases {
        assert_eq!(case.oracle_locator.function, "classify_failover_reason");
        let kind = case.rust_input.error_kind;
        let actual = ReasonOutput {
            reason: normalize_error_kind(kind).to_string(),
        };
        assert_eq!(
            actual, case.rust_output,
            "pinned Rust failover output drifted for `{}`",
            case.id
        );
        assert_eq!(
            kind.failover_eligible(),
            case.rust_failover_eligible,
            "one-sided Rust gate regression data drifted for `{}`; this is not an oracle-equivalence assertion",
            case.id
        );

        assert_eq!(
            case.normalized_oracle_output,
            ReasonOutput {
                reason: case.oracle_raw_output,
            },
            "oracle failover normalization drifted for `{}`",
            case.id
        );
    }
}

fn assert_disposition_truth(case: &impl GoldenCaseContract) {
    match case.disposition() {
        Disposition::Exact => {
            assert!(
                case.blocker().is_none(),
                "exact disposition retains a stale blocker for `{}`",
                case.id()
            );
            assert_eq!(
                case.oracle_raw_value(),
                case.rust_value(),
                "exact disposition changed the raw output for `{}`",
                case.id()
            );
            assert_eq!(
                case.normalized_oracle_value(),
                case.rust_value(),
                "exact disposition drifted after normalization for `{}`",
                case.id()
            );
        }
        Disposition::NormalizedExact => {
            assert!(
                case.blocker().is_none(),
                "normalized equality retains a stale blocker for `{}`",
                case.id()
            );
            assert_eq!(
                case.normalized_oracle_value(),
                case.rust_value(),
                "normalized equality drifted for `{}`",
                case.id()
            );
        }
        Disposition::OpenDifference => {
            assert_ne!(
                case.normalized_oracle_value(),
                case.rust_value(),
                "open difference `{}` unexpectedly became equal; resolve its blocker and update the disposition intentionally",
                case.id()
            );
            assert!(
                case.blocker()
                    .is_some_and(|blocker| blocker.starts_with("BLOCKER ")),
                "open difference `{}` lost its explicit blocker",
                case.id()
            );
        }
    }
}
