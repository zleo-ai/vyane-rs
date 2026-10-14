//! Recompute explicit target coordinates and complete scripted dispatch trails.
//! Only adapter outcomes are fake: resolution, dispatch and ledger persistence
//! use production code. The reference capture is a sanitized, fixed snapshot.

use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use vyane_config::RawFailoverElement;
use vyane_core::{
    AdapterTransport, AttemptOutcome, AuthStyle, BoundTarget, CancellationToken, Harness,
    HarnessJob, HarnessKind, HarnessOutcome, Ledger, RunQuery, Target, VyaneError,
};
use vyane_kernel::{Dispatcher, Executor, ExecutorFactory};
use vyane_ledger::{FsSessionStore, JsonlLedger};
use vyane_provider::Provider;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetInput {
    provider: String,
    provider_protocol: Protocol,
    protocol: Protocol,
    harness: String,
    model: Option<String>,
    default_model: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResolutionInput {
    targets: Vec<TargetInput>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Behavior {
    Success,
    ExecutionError,
    Timeout,
    RateLimit,
    QuotaExhausted,
    Auth,
    PolicyBlocked,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrailInput {
    targets: Vec<TargetInput>,
    behaviors: Vec<Behavior>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case<I> {
    id: String,
    oracle_locator: AutomaticRoutingLocator,
    input: I,
    sanitized_oracle_output: Value,
    normalized_oracle_output: Value,
    rust_output: Value,
    disposition: Disposition,
    blocker: Option<String>,
}

impl<I: Serialize> GoldenCaseContract for Case<I> {
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
            &self.input,
            &self.sanitized_oracle_output,
        )
    }
    fn oracle_raw_value(&self) -> Value {
        self.sanitized_oracle_output.clone()
    }
    fn normalized_oracle_value(&self) -> Value {
        self.normalized_oracle_output.clone()
    }
    fn rust_value(&self) -> Value {
        self.rust_output.clone()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Coordinates {
    provider: String,
    protocol: String,
    harness: String,
    model: String,
}

impl Coordinates {
    fn normalize(mut self) -> Self {
        self.harness = match self.harness.as_str() {
            "direct-chat" => "none".into(),
            "codex-cli" => "codex".into(),
            _ => self.harness,
        };
        self
    }
    fn from_target(target: &Target) -> Self {
        Self {
            provider: target.provider.to_string(),
            protocol: target.protocol.as_str().into(),
            harness: target
                .harness
                .as_ref()
                .map_or("none", HarnessKind::as_str)
                .into(),
            model: target.model.to_string(),
        }
    }
}

fn resolve(inputs: &[TargetInput]) -> Vec<BoundTarget> {
    assert!(!inputs.is_empty());
    let mut config = ResolvedConfig::default();
    for (i, target) in inputs.iter().enumerate() {
        config.providers.insert(
            &target.provider,
            Provider {
                base_url: "https://example.invalid/v1".into(),
                api_key_env: None,
                auth_style: AuthStyle::Bearer,
                protocol: target.provider_protocol,
                default_model: Some(ModelId::new(&target.default_model)),
                extra: Default::default(),
                env_inject: Default::default(),
            },
        );
        config.profiles.insert(
            format!("target-{i}"),
            ProfilePatch {
                provider: Some(target.provider.clone()),
                protocol: Some(target.protocol),
                harness: Some(target.harness.clone()),
                model: target.model.as_ref().map(ModelId::new),
                failover: (i == 0).then(|| {
                    (1..inputs.len())
                        .map(|index| RawFailoverElement::ProfileName(format!("target-{index}")))
                        .collect()
                }),
                ..Default::default()
            },
        );
    }
    config
        .resolve_failover_with("target-0", &|_| {
            panic!("fixture must not request credentials")
        })
        .unwrap()
}

fn suite<C: DeserializeOwned>(id: &str) -> GoldenSuite<C> {
    let manifest = load_manifest();
    load_suite(
        manifest
            .suites
            .iter()
            .find(|suite| suite.id == id)
            .expect("required suite"),
    )
}

#[test]
fn explicit_target_resolution_recomputes_all_four_coordinates() {
    for case in suite::<Case<ResolutionInput>>("target_resolution").cases {
        assert_eq!(case.oracle_locator.function, "resolve_explicit_targets");
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            targets: Vec<Coordinates>,
        }
        let raw: Raw = serde_json::from_value(case.sanitized_oracle_output.clone()).unwrap();
        let normalized = json!({"targets":raw.targets.into_iter().map(Coordinates::normalize).collect::<Vec<_>>()});
        assert_eq!(
            normalized, case.normalized_oracle_output,
            "reference normalization: {}",
            case.id
        );
        let chain = resolve(&case.input.targets);
        let actual = json!({"targets":chain.iter().map(|b|Coordinates::from_target(&b.target)).collect::<Vec<_>>()});
        assert_eq!(actual, case.rust_output, "Rust resolution: {}", case.id);
        assert_disposition_truth(&case);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAttempt {
    ordinal: usize,
    resolved_provider: String,
    protocol: String,
    harness: String,
    model: String,
    status: String,
    error_code: String,
    trigger_reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Invocation {
    provider: String,
    model: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTrail {
    status: String,
    output: String,
    attempts: Vec<RawAttempt>,
    calls: Vec<Invocation>,
}

fn normalized_reference(raw: RawTrail) -> Value {
    assert_eq!(
        raw.attempts.len(),
        raw.calls.len(),
        "capture must include every adapter invocation"
    );
    let count = raw.attempts.len();
    let source_reason = raw
        .attempts
        .first()
        .expect("nonempty capture")
        .error_code
        .clone();
    let attempts: Vec<_> = raw.attempts.into_iter().enumerate().map(|(index, attempt)| {
        assert_eq!(attempt.ordinal, index + 1);
        assert_eq!(attempt.trigger_reason, if index == 0 { "" } else { &source_reason }, "reference trigger retains the primary failure");
        assert_eq!(raw.calls[index].provider, attempt.resolved_provider);
        assert_eq!(raw.calls[index].model, attempt.model);
        json!({"ordinal":attempt.ordinal,"target":Coordinates {
            provider:attempt.resolved_provider,protocol:attempt.protocol,harness:attempt.harness,model:attempt.model
        }.normalize(),"status":attempt.status,"reason":attempt.error_code,"failed_over":index+1<count})
    }).collect();
    json!({"status":raw.status,"output":raw.output,"attempts":attempts,"calls":raw.calls})
}

struct ScriptedHarness {
    target: Target,
    behavior: Behavior,
    calls: Arc<Mutex<Vec<Invocation>>>,
}

#[async_trait]
impl Harness for ScriptedHarness {
    fn kind(&self) -> HarnessKind {
        self.target.harness.clone().unwrap()
    }
    async fn available(&self) -> bool {
        true
    }
    async fn run(
        &self,
        job: HarnessJob,
        _cancel: CancellationToken,
    ) -> vyane_core::Result<HarnessOutcome> {
        assert_eq!(
            job.protocol, self.target.protocol,
            "protocol handed to harness"
        );
        self.calls.lock().unwrap().push(Invocation {
            provider: self.target.provider.to_string(),
            model: job.model.to_string(),
        });
        let kind = match self.behavior {
            Behavior::Success => {
                return Ok(HarnessOutcome {
                    text: "fixture reply".into(),
                    native_session_id: None,
                    usage: None,
                    exit_code: 0,
                    duration: Duration::ZERO,
                });
            }
            Behavior::ExecutionError => ErrorKind::Transport,
            Behavior::Timeout => ErrorKind::Timeout,
            Behavior::RateLimit | Behavior::QuotaExhausted => ErrorKind::RateLimited,
            Behavior::Auth => ErrorKind::Auth,
            Behavior::PolicyBlocked => ErrorKind::Unsupported,
        };
        Err(VyaneError::new(kind, "synthetic adapter failure"))
    }
}

struct ScriptedFactory {
    behaviors: BTreeMap<String, Behavior>,
    calls: Arc<Mutex<Vec<Invocation>>>,
}

impl ExecutorFactory for ScriptedFactory {
    fn make(&self, bound: &BoundTarget) -> vyane_core::Result<Executor> {
        assert_eq!(bound.transport, AdapterTransport::CliWrap);
        Ok(Executor::Agent(Arc::new(ScriptedHarness {
            target: bound.target.clone(),
            behavior: self.behaviors[bound.target.provider.as_str()],
            calls: Arc::clone(&self.calls),
        })))
    }
}

#[tokio::test]
async fn dispatch_trails_recompute_attempt_order_failover_and_persisted_records() {
    for case in suite::<Case<TrailInput>>("dispatch_trail").cases {
        assert_eq!(case.oracle_locator.function, "dispatch_scripted_attempts");
        let normalized = normalized_reference(
            serde_json::from_value(case.sanitized_oracle_output.clone()).unwrap(),
        );
        assert_eq!(
            normalized, case.normalized_oracle_output,
            "reference normalization: {}",
            case.id
        );
        assert_eq!(case.input.targets.len(), case.input.behaviors.len());
        let root = common::private_tempdir();
        let ledger = Arc::new(JsonlLedger::new(root.path().join("history.jsonl")));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let factory = Arc::new(ScriptedFactory {
            behaviors: case
                .input
                .targets
                .iter()
                .zip(&case.input.behaviors)
                .map(|(target, behavior)| (target.provider.clone(), *behavior))
                .collect(),
            calls: Arc::clone(&calls),
        });
        let dispatcher = Dispatcher::new(
            factory,
            ledger.clone(),
            Arc::new(FsSessionStore::new(root.path().join("sessions"))),
        );
        let result = dispatcher
            .dispatch(
                &TaskSpec::new("Return sample reply"),
                resolve(&case.input.targets),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let records = ledger.query(RunQuery::default()).await.unwrap();
        assert_eq!(records.len(), 1, "one persisted run for {}", case.id);
        assert_eq!(
            result.record.target,
            result.record.attempts.last().unwrap().target
        );
        assert_eq!(
            serde_json::to_value(&records[0]).unwrap(),
            serde_json::to_value(&result.record).unwrap()
        );
        let attempts:Vec<_>=result.record.attempts.iter().enumerate().map(|(i,attempt)| {
            let (status,reason,failed_over)=match &attempt.outcome {
                AttemptOutcome::Ok => ("success","",false),
                AttemptOutcome::Err {kind,failed_over,..} => (
                    if *kind==ErrorKind::Timeout {"timeout"} else {"error"},
                    match kind { ErrorKind::Transport=>"execution_error",ErrorKind::RateLimited=>"rate_limit",other=>other.as_str()},
                    *failed_over),
            };
            json!({"ordinal":i+1,"target":Coordinates::from_target(&attempt.target),"status":status,"reason":reason,"failed_over":failed_over})
        }).collect();
        let actual = json!({"status":result.record.status.as_str(),"output":result.output.unwrap_or_default(),"attempts":attempts,"calls":*calls.lock().unwrap()});
        assert_eq!(actual, case.rust_output, "Rust dispatch: {}", case.id);
        assert_disposition_truth(&case);
    }
}
