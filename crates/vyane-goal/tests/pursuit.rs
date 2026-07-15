#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
#[cfg(unix)]
use std::{fs, os::unix::fs::PermissionsExt as _};

use async_trait::async_trait;
use chrono::Utc;
use tempfile::TempDir;
use vyane_goal::{
    AcceptanceCriterion, AcceptanceVerifier, GoalEventKind, GoalPursuer, GoalSegmentRuntime,
    GoalStatus, GoalStore, GoalStoreError, NewGoal, PursuitCheckpointStatus, PursuitConfig,
    PursuitSegmentRequest, PursuitSegmentResult, PursuitSegmentStatus, PursuitStatus,
    SqliteGoalStore,
};

const OWNER: &str = "owner-a";

type Handler = dyn FnMut(&PursuitSegmentRequest) -> PursuitSegmentResult + Send;

struct FakeRuntime {
    requests: Mutex<Vec<PursuitSegmentRequest>>,
    handler: Mutex<Box<Handler>>,
}

impl FakeRuntime {
    fn new(
        handler: impl FnMut(&PursuitSegmentRequest) -> PursuitSegmentResult + Send + 'static,
    ) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            handler: Mutex::new(Box::new(handler)),
        }
    }

    fn call_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }

    fn requests(&self) -> Vec<PursuitSegmentRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

#[async_trait]
impl GoalSegmentRuntime for FakeRuntime {
    async fn run_segment(&self, request: PursuitSegmentRequest) -> PursuitSegmentResult {
        let result = (self.handler.lock().expect("handler lock"))(&request);
        self.requests.lock().expect("requests lock").push(request);
        result
    }
}

struct HangingRuntime;

#[async_trait]
impl GoalSegmentRuntime for HangingRuntime {
    async fn run_segment(&self, _request: PursuitSegmentRequest) -> PursuitSegmentResult {
        tokio::time::sleep(Duration::from_secs(5)).await;
        PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: None,
        }
    }
}

struct CapturingHangingRuntime {
    timeouts: Mutex<Vec<Duration>>,
}

#[async_trait]
impl GoalSegmentRuntime for CapturingHangingRuntime {
    async fn run_segment(&self, request: PursuitSegmentRequest) -> PursuitSegmentResult {
        self.timeouts
            .lock()
            .expect("timeouts lock")
            .push(request.timeout);
        tokio::time::sleep(Duration::from_secs(5)).await;
        PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: None,
        }
    }
}

fn fixture() -> (TempDir, SqliteGoalStore) {
    let directory = TempDir::new().expect("tempdir");
    let store = SqliteGoalStore::open(directory.path().join("goals.sqlite3")).expect("store");
    (directory, store)
}

fn running_goal(store: &SqliteGoalStore, id: &str, criteria: Vec<AcceptanceCriterion>) {
    let mut goal = NewGoal::new("Pursue goal", Utc::now());
    goal.id = Some(id.to_string());
    goal.description = "Make the acceptance criteria true.".into();
    goal.acceptance_criteria = criteria;
    store.create(OWNER, goal).expect("create");
    store
        .claim(OWNER, id, "pursuer", 60, Utc::now())
        .expect("claim");
}

fn config(directory: &TempDir, max_segments: u16, max_failures: u16) -> PursuitConfig {
    PursuitConfig {
        workdir: directory.path().canonicalize().expect("canonical workdir"),
        runtime: "fake".into(),
        worker_id: "pursuer".into(),
        overall_timeout: Duration::from_secs(5),
        segment_timeout: Duration::from_secs(1),
        max_segments,
        max_failures,
    }
}

#[tokio::test]
async fn segment_then_reverify_completes_without_runtime_owned_done() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "achieve",
        vec![AcceptanceCriterion::new("custom", "cmd:test -f done.txt")],
    );
    let workdir = directory.path().to_path_buf();
    let runtime = FakeRuntime::new(move |_| {
        std::fs::write(workdir.join("done.txt"), b"ok").expect("write marker");
        PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: Some("run-1".into()),
        }
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();
    let pursuer = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2)).unwrap();

    let outcome = pursuer.pursue(OWNER, "achieve").await.unwrap();

    assert_eq!(outcome.status, PursuitStatus::Achieved);
    assert_eq!(outcome.final_goal_status, GoalStatus::Completed);
    assert_eq!(outcome.segments_started, 1);
    assert_eq!(runtime.call_count(), 1);
    let requests = runtime.requests();
    assert_eq!(requests[0].goal_id, "achieve");
    assert_eq!(requests[0].segment_index, 1);
    assert_eq!(
        requests[0].workdir,
        directory.path().canonicalize().unwrap()
    );
    assert_eq!(requests[0].timeout, Duration::from_secs(1));
    assert_eq!(requests[0].runtime, "fake");
    assert_eq!(requests[0].verification.goal_id, "achieve");
    assert_eq!(store.verifications(OWNER, "achieve").unwrap().len(), 2);
    let checkpoint = store
        .pursuit_checkpoint(OWNER, "achieve")
        .unwrap()
        .expect("achieved checkpoint");
    assert_eq!(checkpoint.status, PursuitCheckpointStatus::Achieved);
    assert_eq!(checkpoint.segments_started, 1);
    assert_eq!(checkpoint.segments_completed, 1);
    assert_eq!(checkpoint.consecutive_failures, 0);
    assert_eq!(checkpoint.last_run_id.as_deref(), Some("run-1"));
    assert!(checkpoint.last_verification_id.is_some());
    let events = store.events(OWNER, "achieve").unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.stage.as_deref() == Some("acceptance.verify"))
            .count(),
        2
    );
    assert!(events.iter().any(|event| {
        event.stage.as_deref() == Some("pursuit.segment.completed")
            && event
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("run-1"))
    }));
}

#[tokio::test]
async fn paused_checkpoint_survives_reopen_and_new_lease_continues_lifetime_budget() {
    let directory = TempDir::new().expect("tempdir");
    let database = directory.path().join("goals.sqlite3");
    let started_at;
    let first_revision;
    let first_generation;

    {
        let store = SqliteGoalStore::open(&database).expect("store");
        running_goal(
            &store,
            "restart",
            vec![AcceptanceCriterion::new("custom", "cmd:false")],
        );
        let runtime = FakeRuntime::new(|_| PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: Some("run-1".into()),
        });
        let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

        let first = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 1, 2))
            .unwrap()
            .pursue(OWNER, "restart")
            .await
            .unwrap();

        assert_eq!(first.status, PursuitStatus::Paused);
        assert_eq!(first.reason, "pursuit max segments reached");
        assert_eq!(first.segments_started, 1);
        let checkpoint = store
            .pursuit_checkpoint(OWNER, "restart")
            .unwrap()
            .expect("first checkpoint");
        assert_eq!(checkpoint.status, PursuitCheckpointStatus::Paused);
        assert_eq!(checkpoint.last_run_id.as_deref(), Some("run-1"));
        started_at = checkpoint.started_at;
        first_revision = checkpoint.checkpoint_revision;
        first_generation = checkpoint.claim_generation;
    }

    let store = SqliteGoalStore::open(&database).expect("reopened store");
    store
        .resume(OWNER, "restart", None, Utc::now())
        .expect("resume");
    let claimed = store
        .claim(OWNER, "restart", "replacement", 60, Utc::now())
        .expect("replacement claim");
    assert!(claimed.claim_generation > first_generation);
    let runtime = FakeRuntime::new(|_| PursuitSegmentResult {
        status: PursuitSegmentStatus::Success,
        run_id: Some("run-2".into()),
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();
    let mut resumed_config = config(&directory, 2, 2);
    resumed_config.worker_id = "replacement".into();

    let resumed = GoalPursuer::new(&store, &runtime, &verifier, resumed_config)
        .unwrap()
        .pursue(OWNER, "restart")
        .await
        .unwrap();

    assert_eq!(resumed.status, PursuitStatus::Paused);
    assert_eq!(resumed.reason, "pursuit max segments reached");
    assert_eq!(resumed.segments_started, 2);
    assert_eq!(resumed.segments_completed, 2);
    assert_eq!(runtime.call_count(), 1);
    assert_eq!(runtime.requests()[0].segment_index, 2);
    let checkpoint = store
        .pursuit_checkpoint(OWNER, "restart")
        .unwrap()
        .expect("resumed checkpoint");
    assert_eq!(checkpoint.status, PursuitCheckpointStatus::Paused);
    assert_eq!(checkpoint.started_at, started_at);
    assert!(checkpoint.checkpoint_revision > first_revision);
    assert_eq!(checkpoint.claim_generation, claimed.claim_generation);
    assert_eq!(checkpoint.worker_id, "replacement");
    assert_eq!(checkpoint.segments_started, 2);
    assert_eq!(checkpoint.segments_completed, 2);
    assert_eq!(checkpoint.last_run_id.as_deref(), Some("run-2"));
}

#[tokio::test]
async fn manual_and_missing_acceptance_pause_without_runtime() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "manual",
        vec![AcceptanceCriterion::new("manual-confirm", "operator")],
    );
    running_goal(&store, "missing", Vec::new());
    let runtime = FakeRuntime::new(|_| panic!("runtime must not run"));
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let manual = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "manual")
        .await
        .unwrap();
    let missing = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "missing")
        .await
        .unwrap();

    assert_eq!(manual.status, PursuitStatus::Paused);
    assert_eq!(manual.reason, "manual confirmation required");
    assert_eq!(missing.status, PursuitStatus::Paused);
    assert_eq!(missing.reason, "acceptance criteria required");
    assert_eq!(runtime.call_count(), 0);
}

#[tokio::test]
async fn segment_and_failure_budgets_pause_deterministically() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "segments",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    running_goal(
        &store,
        "failures",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    let successful = FakeRuntime::new(|_| PursuitSegmentResult {
        status: PursuitSegmentStatus::Success,
        run_id: None,
    });
    let failed = FakeRuntime::new(|_| PursuitSegmentResult {
        status: PursuitSegmentStatus::Error,
        run_id: None,
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let segments = GoalPursuer::new(&store, &successful, &verifier, config(&directory, 2, 2))
        .unwrap()
        .pursue(OWNER, "segments")
        .await
        .unwrap();
    let failures = GoalPursuer::new(&store, &failed, &verifier, config(&directory, 3, 1))
        .unwrap()
        .pursue(OWNER, "failures")
        .await
        .unwrap();

    assert_eq!(segments.status, PursuitStatus::Paused);
    assert_eq!(segments.reason, "pursuit max segments reached");
    assert_eq!(segments.segments_completed, 2);
    assert_eq!(failures.status, PursuitStatus::Paused);
    assert_eq!(failures.reason, "pursuit max failures reached");
    assert_eq!(failures.segments_completed, 1);
}

#[tokio::test]
async fn verifier_and_runtime_failures_each_consume_one_failure_slot() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "mixed-failures",
        vec![AcceptanceCriterion::new(
            "custom",
            "cmd:definitely-not-a-real-command",
        )],
    );
    let runtime = FakeRuntime::new(|_| PursuitSegmentResult {
        status: PursuitSegmentStatus::Error,
        run_id: None,
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let outcome = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "mixed-failures")
        .await
        .unwrap();

    assert_eq!(outcome.status, PursuitStatus::Paused);
    assert_eq!(outcome.reason, "pursuit max failures reached");
    assert_eq!(outcome.consecutive_failures, 2);
    assert_eq!(outcome.segments_completed, 1);
}

#[tokio::test]
async fn success_resets_only_runtime_failures_not_verifier_errors() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "runtime-recovers",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    running_goal(
        &store,
        "verifier-errors",
        vec![AcceptanceCriterion::new(
            "custom",
            "cmd:definitely-not-a-real-command",
        )],
    );
    let mut calls = 0;
    let recovering = FakeRuntime::new(move |_| {
        let status = if calls == 0 {
            PursuitSegmentStatus::Error
        } else {
            PursuitSegmentStatus::Success
        };
        calls += 1;
        PursuitSegmentResult {
            status,
            run_id: None,
        }
    });
    let successful = FakeRuntime::new(|_| PursuitSegmentResult {
        status: PursuitSegmentStatus::Success,
        run_id: None,
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let recovered = GoalPursuer::new(&store, &recovering, &verifier, config(&directory, 2, 2))
        .unwrap()
        .pursue(OWNER, "runtime-recovers")
        .await
        .unwrap();
    assert_eq!(recovered.reason, "pursuit max segments reached");
    assert_eq!(recovered.consecutive_failures, 0);
    assert_eq!(recovering.call_count(), 2);

    let retained = GoalPursuer::new(&store, &successful, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "verifier-errors")
        .await
        .unwrap();
    assert_eq!(retained.reason, "pursuit max failures reached");
    assert_eq!(retained.consecutive_failures, 2);
    assert_eq!(successful.call_count(), 1);
}

#[tokio::test]
async fn overall_timeout_and_cancellation_pause_without_another_segment() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "overall-timeout",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    running_goal(
        &store,
        "cancelled",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();
    let mut timeout_config = config(&directory, 3, 2);
    timeout_config.overall_timeout = Duration::from_millis(200);
    let slow = CapturingHangingRuntime {
        timeouts: Mutex::new(Vec::new()),
    };
    let timed_out = GoalPursuer::new(&store, &slow, &verifier, timeout_config)
        .unwrap()
        .pursue(OWNER, "overall-timeout")
        .await
        .unwrap();
    assert_eq!(timed_out.status, PursuitStatus::Paused);
    assert_eq!(timed_out.reason, "pursuit overall timeout");
    assert_eq!(timed_out.segments_started, 1);
    {
        let timeouts = slow.timeouts.lock().expect("timeouts lock");
        assert_eq!(timeouts.len(), 1);
        assert!(timeouts[0] <= Duration::from_millis(200));
        assert!(timeouts[0] < Duration::from_secs(1));
    }

    let cancelled_runtime = FakeRuntime::new(|_| PursuitSegmentResult {
        status: PursuitSegmentStatus::Cancelled,
        run_id: None,
    });
    let cancelled = GoalPursuer::new(
        &store,
        &cancelled_runtime,
        &verifier,
        config(&directory, 3, 3),
    )
    .unwrap()
    .pursue(OWNER, "cancelled")
    .await
    .unwrap();
    assert_eq!(cancelled.status, PursuitStatus::Paused);
    assert_eq!(cancelled.reason, "pursuit cancelled");
    assert_eq!(cancelled.segments_started, 1);
    assert_eq!(cancelled_runtime.call_count(), 1);
}

#[tokio::test]
async fn active_lease_mismatch_rejects_before_verifier_or_runtime() {
    let (directory, store) = fixture();
    let mut goal = NewGoal::new("Leased", Utc::now());
    goal.id = Some("leased".into());
    goal.acceptance_criteria = vec![AcceptanceCriterion::new("custom", "cmd:false")];
    store.create(OWNER, goal).unwrap();
    store
        .claim(OWNER, "leased", "worker-a", 60, Utc::now())
        .unwrap();
    let runtime = FakeRuntime::new(|_| panic!("runtime must not run"));
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();
    let mut pursuit_config = config(&directory, 3, 2);
    pursuit_config.worker_id = "worker-b".into();
    let pursuer = GoalPursuer::new(&store, &runtime, &verifier, pursuit_config).unwrap();

    assert!(matches!(
        pursuer.pursue(OWNER, "leased").await,
        Err(GoalStoreError::LeaseHeld { .. })
    ));
    assert_eq!(runtime.call_count(), 0);
    assert!(store.verifications(OWNER, "leased").unwrap().is_empty());
}

#[tokio::test]
async fn runtime_timeout_and_external_pause_stop_without_extra_segments() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "timeout",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    running_goal(
        &store,
        "external",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();
    let mut timeout_config = config(&directory, 3, 1);
    timeout_config.segment_timeout = Duration::from_millis(20);
    let timeout = GoalPursuer::new(&store, &HangingRuntime, &verifier, timeout_config)
        .unwrap()
        .pursue(OWNER, "timeout")
        .await
        .unwrap();
    assert_eq!(timeout.status, PursuitStatus::Paused);
    assert_eq!(timeout.reason, "pursuit max failures reached");
    assert_eq!(timeout.segments_started, 1);

    let other_store = store.clone();
    let external = FakeRuntime::new(move |_| {
        other_store
            .pause(
                OWNER,
                "external",
                Some("pursuer"),
                Some("external pause"),
                Utc::now(),
            )
            .expect("external pause");
        PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: None,
        }
    });
    let stopped = GoalPursuer::new(&store, &external, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "external")
        .await
        .unwrap();
    assert_eq!(stopped.status, PursuitStatus::Stopped);
    assert_eq!(stopped.reason, "goal status is paused");
    assert_eq!(external.call_count(), 1);
}

#[test]
fn pursuit_config_rejects_each_invalid_field() {
    let (directory, store) = fixture();
    let valid = config(&directory, 3, 2);
    let relative = PathBuf::from("relative");
    let missing = directory.path().join("missing");
    let regular_file = directory.path().join("not-a-directory");
    std::fs::write(&regular_file, b"file").expect("write regular file");
    let cases = [
        PursuitConfig {
            workdir: relative,
            ..valid.clone()
        },
        PursuitConfig {
            workdir: missing,
            ..valid.clone()
        },
        PursuitConfig {
            workdir: regular_file,
            ..valid.clone()
        },
        PursuitConfig {
            runtime: " ".into(),
            ..valid.clone()
        },
        PursuitConfig {
            runtime: "x".repeat(257),
            ..valid.clone()
        },
        PursuitConfig {
            worker_id: " ".into(),
            ..valid.clone()
        },
        PursuitConfig {
            overall_timeout: Duration::ZERO,
            ..valid.clone()
        },
        PursuitConfig {
            overall_timeout: Duration::from_secs(86_401),
            ..valid.clone()
        },
        PursuitConfig {
            segment_timeout: Duration::ZERO,
            ..valid.clone()
        },
        PursuitConfig {
            segment_timeout: Duration::from_secs(3_601),
            ..valid.clone()
        },
        PursuitConfig {
            max_segments: 0,
            ..valid.clone()
        },
        PursuitConfig {
            max_segments: 65,
            ..valid.clone()
        },
        PursuitConfig {
            max_failures: 0,
            ..valid.clone()
        },
        PursuitConfig {
            max_failures: 17,
            ..valid.clone()
        },
    ];

    for case in cases {
        assert!(matches!(
            case.validate(),
            Err(GoalStoreError::InvalidInput(_))
        ));
    }

    let runtime = FakeRuntime::new(|_| panic!("invalid config must not run"));
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();
    let invalid = PursuitConfig {
        max_segments: 0,
        ..valid
    };
    assert!(matches!(
        GoalPursuer::new(&store, &runtime, &verifier, invalid),
        Err(GoalStoreError::InvalidInput(_))
    ));
    PursuitConfig {
        overall_timeout: Duration::from_secs(86_400),
        segment_timeout: Duration::from_secs(3_600),
        max_segments: 64,
        max_failures: 16,
        ..config(&directory, 3, 2)
    }
    .validate()
    .expect("maximum inclusive boundaries are valid");
}

#[tokio::test]
async fn external_lease_reclaim_stops_the_pursuit() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "reclaimed",
        vec![AcceptanceCriterion::new("custom", "cmd:false")],
    );
    let other_store = store.clone();
    let runtime = FakeRuntime::new(move |_| {
        other_store
            .reclaim(
                OWNER,
                "reclaimed",
                "replacement",
                60,
                Utc::now() + chrono::Duration::days(2),
            )
            .expect("reclaim lease");
        PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: None,
        }
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let outcome = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "reclaimed")
        .await
        .unwrap();

    assert_eq!(outcome.status, PursuitStatus::Stopped);
    assert_eq!(outcome.reason, "goal lease changed outside pursuit");
    assert_eq!(runtime.call_count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn external_pause_during_verification_stops_before_persistence_or_runtime() {
    let (directory, store) = fixture();
    let verifier_script = directory.path().join("slow-verifier");
    fs::write(
        &verifier_script,
        "#!/bin/sh\n: > \"$PWD/verifier-started\"\n/bin/sleep 1\nexit 1\n",
    )
    .expect("write verifier script");
    fs::set_permissions(&verifier_script, fs::Permissions::from_mode(0o755))
        .expect("chmod verifier script");
    running_goal(
        &store,
        "verify-pause",
        vec![AcceptanceCriterion::new(
            "custom",
            format!("cmd:{}", verifier_script.display()),
        )],
    );
    let marker = directory.path().join("verifier-started");
    let other_store = store.clone();
    let pauser = std::thread::spawn(move || {
        for _ in 0..1_000 {
            if marker.exists() {
                other_store
                    .pause(
                        OWNER,
                        "verify-pause",
                        Some("pursuer"),
                        Some("external pause"),
                        Utc::now(),
                    )
                    .expect("pause during verification");
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("verifier did not start");
    });
    let runtime = FakeRuntime::new(|_| panic!("runtime must not run"));
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let outcome = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "verify-pause")
        .await
        .unwrap();
    pauser.join().expect("join pauser");

    assert_eq!(outcome.status, PursuitStatus::Stopped);
    assert_eq!(outcome.reason, "goal status is paused");
    assert_eq!(outcome.segments_started, 0);
    assert_eq!(runtime.call_count(), 0);
    assert!(
        store
            .verifications(OWNER, "verify-pause")
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn multiple_criteria_are_satisfied_once_across_fresh_segments() {
    let (directory, store) = fixture();
    running_goal(
        &store,
        "progressive",
        vec![
            AcceptanceCriterion::new("custom", "cmd:test -f first.txt"),
            AcceptanceCriterion::new("custom", "cmd:test -f second.txt"),
        ],
    );
    let workdir = directory.path().to_path_buf();
    let runtime = FakeRuntime::new(move |request| {
        let marker = if request.segment_index == 1 {
            "first.txt"
        } else {
            "second.txt"
        };
        std::fs::write(workdir.join(marker), b"ok").expect("write marker");
        PursuitSegmentResult {
            status: PursuitSegmentStatus::Success,
            run_id: None,
        }
    });
    let verifier = AcceptanceVerifier::new(directory.path(), Duration::from_secs(1)).unwrap();

    let outcome = GoalPursuer::new(&store, &runtime, &verifier, config(&directory, 3, 2))
        .unwrap()
        .pursue(OWNER, "progressive")
        .await
        .unwrap();

    assert_eq!(outcome.status, PursuitStatus::Achieved);
    assert_eq!(outcome.segments_completed, 2);
    let completed = store.get(OWNER, "progressive").unwrap().unwrap();
    assert!(
        completed
            .acceptance_criteria
            .iter()
            .all(|criterion| criterion.satisfied_at.is_some())
    );
    assert_eq!(
        store
            .events(OWNER, "progressive")
            .unwrap()
            .iter()
            .filter(|event| event.kind == GoalEventKind::CriterionSatisfied)
            .count(),
        2
    );
}
