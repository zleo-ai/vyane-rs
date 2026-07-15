use chrono::{TimeZone as _, Utc};
use tempfile::TempDir;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityPolicy, GoalContinuityStatus, GoalContinuityStepStatus,
    GoalExecutionTarget, GoalQuotaEvent, GoalStore, NewGoal, SqliteGoalStore,
    apply_quota_handoff_events,
};

const OWNER: &str = "local";

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).unwrap()
}

fn target(role: &str, provider: &str, harness: &str, model: &str) -> GoalExecutionTarget {
    GoalExecutionTarget {
        provider: provider.into(),
        protocol: "openai_responses".into(),
        harness: harness.into(),
        model: model.into(),
        profile: None,
        role: role.into(),
    }
}

fn policy(with_takeover: bool) -> GoalContinuityPolicy {
    let reviewer = target("reviewer", "primary-provider", "codex-cli", "primary-model");
    GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary", "primary-provider", "codex-cli", "primary-model"),
        takeover: if with_takeover {
            vec![target(
                "takeover",
                "backup-provider",
                "claude-code",
                "backup-model",
            )]
        } else {
            Vec::new()
        },
        reviewer: Some(reviewer),
        resume_primary_after_reset: true,
        require_review_before_resume: true,
    }
}

fn store(directory: &TempDir) -> SqliteGoalStore {
    SqliteGoalStore::open(directory.path().join("goals.sqlite3")).expect("open goal store")
}

fn create_started(store: &SqliteGoalStore, id: &str, policy: GoalContinuityPolicy) {
    let mut goal = NewGoal::new("continue safely", at(1_000));
    goal.id = Some(id.into());
    goal.continuity_policy = Some(policy);
    store.create(OWNER, goal).expect("create goal");
    store.start(OWNER, id, at(1_001)).expect("start goal");
}

fn quota(goal_id: Option<&str>, event_id: &str) -> GoalQuotaEvent {
    GoalQuotaEvent {
        event_id: event_id.into(),
        goal_id: goal_id.map(str::to_string),
        provider: "primary-provider".into(),
        harness: "codex-cli".into(),
        model: "primary-model".into(),
        session_id: Some("opaque-session".into()),
        observed_at: at(1_100),
        estimated_reset_at: Some(at(2_000)),
    }
}

#[test]
fn quota_event_persists_visible_handoff_plan_once() {
    let directory = TempDir::new().expect("temporary directory");
    let store = store(&directory);
    create_started(&store, "goal-a", policy(true));

    let actions = apply_quota_handoff_events(
        &store,
        OWNER,
        &[quota(Some("goal-a"), "quota-a")],
        at(1_101),
    )
    .expect("apply quota event");

    assert_eq!(actions.len(), 1);
    let state = &actions[0].state;
    assert_eq!(state.state, GoalContinuityStatus::TakeoverReady);
    assert_eq!(state.handoff_plan.next_ready_step, "takeover");
    assert_eq!(state.handoff_plan.steps.len(), 3);
    assert_eq!(
        state.handoff_plan.steps[0].status,
        GoalContinuityStepStatus::Ready
    );
    assert_eq!(
        state.handoff_plan.steps[2].wait_for,
        ["quota_reset", "review_takeover"]
    );
    let revision = store
        .get(OWNER, "goal-a")
        .expect("read goal")
        .expect("goal exists")
        .revision;
    assert_eq!(
        apply_quota_handoff_events(
            &store,
            OWNER,
            &[quota(Some("goal-a"), "quota-a")],
            at(1_102),
        )
        .expect("replay quota event"),
        []
    );
    assert_eq!(
        store
            .get(OWNER, "goal-a")
            .expect("read replayed goal")
            .expect("replayed goal exists")
            .revision,
        revision
    );
    let events = store.events(OWNER, "goal-a").expect("read goal events");
    let event = events.last().expect("quota progress event");
    assert_eq!(event.stage.as_deref(), Some("quota_handoff"));
    assert_eq!(event.detail.as_deref(), Some("quota event quota-a"));
}

#[test]
fn unmatched_and_unstarted_goals_are_not_mutated() {
    let directory = TempDir::new().expect("temporary directory");
    let store = store(&directory);
    create_started(&store, "running", policy(true));
    let mut queued = NewGoal::new("queued", at(1_000));
    queued.id = Some("queued".into());
    queued.continuity_policy = Some(policy(true));
    store.create(OWNER, queued).expect("create queued goal");
    let mut mismatch = quota(None, "quota-mismatch");
    mismatch.model = "another-model".into();

    assert!(
        apply_quota_handoff_events(&store, OWNER, &[mismatch], at(1_101))
            .expect("apply unmatched event")
            .is_empty()
    );
    assert!(
        apply_quota_handoff_events(
            &store,
            OWNER,
            &[quota(Some("queued"), "quota-queued")],
            at(1_101),
        )
        .expect("apply queued event")
        .is_empty()
    );
}

#[test]
fn missing_takeover_records_a_manual_blocker_without_execution() {
    let directory = TempDir::new().expect("temporary directory");
    let store = store(&directory);
    create_started(&store, "manual", policy(false));

    let actions = apply_quota_handoff_events(
        &store,
        OWNER,
        &[quota(Some("manual"), "quota-manual")],
        at(1_101),
    )
    .expect("apply manual handoff event");

    let state = &actions[0].state;
    assert_eq!(state.state, GoalContinuityStatus::BlockedNoTakeover);
    assert_eq!(state.handoff_plan.next_ready_step, "manual_decision");
    assert_eq!(state.handoff_plan.steps.len(), 1);
    assert_eq!(
        state.handoff_plan.steps[0].status,
        GoalContinuityStepStatus::Ready
    );
    assert!(state.handoff_plan.steps[0].target.is_none());
}
