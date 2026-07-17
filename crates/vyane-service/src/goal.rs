//! Owner-frozen, read-only goal continuity projection.

use std::fmt;
use std::sync::Arc;

use serde::Serialize;
use vyane_goal::{
    GoalContinuityNextAction, GoalContinuityNextActionKind, GoalContinuityOperatorCommand,
    GoalContinuitySignalKind, GoalStore, GoalStoreError, SqliteGoalStore,
    project_continuity_next_action,
};

use crate::StoragePaths;

pub const GOAL_NEXT_ACTION_VIEW_SCHEMA: u32 = 1;

/// Allowlisted next-action projection for authenticated protocol front-ends.
///
/// It deliberately omits the durable owner, goal body, target, workdir,
/// approvals and all free-form reason text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalNextActionView {
    pub view_schema: u32,
    pub goal_id: String,
    pub goal_revision: u64,
    pub quota_event_id: String,
    pub action: GoalNextActionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<GoalOperatorCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepted_signals: Vec<GoalSignalKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_inputs: Vec<String>,
    pub reason_code: GoalNextReasonCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalNextActionKind {
    QueueApproval,
    DecideApproval,
    ExecuteApproval,
    RecordSignal,
    WaitForDependency,
    WaitForExecution,
    ResolveBlockedExecution,
    ManualDecision,
    ContinuityComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalOperatorCommand {
    ContinuityQueue,
    ContinuityDecide,
    ContinuityExecute,
    ContinuitySignal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalSignalKind {
    QuotaReset,
    ReviewChecksPassed,
    ReviewChecksFailed,
}

/// Closed explanation taxonomy. Unlike the core projection's local-operator
/// reason, these values can never contain persisted free-form text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalNextReasonCode {
    ApprovalRequired,
    ApprovalDecisionRequired,
    ApprovedExecutionReady,
    ExternalSignalRequired,
    DependencyPending,
    ExecutionInFlight,
    ExecutionBlocked,
    ManualDecisionRequired,
    ContinuityComplete,
}

/// Closed failure taxonomy suitable for protocol mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalReadError {
    InvalidGoalId,
    NotFound,
    ContinuityUnavailable,
    Unavailable,
}

impl fmt::Display for GoalReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidGoalId => "goal id is invalid",
            Self::NotFound => "goal was not found",
            Self::ContinuityUnavailable => "goal continuity is unavailable",
            Self::Unavailable => "goal read service is unavailable",
        })
    }
}

impl std::error::Error for GoalReadError {}

/// Explicitly opened goal reader with a durable owner frozen by authority.
///
/// No method accepts an owner, and constructing an ordinary [`crate::VyaneService`]
/// does not open this store.
#[derive(Clone)]
pub struct GoalReadService {
    store: Arc<dyn GoalStore>,
    owner: Arc<str>,
}

impl GoalReadService {
    pub(crate) fn open(paths: &StoragePaths, owner: Arc<str>) -> Result<Self, GoalReadError> {
        let store =
            SqliteGoalStore::open(paths.goal_db_path()).map_err(|_| GoalReadError::Unavailable)?;
        Ok(Self {
            store: Arc::new(store),
            owner,
        })
    }

    /// Read one revision-bound operator projection without mutating goal,
    /// approval, signal or pursuit state.
    pub fn continuity_next(&self, goal_id: &str) -> Result<GoalNextActionView, GoalReadError> {
        let snapshot = self
            .store
            .continuity_projection_snapshot(&self.owner, goal_id)
            .map_err(map_snapshot_error)?
            .ok_or(GoalReadError::NotFound)?;
        let action = project_continuity_next_action(&snapshot.goal, &snapshot.approvals)
            .map_err(map_projection_error)?;
        Ok(GoalNextActionView::from(action))
    }
}

impl fmt::Debug for GoalReadService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalReadService")
            .finish_non_exhaustive()
    }
}

fn map_snapshot_error(error: GoalStoreError) -> GoalReadError {
    match error {
        GoalStoreError::InvalidInput(_) => GoalReadError::InvalidGoalId,
        _ => GoalReadError::Unavailable,
    }
}

fn map_projection_error(error: GoalStoreError) -> GoalReadError {
    match error {
        GoalStoreError::InvalidInput(_) | GoalStoreError::InvalidStatus { .. } => {
            GoalReadError::ContinuityUnavailable
        }
        _ => GoalReadError::Unavailable,
    }
}

impl From<GoalContinuityNextAction> for GoalNextActionView {
    fn from(action: GoalContinuityNextAction) -> Self {
        let action_kind = GoalNextActionKind::from(action.action);
        Self {
            view_schema: GOAL_NEXT_ACTION_VIEW_SCHEMA,
            goal_id: action.goal_id,
            goal_revision: action.goal_revision,
            quota_event_id: action.quota_event_id,
            action: action_kind,
            command: action.command.map(GoalOperatorCommand::from),
            step_id: action.step_id,
            step_kind: action.step_kind,
            approval_id: action.approval_id,
            accepted_signals: action
                .accepted_signals
                .into_iter()
                .map(GoalSignalKind::from)
                .collect(),
            required_inputs: action.required_inputs,
            reason_code: GoalNextReasonCode::from(action_kind),
        }
    }
}

impl From<GoalContinuityNextActionKind> for GoalNextActionKind {
    fn from(value: GoalContinuityNextActionKind) -> Self {
        match value {
            GoalContinuityNextActionKind::QueueApproval => Self::QueueApproval,
            GoalContinuityNextActionKind::DecideApproval => Self::DecideApproval,
            GoalContinuityNextActionKind::ExecuteApproval => Self::ExecuteApproval,
            GoalContinuityNextActionKind::RecordSignal => Self::RecordSignal,
            GoalContinuityNextActionKind::WaitForDependency => Self::WaitForDependency,
            GoalContinuityNextActionKind::WaitForExecution => Self::WaitForExecution,
            GoalContinuityNextActionKind::ResolveBlockedExecution => Self::ResolveBlockedExecution,
            GoalContinuityNextActionKind::ManualDecision => Self::ManualDecision,
            GoalContinuityNextActionKind::ContinuityComplete => Self::ContinuityComplete,
        }
    }
}

impl From<GoalContinuityOperatorCommand> for GoalOperatorCommand {
    fn from(value: GoalContinuityOperatorCommand) -> Self {
        match value {
            GoalContinuityOperatorCommand::ContinuityQueue => Self::ContinuityQueue,
            GoalContinuityOperatorCommand::ContinuityDecide => Self::ContinuityDecide,
            GoalContinuityOperatorCommand::ContinuityExecute => Self::ContinuityExecute,
            GoalContinuityOperatorCommand::ContinuitySignal => Self::ContinuitySignal,
        }
    }
}

impl From<GoalContinuitySignalKind> for GoalSignalKind {
    fn from(value: GoalContinuitySignalKind) -> Self {
        match value {
            GoalContinuitySignalKind::QuotaReset => Self::QuotaReset,
            GoalContinuitySignalKind::ReviewChecksPassed => Self::ReviewChecksPassed,
            GoalContinuitySignalKind::ReviewChecksFailed => Self::ReviewChecksFailed,
        }
    }
}

impl From<GoalNextActionKind> for GoalNextReasonCode {
    fn from(value: GoalNextActionKind) -> Self {
        match value {
            GoalNextActionKind::QueueApproval => Self::ApprovalRequired,
            GoalNextActionKind::DecideApproval => Self::ApprovalDecisionRequired,
            GoalNextActionKind::ExecuteApproval => Self::ApprovedExecutionReady,
            GoalNextActionKind::RecordSignal => Self::ExternalSignalRequired,
            GoalNextActionKind::WaitForDependency => Self::DependencyPending,
            GoalNextActionKind::WaitForExecution => Self::ExecutionInFlight,
            GoalNextActionKind::ResolveBlockedExecution => Self::ExecutionBlocked,
            GoalNextActionKind::ManualDecision => Self::ManualDecisionRequired,
            GoalNextActionKind::ContinuityComplete => Self::ContinuityComplete,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use chrono::{TimeZone as _, Utc};
    use tempfile::TempDir;
    use vyane_config::ResolvedConfig;
    use vyane_goal::{
        GoalContinuityMode, GoalContinuityPolicy, GoalExecutionTarget, GoalQuotaEvent, NewGoal,
        TakeoverApprovalRequest, TakeoverBoundTarget, TakeoverDecision, TakeoverFinish,
        TakeoverRunStatus, TakeoverSandbox, apply_quota_handoff_events,
    };

    use super::*;
    use crate::{
        AuthenticatedPrincipal, LoadedConfig, OwnerContext, OwnerContextFactory,
        PrincipalAuthenticator, PrincipalOwnerResolver, VyaneService,
    };

    struct TestAuthenticator;

    impl PrincipalAuthenticator for TestAuthenticator {
        fn authenticate(&self, credential: &[u8]) -> anyhow::Result<String> {
            String::from_utf8(credential.to_vec()).map_err(anyhow::Error::from)
        }
    }

    struct TestOwnerResolver;

    impl PrincipalOwnerResolver for TestOwnerResolver {
        fn resolve_owner(&self, principal: &AuthenticatedPrincipal) -> anyhow::Result<String> {
            Ok(format!("owner:{}", principal.subject()))
        }
    }

    fn service(directory: &TempDir) -> VyaneService {
        VyaneService::from_loaded_with_paths(
            LoadedConfig {
                config: ResolvedConfig::default(),
                files: Vec::new(),
                secrets: BTreeMap::new(),
            },
            StoragePaths::from_data_dir(directory.path()),
        )
        .unwrap()
    }

    fn target(role: &str) -> GoalExecutionTarget {
        GoalExecutionTarget {
            provider: "provider".into(),
            protocol: "openai_chat".into(),
            harness: "harness".into(),
            model: "model".into(),
            profile: None,
            role: role.into(),
        }
    }

    fn create_continuity_goal(
        store: &SqliteGoalStore,
        owner: &str,
        id: &str,
        quota_event_id: &str,
    ) -> vyane_goal::GoalRecord {
        let mut goal = NewGoal::new("CANARY_TITLE", Utc.timestamp_opt(1_000, 0).unwrap());
        goal.id = Some(id.into());
        goal.description = "CANARY_DESCRIPTION".into();
        goal.continuity_policy = Some(GoalContinuityPolicy {
            mode: GoalContinuityMode::QuotaHandoff,
            primary: target("primary"),
            takeover: vec![target("takeover")],
            reviewer: None,
            resume_primary_after_reset: false,
            require_review_before_resume: false,
            wait_for_review_checks_before_resume: false,
        });
        store.create(owner, goal).unwrap();
        store
            .start(owner, id, Utc.timestamp_opt(1_001, 0).unwrap())
            .unwrap();
        apply_quota_handoff_events(
            store,
            owner,
            &[GoalQuotaEvent {
                event_id: quota_event_id.into(),
                goal_id: Some(id.into()),
                provider: "provider".into(),
                harness: "harness".into(),
                model: "model".into(),
                session_id: None,
                observed_at: Utc.timestamp_opt(1_002, 0).unwrap(),
                estimated_reset_at: None,
            }],
            Utc.timestamp_opt(1_003, 0).unwrap(),
        )
        .unwrap();
        store.get(owner, id).unwrap().unwrap()
    }

    #[test]
    fn goal_database_is_opened_only_after_explicit_reader_construction() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        assert!(!service.storage_paths().goal_db_path().exists());

        let _reader = service
            .goal_reader(OwnerContext::single_user_local())
            .unwrap();
        assert!(service.storage_paths().goal_db_path().is_file());
    }

    #[test]
    fn frozen_owner_hides_a_foreign_same_id_goal() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_continuity_goal(&store, "foreign", "shared", "foreign-quota");
        create_continuity_goal(&store, "local", "shared", "local-quota");

        let reader = service
            .goal_reader(OwnerContext::single_user_local())
            .unwrap();
        let view = reader.continuity_next("shared").unwrap();
        assert_eq!(view.quota_event_id, "local-quota");
        let encoded = serde_json::to_string(&view).unwrap();
        assert!(!encoded.contains("foreign"));
        assert!(!encoded.contains("CANARY_TITLE"));
        assert!(!encoded.contains("CANARY_DESCRIPTION"));
    }

    #[test]
    fn authenticated_principal_context_can_freeze_a_non_local_owner() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_continuity_goal(
            &store,
            "owner:principal-a",
            "principal-goal",
            "principal-quota",
        );
        create_continuity_goal(&store, "local", "principal-goal", "local-quota");
        let factory =
            OwnerContextFactory::new(Arc::new(TestAuthenticator), Arc::new(TestOwnerResolver));

        let reader = service
            .goal_reader(factory.authenticate(b"principal-a").unwrap())
            .unwrap();
        let view = reader.continuity_next("principal-goal").unwrap();

        assert_eq!(view.quota_event_id, "principal-quota");
    }

    #[test]
    fn repeated_projection_is_stable_and_performs_no_writes() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        let goal = create_continuity_goal(&store, "local", "stable", "stable-quota");
        let before_events = store.events("local", &goal.id).unwrap();
        let before_approvals = store
            .list_takeover_approvals("local", Some(&goal.id))
            .unwrap();
        let reader = service
            .goal_reader(OwnerContext::single_user_local())
            .unwrap();

        let first = reader.continuity_next(&goal.id).unwrap();
        let second = reader.continuity_next(&goal.id).unwrap();

        assert_eq!(first, second);
        assert_eq!(store.get("local", &goal.id).unwrap().unwrap(), goal);
        assert_eq!(store.events("local", &goal.id).unwrap(), before_events);
        assert_eq!(
            store
                .list_takeover_approvals("local", Some(&goal.id))
                .unwrap(),
            before_approvals
        );
    }

    #[test]
    fn dynamic_blocker_reason_is_replaced_by_a_closed_code() {
        let directory = TempDir::new().unwrap();
        let workdir = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        let goal = create_continuity_goal(&store, "local", "blocked", "blocked-quota");
        let state = goal.continuity_state.as_ref().unwrap();
        let step = state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.status == vyane_goal::GoalContinuityStepStatus::Ready)
            .unwrap();
        let approval = store
            .queue_takeover_approval(
                "local",
                &TakeoverApprovalRequest {
                    goal_id: goal.id.clone(),
                    step_id: step.id.clone(),
                    step_kind: step.kind.clone(),
                    quota_event_id: state.quota_event_id.clone(),
                    target: TakeoverBoundTarget::from_execution(step.target.as_ref().unwrap()),
                    workdir: workdir.path().canonicalize().unwrap(),
                    sandbox: TakeoverSandbox::ReadOnly,
                    timeout: Duration::from_secs(30),
                    goal_revision: goal.revision,
                    plan_snapshot: state.clone(),
                    upstream_approval_id: None,
                    upstream_run_id: None,
                    upstream_run_status: None,
                },
                Utc.timestamp_opt(1_004, 0).unwrap(),
            )
            .unwrap();
        store
            .decide_takeover_approval(
                "local",
                &approval.approval_id,
                TakeoverDecision::Approve,
                "operator",
                None,
                Utc.timestamp_opt(1_005, 0).unwrap(),
            )
            .unwrap();
        store
            .consume_takeover_approval(
                "local",
                &approval.approval_id,
                Utc.timestamp_opt(1_006, 0).unwrap(),
            )
            .unwrap();
        store
            .finish_takeover_approval(
                "local",
                &approval.approval_id,
                &TakeoverFinish {
                    run_id: Some("run-blocked".into()),
                    run_status: TakeoverRunStatus::Error,
                    detail: "CANARY_DYNAMIC_BLOCKER /private/path".into(),
                },
                Utc.timestamp_opt(1_007, 0).unwrap(),
            )
            .unwrap();

        let reader = service
            .goal_reader(OwnerContext::single_user_local())
            .unwrap();
        let view = reader.continuity_next(&goal.id).unwrap();
        assert_eq!(view.reason_code, GoalNextReasonCode::ExecutionBlocked);
        let encoded = serde_json::to_string(&view).unwrap();
        assert!(!encoded.contains("CANARY_DYNAMIC_BLOCKER"));
        assert!(!encoded.contains("private/path"));
    }

    #[test]
    fn failures_use_the_closed_read_taxonomy() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let reader = service
            .goal_reader(OwnerContext::single_user_local())
            .unwrap();
        assert_eq!(
            reader.continuity_next(""),
            Err(GoalReadError::InvalidGoalId)
        );
        assert_eq!(
            reader.continuity_next("missing"),
            Err(GoalReadError::NotFound)
        );

        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        let mut goal = NewGoal::new("no continuity", Utc.timestamp_opt(2_000, 0).unwrap());
        goal.id = Some("plain".into());
        store.create("local", goal).unwrap();
        assert_eq!(
            reader.continuity_next("plain"),
            Err(GoalReadError::ContinuityUnavailable)
        );
    }
}
