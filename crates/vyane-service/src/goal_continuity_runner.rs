//! Separately authenticated, bounded continuity next-action runner.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt as _, stream};
use serde::Serialize;

use crate::{
    GOAL_NEXT_ACTION_VIEW_SCHEMA, GoalNextActionKind, GoalNextActionView, GoalOperatorCommand,
    GoalReadError, GoalReadService, OwnerContext, OwnerContextFactory,
};

pub const MAX_GOAL_CONTINUITY_GOALS: usize = 64;
pub const MAX_GOAL_CONTINUITY_CONCURRENCY: usize = 16;
const MAX_GOAL_CONTINUITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Credentials are purpose-separated. A protocol assembler must authenticate
/// each enabled capability through its corresponding factory.
pub struct GoalContinuityRunnerCredentials<'a> {
    pub read: &'a [u8],
    pub queue: Option<&'a [u8]>,
    pub execute: Option<&'a [u8]>,
}

/// Purpose-separated authentication policy for one runner assembly.
#[derive(Clone)]
pub struct GoalContinuityRunnerAuthorityFactory {
    read: OwnerContextFactory,
    queue: Option<OwnerContextFactory>,
    execute: Option<OwnerContextFactory>,
}

impl GoalContinuityRunnerAuthorityFactory {
    #[must_use]
    pub fn new(
        read: OwnerContextFactory,
        queue: Option<OwnerContextFactory>,
        execute: Option<OwnerContextFactory>,
    ) -> Self {
        Self {
            read,
            queue,
            execute,
        }
    }

    /// Authenticate every supplied purpose independently and require all
    /// enabled capabilities to resolve to the same durable owner.
    pub fn authenticate(
        &self,
        credentials: GoalContinuityRunnerCredentials<'_>,
    ) -> Result<GoalContinuityRunnerAuthority, GoalContinuityRunnerAuthorityError> {
        let read = self
            .read
            .authenticate(credentials.read)
            .map_err(|_| GoalContinuityRunnerAuthorityError::AuthenticationFailed)?;
        let queue = authenticate_optional(&self.queue, credentials.queue)?;
        let execute = authenticate_optional(&self.execute, credentials.execute)?;
        let owner = read.owner();
        if queue
            .as_ref()
            .is_some_and(|context| context.owner() != owner)
            || execute
                .as_ref()
                .is_some_and(|context| context.owner() != owner)
        {
            return Err(GoalContinuityRunnerAuthorityError::OwnerMismatch);
        }
        Ok(GoalContinuityRunnerAuthority {
            read,
            queue,
            execute,
        })
    }
}

impl fmt::Debug for GoalContinuityRunnerAuthorityFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalContinuityRunnerAuthorityFactory")
            .finish_non_exhaustive()
    }
}

fn authenticate_optional(
    factory: &Option<OwnerContextFactory>,
    credential: Option<&[u8]>,
) -> Result<Option<OwnerContext>, GoalContinuityRunnerAuthorityError> {
    match (factory, credential) {
        (None, None) => Ok(None),
        (Some(factory), Some(credential)) => factory
            .authenticate(credential)
            .map(Some)
            .map_err(|_| GoalContinuityRunnerAuthorityError::AuthenticationFailed),
        _ => Err(GoalContinuityRunnerAuthorityError::CapabilityMismatch),
    }
}

/// Opaque authenticated authority. It is deliberately not serializable or
/// cloneable and contains no bearer or principal value.
pub struct GoalContinuityRunnerAuthority {
    pub(crate) read: OwnerContext,
    pub(crate) queue: Option<OwnerContext>,
    pub(crate) execute: Option<OwnerContext>,
}

impl fmt::Debug for GoalContinuityRunnerAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalContinuityRunnerAuthority")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalContinuityRunnerAuthorityError {
    AuthenticationFailed,
    CapabilityMismatch,
    OwnerMismatch,
}

impl fmt::Display for GoalContinuityRunnerAuthorityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AuthenticationFailed => "goal continuity runner authentication failed",
            Self::CapabilityMismatch => "goal continuity runner capability is misconfigured",
            Self::OwnerMismatch => "goal continuity runner authorities have different owners",
        })
    }
}

impl std::error::Error for GoalContinuityRunnerAuthorityError {}

/// Exact projection boundary supplied to a mutation or execution port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalContinuityActionFence {
    pub goal_id: String,
    pub goal_revision: u64,
    pub quota_event_id: String,
    pub step_id: String,
    pub step_kind: String,
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalContinuityPortResult {
    Applied,
    Unchanged,
    Rejected,
    Unavailable,
}

#[async_trait]
pub trait GoalContinuityProjectionReader: Send + Sync {
    async fn continuity_next(&self, goal_id: &str) -> Result<GoalNextActionView, GoalReadError>;
}

#[async_trait]
impl GoalContinuityProjectionReader for GoalReadService {
    async fn continuity_next(&self, goal_id: &str) -> Result<GoalNextActionView, GoalReadError> {
        let reader = self.clone();
        let goal_id = goal_id.to_owned();
        tokio::task::spawn_blocking(move || GoalReadService::continuity_next(&reader, &goal_id))
            .await
            .map_err(|_| GoalReadError::Unavailable)?
    }
}

/// Queue port contract. Implementations must re-read the exact fence and call
/// the existing durable approval queue transaction; this port never decides.
#[async_trait]
pub trait GoalContinuityQueuePort: Send + Sync {
    async fn queue(&self, fence: GoalContinuityActionFence) -> GoalContinuityPortResult;
}

/// Approved one-shot execution port. Implementations must atomically consume
/// the exact approved ID before dispatch and settle that same approval after.
#[async_trait]
pub trait GoalContinuityExecutionPort: Send + Sync {
    async fn execute(&self, fence: GoalContinuityActionFence) -> GoalContinuityPortResult;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoalContinuityRunnerOptions {
    pub max_concurrency: usize,
    pub action_timeout: Duration,
}

impl Default for GoalContinuityRunnerOptions {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            action_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalContinuityRunnerError {
    InvalidOptions,
    InvalidGoalSet,
    Unavailable,
}

impl fmt::Display for GoalContinuityRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOptions => "goal continuity runner options are invalid",
            Self::InvalidGoalSet => "goal continuity runner goal set is invalid",
            Self::Unavailable => "goal continuity runner is unavailable",
        })
    }
}

impl std::error::Error for GoalContinuityRunnerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalContinuityRunStatus {
    QueueApplied,
    QueueUnchanged,
    ExecutionApplied,
    ExecutionUnchanged,
    ManualDecisionRequired,
    Waiting,
    ExecutionBlocked,
    Complete,
    AuthorityUnavailable,
    Rejected,
    Unavailable,
    TimedOut,
}

/// Redacted per-goal result. No owner, credential, target, path, prompt, raw
/// approval record or free-form error crosses the runner boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalContinuityRunItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<GoalNextActionKind>,
    pub status: GoalContinuityRunStatus,
}

pub struct GoalContinuityRunner {
    reader: Arc<dyn GoalContinuityProjectionReader>,
    queue: Option<Arc<dyn GoalContinuityQueuePort>>,
    execute: Option<Arc<dyn GoalContinuityExecutionPort>>,
    options: GoalContinuityRunnerOptions,
}

impl GoalContinuityRunner {
    pub(crate) fn assemble(
        reader: Arc<dyn GoalContinuityProjectionReader>,
        queue_authorized: bool,
        queue: Option<Arc<dyn GoalContinuityQueuePort>>,
        execute_authorized: bool,
        execute: Option<Arc<dyn GoalContinuityExecutionPort>>,
        options: GoalContinuityRunnerOptions,
    ) -> Result<Self, GoalContinuityRunnerError> {
        if options.max_concurrency == 0
            || options.max_concurrency > MAX_GOAL_CONTINUITY_CONCURRENCY
            || options.action_timeout.is_zero()
            || options.action_timeout > MAX_GOAL_CONTINUITY_TIMEOUT
            || queue_authorized != queue.is_some()
            || execute_authorized != execute.is_some()
        {
            return Err(GoalContinuityRunnerError::InvalidOptions);
        }
        Ok(Self {
            reader,
            queue,
            execute,
            options,
        })
    }

    /// Evaluate each goal at most once. Results are returned in input order;
    /// there is no periodic loop, retry, decision or direct dispatch path.
    pub async fn run_once(
        &self,
        goal_ids: Vec<String>,
    ) -> Result<Vec<GoalContinuityRunItem>, GoalContinuityRunnerError> {
        validate_goal_ids(&goal_ids)?;
        let mut results = stream::iter(goal_ids.into_iter().enumerate())
            .map(|(index, goal_id)| async move {
                let result = tokio::time::timeout(
                    self.options.action_timeout,
                    self.run_goal(goal_id.clone()),
                )
                .await
                .unwrap_or(GoalContinuityRunItem {
                    goal_id: Some(goal_id),
                    action: None,
                    status: GoalContinuityRunStatus::TimedOut,
                });
                (index, result)
            })
            .buffer_unordered(self.options.max_concurrency)
            .collect::<Vec<_>>()
            .await;
        results.sort_by_key(|(index, _)| *index);
        Ok(results.into_iter().map(|(_, result)| result).collect())
    }

    async fn run_goal(&self, goal_id: String) -> GoalContinuityRunItem {
        let view = match self.reader.continuity_next(&goal_id).await {
            Ok(view) => view,
            Err(GoalReadError::InvalidGoalId) => {
                return item(None, None, GoalContinuityRunStatus::Rejected);
            }
            Err(GoalReadError::NotFound | GoalReadError::ContinuityUnavailable) => {
                return item(Some(goal_id), None, GoalContinuityRunStatus::Rejected);
            }
            Err(GoalReadError::Unavailable) => {
                return item(Some(goal_id), None, GoalContinuityRunStatus::Unavailable);
            }
        };
        if !valid_view_for_request(&view, &goal_id) {
            return item(
                Some(goal_id),
                Some(view.action),
                GoalContinuityRunStatus::Rejected,
            );
        }
        let action = view.action;
        let status = match action {
            GoalNextActionKind::QueueApproval => {
                let Some(queue) = &self.queue else {
                    return item(
                        Some(goal_id),
                        Some(action),
                        GoalContinuityRunStatus::AuthorityUnavailable,
                    );
                };
                let Some(fence) = fence_from_view(&view, false) else {
                    return item(
                        Some(goal_id),
                        Some(action),
                        GoalContinuityRunStatus::Rejected,
                    );
                };
                map_queue_result(queue.queue(fence).await)
            }
            GoalNextActionKind::ExecuteApproval => {
                let Some(execute) = &self.execute else {
                    return item(
                        Some(goal_id),
                        Some(action),
                        GoalContinuityRunStatus::AuthorityUnavailable,
                    );
                };
                let Some(fence) = fence_from_view(&view, true) else {
                    return item(
                        Some(goal_id),
                        Some(action),
                        GoalContinuityRunStatus::Rejected,
                    );
                };
                map_execute_result(execute.execute(fence).await)
            }
            GoalNextActionKind::DecideApproval | GoalNextActionKind::ManualDecision => {
                GoalContinuityRunStatus::ManualDecisionRequired
            }
            GoalNextActionKind::ResolveBlockedExecution => {
                GoalContinuityRunStatus::ExecutionBlocked
            }
            GoalNextActionKind::ContinuityComplete => GoalContinuityRunStatus::Complete,
            GoalNextActionKind::RecordSignal
            | GoalNextActionKind::WaitForDependency
            | GoalNextActionKind::WaitForExecution => GoalContinuityRunStatus::Waiting,
        };
        item(Some(goal_id), Some(action), status)
    }
}

impl fmt::Debug for GoalContinuityRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalContinuityRunner")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

fn validate_goal_ids(goal_ids: &[String]) -> Result<(), GoalContinuityRunnerError> {
    if goal_ids.is_empty() || goal_ids.len() > MAX_GOAL_CONTINUITY_GOALS {
        return Err(GoalContinuityRunnerError::InvalidGoalSet);
    }
    let mut seen = HashSet::with_capacity(goal_ids.len());
    if goal_ids.iter().any(|id| {
        id.is_empty()
            || id.len() > 256
            || id.trim() != id
            || id.chars().any(char::is_control)
            || !seen.insert(id)
    }) {
        return Err(GoalContinuityRunnerError::InvalidGoalSet);
    }
    Ok(())
}

fn fence_from_view(
    view: &GoalNextActionView,
    require_approval: bool,
) -> Option<GoalContinuityActionFence> {
    let approval_id = view.approval_id.clone();
    if require_approval && approval_id.is_none() {
        return None;
    }
    Some(GoalContinuityActionFence {
        goal_id: view.goal_id.clone(),
        goal_revision: view.goal_revision,
        quota_event_id: view.quota_event_id.clone(),
        step_id: view.step_id.clone()?,
        step_kind: view.step_kind.clone()?,
        approval_id,
    })
}

fn valid_view_for_request(view: &GoalNextActionView, requested_goal_id: &str) -> bool {
    if view.view_schema != GOAL_NEXT_ACTION_VIEW_SCHEMA
        || view.goal_id != requested_goal_id
        || view.goal_revision == 0
        || !valid_projection_id(&view.quota_event_id)
    {
        return false;
    }
    let expected_command = match view.action {
        GoalNextActionKind::QueueApproval => Some(GoalOperatorCommand::ContinuityQueue),
        GoalNextActionKind::DecideApproval => Some(GoalOperatorCommand::ContinuityDecide),
        GoalNextActionKind::ExecuteApproval => Some(GoalOperatorCommand::ContinuityExecute),
        GoalNextActionKind::RecordSignal => Some(GoalOperatorCommand::ContinuitySignal),
        GoalNextActionKind::WaitForDependency
        | GoalNextActionKind::WaitForExecution
        | GoalNextActionKind::ResolveBlockedExecution
        | GoalNextActionKind::ManualDecision
        | GoalNextActionKind::ContinuityComplete => None,
    };
    if view.command != expected_command {
        return false;
    }
    match view.action {
        GoalNextActionKind::QueueApproval => {
            valid_step(view) && view.approval_id.as_deref().is_none_or(valid_projection_id)
        }
        GoalNextActionKind::DecideApproval | GoalNextActionKind::ExecuteApproval => {
            valid_step(view) && view.approval_id.as_deref().is_some_and(valid_projection_id)
        }
        GoalNextActionKind::RecordSignal => !view.accepted_signals.is_empty(),
        GoalNextActionKind::WaitForExecution | GoalNextActionKind::ResolveBlockedExecution => {
            valid_step(view) && view.approval_id.as_deref().is_some_and(valid_projection_id)
        }
        GoalNextActionKind::WaitForDependency
        | GoalNextActionKind::ManualDecision
        | GoalNextActionKind::ContinuityComplete => true,
    }
}

fn valid_step(view: &GoalNextActionView) -> bool {
    view.step_id.as_deref().is_some_and(valid_projection_id)
        && view.step_kind.as_deref().is_some_and(valid_projection_id)
}

fn valid_projection_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn map_queue_result(result: GoalContinuityPortResult) -> GoalContinuityRunStatus {
    match result {
        GoalContinuityPortResult::Applied => GoalContinuityRunStatus::QueueApplied,
        GoalContinuityPortResult::Unchanged => GoalContinuityRunStatus::QueueUnchanged,
        GoalContinuityPortResult::Rejected => GoalContinuityRunStatus::Rejected,
        GoalContinuityPortResult::Unavailable => GoalContinuityRunStatus::Unavailable,
    }
}

fn map_execute_result(result: GoalContinuityPortResult) -> GoalContinuityRunStatus {
    match result {
        GoalContinuityPortResult::Applied => GoalContinuityRunStatus::ExecutionApplied,
        GoalContinuityPortResult::Unchanged => GoalContinuityRunStatus::ExecutionUnchanged,
        GoalContinuityPortResult::Rejected => GoalContinuityRunStatus::Rejected,
        GoalContinuityPortResult::Unavailable => GoalContinuityRunStatus::Unavailable,
    }
}

fn item(
    goal_id: Option<String>,
    action: Option<GoalNextActionKind>,
    status: GoalContinuityRunStatus,
) -> GoalContinuityRunItem {
    GoalContinuityRunItem {
        goal_id,
        action,
        status,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::Result as AnyResult;
    use static_assertions::assert_not_impl_any;

    use super::*;
    use crate::{
        AuthenticatedPrincipal, GoalNextReasonCode, PrincipalAuthenticator, PrincipalOwnerResolver,
    };

    assert_not_impl_any!(GoalContinuityRunnerAuthority: Clone, serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(GoalContinuityRunnerCredentials<'static>: serde::Serialize, serde::de::DeserializeOwned);

    struct PrefixAuthenticator(&'static str);

    impl PrincipalAuthenticator for PrefixAuthenticator {
        fn authenticate(&self, credential: &[u8]) -> AnyResult<String> {
            let value = std::str::from_utf8(credential)?;
            if !value.starts_with(self.0) {
                anyhow::bail!("wrong purpose")
            }
            Ok(value.to_owned())
        }
    }

    struct SuffixOwner;

    impl PrincipalOwnerResolver for SuffixOwner {
        fn resolve_owner(&self, principal: &AuthenticatedPrincipal) -> AnyResult<String> {
            principal
                .subject()
                .split_once(':')
                .map(|(_, owner)| format!("owner:{owner}"))
                .ok_or_else(|| anyhow::anyhow!("missing owner"))
        }
    }

    fn context_factory(prefix: &'static str) -> OwnerContextFactory {
        OwnerContextFactory::new(Arc::new(PrefixAuthenticator(prefix)), Arc::new(SuffixOwner))
    }

    fn authority_factory() -> GoalContinuityRunnerAuthorityFactory {
        GoalContinuityRunnerAuthorityFactory::new(
            context_factory("read:"),
            Some(context_factory("queue:")),
            Some(context_factory("execute:")),
        )
    }

    #[test]
    fn purpose_credentials_are_independent_and_owner_bound() {
        let factory = authority_factory();
        let authority = factory
            .authenticate(GoalContinuityRunnerCredentials {
                read: b"read:alice",
                queue: Some(b"queue:alice"),
                execute: Some(b"execute:alice"),
            })
            .unwrap();
        assert!(authority.queue.is_some());
        assert!(authority.execute.is_some());
        assert_eq!(
            format!("{factory:?}"),
            "GoalContinuityRunnerAuthorityFactory { .. }"
        );

        assert_eq!(
            factory
                .authenticate(GoalContinuityRunnerCredentials {
                    read: b"read:alice",
                    queue: Some(b"read:alice"),
                    execute: Some(b"execute:alice"),
                })
                .unwrap_err(),
            GoalContinuityRunnerAuthorityError::AuthenticationFailed
        );
        assert_eq!(
            factory
                .authenticate(GoalContinuityRunnerCredentials {
                    read: b"read:alice",
                    queue: Some(b"queue:bob"),
                    execute: Some(b"execute:alice"),
                })
                .unwrap_err(),
            GoalContinuityRunnerAuthorityError::OwnerMismatch
        );
    }

    #[test]
    fn capability_configuration_must_match_supplied_credentials() {
        let factory = GoalContinuityRunnerAuthorityFactory::new(
            context_factory("read:"),
            None,
            Some(context_factory("execute:")),
        );
        assert_eq!(
            factory
                .authenticate(GoalContinuityRunnerCredentials {
                    read: b"read:alice",
                    queue: Some(b"queue:alice"),
                    execute: Some(b"execute:alice"),
                })
                .unwrap_err(),
            GoalContinuityRunnerAuthorityError::CapabilityMismatch
        );
    }

    struct FakeReader {
        views: BTreeMap<String, GoalNextActionView>,
        delay: Duration,
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl FakeReader {
        fn new(views: impl IntoIterator<Item = GoalNextActionView>) -> Self {
            Self {
                views: views
                    .into_iter()
                    .map(|view| (view.goal_id.clone(), view))
                    .collect(),
                delay: Duration::ZERO,
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
            }
        }

        fn delayed(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }
    }

    #[async_trait]
    impl GoalContinuityProjectionReader for FakeReader {
        async fn continuity_next(
            &self,
            goal_id: &str,
        ) -> Result<GoalNextActionView, GoalReadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.views
                .get(goal_id)
                .cloned()
                .ok_or(GoalReadError::NotFound)
        }
    }

    struct RecordingPort {
        calls: Mutex<Vec<GoalContinuityActionFence>>,
        result: GoalContinuityPortResult,
    }

    impl RecordingPort {
        fn new(result: GoalContinuityPortResult) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                result,
            }
        }
    }

    #[async_trait]
    impl GoalContinuityQueuePort for RecordingPort {
        async fn queue(&self, fence: GoalContinuityActionFence) -> GoalContinuityPortResult {
            self.calls.lock().unwrap().push(fence);
            self.result
        }
    }

    #[async_trait]
    impl GoalContinuityExecutionPort for RecordingPort {
        async fn execute(&self, fence: GoalContinuityActionFence) -> GoalContinuityPortResult {
            self.calls.lock().unwrap().push(fence);
            self.result
        }
    }

    fn view(goal_id: &str, action: GoalNextActionKind) -> GoalNextActionView {
        let (command, approval_id) = match action {
            GoalNextActionKind::QueueApproval => (Some(GoalOperatorCommand::ContinuityQueue), None),
            GoalNextActionKind::DecideApproval => (
                Some(GoalOperatorCommand::ContinuityDecide),
                Some("approval-7".into()),
            ),
            GoalNextActionKind::ExecuteApproval => (
                Some(GoalOperatorCommand::ContinuityExecute),
                Some("approval-7".into()),
            ),
            _ => (None, None),
        };
        GoalNextActionView {
            view_schema: 1,
            goal_id: goal_id.into(),
            goal_revision: 11,
            quota_event_id: "quota-3".into(),
            action,
            command,
            step_id: (!matches!(
                action,
                GoalNextActionKind::ManualDecision | GoalNextActionKind::ContinuityComplete
            ))
            .then(|| "takeover".into()),
            step_kind: (!matches!(
                action,
                GoalNextActionKind::ManualDecision | GoalNextActionKind::ContinuityComplete
            ))
            .then(|| "start_takeover".into()),
            approval_id,
            accepted_signals: Vec::new(),
            required_inputs: Vec::new(),
            reason_code: match action {
                GoalNextActionKind::QueueApproval => GoalNextReasonCode::ApprovalRequired,
                GoalNextActionKind::DecideApproval => GoalNextReasonCode::ApprovalDecisionRequired,
                GoalNextActionKind::ExecuteApproval => GoalNextReasonCode::ApprovedExecutionReady,
                GoalNextActionKind::ResolveBlockedExecution => GoalNextReasonCode::ExecutionBlocked,
                GoalNextActionKind::ContinuityComplete => GoalNextReasonCode::ContinuityComplete,
                GoalNextActionKind::ManualDecision => GoalNextReasonCode::ManualDecisionRequired,
                GoalNextActionKind::RecordSignal => GoalNextReasonCode::ExternalSignalRequired,
                GoalNextActionKind::WaitForDependency => GoalNextReasonCode::DependencyPending,
                GoalNextActionKind::WaitForExecution => GoalNextReasonCode::ExecutionInFlight,
            },
        }
    }

    fn runner(
        reader: Arc<dyn GoalContinuityProjectionReader>,
        queue: Option<Arc<dyn GoalContinuityQueuePort>>,
        execute: Option<Arc<dyn GoalContinuityExecutionPort>>,
        options: GoalContinuityRunnerOptions,
    ) -> GoalContinuityRunner {
        GoalContinuityRunner::assemble(
            reader,
            queue.is_some(),
            queue,
            execute.is_some(),
            execute,
            options,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn queue_and_execute_receive_only_exact_projection_fences() {
        let queue = Arc::new(RecordingPort::new(GoalContinuityPortResult::Applied));
        let execute = Arc::new(RecordingPort::new(GoalContinuityPortResult::Applied));
        let bounded_runner = runner(
            Arc::new(FakeReader::new([
                view("queue-goal", GoalNextActionKind::QueueApproval),
                view("execute-goal", GoalNextActionKind::ExecuteApproval),
            ])),
            Some(queue.clone()),
            Some(execute.clone()),
            GoalContinuityRunnerOptions::default(),
        );
        let results = bounded_runner
            .run_once(vec!["queue-goal".into(), "execute-goal".into()])
            .await
            .unwrap();
        assert_eq!(results[0].status, GoalContinuityRunStatus::QueueApplied);
        assert_eq!(results[1].status, GoalContinuityRunStatus::ExecutionApplied);
        assert_eq!(
            queue.calls.lock().unwrap().as_slice(),
            &[GoalContinuityActionFence {
                goal_id: "queue-goal".into(),
                goal_revision: 11,
                quota_event_id: "quota-3".into(),
                step_id: "takeover".into(),
                step_kind: "start_takeover".into(),
                approval_id: None,
            }]
        );
        assert_eq!(
            execute.calls.lock().unwrap()[0].approval_id.as_deref(),
            Some("approval-7")
        );
    }

    #[tokio::test]
    async fn decision_and_manual_actions_never_invoke_mutation_ports() {
        let port = Arc::new(RecordingPort::new(GoalContinuityPortResult::Applied));
        let runner = runner(
            Arc::new(FakeReader::new([
                view("decision", GoalNextActionKind::DecideApproval),
                view("manual", GoalNextActionKind::ManualDecision),
            ])),
            Some(port.clone()),
            Some(port.clone()),
            GoalContinuityRunnerOptions::default(),
        );
        let results = runner
            .run_once(vec!["decision".into(), "manual".into()])
            .await
            .unwrap();
        assert!(
            results
                .iter()
                .all(|result| { result.status == GoalContinuityRunStatus::ManualDecisionRequired })
        );
        assert!(port.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn missing_capability_never_falls_back_to_another_port() {
        let execute = Arc::new(RecordingPort::new(GoalContinuityPortResult::Applied));
        let runner = runner(
            Arc::new(FakeReader::new([view(
                "queue-goal",
                GoalNextActionKind::QueueApproval,
            )])),
            None,
            Some(execute.clone()),
            GoalContinuityRunnerOptions::default(),
        );
        let result = runner.run_once(vec!["queue-goal".into()]).await.unwrap();
        assert_eq!(
            result[0].status,
            GoalContinuityRunStatus::AuthorityUnavailable
        );
        assert!(execute.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_or_incoherent_projection_never_reaches_a_port() {
        let mut unknown = view("unknown", GoalNextActionKind::ExecuteApproval);
        unknown.view_schema += 1;
        let mut drifted = view("drifted", GoalNextActionKind::ExecuteApproval);
        drifted.goal_id = "different".into();
        let mut missing_approval = view("missing", GoalNextActionKind::ExecuteApproval);
        missing_approval.approval_id = None;
        let port = Arc::new(RecordingPort::new(GoalContinuityPortResult::Applied));
        let reader = Arc::new(FakeReader {
            views: [
                ("unknown".into(), unknown),
                ("drifted".into(), drifted),
                ("missing".into(), missing_approval),
            ]
            .into_iter()
            .collect(),
            delay: Duration::ZERO,
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        });
        let runner = runner(
            reader,
            None,
            Some(port.clone()),
            GoalContinuityRunnerOptions::default(),
        );
        let results = runner
            .run_once(vec!["unknown".into(), "drifted".into(), "missing".into()])
            .await
            .unwrap();
        assert!(
            results
                .iter()
                .all(|result| result.status == GoalContinuityRunStatus::Rejected)
        );
        assert!(port.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_sets_are_rejected_before_any_read() {
        let reader = Arc::new(FakeReader::new([]));
        let runner = runner(
            reader.clone(),
            None,
            None,
            GoalContinuityRunnerOptions::default(),
        );
        assert_eq!(
            runner.run_once(Vec::new()).await.unwrap_err(),
            GoalContinuityRunnerError::InvalidGoalSet
        );
        assert_eq!(
            runner
                .run_once(vec!["same".into(), "same".into()])
                .await
                .unwrap_err(),
            GoalContinuityRunnerError::InvalidGoalSet
        );
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrency_timeout_and_report_order_are_bounded() {
        let reader = Arc::new(
            FakeReader::new([
                view("first", GoalNextActionKind::ContinuityComplete),
                view("second", GoalNextActionKind::ContinuityComplete),
                view("third", GoalNextActionKind::ContinuityComplete),
            ])
            .delayed(Duration::from_millis(20)),
        );
        let bounded_runner = runner(
            reader.clone(),
            None,
            None,
            GoalContinuityRunnerOptions {
                max_concurrency: 2,
                action_timeout: Duration::from_millis(100),
            },
        );
        let results = bounded_runner
            .run_once(vec!["first".into(), "second".into(), "third".into()])
            .await
            .unwrap();
        assert_eq!(reader.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(results[0].goal_id.as_deref(), Some("first"));
        assert_eq!(results[1].goal_id.as_deref(), Some("second"));
        assert_eq!(results[2].goal_id.as_deref(), Some("third"));

        let timeout = runner(
            Arc::new(
                FakeReader::new([view("slow", GoalNextActionKind::ContinuityComplete)])
                    .delayed(Duration::from_millis(30)),
            ),
            None,
            None,
            GoalContinuityRunnerOptions {
                max_concurrency: 1,
                action_timeout: Duration::from_millis(5),
            },
        )
        .run_once(vec!["slow".into()])
        .await
        .unwrap();
        assert_eq!(timeout[0].status, GoalContinuityRunStatus::TimedOut);
    }

    #[test]
    fn options_and_port_authority_pairing_fail_closed() {
        let reader = Arc::new(FakeReader::new([]));
        let port = Arc::new(RecordingPort::new(GoalContinuityPortResult::Applied));
        assert_eq!(
            GoalContinuityRunner::assemble(
                reader,
                false,
                Some(port),
                false,
                None,
                GoalContinuityRunnerOptions::default(),
            )
            .unwrap_err(),
            GoalContinuityRunnerError::InvalidOptions
        );
    }
}
