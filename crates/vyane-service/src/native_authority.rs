//! Live native-execution authority backed by the durable AgentRun store.
//!
//! This adapter intentionally supports only fresh, sessionless execution. A
//! logical session also requires a live [`vyane_core::SessionExecutionLease`]
//! and, for native resume, exact [`vyane_core::NativeSessionDomain`]
//! revalidation through the final revision-fenced commit. Until that composed
//! authority exists, session-bearing scopes fail closed at construction.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use vyane_agent::{
    ActiveExecutionPermit, AgentStore, AgentStoreError, NativeExecutionScope, NewRunCompletion,
    PreparedRunCompletion,
};
use vyane_core::{ErrorKind, NativeExecutionAuthority, NativeSideEffect, Result, VyaneError};

/// Tokio-aware bridge from one active AgentRun permit to live side-effect
/// authorization.
///
/// The permit and frozen scope remain owned by this value and are never
/// serialized or exposed. Every supported model-send or tool-operation
/// [`NativeExecutionAuthority::revalidate`] call runs the synchronous store
/// check on Tokio's blocking pool, then discards the returned audit snapshot.
/// Checkpoint and session-commit effects fail closed. Store diagnostics are
/// mapped to bounded static errors so run ids and filesystem paths do not cross
/// the execution boundary.
///
/// This type is deliberately not `Clone`. Callers that need a shared trait
/// object may wrap the single authority in an `Arc`.
pub struct AgentRunModelToolAuthority {
    state: Arc<AgentPermitState>,
    scope: NativeExecutionScope,
}

/// Shared private ownership of one live AgentRun permit.
///
/// This state is crate-private so multiple authority adapters can reuse the
/// same non-cloneable permit without exposing either it or the raw store to an
/// operation.
pub(crate) struct AgentPermitState {
    store: Arc<dyn AgentStore>,
    permit: ActiveExecutionPermit,
    _lifetime_guard: Option<Arc<dyn PermitLifetimeGuard>>,
    #[cfg(test)]
    validation_delay: Option<std::time::Duration>,
}

pub(crate) trait PermitLifetimeGuard: Send + Sync {}
impl<T: Send + Sync> PermitLifetimeGuard for T {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PermitValidationError {
    TaskFailed,
    Rejected,
}

impl AgentPermitState {
    pub(crate) fn new(store: Arc<dyn AgentStore>, permit: ActiveExecutionPermit) -> Self {
        Self {
            store,
            permit,
            _lifetime_guard: None,
            #[cfg(test)]
            validation_delay: None,
        }
    }

    pub(crate) fn new_guarded(
        store: Arc<dyn AgentStore>,
        permit: ActiveExecutionPermit,
        lifetime_guard: Arc<dyn PermitLifetimeGuard>,
    ) -> Self {
        Self {
            store,
            permit,
            _lifetime_guard: Some(lifetime_guard),
            #[cfg(test)]
            validation_delay: None,
        }
    }

    pub(crate) fn policy_matches(&self, scope: &NativeExecutionScope) -> bool {
        self.permit.policy_digest() == scope.policy_digest()
    }

    pub(crate) async fn validate_permit(
        self: &Arc<Self>,
    ) -> std::result::Result<(), PermitValidationError> {
        let handle =
            tokio::runtime::Handle::try_current().map_err(|_| PermitValidationError::TaskFailed)?;
        let state = Arc::clone(self);
        handle
            .spawn_blocking(move || {
                #[cfg(test)]
                if let Some(delay) = state.validation_delay {
                    std::thread::sleep(delay);
                }
                state.store.validate_execution_permit(
                    state.permit.owner(),
                    &state.permit,
                    state.permit.policy_digest(),
                )
            })
            .await
            .map_err(|_| PermitValidationError::TaskFailed)?
            .map(|_| ())
            .map_err(|_| PermitValidationError::Rejected)
    }

    pub(crate) async fn validate_native(
        self: &Arc<Self>,
        scope: &NativeExecutionScope,
    ) -> Result<()> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            VyaneError::new(
                ErrorKind::Unsupported,
                "native execution authority requires a Tokio runtime",
            )
        })?;
        let state = Arc::clone(self);
        let scope = scope.clone();
        let validation = handle
            .spawn_blocking(move || {
                #[cfg(test)]
                if let Some(delay) = state.validation_delay {
                    std::thread::sleep(delay);
                }
                state.store.validate_native_execution_permit(
                    state.permit.owner(),
                    &state.permit,
                    &scope,
                )
            })
            .await
            .map_err(|_| {
                VyaneError::new(
                    ErrorKind::Other,
                    "native execution authority validation task failed",
                )
            })?;
        validation.map(|_| ()).map_err(map_store_error)
    }

    /// Atomically prepare completion authority, then revalidate it on the same
    /// blocking task immediately before returning control to the sink caller.
    pub(crate) async fn prepare_completion(
        self: &Arc<Self>,
        completion: NewRunCompletion,
    ) -> Result<PreparedRunCompletion> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            VyaneError::new(
                ErrorKind::Unsupported,
                "completion preparation requires a Tokio runtime",
            )
        })?;
        let state = Arc::clone(self);
        handle
            .spawn_blocking(move || {
                let prepared = state.store.prepare_completion(
                    state.permit.owner(),
                    &state.permit,
                    &completion,
                )?;
                state
                    .store
                    .validate_completion_permit(state.permit.owner(), &prepared.permit)?;
                Ok(prepared)
            })
            .await
            .map_err(|_| VyaneError::new(ErrorKind::Other, "completion preparation task failed"))?
            .map_err(map_store_error)
    }

    pub(crate) fn validate_completion_now(
        &self,
        permit: &vyane_agent::ActiveCompletionPermit,
    ) -> std::result::Result<vyane_agent::CompletionPermitSnapshot, AgentStoreError> {
        self.store
            .validate_completion_permit(self.permit.owner(), permit)
    }
}

impl AgentRunModelToolAuthority {
    /// Construct authority for a fresh AgentRun that has no logical-session
    /// continuity. The first side effect still performs the full durable
    /// revalidation; construction itself grants no authority.
    pub fn for_fresh_sessionless(
        store: Arc<dyn AgentStore>,
        permit: ActiveExecutionPermit,
        scope: NativeExecutionScope,
    ) -> Result<Self> {
        if scope.logical_session_id().is_some() || scope.resume_session_proof().is_some() {
            return Err(VyaneError::new(
                ErrorKind::Unsupported,
                "session-bearing native execution authority is not assembled",
            ));
        }
        if permit.policy_digest() != scope.policy_digest() {
            return Err(stale_authority_error());
        }
        Ok(Self {
            state: Arc::new(AgentPermitState::new(store, permit)),
            scope,
        })
    }
}

impl fmt::Debug for AgentRunModelToolAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRunModelToolAuthority")
            .finish_non_exhaustive()
    }
}

pub(crate) async fn revalidate_model_tool_effect(
    state: &Arc<AgentPermitState>,
    scope: &NativeExecutionScope,
    effect: NativeSideEffect,
) -> Result<()> {
    match effect {
        NativeSideEffect::ModelSend { turn, wire_attempt } if turn > 0 && wire_attempt > 0 => {}
        NativeSideEffect::ToolOperation { turn, ordinal } if turn > 0 && ordinal > 0 => {}
        NativeSideEffect::ModelSend { .. } | NativeSideEffect::ToolOperation { .. } => {
            return Err(VyaneError::new(
                ErrorKind::Config,
                "native model/tool effect coordinates must be one-based",
            ));
        }
        NativeSideEffect::CheckpointPrepare { .. }
        | NativeSideEffect::CheckpointPublish { .. }
        | NativeSideEffect::SessionCommit { .. } => {
            return Err(VyaneError::new(
                ErrorKind::Unsupported,
                "checkpoint and session-commit authority is not assembled",
            ));
        }
        _ => {
            return Err(VyaneError::new(
                ErrorKind::Unsupported,
                "native side-effect authority is not assembled for this operation",
            ));
        }
    }
    state.validate_native(scope).await
}

#[async_trait]
impl NativeExecutionAuthority for AgentRunModelToolAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> Result<()> {
        revalidate_model_tool_effect(&self.state, &self.scope, effect).await
    }
}

fn stale_authority_error() -> VyaneError {
    VyaneError::new(
        ErrorKind::Conflict,
        "native execution authority is stale or invalid",
    )
}

fn map_store_error(error: AgentStoreError) -> VyaneError {
    match error {
        AgentStoreError::InvalidExecutionPermit { .. }
        | AgentStoreError::InvalidCompletionPermit { .. }
        | AgentStoreError::CompletionConflict { .. }
        | AgentStoreError::InvalidReceipt { .. }
        | AgentStoreError::InvalidTransition { .. }
        | AgentStoreError::NotFound { .. }
        | AgentStoreError::Conflict { .. }
        | AgentStoreError::ControlBusy { .. }
        | AgentStoreError::ResumeRejected { .. } => stale_authority_error(),
        AgentStoreError::InvalidInput(_) => VyaneError::new(
            ErrorKind::Config,
            "native execution authority metadata is invalid",
        ),
        AgentStoreError::Io(_) | AgentStoreError::Sqlite(_) => VyaneError::new(
            ErrorKind::Io,
            "native execution authority store is unavailable",
        ),
        AgentStoreError::UnsupportedSchema { .. } | AgentStoreError::CorruptData(_) => {
            VyaneError::new(
                ErrorKind::Other,
                "native execution authority store failed validation",
            )
        }
        AgentStoreError::AlreadyExists { .. }
        | AgentStoreError::InvalidCancelTicket { .. }
        | AgentStoreError::InvalidRecoveryTicket { .. } => VyaneError::new(
            ErrorKind::Other,
            "native execution authority store returned an invalid result",
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use vyane_agent::{
        AgentClock, ControllerKind, ControllerRef, NewAgentRun, NewWorker, ResumeSessionProof,
        RunMode, SqliteAgentStore,
    };

    use super::*;

    assert_impl_all!(AgentRunModelToolAuthority: Send, Sync);
    assert_not_impl_any!(AgentRunModelToolAuthority: Clone, serde::Serialize, serde::de::DeserializeOwned);

    #[derive(Debug)]
    struct TestClock(Mutex<DateTime<Utc>>);

    impl TestClock {
        fn new() -> Self {
            Self(Mutex::new(
                Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0)
                    .single()
                    .unwrap(),
            ))
        }

        fn advance(&self, seconds: i64) {
            let mut now = self.0.lock().unwrap();
            *now = now.checked_add_signed(TimeDelta::seconds(seconds)).unwrap();
        }
    }

    impl AgentClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn build_authority(
        logical_session_id: Option<String>,
    ) -> (
        tempfile::TempDir,
        Arc<TestClock>,
        Arc<SqliteAgentStore>,
        ActiveExecutionPermit,
        NativeExecutionScope,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::new());
        let store = Arc::new(
            SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite"), clock.clone())
                .unwrap(),
        );
        let worker = NewWorker {
            id: "worker-canary".into(),
            logical_session_id: logical_session_id.clone(),
        };
        let run = NewAgentRun {
            id: "run-canary".into(),
            worker_id: worker.id.clone(),
            task_id: None,
            trace_id: None,
            parent_run_id: None,
            mode: RunMode::Autonomous,
            target_key: "provider/model".into(),
            prompt_digest: digest('a'),
            policy_digest: digest('b'),
            available_at: clock.now(),
            timeout_seconds: 600,
            max_resume_attempts: 0,
        };
        store.create_root("owner-canary", &worker, &run).unwrap();
        let claimed = store
            .claim_due("owner-canary", "supervisor-canary", 30, 1)
            .unwrap()
            .remove(0);
        let started = store
            .start(
                "owner-canary",
                &claimed.receipt,
                &ControllerRef {
                    kind: ControllerKind::InProcess,
                    id: "controller-canary".into(),
                    fingerprint: None,
                },
            )
            .unwrap();
        let permit = store
            .issue_execution_permit("owner-canary", &started.receipt, &digest('b'))
            .unwrap();
        let scope = NativeExecutionScope::fresh(
            "provider/model",
            digest('a'),
            digest('b'),
            logical_session_id,
        )
        .unwrap();
        (directory, clock, store, permit, scope)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_effect_revalidates_live_state_and_expiry_revokes() {
        let (_directory, clock, store, permit, scope) = build_authority(None);
        let authority = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            scope,
        )
        .unwrap();

        authority
            .revalidate(NativeSideEffect::ModelSend {
                turn: 1,
                wire_attempt: 1,
            })
            .await
            .unwrap();
        clock.advance(31);
        let error = authority
            .revalidate(NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1,
            })
            .await
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(
            error.message,
            "native execution authority is stale or invalid"
        );
        assert!(!format!("{error:?}").contains("run-canary"));
    }

    #[tokio::test]
    async fn unsupported_effects_and_zero_coordinates_fail_before_store_authority() {
        let (_directory, clock, store, permit, scope) = build_authority(None);
        let authority = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            scope,
        )
        .unwrap();
        clock.advance(31);

        for effect in [
            NativeSideEffect::CheckpointPrepare { sequence: 1 },
            NativeSideEffect::CheckpointPublish { sequence: 1 },
            NativeSideEffect::SessionCommit {
                expected_revision: 1,
            },
        ] {
            let error = authority.revalidate(effect).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::Unsupported);
        }
        for effect in [
            NativeSideEffect::ModelSend {
                turn: 0,
                wire_attempt: 1,
            },
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 0,
            },
        ] {
            let error = authority.revalidate(effect).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::Config);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_store_validation_does_not_block_the_async_executor() {
        let (_directory, _clock, store, permit, scope) = build_authority(None);
        let mut authority = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            scope,
        )
        .unwrap();
        Arc::get_mut(&mut authority.state).unwrap().validation_delay =
            Some(std::time::Duration::from_millis(100));
        let started = tokio::time::Instant::now();

        let (validation, tick_elapsed) = tokio::join!(
            authority.revalidate(NativeSideEffect::ModelSend {
                turn: 1,
                wire_attempt: 1,
            }),
            async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                started.elapsed()
            }
        );

        validation.unwrap();
        assert!(tick_elapsed < std::time::Duration::from_millis(80));
    }

    #[test]
    fn constructor_rejects_session_or_policy_drift_without_exposing_scope() {
        let (_directory, _clock, store, permit, scope) =
            build_authority(Some("logical-canary".into()));
        let error = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            scope,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert!(!format!("{error:?}").contains("logical-canary"));

        let (_directory, _clock, store, permit, _scope) =
            build_authority(Some("resume-logical-canary".into()));
        let resumed = NativeExecutionScope::resumed(
            "provider/model",
            digest('a'),
            digest('b'),
            "resume-logical-canary",
            ResumeSessionProof::derive(
                "owner-canary",
                "resume-logical-canary",
                "native-session-canary",
            )
            .unwrap(),
        )
        .unwrap();
        let error = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            resumed,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        let debug = format!("{error:?}");
        assert!(!debug.contains("resume-logical-canary"));
        assert!(!debug.contains("native-session-canary"));

        let (_directory, _clock, store, permit, _scope) = build_authority(None);
        let mismatched =
            NativeExecutionScope::fresh("provider/model", digest('a'), digest('c'), None).unwrap();
        let error = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            mismatched,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert!(!format!("{error:?}").contains(&digest('c')));
    }

    #[test]
    fn debug_is_opaque() {
        let (_directory, _clock, store, permit, scope) = build_authority(None);
        let authority = AgentRunModelToolAuthority::for_fresh_sessionless(
            store as Arc<dyn AgentStore>,
            permit,
            scope,
        )
        .unwrap();
        assert_eq!(
            format!("{authority:?}"),
            "AgentRunModelToolAuthority { .. }"
        );
    }
}
