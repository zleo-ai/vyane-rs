//! Exact, owner-bound in-process AgentRun controller assembly.
//!
//! The backend in this module is both the execution and recovery boundary for
//! one in-process operation. It deliberately does not provide a resident loop,
//! a second work queue, or message handback. Operation inputs remain behind an
//! owner-bound [`InProcessAgentOperation`] and are resolved from the immutable
//! execution identity only after the durable AgentRun has entered `Running`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use async_trait::async_trait;
use futures::FutureExt as _;
use tokio::sync::Notify;
use tokio::time::timeout_at;
use vyane_agent::{
    ActiveCompletionPermit, ActiveExecutionPermit, AgentStore, ControllerKind, ControllerRef,
    NativeExecutionScope, NewRunCompletion,
};
use vyane_core::{
    CancellationToken, ErrorKind, NativeExecutionAuthority, NativeSideEffect, Result as VyaneResult,
};

use crate::native_authority::{
    AgentPermitState, PermitValidationError, revalidate_model_tool_effect,
};
use crate::{
    AgentCompletionPublisher, AgentCompletionPublisherError, AgentCompletionPublisherOptions,
    AgentCompletionSink, AgentControllerAdapter, AgentExecutionContext, AgentExecutionError,
    AgentExecutionIdentity, AgentExecutionOptions, AgentExecutorOutcome, AgentRecoveryError,
    AgentRecoveryOptions, AgentRunExecutionDriver, AgentRunExecutor, AgentRunRecoveryDriver,
    AgentSupervisorError, AgentSupervisorOptions, ControllerRecoveryContext,
    ControllerRecoveryObservation, ResidentAgentBackend, ResidentInProcessAgentSupervisor,
    StagedRunCompletion,
};

const MAX_OWNER_BYTES: usize = 256;
const MAX_OPERATION_NAME_BYTES: usize = 64;
const MAX_RETIRED_CONTROLLERS: usize = 4096;

/// The kind of external effect about to be linearized by an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProcessAgentEffect {
    ModelSend,
    ToolOperation,
    Other,
}

/// A single-use proof that the durable execution permit was live immediately
/// before an in-process effect.
///
/// This value is intentionally opaque, non-cloneable, and non-serializable.
/// Operations must consume one proof at the corresponding effect boundary;
/// retaining it for a later or different effect violates the port contract.
pub struct InProcessEffectPermit<'operation> {
    effect: InProcessAgentEffect,
    _scope: PhantomData<&'operation mut ()>,
}

impl InProcessEffectPermit<'_> {
    #[must_use]
    pub const fn effect(self) -> InProcessAgentEffect {
        self.effect
    }
}

impl fmt::Debug for InProcessEffectPermit<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessEffectPermit")
            .finish_non_exhaustive()
    }
}

/// Non-cloneable source of per-effect durable authority.
pub struct InProcessEffectAuthority<'operation> {
    state: Arc<AgentPermitState>,
    _scope: PhantomData<&'operation mut ()>,
}

impl<'operation> InProcessEffectAuthority<'operation> {
    fn new(state: Arc<AgentPermitState>, _scope: &'operation mut ()) -> Self {
        Self {
            state,
            _scope: PhantomData,
        }
    }

    /// Revalidate the complete active permit on the blocking pool and return a
    /// fresh single-use proof. Store diagnostics and durable identities are
    /// intentionally erased at this boundary. This is permit-only validation:
    /// even when `effect` names a model or tool, the proof is not sufficient
    /// for a native loop because it carries no target/prompt/session binding.
    /// Native loops must use [`Self::bind_fresh_native_scope`] instead.
    pub async fn authorize(
        &self,
        effect: InProcessAgentEffect,
    ) -> Result<InProcessEffectPermit<'_>, InProcessAuthorityError> {
        self.state
            .validate_permit()
            .await
            .map_err(|error| match error {
                PermitValidationError::TaskFailed => InProcessAuthorityError::ValidationTaskFailed,
                PermitValidationError::Rejected => InProcessAuthorityError::PermitRejected,
            })?;
        Ok(InProcessEffectPermit {
            effect,
            _scope: PhantomData,
        })
    }

    /// Bind this operation to one exact fresh, sessionless native scope.
    ///
    /// Construction performs an atomic durable scope validation before the
    /// returned authority can be used. Every later model send and tool
    /// operation repeats that same validation. The wrapper borrows this
    /// operation authority and cannot expose or outlive its permit.
    pub async fn bind_fresh_native_scope(
        &self,
        scope: NativeExecutionScope,
    ) -> Result<InProcessNativeAuthority<'_>, InProcessNativeBindError> {
        if scope.logical_session_id().is_some() || scope.resume_session_proof().is_some() {
            return Err(InProcessNativeBindError::UnsupportedScope);
        }
        if !self.state.policy_matches(&scope) {
            return Err(InProcessNativeBindError::ScopeRejected);
        }
        self.state
            .validate_native(&scope)
            .await
            .map_err(map_native_bind_error)?;
        Ok(InProcessNativeAuthority {
            state: Arc::clone(&self.state),
            scope,
            _lifetime: PhantomData,
        })
    }

    /// Consume ordinary effect authority and freeze one completion descriptor.
    ///
    /// The store prepares and revalidates the returned permit before this
    /// method returns. The caller must immediately stage the exact external
    /// result through [`InProcessPreparedCompletion::stage_blocking`].
    /// No model, tool, activity, or resume authority remains after preparation.
    pub async fn prepare_completion(
        self,
        completion: NewRunCompletion,
    ) -> Result<InProcessPreparedCompletion<'operation>, InProcessCompletionError> {
        let prepared = self
            .state
            .prepare_completion(completion)
            .await
            .map_err(map_completion_error)?;
        Ok(InProcessPreparedCompletion {
            permit: prepared.permit,
            state: self.state,
            _scope: PhantomData,
        })
    }
}

impl fmt::Debug for InProcessEffectAuthority<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessEffectAuthority")
            .finish_non_exhaustive()
    }
}

/// Body-free authority failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProcessAuthorityError {
    ValidationTaskFailed,
    PermitRejected,
}

impl fmt::Display for InProcessAuthorityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ValidationTaskFailed => "in-process authority validation task failed",
            Self::PermitRejected => "in-process execution authority is stale or invalid",
        })
    }
}

impl std::error::Error for InProcessAuthorityError {}

/// Lifetime-bound completion authority after exact durable preparation.
pub struct InProcessPreparedCompletion<'operation> {
    permit: ActiveCompletionPermit,
    state: Arc<AgentPermitState>,
    _scope: PhantomData<&'operation mut ()>,
}

impl InProcessPreparedCompletion<'_> {
    #[must_use]
    pub fn completion_id(&self) -> &str {
        self.permit.completion_id()
    }

    /// Run the synchronous idempotent sink stage on Tokio's blocking pool.
    ///
    /// This wrapper and its controller lifetime guard move into the blocking
    /// closure. Dropping the awaiting future therefore cannot let recovery
    /// observe the controller as gone before a late stage has finished.
    pub async fn stage_blocking<F>(
        self,
        stage_exact: F,
    ) -> Result<StagedRunCompletion, InProcessCompletionStageError>
    where
        F: FnOnce(&vyane_agent::CompletionPermitSnapshot) -> bool + Send + 'static,
    {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| InProcessCompletionStageError::TaskFailed)?;
        handle
            .spawn_blocking(move || {
                let snapshot = self.state.validate_completion_now(&self.permit).ok()?;
                let exact = stage_exact(&snapshot);
                let _keep_controller_live = self.state;
                exact.then(|| StagedRunCompletion::new(self.permit))
            })
            .await
            .map_err(|_| InProcessCompletionStageError::TaskFailed)?
            .ok_or(InProcessCompletionStageError::Rejected)
    }
}

impl fmt::Debug for InProcessPreparedCompletion<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessPreparedCompletion")
            .field("completion_id", &self.completion_id())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProcessCompletionError {
    Rejected,
    Unavailable,
}

impl fmt::Display for InProcessCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected => "in-process completion authority is stale or invalid",
            Self::Unavailable => "in-process completion authority is unavailable",
        })
    }
}

impl std::error::Error for InProcessCompletionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProcessCompletionStageError {
    TaskFailed,
    Rejected,
}

impl fmt::Display for InProcessCompletionStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TaskFailed => "in-process completion staging task failed",
            Self::Rejected => "in-process completion staging was not exact",
        })
    }
}

impl std::error::Error for InProcessCompletionStageError {}

fn map_completion_error(error: vyane_core::VyaneError) -> InProcessCompletionError {
    match error.kind {
        ErrorKind::Io | ErrorKind::Other | ErrorKind::Unsupported => {
            InProcessCompletionError::Unavailable
        }
        _ => InProcessCompletionError::Rejected,
    }
}

/// Lifetime-bound native model/tool authority for one in-process operation.
///
/// This value has no cloning, serialization, raw-store, permit, checkpoint or
/// session escape hatch. It authorizes only one exact effect at a time through
/// [`NativeExecutionAuthority::revalidate`].
pub struct InProcessNativeAuthority<'authority> {
    state: Arc<AgentPermitState>,
    scope: NativeExecutionScope,
    _lifetime: PhantomData<&'authority mut ()>,
}

impl fmt::Debug for InProcessNativeAuthority<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessNativeAuthority")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl NativeExecutionAuthority for InProcessNativeAuthority<'_> {
    async fn revalidate(&self, effect: NativeSideEffect) -> VyaneResult<()> {
        revalidate_model_tool_effect(&self.state, &self.scope, effect).await
    }
}

/// Body-free failure while binding an exact native execution scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProcessNativeBindError {
    UnsupportedScope,
    ScopeRejected,
    ValidationUnavailable,
}

impl fmt::Display for InProcessNativeBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedScope => "session-bearing in-process native scope is unsupported",
            Self::ScopeRejected => "in-process native scope is stale or invalid",
            Self::ValidationUnavailable => "in-process native scope validation is unavailable",
        })
    }
}

impl std::error::Error for InProcessNativeBindError {}

fn map_native_bind_error(error: vyane_core::VyaneError) -> InProcessNativeBindError {
    match error.kind {
        ErrorKind::Conflict | ErrorKind::Config => InProcessNativeBindError::ScopeRejected,
        ErrorKind::Io | ErrorKind::Other | ErrorKind::Unsupported => {
            InProcessNativeBindError::ValidationUnavailable
        }
        _ => InProcessNativeBindError::ScopeRejected,
    }
}

/// Owner-bound body resolver and structured in-process operation.
///
/// `admit` must be pure, bounded, and non-blocking. `execute` owns all work it
/// starts: it must not spawn or detach unowned effects, and dropping its future
/// must synchronously stop ownership of all effects. The implementation
/// resolves any prompt/body from `identity`; bodies must not be placed in an
/// AgentRun or another queue. Non-native external effects must consume a fresh
/// proof from `authority.authorize(...)`. A native model/tool loop must first
/// bind its exact fresh scope with
/// [`InProcessEffectAuthority::bind_fresh_native_scope`] and pass only the
/// returned lifetime-bound authority into its authorized driver.
/// Both methods must also keep panic payloads body-free: the process panic hook
/// runs before this backend can catch and redact an unwind.
#[async_trait]
pub trait InProcessAgentOperation: Send + Sync {
    /// Stable, non-secret operation identity used only during construction.
    fn name(&self) -> &str;

    /// Fixed owner whose private input namespace this operation resolves.
    fn owner(&self) -> &str;

    /// Pure admission for a prospective controller identity.
    fn admit(&self, identity: &AgentExecutionIdentity, controller: &ControllerRef) -> bool;

    /// Bounded cleanup after durable recovery proved this exact controller is
    /// gone and consumed its recovery ticket.
    ///
    /// Implementations may release private input retained for reconciliation,
    /// but must not recreate or replay work. This hook runs on a blocking
    /// thread and must keep errors and panic payloads body-free.
    fn confirmed_gone(&self, _controller: &ControllerRef) {}

    /// Resolve and run one structured operation.
    async fn execute(
        &self,
        context: InProcessAgentOperationContext,
        identity: AgentExecutionIdentity,
        authority: InProcessEffectAuthority<'_>,
    ) -> AgentExecutorOutcome;
}

/// Cancellation and deadline context for one structured operation.
pub struct InProcessAgentOperationContext {
    deadline: tokio::time::Instant,
    controller: ControllerRef,
    cancellation: CancellationToken,
}

impl InProcessAgentOperationContext {
    #[must_use]
    pub fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }
    #[must_use]
    pub fn controller(&self) -> &ControllerRef {
        &self.controller
    }
    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

impl fmt::Debug for InProcessAgentOperationContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessAgentOperationContext")
            .finish_non_exhaustive()
    }
}

struct ActiveController {
    fingerprint: String,
    cancellation: CancellationToken,
    exited: Notify,
}

struct BackendState {
    owner: String,
    store: Arc<dyn AgentStore>,
    operation: Arc<dyn InProcessAgentOperation>,
    registry: Mutex<ControllerRegistry>,
    #[cfg(test)]
    recheck_hook: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
    #[cfg(test)]
    post_recheck_hook: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

#[derive(Default)]
struct ControllerRegistry {
    active: BTreeMap<String, Arc<ActiveController>>,
    retired: BTreeSet<(String, String)>,
}

/// Paired exact-controller backend for one owner and operation.
struct InProcessAgentBackend {
    state: Arc<BackendState>,
}

impl InProcessAgentBackend {
    fn new(
        owner: String,
        store: Arc<dyn AgentStore>,
        operation: Arc<dyn InProcessAgentOperation>,
    ) -> Self {
        Self {
            state: Arc::new(BackendState {
                owner,
                store,
                operation,
                registry: Mutex::new(ControllerRegistry::default()),
                #[cfg(test)]
                recheck_hook: Mutex::new(None),
                #[cfg(test)]
                post_recheck_hook: Mutex::new(None),
            }),
        }
    }

    async fn observe_exact(
        &self,
        deadline: tokio::time::Instant,
        controller: ControllerRef,
    ) -> ControllerRecoveryObservation {
        if controller.kind != ControllerKind::InProcess {
            return ControllerRecoveryObservation::Unavailable;
        }
        let Some(fingerprint) = controller.fingerprint.as_deref() else {
            return ControllerRecoveryObservation::Unavailable;
        };
        let active = {
            let Ok(mut registry) = self.state.registry.lock() else {
                return ControllerRecoveryObservation::Unavailable;
            };
            let Some(active) = registry.active.get(&controller.id) else {
                return retire_absent(&mut registry, &controller.id, fingerprint);
            };
            if active.fingerprint != fingerprint {
                return ControllerRecoveryObservation::Unavailable;
            }
            Arc::clone(active)
        };

        let exited = active.exited.notified();
        tokio::pin!(exited);
        // `notify_waiters` stores no permit. Enabling the intrusive waiter
        // before the exact recheck closes recheck-to-first-poll guard removal.
        exited.as_mut().enable();
        #[cfg(test)]
        let hook = self
            .state
            .recheck_hook
            .lock()
            .ok()
            .and_then(|hook| hook.clone());
        #[cfg(test)]
        if let Some((entered, release)) = hook {
            entered.notify_one();
            release.notified().await;
        }
        // Arm notification before the exact recheck to close lookup/drop.
        {
            let Ok(mut registry) = self.state.registry.lock() else {
                return ControllerRecoveryObservation::Unavailable;
            };
            match registry.active.get(&controller.id).cloned() {
                None => return retire_absent(&mut registry, &controller.id, fingerprint),
                Some(current) if Arc::ptr_eq(&current, &active) => {}
                Some(_) => return ControllerRecoveryObservation::Unavailable,
            }
        }
        #[cfg(test)]
        let post_hook = self
            .state
            .post_recheck_hook
            .lock()
            .ok()
            .and_then(|hook| hook.clone());
        #[cfg(test)]
        if let Some((entered, release)) = post_hook {
            entered.notify_one();
            release.notified().await;
        }
        active.cancellation.cancel();
        if timeout_at(deadline, &mut exited).await.is_err() {
            return ControllerRecoveryObservation::StillPresent;
        }
        let Ok(mut registry) = self.state.registry.lock() else {
            return ControllerRecoveryObservation::Unavailable;
        };
        if registry.active.contains_key(&controller.id) {
            return ControllerRecoveryObservation::Unavailable;
        }
        retire_absent(&mut registry, &controller.id, fingerprint)
    }

    fn register_exact(&self, controller: &ControllerRef, active: &Arc<ActiveController>) -> bool {
        let Ok(mut registry) = self.state.registry.lock() else {
            return false;
        };
        let retired_key = (controller.id.clone(), active.fingerprint.clone());
        if registry.retired.remove(&retired_key) {
            return false;
        }
        if registry.active.contains_key(&controller.id)
            || registry.retired.iter().any(|(id, _)| id == &controller.id)
        {
            return false;
        }
        registry
            .active
            .insert(controller.id.clone(), Arc::clone(active));
        true
    }
}

fn retire_absent(
    registry: &mut ControllerRegistry,
    controller_id: &str,
    fingerprint: &str,
) -> ControllerRecoveryObservation {
    // In-process effects cannot survive this backend instance. The exact
    // tombstone makes that observation atomic with rejecting a late register.
    let key = (controller_id.to_string(), fingerprint.to_string());
    if registry.retired.contains(&key) {
        return ControllerRecoveryObservation::Gone;
    }
    if registry.retired.iter().any(|(id, _)| id == controller_id)
        || registry.retired.len() >= MAX_RETIRED_CONTROLLERS
    {
        return ControllerRecoveryObservation::Unavailable;
    }
    registry.retired.insert(key);
    ControllerRecoveryObservation::Gone
}

impl fmt::Debug for InProcessAgentBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessAgentBackend")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AgentRunExecutor for InProcessAgentBackend {
    fn kind(&self) -> ControllerKind {
        ControllerKind::InProcess
    }

    fn admit_controller(
        &self,
        identity: &AgentExecutionIdentity,
        controller: &ControllerRef,
    ) -> bool {
        controller.kind == ControllerKind::InProcess
            && controller.fingerprint.is_some()
            && catch_unwind(AssertUnwindSafe(|| {
                self.state.operation.admit(identity, controller)
            }))
            .unwrap_or(false)
    }

    async fn execute(
        &self,
        context: AgentExecutionContext,
        identity: AgentExecutionIdentity,
        permit: ActiveExecutionPermit,
    ) -> AgentExecutorOutcome {
        let controller = context.controller().clone();
        let Some(fingerprint) = controller.fingerprint.clone() else {
            return AgentExecutorOutcome::Unknown;
        };
        if controller.kind != ControllerKind::InProcess
            || permit.owner() != self.state.owner
            || permit.run_id() != identity.run_id()
            || permit.worker_id() != identity.worker_id()
            || permit.generation() != identity.generation()
        {
            return AgentExecutorOutcome::Unknown;
        }

        let active = Arc::new(ActiveController {
            fingerprint,
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        let lifetime_guard = Arc::new(ActiveLifetimeGuard {
            state: Arc::downgrade(&self.state),
            controller_id: controller.id.clone(),
            active: Arc::clone(&active),
        });

        // First-effect fence. Registration happens only after the same store
        // confirms the permit is still current. Every blocking validation or
        // completion-preparation task owns the lifetime guard through this
        // state, so recovery cannot observe Gone while late store work exists.
        let mut authority_scope = ();
        let authority = InProcessEffectAuthority::new(
            Arc::new(AgentPermitState::new_guarded(
                Arc::clone(&self.state.store),
                permit,
                lifetime_guard,
            )),
            &mut authority_scope,
        );
        if authority
            .authorize(InProcessAgentEffect::Other)
            .await
            .map(InProcessEffectPermit::effect)
            .is_err()
        {
            return AgentExecutorOutcome::Unknown;
        }

        if !self.register_exact(&controller, &active) {
            return AgentExecutorOutcome::Unknown;
        }
        // A recovery observation may have been durably confirmed after the
        // first validation and released its tombstone before this late
        // registration. Revalidate once more while registered, before the
        // operation can obtain or perform any effect.
        if authority
            .authorize(InProcessAgentEffect::Other)
            .await
            .map(InProcessEffectPermit::effect)
            .is_err()
        {
            return AgentExecutorOutcome::Unknown;
        }
        let operation_context = InProcessAgentOperationContext {
            deadline: context.deadline(),
            controller,
            cancellation: active.cancellation.clone(),
        };
        let call = AssertUnwindSafe(self.state.operation.execute(
            operation_context,
            identity,
            authority,
        ))
        .catch_unwind();
        tokio::pin!(call);
        tokio::select! {
            outcome = &mut call => outcome.unwrap_or(AgentExecutorOutcome::Unknown),
            () = active.cancellation.cancelled() => AgentExecutorOutcome::Unknown,
            () = context.cancellation().cancelled() => AgentExecutorOutcome::Unknown,
        }
    }
}

struct ActiveLifetimeGuard {
    state: Weak<BackendState>,
    controller_id: String,
    active: Arc<ActiveController>,
}

impl Drop for ActiveLifetimeGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            if let Ok(mut registry) = state.registry.lock() {
                if registry
                    .active
                    .get(&self.controller_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &self.active))
                {
                    registry.active.remove(&self.controller_id);
                }
            }
        }
        self.active.exited.notify_waiters();
    }
}

#[async_trait]
impl AgentControllerAdapter for InProcessAgentBackend {
    fn name(&self) -> &str {
        "in-process-exact-v1"
    }

    fn kind(&self) -> ControllerKind {
        ControllerKind::InProcess
    }

    async fn observe_gone(
        &self,
        context: ControllerRecoveryContext,
        controller: ControllerRef,
    ) -> ControllerRecoveryObservation {
        self.observe_exact(context.deadline(), controller).await
    }

    fn confirmed_gone(&self, controller: &ControllerRef) {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.state.operation.confirmed_gone(controller);
        }));
        let Some(fingerprint) = controller.fingerprint.as_ref() else {
            return;
        };
        if let Ok(mut registry) = self.state.registry.lock() {
            registry
                .retired
                .remove(&(controller.id.clone(), fingerprint.clone()));
        }
    }
}

/// Safe paired assembly. The backend and raw store remain encapsulated.
pub struct InProcessAgentComponents {
    owner: String,
    store: Arc<dyn AgentStore>,
    backend: Arc<InProcessAgentBackend>,
    completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
}

struct AssemblyRegistration {
    owner: String,
    backend: Weak<BackendState>,
}

static LIVE_ASSEMBLIES: OnceLock<Mutex<Vec<AssemblyRegistration>>> = OnceLock::new();

impl InProcessAgentComponents {
    pub fn into_resident_backend(self) -> ResidentAgentBackend {
        let executor: Arc<dyn AgentRunExecutor> = self.backend.clone();
        let adapter: Arc<dyn AgentControllerAdapter> = self.backend;
        ResidentAgentBackend::new(
            self.owner,
            self.store,
            executor,
            vec![adapter],
            self.completion_sinks,
        )
    }

    pub fn new(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        operation: Arc<dyn InProcessAgentOperation>,
    ) -> Result<Self, InProcessAssemblyError> {
        Self::new_with_completion_sinks(owner, store, operation, Vec::new())
    }

    pub fn new_with_completion_sinks(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        operation: Arc<dyn InProcessAgentOperation>,
        completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
    ) -> Result<Self, InProcessAssemblyError> {
        let owner = owner.into();
        validate_text(&owner, MAX_OWNER_BYTES)
            .map_err(|()| InProcessAssemblyError::InvalidOwner)?;
        let metadata = catch_unwind(AssertUnwindSafe(|| {
            (operation.owner().to_string(), operation.name().to_string())
        }))
        .map_err(|_| InProcessAssemblyError::OperationMetadataPanicked)?;
        if metadata.0 != owner
            || validate_text(&metadata.0, MAX_OWNER_BYTES).is_err()
            || validate_text(&metadata.1, MAX_OPERATION_NAME_BYTES).is_err()
        {
            return Err(InProcessAssemblyError::InvalidOperationMetadata);
        }
        let backend = Arc::new(InProcessAgentBackend::new(
            owner.clone(),
            Arc::clone(&store),
            operation,
        ));
        let adapter: Arc<dyn AgentControllerAdapter> = backend.clone();
        AgentRunRecoveryDriver::new_with_completion_sinks(
            owner.clone(),
            Arc::clone(&store),
            "assembly-validation-v1",
            AgentRecoveryOptions::default(),
            vec![adapter],
            completion_sinks.clone(),
        )
        .map_err(|_| InProcessAssemblyError::InvalidCompletionSinks)?;
        let registry = LIVE_ASSEMBLIES.get_or_init(|| Mutex::new(Vec::new()));
        let mut registry = registry
            .lock()
            .map_err(|_| InProcessAssemblyError::AssemblyRegistryUnavailable)?;
        registry.retain(|entry| entry.backend.strong_count() > 0);
        // AgentStore intentionally exposes no stable physical-store identity;
        // two trait objects may reopen or wrap the same durable database.
        // Therefore one owner may have only one live in-process backend in
        // this process, regardless of injected Arc identity.
        if registry.iter().any(|entry| entry.owner == owner) {
            return Err(InProcessAssemblyError::DuplicateAssembly);
        }
        registry.push(AssemblyRegistration {
            owner: owner.clone(),
            backend: Arc::downgrade(&backend.state),
        });
        drop(registry);
        Ok(Self {
            owner: owner.clone(),
            store: Arc::clone(&store),
            backend,
            completion_sinks,
        })
    }

    pub fn execution_driver(
        &self,
        lease_owner: impl Into<String>,
        options: AgentExecutionOptions,
    ) -> Result<AgentRunExecutionDriver, AgentExecutionError> {
        let executor: Arc<dyn AgentRunExecutor> = self.backend.clone();
        AgentRunExecutionDriver::new(
            self.owner.clone(),
            Arc::clone(&self.store),
            lease_owner,
            options,
            executor,
        )
    }

    pub fn recovery_driver(
        &self,
        reconciler: impl Into<String>,
        options: AgentRecoveryOptions,
    ) -> Result<AgentRunRecoveryDriver, AgentRecoveryError> {
        let adapter: Arc<dyn AgentControllerAdapter> = self.backend.clone();
        AgentRunRecoveryDriver::new_with_completion_sinks(
            self.owner.clone(),
            Arc::clone(&self.store),
            reconciler,
            options,
            vec![adapter],
            self.completion_sinks.clone(),
        )
    }

    pub fn completion_publisher(
        &self,
        projector: impl Into<String>,
        options: AgentCompletionPublisherOptions,
    ) -> Result<AgentCompletionPublisher, AgentCompletionPublisherError> {
        AgentCompletionPublisher::new(
            self.owner.clone(),
            projector,
            Arc::clone(&self.store),
            self.completion_sinks.clone(),
            options,
        )
    }

    /// Consume this exact paired backend into a resident polling driver.
    pub fn into_resident_supervisor(
        self,
        lease_owner: impl Into<String>,
        reconciler: impl Into<String>,
        execution: AgentExecutionOptions,
        recovery: AgentRecoveryOptions,
        schedule: AgentSupervisorOptions,
    ) -> Result<ResidentInProcessAgentSupervisor, AgentSupervisorError> {
        ResidentInProcessAgentSupervisor::new(
            self,
            lease_owner.into(),
            reconciler.into(),
            execution,
            recovery,
            schedule,
        )
    }
}

impl fmt::Debug for InProcessAgentComponents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InProcessAgentComponents")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProcessAssemblyError {
    InvalidOwner,
    InvalidOperationMetadata,
    OperationMetadataPanicked,
    DuplicateAssembly,
    AssemblyRegistryUnavailable,
    InvalidCompletionSinks,
}

impl fmt::Display for InProcessAssemblyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "in-process assembly owner is invalid",
            Self::InvalidOperationMetadata => "in-process operation metadata is invalid",
            Self::OperationMetadataPanicked => "in-process operation metadata panicked",
            Self::DuplicateAssembly => "in-process assembly is already active",
            Self::AssemblyRegistryUnavailable => "in-process assembly registry is unavailable",
            Self::InvalidCompletionSinks => "in-process completion sinks are invalid",
        })
    }
}

impl std::error::Error for InProcessAssemblyError {}

fn validate_text(value: &str, max_bytes: usize) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        Err(())
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use vyane_agent::{
        AgentClock, NewAgentRun, NewRunCompletion, NewWorker, ResumeSessionProof,
        RunCompletionRecord, RunMode, RunState, SqliteAgentStore,
    };
    use vyane_message::{
        EndpointKind, EndpointRef, IdempotencyKey, MessageDirection, NewDelivery, NewMessage,
    };

    use super::*;
    use crate::{
        AgentCompletionSinkObservation, AgentExecutionItemStatus, AgentExecutionSettlement,
        AgentRecoveryItemStatus, MessageComponents, StoragePaths, message_run_completion,
    };

    const OWNER: &str = "owner-test";

    #[derive(Debug)]
    struct TestClock(Mutex<DateTime<Utc>>);

    impl TestClock {
        fn new() -> Self {
            Self(Mutex::new(
                Utc.with_ymd_and_hms(2026, 7, 12, 12, 0, 0)
                    .single()
                    .unwrap(),
            ))
        }

        fn advance(&self, seconds: i64) {
            *self.0.lock().unwrap() += TimeDelta::seconds(seconds);
        }
    }

    impl AgentClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        clock: Arc<TestClock>,
        store: Arc<SqliteAgentStore>,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let clock = Arc::new(TestClock::new());
            let store = Arc::new(
                SqliteAgentStore::open_with_clock(
                    directory.path().join("agent.sqlite"),
                    clock.clone(),
                )
                .unwrap(),
            );
            Self {
                _directory: directory,
                clock,
                store,
            }
        }

        fn enqueue(&self, suffix: &str) {
            self.enqueue_for(OWNER, suffix);
        }

        fn enqueue_for(&self, owner: &str, suffix: &str) {
            let worker = NewWorker {
                id: format!("worker-{suffix}"),
                logical_session_id: None,
            };
            let run = NewAgentRun {
                id: format!("run-{suffix}"),
                worker_id: worker.id.clone(),
                task_id: None,
                trace_id: None,
                parent_run_id: None,
                execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
                mode: RunMode::Autonomous,
                target_key: "http:test/model".into(),
                prompt_digest: "a".repeat(64),
                policy_digest: "b".repeat(64),
                available_at: self.clock.now(),
                timeout_seconds: 60,
                max_resume_attempts: 0,
            };
            self.store.create_root(owner, &worker, &run).unwrap();
        }

        fn permit(&self) -> ActiveExecutionPermit {
            self.enqueue("permit");
            let claim = self
                .store
                .claim_due(
                    OWNER,
                    vyane_agent::ExecutionBackend::NativeInProcess,
                    "lease-test",
                    30,
                    1,
                )
                .unwrap()
                .remove(0);
            let started = self
                .store
                .start(
                    OWNER,
                    &claim.receipt,
                    &ControllerRef {
                        kind: ControllerKind::InProcess,
                        id: "controller-test".into(),
                        fingerprint: Some("f".repeat(64)),
                    },
                )
                .unwrap();
            self.store
                .issue_execution_permit(OWNER, &started.receipt, &"b".repeat(64))
                .unwrap()
        }
    }

    struct TestOperation {
        owner: &'static str,
        calls: AtomicUsize,
        panic: bool,
    }

    struct BlockingOperation {
        owner: &'static str,
        entered: Arc<Notify>,
    }

    struct StageBlockingOperation {
        owner: &'static str,
        messages: MessageComponents,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    struct ExactCompletionSink;

    #[async_trait]
    impl AgentCompletionSink for ExactCompletionSink {
        fn kind(&self) -> &str {
            "test-sink"
        }

        async fn inspect(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Exact
        }

        async fn publish(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn discard(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }
    }

    #[async_trait]
    impl InProcessAgentOperation for StageBlockingOperation {
        fn name(&self) -> &str {
            "stage-blocking-operation"
        }

        fn owner(&self) -> &str {
            self.owner
        }

        fn admit(&self, _: &AgentExecutionIdentity, _: &ControllerRef) -> bool {
            true
        }

        async fn execute(
            &self,
            _: InProcessAgentOperationContext,
            identity: AgentExecutionIdentity,
            authority: InProcessEffectAuthority<'_>,
        ) -> AgentExecutorOutcome {
            let key = format!("result.{}", identity.run_id());
            let message = NewMessage {
                conversation_id: "stage-barrier-conversation".into(),
                session_id: None,
                direction: MessageDirection::Internal,
                kind: "completion".into(),
                sender: EndpointRef {
                    kind: EndpointKind::Agent,
                    id: "stage-barrier-agent".into(),
                },
                body: "stage-barrier-body".into(),
                payload: serde_json::json!({"status": "completed"}),
                reply_to: None,
                trace_id: None,
                correlation_id: Some(identity.run_id().into()),
                idempotency: IdempotencyKey {
                    producer: crate::MESSAGE_COMPLETION_PRODUCER.into(),
                    key: key.clone(),
                },
                deliveries: vec![NewDelivery {
                    route: "local".into(),
                    target: EndpointRef {
                        kind: EndpointKind::User,
                        id: "stage-barrier-requester".into(),
                    },
                    available_at: None,
                    expires_at: None,
                    max_attempts: 1,
                }],
            };
            let completion =
                match message_run_completion(format!("completion-{}", identity.run_id()), &message)
                {
                    Ok(completion) => completion,
                    Err(_) => return AgentExecutorOutcome::Unknown,
                };
            let prepared = match authority.prepare_completion(completion).await {
                Ok(prepared) => prepared,
                Err(_) => return AgentExecutorOutcome::Unknown,
            };
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            match self
                .messages
                .stage_completion_with_cleanup(prepared, message, move || {
                    entered.wait();
                    release.wait();
                    true
                })
                .await
            {
                Ok(staged) => AgentExecutorOutcome::Quiesced(
                    AgentExecutionSettlement::CompletionStaged(staged),
                ),
                Err(_) => AgentExecutorOutcome::Unknown,
            }
        }
    }

    #[async_trait]
    impl InProcessAgentOperation for BlockingOperation {
        fn name(&self) -> &str {
            "blocking-operation"
        }

        fn owner(&self) -> &str {
            self.owner
        }

        fn admit(&self, _: &AgentExecutionIdentity, _: &ControllerRef) -> bool {
            true
        }

        async fn execute(
            &self,
            _: InProcessAgentOperationContext,
            _: AgentExecutionIdentity,
            _: InProcessEffectAuthority<'_>,
        ) -> AgentExecutorOutcome {
            self.entered.notify_one();
            futures::future::pending().await
        }
    }

    #[async_trait]
    impl InProcessAgentOperation for TestOperation {
        fn name(&self) -> &str {
            "test-operation"
        }

        fn owner(&self) -> &str {
            self.owner
        }

        fn admit(&self, _: &AgentExecutionIdentity, _: &ControllerRef) -> bool {
            true
        }

        async fn execute(
            &self,
            _: InProcessAgentOperationContext,
            identity: AgentExecutionIdentity,
            authority: InProcessEffectAuthority<'_>,
        ) -> AgentExecutorOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.panic {
                panic!("body-free operation panic");
            }
            if authority
                .authorize(InProcessAgentEffect::ModelSend)
                .await
                .is_err()
            {
                return AgentExecutorOutcome::Unknown;
            }
            let prepared = match authority
                .prepare_completion(NewRunCompletion {
                    id: format!("completion-{}", identity.run_id()),
                    sink_kind: "test-sink".into(),
                    publication_key: format!("result.{}", identity.run_id()),
                    content_digest: "c".repeat(64),
                    content_bytes: 1,
                })
                .await
            {
                Ok(prepared) => prepared,
                Err(_) => return AgentExecutorOutcome::Unknown,
            };
            match prepared.stage_blocking(|_| true).await {
                Ok(staged) => AgentExecutorOutcome::Quiesced(
                    AgentExecutionSettlement::CompletionStaged(staged),
                ),
                Err(_) => AgentExecutorOutcome::Unknown,
            }
        }
    }

    fn operation(owner: &'static str, panic: bool) -> Arc<TestOperation> {
        Arc::new(TestOperation {
            owner,
            calls: AtomicUsize::new(0),
            panic,
        })
    }

    fn backend(operation: Arc<dyn InProcessAgentOperation>) -> Arc<InProcessAgentBackend> {
        let fixture = Fixture::new();
        Arc::new(InProcessAgentBackend::new(
            OWNER.into(),
            fixture.store as Arc<dyn AgentStore>,
            operation,
        ))
    }

    fn controller(id: &str, fingerprint: &str) -> ControllerRef {
        ControllerRef {
            kind: ControllerKind::InProcess,
            id: id.into(),
            fingerprint: Some(fingerprint.into()),
        }
    }

    assert_impl_all!(InProcessAgentBackend: Send, Sync);
    assert_impl_all!(InProcessAgentComponents: Send, Sync);
    assert_not_impl_any!(InProcessAgentBackend: Clone, serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(InProcessAgentComponents: Clone, serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(InProcessEffectAuthority<'static>: Clone, serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(InProcessEffectPermit<'static>: Clone, serde::Serialize, serde::de::DeserializeOwned);
    assert_impl_all!(InProcessNativeAuthority<'static>: Send, Sync);
    assert_not_impl_any!(InProcessNativeAuthority<'static>: Clone, serde::Serialize, serde::de::DeserializeOwned);

    #[test]
    fn assembly_rejects_owner_mismatch_and_debug_is_body_free() {
        let fixture = Fixture::new();
        let error = InProcessAgentComponents::new(
            OWNER,
            fixture.store as Arc<dyn AgentStore>,
            operation("different-owner", false),
        )
        .unwrap_err();
        assert_eq!(error, InProcessAssemblyError::InvalidOperationMetadata);
        assert!(!format!("{error:?}").contains("different-owner"));
    }

    #[test]
    fn duplicate_owner_store_assembly_is_rejected_until_all_backend_users_drop() {
        const TEST_OWNER: &str = "owner-duplicate-lifecycle";
        let fixture = Fixture::new();
        let store = fixture.store.clone() as Arc<dyn AgentStore>;
        let components = InProcessAgentComponents::new(
            TEST_OWNER,
            Arc::clone(&store),
            operation(TEST_OWNER, false),
        )
        .unwrap();
        let driver = components
            .execution_driver("lease-test", AgentExecutionOptions::default())
            .unwrap();
        drop(components);
        assert_eq!(
            InProcessAgentComponents::new(
                TEST_OWNER,
                Arc::clone(&store),
                operation(TEST_OWNER, false),
            )
            .unwrap_err(),
            InProcessAssemblyError::DuplicateAssembly
        );
        drop(driver);
        InProcessAgentComponents::new(TEST_OWNER, store, operation(TEST_OWNER, false)).unwrap();
    }

    #[test]
    fn different_owner_assemblies_can_coexist() {
        let first = Fixture::new();
        let first_store = first.store.clone() as Arc<dyn AgentStore>;
        let _first = InProcessAgentComponents::new(
            "owner-coexist-first",
            Arc::clone(&first_store),
            operation("owner-coexist-first", false),
        )
        .unwrap();
        let _different_owner = InProcessAgentComponents::new(
            "owner-coexist-second",
            first_store,
            operation("owner-coexist-second", false),
        )
        .unwrap();
    }

    #[test]
    fn same_owner_different_store_arc_is_still_rejected() {
        const TEST_OWNER: &str = "owner-alias-rejected";
        let first = Fixture::new();
        let second = Fixture::new();
        let _first = InProcessAgentComponents::new(
            TEST_OWNER,
            first.store as Arc<dyn AgentStore>,
            operation(TEST_OWNER, false),
        )
        .unwrap();
        assert_eq!(
            InProcessAgentComponents::new(
                TEST_OWNER,
                second.store as Arc<dyn AgentStore>,
                operation(TEST_OWNER, false),
            )
            .unwrap_err(),
            InProcessAssemblyError::DuplicateAssembly
        );
    }

    #[tokio::test]
    async fn absent_exact_controller_is_gone_and_tombstoned_against_late_registration() {
        let backend = backend(operation(OWNER, false));
        let reference = controller("future", "fingerprint");
        assert_eq!(
            backend
                .observe_exact(tokio::time::Instant::now(), reference.clone())
                .await,
            ControllerRecoveryObservation::Gone
        );
        let registry = backend.state.registry.lock().unwrap();
        assert!(
            registry
                .retired
                .contains(&(reference.id, "fingerprint".into()))
        );
    }

    #[tokio::test]
    async fn exact_tombstone_is_consumed_by_rejected_late_registration() {
        let backend = backend(operation(OWNER, false));
        let reference = controller("late", "exact");
        assert_eq!(
            backend
                .observe_exact(tokio::time::Instant::now(), reference.clone())
                .await,
            ControllerRecoveryObservation::Gone
        );
        let active = Arc::new(ActiveController {
            fingerprint: "exact".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        assert!(!backend.register_exact(&reference, &active));
        let registry = backend.state.registry.lock().unwrap();
        assert!(registry.retired.is_empty());
        assert!(registry.active.is_empty());
    }

    #[tokio::test]
    async fn retired_capacity_and_same_id_reuse_fail_closed_without_growth() {
        let backend = backend(operation(OWNER, false));
        {
            let mut registry = backend.state.registry.lock().unwrap();
            for index in 0..MAX_RETIRED_CONTROLLERS {
                registry
                    .retired
                    .insert((format!("controller-{index}"), "exact".into()));
            }
        }
        assert_eq!(
            backend
                .observe_exact(tokio::time::Instant::now(), controller("overflow", "exact"),)
                .await,
            ControllerRecoveryObservation::Unavailable
        );
        assert_eq!(
            backend
                .observe_exact(
                    tokio::time::Instant::now(),
                    controller("controller-0", "reused"),
                )
                .await,
            ControllerRecoveryObservation::Unavailable
        );
        assert_eq!(
            backend.state.registry.lock().unwrap().retired.len(),
            MAX_RETIRED_CONTROLLERS
        );
    }

    #[tokio::test]
    async fn durable_confirmation_reclaims_tombstones_beyond_capacity() {
        let backend = backend(operation(OWNER, false));
        for index in 0..=MAX_RETIRED_CONTROLLERS {
            let reference = controller(&format!("confirmed-{index}"), "exact");
            assert_eq!(
                backend
                    .observe_exact(tokio::time::Instant::now(), reference.clone())
                    .await,
                ControllerRecoveryObservation::Gone
            );
            backend.confirmed_gone(&reference);
        }
        assert!(backend.state.registry.lock().unwrap().retired.is_empty());
    }

    #[test]
    fn normal_controller_completion_does_not_consume_retired_capacity() {
        let backend = backend(operation(OWNER, false));
        for index in 0..=MAX_RETIRED_CONTROLLERS {
            let id = format!("normal-{index}");
            let reference = controller(&id, "exact");
            let active = Arc::new(ActiveController {
                fingerprint: "exact".into(),
                cancellation: CancellationToken::new(),
                exited: Notify::new(),
            });
            assert!(backend.register_exact(&reference, &active));
            drop(ActiveLifetimeGuard {
                state: Arc::downgrade(&backend.state),
                controller_id: id,
                active,
            });
        }
        let registry = backend.state.registry.lock().unwrap();
        assert!(registry.active.is_empty());
        assert!(registry.retired.is_empty());
    }

    #[tokio::test]
    async fn fingerprint_mismatch_never_signals_active_controller() {
        let backend = backend(operation(OWNER, false));
        let active = Arc::new(ActiveController {
            fingerprint: "exact".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        backend
            .state
            .registry
            .lock()
            .unwrap()
            .active
            .insert("same-id".into(), Arc::clone(&active));
        assert_eq!(
            backend
                .observe_exact(
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    controller("same-id", "reused"),
                )
                .await,
            ControllerRecoveryObservation::Unavailable
        );
        assert!(!active.cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn process_and_remote_controllers_fail_closed() {
        let backend = backend(operation(OWNER, false));
        for kind in [ControllerKind::Process, ControllerKind::Remote] {
            assert_eq!(
                backend
                    .observe_exact(
                        tokio::time::Instant::now(),
                        ControllerRef {
                            kind,
                            id: "foreign".into(),
                            fingerprint: Some("exact".into()),
                        },
                    )
                    .await,
                ControllerRecoveryObservation::Unavailable
            );
        }
        assert!(backend.state.registry.lock().unwrap().retired.is_empty());
    }

    #[tokio::test]
    async fn lookup_drop_replacement_is_unavailable_not_false_gone() {
        let backend = backend(operation(OWNER, false));
        let original = Arc::new(ActiveController {
            fingerprint: "exact".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        backend
            .state
            .registry
            .lock()
            .unwrap()
            .active
            .insert("same-id".into(), Arc::clone(&original));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *backend.state.recheck_hook.lock().unwrap() =
            Some((Arc::clone(&entered), Arc::clone(&release)));
        let observer = {
            let backend = Arc::clone(&backend);
            tokio::spawn(async move {
                backend
                    .observe_exact(
                        tokio::time::Instant::now() + Duration::from_secs(1),
                        controller("same-id", "exact"),
                    )
                    .await
            })
        };
        entered.notified().await;
        let replacement = Arc::new(ActiveController {
            fingerprint: "replacement".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        backend
            .state
            .registry
            .lock()
            .unwrap()
            .active
            .insert("same-id".into(), Arc::clone(&replacement));
        release.notify_one();
        assert_eq!(
            observer.await.unwrap(),
            ControllerRecoveryObservation::Unavailable
        );
        assert!(!original.cancellation.is_cancelled());
        assert!(!replacement.cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn exact_active_controller_is_gone_only_after_guard_removal() {
        let backend = backend(operation(OWNER, false));
        let active = Arc::new(ActiveController {
            fingerprint: "exact".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        backend
            .state
            .registry
            .lock()
            .unwrap()
            .active
            .insert("same-id".into(), Arc::clone(&active));
        let guard = ActiveLifetimeGuard {
            state: Arc::downgrade(&backend.state),
            controller_id: "same-id".into(),
            active: Arc::clone(&active),
        };
        let remover = tokio::spawn(async move {
            active.cancellation.cancelled().await;
            drop(guard);
        });
        assert_eq!(
            backend
                .observe_exact(
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    controller("same-id", "exact"),
                )
                .await,
            ControllerRecoveryObservation::Gone
        );
        remover.await.unwrap();
    }

    #[tokio::test]
    async fn guard_removal_after_recheck_before_first_poll_is_not_lost() {
        let backend = backend(operation(OWNER, false));
        let active = Arc::new(ActiveController {
            fingerprint: "exact".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        backend
            .state
            .registry
            .lock()
            .unwrap()
            .active
            .insert("same-id".into(), Arc::clone(&active));
        let guard = ActiveLifetimeGuard {
            state: Arc::downgrade(&backend.state),
            controller_id: "same-id".into(),
            active,
        };
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *backend.state.post_recheck_hook.lock().unwrap() =
            Some((Arc::clone(&entered), Arc::clone(&release)));
        let observer = {
            let backend = Arc::clone(&backend);
            tokio::spawn(async move {
                backend
                    .observe_exact(
                        tokio::time::Instant::now() + Duration::from_secs(60),
                        controller("same-id", "exact"),
                    )
                    .await
            })
        };
        entered.notified().await;
        drop(guard);
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), observer)
                .await
                .unwrap()
                .unwrap(),
            ControllerRecoveryObservation::Gone
        );
    }

    #[tokio::test]
    async fn signal_without_exit_times_out_as_still_present() {
        let backend = backend(operation(OWNER, false));
        let active = Arc::new(ActiveController {
            fingerprint: "exact".into(),
            cancellation: CancellationToken::new(),
            exited: Notify::new(),
        });
        backend
            .state
            .registry
            .lock()
            .unwrap()
            .active
            .insert("same-id".into(), Arc::clone(&active));
        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        assert_eq!(
            backend
                .observe_exact(deadline, controller("same-id", "exact"))
                .await,
            ControllerRecoveryObservation::StillPresent
        );
        assert!(active.cancellation.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_effect_authority_revalidates_and_revocation_fails_closed() {
        let fixture = Fixture::new();
        let permit = fixture.permit();
        let mut authority_scope = ();
        let authority = InProcessEffectAuthority::new(
            Arc::new(AgentPermitState::new(
                fixture.store.clone() as Arc<dyn AgentStore>,
                permit,
            )),
            &mut authority_scope,
        );
        assert_eq!(
            authority
                .authorize(InProcessAgentEffect::ToolOperation)
                .await
                .unwrap()
                .effect(),
            InProcessAgentEffect::ToolOperation
        );
        fixture.clock.advance(31);
        assert_eq!(
            authority
                .authorize(InProcessAgentEffect::ModelSend)
                .await
                .unwrap_err(),
            InProcessAuthorityError::PermitRejected
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bound_native_authority_validates_exact_scope_and_every_effect() {
        let fixture = Fixture::new();
        let permit = fixture.permit();
        let mut authority_scope = ();
        let authority = InProcessEffectAuthority::new(
            Arc::new(AgentPermitState::new(
                fixture.store.clone() as Arc<dyn AgentStore>,
                permit,
            )),
            &mut authority_scope,
        );
        let scope =
            NativeExecutionScope::fresh("http:test/model", "a".repeat(64), "b".repeat(64), None)
                .unwrap();
        let native = authority.bind_fresh_native_scope(scope).await.unwrap();
        assert_eq!(format!("{native:?}"), "InProcessNativeAuthority { .. }");
        native
            .revalidate(NativeSideEffect::ModelSend {
                turn: 1,
                wire_attempt: 1,
            })
            .await
            .unwrap();

        fixture.clock.advance(31);
        let error = native
            .revalidate(NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert!(!format!("{error:?}").contains("run-permit"));
    }

    #[tokio::test]
    async fn native_binding_rejects_scope_drift_and_sessions_before_use() {
        for scope in [
            NativeExecutionScope::fresh("http:other/model", "a".repeat(64), "b".repeat(64), None)
                .unwrap(),
            NativeExecutionScope::fresh("http:test/model", "c".repeat(64), "b".repeat(64), None)
                .unwrap(),
            NativeExecutionScope::fresh("http:test/model", "a".repeat(64), "c".repeat(64), None)
                .unwrap(),
        ] {
            let fixture = Fixture::new();
            let permit = fixture.permit();
            let mut authority_scope = ();
            let authority = InProcessEffectAuthority::new(
                Arc::new(AgentPermitState::new(
                    fixture.store.clone() as Arc<dyn AgentStore>,
                    permit,
                )),
                &mut authority_scope,
            );
            assert_eq!(
                authority.bind_fresh_native_scope(scope).await.unwrap_err(),
                InProcessNativeBindError::ScopeRejected
            );
        }

        let fixture = Fixture::new();
        let permit = fixture.permit();
        let mut authority_scope = ();
        let authority = InProcessEffectAuthority::new(
            Arc::new(AgentPermitState::new(
                fixture.store.clone() as Arc<dyn AgentStore>,
                permit,
            )),
            &mut authority_scope,
        );
        let session_scope = NativeExecutionScope::fresh(
            "http:test/model",
            "a".repeat(64),
            "b".repeat(64),
            Some("logical-session".into()),
        )
        .unwrap();
        assert_eq!(
            authority
                .bind_fresh_native_scope(session_scope)
                .await
                .unwrap_err(),
            InProcessNativeBindError::UnsupportedScope
        );
        let resumed_scope = NativeExecutionScope::resumed(
            "http:test/model",
            "a".repeat(64),
            "b".repeat(64),
            "logical-session",
            ResumeSessionProof::derive(OWNER, "logical-session", "opaque-native-session").unwrap(),
        )
        .unwrap();
        assert_eq!(
            authority
                .bind_fresh_native_scope(resumed_scope)
                .await
                .unwrap_err(),
            InProcessNativeBindError::UnsupportedScope
        );
    }

    #[tokio::test]
    async fn bound_native_authority_rejects_invalid_and_unassembled_effects_before_store() {
        let fixture = Fixture::new();
        let permit = fixture.permit();
        let mut authority_scope = ();
        let authority = InProcessEffectAuthority::new(
            Arc::new(AgentPermitState::new(
                fixture.store.clone() as Arc<dyn AgentStore>,
                permit,
            )),
            &mut authority_scope,
        );
        let scope =
            NativeExecutionScope::fresh("http:test/model", "a".repeat(64), "b".repeat(64), None)
                .unwrap();
        let native = authority.bind_fresh_native_scope(scope).await.unwrap();
        fixture.clock.advance(31);

        let invalid = native
            .revalidate(NativeSideEffect::ModelSend {
                turn: 0,
                wire_attempt: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(invalid.kind, ErrorKind::Config);
        let unsupported = native
            .revalidate(NativeSideEffect::CheckpointPrepare { sequence: 1 })
            .await
            .unwrap_err();
        assert_eq!(unsupported.kind, ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn paired_components_execute_and_settle_with_same_backend() {
        const TEST_OWNER: &str = "owner-paired-success";
        let fixture = Fixture::new();
        fixture.enqueue_for(TEST_OWNER, "success");
        let operation = operation(TEST_OWNER, false);
        let components = InProcessAgentComponents::new(
            TEST_OWNER,
            fixture.store.clone() as Arc<dyn AgentStore>,
            operation.clone(),
        )
        .unwrap();
        let report = components
            .execution_driver(
                "lease-test",
                AgentExecutionOptions {
                    batch_limit: 1,
                    max_in_flight: 1,
                    lease_seconds: 10,
                    heartbeat_interval: Duration::from_secs(1),
                },
            )
            .unwrap()
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::Settled]);
        assert_eq!(operation.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .store
                .get_run(TEST_OWNER, "run-success")
                .unwrap()
                .unwrap()
                .state,
            RunState::Succeeded
        );
        assert!(
            components
                .backend
                .state
                .registry
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
    }

    #[tokio::test]
    async fn paired_components_recover_exact_staged_completion_with_injected_sink() {
        const TEST_OWNER: &str = "owner-sink-recovery";
        let fixture = Fixture::new();
        fixture.enqueue_for(TEST_OWNER, "sink-recovery");
        let claim = fixture
            .store
            .claim_due(
                TEST_OWNER,
                vyane_agent::ExecutionBackend::NativeInProcess,
                "executor",
                1,
                1,
            )
            .unwrap()
            .remove(0);
        let started = fixture
            .store
            .start(
                TEST_OWNER,
                &claim.receipt,
                &ControllerRef {
                    kind: ControllerKind::InProcess,
                    id: "controller-sink-recovery".into(),
                    fingerprint: Some("fingerprint-sink-recovery".into()),
                },
            )
            .unwrap();
        let permit = fixture
            .store
            .issue_execution_permit(TEST_OWNER, &started.receipt, &started.run.policy_digest)
            .unwrap();
        fixture
            .store
            .prepare_completion(
                TEST_OWNER,
                &permit,
                &NewRunCompletion {
                    id: "completion-sink-recovery".into(),
                    sink_kind: "test-sink".into(),
                    publication_key: "result.sink-recovery".into(),
                    content_digest: "c".repeat(64),
                    content_bytes: 1,
                },
            )
            .unwrap();
        let sink: Arc<dyn AgentCompletionSink> = Arc::new(ExactCompletionSink);
        let components = InProcessAgentComponents::new_with_completion_sinks(
            TEST_OWNER,
            fixture.store.clone() as Arc<dyn AgentStore>,
            operation(TEST_OWNER, false),
            vec![sink],
        )
        .unwrap();
        fixture.clock.advance(2);
        let report = components
            .recovery_driver("reconciler", AgentRecoveryOptions::default())
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentRecoveryItemStatus::CompletionRecovered]);
        assert_eq!(
            fixture
                .store
                .get_run(TEST_OWNER, "run-sink-recovery")
                .unwrap()
                .unwrap()
                .state,
            RunState::Succeeded
        );
    }

    #[tokio::test]
    async fn operation_panic_is_body_free_unknown_and_guard_is_removed() {
        const TEST_OWNER: &str = "owner-operation-panic";
        let fixture = Fixture::new();
        fixture.enqueue_for(TEST_OWNER, "panic");
        let components = InProcessAgentComponents::new(
            TEST_OWNER,
            fixture.store.clone() as Arc<dyn AgentStore>,
            operation(TEST_OWNER, true),
        )
        .unwrap();
        let report = components
            .execution_driver(
                "lease-test",
                AgentExecutionOptions {
                    batch_limit: 1,
                    max_in_flight: 1,
                    lease_seconds: 10,
                    heartbeat_interval: Duration::from_secs(1),
                },
            )
            .unwrap()
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::ControllerUnknown]);
        assert!(
            components
                .backend
                .state
                .registry
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
    }

    #[tokio::test]
    async fn caller_cancellation_drops_operation_and_exact_registry_entry() {
        const TEST_OWNER: &str = "owner-caller-cancel";
        let fixture = Fixture::new();
        fixture.enqueue_for(TEST_OWNER, "cancel");
        let entered = Arc::new(Notify::new());
        let components = Arc::new(
            InProcessAgentComponents::new(
                TEST_OWNER,
                fixture.store.clone() as Arc<dyn AgentStore>,
                Arc::new(BlockingOperation {
                    owner: TEST_OWNER,
                    entered: Arc::clone(&entered),
                }),
            )
            .unwrap(),
        );
        let cancellation = CancellationToken::new();
        let execution = {
            let components = Arc::clone(&components);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                components
                    .execution_driver(
                        "lease-test",
                        AgentExecutionOptions {
                            batch_limit: 1,
                            max_in_flight: 1,
                            lease_seconds: 10,
                            heartbeat_interval: Duration::from_secs(1),
                        },
                    )
                    .unwrap()
                    .execute_once(cancellation)
                    .await
                    .unwrap()
            })
        };
        entered.notified().await;
        cancellation.cancel();
        let report = execution.await.unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::Cancelled]);
        assert!(
            components
                .backend
                .state
                .registry
                .lock()
                .unwrap()
                .active
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dropped_stage_future_keeps_controller_present_until_blocking_stage_finishes() {
        const TEST_OWNER: &str = "owner-stage-barrier";
        let fixture = Fixture::new();
        fixture.enqueue_for(TEST_OWNER, "stage-barrier");
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let components = Arc::new(
            InProcessAgentComponents::new(
                TEST_OWNER,
                fixture.store.clone() as Arc<dyn AgentStore>,
                Arc::new(StageBlockingOperation {
                    owner: TEST_OWNER,
                    messages: MessageComponents::open(
                        &StoragePaths::from_data_dir(
                            fixture._directory.path().join("stage-barrier-data"),
                        ),
                        TEST_OWNER,
                    )
                    .unwrap(),
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            )
            .unwrap(),
        );
        let cancellation = CancellationToken::new();
        let execution = {
            let components = Arc::clone(&components);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                components
                    .execution_driver(
                        "lease-test",
                        AgentExecutionOptions {
                            batch_limit: 1,
                            max_in_flight: 1,
                            lease_seconds: 10,
                            heartbeat_interval: Duration::from_secs(1),
                        },
                    )
                    .unwrap()
                    .execute_once(cancellation)
                    .await
                    .unwrap()
            })
        };
        let entered_wait = Arc::clone(&entered);
        tokio::task::spawn_blocking(move || entered_wait.wait())
            .await
            .unwrap();
        let controller = fixture
            .store
            .get_run(TEST_OWNER, "run-stage-barrier")
            .unwrap()
            .unwrap()
            .controller
            .unwrap();
        cancellation.cancel();
        let report = execution.await.unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::Cancelled]);
        assert_eq!(
            components
                .backend
                .observe_exact(
                    tokio::time::Instant::now() + Duration::from_millis(50),
                    controller.clone(),
                )
                .await,
            ControllerRecoveryObservation::StillPresent
        );
        let release_wait = Arc::clone(&release);
        tokio::task::spawn_blocking(move || release_wait.wait())
            .await
            .unwrap();
        assert_eq!(
            components
                .backend
                .observe_exact(
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    controller,
                )
                .await,
            ControllerRecoveryObservation::Gone
        );
    }

    #[tokio::test]
    async fn duplicate_assembly_is_rejected_while_first_controller_is_active() {
        const TEST_OWNER: &str = "owner-active-duplicate";
        let fixture = Fixture::new();
        fixture.enqueue_for(TEST_OWNER, "exclusive");
        let store = fixture.store.clone() as Arc<dyn AgentStore>;
        let entered = Arc::new(Notify::new());
        let components = Arc::new(
            InProcessAgentComponents::new(
                TEST_OWNER,
                Arc::clone(&store),
                Arc::new(BlockingOperation {
                    owner: TEST_OWNER,
                    entered: Arc::clone(&entered),
                }),
            )
            .unwrap(),
        );
        let cancellation = CancellationToken::new();
        let execution = {
            let components = Arc::clone(&components);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                components
                    .execution_driver(
                        "lease-test",
                        AgentExecutionOptions {
                            batch_limit: 1,
                            max_in_flight: 1,
                            lease_seconds: 10,
                            heartbeat_interval: Duration::from_secs(1),
                        },
                    )
                    .unwrap()
                    .execute_once(cancellation)
                    .await
                    .unwrap()
            })
        };
        entered.notified().await;
        assert_eq!(
            InProcessAgentComponents::new(TEST_OWNER, store, operation(TEST_OWNER, false),)
                .unwrap_err(),
            InProcessAssemblyError::DuplicateAssembly
        );
        cancellation.cancel();
        assert_eq!(
            execution.await.unwrap().items,
            [AgentExecutionItemStatus::Cancelled]
        );
    }
}
