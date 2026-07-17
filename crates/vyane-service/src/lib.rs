//! # vyane-service
//!
//! The shared service layer that sits between the dispatch kernel and every
//! protocol front-end (CLI, REST API, MCP server). It owns four things that
//! were previously private to the CLI binary:
//!
//! 1. **Config loading** — reading layered TOML files plus a secrets file into
//!    a [`LoadedConfig`] that carries an env-lookup closure (secrets file wins
//!    over real process env).
//! 2. **Selector resolution** — turning a raw selector string (a profile name
//!    or a `provider/model` pair) into a `Vec<BoundTarget>` failover chain.
//!    This is the single chokepoint the dispatch path, the detached worker, and
//!    the workflow engine all share.
//! 3. **High-level operations** — [`VyaneService::dispatch`],
//!    [`VyaneService::broadcast`], source-compatible durable queries, and the
//!    allowlisted `history_views` / `session_views` plus owner-local session
//!    inspect/reset control used by generic front-ends.
//! 4. **Explicit optional components** — owner-bound message delivery and
//!    AgentRun lifecycle projection remain opt-in and do not start resident
//!    work during ordinary dispatch.
//! 5. **Safe diagnostics** — [`VyaneService::route_preview`] and
//!    [`VyaneService::check_config`], which expose bounded, static-only DTOs
//!    without serializing paths, endpoints, credentials, prompts, or raw errors.
//!
//! The CLI is now a thin assembler: it parses arguments, calls into this crate,
//! and formats output. The REST API and MCP server will do the same, sharing
//! identical resolution and dispatch semantics.

mod agent;
mod agent_completion;
mod agent_completion_publisher;
mod agent_execution;
mod agent_recovery;
mod agent_supervisor;
mod config;
mod diagnostics;
mod factory;
mod goal;
mod goal_observation;
mod inprocess_agent;
mod message;
mod native_authority;
mod owner;
mod routing;
mod selector;
mod service;
mod task;

pub use agent::AgentProjectionComponents;
pub use agent_completion::{
    AgentCompletionSink, AgentCompletionSinkObservation, AgentCompletionSinkTransition,
    MESSAGE_COMPLETION_PRODUCER, MESSAGE_COMPLETION_SINK_KIND, MessageAgentCompletionSink,
    MessageAgentCompletionSinkConfigError, message_run_completion,
};
pub use agent_completion_publisher::{
    AgentCompletionProjectionReport, AgentCompletionProjectionStatus, AgentCompletionPublisher,
    AgentCompletionPublisherError, AgentCompletionPublisherOptions,
};
pub use agent_execution::{
    AgentExecutionContext, AgentExecutionError, AgentExecutionIdentity, AgentExecutionItemStatus,
    AgentExecutionOptions, AgentExecutionReport, AgentExecutionSettlement, AgentExecutorOutcome,
    AgentRunExecutionDriver, AgentRunExecutor, StagedRunCompletion,
};
pub use agent_recovery::{
    AgentControllerAdapter, AgentRecoveryError, AgentRecoveryItemStatus, AgentRecoveryOptions,
    AgentRecoveryReport, AgentRunRecoveryDriver, ControllerRecoveryContext,
    ControllerRecoveryObservation,
};
pub use agent_supervisor::{
    AgentExecutionLaneExit, AgentSupervisorError, AgentSupervisorExit, AgentSupervisorLoopExit,
    AgentSupervisorOptions, ResidentAgentBackend, ResidentAgentExecutionLane, ResidentAgentHost,
    ResidentAgentHostBackend, ResidentAgentHostExit, ResidentAgentSupervisor,
    ResidentInProcessAgentSupervisor,
};
pub use config::{LoadedConfig, Runtime, StoragePaths, load_config};
pub use diagnostics::{
    ConfigCheckReport, ConfigCheckStatus, ConfigIssue, ConfigIssueCode, CredentialStatus,
    DIAGNOSTIC_MAX_CONFIG_ITEMS, DIAGNOSTIC_MAX_ENDPOINT_BYTES, DIAGNOSTIC_MAX_FAILOVER_LEGS,
    DIAGNOSTIC_MAX_METADATA_BYTES, DIAGNOSTIC_MAX_METADATA_DEPTH, DIAGNOSTIC_MAX_METADATA_ITEMS,
    DIAGNOSTIC_MAX_OUTPUT_BYTES, DiagnosticError, DiagnosticErrorKind, ProfileCheck,
    ProfileCheckStatus, ProviderCheck, ROUTE_PREVIEW_MAX_LIST_ITEMS, ROUTE_PREVIEW_MAX_SIGNAL,
    ROUTE_PREVIEW_MAX_TASK_BYTES, ROUTE_PREVIEW_MAX_VALUE_BYTES, RoutePreview, RoutePreviewParams,
    RouteSelectionBasis,
};
pub use factory::{AssemblerFactory, authorized_native_client, direct_http_client};
pub use goal::{
    GOAL_NEXT_ACTION_VIEW_SCHEMA, GoalNextActionKind, GoalNextActionView, GoalNextReasonCode,
    GoalOperatorCommand, GoalReadError, GoalReadService, GoalSignalKind,
};
pub use goal_observation::{
    GoalObservation, GoalObservationIngress, GoalObservationIngressError, GoalObservationKind,
    GoalObservationReceipt, GoalObservationRunner, GoalObservationRunnerError,
    GoalObservationSignalKind, GoalObservationSink, GoalObservationStatus, GoalObservationTarget,
    GoalObservationWatchContext, GoalObservationWatchPolicy, GoalObservationWatchReport,
    GoalObservationWatchStatus, GoalObservationWatcher, GoalObservationWatcherError,
    GoalObservationWatcherErrorCode, MAX_GOAL_OBSERVATION_CONCURRENCY,
    MAX_GOAL_OBSERVATION_WATCHERS, MAX_GOAL_OBSERVATIONS_PER_WATCHER,
};
pub use inprocess_agent::{
    InProcessAgentComponents, InProcessAgentEffect, InProcessAgentOperation,
    InProcessAgentOperationContext, InProcessAssemblyError, InProcessAuthorityError,
    InProcessCompletionError, InProcessCompletionStageError, InProcessEffectAuthority,
    InProcessEffectPermit, InProcessNativeAuthority, InProcessNativeBindError,
    InProcessPreparedCompletion,
};
pub use message::{
    AgentMessageCompletionReadError, AgentMessageCompletionStageError, MessageComponents,
};
pub use native_authority::AgentRunModelToolAuthority;
pub use owner::{
    AuthenticatedPrincipal, OwnerContext, OwnerContextError, OwnerContextFactory,
    PrincipalAuthenticator, PrincipalOwnerResolver,
};
pub use routing::{
    DispatchPlan, RouteParams, RouteResult, plan_dispatch, replay_recorded_auto_chain, route_task,
    validate_auto_route_candidates,
};
pub use selector::{resolve_target_chain, split_targets};
pub use service::{
    BroadcastParams, DispatchParams, HistoryFilter, OwnerScopedService, PreparedHarnessDispatch,
    RUN_VIEW_SCHEMA, RunAttemptOutcomeView, RunAttemptView, RunView, SESSION_VIEW_SCHEMA,
    SessionNativeState, SessionView, VyaneService,
};
pub use task::{build_task_spec, parse_labels, validate_user_routing_labels};
