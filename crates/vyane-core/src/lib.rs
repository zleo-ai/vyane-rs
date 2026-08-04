//! # vyane-core
//!
//! Core vocabulary of the Vyane orchestration kernel.
//!
//! Everything in Vyane is built on a four-layer target model. The layers are
//! deliberately independent — none of them can be derived from another:
//!
//! | layer      | question it answers                                  | examples |
//! |------------|------------------------------------------------------|----------|
//! | *provider* | who supplies the endpoint, key, quota and billing    | an official vendor account, an OpenAI-compatible relay, a cloud platform |
//! | *protocol* | what the wire format is                              | OpenAI Chat Completions, OpenAI Responses, Anthropic Messages |
//! | *harness*  | which execution shell the model works in, and hence  | Claude Code, Codex CLI, OpenCode — or none (direct HTTP chat) |
//! |            | whether it has files, shell, tools and long sessions  | |
//! | *model*    | which inference model actually runs                   | a concrete model id string |
//!
//! Conflating these layers is the root cause of most real-world breakage in
//! multi-model tooling (a relay is not a protocol; a coding CLI is not a
//! provider; a model id is only valid within one provider). Vyane keeps them
//! as separate fields from configuration all the way into the run ledger.
//!
//! This crate contains only types, traits and pure helpers. Runtime behaviour
//! lives in the sibling crates (`vyane-kernel`, `vyane-protocol`,
//! `vyane-harness`, `vyane-ledger`, …).

pub mod chat;
pub mod env;
pub mod error;
pub mod native_authority;
pub mod receipt;
pub mod run;
pub mod session;
pub mod target;
pub mod task;
pub mod tool_chat;
pub mod traits;
pub mod web_fetch;
pub mod web_search;
pub mod workdir;

pub use chat::{ChatMessage, ChatOutcome, ChatRequest, Role, StreamEvent};
pub use env::{BASELINE_ENV, EnvPolicy, InheritMode};
pub use error::{ErrorKind, Result, VyaneError};
pub use native_authority::{NativeExecutionAuthority, NativeSideEffect};
pub use receipt::{
    AttemptFailureClass, AttemptStatus, BillingModeCategory, CompletionReceipt, CostEvidence,
    EndpointClass, GATE_CI_PACKAGING, GATE_INDEPENDENT_REVIEW, GATE_INTEGRATION, GATE_TRUTH_PROBE,
    GATE_UNIT, GateOutcome, GateResult, MemoryReceiptLedger, NamedGate, RECEIPT_SCHEMA_VERSION,
    ReceiptAttempt, ReceiptError, ReceiptFinalStatus, ReceiptResult, RecoveryCleanupState,
    ReviewFindingDisposition, RiskClass, RouteConfig, TaskCase,
};
pub use run::{Attempt, AttemptOutcome, RunQuery, RunRecord, RunStatus, Usage};
pub use session::{
    NativeSessionBinding, NativeSessionDomain, NativeSessionState, NativeSessionTransition,
    SessionRecord, SessionRef, SessionSnapshot, SessionUpdate,
};
pub use target::{
    AdapterTransport, AuthMaterial, AuthStyle, BoundTarget, Endpoint, HarnessKind, ModelId,
    Protocol, ProviderId, Sandbox, Secret, Target,
};
pub use task::{
    Effort, GenParams, HarnessLifecycleEvent, HarnessLifecycleReporter, HarnessSpawnAuthority,
    TaskSpec,
};
pub use tool_chat::{
    AssistantContentPart, AssistantToolTurn, ModelToolCall, ToolCallArguments, ToolChatLimits,
    ToolChatMessage, ToolChatOutcome, ToolChatRequest, ToolChatValidationError, ToolChoice,
    ToolDefinition, ToolResultMessage, validate_conversation,
};
pub use traits::{
    AuthorizedToolChatClient, AuthorizedWebFetchClient, AuthorizedWebSearchClient, ChatClient,
    Harness, HarnessExecutionContext, HarnessJob, HarnessOutcome, HarnessStreamEvent, Ledger,
    SessionExecutionLease, SessionStore,
};
pub use web_fetch::{WebFetchOutcome, WebFetchRequest};
pub use web_search::{WebSearchContextSize, WebSearchOutcome, WebSearchRequest, WebSearchSource};
pub use workdir::{PinnedWorkdir, WorkdirIdentity};

/// Re-exported so downstream crates use the same cancellation primitive.
pub use tokio_util::sync::CancellationToken;
