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
pub mod run;
pub mod session;
pub mod target;
pub mod task;
pub mod traits;

pub use chat::{ChatMessage, ChatOutcome, ChatRequest, Role, StreamEvent};
pub use env::{BASELINE_ENV, EnvPolicy, InheritMode};
pub use error::{ErrorKind, Result, VyaneError};
pub use run::{Attempt, AttemptOutcome, RunQuery, RunRecord, RunStatus, Usage};
pub use session::{SessionRecord, SessionRef};
pub use target::{
    AdapterTransport, AuthMaterial, AuthStyle, BoundTarget, Endpoint, HarnessKind, ModelId,
    Protocol, ProviderId, Sandbox, Secret, Target,
};
pub use task::{Effort, GenParams, TaskSpec};
pub use traits::{
    ChatClient, Harness, HarnessJob, HarnessOutcome, HarnessStreamEvent, Ledger, SessionStore,
};

/// Re-exported so downstream crates use the same cancellation primitive.
pub use tokio_util::sync::CancellationToken;
