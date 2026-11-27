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
//! This crate contains types, traits and pure helpers, plus the [`securefile`],
//! [`localipc`], and [`proc`] primitives. Those primitives are not used by the
//! kernel, the ledger, the CLI, or the harness spawn path. Runtime orchestration
//! lives in the sibling crates
//! (`vyane-kernel`, `vyane-protocol`, `vyane-harness`, `vyane-ledger`, …).

pub mod chat;
pub mod cost;
pub mod env;
pub mod error;
pub mod identifier;
pub mod localipc;
pub mod native_authority;
pub mod path_map;
pub mod pathsformats;
pub mod proc;
pub mod receipt;
pub mod run;
pub mod securefile;
pub mod session;
pub mod target;
pub mod task;
pub mod tool_chat;
pub mod traits;
pub mod web_fetch;
pub mod web_search;
pub mod workdir;

pub use chat::{ChatMessage, ChatOutcome, ChatRequest, Role, StreamEvent};
pub use cost::{ModelTokenRate, estimate_configured_cost};
pub use env::{BASELINE_ENV, EnvPolicy, InheritMode};
pub use error::{ErrorKind, Result, VyaneError};
pub use identifier::{
    IdentifierProblem, InvalidIdentifier, identifier_problem, validate_identifier,
};
pub use localipc::{
    AsWaitable, DuplicateSocketError, LocalIpcError, LoopbackDatagramListener, LoopbackError,
    PORT_FILE_MAGIC, duplicate_socket, send_loopback_datagram, wait_readable,
};
pub use native_authority::{NativeExecutionAuthority, NativeSideEffect};
pub use path_map::{
    DisplayPathAliases, LocalhostMapping, PathIdentity, checkout_identity_key, default_wsl_distro,
    display_path_aliases, identify_path, map_localhost_url, parse_gitdir_pointer,
    paths_refer_to_same_location, probe_checkout_identity, worktree_common_git_dir,
};
pub use pathsformats::{
    local_locator_kind, read_text_exact, strip_extended_path_prefix, windows_filename_problem,
};
pub use proc::{
    CTRL_BREAK_EVENT, CTRL_C_EVENT, JobObject, ProcError, SIGINT, SIGKILL, SIGTERM, SpawnKwargs,
    StdioMode, TREE_KILLED_EXIT_CODE, TrackedProcess, UnsupportedOnWindows,
    WINDOWS_COMPLETED_TURN_EXIT_CODE, add_loop_signal_handler, available_signals, boot_id,
    contain_process, contained_spawn_kwargs, create_subprocess_exec_contained, kill_exact_process,
    kill_process_tree, pid_running, popen_contained, probe_pid, process_job,
    remove_loop_signal_handler, signal_process_tree, terminate_completed_turn,
    terminate_process_tree, windows_process_start_time,
};
pub use receipt::{
    AttemptFailureClass, AttemptStatus, BillingModeCategory, CompletionReceipt, CostEvidence,
    EndpointClass, GATE_CI_PACKAGING, GATE_INDEPENDENT_REVIEW, GATE_INTEGRATION, GATE_TRUTH_PROBE,
    GATE_UNIT, GateOutcome, GateResult, MemoryReceiptLedger, NamedGate, RECEIPT_SCHEMA_VERSION,
    ReceiptAttempt, ReceiptError, ReceiptFinalStatus, ReceiptResult, RecoveryCleanupState,
    ReviewFindingDisposition, RiskClass, RouteConfig, TaskCase,
};
pub use run::{Attempt, AttemptOutcome, RunQuery, RunRecord, RunStatus, Usage};
pub use securefile::{
    IS_WINDOWS, LinkStat, OpenFlags, PRIVATE_DIR_MODE, PRIVATE_FILE_MODE, PinnedDirectory,
    PrivateProblem, SecureFileError, SecureTarget, StatSnapshot, current_user_id, fsync_directory,
    is_link_stat, make_private, make_private_fd, open_nofollow, owned_by_current_user,
    pin_directory, private_problem, replace,
};
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
pub use workdir::{
    HOST_CONTROL_COMPONENTS, HOST_CONTROL_FILE_NAMES, PinnedWorkdir, WorkdirIdentity,
    is_host_control_component,
};

/// Re-exported so downstream crates use the same cancellation primitive.
pub use tokio_util::sync::CancellationToken;
