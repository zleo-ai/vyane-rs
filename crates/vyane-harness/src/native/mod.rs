//! Provider-neutral building blocks for Vyane's in-process native harness.
//!
//! This module deliberately does not alter the existing Claude Code or Codex
//! CLI wrappers. It establishes the first executable native-harness seam: a
//! model-produced [`ToolCall`] passes through an ordered [`PermissionPolicy`]
//! and then, only when allowed, into a real [`ToolRegistry`] executor.
//!
//! Permission matching is not an OS sandbox. In particular,
//! [`risky_operations_policy`] is only an approval classifier and does not
//! include the protected-path security floor. Until path capabilities are
//! enforced below the shell, [`protected_paths_policy`] denies `run_bash`
//! entirely; native tools must independently resolve paths and reject symlink
//! or capability escapes before performing side effects.

mod permissions;
mod text_edit;
mod tools;
mod turn_driver;

pub use permissions::{
    ApprovalPlan, PermissionDecision, PermissionEffect, PermissionPolicy, PermissionRule,
    PermissionRuleError, protected_paths_policy, risky_operations_policy,
};
pub use text_edit::{
    EditError, EditOutcome, EditRequest, MatchPass, MatchSearch, ReplacedSpan, compute_edit, locate,
};
pub use tools::{
    MAX_TOOL_OUTPUT_CHARS, NativeTool, ToolCall, ToolCallLimits, ToolContext, ToolContextError,
    ToolError, ToolInvocation, ToolInvocationStatus, ToolRegistry, ToolRegistryError,
};
pub use turn_driver::{
    DEFAULT_NATIVE_MODEL_TURNS, MAX_NATIVE_MODEL_TURNS, NativeAssistantReply, NativeTurnDriver,
    NativeTurnLimitError, NativeTurnLimits, NativeTurnOutcome, NativeTurnStop,
};
