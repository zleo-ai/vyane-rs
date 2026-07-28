//! Provider-neutral building blocks for Vyane's in-process native harness.
//!
//! This module deliberately does not alter the existing Claude Code or Codex
//! CLI wrappers. It establishes the first executable native-harness seam: a
//! model-produced [`ToolCall`] passes through an ordered [`PermissionPolicy`]
//! and then, only when allowed, into a real [`ToolRegistry`] executor.
//!
//! Permission matching is not an OS sandbox. The trusted read/search built-ins
//! add a narrower Linux descriptor-relative filesystem capability and recheck
//! live authority at each open; that does not authorize subprocesses or network
//! access. [`risky_operations_policy`] remains only an approval classifier, and
//! [`protected_paths_policy`] continues to deny `run_bash` until a separately
//! enforced child-process sandbox exists.

mod permissions;
mod text_edit;
mod tools;
mod trusted_files;
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
pub use trusted_files::{
    NativeReadPolicy, NativeReadPolicyError, read_only_permission_policy,
    read_only_tool_definitions, read_only_tool_registry, read_only_tool_registry_with_policy,
};
pub use turn_driver::{
    DEFAULT_NATIVE_MODEL_TURNS, MAX_NATIVE_MODEL_TURNS, NativeAssistantReply, NativeTurnDriver,
    NativeTurnLimitError, NativeTurnLimits, NativeTurnOutcome, NativeTurnStop,
};
