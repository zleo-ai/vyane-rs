//! # vyane-harness
//!
//! CLI harness adapters implementing [`vyane_core::Harness`]: Claude Code, the
//! Codex CLI, and a Cursor CLI one-round JSON adapter. Each wraps a coding CLI
//! as a headless one-shot execution shell — a scrubbed child environment built
//! strictly through [`vyane_core::EnvPolicy`], a process-group spawn so
//! cancel/timeout kills the whole tree (not just the direct child), and
//! machine-readable output parsed down to the final answer + native session id
//! + usage.
//!
//! ## Environment variable names (endpoint injection)
//!
//! When a job carries an [`vyane_core::Endpoint`] override, Claude Code and the
//! Codex CLI inject the base URL, credential, and model into the child env
//! under the public documented names. Cursor CLI refuses a non-empty base URL
//! before spawn and injects `CURSOR_API_KEY` only when that URL is empty:
//!
//! | purpose | Claude Code | Codex CLI | Cursor CLI |
//! |---------|-------------|-----------|------------|
//! | base URL | `ANTHROPIC_BASE_URL` | (via `-c model_providers.<name>.base_url`) | refused before spawn |
//! | auth (Bearer) | `ANTHROPIC_AUTH_TOKEN` | `OPENAI_API_KEY` | `CURSOR_API_KEY` when the base URL is empty |
//! | auth (x-api-key) | `ANTHROPIC_API_KEY` | `OPENAI_API_KEY` | `CURSOR_API_KEY` when the base URL is empty |
//! | model | `ANTHROPIC_MODEL` (+ `--model`) | `--model` | `--model` |
//!
//! [`CursorCliHarness`] invokes the existing `agent` binary for one print-mode
//! round (`-p --output-format json --sandbox disabled --force`) and maps that
//! single result object onto [`vyane_core::HarnessOutcome`]. [`vyane_core::Sandbox::ReadOnly`]
//! and [`vyane_core::Sandbox::Write`] are refused before spawn: documented print mode can
//! still widen those requests through global `approvalMode`, project allow
//! rules, and trusted-workspace hooks. A non-empty endpoint base URL is also
//! refused before spawn. It does not start `agent acp`. The service assembler
//! support matrix stays Claude Code and Codex CLI; callers construct this
//! adapter directly.
//!
//! When the endpoint is `None` the harness authenticates natively and injects
//! nothing for auth — the scrubbed baseline is all the child sees.
//!
//! ## Error classification
//!
//! Failures map onto [`vyane_core::ErrorKind`] exactly:
//! binary missing/not executable → `SpawnFailed`; ran but exited non-zero →
//! `HarnessFailed`; `job.timeout` elapsed → `Timeout`; cancelled via the token
//! → `Cancelled`.
//!
//! See `docs/plan/WP-03.md` for the full work-package specification.

mod claude_code;
mod codex_cli;
mod cursor_cli;
pub mod native;
mod parse;
mod probe;
mod spawn;

pub use claude_code::ClaudeCodeHarness;
pub use codex_cli::CodexCliHarness;
pub use cursor_cli::CursorCliHarness;
