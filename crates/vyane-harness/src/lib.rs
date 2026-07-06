//! # vyane-harness
//!
//! CLI harness adapters implementing [`vyane_core::Harness`]: Claude Code and
//! the Codex CLI. Each wraps a coding CLI as a headless one-shot execution
//! shell — a scrubbed child environment built strictly through
//! [`vyane_core::EnvPolicy`], a process-group spawn so cancel/timeout kills the
//! whole tree (not just the direct child), and machine-readable output parsed
//! down to the final answer + native session id + usage.
//!
//! ## Environment variable names (endpoint injection)
//!
//! When a job carries an [`vyane_core::Endpoint`] override, the harness injects
//! the base URL, credential, and model into the child env under the names each
//! CLI reads. These names are the public, documented ones:
//!
//! | purpose | Claude Code | Codex CLI |
//! |---------|-------------|-----------|
//! | base URL | `ANTHROPIC_BASE_URL` | (via `-c model_providers.<name>.base_url`) |
//! | auth (Bearer) | `ANTHROPIC_AUTH_TOKEN` | `OPENAI_API_KEY` |
//! | auth (x-api-key) | `ANTHROPIC_API_KEY` | `OPENAI_API_KEY` |
//! | model | `ANTHROPIC_MODEL` (+ `--model`) | `--model` |
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
mod parse;
mod probe;
mod spawn;

pub use claude_code::ClaudeCodeHarness;
pub use codex_cli::CodexCliHarness;
