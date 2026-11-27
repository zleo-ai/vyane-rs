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
//! before spawn and does not inject a credential:
//!
//! | purpose | Claude Code | Codex CLI | Cursor CLI |
//! |---------|-------------|-----------|------------|
//! | base URL | `ANTHROPIC_BASE_URL` | (via `-c model_providers.<name>.base_url`) | refused before spawn; `CURSOR_API_ENDPOINT` is removed |
//! | auth (Bearer) | `ANTHROPIC_AUTH_TOKEN` | `OPENAI_API_KEY` | not injected; `CURSOR_API_KEY` is removed |
//! | auth (x-api-key) | `ANTHROPIC_API_KEY` | `OPENAI_API_KEY` | not injected; `CURSOR_API_KEY` is removed |
//! | model | `ANTHROPIC_MODEL` (+ `--model`) | `--model` | `--model` |
//!
//! [`CursorCliHarness`] invokes the Windows standalone `cursor-agent` binary
//! for one print-mode round. Read-only is `-p --output-format json --mode ask`.
//! `Full` stays `-p --output-format json --sandbox disabled --force`. There is
//! no `agent` subcommand. It maps that single result object onto
//! [`vyane_core::HarnessOutcome`]. [`vyane_core::Sandbox::Write`] is refused
//! before spawn. `CURSOR_API_KEY` and `CURSOR_API_ENDPOINT` are removed after
//! the child environment is built, so the CLI uses the local subscription
//! login. A non-empty endpoint base URL is also refused before spawn. It does
//! not start `agent acp`. The service assembler support matrix stays Claude
//! Code and Codex CLI; callers construct this adapter directly. Auto,
//! failover, and broadcast do not gain a Cursor default pool.
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
//!
//! Same-machine session presence is a separate JSON table in [`presence`].
//! [`native::NativePresence`] updates that table in this process. It does not
//! spawn a helper, and it does not change the Claude Code or Codex adapters.

mod claude_code;
mod codex_cli;
mod cursor_cli;
pub mod native;
mod parse;
pub mod presence;
mod probe;
mod spawn;

pub use claude_code::ClaudeCodeHarness;
pub use codex_cli::CodexCliHarness;
pub use cursor_cli::CursorCliHarness;
