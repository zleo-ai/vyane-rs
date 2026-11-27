//! # vyane-harness
//!
//! CLI harness adapters implementing [`vyane_core::Harness`]: Claude Code, the
//! Codex CLI, a Cursor CLI one-round JSON adapter, a Grok Build one-round JSON
//! adapter, a Kimi Code one-shot `stream-json` adapter, and a caller-constructed
//! generic CLI template adapter. Each wraps a command as a headless one-shot
//! execution shell — a scrubbed child environment built strictly through
//! [`vyane_core::EnvPolicy`], a process-group spawn so cancel/timeout kills the
//! whole tree (not just the direct child), and, for the named coding CLIs,
//! machine-readable output parsed down to the final answer + native session id
//! + usage. [`GenericCliHarness`] keeps the child's stdout as plain text and
//! reports neither a session id nor usage.
//!
//! ## Environment variable names (endpoint injection)
//!
//! When a job carries an [`vyane_core::Endpoint`] override, Claude Code and the
//! Codex CLI inject the base URL, credential, and model into the child env
//! under the public documented names. Cursor CLI refuses a non-empty base URL
//! before spawn and does not inject a credential:
//!
//! | purpose | Claude Code | Codex CLI | Cursor CLI | Grok Build |
//! |---------|-------------|-----------|------------|------------|
//! | base URL | `ANTHROPIC_BASE_URL` | (via `-c model_providers.<name>.base_url`) | refused before spawn; `CURSOR_API_ENDPOINT` is removed | refused before spawn |
//! | auth (Bearer) | `ANTHROPIC_AUTH_TOKEN` | `OPENAI_API_KEY` | not injected; `CURSOR_API_KEY` is removed | refused before spawn; `XAI_API_KEY` is removed |
//! | auth (x-api-key) | `ANTHROPIC_API_KEY` | `OPENAI_API_KEY` | not injected; `CURSOR_API_KEY` is removed | refused before spawn; `XAI_API_KEY` is removed |
//! | model | `ANTHROPIC_MODEL` (+ `--model`) | `--model` | `--model` | `-m` |
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
//! [`GrokBuildHarness`] invokes the existing `grok` binary for one POSIX JSON
//! round (`--prompt-file /dev/stdin --output-format json`) and maps `text`,
//! `sessionId`, and `usage` onto [`vyane_core::HarnessOutcome`]. The prompt is
//! the stdin payload, never an argv token. [`vyane_core::Sandbox::ReadOnly`] is
//! refused before spawn: vendor sandbox flags are not enforcement, and this
//! adapter does not build a bubblewrap read-only view, so the bare CLI is not
//! started. `Write` and `Full` spawn with no sandbox flag. A non-empty endpoint
//! base URL or any endpoint credential is refused before spawn, and
//! `XAI_API_KEY` is removed from the child environment. Callers construct this
//! adapter directly. The assembler support matrix stays Claude Code and Codex
//! CLI.
//! [`KimiCodeHarness`] invokes the existing `kimi` binary for one
//! `stream-json` round (`kimi -p <prompt> --output-format stream-json`, with
//! optional `-m` and `-S`). The prompt is an argument. There is no stdin
//! prompt and no `--prompt-file` flag. Sandbox tiers add no flag. Assistant
//! `content` is the answer. The session id is the last
//! `session.resume_hint`. Token usage stays empty. `KIMI_API_KEY` and
//! `KIMI_MODEL_API_KEY` are removed from the child environment. An endpoint
//! base URL or credential is not copied into the environment or the argument
//! list. Callers construct this adapter directly. Auto, failover, and
//! broadcast do not gain a Kimi Code default pool.
//! [`GenericCliHarness`] runs one caller-supplied command. Its argv template
//! substitutes `{task}`, `{workdir}`, `{sandbox}`, `{session_id}`, and
//! `params.extra` keys other than those four. The whole stdout is the answer.
//! It does not read an endpoint. Callers construct it directly. Provider
//! command configuration and the auto, failover, and broadcast pools do not
//! select it.
//!
//! When the endpoint is `None`, Claude Code and the Codex CLI authenticate
//! natively and inject nothing for auth — the scrubbed baseline is all the
//! child sees. The generic adapter never injects endpoint auth.
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
mod generic;
mod grok_build;
mod kimi_code;
pub mod native;
mod parse;
pub mod presence;
mod probe;
mod spawn;

pub use claude_code::ClaudeCodeHarness;
pub use codex_cli::CodexCliHarness;
pub use cursor_cli::CursorCliHarness;
pub use generic::{GenericCliHarness, GenericCliOutput};
pub use grok_build::GrokBuildHarness;
pub use kimi_code::KimiCodeHarness;
