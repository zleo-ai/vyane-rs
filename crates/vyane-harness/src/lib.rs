//! # vyane-harness
//!
//! CLI harness adapters implementing [`vyane_core::Harness`]: Claude Code, the
//! Codex CLI, a Cursor CLI one-round JSON adapter, a Grok Build one-round JSON
//! adapter, a Kimi Code one-shot `stream-json` adapter, an OpenCode one-round
//! JSON adapter, a CodeBuddy one-shot JSON adapter, and a caller-constructed
//! generic CLI template adapter. Each
//! wraps a command as a headless one-shot
//! execution shell — a scrubbed child environment built strictly through
//! [`vyane_core::EnvPolicy`], a process-group spawn so cancel/timeout kills the
//! whole tree (not just the direct child), and, for the named coding CLIs,
//! machine-readable output parsed down to the final answer, native session id,
//! and usage. [`GenericCliHarness`] keeps the child's stdout as plain text and
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
//! [`OpencodeHarness`] invokes `opencode run <prompt> --format json` for one
//! shot. The prompt is an argument and stdin stays closed. `--model` comes
//! from the job model, or from `params.extra["model"]` when the job model is
//! empty. A provider string in `params.extra` is joined as `provider/model`
//! when the model does not already contain `/`. `job.resume` is `--session`.
//! No sandbox tier adds a flag, and no environment name is removed after the
//! policy is applied. An endpoint on the job is not injected and is not
//! refused. NDJSON `type=text` parts, plus non-JSON lines other than a
//! `Session:` fallback, are the answer. `sessionID` is the session id.
//! `type=error` and `event_error` fail the run. `type=step_finish` token
//! input and output map onto [`vyane_core::Usage`]; `total` is not a field of
//! that type. Callers construct this adapter directly. The assembler support
//! matrix stays Claude Code and Codex CLI. Auto, failover, and broadcast do
//! not gain an OpenCode default pool. It is not listed in the endpoint table
//! above: there is no documented environment override to inject or remove.
//! [`CodebuddyHarness`] invokes `codebuddy -p <prompt> --model <id>
//! --output-format json --setting-sources user` for one shot. The prompt is
//! an argument and stdin stays closed. The model is the job model when that
//! id is non-empty, otherwise a non-empty `params.extra["model"]`, otherwise
//! `deepseek-v4.1-flash`. `Write` passes `--permission-mode acceptEdits`.
//! `Full` passes `-y`. `ReadOnly` passes no sandbox flag. `job.resume` is
//! `--resume` when the id is non-empty. Optional extras are `effort`,
//! `allowed_tools`, `disallowed_tools`, `add_dir`, and `fallback_model`.
//! `fallback_model` is omitted unless the caller supplied a truthy value.
//! Every other extra key is ignored, including `owner_user_id`, `bound_owner`,
//! `run_id`, `task_type`, and `chain_index`. No environment name is removed
//! after the env policy is applied, and an endpoint on the job is not
//! injected. On Windows, an npm `codebuddy.cmd` forwarding shim is unwrapped
//! to `node.exe` plus its script. SGR color is stripped and only the first
//! JSON value is decoded. A later JSON document fails the run. The last
//! `type=result` event supplies the answer, `session_id` or `sessionId`, and
//! usage `input_tokens` and `output_tokens`. `is_error` true, or a `subtype`
//! other than `success`, fails the run. Truncated JSON and a missing result
//! fail closed. The harness kind is [`vyane_core::HarnessKind::Other`] with token
//! `codebuddy`. Callers construct this adapter directly. The assembler
//! support matrix stays Claude Code and Codex CLI. Auto, failover, and
//! broadcast do not gain a CodeBuddy default pool.
//! [`GenericCliHarness`] runs one caller-supplied command. Its argv template
//! substitutes `{task}`, `{workdir}`, `{sandbox}`, `{session_id}`, and
//! `params.extra` keys other than those four. The whole stdout is the answer.
//! It does not read an endpoint. Callers construct it directly. Provider
//! command configuration and the auto, failover, and broadcast pools do not
//! select it.
//!
//! [`readonly_view`] builds a bubblewrap argv prefix that bind-mounts the host
//! read-only, with explicit writable directories and hidden-path mounts. No
//! adapter calls it. Grok Build still refuses a read-only sandbox before
//! spawn, and Claude Code and Codex are unchanged.
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
mod codebuddy;
mod codex_cli;
mod cursor_cli;
mod generic;
mod grok_build;
mod kimi_code;
pub mod native;
mod opencode;
mod parse;
pub mod presence;
mod probe;
pub mod readonly_view;
mod spawn;

pub use claude_code::ClaudeCodeHarness;
pub use codebuddy::CodebuddyHarness;
pub use codex_cli::CodexCliHarness;
pub use cursor_cli::CursorCliHarness;
pub use generic::{GenericCliHarness, GenericCliOutput};
pub use grok_build::GrokBuildHarness;
pub use kimi_code::KimiCodeHarness;
pub use opencode::OpencodeHarness;
