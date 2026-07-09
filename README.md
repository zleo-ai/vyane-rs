# Vyane

**A multi-model agent-orchestration kernel, in Rust.**

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
&nbsp;·&nbsp; [简体中文](README.zh-CN.md)

Vyane is one kernel — and one CLI over it — for dispatching, broadcasting and
failing over tasks across *both* coding-agent harnesses (Claude Code, Codex
CLI, …) and raw HTTP model endpoints. Point it at a task and a target and it
runs it; point it at several targets and it fans out or fails over. Whether a
run happens inside a coding CLI with a filesystem and tools, or as a plain
chat completion over HTTP, is one field on the target — not a different tool,
a different config file, or a different mental model.

The differentiator is the **four-layer target model**. *Provider*, *protocol*,
*harness* and *model* are independent axes and are never conflated. A provider
is who supplies the endpoint, key and billing; a protocol is the wire format; a
harness is the execution shell (or none, for direct chat); a model is the
inference model. So: **a relay is not a protocol** (it is a provider that
happens to speak one), **a coding CLI is not a provider** (it is a harness that
still needs one), and **a model id is only valid within one provider**. These
stay four separate fields from configuration all the way into the run ledger,
which is what lets Vyane do the correct thing at the boundaries where lazier
tools quietly break.

Concretely, that buys you: **clean-env subprocess spawning by construction** —
child agents are launched from a scrubbed baseline environment, so the calling
session's credentials and base-URL overrides never leak into them; **failover
chains that never leak a model id across providers** — each element of a chain
is resolved fully and independently, so a fallback always uses the model that
belongs to the provider it runs against; and an **append-only JSONL run
ledger** that records prompt *digests* (not prompt bodies), the full attempt
trail of every dispatch, and **owner-scoped records from day one** so
multi-user isolation never needs a schema retrofit.

## Origin

`vyane-rs` is the open-source Rust implementation of Vyane, a private personal
AI-OS execution substrate that has been running real multi-model development
pipelines since early 2026. It is being rebuilt here in the open, capability by
capability, tracking the private system as it evolves — so the design reflects
what actually held up in daily use, not a greenfield guess.

## How it's built

This repo is developed by an orchestrated fleet of AI coding agents: different
frontier models write the code, adversarially cross-review each other's work,
and fix what the review turns up, all under a human-owned architecture and
integration gate. Every merge passes independent cross-model review plus the
`cargo fmt` / `clippy` / `cargo test` gates described in
[CONTRIBUTING.md](CONTRIBUTING.md) — no change lands on the say-so of the
model that wrote it.

## Status

**v0.1 + v0.2 surface complete end-to-end; v0.3 protocol front-ends (REST API +
MCP) delivered. APIs are still unstable pre-release and will change without
notice.** Every crate is implemented and covered by tests; the CLI runs real
dispatch/broadcast/failover today.

| capability | crate | state |
|------------|-------|-------|
| core type system (four-layer model, traits, errors, env policy) | `vyane-core` | [x] |
| config & profiles | `vyane-config` | [x] |
| OpenAI-Chat + Responses + Anthropic-Messages clients | `vyane-protocol` | [x] |
| Claude Code + Codex CLI harnesses | `vyane-harness` | [x] |
| dispatch / broadcast / failover kernel | `vyane-kernel` | [x] |
| JSONL ledger & sessions | `vyane-ledger` | [x] |
| declarative workflow engine (DAG + journal/resume) | `vyane-workflow` | [x] |
| detached background runs (`--detach` + `task` commands) | `vyane-cli` | [x] |
| CLI (check / dispatch / broadcast / history / sessions / workflow / task) | `vyane-cli` | [x] |
| shared service layer | `vyane-service` | [x] |
| **REST API** (`vyane serve` — dispatch/broadcast/runs/sessions/health) | `vyane-cli` + `axum` | [x] |
| **MCP server** (`vyane mcp` — dispatch/broadcast/history/sessions tools) | `vyane-mcp` + `rmcp` | [x] |
| pluggable routing | `vyane-router` | [ ] (v0.3, remaining) |

### Protocol entry points

Vyane supports three interchangeable front-ends, all sharing the same
`vyane-service` layer so dispatch semantics are identical:

| protocol | command | use case |
|----------|---------|----------|
| **CLI** | `vyane dispatch --target prod "task"` | interactive / scripted one-shot runs |
| **REST API** | `vyane serve --addr 127.0.0.1:9721` | programmatic access from any HTTP client |
| **MCP** | `vyane mcp` | let other agents (Claude, Codex, …) call vyane as a tool |

## Architecture

```
       CLI              REST API           MCP
   vyane dispatch     vyane serve       vyane mcp
        │                  │                  │
        └──────────┬───────┴──────────┬───────┘
                   ▼                  ▼
              vyane-service (shared facade)
                   │
                   ▼
            ┌─────────────┐
            │   kernel    │  resolve target chain,
            │  dispatch / │  attempt loop, failover,
            │  broadcast  │  assemble RunRecord
            └──────┬──────┘
      ┌────────────┴────────────┐
      ▼                         ▼
direct-http protocol         cli-wrap harnesses
clients (ChatClient)         (Harness, scrubbed env)
OpenAI Chat / Responses,     Claude Code, Codex CLI, …
Anthropic Messages                    │
      └────────────┬──────────────┘
                   ▼
       append-only JSONL ledger
        (digests, attempt trail)
          + session store
```

The kernel depends only on the traits and types in `vyane-core`; the concrete
clients, harnesses and ledger are assembled behind those traits in the CLI
layer. Nine crates:

| crate | responsibility |
|-------|----------------|
| `vyane-core` | four-layer target model, capability traits, error taxonomy, env policy — the shared vocabulary everything else speaks |
| `vyane-config` | TOML config + profiles; resolves a profile (and its failover chain) to a `BoundTarget` |
| `vyane-provider` | provider registry; endpoints, auth styles, per-provider env-injection rules |
| `vyane-protocol` | `ChatClient` implementations for the HTTP protocols (OpenAI Chat / Responses, Anthropic Messages) |
| `vyane-harness` | `Harness` implementations wrapping coding CLIs headlessly, with process-group control |
| `vyane-kernel` | the orchestration state machine: dispatch, broadcast, failover gating, run-record assembly |
| `vyane-ledger` | JSONL `Ledger` + filesystem `SessionStore`, cost estimation |
| `vyane-router` | target selection / routing policy (grows into pluggable routing) |
| `vyane-cli` | the assembler and entry point: wires the crates together behind a command-line UI |

## Usage

Configuration is a TOML file (a platform config directory for user defaults —
e.g. `~/Library/Application Support/vyane/config.toml` on macOS — merged with
`.vyane/config.toml` for project overrides). See
[`profiles.example.toml`](profiles.example.toml) for the full shape; a
provider and a profile look like:

```toml
[providers.anthropic]
base_url      = "https://api.anthropic.com"
api_key_env   = "ANTHROPIC_API_KEY"   # names an env var; no key material in config
auth_style    = "x_api_key"           # bearer | x_api_key
protocol      = "anthropic_messages"
default_model = "a-capable-anthropic-model"

# A named bundle of provider + protocol + harness + model → one BoundTarget.
[profiles.review]
provider = "anthropic"
protocol = "anthropic_messages"
harness  = "none"                     # "none" = direct HTTP chat, no workspace
model    = "a-capable-anthropic-model"
```

Then, from the shell:

```sh
# Validate config, resolve every profile, probe harness binaries and env vars.
vyane check

# Dispatch one task to one target (a profile name).
vyane dispatch "review this diff" --target myprofile

# Broadcast the same task to several target chains, concurrently.
vyane broadcast "compare approaches" --targets a,b,c
```

Sample `vyane check` output against a config with two providers and three
profiles (one profile's required env var deliberately left unset, to show
what a missing-key warning looks like):

```
config files:
  .vyane/config.toml (loaded)
providers:
  anthropic: anthropic_messages default_model=a-capable-anthropic-model
  openai: openai_chat default_model=a-fast-openai-model
profiles:
  builder: anthropic/a-capable-anthropic-model via claude-code (anthropic_messages)
  codex: warning: provider requires environment variable `OPENAI_API_KEY` for its API key, but it is not set
  review: anthropic/a-capable-anthropic-model (anthropic_messages)
harnesses:
  claude-code: available
  codex-cli: available
profile environment:
  builder: ANTHROPIC_API_KEY present
  codex: OPENAI_API_KEY missing
  review: ANTHROPIC_API_KEY present
```

## Design principles

- **Boring dependencies.** Widely-used, well-understood crates; no clever
  infrastructure where a plain file will do.
- **Secrets never serialize.** Credential types are non-serializable by
  construction; config stores the *name* of an env var, never the value.
- **The ledger stores digests, not prompts.** Run accounting records a
  prompt digest (and, optionally, a short preview) — not the prompt body.
- **Scrubbed child environment by default.** Harnesses spawn from a minimal
  baseline env plus an explicit per-target injection set; inheriting the full
  parent environment is opt-in.
- **No hidden timeouts.** Long agentic runs legitimately take hours; timeouts
  are opt-in per task, never a silent default.
- **Owner-aware records.** Every run and session carries an owner field from
  day one, so multi-user isolation is never a retrofit.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — the four-layer model, crate map,
  dispatch lifecycle, env policy, failover and ledger semantics.
- [Roadmap](docs/ROADMAP.md) — milestones for v0.1, v0.2, v0.3.
- [Contributing](CONTRIBUTING.md) — toolchain, checks, and PR conventions.

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache License, Version 2.0](LICENSE-APACHE) at your option. Unless you
explicitly state otherwise, any contribution you submit for inclusion is
dual-licensed as above, with no additional terms or conditions.
