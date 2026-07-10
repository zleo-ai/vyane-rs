# Contributing

Vyane is early-bootstrap and its APIs are unstable, but the bar for merged code
is already firm. Thanks for helping build it.

## Toolchain

- **Stable Rust**, **edition 2024** (see `rust-toolchain.toml`; MSRV is pinned
  in the workspace `Cargo.toml`).
- Install the pinned toolchain with `rustup` — it will pick up
  `rust-toolchain.toml` and the `rustfmt` + `clippy` components automatically.

## Checks that must pass

Every change must be clean under all three before review:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Clippy runs as an error gate. `unsafe_code` is denied workspace-wide and
`unwrap_used` is warned — prefer explicit error handling over `unwrap`/`expect`
in non-test code.

## Crate map

```
vyane-core        types, traits, errors, env policy — no runtime
vyane-provider    provider registry, endpoints, auth styles
vyane-config      TOML parsing, profile + failover chain resolution
vyane-protocol    ChatClient over HTTP (OpenAI Chat/Responses, Anthropic Messages)
vyane-harness     Harness wrapping coding CLIs (Claude Code, Codex CLI)
vyane-ledger      JSONL Ledger, SessionStore, cost table
vyane-kernel      dispatch / broadcast / failover state machine + streaming
vyane-router      deterministic routing: complexity scoring, tag inference, tier mapping
vyane-workflow    declarative workflow engine (DAG + journal/resume)
vyane-service     shared facade: config loading, selector resolution, routing adapter
vyane-mcp         MCP server (rmcp SDK, 4 tools over stdio)
vyane-cli         front-end: CLI + REST API (axum) + MCP launcher
```

The load-bearing rule: **the kernel depends only on `vyane-core` traits.** It
never names a concrete protocol client, harness, or ledger. Concrete types are
constructed and wired together in `vyane-service` and injected through the
`ExecutorFactory` seam.

## Protocol entry points

Vyane supports three interchangeable front-ends, all sharing the same
`vyane-service` layer:

| protocol | command | new code location |
|----------|---------|-------------------|
| CLI | `vyane dispatch/broadcast/review/route/task/workflow` | `vyane-cli/src/command.rs` |
| REST API | `vyane serve` (axum, 10 endpoints + SSE) | `vyane-cli/src/api.rs` |
| MCP | `vyane mcp` (rmcp, 4 tools over stdio) | `vyane-mcp/src/lib.rs` |

When adding a new operation, implement it in `vyane-service` (the shared layer)
and expose it through each front-end. Do not duplicate logic across front-ends.

## Streaming

The kernel owns streaming via `Dispatcher::dispatch_stream`. It takes a callback
for delta events and returns the assembled, ledger-appended `DispatchOutcome`.
The CLI `--stream` flag and the REST `/v1/dispatch/stream` SSE endpoint both
call this method — do not hand-roll streaming logic in front-ends.

## Routing

`vyane-router` is a standalone crate (depends only on `serde`) implementing
deterministic target selection: complexity scoring, tag inference, tier mapping,
and preference resolution. `vyane-service/src/routing.rs` is the adapter that
builds a `RoutePreferenceTable` from configured profiles (tier/tags/stage
metadata) and maps the router's decision back to a profile name. Use
`vyane route` for dry-run testing.

## Tests accompany code

New behaviour ships with its tests in the same change. Follow the patterns the
work packages call for — HTTP clients are tested against `wiremock`, harnesses
against fake CLI shell scripts (no real external CLIs in CI; real-CLI smoke
tests live behind `#[ignore]`).

Test placement:
- **Unit tests** (`#[cfg(test)] mod tests` inside each source file) for
  serialization, parsing, pure logic.
- **Integration tests** (`crates/*/tests/*.rs`) for multi-crate flows,
  dispatch/broadcast/broadcast/failover, workflow engine, ledger roundtrips.
- **Acceptance tests** (`crates/vyane-cli/tests/*.rs`) for end-to-end CLI +
  API behavior against wiremock servers.

## Pull requests

- **Small and single-purpose.** One logical change per PR; split unrelated work
  apart. Large sweeping PRs are hard to review and will be asked to be broken up.
- **Conventional commits.** Use `type(scope): summary` — e.g.
  `feat(protocol): add Anthropic Messages streaming`, `fix(harness): kill the
  process group on cancel`, `docs(architecture): clarify failover gate`.
- **Keep the docs in sync.** If you change a public type or config shape, update
  the affected docs and `profiles.example.toml` in the same PR.

## Licensing

By submitting a contribution you agree that it is dual-licensed under
**MIT OR Apache-2.0**, matching the project license, with no additional terms.
