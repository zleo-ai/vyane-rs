# AGENTS.md

This is the public crate `zleo-ai/vyane-rs`. **Do not write private context
into any file in this repository:** host paths, device names, credentials,
account pools, internal board IDs, personal data, or employer details.
Private AIOS rules live in the private repos (`zleo-ai/vyane`,
`zleo-ai/Eosphor`); this crate is a public Rust kernel, not a mirror of
that private OS.

## What this repo is

A multi-model agent-orchestration kernel plus one CLI. Dispatch, broadcast
and failover run across coding-agent harnesses and raw HTTP model endpoints.
The four-layer target model (provider / protocol / harness / model) must
stay unconfused. See `README.md` and `docs/ARCHITECTURE.md`.

## Working in this repo

- Before every commit, inspect the diff for secrets, tokens, private
  hostnames, and private paths. Credentials belong in environment
  variables only.
- Merge bar: `cargo fmt --all -- --check`, `cargo clippy --workspace
  --all-targets -- -D warnings`, `cargo test --workspace` (see
  `CONTRIBUTING.md`). `unsafe_code` is denied workspace-wide.
- The author of a change does not self-review it as the independent
  reviewer.
- Do not describe private, unpublished source as a "verifiable" step for
  public readers.

Claude Code loads [`CLAUDE.md`](CLAUDE.md), which only imports this file.

(EOS-690: `origin/main` had no agent entry; this file closes that gap
without copying private device or model-effort facts.)
