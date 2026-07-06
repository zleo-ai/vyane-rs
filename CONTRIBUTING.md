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

## Tests accompany code

New behaviour ships with its tests in the same change. Follow the patterns the
work packages call for — HTTP clients are tested against `wiremock`, harnesses
against fake CLI shell scripts (no real external CLIs in CI; real-CLI smoke
tests live behind `#[ignore]`).

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
