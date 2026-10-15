# Vyane

**Run and coordinate AI model tasks from one Rust CLI and orchestration kernel.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Source baseline: `b46c194` · Workspace version: `0.1.0` · Updated: 2026-10-15.
English is authoritative. See the [translation policy](docs/TRANSLATING.md).

Vyane dispatches a task to one configured target, broadcasts it to several, or tries an explicit failover chain. Targets can use Claude Code or Codex CLI harnesses, or call HTTP model endpoints directly. It is for developers building local agent workflows and Rust applications that need explicit routing, execution permissions, cancellation, and run records.

![Abstract terminal windows connect through a copper coordination hub to result cards.](docs/assets/vyane-hero.webp)

*AI-generated concept illustration; not an architecture diagram. [Asset notes](docs/assets/README.md).*

## Status and limits

This is an early, source-build project with unstable APIs. The Cargo version is **0.1.0**; roadmap labels such as v0.2–v0.4 describe implementation milestones, not published releases. On 2026-10-15 the repository had no Git tags or GitHub Releases. Use the source quickstart below; registry installation is not a verified distribution path. The [release checklist](docs/RELEASING.md) records a current blocker: 18 workspace crates, but only 17 entries in the publication order (`vyane-quota` is missing).

| Available in the public source | Boundary |
| --- | --- |
| Dispatch, broadcast, explicit failover, streaming and local history | A direct HTTP target provides model responses, not filesystem or shell tools. |
| Deterministic routing and declarative workflows | Routing depends on your profiles; it is not an automatic quality guarantee. |
| CLI, loopback REST API and stdio MCP | Frontends expose different subsets; see the [CLI guide](docs/CLI-GUIDE.md). |
| Durable tasks, local messages/goals and Linux resident Process/Native runs | Native runs are fresh and sessionless; approval resume, native session resume, and automatic restart replay remain open. |
| Explicit permissions, scrubbed child environments and run accounting | CLI sandbox enforcement is delegated to the selected harness. Owner fields and loopback authentication do not provide hostile same-user or multi-user isolation. |

Linux and macOS are the CI platforms; some resident execution and sandbox features are Linux-specific. Windows support is not claimed. Model access, credentials and any coding-agent CLI must be supplied separately. Provider calls may incur costs; building and the offline checks below do not call models.

## Quickstart

Install stable Rust with Cargo and a platform C/C++ build toolchain. The declared minimum Rust version is 1.88. The repository selects stable with rustfmt and Clippy. Run from the repository root:

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

The version should print `vyane 0.1.0`. This installs into this checkout, so it does not replace another `vyane` on your PATH. Keep this shell open for the following commands. To reproduce this documentation snapshot, check out `b46c194` before installing; `main` can change.

### First run without credentials

```sh
cat > ./target/offline-config.toml <<'TOML'
[providers.offline]
base_url = "https://example.invalid/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "offline-placeholder"

[profiles.offline]
provider = "offline"
harness = "none"
model = "offline-placeholder"
TOML
"$VYANE" --config ./target/offline-config.toml check
"$VYANE" --config ./target/offline-config.toml route "Explain this function" --json
```

`check` should report a valid configuration with the `offline` profile. `route` returns JSON with `"selection_key": "offline"` and `"model": "offline-placeholder"` without dispatching. This is an offline configuration example, not a mock model response or a working provider; do not dispatch to its deliberately invalid endpoint.

### Configure a real target

Create `./target/model-config.toml` with the following shape. Replace the example endpoint and model with values supported by your provider; `example.invalid` is deliberately not a working service.

```toml
[providers.demo]
base_url = "https://example.invalid/v1"
api_key_env = "VYANE_DEMO_API_KEY"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "YOUR_PROVIDER_MODEL_ID"

[profiles.chat]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_PROVIDER_MODEL_ID"
```

Set `VYANE_DEMO_API_KEY` through your normal secret manager or shell environment; never put its value in the TOML or commit it. Then:

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

The first command checks configuration and credential presence; it does not prove the endpoint accepts your credentials or model. The second sends a real request once the placeholders are replaced. Live provider success is not part of the offline quickstart verification. For coding harnesses, permissions, multiple profiles and failover, see [configuration examples](profiles.example.toml) and the [CLI guide](docs/CLI-GUIDE.md).

## How targets work

![CLI, REST and MCP call the service and kernel; each target binds provider, protocol, harness and model; executions produce run records.](docs/assets/target-flow.svg)

| Field | Meaning |
| --- | --- |
| `provider` | Endpoint, credential source and billing identity. |
| `protocol` | Wire format: OpenAI Chat, OpenAI Responses or Anthropic Messages. |
| `harness` | Execution adapter such as Claude Code or Codex CLI; `none` means direct HTTP. |
| `model` | A model identifier within that provider's namespace. |

A profile binds these four fields. Each failover target is resolved separately, so a model identifier is not silently reused against another provider. The kernel depends on shared traits; the service assembles concrete adapters. See [architecture](docs/ARCHITECTURE.md) for dependency and authority details.

## Examples and reference

After configuring the named profiles, these commands illustrate the next steps:

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

The broadcast example requires a second profile named `review`. Prompts are literal text: mentioning a file does not attach its contents to a direct HTTP request. The [CLI guide](docs/CLI-GUIDE.md) covers workflows, daemon lifecycle, REST authentication, MCP, local messaging and goals. Use `--help` on each subcommand for the current command surface.

## Relationship to Python Vyane

This repository is the public Rust implementation of capabilities from the original private Python Vyane system. It is not a drop-in replacement: Python commands, configuration, stored sessions, workflows and operational integrations must be checked individually before migration. Do not point both implementations at the same state directory without a documented migration path.

The [public capability matrix](docs/parity/ORIGINAL-VYANE-PARITY.md) tracks 53 capabilities across eight domains: 7 implemented, 23 partial, 12 missing, 9 deliberately different or awaiting a decision, and 2 planned. The [sanitized manifest](docs/parity/fixtures/v1/manifest.json) contains 44 cases in 5 suites: 30 normalized matches and 14 open differences. These are bounded behavior checks, not a whole-system parity percentage. Recompute the public Rust evidence with:

```sh
python3 .github/scripts/parity-report.py --format markdown
```

This runs Rust fixture checks and validates the stored sanitized attestation; it does not run the private reference implementation. Public contributors do not need access to private source. See the [roadmap](docs/ROADMAP.md) for open work and the [changelog](docs/CHANGELOG.md) for changes in the public repository.

## Contribute and get help

Start with [CONTRIBUTING.md](CONTRIBUTING.md) for checks and review rules. Useful contributions include reproducible bug reports, smaller examples, translation corrections and tests for documented capability gaps. Search [Issues](https://github.com/zleo-ai/vyane-rs/issues) and [open PRs](https://github.com/zleo-ai/vyane-rs/pulls) before starting overlapping work. Include the source revision, OS, Rust version, a minimal reproduction and sanitized output. Never include keys, private prompts or personal filesystem paths.

For vulnerabilities, follow [SECURITY.md](SECURITY.md) before posting details. Translation coverage and review status are in [TRANSLATING.md](docs/TRANSLATING.md); release and version maintenance are in [RELEASING.md](docs/RELEASING.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. Contributions use the same terms; see [contributing](CONTRIBUTING.md#licensing).
