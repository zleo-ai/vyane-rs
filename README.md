# Vyane

**Run coding agents and model APIs through one Rust CLI.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

For developers who use more than one coding agent or model provider: give your targets names, send a task to one or several, and keep the results and attempt history together. Use Claude Code or Codex CLI for coding tasks, or call a model API directly for text responses.

> **Early development.** Source installation only; APIs are unstable and some capabilities remain partial or missing. Native session resume and automatic restart replay are not complete. See [current boundaries](docs/PROJECT-STATUS.md).

## See it run

[Run the terminal demo](docs/EXAMPLES.md): the installed CLI dispatches a task, broadcasts to two targets, then recovers from a scripted primary failure. It uses local HTTP fixtures, needs no keys, and makes no live-model calls.

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## Three ways to use it

| When you want to… | Use | What you get |
| --- | --- | --- |
| Run a task with a chosen agent or model | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | One result and a local run record. Coding harnesses require separate installation and setup. |
| Compare answers to the same prompt | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | Concurrent runs with separate results; you judge the answers. |
| Try a configured backup when a call fails | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | An ordered failover chain for eligible errors, with each attempt recorded. This is not session handoff. |

## Quickstart

**1. Install from source.** Requires stable Rust/Cargo (minimum Rust 1.88) and a C/C++ build toolchain. Linux and macOS are the CI platforms; Windows support is not claimed.

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

Expected version: `vyane 0.1.0`. The binary stays inside this checkout, so an existing `vyane` on your PATH is not replaced. Keep this shell open.

**2. Get a first result without keys.** The bundled example only previews routing; it sends no model request.

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

Look for the `offline` profile and `"selection_key": "offline"` in the JSON. Its endpoint is deliberately invalid; do not dispatch to it.

**3. Connect your own target.** Follow [provider setup](docs/QUICKSTART.md#configure-a-real-target), then try the [three examples](docs/EXAMPLES.md). Supply your own model access and credentials; real calls may incur provider charges. The offline preview does not validate a live provider.

## What works today

| Available in the source | Boundary |
| --- | --- |
| Dispatch, broadcast, explicit failover and streaming | Direct HTTP returns model text; it does not attach local files or provide shell tools. |
| Claude Code and Codex CLI adapters | CLI subprocesses, not Codex App Server RPC or a direct Claude Agent SDK integration. You supply the CLI and access; the harness enforces its sandbox. |
| Profile-based routing and local run history | Routing follows your configuration; it does not guarantee answer quality. |
| CLI, REST and MCP entry points | Each exposes a different subset. See the [CLI guide](docs/CLI-GUIDE.md). |

Resident execution, durable workflows and local goals are evolving. Native runs are fresh and sessionless; native approval/session resume and automatic restart replay remain open. Owner fields and loopback authentication do not provide hostile same-user or multi-user isolation. See [status and limits](docs/PROJECT-STATUS.md) before building on these surfaces.

## Go deeper

- [Architecture and target model](docs/ARCHITECTURE.md) · [Configuration reference](profiles.example.toml) · [CLI, REST and MCP guide](docs/CLI-GUIDE.md)
- [Capability matrix and reproducible evidence](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [Roadmap](docs/ROADMAP.md)
- [Changelog](docs/CHANGELOG.md) · [Version and release policy](docs/RELEASING.md)

<details>
<summary>Evidence snapshot</summary>

The [public capability matrix](docs/parity/ORIGINAL-VYANE-PARITY.md) tracks 53 capabilities across eight domains: 7 implemented, 23 partial, 12 missing, 9 deliberately different or awaiting a decision, and 2 planned. The [sanitized manifest](docs/parity/fixtures/v1/manifest.json) contains 44 cases in 5 suites: 30 normalized matches and 14 open differences. These bounded checks are not a whole-system parity percentage.

</details>

This is the public Rust implementation of capabilities from the original private Python Vyane system. It is not a drop-in replacement: commands, configuration, stored sessions and workflows need individual migration checks. Do not share state directories without a documented migration path. Public contributors do not need the private source.

## Contribute and get help

Try an example, report a reproducible problem, improve a translation, or tackle a documented gap. Check [Issues](https://github.com/zleo-ai/vyane-rs/issues) and [open PRs](https://github.com/zleo-ai/vyane-rs/pulls), then read [CONTRIBUTING.md](CONTRIBUTING.md). Include a source revision and sanitized output; omit keys, private prompts and personal paths. Report vulnerabilities through [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. Contributions use the same terms.

---

Source baseline: `fe43941` · Workspace: `0.1.0` · Updated: 2026-10-15. English is authoritative; see [translation coverage and review status](docs/TRANSLATING.md). Roadmap milestones are not published releases.
