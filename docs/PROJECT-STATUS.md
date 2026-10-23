# Project status and migration boundaries

Checked against source `fe43941` on 2026-10-15. See the [README](../README.md) for the short introduction.

## Availability


This is an early, source-build project with unstable APIs. The workspace version is **0.1.0**. Git tag `v0.1.0` names that source snapshot; it is not a crates.io release. Roadmap labels such as v0.2–v0.4 remain implementation milestones, not the next package numbers. See [VERSIONING.md](VERSIONING.md). Use the [source quickstart](../README.md#quickstart). Registry installation is not a verified distribution path. The publication order includes all 18 workspace crates. Uploading them still needs the separate publish approval in [RELEASING.md](RELEASING.md).

| Available in the public source | Boundary |
| --- | --- |
| Dispatch, broadcast, explicit failover, streaming and local history | A direct HTTP target provides model responses, not filesystem or shell tools. |
| Deterministic routing and declarative workflows | Routing depends on your profiles; it is not an automatic quality guarantee. |
| CLI, loopback REST API and stdio MCP | Frontends expose different subsets; see the [CLI guide](CLI-GUIDE.md). |
| Durable tasks, local messages/goals and Linux resident Process/Native runs | Native runs are fresh and sessionless; approval resume, native session resume, and automatic restart replay remain open. |
| Explicit permissions, scrubbed child environments and run accounting | CLI sandbox enforcement is delegated to the selected harness. Owner fields and loopback authentication do not provide hostile same-user or multi-user isolation. |

Linux and macOS are the CI platforms; some resident execution and sandbox features are Linux-specific. Windows support is not claimed. Model access, credentials and any coding-agent CLI must be supplied separately. Provider calls may incur costs; building and the [offline checks](QUICKSTART.md#first-run-without-credentials) do not call models.

## Coding-agent integration

At this source baseline, the Rust adapters launch CLI subprocesses:

- [CodexCliHarness](../crates/vyane-harness/src/codex_cli.rs) invokes Codex CLI
  execution/resume commands. It is not a Codex App Server JSON-RPC client.
- [ClaudeCodeHarness](../crates/vyane-harness/src/claude_code.rs) invokes Claude
  Code print/stream-json modes. It is not a direct Claude Agent SDK integration.
- Vyane's [native turn driver](../crates/vyane-harness/src/native/turn_driver.rs)
  is its own bounded model/tool loop, not a vendor Agent SDK.

CLI fixture tests do not certify all upstream CLI versions, models or provider
combinations. Real vendor CLI smoke tests are separate and ignored by default;
the README's demo uses local scripted HTTP responses. No new live-provider or
live-harness acceptance is claimed for this documentation update.

## Relationship to Python Vyane


This repository is the public Rust implementation of capabilities from the original private Python Vyane system. It is not a drop-in replacement: Python commands, configuration, stored sessions, workflows and operational integrations must be checked individually before migration. Do not point both implementations at the same state directory without a documented migration path.

The [public capability matrix](parity/ORIGINAL-VYANE-PARITY.md) tracks 53 capabilities across eight domains: 7 implemented, 23 partial, 12 missing, 9 deliberately different or awaiting a decision, and 2 planned. The [sanitized manifest](parity/fixtures/v1/manifest.json) contains 44 cases in 5 suites: 30 normalized matches and 14 open differences. These are bounded behavior checks, not a whole-system parity percentage. Recompute the public Rust evidence with:

```sh
python3 .github/scripts/parity-report.py --format markdown
```

This runs Rust fixture checks and validates the stored sanitized attestation; it does not run the private reference implementation. Public contributors do not need access to private source. See the [roadmap](ROADMAP.md) for open work and the [changelog](CHANGELOG.md) for changes in the public repository.

