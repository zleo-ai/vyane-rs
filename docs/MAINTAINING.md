# Maintaining the public project

## Help someone reach a first successful run

Keep the README source installation and credential-free first run reproducible.
Show an expected result and distinguish configuration readiness from a real model
response. Reports should include a source revision, OS/toolchain, exact command,
expected behavior and sanitized output. Use existing
[Issues](https://github.com/zleo-ai/vyane-rs/issues) for help and bug tracking;
GitHub Discussions was not enabled when checked on 2026-10-15.

Before promoting a new workflow, verify a small real read-only run with explicit
model, toolchain, timeout and result assertion. Record what was actually run and
what remains untested. Offline fixtures demonstrate bounded contracts; they are
not a substitute for live provider verification. A screenshot or recording must
come from the actual run. Never imply a recording exists when it does not.

## Triage and contribution

Search existing issues and PRs before creating work. Give a newcomer a bounded
problem with a public reproduction, an expected outcome and a validation command.
Do not create bulk placeholder issues or promise private source access. Keep
vulnerability details out of ordinary issues; use [SECURITY.md](../SECURITY.md).

Use [CONTRIBUTING.md](../CONTRIBUTING.md) for review and checks. Release procedures
live in [RELEASING.md](RELEASING.md), and translation changes follow
[TRANSLATING.md](TRANSLATING.md). Keep AI-generated concept art labeled as such;
use the maintained diagram and prose for exact architecture.

## Measure usefulness without hidden telemetry

In voluntary feedback, ask whether the first run succeeded, what blocked it,
whether the user returned for a second task, and which instructions cost effort.
Use those reports to improve the quickstart and maintenance burden. Star counts
are not usage or adoption measurements. This documentation introduces no
telemetry, external promotion, scheduled workflow, or new token permission.
