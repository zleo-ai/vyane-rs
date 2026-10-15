# Security reporting

## Report privately first

Do not post credentials, private prompts, exploit payloads, personal paths or
sensitive logs in a public issue or pull request.

As checked on 2026-10-15, GitHub private vulnerability reporting is **not enabled**
for this repository, and no dedicated security email is documented. Until a
private channel is confirmed, open a minimal
[issue](https://github.com/zleo-ai/vyane-rs/issues/new) titled
“Request a private security reporting channel”, with no vulnerability details.
Wait for a maintainer to establish a private channel before sharing the report.
If GitHub later offers “Report a vulnerability” under the repository Security
tab, use that private channel instead. No response-time guarantee is published.

Once a private channel is available, include the affected source revision,
platform, minimal sanitized reproduction, expected boundary and observed impact.
Retain secrets locally; use placeholders in the report. Coordinate any public
technical disclosure with the maintainers.

## Scope and support status

This is an early source-build project with unstable APIs. No released-version
support matrix or security backport promise has been established. State the
exact commit in a report; a Cargo version alone is not precise enough.

The local REST/daemon interfaces bind to loopback and use authentication.
They do not claim isolation from a hostile process running as the same OS user
or provide a public multi-user service. CLI harness permissions depend on the
selected coding CLI; direct HTTP targets do not provide a workspace sandbox.
These limits do not waive reports about broken documented boundaries. See
[architecture](docs/ARCHITECTURE.md) for execution and authority contracts.
