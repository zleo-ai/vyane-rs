# Versions and releases

## Current evidence

Checked on 2026-10-15 against source baseline `b46c194`:

| Surface | Observed state |
| --- | --- |
| `Cargo.toml`, workspace package manifests and `Cargo.lock` | 18 workspace crates at `0.1.0`; member versions inherit the workspace version. |
| Remote Git tags / GitHub Releases | No tags and no Releases returned by the GitHub API. |
| `docs/CHANGELOG.md` | One `Unreleased` section; no historical release date is inferred. |
| Roadmap v0.1–v0.4 headings | Implementation scope labels, not registry versions or release history. |
| Publication preflight | Fails before packaging: `workspace packages missing from PUBLISH_ORDER: vyane-quota`. The publication script enumerates 17 of 18 workspace crates. |
| Registry distribution | Not established by a source build or GitHub release inventory. Do not recommend `cargo install vyane-cli` until the actual published artifact and ownership are verified. |

Do not bump versions, manufacture historical tags or dates, or claim package
readiness to make these surfaces appear consistent. Fix the missing publication
member in a separately reviewed release-script change, then rerun preflight.
No publication, tag creation, or protected-environment approval is part of a
documentation update.

## Version policy

The workspace uses one coordinated version. `Cargo.toml` is the source of truth;
all workspace crates and internal dependency version requirements move together.
Use semantic versions:

- Before 1.0, a breaking public contract change requires a minor increment
  (for example, `0.1.x` to `0.2.0`). Document configuration, CLI/JSON, state and
  Rust API migration requirements. A patch must remain compatible.
- At 1.0 and later, increment major for incompatible changes, minor for compatible
  additions, and patch for compatible fixes. Do not promise 1.0 stability early.
- Documentation-only changes usually stay in `Unreleased` without a version bump.
- The existing publication gate accepts only `vMAJOR.MINOR.PATCH`; prerelease
  suffixes need a separately reviewed gate change before use.

## Every pull request

Record user-visible changes in the existing `Unreleased` section of
[CHANGELOG.md](CHANGELOG.md), under Added, Changed, Fixed, Removed, Security or
Known limitations as appropriate. Explain the observable effect and any
migration action; link public commits or PRs. Do not paste private task IDs,
internal deployment details or a raw commit dump. A refactor with no public
behavior change need not invent a changelog entry.

Keep one `Unreleased` heading. English is the canonical changelog; README
translations point to it. Follow [TRANSLATING.md](TRANSLATING.md) when entry-point
commands, limits or release claims change.

## Prepare a release candidate

1. Verify current `origin/main`, open PR ownership, remote tags, GitHub Releases
   and relevant crates.io records. Inventory failures are unknown, not absence.
2. Choose the intended version from the actual change scope. Update
   `[workspace.package].version`, every versioned internal dependency in
   `[workspace.dependencies]`, and any explicit member versions in one PR.
   Regenerate `Cargo.lock` with Cargo and inspect the dependency diff; do not
   opportunistically upgrade unrelated packages.
3. Check `cargo metadata --locked --format-version 1 --no-deps`: workspace set,
   versions, metadata and dependency order must match the publication plan.
   Update the README version and migration instructions together.
4. Run the [contribution gates](../CONTRIBUTING.md#checks-that-must-pass), public
   parity checks, release-gate tests and token-free package preflight. The local
   preflight command is `bash .github/scripts/publish-workspace.sh preflight
   --tag vX.Y.Z` with the intended version substituted. It checks package/version
   readiness; it is not the workflow's exact-source authorization check.
   `--allow-dirty` is available only for local preflight and never for publication.
5. Independently review release-script or security-sensitive changes at the
   strict tier. Preserve exact commit, check results, package checksums, known
   failures and unresolved migration limits in the candidate notes.
6. Prepare release notes from `Unreleased`: changes, compatibility/migration,
   platforms, installation, checks and known issues. Keep them a draft until
   publication authority and the intended artifact are confirmed.

## Publish only with separate authority

Follow [CONTRIBUTING.md — Releases](../CONTRIBUTING.md#releases) and
[`publish.yml`](../.github/workflows/publish.yml). A merge, token or tag does not
itself authorize registry upload. The workflow is manually dispatched from
`main`, uses an existing exact-current-main tag, and requires a separate reviewer
in the protected `crates-io` environment with self-review prevented. Do not
change permissions, add secrets or bypass this gate to complete a release.

When the release is actually authorized and published, record the real release
date and immutable tag/commit, move the shipped notes from `Unreleased` to that
version, and leave a fresh `Unreleased` section. If the notes are finalized in a
candidate PR before publication, label the date as pending until confirmed.
Check GitHub release content and registry artifact versions/checksums separately;
a GitHub Release does not prove crates.io upload. Never rewrite an existing tag
to hide a failed or partial release. Preserve partial-upload facts and use the
existing script's identity checks when planning recovery.
