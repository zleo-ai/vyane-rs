# Versioning

The workspace has one version. It lives in the root `Cargo.toml` and is the
latest named source snapshot. Change it only when cutting the next node.
Put an annotated git tag `vX.Y.Z` on the `main` commit that contains that
version and its changelog section.

A tag names the source tree. It does not upload crates to crates.io and it
does not approve the publish workflow. Publication stays a separate decision
in [RELEASING.md](RELEASING.md).

## What `vyane --version` prints

| Checkout | Example |
| --- | --- |
| Exactly the tag, clean tree | `vyane 0.1.0` |
| Commits after the tag | `vyane 0.1.0 (v0.1.0-12-gabcdef0)` |
| Uncommitted changes | `vyane 0.1.0 (v0.1.0-12-gabcdef0-dirty)` |
| No matching tag visible | `vyane 0.1.0` |

The number outside parentheses is the Cargo package version. The parentheses
are `git describe` and identify the commit. A shallow clone that cannot see
tags prints the package version only. Do not compare that bare number with a
later tag as if it were newer.

Rebuild after pulling if you need the suffix to move. The suffix is recorded
when the binary is compiled.

## When the number changes

This project is still before 1.0. Choose the next number by what an existing
user must do, not by how large the diff looks.

| Digit | When it increases | Examples |
| --- | --- | --- |
| Z, patch | Fixes and internal work. An upgrade needs no action, and behavior only becomes more correct. | A bug fix, tests, refactor, docs. |
| Y, minor | A new user-visible capability, or any incompatible contract: CLI, HTTP, MCP, config, or stored state that needs a migration or a manual step. Z returns to 0. | A new subcommand, a renamed config field, a ledger change that old files cannot read. |
| X, major | Only when a public contract is frozen and compatibility is promised. That is a separate decision. Do not take 1.0 because many commits landed. | Not used yet. |

Before 1.0, incompatible changes increment Y, not X. If a change might be
either Y or Z, use Y and write the migration in the changelog.

## Each pull request

Record user-visible changes under `Unreleased` in
[CHANGELOG.md](CHANGELOG.md). Use only `Added`, `Changed`, `Fixed`,
`Removed`, `Security`, and `Known limitations`. Do not change `Cargo.toml`
version fields in an ordinary pull request. Do not create a tag there.

## Cutting a node

1. Move `Unreleased` to `X.Y.Z - YYYY-MM-DD` and leave a new empty
   `Unreleased` section.
2. If the number changed, set `[workspace.package].version` and every
   workspace dependency version together. Regenerate `Cargo.lock` with Cargo
   and do not upgrade unrelated packages.
3. Review and merge to `main`.
4. On that merge commit, create and push an annotated tag:
   `git tag -a vX.Y.Z -m "vX.Y.Z" <sha>` then `git push origin vX.Y.Z`.
5. Stop. Do not dispatch the publish workflow unless publication is
   authorized on its own.

Do not invent tags for older commits, and do not move a tag that already
exists. The first tag is `v0.1.0` because the workspace has carried `0.1.0`
and no earlier tag was ever cut.
