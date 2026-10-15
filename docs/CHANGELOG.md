# Changelog

User-visible changes in the public repository. Versions follow the
[release policy](RELEASING.md). There are no confirmed tagged releases as of
2026-10-15; the workspace version `0.1.0` is not evidence of publication.
This is a curated history, not a complete reconstruction of all development.

## Unreleased

### Added

- `vyane workflow plan FILE [--json]` inspects a static workflow before
  execution. Human output lists dependencies and requested capabilities;
  JSON exports the existing full plan payload, including prompt text
  ([#214](https://github.com/zleo-ai/vyane-rs/pull/214)).
- Local history filters for session and labels, measured whole-run duration
  statistics, and durations grouped by final provider
  ([#208](https://github.com/zleo-ai/vyane-rs/pull/208),
  [#209](https://github.com/zleo-ai/vyane-rs/pull/209),
  [#210](https://github.com/zleo-ai/vyane-rs/pull/210)).
- A source-build quickstart, seven README translations, a shared translation
  policy, security-reporting guidance and a documented version/release checklist.

### Changed

- Lead all eight READMEs with user scenarios, a reproducible local CLI demo and
  a shorter first-run path; retain full setup and migration detail in
  [QUICKSTART.md](QUICKSTART.md) and [PROJECT-STATUS.md](PROJECT-STATUS.md).

- Move detailed CLI, REST, MCP and workflow examples from the README into
  [CLI-GUIDE.md](CLI-GUIDE.md). The README now distinguishes source capabilities,
  release availability and migration limits.
- Raise the `async-trait` minimum to `0.1.92` to avoid generated redundant
  `#[must_use]` warnings on newer Rust toolchains
  ([#197](https://github.com/zleo-ai/vyane-rs/pull/197)).

### Fixed

- Include `vyane-quota` in the workspace publication order so all 18 crates
  pass the membership and dependency-order checks before packaging; add offline
  preflight regression coverage with synthetic archives
  ([#215](https://github.com/zleo-ai/vyane-rs/pull/215)).
- Anthropic input-usage normalization includes ordinary, cache-creation and
  cache-read input. Cache-read subcounts include reads only; cache creation still
  uses the ordinary input rate for estimates. Specialized cache/reasoning rates
  deduct their subcounts from totals before billing, with anomalous subcounts
  capped to totals to avoid double charging
  ([#194](https://github.com/zleo-ai/vyane-rs/pull/194)).
- Security routing recognizes inflections of `authenticate` and `authorize`
  while retaining word boundaries to avoid unrelated-word matches
  ([#195](https://github.com/zleo-ai/vyane-rs/pull/195)).
- Routing diagnostics and preferences normalize labels such as `front end` and
  `front-end` consistently while preserving the original matched label
  ([#196](https://github.com/zleo-ai/vyane-rs/pull/196)).
- A zero local-history limit returns before storage access
  ([#211](https://github.com/zleo-ai/vyane-rs/pull/211)).
- Offline parity reports pin capability identities and run their report
  regression tests in CI
  ([#212](https://github.com/zleo-ai/vyane-rs/pull/212)).

### Known limitations

- Full Python Vyane parity is not delivered. The public snapshot has 44 selected
  behavior cases (30 normalized matches, 14 open differences); see the
  [capability matrix](parity/ORIGINAL-VYANE-PARITY.md) for broader gaps.
- Real package and registry readiness remain unverified. The publication-order
  regression uses synthetic archives; its success and source installation do
  not establish publishable artifacts. See [RELEASING.md](RELEASING.md).
- README translations are machine-assisted drafts pending native-speaker review;
  structural checks do not establish linguistic or semantic accuracy.
