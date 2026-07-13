# ADR 0003: solution review and repository-change review are separate products

- Status: accepted
- Date: 2026-07-10
- Parity rows: QUA-01, COL-02

## Context

The current Rust `review` workflow asks one target to propose a solution, fans
that output to reviewers, then synthesizes their responses. The fixed original
Vyane review surface is repository-change oriented: it collects git/PR evidence,
assigns findings and verifies revisions. Both use multiple agents, but their
inputs, safety boundary and success criteria are not interchangeable.

## Decision

1. **Solution review** evaluates a proposed answer or implementation plan. Its
   evidence is bounded text and its result is a synthesized recommendation.
2. **Change review** evaluates a concrete repository diff/commit/PR. Its
   evidence model contains immutable source revision, scoped diff, structured
   findings, severity, file/line anchors, verifier disposition and revision
   state.
3. Both products may share a generic round/convergence engine, but neither may
   infer the other's success from free-form prose. A change review is successful
   only after structured finding reconciliation and an explicit verifier result.
4. The CLI/API must expose the product kind. The current command remains
   documented as solution review until an explicit change-review entry point is
   implemented; it is not evidence of original review parity.

## Consequences

- Existing functionality is preserved without overstating its scope.
- Future consensus/debate work can reuse orchestration primitives while review
  records retain product-specific schemas.
- QUA-01 remains `different` until change-review acceptance is present.

## Acceptance gates

- Solution-review tests cover partial reviewer failure, cancellation, bounded
  rounds and synthesis provenance.
- Change-review tests use a hermetic git repository and cover rename/binary/large
  diff policy, structured findings, duplicate reconciliation, revision drift and
  verifier closure.
- Ledger events record product kind and evidence digests, never unbounded diff or
  private repository content.
