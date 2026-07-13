# ADR 0001: deterministic routing remains the public-core default

- Status: accepted
- Date: 2026-07-10
- Parity rows: EXE-05, OBS-05

## Context

The fixed original Vyane baseline combines keyword classification with mutable
history, benchmark and feedback files. Its process-local cache and ambient
home-directory state make the same request capable of producing a different
route without an explicit input change. The Rust router instead derives an
intent, tags, complexity tier and effort from the request plus caller-supplied
signals.

Both behaviours are useful. Reproducing the original ambient state inside the
Rust core would, however, weaken reproducibility, owner isolation and testability.
Pretending the current Rust keyword table is already behaviour-equivalent would
also be incorrect.

## Decision

1. `vyane-router` remains a pure, deterministic policy library. Every input that
   can affect a decision is passed explicitly and can be serialized into a
   redacted decision trace.
2. Stateful benchmark or feedback learning is an optional service-layer signal
   provider. It must be explicitly enabled, owner-scoped, snapshot/version
   identified, bounded, and recorded by digest in the route decision. Absence or
   failure of that provider falls back to the deterministic policy.
3. The fixed original classifier fixtures are a compatibility oracle, not a
   reason to hide differences. Exact matches, normalized matches and open
   differences are versioned separately. An open difference remains a parity
   blocker until accepted with a migration consequence.
4. The public core will not read implicit `history.jsonl`, `benchmark.json` or
   `feedback.jsonl` files, nor keep a global mutable routing cache.

## Consequences

- Identical explicit inputs produce the same default route across process
  restarts.
- OBS-05 can add learning without making the default router nondeterministic.
- EXE-05 remains `different` until golden coverage and migration behaviour meet
  the gates below; acceptance of this ADR is not an implementation claim.

## Acceptance gates

- A versioned golden suite covers intent, tags, tier, effort and target choice.
- Every stateful signal contribution is owner-isolated, snapshot-addressed and
  visible in a redacted decision trace.
- Hermetic tests prove deterministic fallback when the signal provider is
  absent, stale, malformed or unavailable.
- The CLI/API expose whether a decision used deterministic-only or augmented
  policy without exposing prompts or private feedback data.
