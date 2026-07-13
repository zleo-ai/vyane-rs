# ADR 0002: typed workflow plans and explicit continuation modes

- Status: accepted
- Date: 2026-07-10
- Parity rows: EXE-06, CON-03, CON-05

## Context

The fixed original Vyane baseline executes a programmable JavaScript/Deno DSL
and resumes by reusing a matching prefix of calls in a new run. The Rust engine
executes a declarative TOML DAG and resumes non-successful steps in the same
journal after verifying a versioned source bundle. These are different input
languages and different continuation semantics.

Embedding arbitrary JavaScript into the Rust engine would expand the trusted
runtime and sandbox surface. Replacing the declarative engine would discard its
bounded graph, source-digest and recovery properties. Calling both operations
`resume` would continue to conceal a user-visible difference.

## Decision

1. A versioned, typed `WorkflowPlan` will become the execution contract. The
   current declarative TOML model remains the canonical public frontend and
   will compile to that plan; no such shared plan type is claimed to exist yet.
2. Original-workflow compatibility is an optional frontend/adapter. It may run
   an isolated Deno bridge or translate a documented portable subset, but it
   must emit the same bounded `WorkflowPlan` and capability manifest before any
   agent call. Unsupported dynamic behaviour fails explicitly; it is never
   silently approximated.
3. Continuation has two named operations:
   - **resume** keeps the same run identity and journal, requires an exact
     versioned source bundle, and reruns only admissible non-successful work;
   - **replay/fork** creates a new run identity and may reuse a verified matching
     prefix from a prior run before continuing live.
4. Daemon restart does not imply either operation. Automatic payload replay
   remains fail-closed until a separate encrypted/retained payload policy and
   explicit admission contract exist.
5. Deferred single-target route hints use closed typed values. Explicit effort
   takes precedence over the selected profile's configured effort and then the
   decision-tier default; the canonical effective value is frozen across the
   admitted failover chain and all recorded replay surfaces. Route hints on an
   explicit target or `fan_out` fail before dispatch rather than being ignored.

## Consequences

- The Rust engine stays deterministic and does not make Deno part of its trusted
  core.
- Portable original workflows can gain a migration path without claiming that
  arbitrary JavaScript is declarative TOML.
- Existing same-journal resume remains valid, while original prefix replay gets
  an unambiguous new-run operation.
- EXE-06 and CON-03 remain `different` until the shared plan and compatibility
  acceptance scenarios are implemented.
- The repository-local effort contract is delivered, but it does not establish
  cross-implementation parity or a production-complete model-tier policy.

## Acceptance gates

- A schema-versioned plan validates DAG edges, concurrency, budgets, sandbox,
  workdir and target capabilities before execution.
- The 3-finder/1-synth, nested workflow, shared budget and partial-resume
  fixtures either produce equivalent plans/results or an explicit unsupported
  report.
- Resume tests cover source, prompt-file, policy and target drift plus concurrent
  CAS; replay tests prove a new run id and verified call-prefix reuse.
- No restart path persists or replays prompt/source payload merely because a
  metadata row is queued or interrupted.
