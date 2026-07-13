# Architecture decision records

These records make deliberate Vyane-to-Rust product differences reviewable.
An accepted decision fixes the target contract; it does **not** by itself turn
a parity-matrix row into `implemented` or prove cross-repository equivalence.
Each affected row still needs the acceptance evidence named in its ADR and in
[`ORIGINAL-VYANE-PARITY.md`](../parity/ORIGINAL-VYANE-PARITY.md).

| ADR | Decision | Parity rows |
| --- | --- | --- |
| [0001](0001-deterministic-routing-core.md) | deterministic routing remains the public-core default | EXE-05, OBS-05 |
| [0002](0002-workflow-frontends-and-resume.md) | one typed workflow plan, explicit compatibility frontend, two distinct continuation operations | EXE-06, CON-03, CON-05 |
| [0003](0003-separate-solution-and-change-review.md) | solution review and repository-change review are separate products | QUA-01, COL-02 |
| [0004](0004-modular-supervisor-host.md) | compose narrow supervisors instead of recreating the original god daemon | INT-04, CON-06, COL-03, COL-05 |
| [0005](0005-execution-authority-and-capability-admission.md) | separate audit identity, capability admission evidence, active authority and native-session domains | EXE-07, GOV-03, GOV-04, GOV-05, CON-01, CON-06 |
