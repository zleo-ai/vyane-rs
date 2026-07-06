//! # vyane-kernel
//!
//! The dispatch kernel: target resolution, failover chains, broadcast
//! fan-out, and run accounting. Composes [`vyane_core`]'s capability traits
//! (`ChatClient`, `Harness`, `Ledger`, `SessionStore`) at runtime — it never
//! depends on the sibling crates that implement them.
//!
//! See `docs/plan/WP-04-kernel.md` for the work-package plan.
