//! # vyane-kernel
//!
//! The dispatch kernel: the orchestration state machine at the heart of Vyane.
//! It walks a resolved chain of targets with correct failover gating and a
//! complete attempt trail ([`Dispatcher::dispatch`]), and fans one task across
//! many chains concurrently in input order ([`Dispatcher::broadcast`]). Every
//! run produces exactly one [`vyane_core::RunRecord`], appended to the ledger
//! and reflected in the session store — on success **and** on failure.
//!
//! The kernel composes [`vyane_core`]'s capability traits (`ChatClient`,
//! `Harness`, `Ledger`, `SessionStore`) at runtime and depends on **no** other
//! crate: it never names a concrete protocol client, harness, or ledger.
//! Concrete adapters are constructed in the assembler (CLI) layer and injected
//! through the [`ExecutorFactory`] seam. This keeps the kernel runtime-free
//! beyond `vyane-core` and lets tests supply deterministic mock executors.
//!
//! See `docs/plan/WP-04.md` for the work-package plan.

mod broadcast;
mod digest;
mod dispatch;
mod executor;

pub use broadcast::DEFAULT_BROADCAST_CONCURRENCY;
pub use digest::task_digest;
pub use dispatch::{DispatchOutcome, Dispatcher, StreamDispatchEvent};
pub use executor::{Executor, ExecutorFactory};

// Re-export the cancellation primitive so callers driving the kernel use the
// same type the state machine expects, without depending on `tokio-util`
// directly.
pub use vyane_core::CancellationToken;
