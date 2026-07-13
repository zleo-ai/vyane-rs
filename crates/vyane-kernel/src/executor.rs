//! The executor abstraction and the factory seam.
//!
//! The kernel never names a concrete client or harness type. It asks an
//! injected [`ExecutorFactory`] to turn a resolved [`BoundTarget`] into an
//! [`Executor`], then drives that executor for one attempt. Concrete adapters
//! (HTTP protocol clients, CLI harnesses) are constructed in the assembler
//! layer and handed in behind `Arc<dyn ChatClient>` / `Arc<dyn Harness>`; this
//! is the seam tests use to supply deterministic mock executors.

use std::sync::Arc;

use vyane_core::{BoundTarget, ChatClient, Harness, Result};

use crate::capability::{AttemptScope, CapabilityManifest};

/// One resolved way to run a single attempt.
///
/// The variant is selected by the target's transport, not by the kernel
/// guessing: `DirectHttp` targets become [`Executor::Chat`] (speak the protocol
/// over HTTP, no workspace), `CliWrap` targets become [`Executor::Agent`]
/// (spawn a harness subprocess). Keeping the two behind one enum lets the
/// dispatch loop stay transport-agnostic.
#[derive(Clone)]
pub enum Executor {
    /// A direct-HTTP chat target.
    Chat(Arc<dyn ChatClient>),
    /// A CLI-harness target.
    Agent(Arc<dyn Harness>),
}

/// Builds an [`Executor`] for a resolved target.
///
/// Injected into the kernel so the kernel stays runtime-free: it composes
/// `vyane-core` traits without ever constructing a concrete adapter. The
/// implementation is expected to route on [`BoundTarget::transport`]
/// (`DirectHttp` → [`Executor::Chat`], `CliWrap` → [`Executor::Agent`]) and may
/// fail — e.g. when a required harness binary is absent — in which case the
/// dispatch loop classifies the error and applies the failover gate to it like
/// any other attempt failure.
pub trait ExecutorFactory: Send + Sync {
    /// Trusted, side-effect-free capability declaration for `target`.
    ///
    /// The default is deliberately chat-only so existing custom factories
    /// remain source-compatible without silently gaining filesystem powers.
    fn capability_manifest(&self, _target: &BoundTarget) -> CapabilityManifest {
        CapabilityManifest::chat_only()
    }

    /// Construct the executor for `target`, or fail if it cannot be built.
    fn make(&self, target: &BoundTarget) -> Result<Executor>;

    /// Construct an executor with stable audit context for this attempt.
    ///
    /// Existing factories remain compatible through the default delegation.
    /// `scope` is serializable evidence only and grants no runtime authority.
    fn make_scoped(&self, target: &BoundTarget, _scope: &AttemptScope) -> Result<Executor> {
        self.make(target)
    }
}
