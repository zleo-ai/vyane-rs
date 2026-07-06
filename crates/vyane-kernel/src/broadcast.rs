//! Broadcast: fan one task across many target chains concurrently.
//!
//! Each chain is dispatched through the same [`Dispatcher::dispatch`] state
//! machine, bounded by a concurrency semaphore, and results are returned **in
//! input order** even though chains complete out of order. Partial failure is
//! the normal case: some chains end in `Error`, others in `Success`, each with
//! its own [`vyane_core::RunRecord`].

use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::future::join_all;
use tokio::sync::Semaphore;
use vyane_core::{BoundTarget, CancellationToken, Result, TaskSpec};

use crate::dispatch::{DispatchOutcome, Dispatcher};

/// Default cap on chains dispatched concurrently by [`Dispatcher::broadcast`].
///
/// A finite default keeps a wide fan-out from opening an unbounded number of
/// in-flight attempts at once; callers who want a different width use
/// [`Dispatcher::broadcast_with_concurrency`].
pub const DEFAULT_BROADCAST_CONCURRENCY: usize = 8;

impl Dispatcher {
    /// Run `task` against every chain in `chains` concurrently, at the default
    /// concurrency, returning one result per chain **in input order**.
    ///
    /// See [`Dispatcher::broadcast_with_concurrency`] for the semantics; this is
    /// the same call with [`DEFAULT_BROADCAST_CONCURRENCY`].
    pub async fn broadcast(
        &self,
        task: &TaskSpec,
        chains: Vec<Vec<BoundTarget>>,
        cancel: CancellationToken,
    ) -> Vec<Result<DispatchOutcome>> {
        let width = NonZeroUsize::new(DEFAULT_BROADCAST_CONCURRENCY).unwrap_or(NonZeroUsize::MIN);
        self.broadcast_with_concurrency(task, chains, cancel, width)
            .await
    }

    /// Run `task` against every chain concurrently under a bound of
    /// `concurrency` simultaneous dispatches, returning results aligned to the
    /// input positions.
    ///
    /// Ordering is by construction, not by sorting: the returned vector's
    /// element `i` is the outcome of `chains[i]`, regardless of the order in
    /// which chains finished. Each element is an independent dispatch, so one
    /// chain failing (or even erroring at the kernel level) never disturbs the
    /// others' results or positions.
    pub async fn broadcast_with_concurrency(
        &self,
        task: &TaskSpec,
        chains: Vec<Vec<BoundTarget>>,
        cancel: CancellationToken,
        concurrency: NonZeroUsize,
    ) -> Vec<Result<DispatchOutcome>> {
        let permits = Arc::new(Semaphore::new(concurrency.get()));

        // Build one future per chain. `join_all` yields results positionally
        // aligned with this vector, which *is* the input order — so no explicit
        // re-ordering is needed despite out-of-order completion. The semaphore
        // caps how many are past `acquire` (i.e. actually dispatching) at once.
        let futures = chains.into_iter().map(|chain| {
            let permits = Arc::clone(&permits);
            let cancel = cancel.clone();
            async move {
                // Semaphore is never closed, so acquisition cannot fail; if it
                // somehow did, fall through and still dispatch rather than drop
                // the chain silently.
                let _permit = permits.acquire().await.ok();
                self.dispatch(task, chain, cancel).await
            }
        });

        join_all(futures).await
    }
}
