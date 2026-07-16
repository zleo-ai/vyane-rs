//! Bounded, platform-neutral upstream quota snapshots.
//!
//! This crate deliberately contains no provider endpoint, credential loader,
//! background task, or automatic routing action. Optional adapters implement
//! [`QuotaConnector`]; [`QuotaSnapshotRunner`] reads a finite connector set
//! with stable ordering, bounded concurrency and per-connector timeouts.

mod http;
mod model;
mod runner;

pub use http::{QuotaHttpReader, QuotaHttpResponse, QuotaTransportError};
pub use model::{
    MAX_CONNECTOR_ID_BYTES, MAX_PROVIDER_BYTES, MAX_QUOTA_WINDOWS, QuotaBalance, QuotaCard,
    QuotaStatus, QuotaUnit, QuotaValidationError, QuotaWindow,
};
pub use runner::{
    MAX_CONNECTORS, MAX_QUOTA_CONCURRENCY, QuotaConnector, QuotaConnectorError,
    QuotaConnectorErrorCode, QuotaReadPolicy, QuotaRunnerError, QuotaSnapshot, QuotaSnapshotRunner,
    QuotaSnapshotStatus,
};
