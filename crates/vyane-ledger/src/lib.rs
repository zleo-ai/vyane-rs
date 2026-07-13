//! # vyane-ledger
//!
//! Persistence for Vyane: an append-only JSONL [`vyane_core::Ledger`], a
//! filesystem [`vyane_core::SessionStore`], owner-isolated [`EventLog`] streams,
//! and cost estimation from a price table.
//!
//! - [`JsonlLedger`] appends [`vyane_core::RunRecord`]s to one file, guarded by
//!   an advisory lock for cross-process safety, and answers [`vyane_core::RunQuery`]
//!   with a most-recent-first reverse scan that tolerates corrupt lines.
//! - [`FsSessionStore`] writes one JSON file per session via tmp + atomic
//!   rename, so readers never observe a half-written session.
//! - [`EventLog`] keeps owner-scoped, replayable event streams with monotonic
//!   sequence allocation and selectable buffered or durable appends.
//! - [`PriceTable`] turns recorded [`vyane_core::Usage`] into a `cost_usd`,
//!   never guessing an unknown model (it returns `None`).
//!
//! See `docs/plan/WP-05.md` for the work-package specification.

pub mod cost;
pub mod event;
pub mod jsonl;
pub mod session;

pub use cost::{ModelPricing, PriceTable};
pub use event::{
    EventCategory, EventCursor, EventDurability, EventLog, EventLogError, EventPage, EventRecord,
    EventResult, EventSource, NewEvent,
};
pub use jsonl::JsonlLedger;
pub use session::FsSessionStore;
