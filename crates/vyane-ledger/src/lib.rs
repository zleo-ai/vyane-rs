//! # vyane-ledger
//!
//! Persistence for Vyane: an append-only JSONL [`vyane_core::Ledger`], a
//! filesystem [`vyane_core::SessionStore`], owner-isolated [`EventLog`] streams,
//! and cost estimation from a price table.
//!
//! - [`JsonlLedger`] appends [`vyane_core::RunRecord`]s to one file, guarded by
//!   an advisory lock for cross-process safety, and answers [`vyane_core::RunQuery`]
//!   with a most-recent-first reverse scan that tolerates corrupt lines.
//! - [`history_archive`] moves the older half of a ledger that grows past
//!   [`ROTATE_MAX_BYTES`] into monthly gzip files next to it. [`JsonlLedger`]
//!   does not call it, and `vyane history` does not read those files.
//! - [`FsSessionStore`] writes one JSON file per session via tmp + atomic
//!   rename, so readers never observe a half-written session.
//! - [`EventLog`] keeps owner-scoped, replayable event streams with monotonic
//!   sequence allocation and selectable buffered or durable appends.
//! - [`JsonlFeedbackStore`] appends owner-scoped 1–5 run ratings and folds
//!   them so the latest rating per run and source wins.
//! - [`PriceTable`] turns recorded [`vyane_core::Usage`] into a `cost_usd`,
//!   never guessing an unknown model (it returns `None`).
//!
//! See `docs/plan/WP-05.md` for the work-package specification.

pub mod cost;
pub mod event;
pub mod feedback;
pub mod history_archive;
pub mod jsonl;
pub mod session;

pub use cost::{ModelPricing, PriceTable};
pub use event::{
    EventCategory, EventCursor, EventDurability, EventLog, EventLogError, EventPage, EventRecord,
    EventResult, EventSource, NewEvent,
};
pub use feedback::{
    AutoRule, FeedbackPage, FeedbackQuery, FeedbackRecord, FeedbackSource, JsonlFeedbackStore,
    Rating, RatingIndex,
};
pub use history_archive::{
    ROTATE_MAX_BYTES, RecentPage, RecentQuery, RecordRead, maybe_rotate, maybe_rotate_with_cap,
    month_of_record, query_recent, read_record_lines,
};
pub use jsonl::JsonlLedger;
pub use session::FsSessionStore;
