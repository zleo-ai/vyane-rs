//! # vyane-router
//!
//! Target selection policy: deterministic, side-effect-free routing that picks
//! a target based on task complexity signals, inferred tags, and user-configured
//! preferences.
//!
//! This is a clean-room reimplementation of Vyane's v5 routing design. The core
//! is a pure function: given a task text and structural signals (changed files,
//! retry count, stage), it computes a complexity score, maps that to a tier
//! (economy / mainline / frontier), infers tags from the text, and resolves a
//! preference. No LLM calls, no network access, no stateful caches.
//!
//! Feedback notes are an explicit caller input. Scoring ignores them unless
//! the caller sets [`FeedbackScoring::Enabled`], and an enabled note is only
//! recorded: it does not change the chosen target. The default path stays
//! deterministic.
//!
//! ## How it fits
//!
//! The router sits *between* having a task and dispatching it. A front-end
//! calls [`route_task`] with the task text, available profiles, and optional
//! signals, and receives a [`RouteDecision`] naming the provider, model, effort,
//! and tier to use. The decision's provider/model can then be resolved into a
//! target by the existing config/service layer.

mod decision;
mod feedback;
mod intent;
mod preference;
mod route;
mod score;
mod tags;

pub use decision::{RouteDecision, RouteEffort, RouteTier};
pub use feedback::{FeedbackNote, FeedbackOutcome, FeedbackScoring};
pub use intent::{IntentCategory, IntentResult, classify_intent};
pub use preference::{RoutePreferenceTable, RouteTargetPreference, normalize_key};
pub use route::{RoutedTask, route_task, route_task_with_feedback};
pub use score::{ComplexitySignals, complexity_score, effort_for_tier, tier_for_score};
pub use tags::infer_route_tags;
