//! # vyane-harness
//!
//! CLI harness adapters implementing [`vyane_core::Harness`]: Claude Code,
//! Codex CLI. Spawns each as a subprocess with an environment built strictly
//! through [`vyane_core::EnvPolicy`] — clean by default, never the parent's
//! full environment.
//!
//! See `docs/plan/WP-03-harness-adapters.md` for the work-package plan.
