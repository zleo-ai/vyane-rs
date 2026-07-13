//! What the caller asks for: a task plus generation parameters.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::session::SessionRef;
use crate::target::Sandbox;

/// Process-local authority that must remain valid at harness spawn boundaries.
///
/// The callback is invoked synchronously immediately before every physical
/// spawn attempt and, for lifecycle-gated children, again before releasing the
/// real target. It must perform a bounded live revalidation and return `false`
/// on stale, revoked, unavailable, or uncertain authority. The callback is
/// executable state: it must never be serialized, persisted, logged, or passed
/// into the child environment. It must also keep panic payloads body-free:
/// [`Self::revalidate`] catches an unwind, but the process panic hook runs
/// before that catch can redact anything.
#[derive(Clone)]
pub struct HarnessSpawnAuthority {
    callback: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
}

impl HarnessSpawnAuthority {
    pub fn new(callback: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    /// Revalidate and fail closed without propagating a callback panic.
    #[must_use]
    pub fn revalidate(&self) -> bool {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.callback)()))
            .unwrap_or(false)
    }
}

impl fmt::Debug for HarnessSpawnAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarnessSpawnAuthority")
            .finish_non_exhaustive()
    }
}

/// Runtime lifecycle event for a process group owned by a CLI harness.
///
/// Harness children run in their own session, so their process-group id is
/// intentionally distinct from a detached Vyane worker's process group. A
/// detached caller can use these events to maintain a private sidecar with the
/// currently-live inner process group and cancel it independently if the
/// worker is killed before Rust destructors can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessLifecycleEvent {
    /// The harness child was spawned successfully and its process group is
    /// live. On Unix, `pid == pgid` because the child calls `setsid(2)`.
    Started { pid: u32, pgid: i32 },
    /// The harness has completed normal cleanup, or its owning future was
    /// dropped and synchronously signalled the process group for cleanup.
    Stopped {
        pid: u32,
        pgid: i32,
        /// True only after the harness observed the entire isolated process
        /// group disappear. Abrupt future drop reports false after issuing its
        /// best-effort SIGKILL, so a durable controller retains the sidecar.
        group_empty: bool,
    },
}

/// Clone-safe, runtime-only observer for harness process-group lifecycle.
///
/// The callback is deliberately held behind an [`Arc`]: cloned [`TaskSpec`]s
/// and failover attempts must all report to the same runtime observer. This
/// value is executable process-local state and must never be serialized into a
/// task request, ledger record, or durable task database.
#[derive(Clone)]
pub struct HarnessLifecycleReporter {
    callback:
        Arc<dyn Fn(HarnessLifecycleEvent) -> crate::error::Result<()> + Send + Sync + 'static>,
}

impl HarnessLifecycleReporter {
    pub fn new(
        callback: impl Fn(HarnessLifecycleEvent) -> crate::error::Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    /// Report one lifecycle transition synchronously.
    ///
    /// A failed [`HarnessLifecycleEvent::Started`] is a hard coordination
    /// failure: harness implementations kill and reap the just-spawned process
    /// group instead of running a child that an independent controller cannot
    /// discover. A failed `Stopped` is best-effort because the group is already
    /// dead (or has already been synchronously signalled from a drop guard).
    pub fn report(&self, event: HarnessLifecycleEvent) -> crate::error::Result<()> {
        (self.callback)(event)
    }
}

impl fmt::Debug for HarnessLifecycleReporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarnessLifecycleReporter")
            .finish_non_exhaustive()
    }
}

/// Reasoning-effort level, passed through to targets that support it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
}

impl Effort {
    pub fn as_str(&self) -> &str {
        match self {
            Effort::Low => "low",
            Effort::Medium => "medium",
            Effort::High => "high",
            Effort::Xhigh => "xhigh",
        }
    }
}

/// Generation parameters, normalized across protocols.
///
/// Each protocol client maps these onto its own wire fields (for example
/// `max_output_tokens` becomes the appropriate output-limit field per
/// protocol). Reasoning models may count "thinking" tokens against output
/// limits — leaving `max_output_tokens` unset is the safe default.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,
    /// Provider/protocol-specific passthrough values, applied last.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One unit of work submitted to the kernel.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    /// The task / prompt text.
    pub prompt: String,
    /// Optional system prompt (direct chat) or appended instructions (harness).
    pub system: Option<String>,
    /// Working directory for harness runs. Ignored by direct chat.
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    /// Continue an existing session instead of starting fresh.
    pub session: Option<SessionRef>,
    /// `None` = no timeout. Long agentic runs legitimately take hours;
    /// timeouts are opt-in, not a hidden default.
    pub timeout: Option<Duration>,
    /// Free-form labels recorded into the ledger (task tags, ticket ids…).
    pub labels: BTreeMap<String, String>,
    /// Optional process-local observer for harness child process groups.
    ///
    /// Runtime coordination only: this callback must not enter persisted task
    /// metadata or run records.
    pub harness_lifecycle_reporter: Option<HarnessLifecycleReporter>,
}

impl TaskSpec {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            system: None,
            workdir: None,
            sandbox: Sandbox::default(),
            session: None,
            timeout: None,
            labels: BTreeMap::new(),
            harness_lifecycle_reporter: None,
        }
    }

    pub fn with_workdir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.workdir = Some(dir.into());
        self
    }

    pub fn with_sandbox(mut self, sandbox: Sandbox) -> Self {
        self.sandbox = sandbox;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
