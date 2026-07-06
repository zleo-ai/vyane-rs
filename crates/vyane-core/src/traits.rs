//! The capability traits the kernel composes.
//!
//! * [`ChatClient`] — speaks one wire protocol over HTTP (no workspace).
//! * [`Harness`] — spawns a CLI execution shell as a subprocess.
//! * [`Ledger`] — append-only run accounting.
//! * [`SessionStore`] — session persistence.
//!
//! All traits are object-safe: the kernel holds them as trait objects and
//! composes targets at runtime from configuration.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;

use crate::chat::{ChatOutcome, ChatRequest, StreamEvent};
use crate::env::EnvPolicy;
use crate::error::Result;
use crate::run::{RunQuery, RunRecord, Usage};
use crate::session::SessionRecord;
use crate::target::{Endpoint, HarnessKind, ModelId, Protocol, Sandbox};
use crate::task::GenParams;

/// A client for one wire protocol against one endpoint.
#[async_trait]
pub trait ChatClient: Send + Sync {
    fn protocol(&self) -> Protocol;

    /// Perform a non-streaming completion.
    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome>;

    /// Perform a streaming completion. Default: unsupported.
    ///
    /// Implementations must remain live even when a target emits no
    /// reasoning deltas at all — completion is signalled by
    /// [`StreamEvent::Done`], never by reasoning traffic.
    async fn stream(&self, req: ChatRequest) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let _ = req;
        Err(crate::error::VyaneError::unsupported(format!(
            "{} client does not support streaming",
            self.protocol()
        )))
    }
}

/// Everything a harness needs to execute one job.
#[derive(Debug, Clone)]
pub struct HarnessJob {
    /// The task / prompt text (already assembled by the kernel).
    pub prompt: String,
    pub model: ModelId,
    /// wire protocol of the endpoint, letting harnesses pick a matching wire_api for custom endpoints
    pub protocol: Protocol,
    /// Endpoint override. `None` = the harness authenticates natively.
    pub endpoint: Option<Endpoint>,
    pub params: GenParams,
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    /// Native session id to resume, if continuing a session.
    pub resume: Option<String>,
    /// Environment policy; harnesses must build the child environment
    /// exclusively through [`EnvPolicy::build`].
    pub env: EnvPolicy,
    /// `None` = run until completion.
    pub timeout: Option<Duration>,
}

/// Result of a harness run.
#[derive(Debug, Clone)]
pub struct HarnessOutcome {
    /// The final answer text extracted from the harness output.
    pub text: String,
    /// Native session id for future resumes, when the harness reports one.
    pub native_session_id: Option<String>,
    pub usage: Option<Usage>,
    pub exit_code: i32,
    pub duration: Duration,
}

/// An execution shell that runs jobs as a subprocess.
#[async_trait]
pub trait Harness: Send + Sync {
    fn kind(&self) -> HarnessKind;

    /// Whether the harness binary is present and runnable on this machine.
    async fn available(&self) -> bool;

    /// Run a job to completion.
    ///
    /// Implementations must honour `cancel` promptly (kill the child process
    /// group — a bare child kill leaves grandchildren running), honour
    /// `job.timeout` when set, and classify failures onto
    /// [`crate::ErrorKind`] faithfully.
    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome>;
}

/// Append-only run accounting.
#[async_trait]
pub trait Ledger: Send + Sync {
    async fn append(&self, record: &RunRecord) -> Result<()>;
    async fn query(&self, query: RunQuery) -> Result<Vec<RunRecord>>;
}

/// Session persistence.
#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn load(&self, session_id: &str) -> Result<Option<SessionRecord>>;
    async fn save(&self, record: &SessionRecord) -> Result<()>;
    async fn list(&self, owner: Option<&str>) -> Result<Vec<SessionRecord>>;
}
