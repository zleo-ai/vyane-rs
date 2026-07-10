//! The REST API layer: thin axum routing on top of [`VyaneService`].
//!
//! Every handler constructs the same `DispatchParams` / `BroadcastParams` /
//! `HistoryFilter` the CLI does and hands them to one shared service, so dispatch
//! semantics are identical regardless of whether a request arrives over the
//! command line or over HTTP. The service is loaded once at startup and shared
//! across requests via axum `State` (it is `Clone`-cheap — everything is behind
//! an `Arc`).
//!
//! JSON fields are snake_case throughout, matching the kernel's own
//! `Serialize`/`Deserialize` derives. The one wire-format wrinkle is `sandbox`:
//! the request body accepts the snake-case spellings (`read_only`, `write`,
//! `full`) that read naturally in JSON, while the kernel's `Sandbox` enum
//! serializes *back* as kebab-case (`read-only`) — that is the form already
//! pinned by `RunRecord` in the ledger, so the response preserves it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;
use vyane_core::{CancellationToken, RunStatus, Sandbox, SessionRecord};
use vyane_kernel::{DispatchOutcome, StreamDispatchEvent};
use vyane_service::{
    BroadcastParams, DispatchParams, HistoryFilter, VyaneService, parse_labels,
    resolve_target_chain,
};

/// Shared service state. [`VyaneService`] is already cheap to clone (all `Arc`
/// internally), but wrapping it once avoids even the atomic bump per request.
#[derive(Clone)]
pub struct ApiState {
    service: Arc<VyaneService>,
}

/// Body for `POST /v1/dispatch`. Field names mirror [`DispatchParams`] minus the
/// fields the server owns (cancellation, runtime config).
#[derive(Debug, Deserialize)]
pub struct DispatchRequest {
    pub task: String,
    /// Profile name or `provider/model`.
    pub target: String,
    #[serde(default)]
    pub workdir: Option<String>,
    /// `read_only` | `write` | `full`. Defaults to `read_only`.
    #[serde(default)]
    pub sandbox: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Each entry is a `key=value` label, matching `--label` on the CLI.
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

/// Body for `POST /v1/broadcast`. Like [`DispatchRequest`] but `targets` is a
/// single comma-separated string, matching the CLI's `--targets` flag.
#[derive(Debug, Deserialize)]
pub struct BroadcastRequest {
    pub task: String,
    /// Comma-separated list; each element is a profile or `provider/model`.
    pub targets: String,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub sandbox: Option<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

/// Query params for `GET /v1/runs`.
#[derive(Debug, Default, Deserialize)]
pub struct RunsQuery {
    /// Max records to return. `None` defaults to 100. `0` is rejected as 400.
    #[serde(default)]
    pub limit: Option<usize>,
    /// `success` | `error` | `timeout` | `cancelled`.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
}

/// Default and max record limits for `GET /v1/runs`.
const DEFAULT_RUN_LIMIT: usize = 100;
const MAX_RUN_LIMIT: usize = 10_000;

/// One row in a `{"items":[...]}` envelope. Mirrors the CLI's `BroadcastJson`
/// shape so a broadcast result is identical over HTTP and `--json`.
#[derive(Debug, Serialize)]
pub struct BroadcastItem {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<vyane_core::RunRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ItemsEnvelope<T: Serialize> {
    items: Vec<T>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// An API error that maps onto an HTTP status code. The conversion below keeps
/// the mapping in one place: config/resolution errors are caller faults (400),
/// everything else is a server fault (500).
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    /// Classify a service error: config/resolution/label-parsing failures are
    /// caller faults (400), everything else is a server fault (500).
    ///
    /// The error chain is logged server-side (stderr) for debugging; only a
    /// generic message reaches the client to avoid leaking internal paths,
    /// endpoint URLs, or secret-resolution details.
    fn from_service_error(e: anyhow::Error) -> Self {
        let msg = e.to_string();
        let display = format!("{e:#}");
        eprintln!("dispatch/broadcast error: {display}");
        if is_caller_fault(&msg) {
            Self::bad_request(msg)
        } else {
            Self::internal("internal error")
        }
    }
}

/// Heuristic: a resolution/config error message mentions profiles, providers,
/// endpoints, labels, or "not found" — all caller-input problems. A genuine
/// server fault (transport, auth upstream, spawn) does not.
fn is_caller_fault(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("profile")
        || lower.contains("provider")
        || lower.contains("endpoint")
        || lower.contains("label")
        || lower.contains("not found")
        || lower.contains("no such")
        || lower.contains("missing")
        || lower.contains("invalid")
        || lower.contains("targets must")
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

/// Maximum request body size (16 MiB). Large enough for substantial task/system
/// prompts, small enough to prevent a single client from OOMing the process.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Build the axum router for the v1 API. The service is held in shared state so
/// config is loaded exactly once at startup.
pub fn build_router(service: VyaneService) -> Router {
    let state = ApiState {
        service: Arc::new(service),
    };

    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/dispatch", post(dispatch))
        .route("/v1/dispatch/stream", post(dispatch_stream))
        .route("/v1/broadcast", post(broadcast))
        .route("/v1/runs", get(runs))
        .route("/v1/sessions", get(sessions))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Run the API server until interrupted. The caller loads the service and hands
/// it in; this function owns the listener and graceful shutdown.
pub async fn run_server(service: VyaneService, addr: SocketAddr) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("bind {addr}: {e}"))?;
    let router = build_router(service);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!("serve {addr}: {e}"))?;
    Ok(())
}

/// Wait for ctrl-c (or SIGTERM on Unix) to trigger graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            // Ignore the error — the only observable effect of a failed install
            // is that ctrl-c won't trigger shutdown, which is not fatal.
        }
    };

    #[cfg(unix)]
    let term = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };

    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn dispatch(
    State(state): State<ApiState>,
    Json(req): Json<DispatchRequest>,
) -> Result<Json<DispatchOutcome>, ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    // Validate label shape up front (mirrors the CLI's input-phase check) so a
    // malformed `key=value` is rejected before any dispatch work begins.
    let _ = parse_labels(labels.clone()).map_err(|e| ApiError::bad_request(format!("{e:#}")))?;

    let params = DispatchParams {
        task: req.task,
        target: req.target,
        workdir: req.workdir.map(PathBuf::from),
        sandbox,
        session: req.session,
        system: req.system,
        timeout_secs: req.timeout_secs,
        labels,
    };

    // v1: a fresh, never-cancelled token. Timeout-to-cancel wiring is a future
    // concern; the kernel already enforces `timeout_secs` per attempt.
    let cancel = CancellationToken::new();
    let outcome = state
        .service
        .dispatch(params, cancel)
        .await
        .map_err(ApiError::from_service_error)?;
    Ok(Json(outcome))
}

/// SSE event type sent over the streaming endpoint.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SsePayload {
    Delta {
        text: String,
    },
    ReasoningDelta {
        text: String,
    },
    Finished {
        record: Box<vyane_core::RunRecord>,
        output: Option<String>,
    },
    Unsupported,
}

/// `POST /v1/dispatch/stream` — dispatch a task and stream deltas as
/// Server-Sent Events. Each event's `data` field is a JSON object with a
/// `type` discriminator: `delta`, `reasoning_delta`, `finished`, or
/// `unsupported`.
///
/// Only works for single direct-HTTP targets (no harness, no failover). When
/// the client declines streaming, an `unsupported` event is sent and the
/// caller should retry with the non-streaming `/v1/dispatch` endpoint.
async fn dispatch_stream(
    State(state): State<ApiState>,
    Json(req): Json<DispatchRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    let _ = parse_labels(labels.clone()).map_err(|e| ApiError::bad_request(format!("{e:#}")))?;

    // Resolve the target chain up front so we can validate the selector and
    // extract the single bound target for streaming.
    let loaded = state.service.config();
    let chain = resolve_target_chain(loaded, &req.target)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    // Streaming only works for a single direct-HTTP target.
    let bound = match chain.as_slice() {
        [b] if b.transport == vyane_core::AdapterTransport::DirectHttp => b.clone(),
        _ => {
            return Err(ApiError::bad_request(
                "streaming requires a single direct-HTTP target (no harness, no failover)",
            ));
        }
    };

    let task = vyane_service::build_task_spec(
        req.task,
        req.workdir.map(PathBuf::from),
        sandbox,
        req.system,
        req.timeout_secs,
        labels,
    )
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    // Sessions are not supported on the streaming path (same as CLI --stream).
    let task = if let Some(session) = req.session {
        let mut t = task;
        t.session = Some(vyane_core::SessionRef::new(session));
        t
    } else {
        task
    };

    let cancel = CancellationToken::new();
    let dispatcher = state.service.runtime().dispatcher.clone();

    // Bridge the callback-based dispatch_stream to an SSE stream via a channel.
    let (tx, mut rx) = mpsc::channel::<SsePayload>(64);

    tokio::spawn(async move {
        let tx = tx;
        let outcome = dispatcher
            .dispatch_stream(&task, &bound, cancel, |event| {
                let payload = match event {
                    StreamDispatchEvent::Delta(text) => SsePayload::Delta { text },
                    StreamDispatchEvent::ReasoningDelta(text) => {
                        SsePayload::ReasoningDelta { text }
                    }
                };
                // Best-effort send: if the client disconnected (receiver dropped),
                // the error is silently ignored — the dispatch continues and the
                // RunRecord is still ledger-appended by the kernel.
                let _ = tx.try_send(payload);
            })
            .await;

        let final_payload = match outcome {
            Ok(None) => SsePayload::Unsupported,
            Ok(Some(outcome)) => SsePayload::Finished {
                record: Box::new(outcome.record),
                output: outcome.output,
            },
            Err(e) => {
                eprintln!("dispatch_stream error: {e:#}");
                SsePayload::Finished {
                    record: Box::new(vyane_core::RunRecord {
                        run_id: String::new(),
                        owner: "local".into(),
                        started_at: chrono::Utc::now(),
                        finished_at: chrono::Utc::now(),
                        task_digest: String::new(),
                        task_preview: None,
                        workdir: None,
                        sandbox: task.sandbox,
                        target: bound.target.clone(),
                        transport: bound.transport,
                        attempts: vec![],
                        status: RunStatus::Error,
                        usage: None,
                        cost_usd: None,
                        session_id: None,
                        output_chars: None,
                        error: Some(e.to_string()),
                        labels: task.labels.clone(),
                    }),
                    output: None,
                }
            }
        };
        let _ = tx.try_send(final_payload);
    });

    // Convert the receiver into a futures::Stream of SSE Events.
    let stream = async_stream::stream! {
        while let Some(payload) = rx.recv().await {
            let json = serde_json::to_string(&payload).unwrap_or_else(|_| {
                r#"{"type":"delta","text":"(serialization error)"}"#.to_string()
            });
            yield Ok::<Event, std::convert::Infallible>(Event::default().data(json));
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn broadcast(
    State(state): State<ApiState>,
    Json(req): Json<BroadcastRequest>,
) -> Result<Json<ItemsEnvelope<BroadcastItem>>, ApiError> {
    let sandbox = parse_sandbox(req.sandbox.as_deref())?;
    let labels = req.labels.unwrap_or_default();
    let _ = parse_labels(labels.clone()).map_err(|e| ApiError::bad_request(format!("{e:#}")))?;

    let params = BroadcastParams {
        task: req.task,
        targets: req.targets,
        workdir: req.workdir.map(PathBuf::from),
        sandbox,
        system: req.system,
        timeout_secs: req.timeout_secs,
        labels,
    };

    let cancel = CancellationToken::new();
    let results = state.service.broadcast(params, cancel).await.map_err(|e| {
        // Only caller-fault errors (bad targets list, bad labels, bad task
        // spec) reach here — per-target resolution errors are already in
        // the per-item results.
        eprintln!("broadcast setup error: {e:#}");
        ApiError::bad_request(e.to_string())
    })?;

    let items = results
        .into_iter()
        .map(|(target, result)| match result {
            Ok(outcome) => BroadcastItem {
                target,
                record: Some(outcome.record),
                output: outcome.output,
                error: None,
            },
            Err(e) => {
                // Per-target error: log the full chain server-side, surface a
                // concise message to the client.
                eprintln!("broadcast target `{target}` error: {e:#}");
                BroadcastItem {
                    target,
                    record: None,
                    output: None,
                    error: Some(e.to_string()),
                }
            }
        })
        .collect();

    Ok(Json(ItemsEnvelope { items }))
}

async fn runs(
    State(state): State<ApiState>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<ItemsEnvelope<vyane_core::RunRecord>>, ApiError> {
    let status = match query.status.as_deref() {
        Some(s) => Some(parse_run_status(s)?),
        None => None,
    };

    let limit = match query.limit {
        Some(0) => {
            return Err(ApiError::bad_request(
                "limit must be greater than 0 (omit for default, or use a positive number)",
            ));
        }
        Some(n) if n > MAX_RUN_LIMIT => {
            return Err(ApiError::bad_request(format!(
                "limit {n} exceeds maximum of {MAX_RUN_LIMIT}"
            )));
        }
        Some(n) => Some(n),
        None => Some(DEFAULT_RUN_LIMIT),
    };

    let filter = HistoryFilter {
        limit,
        status,
        provider: query.provider,
    };

    let records = state
        .service
        .history(filter)
        .await
        .map_err(|e| ApiError::internal(format!("{e:#}")))?;
    Ok(Json(ItemsEnvelope { items: records }))
}

async fn sessions(
    State(state): State<ApiState>,
) -> Result<Json<ItemsEnvelope<SessionRecord>>, ApiError> {
    let records = state
        .service
        .sessions()
        .await
        .map_err(|e| ApiError::internal(format!("{e:#}")))?;
    Ok(Json(ItemsEnvelope { items: records }))
}

/// Parse a sandbox string from a request body. Accepts the snake-case spellings
/// (`read_only`, `write`, `full`) that read naturally in JSON; `read-only` is
/// also accepted so the value round-trips with the kernel's own serialization.
fn parse_sandbox(raw: Option<&str>) -> Result<Sandbox, ApiError> {
    match raw {
        None | Some("read_only") | Some("read-only") => Ok(Sandbox::ReadOnly),
        Some("write") => Ok(Sandbox::Write),
        Some("full") => Ok(Sandbox::Full),
        Some(other) => Err(ApiError::bad_request(format!(
            "unknown sandbox `{other}` (expected read_only, write, or full)"
        ))),
    }
}

/// Parse a run-status filter string. Matches the `snake_case` serialization of
/// [`RunStatus`].
fn parse_run_status(raw: &str) -> Result<RunStatus, ApiError> {
    match raw {
        "success" => Ok(RunStatus::Success),
        "error" => Ok(RunStatus::Error),
        "timeout" => Ok(RunStatus::Timeout),
        "cancelled" => Ok(RunStatus::Cancelled),
        _ => Err(ApiError::bad_request(format!(
            "unknown status `{raw}` (expected success, error, timeout, or cancelled)"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // --- Request deserialization ------------------------------------------

    #[test]
    fn dispatch_request_minimal() {
        let json = r#"{"task":"say hi","target":"openai/gpt-4"}"#;
        let req: DispatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.task, "say hi");
        assert_eq!(req.target, "openai/gpt-4");
        assert_eq!(req.workdir, None);
        assert_eq!(req.sandbox, None);
        assert_eq!(req.session, None);
        assert_eq!(req.system, None);
        assert_eq!(req.timeout_secs, None);
        assert_eq!(req.labels, None);
    }

    #[test]
    fn dispatch_request_full() {
        let json = r#"{
            "task":"do the thing",
            "target":"prod",
            "workdir":"/tmp/work",
            "sandbox":"write",
            "session":"s1",
            "system":"be terse",
            "timeout_secs":30,
            "labels":["env=prod","team=ops"]
        }"#;
        let req: DispatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.task, "do the thing");
        assert_eq!(req.target, "prod");
        assert_eq!(req.workdir.as_deref(), Some("/tmp/work"));
        assert_eq!(req.sandbox.as_deref(), Some("write"));
        assert_eq!(req.session.as_deref(), Some("s1"));
        assert_eq!(req.system.as_deref(), Some("be terse"));
        assert_eq!(req.timeout_secs, Some(30));
        assert_eq!(
            req.labels.unwrap(),
            vec!["env=prod".to_string(), "team=ops".to_string()]
        );
    }

    #[test]
    fn broadcast_request_minimal() {
        let json = r#"{"task":"hi","targets":"a,b,c"}"#;
        let req: BroadcastRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.task, "hi");
        assert_eq!(req.targets, "a,b,c");
        assert_eq!(req.sandbox, None);
    }

    #[test]
    fn dispatch_request_requires_task() {
        let json = r#"{"target":"x"}"#;
        assert!(serde_json::from_str::<DispatchRequest>(json).is_err());
    }

    #[test]
    fn dispatch_request_requires_target() {
        let json = r#"{"task":"x"}"#;
        assert!(serde_json::from_str::<DispatchRequest>(json).is_err());
    }

    // --- Sandbox parsing --------------------------------------------------

    #[test]
    fn sandbox_default_is_read_only() {
        assert_eq!(parse_sandbox(None).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_snake_case_read_only() {
        assert_eq!(parse_sandbox(Some("read_only")).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_kebab_case_read_only() {
        assert_eq!(parse_sandbox(Some("read-only")).unwrap(), Sandbox::ReadOnly);
    }

    #[test]
    fn sandbox_write() {
        assert_eq!(parse_sandbox(Some("write")).unwrap(), Sandbox::Write);
    }

    #[test]
    fn sandbox_full() {
        assert_eq!(parse_sandbox(Some("full")).unwrap(), Sandbox::Full);
    }

    #[test]
    fn sandbox_unknown_errors() {
        let err = parse_sandbox(Some("danger")).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("danger"));
    }

    // --- Run status parsing -----------------------------------------------

    #[test]
    fn run_status_all_variants() {
        assert_eq!(parse_run_status("success").unwrap(), RunStatus::Success);
        assert_eq!(parse_run_status("error").unwrap(), RunStatus::Error);
        assert_eq!(parse_run_status("timeout").unwrap(), RunStatus::Timeout);
        assert_eq!(parse_run_status("cancelled").unwrap(), RunStatus::Cancelled);
    }

    #[test]
    fn run_status_unknown_errors() {
        let err = parse_run_status("pending").unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("pending"));
    }

    // --- Error response formatting ----------------------------------------

    #[test]
    fn error_body_serializes() {
        let body = ErrorBody {
            error: "bad target".into(),
        };
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(json, r#"{"error":"bad target"}"#);
    }

    #[test]
    fn api_error_bad_request_status() {
        let err = ApiError::bad_request("nope");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.message, "nope");
    }

    #[test]
    fn api_error_internal_status() {
        let err = ApiError::internal("boom");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.message, "boom");
    }

    // --- Envelope ---------------------------------------------------------

    #[test]
    fn items_envelope_serializes_empty() {
        let env = ItemsEnvelope {
            items: Vec::<BroadcastItem>::new(),
        };
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(json, r#"{"items":[]}"#);
    }

    #[test]
    fn health_response_serializes() {
        let json = serde_json::to_string(&HealthResponse { status: "ok" }).unwrap();
        assert_eq!(json, r#"{"status":"ok"}"#);
    }
}
