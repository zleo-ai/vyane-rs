//! Fail-closed client for the authenticated loopback workflow daemon.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use futures::StreamExt as _;
use reqwest::{Method, StatusCode, header};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use vyane_task::TaskRecord;
use vyane_workflow::{WorkflowRunId, WorkflowSourceBundle};

use crate::daemon::read_verified_client_control;
use crate::daemon_workflow::WorkflowSubmitRequest;
pub(crate) use crate::daemon_workflow::WorkflowTaskView;

const HEALTH_RESPONSE_LIMIT: usize = 16 * 1024;
const API_RESPONSE_LIMIT: usize = 256 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize)]
struct HealthResponse {
    status: String,
    instance_id: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: String,
    message: String,
}

/// A submission failure whose user-facing representation is deliberately
/// bounded and contains no request body, variables, daemon token, or raw
/// transport error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkflowSubmitError {
    /// Request serialization failed before reqwest was allowed to send it.
    NotSubmitted {
        run_id: WorkflowRunId,
        reason: &'static str,
    },
    /// The daemon returned an explicit HTTP 4xx rejection.
    Rejected {
        run_id: WorkflowRunId,
        status: u16,
        code: &'static str,
    },
    /// Transmission began, but the client cannot prove whether the daemon
    /// durably accepted the idempotent submission.
    OutcomeUnknown {
        run_id: WorkflowRunId,
        status: Option<u16>,
        reason: &'static str,
    },
}

impl WorkflowSubmitError {
    pub(crate) fn exit_code(&self) -> u8 {
        match self {
            Self::Rejected { .. } => 2,
            Self::NotSubmitted { .. } | Self::OutcomeUnknown { .. } => 1,
        }
    }

    /// Structured stderr payload for `workflow submit --json` failures.
    /// stdout is reserved for a successful `WorkflowTaskView`.
    pub(crate) fn json_value(&self) -> serde_json::Value {
        match self {
            Self::NotSubmitted { run_id, reason } => serde_json::json!({
                "error": "workflow_submission_not_submitted",
                "outcome": "not_submitted",
                "workflow_run_id": run_id,
                "http_status": null,
                "reason": reason,
                "status_command": format!("vyane workflow status {run_id}"),
                "safe_retry": format!(
                    "retry the same intended submission with --id {run_id}"
                ),
            }),
            Self::Rejected {
                run_id,
                status,
                code,
            } => serde_json::json!({
                "error": "workflow_submission_rejected",
                "outcome": "rejected",
                "workflow_run_id": run_id,
                "http_status": status,
                "code": code,
                "status_command": format!("vyane workflow status {run_id}"),
                "safe_retry": format!(
                    "correct the rejection first; reuse --id {run_id} only for the same intended submission"
                ),
            }),
            Self::OutcomeUnknown {
                run_id,
                status,
                reason,
            } => serde_json::json!({
                "error": "workflow_submission_outcome_unknown",
                "outcome": "outcome_unknown",
                "workflow_run_id": run_id,
                "http_status": status,
                "reason": reason,
                "status_command": format!("vyane workflow status {run_id}"),
                "safe_retry": format!(
                    "check status first, then retry the identical submission with --id {run_id}"
                ),
            }),
        }
    }
}

impl fmt::Display for WorkflowSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSubmitted { run_id, reason } => write!(
                formatter,
                "workflow submission {run_id} was definitely not submitted: {reason}; safe retry: retry the same intended submission with --id {run_id}"
            ),
            Self::Rejected {
                run_id,
                status,
                code,
            } => write!(
                formatter,
                "workflow submission {run_id} was explicitly rejected (HTTP {status}, {code}); status check: `vyane workflow status {run_id}`; safe retry: correct the rejection first and reuse --id {run_id} only for the same intended submission"
            ),
            Self::OutcomeUnknown {
                run_id,
                status,
                reason,
            } => {
                let status =
                    status.map_or_else(|| "unavailable".to_string(), |status| status.to_string());
                write!(
                    formatter,
                    "workflow submission outcome_unknown (id: {run_id}, HTTP status: {status}, reason: {reason}); status check: `vyane workflow status {run_id}`; safe retry: check status first, then retry the identical submission with --id {run_id}"
                )
            }
        }
    }
}

impl std::error::Error for WorkflowSubmitError {}

pub(crate) struct DaemonWorkflowClient {
    http: reqwest::Client,
    addr: SocketAddr,
    instance_id: Arc<str>,
    token: Arc<str>,
}

impl DaemonWorkflowClient {
    pub(crate) async fn connect() -> Result<Self> {
        let control = read_verified_client_control()?;
        let client = Self {
            http: build_http_client()?,
            addr: control.addr,
            instance_id: Arc::from(control.instance_id),
            token: Arc::from(control.token),
        };
        let health: HealthResponse = client
            .send_json(
                client.request(Method::GET, "/health"),
                StatusCode::OK,
                HEALTH_RESPONSE_LIMIT,
            )
            .await
            .context("authenticate resident workflow daemon")?;
        if health.status != "ok" || health.instance_id != client.instance_id.as_ref() {
            bail!("daemon health identity does not match its control descriptor");
        }
        Ok(client)
    }

    pub(crate) async fn submit(
        &self,
        run_id: &WorkflowRunId,
        execution_cwd: PathBuf,
        bundle: WorkflowSourceBundle,
        vars: BTreeMap<String, String>,
    ) -> std::result::Result<WorkflowTaskView, WorkflowSubmitError> {
        let request = WorkflowSubmitRequest {
            run_id: run_id.clone(),
            execution_cwd,
            bundle,
            vars,
        };
        let body = serialize_submission_request(run_id, &request)?;
        let response = self
            .request(Method::POST, "/v1/workflows")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() {
                    WorkflowSubmitError::NotSubmitted {
                        run_id: run_id.clone(),
                        reason: "connection was not established, so no submission was sent",
                    }
                } else {
                    WorkflowSubmitError::OutcomeUnknown {
                        run_id: run_id.clone(),
                        status: None,
                        reason: if error.is_timeout() {
                            "request timed out after submission began"
                        } else {
                            "request transport failed after submission began"
                        },
                    }
                }
            })?;
        let status = response.status();

        if status.is_client_error() {
            let code = match read_response_limited(response, API_RESPONSE_LIMIT).await {
                Ok(bytes) => safe_rejection_code(&bytes),
                Err(_) => "http_rejection",
            };
            return Err(WorkflowSubmitError::Rejected {
                run_id: run_id.clone(),
                status: status.as_u16(),
                code,
            });
        }

        if status != StatusCode::ACCEPTED {
            let reason = if status.is_server_error() {
                "daemon returned a server error after submission began"
            } else {
                "daemon returned an unexpected response after submission began"
            };
            return Err(WorkflowSubmitError::OutcomeUnknown {
                run_id: run_id.clone(),
                status: Some(status.as_u16()),
                reason,
            });
        }

        let bytes = read_response_limited(response, API_RESPONSE_LIMIT)
            .await
            .map_err(|_| WorkflowSubmitError::OutcomeUnknown {
                run_id: run_id.clone(),
                status: Some(status.as_u16()),
                reason: "accepted response could not be read within the response bound",
            })?;
        let view: WorkflowTaskView =
            serde_json::from_slice(&bytes).map_err(|_| WorkflowSubmitError::OutcomeUnknown {
                run_id: run_id.clone(),
                status: Some(status.as_u16()),
                reason: "accepted response was not valid workflow JSON",
            })?;
        if view.task.id != run_id.as_str() {
            return Err(WorkflowSubmitError::OutcomeUnknown {
                run_id: run_id.clone(),
                status: Some(status.as_u16()),
                reason: "accepted response did not confirm the requested workflow id",
            });
        }
        Ok(view)
    }

    pub(crate) async fn status(&self, id: &WorkflowRunId) -> Result<WorkflowTaskView> {
        self.send_json(
            self.request(Method::GET, &format!("/v1/workflows/{id}")),
            StatusCode::OK,
            API_RESPONSE_LIMIT,
        )
        .await
    }

    pub(crate) async fn cancel(&self, id: &WorkflowRunId) -> Result<TaskRecord> {
        self.send_json(
            self.request(Method::POST, &format!("/v1/workflows/{id}/cancel")),
            StatusCode::OK,
            API_RESPONSE_LIMIT,
        )
        .await
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("http://{}{}", self.addr, path))
            .header(header::ACCEPT, "application/json")
            .bearer_auth(self.token.as_ref())
    }

    async fn send_json<T>(
        &self,
        request: reqwest::RequestBuilder,
        expected: StatusCode,
        limit: usize,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let response = request
            .send()
            .await
            .with_context(|| format!("connect to workflow daemon at {}", self.addr))?;
        let status = response.status();
        let bytes = read_response_limited(response, limit).await?;
        if status != expected {
            if let Ok(error) = serde_json::from_slice::<ErrorResponse>(&bytes) {
                let code = error.code.chars().take(64).collect::<String>();
                let message = error.message.chars().take(512).collect::<String>();
                bail!(
                    "workflow daemon rejected request with HTTP {status} ({}): {}",
                    code,
                    message
                );
            }
            bail!("workflow daemon returned unexpected HTTP {status}");
        }
        serde_json::from_slice(&bytes).context("parse bounded workflow daemon response")
    }

    #[cfg(test)]
    fn for_test(addr: SocketAddr, token: &str) -> Self {
        Self::for_test_with_timeout(addr, token, REQUEST_TIMEOUT)
    }

    #[cfg(test)]
    fn for_test_with_timeout(addr: SocketAddr, token: &str, timeout: Duration) -> Self {
        Self {
            http: build_http_client_with_timeout(timeout).expect("test HTTP client builds"),
            addr,
            instance_id: Arc::from("daemon:test"),
            token: Arc::from(token),
        }
    }
}

fn build_http_client() -> Result<reqwest::Client> {
    build_http_client_with_timeout(REQUEST_TIMEOUT)
}

fn build_http_client_with_timeout(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(timeout)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .gzip(false)
        .build()
        .context("build fail-closed workflow daemon HTTP client")
}

fn safe_rejection_code(bytes: &[u8]) -> &'static str {
    let Ok(error) = serde_json::from_slice::<ErrorResponse>(bytes) else {
        return "http_rejection";
    };
    match error.code.as_str() {
        "invalid_request" => "invalid_request",
        "conflict" => "conflict",
        "not_found" => "not_found",
        _ => "http_rejection",
    }
}

fn serialize_submission_request<T>(
    run_id: &WorkflowRunId,
    request: &T,
) -> std::result::Result<Vec<u8>, WorkflowSubmitError>
where
    T: serde::Serialize,
{
    serde_json::to_vec(request).map_err(|_| WorkflowSubmitError::NotSubmitted {
        run_id: run_id.clone(),
        reason: "request serialization failed before transmission",
    })
}

async fn read_response_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read workflow daemon response chunk")?;
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .context("workflow daemon response size overflow")?;
        if next_len > limit {
            bail!("workflow daemon response exceeds {limit} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const RUN_ID: &str = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e";
    const OTHER_RUN_ID: &str = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8f";
    const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const PRIVATE_PROMPT: &str = "private prompt must never appear in an error";
    const PRIVATE_VAR: &str = "private variable must never appear in an error";

    fn submission_bundle() -> WorkflowSourceBundle {
        WorkflowSourceBundle {
            workflow_toml: format!(
                r#"[workflow]
name = "client-test"

[[step]]
id = "only"
target = "test"
prompt = "{PRIVATE_PROMPT}"
"#
            ),
            prompt_files: Vec::new(),
        }
    }

    fn submission_vars() -> BTreeMap<String, String> {
        BTreeMap::from([("private".to_string(), PRIVATE_VAR.to_string())])
    }

    fn task_view_json(id: &str) -> serde_json::Value {
        serde_json::json!({
            "task": {
                "id": id,
                "owner": "local",
                "kind": "workflow",
                "origin": "daemon",
                "state": "queued",
                "task_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "target_key": "workflow",
                "created_at": "2026-07-10T00:00:00Z",
                "started_at": null,
                "updated_at": "2026-07-10T00:00:00Z",
                "finished_at": null,
                "revision": 0,
                "executor_epoch": 0,
                "controller": null,
                "lease": null,
                "ledger_run_id": null,
                "failure_code": null
            },
            "journal": null,
        })
    }

    struct SerializationFailure;

    impl serde::Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(<S::Error as serde::ser::Error>::custom(format!(
                "{PRIVATE_PROMPT}: {PRIVATE_VAR}: {TOKEN}"
            )))
        }
    }

    #[test]
    fn request_serialization_failure_is_bounded_not_submitted() {
        let id: WorkflowRunId = RUN_ID.parse().unwrap();
        let error = serialize_submission_request(&id, &SerializationFailure).unwrap_err();

        assert!(matches!(error, WorkflowSubmitError::NotSubmitted { .. }));
        let rendered = error.to_string();
        assert!(rendered.contains("definitely not submitted"));
        assert!(rendered.contains(RUN_ID));
        assert!(rendered.contains(&format!("--id {RUN_ID}")));
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn client_sends_bearer_and_never_follows_redirects() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/workflows/{RUN_ID}")))
            .and(header("authorization", format!("Bearer {TOKEN}")))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/must-not-follow"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/must-not-follow"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client.status(&id).await.unwrap_err().to_string();
        assert!(error.contains("HTTP 302"));
    }

    #[tokio::test]
    async fn client_rejects_oversized_response_before_json_parsing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/workflows/{RUN_ID}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(vec![b'x'; API_RESPONSE_LIMIT + 1]),
            )
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client.status(&id).await.unwrap_err().to_string();
        assert!(error.contains("response exceeds"));
    }

    #[tokio::test]
    async fn submission_sends_client_id_and_canonical_cwd_and_bounds_4xx_error() {
        let server = MockServer::start().await;
        let execution_cwd = PathBuf::from("/tmp/vyane-client-test");
        Mock::given(method("POST"))
            .and(path("/v1/workflows"))
            .and(header("authorization", format!("Bearer {TOKEN}")))
            .and(body_json(serde_json::json!({
                "run_id": RUN_ID,
                "execution_cwd": execution_cwd,
                "bundle": submission_bundle(),
                "vars": {"private": PRIVATE_VAR},
            })))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "code": "invalid_request",
                "message": format!("{PRIVATE_PROMPT}: {PRIVATE_VAR}: {TOKEN}"),
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client
            .submit(
                &id,
                PathBuf::from("/tmp/vyane-client-test"),
                submission_bundle(),
                submission_vars(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowSubmitError::Rejected {
                status: 400,
                code: "invalid_request",
                ..
            }
        ));
        let rendered = error.to_string();
        assert!(rendered.contains(RUN_ID));
        assert!(rendered.contains("HTTP 400"));
        assert!(rendered.contains("vyane workflow status"));
        assert!(rendered.contains(&format!("--id {RUN_ID}")));
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!rendered.contains(secret));
        }

        let json = error.json_value().to_string();
        assert!(json.contains("workflow_submission_rejected"));
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!json.contains(secret));
        }
    }

    #[tokio::test]
    async fn submission_5xx_is_bounded_outcome_unknown() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/workflows"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string(format!("{PRIVATE_PROMPT}: {PRIVATE_VAR}: {TOKEN}")),
            )
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client
            .submit(
                &id,
                PathBuf::from("/tmp"),
                submission_bundle(),
                submission_vars(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowSubmitError::OutcomeUnknown {
                status: Some(500),
                ..
            }
        ));
        let rendered = error.to_string();
        assert!(rendered.contains("outcome_unknown"));
        assert!(rendered.contains("HTTP status: 500"));
        assert!(rendered.contains(&format!("--id {RUN_ID}")));
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!rendered.contains(secret));
        }

        let json = error.json_value();
        assert_eq!(json["error"], "workflow_submission_outcome_unknown");
        assert_eq!(json["outcome"], "outcome_unknown");
        assert_eq!(json["workflow_run_id"], RUN_ID);
        assert_eq!(json["http_status"], 500);
    }

    #[tokio::test]
    async fn malformed_accepted_response_is_outcome_unknown_without_body_echo() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/workflows"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_string(format!("not-json {PRIVATE_PROMPT}: {PRIVATE_VAR}: {TOKEN}")),
            )
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client
            .submit(
                &id,
                PathBuf::from("/tmp"),
                submission_bundle(),
                submission_vars(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowSubmitError::OutcomeUnknown {
                status: Some(202),
                ..
            }
        ));
        let rendered = error.to_string();
        assert!(rendered.contains("HTTP status: 202"));
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn oversized_accepted_response_is_outcome_unknown_without_body_echo() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/workflows"))
            .respond_with(
                ResponseTemplate::new(202).set_body_bytes(vec![b'x'; API_RESPONSE_LIMIT + 1]),
            )
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client
            .submit(
                &id,
                PathBuf::from("/tmp"),
                submission_bundle(),
                submission_vars(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowSubmitError::OutcomeUnknown {
                status: Some(202),
                ..
            }
        ));
        let rendered = error.to_string();
        assert!(rendered.contains("HTTP status: 202"));
        assert!(rendered.contains("response bound"));
        assert!(rendered.len() < 1_024);
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn accepted_response_with_another_id_is_outcome_unknown() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/workflows"))
            .respond_with(ResponseTemplate::new(202).set_body_json(task_view_json(OTHER_RUN_ID)))
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test(*server.address(), TOKEN);
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client
            .submit(
                &id,
                PathBuf::from("/tmp"),
                submission_bundle(),
                submission_vars(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowSubmitError::OutcomeUnknown {
                status: Some(202),
                ..
            }
        ));
        let rendered = error.to_string();
        assert!(rendered.contains(RUN_ID));
        assert!(!rendered.contains(OTHER_RUN_ID));
        assert!(rendered.contains("did not confirm"));
        for secret in [PRIVATE_PROMPT, PRIVATE_VAR, TOKEN] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn submission_timeout_is_outcome_unknown_with_no_http_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/workflows"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_delay(Duration::from_millis(100))
                    .set_body_string("{}"),
            )
            .mount(&server)
            .await;
        let client = DaemonWorkflowClient::for_test_with_timeout(
            *server.address(),
            TOKEN,
            Duration::from_millis(20),
        );
        let id: WorkflowRunId = RUN_ID.parse().unwrap();

        let error = client
            .submit(
                &id,
                PathBuf::from("/tmp"),
                submission_bundle(),
                submission_vars(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowSubmitError::OutcomeUnknown { status: None, .. }
        ));
        let rendered = error.to_string();
        assert!(rendered.contains("HTTP status: unavailable"));
        assert!(rendered.contains("check status first"));
        assert!(rendered.len() < 1_024);
    }
}
