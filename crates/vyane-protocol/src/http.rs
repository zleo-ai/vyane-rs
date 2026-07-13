use std::future::Future;
use std::time::Duration;

use futures::StreamExt as _;
use reqwest::{RequestBuilder, Response, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use vyane_core::{
    AuthStyle, CancellationToken, Endpoint, ErrorKind, NativeExecutionAuthority, NativeSideEffect,
    Result, VyaneError,
};

use crate::retry::{RetryConfig, RetryDecision, retry_after};

/// Reqwest's connect timeout. Request timeouts are caller supplied through
/// [`ClientOptions::request_timeout`] or disabled when unset.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum accepted body size for a successful non-streaming JSON response.
/// Tool-call arguments are model-controlled, so relying on `Response::json`
/// would otherwise allow an endpoint to buffer an unbounded body.
pub(crate) const MAX_JSON_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    pub retry: RetryConfig,
    pub request_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    endpoint: Endpoint,
    client: reqwest::Client,
    options: ClientOptions,
}

impl HttpClient {
    pub(crate) fn new(endpoint: Endpoint, options: ClientOptions) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            // One explicit loop below owns every retry and, for guarded
            // requests, every authority check. Reqwest defaults to following
            // redirects and retrying protocol NACKs internally; either would
            // turn one authorized `send` into multiple physical requests.
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|e| {
                VyaneError::with_source(ErrorKind::Config, "failed to build HTTP client", e)
            })?;

        Ok(Self {
            endpoint,
            client,
            options,
        })
    }

    pub(crate) fn url(&self, path: &str) -> Result<Url> {
        join_url(&self.endpoint.base_url, path)
    }

    pub(crate) async fn post_json<T, U, F>(
        &self,
        path: &'static str,
        body: T,
        decorate: F,
    ) -> Result<U>
    where
        T: Serialize + Clone,
        U: DeserializeOwned,
        F: Fn(RequestBuilder) -> RequestBuilder + Copy,
    {
        let mut last_error = None;
        for attempt in 1..=self.options.retry.max_attempts() {
            let request = self.request(path, &body, decorate)?;
            match request.send().await {
                Ok(response) => match classify_status(response.status()) {
                    StatusClass::Success => return parse_json(response).await,
                    StatusClass::Retryable(kind) => {
                        let status = response.status();
                        let delay = retry_after(response.headers())
                            .unwrap_or_else(|| self.options.retry.delay_for(attempt));
                        last_error = Some(status_error(kind, status));
                        if self.options.retry.should_retry(attempt) {
                            self.options.retry.sleep(delay).await;
                            continue;
                        }
                        return Err(status_error(kind, status));
                    }
                    StatusClass::Terminal(kind) => {
                        return Err(status_error(kind, response.status()));
                    }
                },
                Err(error) => {
                    let kind = reqwest_error_kind(&error);
                    if kind == ErrorKind::Transport && self.options.retry.should_retry(attempt) {
                        let delay = self.options.retry.delay_for(attempt);
                        last_error = Some(VyaneError::with_source(
                            kind,
                            "transport error while sending request",
                            error,
                        ));
                        self.options.retry.sleep(delay).await;
                        continue;
                    }
                    return Err(VyaneError::with_source(
                        kind,
                        "failed to send request",
                        error,
                    ));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            VyaneError::new(ErrorKind::Other, "retry loop exited without a result")
        }))
    }

    /// Send one native-loop model turn through an execution authority.
    ///
    /// Every physical wire attempt is authorized independently immediately
    /// before `send`. An authority failure is terminal for this operation and
    /// is returned unchanged; only wire/status failures enter the HTTP retry
    /// policy.
    pub(crate) async fn post_json_authorized<T, U, F>(
        &self,
        path: &'static str,
        body: T,
        decorate: F,
        turn: u32,
        authority: &dyn NativeExecutionAuthority,
        cancel: &CancellationToken,
    ) -> Result<U>
    where
        T: Serialize + Clone,
        U: DeserializeOwned,
        F: Fn(RequestBuilder) -> RequestBuilder + Copy,
    {
        let mut last_error = None;
        for wire_attempt in 1..=self.options.retry.max_attempts() {
            check_cancelled(cancel)?;
            let request = self.request(path, &body, decorate)?;
            let revalidation =
                authority.revalidate(NativeSideEffect::ModelSend { turn, wire_attempt });
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(VyaneError::cancelled()),
                result = revalidation => result?,
            }
            check_cancelled(cancel)?;

            let send_result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(VyaneError::cancelled()),
                result = request.send() => result,
            };
            match send_result {
                Ok(response) => match classify_status(response.status()) {
                    StatusClass::Success => {
                        return parse_json_or_cancel(response, cancel).await;
                    }
                    StatusClass::Retryable(kind) => {
                        let status = response.status();
                        let delay = retry_after(response.headers())
                            .unwrap_or_else(|| self.options.retry.delay_for(wire_attempt));
                        last_error = Some(status_error(kind, status));
                        if self.options.retry.should_retry(wire_attempt) {
                            sleep_or_cancel(&self.options.retry, delay, cancel).await?;
                            continue;
                        }
                        return Err(status_error(kind, status));
                    }
                    StatusClass::Terminal(kind) => {
                        return Err(status_error(kind, response.status()));
                    }
                },
                Err(error) => {
                    let kind = reqwest_error_kind(&error);
                    if kind == ErrorKind::Transport && self.options.retry.should_retry(wire_attempt)
                    {
                        let delay = self.options.retry.delay_for(wire_attempt);
                        last_error = Some(VyaneError::with_source(
                            kind,
                            "transport error while sending request",
                            error,
                        ));
                        sleep_or_cancel(&self.options.retry, delay, cancel).await?;
                        continue;
                    }
                    return Err(VyaneError::with_source(
                        kind,
                        "failed to send request",
                        error,
                    ));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            VyaneError::new(ErrorKind::Other, "retry loop exited without a result")
        }))
    }

    pub(crate) async fn post_stream<T, F>(
        &self,
        path: &'static str,
        body: T,
        decorate: F,
    ) -> Result<Response>
    where
        T: Serialize + Clone,
        F: Fn(RequestBuilder) -> RequestBuilder + Copy,
    {
        let mut last_error = None;
        for attempt in 1..=self.options.retry.max_attempts() {
            let request = self.request(path, &body, decorate)?;
            match request.send().await {
                Ok(response) => match classify_status(response.status()) {
                    StatusClass::Success => return Ok(response),
                    StatusClass::Retryable(kind) => {
                        let status = response.status();
                        let delay = retry_after(response.headers())
                            .unwrap_or_else(|| self.options.retry.delay_for(attempt));
                        last_error = Some(status_error(kind, status));
                        if self.options.retry.should_retry(attempt) {
                            self.options.retry.sleep(delay).await;
                            continue;
                        }
                        return Err(status_error(kind, status));
                    }
                    StatusClass::Terminal(kind) => {
                        return Err(status_error(kind, response.status()));
                    }
                },
                Err(error) => {
                    let kind = reqwest_error_kind(&error);
                    if kind == ErrorKind::Transport && self.options.retry.should_retry(attempt) {
                        let delay = self.options.retry.delay_for(attempt);
                        last_error = Some(VyaneError::with_source(
                            kind,
                            "transport error while opening stream",
                            error,
                        ));
                        self.options.retry.sleep(delay).await;
                        continue;
                    }
                    return Err(VyaneError::with_source(
                        kind,
                        "failed to open stream",
                        error,
                    ));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            VyaneError::new(ErrorKind::Other, "retry loop exited without a result")
        }))
    }

    fn request<T, F>(&self, path: &'static str, body: &T, decorate: F) -> Result<RequestBuilder>
    where
        T: Serialize,
        F: Fn(RequestBuilder) -> RequestBuilder,
    {
        let url = self.url(path)?;
        let mut request = self.client.post(url).json(body);
        if let Some(timeout) = self.options.request_timeout {
            request = request.timeout(timeout);
        }
        request = self.apply_auth(request);
        Ok(decorate(request))
    }

    fn apply_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.endpoint.auth {
            Some(auth) => match auth.style {
                AuthStyle::Bearer => request.bearer_auth(auth.secret.expose()),
                AuthStyle::XApiKey => request.header("x-api-key", auth.secret.expose()),
            },
            None => request,
        }
    }
}

fn check_cancelled(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        Err(VyaneError::cancelled())
    } else {
        Ok(())
    }
}

async fn sleep_or_cancel(
    retry: &RetryConfig,
    delay: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(VyaneError::cancelled()),
        _ = retry.sleep(delay) => Ok(()),
    }
}

async fn parse_json_or_cancel<T: DeserializeOwned>(
    response: Response,
    cancel: &CancellationToken,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(VyaneError::cancelled()),
        result = parse_json(response) => result,
    }
}

pub(crate) fn status_error(kind: ErrorKind, status: StatusCode) -> VyaneError {
    VyaneError::new(kind, format!("HTTP status {}", status.as_u16()))
}

pub(crate) fn reqwest_error_kind(error: &reqwest::Error) -> ErrorKind {
    if error.is_timeout() {
        ErrorKind::Timeout
    } else if error.is_connect() || error.is_request() || error.is_body() {
        ErrorKind::Transport
    } else if error.is_decode() {
        ErrorKind::Protocol
    } else {
        ErrorKind::Transport
    }
}

enum StatusClass {
    Success,
    Retryable(ErrorKind),
    Terminal(ErrorKind),
}

fn classify_status(status: StatusCode) -> StatusClass {
    if status.is_success() {
        StatusClass::Success
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        StatusClass::Terminal(ErrorKind::Auth)
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        StatusClass::Retryable(ErrorKind::RateLimited)
    } else if status.is_server_error() {
        StatusClass::Retryable(ErrorKind::Protocol)
    } else {
        StatusClass::Terminal(ErrorKind::Protocol)
    }
}

async fn parse_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            let kind = reqwest_error_kind(&error);
            VyaneError::with_source(kind, "failed while reading JSON response body", error)
        })?;
        if chunk.len() > MAX_JSON_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(VyaneError::new(
                ErrorKind::Protocol,
                format!(
                    "JSON response body exceeds {} bytes",
                    MAX_JSON_RESPONSE_BYTES
                ),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice::<T>(&body).map_err(|error| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed JSON response body", error)
    })
}

/// Validate the exact endpoint-base contract used by every HTTP protocol
/// client without making a request or retaining authentication material.
///
/// Keeping this validator beside `join_url` lets static front-ends reject a
/// malformed or non-HTTP endpoint without duplicating the wire client's URL
/// semantics.
pub fn validate_http_base_url(base: &str) -> Result<()> {
    let mut url = parse_http_base_url(base)?;
    url.path_segments_mut()
        .map_err(|_| VyaneError::config("endpoint base URL cannot be used as an HTTP base URL"))?;
    Ok(())
}

/// Produce the secret-free routing identity stored in a native session domain.
///
/// The URL parser supplies normalized scheme/IDNA host/default-port semantics.
/// Empty and duplicate path segments are normalized exactly like `join_url`.
/// Only the explicitly non-secret `api-version`/`api_version` routing query is
/// accepted; unknown, duplicate, or malformed query fields fail closed. No
/// plaintext URL component is returned. Embedded URL credentials and
/// fragments are rejected by the same parser used for live requests.
pub fn endpoint_routing_digest(base: &str) -> Result<String> {
    let url = parse_http_base_url(base)?;
    let effective_port = url.port_or_known_default().ok_or_else(|| {
        VyaneError::config("endpoint base URL does not have a known effective port")
    })?;
    let effective_port = effective_port.to_string();
    let canonical_path = canonical_base_path(&url)?;
    let canonical_query = canonical_routing_query(&url)?;
    let mut digest = Sha256::new();
    digest.update(b"vyane.endpoint-routing.v1\0");
    for field in [
        url.scheme().as_bytes(),
        url.host_str().unwrap_or_default().as_bytes(),
        effective_port.as_bytes(),
        canonical_path.as_bytes(),
        canonical_query.as_bytes(),
    ] {
        digest.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(field);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn canonical_base_path(url: &Url) -> Result<String> {
    let segments = url.path_segments().ok_or_else(|| {
        VyaneError::config("endpoint base URL cannot be used as an HTTP base URL")
    })?;
    let segments = segments
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

fn canonical_routing_query(url: &Url) -> Result<String> {
    let mut routing_query = None;
    for (key, value) in url.query_pairs() {
        if !matches!(key.as_ref(), "api-version" | "api_version")
            || value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(VyaneError::config(
                "endpoint routing query is not an allowed non-secret api-version field",
            ));
        }
        if routing_query.is_some() {
            return Err(VyaneError::config(
                "endpoint routing query must contain at most one api-version field",
            ));
        }
        routing_query = Some(format!("{}={value}", key.as_ref()));
    }
    Ok(routing_query.unwrap_or_default())
}

fn parse_http_base_url(base: &str) -> Result<Url> {
    let url = Url::parse(base)
        .map_err(|e| VyaneError::with_source(ErrorKind::Config, "invalid endpoint base URL", e))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(VyaneError::config(
            "endpoint base URL must use HTTP or HTTPS and include a host",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(VyaneError::config(
            "endpoint base URL must not embed user credentials",
        ));
    }
    let authority = base
        .split_once("://")
        .map(|(_, remainder)| remainder.split(['/', '?', '#']).next().unwrap_or_default())
        .unwrap_or_default();
    if authority.contains('@') {
        return Err(VyaneError::config(
            "endpoint base URL must not contain userinfo",
        ));
    }
    if url.fragment().is_some() {
        return Err(VyaneError::config(
            "endpoint base URL must not contain a fragment",
        ));
    }
    Ok(url)
}

fn join_url(base: &str, path: &str) -> Result<Url> {
    let mut url = parse_http_base_url(base)?;

    let trimmed_path = path.trim_start_matches('/');
    let existing: Vec<String> = url
        .path_segments()
        .map(|segments| {
            segments
                .filter(|segment| !segment.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default();
    {
        let mut segments = url.path_segments_mut().map_err(|_| {
            VyaneError::config("endpoint base URL cannot be used as an HTTP base URL")
        })?;
        segments.clear();
        if existing.last().is_some_and(|segment| segment == "v1") && trimmed_path.starts_with("v1/")
        {
            for segment in existing {
                segments.push(&segment);
            }
            for segment in trimmed_path.trim_start_matches("v1/").split('/') {
                segments.push(segment);
            }
        } else {
            for segment in existing {
                segments.push(&segment);
            }
            for segment in trimmed_path.split('/') {
                segments.push(segment);
            }
        }
    }

    Ok(url)
}

#[allow(dead_code)]
pub(crate) async fn retry_with_policy<T, Fut, Op>(
    retry: &RetryConfig,
    mut operation: Op,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
    Op: FnMut(u32) -> Fut,
{
    let mut last = None;
    for attempt in 1..=retry.max_attempts() {
        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let decision = retry.decision_for(attempt, error.kind);
                if let RetryDecision::Retry(delay) = decision {
                    last = Some(error);
                    retry.sleep(delay).await;
                    continue;
                }
                return Err(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| VyaneError::new(ErrorKind::Other, "retry loop exited empty")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[test]
    fn join_url_does_not_duplicate_v1() {
        let url =
            join_url("https://api.example.com/v1", "/v1/chat/completions").expect("valid URL");
        assert_eq!(url.as_str(), "https://api.example.com/v1/chat/completions");
    }

    #[test]
    fn base_url_validation_matches_http_client_scheme_and_host_contract() {
        for valid in ["http://localhost:8080/v1", "https://api.example.com"] {
            validate_http_base_url(valid).expect("HTTP base URL should be accepted");
        }
        for invalid in [
            "not a URL",
            "file:///tmp/socket",
            "mailto:model@example.com",
            "https://",
            "https://user:password@example.com/v1",
            "https://@example.com/v1",
            "https://example.com/v1#fragment",
        ] {
            let error = validate_http_base_url(invalid).expect_err("base URL must be rejected");
            assert_eq!(error.kind, ErrorKind::Config);
        }
    }

    #[test]
    fn endpoint_routing_digest_is_canonical_and_never_returns_url_text() {
        let left = endpoint_routing_digest("HTTPS://EXAMPLE.COM:443//v1/?api-version=2025-01-01")
            .expect("canonical HTTPS URL");
        let equivalent = endpoint_routing_digest("https://example.com/v1?api-version=2025-01-01")
            .expect("equivalent HTTPS URL");
        let different_query =
            endpoint_routing_digest("https://example.com/v1?api-version=2025-02-02")
                .expect("different query URL");
        let different_path =
            endpoint_routing_digest("https://example.com/v2?api-version=2025-01-01")
                .expect("different path URL");

        assert_eq!(left, equivalent);
        assert_ne!(left, different_query);
        assert_ne!(left, different_path);
        assert_eq!(left.len(), 64);
        assert!(left.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!left.contains("example.com"));
        assert!(endpoint_routing_digest("https://example.com/v1?token=CANARY_SECRET").is_err());
        assert!(
            endpoint_routing_digest("https://example.com/v1?api-version=one&api-version=two")
                .is_err()
        );
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn non_streaming_json_response_has_a_hard_total_byte_limit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/oversized"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![
                b' ';
                MAX_JSON_RESPONSE_BYTES
                    + 1
            ]))
            .mount(&server)
            .await;
        let client = HttpClient::new(
            Endpoint {
                base_url: server.uri(),
                auth: None,
            },
            ClientOptions::default(),
        )
        .unwrap();

        let result: Result<Value> = client
            .post_json("/v1/oversized", json!({}), |request| request)
            .await;
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(error.message.contains("exceeds"));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn authorized_json_body_read_is_cancellation_aware_after_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_for_server = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let bytes_read = socket.read(&mut request).await.unwrap();
            assert!(bytes_read > 0);
            requests_for_server.fetch_add(1, Ordering::SeqCst);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 32\r\nconnection: close\r\n\r\n{",
                )
                .await
                .unwrap();
            socket.flush().await.unwrap();
            std::future::pending::<()>().await;
        });

        let response = reqwest::Client::new()
            .post(format!("http://{address}/slow-json"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(requests.load(Ordering::SeqCst), 1);

        let cancel = CancellationToken::new();
        let cancel_after_parse_starts = cancel.clone();
        let canceller = tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel_after_parse_starts.cancel();
        });
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            parse_json_or_cancel::<Value>(response, &cancel),
        )
        .await
        .expect("slow authorized response body ignored cancellation")
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        canceller.await.unwrap();
        server.abort();
        let _ = server.await;
    }
}
