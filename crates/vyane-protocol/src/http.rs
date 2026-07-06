use std::future::Future;
use std::time::Duration;

use reqwest::{RequestBuilder, Response, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use vyane_core::{AuthStyle, Endpoint, ErrorKind, Result, VyaneError};

use crate::retry::{RetryConfig, RetryDecision, retry_after};

/// Reqwest's connect timeout. Request timeouts are caller supplied through
/// [`ClientOptions::request_timeout`] or disabled when unset.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

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
    response.json::<T>().await.map_err(|e| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed JSON response body", e)
    })
}

fn join_url(base: &str, path: &str) -> Result<Url> {
    let mut url = Url::parse(base)
        .map_err(|e| VyaneError::with_source(ErrorKind::Config, "invalid endpoint base URL", e))?;

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
    use super::*;

    #[test]
    fn join_url_does_not_duplicate_v1() {
        let url =
            join_url("https://api.example.com/v1", "/v1/chat/completions").expect("valid URL");
        assert_eq!(url.as_str(), "https://api.example.com/v1/chat/completions");
    }
}
