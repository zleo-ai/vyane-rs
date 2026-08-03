use futures::StreamExt as _;
use reqwest::header::HeaderMap;
use thiserror::Error;

use crate::QuotaReadPolicy;

#[derive(Clone)]
pub struct QuotaHttpReader {
    client: reqwest::Client,
}

impl QuotaHttpReader {
    pub fn new() -> Result<Self, QuotaTransportError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| QuotaTransportError::ClientUnavailable)?;
        Ok(Self { client })
    }

    pub async fn get(
        &self,
        url: &str,
        headers: HeaderMap,
        policy: QuotaReadPolicy,
    ) -> Result<QuotaHttpResponse, QuotaTransportError> {
        let policy = policy
            .validate()
            .map_err(|_| QuotaTransportError::InvalidPolicy)?;
        let url = reqwest::Url::parse(url).map_err(|_| QuotaTransportError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(QuotaTransportError::InvalidUrl);
        }
        tokio::time::timeout(policy.timeout, self.get_bounded(url, headers, policy))
            .await
            .map_err(|_| QuotaTransportError::Timeout)?
    }

    async fn get_bounded(
        &self,
        url: reqwest::Url,
        headers: HeaderMap,
        policy: QuotaReadPolicy,
    ) -> Result<QuotaHttpResponse, QuotaTransportError> {
        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(|_| QuotaTransportError::RequestFailed)?;
        if response.status().is_redirection() {
            return Err(QuotaTransportError::RedirectRejected);
        }
        let status = response.status().as_u16();
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            let next = stream.next().await;
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(|_| QuotaTransportError::RequestFailed)?;
            if body.len().saturating_add(chunk.len()) > policy.max_body_bytes {
                return Err(QuotaTransportError::BodyTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(QuotaHttpResponse { status, body })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QuotaTransportError {
    #[error("quota HTTP client is unavailable")]
    ClientUnavailable,
    #[error("quota HTTP URL is invalid")]
    InvalidUrl,
    #[error("quota HTTP policy is invalid")]
    InvalidPolicy,
    #[error("quota HTTP request timed out")]
    Timeout,
    #[error("quota HTTP redirect was rejected")]
    RedirectRejected,
    #[error("quota HTTP response exceeded the body limit")]
    BodyTooLarge,
    #[error("quota HTTP request failed")]
    RequestFailed,
}

impl QuotaTransportError {
    /// Stable snake_case *kind* token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientUnavailable => "client_unavailable",
            Self::InvalidUrl => "invalid_url",
            Self::InvalidPolicy => "invalid_policy",
            Self::Timeout => "timeout",
            Self::RedirectRejected => "redirect_rejected",
            Self::BodyTooLarge => "body_too_large",
            Self::RequestFailed => "request_failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QuotaTransportError;

    #[test]
    fn quota_transport_error_kind_tokens_are_snake_case() {
        assert_eq!(
            QuotaTransportError::ClientUnavailable.as_str(),
            "client_unavailable"
        );
        assert_eq!(QuotaTransportError::InvalidUrl.as_str(), "invalid_url");
        assert_eq!(
            QuotaTransportError::InvalidPolicy.as_str(),
            "invalid_policy"
        );
        assert_eq!(QuotaTransportError::Timeout.as_str(), "timeout");
        assert_eq!(
            QuotaTransportError::RedirectRejected.as_str(),
            "redirect_rejected"
        );
        assert_eq!(QuotaTransportError::BodyTooLarge.as_str(), "body_too_large");
        assert_eq!(
            QuotaTransportError::RequestFailed.as_str(),
            "request_failed"
        );
    }
}
