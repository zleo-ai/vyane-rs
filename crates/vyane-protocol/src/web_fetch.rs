//! Bounded HTTPS retrieval for the native `web_fetch` tool.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use reqwest::{StatusCode, Url};
use vyane_core::{
    AuthorizedWebFetchClient, CancellationToken, ErrorKind, NativeExecutionAuthority,
    NativeSideEffect, Result, VyaneError, WebFetchOutcome, WebFetchRequest, WebFetchRoute,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DOMAINS: usize = 128;
const MIN_RESPONSE_BYTES: usize = 1024;
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_REDIRECTS: u32 = 8;

#[derive(Debug, Clone, Default)]
pub struct WebFetchClient;

impl WebFetchClient {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuthorizedWebFetchClient for WebFetchClient {
    async fn fetch_authorized(
        &self,
        req: WebFetchRequest,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
        cancel: &CancellationToken,
    ) -> Result<WebFetchOutcome> {
        validate_request_bounds(&req)?;
        let mut url = validate_fetch_url(&req.url)?;
        let mut redirects = 0;
        loop {
            let admitted_host = url
                .domain()
                .is_some_and(|host| domain_is_allowed(host, &req.allowed_domains));
            if !admitted_host {
                return Err(VyaneError::new(
                    ErrorKind::Auth,
                    "web fetch URL exceeds the admitted domain policy",
                ));
            }
            let host = url
                .host_str()
                .ok_or_else(|| invalid_url("web fetch URL has no host"))?
                .to_string();
            let port = url
                .port_or_known_default()
                .ok_or_else(|| invalid_url("web fetch URL has no port"))?;
            let addrs = tokio::select! {
                () = cancel.cancelled() => return Err(cancelled()),
                result = tokio::net::lookup_host((host.as_str(), port)) => result
                    .map_err(|error| VyaneError::with_source(
                        ErrorKind::Transport,
                        "web fetch DNS lookup failed",
                        error,
                    ))?
                    .collect::<Vec<_>>(),
            };
            if addrs.is_empty()
                || addrs
                    .iter()
                    .any(|addr| !route_accepts_ip(req.route, addr.ip()))
            {
                return Err(VyaneError::new(
                    ErrorKind::Auth,
                    "web fetch resolved outside the public Internet",
                ));
            }
            let client = build_client(&host, &addrs, req.route)?;
            authority.revalidate(effect).await?;
            let response = tokio::select! {
                () = cancel.cancelled() => return Err(cancelled()),
                response = client
                    .get(url.clone())
                    .header(reqwest::header::ACCEPT, "text/*, application/json, application/xml;q=0.9")
                    .header(reqwest::header::USER_AGENT, "vyane-rs-web-fetch/0.1")
                    .send() => response.map_err(|error| {
                    VyaneError::with_source(ErrorKind::Transport, "web fetch request failed", error)
                })?,
            };

            if is_redirect(response.status()) {
                if redirects >= req.max_redirects {
                    return Err(VyaneError::new(
                        ErrorKind::Protocol,
                        "web fetch exceeded the redirect limit",
                    ));
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| {
                        VyaneError::new(
                            ErrorKind::Protocol,
                            "web fetch redirect has no valid location",
                        )
                    })?;
                url = validate_fetch_url(
                    url.join(location)
                        .map_err(|_| invalid_url("web fetch redirect URL is invalid"))?
                        .as_str(),
                )?;
                redirects += 1;
                continue;
            }
            if !response.status().is_success() {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    format!("web fetch returned HTTP {}", response.status().as_u16()),
                ));
            }

            let content_type = accepted_content_type(
                response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
            )?;
            if response
                .content_length()
                .is_some_and(|length| length > req.max_response_bytes as u64)
            {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    "web fetch response exceeds the configured byte limit",
                ));
            }
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            loop {
                let chunk = tokio::select! {
                    () = cancel.cancelled() => return Err(cancelled()),
                    chunk = stream.next() => chunk,
                };
                let Some(chunk) = chunk else {
                    break;
                };
                let chunk = chunk.map_err(|error| {
                    VyaneError::with_source(
                        ErrorKind::Transport,
                        "web fetch response body failed",
                        error,
                    )
                })?;
                if bytes.len().saturating_add(chunk.len()) > req.max_response_bytes {
                    return Err(VyaneError::new(
                        ErrorKind::Protocol,
                        "web fetch response exceeds the configured byte limit",
                    ));
                }
                bytes.extend_from_slice(&chunk);
            }
            let text = String::from_utf8(bytes).map_err(|_| {
                VyaneError::new(
                    ErrorKind::Protocol,
                    "web fetch response is not valid UTF-8 text",
                )
            })?;
            return Ok(WebFetchOutcome {
                final_url: url.to_string(),
                content_type,
                text,
            });
        }
    }
}

fn validate_request_bounds(req: &WebFetchRequest) -> Result<()> {
    if req.allowed_domains.is_empty()
        || req.allowed_domains.len() > MAX_DOMAINS
        || !(MIN_RESPONSE_BYTES..=MAX_RESPONSE_BYTES).contains(&req.max_response_bytes)
        || req.max_redirects > MAX_REDIRECTS
        || req
            .allowed_domains
            .iter()
            .any(|domain| !valid_policy_domain(domain))
    {
        return Err(VyaneError::new(
            ErrorKind::Config,
            "web fetch request policy is invalid",
        ));
    }
    let mut sorted = req.allowed_domains.clone();
    sorted.sort();
    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(VyaneError::new(
            ErrorKind::Config,
            "web fetch request policy is invalid",
        ));
    }
    Ok(())
}

fn valid_policy_domain(domain: &str) -> bool {
    if domain.is_empty()
        || domain.len() > 253
        || domain != domain.to_ascii_lowercase()
        || domain.starts_with('.')
        || domain.ends_with('.')
        || domain.parse::<IpAddr>().is_ok()
        || !domain.contains('.')
    {
        return false;
    }
    domain.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

fn build_client(host: &str, addrs: &[SocketAddr], route: WebFetchRoute) -> Result<reqwest::Client> {
    let builder = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never());
    let builder = match route {
        WebFetchRoute::Direct => builder.no_proxy().resolve_to_addrs(host, addrs),
        WebFetchRoute::EnvironmentProxy => {
            let proxy = environment_https_proxy()?;
            builder.proxy(proxy)
        }
    };
    builder.build().map_err(|error| {
        VyaneError::with_source(ErrorKind::Config, "failed to build web fetch client", error)
    })
}

fn environment_https_proxy() -> Result<reqwest::Proxy> {
    let raw = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("https_proxy"))
        .map_err(|_| {
            VyaneError::new(
                ErrorKind::Config,
                "web fetch environment-proxy route requires HTTPS_PROXY",
            )
        })?;
    let url = Url::parse(&raw).map_err(|_| {
        VyaneError::new(
            ErrorKind::Config,
            "web fetch HTTPS_PROXY configuration is invalid",
        )
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(VyaneError::new(
            ErrorKind::Config,
            "web fetch HTTPS_PROXY configuration is invalid",
        ));
    }
    reqwest::Proxy::all(url.as_str()).map_err(|_| {
        VyaneError::new(
            ErrorKind::Config,
            "web fetch HTTPS_PROXY configuration is invalid",
        )
    })
}

fn validate_fetch_url(input: &str) -> Result<Url> {
    if input.is_empty() || input.len() > 8 * 1024 {
        return Err(invalid_url("web fetch URL is invalid"));
    }
    let mut url = Url::parse(input).map_err(|_| invalid_url("web fetch URL is invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.port().is_some_and(|port| port != 443)
    {
        return Err(invalid_url("web fetch requires a public HTTPS URL"));
    }
    if matches!(url.host(), Some(url::Host::Ipv4(_) | url::Host::Ipv6(_))) {
        return Err(invalid_url("web fetch does not accept IP-literal hosts"));
    }
    url.set_fragment(None);
    Ok(url)
}

fn domain_is_allowed(host: &str, allowed: &[String]) -> bool {
    allowed
        .iter()
        .any(|base| host == base || host.ends_with(&format!(".{base}")))
}

fn accepted_content_type(value: Option<&str>) -> Result<String> {
    let content_type = value
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .filter(|value| {
            value.starts_with("text/")
                || matches!(
                    value.as_str(),
                    "application/json"
                        | "application/ld+json"
                        | "application/xml"
                        | "application/xhtml+xml"
                        | "application/rss+xml"
                        | "application/atom+xml"
                )
        })
        .ok_or_else(|| {
            VyaneError::new(
                ErrorKind::Protocol,
                "web fetch accepts only declared text content",
            )
        })?;
    Ok(content_type)
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn route_accepts_ip(route: WebFetchRoute, ip: IpAddr) -> bool {
    is_public_ip(ip)
        || (route == WebFetchRoute::EnvironmentProxy
            && matches!(ip, IpAddr::V4(ip) if is_fake_ipv4(ip)))
}

fn is_fake_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 198 && (octets[1] == 18 || octets[1] == 19)
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        || octets[0] == 0
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        || octets[0] >= 240)
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(ipv4) = ip.to_ipv4() {
        return is_public_ipv4(ipv4);
    }
    let segments = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x0064 && segments[1] == 0xff9b)
        || (segments[0] == 0x2001 && segments[1] == 0x0000)
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x2001 && segments[1] == 0x0002)
        || (segments[0] == 0x2001 && (segments[1] & 0xfff0) == 0x0010)
        || (segments[0] == 0x2001 && (segments[1] & 0xfff0) == 0x0020)
        || segments[0] == 0x2002
        || (segments[0] & 0xfff0) == 0x3ff0
        || (segments[0] == 0x0100 && segments[1..].iter().all(|part| *part == 0)))
}

fn invalid_url(message: &'static str) -> VyaneError {
    VyaneError::new(ErrorKind::Config, message)
}

fn cancelled() -> VyaneError {
    VyaneError::new(ErrorKind::Cancelled, "web fetch cancelled")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn url_policy_is_https_dns_only_and_strips_fragments() {
        assert_eq!(
            validate_fetch_url("https://docs.rs/a?q=1#section")
                .unwrap()
                .as_str(),
            "https://docs.rs/a?q=1"
        );
        for invalid in [
            "http://docs.rs",
            "https://user@docs.rs",
            "https://docs.rs:444",
            "https://127.0.0.1",
            "https://[::1]",
        ] {
            assert!(validate_fetch_url(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn address_policy_rejects_local_reserved_and_documentation_ranges() {
        for invalid in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "192.0.2.1",
            "198.18.0.1",
            "224.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
            "64:ff9b::127.0.0.1",
            "2001::1",
            "2002:7f00:1::",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public_ip(invalid.parse().unwrap()), "{invalid}");
        }
        for valid in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(is_public_ip(valid.parse().unwrap()), "{valid}");
        }
        let fake_ip = "198.18.1.10".parse().unwrap();
        assert!(!route_accepts_ip(WebFetchRoute::Direct, fake_ip));
        assert!(route_accepts_ip(WebFetchRoute::EnvironmentProxy, fake_ip));
        assert!(!route_accepts_ip(
            WebFetchRoute::EnvironmentProxy,
            "10.0.0.1".parse().unwrap()
        ));
    }

    #[test]
    fn content_policy_accepts_text_and_rejects_binary_or_missing_types() {
        assert_eq!(
            accepted_content_type(Some("text/html; charset=utf-8")).unwrap(),
            "text/html"
        );
        assert_eq!(
            accepted_content_type(Some("application/json")).unwrap(),
            "application/json"
        );
        assert!(accepted_content_type(Some("application/octet-stream")).is_err());
        assert!(accepted_content_type(None).is_err());
    }

    #[test]
    fn request_policy_is_closed_and_bounded_even_for_direct_client_callers() {
        let base = WebFetchRequest {
            url: "https://docs.rs".into(),
            allowed_domains: vec!["docs.rs".into()],
            route: WebFetchRoute::Direct,
            max_response_bytes: 4096,
            max_redirects: 2,
        };
        assert!(validate_request_bounds(&base).is_ok());
        for invalid in [
            vec![],
            vec!["com".into()],
            vec!["*.docs.rs".into()],
            vec!["Docs.rs".into()],
            vec!["docs.rs".into(), "docs.rs".into()],
        ] {
            let mut request = base.clone();
            request.allowed_domains = invalid;
            assert!(validate_request_bounds(&request).is_err());
        }
    }
}
