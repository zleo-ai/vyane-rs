use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;
use vyane_core::{
    AuthorizedWebFetchClient, NativeExecutionAuthority, NativeSideEffect, Result as VyaneResult,
    ToolDefinition, WebFetchRequest, WebFetchRoute,
};

use super::{
    MAX_TOOL_OUTPUT_CHARS, NativeTool, PermissionEffect, PermissionPolicy, PermissionRule,
    PermissionRuleError, ToolContext, ToolError, ToolRegistry, ToolRegistryError,
    validate_search_domain,
};

const MAX_DOMAINS: usize = 128;
const MAX_FETCHES: u32 = 16;
const MIN_RESPONSE_BYTES: usize = 1024;
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_REDIRECTS: u32 = 8;
const MAX_URL_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWebFetchPolicy {
    pub allow_domains: Vec<String>,
    #[serde(default)]
    pub route: WebFetchRoute,
    #[serde(default = "default_max_fetches")]
    pub max_fetches: u32,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
    #[serde(default = "default_max_redirects")]
    pub max_redirects: u32,
}

impl NativeWebFetchPolicy {
    pub fn validate(&self) -> Result<(), NativeWebFetchPolicyError> {
        if self.allow_domains.is_empty() {
            return Err(NativeWebFetchPolicyError::EmptyAllowlist);
        }
        if self.allow_domains.len() > MAX_DOMAINS {
            return Err(NativeWebFetchPolicyError::TooManyDomains);
        }
        let mut unique = BTreeSet::new();
        for domain in &self.allow_domains {
            validate_search_domain(domain).map_err(|_| NativeWebFetchPolicyError::InvalidDomain)?;
            if !unique.insert(domain) {
                return Err(NativeWebFetchPolicyError::DuplicateDomain);
            }
        }
        if self.max_fetches == 0 || self.max_fetches > MAX_FETCHES {
            return Err(NativeWebFetchPolicyError::InvalidFetchLimit);
        }
        if !(MIN_RESPONSE_BYTES..=MAX_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(NativeWebFetchPolicyError::InvalidResponseLimit);
        }
        if self.max_redirects > MAX_REDIRECTS {
            return Err(NativeWebFetchPolicyError::InvalidRedirectLimit);
        }
        Ok(())
    }

    fn permits(&self, host: &str) -> bool {
        self.allow_domains
            .iter()
            .any(|base| host == base || host.ends_with(&format!(".{base}")))
    }
}

const fn default_max_fetches() -> u32 {
    4
}

const fn default_max_response_bytes() -> usize {
    512 * 1024
}

const fn default_max_redirects() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeWebFetchPolicyError {
    #[error("native web-fetch domain allowlist must not be empty")]
    EmptyAllowlist,
    #[error("native web-fetch policy contains too many domains")]
    TooManyDomains,
    #[error("native web-fetch policy contains an invalid domain")]
    InvalidDomain,
    #[error("native web-fetch policy contains a duplicate domain")]
    DuplicateDomain,
    #[error("native web-fetch count limit is invalid")]
    InvalidFetchLimit,
    #[error("native web-fetch response limit is invalid")]
    InvalidResponseLimit,
    #[error("native web-fetch redirect limit is invalid")]
    InvalidRedirectLimit,
}

pub fn web_fetch_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: "web_fetch".into(),
        description: "Fetch one public HTTPS text resource through a separate domain-restricted \
            permission. Redirects remain inside the same admitted domain policy."
            .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_URL_BYTES
                }
            },
            "required": ["url"],
            "additionalProperties": false
        }),
    }
}

pub fn register_web_fetch_tool(
    registry: &mut ToolRegistry,
    policy: NativeWebFetchPolicy,
    client: Arc<dyn AuthorizedWebFetchClient>,
) -> Result<(), RegisterWebFetchToolError> {
    policy.validate()?;
    registry
        .register(Arc::new(WebFetchTool {
            policy,
            client,
            used: AtomicU32::new(0),
        }))
        .map_err(RegisterWebFetchToolError::Registry)
}

pub fn web_fetch_permission_policy(
    mut policy: PermissionPolicy,
) -> Result<PermissionPolicy, PermissionRuleError> {
    policy.push_rule(PermissionRule::new("web_fetch", PermissionEffect::Allow)?);
    Ok(policy)
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterWebFetchToolError {
    #[error(transparent)]
    Policy(#[from] NativeWebFetchPolicyError),
    #[error(transparent)]
    Registry(#[from] ToolRegistryError),
}

struct WebFetchTool {
    policy: NativeWebFetchPolicy,
    client: Arc<dyn AuthorizedWebFetchClient>,
    used: AtomicU32,
}

#[async_trait]
impl NativeTool for WebFetchTool {
    fn name(&self) -> &str {
        "web_fetch"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "web_fetch requires live native execution authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        let url = match parse_url(arguments, &self.policy) {
            Ok(url) => url,
            Err(error) => return Ok(Err(error)),
        };
        let prior = self.used.fetch_add(1, Ordering::AcqRel);
        if prior >= self.policy.max_fetches {
            self.used.fetch_sub(1, Ordering::AcqRel);
            return Ok(Err(ToolError::new(
                "web_fetch exceeded the configured per-run fetch limit",
            )));
        }
        let outcome = self
            .client
            .fetch_authorized(
                WebFetchRequest {
                    url,
                    allowed_domains: self.policy.allow_domains.clone(),
                    route: self.policy.route,
                    max_response_bytes: self.policy.max_response_bytes,
                    max_redirects: self.policy.max_redirects,
                },
                authority,
                effect,
                context.cancellation_token(),
            )
            .await?;
        let final_url = Url::parse(&outcome.final_url)
            .ok()
            .filter(|url| {
                url.scheme() == "https"
                    && url.as_str().len() <= MAX_URL_BYTES
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.port().is_none_or(|port| port == 443)
            })
            .filter(|url| url.domain().is_some_and(|host| self.policy.permits(host)))
            .ok_or_else(|| {
                vyane_core::VyaneError::new(
                    vyane_core::ErrorKind::Protocol,
                    "web fetch client returned an off-policy final URL",
                )
            })?;
        if outcome.content_type.is_empty()
            || outcome.content_type.len() > 128
            || outcome.content_type.chars().any(char::is_control)
        {
            return Err(vyane_core::VyaneError::new(
                vyane_core::ErrorKind::Protocol,
                "web fetch client returned an invalid content type",
            ));
        }
        let prefix = format!(
            "Fetched URL: {}\nContent-Type: {}\n\nUntrusted web content:\n",
            final_url, outcome.content_type
        );
        let remaining = MAX_TOOL_OUTPUT_CHARS.saturating_sub(prefix.chars().count());
        let body = outcome.text.chars().take(remaining).collect::<String>();
        Ok(Ok(format!("{prefix}{body}")))
    }
}

fn parse_url(
    arguments: &BTreeMap<String, Value>,
    policy: &NativeWebFetchPolicy,
) -> Result<String, ToolError> {
    if arguments.keys().any(|key| key != "url") {
        return Err(ToolError::new("web_fetch received an unknown argument"));
    }
    let raw = arguments
        .get("url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty() && url.len() <= MAX_URL_BYTES)
        .ok_or_else(|| ToolError::new("web_fetch URL is invalid"))?;
    let url = Url::parse(raw).map_err(|_| ToolError::new("web_fetch URL is invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some_and(|port| port != 443)
        || !url.domain().is_some_and(|host| policy.permits(host))
    {
        return Err(ToolError::new(
            "web_fetch URL exceeds the configured HTTPS domain policy",
        ));
    }
    Ok(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> NativeWebFetchPolicy {
        NativeWebFetchPolicy {
            allow_domains: vec!["example.com".into()],
            route: WebFetchRoute::Direct,
            max_fetches: 2,
            max_response_bytes: 4096,
            max_redirects: 1,
        }
    }

    #[test]
    fn policy_requires_a_bounded_plain_dns_allowlist() {
        assert!(policy().validate().is_ok());
        for invalid in ["localhost", "*.example.com", "127.0.0.1", "Example.com"] {
            let mut value = policy();
            value.allow_domains = vec![invalid.into()];
            assert!(value.validate().is_err(), "{invalid}");
        }
    }

    #[test]
    fn request_may_use_an_admitted_subdomain_but_not_another_origin() {
        let allowed = BTreeMap::from([(
            "url".into(),
            Value::String("https://docs.example.com/a?q=1".into()),
        )]);
        assert!(parse_url(&allowed, &policy()).is_ok());
        for invalid in [
            "http://example.com",
            "https://evil.example.net",
            "https://user@example.com",
            "https://example.com:444",
        ] {
            let call = BTreeMap::from([("url".into(), Value::String(invalid.into()))]);
            assert!(parse_url(&call, &policy()).is_err(), "{invalid}");
        }
    }
}
