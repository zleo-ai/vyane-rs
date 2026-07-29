use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;
use vyane_core::{
    AuthorizedWebSearchClient, ErrorKind, GenParams, ModelId, NativeExecutionAuthority,
    NativeSideEffect, Result as VyaneResult, ToolDefinition, VyaneError, WebSearchContextSize,
    WebSearchRequest,
};

use super::{
    MAX_TOOL_OUTPUT_CHARS, NativeTool, PermissionEffect, PermissionPolicy, PermissionRule,
    PermissionRuleError, ToolContext, ToolError, ToolRegistry, ToolRegistryError,
};

const MAX_DOMAINS: usize = 128;
const MAX_DOMAIN_BYTES: usize = 253;
const MAX_QUERY_BYTES: usize = 16 * 1024;
const MAX_REQUEST_DOMAINS: usize = 32;
const MAX_SEARCHES: u32 = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWebSearchPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_domains: Option<Vec<String>>,
    #[serde(default = "default_max_searches")]
    pub max_searches: u32,
    #[serde(default)]
    pub search_context_size: WebSearchContextSize,
}

impl NativeWebSearchPolicy {
    pub fn validate(&self) -> Result<(), NativeWebSearchPolicyError> {
        if self.max_searches == 0 || self.max_searches > MAX_SEARCHES {
            return Err(NativeWebSearchPolicyError::InvalidSearchLimit);
        }
        if let Some(domains) = &self.allow_domains {
            if domains.is_empty() {
                return Err(NativeWebSearchPolicyError::EmptyAllowlist);
            }
            if domains.len() > MAX_DOMAINS {
                return Err(NativeWebSearchPolicyError::TooManyDomains);
            }
            let mut unique = BTreeSet::new();
            for domain in domains {
                validate_search_domain(domain)?;
                if !unique.insert(domain) {
                    return Err(NativeWebSearchPolicyError::DuplicateDomain);
                }
            }
        }
        Ok(())
    }

    fn permits_domain(&self, domain: &str) -> bool {
        self.allow_domains.as_ref().is_none_or(|allowed| {
            allowed
                .iter()
                .any(|base| domain == base || domain.ends_with(&format!(".{base}")))
        })
    }
}

const fn default_max_searches() -> u32 {
    4
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeWebSearchPolicyError {
    #[error("native web-search domain allowlist must not be empty")]
    EmptyAllowlist,
    #[error("native web-search policy contains too many domains")]
    TooManyDomains,
    #[error("native web-search policy contains an invalid domain")]
    InvalidDomain,
    #[error("native web-search policy contains a duplicate domain")]
    DuplicateDomain,
    #[error("native web-search limit is invalid")]
    InvalidSearchLimit,
}

pub fn validate_search_domain(domain: &str) -> Result<(), NativeWebSearchPolicyError> {
    if domain.is_empty()
        || domain.len() > MAX_DOMAIN_BYTES
        || domain != domain.to_ascii_lowercase()
        || domain.starts_with('.')
        || domain.ends_with('.')
        || domain.parse::<std::net::IpAddr>().is_ok()
        || !domain.contains('.')
    {
        return Err(NativeWebSearchPolicyError::InvalidDomain);
    }
    for label in domain.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(NativeWebSearchPolicyError::InvalidDomain);
        }
    }
    Ok(())
}

pub fn web_search_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: "web_search".into(),
        description: "Search the public web through the separately authorized hosted-search \
            target. Optional domains may narrow, but never widen, the submission policy."
            .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_QUERY_BYTES
                },
                "allowed_domains": {
                    "type": "array",
                    "items": { "type": "string" },
                    "minItems": 1,
                    "maxItems": MAX_REQUEST_DOMAINS
                }
            },
            "required": ["query"],
            "additionalProperties": false
        }),
    }
}

pub fn register_web_search_tool(
    registry: &mut ToolRegistry,
    policy: NativeWebSearchPolicy,
    client: Arc<dyn AuthorizedWebSearchClient>,
    model: ModelId,
    params: GenParams,
) -> Result<(), RegisterWebSearchToolError> {
    policy.validate()?;
    registry
        .register(Arc::new(WebSearchTool {
            policy,
            client,
            model,
            params,
        }))
        .map_err(RegisterWebSearchToolError::Registry)
}

pub fn web_search_permission_policy(
    mut policy: PermissionPolicy,
) -> Result<PermissionPolicy, PermissionRuleError> {
    policy.push_rule(PermissionRule::new("web_search", PermissionEffect::Allow)?);
    Ok(policy)
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterWebSearchToolError {
    #[error(transparent)]
    Policy(#[from] NativeWebSearchPolicyError),
    #[error(transparent)]
    Registry(#[from] ToolRegistryError),
}

struct WebSearchTool {
    policy: NativeWebSearchPolicy,
    client: Arc<dyn AuthorizedWebSearchClient>,
    model: ModelId,
    params: GenParams,
}

#[async_trait]
impl NativeTool for WebSearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "web_search requires live native execution authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        let request = match SearchCall::parse(arguments, &self.policy) {
            Ok(request) => request,
            Err(error) => return Ok(Err(error)),
        };
        authority.revalidate(effect).await?;
        let outcome = self
            .client
            .search_authorized(
                WebSearchRequest {
                    model: self.model.clone(),
                    query: request.query,
                    allowed_domains: request.allowed_domains,
                    max_searches: self.policy.max_searches,
                    context_size: self.policy.search_context_size,
                    params: self.params.clone(),
                },
                authority,
                effect,
                context.cancellation_token(),
            )
            .await?;
        if outcome.sources.is_empty() {
            return Err(VyaneError::new(
                ErrorKind::Protocol,
                "hosted web-search returned no cited sources",
            ));
        }
        for source in &outcome.sources {
            let url = Url::parse(&source.url).map_err(|_| invalid_source())?;
            if !matches!(url.scheme(), "http" | "https")
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(invalid_source());
            }
            let domain = url.domain().ok_or_else(invalid_source)?;
            if !self.policy.permits_domain(domain)
                || request_domains_reject(request.requested_domains.as_deref(), domain)
            {
                return Err(VyaneError::new(
                    ErrorKind::Protocol,
                    "hosted web-search returned a source outside the admitted domain policy",
                ));
            }
        }
        Ok(Ok(format_outcome(outcome)))
    }
}

struct SearchCall {
    query: String,
    allowed_domains: Option<Vec<String>>,
    requested_domains: Option<Vec<String>>,
}

impl SearchCall {
    fn parse(
        arguments: &BTreeMap<String, Value>,
        policy: &NativeWebSearchPolicy,
    ) -> Result<Self, ToolError> {
        if arguments
            .keys()
            .any(|key| key != "query" && key != "allowed_domains")
        {
            return Err(ToolError::new("web_search received an unknown argument"));
        }
        let query = arguments
            .get("query")
            .and_then(Value::as_str)
            .filter(|query| !query.trim().is_empty() && query.len() <= MAX_QUERY_BYTES)
            .ok_or_else(|| ToolError::new("web_search query is invalid"))?
            .to_string();
        let requested_domains = arguments
            .get("allowed_domains")
            .map(parse_requested_domains)
            .transpose()?;
        if let Some(domains) = &requested_domains
            && domains.iter().any(|domain| !policy.permits_domain(domain))
        {
            return Err(ToolError::new(
                "web_search domains exceed the submission policy",
            ));
        }
        let allowed_domains = requested_domains
            .clone()
            .or_else(|| policy.allow_domains.clone());
        Ok(Self {
            query,
            allowed_domains,
            requested_domains,
        })
    }
}

fn parse_requested_domains(value: &Value) -> Result<Vec<String>, ToolError> {
    let values = value
        .as_array()
        .filter(|values| !values.is_empty() && values.len() <= MAX_REQUEST_DOMAINS)
        .ok_or_else(|| ToolError::new("web_search domains are invalid"))?;
    let mut domains = Vec::with_capacity(values.len());
    let mut unique = BTreeSet::new();
    for value in values {
        let domain = value
            .as_str()
            .ok_or_else(|| ToolError::new("web_search domains are invalid"))?;
        validate_search_domain(domain)
            .map_err(|_| ToolError::new("web_search domains are invalid"))?;
        if !unique.insert(domain) {
            return Err(ToolError::new("web_search domains contain a duplicate"));
        }
        domains.push(domain.to_string());
    }
    Ok(domains)
}

fn request_domains_reject(domains: Option<&[String]>, domain: &str) -> bool {
    domains.is_some_and(|domains| {
        !domains
            .iter()
            .any(|base| domain == base || domain.ends_with(&format!(".{base}")))
    })
}

fn invalid_source() -> VyaneError {
    VyaneError::new(
        ErrorKind::Protocol,
        "hosted web-search returned an invalid source URL",
    )
}

fn format_outcome(outcome: vyane_core::WebSearchOutcome) -> String {
    let answer_limit = MAX_TOOL_OUTPUT_CHARS.saturating_mul(3) / 4;
    let mut output = truncate_chars(outcome.text, answer_limit);
    output.push_str("\n\nSources:\n");
    for (index, source) in outcome.sources.iter().enumerate() {
        let mut line = format!("{}. ", index + 1);
        if let Some(title) = &source.title {
            line.push_str(&single_line(title));
            line.push_str(" — ");
        }
        line.push_str(&source.url);
        line.push('\n');
        let remaining = MAX_TOOL_OUTPUT_CHARS.saturating_sub(output.chars().count());
        if remaining == 0 {
            break;
        }
        output.push_str(&line.chars().take(remaining).collect::<String>());
        if line.chars().count() > remaining {
            break;
        }
    }
    output
}

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(value: String, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value;
    }
    value.chars().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_are_plain_lowercase_dns_names() {
        assert!(validate_search_domain("docs.example.com").is_ok());
        for invalid in [
            "",
            "localhost",
            "*.example.com",
            "Example.com",
            "127.0.0.1",
            "-bad.example",
            "bad_.example",
        ] {
            assert!(validate_search_domain(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn policy_domain_includes_subdomains() {
        let policy = NativeWebSearchPolicy {
            allow_domains: Some(vec!["example.com".into()]),
            max_searches: 4,
            search_context_size: WebSearchContextSize::Medium,
        };
        assert!(policy.permits_domain("example.com"));
        assert!(policy.permits_domain("docs.example.com"));
        assert!(!policy.permits_domain("notexample.com"));
    }
}
