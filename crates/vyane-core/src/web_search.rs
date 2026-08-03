//! Provider-neutral vocabulary for one hosted web-search operation.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{GenParams, ModelId, Usage};

/// Search context requested from a hosted provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSearchContextSize {
    Low,
    #[default]
    Medium,
    High,
}

impl WebSearchContextSize {
    /// Stable snake_case token matching the serde rename for this context size.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl fmt::Display for WebSearchContextSize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::WebSearchContextSize;

    #[test]
    fn web_search_context_size_tokens_match_serde_snake_case() {
        assert_eq!(WebSearchContextSize::Low.as_str(), "low");
        assert_eq!(WebSearchContextSize::Medium.to_string(), "medium");
        assert_eq!(WebSearchContextSize::High.as_str(), "high");
    }
}

/// One bounded request to a provider-hosted search capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebSearchRequest {
    pub model: ModelId,
    pub query: String,
    pub allowed_domains: Option<Vec<String>>,
    pub max_searches: u32,
    pub context_size: WebSearchContextSize,
    pub params: GenParams,
}

/// One cited public source returned by hosted search.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSearchSource {
    pub url: String,
    pub title: Option<String>,
}

/// Normalized output from one hosted search request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebSearchOutcome {
    pub text: String,
    pub sources: Vec<WebSearchSource>,
    pub usage: Option<Usage>,
    pub model_echo: Option<String>,
}
