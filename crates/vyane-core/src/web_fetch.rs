//! Provider-neutral vocabulary for one bounded public-web fetch.

use serde::{Deserialize, Serialize};

/// Network route used by the bounded public-web client.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebFetchRoute {
    #[default]
    Direct,
    EnvironmentProxy,
}

/// One HTTPS URL retrieval after native permission admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebFetchRequest {
    pub url: String,
    pub allowed_domains: Vec<String>,
    pub route: WebFetchRoute,
    pub max_response_bytes: usize,
    pub max_redirects: u32,
}

/// Normalized text returned by one public-web fetch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebFetchOutcome {
    pub final_url: String,
    pub content_type: String,
    pub text: String,
}
