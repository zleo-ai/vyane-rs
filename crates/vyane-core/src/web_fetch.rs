//! Provider-neutral vocabulary for one bounded public-web fetch.

use serde::{Deserialize, Serialize};

/// One HTTPS URL retrieval after native permission admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebFetchRequest {
    pub url: String,
    pub allowed_domains: Vec<String>,
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
