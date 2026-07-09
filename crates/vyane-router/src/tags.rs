//! Tag inference: derive routing tags from task text and intent.
//!
//! Tags are short domain labels (`frontend`, `security`, `architecture`,
//! `code`, `debug`, …) that drive preference resolution. They come from two
//! sources merged in order: regex matches on the task text, and a tag derived
//! from the classified intent category.

use crate::intent::{IntentCategory, IntentResult, classify_intent};

/// Infer routing tags from task text. Runs intent classification internally
/// when no intent is supplied.
pub fn infer_route_tags(task: &str) -> Vec<String> {
    let intent = classify_intent(task);
    infer_route_tags_with_intent(task, &intent)
}

/// Infer routing tags with a pre-computed intent (avoids re-classifying when
/// the caller already has the result).
pub fn infer_route_tags_with_intent(task: &str, intent: &IntentResult) -> Vec<String> {
    let text = task.to_ascii_lowercase();
    let mut tags: Vec<String> = Vec::new();

    macro_rules! add {
        ($tag:expr) => {
            if !tags.iter().any(|t| t == $tag) {
                tags.push($tag.to_string());
            }
        };
    }

    // Frontend
    if contains_any(
        &text,
        &[
            "frontend", "ui", "ux", "css", "html", "react", "vue", "svelte", "tailwind",
        ],
    ) {
        add!("frontend");
    }
    // Security — avoid matching "author/authority"
    if contains_any(
        &text,
        &[
            "security",
            "audit",
            "vulnerab",
            "threat",
            "auth",
            "oauth",
            "permissions",
        ],
    ) && !text.contains("authority")
        && !text.contains("author")
    {
        add!("security");
    }
    // Architecture
    if contains_any(
        &text,
        &[
            "architect",
            "architecture",
            "adr",
            "rfc",
            "design pattern",
            "system design",
        ],
    ) {
        add!("architecture");
    }
    // Multimodal
    if contains_any(
        &text,
        &["multimodal", "audio", "video", "voice", "tts", "vision"],
    ) {
        add!("multimodal");
    }
    // Long document
    if contains_any(
        &text,
        &[
            "long document",
            "longform",
            "book",
            "paper",
            "large doc",
            "long report",
            "large report",
        ],
    ) {
        add!("long-doc");
    }

    // Intent-derived tag
    let intent_tag = match intent.primary {
        IntentCategory::CodeGen => Some("code"),
        IntentCategory::Review => Some("review"),
        IntentCategory::Analysis => Some("analysis"),
        IntentCategory::Debug => Some("debug"),
        IntentCategory::Docs => Some("docs"),
        IntentCategory::Research => Some("research"),
        IntentCategory::Refactor => Some("refactor"),
        IntentCategory::Test => Some("test"),
    };
    if let Some(tag) = intent_tag {
        add!(tag);
    }

    tags
}

/// Check if the (already-lowercased) text contains any of the substrings.
fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|n| text.contains(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontend_tag() {
        let tags = infer_route_tags("build a react frontend component");
        assert!(tags.contains(&"frontend".to_string()));
    }

    #[test]
    fn security_tag() {
        let tags = infer_route_tags("fix the security vulnerability");
        assert!(tags.contains(&"security".to_string()));
    }

    #[test]
    fn security_excludes_authority() {
        let tags = infer_route_tags("check the authority of the user");
        assert!(!tags.contains(&"security".to_string()));
    }

    #[test]
    fn architecture_tag() {
        let tags = infer_route_tags("write an ADR for the system design");
        assert!(tags.contains(&"architecture".to_string()));
    }

    #[test]
    fn intent_tag_code() {
        let tags = infer_route_tags("implement the function");
        assert!(tags.contains(&"code".to_string()));
    }

    #[test]
    fn intent_tag_debug() {
        let tags = infer_route_tags("fix the crash");
        assert!(tags.contains(&"debug".to_string()));
    }

    #[test]
    fn tags_deduplicated() {
        let tags = infer_route_tags("debug the debug issue");
        let debug_count = tags.iter().filter(|t| t.as_str() == "debug").count();
        assert_eq!(debug_count, 1);
    }

    #[test]
    fn no_tags_for_generic_text() {
        let tags = infer_route_tags("hello world");
        // Intent defaults to code_gen → "code" tag
        assert!(tags.contains(&"code".to_string()));
    }
}
