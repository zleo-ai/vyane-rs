//! Tag inference: derive routing tags from task text and intent.
//!
//! Tags are short domain labels (`frontend`, `security`, `architecture`,
//! `code`, `debug`, …) that drive preference resolution. They come from two
//! sources merged in order: word-boundary matches on the task text, and a tag
//! derived from the classified intent category.
//!
//! Word boundaries are load-bearing: `"ui"` must not match `"build"`, `"auth"`
//! must not match `"author"`. We implement `\b` semantics without a regex
//! dependency by checking that the needle is flanked by non-word characters or
//! string edges.

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
    if contains_any_word(
        &text,
        &[
            "frontend", "ui", "ux", "css", "html", "react", "vue", "svelte", "tailwind",
        ],
    ) {
        add!("frontend");
    }
    // Security — word-boundary matching ensures "auth" doesn't hit "author"/"authority"
    if contains_any_word(
        &text,
        &[
            "security",
            "audit",
            "vulnerab",
            "threat",
            "oauth",
            "oauth2",
            "permissions",
        ],
    ) || contains_word_prefix(&text, "auth", &["entication", "orization"])
    {
        add!("security");
    }
    // Architecture
    if contains_any_word(
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
    if contains_any_word(
        &text,
        &["multimodal", "audio", "video", "voice", "tts", "vision"],
    ) {
        add!("multimodal");
    }
    // Long document
    if contains_any_word(
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

/// Check if the text contains any of the needles at a word boundary.
///
/// A word boundary means the character before the needle (if any) is not an
/// ASCII alphanumeric character, and the character after the needle (if any) is
/// not an ASCII alphanumeric character. This mirrors `\b` in regex for ASCII
/// text. Multi-word needles (e.g. "design pattern") check boundaries at the
/// outermost edges only.
fn contains_any_word(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|n| contains_word(text, n))
}

/// Check if `needle` appears in `text` at a word boundary.
fn contains_word(text: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let mut start = 0;
    while let Some(pos) = text[start..].find(needle) {
        let abs_start = start + pos;
        let abs_end = abs_start + needle.len();

        let left_ok = abs_start == 0 || !text.as_bytes()[abs_start - 1].is_ascii_alphanumeric();
        let right_ok = abs_end >= text.len() || !text.as_bytes()[abs_end].is_ascii_alphanumeric();

        if left_ok && right_ok {
            return true;
        }
        start = abs_start + 1;
    }
    false
}

/// Check if `prefix` appears at a word boundary, optionally followed by one of
/// `suffixes` (also at a word boundary at the suffix end). Used for
/// `auth(entication|orization)` style patterns.
fn contains_word_prefix(text: &str, prefix: &str, suffixes: &[&str]) -> bool {
    let mut start = 0;
    while let Some(pos) = text[start..].find(prefix) {
        let abs_start = start + pos;
        let left_ok = abs_start == 0 || !text.as_bytes()[abs_start - 1].is_ascii_alphanumeric();
        if left_ok {
            let after = &text[abs_start + prefix.len()..];
            // Check if followed by a known suffix at a word boundary
            for suffix in suffixes {
                if after.starts_with(suffix) {
                    let end = abs_start + prefix.len() + suffix.len();
                    let right_ok =
                        end >= text.len() || !text.as_bytes()[end].is_ascii_alphanumeric();
                    if right_ok {
                        return true;
                    }
                }
            }
            // Or `auth` as a standalone word
            let after_byte = text.as_bytes().get(abs_start + prefix.len());
            if after_byte
                .map(|b| !b.is_ascii_alphanumeric())
                .unwrap_or(true)
            {
                return true;
            }
        }
        start = abs_start + 1;
    }
    false
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
    fn frontend_word_boundary() {
        // "ui" must NOT match "build"
        let tags = infer_route_tags("build the thing");
        assert!(!tags.contains(&"frontend".to_string()));
        // "ui" as a standalone word SHOULD match
        let tags = infer_route_tags("improve the ui of the app");
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
    fn security_excludes_author() {
        let tags = infer_route_tags("the author wrote a book");
        assert!(!tags.contains(&"security".to_string()));
    }

    #[test]
    fn security_matches_authentication() {
        let tags = infer_route_tags("add authentication to the endpoint");
        assert!(tags.contains(&"security".to_string()));
    }

    #[test]
    fn security_matches_oauth() {
        let tags = infer_route_tags("set up oauth2 for the app");
        assert!(tags.contains(&"security".to_string()));
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

    #[test]
    fn multimodal_word_boundary() {
        // "video" must not match "television" (which contains "vision")
        let tags = infer_route_tags("watch television");
        assert!(!tags.contains(&"multimodal".to_string()));
        // "audio" as standalone word should match
        let tags = infer_route_tags("process the audio file");
        assert!(tags.contains(&"multimodal".to_string()));
    }
}
