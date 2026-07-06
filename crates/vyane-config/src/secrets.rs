//! Optional gitignored secrets file: `KEY=VALUE` lines folded into env
//! lookup, so a key can live in a local file instead of the shell
//! environment without ever being written into tracked config
//! (`profiles.example.toml` documents `secrets.env` as the conventional
//! name; see the repo's `.gitignore`).

use std::collections::BTreeMap;
use std::path::Path;

use vyane_core::{ErrorKind, Result, VyaneError};

/// Parse a `KEY=VALUE`-per-line secrets file into a map.
///
/// Blank lines and lines starting with `#` are ignored. Values are taken
/// verbatim after the first `=` (so a value may itself contain `=`); no
/// quoting or escaping is supported — this is intentionally a minimal
/// format, not a `.env` parser with shell-style semantics.
///
/// A missing file is not an error (the secrets file is optional); malformed
/// lines (no `=`) are.
pub fn load_secrets_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => {
            return Err(VyaneError::with_source(
                ErrorKind::Io,
                format!("failed to read secrets file {}", path.display()),
                e,
            ));
        }
    };
    parse_secrets(&text, path)
}

fn parse_secrets(text: &str, path: &Path) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for (line_no, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (key, value) = trimmed.split_once('=').ok_or_else(|| {
            VyaneError::new(
                ErrorKind::Config,
                format!(
                    "{}:{}: expected `KEY=VALUE`, got `{trimmed}`",
                    path.display(),
                    line_no + 1
                ),
            )
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(VyaneError::new(
                ErrorKind::Config,
                format!(
                    "{}:{}: empty key in `{trimmed}`",
                    path.display(),
                    line_no + 1
                ),
            ));
        }
        out.insert(key.to_string(), value.trim().to_string());
    }
    Ok(out)
}

/// Build an env-lookup closure that checks the given secrets map first,
/// falling back to the real process environment — the composition point
/// between the secrets file and `Provider::resolve_endpoint`'s env lookup.
pub fn env_lookup_with_secrets(
    secrets: BTreeMap<String, String>,
) -> impl Fn(&str) -> Option<String> {
    move |name: &str| {
        secrets
            .get(name)
            .cloned()
            .or_else(|| std::env::var(name).ok())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_key_value_lines_ignoring_blanks_and_comments() {
        let text = "\n# a comment\nFOO_KEY=bar\n\nBAZ_KEY=qux=with-equals\n";
        let parsed = parse_secrets(text, Path::new("secrets.env")).unwrap();
        assert_eq!(parsed.get("FOO_KEY").map(String::as_str), Some("bar"));
        assert_eq!(
            parsed.get("BAZ_KEY").map(String::as_str),
            Some("qux=with-equals")
        );
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn malformed_line_without_equals_is_a_config_error() {
        let text = "NOT_A_VALID_LINE\n";
        let err = parse_secrets(text, Path::new("secrets.env")).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
    }

    #[test]
    fn missing_file_returns_empty_map() {
        let path = Path::new("/nonexistent/path/that/should/not/exist/secrets.env");
        let parsed = load_secrets_file(path).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn load_secrets_file_reads_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.env");
        std::fs::write(&path, "SOME_KEY=some-value\n").unwrap();
        let parsed = load_secrets_file(&path).unwrap();
        assert_eq!(
            parsed.get("SOME_KEY").map(String::as_str),
            Some("some-value")
        );
    }

    #[test]
    fn env_lookup_with_secrets_prefers_secrets_over_process_env() {
        let mut secrets = BTreeMap::new();
        secrets.insert(
            "VYANE_SECRETS_TEST_VAR".to_string(),
            "from-secrets-file".to_string(),
        );
        let lookup = env_lookup_with_secrets(secrets);
        assert_eq!(
            lookup("VYANE_SECRETS_TEST_VAR"),
            Some("from-secrets-file".to_string())
        );
        // Falls through to a name the secrets map doesn't have — real env
        // resolution happens here, but a var this specific and unlikely to
        // exist should simply resolve to None rather than panic.
        assert_eq!(
            lookup("VYANE_SECRETS_TEST_VAR_definitely_unset_anywhere"),
            None
        );
    }
}
