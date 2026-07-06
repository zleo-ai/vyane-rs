//! Layered config merge: built-in defaults < user file < project file <
//! explicit CLI args, each layer overriding the ones below it **per field**
//! (see `docs/plan/WP-01.md` — "Layering precedence").

use std::collections::BTreeMap;
use std::path::Path;

use vyane_core::{ErrorKind, Result, VyaneError};
use vyane_provider::{ProviderPatchSet, ProviderRegistry};

use crate::model::{ProfilePatch, RawRoot};

/// The stack of config layers, already merged in precedence order (lowest
/// first): built-ins, then user file, then project file, then CLI-derived
/// overrides.
#[derive(Debug, Clone, Default)]
pub struct ConfigLayers {
    pub providers: ProviderRegistry,
    pub profiles: BTreeMap<String, ProfilePatch>,
}

impl ConfigLayers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge one layer's parsed [`RawRoot`] on top of the accumulated state.
    /// Call in precedence order: built-ins first, then user file, then
    /// project file, then CLI-derived overrides last.
    pub fn merge(&mut self, root: &RawRoot) -> Result<()> {
        let provider_patches = ProviderPatchSet {
            providers: root.providers.clone(),
        };
        self.providers.merge_over(&provider_patches)?;

        for (name, patch) in &root.profiles {
            match self.profiles.get_mut(name) {
                Some(existing) => patch.apply_over(existing),
                None => {
                    self.profiles.insert(name.clone(), patch.clone());
                }
            }
        }
        Ok(())
    }

    /// Parse and merge a TOML file layer, if it exists. Missing files are
    /// not an error (a layer is optional) — only unreadable-but-present or
    /// unparseable files are.
    pub fn merge_file(&mut self, path: &Path) -> Result<()> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(VyaneError::with_source(
                    ErrorKind::Io,
                    format!("failed to read config file {}", path.display()),
                    e,
                ));
            }
        };
        let root: RawRoot = toml::from_str(&text).map_err(|e| {
            VyaneError::with_source(
                ErrorKind::Config,
                format!("failed to parse config file {}", path.display()),
                e,
            )
        })?;
        self.merge(&root)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use vyane_core::{AuthStyle, ModelId, Protocol};

    fn provider_toml(base_url: &str) -> String {
        format!(
            r#"
            [providers.anthropic]
            base_url = "{base_url}"
            api_key_env = "ANTHROPIC_API_KEY"
            auth_style = "x_api_key"
            protocol = "anthropic_messages"
            default_model = "a-capable-anthropic-model"
            "#
        )
    }

    #[test]
    fn precedence_built_in_lt_user_lt_project_lt_cli() {
        let mut layers = ConfigLayers::new();

        // built-in: nothing (no compiled-in provider defaults for v0.1).
        layers.merge(&RawRoot::default()).unwrap();

        // user file: full provider definition.
        let user: RawRoot = toml::from_str(&provider_toml("https://user.example")).unwrap();
        layers.merge(&user).unwrap();
        assert_eq!(
            layers.providers.get("anthropic").unwrap().base_url,
            "https://user.example"
        );

        // project file: overrides only base_url.
        let project_toml = r#"
            [providers.anthropic]
            base_url = "https://project.example"
        "#;
        let project: RawRoot = toml::from_str(project_toml).unwrap();
        layers.merge(&project).unwrap();
        let merged = layers.providers.get("anthropic").unwrap();
        assert_eq!(merged.base_url, "https://project.example");
        // Sibling fields from the user layer survive the project override.
        assert_eq!(merged.auth_style, AuthStyle::XApiKey);
        assert_eq!(merged.protocol, Protocol::AnthropicMessages);
        assert_eq!(
            merged.default_model,
            Some(ModelId::new("a-capable-anthropic-model"))
        );

        // CLI args: overrides only default_model, must not disturb base_url.
        let cli_toml = r#"
            [providers.anthropic]
            default_model = "cli-overridden-model"
        "#;
        let cli: RawRoot = toml::from_str(cli_toml).unwrap();
        layers.merge(&cli).unwrap();
        let merged = layers.providers.get("anthropic").unwrap();
        assert_eq!(merged.base_url, "https://project.example");
        assert_eq!(
            merged.default_model,
            Some(ModelId::new("cli-overridden-model"))
        );
    }

    #[test]
    fn precedence_profile_field_override_leaves_siblings_intact() {
        let mut layers = ConfigLayers::new();
        let base_toml = r#"
            [profiles.review]
            provider = "anthropic"
            protocol = "anthropic_messages"
            harness  = "none"
            model    = "a-capable-anthropic-model"

            [profiles.review.params]
            effort = "high"
        "#;
        let base: RawRoot = toml::from_str(base_toml).unwrap();
        layers.merge(&base).unwrap();

        // Project layer overrides only the model.
        let override_toml = r#"
            [profiles.review]
            model = "a-different-model"
        "#;
        let over: RawRoot = toml::from_str(override_toml).unwrap();
        layers.merge(&over).unwrap();

        let profile = layers.profiles.get("review").unwrap();
        assert_eq!(profile.model, Some(ModelId::new("a-different-model")));
        assert_eq!(profile.provider.as_deref(), Some("anthropic"));
        assert_eq!(profile.harness.as_deref(), Some("none"));
        assert_eq!(
            profile.params.as_ref().unwrap().effort,
            Some(vyane_core::Effort::High)
        );
    }

    #[test]
    fn incomplete_new_provider_patch_errors_instead_of_silently_dropping() {
        let mut layers = ConfigLayers::new();
        // A provider introduced for the first time with only base_url set:
        // missing required auth_style/protocol should surface as an error,
        // not vanish silently.
        let toml_src = r#"
            [providers.incomplete]
            base_url = "https://incomplete.example"
        "#;
        let root: RawRoot = toml::from_str(toml_src).unwrap();
        let err = layers.merge(&root).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(err.message.contains("incomplete"));
    }

    #[test]
    fn merge_file_missing_file_is_not_an_error() {
        let mut layers = ConfigLayers::new();
        let missing = Path::new("/nonexistent/path/that/should/not/exist/config.toml");
        layers.merge_file(missing).unwrap();
        assert!(layers.providers.providers.is_empty());
        assert!(layers.profiles.is_empty());
    }

    #[test]
    fn merge_file_reads_and_merges_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, provider_toml("https://file.example")).unwrap();

        let mut layers = ConfigLayers::new();
        layers.merge_file(&path).unwrap();
        assert_eq!(
            layers.providers.get("anthropic").unwrap().base_url,
            "https://file.example"
        );
    }
}
