//! Layered config merge: built-in defaults < user file < project file <
//! explicit CLI args, each ordinary layer overriding the ones below it **per
//! field** (see `docs/plan/WP-01.md` — "Layering precedence"). Native
//! permission sections are the exception: every exact file contributes an
//! independent monotonic ceiling.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use vyane_core::{ErrorKind, Result, VyaneError};
use vyane_provider::{ProviderPatchSet, ProviderRegistry};

use crate::model::{HarnessPermissionCeiling, NativePermissionCeiling, ProfilePatch, RawRoot};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedPermissionRoot {
    #[serde(default)]
    native_permissions: Option<NativePermissionCeiling>,
    #[serde(default)]
    harness_permissions: Option<HarnessPermissionCeiling>,
}

/// The stack of config layers, already merged in precedence order (lowest
/// first): built-ins, then user file, then project file, then CLI-derived
/// overrides.
#[derive(Debug, Clone, Default)]
pub struct ConfigLayers {
    pub providers: ProviderRegistry,
    pub profiles: BTreeMap<String, ProfilePatch>,
    /// Independent monotonic ceilings contributed by exact config files.
    pub native_permission_ceilings: Vec<NativePermissionCeiling>,
    /// Independent CLI-harness sandbox ceilings from exact config files.
    pub harness_permission_ceilings: Vec<HarnessPermissionCeiling>,
    /// Explicit model rates. Later layers replace a model entry entirely.
    pub model_rates: BTreeMap<String, vyane_core::ModelTokenRate>,
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
        if let Some(ceiling) = &root.native_permissions {
            self.native_permission_ceilings.push(ceiling.clone());
        }
        if let Some(ceiling) = root.harness_permissions {
            self.harness_permission_ceilings.push(ceiling);
        }
        let mut rates = Vec::with_capacity(root.cost.models.len());
        for (model, raw) in &root.cost.models {
            rates.push((model.clone(), raw.to_rate(model)?));
        }
        for (model, rate) in rates {
            self.model_rates.insert(model, rate);
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

    /// Load a managed file that may contain only monotonic permission
    /// ceilings. Provider/profile changes are deliberately impossible through
    /// this operator-controlled path.
    pub fn merge_managed_permission_file(&mut self, path: &Path) -> Result<()> {
        let text = std::fs::read_to_string(path).map_err(|error| {
            VyaneError::with_source(
                ErrorKind::Io,
                format!(
                    "failed to read managed permission config {}",
                    path.display()
                ),
                error,
            )
        })?;
        let root: ManagedPermissionRoot = toml::from_str(&text).map_err(|error| {
            VyaneError::with_source(
                ErrorKind::Config,
                format!(
                    "failed to parse managed permission config {}",
                    path.display()
                ),
                error,
            )
        })?;
        if root.native_permissions.is_none() && root.harness_permissions.is_none() {
            return Err(VyaneError::new(
                ErrorKind::Config,
                "managed permission config contains no permission ceiling",
            ));
        }
        if let Some(ceiling) = root.native_permissions {
            self.native_permission_ceilings.push(ceiling);
        }
        if let Some(ceiling) = root.harness_permissions {
            self.harness_permission_ceilings.push(ceiling);
        }
        Ok(())
    }

    /// Backward-compatible name for the original managed native-only entry.
    pub fn merge_managed_native_file(&mut self, path: &Path) -> Result<()> {
        self.merge_managed_permission_file(path)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::CONFIG_SCHEMA_VERSION;
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

    #[test]
    fn permission_layers_accumulate_as_independent_ceilings() {
        let user: RawRoot = toml::from_str(
            r#"
            [native_permissions.filesystem_read]
            exclude = [".env*"]

            [native_permissions.web_fetch]
            allow_domains = ["example.com"]
            max_fetches = 4
            "#,
        )
        .unwrap();
        let project: RawRoot = toml::from_str(
            r#"
            [native_permissions.filesystem_read]
            exclude = ["private/**"]
            "#,
        )
        .unwrap();
        let mut layers = ConfigLayers::new();
        layers.merge(&user).unwrap();
        layers.merge(&project).unwrap();

        assert_eq!(layers.native_permission_ceilings.len(), 2);
        assert_eq!(
            layers.native_permission_ceilings[0].filesystem_read.exclude,
            [".env*"]
        );
        assert_eq!(
            layers.native_permission_ceilings[1].filesystem_read.exclude,
            ["private/**"]
        );
        assert!(layers.native_permission_ceilings[1].web_fetch.is_none());
    }

    #[test]
    fn harness_permission_layers_cannot_replace_one_another() {
        let user: RawRoot = toml::from_str(
            r#"
            [harness_permissions]
            max_sandbox = "write"
            "#,
        )
        .unwrap();
        let project: RawRoot = toml::from_str(
            r#"
            [harness_permissions]
            max_sandbox = "full"
            "#,
        )
        .unwrap();
        let mut layers = ConfigLayers::new();
        layers.merge(&user).unwrap();
        layers.merge(&project).unwrap();

        assert_eq!(
            layers.harness_permission_ceilings,
            [
                HarnessPermissionCeiling {
                    max_sandbox: vyane_core::Sandbox::Write,
                },
                HarnessPermissionCeiling {
                    max_sandbox: vyane_core::Sandbox::Full,
                },
            ]
        );
    }

    #[test]
    fn managed_permission_file_accepts_only_permission_ceilings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("managed.toml");
        std::fs::write(
            &path,
            r#"
            [native_permissions.filesystem_read]
            exclude = [".env*"]

            [harness_permissions]
            max_sandbox = "read-only"
            "#,
        )
        .unwrap();

        let mut layers = ConfigLayers::new();
        layers.merge_managed_permission_file(&path).unwrap();
        assert_eq!(layers.native_permission_ceilings.len(), 1);
        assert_eq!(
            layers.harness_permission_ceilings[0].max_sandbox,
            vyane_core::Sandbox::ReadOnly
        );
    }

    #[test]
    fn managed_native_file_rejects_provider_or_unknown_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("managed.toml");
        std::fs::write(
            &path,
            r#"
            [native_permissions.filesystem_read]
            exclude = [".env*"]

            [providers.forbidden]
            base_url = "https://example.com"
            "#,
        )
        .unwrap();

        let mut layers = ConfigLayers::new();
        assert!(layers.merge_managed_native_file(&path).is_err());
        assert!(layers.native_permission_ceilings.is_empty());
    }

    #[test]
    fn configured_model_rates_replace_per_model_and_reject_bad_numbers() {
        let user: RawRoot = toml::from_str(
            r#"
            [cost.models.alpha]
            input_per_mtok = 1.0
            output_per_mtok = 2.0
            reasoning_per_mtok = 8.0

            [cost.models.beta]
            input_per_mtok = 4.0
            output_per_mtok = 5.0
            "#,
        )
        .unwrap();
        let project: RawRoot = toml::from_str(
            r#"
            [cost.models.alpha]
            input_per_mtok = 3.0
            output_per_mtok = 6.0
            cache_read_per_mtok = 0.1
            "#,
        )
        .unwrap();
        let mut layers = ConfigLayers::new();
        layers.merge(&user).unwrap();
        layers.merge(&project).unwrap();

        let alpha = layers.model_rates["alpha"];
        assert_eq!(alpha.input_per_1m, 3.0);
        assert_eq!(alpha.output_per_1m, 6.0);
        assert_eq!(alpha.reasoning_per_1m, None);
        assert_eq!(alpha.cache_read_per_1m, Some(0.1));
        assert_eq!(layers.model_rates["beta"].input_per_1m, 4.0);
        assert!(!layers.model_rates.contains_key("unlisted"));

        let negative: RawRoot = toml::from_str(
            r#"
            [cost.models.alpha]
            input_per_mtok = -1.0
            output_per_mtok = 2.0
            "#,
        )
        .unwrap();
        assert!(layers.merge(&negative).is_err());
        assert_eq!(layers.model_rates["alpha"].input_per_1m, 3.0);
    }

    #[test]
    fn example_config_carries_its_explicit_model_rate() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../profiles.example.toml");
        let mut layers = ConfigLayers::new();
        layers.merge_file(&path).unwrap();
        let rate = layers
            .model_rates
            .get("a-capable-anthropic-model")
            .expect("example cost table");
        assert_eq!(rate.input_per_1m, 5.0);
        assert_eq!(rate.output_per_1m, 25.0);
        assert_eq!(rate.reasoning_per_1m, None);
        assert_eq!(rate.cache_read_per_1m, None);
    }

    #[test]
    fn unknown_cost_field_is_rejected() {
        let parsed = toml::from_str::<RawRoot>(
            r#"
            [cost.models.alpha]
            input_per_mtok = 1.0
            output_per_mtok = 2.0
            surprise = 1.0
            "#,
        );
        assert!(parsed.is_err());
    }

    #[test]
    fn omitted_schema_version_still_loads() {
        let text = format!(
            "{}\n[notes]\nleftover = true\n",
            provider_toml("https://omitted.example")
        );
        let root: RawRoot = toml::from_str(&text).unwrap();
        assert_eq!(
            root.providers["anthropic"].base_url.as_deref(),
            Some("https://omitted.example")
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, &text).unwrap();
        let mut layers = ConfigLayers::new();
        layers.merge_file(&path).unwrap();
        assert_eq!(
            layers.providers.get("anthropic").unwrap().base_url,
            "https://omitted.example"
        );

        let current = format!("schema_version = {CONFIG_SCHEMA_VERSION}\n{text}");
        let current_root: RawRoot = toml::from_str(&current).unwrap();
        assert_eq!(
            current_root.providers["anthropic"].base_url.as_deref(),
            Some("https://omitted.example")
        );
        let current_path = dir.path().join("current.toml");
        std::fs::write(&current_path, &current).unwrap();
        let mut current_layers = ConfigLayers::new();
        current_layers.merge_file(&current_path).unwrap();
        assert_eq!(
            current_layers.providers.get("anthropic").unwrap().base_url,
            "https://omitted.example"
        );
    }

    #[test]
    fn unknown_schema_version_fails_closed_before_use() {
        let text = format!(
            "schema_version = 99\n{}\n",
            provider_toml("https://rejected.example")
        );
        let parsed = toml::from_str::<RawRoot>(&text).unwrap_err();
        let parsed_text = parsed.to_string();
        assert!(
            parsed_text.contains("schema_version") && parsed_text.contains("99"),
            "{parsed_text}"
        );

        let weird = toml::from_str::<RawRoot>("schema_version = \"future\"\n").unwrap_err();
        assert!(weird.to_string().contains("schema_version"), "{weird}");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, &text).unwrap();
        let mut layers = ConfigLayers::new();
        layers
            .merge(&toml::from_str(&provider_toml("https://kept.example")).unwrap())
            .unwrap();
        let err = layers.merge_file(&path).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        let mut detail = err.to_string();
        let mut source = std::error::Error::source(&err);
        while let Some(next) = source {
            detail.push('\n');
            detail.push_str(&next.to_string());
            source = std::error::Error::source(next);
        }
        assert!(
            detail.contains("schema_version") && detail.contains("99"),
            "{detail}"
        );
        assert_eq!(
            layers.providers.get("anthropic").unwrap().base_url,
            "https://kept.example"
        );
        assert!(layers.profiles.is_empty());
        assert!(layers.model_rates.is_empty());
    }
}
