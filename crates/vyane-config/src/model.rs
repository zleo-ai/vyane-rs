//! The raw TOML config shape, as it parses directly out of any single layer
//! (built-in defaults, user file, project file). Every field a layer might
//! reasonably omit is `Option`, so a higher layer can override just one
//! field of a profile or provider without restating the rest — the merge
//! step in [`crate::layer`] is what turns a stack of these into one
//! complete [`crate::resolve::ResolvedConfig`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use vyane_core::{Effort, ErrorKind, ModelId, Protocol, Sandbox, VyaneError};
use vyane_provider::ProviderPatch;

/// One parsed config layer: `[providers.*]` and `[profiles.*]` tables.
///
/// `cost` and other v0.1-future sections (see `profiles.example.toml`) are
/// intentionally not modeled here — parsing them is out of this work
/// package's scope, and `#[serde(deny_unknown_fields)]` is deliberately
/// *not* set so unrelated sections don't break parsing of the sections this
/// crate does own.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RawRoot {
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderPatch>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfilePatch>,
}

/// A generation-parameters patch, mirroring `vyane_core::GenParams` but with
/// every field optional so a layer can set just one.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenParamsPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl GenParamsPatch {
    /// Apply this patch's `Some` fields onto `base`, leaving fields it left
    /// unset untouched. `extra` entries are merged key-by-key rather than
    /// wholesale-replaced — the same sub-map merge `Provider::apply_patch` uses
    /// for its `extra`, matching the "override per field" precedence rule
    /// applied to every field throughout this crate.
    pub fn apply_over(&self, base: &mut vyane_core::GenParams) {
        if let Some(v) = self.temperature {
            base.temperature = Some(v);
        }
        if let Some(v) = self.top_p {
            base.top_p = Some(v);
        }
        if let Some(v) = self.max_output_tokens {
            base.max_output_tokens = Some(v);
        }
        if let Some(v) = self.effort {
            base.effort = Some(v);
        }
        for (k, v) in &self.extra {
            base.extra.insert(k.clone(), v.clone());
        }
    }
}

/// An element of a `[profiles.<name>].failover` list: either the name of
/// another profile, or an explicit `provider/model` pair. Parsed from the
/// plain string form (`"review"` or `"openai/gpt-x"`) — see
/// [`RawFailoverElement::parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawFailoverElement {
    ProfileName(String),
    ProviderModel { provider: String, model: ModelId },
}

impl RawFailoverElement {
    /// A `provider/model` pair is distinguished from a bare profile name by
    /// containing exactly one `/`. Profile names are free-form identifiers
    /// and are never required to avoid `/`, so this is a syntax convention
    /// documented in `profiles.example.toml`, not a strict grammar — a
    /// string containing a `/` is always treated as `provider/model`.
    ///
    /// A `/` with an empty provider (`"/gpt"`) or empty model (`"openai/"`)
    /// is a clear syntax error rather than a bare profile name: the author
    /// clearly intended a `provider/model` pair but left one side blank.
    pub fn parse(raw: &str) -> vyane_core::Result<Self> {
        match raw.split_once('/') {
            None => Ok(RawFailoverElement::ProfileName(raw.to_string())),
            Some((provider, model)) if !provider.is_empty() && !model.is_empty() => {
                Ok(RawFailoverElement::ProviderModel {
                    provider: provider.to_string(),
                    model: ModelId::new(model),
                })
            }
            Some((provider, model)) => Err(VyaneError::new(
                ErrorKind::Config,
                format!(
                    "invalid failover element `{raw}`: a `provider/model` pair needs a non-empty \
                     provider and model (got provider=`{provider}`, model=`{model}`)"
                ),
            )),
        }
    }
}

impl Serialize for RawFailoverElement {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let raw = match self {
            RawFailoverElement::ProfileName(name) => name.clone(),
            RawFailoverElement::ProviderModel { provider, model } => {
                format!("{provider}/{}", model.as_str())
            }
        };
        s.serialize_str(&raw)
    }
}

impl<'de> Deserialize<'de> for RawFailoverElement {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        RawFailoverElement::parse(&raw).map_err(<D::Error as serde::de::Error>::custom)
    }
}

/// A profile patch: every field optional, mirroring `[profiles.<name>]`.
/// Higher layers override a profile per-field the same way provider patches
/// do (see `vyane_provider::ProviderPatch`); the failover list, when
/// present, replaces the whole list rather than element-merging (an ordered
/// chain has no natural per-element merge key).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProfilePatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    /// `"none"` (or the field omitted entirely) ⇒ direct HTTP, no harness.
    /// Any other value names a harness kind (`vyane_core::HarnessKind`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<Sandbox>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<GenParamsPatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failover: Option<Vec<RawFailoverElement>>,
    /// Routing tier: `"economy"`, `"mainline"`, or `"frontier"`. Used by
    /// `vyane-router` to classify this profile when routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    /// Routing tags for this profile (e.g. `["frontend", "code"]`). Used by
    /// `vyane-router` for tag-based preference resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Routing stage for this profile (e.g. `"plan"`, `"review"`, `"architecture"`).
    /// Used by `vyane-router` for stage-based preference resolution (highest
    /// precedence after explicit tier override).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
}

impl ProfilePatch {
    /// Apply this patch's `Some` fields onto `base`, per field.
    pub fn apply_over(&self, base: &mut ProfilePatch) {
        if self.provider.is_some() {
            base.provider = self.provider.clone();
        }
        if self.protocol.is_some() {
            base.protocol = self.protocol;
        }
        if self.harness.is_some() {
            base.harness = self.harness.clone();
        }
        if self.model.is_some() {
            base.model = self.model.clone();
        }
        if self.sandbox.is_some() {
            base.sandbox = self.sandbox;
        }
        match (&self.params, &mut base.params) {
            (Some(patch), Some(existing)) => {
                if patch.temperature.is_some() {
                    existing.temperature = patch.temperature;
                }
                if patch.top_p.is_some() {
                    existing.top_p = patch.top_p;
                }
                if patch.max_output_tokens.is_some() {
                    existing.max_output_tokens = patch.max_output_tokens;
                }
                if patch.effort.is_some() {
                    existing.effort = patch.effort;
                }
                for (k, v) in &patch.extra {
                    existing.extra.insert(k.clone(), v.clone());
                }
            }
            (Some(patch), None) => base.params = Some(patch.clone()),
            (None, _) => {}
        }
        if self.failover.is_some() {
            base.failover = self.failover.clone();
        }
        if self.tier.is_some() {
            base.tier = self.tier.clone();
        }
        if self.tags.is_some() {
            base.tags = self.tags.clone();
        }
        if self.stage.is_some() {
            base.stage = self.stage.clone();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn failover_element_parses_provider_model_pair() {
        let parsed = RawFailoverElement::parse("openai/gpt-fast").unwrap();
        assert_eq!(
            parsed,
            RawFailoverElement::ProviderModel {
                provider: "openai".to_string(),
                model: ModelId::new("gpt-fast"),
            }
        );
    }

    #[test]
    fn failover_element_parses_bare_profile_name() {
        let parsed = RawFailoverElement::parse("review").unwrap();
        assert_eq!(
            parsed,
            RawFailoverElement::ProfileName("review".to_string())
        );
    }

    #[test]
    fn failover_element_empty_model_is_a_syntax_error() {
        // `"openai/"` must surface as a clear syntax error rather than fall
        // through to a (nonsensical) bare profile name.
        let err = RawFailoverElement::parse("openai/").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(
            err.message.contains("openai/"),
            "error must name the offending element: {}",
            err.message
        );
    }

    #[test]
    fn failover_element_empty_provider_is_a_syntax_error() {
        // `"/gpt"` likewise must not become a bare profile name.
        let err = RawFailoverElement::parse("/gpt").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(
            err.message.contains("/gpt"),
            "error must name the offending element: {}",
            err.message
        );
    }

    #[test]
    fn failover_element_roundtrips_inside_a_profile_list() {
        // `toml::to_string` requires a top-level table, so exercise the
        // roundtrip the way it actually appears in config: as an element of
        // a `failover` list inside a profile.
        let toml_src = r#"
            [profiles.p]
            provider = "anthropic"
            protocol = "anthropic_messages"
            model    = "m"
            failover = ["review", "openai/gpt-fast"]
        "#;
        let root: RawRoot = toml::from_str(toml_src).unwrap();
        let profile = root.profiles.get("p").unwrap();
        let failover = profile.failover.as_ref().unwrap();
        assert_eq!(
            failover,
            &vec![
                RawFailoverElement::ProfileName("review".to_string()),
                RawFailoverElement::ProviderModel {
                    provider: "openai".to_string(),
                    model: ModelId::new("gpt-fast"),
                },
            ]
        );

        let serialized = toml::to_string(profile).unwrap();
        let reparsed: ProfilePatch = toml::from_str(&serialized).unwrap();
        assert_eq!(&reparsed, profile);
    }

    #[test]
    fn raw_root_parses_minimal_toml() {
        let toml_src = r#"
            [providers.anthropic]
            base_url = "https://api.anthropic.example"
            api_key_env = "ANTHROPIC_API_KEY"
            auth_style = "x_api_key"
            protocol = "anthropic_messages"
            default_model = "a-capable-anthropic-model"

            [profiles.review]
            provider = "anthropic"
            protocol = "anthropic_messages"
            harness = "none"
            model = "a-capable-anthropic-model"
        "#;
        let root: RawRoot = toml::from_str(toml_src).unwrap();
        assert!(root.providers.contains_key("anthropic"));
        assert!(root.profiles.contains_key("review"));
    }
}
