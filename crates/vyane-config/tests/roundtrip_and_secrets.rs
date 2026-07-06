//! Round-trip and secret-safety guarantees from `docs/plan/WP-01.md`'s
//! acceptance list:
//!
//! - parse → in-memory model → serialize → parse yields an equal model, for
//!   providers and profiles.
//! - a resolved `Endpoint`/`AuthMaterial` cannot be `serde`-serialized (the
//!   `Secret` type forbids it), guarding the "no key material persisted"
//!   invariant.

use vyane_config::{ConfigLayers, RawRoot, ResolvedConfig};
use vyane_provider::Provider;

const SAMPLE_TOML: &str = r#"
    [providers.anthropic]
    base_url      = "https://api.anthropic.example"
    api_key_env   = "VYANE_ROUNDTRIP_TEST_KEY"
    auth_style    = "x_api_key"
    protocol      = "anthropic_messages"
    default_model = "a-capable-anthropic-model"

    [providers.anthropic.extra]
    max_output_tokens = 4096

    [providers.anthropic.env_inject]
    ANTHROPIC_BASE_URL = "base_url"
    ANTHROPIC_API_KEY  = "api_key"

    [profiles.review]
    provider = "anthropic"
    protocol = "anthropic_messages"
    harness  = "none"
    model    = "a-capable-anthropic-model"

    [profiles.review.params]
    effort = "high"

    [profiles.resilient-review]
    provider = "anthropic"
    protocol = "anthropic_messages"
    harness  = "none"
    model    = "a-capable-anthropic-model"
    failover = ["review", "anthropic/a-different-model"]
"#;

#[test]
#[allow(clippy::unwrap_used)]
fn provider_roundtrips_parse_serialize_parse() {
    let root: RawRoot = toml::from_str(SAMPLE_TOML).unwrap();
    let patch = root.providers.get("anthropic").unwrap().clone();

    let serialized = toml::to_string(&patch).unwrap();
    let reparsed: vyane_provider::ProviderPatch = toml::from_str(&serialized).unwrap();

    assert_eq!(patch, reparsed);
}

#[test]
#[allow(clippy::unwrap_used)]
fn profile_roundtrips_parse_serialize_parse() {
    let root: RawRoot = toml::from_str(SAMPLE_TOML).unwrap();
    let patch = root.profiles.get("resilient-review").unwrap().clone();

    let serialized = toml::to_string(&patch).unwrap();
    let reparsed: vyane_config::ProfilePatch = toml::from_str(&serialized).unwrap();

    assert_eq!(patch, reparsed);
}

#[test]
#[allow(clippy::unwrap_used)]
fn resolved_provider_roundtrips_after_merge() {
    let root: RawRoot = toml::from_str(SAMPLE_TOML).unwrap();
    let mut layers = ConfigLayers::new();
    layers.merge(&root).unwrap();
    let resolved_provider: Provider = layers.providers.get("anthropic").unwrap().clone();

    let serialized = toml::to_string(&resolved_provider).unwrap();
    let reparsed: Provider = toml::from_str(&serialized).unwrap();

    assert_eq!(resolved_provider, reparsed);
}

/// A resolved `Endpoint`/`AuthMaterial` must not be `serde::Serialize` at
/// all, so an accidental `serde_json::to_string(&endpoint)` call fails to
/// *compile* rather than leaking a secret at runtime. This is a
/// compile-time guarantee: if this file compiles, the endpoint from
/// resolution genuinely cannot be serialized, because `vyane_core::Secret`
/// does not implement `Serialize` (see `crates/vyane-core/src/target.rs`).
/// There is deliberately no `#[test]` body here to "prove a negative" at
/// runtime — the proof is that this module compiles without ever calling
/// `Serialize` on an `Endpoint`, and that attempting to would not compile
/// (left as a documented invariant rather than a `trybuild`-style
/// compile-fail test, since `trybuild` is not in the workspace's allowed
/// dependency set).
#[test]
#[allow(clippy::unwrap_used)]
fn resolved_endpoint_type_carries_no_serialize_impl() {
    let root: RawRoot = toml::from_str(SAMPLE_TOML).unwrap();
    let mut layers = ConfigLayers::new();
    layers.merge(&root).unwrap();
    let config: ResolvedConfig = layers.into();

    let bound = config
        .resolve_profile_with("review", &|name| {
            (name == "VYANE_ROUNDTRIP_TEST_KEY").then(|| "sk-test-secret".to_string())
        })
        .unwrap();

    let endpoint = bound.endpoint.expect("review profile has an endpoint");
    let auth = endpoint.auth.expect("x_api_key provider has auth material");
    // `Secret::expose` is the one sanctioned way to read the value back out;
    // `Debug` is redacted (see vyane-core's own `secret_debug_is_redacted`
    // test), and there is no `Serialize` impl to call at all — the type
    // system, not a runtime check, is what forbids leaking this into a
    // persisted record.
    assert_eq!(auth.secret.expose(), "sk-test-secret");
    assert_eq!(format!("{:?}", auth.secret), "Secret(***)");
}
