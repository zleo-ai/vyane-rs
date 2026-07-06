//! Round-trip and secret-safety guarantees from `docs/plan/WP-01.md`'s
//! acceptance list:
//!
//! - parse → in-memory model → serialize → parse yields an equal model, for
//!   providers and profiles.
//! - a resolved `Endpoint`/`AuthMaterial`/`Secret` can never be
//!   `serde`-serialized (the `Secret` type forbids it), guarding the "no key
//!   material persisted" invariant. This is enforced as a *compile-time* gate
//!   via the `static_assertions` macros below: if any of those types ever gains
//!   a `Serialize` impl, this file stops compiling. A runtime test cannot prove
//!   a negative ("this value is not serializable"), but the type system can.

use vyane_config::{ConfigLayers, RawRoot, ResolvedConfig};
use vyane_provider::Provider;

// Compile-time proof that no secret-bearing type implements `serde::Serialize`,
// so an accidental `serde_json::to_string(&secret)` (or `&AuthMaterial`, or
// `&Endpoint`) fails to *compile* rather than leaking key material into a
// persisted record at runtime. The macros exploit coherence: a `Serialize`
// impl would make the trait reference ambiguous, so adding it breaks the
// build. This is the real gate behind the "secrets never serialize"
// acceptance item — not a runtime test asserting its own non-serialization.
static_assertions::assert_not_impl_any!(vyane_core::Secret: serde::Serialize);
static_assertions::assert_not_impl_any!(vyane_core::AuthMaterial: serde::Serialize);
static_assertions::assert_not_impl_any!(vyane_core::Endpoint: serde::Serialize);

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

/// The resolved endpoint still carries the secret value (readable only via the
/// sanctioned `Secret::expose`) and redacts it in `Debug`. This pins the
/// runtime half of secret handling; the compile-time guarantee that the secret
/// can never be *serialized* is the `static_assertions` gate at the top of this
/// file, not this test.
#[test]
#[allow(clippy::unwrap_used)]
fn resolved_endpoint_carries_secret_with_redacted_debug() {
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
    // test). The fact that this secret can never reach a persisted record is
    // enforced by the absence of a `Serialize` impl — pinned at compile time
    // above, not here.
    assert_eq!(auth.secret.expose(), "sk-test-secret");
    assert_eq!(format!("{:?}", auth.secret), "Secret(***)");
}
