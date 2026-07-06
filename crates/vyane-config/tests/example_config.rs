//! Integration test: the repo's tracked `profiles.example.toml` is the
//! single source of truth for the config shape (per `docs/plan/WP-01.md`).
//! This test parses it and resolves every profile without error, so the
//! example can never silently drift from what the parser actually accepts.

use std::path::PathBuf;

fn example_toml_path() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<repo>/crates/vyane-config`; the example lives
    // at the repo root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../profiles.example.toml")
}

fn env_lookup_for_example(name: &str) -> Option<String> {
    // `profiles.example.toml` names these three env vars via `api_key_env`.
    // None of them need to hold a real key for *resolution* to succeed —
    // only that the named variable resolves to *some* value, proving the
    // indirection mechanism and the failover chain both work end to end.
    match name {
        "ANTHROPIC_API_KEY" | "OPENAI_API_KEY" | "SOME_RELAY_API_KEY" => {
            Some("example-test-value".to_string())
        }
        _ => None,
    }
}

#[test]
#[allow(clippy::unwrap_used)]
fn example_config_parses_and_every_profile_resolves() {
    let text = std::fs::read_to_string(example_toml_path())
        .expect("profiles.example.toml must be readable from the repo root");
    let root: vyane_config::RawRoot = toml::from_str(&text)
        .expect("profiles.example.toml must parse as the current config shape");

    let mut layers = vyane_config::ConfigLayers::new();
    layers
        .merge(&root)
        .expect("profiles.example.toml must merge without error");
    let config: vyane_config::ResolvedConfig = layers.into();

    let profile_names: Vec<&String> = config.profiles.keys().collect();
    assert!(
        !profile_names.is_empty(),
        "profiles.example.toml must define at least one profile"
    );

    for name in profile_names {
        config
            .resolve_profile_with(name, &env_lookup_for_example)
            .unwrap_or_else(|e| {
                panic!("profile `{name}` in profiles.example.toml must resolve: {e}")
            });
    }

    // The failover chain is the shape's most structurally interesting
    // profile — resolve it explicitly and check the chain length matches
    // the documented `failover` list (primary + 3 legs).
    let chain = config
        .resolve_failover_with("resilient-review", &env_lookup_for_example)
        .expect("resilient-review failover chain must resolve");
    assert_eq!(chain.len(), 4);
}
