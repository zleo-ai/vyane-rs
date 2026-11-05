use std::fs;
use std::path::Path;

use serde_json::{json, Value};
use vyane_core::{BoundTarget, VyaneError};
use vyane_service::{load_config, resolve_target_chain, split_targets};

const NEUTRAL_CONFIG: &str = r#"
[providers.relay]
base_url = "https://relay.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "model-a"

[providers.backup]
base_url = "https://backup.example.invalid"
auth_style = "bearer"
protocol = "anthropic_messages"
default_model = "model-b"

[profiles.primary]
provider = "relay"
protocol = "openai_chat"
harness = "none"
model = "model-a"
failover = ["backup/model-b"]

[profiles.auto]
provider = "relay"
harness = "none"
model = "model-auto"

[profiles.effort-high]
provider = "relay"
harness = "none"
model = "model-a"

[profiles.effort-high.params]
effort = "high"
"#;

pub(super) fn replay(input: &Value, temp: &Path) -> Value {
    match input["op"].as_str().expect("selector op") {
        "resolve" => resolve(temp, input["selector"].as_str().expect("selector")),
        "split" => split(input["value"].as_str().expect("split value")),
        other => panic!("unknown selector op {other}"),
    }
}

fn resolve(temp: &Path, selector: &str) -> Value {
    let path = temp.join("neutral.toml");
    fs::write(&path, NEUTRAL_CONFIG).expect("write neutral config");
    let loaded = match load_config(Some(&path)) {
        Ok(loaded) => loaded,
        Err(_) => {
            return json!({
                "chain": Value::Null,
                "class": "config",
            });
        }
    };
    match resolve_target_chain(&loaded, selector) {
        Ok(chain) => json!({
            "chain": chain.iter().map(project).collect::<Vec<_>>(),
            "class": "ok",
        }),
        Err(error) => json!({
            "chain": Value::Null,
            "class": class_of(&error),
        }),
    }
}

fn split(value: &str) -> Value {
    match split_targets(value) {
        Ok(values) => json!({ "class": "ok", "values": values }),
        Err(error) => json!({
            "class": class_of(&error),
            "values": Value::Null,
        }),
    }
}

fn project(bound: &BoundTarget) -> Value {
    assert!(
        bound
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.auth.is_none()),
        "neutral config must not resolve credential material"
    );
    json!({
        "effort": bound.params.effort.map(|effort| effort.as_str()),
        "harness": bound
            .target
            .harness
            .as_ref()
            .map(|harness| harness.as_str())
            .unwrap_or("none"),
        "model": bound.target.model.as_str(),
        "protocol": bound.target.protocol.as_str(),
        "provider": bound.target.provider.as_str(),
        "transport": bound.transport.as_str(),
    })
}

fn class_of(error: &anyhow::Error) -> &'static str {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<VyaneError>())
        .map(|error| error.kind.as_str())
        .unwrap_or("selector_syntax")
}
