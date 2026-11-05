use std::fs;
use std::path::Path;

use serde_json::{json, Value};
use vyane_core::Effort;
use vyane_router::{route_task, ComplexitySignals, RoutePreferenceTable, RouteTargetPreference};
use vyane_service::load_config;

const CLOSED: &[&str] = &["low", "medium", "high", "xhigh"];

pub(super) fn replay(input: &Value, temp: &Path) -> Value {
    match input["op"].as_str().expect("effort op") {
        "serde_closed" => {
            let mut out = serde_json::Map::new();
            for token in CLOSED {
                let parsed: Effort =
                    serde_json::from_str(&format!("\"{token}\"")).expect("closed effort token");
                out.insert((*token).to_string(), json!(parsed.as_str()));
            }
            Value::Object(out)
        }
        "serde_open" => {
            let token = input["token"].as_str().expect("token");
            let parsed = serde_json::from_str::<Effort>(&format!("\"{token}\""));
            json!({ "accepted": parsed.is_ok() })
        }
        "route" => {
            let decision = decide(
                input["preference_effort"]
                    .as_str()
                    .expect("preference effort"),
                input["explicit_tier"].as_str().expect("tier"),
            );
            json!({
                "effort": decision.effort.as_str(),
                "tier": decision.tier.as_str(),
            })
        }
        "inherit" => json!({
            "economy": decide("", "economy").effort.as_str(),
            "frontier": decide("", "frontier").effort.as_str(),
            "mainline": decide("", "mainline").effort.as_str(),
        }),
        "toml_max" => toml_max(temp),
        other => panic!("unknown effort op {other}"),
    }
}

fn decide(preference_effort: &str, explicit_tier: &str) -> vyane_router::RouteDecision {
    let mut signals = ComplexitySignals::new();
    signals.explicit_tier = Some(explicit_tier.to_string());
    let mut table = RoutePreferenceTable::default();
    table.default = Some(RouteTargetPreference {
        selection_key: String::new(),
        provider: "relay".to_string(),
        model: "model-a".to_string(),
        effort: preference_effort.to_string(),
        tier: String::new(),
    });
    let available = vec!["relay".to_string()];
    route_task("ping", &available, Some(&signals), Some(&table), "relay")
}

fn toml_max(temp: &Path) -> Value {
    let path = temp.join("effort.toml");
    fs::write(
        &path,
        r#"
[providers.relay]
base_url = "https://relay.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "model-a"

[profiles.effort-max]
provider = "relay"
harness = "none"
model = "model-a"

[profiles.effort-max.params]
effort = "max"
"#,
    )
    .expect("write effort config");
    match load_config(Some(&path)) {
        Ok(_) => json!({ "class": "ok", "loaded": true }),
        Err(_) => json!({ "class": "config", "loaded": false }),
    }
}
