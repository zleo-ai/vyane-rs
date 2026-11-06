use serde_json::{Value, json};
use vyane_core::{ModelTokenRate, Usage, estimate_configured_cost};

pub(super) fn replay(input: &Value) -> Value {
    match input["op"].as_str().expect("usage op") {
        "estimate" => estimate(input),
        "saturating_add" => saturating_add(),
        other => panic!("unknown usage op {other}"),
    }
}

fn estimate(input: &Value) -> Value {
    let rate = if input["rate_present"].as_bool().expect("rate flag") {
        let mut rate = ModelTokenRate::per_1m(
            input["input_per_1m"].as_f64().expect("input rate"),
            input["output_per_1m"].as_f64().expect("output rate"),
        );
        if let Some(rate_value) = input["cache_read_per_1m"].as_f64() {
            rate = rate.with_cache_read(rate_value);
        }
        if let Some(rate_value) = input["reasoning_per_1m"].as_f64() {
            rate = rate.with_reasoning(rate_value);
        }
        Some(rate)
    } else {
        None
    };
    let usage = if input["usage_present"].as_bool().expect("usage flag") {
        Some(Usage {
            input_tokens: input["input_tokens"].as_u64().expect("input tokens"),
            output_tokens: input["output_tokens"].as_u64().expect("output tokens"),
            reasoning_tokens: input["reasoning_tokens"].as_u64(),
            cached_input_tokens: input["cached_input_tokens"].as_u64(),
        })
    } else {
        None
    };
    match estimate_configured_cost(rate.as_ref(), usage.as_ref()) {
        Some(cost) => json!({
            "micro_usd": (cost * 1_000_000.0).round() as i64,
            "status": "priced",
        }),
        None => json!({
            "micro_usd": Value::Null,
            "status": "unknown",
        }),
    }
}

fn saturating_add() -> Value {
    let mut total = Usage {
        input_tokens: u64::MAX,
        output_tokens: 0,
        reasoning_tokens: None,
        cached_input_tokens: None,
    };
    total.add(&Usage {
        input_tokens: 1,
        output_tokens: 0,
        reasoning_tokens: None,
        cached_input_tokens: None,
    });
    json!({
        "input": if total.input_tokens == u64::MAX {
            "saturated"
        } else {
            "wrapped"
        },
    })
}
