#![allow(clippy::unwrap_used)]
use serde_json::json;
use vyane_core::RunRecord;
use vyane_service::HistoryStats;

fn records(costs: &[Option<f64>]) -> Vec<RunRecord> {
    let row: RunRecord = serde_json::from_str(
        include_str!("fixtures/history_cost.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    costs
        .iter()
        .map(|&cost| {
            let mut row = row.clone();
            row.cost_usd = cost;
            row
        })
        .collect()
}

#[test]
fn history_cost_rejects_nonfinite_and_negative_values_without_poisoning_known_sum() {
    let rows = records(&[
        Some(0.0),
        Some(1.25),
        None,
        Some(-1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ]);
    let body = serde_json::to_value(HistoryStats::from_records(&rows)).unwrap();
    assert_eq!(
        body["cost"],
        json!({"known":2,"unknown":1,"invalid":4,"known_total_usd":1.25,"overflowed":false})
    );
    assert_eq!(body["total"], 7);
    assert!(!body.to_string().contains("COST_PRIVATE_CANARY"));
}

#[test]
fn history_cost_overflow_remains_unknown_while_every_record_is_counted() {
    let rows = records(&[Some(f64::MAX), Some(f64::MAX), None, Some(-1.0), Some(0.0)]);
    let body = serde_json::to_value(HistoryStats::from_records(&rows)).unwrap();
    assert_eq!(
        body["cost"],
        json!({"known":3,"unknown":1,"invalid":1,"known_total_usd":null,"overflowed":true})
    );
}

#[test]
fn history_cost_distinguishes_empty_unknown_and_real_zero_without_rounding_small_values() {
    for (costs, expected) in [
        (
            vec![],
            json!({"known":0,"unknown":0,"invalid":0,"known_total_usd":null,"overflowed":false}),
        ),
        (
            vec![None, None],
            json!({"known":0,"unknown":2,"invalid":0,"known_total_usd":null,"overflowed":false}),
        ),
        (
            vec![Some(0.0), Some(-0.0), None],
            json!({"known":2,"unknown":1,"invalid":0,"known_total_usd":0.0,"overflowed":false}),
        ),
        (
            vec![Some(0.0000001)],
            json!({"known":1,"unknown":0,"invalid":0,"known_total_usd":0.0000001,"overflowed":false}),
        ),
    ] {
        assert_eq!(
            serde_json::to_value(HistoryStats::from_records(&records(&costs))).unwrap()["cost"],
            expected
        );
    }
}
