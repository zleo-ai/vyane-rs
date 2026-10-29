#![allow(clippy::unwrap_used)]

use serde_json::json;
use vyane_core::{RunRecord, RunStatus};
use vyane_service::{
    BROADCAST_COMPARE_INSUFFICIENT_TEXT, BroadcastCompareInput, MAX_BROADCAST_COMPARE_TARGETS,
    compare_broadcast_results,
};

fn record(elapsed_ns: i64) -> RunRecord {
    let mut record: RunRecord = serde_json::from_value(json!({
        "run_id": "fixture",
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:00:00Z",
        "task_digest": "fixture",
        "sandbox": "read-only",
        "target": {"provider":"fixture","protocol":"openai_chat","harness":null,"model":"test"},
        "transport": "direct_http",
        "attempts": [],
        "status": "success"
    }))
    .unwrap();
    record.finished_at = record.started_at + chrono::Duration::nanoseconds(elapsed_ns);
    record
}

fn input<'a>(record: &'a RunRecord, output: &'a str) -> BroadcastCompareInput<'a> {
    BroadcastCompareInput {
        target: "same-target",
        record: Some(record),
        output: Some(output),
    }
}

#[test]
fn shared_entry_point_bounds_all_slots_before_filtering_and_accepts_exact_limit() {
    let record = record(0);
    let mut slots = vec![input(&record, "x"); MAX_BROADCAST_COMPARE_TARGETS];
    let report = compare_broadcast_results(&slots).unwrap();
    assert_eq!(report.target_count, 64);
    assert_eq!(report.pairwise_similarity.len(), 2016);
    assert!(
        report
            .pairwise_similarity
            .iter()
            .all(|pair| pair.similarity == Some(1.0))
    );
    assert_eq!(report.speed_ranking, (0..64).collect::<Vec<_>>());
    assert_eq!(report.per_target[63].index, 63);
    slots.push(BroadcastCompareInput {
        target: "failure",
        record: None,
        output: None,
    });
    let error = compare_broadcast_results(&slots).unwrap_err();
    assert_eq!(
        error.to_string(),
        "broadcast comparison supports at most 64 targets"
    );
}

#[test]
fn invalid_submillisecond_times_stay_null_and_equal_timings_use_input_order() {
    let records = [
        record(-1),
        record(2_000_000_000),
        record(0),
        record(2_000_000_000),
    ];
    let slots: Vec<_> = records.iter().map(|record| input(record, "a")).collect();
    let report = compare_broadcast_results(&slots).unwrap();
    assert!(report.per_target[0].duration_seconds.is_none());
    assert_eq!(report.per_target[1].duration_seconds, Some(2.0));
    assert_eq!(report.per_target[2].duration_seconds, Some(0.0));
    assert_eq!(report.speed_ranking, [2, 1, 3]);
    assert!(serde_json::to_value(report).unwrap()["per_target"][0]["duration_seconds"].is_null());
}

#[test]
fn unscorable_pairs_do_not_dilute_the_average_or_imply_identical_answers() {
    let record = record(0);
    let slots = [
        input(&record, "a b"),
        input(&record, "b c"),
        input(&record, "你好🦀"),
        input(&record, "!!!"),
    ];
    let report = compare_broadcast_results(&slots).unwrap();
    assert!(report.comparable);
    assert_eq!(report.pairwise_similarity[0].similarity, Some(0.333));
    assert!(
        report.pairwise_similarity[1..]
            .iter()
            .all(|pair| pair.similarity.is_none())
    );
    assert_eq!(report.average_similarity, Some(0.333));
    assert_eq!(report.per_target[2].char_count, 3);
    let unscorable = compare_broadcast_results(&slots[2..]).unwrap();
    assert!(unscorable.comparable);
    assert!(unscorable.average_similarity.is_none());
    assert!(unscorable.pairwise_similarity[0].similarity.is_none());
}

#[test]
fn missing_failed_and_blank_outputs_are_excluded_without_renumbering() {
    let record = record(0);
    let mut failed = record.clone();
    failed.status = RunStatus::Error;
    let mut cancelled = record.clone();
    cancelled.status = RunStatus::Cancelled;
    let mut timeout = record.clone();
    timeout.status = RunStatus::Timeout;
    let slots = [
        input(&failed, "a"),
        input(&cancelled, "a"),
        input(&timeout, "a"),
        BroadcastCompareInput {
            record: None,
            ..input(&record, "a")
        },
        BroadcastCompareInput {
            output: None,
            ..input(&record, "a")
        },
        input(&record, ""),
        input(&record, " \t\n"),
        input(&record, "a"),
    ];
    let report = compare_broadcast_results(&slots).unwrap();
    assert!(!report.comparable);
    assert_eq!(report.reason, Some(BROADCAST_COMPARE_INSUFFICIENT_TEXT));
    assert_eq!(report.target_count, 1);
    assert_eq!(report.per_target[0].index, 7);
    assert_eq!(report.speed_ranking, [7]);
    assert!(report.average_similarity.is_none());
    let empty = compare_broadcast_results(&[]).unwrap();
    assert!(!empty.comparable);
    assert_eq!(empty.reason, report.reason);
    assert!(empty.per_target.is_empty());
}

#[test]
fn term_lists_are_capped_and_frequency_ties_follow_first_occurrence() {
    let record = record(0);
    let slots = [
        input(
            &record,
            "a bb ccc dddd eeeee ffffff ggggggg hhhhhhhh iiiiiiiii jjjjjjjjjj kkkkkkkkkkk A BB",
        ),
        input(&record, "shared"),
    ];
    let report = compare_broadcast_results(&slots).unwrap();
    let metrics = &report.per_target[0];
    assert_eq!(metrics.word_count, 13);
    assert_eq!(metrics.top_terms.len(), 10);
    assert_eq!(&metrics.top_terms[..3], ["a", "bb", "ccc"]);
    assert_eq!(metrics.unique_terms.len(), 10);
    assert_eq!(metrics.unique_terms[0], "kkkkkkkkkkk");
    assert_eq!(metrics.unique_terms[9], "bb");
    assert_eq!(report.pairwise_similarity[0].similarity, Some(0.0));
    assert_eq!(report.average_similarity, Some(0.0));
    let unicode_lower = compare_broadcast_results(&[input(&record, "İ K A_1 １２ 中文")]).unwrap();
    assert_eq!(unicode_lower.per_target[0].top_terms, ["i", "k", "a", "1"]);
    assert_eq!(unicode_lower.per_target[0].word_count, 4);
    assert_eq!(unicode_lower.per_target[0].char_count, 13);
}
