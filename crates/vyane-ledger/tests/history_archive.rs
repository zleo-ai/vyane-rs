//! Monthly history-archive fixtures. Rotation is explicit: nothing here is the
//! default `vyane history` read path, and [`JsonlLedger::append`] does not call it.

#![allow(clippy::unwrap_used)]

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tempfile::TempDir;
use vyane_core::{
    AdapterTransport, Ledger, ModelId, Protocol, ProviderId, RunQuery, RunRecord, RunStatus,
    Sandbox, Target,
};
use vyane_ledger::history_archive::{
    ArchivePlan, PendingRotation, RecentQuery, RecordRead, archive_directory, commit_pending,
    create_pending, install_kept_suffix, maybe_rotate, maybe_rotate_with_cap, pending_directory,
    plan_archive, query_recent, read_record_lines,
};
use vyane_ledger::{JsonlLedger, ROTATE_MAX_BYTES};

const SEP: &str = "2026-09-15T12:00:00Z";
const OCT: &str = "2026-10-02T12:00:00Z";

fn line(id: &str, started_at: &str) -> String {
    format!(r#"{{"run_id":"{id}","started_at":"{started_at}"}}"#)
}

fn rows(start: usize, count: usize, started_at: &str) -> Vec<String> {
    (start..start + count)
        .map(|index| line(&format!("r{index}"), started_at))
        .collect()
}

fn write_lines(path: &Path, lines: &[String]) {
    let mut body = String::new();
    for line in lines {
        body.push_str(line);
        body.push('\n');
    }
    fs::write(path, body).unwrap();
}

fn ids(path: &Path, since: Option<DateTime<Utc>>) -> Vec<String> {
    read_record_lines(
        path,
        &RecordRead {
            since,
            ..RecordRead::default()
        },
    )
    .unwrap()
    .into_iter()
    .filter_map(|line| run_id(&line))
    .collect()
}

fn run_id(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    value.get("run_id")?.as_str().map(str::to_string)
}

fn live_ids(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter_map(run_id)
        .collect()
}

fn gzip_text(path: &Path) -> String {
    let raw = fs::read(path).unwrap();
    let mut decoder = flate2::read::MultiGzDecoder::new(raw.as_slice());
    let mut text = String::new();
    decoder.read_to_string(&mut text).unwrap();
    text
}

fn write_gzip(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut encoder = flate2::GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), flate2::Compression::new(6));
    encoder.write_all(text.as_bytes()).unwrap();
    fs::write(path, encoder.finish().unwrap()).unwrap();
}

fn stamp(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn numbered(start: usize, count: usize) -> Vec<String> {
    (start..start + count)
        .map(|index| format!("r{index}"))
        .collect()
}

fn commit_without_replacing(path: &Path) -> (ArchivePlan, PendingRotation) {
    let raw = fs::read(path).unwrap();
    let plan = plan_archive(&raw).unwrap();
    let pending = create_pending(path, &plan.prefix, &plan.segments).unwrap();
    commit_pending(&pending, 0o644).unwrap();
    (plan, pending)
}

#[test]
fn rotation_moves_older_half_into_utc_month_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let mut lines = rows(0, 2, SEP);
    lines.extend(rows(2, 8, OCT));
    write_lines(&path, &lines);

    maybe_rotate_with_cap(&path, 1).unwrap();

    assert_eq!(
        live_ids(&path),
        numbered(5, 5),
        "live file keeps the newer half"
    );
    let archive = archive_directory(&path);
    let september = gzip_text(&archive.join("2026-09.jsonl.gz"));
    let october = gzip_text(&archive.join("2026-10.jsonl.gz"));
    assert!(september.contains("\"run_id\":\"r0\""));
    assert!(september.contains("\"run_id\":\"r1\""));
    assert!(!september.contains("\"run_id\":\"r2\""));
    for index in 2..5 {
        assert!(october.contains(&format!("\"run_id\":\"r{index}\"")));
    }
    assert!(!october.contains("\"run_id\":\"r5\""));
    assert_eq!(ids(&path, None), numbered(0, 10));
    assert!(!pending_directory(&path).exists());
}

#[test]
fn non_json_line_is_archived_not_deleted() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let mut body = String::from("not-json\n");
    for index in 0..3 {
        body.push_str(&line(&format!("r{index}"), OCT));
        body.push('\n');
    }
    fs::write(&path, &body).unwrap();

    maybe_rotate_with_cap(&path, 1).unwrap();

    let undated = gzip_text(&archive_directory(&path).join("undated.jsonl.gz"));
    assert!(undated.contains("not-json\n"));
    assert_eq!(ids(&path, None), vec!["r0", "r1", "r2"]);
    assert!(!fs::read_to_string(&path).unwrap().contains("not-json"));
}

#[test]
fn numeric_timestamp_without_started_at_is_undated() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let mut lines = vec![
        r#"{"run_id":"ts-only","ts":1757937600}"#.to_string(),
        line("r0", OCT),
    ];
    lines.extend(rows(1, 2, OCT));
    write_lines(&path, &lines);

    maybe_rotate_with_cap(&path, 1).unwrap();

    let archive = archive_directory(&path);
    let undated = gzip_text(&archive.join("undated.jsonl.gz"));
    assert!(undated.contains("\"ts\":1757937600"));
    assert!(undated.contains("\"run_id\":\"ts-only\""));
    assert!(archive.join("2026-10.jsonl.gz").is_file());
    let month_names: Vec<_> = fs::read_dir(&archive)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".jsonl.gz") && name != "undated.jsonl.gz")
        .collect();
    assert_eq!(month_names, vec!["2026-10.jsonl.gz".to_string()]);
}

#[test]
fn offset_started_at_uses_the_utc_month() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    // 00:30 at UTC+2 is 22:30 the previous UTC day, still September.
    let shifted = "2026-10-01T00:30:00+02:00";
    let mut lines = vec![line("early", shifted), line("early-2", shifted)];
    lines.extend(rows(0, 2, OCT));
    write_lines(&path, &lines);

    maybe_rotate_with_cap(&path, 1).unwrap();

    let september = gzip_text(&archive_directory(&path).join("2026-09.jsonl.gz"));
    assert!(september.contains("\"run_id\":\"early\""));
    assert!(september.contains("\"run_id\":\"early-2\""));
    assert!(!archive_directory(&path).join("2026-10.jsonl.gz").exists());
}

#[test]
fn interrupted_replace_retries_without_loss_or_duplication() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let lines = rows(0, 8, OCT);
    write_lines(&path, &lines);
    let original = fs::read(&path).unwrap();

    // Crash window: gzip and the committed-prefix marker are durable, and the
    // live file has not been replaced yet.
    let (_plan, pending) = commit_without_replacing(&path);
    assert_eq!(fs::read(&path).unwrap(), original);
    let october = archive_directory(&path).join("2026-10.jsonl.gz");
    let archived_once = fs::read(&october).unwrap();
    assert_eq!(gzip_text(&october).matches("\"run_id\":\"r0\"").count(), 1);
    assert!(pending.directory().is_dir());
    let stitched = read_record_lines(&path, &RecordRead::default()).unwrap();
    assert!(
        stitched.iter().all(|line| !line.contains("prefix_sha256")),
        "pending marker is not a ledger row"
    );
    assert_eq!(ids(&path, None), numbered(0, 8));

    maybe_rotate_with_cap(&path, 1).unwrap();

    assert_eq!(live_ids(&path), numbered(4, 4));
    assert_eq!(fs::read(&october).unwrap(), archived_once);
    assert_eq!(ids(&path, None), numbered(0, 8));
    assert!(!pending.directory().exists());
}

#[test]
fn commit_failure_leaves_live_file_and_retry_finishes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let lines = rows(0, 6, OCT);
    write_lines(&path, &lines);
    let before = fs::read(&path).unwrap();
    let gzip_path = archive_directory(&path).join("2026-10.jsonl.gz");
    fs::create_dir_all(&gzip_path).unwrap();

    assert!(
        maybe_rotate_with_cap(&path, 1).is_err(),
        "commit must fail while the gzip path is a directory"
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(gzip_path.is_dir());
    assert!(pending_directory(&path).is_dir());
    assert_eq!(ids(&path, None), numbered(0, 6));

    fs::remove_dir(&gzip_path).unwrap();
    maybe_rotate_with_cap(&path, 1).unwrap();

    assert_eq!(ids(&path, None), numbered(0, 6));
    assert_eq!(gzip_text(&gzip_path).matches('\n').count(), 3);
    assert!(!pending_directory(&path).exists());
}

#[test]
fn cleanup_failure_does_not_append_the_segment_twice() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    write_lines(&path, &rows(0, 4, OCT));
    let (_plan, pending) = commit_without_replacing(&path);
    install_kept_suffix(&path, &pending).unwrap();
    let october = archive_directory(&path).join("2026-10.jsonl.gz");
    let archived = fs::read(&october).unwrap();
    assert!(
        !fs::read_to_string(&path)
            .unwrap()
            .contains("\"run_id\":\"r0\"")
    );
    fs::create_dir(pending.directory().join("blocker")).unwrap();

    assert!(maybe_rotate_with_cap(&path, 1).is_err());
    assert_eq!(fs::read(&october).unwrap(), archived);
    assert!(pending.directory().is_dir());

    fs::remove_dir(pending.directory().join("blocker")).unwrap();
    maybe_rotate_with_cap(&path, 1).unwrap();

    assert_eq!(fs::read(&october).unwrap(), archived);
    assert_eq!(ids(&path, None), numbered(0, 4));
    assert!(!pending.directory().exists());
}

#[test]
fn recent_query_does_not_open_older_month_archives() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let archive = archive_directory(&path);
    let now = stamp("2026-10-01T12:00:00Z");
    let cutoff = now - chrono::TimeDelta::hours(24);
    write_gzip(
        &archive.join("2020-01.jsonl.gz"),
        &format!("{}\n", line("ancient", "2020-01-15T00:00:00Z")),
    );
    write_gzip(
        &archive.join("2026-09.jsonl.gz"),
        &format!("{}\n", line("rotated", "2026-09-30T18:00:00Z")),
    );
    write_lines(
        &path,
        &(0..4)
            .map(|index| line(&format!("new-{index}"), "2026-10-01T12:00:00Z"))
            .collect::<Vec<_>>(),
    );

    let found = query_recent(
        &path,
        &RecentQuery {
            limit: Some(2),
            since: Some(cutoff),
        },
    )
    .unwrap();
    assert_eq!(
        found
            .lines
            .iter()
            .filter_map(|line| run_id(line))
            .collect::<Vec<_>>(),
        vec!["new-3".to_string(), "new-2".to_string()]
    );
    assert!(
        found.opened_archives.is_empty(),
        "live file already held the limit, opened {:?}",
        found.opened_archives
    );

    // The same cutoff does not skip September by month name. A wider limit
    // therefore opens September and still does not open January.
    let wider = query_recent(
        &path,
        &RecentQuery {
            limit: Some(100),
            since: Some(cutoff),
        },
    )
    .unwrap();
    let opened = names(&wider.opened_archives);
    assert_eq!(opened, vec!["2026-09.jsonl.gz".to_string()]);
    assert!(wider.lines.iter().any(|line| line.contains("\"rotated\"")));
    assert!(wider.lines.iter().all(|line| !line.contains("ancient")));
}

#[test]
fn recent_query_stops_before_older_months() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let archive = archive_directory(&path);
    let now = "2026-10-26T12:00:00Z";
    write_gzip(
        &archive.join("2020-01.jsonl.gz"),
        &format!("{}\n", line("ancient", "2020-01-15T00:00:00Z")),
    );
    let october: String = (0..5)
        .map(|index| format!("{}\n", line(&format!("oct-{index}"), now)))
        .collect();
    write_gzip(&archive.join("2026-10.jsonl.gz"), &october);
    write_lines(&path, &[line("live", now)]);

    let found = query_recent(
        &path,
        &RecentQuery {
            limit: Some(3),
            since: None,
        },
    )
    .unwrap();
    assert_eq!(
        found
            .lines
            .iter()
            .filter_map(|line| run_id(line))
            .collect::<Vec<_>>(),
        vec!["live".to_string(), "oct-4".to_string(), "oct-3".to_string()]
    );
    assert_eq!(names(&found.opened_archives), vec!["2026-10.jsonl.gz"]);
    assert!(archive.join("2020-01.jsonl.gz").is_file());
}

#[test]
fn plain_jsonl_archive_is_read_with_gzip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let archive = archive_directory(&path);
    fs::create_dir_all(&archive).unwrap();
    fs::write(
        archive.join("2026-09.jsonl"),
        format!("{}\n", line("plain", SEP)),
    )
    .unwrap();
    write_gzip(
        &archive.join("2026-09.jsonl.gz"),
        &format!("{}\n", line("plain-gz", SEP)),
    );
    write_gzip(
        &archive.join("2026-10.jsonl.gz"),
        &format!("{}\n", line("gzipped", OCT)),
    );
    fs::write(archive.join("notes.jsonl"), "{\"run_id\":\"nope\"}\n").unwrap();
    fs::write(archive.join(".hidden.jsonl"), "{\"run_id\":\"hidden\"}\n").unwrap();
    write_lines(&path, &[line("live", OCT)]);

    assert_eq!(
        ids(&path, None),
        vec![
            "plain".to_string(),
            "plain-gz".to_string(),
            "gzipped".to_string(),
            "live".to_string()
        ]
    );
}

#[test]
fn identical_line_outside_the_rotation_overlap_is_kept() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let archive = archive_directory(&path);
    fs::create_dir_all(&archive).unwrap();
    let shared = line("same", SEP);
    fs::write(archive.join("2026-09.jsonl"), format!("{shared}\n")).unwrap();
    write_lines(&path, &[line("later", OCT), shared]);

    assert_eq!(
        ids(&path, None),
        vec!["same".to_string(), "later".to_string(), "same".to_string()]
    );
}

#[test]
fn kept_line_equal_to_last_archived_line_stays_visible() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let twin = line("twin", OCT);
    let lines = vec![line("r0", OCT), twin.clone(), twin, line("r3", OCT)];
    write_lines(&path, &lines);

    maybe_rotate_with_cap(&path, 1).unwrap();

    assert_eq!(live_ids(&path), vec!["twin".to_string(), "r3".to_string()]);
    assert!(!pending_directory(&path).exists());
    assert_eq!(
        ids(&path, None),
        vec![
            "r0".to_string(),
            "twin".to_string(),
            "twin".to_string(),
            "r3".to_string()
        ]
    );
}

#[test]
fn kept_rows_keep_their_bytes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let marker = "\u{2028}";
    let second =
        format!("{{\"run_id\":\"keep\",\"started_at\":\"{OCT}\",\"task\":\"a{marker}b\"}}\n");
    let body =
        format!("{{\"run_id\":\"old\",\"started_at\":\"{SEP}\",\"task\":\"drop\"}}\n{second}");
    fs::write(&path, &body).unwrap();

    maybe_rotate_with_cap(&path, 1).unwrap();

    assert_eq!(fs::read(&path).unwrap(), second.as_bytes());
    assert_eq!(
        ids(&path, None),
        vec!["old".to_string(), "keep".to_string()]
    );
}

#[test]
fn untrimmed_prefix_not_doubled_when_undated_archive_sorts_last() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let archive = archive_directory(&path);
    write_gzip(
        &archive.join("undated.jsonl.gz"),
        "{\"run_id\":\"old-undated\"}\n",
    );
    write_lines(&path, &rows(0, 8, OCT));
    commit_without_replacing(&path);

    assert_eq!(
        gzip_text(&archive.join("2026-10.jsonl.gz"))
            .matches("\"run_id\":\"r0\"")
            .count(),
        1
    );
    assert!(pending_directory(&path).is_dir());
    assert_eq!(
        ids(&path, None),
        vec![
            "r0".to_string(),
            "r1".to_string(),
            "r2".to_string(),
            "r3".to_string(),
            "old-undated".to_string(),
            "r4".to_string(),
            "r5".to_string(),
            "r6".to_string(),
            "r7".to_string(),
        ]
    );
}

#[test]
fn untrimmed_cross_month_prefix_not_doubled_when_since_skips_earlier_month() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let mut lines = rows(0, 2, SEP);
    lines.extend(rows(2, 8, OCT));
    write_lines(&path, &lines);
    commit_without_replacing(&path);

    let october_start = stamp("2026-10-01T00:00:00Z");
    assert_eq!(ids(&path, Some(october_start)), numbered(2, 8));
    assert_eq!(ids(&path, None), numbered(0, 10));
}

#[test]
fn second_rotation_appends_a_gzip_member() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    write_lines(&path, &rows(0, 4, OCT));
    maybe_rotate_with_cap(&path, 1).unwrap();
    let october = archive_directory(&path).join("2026-10.jsonl.gz");
    let first = fs::read(&october).unwrap();

    let mut extra = String::new();
    for line in rows(4, 2, OCT) {
        extra.push_str(&line);
        extra.push('\n');
    }
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(extra.as_bytes()).unwrap();
    maybe_rotate_with_cap(&path, 1).unwrap();

    let second = fs::read(&october).unwrap();
    assert!(
        second.starts_with(&first),
        "the previous gzip member stays at the front"
    );
    assert!(second.len() > first.len() + 2);
    assert_eq!(&second[first.len()..first.len() + 2], &[0x1f, 0x8b]);
    assert_eq!(gzip_text(&october).matches('\n').count(), 4);
    assert_eq!(ids(&path, None), numbered(0, 6));
}

#[test]
fn cap_is_exclusive_and_one_record_does_not_rotate() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    write_lines(&path, &rows(0, 4, OCT));
    let len = fs::metadata(&path).unwrap().len();
    maybe_rotate_with_cap(&path, len).unwrap();
    assert!(!archive_directory(&path).exists());
    maybe_rotate(&path).unwrap();
    assert!(
        !archive_directory(&path).exists(),
        "the default cap is {ROTATE_MAX_BYTES} bytes"
    );

    maybe_rotate_with_cap(&path, len - 1).unwrap();
    assert!(archive_directory(&path).join("2026-10.jsonl.gz").is_file());

    let single_dir = TempDir::new().unwrap();
    let single = single_dir.path().join("single.jsonl");
    let body = format!("{}\n", "x".repeat(64));
    fs::write(&single, &body).unwrap();
    maybe_rotate_with_cap(&single, 1).unwrap();
    assert!(!archive_directory(&single).exists());
    assert_eq!(fs::read(&single).unwrap(), body.into_bytes());
}

#[test]
fn incomplete_pending_is_replaced_and_corrupt_manifest_is_kept() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    write_lines(&path, &rows(0, 4, OCT));
    let pending = pending_directory(&path);
    fs::create_dir_all(&pending).unwrap();
    fs::write(pending.join("prefix.bin"), b"stale").unwrap();
    fs::write(pending.join("manifest.json"), "{\"complete\":false}\n").unwrap();

    maybe_rotate_with_cap(&path, 1).unwrap();
    assert_eq!(ids(&path, None), numbered(0, 4));
    assert!(!pending.exists());

    let broken_dir = TempDir::new().unwrap();
    let broken = broken_dir.path().join("broken.jsonl");
    write_lines(&broken, &rows(0, 4, OCT));
    let raw = fs::read(&broken).unwrap();
    let plan = plan_archive(&raw).unwrap();
    let pending = create_pending(&broken, &plan.prefix, &plan.segments).unwrap();
    let prefix_path = pending.directory().join("prefix.bin");
    let mut prefix = fs::read(&prefix_path).unwrap();
    prefix[0] ^= 0x01;
    fs::write(&prefix_path, &prefix).unwrap();

    assert!(maybe_rotate_with_cap(&broken, 1).is_err());
    assert_eq!(fs::read(&broken).unwrap(), raw);
    assert!(pending.directory().is_dir());
    assert!(!archive_directory(&broken).join("2026-10.jsonl.gz").exists());
}

#[cfg(unix)]
#[test]
fn symlink_live_file_is_replaced_and_archive_stays_beside_the_link() {
    let dir = TempDir::new().unwrap();
    let real_dir = dir.path().join("data");
    fs::create_dir(&real_dir).unwrap();
    let real = real_dir.join("ledger.jsonl");
    let link = dir.path().join("history.jsonl");
    write_lines(&real, &rows(0, 4, OCT));
    std::os::unix::fs::symlink(&real, &link).unwrap();

    maybe_rotate_with_cap(&link, 1).unwrap();

    assert!(archive_directory(&link).join("2026-10.jsonl.gz").is_file());
    assert!(!archive_directory(&real).exists());
    assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    assert_eq!(live_ids(&real), numbered(2, 2));
    assert_eq!(ids(&link, None), numbered(0, 4));
}

#[test]
fn mismatched_committed_marker_keeps_every_live_line() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    write_lines(&path, &rows(0, 4, OCT));
    let (_plan, pending) = commit_without_replacing(&path);
    let marker = pending.directory().join("committed.json");
    let mut text = fs::read_to_string(&marker).unwrap();
    text = text.replacen('a', "b", 1);
    fs::write(&marker, text).unwrap();

    assert_eq!(
        ids(&path, None),
        vec![
            "r0".to_string(),
            "r1".to_string(),
            "r0".to_string(),
            "r1".to_string(),
            "r2".to_string(),
            "r3".to_string(),
        ]
    );
}

#[test]
fn strict_read_rejects_a_missing_trailing_newline() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    fs::write(
        &path,
        r#"{"run_id":"r","started_at":"2026-10-02T12:00:00Z"}"#,
    )
    .unwrap();
    let err = read_record_lines(
        &path,
        &RecordRead {
            include_archives: false,
            strict: true,
            ..RecordRead::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("incomplete live history record"));
}

#[tokio::test]
async fn jsonl_append_does_not_rotate_an_oversized_ledger() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.jsonl");
    let mut body = Vec::new();
    body.extend_from_slice(b"{\"run_id\":\"seed\",\"started_at\":\"2026-10-02T12:00:00Z\"}\n");
    body.resize(
        body.len() + usize::try_from(ROTATE_MAX_BYTES).unwrap(),
        b'x',
    );
    body.push(b'\n');
    fs::write(&path, &body).unwrap();

    let ledger = JsonlLedger::new(&path);
    ledger.append(&sample_record("appended")).await.unwrap();

    assert!(!archive_directory(&path).exists());
    let text = fs::read_to_string(&path).unwrap();
    assert!(text.contains("\"run_id\":\"seed\""));
    assert!(text.contains("\"run_id\":\"appended\""));
    let found = ledger
        .query(RunQuery {
            limit: Some(1),
            ..RunQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(found[0].run_id, "appended");
}

fn names(paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect()
}

fn sample_record(run_id: &str) -> RunRecord {
    let started = stamp(OCT);
    RunRecord {
        run_id: run_id.to_string(),
        owner: "local".into(),
        started_at: started,
        finished_at: started,
        task_digest: "0123456789abcdef".into(),
        task_preview: None,
        workdir: None,
        sandbox: Sandbox::ReadOnly,
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o-mini"),
        },
        transport: AdapterTransport::DirectHttp,
        attempts: Vec::new(),
        status: RunStatus::Success,
        usage: None,
        cost_usd: None,
        session_id: None,
        output_chars: None,
        error: None,
        labels: Default::default(),
    }
}
