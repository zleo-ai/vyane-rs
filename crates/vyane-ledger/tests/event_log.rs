#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use vyane_ledger::{
    EventCategory, EventCursor, EventDurability, EventLog, EventLogError, EventSource, NewEvent,
};

fn event(owner: &str, event_type: &str) -> NewEvent {
    let mut event = NewEvent::new(
        owner,
        EventCategory::Lifecycle,
        event_type,
        EventSource::Daemon,
    );
    event.summary = Some(format!("event {event_type}"));
    event
}

fn owner_root(root: &Path, owner: &str) -> PathBuf {
    let digest = Sha256::digest(owner.as_bytes());
    let key = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join(key)
}

fn allocator_checksum(sequence: u64, file_len: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vyane.event-allocator.v1\0");
    digest.update(1_u32.to_le_bytes());
    digest.update(1_u32.to_le_bytes());
    digest.update(sequence.to_le_bytes());
    digest.update(file_len.to_le_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[tokio::test]
async fn append_and_replay_assign_monotonic_stream_sequences() {
    let directory = TempDir::new().unwrap();
    let log = EventLog::new(directory.path().join("events"));

    let first = log
        .append(
            "task-1",
            event("alice", "worker.started"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    let second = log
        .append(
            "task-1",
            event("alice", "worker.completed"),
            EventDurability::Buffered,
        )
        .await
        .unwrap();

    assert_eq!((first.sequence, second.sequence), (1, 2));
    let page = log.read_after("alice", "task-1", 0, 10).await.unwrap();
    assert_eq!(page.events, [first, second]);
    assert_eq!(page.skipped_lines, 0);
    assert_eq!(page.next_sequence, 2);
    let tail = log.read_after("alice", "task-1", 1, 10).await.unwrap();
    assert_eq!(tail.events.len(), 1);
    assert_eq!(tail.events[0].sequence, 2);
}

#[tokio::test]
async fn retrying_the_same_event_keeps_a_stable_deduplication_id() {
    let directory = TempDir::new().unwrap();
    let log = EventLog::new(directory.path().join("events"));
    let input = event("alice", "worker.started");
    let first = log
        .append("task-1", input.clone(), EventDurability::Durable)
        .await
        .unwrap();
    let retry = log
        .append("task-1", input, EventDurability::Durable)
        .await
        .unwrap();

    assert_eq!(first.event_id, retry.event_id);
    assert_eq!((first.sequence, retry.sequence), (1, 2));
}

#[tokio::test]
async fn replay_cursor_pages_without_rescanning_and_is_bound_to_stream() {
    let directory = TempDir::new().unwrap();
    let log = EventLog::new(directory.path().join("events"));
    for event_type in ["worker.started", "worker.progress", "worker.completed"] {
        log.append(
            "task-1",
            event("alice", event_type),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    }

    let first = log.read_after("alice", "task-1", 0, 1).await.unwrap();
    assert_eq!(first.events[0].sequence, 1);
    assert!(first.has_more);
    assert!(first.next_cursor.byte_offset > 0);
    assert!(!first.next_cursor.stream_digest.is_empty());

    let second = log
        .read_from("alice", "task-1", first.next_cursor.clone(), 1)
        .await
        .unwrap();
    assert_eq!(second.events[0].sequence, 2);
    assert!(second.has_more);
    let third = log
        .read_from("alice", "task-1", second.next_cursor, 1)
        .await
        .unwrap();
    assert_eq!(third.events[0].sequence, 3);
    assert!(!third.has_more);

    let cross_stream = log
        .read_from("bob", "task-1", first.next_cursor.clone(), 1)
        .await;
    assert!(matches!(cross_stream, Err(EventLogError::InvalidInput(_))));
    let mut mid_record = first.next_cursor;
    mid_record.byte_offset -= 1;
    let mid_record = log.read_from("alice", "task-1", mid_record, 1).await;
    assert!(matches!(mid_record, Err(EventLogError::InvalidInput(_))));

    let empty = log
        .read_from("alice", "missing", EventCursor::default(), 10)
        .await
        .unwrap();
    assert!(empty.events.is_empty());
    assert!(!empty.next_cursor.stream_digest.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_process_style_writers_allocate_each_sequence_once() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let first = EventLog::new(&root);
    let second = EventLog::new(&root);

    let writer = |log: EventLog, label: &'static str| async move {
        for index in 0..100_u16 {
            let mut event = event("alice", "tool.result");
            event.category = EventCategory::Tool;
            event.payload = BTreeMap::from([
                ("writer".into(), label.into()),
                ("index".into(), index.into()),
            ]);
            log.append("run-shared", event, EventDurability::Buffered)
                .await
                .unwrap();
        }
    };
    tokio::join!(writer(first, "a"), writer(second, "b"));

    let page = EventLog::new(&root)
        .read_after("alice", "run-shared", 0, 1_000)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 200);
    assert_eq!(page.skipped_lines, 0);
    assert_eq!(page.next_sequence, 200);
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.sequence)
            .collect::<BTreeSet<_>>(),
        (1..=200).collect()
    );
}

#[tokio::test]
async fn corrupt_mismatched_and_regressing_rows_are_skipped() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    let valid = log
        .append(
            "task-1",
            event("alice", "worker.started"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    let path = owner_root(&root, "alice").join("task-1.jsonl");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(b"not-json\n");
    let mut mismatched = valid.clone();
    mismatched.stream_id = "task-2".into();
    mismatched.sequence = 2;
    bytes.extend_from_slice(serde_json::to_string(&mismatched).unwrap().as_bytes());
    bytes.push(b'\n');
    let mut high = valid.clone();
    high.sequence = 3;
    bytes.extend_from_slice(serde_json::to_string(&high).unwrap().as_bytes());
    bytes.push(b'\n');
    bytes.extend_from_slice(serde_json::to_string(&valid).unwrap().as_bytes());
    bytes.push(b'\n');
    std::fs::write(&path, bytes).unwrap();

    let page = log.read_after("alice", "task-1", 0, 10).await.unwrap();

    assert_eq!(page.events, [valid, high]);
    assert_eq!(page.skipped_lines, 3);
    assert_eq!(page.next_sequence, 3);
    let recovered = log
        .append(
            "task-1",
            event("alice", "worker.recovered"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(recovered.sequence, 4);
}

#[tokio::test]
async fn partial_tail_is_not_visible_and_recovery_keeps_sequence_unique() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    log.append(
        "task-1",
        event("alice", "worker.started"),
        EventDurability::Durable,
    )
    .await
    .unwrap();
    let first = log.read_after("alice", "task-1", 0, 10).await.unwrap();
    let path = owner_root(&root, "alice").join("task-1.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    use std::io::Write as _;
    file.write_all(br#"{"schema":1"#).unwrap();
    file.flush().unwrap();

    let before_recovery = log.read_after("alice", "task-1", 0, 10).await.unwrap();
    assert_eq!(before_recovery.events.len(), 1);
    assert!(before_recovery.has_more);
    assert_eq!(before_recovery.next_cursor, first.next_cursor);

    let recovered = log
        .append(
            "task-1",
            event("alice", "worker.recovered"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(recovered.sequence, 2);
    let tail = log
        .read_from("alice", "task-1", first.next_cursor, 10)
        .await
        .unwrap();
    assert_eq!(tail.events, [recovered]);
    assert_eq!(tail.skipped_lines, 1);
    assert!(!tail.has_more);
}

#[tokio::test]
async fn newer_event_or_allocator_schema_fails_closed() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    log.append(
        "task-1",
        event("alice", "worker.started"),
        EventDurability::Durable,
    )
    .await
    .unwrap();
    let owner_root = owner_root(&root, "alice");
    let path = owner_root.join("task-1.jsonl");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(b"{\"schema\":2,\"future\":true}\n");
    std::fs::write(&path, bytes).unwrap();

    assert!(matches!(
        log.read_after("alice", "task-1", 0, 10).await,
        Err(EventLogError::UnsupportedSchema { found: 2, .. })
    ));
    assert!(matches!(
        log.append(
            "task-1",
            event("alice", "worker.progress"),
            EventDurability::Durable
        )
        .await,
        Err(EventLogError::UnsupportedSchema { found: 2, .. })
    ));

    std::fs::write(
        owner_root.join("task-1.lock"),
        b"{\"schema\":2,\"event_schema\":2,\"sequence\":1,\"file_len\":1,\"checksum\":\"future\",\"extra\":true}\n",
    )
    .unwrap();
    assert!(matches!(
        log.append(
            "task-1",
            event("alice", "worker.progress"),
            EventDurability::Durable
        )
        .await,
        Err(EventLogError::UnsupportedSchema { found: 2, .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_waits_for_writer_lock_before_observing_stream() {
    use fs4::fs_std::FileExt as _;

    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    log.append(
        "task-1",
        event("alice", "worker.started"),
        EventDurability::Durable,
    )
    .await
    .unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(owner_root(&root, "alice").join("task-1.lock"))
        .unwrap();
    assert!(lock.try_lock_exclusive().unwrap());

    let reader = tokio::spawn({
        let log = log.clone();
        async move { log.read_after("alice", "task-1", 0, 10).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!reader.is_finished());
    fs4::fs_std::FileExt::unlock(&lock).unwrap();
    assert_eq!(reader.await.unwrap().unwrap().events.len(), 1);
}

#[tokio::test]
async fn allocator_recovery_preserves_reserved_sequence_and_bounds_corruption() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    log.append(
        "task-1",
        event("alice", "worker.started"),
        EventDurability::Durable,
    )
    .await
    .unwrap();
    let lock_path = owner_root(&root, "alice").join("task-1.lock");
    let reserved = serde_json::json!({
        "schema": 1,
        "event_schema": 1,
        "sequence": 7,
        "file_len": 999,
        "checksum": allocator_checksum(7, 999),
    });
    std::fs::write(&lock_path, serde_json::to_vec(&reserved).unwrap()).unwrap();
    let after_reservation = log
        .append(
            "task-1",
            event("alice", "worker.recovered"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(after_reservation.sequence, 8);

    std::fs::write(&lock_path, vec![b'x'; 64 * 1024]).unwrap();
    let after_corruption = log
        .append(
            "task-1",
            event("alice", "worker.progress"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(after_corruption.sequence, 9);
}

#[tokio::test]
async fn page_byte_cap_advertises_more_and_cursor_reads_every_event() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    let mut seed = log
        .append(
            "task-1",
            event("alice", "worker.progress"),
            EventDurability::Buffered,
        )
        .await
        .unwrap();
    seed.payload
        .insert("metadata".into(), "x".repeat(15_000).into());
    let path = owner_root(&root, "alice").join("task-1.jsonl");
    let mut bytes = Vec::new();
    for sequence in 1..=600_u64 {
        seed.sequence = sequence;
        serde_json::to_writer(&mut bytes, &seed).unwrap();
        bytes.push(b'\n');
    }
    std::fs::write(path, bytes).unwrap();

    let first = log.read_after("alice", "task-1", 0, 1_000).await.unwrap();
    assert!(first.events.len() < 600);
    assert!(first.has_more);
    let first_len = first.events.len();
    let second = log
        .read_from("alice", "task-1", first.next_cursor, 1_000)
        .await
        .unwrap();
    assert_eq!(first_len + second.events.len(), 600);
    assert!(!second.has_more);
}

#[tokio::test]
async fn input_bounds_and_path_identity_fail_closed() {
    let directory = TempDir::new().unwrap();
    let log = EventLog::new(directory.path().join("events"));
    for stream in ["", "../escape", "slash/name", &"x".repeat(129)] {
        assert!(
            log.append(
                stream,
                event("alice", "worker.started"),
                EventDurability::Durable
            )
            .await
            .is_err()
        );
    }
    let mut oversized = event("alice", "tool.result");
    oversized
        .payload
        .insert("content".into(), "x".repeat(65_536).into());
    assert!(
        log.append("task-1", oversized, EventDurability::Durable)
            .await
            .is_err()
    );
    assert!(log.read_after("alice", "task-1", 0, 0).await.is_err());
}

#[tokio::test]
async fn identical_stream_ids_are_isolated_by_owner() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);

    let alice = log
        .append(
            "shared-stream",
            event("alice", "worker.started"),
            EventDurability::Durable,
        )
        .await
        .unwrap();
    let bob = log
        .append(
            "shared-stream",
            event("bob", "worker.started"),
            EventDurability::Durable,
        )
        .await
        .unwrap();

    assert_eq!((alice.sequence, bob.sequence), (1, 1));
    assert_eq!(
        log.read_after("alice", "shared-stream", 0, 10)
            .await
            .unwrap()
            .events,
        [alice]
    );
    assert_eq!(
        log.read_after("bob", "shared-stream", 0, 10)
            .await
            .unwrap()
            .events,
        [bob]
    );
    assert_ne!(owner_root(&root, "alice"), owner_root(&root, "bob"));
}

#[cfg(unix)]
#[tokio::test]
async fn event_directory_files_and_locks_are_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = TempDir::new().unwrap();
    let root = directory.path().join("events");
    let log = EventLog::new(&root);
    log.append(
        "task-1",
        event("alice", "worker.started"),
        EventDurability::Durable,
    )
    .await
    .unwrap();

    assert_eq!(
        std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let owner_root = owner_root(&root, "alice");
    assert_eq!(
        std::fs::metadata(&owner_root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for path in [
        owner_root.join("task-1.jsonl"),
        owner_root.join("task-1.lock"),
    ] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
