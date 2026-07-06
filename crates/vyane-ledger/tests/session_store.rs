//! Acceptance tests for [`vyane_ledger::FsSessionStore`].
//!
//! Missing-session load, save/load round-trip, owner-filtered listing, and the
//! atomic-rename guarantee that a concurrent reader never observes a partial
//! file.

#![allow(clippy::unwrap_used)]

use chrono::Utc;
use tempfile::TempDir;
use vyane_core::{ChatMessage, ModelId, Protocol, ProviderId, SessionRecord, SessionStore, Target};
use vyane_ledger::FsSessionStore;

fn session(id: &str, owner: &str) -> SessionRecord {
    let now = Utc::now();
    SessionRecord {
        session_id: id.to_string(),
        owner: owner.into(),
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o-mini"),
        },
        native_session_id: Some("native-xyz".into()),
        transcript: vec![ChatMessage::user("hello")],
        created_at: now,
        updated_at: now,
        run_count: 1,
    }
}

#[tokio::test]
async fn load_missing_session_returns_none() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    assert!(store.load("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn save_then_load_roundtrips() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let rec = session("s1", "local");
    store.save(&rec).await.unwrap();

    let back = store
        .load("s1")
        .await
        .unwrap()
        .expect("session should exist");
    assert_eq!(back.session_id, "s1");
    assert_eq!(back.owner, "local");
    assert_eq!(back.native_session_id.as_deref(), Some("native-xyz"));
    assert_eq!(back.transcript.len(), 1);
    assert_eq!(back.transcript[0].content, "hello");
    assert_eq!(back.run_count, 1);
    assert_eq!(back.target.model.as_str(), "gpt-4o-mini");
}

#[tokio::test]
async fn save_overwrites_existing_session() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let mut rec = session("s1", "local");
    rec.run_count = 1;
    store.save(&rec).await.unwrap();

    rec.run_count = 7;
    store.save(&rec).await.unwrap();

    let back = store.load("s1").await.unwrap().unwrap();
    assert_eq!(back.run_count, 7);
}

#[tokio::test]
async fn list_returns_all_when_no_owner_filter() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    store.save(&session("s1", "alice")).await.unwrap();
    store.save(&session("s2", "bob")).await.unwrap();
    store.save(&session("s3", "alice")).await.unwrap();

    let all = store.list(None).await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn list_filters_by_owner() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    store.save(&session("s1", "alice")).await.unwrap();
    store.save(&session("s2", "bob")).await.unwrap();
    store.save(&session("s3", "alice")).await.unwrap();

    let alice = store.list(Some("alice")).await.unwrap();
    assert_eq!(alice.len(), 2);
    assert!(alice.iter().all(|s| s.owner == "alice"));

    let bob = store.list(Some("bob")).await.unwrap();
    assert_eq!(bob.len(), 1);
    assert_eq!(bob[0].session_id, "s2");
}

#[tokio::test]
async fn list_on_missing_dir_returns_empty() {
    // A store directory that was never created is an empty store, not an error.
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path().join("does-not-exist"));
    assert!(store.list(None).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reader_never_sees_partial_file() {
    // Hammer save and load concurrently. Because saves publish via atomic
    // rename, every load must return either None (before the first save) or a
    // fully-deserializable SessionRecord — never a truncated/partial file. Any
    // partial read would surface as a load error or a mismatched transcript.
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    // Seed with a known-good transcript so every subsequent save has the same
    // expected shape; we verify the transcript is always complete.
    let marker = "the-quick-brown-fox".to_string();
    store
        .save(&session_with_marker("sess", "local", &marker))
        .await
        .unwrap();

    let writers = (0..4).map(|_| {
        let store = store.dir().to_path_buf();
        let marker = marker.clone();
        tokio::spawn(async move {
            let s = FsSessionStore::new(store);
            for i in 0..50 {
                let mut rec = session_with_marker("sess", "local", &marker);
                rec.run_count = i;
                s.save(&rec).await.unwrap();
            }
        })
    });

    let reader_dir = store.dir().to_path_buf();
    let marker_clone = marker.clone();
    let reader = tokio::spawn(async move {
        let s = FsSessionStore::new(reader_dir);
        for _ in 0..200 {
            // Every successful load must carry the complete marker transcript.
            if let Some(rec) = s.load("sess").await.unwrap() {
                assert_eq!(
                    rec.transcript.len(),
                    1,
                    "partial file observed: transcript truncated"
                );
                assert_eq!(
                    rec.transcript[0].content, marker_clone,
                    "partial file observed: transcript content mismatch"
                );
            }
        }
    });

    for w in writers {
        w.await.unwrap();
    }
    reader.await.unwrap();
}

/// Helper that builds a session whose single transcript message is `marker`,
/// so the concurrent-reader test can assert the full body is always present.
fn session_with_marker(id: &str, owner: &str, marker: &str) -> SessionRecord {
    let now = Utc::now();
    SessionRecord {
        session_id: id.to_string(),
        owner: owner.into(),
        target: Target {
            provider: ProviderId::new("openai"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("gpt-4o-mini"),
        },
        native_session_id: None,
        transcript: vec![ChatMessage::user(marker)],
        created_at: now,
        updated_at: now,
        run_count: 0,
    }
}

#[tokio::test]
async fn no_temp_files_left_behind() {
    // After a clean save, the directory should contain only the final file —
    // no lingering `.tmp` artifacts from the atomic-rename write.
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store.save(&session("s1", "local")).await.unwrap();

    let entries: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries, vec!["s1.json".to_string()]);
}
