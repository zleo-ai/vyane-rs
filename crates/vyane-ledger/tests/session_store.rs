//! Acceptance tests for [`vyane_ledger::FsSessionStore`].
//!
//! Missing-session load, save/load round-trip, owner-filtered listing, and the
//! atomic-rename guarantee that a concurrent reader never observes a partial
//! file.

#![allow(clippy::unwrap_used)]

use chrono::Utc;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use vyane_core::{
    ChatMessage, ErrorKind, HarnessKind, ModelId, NativeSessionBinding, NativeSessionDomain,
    NativeSessionState, NativeSessionTransition, Protocol, ProviderId, SessionRecord, SessionStore,
    SessionUpdate, Target, WorkdirIdentity,
};
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

fn update(id: &str, owner: &str, marker: &str) -> SessionUpdate {
    let seed = session(id, owner);
    SessionUpdate {
        owner: owner.into(),
        session_id: id.into(),
        target: seed.target,
        native_session_id: None,
        transcript_delta: vec![
            ChatMessage::user(format!("question-{marker}")),
            ChatMessage::assistant(format!("answer-{marker}")),
        ],
        occurred_at: Utc::now(),
    }
}

fn native_target() -> Target {
    Target {
        provider: ProviderId::new("provider-a"),
        protocol: Protocol::OpenaiResponses,
        harness: Some(HarnessKind::CodexCli),
        model: ModelId::new("model-a"),
    }
}

fn native_update(id: &str, owner: &str, marker: &str) -> SessionUpdate {
    SessionUpdate {
        owner: owner.into(),
        session_id: id.into(),
        target: native_target(),
        native_session_id: None,
        transcript_delta: vec![ChatMessage::assistant(format!("native-{marker}"))],
        occurred_at: Utc::now(),
    }
}

fn digest(marker: &str) -> String {
    Sha256::digest(marker.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn binding(native_session_id: &str, marker: &str) -> NativeSessionBinding {
    NativeSessionBinding {
        native_session_id: native_session_id.into(),
        domain: NativeSessionDomain {
            runtime: format!("native-runtime-{marker}"),
            harness: HarnessKind::CodexCli,
            provider: ProviderId::new("provider-a"),
            protocol: Protocol::OpenaiResponses,
            model: ModelId::new("model-a"),
            endpoint_routing_digest: digest(&format!("endpoint-{marker}")),
            canonical_workdir: "/workspace".into(),
            workdir_identity: WorkdirIdentity {
                device: 11,
                inode: 22,
            },
            checkpoint_namespace: "native-checkpoint-v1".into(),
            checkpoint_schema: 1,
            account_scope_digest: digest(&format!("account-{marker}")),
            runtime_scope_digest: digest(&format!("runtime-{marker}")),
        },
    }
}

fn owner_dir(root: &std::path::Path, owner: &str) -> std::path::PathBuf {
    let key = Sha256::digest(owner.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join(key)
}

fn json_files(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().and_then(|extension| extension.to_str()) == Some("json"))
                .then_some(path)
        })
        .collect()
}

#[tokio::test]
async fn load_missing_session_returns_none() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    assert!(store.load("local", "nope").await.unwrap().is_none());
}

#[tokio::test]
async fn save_then_load_roundtrips() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let rec = session("s1", "local");
    store.save("local", &rec).await.unwrap();

    let back = store
        .load("local", "s1")
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
async fn plain_legacy_native_id_loads_only_as_legacy_unbound_revision_zero() {
    let dir = TempDir::new().unwrap();
    let legacy = session("legacy", "alice");
    std::fs::write(
        dir.path().join("legacy.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();
    let store = FsSessionStore::new(dir.path());

    let snapshot = store
        .load_snapshot("alice", "legacy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.session_revision, 0);
    assert!(matches!(
        snapshot.native_session,
        NativeSessionState::LegacyUnbound { native_session_id }
            if native_session_id == "native-xyz"
    ));
    assert_eq!(
        snapshot.record.native_session_id.as_deref(),
        Some("native-xyz")
    );
}

#[tokio::test]
async fn native_commit_publishes_binding_and_logical_update_together() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let expected_binding = binding("native-a", "a");

    let snapshot = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::Commit {
                expected_revision: 0,
                update: native_update("native", "alice", "first"),
                binding: expected_binding.clone(),
            },
        )
        .await
        .unwrap();

    assert_eq!(snapshot.session_revision, 1);
    assert_eq!(snapshot.record.run_count, 1);
    assert_eq!(snapshot.record.native_session_id, None);
    assert!(matches!(
        snapshot.native_session,
        NativeSessionState::Bound { binding } if *binding == expected_binding
    ));
    let disk = std::fs::read(json_files(&owner_dir(dir.path(), "alice"))[0].clone()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&disk).unwrap();
    assert_eq!(value["schema"], 2);
    assert_eq!(value["session_revision"], 1);
    assert_eq!(value["native_session"]["state"], "bound");
    assert!(value.get("session_id").is_none());
    assert_eq!(
        value["session"]["native_session_id"],
        serde_json::Value::Null
    );

    // A pre-V2 reader cannot silently ignore revision/binding authority.
    assert!(serde_json::from_slice::<SessionRecord>(&disk).is_err());
    assert_eq!(
        store.load("alice", "native").await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    let snapshots = store.list_snapshots("alice").await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].session_revision, 1);
    assert!(matches!(
        &snapshots[0].native_session,
        NativeSessionState::Bound { binding } if **binding == expected_binding
    ));
    let projected = store.list("alice").await.unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].native_session_id, None);
}

#[tokio::test]
async fn v2_legacy_state_is_explicit_and_survives_legacy_mutators_until_reset() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store
        .save("alice", &session("legacy-v2", "alice"))
        .await
        .unwrap();

    let path = json_files(&owner_dir(dir.path(), "alice"))[0].clone();
    let disk: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(disk["native_session"]["state"], "legacy_unbound");
    assert_eq!(disk["native_session"]["native_session_id"], "native-xyz");
    assert!(disk["session"].get("native_session_id").is_none());

    store
        .apply_update("alice", &update("legacy-v2", "alice", "transcript"))
        .await
        .unwrap();
    let mut snapshot = store
        .load_snapshot("alice", "legacy-v2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.session_revision, 2);
    assert!(matches!(
        snapshot.native_session,
        NativeSessionState::LegacyUnbound { ref native_session_id }
            if native_session_id == "native-xyz"
    ));
    assert_eq!(
        snapshot.record.native_session_id.as_deref(),
        Some("native-xyz")
    );

    // A legacy writer that omits the id must not silently erase continuity
    // authority; only an explicit revision-fenced reset may do that.
    snapshot.record.native_session_id = None;
    store.save("alice", &snapshot.record).await.unwrap();
    let preserved = store
        .load_snapshot("alice", "legacy-v2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(preserved.session_revision, 3);
    assert!(matches!(
        preserved.native_session,
        NativeSessionState::LegacyUnbound { .. }
    ));

    let reset = store
        .apply_native_transition(
            "alice",
            "legacy-v2",
            &NativeSessionTransition::Reset {
                expected_revision: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(reset.session_revision, 4);
    assert!(matches!(reset.native_session, NativeSessionState::Absent));
}

#[tokio::test]
async fn legacy_mutators_advance_revision_without_erasing_a_binding() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let expected_binding = binding("native-a", "a");
    let mut snapshot = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::Commit {
                expected_revision: 0,
                update: native_update("native", "alice", "first"),
                binding: expected_binding.clone(),
            },
        )
        .await
        .unwrap();

    snapshot.record.run_count = 9;
    store.save("alice", &snapshot.record).await.unwrap();
    let after_save = store
        .load_snapshot("alice", "native")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_save.session_revision, 2);
    assert!(matches!(
        after_save.native_session,
        NativeSessionState::Bound { binding } if *binding == expected_binding
    ));

    // Supplying a legacy id cannot downgrade or replace a bound envelope.
    let mut downgrade = after_save.record.clone();
    downgrade.native_session_id = Some("legacy-overwrite".into());
    assert_eq!(
        store.save("alice", &downgrade).await.unwrap_err().kind,
        ErrorKind::Config
    );
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap()
            .unwrap()
            .session_revision,
        2
    );

    // A legacy transcript update is still serialized under the same lock and
    // fences any stale native CAS while preserving its exact binding.
    store
        .apply_update("alice", &update("native", "alice", "direct"))
        .await
        .unwrap();
    let after_update = store
        .load_snapshot("alice", "native")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_update.session_revision, 3);
    assert!(matches!(
        after_update.native_session,
        NativeSessionState::Bound { binding } if *binding == expected_binding
    ));
}

#[tokio::test]
async fn native_transitions_enforce_cas_and_explicit_legacy_migration() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store
        .save("alice", &session("native", "alice"))
        .await
        .unwrap();
    let first_binding = binding("native-a", "a");

    let commit_error = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::Commit {
                expected_revision: 1,
                update: native_update("native", "alice", "commit"),
                binding: first_binding.clone(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(commit_error.kind, ErrorKind::Config);
    assert!(commit_error.message.contains("reset or forked fresh"));

    let forked = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::ForkFresh {
                expected_revision: 1,
                update: native_update("native", "alice", "fork"),
                binding: first_binding.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(forked.session_revision, 2);
    assert_eq!(forked.record.native_session_id, None);

    assert_eq!(
        store
            .apply_native_transition(
                "alice",
                "native",
                &NativeSessionTransition::Reset {
                    expected_revision: 1,
                },
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );

    let second_binding = binding("native-b", "b");
    assert_eq!(
        store
            .apply_native_transition(
                "alice",
                "native",
                &NativeSessionTransition::Commit {
                    expected_revision: 2,
                    update: native_update("native", "alice", "drift"),
                    binding: second_binding.clone(),
                },
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
    let replaced = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::ForkFresh {
                expected_revision: 2,
                update: native_update("native", "alice", "replace"),
                binding: second_binding.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(replaced.session_revision, 3);

    let reset = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::Reset {
                expected_revision: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(reset.session_revision, 4);
    assert_eq!(reset.record.native_session_id, None);
    assert!(matches!(reset.native_session, NativeSessionState::Absent));
}

#[tokio::test]
async fn strict_v2_rejects_missing_unknown_duplicate_and_dual_authority() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let expected_binding = binding("native-a", "a");
    store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::Commit {
                expected_revision: 0,
                update: native_update("native", "alice", "first"),
                binding: expected_binding,
            },
        )
        .await
        .unwrap();
    let path = json_files(&owner_dir(dir.path(), "alice"))[0].clone();
    let valid = std::fs::read(&path).unwrap();
    let valid_value: serde_json::Value = serde_json::from_slice(&valid).unwrap();

    let mut missing_state = valid_value.clone();
    missing_state
        .as_object_mut()
        .unwrap()
        .remove("native_session");
    std::fs::write(&path, serde_json::to_vec(&missing_state).unwrap()).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );

    let mut missing_revision = valid_value.clone();
    missing_revision
        .as_object_mut()
        .unwrap()
        .remove("session_revision");
    std::fs::write(&path, serde_json::to_vec(&missing_revision).unwrap()).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );

    let mut missing_owner = valid_value.clone();
    missing_owner["session"]
        .as_object_mut()
        .unwrap()
        .remove("owner");
    std::fs::write(&path, serde_json::to_vec(&missing_owner).unwrap()).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );

    let mut unknown_schema = valid_value.clone();
    unknown_schema["schema"] = 3.into();
    std::fs::write(&path, serde_json::to_vec(&unknown_schema).unwrap()).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );

    let mut unknown_field = valid_value.clone();
    unknown_field["unexpected_authority"] = true.into();
    std::fs::write(&path, serde_json::to_vec(&unknown_field).unwrap()).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );

    let mut dual = valid_value.clone();
    dual["session"]["native_session_id"] = "legacy-too".into();
    std::fs::write(&path, serde_json::to_vec(&dual).unwrap()).unwrap();
    let dual_error = store.load_snapshot("alice", "native").await.unwrap_err();
    assert_eq!(dual_error.kind, ErrorKind::Config);
    assert!(dual_error.message.contains("native_session_id"));

    let mut unknown_domain = valid_value.clone();
    unknown_domain["native_session"]["binding"]["domain"]["future_authority"] = true.into();
    std::fs::write(&path, serde_json::to_vec(&unknown_domain).unwrap()).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );

    let raw = String::from_utf8(valid).unwrap();
    let duplicate = raw.replacen(
        "\"native_session\":",
        "\"native_session\":{\"state\":\"absent\"},\"native_session\":",
        1,
    );
    std::fs::write(&path, duplicate).unwrap();
    assert_eq!(
        store
            .load_snapshot("alice", "native")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
}

#[tokio::test]
async fn strict_legacy_reader_rejects_unknown_authority_like_fields() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let mut value = serde_json::to_value(session("legacy", "alice")).unwrap();
    value["native_session_bindng"] = serde_json::json!({ "state": "bound" });
    std::fs::write(
        dir.path().join("legacy.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();

    assert_eq!(
        store
            .load_snapshot("alice", "legacy")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
}

#[tokio::test]
async fn snapshot_listing_surfaces_corrupt_rows_that_legacy_listing_skips() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store
        .save("alice", &session("healthy", "alice"))
        .await
        .unwrap();
    store
        .save("alice", &session("corrupt", "alice"))
        .await
        .unwrap();
    let owner_root = owner_dir(dir.path(), "alice");
    let corrupt_path = json_files(&owner_root)
        .into_iter()
        .find(|path| {
            std::fs::read_to_string(path)
                .unwrap()
                .contains("\"session_id\":\"corrupt\"")
        })
        .unwrap();
    std::fs::write(corrupt_path, b"{not-json").unwrap();

    assert_eq!(store.list("alice").await.unwrap().len(), 1);
    assert!(store.list_snapshots("alice").await.is_err());
}

#[tokio::test]
async fn invalid_native_domains_do_not_advance_revision_or_modify_the_record() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let mut record = session("native", "alice");
    record.native_session_id = None;
    store.save("alice", &record).await.unwrap();
    let before = store
        .load_snapshot("alice", "native")
        .await
        .unwrap()
        .unwrap();

    let mut cases = Vec::new();
    let mut empty_id = binding("native-a", "empty-id");
    empty_id.native_session_id.clear();
    cases.push(empty_id);
    let mut overlong_id = binding("native-a", "long-id");
    overlong_id.native_session_id = "x".repeat(513);
    cases.push(overlong_id);
    let mut uppercase_digest = binding("native-a", "uppercase");
    uppercase_digest.domain.endpoint_routing_digest = "A".repeat(64);
    cases.push(uppercase_digest);
    let mut relative_workdir = binding("native-a", "relative");
    relative_workdir.domain.canonical_workdir = "relative/path".into();
    cases.push(relative_workdir);
    let mut nul_workdir = binding("native-a", "nul");
    nul_workdir.domain.canonical_workdir = "/workspace/with\0nul".into();
    cases.push(nul_workdir);
    let mut zero_schema = binding("native-a", "zero-schema");
    zero_schema.domain.checkpoint_schema = 0;
    cases.push(zero_schema);
    let mut control_runtime = binding("native-a", "control-runtime");
    control_runtime.domain.runtime = "runtime\nforged".into();
    cases.push(control_runtime);
    let mut target_drift = binding("native-a", "target-drift");
    target_drift.domain.provider = ProviderId::new("other-provider");
    cases.push(target_drift);

    for invalid in cases {
        let error = store
            .apply_native_transition(
                "alice",
                "native",
                &NativeSessionTransition::ForkFresh {
                    expected_revision: 1,
                    update: native_update("native", "alice", "invalid"),
                    binding: invalid,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Config);
        let after = store
            .load_snapshot("alice", "native")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.session_revision, before.session_revision);
        assert_eq!(after.record.run_count, before.record.run_count);
        assert!(matches!(after.native_session, NativeSessionState::Absent));
    }
}

#[tokio::test]
async fn revision_exhaustion_fails_without_overwriting_the_last_snapshot() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let mut record = session("native", "alice");
    record.native_session_id = None;
    store.save("alice", &record).await.unwrap();
    let path = json_files(&owner_dir(dir.path(), "alice"))[0].clone();
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["session_revision"] = u64::MAX.into();
    let frozen = serde_json::to_vec(&value).unwrap();
    std::fs::write(&path, &frozen).unwrap();

    let error = store
        .apply_native_transition(
            "alice",
            "native",
            &NativeSessionTransition::Reset {
                expected_revision: u64::MAX,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Config);
    assert!(error.message.contains("revision exhausted"));
    assert_eq!(std::fs::read(&path).unwrap(), frozen);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_native_cas_has_exactly_one_winner() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("sessions");
    let seed = FsSessionStore::new(&root);
    let mut record = session("native", "alice");
    record.native_session_id = None;
    seed.save("alice", &record).await.unwrap();

    let run = |marker: &'static str| {
        let store = FsSessionStore::new(&root);
        async move {
            store
                .apply_native_transition(
                    "alice",
                    "native",
                    &NativeSessionTransition::ForkFresh {
                        expected_revision: 1,
                        update: native_update("native", "alice", marker),
                        binding: binding(&format!("native-{marker}"), marker),
                    },
                )
                .await
        }
    };
    let (left, right) = tokio::join!(run("left"), run("right"));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let loser = left.err().or_else(|| right.err()).unwrap();
    assert_eq!(loser.kind, ErrorKind::Conflict);
    assert!(loser.message.contains("revision conflict"));
    assert_eq!(
        seed.load_snapshot("alice", "native")
            .await
            .unwrap()
            .unwrap()
            .session_revision,
        2
    );
}

#[tokio::test]
async fn save_overwrites_existing_session() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let mut rec = session("s1", "local");
    rec.run_count = 1;
    store.save("local", &rec).await.unwrap();

    rec.run_count = 7;
    store.save("local", &rec).await.unwrap();

    let back = store.load("local", "s1").await.unwrap().unwrap();
    assert_eq!(back.run_count, 7);
}

#[tokio::test]
async fn owners_with_the_same_session_id_never_overwrite_or_cross_read() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let mut alice = session("shared", "alice");
    alice.run_count = 11;
    let mut bob = session("shared", "bob");
    bob.run_count = 22;
    store.save("alice", &alice).await.unwrap();
    store.save("bob", &bob).await.unwrap();

    assert_eq!(
        store
            .load("alice", "shared")
            .await
            .unwrap()
            .unwrap()
            .run_count,
        11
    );
    assert_eq!(
        store
            .load("bob", "shared")
            .await
            .unwrap()
            .unwrap()
            .run_count,
        22
    );
    assert_ne!(owner_dir(dir.path(), "alice"), owner_dir(dir.path(), "bob"));
}

#[tokio::test]
async fn save_and_update_reject_mismatched_owner_authority() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    assert_eq!(
        store
            .save("bob", &session("shared", "alice"))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
    assert_eq!(
        store
            .apply_update("bob", &update("shared", "alice", "one"))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
    assert!(store.load("alice", "shared").await.unwrap().is_none());
    assert!(store.load("bob", "shared").await.unwrap().is_none());
}

#[tokio::test]
async fn session_ids_that_collided_in_the_legacy_safe_id_layout_remain_distinct() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let mut slash = session("a/b", "alice");
    slash.run_count = 1;
    let mut underscore = session("a_b", "alice");
    underscore.run_count = 2;
    let mut backslash = session("a\\b", "alice");
    backslash.run_count = 3;
    store.save("alice", &slash).await.unwrap();
    store.save("alice", &underscore).await.unwrap();
    store.save("alice", &backslash).await.unwrap();

    assert_eq!(
        store.load("alice", "a/b").await.unwrap().unwrap().run_count,
        1
    );
    assert_eq!(
        store.load("alice", "a_b").await.unwrap().unwrap().run_count,
        2
    );
    assert_eq!(
        store
            .load("alice", "a\\b")
            .await
            .unwrap()
            .unwrap()
            .run_count,
        3
    );
    assert_eq!(json_files(&owner_dir(dir.path(), "alice")).len(), 3);
}

#[tokio::test]
async fn path_like_owner_and_session_ids_cannot_escape_the_store() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("sessions");
    let store = FsSessionStore::new(&root);
    let owner = "../../outside-owner";
    let session_id = "../../../outside-session";

    store
        .save(owner, &session(session_id, owner))
        .await
        .unwrap();
    let loaded = store.load(owner, session_id).await.unwrap().unwrap();
    assert_eq!(loaded.owner, owner);
    assert_eq!(loaded.session_id, session_id);
    assert!(!dir.path().join("outside-session.json").exists());

    let root_entries = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(root_entries.len(), 1);
    assert_eq!(root_entries[0].len(), 64);
    assert!(root_entries[0].bytes().all(|byte| byte.is_ascii_hexdigit()));
}

#[tokio::test]
async fn load_rejects_a_record_whose_embedded_identity_does_not_match_its_path() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store.save("alice", &session("s1", "alice")).await.unwrap();

    let path = json_files(&owner_dir(dir.path(), "alice"))
        .into_iter()
        .next()
        .unwrap();
    std::fs::write(&path, serde_json::to_vec(&session("s1", "bob")).unwrap()).unwrap();

    let error = store.load("alice", "s1").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Config);
    assert!(error.message.contains("identity mismatch"));
}

#[tokio::test]
async fn oversized_session_file_is_rejected_before_reading_it() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store.save("alice", &session("s1", "alice")).await.unwrap();
    let path = json_files(&owner_dir(dir.path(), "alice"))
        .into_iter()
        .next()
        .unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(64 * 1024 * 1024 + 1)
        .unwrap();

    let error = store.load("alice", "s1").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.message.contains("exceeds"));
}

#[tokio::test]
async fn exact_legacy_flat_record_is_migrated_without_trusting_its_filename() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let legacy = session("a/b", "alice");
    std::fs::write(
        dir.path().join("a_b.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();

    let loaded = store.load("alice", "a/b").await.unwrap().unwrap();
    assert_eq!(loaded.session_id, "a/b");
    assert_eq!(loaded.owner, "alice");
    assert!(!dir.path().join("a_b.json").exists());
    assert_eq!(store.list("alice").await.unwrap().len(), 1);
    assert_eq!(store.list_all_admin().await.unwrap().len(), 1);
    store.save("alice", &legacy).await.unwrap();
}

#[tokio::test]
async fn legacy_safe_id_collision_is_never_attributed_to_the_wrong_session() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    let legacy = session("a/b", "alice");
    std::fs::write(
        dir.path().join("a_b.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();

    assert!(store.load("alice", "a_b").await.unwrap().is_none());
    assert!(dir.path().join("a_b.json").exists());
    assert!(store.load("bob", "a/b").await.unwrap().is_none());
    assert!(dir.path().join("a_b.json").exists());
}

#[tokio::test]
async fn explicit_admin_list_returns_every_owner() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    store.save("alice", &session("s1", "alice")).await.unwrap();
    store.save("bob", &session("s2", "bob")).await.unwrap();
    store.save("alice", &session("s3", "alice")).await.unwrap();

    let all = store.list_all_admin().await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn list_filters_by_owner() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    store.save("alice", &session("s1", "alice")).await.unwrap();
    store.save("bob", &session("s2", "bob")).await.unwrap();
    store.save("alice", &session("s3", "alice")).await.unwrap();

    let alice = store.list("alice").await.unwrap();
    assert_eq!(alice.len(), 2);
    assert!(alice.iter().all(|s| s.owner == "alice"));

    let bob = store.list("bob").await.unwrap();
    assert_eq!(bob.len(), 1);
    assert_eq!(bob[0].session_id, "s2");
}

#[tokio::test]
async fn list_on_missing_dir_returns_empty() {
    // A store directory that was never created is an empty store, not an error.
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path().join("does-not-exist"));
    assert!(store.list_all_admin().await.unwrap().is_empty());
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
        .save("local", &session_with_marker("sess", "local", &marker))
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
                s.save("local", &rec).await.unwrap();
            }
        })
    });

    let reader_dir = store.dir().to_path_buf();
    let marker_clone = marker.clone();
    let reader = tokio::spawn(async move {
        let s = FsSessionStore::new(reader_dir);
        for _ in 0..200 {
            // Every successful load must carry the complete marker transcript.
            if let Some(rec) = s.load("local", "sess").await.unwrap() {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_atomic_updates_never_lose_transcript_or_run_count() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("sessions");
    let first = FsSessionStore::new(&root);
    let second = FsSessionStore::new(&root);

    let writer = |store: FsSessionStore, prefix: &'static str| async move {
        for index in 0..50 {
            store
                .apply_update(
                    "alice",
                    &update("shared", "alice", &format!("{prefix}-{index}")),
                )
                .await
                .unwrap();
        }
    };
    tokio::join!(writer(first, "a"), writer(second, "b"));

    let record = FsSessionStore::new(&root)
        .load("alice", "shared")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.run_count, 100);
    assert_eq!(record.transcript.len(), 200);
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
    // Persistent mutation and execution lock files are expected; no
    // uniquely-named temp artifact may remain after the atomic publish.
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store.save("local", &session("s1", "local")).await.unwrap();

    let entries: Vec<String> = std::fs::read_dir(owner_dir(dir.path(), "local"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries.len(), 3);
    assert!(entries.iter().any(|entry| entry.ends_with(".json")));
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.ends_with(".execution.lock"))
            .count(),
        1
    );
    assert_eq!(
        entries
            .iter()
            .filter(|entry| { entry.ends_with(".lock") && !entry.ends_with(".execution.lock") })
            .count(),
        1
    );
    assert!(entries.iter().all(|entry| !entry.contains(".tmp.")));
}

#[tokio::test]
async fn execution_lease_is_exact_scoped_fences_controls_and_releases_on_drop() {
    let dir = TempDir::new().unwrap();
    let first_store = FsSessionStore::new(dir.path());
    let second_store = FsSessionStore::new(dir.path());
    first_store
        .save("alice", &session("shared", "alice"))
        .await
        .unwrap();

    let lease = first_store
        .acquire_execution_lease("alice", "shared", "run-first")
        .await
        .unwrap();
    assert_eq!(lease.owner(), "alice");
    assert_eq!(lease.session_id(), "shared");
    assert_eq!(lease.execution_id(), "run-first");
    let snapshot = lease.load_snapshot().await.unwrap().unwrap();

    let conflict = second_store
        .acquire_execution_lease("alice", "shared", "run-conflict")
        .await
        .err()
        .expect("same owner/session lease must conflict");
    assert_eq!(conflict.kind, ErrorKind::Conflict);

    // Owner and session are both part of the lock namespace.
    let different_session = second_store
        .acquire_execution_lease("alice", "other", "run-other-session")
        .await
        .unwrap();
    let different_owner = second_store
        .acquire_execution_lease("bob", "shared", "run-other-owner")
        .await
        .unwrap();
    drop(different_session);
    drop(different_owner);

    // A control-plane transition cannot race a live model execution.
    let control_conflict = second_store
        .apply_native_transition(
            "alice",
            "shared",
            &NativeSessionTransition::Reset {
                expected_revision: snapshot.session_revision,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(control_conflict.kind, ErrorKind::Conflict);

    // The live lease cannot be confused into mutating another session.
    let wrong_identity = lease
        .apply_update(
            snapshot.session_revision,
            &update("other", "alice", "wrong"),
        )
        .await
        .unwrap_err();
    assert_eq!(wrong_identity.kind, ErrorKind::Config);

    let reset = lease
        .apply_native_transition(&NativeSessionTransition::Reset {
            expected_revision: snapshot.session_revision,
        })
        .await
        .unwrap();
    assert!(matches!(reset.native_session, NativeSessionState::Absent));
    drop(lease);

    let reacquired = second_store
        .acquire_execution_lease("alice", "shared", "run-after-drop")
        .await
        .unwrap();
    let current = reacquired.load_snapshot().await.unwrap().unwrap();
    let updated = reacquired
        .apply_update(
            current.session_revision,
            &update("shared", "alice", "after-drop"),
        )
        .await
        .unwrap();
    assert_eq!(updated.record.run_count, 2);
    let duplicate = reacquired
        .apply_update(
            updated.session_revision,
            &update("shared", "alice", "duplicate"),
        )
        .await
        .unwrap_err();
    assert_eq!(duplicate.kind, ErrorKind::Conflict);
    drop(reacquired);

    let stale = second_store
        .acquire_execution_lease("alice", "shared", "run-stale")
        .await
        .unwrap();
    let stale_error = stale
        .apply_update(
            updated.session_revision - 1,
            &update("shared", "alice", "stale"),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_error.kind, ErrorKind::Conflict);
    drop(stale);
    let unchanged = second_store
        .load_snapshot("alice", "shared")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.session_revision, updated.session_revision);
    assert_eq!(unchanged.record.run_count, 2);
}

#[tokio::test]
async fn empty_or_nul_identities_are_rejected() {
    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());

    let mut empty_owner = session("s1", "");
    assert_eq!(
        store.save("", &empty_owner).await.unwrap_err().kind,
        ErrorKind::Config
    );
    empty_owner.owner = "alice".into();
    empty_owner.session_id = "bad\0id".into();
    assert_eq!(
        store.save("alice", &empty_owner).await.unwrap_err().kind,
        ErrorKind::Config
    );
    assert_eq!(
        store.load("", "s1").await.unwrap_err().kind,
        ErrorKind::Config
    );
}

#[cfg(unix)]
#[tokio::test]
async fn owner_namespaces_and_session_files_are_private() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = TempDir::new().unwrap();
    let store = FsSessionStore::new(dir.path());
    store.save("alice", &session("s1", "alice")).await.unwrap();

    let owner_root = owner_dir(dir.path(), "alice");
    assert_eq!(
        std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&owner_root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for entry in std::fs::read_dir(owner_root).unwrap() {
        assert_eq!(
            entry.unwrap().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
