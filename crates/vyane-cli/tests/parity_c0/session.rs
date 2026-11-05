use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use vyane_core::{
    ChatMessage, ModelId, NativeSessionBinding, NativeSessionDomain, NativeSessionState,
    NativeSessionTransition, Protocol, ProviderId, SessionRef, SessionUpdate, Target,
    WorkdirIdentity,
};

pub(super) fn replay(input: &Value, _temp: &Path) -> Value {
    match input["op"].as_str().expect("session op") {
        "session_id" => session_id(input["mode"].as_str().expect("mode")),
        "native_resume" => json!({
            "absent": "fallthrough",
            "bound": "unsupported",
            "legacy_unbound": "unsupported",
            "states": [
                NativeSessionState::Absent.as_str(),
                NativeSessionState::LegacyUnbound {
                    native_session_id: "native-public".to_string(),
                }
                .as_str(),
                NativeSessionState::Bound {
                    binding: Box::new(binding()),
                }
                .as_str(),
            ],
        }),
        "transitions" => transitions(),
        "apply_update" => apply_update(),
        "chat_roles" => json!({
            "roles": [
                role_of(ChatMessage::system("note")),
                role_of(ChatMessage::user("note")),
                role_of(ChatMessage::assistant("note")),
            ],
        }),
        other => panic!("unknown session op {other}"),
    }
}

fn session_id(mode: &str) -> Value {
    let id = match mode {
        "empty" => String::new(),
        "nul" => "a\0b".to_string(),
        "space" => " ".to_string(),
        "exact_cap" => "a".repeat(SessionRef::MAX_ID_BYTES),
        "overlong" => "a".repeat(SessionRef::MAX_ID_BYTES + 1),
        other => panic!("unknown session id mode {other}"),
    };
    let bytes = id.len();
    match SessionRef::try_new(id) {
        Ok(_) => json!({ "accepted": true, "bytes": bytes, "class": "ok" }),
        Err(error) => json!({
            "accepted": false,
            "bytes": bytes,
            "class": error.kind.as_str(),
        }),
    }
}

fn transitions() -> Value {
    let update = update(Some("native-public".to_string()));
    let reset = NativeSessionTransition::Reset {
        expected_revision: 1,
    };
    let fork = NativeSessionTransition::ForkFresh {
        expected_revision: 1,
        update: update.clone(),
        binding: binding(),
    };
    let commit = NativeSessionTransition::Commit {
        expected_revision: 1,
        update,
        binding: binding(),
    };
    json!({
        "commit": commit.as_str(),
        "fork_fresh": fork.as_str(),
        "reset": reset.as_str(),
    })
}

fn apply_update() -> Value {
    let first = update(Some("native-public".to_string())).apply_to(None);
    let second = update(None).apply_to(Some(first));
    json!({
        "model": second.target.model.as_str(),
        "native_session_id": second.native_session_id,
        "owner": second.owner,
        "provider": second.target.provider.as_str(),
        "run_count": second.run_count,
        "session_id": second.session_id,
        "transcript_len": second.transcript.len(),
    })
}

fn update(native_session_id: Option<String>) -> SessionUpdate {
    SessionUpdate {
        owner: "local".to_string(),
        session_id: "sess-public".to_string(),
        target: Target {
            provider: ProviderId::new("relay"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("model-a"),
        },
        native_session_id,
        transcript_delta: vec![ChatMessage::user("note")],
        occurred_at: stamp(),
    }
}

fn binding() -> NativeSessionBinding {
    NativeSessionBinding {
        native_session_id: "native-public".to_string(),
        domain: NativeSessionDomain {
            runtime: "public".to_string(),
            harness: vyane_core::HarnessKind::ClaudeCode,
            provider: ProviderId::new("relay"),
            protocol: Protocol::OpenaiChat,
            model: ModelId::new("model-a"),
            endpoint_routing_digest: "ab".to_string(),
            canonical_workdir: Path::new("workdir").to_path_buf(),
            workdir_identity: WorkdirIdentity {
                device: 1,
                inode: 1,
            },
            checkpoint_namespace: "public".to_string(),
            checkpoint_schema: 1,
            account_scope_digest: "aa".to_string(),
            runtime_scope_digest: "bb".to_string(),
        },
    }
}

fn role_of(message: ChatMessage) -> &'static str {
    match message.role {
        vyane_core::Role::System => "system",
        vyane_core::Role::User => "user",
        vyane_core::Role::Assistant => "assistant",
    }
}

fn stamp() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch")
}
