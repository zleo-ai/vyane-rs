//! Read-only session inspection through real loopback HTTP and stdio MCP.
#![allow(clippy::unwrap_used)]

mod common;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::Context as _;
use rmcp::model::CallToolRequestParams;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use vyane_core::{
    ChatMessage, HarnessKind, ModelId, NativeSessionBinding, NativeSessionDomain,
    NativeSessionState, NativeSessionTransition, Protocol, ProviderId, SessionRecord,
    SessionSnapshot, SessionUpdate, Target, WorkdirIdentity,
};
use vyane_service::{SessionView, StoragePaths, VyaneService};

const TIMEOUT: Duration = Duration::from_secs(10);

struct Fixture {
    root: tempfile::TempDir,
    service: VyaneService,
}

#[derive(Debug, PartialEq, Eq)]
struct DiskEntry {
    contents: Option<Vec<u8>>,
    #[cfg(unix)]
    mode: u32,
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let root = common::private_tempdir();
        let config = root.path().join("config.toml");
        fs::write(&config, "")?;
        let service = VyaneService::from_loaded_with_paths(
            vyane_service::load_config(Some(&config))?,
            StoragePaths::from_data_dir(root.path().join("data")),
        )?;
        let target = Target {
            provider: ProviderId::new("fixture-provider"),
            protocol: Protocol::AnthropicMessages,
            model: ModelId::new("fixture-model"),
            harness: Some(HarnessKind::ClaudeCode),
        };
        let now = chrono::Utc::now();
        let store = &service.runtime().sessions;
        for (owner, id, native) in [
            ("local", "shared", None),
            ("local", "absent-native", None),
            ("local", "legacy", Some("CANARY_LEGACY_ID")),
            ("CANARY_FOREIGN_OWNER", "shared", None),
            ("CANARY_FOREIGN_OWNER", "foreign-only", None),
        ] {
            store
                .save(
                    owner,
                    &SessionRecord {
                        session_id: id.into(),
                        owner: owner.into(),
                        target: target.clone(),
                        native_session_id: native.map(str::to_owned),
                        transcript: vec![ChatMessage::user("CANARY_TRANSCRIPT")],
                        created_at: now,
                        updated_at: now,
                        run_count: if owner == "local" { 4 } else { 99 },
                    },
                )
                .await?;
        }
        let revision = store
            .load_snapshot("local", "shared")
            .await?
            .unwrap()
            .session_revision;
        store
            .apply_native_transition(
                "local",
                "shared",
                &NativeSessionTransition::Commit {
                    expected_revision: revision,
                    update: SessionUpdate {
                        owner: "local".into(),
                        session_id: "shared".into(),
                        target: target.clone(),
                        native_session_id: None,
                        transcript_delta: vec![ChatMessage::assistant("CANARY_REPLY")],
                        occurred_at: now,
                    },
                    binding: NativeSessionBinding {
                        native_session_id: "CANARY_BOUND_ID".into(),
                        domain: NativeSessionDomain {
                            runtime: "CANARY_RUNTIME".into(),
                            harness: HarnessKind::ClaudeCode,
                            provider: target.provider,
                            protocol: target.protocol,
                            model: target.model,
                            endpoint_routing_digest: "a".repeat(64),
                            canonical_workdir: "/CANARY_WORKDIR".into(),
                            workdir_identity: WorkdirIdentity {
                                device: 123,
                                inode: 456,
                            },
                            checkpoint_namespace: "CANARY_CHECKPOINT".into(),
                            checkpoint_schema: 1,
                            account_scope_digest: "b".repeat(64),
                            runtime_scope_digest: "c".repeat(64),
                        },
                    },
                },
            )
            .await?;
        Ok(Self { root, service })
    }

    fn command(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_vyane"));
        cmd.env_clear()
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("absent-config"))
            .env("VYANE_DATA_DIR", self.root.path().join("data"))
            .current_dir(self.root.path())
            .arg("--config")
            .arg(self.root.path().join("config.toml"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(path) = std::env::var_os("LLVM_PROFILE_FILE") {
            cmd.env("LLVM_PROFILE_FILE", path);
        }
        cmd
    }

    fn snapshot(&self) -> anyhow::Result<BTreeMap<String, DiskEntry>> {
        snapshot(&self.service.storage_paths().sessions_dir)
    }

    fn seed_record(&self, id: &str, flat: bool) -> anyhow::Result<Value> {
        let now = chrono::Utc::now();
        let record = SessionRecord {
            session_id: id.into(),
            owner: "local".into(),
            target: Target {
                provider: ProviderId::new("fixture-provider"),
                protocol: Protocol::OpenaiChat,
                model: ModelId::new("fixture-model"),
                harness: None,
            },
            native_session_id: None,
            transcript: vec![ChatMessage::user("CANARY_TRANSCRIPT")],
            created_at: now,
            updated_at: now,
            run_count: 1,
        };
        let root = &self.service.storage_paths().sessions_dir;
        let path = if flat {
            root.join(format!("{id}.json"))
        } else {
            let owner = format!("{:x}", Sha256::digest(b"local"));
            let mut hash = Sha256::new();
            hash.update(b"vyane.session.v1\0");
            hash.update(id.as_bytes());
            root.join(owner).join(format!("{:x}.json", hash.finalize()))
        };
        fs::write(&path, serde_json::to_vec(&record)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(serde_json::to_value(SessionView::from(SessionSnapshot {
            record,
            session_revision: 0,
            native_session: NativeSessionState::Absent,
        }))?)
    }

    fn corrupt(&self) -> anyhow::Result<PathBuf> {
        let dir = &self.service.storage_paths().sessions_dir;
        for (relative, entry) in self.snapshot()? {
            if let Some(bytes) = entry.contents
                && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
                && value["session"]["owner"] == "local"
                && value["session"]["session_id"] == "shared"
            {
                // Preserve valid JSON but violate the stored identity contract.
                let mut value = value;
                value["session"]["owner"] = json!("CANARY_CORRUPT_OWNER");
                let path = dir.join(relative);
                fs::write(&path, serde_json::to_vec(&value)?)?;
                return Ok(path);
            }
        }
        anyhow::bail!("fixture session file missing")
    }
}

fn snapshot(root: &Path) -> anyhow::Result<BTreeMap<String, DiskEntry>> {
    fn visit(
        root: &Path,
        dir: &Path,
        files: &mut BTreeMap<String, DiskEntry>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry.metadata()?;
            files.insert(
                path.strip_prefix(root)?.to_string_lossy().into_owned(),
                DiskEntry {
                    contents: if metadata.is_dir() {
                        None
                    } else {
                        Some(fs::read(&path)?)
                    },
                    #[cfg(unix)]
                    mode: {
                        use std::os::unix::fs::PermissionsExt as _;
                        metadata.permissions().mode()
                    },
                },
            );
            if metadata.is_dir() {
                visit(root, &path, files)?;
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    let metadata = fs::metadata(root)?;
    files.insert(
        String::new(),
        DiskEntry {
            contents: None,
            #[cfg(unix)]
            mode: {
                use std::os::unix::fs::PermissionsExt as _;
                metadata.permissions().mode()
            },
        },
    );
    visit(root, root, &mut files)?;
    Ok(files)
}

fn legal_missing_ids() -> Vec<String> {
    vec![
        "x".repeat(250),
        "x".repeat(251),
        "x".repeat(1024),
        "界".repeat(341),
    ]
}

fn opaque_ids() -> Vec<String> {
    vec!["x".repeat(1024), "界".repeat(341), "opaque/a?b#c%20".into()]
}

fn session_url(base: &str, id: &str) -> reqwest::Url {
    let mut url = reqwest::Url::parse(base).unwrap();
    url.path_segments_mut()
        .unwrap()
        .extend(["v1", "sessions", id]);
    url
}

#[tokio::test]
async fn service_session_inspect_reads_flat_legacy_without_migration_or_disk_changes()
-> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let expected = fixture.seed_record("legacy-disk", true)?;
    let before = fixture.snapshot()?;
    let view = fixture.service.session("legacy-disk").await?.unwrap();
    assert_inspect(&json!({"session": view}), &expected);
    assert_eq!(
        fixture.snapshot()?,
        before,
        "inspection must not migrate, create locks, delete the source, or change permissions"
    );
    Ok(())
}

#[tokio::test]
async fn service_session_inspect_unknown_legal_long_ids_are_not_found() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let before = fixture.snapshot()?;
    for id in legal_missing_ids() {
        assert!(
            fixture.service.session(&id).await?.is_none(),
            "unknown legal {}-byte id",
            id.len()
        );
    }
    assert_eq!(fixture.snapshot()?, before);
    Ok(())
}

fn assert_safe(value: &Value) {
    let wire = value.to_string();
    assert!(!wire.contains("CANARY"), "leaked fixture secret: {wire}");
    for key in [
        "owner",
        "transcript",
        "native_session_id",
        "native_session",
        "binding",
        "canonical_workdir",
        "workdir_identity",
        "endpoint_routing_digest",
        "account_scope_digest",
        "runtime_scope_digest",
        "checkpoint_namespace",
        "runtime",
    ] {
        assert!(
            !wire.contains(&format!("\"{key}\"")),
            "leaked {key}: {wire}"
        );
    }
}

fn assert_inspect(value: &Value, expected: &Value) {
    assert_eq!(value, &json!({"session": expected}));
    assert_safe(value);
    let session = &value["session"];
    assert_eq!(session["native_resume_available"], false);
    let keys = session.as_object().unwrap();
    assert_eq!(keys.len(), 10);
    for key in [
        "view_schema",
        "session_id",
        "target",
        "created_at",
        "updated_at",
        "run_count",
        "transcript_messages",
        "session_revision",
        "native_state",
        "native_resume_available",
    ] {
        assert!(keys.contains_key(key), "missing {key}");
    }
}

async fn mcp_call(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    name: &str,
    args: Value,
) -> anyhow::Result<Value> {
    let result = tokio::time::timeout(
        TIMEOUT,
        client.call_tool(
            CallToolRequestParams::new(name.to_owned())
                .with_arguments(args.as_object().unwrap().clone()),
        ),
    )
    .await??;
    assert_ne!(result.is_error, Some(true));
    let wire = serde_json::to_value(result)?;
    Ok(serde_json::from_str(
        wire["content"][0]["text"].as_str().unwrap(),
    )?)
}

#[tokio::test]
async fn stdio_session_inspect_is_scoped_redacted_strict_and_read_only() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let legacy_expected = fixture.seed_record("legacy-disk", true)?;
    let before = fixture.snapshot()?;
    let mut child = fixture.command().arg("mcp").spawn()?;
    let stdout = child.stdout.take().unwrap();
    let stdin = child.stdin.take().unwrap();
    let client = tokio::time::timeout(
        TIMEOUT,
        <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), (stdout, stdin)),
    )
    .await??;
    let tools = client.list_all_tools().await?;
    assert_eq!(tools.len(), 11);
    let tool = tools
        .iter()
        .find(|t| t.name == "vyane_session_inspect")
        .unwrap();
    assert_eq!(tool.input_schema["additionalProperties"], false);
    assert_eq!(tool.input_schema["required"], json!(["session_id"]));
    assert_eq!(
        tool.input_schema["properties"].as_object().unwrap().len(),
        1
    );
    assert_eq!(
        tool.input_schema["properties"]["session_id"]["type"],
        "string"
    );
    let expected_list = serde_json::to_value(fixture.service.session_views().await?)?;
    let list = mcp_call(&client, "vyane_sessions", json!({})).await?;
    assert_eq!(list, json!({"items": expected_list}));
    assert_safe(&list);
    for (id, state) in [
        ("shared", "bound"),
        ("legacy", "legacy_unbound"),
        ("absent-native", "absent"),
    ] {
        let expected = serde_json::to_value(fixture.service.session(id).await?.unwrap())?;
        let inspected =
            mcp_call(&client, "vyane_session_inspect", json!({"session_id": id})).await?;
        assert_inspect(&inspected, &expected);
        assert_eq!(inspected["session"]["native_state"], state);
        if id == "shared" {
            assert_eq!(inspected["session"]["run_count"], 5);
            assert_eq!(inspected["session"]["transcript_messages"], 2);
            assert_eq!(inspected["session"]["session_revision"], 2);
        }
    }
    let legacy = mcp_call(
        &client,
        "vyane_session_inspect",
        json!({"session_id":"legacy-disk"}),
    )
    .await?;
    assert_inspect(&legacy, &legacy_expected);
    assert_eq!(fixture.snapshot()?, before);
    let missing = mcp_call(
        &client,
        "vyane_session_inspect",
        json!({"session_id": "missing"}),
    )
    .await?;
    assert_eq!(
        missing,
        json!({"status":"error","error":{"code":"not_found","message":"session not found"}})
    );
    assert_eq!(
        mcp_call(
            &client,
            "vyane_session_inspect",
            json!({"session_id":"foreign-only"})
        )
        .await?,
        missing
    );
    assert_eq!(fixture.snapshot()?, before);
    for id in legal_missing_ids() {
        assert_eq!(
            mcp_call(&client, "vyane_session_inspect", json!({"session_id":id})).await?,
            missing
        );
        assert_eq!(fixture.snapshot()?, before);
    }
    for id in opaque_ids() {
        let expected = fixture.seed_record(&id, false)?;
        let before = fixture.snapshot()?;
        let inspected =
            mcp_call(&client, "vyane_session_inspect", json!({"session_id":id})).await?;
        assert_inspect(&inspected, &expected);
        assert_eq!(fixture.snapshot()?, before);
    }
    let corrupt_path = fixture.corrupt()?;
    let corrupted = fixture.snapshot()?;
    for args in [
        json!({}),
        json!({"session_id":42}),
        json!({"session_id":""}),
        json!({"session_id":"bad\u{0}id"}),
        json!({"session_id":"界".repeat(342)}),
        json!({"session_id":"shared","owner":"CANARY_FOREIGN_OWNER"}),
        json!({"session_id":"shared","extra":"CANARY_EXTRA"}),
    ] {
        let rejected = mcp_call(&client, "vyane_session_inspect", args).await?;
        assert_eq!(rejected["error"]["code"], "invalid_argument");
        assert_safe(&rejected);
    }
    let failed = mcp_call(
        &client,
        "vyane_session_inspect",
        json!({"session_id":"shared"}),
    )
    .await?;
    assert_eq!(
        failed,
        json!({"status":"error","error":{"code":"internal","message":"session storage unavailable"}})
    );
    assert_eq!(fixture.snapshot()?, corrupted);
    fs::write(&corrupt_path, "{CANARY_CORRUPT_BODY")?;
    let malformed = fixture.snapshot()?;
    assert_eq!(
        mcp_call(
            &client,
            "vyane_session_inspect",
            json!({"session_id":"shared"})
        )
        .await?,
        failed
    );
    assert_eq!(fixture.snapshot()?, malformed);
    // Even when the store root cannot be traversed, invalid ids fail at the boundary.
    let sessions = &fixture.service.storage_paths().sessions_dir;
    let parked = sessions.with_extension("parked");
    fs::rename(sessions, &parked)?;
    fs::write(sessions, "CANARY_STORAGE_BODY")?;
    for id in ["".to_owned(), "bad\0id".to_owned(), "x".repeat(1025)] {
        let rejected = mcp_call(&client, "vyane_session_inspect", json!({"session_id":id})).await?;
        assert_eq!(rejected["error"]["code"], "invalid_argument");
        assert_safe(&rejected);
    }
    assert_eq!(fs::read_to_string(sessions)?, "CANARY_STORAGE_BODY");
    client.cancel().await?;
    assert!(
        tokio::time::timeout(TIMEOUT, child.wait())
            .await??
            .success()
    );
    Ok(())
}

#[tokio::test]
async fn loopback_session_inspect_is_authenticated_scoped_and_read_only() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let legacy_expected = fixture.seed_record("legacy-disk", true)?;
    let before = fixture.snapshot()?;
    let reservation = std::net::TcpListener::bind("127.0.0.1:0")
        .context("bind loopback listener for HTTP acceptance")?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let mut child = fixture
        .command()
        .args(["serve", "--addr", &address.to_string()])
        .spawn()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?;
    let base = format!("http://{address}");
    let token_path = fixture.root.path().join("data/serve.token");
    let token = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Ok(token) = fs::read_to_string(&token_path)
                && client
                    .get(format!("{base}/v1/health"))
                    .bearer_auth(&token)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
            {
                break token;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "serve exited before ready"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;
    let url = format!("{base}/v1/sessions/shared");
    for auth in [None, Some("wrong")] {
        let mut request = client.get(&url);
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        assert_eq!(request.send().await?.status(), 401);
    }
    let list: Value = client
        .get(format!("{base}/v1/sessions"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        list,
        json!({"items":fixture.service.session_views().await?})
    );
    assert_safe(&list);
    for id in ["shared", "legacy", "absent-native"] {
        let expected = serde_json::to_value(fixture.service.session(id).await?.unwrap())?;
        let response = client
            .get(format!("{base}/v1/sessions/{id}"))
            .bearer_auth(&token)
            .send()
            .await?;
        assert_eq!(response.status(), 200);
        assert_inspect(&response.json().await?, &expected);
    }
    let response = client
        .get(session_url(&base, "legacy-disk"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_inspect(&response.json().await?, &legacy_expected);
    assert_eq!(fixture.snapshot()?, before);
    for id in ["missing".into(), "foreign-only".into()]
        .into_iter()
        .chain(legal_missing_ids())
    {
        let response = client
            .get(session_url(&base, &id))
            .bearer_auth(&token)
            .send()
            .await?;
        assert_eq!(response.status(), 404);
        assert_eq!(
            response.json::<Value>().await?,
            json!({"error":"session not found"})
        );
    }
    assert_eq!(fixture.snapshot()?, before);
    for id in opaque_ids() {
        let expected = fixture.seed_record(&id, false)?;
        let before = fixture.snapshot()?;
        let response = client
            .get(session_url(&base, &id))
            .bearer_auth(&token)
            .send()
            .await?;
        assert_eq!(response.status(), 200);
        assert_inspect(&response.json().await?, &expected);
        assert_eq!(fixture.snapshot()?, before);
    }
    let corrupt_path = fixture.corrupt()?;
    let corrupted = fixture.snapshot()?;
    for suffix in ["?owner=CANARY_FOREIGN_OWNER", "?extra=CANARY_EXTRA", "?"] {
        let response = client
            .get(format!("{url}{suffix}"))
            .bearer_auth(&token)
            .send()
            .await?;
        assert_eq!(response.status(), 400);
        assert_safe(&response.json().await?);
    }
    for body in [
        json!({"owner":"CANARY_FOREIGN_OWNER"}),
        json!({"extra":true}),
        json!({}),
    ] {
        let response = client
            .get(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;
        assert_eq!(response.status(), 400);
        assert_safe(&response.json().await?);
    }
    let response = client.get(&url).bearer_auth(&token).send().await?;
    assert_eq!(response.status(), 500);
    assert_eq!(
        response.json::<Value>().await?,
        json!({"error":"session storage unavailable"})
    );
    assert_eq!(fixture.snapshot()?, corrupted);
    fs::write(&corrupt_path, "{CANARY_CORRUPT_BODY")?;
    let malformed = fixture.snapshot()?;
    let response = client.get(&url).bearer_auth(&token).send().await?;
    assert_eq!(response.status(), 500);
    assert_eq!(
        response.json::<Value>().await?,
        json!({"error":"session storage unavailable"})
    );
    assert_eq!(fixture.snapshot()?, malformed);
    let sessions = &fixture.service.storage_paths().sessions_dir;
    fs::rename(sessions, sessions.with_extension("parked"))?;
    fs::write(sessions, "CANARY_STORAGE_BODY")?;
    for id in [
        "bad%00id".to_owned(),
        "x".repeat(1025),
        "界".repeat(342),
        "%FF".to_owned(),
    ] {
        let response = client
            .get(format!("{base}/v1/sessions/{id}"))
            .bearer_auth(&token)
            .send()
            .await?;
        assert_eq!(response.status(), 400);
        assert_eq!(
            response.json::<Value>().await?,
            json!({"error":"invalid session id"})
        );
    }
    assert_eq!(fs::read_to_string(sessions)?, "CANARY_STORAGE_BODY");
    child.kill().await?;
    child.wait().await?;
    Ok(())
}
