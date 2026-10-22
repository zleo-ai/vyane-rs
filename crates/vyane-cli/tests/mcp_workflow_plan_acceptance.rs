mod common;

use std::{collections::BTreeMap, fs, path::Path, process::Stdio, time::Duration};

use rmcp::model::CallToolRequestParams;
use serde_json::{Value, json};
use vyane_workflow::{Workflow, WorkflowPlan};

const SOURCE: &str = r#"
[workflow]
name = "inspect-before-run"
max_concurrency = 2
[[step]]
id = "gather"
fan_out = ["not-configured-a", "not-configured-b"]
prompt_file = "prompt.txt"
system = "caller system text"
workdir = "/caller-requested-workspace"
sandbox = "full"
timeout_secs = 7
[[step]]
id = "summarize"
needs = ["gather"]
target = "auto"
prompt = "Summarize {{steps.gather.outputs}}"
[step.route]
effort = "high"
"#;
const PROMPT: &str = "Find {{vars.topic}}\n";

struct Session {
    root: tempfile::TempDir,
    child: tokio::process::Child,
    client: rmcp::service::RunningService<rmcp::RoleClient, ()>,
    initial_files: BTreeMap<String, Option<Vec<u8>>>,
}

impl Session {
    async fn start() -> anyhow::Result<Self> {
        let root = common::private_tempdir();
        let config = root.path().join("config.toml");
        fs::write(&config, "")?;
        fs::write(root.path().join("workflow.toml"), SOURCE)?;
        fs::write(root.path().join("prompt.txt"), PROMPT)?;
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_vyane"));
        command
            .env_clear()
            .env("HOME", root.path())
            .env("XDG_CONFIG_HOME", root.path().join("absent-config"))
            .env("VYANE_DATA_DIR", root.path().join("data"))
            .current_dir(root.path())
            .arg("--config")
            .arg(config)
            .arg("mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(path) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", path);
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stdin = child.stdin.take().expect("piped stdin");
        let client = tokio::time::timeout(
            Duration::from_secs(10),
            <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), (stdout, stdin)),
        )
        .await??;
        let initial_files = snapshot(root.path())?;
        Ok(Self {
            root,
            child,
            client,
            initial_files,
        })
    }

    async fn call(&self, arguments: Value) -> anyhow::Result<Value> {
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            self.client.call_tool(
                CallToolRequestParams::new("vyane_workflow_plan")
                    .with_arguments(arguments.as_object().expect("object fixture").clone()),
            ),
        )
        .await??;
        assert_ne!(result.is_error, Some(true));
        let result = serde_json::to_value(result)?;
        let text = result["content"][0]["text"]
            .as_str()
            .expect("JSON text result");
        assert!(text.len() <= 1024 * 1024, "bounded wire payload");
        Ok(serde_json::from_str(text)?)
    }

    async fn finish(mut self) -> anyhow::Result<()> {
        assert_eq!(
            snapshot(self.root.path())?,
            self.initial_files,
            "planning must not modify config, sources, or storage"
        );
        self.client.cancel().await?;
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await??;
        assert!(status.success(), "MCP child failed: {status}");
        Ok(())
    }
}

fn snapshot(root: &Path) -> anyhow::Result<BTreeMap<String, Option<Vec<u8>>>> {
    fn visit(
        root: &Path,
        path: &Path,
        files: &mut BTreeMap<String, Option<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            let key = path.strip_prefix(root)?.to_string_lossy().into_owned();
            if entry.file_type()?.is_dir() {
                files.insert(key, None);
                visit(root, &path, files)?;
            } else {
                files.insert(key, Some(fs::read(path)?));
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

fn arguments() -> Value {
    json!({"workflow_toml": SOURCE, "prompt_files": [{"path": "prompt.txt", "content": PROMPT}]})
}

fn assert_invalid(value: &Value) {
    assert_eq!(value["status"], "error");
    assert_eq!(value["error"]["code"], "invalid_argument");
    assert!(!value.to_string().contains("UNTRUSTED_CANARY"));
}

#[tokio::test]
async fn stdio_plan_matches_cli_schema_without_daemon_targets_or_execution() -> anyhow::Result<()> {
    let session = Session::start().await?;
    let tools = session.client.list_all_tools().await?;
    assert_eq!(tools.len(), 10);
    let tool = tools
        .iter()
        .find(|t| t.name == "vyane_workflow_plan")
        .expect("static plan tool");
    let schema = serde_json::to_value(&tool.input_schema)?;
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["required"], json!(["workflow_toml"]));
    assert_eq!(
        schema["properties"]["workflow_toml"]["maxLength"],
        1_048_576
    );
    let value = session.call(arguments()).await?;
    let plan: WorkflowPlan = serde_json::from_value(value.clone())?;
    plan.verify()?;
    let expected =
        Workflow::from_path(session.root.path().join("workflow.toml"))?.compile_plan()?;
    assert_eq!(plan, expected);
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["steps"][0]["prompt_template"], PROMPT);
    assert_eq!(value["steps"][0]["system"], "caller system text");
    assert_eq!(value["steps"][0]["workdir"], "/caller-requested-workspace");
    assert_eq!(value["capability_manifest"]["maximum_sandbox"], "full");
    assert_eq!(value["steps"][1]["needs"], json!(["gather"]));
    assert_eq!(value["steps"][1]["route"]["effort"], "high");
    assert_eq!(session.call(arguments()).await?, value);
    let mut changed = arguments();
    changed["prompt_files"][0]["content"] = json!("Changed {{vars.topic}}");
    let changed = session.call(changed).await?;
    assert_ne!(changed["source_sha256"], value["source_sha256"]);
    assert_ne!(changed["plan_sha256"], value["plan_sha256"]);
    session.finish().await
}

#[tokio::test]
async fn stdio_plan_rejects_invalid_sources_graphs_and_authority_fields() -> anyhow::Result<()> {
    let session = Session::start().await?;
    // A real host prompt exists. Omitting it from the bundle must still fail.
    assert_invalid(&session.call(json!({"workflow_toml": SOURCE})).await?);
    for (key, value) in [
        ("vars", json!({"topic": "UNTRUSTED_CANARY"})),
        ("owner", json!("UNTRUSTED_CANARY")),
        ("caller_id", json!("UNTRUSTED_CANARY")),
        ("execution_cwd", json!("UNTRUSTED_CANARY")),
        ("token", json!("UNTRUSTED_CANARY")),
    ] {
        let mut args = arguments();
        args[key] = value;
        assert_invalid(&session.call(args).await?);
    }
    for source in [
        "[UNTRUSTED_CANARY",
        "",
        "[workflow]\nname='UNTRUSTED_CANARY'\n[[step]]\nid='a'\nneeds=['a']\nprompt='x'\ntarget='unknown'",
        "[workflow]\nname='UNTRUSTED_CANARY'\n[[step]]\nid='a'\nneeds=['missing']\nprompt='x'\ntarget='unknown'",
    ] {
        assert_invalid(&session.call(json!({"workflow_toml": source})).await?);
    }
    for path in [
        "../UNTRUSTED_CANARY",
        "/UNTRUSTED_CANARY",
        "a//UNTRUSTED_CANARY",
    ] {
        let mut args = arguments();
        args["prompt_files"][0]["path"] = json!(path);
        assert_invalid(&session.call(args).await?);
    }
    for files in [
        json!([{"path":"extra", "content":"UNTRUSTED_CANARY"}]),
        json!([{"path":"prompt.txt","content":"a"},{"path":"prompt.txt","content":"UNTRUSTED_CANARY"}]),
        json!([{"path":"prompt.txt","content":"a","extra":"UNTRUSTED_CANARY"}]),
    ] {
        let mut args = arguments();
        args["prompt_files"] = files;
        assert_invalid(&session.call(args).await?);
    }
    session.finish().await
}

#[tokio::test]
async fn stdio_plan_enforces_input_bytes_and_bounded_output() -> anyhow::Result<()> {
    let session = Session::start().await?;
    // Byte limits apply even though JSON Schema maxLength counts characters.
    for source in ["a".repeat(1_048_577), "界".repeat(400_000)] {
        assert_invalid(&session.call(json!({"workflow_toml": source})).await?);
    }
    let mut oversized = arguments();
    oversized["prompt_files"][0]["content"] = json!("a".repeat(4 * 1024 * 1024 + 1));
    assert_invalid(&session.call(oversized).await?);
    let mut long_path = arguments();
    long_path["prompt_files"][0]["path"] = json!("p".repeat(4097));
    assert_invalid(&session.call(long_path).await?);
    let mut too_many = arguments();
    too_many["prompt_files"] = json!(
        (0..129)
            .map(|i| json!({"path":format!("p{i}"),"content":""}))
            .collect::<Vec<_>>()
    );
    assert_invalid(&session.call(too_many).await?);
    let mut too_large = arguments();
    too_large["prompt_files"] = json!(
        (0..4)
            .map(|i| json!({"path":format!("p{i}"),"content":"a".repeat(4*1024*1024)}))
            .collect::<Vec<_>>()
    );
    assert_invalid(&session.call(too_large).await?);
    let mut large_plan = arguments();
    large_plan["prompt_files"][0]["content"] = json!("a".repeat(1024 * 1024));
    let value = session.call(large_plan).await?;
    assert_eq!(value["status"], "error");
    assert_eq!(value["error"]["code"], "limit_exceeded");
    assert!(value.to_string().len() < 1024);
    session.finish().await
}
