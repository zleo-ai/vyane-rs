//! 从真实 dispatch 子进程和落盘用量进入公共估算接口。
//! dispatch 目前不会自动填充 cost_usd；此处验证它实际产出的 Usage。

use std::fs;
use std::path::Path;

use assert_cmd::Command;
use serde_json::{Value, json};
use tempfile::TempDir;
use vyane_core::{RunRecord, Usage};
use vyane_ledger::{ModelPricing, PriceTable};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn dispatch_record(config: &str, binary_dir: Option<&Path>) -> RunRecord {
    let dir = TempDir::new().expect("创建临时目录");
    let config_path = dir.path().join("config.toml");
    fs::write(&config_path, config).expect("写入测试配置");
    let mut command = Command::cargo_bin("vyane").expect("找到 vyane 程序");
    command
        .env("VYANE_DATA_DIR", dir.path())
        .arg("--config")
        .arg(config_path)
        .args(["dispatch", "billing fixture", "--target", "test", "--json"]);
    if let Some(binary_dir) = binary_dir {
        command.env("PATH", binary_dir);
    }
    let output = command.assert().success().get_output().stdout.clone();
    let output: Value = serde_json::from_slice(&output).expect("解析 dispatch JSON");
    let record: RunRecord =
        serde_json::from_value(output["record"].clone()).expect("读取实际运行记录");
    assert_eq!(output["output"], "billing answer");
    assert_eq!(output["record"]["status"], "success");

    let ledger = fs::read_to_string(dir.path().join("ledger.jsonl")).expect("读取实际账本");
    let records: Vec<RunRecord> = ledger
        .lines()
        .map(|line| serde_json::from_str(line).expect("解析账本行"))
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].run_id, record.run_id);
    assert_eq!(records[0].usage, record.usage);
    records.into_iter().next().expect("账本中的运行记录")
}

fn assert_estimate(record: &RunRecord, pricing: ModelPricing, expected: f64) {
    let table =
        PriceTable::new().with_overrides([(record.target.model.as_str().to_string(), pricing)]);
    let usage = record.usage.as_ref().expect("真实入口应报告用量");
    let actual = table
        .estimate(&record.target.model, usage)
        .expect("已配置测试费率");
    assert!(
        (actual - expected).abs() < 1e-12,
        "实际估算 {actual}，预期 {expected}；用量 {usage:?}"
    );
}

#[cfg(unix)]
#[test]
fn claude_dispatch_cache_subset_is_not_double_billed() {
    use std::os::unix::fs::PermissionsExt;

    let bin_dir = TempDir::new().expect("创建子进程目录");
    let binary = bin_dir.path().join("claude");
    fs::write(
        &binary,
        r#"#!/bin/sh
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"billing answer","session_id":"billing-session","usage":{"input_tokens":10,"cache_creation_input_tokens":5,"cache_read_input_tokens":3,"output_tokens":7}}'
"#,
    )
    .expect("写入 CLI 夹具");
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).expect("使夹具可以执行");

    let record = dispatch_record(
        r#"
        [providers.native]
        base_url = "https://unused.invalid"
        auth_style = "x_api_key"
        protocol = "anthropic_messages"

        [profiles.test]
        provider = "native"
        protocol = "anthropic_messages"
        harness = "claude-code"
        model = "billing-fixture"
        "#,
        Some(bin_dir.path()),
    );
    assert_eq!(
        record.usage,
        Some(Usage {
            input_tokens: 18,
            output_tokens: 7,
            cached_input_tokens: Some(3),
            reasoning_tokens: None,
        })
    );
    // 15 个非缓存输入、3 个缓存输入、7 个输出，分别计费一次。
    assert_estimate(
        &record,
        ModelPricing::per_1m(3.0, 15.0).with_cache(0.3),
        0.000_150_9,
    );
}

async fn openai_dispatch_record(protocol: &str, cached: u64, reasoning: u64) -> RunRecord {
    let server = MockServer::start().await;
    let (endpoint, body) = match protocol {
        "openai_chat" => (
            "/v1/chat/completions",
            json!({
                "model": "billing-fixture",
                "choices": [{"message": {"role": "assistant", "content": "billing answer"}, "finish_reason": "stop"}],
                "usage": {
                    "prompt_tokens": 1_000, "completion_tokens": 1_000,
                    "prompt_tokens_details": {"cached_tokens": cached},
                    "completion_tokens_details": {"reasoning_tokens": reasoning}
                }
            }),
        ),
        "openai_responses" => (
            "/v1/responses",
            json!({
                "id": "billing-response", "status": "completed", "model": "billing-fixture",
                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "billing answer"}]}],
                "usage": {
                    "input_tokens": 1_000, "output_tokens": 1_000,
                    "input_tokens_details": {"cached_tokens": cached},
                    "output_tokens_details": {"reasoning_tokens": reasoning}
                }
            }),
        ),
        _ => panic!("测试协议未定义"),
    };
    Mock::given(method("POST"))
        .and(path(endpoint))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&server)
        .await;

    let record = dispatch_record(
        &format!(
            r#"
            [providers.fixture]
            base_url = "{}"
            auth_style = "bearer"
            protocol = "{protocol}"

            [profiles.test]
            provider = "fixture"
            protocol = "{protocol}"
            harness = "none"
            model = "billing-fixture"
            "#,
            server.uri()
        ),
        None,
    );
    assert_eq!(
        record.usage,
        Some(Usage {
            input_tokens: 1_000,
            output_tokens: 1_000,
            cached_input_tokens: Some(cached),
            reasoning_tokens: Some(reasoning),
        })
    );
    record
}

// 真实 CLI 同步等待 HTTP 响应，因此服务器需要独立的运行线程。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openai_dispatch_subsets_replace_parent_rates() {
    for protocol in ["openai_chat", "openai_responses"] {
        let record = openai_dispatch_record(protocol, 900, 400).await;
        // 输入 100*1 + 900*0.1，输出 600*2 + 400*8，合计除以一百万。
        assert_estimate(
            &record,
            ModelPricing::per_1m(1.0, 2.0)
                .with_cache(0.1)
                .with_reasoning(8.0),
            0.004_59,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openai_dispatch_oversized_subsets_are_clamped() {
    for protocol in ["openai_chat", "openai_responses"] {
        let record = openai_dispatch_record(protocol, 5_000, 4_000).await;
        assert_estimate(
            &record,
            ModelPricing::per_1m(1.0, 2.0)
                .with_cache(0.1)
                .with_reasoning(8.0),
            0.008_1,
        );
    }
}
