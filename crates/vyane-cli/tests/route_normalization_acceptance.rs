use std::fs;

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;

// 运行真实 CLI 子进程：经过参数解析、配置插入、服务适配器和路由器，
// 同时断言所选目标与诊断标签，避免把回退目标误当成标签匹配成功。
fn route(config_tag: &str, input_tags: &str) -> Value {
    let dir = TempDir::new().expect("创建临时目录");
    let config = dir.path().join("config.toml");
    fs::write(
        &config,
        format!(
            r#"
            [providers.fallback]
            base_url = "http://localhost"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "fallback-model"

            [providers.selected]
            base_url = "http://localhost"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "selected-model"

            [profiles.cheap]
            provider = "fallback"
            protocol = "openai_chat"
            harness = "none"
            model = "fallback-model"
            tier = "economy"

            [profiles.tagged]
            provider = "selected"
            protocol = "openai_chat"
            harness = "none"
            model = "selected-model"
            tier = "mainline"
            tags = ["{config_tag}"]
            "#
        ),
    )
    .expect("写入配置");

    let output = Command::cargo_bin("vyane")
        .expect("定位真实二进制")
        .arg("--config")
        .arg(config)
        .args(["route", "say hello", "--tags", input_tags, "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).expect("解析路由 JSON")
}

#[test]
fn route_cli_normalized_tags_agree_with_selected_target() {
    for (config_tag, input_tag, diagnostic_tag) in [
        ("front-end", "front end", "front end"),
        ("front end", "front-end", "front-end"),
        ("Front-End", "  Front End  ", "Front End"),
        ("web-frontend", "web/frontend", "web/frontend"),
        ("front-end", "unknown,front end,front-end", "front end"),
    ] {
        let decision = route(config_tag, input_tag);
        assert_eq!(decision["selection_key"], "tagged", "{input_tag}");
        assert_eq!(decision["provider"], "selected", "{input_tag}");
        assert_eq!(decision["model"], "selected-model", "{input_tag}");
        assert_eq!(decision["tag"], diagnostic_tag, "{input_tag}");
    }
}

#[test]
fn route_cli_unmatched_tag_has_no_diagnostic_match() {
    let decision = route("front-end", "unknown");
    assert_eq!(decision["selection_key"], "cheap");
    assert_eq!(decision["provider"], "fallback");
    assert_eq!(decision["model"], "fallback-model");
    assert_eq!(decision["tag"], "");
}
