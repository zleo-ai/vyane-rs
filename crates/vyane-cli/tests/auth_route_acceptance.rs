//! 经真实 CLI 子进程验证安全动词的标签推断和配置选择，无需联网。

use std::fs;

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;

fn route_fixture() -> TempDir {
    let directory = TempDir::new().expect("创建测试目录");
    fs::write(
        directory.path().join("config.toml"),
        r#"
[providers.fixture]
base_url = "https://routing.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "general-model"

[profiles.general]
provider = "fixture"
protocol = "openai_chat"
harness = "none"
model = "general-model"
tier = "economy"

[profiles.security]
provider = "fixture"
protocol = "openai_chat"
harness = "none"
model = "security-model"
tier = "economy"
tags = ["security"]
"#,
    )
    .expect("写入测试配置");
    directory
}

fn assert_route(directory: &TempDir, task: &str, security: bool) {
    let assertion = Command::cargo_bin("vyane")
        .expect("读取真实 vyane 二进制")
        .env_remove("VYANE_MANAGED_PERMISSION_CONFIG")
        .env_remove("VYANE_MANAGED_NATIVE_CONFIG")
        .current_dir(directory.path())
        .arg("--config")
        .arg(directory.path().join("config.toml"))
        .args(["route", task, "--json"])
        .assert()
        .success();
    let decision: Value =
        serde_json::from_slice(&assertion.get_output().stdout).expect("解析真实 CLI 的路由结果");
    assert_eq!(decision["provider"], "fixture", "{task}: {decision}");
    assert_eq!(
        decision["selection_key"],
        if security { "security" } else { "general" },
        "{task}: {decision}"
    );
    assert_eq!(
        decision["model"],
        if security {
            "security-model"
        } else {
            "general-model"
        },
        "{task}: {decision}"
    );
    assert_eq!(
        decision["tag"] == "security",
        security,
        "{task}: {decision}"
    );
}

#[test]
fn route_auth_inflections_select_security_profile() {
    let directory = route_fixture();
    for task in [
        "authenticate the user",
        "add authenticated endpoints to the app",
        "authenticates every request",
        "authenticating the session",
        "authorize this action",
        "the request was authorized",
        "authorizes admin users",
        "authorizing the transaction",
        "(AUTHENTICATED), Authorizing!",
        "重新 authenticating 会话",
        "reauthenticate first; authorize next",
        // 保留主线原有的独立 auth 和两个名词行为。
        "auth",
        "authentication",
        "authorization",
    ] {
        assert_route(&directory, task, true);
    }
}

#[test]
fn route_auth_boundaries_keep_general_profile() {
    let directory = route_fixture();
    for word in [
        "author",
        "authors",
        "authority",
        "authoritative",
        "authentic",
        "authenticity",
        "authenticator",
        "reauthenticate",
        "unauthorized",
    ] {
        assert_route(&directory, word, false);
    }
    for word in [
        "authenticate",
        "authenticated",
        "authenticates",
        "authenticating",
        "authorize",
        "authorized",
        "authorizes",
        "authorizing",
    ] {
        for task in [format!("x{word}"), format!("{word}x")] {
            assert_route(&directory, &task, false);
        }
    }
}
