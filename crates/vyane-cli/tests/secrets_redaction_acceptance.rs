#![allow(clippy::unwrap_used)]

mod common;

use assert_cmd::Command;

#[test]
fn check_does_not_echo_malformed_secret_lines() {
    const CANARY: &str = "AUDIT_CANARY_NOT_A_SECRET";
    for (line, reason) in [
        (CANARY.to_string(), "expected `KEY=VALUE`"),
        (format!("={CANARY}"), "empty key"),
    ] {
        let directory = common::private_tempdir();
        let config = directory.path().join("profiles.toml");
        std::fs::write(&config, "").unwrap();
        std::fs::write(
            directory.path().join("secrets.env"),
            format!("# synthetic fixture\n{line}\n"),
        )
        .unwrap();

        let result = Command::cargo_bin("vyane")
            .unwrap()
            .env_clear()
            .env("HOME", directory.path())
            .env("XDG_CONFIG_HOME", directory.path())
            .env("XDG_DATA_HOME", directory.path())
            .current_dir(directory.path())
            .arg("--config")
            .arg(&config)
            .arg("check")
            .output()
            .unwrap();

        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        let stderr = String::from_utf8(result.stderr).unwrap();
        assert!(stderr.starts_with("config error:"), "{stderr}");
        assert!(stderr.contains("load secrets"), "{stderr}");
        assert!(
            stderr.contains(&format!("secrets.env:2: {reason}")),
            "{stderr}"
        );
        assert!(!stderr.contains(CANARY), "{stderr}");
    }
}
