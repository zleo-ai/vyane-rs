// Package version plus an optional `git describe` suffix.
// `build.rs` includes this file. Keep it free of crate dependencies.

pub fn display_version(package: &str, describe: Option<&str>) -> String {
    let Some(describe) = describe.map(str::trim).filter(|value| !value.is_empty()) else {
        return package.to_string();
    };
    if describe == format!("v{package}") {
        return package.to_string();
    }
    format!("{package} ({describe})")
}

#[cfg(test)]
mod tests {
    use super::display_version;

    #[test]
    fn exact_tag_stays_the_package_version() {
        assert_eq!(display_version("0.1.0", Some("v0.1.0")), "0.1.0");
        assert_eq!(display_version("0.1.0", Some(" v0.1.0 ")), "0.1.0");
    }

    #[test]
    fn later_or_dirty_commits_keep_the_git_description() {
        assert_eq!(
            display_version("0.1.0", Some("v0.1.0-3-gabc1234")),
            "0.1.0 (v0.1.0-3-gabc1234)"
        );
        assert_eq!(
            display_version("0.1.0", Some("v0.1.0-dirty")),
            "0.1.0 (v0.1.0-dirty)"
        );
    }

    #[test]
    fn missing_description_uses_the_package_version() {
        assert_eq!(display_version("0.1.0", None), "0.1.0");
        assert_eq!(display_version("0.1.0", Some("  ")), "0.1.0");
    }
}
