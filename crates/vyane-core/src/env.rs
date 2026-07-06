//! Child-process environment policy.
//!
//! Spawning a CLI harness with the parent's full environment is a real-world
//! footgun: credentials and base-URL overrides from the *calling* agent
//! session leak into the child and silently redirect or break its
//! authentication (e.g. a child meant to use provider A's key inherits
//! provider B's `*_API_KEY` / `*_BASE_URL` overrides and starts failing with
//! 401s — or worse, quietly bills the wrong account).
//!
//! Vyane therefore spawns harnesses **scrubbed by default**: a minimal
//! baseline of the parent environment, plus an explicit per-target injection
//! set derived from configuration. A run's environment is self-contained and
//! reproducible; inheriting the full parent environment is an opt-in.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Parent variables that survive scrubbing. Everything a well-behaved CLI
/// needs to start, nothing that redirects model traffic.
pub const BASELINE_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "SHELL",
    "TERM",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "TZ",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_CACHE_HOME",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InheritMode {
    /// Start from [`BASELINE_ENV`] only (default).
    #[default]
    Scrub,
    /// Inherit the full parent environment. Opt-in; injected values still win.
    Full,
}

/// Policy for building a child process environment.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnvPolicy {
    #[serde(default)]
    pub mode: InheritMode,
    /// Extra parent variables to allow through when scrubbing.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Variables injected for this run (auth, base URL, model overrides).
    /// Injection always wins over inherited values.
    #[serde(default)]
    pub inject: BTreeMap<String, String>,
}

impl EnvPolicy {
    /// The default policy: scrubbed baseline, nothing extra.
    pub fn scrubbed() -> Self {
        Self::default()
    }

    /// Add an injected variable.
    pub fn inject(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inject.insert(key.into(), value.into());
        self
    }

    /// Build the concrete child environment from a snapshot of the parent's.
    ///
    /// Pure function of `(policy, parent)` — the same inputs always produce
    /// the same child environment.
    pub fn build(
        &self,
        parent: impl IntoIterator<Item = (String, String)>,
    ) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        match self.mode {
            InheritMode::Full => {
                out.extend(parent);
            }
            InheritMode::Scrub => {
                let parent: BTreeMap<String, String> = parent.into_iter().collect();
                for key in BASELINE_ENV {
                    if let Some(v) = parent.get(*key) {
                        out.insert((*key).to_string(), v.clone());
                    }
                }
                for key in &self.allow {
                    if let Some(v) = parent.get(key) {
                        out.insert(key.clone(), v.clone());
                    }
                }
            }
        }
        for (k, v) in &self.inject {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent() -> Vec<(String, String)> {
        [
            ("PATH", "/usr/bin"),
            ("HOME", "/home/u"),
            ("SOME_API_KEY", "parent-secret"),
            ("SOME_BASE_URL", "https://parent.example"),
            ("EDITOR", "vim"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    #[test]
    fn scrub_drops_credential_lookalikes() {
        let env = EnvPolicy::scrubbed().build(parent());
        assert_eq!(env.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert!(!env.contains_key("SOME_API_KEY"));
        assert!(!env.contains_key("SOME_BASE_URL"));
        assert!(!env.contains_key("EDITOR"));
    }

    #[test]
    fn allowlist_lets_named_vars_through() {
        let mut policy = EnvPolicy::scrubbed();
        policy.allow.push("EDITOR".to_string());
        let env = policy.build(parent());
        assert_eq!(env.get("EDITOR").map(String::as_str), Some("vim"));
        assert!(!env.contains_key("SOME_API_KEY"));
    }

    #[test]
    fn injection_wins_over_parent_even_in_full_mode() {
        let mut policy = EnvPolicy {
            mode: InheritMode::Full,
            ..Default::default()
        };
        policy
            .inject
            .insert("SOME_API_KEY".into(), "child-secret".into());
        let env = policy.build(parent());
        assert_eq!(
            env.get("SOME_API_KEY").map(String::as_str),
            Some("child-secret")
        );
        assert_eq!(env.get("EDITOR").map(String::as_str), Some("vim"));
    }

    #[test]
    fn build_is_deterministic() {
        let policy = EnvPolicy::scrubbed().inject("A", "1");
        assert_eq!(policy.build(parent()), policy.build(parent()));
    }
}
