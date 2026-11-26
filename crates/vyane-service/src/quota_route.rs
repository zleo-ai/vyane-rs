//! Local subscription-pace filter for automatic routing.
//!
//! The caller passes a snapshot path. This module reads that file, and only
//! that file: it does not probe a provider, open a network connection, or
//! choose a route by itself. [`narrow_auto_candidates`] returns the profile
//! names that remain eligible. The routing adapter applies that list.
//!
//! A missing, unreadable, stale, or schema-mismatched snapshot returns
//! `None`. The caller then keeps the unfiltered candidate list, so the route
//! matches the filter-off path. Extra JSON fields are not stored.

use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

const HOT_PACE: f64 = 1.3;
const LAGGING_PACE: f64 = 0.7;
const EXHAUSTED_PCT: f64 = 95.0;
const WEEK_MIN_MS: f64 = 6.0 * 24.0 * 3_600.0 * 1_000.0;
const WEEK_MAX_MS: f64 = 8.0 * 24.0 * 3_600.0 * 1_000.0;
const STALE_AFTER_MS: i64 = 45 * 60 * 1_000;
const FUTURE_TOLERANCE_MS: i64 = 60 * 1_000;
const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024;
const MAX_EPOCH_MS: i64 = (1_i64 << 53) - 1;
const MAX_WINDOW_ID_BYTES: usize = 256;

/// Separate model or product views of one shared pool. They are not the
/// subscription's main weekly budget, so they never decide the filter.
const SUBVIEW_GLOBS: &[&str] = &[
    "anthropic:7d:fable",
    "xai-oauth:product:*",
    "google-antigravity:*:3p-*",
];

/// Profile names to keep, in candidate order.
///
/// `candidates` are `(profile name, provider id)` pairs. The snapshot's
/// `provider` field is matched to that provider id. `None` means the snapshot
/// must not change the candidate list.
pub(crate) fn narrow_auto_candidates(
    candidates: &[(String, String)],
    path: &Path,
) -> Option<Vec<String>> {
    if candidates.is_empty() || path.as_os_str().is_empty() {
        return None;
    }
    let now_ms = unix_now_ms()?;
    let bytes = read_snapshot_bytes(path)?;
    let snapshot = fresh_snapshot(&bytes, now_ms)?;
    choose_pool(candidates, &snapshot)
}

fn choose_pool(candidates: &[(String, String)], snapshot: &SnapshotFile) -> Option<Vec<String>> {
    let mut known = Vec::new();
    for (name, provider) in candidates {
        if let Some(window) = weekly_window(snapshot, provider) {
            known.push((name.as_str(), window));
        }
    }
    if known.is_empty() {
        return None;
    }

    let healthy: Vec<&str> = known
        .iter()
        .filter(|(_, window)| !is_hot(window))
        .map(|(name, _)| *name)
        .collect();
    let lagging: Vec<&str> = healthy
        .iter()
        .copied()
        .filter(|name| {
            known
                .iter()
                .find(|(candidate, _)| candidate == name)
                .is_some_and(|(_, window)| is_lagging(window))
        })
        .collect();
    let selected: Vec<&str> = if !lagging.is_empty() {
        lagging
    } else if !healthy.is_empty() {
        healthy
    } else {
        let (name, _) = known.iter().min_by(|left, right| {
            heat(&left.1)
                .partial_cmp(&heat(&right.1))
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
        vec![*name]
    };
    if selected.is_empty() {
        return None;
    }
    Some(selected.into_iter().map(str::to_string).collect())
}

fn weekly_window(snapshot: &SnapshotFile, provider: &str) -> Option<WeeklyWindow> {
    let group = snapshot.providers.iter().find(|group| {
        group.provider == provider && group.kind.as_deref() == Some("subscription")
    })?;
    for window in &group.windows {
        let Some(id) = window.id.as_deref() else {
            continue;
        };
        if id.len() > MAX_WINDOW_ID_BYTES || is_subview(id) {
            continue;
        }
        let Some(duration) = window.duration_ms.as_ref().and_then(finite_f64) else {
            continue;
        };
        if !(WEEK_MIN_MS..=WEEK_MAX_MS).contains(&duration) {
            continue;
        }
        let used_pct = window.used_pct.as_ref().and_then(finite_f64)?;
        return Some(WeeklyWindow {
            used_pct,
            pace_ratio: window.pace_ratio.as_ref().and_then(finite_f64),
        });
    }
    None
}

fn is_hot(window: &WeeklyWindow) -> bool {
    window.used_pct >= EXHAUSTED_PCT || window.pace_ratio.is_some_and(|pace| pace > HOT_PACE)
}

fn is_lagging(window: &WeeklyWindow) -> bool {
    window.pace_ratio.is_some_and(|pace| pace < LAGGING_PACE)
}

fn heat(window: &WeeklyWindow) -> (u8, f64) {
    (
        u8::from(window.used_pct >= EXHAUSTED_PCT),
        window.pace_ratio.unwrap_or(0.0),
    )
}

fn is_subview(window_id: &str) -> bool {
    SUBVIEW_GLOBS
        .iter()
        .any(|pattern| glob_match(pattern, window_id))
}

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    let mut pattern_index = 0;
    let mut text_index = 0;
    let mut star_pattern = None;
    let mut star_text = 0;
    while text_index < text.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_pattern = Some(pattern_index);
            star_text = text_index;
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == text[text_index] {
            pattern_index += 1;
            text_index += 1;
        } else if let Some(star) = star_pattern {
            pattern_index = star + 1;
            star_text += 1;
            text_index = star_text;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn fresh_snapshot(bytes: &[u8], now_ms: i64) -> Option<SnapshotFile> {
    let snapshot: SnapshotFile = serde_json::from_slice(bytes).ok()?;
    if snapshot.schema_version != 1 {
        return None;
    }
    let generated = epoch_ms(snapshot.generated_at_ms.as_ref()?)?;
    if let Some(raw) = snapshot.invalidated_at_ms.as_ref() {
        let invalidated = epoch_ms(raw)?;
        if invalidated >= generated {
            return None;
        }
    }
    if snapshot.probe_status.as_deref() != Some("ok") {
        return None;
    }
    let age = now_ms.checked_sub(generated)?;
    if !(-FUTURE_TOLERANCE_MS..=STALE_AFTER_MS).contains(&age) {
        return None;
    }
    Some(snapshot)
}

fn epoch_ms(number: &serde_json::Number) -> Option<i64> {
    if let Some(value) = number.as_i64() {
        return (0..=MAX_EPOCH_MS).contains(&value).then_some(value);
    }
    let value = number.as_u64()?;
    let max = u64::try_from(MAX_EPOCH_MS).ok()?;
    if value > max {
        return None;
    }
    i64::try_from(value).ok()
}

fn finite_f64(number: &serde_json::Number) -> Option<f64> {
    let value = number.as_f64()?;
    value.is_finite().then_some(value)
}

fn unix_now_ms() -> Option<i64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(elapsed.as_millis()).ok()
}

fn read_snapshot_bytes(path: &Path) -> Option<Vec<u8>> {
    let file = open_snapshot(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > MAX_SNAPSHOT_BYTES {
        return None;
    }
    let mut buffer = Vec::new();
    file.take(MAX_SNAPSHOT_BYTES.saturating_add(1))
        .read_to_end(&mut buffer)
        .ok()?;
    if u64::try_from(buffer.len()).ok()? > MAX_SNAPSHOT_BYTES {
        return None;
    }
    Some(buffer)
}

fn open_snapshot(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(unix_nonblock_flag())
            .open(path)
    }
    #[cfg(not(unix))]
    {
        std::fs::File::open(path)
    }
}

#[cfg(unix)]
const fn unix_nonblock_flag() -> i32 {
    // Linux and Android use 0x800. Other Unix targets this crate builds use 0x4.
    if cfg!(any(target_os = "linux", target_os = "android")) {
        0x800
    } else {
        0x4
    }
}

#[derive(Clone, Copy)]
struct WeeklyWindow {
    used_pct: f64,
    pace_ratio: Option<f64>,
}

#[derive(Deserialize)]
struct SnapshotFile {
    schema_version: u64,
    #[serde(default)]
    generated_at_ms: Option<serde_json::Number>,
    #[serde(default)]
    invalidated_at_ms: Option<serde_json::Number>,
    #[serde(default)]
    probe_status: Option<String>,
    providers: Vec<ProviderGroup>,
}

#[derive(Deserialize)]
struct ProviderGroup {
    provider: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    windows: Vec<WindowRecord>,
}

#[derive(Deserialize)]
struct WindowRecord {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    duration_ms: Option<serde_json::Number>,
    #[serde(default)]
    used_pct: Option<serde_json::Number>,
    #[serde(default)]
    pace_ratio: Option<serde_json::Number>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subview_globs_skip_product_windows_and_keep_main_ids() {
        assert!(is_subview("anthropic:7d:fable"));
        assert!(is_subview("xai-oauth:product:side"));
        assert!(is_subview("google-antigravity:week:3p-extra"));
        assert!(!is_subview("busy-provider:7d"));
        assert!(!is_subview("free-provider:7d"));
    }

    #[test]
    fn pace_boundaries_use_the_builtin_thresholds() {
        let on_hot_pace = WeeklyWindow {
            used_pct: 10.0,
            pace_ratio: Some(HOT_PACE),
        };
        let above_hot_pace = WeeklyWindow {
            used_pct: 10.0,
            pace_ratio: Some(HOT_PACE + 0.01),
        };
        let exhausted = WeeklyWindow {
            used_pct: EXHAUSTED_PCT,
            pace_ratio: Some(0.1),
        };
        let on_lagging_pace = WeeklyWindow {
            used_pct: 10.0,
            pace_ratio: Some(LAGGING_PACE),
        };
        let below_lagging_pace = WeeklyWindow {
            used_pct: 10.0,
            pace_ratio: Some(LAGGING_PACE - 0.1),
        };

        assert!(!is_hot(&on_hot_pace));
        assert!(is_hot(&above_hot_pace));
        assert!(is_hot(&exhausted));
        assert!(!is_lagging(&on_lagging_pace));
        assert!(is_lagging(&below_lagging_pace));
    }

    #[test]
    fn freer_subscription_removes_the_busy_profile_and_keeps_order() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let week_ms = 7 * 24 * 3_600 * 1_000;
        std::fs::write(
            &path,
            serde_json::to_string(&serde_json::json!({
                "schema_version": 1,
                "generated_at_ms": now,
                "probe_status": "ok",
                "providers": [
                    provider("busy-provider", 80.0, 2.0, week_ms),
                    provider("steady-provider", 30.0, 1.0, week_ms),
                    provider("free-provider", 10.0, 0.4, week_ms),
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let candidates = [
            ("metered".into(), "metered-provider".into()),
            ("busy".into(), "busy-provider".into()),
            ("steady".into(), "steady-provider".into()),
            ("free".into(), "free-provider".into()),
        ];
        let pool = narrow_auto_candidates(&candidates, &path).unwrap();
        assert_eq!(pool, vec!["free".to_string()]);
    }

    #[test]
    fn all_hot_subscriptions_keep_the_least_hot_profile() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let week_ms = 7 * 24 * 3_600 * 1_000;
        std::fs::write(
            &path,
            serde_json::to_string(&serde_json::json!({
                "schema_version": 1,
                "generated_at_ms": now,
                "probe_status": "ok",
                "providers": [
                    provider("alpha-provider", 50.0, 2.0, week_ms),
                    provider("beta-provider", 96.0, 1.1, week_ms),
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let candidates = [
            ("metered".into(), "metered-provider".into()),
            ("beta".into(), "beta-provider".into()),
            ("alpha".into(), "alpha-provider".into()),
        ];
        let pool = narrow_auto_candidates(&candidates, &path).unwrap();
        assert_eq!(pool, vec!["alpha".to_string()]);
    }

    fn provider(name: &str, used_pct: f64, pace_ratio: f64, duration_ms: i64) -> serde_json::Value {
        serde_json::json!({
            "provider": name,
            "kind": "subscription",
            "windows": [{
                "id": format!("{name}:7d"),
                "duration_ms": duration_ms,
                "used_pct": used_pct,
                "pace_ratio": pace_ratio,
            }]
        })
    }
}
