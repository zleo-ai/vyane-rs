//! Cross-view identity for POSIX paths, WSL UNC paths, Windows drives, and
//! loopback URLs.
//!
//! The translation is pure. It does not probe the network, does not list WSL
//! distros, and does not treat a WSL listener and a Windows listener as the
//! same socket unless the caller marks loopback as mirrored. Dispatch,
//! worktree isolation, harness adapters, and the CLI stay unchanged.
//!
//! [`identify_path`] judges a locator from its text. A caller may pass
//! `windows_absolute_path` or `posix_absolute_path` (the same strings as
//! [`local_locator_kind`](crate::pathsformats::local_locator_kind)). When
//! that argument is absent, only the string shape is used. A Win32 `\\?\`
//! prefix stays in the text; a caller that wants it removed can pass the
//! result of [`strip_extended_path_prefix`](crate::pathsformats::strip_extended_path_prefix)
//! first.

use std::path::Path;

/// Canonical identity for one filesystem locator across OS views.
///
/// `scheme` is `wsl`, `windows_drive`, or `unknown`. `confidence` is
/// `exact`, `partial`, or `unknown`. `reason` is one of `empty_locator`,
/// `unc_distro_invalid`, `wsl_unc`, `windows_locator_unparsed`,
/// `windows_drive`, `wsl_mnt_drive`, `wsl_distro_unknown`, `wsl_posix`, or
/// `unrecognized_locator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathIdentity {
    /// `wsl`, `windows_drive`, or `unknown`.
    pub scheme: &'static str,
    /// POSIX spelling of the locator, when one exists.
    pub posix: Option<String>,
    /// `wsl.localhost` UNC spelling, when the distro is known.
    pub windows_unc: Option<String>,
    /// `wsl$` admin UNC spelling, when the distro is known.
    pub windows_unc_admin: Option<String>,
    /// Windows drive spelling, when the locator is a drive path.
    pub windows_drive: Option<String>,
    /// WSL distro name, when the locator carries one.
    pub distro: Option<String>,
    /// Key shared by locators that name the same place.
    pub identity_key: String,
    /// `exact`, `partial`, or `unknown`.
    pub confidence: &'static str,
    /// Why the identity was built. See the struct docs for the literals.
    pub reason: &'static str,
}

/// Loopback URL mapping.
///
/// `cross_os_same_listener` is `unknown` or `same_listener`. `reason` is
/// `empty_url`, `unparsed_url`, `unsupported_scheme`, `not_loopback`,
/// `mirrored_loopback`, or `cross_os_unproven`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalhostMapping {
    /// Stripped URL, or the original text when that text is empty.
    pub original: String,
    /// Hostname from the URL, when the split succeeded.
    pub origin_host: Option<String>,
    /// Explicit port, when the split succeeded and the port is in range.
    pub port: Option<u16>,
    /// `127.0.0.1` and `localhost` spellings of one loopback URL.
    pub same_os_equivalents: Vec<String>,
    /// `unknown` or `same_listener`.
    pub cross_os_same_listener: &'static str,
    /// Why the mapping stopped where it did.
    pub reason: &'static str,
}

/// Trust-gated alias payload for one locator.
///
/// [`display_path_aliases`] returns `None` when `trusted` is false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayPathAliases {
    /// Identity of `display_locator`.
    pub identity: PathIdentity,
    /// Other spellings of the same locator, omitting the original text.
    pub aliases: Vec<String>,
    /// Locator text the caller passed in.
    pub display_locator: String,
}

const WSL_LOCALHOST_SHARE: &str = "wsl.localhost";
const WSL_ADMIN_SHARE: &str = "wsl$";
const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "localhost", "::1", "[::1]"];

/// Return the cross-view identity of one locator.
///
/// `distro` is required before a native POSIX path gains WSL UNC aliases.
/// A missing distro stays partial with reason `wsl_distro_unknown`.
#[must_use]
pub fn identify_path(
    locator: &str,
    locator_kind: Option<&str>,
    distro: Option<&str>,
) -> PathIdentity {
    let raw = python_strip(locator);
    if raw.is_empty() {
        return unknown_identity("empty_locator");
    }
    let known_distro = normalize_distro(distro);
    match classify_unc(raw) {
        UncClass::InvalidDistro => return unknown_identity("unc_distro_invalid"),
        UncClass::Parsed { distro, posix } => return identity_from_unc(distro, posix),
        UncClass::Absent => {}
    }
    if let Some((letter, rest)) = parse_windows_drive(raw) {
        return identity_from_windows_drive(letter, rest);
    }
    if locator_kind == Some("windows_absolute_path") {
        return unknown_identity("windows_locator_unparsed");
    }
    if raw.starts_with('/') || locator_kind == Some("posix_absolute_path") {
        let posix = if raw.starts_with('/') {
            raw.to_owned()
        } else {
            format!("/{raw}")
        };
        if let Some((letter, tail)) = parse_mnt(&posix) {
            return identity_from_posix_mnt(posix, letter, &tail);
        }
        return identity_from_posix(posix, known_distro);
    }
    unknown_identity("unrecognized_locator")
}

/// True only when both locators have confidence `exact` and one identity key.
#[must_use]
pub fn paths_refer_to_same_location(left: &str, right: &str, distro: Option<&str>) -> bool {
    let left = identify_path(left, None, distro);
    let right = identify_path(right, None, distro);
    left.confidence == "exact"
        && right.confidence == "exact"
        && left.identity_key == right.identity_key
}

/// Alias payload for a trusted caller.
///
/// `trusted` false returns `None`. A trusted payload lists spellings other
/// than `locator` itself.
#[must_use]
pub fn display_path_aliases(
    locator: &str,
    locator_kind: Option<&str>,
    distro: Option<&str>,
    trusted: bool,
) -> Option<DisplayPathAliases> {
    if !trusted {
        return None;
    }
    let identity = identify_path(locator, locator_kind, distro);
    let mut aliases = Vec::new();
    for value in [
        identity.posix.as_deref(),
        identity.windows_unc.as_deref(),
        identity.windows_unc_admin.as_deref(),
        identity.windows_drive.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if value.is_empty() || value == locator || aliases.iter().any(|existing| existing == value)
        {
            continue;
        }
        aliases.push(value.to_owned());
    }
    Some(DisplayPathAliases {
        identity,
        aliases,
        display_locator: locator.to_owned(),
    })
}

/// Return the gitdir path from a worktree `.git` file, or `None`.
#[must_use]
pub fn parse_gitdir_pointer(contents: &str) -> Option<String> {
    let text = python_strip(contents);
    if text.is_empty() {
        return None;
    }
    let first = python_strip(python_first_line(text));
    let rest = strip_prefix_ignore_ascii_case(first, "gitdir:")?;
    let pointer = python_strip(rest).trim_matches('"');
    if pointer.is_empty() {
        None
    } else {
        Some(pointer.to_owned())
    }
}

/// Map a worktree gitdir onto the shared `.git` directory.
///
/// A primary checkout ends in `/.git`. A linked worktree contains
/// `/.git/worktrees/<name>`. Any other shape returns `None`.
#[must_use]
pub fn worktree_common_git_dir(gitdir: &str) -> Option<String> {
    let raw = python_strip(gitdir).trim_end_matches(['/', '\\']);
    if raw.is_empty() {
        return None;
    }
    let normalized = raw.replace('\\', "/");
    const MARKER: &str = "/.git/worktrees/";
    if let Some(index) = rfind_ignore_ascii_case(&normalized, MARKER) {
        let end = index + "/.git".len();
        debug_assert!(normalized.is_char_boundary(end));
        return Some(normalized[..end].to_owned());
    }
    if ends_with_ignore_ascii_case(&normalized, "/.git") {
        return Some(normalized);
    }
    None
}

/// Stable identity for every worktree that shares one git common dir.
///
/// Confidence `unknown` returns `None`. Partial confidence still yields a key.
#[must_use]
pub fn checkout_identity_key(common_git_dir: &str, distro: Option<&str>) -> Option<String> {
    let fallback = python_strip(common_git_dir);
    let common = worktree_common_git_dir(common_git_dir).unwrap_or_else(|| fallback.to_owned());
    if common.is_empty() {
        return None;
    }
    let identity = identify_path(&common, None, distro);
    if identity.confidence == "unknown" {
        None
    } else {
        Some(format!("git:{}", identity.identity_key))
    }
}

/// Read `path/.git` when it is present and return the shared worktree identity.
///
/// A missing, unreadable, or non-UTF-8 entry returns `None`.
#[must_use]
pub fn probe_checkout_identity(path: impl AsRef<Path>, distro: Option<&str>) -> Option<String> {
    let git_entry = path.as_ref().join(".git");
    let metadata = std::fs::metadata(&git_entry).ok()?;
    if metadata.is_file() {
        let text = read_git_text(&git_entry)?;
        let pointer = parse_gitdir_pointer(&text)?;
        let common = worktree_common_git_dir(&pointer);
        return checkout_identity_key(common.as_deref().unwrap_or(&pointer), distro);
    }
    if metadata.is_dir() {
        let text = git_entry.to_str()?;
        return checkout_identity_key(text, distro);
    }
    None
}

/// Map a loopback URL.
///
/// `loopback_mirrored` `None` reads `VYANE_LOOPBACK_MIRRORED`. Only `1`,
/// `true`, or `yes` (after trimming) mark a mirrored listener. Any other
/// value leaves `cross_os_same_listener` as `unknown`.
#[must_use]
pub fn map_localhost_url(url: &str, loopback_mirrored: Option<bool>) -> LocalhostMapping {
    let raw = python_strip(url);
    if raw.is_empty() {
        return unknown_url(url, None, None, "empty_url");
    }
    let env_value = std::env::var("VYANE_LOOPBACK_MIRRORED").ok();
    let mirrored = resolve_mirrored(loopback_mirrored, env_value.as_deref());
    map_parsed_localhost(raw, mirrored)
}

/// Distro name from `WSL_DISTRO_NAME`, or `None` when that variable is absent.
///
/// An empty value, or one that contains `\`, `/`, or NUL, is absent.
#[must_use]
pub fn default_wsl_distro() -> Option<String> {
    normalize_distro(std::env::var("WSL_DISTRO_NAME").ok().as_deref())
}

fn unknown_identity(reason: &'static str) -> PathIdentity {
    PathIdentity {
        scheme: "unknown",
        posix: None,
        windows_unc: None,
        windows_unc_admin: None,
        windows_drive: None,
        distro: None,
        identity_key: "unknown:".to_owned(),
        confidence: "unknown",
        reason,
    }
}

fn identity_from_unc(distro: String, posix: String) -> PathIdentity {
    let (windows_unc, windows_unc_admin) = unc_pair(&posix, &distro);
    let (scheme, windows_drive, identity_key) = if let Some((letter, tail)) = parse_mnt(&posix) {
        let rest = mnt_group_rest(&tail);
        let (windows_drive, identity_key) = drive_letter_forms(letter, rest);
        ("windows_drive", Some(windows_drive), identity_key)
    } else {
        let identity_key = format!("wsl:{}:{posix}", casefold(&distro));
        ("wsl", None, identity_key)
    };
    PathIdentity {
        scheme,
        posix: Some(posix),
        windows_unc: Some(windows_unc),
        windows_unc_admin: Some(windows_unc_admin),
        windows_drive,
        distro: Some(distro),
        identity_key,
        confidence: "exact",
        reason: "wsl_unc",
    }
}

fn identity_from_windows_drive(letter: char, rest_raw: &str) -> PathIdentity {
    let rest = rest_raw.replace('\\', "/");
    let rest_stripped = rest.trim_matches('/');
    let posix = posix_for_drive(letter, rest_stripped);
    let (windows_drive, identity_key) = drive_letter_forms(letter, rest_stripped);
    PathIdentity {
        scheme: "windows_drive",
        posix: Some(posix),
        windows_unc: None,
        windows_unc_admin: None,
        windows_drive: Some(windows_drive),
        distro: None,
        identity_key,
        confidence: "exact",
        reason: "windows_drive",
    }
}

fn identity_from_posix_mnt(posix: String, letter: char, tail: &MntTail) -> PathIdentity {
    let rest = mnt_group_rest(tail);
    let (windows_drive, identity_key) = drive_letter_forms(letter, rest);
    PathIdentity {
        scheme: "windows_drive",
        posix: Some(posix),
        windows_unc: None,
        windows_unc_admin: None,
        windows_drive: Some(windows_drive),
        distro: None,
        identity_key,
        confidence: "exact",
        reason: "wsl_mnt_drive",
    }
}

fn identity_from_posix(posix: String, distro: Option<String>) -> PathIdentity {
    let Some(distro) = distro else {
        let identity_key = format!("posix:{posix}");
        return PathIdentity {
            scheme: "wsl",
            posix: Some(posix),
            windows_unc: None,
            windows_unc_admin: None,
            windows_drive: None,
            distro: None,
            identity_key,
            confidence: "partial",
            reason: "wsl_distro_unknown",
        };
    };
    let (windows_unc, windows_unc_admin) = unc_pair(&posix, &distro);
    let identity_key = format!("wsl:{}:{posix}", casefold(&distro));
    PathIdentity {
        scheme: "wsl",
        posix: Some(posix),
        windows_unc: Some(windows_unc),
        windows_unc_admin: Some(windows_unc_admin),
        windows_drive: None,
        distro: Some(distro),
        identity_key,
        confidence: "exact",
        reason: "wsl_posix",
    }
}

enum UncClass {
    Absent,
    InvalidDistro,
    Parsed { distro: String, posix: String },
}

fn classify_unc(raw: &str) -> UncClass {
    let Some((distro, remainder)) = match_unc_parts(raw) else {
        return UncClass::Absent;
    };
    let Some(distro) = normalize_distro(Some(distro)) else {
        return UncClass::InvalidDistro;
    };
    UncClass::Parsed {
        distro,
        posix: posix_from_remainder(remainder),
    }
}

fn match_unc_parts(raw: &str) -> Option<(&str, &str)> {
    for (share, forward) in [
        (WSL_LOCALHOST_SHARE, false),
        (WSL_ADMIN_SHARE, false),
        (WSL_LOCALHOST_SHARE, true),
        (WSL_ADMIN_SHARE, true),
    ] {
        if let Some(parts) = split_unc_share(raw, share, forward) {
            return Some(parts);
        }
    }
    None
}

fn split_unc_share<'a>(raw: &'a str, share: &str, forward: bool) -> Option<(&'a str, &'a str)> {
    let slash = if forward { b'/' } else { b'\\' };
    let bytes = raw.as_bytes();
    let share_bytes = share.as_bytes();
    if !share.is_ascii() {
        return None;
    }
    let head = 2 + share_bytes.len() + 1;
    if bytes.len() < head || bytes[0] != slash || bytes[1] != slash {
        return None;
    }
    if !bytes[2..2 + share_bytes.len()].eq_ignore_ascii_case(share_bytes)
        || bytes[head - 1] != slash
    {
        return None;
    }
    let rest = &raw[head..];
    if rest.is_empty() {
        return None;
    }
    let slash_char = if forward { '/' } else { '\\' };
    let (distro, remainder) = match rest.split_once(slash_char) {
        Some((distro, remainder)) => (distro, remainder),
        None => (rest, ""),
    };
    if distro.is_empty() {
        None
    } else {
        Some((distro, remainder))
    }
}

fn posix_from_remainder(remainder: &str) -> String {
    let forward = remainder.replace('\\', "/");
    let parts: Vec<&str> = forward
        .split('/')
        .filter_map(|part| {
            let trimmed = part.trim_matches(['/', '\\']);
            if part.is_empty() || trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect();
    if parts.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn unc_pair(posix: &str, distro: &str) -> (String, String) {
    let mut remainder = posix.replace('/', "\\");
    if let Some(rest) = remainder.strip_prefix('\\') {
        remainder = rest.to_owned();
    }
    (
        backslash_unc(WSL_LOCALHOST_SHARE, distro, &remainder),
        backslash_unc(WSL_ADMIN_SHARE, distro, &remainder),
    )
}

fn backslash_unc(share: &str, distro: &str, remainder: &str) -> String {
    format!("\\\\{share}\\{distro}\\{remainder}")
}

fn parse_windows_drive(raw: &str) -> Option<(char, &str)> {
    let mut chars = raw.chars();
    let letter = chars.next()?;
    if !letter.is_ascii_alphabetic() || chars.next() != Some(':') {
        return None;
    }
    let slash = chars.next()?;
    if slash != '\\' && slash != '/' {
        return None;
    }
    Some((letter, chars.as_str()))
}

enum MntTail {
    Absent,
    Present(String),
}

fn parse_mnt(posix: &str) -> Option<(char, MntTail)> {
    let rest = posix.strip_prefix("/mnt/")?;
    let mut chars = rest.chars();
    let letter = chars.next()?;
    if !letter.is_ascii_lowercase() {
        return None;
    }
    match chars.next() {
        None => Some((letter, MntTail::Absent)),
        Some('/') => Some((letter, MntTail::Present(chars.as_str().to_owned()))),
        Some(_) => None,
    }
}

fn mnt_group_rest(tail: &MntTail) -> &str {
    match tail {
        MntTail::Absent => "",
        MntTail::Present(text) => text,
    }
}

fn posix_for_drive(letter: char, rest: &str) -> String {
    let lower = letter.to_ascii_lowercase();
    if rest.is_empty() {
        format!("/mnt/{lower}")
    } else {
        format!("/mnt/{lower}/{rest}")
    }
}

fn drive_letter_forms(letter: char, rest: &str) -> (String, String) {
    let upper = letter.to_ascii_uppercase();
    let lower = letter.to_ascii_lowercase();
    let windows_drive = if rest.is_empty() {
        format!("{upper}:\\")
    } else {
        format!("{upper}:\\{}", rest.replace('/', "\\"))
    };
    let identity_key = format!("win:{lower}:/{}", python_lower(rest));
    (windows_drive, identity_key)
}

fn normalize_distro(distro: Option<&str>) -> Option<String> {
    let text = python_strip(distro.unwrap_or(""));
    if text.is_empty() || text.contains(['\\', '/']) || text.contains('\0') {
        None
    } else {
        Some(text.to_owned())
    }
}

fn read_git_text(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    String::from_utf8(bytes).ok()
}

fn resolve_mirrored(explicit: Option<bool>, env_value: Option<&str>) -> bool {
    match explicit {
        Some(value) => value,
        None => env_value.is_some_and(mirrored_env_value),
    }
}

fn mirrored_env_value(value: &str) -> bool {
    matches!(python_strip(value), "1" | "true" | "yes")
}

fn map_parsed_localhost(raw: &str, mirrored: bool) -> LocalhostMapping {
    let parsed = match urlsplit(raw) {
        Ok(parsed) => parsed,
        Err(()) => return unknown_url(raw, None, None, "unparsed_url"),
    };
    let (origin_host, port_text) = hostname_and_port_text(&parsed.netloc);
    let port = match parse_port_text(port_text.as_deref()) {
        Ok(port) => port,
        Err(()) => return unknown_url(raw, None, None, "unparsed_url"),
    };
    let host_key = origin_host.as_deref().unwrap_or("").to_owned();
    let scheme = parsed.scheme.as_str();
    if (scheme != "http" && scheme != "https") || parsed.netloc.is_empty() || host_key.is_empty() {
        let reason = if parsed.netloc.is_empty() || scheme.is_empty() || host_key.is_empty() {
            "unparsed_url"
        } else {
            "unsupported_scheme"
        };
        return unknown_url(raw, origin_host, port, reason);
    }
    let lowered_host = python_lower(&host_key);
    if !LOOPBACK_HOSTS.contains(&lowered_host.as_str()) {
        return unknown_url(raw, origin_host, port, "not_loopback");
    }
    let (listener, reason) = if mirrored {
        ("same_listener", "mirrored_loopback")
    } else {
        ("unknown", "cross_os_unproven")
    };
    LocalhostMapping {
        original: raw.to_owned(),
        origin_host,
        port,
        same_os_equivalents: same_os_equivalents(&parsed, &host_key),
        cross_os_same_listener: listener,
        reason,
    }
}

fn unknown_url(
    original: &str,
    origin_host: Option<String>,
    port: Option<u16>,
    reason: &'static str,
) -> LocalhostMapping {
    LocalhostMapping {
        original: original.to_owned(),
        origin_host,
        port,
        same_os_equivalents: Vec::new(),
        cross_os_same_listener: "unknown",
        reason,
    }
}

struct SplitUrl {
    scheme: String,
    netloc: String,
    path: String,
    query: String,
    fragment: String,
}

fn urlsplit(input: &str) -> Result<SplitUrl, ()> {
    let stripped = lstrip_c0_space(input);
    let owned = remove_tab_cr_lf(stripped);
    let (scheme, rest) = split_scheme(&owned);
    let (netloc, path) = if let Some(after) = rest.strip_prefix("//") {
        let (netloc, path) = split_netloc(after);
        if (netloc.contains('[') && !netloc.contains(']'))
            || (netloc.contains(']') && !netloc.contains('['))
        {
            return Err(());
        }
        if netloc.contains('[') && netloc.contains(']') {
            check_bracketed_netloc(netloc)?;
        }
        (netloc.to_owned(), path.to_owned())
    } else {
        (String::new(), rest)
    };
    let (path, fragment) = split_once_owned(&path, '#');
    let (path, query) = split_once_owned(&path, '?');
    Ok(SplitUrl {
        scheme,
        netloc,
        path,
        query,
        fragment,
    })
}

fn split_scheme(url: &str) -> (String, String) {
    let Some(index) = url.find(':') else {
        return (String::new(), url.to_owned());
    };
    if index == 0 {
        return (String::new(), url.to_owned());
    }
    let scheme = &url[..index];
    let Some(first) = scheme.bytes().next() else {
        return (String::new(), url.to_owned());
    };
    if !first.is_ascii_alphabetic() || !scheme.bytes().all(is_scheme_byte) {
        return (String::new(), url.to_owned());
    }
    (scheme.to_ascii_lowercase(), url[index + 1..].to_owned())
}

fn is_scheme_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.')
}

fn split_netloc(url: &str) -> (&str, &str) {
    let index = url.find(['/', '?', '#']).unwrap_or(url.len());
    (&url[..index], &url[index..])
}

fn split_once_owned(text: &str, separator: char) -> (String, String) {
    match text.split_once(separator) {
        Some((left, right)) => (left.to_owned(), right.to_owned()),
        None => (text.to_owned(), String::new()),
    }
}

fn check_bracketed_netloc(netloc: &str) -> Result<(), ()> {
    let hostinfo = netloc
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(netloc);
    if let Some((before, bracketed)) = hostinfo.split_once('[') {
        if !before.is_empty() {
            return Err(());
        }
        let Some((hostname, after)) = bracketed.split_once(']') else {
            return Err(());
        };
        if !after.is_empty() && !after.starts_with(':') {
            return Err(());
        }
        return check_bracketed_host(hostname);
    }
    let hostname = hostinfo
        .split_once(':')
        .map(|(host, _)| host)
        .unwrap_or(hostinfo);
    check_bracketed_host(hostname)
}

fn check_bracketed_host(hostname: &str) -> Result<(), ()> {
    if let Some(rest) = hostname.strip_prefix('v') {
        return if ipv_future_rest(rest) {
            Ok(())
        } else {
            Err(())
        };
    }
    let bare = hostname
        .split_once('%')
        .map(|(host, _)| host)
        .unwrap_or(hostname);
    if bare.parse::<std::net::Ipv6Addr>().is_ok() {
        Ok(())
    } else {
        Err(())
    }
}

fn ipv_future_rest(rest: &str) -> bool {
    let mut hexes = 0usize;
    let mut chars = rest.chars();
    for character in chars.by_ref() {
        if character.is_ascii_hexdigit() {
            hexes += 1;
            continue;
        }
        if character == '.' && hexes > 0 {
            return chars.next().is_some();
        }
        return false;
    }
    false
}

fn hostname_and_port_text(netloc: &str) -> (Option<String>, Option<String>) {
    let hostinfo = netloc
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(netloc);
    let (hostname, port) = if let Some((_, bracketed)) = hostinfo.split_once('[') {
        let (hostname, after) = bracketed.split_once(']').unwrap_or((bracketed, ""));
        let port = after.split_once(':').map(|(_, port)| port).unwrap_or("");
        (hostname, port)
    } else {
        match hostinfo.split_once(':') {
            Some((hostname, port)) => (hostname, port),
            None => (hostinfo, ""),
        }
    };
    let hostname = if hostname.is_empty() {
        None
    } else {
        Some(format_hostname(hostname))
    };
    let port = if port.is_empty() {
        None
    } else {
        Some(port.to_owned())
    };
    (hostname, port)
}

fn format_hostname(hostname: &str) -> String {
    match hostname.split_once('%') {
        Some((host, zone)) => format!("{}%{zone}", python_lower(host)),
        None => python_lower(hostname),
    }
}

fn parse_port_text(port: Option<&str>) -> Result<Option<u16>, ()> {
    let Some(port) = port else {
        return Ok(None);
    };
    if port.is_empty() {
        return Ok(None);
    }
    if !port.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    let value = port.parse::<u32>().map_err(|_| ())?;
    if value > u32::from(u16::MAX) {
        return Err(());
    }
    Ok(Some(u16::try_from(value).map_err(|_| ())?))
}

fn same_os_equivalents(parsed: &SplitUrl, hostname: &str) -> Vec<String> {
    let mut equivalents = Vec::with_capacity(2);
    for alt_host in ["127.0.0.1", "localhost"] {
        let netloc = replace_host(&parsed.netloc, hostname, alt_host);
        let candidate = urlunsplit(
            &parsed.scheme,
            &netloc,
            &parsed.path,
            &parsed.query,
            &parsed.fragment,
        );
        if !equivalents.iter().any(|existing| existing == &candidate) {
            equivalents.push(candidate);
        }
    }
    equivalents
}

fn replace_host(netloc: &str, current: &str, replacement: &str) -> String {
    if current.is_empty() {
        return netloc.to_owned();
    }
    let (userinfo, hostport) = match netloc.rsplit_once('@') {
        Some((user, host)) => (format!("{user}@"), host),
        None => (String::new(), netloc),
    };
    if hostport.starts_with('[') {
        let after = hostport
            .find(']')
            .map_or("", |index| &hostport[index + 1..]);
        if replacement == "::1" {
            return format!("{userinfo}[::1]{after}");
        }
        return format!("{userinfo}{replacement}{after}");
    }
    if let Some((_, port)) = hostport.rsplit_once(':') {
        return format!("{userinfo}{replacement}:{port}");
    }
    format!("{userinfo}{replacement}")
}

fn urlunsplit(scheme: &str, netloc: &str, path: &str, query: &str, fragment: &str) -> String {
    let mut path = path.to_owned();
    if !netloc.is_empty() {
        if !path.is_empty() && !path.starts_with('/') {
            path.insert(0, '/');
        }
        path = format!("//{netloc}{path}");
    }
    let mut out = if scheme.is_empty() {
        path
    } else {
        format!("{scheme}:{path}")
    };
    if !query.is_empty() {
        out.push('?');
        out.push_str(query);
    }
    if !fragment.is_empty() {
        out.push('#');
        out.push_str(fragment);
    }
    out
}

fn lstrip_c0_space(text: &str) -> &str {
    text.find(|character: char| !matches!(character, '\0'..='\u{20}'))
        .map_or("", |index| &text[index..])
}

fn remove_tab_cr_lf(text: &str) -> String {
    if text.contains(['\t', '\r', '\n']) {
        text.chars()
            .filter(|character| !matches!(character, '\t' | '\r' | '\n'))
            .collect()
    } else {
        text.to_owned()
    }
}

fn python_strip(text: &str) -> &str {
    let start = text
        .char_indices()
        .find(|(_, character)| !is_python_space(*character))
        .map_or(text.len(), |(index, _)| index);
    let end = text
        .char_indices()
        .rev()
        .find(|(_, character)| !is_python_space(*character))
        .map_or(0, |(index, character)| index + character.len_utf8());
    if start >= end { "" } else { &text[start..end] }
}

fn is_python_space(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{001c}'..='\u{001f}'
            | '\u{0020}'
            | '\u{0085}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
    )
}

fn python_first_line(text: &str) -> &str {
    text.find(is_python_line_break)
        .map_or(text, |index| &text[..index])
}

fn is_python_line_break(character: char) -> bool {
    matches!(
        character,
        '\n' | '\r'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{001c}'
            | '\u{001d}'
            | '\u{001e}'
            | '\u{0085}'
            | '\u{2028}'
            | '\u{2029}'
    )
}

fn strip_prefix_ignore_ascii_case<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let prefix_bytes = prefix.as_bytes();
    let text_bytes = text.as_bytes();
    if !prefix.is_ascii() || text_bytes.len() < prefix_bytes.len() {
        return None;
    }
    if text_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes) {
        Some(&text[prefix_bytes.len()..])
    } else {
        None
    }
}

fn rfind_ignore_ascii_case(haystack: &str, needle: &str) -> Option<usize> {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if !needle.is_ascii() || needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    let mut index = haystack.len() - needle.len();
    loop {
        if haystack[index..index + needle.len()].eq_ignore_ascii_case(needle) {
            return Some(index);
        }
        if index == 0 {
            return None;
        }
        index -= 1;
    }
}

fn ends_with_ignore_ascii_case(text: &str, suffix: &str) -> bool {
    let text = text.as_bytes();
    let suffix = suffix.as_bytes();
    suffix.is_ascii()
        && text.len() >= suffix.len()
        && text[text.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
}

fn python_lower(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        if casefold_keep(character) {
            out.push(character);
        } else {
            for lower in character.to_lowercase() {
                out.push(lower);
            }
        }
    }
    out
}

fn casefold(text: &str) -> String {
    let lowered = python_lower(text);
    if lowered
        .chars()
        .all(|character| casefold_fix(character).is_none())
    {
        return lowered;
    }
    let mut out = String::with_capacity(lowered.len());
    for character in lowered.chars() {
        match casefold_fix(character) {
            Some(replacement) => out.push_str(replacement),
            None => out.push(character),
        }
    }
    out
}

fn casefold_keep(character: char) -> bool {
    CASEFOLD_KEEP.binary_search(&character).is_ok()
}

fn casefold_fix(character: char) -> Option<&'static str> {
    CASEFOLD_FIX
        .binary_search_by_key(&character, |row| row.0)
        .ok()
        .map(|index| CASEFOLD_FIX[index].1)
}

// Scalars whose case fold is the scalar itself, and scalars whose full case
// fold differs from lowercase. `CASEFOLD_KEEP` is consulted before lowercase
// so a newer lowercase mapping does not change a scalar that folds to itself.
const CASEFOLD_KEEP: &[char] = &[
    '\u{1c89}',
    '\u{a7cb}',
    '\u{a7cc}',
    '\u{a7ce}',
    '\u{a7d2}',
    '\u{a7d4}',
    '\u{a7da}',
    '\u{a7dc}',
    '\u{a7dd}',
    '\u{a7e2}',
    '\u{ab6c}',
    '\u{ab6d}',
    '\u{10d50}',
    '\u{10d51}',
    '\u{10d52}',
    '\u{10d53}',
    '\u{10d54}',
    '\u{10d55}',
    '\u{10d56}',
    '\u{10d57}',
    '\u{10d58}',
    '\u{10d59}',
    '\u{10d5a}',
    '\u{10d5b}',
    '\u{10d5c}',
    '\u{10d5d}',
    '\u{10d5e}',
    '\u{10d5f}',
    '\u{10d60}',
    '\u{10d61}',
    '\u{10d62}',
    '\u{10d63}',
    '\u{10d64}',
    '\u{10d65}',
    '\u{16ea0}',
    '\u{16ea1}',
    '\u{16ea2}',
    '\u{16ea3}',
    '\u{16ea4}',
    '\u{16ea5}',
    '\u{16ea6}',
    '\u{16ea7}',
    '\u{16ea8}',
    '\u{16ea9}',
    '\u{16eaa}',
    '\u{16eab}',
    '\u{16eac}',
    '\u{16ead}',
    '\u{16eae}',
    '\u{16eaf}',
    '\u{16eb0}',
    '\u{16eb1}',
    '\u{16eb2}',
    '\u{16eb3}',
    '\u{16eb4}',
    '\u{16eb5}',
    '\u{16eb6}',
    '\u{16eb7}',
    '\u{16eb8}',
    '\u{1df40}',
    '\u{1df48}',
    '\u{1df4a}',
    '\u{1df4d}',
    '\u{1df51}',
    '\u{1df68}',
    '\u{1df6a}',
    '\u{1df6c}',
    '\u{1df6e}',
    '\u{1df72}',
    '\u{1df74}',
    '\u{1df76}',
    '\u{1df78}',
    '\u{1df7a}',
    '\u{1df7c}',
    '\u{1df7e}',
];

const CASEFOLD_FIX: &[(char, &str)] = &[
    ('\u{b5}', "\u{3bc}"),
    ('\u{df}', "ss"),
    ('\u{149}', "\u{2bc}n"),
    ('\u{17f}', "s"),
    ('\u{1f0}', "j\u{30c}"),
    ('\u{345}', "\u{3b9}"),
    ('\u{390}', "\u{3b9}\u{308}\u{301}"),
    ('\u{3b0}', "\u{3c5}\u{308}\u{301}"),
    ('\u{3c2}', "\u{3c3}"),
    ('\u{3d0}', "\u{3b2}"),
    ('\u{3d1}', "\u{3b8}"),
    ('\u{3d5}', "\u{3c6}"),
    ('\u{3d6}', "\u{3c0}"),
    ('\u{3f0}', "\u{3ba}"),
    ('\u{3f1}', "\u{3c1}"),
    ('\u{3f5}', "\u{3b5}"),
    ('\u{587}', "\u{565}\u{582}"),
    ('\u{13f8}', "\u{13f0}"),
    ('\u{13f9}', "\u{13f1}"),
    ('\u{13fa}', "\u{13f2}"),
    ('\u{13fb}', "\u{13f3}"),
    ('\u{13fc}', "\u{13f4}"),
    ('\u{13fd}', "\u{13f5}"),
    ('\u{1c80}', "\u{432}"),
    ('\u{1c81}', "\u{434}"),
    ('\u{1c82}', "\u{43e}"),
    ('\u{1c83}', "\u{441}"),
    ('\u{1c84}', "\u{442}"),
    ('\u{1c85}', "\u{442}"),
    ('\u{1c86}', "\u{44a}"),
    ('\u{1c87}', "\u{463}"),
    ('\u{1c88}', "\u{a64b}"),
    ('\u{1e96}', "h\u{331}"),
    ('\u{1e97}', "t\u{308}"),
    ('\u{1e98}', "w\u{30a}"),
    ('\u{1e99}', "y\u{30a}"),
    ('\u{1e9a}', "a\u{2be}"),
    ('\u{1e9b}', "\u{1e61}"),
    ('\u{1f50}', "\u{3c5}\u{313}"),
    ('\u{1f52}', "\u{3c5}\u{313}\u{300}"),
    ('\u{1f54}', "\u{3c5}\u{313}\u{301}"),
    ('\u{1f56}', "\u{3c5}\u{313}\u{342}"),
    ('\u{1f80}', "\u{1f00}\u{3b9}"),
    ('\u{1f81}', "\u{1f01}\u{3b9}"),
    ('\u{1f82}', "\u{1f02}\u{3b9}"),
    ('\u{1f83}', "\u{1f03}\u{3b9}"),
    ('\u{1f84}', "\u{1f04}\u{3b9}"),
    ('\u{1f85}', "\u{1f05}\u{3b9}"),
    ('\u{1f86}', "\u{1f06}\u{3b9}"),
    ('\u{1f87}', "\u{1f07}\u{3b9}"),
    ('\u{1f90}', "\u{1f20}\u{3b9}"),
    ('\u{1f91}', "\u{1f21}\u{3b9}"),
    ('\u{1f92}', "\u{1f22}\u{3b9}"),
    ('\u{1f93}', "\u{1f23}\u{3b9}"),
    ('\u{1f94}', "\u{1f24}\u{3b9}"),
    ('\u{1f95}', "\u{1f25}\u{3b9}"),
    ('\u{1f96}', "\u{1f26}\u{3b9}"),
    ('\u{1f97}', "\u{1f27}\u{3b9}"),
    ('\u{1fa0}', "\u{1f60}\u{3b9}"),
    ('\u{1fa1}', "\u{1f61}\u{3b9}"),
    ('\u{1fa2}', "\u{1f62}\u{3b9}"),
    ('\u{1fa3}', "\u{1f63}\u{3b9}"),
    ('\u{1fa4}', "\u{1f64}\u{3b9}"),
    ('\u{1fa5}', "\u{1f65}\u{3b9}"),
    ('\u{1fa6}', "\u{1f66}\u{3b9}"),
    ('\u{1fa7}', "\u{1f67}\u{3b9}"),
    ('\u{1fb2}', "\u{1f70}\u{3b9}"),
    ('\u{1fb3}', "\u{3b1}\u{3b9}"),
    ('\u{1fb4}', "\u{3ac}\u{3b9}"),
    ('\u{1fb6}', "\u{3b1}\u{342}"),
    ('\u{1fb7}', "\u{3b1}\u{342}\u{3b9}"),
    ('\u{1fbe}', "\u{3b9}"),
    ('\u{1fc2}', "\u{1f74}\u{3b9}"),
    ('\u{1fc3}', "\u{3b7}\u{3b9}"),
    ('\u{1fc4}', "\u{3ae}\u{3b9}"),
    ('\u{1fc6}', "\u{3b7}\u{342}"),
    ('\u{1fc7}', "\u{3b7}\u{342}\u{3b9}"),
    ('\u{1fd2}', "\u{3b9}\u{308}\u{300}"),
    ('\u{1fd3}', "\u{3b9}\u{308}\u{301}"),
    ('\u{1fd6}', "\u{3b9}\u{342}"),
    ('\u{1fd7}', "\u{3b9}\u{308}\u{342}"),
    ('\u{1fe2}', "\u{3c5}\u{308}\u{300}"),
    ('\u{1fe3}', "\u{3c5}\u{308}\u{301}"),
    ('\u{1fe4}', "\u{3c1}\u{313}"),
    ('\u{1fe6}', "\u{3c5}\u{342}"),
    ('\u{1fe7}', "\u{3c5}\u{308}\u{342}"),
    ('\u{1ff2}', "\u{1f7c}\u{3b9}"),
    ('\u{1ff3}', "\u{3c9}\u{3b9}"),
    ('\u{1ff4}', "\u{3ce}\u{3b9}"),
    ('\u{1ff6}', "\u{3c9}\u{342}"),
    ('\u{1ff7}', "\u{3c9}\u{342}\u{3b9}"),
    ('\u{ab70}', "\u{13a0}"),
    ('\u{ab71}', "\u{13a1}"),
    ('\u{ab72}', "\u{13a2}"),
    ('\u{ab73}', "\u{13a3}"),
    ('\u{ab74}', "\u{13a4}"),
    ('\u{ab75}', "\u{13a5}"),
    ('\u{ab76}', "\u{13a6}"),
    ('\u{ab77}', "\u{13a7}"),
    ('\u{ab78}', "\u{13a8}"),
    ('\u{ab79}', "\u{13a9}"),
    ('\u{ab7a}', "\u{13aa}"),
    ('\u{ab7b}', "\u{13ab}"),
    ('\u{ab7c}', "\u{13ac}"),
    ('\u{ab7d}', "\u{13ad}"),
    ('\u{ab7e}', "\u{13ae}"),
    ('\u{ab7f}', "\u{13af}"),
    ('\u{ab80}', "\u{13b0}"),
    ('\u{ab81}', "\u{13b1}"),
    ('\u{ab82}', "\u{13b2}"),
    ('\u{ab83}', "\u{13b3}"),
    ('\u{ab84}', "\u{13b4}"),
    ('\u{ab85}', "\u{13b5}"),
    ('\u{ab86}', "\u{13b6}"),
    ('\u{ab87}', "\u{13b7}"),
    ('\u{ab88}', "\u{13b8}"),
    ('\u{ab89}', "\u{13b9}"),
    ('\u{ab8a}', "\u{13ba}"),
    ('\u{ab8b}', "\u{13bb}"),
    ('\u{ab8c}', "\u{13bc}"),
    ('\u{ab8d}', "\u{13bd}"),
    ('\u{ab8e}', "\u{13be}"),
    ('\u{ab8f}', "\u{13bf}"),
    ('\u{ab90}', "\u{13c0}"),
    ('\u{ab91}', "\u{13c1}"),
    ('\u{ab92}', "\u{13c2}"),
    ('\u{ab93}', "\u{13c3}"),
    ('\u{ab94}', "\u{13c4}"),
    ('\u{ab95}', "\u{13c5}"),
    ('\u{ab96}', "\u{13c6}"),
    ('\u{ab97}', "\u{13c7}"),
    ('\u{ab98}', "\u{13c8}"),
    ('\u{ab99}', "\u{13c9}"),
    ('\u{ab9a}', "\u{13ca}"),
    ('\u{ab9b}', "\u{13cb}"),
    ('\u{ab9c}', "\u{13cc}"),
    ('\u{ab9d}', "\u{13cd}"),
    ('\u{ab9e}', "\u{13ce}"),
    ('\u{ab9f}', "\u{13cf}"),
    ('\u{aba0}', "\u{13d0}"),
    ('\u{aba1}', "\u{13d1}"),
    ('\u{aba2}', "\u{13d2}"),
    ('\u{aba3}', "\u{13d3}"),
    ('\u{aba4}', "\u{13d4}"),
    ('\u{aba5}', "\u{13d5}"),
    ('\u{aba6}', "\u{13d6}"),
    ('\u{aba7}', "\u{13d7}"),
    ('\u{aba8}', "\u{13d8}"),
    ('\u{aba9}', "\u{13d9}"),
    ('\u{abaa}', "\u{13da}"),
    ('\u{abab}', "\u{13db}"),
    ('\u{abac}', "\u{13dc}"),
    ('\u{abad}', "\u{13dd}"),
    ('\u{abae}', "\u{13de}"),
    ('\u{abaf}', "\u{13df}"),
    ('\u{abb0}', "\u{13e0}"),
    ('\u{abb1}', "\u{13e1}"),
    ('\u{abb2}', "\u{13e2}"),
    ('\u{abb3}', "\u{13e3}"),
    ('\u{abb4}', "\u{13e4}"),
    ('\u{abb5}', "\u{13e5}"),
    ('\u{abb6}', "\u{13e6}"),
    ('\u{abb7}', "\u{13e7}"),
    ('\u{abb8}', "\u{13e8}"),
    ('\u{abb9}', "\u{13e9}"),
    ('\u{abba}', "\u{13ea}"),
    ('\u{abbb}', "\u{13eb}"),
    ('\u{abbc}', "\u{13ec}"),
    ('\u{abbd}', "\u{13ed}"),
    ('\u{abbe}', "\u{13ee}"),
    ('\u{abbf}', "\u{13ef}"),
    ('\u{fb00}', "ff"),
    ('\u{fb01}', "fi"),
    ('\u{fb02}', "fl"),
    ('\u{fb03}', "ffi"),
    ('\u{fb04}', "ffl"),
    ('\u{fb05}', "st"),
    ('\u{fb06}', "st"),
    ('\u{fb13}', "\u{574}\u{576}"),
    ('\u{fb14}', "\u{574}\u{565}"),
    ('\u{fb15}', "\u{574}\u{56b}"),
    ('\u{fb16}', "\u{57e}\u{576}"),
    ('\u{fb17}', "\u{574}\u{56d}"),
];

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;

    use super::*;

    fn localhost_unc(distro: &str, tail: &str) -> String {
        format!(r"\\wsl.localhost\{distro}\{tail}")
    }

    fn admin_unc(distro: &str, tail: &str) -> String {
        format!("\\\\{share}\\{distro}\\{tail}", share = WSL_ADMIN_SHARE)
    }

    fn forward_unc(distro: &str, tail: &str) -> String {
        format!("//{share}/{distro}/{tail}", share = WSL_LOCALHOST_SHARE)
    }

    #[test]
    fn posix_and_wsl_unc_share_identity() {
        let posix = "/home/alice/src/vyane";
        let tail = r"home\alice\src\vyane";
        let distro = "Ubuntu-24.04";
        let unc = localhost_unc(distro, tail);
        let admin = admin_unc(distro, tail);
        let forward = forward_unc(distro, "home/alice/src/vyane");
        let left = identify_path(posix, None, Some(distro));
        let right = identify_path(&unc, None, Some(distro));
        assert_eq!(left.confidence, "exact");
        assert_eq!(left.reason, "wsl_posix");
        assert_eq!(right.reason, "wsl_unc");
        assert_eq!(right.identity_key, left.identity_key);
        assert_eq!(left.identity_key, "wsl:ubuntu-24.04:/home/alice/src/vyane");
        assert!(paths_refer_to_same_location(posix, &unc, Some(distro)));
        assert!(paths_refer_to_same_location(posix, &admin, Some(distro)));
        assert!(paths_refer_to_same_location(posix, &forward, Some(distro)));
        assert_eq!(left.windows_unc.as_deref(), Some(unc.as_str()));
        assert_eq!(left.windows_unc_admin.as_deref(), Some(admin.as_str()));
        assert_eq!(left.distro.as_deref(), Some(distro));
        assert!(left.windows_drive.is_none());
    }

    #[test]
    fn posix_without_distro_does_not_invent_unc() {
        let posix = "/home/alice/src/vyane";
        let unc = localhost_unc("Ubuntu-24.04", r"home\alice\src\vyane");
        let identity = identify_path(posix, None, None);
        assert!(identity.windows_unc.is_none());
        assert!(identity.windows_unc_admin.is_none());
        assert_eq!(identity.confidence, "partial");
        assert_eq!(identity.reason, "wsl_distro_unknown");
        assert_eq!(identity.scheme, "wsl");
        assert_eq!(identity.identity_key, "posix:/home/alice/src/vyane");
        assert!(!paths_refer_to_same_location(posix, &unc, None));
        let guessed = identify_path(posix, None, Some("Ubuntu"));
        assert_ne!(identity.identity_key, guessed.identity_key);
        assert!(!paths_refer_to_same_location(posix, posix, None));
    }

    #[test]
    fn mnt_drive_maps_to_windows_drive_not_wsl_unc() {
        let identity = identify_path("/mnt/c/Users/alice/src", None, Some("Ubuntu-24.04"));
        assert_eq!(identity.scheme, "windows_drive");
        assert_eq!(identity.reason, "wsl_mnt_drive");
        assert_eq!(
            identity.windows_drive.as_deref(),
            Some(r"C:\Users\alice\src")
        );
        assert!(identity.windows_unc.is_none());
        assert!(identity.distro.is_none());
        assert!(paths_refer_to_same_location(
            "/mnt/c/Users/alice/src",
            r"C:\Users\alice\src",
            None
        ));
        assert!(paths_refer_to_same_location(
            r"C:/Users/alice/src",
            r"c:\Users\alice\src",
            None
        ));
        assert_eq!(
            identify_path(r"c:\Users\alice\src", None, None).identity_key,
            "win:c:/users/alice/src"
        );
        // A drive locator strips a trailing slash. The `/mnt` capture keeps it.
        assert_eq!(
            identify_path(r"C:\Users\", None, None).identity_key,
            "win:c:/users"
        );
        assert_eq!(
            identify_path("/mnt/c/Users/", None, None).identity_key,
            "win:c:/users/"
        );
        assert!(!paths_refer_to_same_location(
            "/mnt/c/Users/",
            r"C:\Users\",
            None
        ));
        assert_eq!(
            identify_path(r"C:\", None, None).windows_drive.as_deref(),
            Some(r"C:\")
        );
        assert_eq!(
            identify_path(r"C:\", None, None).posix.as_deref(),
            Some("/mnt/c")
        );
    }

    #[test]
    fn unc_mnt_drive_keeps_unc_and_shares_the_windows_key() {
        let unc = localhost_unc("Ubuntu", r"mnt\c\Users\alice");
        let identity = identify_path(&unc, None, None);
        assert_eq!(identity.scheme, "windows_drive");
        assert_eq!(identity.reason, "wsl_unc");
        assert_eq!(identity.confidence, "exact");
        assert_eq!(identity.distro.as_deref(), Some("Ubuntu"));
        assert_eq!(identity.posix.as_deref(), Some("/mnt/c/Users/alice"));
        assert_eq!(identity.windows_drive.as_deref(), Some(r"C:\Users\alice"));
        assert_eq!(identity.windows_unc.as_deref(), Some(unc.as_str()));
        assert_eq!(identity.identity_key, "win:c:/users/alice");
        assert!(paths_refer_to_same_location(&unc, r"C:\Users\alice", None));
        assert!(paths_refer_to_same_location(
            &unc,
            &localhost_unc("Debian", r"mnt\c\Users\alice"),
            None,
        ));
    }

    #[test]
    fn unrelated_paths_do_not_match() {
        assert!(!paths_refer_to_same_location(
            "/home/alice/src/vyane",
            "/home/alice/src/horus",
            Some("Ubuntu-24.04"),
        ));
        assert!(!paths_refer_to_same_location(
            &localhost_unc("Ubuntu-24.04", r"home\alice\src\vyane"),
            &localhost_unc("Debian", r"home\alice\src\vyane"),
            None,
        ));
    }

    #[test]
    fn locator_kind_stays_unset_until_the_caller_passes_one() {
        let host_kind = crate::local_locator_kind();
        assert!(host_kind == "posix_absolute_path" || host_kind == "windows_absolute_path");
        let shaped = identify_path("home/alice", None, Some("Ubuntu"));
        assert_eq!(shaped.reason, "unrecognized_locator");
        assert_eq!(shaped.confidence, "unknown");
        let forced = identify_path("home/alice", Some("posix_absolute_path"), Some("Ubuntu"));
        assert_eq!(forced.reason, "wsl_posix");
        assert_eq!(forced.posix.as_deref(), Some("/home/alice"));
        assert_eq!(forced.confidence, "exact");
        let windows = identify_path("not-a-drive", Some("windows_absolute_path"), None);
        assert_eq!(windows.reason, "windows_locator_unparsed");
        assert_eq!(windows.identity_key, "unknown:");
        let mnt_as_windows =
            identify_path("/mnt/c/Users/alice", Some("windows_absolute_path"), None);
        assert_eq!(mnt_as_windows.reason, "windows_locator_unparsed");
    }

    #[test]
    fn extended_prefix_stays_in_the_locator() {
        let extended = r"\\?\C:\Users\alice";
        let identity = identify_path(extended, None, None);
        assert_eq!(identity.reason, "unrecognized_locator");
        assert_eq!(identity.confidence, "unknown");
        assert!(identity.windows_drive.is_none());
    }

    #[test]
    fn casefold_tables_stay_sorted_for_binary_search() {
        assert!(CASEFOLD_KEEP.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(CASEFOLD_FIX.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }

    #[test]
    fn distro_normalization_and_casefold() {
        let padded = identify_path("/home/alice/src/vyane", None, Some(" Ubuntu-24.04 "));
        assert_eq!(padded.distro.as_deref(), Some("Ubuntu-24.04"));
        assert_eq!(padded.confidence, "exact");
        assert_eq!(padded.reason, "wsl_posix");
        for rejected in ["Ubuntu/evil", r"Ubuntu\evil", "bad\0name", "", "   "] {
            let identity = identify_path("/home/alice/src/vyane", None, Some(rejected));
            assert_eq!(identity.reason, "wsl_distro_unknown", "{rejected:?}");
            assert!(identity.windows_unc.is_none());
        }
        let upper = identify_path("/home/alice/src/vyane", None, Some("Ubuntu"));
        let lower = identify_path("/home/alice/src/vyane", None, Some("ubuntu"));
        assert_eq!(upper.identity_key, lower.identity_key);
        assert!(paths_refer_to_same_location(
            "/home/alice/src/vyane",
            &localhost_unc("UBUNTU", r"home\alice\src\vyane"),
            Some("Ubuntu"),
        ));
        assert_eq!(casefold("Ubuntu-24.04"), "ubuntu-24.04");
        assert_eq!(casefold("\u{00df}"), "ss");
        assert_eq!(casefold("\u{1e9e}"), "ss");
        assert_eq!(casefold("\u{00b5}"), "\u{03bc}");
        assert_eq!(casefold("\u{03c2}"), "\u{03c3}");
        assert_eq!(casefold("\u{fb01}"), "fi");
        assert_eq!(casefold("\u{0130}"), "i\u{307}");
        assert_eq!(casefold("\u{a7cb}"), "\u{a7cb}");
    }

    #[test]
    fn unc_prefix_case_and_invalid_distro() {
        let forward = "//WSL.LOCALHOST/ubuntu-24.04/home/alice/src/vyane";
        let identity = identify_path(forward, None, None);
        assert_eq!(identity.confidence, "exact");
        assert_eq!(identity.distro.as_deref(), Some("ubuntu-24.04"));
        assert_eq!(
            identity.identity_key,
            "wsl:ubuntu-24.04:/home/alice/src/vyane"
        );
        let invalid = identify_path(r"\\wsl.localhost\Ub/untu\home", None, None);
        assert_eq!(invalid.reason, "unc_distro_invalid");
        assert_eq!(invalid.confidence, "unknown");
        let empty = identify_path("   ", None, Some("Ubuntu"));
        assert_eq!(empty.reason, "empty_locator");
        assert_eq!(empty.identity_key, "unknown:");
    }

    #[test]
    fn worktree_gitdir_shares_primary_identity() {
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("repo");
        let worktree = dir.path().join("worktrees").join("feature");
        let git_dir = primary.join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::create_dir_all(&worktree).unwrap();
        let pointer_target = git_dir.join("worktrees").join("feature");
        let pointer = pointer_target.to_str().unwrap();
        fs::write(worktree.join(".git"), format!("gitdir: {pointer}\n")).unwrap();

        let parsed = parse_gitdir_pointer(&fs::read_to_string(worktree.join(".git")).unwrap());
        assert_eq!(parsed.as_deref(), Some(pointer));
        let common = worktree_common_git_dir(parsed.as_deref().unwrap()).unwrap();
        let expected = git_dir.to_str().unwrap().replace('\\', "/");
        assert_eq!(common, expected);

        let primary_id = probe_checkout_identity(&primary, None);
        let worktree_id = probe_checkout_identity(&worktree, None);
        assert!(primary_id.is_some());
        assert_eq!(primary_id, worktree_id);
        assert_eq!(
            checkout_identity_key(git_dir.to_str().unwrap(), None),
            primary_id
        );
    }

    #[test]
    fn missing_git_metadata_is_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let bare = dir.path().join("not-a-repo");
        fs::create_dir(&bare).unwrap();
        assert!(probe_checkout_identity(&bare, None).is_none());
        assert!(probe_checkout_identity(dir.path().join("missing"), None).is_none());

        let broken = dir.path().join("broken");
        fs::create_dir(&broken).unwrap();
        fs::write(broken.join(".git"), "not a gitdir pointer\n").unwrap();
        assert!(probe_checkout_identity(&broken, None).is_none());

        assert!(parse_gitdir_pointer("not a gitdir pointer").is_none());
        assert!(parse_gitdir_pointer("").is_none());
        assert!(parse_gitdir_pointer("gitdir:   \n").is_none());
        assert_eq!(
            parse_gitdir_pointer("GITDIR: \"/repo/.git/worktrees/feature\"\r\nextra\n").as_deref(),
            Some("/repo/.git/worktrees/feature")
        );
        assert!(worktree_common_git_dir("/tmp/random").is_none());
        assert_eq!(
            worktree_common_git_dir(r"C:\repo\.git\worktrees\feature").as_deref(),
            Some("C:/repo/.git")
        );
        assert_eq!(
            worktree_common_git_dir(r"C:\repo\.git\").as_deref(),
            Some("C:/repo/.git")
        );
        assert_eq!(
            worktree_common_git_dir("/repo/.GIT/worktrees/Feature").as_deref(),
            Some("/repo/.GIT")
        );
        assert_eq!(
            checkout_identity_key(r"C:\repo\.git", None).as_deref(),
            Some("git:win:c:/repo/.git")
        );
        assert_eq!(
            checkout_identity_key(r"C:\repo\.git\worktrees\feature", None).as_deref(),
            Some("git:win:c:/repo/.git")
        );
        assert_eq!(
            worktree_common_git_dir("repo/.git").as_deref(),
            Some("repo/.git")
        );
        assert!(checkout_identity_key("repo/.git", None).is_none());
    }

    #[test]
    fn untrusted_display_aliases_are_empty() {
        assert!(
            display_path_aliases("/home/alice/src/vyane", None, Some("Ubuntu-24.04"), false)
                .is_none()
        );
        assert!(display_path_aliases("", None, None, false).is_none());
        let shown = display_path_aliases("/home/alice/src/vyane", None, Some("Ubuntu-24.04"), true)
            .unwrap();
        assert_eq!(shown.display_locator, "/home/alice/src/vyane");
        assert_eq!(shown.identity.reason, "wsl_posix");
        assert_eq!(
            shown.aliases,
            vec![
                localhost_unc("Ubuntu-24.04", r"home\alice\src\vyane"),
                admin_unc("Ubuntu-24.04", r"home\alice\src\vyane"),
            ]
        );
        let drive = display_path_aliases(r"C:/Users/alice/src", None, None, true).unwrap();
        assert_eq!(
            drive.aliases,
            vec![
                "/mnt/c/Users/alice/src".to_owned(),
                r"C:\Users\alice\src".to_owned()
            ]
        );
    }

    #[test]
    fn localhost_same_os_equivalents_do_not_claim_cross_os() {
        let mapping = map_localhost_url("http://127.0.0.1:41522/health", Some(false));
        assert_eq!(mapping.origin_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(mapping.port, Some(41522));
        assert_eq!(
            mapping.same_os_equivalents,
            vec![
                "http://127.0.0.1:41522/health".to_owned(),
                "http://localhost:41522/health".to_owned(),
            ]
        );
        assert_eq!(mapping.cross_os_same_listener, "unknown");
        assert_eq!(mapping.reason, "cross_os_unproven");
    }

    #[test]
    fn localhost_mirrored_is_opt_in() {
        let unknown = map_localhost_url("http://localhost:8080", Some(false));
        assert_eq!(unknown.cross_os_same_listener, "unknown");
        assert_eq!(unknown.reason, "cross_os_unproven");
        let proven = map_localhost_url("http://localhost:8080", Some(true));
        assert_eq!(proven.cross_os_same_listener, "same_listener");
        assert_eq!(proven.reason, "mirrored_loopback");
        assert!(resolve_mirrored(None, Some("1")));
        assert!(resolve_mirrored(None, Some("true")));
        assert!(resolve_mirrored(None, Some("yes")));
        assert!(resolve_mirrored(None, Some(" yes ")));
        assert!(!resolve_mirrored(None, Some("True")));
        assert!(!resolve_mirrored(None, Some("YES")));
        assert!(!resolve_mirrored(None, Some("0")));
        assert!(!resolve_mirrored(None, None));
        assert!(!resolve_mirrored(Some(false), Some("1")));
        assert!(resolve_mirrored(Some(true), Some("0")));

        let env_value = std::env::var("VYANE_LOOPBACK_MIRRORED").ok();
        let from_env = map_localhost_url("http://127.0.0.1:8080", None);
        let expected = if resolve_mirrored(None, env_value.as_deref()) {
            "same_listener"
        } else {
            "unknown"
        };
        assert_eq!(from_env.cross_os_same_listener, expected);
    }

    #[test]
    fn malformed_localhost_does_not_raise() {
        let broken_ipv6 = map_localhost_url("http://[::1", Some(false));
        assert_eq!(broken_ipv6.cross_os_same_listener, "unknown");
        assert_eq!(broken_ipv6.reason, "unparsed_url");
        assert!(broken_ipv6.origin_host.is_none());
        assert!(broken_ipv6.port.is_none());

        let broken_port = map_localhost_url("http://localhost:notaport/x", Some(false));
        assert_eq!(broken_port.reason, "unparsed_url");
        let schemeless = map_localhost_url("localhost:8080", Some(false));
        assert_eq!(schemeless.reason, "unparsed_url");
        let no_authority = map_localhost_url("http:localhost:8080", Some(false));
        assert_eq!(no_authority.reason, "unparsed_url");
        let ftp = map_localhost_url("ftp://localhost:8080", Some(false));
        assert_eq!(ftp.reason, "unsupported_scheme");
        assert_eq!(ftp.cross_os_same_listener, "unknown");
        assert_eq!(ftp.origin_host.as_deref(), Some("localhost"));
        assert_eq!(ftp.port, Some(8080));
        let empty_host = map_localhost_url("http://:8080", Some(false));
        assert_eq!(empty_host.reason, "unparsed_url");
        let userinfo_empty_host = map_localhost_url("http://user@:8080", Some(false));
        assert_eq!(userinfo_empty_host.reason, "unparsed_url");
        let blank = map_localhost_url("   ", Some(false));
        assert_eq!(blank.original, "   ");
        assert_eq!(blank.reason, "empty_url");
        let out_of_range = map_localhost_url("http://localhost:65536", Some(false));
        assert_eq!(out_of_range.reason, "unparsed_url");
        let bracketed_ipv4 = map_localhost_url("http://[127.0.0.1]/", Some(false));
        assert_eq!(bracketed_ipv4.reason, "unparsed_url");
        let ipv_future = map_localhost_url("http://[v1.foo]/", Some(false));
        assert_eq!(ipv_future.reason, "not_loopback");
        assert_eq!(ipv_future.origin_host.as_deref(), Some("v1.foo"));
        let with_tab = map_localhost_url("http://local\thost:1/health", Some(false));
        assert_eq!(with_tab.reason, "cross_os_unproven");
        assert_eq!(with_tab.origin_host.as_deref(), Some("localhost"));
        assert!(
            with_tab
                .same_os_equivalents
                .iter()
                .any(|url| url == "http://127.0.0.1:1/health")
        );
    }

    #[test]
    fn non_loopback_stays_unknown() {
        let mapping = map_localhost_url("http://192.168.1.4:41522", Some(false));
        assert!(mapping.same_os_equivalents.is_empty());
        assert_eq!(mapping.cross_os_same_listener, "unknown");
        assert_eq!(mapping.reason, "not_loopback");
        assert_eq!(mapping.origin_host.as_deref(), Some("192.168.1.4"));
        assert_eq!(mapping.port, Some(41522));
    }

    #[test]
    fn loopback_equivalents_keep_userinfo_query_and_ipv6() {
        let userinfo =
            map_localhost_url("http://user:pass@127.0.0.1:41522/health?q=1#f", Some(false));
        assert_eq!(
            userinfo.same_os_equivalents,
            vec![
                "http://user:pass@127.0.0.1:41522/health?q=1#f".to_owned(),
                "http://user:pass@localhost:41522/health?q=1#f".to_owned(),
            ]
        );
        let ipv6 = map_localhost_url("http://[::1]:41522/health", Some(false));
        assert_eq!(ipv6.origin_host.as_deref(), Some("::1"));
        assert_eq!(ipv6.port, Some(41522));
        assert_eq!(ipv6.reason, "cross_os_unproven");
        assert_eq!(
            ipv6.same_os_equivalents,
            vec![
                "http://127.0.0.1:41522/health".to_owned(),
                "http://localhost:41522/health".to_owned(),
            ]
        );
        let padded_port = map_localhost_url("http://localhost:080/x", Some(false));
        assert_eq!(padded_port.port, Some(80));
        assert_eq!(
            padded_port.same_os_equivalents,
            vec![
                "http://127.0.0.1:080/x".to_owned(),
                "http://localhost:080/x".to_owned()
            ]
        );
    }

    #[test]
    fn default_wsl_distro_follows_wsl_distro_name() {
        assert_eq!(normalize_distro(None), None);
        assert_eq!(normalize_distro(Some("")), None);
        assert_eq!(normalize_distro(Some("   ")), None);
        assert_eq!(
            normalize_distro(Some(" Ubuntu-24.04 ")).as_deref(),
            Some("Ubuntu-24.04")
        );
        assert_eq!(normalize_distro(Some("Ubuntu/evil")), None);
        assert_eq!(normalize_distro(Some(r"Ubuntu\evil")), None);
        assert_eq!(normalize_distro(Some("bad\0name")), None);
        let expected = std::env::var("WSL_DISTRO_NAME")
            .ok()
            .as_deref()
            .and_then(|value| normalize_distro(Some(value)));
        assert_eq!(default_wsl_distro(), expected);
        if std::env::var_os("WSL_DISTRO_NAME").is_none() {
            assert_eq!(default_wsl_distro(), None);
        }
    }
}
