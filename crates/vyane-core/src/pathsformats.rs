//! POSIX and Windows path strings and exact UTF-8 reads.
//!
//! A later read has to reproduce text that was stored without newline
//! translation, including a carriage return that a Windows checkout leaves in
//! the file. On POSIX, [`read_text_exact`] matches `pathlib.Path.read_text`:
//! `\r\n` and a lone `\r` become `\n`. On Windows it leaves both in place, so
//! the string still matches a write that disabled newline translation.
//!
//! [`strip_extended_path_prefix`] removes the Win32 `\\?\` prefix on Windows
//! and returns its input unchanged on POSIX. [`windows_filename_problem`]
//! reports one unsafe Windows filename component; the check is the same on
//! every host, and the caller decides whether to apply it.
//! [`local_locator_kind`] names the absolute-path locator for this host.
//!
//! The helpers stand alone. Dispatch, receipts, the CLI, and the auto,
//! failover, and broadcast pools stay unchanged.

use std::collections::BTreeSet;
use std::path::Path;

use crate::Result;

/// Win32 extended-length UNC prefix (`\\?\UNC\`). Longer than [`_EXTENDED`],
/// so it has to be tested first.
const _EXTENDED_UNC: &str = r"\\?\UNC\";

/// Win32 extended-length prefix (`\\?\`).
const _EXTENDED: &str = r"\\?\";

/// Characters Win32 refuses in a single filename component.
const _WINDOWS_FORBIDDEN_CHARS: &str = r#"<>:"/\|?*"#;

/// DOS device names. `NUL.json` still names the `NUL` device, so the
/// comparison uses the stem before the first `.`.
const _WINDOWS_DEVICE_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "COM1", "COM2", "COM3", "COM4", "COM5",
    "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8",
    "LPT9",
];

/// Read UTF-8 text the way a write with newline translation disabled stored it.
///
/// On POSIX this matches `pathlib.Path.read_text(encoding="utf-8")`: `\r\n`
/// and a lone `\r` become `\n`. On Windows the read does not translate
/// newlines, so `\r\n` and a lone `\r` stay in the string.
///
/// A missing path and invalid UTF-8 are I/O errors. The encoding is UTF-8.
pub fn read_text_exact(path: impl AsRef<Path>) -> Result<String> {
    read_text_exact_on(path.as_ref(), cfg!(windows))
}

/// Drop a Win32 extended-length prefix.
///
/// On POSIX `path` is returned unchanged. On Windows, a string that starts
/// with `\\?\UNC\` becomes `\\` plus the remainder, and any other string that
/// starts with `\\?\` loses that prefix. `\\?\C:\work\repo` becomes
/// `C:\work\repo`, and `\\?\UNC\host\share` becomes `\\host\share`. The `UNC`
/// prefix is tested first, and the match is case-sensitive. Every other
/// string is returned unchanged.
#[must_use]
pub fn strip_extended_path_prefix(path: &str) -> String {
    strip_extended_path_prefix_on(path, cfg!(windows))
}

/// Why `name` is not a safe single Windows filename component, or `None`
/// when it is safe.
///
/// The check is pure string logic and is the same on every host. Callers
/// decide whether the host is Windows.
///
/// A component is unsafe when it contains one of `<>:"/\|?*` (`:` addresses
/// an NTFS alternate data stream), a character whose code point is below 32,
/// a trailing `.` or space (Win32 strips those, so two names would refer to
/// one file), or a DOS device name with or without an extension (`NUL.json`
/// is the `NUL` device).
///
/// Device names are `CON`, `PRN`, `AUX`, `NUL`, `CONIN$`, `CONOUT$`, `COM1`
/// through `COM9`, and `LPT1` through `LPT9`. Comparison takes the text
/// before the first `.`, removes trailing spaces from that stem, and converts
/// it to uppercase.
#[must_use]
pub fn windows_filename_problem(name: &str) -> Option<String> {
    let bad: BTreeSet<char> = name
        .chars()
        .filter(|&ch| is_forbidden_or_control(ch))
        .collect();
    if !bad.is_empty() {
        let shown = bad
            .iter()
            .map(|ch| python_char_repr(*ch))
            .collect::<Vec<_>>()
            .join(", ");
        return Some(format!(
            "contains characters Windows does not allow in file names ({shown})"
        ));
    }
    if name.ends_with('.') || name.ends_with(' ') {
        return Some("ends with a dot or space, which Windows strips from file names".to_owned());
    }
    let stem = device_stem(name);
    if _WINDOWS_DEVICE_NAMES.contains(&stem.as_str()) {
        return Some(format!("uses the reserved Windows device name {stem}"));
    }
    None
}

/// Locator kind for an absolute path that is native to this host.
///
/// Windows returns `windows_absolute_path`. Every other host returns
/// `posix_absolute_path`.
#[must_use]
pub fn local_locator_kind() -> &'static str {
    local_locator_kind_on(cfg!(windows))
}

fn read_text_exact_on(path: &Path, on_windows: bool) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
    Ok(normalize_newlines(text, on_windows))
}

/// `on_windows` keeps every carriage return. POSIX applies the universal
/// newline translation of `Path.read_text`.
fn normalize_newlines(text: String, on_windows: bool) -> String {
    if on_windows || !text.contains('\r') {
        text
    } else {
        translate_universal_newlines(&text)
    }
}

/// Translate `\r\n` and a lone `\r` to `\n`. `\n` stays `\n`.
fn translate_universal_newlines(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\r' {
            if chars.peek() == Some(&'\n') {
                chars.next();
            }
            out.push('\n');
        } else {
            out.push(ch);
        }
    }
    out
}

fn strip_extended_path_prefix_on(path: &str, on_windows: bool) -> String {
    if !on_windows {
        return path.to_owned();
    }
    if let Some(rest) = path.strip_prefix(_EXTENDED_UNC) {
        return format!("\\\\{rest}");
    }
    if let Some(rest) = path.strip_prefix(_EXTENDED) {
        return rest.to_owned();
    }
    path.to_owned()
}

fn local_locator_kind_on(on_windows: bool) -> &'static str {
    if on_windows {
        "windows_absolute_path"
    } else {
        "posix_absolute_path"
    }
}

fn is_forbidden_or_control(ch: char) -> bool {
    _WINDOWS_FORBIDDEN_CHARS.contains(ch) || u32::from(ch) < 32
}

/// Python `repr` of one character, for the messages
/// [`windows_filename_problem`] returns.
///
/// Rust's `Debug` for `char` is a different spelling (`'\u{1}'` rather than
/// `'\x01'`), so the text is spelled out here.
fn python_char_repr(ch: char) -> String {
    match ch {
        '\\' => r"'\\'".to_owned(),
        '\t' => r"'\t'".to_owned(),
        '\n' => r"'\n'".to_owned(),
        '\r' => r"'\r'".to_owned(),
        ch if u32::from(ch) < 32 => format!("'\\x{:02x}'", u32::from(ch)),
        ch => format!("'{ch}'"),
    }
}

fn device_stem(name: &str) -> String {
    let head = match name.split_once('.') {
        Some((head, _)) => head,
        None => name,
    };
    head.trim_end_matches(' ').to_uppercase()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;

    /// `(payload, posix Path.read_text result)`. The Windows read returns
    /// `payload` unchanged.
    const EXACT_READS: &[(&str, &str)] = &[
        ("plain\n", "plain\n"),
        ("crlf\r\nline\r\n", "crlf\nline\n"),
        ("lone\rcr", "lone\ncr"),
        ("mixed\n\r\n\r", "mixed\n\n\n"),
        ("a\r\nb\n", "a\nb\n"),
        ("运行\r\n甲", "运行\n甲"),
        ("\u{feff}bom\r\n", "\u{feff}bom\n"),
    ];

    fn write_payload(dir: &Path, name: &str, payload: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, payload.as_bytes()).unwrap();
        path
    }

    #[test]
    fn exact_read_round_trips_crlf_and_lone_cr() {
        let dir = tempfile::tempdir().unwrap();
        for (payload, posix) in EXACT_READS {
            let path = write_payload(dir.path(), "context.md", payload);
            assert_eq!(fs::read(&path).unwrap(), payload.as_bytes());

            assert_eq!(read_text_exact_on(&path, true).unwrap(), *payload);
            assert_eq!(read_text_exact_on(&path, false).unwrap(), *posix);
            assert_eq!(fs::read(&path).unwrap(), payload.as_bytes());

            let host = read_text_exact(&path).unwrap();
            if cfg!(windows) || !payload.contains('\r') {
                assert_eq!(host, *payload);
            } else {
                assert_eq!(host, *posix);
            }
        }
    }

    #[test]
    fn read_text_exact_matches_universal_newlines_on_posix() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_payload(dir.path(), "x.txt", "a\r\nb\n");
        assert_eq!(read_text_exact_on(&path, false).unwrap(), "a\nb\n");
        if !cfg!(windows) {
            assert_eq!(read_text_exact(&path).unwrap(), "a\nb\n");
        }
    }

    #[test]
    fn read_text_exact_preserves_cr_on_windows() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_payload(dir.path(), "x.txt", "crlf\r\nline\r\nlone\rcr");
        assert_eq!(
            read_text_exact_on(&path, true).unwrap(),
            "crlf\r\nline\r\nlone\rcr"
        );
        if cfg!(windows) {
            assert_eq!(read_text_exact(&path).unwrap(), "crlf\r\nline\r\nlone\rcr");
        }
    }

    #[test]
    fn read_text_exact_rejects_invalid_utf8_and_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.txt");
        let missing_err = read_text_exact(&missing).unwrap_err();
        assert_eq!(missing_err.kind, crate::ErrorKind::Io);

        let bad = dir.path().join("bad.txt");
        fs::write(&bad, b"a\xffb").unwrap();
        let bad_err = read_text_exact(&bad).unwrap_err();
        assert_eq!(bad_err.kind, crate::ErrorKind::Io);
    }

    #[test]
    fn strip_extended_path_prefix_is_identity_on_posix() {
        for path in [
            r"\\?\C:\x",
            r"\\?\C:\work\repo",
            r"\\?\UNC\host\share\dir",
            r"C:\plain",
        ] {
            assert_eq!(strip_extended_path_prefix_on(path, false), path);
        }
        if !cfg!(windows) {
            assert_eq!(strip_extended_path_prefix(r"\\?\C:\x"), r"\\?\C:\x");
        }
    }

    #[test]
    fn strip_extended_path_prefix_on_windows() {
        let cases = [
            (r"\\?\C:\work\repo", r"C:\work\repo"),
            (r"\\?\C:\x", r"C:\x"),
            (r"\\?\UNC\host\share", r"\\host\share"),
            (r"\\?\UNC\host\share\dir", r"\\host\share\dir"),
            (r"C:\plain", r"C:\plain"),
            // `UNC` is case-sensitive, and the longer prefix requires the
            // trailing separator. `\\.\` is a different namespace.
            (r"\\?\unc\host\share", r"unc\host\share"),
            (r"\\?\UNC", "UNC"),
            (r"\\?\", ""),
            (r"\\.\C:\x", r"\\.\C:\x"),
            ("", ""),
        ];
        for (path, expected) in cases {
            assert_eq!(
                strip_extended_path_prefix_on(path, true),
                expected,
                "{path}"
            );
        }
        if cfg!(windows) {
            assert_eq!(
                strip_extended_path_prefix(r"\\?\C:\work\repo"),
                r"C:\work\repo"
            );
            assert_eq!(
                strip_extended_path_prefix(r"\\?\UNC\host\share"),
                r"\\host\share"
            );
        }
    }

    #[test]
    fn windows_filename_problem_rejects_unsafe_components() {
        for name in [
            "run:stream",
            "a<b",
            "a>b",
            "a\"b",
            "a|b",
            "a?b",
            "a*b",
            "a/b",
            "a\\b",
            "tab\tname",
            "trailing.",
            "trailing ",
            "NUL",
            "nul.json",
            "Com1.txt",
            "LPT9",
            "CONOUT$",
        ] {
            assert!(windows_filename_problem(name).is_some(), "{name:?}");
        }
    }

    #[test]
    fn windows_filename_problem_messages_match_python() {
        let cases = [
            (
                "run:stream",
                "contains characters Windows does not allow in file names (':')",
            ),
            ("nul.json", "uses the reserved Windows device name NUL"),
            (
                "trailing.",
                "ends with a dot or space, which Windows strips from file names",
            ),
            (
                "trailing ",
                "ends with a dot or space, which Windows strips from file names",
            ),
            (
                "a\"b",
                "contains characters Windows does not allow in file names ('\"')",
            ),
            (
                "a\\b",
                "contains characters Windows does not allow in file names ('\\\\')",
            ),
            (
                "tab\tname",
                "contains characters Windows does not allow in file names ('\\t')",
            ),
            (
                "\u{1}",
                "contains characters Windows does not allow in file names ('\\x01')",
            ),
            (
                "\r\n",
                "contains characters Windows does not allow in file names ('\\n', '\\r')",
            ),
            (
                "a<b>c",
                "contains characters Windows does not allow in file names ('<', '>')",
            ),
            (
                "a:b<c",
                "contains characters Windows does not allow in file names (':', '<')",
            ),
            (
                ":::",
                "contains characters Windows does not allow in file names (':')",
            ),
            ("Com1.txt", "uses the reserved Windows device name COM1"),
            ("CON .txt", "uses the reserved Windows device name CON"),
            ("conin$", "uses the reserved Windows device name CONIN$"),
            (
                "NUL.",
                "ends with a dot or space, which Windows strips from file names",
            ),
            (
                "COM1.",
                "ends with a dot or space, which Windows strips from file names",
            ),
        ];
        for (name, expected) in cases {
            assert_eq!(
                windows_filename_problem(name).as_deref(),
                Some(expected),
                "{name:?}"
            );
        }
    }

    #[test]
    fn windows_filename_problem_accepts_ordinary_components() {
        for name in [
            "run-1",
            "运行 甲",
            "run.v1-2_3",
            "console",
            "nullable.json",
            "COM10",
            "COM0",
            "LPT0",
            "LPT10",
            "CONIN",
            "CONOUT",
            ".CON",
            "",
            "\u{7f}",
            "\u{85}",
            "NUL\u{a0}",
        ] {
            assert_eq!(windows_filename_problem(name), None, "{name:?}");
        }
    }

    #[test]
    fn dos_device_names_match_with_or_without_an_extension() {
        for device in _WINDOWS_DEVICE_NAMES {
            let expected = format!("uses the reserved Windows device name {device}");
            assert_eq!(
                windows_filename_problem(device).as_deref(),
                Some(expected.as_str())
            );
            let lower = device.to_lowercase();
            assert_eq!(
                windows_filename_problem(&lower).as_deref(),
                Some(expected.as_str()),
                "{lower}"
            );
            let with_ext = format!("{lower}.json");
            assert_eq!(
                windows_filename_problem(&with_ext).as_deref(),
                Some(expected.as_str()),
                "{with_ext}"
            );
            let spaced = format!("{device} .bin");
            assert_eq!(
                windows_filename_problem(&spaced).as_deref(),
                Some(expected.as_str()),
                "{spaced}"
            );
        }
    }

    #[test]
    fn local_locator_kind_matches_host() {
        assert_eq!(local_locator_kind_on(true), "windows_absolute_path");
        assert_eq!(local_locator_kind_on(false), "posix_absolute_path");
        if cfg!(windows) {
            assert_eq!(local_locator_kind(), "windows_absolute_path");
        } else {
            assert_eq!(local_locator_kind(), "posix_absolute_path");
        }
    }
}
