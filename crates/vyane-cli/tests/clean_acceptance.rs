//! `vyane clean` deletes only the local ledger and feedback file.
//!
//! These tests point `VYANE_DATA_DIR` at a temp directory. They do not use
//! the default home directory as a required case.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

use assert_cmd::Command;

struct Layout {
    root: tempfile::TempDir,
    data: PathBuf,
    home: PathBuf,
}

fn layout() -> Layout {
    let root = common::private_tempdir();
    let data = root.path().join("data");
    let home = root.path().join("home");
    fs::create_dir_all(&data).unwrap();
    fs::create_dir_all(&home).unwrap();
    Layout { root, data, home }
}

fn vyane(layout: &Layout) -> Command {
    let mut command = Command::cargo_bin("vyane").unwrap();
    command
        .current_dir(layout.root.path())
        .env_remove("RUST_LOG")
        .env_remove("VYANE_ENV_FILE")
        .env("VYANE_DATA_DIR", &layout.data)
        .env("HOME", &layout.home)
        .env("USERPROFILE", &layout.home)
        .env("XDG_DATA_HOME", layout.home.join("xdg-data"))
        .env("XDG_CONFIG_HOME", layout.home.join("xdg-config"));
    command
}

fn run(layout: &Layout, args: &[&str]) -> Output {
    let output = vyane(layout).args(args).output().unwrap();
    assert_home_untouched(layout);
    output
}

fn assert_home_untouched(layout: &Layout) {
    let entries: Vec<_> = fs::read_dir(&layout.home)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(
        entries.is_empty(),
        "clean touched the home directory: {entries:?}"
    );
}

fn write(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, bytes).unwrap();
}

fn seed_acceptance(data: &Path) {
    write(&data.join("ledger.jsonl"), b"ledger\n");
    write(&data.join("feedback.jsonl"), b"feedback\n");
    write(&data.join("history-archive/2026-11.jsonl.gz"), b"keep-me");
    write(
        &data.join("history-archive/.pending/prefix.bin"),
        b"pending",
    );
    write(&data.join("sessions/keep.json"), b"session");
    write(&data.join("workflows/run.json"), b"workflow");
    write(&data.join("worktrees/keep"), b"worktree");
    write(&data.join("tasks.sqlite3"), b"tasks");
    write(&data.join("agent-runs.sqlite3"), b"agents");
    write(&data.join("messages.sqlite3"), b"messages");
    write(&data.join("goals.sqlite3"), b"goals");
    write(&data.join("events/local.jsonl"), b"event");
    write(&data.join("secrets.env"), b"TOKEN=example\n");
}

fn seed_rotate(data: &Path) {
    seed_acceptance(data);
    write(&data.join("ledger.jsonl.rotate"), b"rotate\n");
    write(&data.join("ledger.jsonl.partial.rotate"), b"part\n");
    write(&data.join(".ledger.jsonl.rotate"), b"hide\n");
    write(&data.join("ledger.jsonl.tmp"), b"tmp\n");
    write(&data.join("ledger.jsonl.rotate.tmp"), b"tmp2\n");
    write(&data.join("feedback.jsonl.rotate"), b"fbrot\n");
    write(&data.join("notes.rotate"), b"notes");
    write(
        &data.join("history-archive/ledger.jsonl.rotate"),
        b"archived-rotate",
    );
    write(&data.join("sessions/ledger.jsonl.rotate"), b"nested-rotate");
    write(&data.join("other/ledger.jsonl.rotate"), b"other-rotate");
    fs::create_dir_all(data.join("ledger.jsonl.rotate.dir")).unwrap();
    write(&data.join("ledger.jsonl.rotate.dir/keep"), b"dir-keep");
}

fn tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        let rel = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let kind = fs::symlink_metadata(&path).unwrap().file_type();
        if kind.is_symlink() {
            let target = fs::read_link(&path).unwrap();
            out.insert(
                rel,
                format!("symlink:{}", target.to_string_lossy()).into_bytes(),
            );
        } else if kind.is_dir() {
            walk(root, &path, out);
        } else {
            out.insert(rel, fs::read(&path).unwrap());
        }
    }
}

fn assert_removed(
    before: &BTreeMap<String, Vec<u8>>,
    after: &BTreeMap<String, Vec<u8>>,
    removed: &[&str],
) {
    let mut expected = before.clone();
    for key in removed {
        assert!(
            expected.remove(*key).is_some(),
            "seed did not contain {key}"
        );
    }
    assert_eq!(after, &expected);
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

#[test]
fn dry_run_and_missing_confirmation_leave_every_file_in_place() {
    let layout = layout();
    seed_acceptance(&layout.data);
    let before = tree(&layout.data);

    let dry = run(&layout, &["clean", "--dry-run"]);
    assert!(dry.status.success(), "{}", stderr(&dry));
    assert_eq!(
        stdout(&dry),
        "  Would remove history (7 bytes)\n  Would remove feedback (9 bytes)\n\n  (dry run \u{2014} no files removed)\n"
    );
    assert!(stderr(&dry).is_empty(), "{}", stderr(&dry));
    assert_eq!(tree(&layout.data), before);

    let dry_yes = run(&layout, &["clean", "all", "--dry-run", "--yes"]);
    assert!(dry_yes.status.success(), "{}", stderr(&dry_yes));
    assert_eq!(tree(&layout.data), before);

    let refused = run(&layout, &["clean", "history"]);
    assert_eq!(refused.status.code(), Some(2));
    assert!(stdout(&refused).is_empty(), "{}", stdout(&refused));
    assert_eq!(
        stderr(&refused),
        "error: Refusing to delete local Vyane data without --yes. Use --dry-run to preview or pass --yes to delete.\n"
    );
    assert_eq!(tree(&layout.data), before);

    let refused_all = run(&layout, &["clean"]);
    assert_eq!(refused_all.status.code(), Some(2));
    assert_eq!(tree(&layout.data), before);
}

#[test]
fn history_yes_removes_only_the_ledger_and_feedback_yes_removes_only_the_rating() {
    let history_layout = layout();
    seed_acceptance(&history_layout.data);
    let before = tree(&history_layout.data);
    let output = run(&history_layout, &["clean", "--yes", "history"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "  Cleaned history (7 bytes)\n");
    assert!(stderr(&output).is_empty(), "{}", stderr(&output));
    assert_removed(&before, &tree(&history_layout.data), &["ledger.jsonl"]);
    assert_eq!(
        fs::read(history_layout.data.join("history-archive/2026-11.jsonl.gz")).unwrap(),
        b"keep-me"
    );
    assert_eq!(
        fs::read(history_layout.data.join("sessions/keep.json")).unwrap(),
        b"session"
    );

    let feedback_layout = layout();
    seed_acceptance(&feedback_layout.data);
    let before = tree(&feedback_layout.data);
    let output = run(&feedback_layout, &["clean", "feedback", "--yes"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "  Cleaned feedback (9 bytes)\n");
    assert_removed(&before, &tree(&feedback_layout.data), &["feedback.jsonl"]);
    assert!(feedback_layout.data.join("ledger.jsonl").is_file());
    assert!(
        feedback_layout
            .data
            .join("history-archive/2026-11.jsonl.gz")
            .is_file()
    );
}

#[test]
fn history_removes_rotate_sidecars_without_entering_the_archive_or_other_trees() {
    let layout = layout();
    seed_rotate(&layout.data);
    let before = tree(&layout.data);
    let output = run(&layout, &["clean", "history", "--yes"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "  Cleaned history (4 files, 24 bytes)\n");
    assert_removed(
        &before,
        &tree(&layout.data),
        &[
            "ledger.jsonl",
            "ledger.jsonl.rotate",
            "ledger.jsonl.partial.rotate",
            ".ledger.jsonl.rotate",
        ],
    );
}

#[test]
fn a_rotate_directory_is_not_removed() {
    let layout = layout();
    write(&layout.data.join("ledger.jsonl"), b"ledger\n");
    write(
        &layout.data.join("ledger.jsonl.rotate/keep"),
        b"keep-rotate-dir",
    );
    write(
        &layout.data.join("history-archive/2026-11.jsonl.gz"),
        b"keep-me",
    );
    let output = run(&layout, &["clean", "history", "--yes"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), "  Cleaned history (7 bytes)\n");
    assert!(!layout.data.join("ledger.jsonl").exists());
    assert_eq!(
        fs::read(layout.data.join("ledger.jsonl.rotate/keep")).unwrap(),
        b"keep-rotate-dir"
    );
    assert_eq!(
        fs::read(layout.data.join("history-archive/2026-11.jsonl.gz")).unwrap(),
        b"keep-me"
    );
}

#[test]
fn all_removes_the_ledger_its_sidecars_and_feedback_only() {
    let layout = layout();
    seed_rotate(&layout.data);
    let before = tree(&layout.data);
    let output = run(&layout, &["clean", "--yes"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        stdout(&output),
        "  Cleaned history (4 files, 24 bytes)\n  Cleaned feedback (9 bytes)\n"
    );
    assert_removed(
        &before,
        &tree(&layout.data),
        &[
            "ledger.jsonl",
            "ledger.jsonl.rotate",
            "ledger.jsonl.partial.rotate",
            ".ledger.jsonl.rotate",
            "feedback.jsonl",
        ],
    );
}

#[test]
fn unknown_what_fails_closed_and_leaves_files_in_place() {
    let layout = layout();
    seed_acceptance(&layout.data);
    let before = tree(&layout.data);
    for unknown in ["audit", "status", "benchmark", "sessions", ""] {
        let output = run(&layout, &["clean", unknown, "--yes"]);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{unknown}: {}",
            stderr(&output)
        );
        assert!(stdout(&output).is_empty(), "{unknown}: {}", stdout(&output));
        assert!(!stderr(&output).is_empty(), "{unknown} produced no error");
        assert_eq!(tree(&layout.data), before, "{unknown}");
    }
    let extra = run(&layout, &["clean", "all", "extra", "--yes"]);
    assert_eq!(extra.status.code(), Some(2));
    assert_eq!(tree(&layout.data), before);
}

#[test]
fn missing_files_do_not_create_storage() {
    let layout = layout();
    fs::remove_dir_all(&layout.data).unwrap();
    assert!(!layout.data.exists());

    let refused = run(&layout, &["clean"]);
    assert_eq!(refused.status.code(), Some(2));
    assert!(!layout.data.exists());

    let dry = run(&layout, &["clean", "--dry-run"]);
    assert!(dry.status.success(), "{}", stderr(&dry));
    assert_eq!(
        stdout(&dry),
        "  history: not found (skip)\n  feedback: not found (skip)\n  Nothing to clean.\n"
    );
    assert!(!layout.data.exists());

    let deleted = run(&layout, &["clean", "history", "--yes"]);
    assert!(deleted.status.success(), "{}", stderr(&deleted));
    assert_eq!(stdout(&deleted), "  Nothing to clean.\n");
    assert!(!layout.data.exists());
}

#[test]
fn a_directory_is_never_removed() {
    let layout = layout();
    fs::create_dir_all(layout.data.join("ledger.jsonl")).unwrap();
    write(&layout.data.join("ledger.jsonl/keep"), b"keep");
    write(&layout.data.join("feedback.jsonl"), b"feedback\n");
    let before = tree(&layout.data);

    let output = run(&layout, &["clean", "all", "--yes"]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("ledger.jsonl is a directory and was not removed"),
        "{}",
        stderr(&output)
    );
    assert_eq!(tree(&layout.data), before);

    let feedback = run(&layout, &["clean", "feedback", "--yes"]);
    assert!(feedback.status.success(), "{}", stderr(&feedback));
    assert_eq!(stdout(&feedback), "  Cleaned feedback (9 bytes)\n");
    assert!(layout.data.join("ledger.jsonl/keep").is_file());
    assert!(!layout.data.join("feedback.jsonl").exists());
}

#[test]
fn clean_does_not_read_configuration() {
    let layout = layout();
    seed_acceptance(&layout.data);
    let config = layout.root.path().join("broken.toml");
    let contents = "this is not toml\nTOKEN = \"not-read\"\n";
    fs::write(&config, contents).unwrap();
    let before = tree(&layout.data);
    let output = vyane(&layout)
        .args(["--config", config.to_str().unwrap(), "clean", "--dry-run"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(!stdout(&output).contains("not-read"));
    assert_eq!(fs::read_to_string(&config).unwrap(), contents);
    assert_eq!(tree(&layout.data), before);
    assert_home_untouched(&layout);
}

#[test]
fn thousand_byte_ledger_is_grouped_in_the_message() {
    let layout = layout();
    write(&layout.data.join("ledger.jsonl"), &vec![b'x'; 1000]);
    let output = run(&layout, &["clean", "history", "--dry-run"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        stdout(&output),
        "  Would remove history (1,000 bytes)\n\n  (dry run \u{2014} no files removed)\n"
    );
    assert_eq!(
        fs::read(layout.data.join("ledger.jsonl")).unwrap(),
        vec![b'x'; 1000]
    );
}

#[test]
fn help_names_the_archive_and_does_not_create_storage() {
    let layout = layout();
    fs::remove_dir_all(&layout.data).unwrap();
    let output = run(&layout, &["clean", "--help"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("history-archive/"));
    assert!(text.contains("--dry-run"));
    assert!(text.contains("--yes"));
    assert!(text.contains("audit"));
    assert!(!layout.data.exists());
}

#[cfg(unix)]
#[test]
fn history_unlinks_ledger_symlinks_without_deleting_archive_bytes() {
    let layout = layout();
    write(
        &layout.data.join("history-archive/2026-11.jsonl.gz"),
        b"keep-me",
    );
    write(&layout.data.join("sessions/keep.json"), b"session");
    std::os::unix::fs::symlink(
        "history-archive/2026-11.jsonl.gz",
        layout.data.join("ledger.jsonl"),
    )
    .unwrap();
    std::os::unix::fs::symlink("history-archive", layout.data.join("ledger.jsonl.rotate")).unwrap();
    let output = run(&layout, &["clean", "history", "--yes"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(!layout.data.join("ledger.jsonl").exists());
    assert!(!layout.data.join("ledger.jsonl.rotate").exists());
    assert_eq!(
        fs::read(layout.data.join("history-archive/2026-11.jsonl.gz")).unwrap(),
        b"keep-me"
    );
    assert!(layout.data.join("history-archive").is_dir());
    assert_eq!(
        fs::read(layout.data.join("sessions/keep.json")).unwrap(),
        b"session"
    );
}
