//! `vyane clean` deletes the local run ledger and feedback file.
//!
//! `history` removes `ledger.jsonl` and sibling rotate sidecars. The
//! `history-archive/` directory next to that ledger is never entered or
//! deleted. Feedback removal is the single `feedback.jsonl` file. Sessions,
//! workflows, worktrees, and the goal, task, agent, and message databases
//! are not targets. The command does not read file contents, configuration,
//! or secrets.

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use vyane_service::StoragePaths;

use crate::cli::{CleanArgs, CleanWhat};

const ARCHIVE_DIR_NAME: &str = "history-archive";
const CONFIRMATION: &str = concat!(
    "Refusing to delete local Vyane data without --yes. ",
    "Use --dry-run to preview or pass --yes to delete."
);

struct TargetPlan {
    name: &'static str,
    files: Vec<TargetFile>,
}

struct TargetFile {
    path: PathBuf,
    bytes: u64,
}

pub fn run(args: CleanArgs) -> Result<ExitCode> {
    if !args.dry_run && !args.yes {
        eprintln!("{}", crate::output::format_error_line(CONFIRMATION));
        return Ok(ExitCode::from(2));
    }
    let paths = StoragePaths::resolve().context("could not resolve the Vyane data directory")?;
    let plans = plan_targets(&paths, args.what)?;
    apply_plans(&plans, &archive_dir(&paths), args.dry_run)
}

fn archive_dir(paths: &StoragePaths) -> PathBuf {
    paths.data_dir.join(ARCHIVE_DIR_NAME)
}

fn plan_targets(paths: &StoragePaths, what: CleanWhat) -> Result<Vec<TargetPlan>> {
    let archive = archive_dir(paths);
    let mut plans = Vec::new();
    if matches!(what, CleanWhat::All | CleanWhat::History) {
        plans.push(plan_history(paths, &archive)?);
    }
    if matches!(what, CleanWhat::All | CleanWhat::Feedback) {
        plans.push(plan_feedback(paths, &archive)?);
    }
    Ok(plans)
}

fn plan_history(paths: &StoragePaths, archive: &Path) -> Result<TargetPlan> {
    let mut files = Vec::new();
    if let Some(primary) = require_file_or_absent(&paths.ledger_path)? {
        refuse_archive(&primary.path, archive)?;
        files.push(primary);
    }
    if let Some(name) = paths.ledger_path.file_name().and_then(|name| name.to_str()) {
        files.extend(rotate_sidecars(&paths.data_dir, name, archive)?);
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files.dedup_by(|left, right| left.path == right.path);
    Ok(TargetPlan {
        name: "history",
        files,
    })
}

fn plan_feedback(paths: &StoragePaths, archive: &Path) -> Result<TargetPlan> {
    let mut files = Vec::new();
    if let Some(primary) = require_file_or_absent(&paths.feedback_path())? {
        refuse_archive(&primary.path, archive)?;
        files.push(primary);
    }
    Ok(TargetPlan {
        name: "feedback",
        files,
    })
}

fn rotate_sidecars(data_dir: &Path, ledger_name: &str, archive: &Path) -> Result<Vec<TargetFile>> {
    let mut files = Vec::new();
    let entries = match fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(files),
        Err(err) => return Err(err).context("could not read the Vyane data directory"),
    };
    for entry in entries {
        let entry = entry.context("could not read the Vyane data directory")?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name == ARCHIVE_DIR_NAME || !is_ledger_rotate_sidecar(name, ledger_name) {
            continue;
        }
        let path = data_dir.join(name);
        if is_inside_archive(&path, archive) {
            continue;
        }
        if let Some(file) = optional_symlink_or_file(&path)? {
            files.push(file);
        }
    }
    Ok(files)
}

fn apply_plans(plans: &[TargetPlan], archive: &Path, dry_run: bool) -> Result<ExitCode> {
    let mut cleaned = 0usize;
    for plan in plans {
        if plan.files.is_empty() {
            if dry_run {
                write_line(&format!("  {}: not found (skip)", plan.name))?;
            }
            continue;
        }
        let message = removal_line(plan, dry_run);
        if !dry_run {
            for file in &plan.files {
                delete_target(&file.path, archive)?;
            }
        }
        write_line(&message)?;
        cleaned += 1;
    }
    if cleaned == 0 {
        write_line("  Nothing to clean.")?;
    } else if dry_run {
        write_line("")?;
        write_line("  (dry run \u{2014} no files removed)")?;
    }
    Ok(ExitCode::SUCCESS)
}

fn removal_line(plan: &TargetPlan, dry_run: bool) -> String {
    let bytes = plan.files.iter().map(|file| file.bytes).sum::<u64>();
    let size = format_bytes(bytes);
    let verb = if dry_run { "Would remove" } else { "Cleaned" };
    if plan.files.len() == 1 {
        format!("  {verb} {} ({size} bytes)", plan.name)
    } else {
        format!(
            "  {verb} {} ({} files, {size} bytes)",
            plan.name,
            plan.files.len()
        )
    }
}

fn require_file_or_absent(path: &Path) -> Result<Option<TargetFile>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            let kind = meta.file_type();
            if kind.is_dir() {
                bail!("{} is a directory and was not removed", file_label(path));
            }
            if kind.is_file() || kind.is_symlink() {
                Ok(Some(TargetFile {
                    path: path.to_path_buf(),
                    bytes: meta.len(),
                }))
            } else {
                bail!(
                    "{} is not a regular file and was not removed",
                    file_label(path)
                );
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).context(format!("could not stat {}", file_label(path))),
    }
}

fn optional_symlink_or_file(path: &Path) -> Result<Option<TargetFile>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            let kind = meta.file_type();
            if kind.is_file() || kind.is_symlink() {
                Ok(Some(TargetFile {
                    path: path.to_path_buf(),
                    bytes: meta.len(),
                }))
            } else {
                Ok(None)
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).context(format!("could not stat {}", file_label(path))),
    }
}

fn delete_target(path: &Path, archive: &Path) -> Result<()> {
    refuse_archive(path, archive)?;
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => {
            bail!("{} is a directory and was not removed", file_label(path));
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).context(format!("could not stat {}", file_label(path))),
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).context(format!("could not remove {}", file_label(path))),
    }
}

fn refuse_archive(path: &Path, archive: &Path) -> Result<()> {
    if is_inside_archive(path, archive) {
        bail!("refusing to delete a path inside history-archive");
    }
    Ok(())
}

fn is_inside_archive(path: &Path, archive: &Path) -> bool {
    path.starts_with(archive)
}

/// A rotate sidecar is a direct sibling of the ledger whose name ends in
/// `.rotate` and is that ledger's file name, with an optional extra dotted
/// suffix and an optional leading dot.
fn is_ledger_rotate_sidecar(name: &str, ledger_file_name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".rotate") else {
        return false;
    };
    if stem == ledger_file_name {
        return true;
    }
    if stem
        .strip_prefix(ledger_file_name)
        .is_some_and(|rest| rest.starts_with('.'))
    {
        return true;
    }
    let Some(hidden) = stem.strip_prefix('.') else {
        return false;
    };
    hidden == ledger_file_name
        || hidden
            .strip_prefix(ledger_file_name)
            .is_some_and(|rest| rest.starts_with('.'))
}

fn file_label(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file")
        .to_string()
}

fn format_bytes(bytes: u64) -> String {
    let digits = bytes.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().rev().enumerate() {
        if index > 0 && index.is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    grouped.chars().rev().collect()
}

fn write_line(line: &str) -> Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{line}").context("could not write the clean result")?;
    stdout.flush().context("could not write the clean result")?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;

    use super::{format_bytes, is_inside_archive, is_ledger_rotate_sidecar};

    #[test]
    fn rotate_sidecar_names_match_only_the_ledger() {
        let ledger = "ledger.jsonl";
        assert!(is_ledger_rotate_sidecar("ledger.jsonl.rotate", ledger));
        assert!(is_ledger_rotate_sidecar(
            "ledger.jsonl.partial.rotate",
            ledger
        ));
        assert!(is_ledger_rotate_sidecar(".ledger.jsonl.rotate", ledger));
        assert!(is_ledger_rotate_sidecar(".ledger.jsonl.1.rotate", ledger));
        assert!(!is_ledger_rotate_sidecar("ledger.jsonl", ledger));
        assert!(!is_ledger_rotate_sidecar("ledger.jsonl.rotate.tmp", ledger));
        assert!(!is_ledger_rotate_sidecar("ledger.jsonl.tmp", ledger));
        assert!(!is_ledger_rotate_sidecar("feedback.jsonl.rotate", ledger));
        assert!(!is_ledger_rotate_sidecar("notes.rotate", ledger));
        assert!(!is_ledger_rotate_sidecar("history-archive", ledger));
        assert!(!is_ledger_rotate_sidecar("2026-11.jsonl.gz", ledger));
    }

    #[test]
    fn byte_counts_use_thousands_separators() {
        assert_eq!(format_bytes(0), "0");
        assert_eq!(format_bytes(999), "999");
        assert_eq!(format_bytes(1000), "1,000");
        assert_eq!(format_bytes(1_234_567), "1,234,567");
    }

    #[test]
    fn archive_protection_is_component_wise() {
        let archive = Path::new("/data/history-archive");
        assert!(is_inside_archive(
            Path::new("/data/history-archive"),
            archive
        ));
        assert!(is_inside_archive(
            Path::new("/data/history-archive/2026-11.jsonl.gz"),
            archive
        ));
        assert!(!is_inside_archive(
            Path::new("/data/history-archive-extra"),
            archive
        ));
        assert!(!is_inside_archive(Path::new("/data/ledger.jsonl"), archive));
    }
}
