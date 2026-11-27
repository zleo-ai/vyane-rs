//! Read-only scan for Codex Linux sandbox processes reparented off the Codex binary.
//!
//! A candidate is `codex-linux-sandbox` (process name or argv0; Linux `comm` may be
//! the 15-character `codex-linux-san`), or a direct `bwrap` / `bubblewrap` parent
//! when a sandbox child did not start before that parent. It is an orphan only when
//! that parent is the current user's `systemd --user`, PID 1, or a process whose
//! status says `ChildSubreaper: 1` and is not itself Codex, bwrap, or the sandbox.
//! The visible ancestor chain, including a visible PID 1, must not contain the
//! `codex` binary. An unseen PID 1 stays a reaper root. An incomplete chain is
//! skipped. A missing `ChildSubreaper` field is not evidence that a process is a
//! subreaper. The scan only reads procfs. It does not signal or stop any process.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Document name for one orphan scan.
pub const SCHEMA: &str = "codex-sandbox-orphans.v1";

/// Age, in seconds, after which the oldest known orphan is `warn`.
pub const DEFAULT_WARN_AFTER_SECONDS: u64 = 6 * 60 * 60;

const CMDLINE_LIMIT: usize = 200;
const SANDBOX_NAME: &str = "codex-linux-sandbox";
const SANDBOX_COMM: &str = "codex-linux-san";
const BWRAP_NAMES: &[&str] = &["bwrap", "bubblewrap"];

/// Where the scan reads the subject user id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectUid {
    /// Real user id of this process. Platforms without one are `uid_unavailable`.
    Current,
    /// Use this id and do not ask the operating system.
    Explicit(u32),
    /// Report `uid_unavailable` without reading procfs.
    Unavailable,
}

/// Inputs for one point-in-time scan. The scan never signals a process.
#[derive(Debug, Clone, PartialEq)]
pub struct CodexSandboxOrphanQuery {
    /// Procfs root. `None` is `/proc` on Linux and unavailable elsewhere.
    pub proc_root: Option<PathBuf>,
    /// Unix seconds used as "now". `None` is the current time.
    pub now_unix_seconds: Option<f64>,
    /// Clock ticks per second. `None` asks the operating system.
    pub clk_tck: Option<u64>,
    /// Subject user id.
    pub uid: SubjectUid,
    /// Strict lower bound for `status: warn`. The default is six hours.
    pub warn_after_seconds: u64,
}

impl Default for CodexSandboxOrphanQuery {
    fn default() -> Self {
        Self {
            proc_root: None,
            now_unix_seconds: None,
            clk_tck: None,
            uid: SubjectUid::Current,
            warn_after_seconds: DEFAULT_WARN_AFTER_SECONDS,
        }
    }
}

/// One `codex-sandbox-orphans.v1` document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexSandboxOrphanReport {
    /// Schema name.
    pub schema: &'static str,
    /// False when the scan could not be performed.
    pub available: bool,
    /// Always true. The scan does not write.
    pub readonly: bool,
    /// Always false. The scan does not stop processes.
    pub cleanup: bool,
    /// `ok`, `warn`, or `unavailable`.
    pub status: &'static str,
    /// Why the scan is unavailable. `None` when it ran.
    pub reason: Option<&'static str>,
    /// User id the scan was scoped to.
    pub subject_uid: Option<u32>,
    /// Observation time, UTC, truncated to a second.
    pub observed_at: String,
    /// Number of recorded orphans.
    pub orphan_count: u64,
    /// Largest known age. Unknown ages are ignored.
    pub oldest_age_seconds: Option<i64>,
    /// Threshold copied from the query.
    pub warn_after_seconds: u64,
    /// Processes or links skipped because they could not be read or changed.
    pub skipped: u64,
    /// Orphans, oldest known age first.
    pub orphans: Vec<CodexSandboxOrphan>,
}

/// One reparented sandbox or sandbox wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexSandboxOrphan {
    /// Process id.
    pub pid: u32,
    /// Parent process id at the time of the scan.
    pub ppid: u32,
    /// `comm`, falling back to the `stat` name.
    pub comm: Option<String>,
    /// `codex-linux-sandbox` or `bwrap`.
    pub kind: &'static str,
    /// `pid1`, `systemd_user`, or `subreaper`.
    pub parent_kind: &'static str,
    /// Real user id from `status`.
    pub uid: Option<u32>,
    /// Start time, UTC, truncated to a second.
    pub started_at: Option<String>,
    /// Whole seconds since start. `None` when the age is unknown or negative.
    pub age_seconds: Option<i64>,
    /// `cwd` link text, not a canonical path.
    pub cwd: Option<String>,
    /// Joined argv, truncated to 200 characters. `None` when `cmdline` was unread.
    pub cmdline: Option<String>,
    /// Sandbox children this wrapper still owns. Empty for a sandbox row.
    pub wrapped_sandbox_pids: Vec<u32>,
}

#[derive(Debug, Clone)]
struct Proc {
    pid: u32,
    ppid: u32,
    comm: Option<String>,
    argv: Vec<String>,
    cmdline_known: bool,
    exe: Option<String>,
    cwd: Option<String>,
    uid: Option<u32>,
    state: Option<String>,
    start_ticks: u64,
    child_subreaper: Option<bool>,
}

struct StatFields {
    comm: Option<String>,
    state: Option<String>,
    ppid: Option<u32>,
    start_ticks: Option<u64>,
}

struct ProcEntry {
    name: String,
    symlink: bool,
}

enum RootKind {
    Directory,
}

#[derive(Debug)]
enum RootError {
    Missing,
    NotDirectory,
    Unreadable,
}

#[derive(Debug)]
struct Unreadable;

trait ProcFs {
    fn root_kind(&mut self, path: &Path) -> Result<RootKind, RootError>;
    fn list(&mut self, path: &Path) -> Result<Vec<ProcEntry>, RootError>;
    fn read_text(&mut self, path: &Path) -> Result<Option<String>, Unreadable>;
    fn read_bytes(&mut self, path: &Path) -> Result<Option<Vec<u8>>, Unreadable>;
    fn read_link(&mut self, path: &Path) -> Option<String>;
}

struct StdProcFs;

impl ProcFs for StdProcFs {
    fn root_kind(&mut self, path: &Path) -> Result<RootKind, RootError> {
        match fs::metadata(path) {
            Ok(meta) if meta.is_dir() => Ok(RootKind::Directory),
            Ok(_) => Err(RootError::NotDirectory),
            Err(err) if err.kind() == ErrorKind::NotFound => Err(RootError::Missing),
            Err(_) => Err(RootError::Unreadable),
        }
    }

    fn list(&mut self, path: &Path) -> Result<Vec<ProcEntry>, RootError> {
        let entries = fs::read_dir(path).map_err(|_| RootError::Unreadable)?;
        let mut found = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|_| RootError::Unreadable)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let symlink = entry
                .file_type()
                .map(|kind| kind.is_symlink())
                .unwrap_or(false);
            found.push(ProcEntry { name, symlink });
        }
        Ok(found)
    }

    fn read_text(&mut self, path: &Path) -> Result<Option<String>, Unreadable> {
        match fs::read(path) {
            Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(Unreadable),
        }
    }

    fn read_bytes(&mut self, path: &Path) -> Result<Option<Vec<u8>>, Unreadable> {
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(Unreadable),
        }
    }

    fn read_link(&mut self, path: &Path) -> Option<String> {
        fs::read_link(path)
            .ok()
            .map(|target| target.to_string_lossy().into_owned())
    }
}

/// Scan `query` and return one report. This function does not signal a process.
#[must_use]
pub fn scan_codex_sandbox_orphans(query: &CodexSandboxOrphanQuery) -> CodexSandboxOrphanReport {
    let now = match query.now_unix_seconds {
        Some(seconds) if seconds.is_finite() => seconds,
        _ => unix_now(),
    };
    let Some(uid) = resolve_uid(query.uid) else {
        return blank(
            false,
            "unavailable",
            Some("uid_unavailable"),
            query.warn_after_seconds,
            None,
            now,
            0,
        );
    };
    #[cfg(not(target_os = "linux"))]
    if query.proc_root.is_none() {
        return blank(
            false,
            "unavailable",
            Some("proc_unavailable"),
            query.warn_after_seconds,
            Some(uid),
            now,
            0,
        );
    }
    let root = query.proc_root.as_deref().unwrap_or(Path::new("/proc"));
    let mut fs = StdProcFs;
    scan_with(
        &mut fs,
        root,
        uid,
        now,
        query.clk_tck,
        query.warn_after_seconds,
    )
}

/// English text for one report. JSON callers serialize the report instead.
#[must_use]
pub fn render_sandbox_orphan_report(report: &CodexSandboxOrphanReport) -> String {
    if !report.available {
        let reason = report.reason.unwrap_or("unavailable");
        return format!("Codex sandbox orphan check unavailable: {reason}\n");
    }
    let status = if report.status == "warn" {
        "warn"
    } else {
        "ok"
    };
    let mut lines = vec![
        "Codex sandbox orphans (read-only, no cleanup)".to_string(),
        format!(
            "orphans {count}; oldest {oldest}; status {status}; threshold {threshold}; skipped {skipped}",
            count = report.orphan_count,
            oldest = format_age(report.oldest_age_seconds),
            threshold = format_age(Some(
                i64::try_from(report.warn_after_seconds).unwrap_or(i64::MAX)
            )),
            skipped = report.skipped,
        ),
    ];
    for row in &report.orphans {
        let cwd = row.cwd.as_deref().unwrap_or("unreadable");
        let started = row.started_at.as_deref().unwrap_or("unknown");
        let cmdline = row.cmdline.as_deref().unwrap_or("unreadable");
        lines.push(format!(
            "pid={pid} ppid={ppid} kind={kind} parent={parent} age={age} started={started} cwd={cwd} cmd={cmd}",
            pid = row.pid,
            ppid = row.ppid,
            kind = row.kind,
            parent = row.parent_kind,
            age = format_age(row.age_seconds),
            cmd = cmdline,
        ));
    }
    lines.join("\n") + "\n"
}

fn scan_with<F: ProcFs>(
    fs: &mut F,
    root: &Path,
    uid: u32,
    now: f64,
    clk_tck: Option<u64>,
    warn_after: u64,
) -> CodexSandboxOrphanReport {
    match fs.root_kind(root) {
        Ok(RootKind::Directory) => {}
        Err(RootError::Unreadable) => {
            return blank(
                false,
                "unavailable",
                Some("proc_unreadable"),
                warn_after,
                Some(uid),
                now,
                0,
            );
        }
        Err(RootError::Missing | RootError::NotDirectory) => {
            return blank(
                false,
                "unavailable",
                Some("proc_unavailable"),
                warn_after,
                Some(uid),
                now,
                0,
            );
        }
    }
    let (loaded, mut skipped) = match load_all(fs, root, uid) {
        Ok(loaded) => loaded,
        Err(_) => {
            return blank(
                false,
                "unavailable",
                Some("proc_unreadable"),
                warn_after,
                Some(uid),
                now,
                0,
            );
        }
    };
    let by_pid = index_by_pid(&loaded);
    let children = children_of(&loaded);
    let btime = read_btime(fs, root);
    let clk = effective_clk(clk_tck);
    let mut orphans = Vec::new();
    for index in 0..loaded.len() {
        if !is_sandbox(&loaded[index]) && !is_bwrap(&loaded[index]) {
            continue;
        }
        let proc = loaded[index].clone();
        let wrapped = if is_sandbox(&proc) {
            Vec::new()
        } else {
            let kids = children.get(&proc.pid).cloned().unwrap_or_default();
            let wrapped = confirmed_wrapped(fs, root, &loaded, &kids, &proc, &mut skipped);
            if wrapped.is_empty() {
                continue;
            }
            wrapped
        };
        let kind = if is_sandbox(&proc) {
            SANDBOX_NAME
        } else {
            "bwrap"
        };
        let Some(parent_kind) = reaper_kind(proc.ppid, &loaded, &by_pid, uid) else {
            continue;
        };
        match visible_unowned(fs, root, &loaded, &by_pid, &proc, uid) {
            Ok(Some(true)) => {}
            Ok(Some(false)) => continue,
            Ok(None) | Err(Unreadable) => {
                skipped += 1;
                continue;
            }
        }
        let (started_at, age_seconds) = started_age(proc.start_ticks, btime, clk, now);
        let cmdline = cmdline_text(&proc);
        orphans.push(CodexSandboxOrphan {
            pid: proc.pid,
            ppid: proc.ppid,
            comm: proc.comm,
            kind,
            parent_kind,
            uid: proc.uid,
            started_at,
            age_seconds,
            cwd: proc.cwd,
            cmdline,
            wrapped_sandbox_pids: wrapped,
        });
    }
    sort_orphans(&mut orphans);
    let oldest = orphans.iter().filter_map(|row| row.age_seconds).max();
    let status = match oldest {
        Some(age) if age_exceeds(age, warn_after) => "warn",
        _ => "ok",
    };
    let mut report = blank(true, status, None, warn_after, Some(uid), now, skipped);
    report.orphan_count = u64::try_from(orphans.len()).unwrap_or(u64::MAX);
    report.oldest_age_seconds = oldest;
    report.orphans = orphans;
    report
}

fn load_all<F: ProcFs>(fs: &mut F, root: &Path, uid: u32) -> Result<(Vec<Proc>, u64), RootError> {
    let entries = fs.list(root)?;
    let mut loaded = Vec::new();
    let mut skipped = 0u64;
    for entry in entries {
        if entry.symlink || !is_pid_name(&entry.name) {
            continue;
        }
        let Ok(pid) = entry.name.parse::<u32>() else {
            continue;
        };
        match load_proc(fs, &root.join(&entry.name), pid) {
            Ok(Some(proc)) => {
                if proc.uid.is_none() && (is_sandbox(&proc) || is_bwrap(&proc)) {
                    skipped += 1;
                    continue;
                }
                if proc.uid != Some(uid) {
                    continue;
                }
                if proc.state.as_deref().is_some_and(is_zombie) {
                    continue;
                }
                loaded.push(proc);
            }
            Ok(None) => {}
            Err(Unreadable) => skipped += 1,
        }
    }
    Ok((loaded, skipped))
}

fn load_proc<F: ProcFs>(fs: &mut F, dir: &Path, pid: u32) -> Result<Option<Proc>, Unreadable> {
    let stat_path = dir.join("stat");
    let Some(stat_raw) = fs.read_text(&stat_path)? else {
        return Ok(None);
    };
    let stat = parse_stat(&stat_raw);
    let (Some(ppid), Some(start_ticks)) = (stat.ppid, stat.start_ticks) else {
        return Err(Unreadable);
    };
    let comm = display_comm(fs.read_text(&dir.join("comm"))?, stat.comm.clone());
    let (argv, cmdline_known) = match fs.read_bytes(&dir.join("cmdline")) {
        Ok(Some(raw)) => (split_cmdline(&raw), true),
        Ok(None) | Err(Unreadable) => (Vec::new(), false),
    };
    let status = fs.read_text(&dir.join("status"))?.unwrap_or_default();
    let cwd = fs.read_link(&dir.join("cwd"));
    let exe = fs.read_link(&dir.join("exe"));
    // A state change during the read is normal. A new comm, parent, or start
    // means the pid was reused or reparented and must not be published.
    let Some(final_raw) = fs.read_text(&stat_path)? else {
        return Err(Unreadable);
    };
    let final_stat = parse_stat(&final_raw);
    if final_stat.comm != stat.comm
        || final_stat.ppid != Some(ppid)
        || final_stat.start_ticks != Some(start_ticks)
    {
        return Err(Unreadable);
    }
    Ok(Some(Proc {
        pid,
        ppid,
        comm,
        argv,
        cmdline_known,
        exe,
        cwd,
        uid: parse_uid(&status),
        state: stat.state,
        start_ticks,
        child_subreaper: parse_subreaper(&status),
    }))
}

fn display_comm(raw: Option<String>, stat_comm: Option<String>) -> Option<String> {
    match raw {
        Some(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                stat_comm
            } else {
                Some(trimmed.to_string())
            }
        }
        None => stat_comm,
    }
}

fn confirmed_wrapped<F: ProcFs>(
    fs: &mut F,
    root: &Path,
    loaded: &[Proc],
    children: &[usize],
    wrapper: &Proc,
    skipped: &mut u64,
) -> Vec<u32> {
    let mut confirmed = Vec::new();
    for index in children {
        let Some(child) = loaded.get(*index) else {
            continue;
        };
        if !is_sandbox(child) {
            continue;
        }
        if wrapper.start_ticks > child.start_ticks {
            continue;
        }
        let child_pid = child.pid;
        let child_comm = child.comm.clone();
        let child_ppid = child.ppid;
        let child_start = child.start_ticks;
        let current = match fs.read_text(&root.join(child_pid.to_string()).join("stat")) {
            Ok(Some(text)) => text,
            Ok(None) | Err(Unreadable) => {
                *skipped += 1;
                continue;
            }
        };
        let parsed = parse_stat(&current);
        if parsed.comm != child_comm
            || parsed.ppid != Some(child_ppid)
            || parsed.start_ticks != Some(child_start)
        {
            *skipped += 1;
            continue;
        }
        if parsed.ppid != Some(wrapper.pid) {
            *skipped += 1;
            continue;
        }
        confirmed.push(child_pid);
    }
    confirmed.sort_unstable();
    confirmed
}

fn visible_unowned<F: ProcFs>(
    fs: &mut F,
    root: &Path,
    loaded: &[Proc],
    by_pid: &HashMap<u32, usize>,
    proc: &Proc,
    uid: u32,
) -> Result<Option<bool>, Unreadable> {
    let mut seen = HashSet::from([proc.pid]);
    let mut child_ppid = proc.ppid;
    let mut child_start = proc.start_ticks;
    while child_ppid != 1 {
        let Some(parent) = proc_at(loaded, by_pid, child_ppid) else {
            return Ok(None);
        };
        if !seen.insert(parent.pid) {
            return Ok(None);
        }
        if !stable_parent(fs, root, parent, child_start)? {
            return Ok(None);
        }
        if is_codex_binary(parent) {
            return Ok(Some(false));
        }
        if is_systemd_user(parent, uid) {
            return Ok(Some(true));
        }
        if parent.exe.is_none() {
            return Ok(None);
        }
        child_ppid = parent.ppid;
        child_start = parent.start_ticks;
    }
    let Some(init) = proc_at(loaded, by_pid, 1) else {
        return Ok(Some(true));
    };
    if seen.contains(&init.pid) || !stable_parent(fs, root, init, child_start)? {
        return Ok(None);
    }
    if is_codex_binary(init) {
        return Ok(Some(false));
    }
    if init.exe.is_none() {
        return Ok(None);
    }
    Ok(Some(true))
}

fn stable_parent<F: ProcFs>(
    fs: &mut F,
    root: &Path,
    parent: &Proc,
    child_start: u64,
) -> Result<bool, Unreadable> {
    let Some(current) = fs.read_text(&root.join(parent.pid.to_string()).join("stat"))? else {
        return Ok(false);
    };
    let parsed = parse_stat(&current);
    if parsed.ppid != Some(parent.ppid) || parsed.start_ticks != Some(parent.start_ticks) {
        return Ok(false);
    }
    let Some(start) = parsed.start_ticks else {
        return Ok(false);
    };
    Ok(start <= child_start)
}

fn proc_at<'a>(loaded: &'a [Proc], by_pid: &HashMap<u32, usize>, pid: u32) -> Option<&'a Proc> {
    by_pid.get(&pid).and_then(|index| loaded.get(*index))
}

fn reaper_kind(
    ppid: u32,
    loaded: &[Proc],
    by_pid: &HashMap<u32, usize>,
    uid: u32,
) -> Option<&'static str> {
    if ppid == 1 {
        return Some("pid1");
    }
    let parent = proc_at(loaded, by_pid, ppid)?;
    if is_systemd_user(parent, uid) {
        return Some("systemd_user");
    }
    if is_unowned_subreaper(parent) {
        return Some("subreaper");
    }
    None
}

fn index_by_pid(loaded: &[Proc]) -> HashMap<u32, usize> {
    loaded
        .iter()
        .enumerate()
        .map(|(index, proc)| (proc.pid, index))
        .collect()
}

fn children_of(loaded: &[Proc]) -> HashMap<u32, Vec<usize>> {
    let mut children: HashMap<u32, Vec<usize>> = HashMap::new();
    for (index, proc) in loaded.iter().enumerate() {
        children.entry(proc.ppid).or_default().push(index);
    }
    children
}

fn read_btime<F: ProcFs>(fs: &mut F, root: &Path) -> Option<i64> {
    let text = fs.read_text(&root.join("stat")).ok()??;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("btime ") else {
            continue;
        };
        return rest.split_whitespace().next()?.parse().ok();
    }
    None
}

fn parse_stat(raw: &str) -> StatFields {
    let Some(open) = raw.find('(') else {
        return StatFields::empty();
    };
    let Some(close) = raw.rfind(')') else {
        return StatFields::empty();
    };
    if close < open {
        return StatFields::empty();
    }
    let comm_raw = &raw[open + 1..close];
    let comm = if comm_raw.is_empty() {
        None
    } else {
        Some(comm_raw.to_string())
    };
    let rest = raw.get(close + 2..).unwrap_or("");
    let mut fields = rest.split_whitespace();
    let state = fields.next().map(str::to_string);
    let ppid = fields.next().and_then(|token| token.parse().ok());
    let start_ticks = fields.nth(17).and_then(|token| token.parse().ok());
    StatFields {
        comm,
        state,
        ppid,
        start_ticks,
    }
}

impl StatFields {
    fn empty() -> Self {
        Self {
            comm: None,
            state: None,
            ppid: None,
            start_ticks: None,
        }
    }
}

fn parse_uid(status: &str) -> Option<u32> {
    for line in status.lines() {
        if !line.starts_with("Uid:") {
            continue;
        }
        return line.split_whitespace().nth(1)?.parse().ok();
    }
    None
}

fn parse_subreaper(status: &str) -> Option<bool> {
    for line in status.lines() {
        if !line.starts_with("ChildSubreaper:") {
            continue;
        }
        return match line.split_whitespace().nth(1)? {
            "1" => Some(true),
            "0" => Some(false),
            _ => None,
        };
    }
    None
}

fn split_cmdline(raw: &[u8]) -> Vec<String> {
    raw.split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| String::from_utf8_lossy(part).into_owned())
        .collect()
}

fn cmdline_text(proc: &Proc) -> Option<String> {
    if !proc.cmdline_known {
        return None;
    }
    let text = proc.argv.join(" ");
    if text.chars().count() <= CMDLINE_LIMIT {
        return Some(text);
    }
    let mut truncated: String = text.chars().take(CMDLINE_LIMIT - 1).collect();
    truncated.push('…');
    Some(truncated)
}

fn file_name(path: &str) -> Option<&str> {
    let name = path.rsplit('/').next().unwrap_or("");
    if name.is_empty() { None } else { Some(name) }
}

fn basenames(proc: &Proc) -> Vec<&str> {
    let mut names = Vec::new();
    if let Some(comm) = proc.comm.as_deref() {
        names.push(comm);
    }
    if let Some(argv0) = proc.argv.first().and_then(|path| file_name(path)) {
        names.push(argv0);
    }
    if let Some(exe) = proc.exe.as_deref().and_then(file_name) {
        names.push(exe);
    }
    names
}

fn is_sandbox(proc: &Proc) -> bool {
    basenames(proc).contains(&SANDBOX_NAME) || proc.comm.as_deref() == Some(SANDBOX_COMM)
}

fn is_bwrap(proc: &Proc) -> bool {
    basenames(proc)
        .iter()
        .any(|name| BWRAP_NAMES.contains(name))
}

fn is_codex_binary(proc: &Proc) -> bool {
    basenames(proc).contains(&"codex")
}

fn is_systemd_user(proc: &Proc, uid: u32) -> bool {
    basenames(proc).contains(&"systemd")
        && proc.argv.iter().any(|arg| arg == "--user")
        && proc.uid == Some(uid)
}

fn is_unowned_subreaper(proc: &Proc) -> bool {
    proc.child_subreaper == Some(true)
        && !is_codex_binary(proc)
        && !is_bwrap(proc)
        && !is_sandbox(proc)
}

fn is_zombie(state: &str) -> bool {
    matches!(state, "Z" | "X" | "x")
}

fn is_pid_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|byte| byte.is_ascii_digit())
}

fn started_age(
    start_ticks: u64,
    btime: Option<i64>,
    clk: Option<u64>,
    now: f64,
) -> (Option<String>, Option<i64>) {
    let (Some(btime), Some(clk)) = (btime, clk) else {
        return (None, None);
    };
    if clk == 0 {
        return (None, None);
    }
    let started = btime as f64 + (start_ticks as f64 / clk as f64);
    let age = now - started;
    let stamp = format_unix(started);
    if age < 0.0 {
        return (stamp, None);
    }
    (stamp, whole_seconds(age))
}

fn whole_seconds(age: f64) -> Option<i64> {
    if !age.is_finite() || age < 0.0 || age > i64::MAX as f64 {
        return None;
    }
    Some(age as i64)
}

fn age_exceeds(age: i64, warn_after: u64) -> bool {
    age > 0
        && u64::try_from(age)
            .ok()
            .is_some_and(|seconds| seconds > warn_after)
}

fn sort_orphans(orphans: &mut [CodexSandboxOrphan]) {
    orphans.sort_by(|left, right| {
        left.age_seconds
            .is_none()
            .cmp(&right.age_seconds.is_none())
            .then_with(|| {
                right
                    .age_seconds
                    .unwrap_or(0)
                    .cmp(&left.age_seconds.unwrap_or(0))
            })
            .then_with(|| left.pid.cmp(&right.pid))
    });
}

fn format_age(seconds: Option<i64>) -> String {
    let Some(seconds) = seconds else {
        return "unknown".to_string();
    };
    let remaining = u64::try_from(seconds).unwrap_or(0);
    let hours = remaining / 3600;
    let rest = remaining % 3600;
    let minutes = rest / 60;
    let secs = rest % 60;
    if hours > 0 {
        format!("{hours}h{minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m{secs}s")
    } else {
        format!("{secs}s")
    }
}

fn format_unix(seconds: f64) -> Option<String> {
    if !seconds.is_finite() {
        return None;
    }
    let whole = seconds.trunc();
    if whole < i64::MIN as f64 || whole > i64::MAX as f64 {
        return None;
    }
    #[allow(clippy::cast_possible_truncation)]
    let secs = whole as i64;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|stamp| stamp.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn blank(
    available: bool,
    status: &'static str,
    reason: Option<&'static str>,
    warn_after: u64,
    uid: Option<u32>,
    now: f64,
    skipped: u64,
) -> CodexSandboxOrphanReport {
    CodexSandboxOrphanReport {
        schema: SCHEMA,
        available,
        readonly: true,
        cleanup: false,
        status,
        reason,
        subject_uid: uid,
        observed_at: format_unix(now).unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string()),
        orphan_count: 0,
        oldest_age_seconds: None,
        warn_after_seconds: warn_after,
        skipped,
        orphans: Vec::new(),
    }
}

fn resolve_uid(uid: SubjectUid) -> Option<u32> {
    match uid {
        SubjectUid::Explicit(uid) => Some(uid),
        SubjectUid::Unavailable => None,
        SubjectUid::Current => lookup_uid(),
    }
}

fn lookup_uid() -> Option<u32> {
    #[cfg(unix)]
    {
        Some(rustix::process::getuid().as_raw())
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn effective_clk(explicit: Option<u64>) -> Option<u64> {
    match explicit {
        Some(ticks) if ticks > 0 => Some(ticks),
        Some(_) => None,
        None => lookup_clk(),
    }
}

fn lookup_clk() -> Option<u64> {
    // `rustix::param` is not compiled for Windows or ESP-IDF. Horizon, Vita,
    // and WASI build the module without `clock_ticks_per_second`.
    #[cfg(not(any(
        windows,
        target_os = "espidf",
        target_os = "horizon",
        target_os = "vita",
        target_os = "wasi"
    )))]
    {
        let ticks = rustix::param::clock_ticks_per_second();
        if (1..=1_000_000).contains(&ticks) {
            Some(ticks)
        } else {
            None
        }
    }
    #[cfg(any(
        windows,
        target_os = "espidf",
        target_os = "horizon",
        target_os = "vita",
        target_os = "wasi"
    ))]
    {
        None
    }
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;

    use super::{
        CMDLINE_LIMIT, DEFAULT_WARN_AFTER_SECONDS, ProcEntry, ProcFs, RootError, RootKind,
        SANDBOX_COMM, SANDBOX_NAME, SCHEMA, SubjectUid, Unreadable, scan_codex_sandbox_orphans,
        scan_with,
    };
    use crate::codex_sandbox_orphans::{CodexSandboxOrphanQuery, render_sandbox_orphan_report};

    const UID: u32 = 1000;
    const BTIME: i64 = 1_700_000_000;
    const CLK: u64 = 100;
    const SANDBOX: &str = "/usr/lib/codex/codex-linux-sandbox";

    #[derive(Clone)]
    enum Flat {
        Dir,
        File(Vec<u8>),
        Link(String),
        Unreadable,
    }

    #[derive(Clone, Copy)]
    enum Phase {
        Before,
        After,
    }

    struct HookEvent {
        key: String,
        phase: Phase,
        index: u32,
    }

    type MemHook = Box<dyn FnMut(HookEvent, &mut BTreeMap<String, Flat>)>;

    struct MemFs {
        files: BTreeMap<String, Flat>,
        text_reads: BTreeMap<String, u32>,
        fail_list: bool,
        hook: Option<MemHook>,
    }

    impl MemFs {
        fn new() -> Self {
            let mut files = BTreeMap::new();
            files.insert("fixture".to_string(), Flat::Dir);
            files.insert(
                "fixture/stat".to_string(),
                Flat::File(format!("cpu 0 0 0\nbtime {BTIME}\n").into_bytes()),
            );
            Self {
                files,
                text_reads: BTreeMap::new(),
                fail_list: false,
                hook: None,
            }
        }

        fn fire(&mut self, event: HookEvent) {
            let Some(mut hook) = self.hook.take() else {
                return;
            };
            hook(event, &mut self.files);
            self.hook = Some(hook);
        }
    }

    impl Clone for MemFs {
        fn clone(&self) -> Self {
            Self {
                files: self.files.clone(),
                text_reads: self.text_reads.clone(),
                fail_list: self.fail_list,
                hook: None,
            }
        }
    }

    fn norm(path: &Path) -> String {
        let mut parts = Vec::new();
        for component in path.components() {
            match component {
                std::path::Component::Normal(part) => {
                    parts.push(part.to_string_lossy().into_owned());
                }
                std::path::Component::RootDir => parts.push(String::new()),
                std::path::Component::Prefix(prefix) => {
                    parts.push(prefix.as_os_str().to_string_lossy().into_owned());
                }
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => parts.push("..".to_string()),
            }
        }
        parts.join("/")
    }

    impl ProcFs for MemFs {
        fn root_kind(&mut self, path: &Path) -> Result<RootKind, RootError> {
            match self.files.get(&norm(path)) {
                Some(Flat::Dir) => Ok(RootKind::Directory),
                Some(Flat::Unreadable) => Err(RootError::Unreadable),
                Some(_) => Err(RootError::NotDirectory),
                None => Err(RootError::Missing),
            }
        }

        fn list(&mut self, path: &Path) -> Result<Vec<ProcEntry>, RootError> {
            if self.fail_list {
                return Err(RootError::Unreadable);
            }
            let key = norm(path);
            match self.files.get(&key) {
                Some(Flat::Dir) => {}
                Some(Flat::Unreadable) => return Err(RootError::Unreadable),
                Some(_) => return Err(RootError::NotDirectory),
                None => return Err(RootError::Missing),
            }
            let prefix = format!("{key}/");
            let mut entries = Vec::new();
            for (child, node) in &self.files {
                let Some(rest) = child.strip_prefix(&prefix) else {
                    continue;
                };
                if rest.is_empty() || rest.contains('/') {
                    continue;
                }
                entries.push(ProcEntry {
                    name: rest.to_string(),
                    symlink: matches!(node, Flat::Link(_)),
                });
            }
            Ok(entries)
        }

        fn read_text(&mut self, path: &Path) -> Result<Option<String>, Unreadable> {
            let key = norm(path);
            let index = {
                let slot = self.text_reads.entry(key.clone()).or_insert(0);
                *slot += 1;
                *slot
            };
            self.fire(HookEvent {
                key: key.clone(),
                phase: Phase::Before,
                index,
            });
            let value = match self.files.get(&key) {
                Some(Flat::File(bytes)) => Ok(Some(String::from_utf8_lossy(bytes).into_owned())),
                Some(Flat::Unreadable) => Err(Unreadable),
                _ => Ok(None),
            };
            self.fire(HookEvent {
                key,
                phase: Phase::After,
                index,
            });
            value
        }

        fn read_bytes(&mut self, path: &Path) -> Result<Option<Vec<u8>>, Unreadable> {
            match self.files.get(&norm(path)) {
                Some(Flat::File(bytes)) => Ok(Some(bytes.clone())),
                Some(Flat::Unreadable) => Err(Unreadable),
                _ => Ok(None),
            }
        }

        fn read_link(&mut self, path: &Path) -> Option<String> {
            match self.files.get(&norm(path)) {
                Some(Flat::Link(target)) => Some(target.clone()),
                _ => None,
            }
        }
    }

    struct Spec {
        comm: String,
        argv: Vec<String>,
        ppid: u32,
        uid: u32,
        ticks: u64,
        state: String,
        cwd: Option<String>,
        subreaper: Option<bool>,
        exe: Option<String>,
        hide_stat: bool,
        hide_status: bool,
        omit_stat: bool,
        omit_cmdline: bool,
        stat_override: Option<String>,
    }

    fn spec(comm: &str, argv: &[&str], ppid: u32) -> Spec {
        let argv: Vec<String> = argv.iter().map(|arg| (*arg).to_string()).collect();
        let exe = argv.first().cloned();
        Spec {
            comm: comm.to_string(),
            argv,
            ppid,
            uid: UID,
            ticks: 0,
            state: "S".to_string(),
            cwd: None,
            subreaper: None,
            exe,
            hide_stat: false,
            hide_status: false,
            omit_stat: false,
            omit_cmdline: false,
            stat_override: None,
        }
    }

    fn stat_text(pid: u32, comm: &str, ppid: u32, state: &str, ticks: u64) -> String {
        let mut fields = vec![state.to_string(), ppid.to_string()];
        fields.extend(std::iter::repeat_n("0".to_string(), 18));
        fields[19] = ticks.to_string();
        format!("{pid} ({comm}) {}\n", fields.join(" "))
    }

    fn cmdline_bytes(argv: &[String]) -> Vec<u8> {
        let mut raw = Vec::new();
        for (index, arg) in argv.iter().enumerate() {
            if index > 0 {
                raw.push(0);
            }
            raw.extend(arg.as_bytes());
        }
        raw.push(0);
        raw
    }

    fn write_proc(fs: &mut MemFs, pid: u32, spec: &Spec) {
        let base = format!("fixture/{pid}");
        fs.files.insert(base.clone(), Flat::Dir);
        fs.files.insert(
            format!("{base}/comm"),
            Flat::File(format!("{}\n", spec.comm).into_bytes()),
        );
        if !spec.omit_cmdline {
            fs.files.insert(
                format!("{base}/cmdline"),
                Flat::File(cmdline_bytes(&spec.argv)),
            );
        }
        if spec.omit_stat {
            fs.files.remove(&format!("{base}/stat"));
        } else if spec.hide_stat {
            fs.files.insert(format!("{base}/stat"), Flat::Unreadable);
        } else {
            let text = spec
                .stat_override
                .clone()
                .unwrap_or_else(|| stat_text(pid, &spec.comm, spec.ppid, &spec.state, spec.ticks));
            fs.files
                .insert(format!("{base}/stat"), Flat::File(text.into_bytes()));
        }
        if spec.hide_status {
            fs.files.insert(format!("{base}/status"), Flat::Unreadable);
        } else {
            let mut status = format!(
                "Name:\t{comm}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n",
                comm = spec.comm,
                uid = spec.uid
            );
            if let Some(flag) = spec.subreaper {
                let bit = if flag { "1" } else { "0" };
                status.push_str(&format!("ChildSubreaper:\t{bit}\n"));
            }
            fs.files
                .insert(format!("{base}/status"), Flat::File(status.into_bytes()));
        }
        if let Some(cwd) = &spec.cwd {
            fs.files
                .insert(format!("{base}/cwd"), Flat::Link(cwd.clone()));
        }
        if let Some(exe) = &spec.exe {
            fs.files
                .insert(format!("{base}/exe"), Flat::Link(exe.clone()));
        }
    }

    fn systemd(fs: &mut MemFs, pid: u32, uid: u32) {
        let mut spec = spec("systemd", &["/usr/lib/systemd/systemd", "--user"], 1);
        spec.uid = uid;
        spec.subreaper = Some(true);
        write_proc(fs, pid, &spec);
    }

    fn sandbox(fs: &mut MemFs, pid: u32, ppid: u32, ticks: u64) -> Spec {
        let mut spec = spec(SANDBOX_COMM, &[SANDBOX, "--", "sleep", "30"], ppid);
        spec.ticks = ticks;
        write_proc(fs, pid, &spec);
        spec
    }

    fn scan_mem(fs: &mut MemFs, now: f64, warn: u64) -> super::CodexSandboxOrphanReport {
        scan_with(fs, Path::new("fixture"), UID, now, Some(CLK), warn)
    }

    fn query_at(proc_root: Option<PathBuf>, uid: SubjectUid) -> CodexSandboxOrphanQuery {
        CodexSandboxOrphanQuery {
            proc_root,
            now_unix_seconds: Some(BTIME as f64),
            clk_tck: Some(CLK),
            uid,
            warn_after_seconds: DEFAULT_WARN_AFTER_SECONDS,
        }
    }

    #[test]
    fn truncated_comm_matches_the_linux_task_name() {
        assert_eq!(&SANDBOX_NAME[..15], SANDBOX_COMM);
        assert_eq!(SCHEMA, "codex-sandbox-orphans.v1");
        assert_eq!(DEFAULT_WARN_AFTER_SECONDS, 21_600);
    }

    #[test]
    fn orphans_under_systemd_include_cwd_and_truncated_cmd() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut first = spec(SANDBOX_COMM, &[SANDBOX, "--", "sleep", "30"], 10);
        first.cwd = Some("/work".to_string());
        write_proc(&mut fs, 30, &first);
        let long_arg = "x".repeat(400);
        let mut second = spec(SANDBOX_COMM, &[SANDBOX, long_arg.as_str()], 10);
        second.cwd = None;
        second.exe = Some(SANDBOX.to_string());
        write_proc(&mut fs, 31, &second);
        write_proc(
            &mut fs,
            32,
            &spec("zsh", &["/usr/bin/zsh", "-c", "sleep 30"], 30),
        );
        let report = scan_mem(&mut fs, BTIME as f64 + 60.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.available);
        assert!(report.readonly);
        assert!(!report.cleanup);
        assert_eq!(report.status, "ok");
        assert_eq!(report.orphan_count, 2);
        assert_eq!(report.subject_uid, Some(UID));
        assert_eq!(report.observed_at, "2023-11-14T22:14:20Z");
        let by_pid: BTreeMap<_, _> = report.orphans.iter().map(|row| (row.pid, row)).collect();
        assert_eq!(by_pid.len(), 2);
        let row = by_pid[&30];
        assert_eq!(row.ppid, 10);
        assert_eq!(row.parent_kind, "systemd_user");
        assert_eq!(row.kind, SANDBOX_NAME);
        assert_eq!(row.cwd.as_deref(), Some("/work"));
        assert_eq!(row.age_seconds, Some(60));
        assert_eq!(row.started_at.as_deref(), Some("2023-11-14T22:13:20Z"));
        assert!(by_pid[&31].cwd.is_none());
        let cmdline = by_pid[&31].cmdline.as_deref().unwrap();
        assert_eq!(cmdline.chars().count(), CMDLINE_LIMIT);
        assert!(cmdline.ends_with('…'));
        let text = serde_json::to_string(&report.orphans).unwrap();
        assert!(!text.contains("zsh"));
        assert!(text.starts_with("[{\"pid\":30,"));
    }

    #[test]
    fn age_truncates_toward_zero_and_equal_ages_sort_by_pid() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        sandbox(&mut fs, 31, 10, 0);
        sandbox(&mut fs, 30, 10, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 60.5, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![30, 31]
        );
        assert_eq!(report.orphans[0].age_seconds, Some(60));
        assert_eq!(report.oldest_age_seconds, Some(60));
    }

    #[test]
    fn no_orphans_when_the_parent_is_an_ordinary_process() {
        let mut fs = MemFs::new();
        write_proc(
            &mut fs,
            7,
            &spec("python", &["/usr/bin/python", "tool.py"], 1),
        );
        sandbox(&mut fs, 8, 7, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 10.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 0);
        assert!(report.orphans.is_empty());
        assert_eq!(report.status, "ok");
        assert!(report.oldest_age_seconds.is_none());
        assert_eq!(report.skipped, 0);
    }

    #[test]
    fn codex_parent_is_not_an_orphan_even_when_it_is_a_subreaper() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut codex = spec(
            "codex",
            &["codex", "app-server", "--listen", "stdio://"],
            10,
        );
        codex.subreaper = Some(true);
        write_proc(&mut fs, 50, &codex);
        sandbox(&mut fs, 51, 50, 0);
        write_proc(
            &mut fs,
            52,
            &spec("bwrap", &["/usr/bin/bwrap", "--", "sleep"], 50),
        );
        sandbox(&mut fs, 53, 52, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 10.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.skipped, 0);
    }

    #[test]
    fn unreadable_stat_or_status_is_skipped() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        sandbox(&mut fs, 30, 10, 0);
        let mut hidden = spec(SANDBOX_COMM, &[SANDBOX, "--", "sleep", "30"], 10);
        hidden.hide_stat = true;
        write_proc(&mut fs, 31, &hidden);
        let report = scan_mem(&mut fs, BTIME as f64 + 10.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.available);
        assert_eq!(report.skipped, 1);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![30]
        );

        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut hidden = spec(SANDBOX_COMM, &[SANDBOX, "--", "sleep", "30"], 10);
        hidden.hide_status = true;
        write_proc(&mut fs, 30, &hidden);
        let report = scan_mem(&mut fs, BTIME as f64 + 10.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.skipped, 1);
        assert!(report.available);
    }

    #[test]
    fn warns_only_when_age_exceeds_the_threshold() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        sandbox(&mut fs, 30, 10, 0);
        let under = scan_mem(
            &mut fs,
            BTIME as f64 + (DEFAULT_WARN_AFTER_SECONDS - 1) as f64,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        let exact = scan_mem(
            &mut fs,
            BTIME as f64 + DEFAULT_WARN_AFTER_SECONDS as f64,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        let over = scan_mem(
            &mut fs,
            BTIME as f64 + (DEFAULT_WARN_AFTER_SECONDS + 1) as f64,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        assert_eq!(under.status, "ok");
        assert_eq!(
            under.oldest_age_seconds,
            Some(i64::try_from(DEFAULT_WARN_AFTER_SECONDS - 1).unwrap())
        );
        assert_eq!(exact.status, "ok");
        assert_eq!(
            exact.oldest_age_seconds,
            Some(i64::try_from(DEFAULT_WARN_AFTER_SECONDS).unwrap())
        );
        assert_eq!(over.status, "warn");
        assert_eq!(
            over.oldest_age_seconds,
            Some(i64::try_from(DEFAULT_WARN_AFTER_SECONDS + 1).unwrap())
        );
        assert_eq!(over.warn_after_seconds, DEFAULT_WARN_AFTER_SECONDS);
        let custom = scan_mem(&mut fs, BTIME as f64 + 60.0, 30);
        assert_eq!(custom.status, "warn");
        assert_eq!(custom.warn_after_seconds, 30);
    }

    #[test]
    fn direct_bwrap_and_bubblewrap_are_listed_when_the_parent_is_a_reaper() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        write_proc(
            &mut fs,
            40,
            &spec(
                "bwrap",
                &["/usr/bin/bwrap", "--ro-bind", "/", "/", "--", SANDBOX],
                10,
            ),
        );
        sandbox(&mut fs, 41, 40, 0);
        write_proc(
            &mut fs,
            42,
            &spec("bwrap", &["/usr/bin/bwrap", "--", "true"], 10),
        );
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![40]
        );
        assert_eq!(report.orphans[0].kind, "bwrap");
        assert_eq!(report.orphans[0].parent_kind, "systemd_user");
        assert_eq!(report.orphans[0].wrapped_sandbox_pids, vec![41]);

        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        write_proc(
            &mut fs,
            40,
            &spec("bubblewrap", &["/usr/bin/bubblewrap", "--", SANDBOX], 10),
        );
        sandbox(&mut fs, 43, 40, 0);
        sandbox(&mut fs, 41, 40, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphans[0].kind, "bwrap");
        assert_eq!(report.orphans[0].wrapped_sandbox_pids, vec![41, 43]);
    }

    #[test]
    fn pid1_and_non_codex_subreaper_are_orphans() {
        let mut fs = MemFs::new();
        let mut init = spec("systemd", &["/sbin/init"], 0);
        init.uid = 0;
        write_proc(&mut fs, 1, &init);
        sandbox(&mut fs, 70, 1, 0);
        let mut session = spec("python", &["/usr/bin/python", "session"], 1);
        session.subreaper = Some(true);
        write_proc(&mut fs, 80, &session);
        sandbox(&mut fs, 81, 80, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        let by_pid: BTreeMap<_, _> = report.orphans.iter().map(|row| (row.pid, row)).collect();
        assert_eq!(by_pid.keys().copied().collect::<Vec<_>>(), vec![70, 81]);
        assert_eq!(by_pid[&70].parent_kind, "pid1");
        assert_eq!(by_pid[&81].parent_kind, "subreaper");
    }

    #[test]
    fn other_users_systemd_child_subreaper_zero_and_zombies_are_not_orphans() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID + 1);
        sandbox(&mut fs, 30, 10, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 0);

        let mut fs = MemFs::new();
        let mut user = spec("systemd", &["/usr/lib/systemd/systemd"], 1);
        user.subreaper = Some(false);
        write_proc(&mut fs, 10, &user);
        sandbox(&mut fs, 30, 10, 0);
        assert!(
            scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS)
                .orphans
                .is_empty()
        );

        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut dead = spec(SANDBOX_COMM, &[SANDBOX], 10);
        dead.state = "Z".to_string();
        write_proc(&mut fs, 30, &dead);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 0);
    }

    #[test]
    fn missing_uid_status_and_cmdline_are_reported_without_guessing() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut spec = spec(SANDBOX_COMM, &[SANDBOX], 10);
        spec.omit_cmdline = true;
        write_proc(&mut fs, 30, &spec);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans[0].cmdline.is_none());

        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let base = "fixture/30".to_string();
        sandbox(&mut fs, 30, 10, 0);
        fs.files.remove(&format!("{base}/status"));
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn unavailable_reasons_do_not_invent_orphans() {
        let mut fs = MemFs::new();
        fs.files
            .insert("fixture".to_string(), Flat::File(Vec::new()));
        let report = scan_mem(&mut fs, BTIME as f64, DEFAULT_WARN_AFTER_SECONDS);
        assert!(!report.available);
        assert_eq!(report.reason, Some("proc_unavailable"));
        assert!(!report.cleanup);
        assert_eq!(report.orphan_count, 0);

        let mut fs = MemFs::new();
        fs.fail_list = true;
        let report = scan_mem(&mut fs, BTIME as f64, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.reason, Some("proc_unreadable"));
        assert_eq!(report.status, "unavailable");
    }

    #[test]
    fn missing_uid_is_reported_before_procfs_and_does_not_require_a_readable_tree() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing-proc-root");
        let report =
            scan_codex_sandbox_orphans(&query_at(Some(missing.clone()), SubjectUid::Unavailable));
        assert!(!report.available);
        assert_eq!(report.reason, Some("uid_unavailable"));
        assert!(report.subject_uid.is_none());
        assert!(report.orphans.is_empty());
        assert!(!report.cleanup);
        assert!(!missing.exists());
    }

    #[test]
    fn human_text_uses_the_english_catalog() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut spec = spec(SANDBOX_COMM, &[SANDBOX], 10);
        spec.cwd = Some("/work".to_string());
        write_proc(&mut fs, 30, &spec);
        let report = scan_mem(
            &mut fs,
            BTIME as f64 + (DEFAULT_WARN_AFTER_SECONDS + 5) as f64,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        let text = render_sandbox_orphan_report(&report);
        assert!(text.starts_with("Codex sandbox orphans (read-only, no cleanup)\n"));
        assert!(text.contains("status warn"));
        assert!(text.contains("pid=30"));
        assert!(text.contains("cwd=/work"));
        assert!(text.contains("6h0m"));
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing-proc-root");
        let unavailable = render_sandbox_orphan_report(&scan_codex_sandbox_orphans(&query_at(
            Some(missing),
            SubjectUid::Explicit(UID),
        )));
        assert_eq!(
            unavailable,
            "Codex sandbox orphan check unavailable: proc_unavailable\n"
        );
    }

    #[test]
    fn live_codex_ancestor_behind_a_subreaper_is_not_an_orphan() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut codex = spec("codex", &["codex", "app-server"], 10);
        codex.exe = Some("/usr/bin/codex".to_string());
        write_proc(&mut fs, 50, &codex);
        let mut parent = spec("python", &["python", "session.py"], 50);
        parent.subreaper = Some(true);
        parent.exe = Some("/usr/bin/python".to_string());
        write_proc(&mut fs, 60, &parent);
        sandbox(&mut fs, 70, 60, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.status, "ok");
        assert_eq!(report.skipped, 0);
    }

    #[test]
    fn pid_reuse_between_stat_and_comm_is_not_published() {
        let mut fs = MemFs::new();
        write_proc(&mut fs, 50, &spec("codex", &["codex", "app-server"], 1));
        let mut old = spec("python", &["python", "old.py"], 1);
        old.ticks = 100;
        write_proc(&mut fs, 70, &old);
        let changed = Rc::new(Cell::new(false));
        let flag = Rc::clone(&changed);
        fs.hook = Some(Box::new(move |event, files| {
            if event.key == "fixture/70/stat"
                && event.index == 1
                && matches!(event.phase, Phase::After)
            {
                flag.set(true);
                files.insert(
                    "fixture/70/stat".to_string(),
                    Flat::File(stat_text(70, SANDBOX_COMM, 50, "S", 2_199_900).into_bytes()),
                );
                files.insert(
                    "fixture/70/comm".to_string(),
                    Flat::File(format!("{SANDBOX_COMM}\n").into_bytes()),
                );
                files.insert(
                    "fixture/70/cmdline".to_string(),
                    Flat::File(cmdline_bytes(&[SANDBOX.to_string()])),
                );
            }
        }));
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(changed.get());
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.status, "ok");
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn incomplete_ancestor_chains_are_not_negative_evidence() {
        let mut base = MemFs::new();
        write_proc(
            &mut base,
            50,
            &spec("python", &["python", "ancestor.py"], 1),
        );
        let mut parent = spec("python", &["python", "session.py"], 50);
        parent.subreaper = Some(true);
        write_proc(&mut base, 60, &parent);
        sandbox(&mut base, 70, 60, 0);

        let mut missing_parent = base.clone();
        missing_parent.files.remove("fixture/50/stat");
        let report = scan_mem(
            &mut missing_parent,
            BTIME as f64 + 22_000.0,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        assert_eq!(report.orphan_count, 0);
        assert!(report.skipped >= 1);

        let mut missing_exe = base.clone();
        missing_exe.files.remove("fixture/50/exe");
        let report = scan_mem(
            &mut missing_exe,
            BTIME as f64 + 22_000.0,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        assert_eq!(report.orphan_count, 0);
        assert!(report.skipped >= 1);

        let mut denied = base;
        denied
            .files
            .insert("fixture/50/status".to_string(), Flat::Unreadable);
        let report = scan_mem(
            &mut denied,
            BTIME as f64 + 22_000.0,
            DEFAULT_WARN_AFTER_SECONDS,
        );
        assert_eq!(report.orphan_count, 0);
        assert!(report.skipped >= 1);
    }

    #[test]
    fn reparent_during_load_is_skipped_but_a_state_change_is_allowed() {
        let mut fs = MemFs::new();
        sandbox(&mut fs, 70, 1, 0);
        let mode = Rc::new(Cell::new("reparent"));
        let mode_hook = Rc::clone(&mode);
        let changed = Rc::new(Cell::new(false));
        let changed_hook = Rc::clone(&changed);
        fs.hook = Some(Box::new(move |event, files| {
            if event.key == "fixture/70/stat"
                && event.index == 1
                && matches!(event.phase, Phase::After)
                && !changed_hook.get()
            {
                changed_hook.set(true);
                let ppid = if mode_hook.get() == "reparent" { 50 } else { 1 };
                files.insert(
                    "fixture/70/stat".to_string(),
                    Flat::File(stat_text(70, SANDBOX_COMM, ppid, "R", 0).into_bytes()),
                );
            }
        }));
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.skipped, 1);

        fs.text_reads.clear();
        fs.files.insert(
            "fixture/70/stat".to_string(),
            Flat::File(stat_text(70, SANDBOX_COMM, 1, "S", 0).into_bytes()),
        );
        mode.set("state");
        changed.set(false);
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 1);
        assert_eq!(report.status, "warn");
        assert_eq!(report.orphans[0].parent_kind, "pid1");
    }

    #[test]
    fn parent_identity_change_after_load_is_skipped() {
        let mut fs = MemFs::new();
        let mut parent = spec("python", &["python", "session.py"], 1);
        parent.subreaper = Some(true);
        write_proc(&mut fs, 60, &parent);
        sandbox(&mut fs, 70, 60, 100);
        let reads = Rc::new(Cell::new(0u32));
        let reads_hook = Rc::clone(&reads);
        fs.hook = Some(Box::new(move |event, files| {
            if event.key == "fixture/60/stat" && matches!(event.phase, Phase::Before) {
                reads_hook.set(event.index);
                if event.index == 3 {
                    files.insert(
                        "fixture/60/stat".to_string(),
                        Flat::File(stat_text(60, "python", 1, "S", 500).into_bytes()),
                    );
                }
            }
        }));
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(reads.get(), 3);
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn visible_pid1_is_checked_and_an_unseen_pid1_stays_a_reaper() {
        let mut fs = MemFs::new();
        let mut init = spec("codex", &["codex", "app-server"], 0);
        init.exe = Some("/usr/bin/codex".to_string());
        write_proc(&mut fs, 1, &init);
        sandbox(&mut fs, 70, 1, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 0);

        let mut fs = MemFs::new();
        write_proc(&mut fs, 1, &spec("systemd", &["/sbin/init"], 0));
        sandbox(&mut fs, 70, 1, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![70]
        );
        assert_eq!(report.orphans[0].parent_kind, "pid1");

        let mut fs = MemFs::new();
        sandbox(&mut fs, 70, 1, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphans[0].parent_kind, "pid1");
        assert_eq!(report.skipped, 0);

        let mut fs = MemFs::new();
        let mut init = spec("systemd", &["/sbin/init"], 0);
        init.exe = None;
        write_proc(&mut fs, 1, &init);
        sandbox(&mut fs, 70, 1, 0);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn bwrap_does_not_adopt_an_older_sandbox_and_keeps_one_that_started_with_it() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut wrapper = spec(
            "bwrap",
            &["/usr/bin/bwrap", "--ro-bind", "/", "/", "--", SANDBOX],
            10,
        );
        wrapper.ticks = 200;
        write_proc(&mut fs, 40, &wrapper);
        sandbox(&mut fs, 41, 40, 100);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 0);

        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut wrapper = spec(
            "bwrap",
            &["/usr/bin/bwrap", "--ro-bind", "/", "/", "--", SANDBOX],
            10,
        );
        wrapper.ticks = 100;
        write_proc(&mut fs, 40, &wrapper);
        sandbox(&mut fs, 41, 40, 100);
        sandbox(&mut fs, 42, 40, 200);
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![40]
        );
        assert_eq!(report.orphans[0].wrapped_sandbox_pids, vec![41, 42]);
    }

    #[test]
    fn wrapped_sandbox_identity_change_drops_the_link() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        let mut wrapper = spec("bwrap", &["/usr/bin/bwrap", "--", SANDBOX], 10);
        wrapper.ticks = 100;
        write_proc(&mut fs, 40, &wrapper);
        sandbox(&mut fs, 41, 40, 200);
        let reads = Rc::new(Cell::new(0u32));
        let reads_hook = Rc::clone(&reads);
        fs.hook = Some(Box::new(move |event, files| {
            if event.key == "fixture/41/stat" && matches!(event.phase, Phase::Before) {
                reads_hook.set(event.index);
                if event.index == 3 {
                    files.insert(
                        "fixture/41/stat".to_string(),
                        Flat::File(stat_text(41, "python", 10, "S", 200).into_bytes()),
                    );
                }
            }
        }));
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(reads.get(), 3);
        assert!(report.orphans.is_empty());
        assert!(report.skipped >= 1);
    }

    #[test]
    fn missing_start_ticks_is_an_unknown_identity() {
        let mut fs = MemFs::new();
        let mut spec = spec(SANDBOX_COMM, &[SANDBOX], 1);
        spec.stat_override = Some("70 (codex-linux-san) S 1\n".to_string());
        write_proc(&mut fs, 70, &spec);
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(report.orphan_count, 0);
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn a_parent_that_started_after_its_child_is_not_proof() {
        let mut fs = MemFs::new();
        let mut parent = spec("python", &["python", "session.py"], 1);
        parent.subreaper = Some(true);
        parent.ticks = 500;
        write_proc(&mut fs, 60, &parent);
        sandbox(&mut fs, 70, 60, 100);
        let report = scan_mem(&mut fs, BTIME as f64 + 22_000.0, DEFAULT_WARN_AFTER_SECONDS);
        assert!(report.orphans.is_empty());
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn negative_age_is_unknown_and_sorts_after_known_ages() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        sandbox(&mut fs, 30, 10, 0);
        sandbox(&mut fs, 40, 10, 1_000_000);
        let report = scan_mem(&mut fs, BTIME as f64 + 10.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![30, 40]
        );
        assert_eq!(report.orphans[0].age_seconds, Some(10));
        assert!(report.orphans[1].age_seconds.is_none());
        assert!(report.orphans[1].started_at.is_some());
        assert_eq!(report.oldest_age_seconds, Some(10));
        assert_eq!(report.status, "ok");
    }

    #[test]
    fn symlink_pid_entries_are_not_scanned() {
        let mut fs = MemFs::new();
        systemd(&mut fs, 10, UID);
        sandbox(&mut fs, 30, 10, 0);
        fs.files.insert(
            "fixture/99".to_string(),
            Flat::Link("fixture/30".to_string()),
        );
        let report = scan_mem(&mut fs, BTIME as f64 + 5.0, DEFAULT_WARN_AFTER_SECONDS);
        assert_eq!(
            report.orphans.iter().map(|row| row.pid).collect::<Vec<_>>(),
            vec![30]
        );
    }

    #[test]
    fn a_real_proc_tree_matches_the_memory_fixture() {
        let root = tempfile::tempdir().unwrap();
        let proc = root.path().join("proc");
        write_real(
            &proc,
            10,
            "systemd",
            &["/usr/lib/systemd/systemd", "--user"],
            1,
            true,
            None,
        );
        let work = root.path().join("work");
        std::fs::create_dir(&work).unwrap();
        write_real(
            &proc,
            30,
            SANDBOX_COMM,
            &[SANDBOX, "--", "sleep", "30"],
            10,
            false,
            Some(&work),
        );
        let before = std::fs::read(proc.join("30").join("stat")).unwrap();
        let report = scan_codex_sandbox_orphans(&CodexSandboxOrphanQuery {
            proc_root: Some(proc.clone()),
            now_unix_seconds: Some(BTIME as f64 + 60.0),
            clk_tck: Some(CLK),
            uid: SubjectUid::Explicit(UID),
            warn_after_seconds: DEFAULT_WARN_AFTER_SECONDS,
        });
        assert_eq!(report.orphan_count, 1);
        assert_eq!(report.orphans[0].pid, 30);
        assert_eq!(report.orphans[0].parent_kind, "systemd_user");
        assert_eq!(report.orphans[0].age_seconds, Some(60));
        #[cfg(unix)]
        assert_eq!(
            report.orphans[0].cwd.as_deref(),
            Some(work.to_str().unwrap())
        );
        assert_eq!(std::fs::read(proc.join("30").join("stat")).unwrap(), before);
    }

    fn write_real(
        proc: &Path,
        pid: u32,
        comm: &str,
        argv: &[&str],
        ppid: u32,
        subreaper: bool,
        cwd: Option<&Path>,
    ) {
        std::fs::create_dir_all(proc).unwrap();
        if !proc.join("stat").exists() {
            std::fs::write(proc.join("stat"), format!("cpu 0 0 0\nbtime {BTIME}\n")).unwrap();
        }
        let base = proc.join(pid.to_string());
        std::fs::create_dir(&base).unwrap();
        std::fs::write(base.join("comm"), format!("{comm}\n")).unwrap();
        let argv: Vec<String> = argv.iter().map(|arg| (*arg).to_string()).collect();
        std::fs::write(base.join("cmdline"), cmdline_bytes(&argv)).unwrap();
        std::fs::write(base.join("stat"), stat_text(pid, comm, ppid, "S", 0)).unwrap();
        let mut status = format!("Name:\t{comm}\nUid:\t{UID}\t{UID}\t{UID}\t{UID}\n");
        if subreaper {
            status.push_str("ChildSubreaper:\t1\n");
        }
        std::fs::write(base.join("status"), status).unwrap();
        #[cfg(unix)]
        {
            if let Some(argv0) = argv.first() {
                std::os::unix::fs::symlink(argv0, base.join("exe")).unwrap();
            }
            if let Some(cwd) = cwd {
                std::os::unix::fs::symlink(cwd, base.join("cwd")).unwrap();
            }
        }
        #[cfg(not(unix))]
        {
            let _ = cwd;
        }
    }

    #[cfg(unix)]
    #[test]
    fn chmod_hides_are_skipped_and_an_unreadable_root_is_unavailable() {
        if rustix::process::getuid().as_raw() == 0 {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let proc = root.path().join("proc");
        write_real(
            &proc,
            10,
            "systemd",
            &["/usr/lib/systemd/systemd", "--user"],
            1,
            true,
            None,
        );
        write_real(&proc, 30, SANDBOX_COMM, &[SANDBOX], 10, false, None);
        let hidden = proc.join("31");
        write_real(
            hidden.parent().unwrap(),
            31,
            SANDBOX_COMM,
            &[SANDBOX],
            10,
            false,
            None,
        );
        let stat_path = hidden.join("stat");
        let _guard = ModeGuard::new(&stat_path, 0o000);
        let report = scan_codex_sandbox_orphans(&CodexSandboxOrphanQuery {
            proc_root: Some(proc.clone()),
            now_unix_seconds: Some(BTIME as f64 + 10.0),
            clk_tck: Some(CLK),
            uid: SubjectUid::Explicit(UID),
            ..CodexSandboxOrphanQuery::default()
        });
        assert!(report.available);
        assert_eq!(report.skipped, 1);
        assert_eq!(report.orphans.len(), 1);

        let blocked = root.path().join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        let _guard = ModeGuard::new(&blocked, 0o000);
        let report =
            scan_codex_sandbox_orphans(&query_at(Some(blocked.clone()), SubjectUid::Explicit(UID)));
        assert_eq!(report.reason, Some("proc_unreadable"));
    }

    #[cfg(unix)]
    struct ModeGuard<'a> {
        path: &'a Path,
    }

    #[cfg(unix)]
    impl<'a> ModeGuard<'a> {
        fn new(path: &'a Path, mode: u32) -> Self {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(path).unwrap().permissions();
            permissions.set_mode(mode);
            std::fs::set_permissions(path, permissions).unwrap();
            Self { path }
        }
    }

    #[cfg(unix)]
    impl Drop for ModeGuard<'_> {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(self.path)
                .map(|meta| meta.permissions())
                .unwrap_or_else(|_| std::fs::Permissions::from_mode(0o644));
            permissions.set_mode(0o644);
            let _ = std::fs::set_permissions(self.path, permissions);
        }
    }

    #[test]
    fn the_scanner_source_does_not_signal_or_spawn() {
        let source = include_str!("codex_sandbox_orphans.rs");
        let production = source
            .split("mod tests {")
            .next()
            .expect("production section");
        for needle in [
            concat!("SIG", "KILL"),
            concat!("SIG", "TERM"),
            concat!("SIG", "STOP"),
            concat!("kill", "pg"),
            concat!("p", "kill"),
            concat!("send", "_signal"),
            concat!("sub", "process"),
            concat!("os.", "kill"),
            concat!("libc::", "kill"),
            concat!("rustix::process::", "kill"),
            concat!("std::", "process"),
        ] {
            assert!(
                !production.contains(needle),
                "scanner source contains {needle}"
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn the_host_proc_root_is_unavailable_off_linux() {
        let report = scan_codex_sandbox_orphans(&CodexSandboxOrphanQuery {
            proc_root: None,
            uid: SubjectUid::Explicit(UID),
            now_unix_seconds: Some(BTIME as f64),
            clk_tck: Some(CLK),
            warn_after_seconds: DEFAULT_WARN_AFTER_SECONDS,
        });
        assert!(!report.available);
        assert_eq!(report.reason, Some("proc_unavailable"));
        assert_eq!(report.subject_uid, Some(UID));
        assert!(report.orphans.is_empty());
    }
}
