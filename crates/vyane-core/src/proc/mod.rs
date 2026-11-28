//! Process-tree helpers for POSIX and Windows.
//!
//! On POSIX each helper is a straight passthrough of the original call.
//! `probe_pid` is `kill(pid, 0)`. Tree signals are `killpg`. A contained
//! child is started with `setsid` (`start_new_session`). `pid_running`
//! counts a permission error as alive and a missing process as gone.
//!
//! Windows has no session and no numeric process group. `SIGKILL`,
//! `SIGSTOP`, and `SIGHUP` do not exist there, and `kill(pid, 0)` is
//! `CTRL_C_EVENT`, not a liveness probe. A contained child is created
//! suspended in a new console process group, assigned to a new Job Object
//! with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, and only then resumed.
//! `TerminateJobObject` kills the tree. Closing the last job handle,
//! including when the owning value is dropped, kills anything still in the
//! job. A graceful stop sends `CTRL_BREAK_EVENT` only when that child was
//! contained and its leader is still alive. Anything else is killed.
//! `UnsupportedOnWindows` is not an [`std::io::Error`], so a cleanup path
//! that only handles OS errors does not swallow it.
//!
//! This module does not replace the harness spawn path, agent process
//! control, or task process control. Those callers keep their existing
//! process-group logic.
//!
//! # What CI runs
//!
//! Linux and macOS CI run the POSIX passthrough: `kill(pid, 0)`, `killpg`,
//! a contained session, a tree killed from a temporary directory, a liveness
//! probe that does not signal, and `SIGSTOP`. They also run the pure Windows
//! decision tests (which signals can be delivered, how creation flags are
//! merged, when a failed containment kills a suspended child, and when an
//! exact kill must spare a reused pid). They do not open a Job Object.
//!
//! The Win32 calls live in [`win`] and are `cfg(windows)`. Windows CI
//! compiles and runs them. A green Linux run does not certify a live job.
//! Constants used by the decision tables are checked against `windows-sys`
//! when that target is built.
//!
//! [`win`]: crate::proc::win

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Mutex;
#[cfg(unix)]
use std::sync::OnceLock;

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Win32 job, process, registry, and console calls have no safe wrapper in this workspace. POSIX passthrough and the decision helpers stay in this module."
)]
mod win;

#[cfg(test)]
mod tests;

/// `true` when this build's process helpers use the Windows job contract.
///
/// The same value is exported at the crate root as [`crate::IS_WINDOWS`].
/// This copy is not re-exported again.
///
/// ```
/// assert_eq!(vyane_core::proc::IS_WINDOWS, vyane_core::IS_WINDOWS);
/// ```
pub const IS_WINDOWS: bool = cfg!(windows);

/// `SIGKILL` where the platform has it, and the number 9 on Windows.
///
/// On Windows the number is distinct from [`SIGTERM`] so a tree kill is not
/// mistaken for a graceful stop.
#[cfg(unix)]
pub const SIGKILL: i32 = rustix::process::Signal::KILL.as_raw();
#[cfg(windows)]
pub const SIGKILL: i32 = 9;

/// `SIGINT` on this platform.
#[cfg(unix)]
pub const SIGINT: i32 = rustix::process::Signal::INT.as_raw();
#[cfg(windows)]
pub const SIGINT: i32 = 2;

/// `SIGTERM` on this platform.
#[cfg(unix)]
pub const SIGTERM: i32 = rustix::process::Signal::TERM.as_raw();
#[cfg(windows)]
pub const SIGTERM: i32 = 15;

/// Console `CTRL_C_EVENT`. On Windows, `kill(pid, 0)` sends this. It is not
/// a liveness probe.
pub const CTRL_C_EVENT: i32 = 0;

/// Console `CTRL_BREAK_EVENT`. A contained Windows tree can receive this.
/// A process group this library did not create never does.
pub const CTRL_BREAK_EVENT: i32 = 1;

/// Python `returncode` after [`kill_process_tree`].
///
/// On POSIX this is `-SIGKILL`. Rust reports that death as
/// [`ExitStatus::signal`], not [`ExitStatus::code`]. On Windows the job is
/// terminated with 137.
///
/// ```
/// let code = vyane_core::TREE_KILLED_EXIT_CODE;
/// if vyane_core::proc::IS_WINDOWS {
///     assert_eq!(code, 137);
/// } else {
///     assert_eq!(code, -vyane_core::SIGKILL);
/// }
/// ```
pub const TREE_KILLED_EXIT_CODE: i32 = if cfg!(windows) { 137 } else { -SIGKILL };

/// Windows completed-turn status, `0xE0001843`.
///
/// ```
/// assert_eq!(vyane_core::WINDOWS_COMPLETED_TURN_EXIT_CODE, 3_758_102_595);
/// ```
pub const WINDOWS_COMPLETED_TURN_EXIT_CODE: u32 = 0xE000_1843;

/// A POSIX process-control feature Windows cannot provide safely.
///
/// This is not an [`std::io::Error`]. Matching only OS errors leaves it
/// untouched.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct UnsupportedOnWindows {
    message: String,
}

impl UnsupportedOnWindows {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Failure from a process-tree helper.
///
/// [`ProcError::Unsupported`] is not an OS error. There is no conversion
/// from this type into [`std::io::Error`].
#[derive(Debug, thiserror::Error)]
pub enum ProcError {
    #[error(transparent)]
    Os(#[from] io::Error),
    #[error(transparent)]
    Unsupported(#[from] UnsupportedOnWindows),
    #[error("{0}")]
    Invalid(String),
}

impl ProcError {
    pub fn is_process_lookup(&self) -> bool {
        match self {
            Self::Os(error) => error.kind() == io::ErrorKind::NotFound || is_esrch(error),
            _ => false,
        }
    }

    pub fn is_permission(&self) -> bool {
        match self {
            Self::Os(error) => error.kind() == io::ErrorKind::PermissionDenied || is_eperm(error),
            _ => false,
        }
    }

    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported(_))
    }

    pub fn unsupported(&self) -> Option<&UnsupportedOnWindows> {
        match self {
            Self::Unsupported(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(unix)]
impl From<rustix::io::Errno> for ProcError {
    fn from(err: rustix::io::Errno) -> Self {
        Self::Os(io::Error::from(err))
    }
}

#[cfg(unix)]
fn is_esrch(error: &io::Error) -> bool {
    error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error())
}

#[cfg(not(unix))]
fn is_esrch(_error: &io::Error) -> bool {
    false
}

#[cfg(unix)]
fn is_eperm(error: &io::Error) -> bool {
    error.raw_os_error() == Some(rustix::io::Errno::PERM.raw_os_error())
}

#[cfg(not(unix))]
fn is_eperm(_error: &io::Error) -> bool {
    false
}

fn not_found(message: impl Into<String>) -> ProcError {
    ProcError::Os(io::Error::new(io::ErrorKind::NotFound, message.into()))
}

#[cfg(any(windows, test))]
fn permission(message: impl Into<String>) -> ProcError {
    ProcError::Os(io::Error::new(
        io::ErrorKind::PermissionDenied,
        message.into(),
    ))
}

fn invalid(message: impl Into<String>) -> ProcError {
    ProcError::Invalid(message.into())
}

fn unsupported(message: impl Into<String>) -> ProcError {
    ProcError::Unsupported(UnsupportedOnWindows::new(message))
}

#[cfg(unix)]
fn missing_loop() -> ProcError {
    invalid("add_loop_signal_handler must be called from a running async loop")
}

/// Where a contained child's standard stream is connected.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StdioMode {
    #[default]
    Inherit,
    Null,
    Piped,
}

/// Fields a contained spawn keeps or rewrites.
///
/// `None` means the corresponding Python keyword was absent. An empty
/// [`env`](Self::env) map clears the child environment. On POSIX,
/// [`contained_spawn_kwargs`] sets [`start_new_session`](Self::start_new_session)
/// and leaves every other field unchanged. On Windows it drops
/// `start_new_session` and `process_group`, and ORs
/// `CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP` into `creationflags`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpawnKwargs {
    pub env: Option<BTreeMap<String, String>>,
    pub cwd: Option<PathBuf>,
    pub start_new_session: Option<bool>,
    pub process_group: Option<i64>,
    pub creationflags: Option<u32>,
    pub stdin: StdioMode,
    pub stdout: StdioMode,
    pub stderr: StdioMode,
}

/// A child this library spawned, or a test double with no OS handle.
///
/// Dropping a value that owns a Windows job closes that job. On Windows that
/// kills every process still in the job. On POSIX, drop does not signal the
/// child. Call [`kill_process_tree`] or [`TrackedProcess::wait`] explicitly.
#[derive(Debug)]
#[must_use = "dropping a contained process closes its Windows job and kills the tree"]
pub struct TrackedProcess {
    job: Option<JobObject>,
    #[cfg(windows)]
    group_recorded: bool,
    kind: ProcessKind,
}

#[derive(Debug)]
enum ProcessKind {
    Std(Child),
    Manual(ManualProcess),
}

#[derive(Debug)]
#[allow(dead_code)]
struct ManualProcess {
    pid: u32,
    killed: bool,
    terminated: bool,
}

impl TrackedProcess {
    pub fn from_child(child: Child) -> Self {
        Self::from_kind(ProcessKind::Std(child))
    }

    /// A stand-in with no process handle.
    ///
    /// [`contain_process`] leaves it untouched. On Windows,
    /// [`kill_process_tree`] calls the double's own kill and does not signal
    /// `pid`. On POSIX the tree helpers still call `killpg` with `pid`.
    pub fn manual(pid: u32) -> Self {
        Self::from_kind(ProcessKind::Manual(ManualProcess {
            pid,
            killed: false,
            terminated: false,
        }))
    }

    fn from_kind(kind: ProcessKind) -> Self {
        Self {
            job: None,
            #[cfg(windows)]
            group_recorded: false,
            kind,
        }
    }

    pub fn id(&self) -> u32 {
        match &self.kind {
            ProcessKind::Std(child) => child.id(),
            ProcessKind::Manual(manual) => manual.pid,
        }
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProcError> {
        match &mut self.kind {
            ProcessKind::Std(child) => child.try_wait().map_err(ProcError::from),
            ProcessKind::Manual(_) => Ok(None),
        }
    }

    pub fn wait(&mut self) -> Result<ExitStatus, ProcError> {
        match &mut self.kind {
            ProcessKind::Std(child) => child.wait().map_err(ProcError::from),
            ProcessKind::Manual(_) => Err(invalid("manual process has no exit status")),
        }
    }

    pub fn stdout(&mut self) -> Option<&mut std::process::ChildStdout> {
        match &mut self.kind {
            ProcessKind::Std(child) => child.stdout.as_mut(),
            ProcessKind::Manual(_) => None,
        }
    }

    #[cfg(any(windows, test))]
    fn kill_direct(&mut self) -> Result<(), ProcError> {
        match &mut self.kind {
            ProcessKind::Std(child) => child.kill().map_err(ProcError::from),
            ProcessKind::Manual(manual) => {
                manual.killed = true;
                Ok(())
            }
        }
    }

    #[cfg(unix)]
    fn terminate_direct(&mut self) -> Result<(), ProcError> {
        match &mut self.kind {
            ProcessKind::Std(child) => {
                let pid = rustix::process::Pid::from_child(child);
                rustix::process::kill_process(pid, rustix::process::Signal::TERM)?;
                Ok(())
            }
            ProcessKind::Manual(manual) => {
                manual.terminated = true;
                Ok(())
            }
        }
    }

    #[cfg(windows)]
    fn raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
        use std::os::windows::io::AsRawHandle;
        match &self.kind {
            ProcessKind::Std(child) => Some(child.as_raw_handle()),
            ProcessKind::Manual(_) => None,
        }
    }

    #[cfg(windows)]
    fn set_job(&mut self, job: JobObject) {
        self.group_recorded = true;
        self.job = Some(job);
    }

    #[cfg(windows)]
    fn group_recorded(&self) -> bool {
        self.group_recorded
    }

    #[cfg(test)]
    fn manual_killed(&self) -> bool {
        match &self.kind {
            ProcessKind::Manual(manual) => manual.killed,
            ProcessKind::Std(_) => false,
        }
    }

    #[cfg(test)]
    fn manual_terminated(&self) -> bool {
        match &self.kind {
            ProcessKind::Manual(manual) => manual.terminated,
            ProcessKind::Std(_) => false,
        }
    }
}

/// Anonymous Windows Job Object that kills its processes when closed.
///
/// On POSIX [`JobObject::new`] returns [`UnsupportedOnWindows`].
pub struct JobObject {
    #[cfg(windows)]
    inner: Mutex<Option<std::os::windows::io::OwnedHandle>>,
    #[cfg(not(windows))]
    _private: (),
}

impl fmt::Debug for JobObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobObject")
            .field("closed", &self.closed())
            .finish()
    }
}

impl JobObject {
    pub fn new() -> Result<Self, ProcError> {
        #[cfg(windows)]
        {
            Ok(Self {
                inner: Mutex::new(Some(win::create_job()?)),
            })
        }
        #[cfg(not(windows))]
        {
            Err(unsupported("Job Objects exist only on Windows"))
        }
    }

    pub fn closed(&self) -> bool {
        #[cfg(windows)]
        {
            self.inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_none()
        }
        #[cfg(not(windows))]
        {
            true
        }
    }

    /// Put `process` in this job. `process` is the creator's handle, never a pid.
    #[cfg(windows)]
    pub fn assign(&self, process: std::os::windows::io::RawHandle) -> Result<(), ProcError> {
        self.with_handle(|job| win::assign(job, process))
    }

    pub fn active_processes(&self) -> Result<u32, ProcError> {
        #[cfg(windows)]
        {
            self.with_handle(win::active_processes)
        }
        #[cfg(not(windows))]
        {
            Err(unsupported("Job Objects exist only on Windows"))
        }
    }

    /// Cumulative CPU seconds and transferred bytes, including exited descendants.
    pub fn activity(&self) -> Result<(f64, u64), ProcError> {
        #[cfg(windows)]
        {
            self.with_handle(win::activity)
        }
        #[cfg(not(windows))]
        {
            Err(unsupported("Job Objects exist only on Windows"))
        }
    }

    pub fn terminate(&self) -> Result<(), ProcError> {
        self.terminate_with_code(TREE_KILLED_EXIT_CODE)
    }

    pub fn terminate_with_code(&self, exit_code: i32) -> Result<(), ProcError> {
        #[cfg(windows)]
        {
            let code = u32::try_from(exit_code)
                .map_err(|_| invalid("job exit code does not fit a DWORD"))?;
            self.with_handle(|job| win::terminate_job(job, code))
        }
        #[cfg(not(windows))]
        {
            let _ = exit_code;
            Err(unsupported("Job Objects exist only on Windows"))
        }
    }

    /// Close the job handle. A process still inside is killed.
    ///
    /// A second call does nothing.
    pub fn close(&self) {
        #[cfg(windows)]
        {
            let mut guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            guard.take();
        }
    }

    #[cfg(windows)]
    fn with_handle<T>(
        &self,
        body: impl FnOnce(std::os::windows::io::RawHandle) -> Result<T, ProcError>,
    ) -> Result<T, ProcError> {
        use std::os::windows::io::AsRawHandle;
        let guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let handle = guard
            .as_ref()
            .ok_or_else(|| invalid("job object is closed"))?;
        body(handle.as_raw_handle())
    }
}

#[cfg(windows)]
impl Drop for JobObject {
    fn drop(&mut self) {
        self.close();
    }
}

/// Named signals that exist on this platform, in the given order.
///
/// Unknown names are dropped. On Windows `SIGKILL`, `SIGHUP`, and `SIGSTOP`
/// are unknown even though [`SIGKILL`] is the number 9 for tree helpers.
pub fn available_signals(names: &[&str]) -> Vec<i32> {
    names.iter().copied().filter_map(signal_number).collect()
}

#[cfg(unix)]
fn signal_number(name: &str) -> Option<i32> {
    use rustix::process::Signal;
    let signal = match name {
        "SIGHUP" => Signal::HUP,
        "SIGINT" => Signal::INT,
        "SIGQUIT" => Signal::QUIT,
        "SIGILL" => Signal::ILL,
        "SIGTRAP" => Signal::TRAP,
        "SIGABRT" | "SIGIOT" => Signal::ABORT,
        "SIGBUS" => Signal::BUS,
        "SIGFPE" => Signal::FPE,
        "SIGKILL" => Signal::KILL,
        "SIGUSR1" => Signal::USR1,
        "SIGSEGV" => Signal::SEGV,
        "SIGUSR2" => Signal::USR2,
        "SIGPIPE" => Signal::PIPE,
        "SIGALRM" => Signal::ALARM,
        "SIGTERM" => Signal::TERM,
        "SIGCHLD" => Signal::CHILD,
        "SIGCONT" => Signal::CONT,
        "SIGSTOP" => Signal::STOP,
        "SIGTSTP" => Signal::TSTP,
        "SIGTTIN" => Signal::TTIN,
        "SIGTTOU" => Signal::TTOU,
        "SIGURG" => Signal::URG,
        "SIGXCPU" => Signal::XCPU,
        "SIGXFSZ" => Signal::XFSZ,
        "SIGVTALRM" => Signal::VTALARM,
        "SIGPROF" => Signal::PROF,
        "SIGWINCH" => Signal::WINCH,
        "SIGIO" | "SIGPOLL" => Signal::IO,
        "SIGSYS" => Signal::SYS,
        _ => return None,
    };
    Some(signal.as_raw())
}

#[cfg(windows)]
fn signal_number(name: &str) -> Option<i32> {
    match name {
        "SIGABRT" => Some(libc::SIGABRT),
        "SIGFPE" => Some(libc::SIGFPE),
        "SIGILL" => Some(libc::SIGILL),
        "SIGINT" => Some(libc::SIGINT),
        "SIGSEGV" => Some(libc::SIGSEGV),
        "SIGTERM" => Some(libc::SIGTERM),
        // MSVC's `SIGBREAK`. The libc crate does not export that constant.
        "SIGBREAK" => Some(21),
        "CTRL_C_EVENT" => Some(CTRL_C_EVENT),
        "CTRL_BREAK_EVENT" => Some(CTRL_BREAK_EVENT),
        _ => None,
    }
}

/// Install `callback` for `sig` on the current Tokio runtime.
///
/// The callback captures its arguments. POSIX uses
/// `tokio::signal`; the task is aborted by [`remove_loop_signal_handler`].
/// `SIGKILL` and `SIGSTOP` cannot be caught.
///
/// Windows installs a process-wide CRT handler and schedules the callback on
/// the runtime. Only `SIGINT` and `SIGBREAK` are delivered by Windows itself.
/// [`remove_loop_signal_handler`] restores the handler that was installed
/// before this call. Python instead forces `default_int_handler` for
/// `SIGINT` and `SIG_DFL` for every other signal.
pub fn add_loop_signal_handler(
    sig: i32,
    callback: impl Fn() + Send + Sync + 'static,
) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        add_unix_loop_signal_handler(sig, callback)
    }
    #[cfg(windows)]
    {
        win::add_loop_signal_handler(sig, callback)
    }
}

pub fn remove_loop_signal_handler(sig: i32) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        remove_unix_loop_signal_handler(sig);
        Ok(())
    }
    #[cfg(windows)]
    {
        win::remove_loop_signal_handler(sig)
    }
}

#[cfg(unix)]
fn unix_slots() -> &'static Mutex<std::collections::HashMap<i32, tokio::task::JoinHandle<()>>> {
    static SLOTS: OnceLock<Mutex<std::collections::HashMap<i32, tokio::task::JoinHandle<()>>>> =
        OnceLock::new();
    SLOTS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

#[cfg(unix)]
fn add_unix_loop_signal_handler(
    sig: i32,
    callback: impl Fn() + Send + Sync + 'static,
) -> Result<(), ProcError> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(missing_loop());
    }
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(sig))?;
    let callback = std::sync::Arc::new(callback);
    let task = tokio::spawn(async move {
        loop {
            if signal.recv().await.is_none() {
                break;
            }
            callback();
        }
    });
    let mut slots = unix_slots()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(previous) = slots.insert(sig, task) {
        previous.abort();
    }
    Ok(())
}

#[cfg(unix)]
fn remove_unix_loop_signal_handler(sig: i32) {
    if let Some(task) = unix_slots()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&sig)
    {
        task.abort();
    }
}

/// `kill(pid, 0)` on POSIX.
///
/// On Windows this opens the process and does not send a signal. A pid that
/// is not a single positive process is reported missing.
pub fn probe_pid(pid: i64) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        posix_signal(pid, 0)
    }
    #[cfg(windows)]
    {
        win::probe_pid(pid)
    }
}

/// Whether [`probe_pid`] finds `pid`.
///
/// A permission error counts as alive. A missing process counts as gone.
/// Any other OS error is returned. On POSIX a zombie still counts as present.
pub fn pid_running(pid: i64) -> Result<bool, ProcError> {
    running_from_probe(probe_pid(pid))
}

fn running_from_probe(result: Result<(), ProcError>) -> Result<bool, ProcError> {
    match result {
        Ok(()) => Ok(true),
        Err(error) if error.is_process_lookup() => Ok(false),
        Err(error) if error.is_permission() => Ok(true),
        Err(error) => Err(error),
    }
}

/// Creation time of a running Windows process, in 100 ns ticks since 1601.
pub fn windows_process_start_time(pid: i64) -> Result<u64, ProcError> {
    #[cfg(windows)]
    {
        win::process_start_time(pid)
    }
    #[cfg(not(windows))]
    {
        let _ = pid;
        Err(unsupported("windows_process_start_time is Windows-only"))
    }
}

/// Kill `pid` only when its creation time is still `start_time`.
///
/// The identity check and `TerminateProcess` share one handle. `true` means
/// that generation was running and was told to exit with
/// [`TREE_KILLED_EXIT_CODE`]. `false` means it was already gone or the pid
/// now belongs to a later process.
pub fn kill_exact_process(pid: i64, start_time: u64) -> Result<bool, ProcError> {
    #[cfg(windows)]
    {
        win::kill_exact(pid, start_time)
    }
    #[cfg(not(windows))]
    {
        let _ = (pid, start_time);
        Err(unsupported("kill_exact_process is Windows-only"))
    }
}

/// Windows kernel boot counter, formatted `windows-boot-{BootId}`.
///
/// `Ok(None)` means the registry value is missing or not a DWORD. On POSIX
/// this is [`UnsupportedOnWindows`].
pub fn boot_id() -> Result<Option<String>, ProcError> {
    #[cfg(windows)]
    {
        Ok(win::boot_id())
    }
    #[cfg(not(windows))]
    {
        Err(unsupported("use the platform boot id reader on POSIX"))
    }
}

/// Spawn fields for a child whose whole tree can be killed.
pub fn contained_spawn_kwargs(kwargs: SpawnKwargs) -> SpawnKwargs {
    #[cfg(unix)]
    {
        let mut kwargs = kwargs;
        kwargs.start_new_session = Some(true);
        kwargs
    }
    #[cfg(windows)]
    {
        merge_creationflags(kwargs, CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP)
    }
}

/// Finish a contained spawn.
///
/// On POSIX this is a no-op: the new session is enough. On Windows a real
/// child is assigned to a new kill-on-close job and then resumed. A value
/// with no process handle is left untouched. If containment fails, the
/// suspended child is killed before the error is returned.
pub fn contain_process(process: &mut TrackedProcess) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        let _ = process;
        Ok(())
    }
    #[cfg(windows)]
    {
        win::contain_process(process)
    }
}

/// The job [`contain_process`] attached, if any.
pub fn process_job(process: &TrackedProcess) -> Option<&JobObject> {
    process.job.as_ref()
}

/// Spawn `program` already contained.
///
/// On failure the child is waited and the error is returned. The caller does
/// not receive a live uncontained process.
pub fn popen_contained<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
    kwargs: SpawnKwargs,
) -> Result<TrackedProcess, ProcError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let kwargs = contained_spawn_kwargs(kwargs);
    let command = command_from_args(program.as_ref(), args, &kwargs)?;
    let child = spawn_contained(command, &kwargs)?;
    let mut process = TrackedProcess::from_child(child);
    if let Err(error) = contain_process(&mut process) {
        let _ = process.wait();
        return Err(error);
    }
    Ok(process)
}

/// Async form of [`popen_contained`].
///
/// The spawn runs inline, then this yields once when the caller is already
/// on a Tokio runtime. Realtime POSIX signals outside the named set are
/// rejected by the tree helpers; this function does not change that.
pub async fn create_subprocess_exec_contained<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
    kwargs: SpawnKwargs,
) -> Result<TrackedProcess, ProcError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let process = popen_contained(program, args, kwargs)?;
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::yield_now().await;
    }
    Ok(process)
}

/// `killpg(pid, SIGKILL)` on POSIX. On Windows, `TerminateJobObject`.
///
/// A Windows process with no open job is killed directly. A test double's
/// own kill runs in that case, and its pid is not signalled.
pub fn kill_process_tree(process: &mut TrackedProcess) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        posix_killpg(i64::from(process.id()), SIGKILL)
    }
    #[cfg(windows)]
    {
        let has_open_job = process.job.as_ref().is_some_and(|job| !job.closed());
        if has_open_job {
            let job = process
                .job
                .as_ref()
                .ok_or_else(|| invalid("job object is closed"))?;
            job.terminate()
        } else {
            hard_kill_direct(process)
        }
    }
}

/// `killpg(pid, SIGTERM)` on POSIX.
///
/// On Windows this sends `CTRL_BREAK_EVENT` when that is safe, and otherwise
/// kills the tree.
pub fn terminate_process_tree(process: &mut TrackedProcess) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        posix_killpg(i64::from(process.id()), SIGTERM)
    }
    #[cfg(windows)]
    {
        if !win::send_ctrl_break(process)? {
            kill_process_tree(process)?;
        }
        Ok(())
    }
}

/// `killpg(pid, sig)` on POSIX.
///
/// On Windows, [`SIGKILL`] kills the tree, [`SIGTERM`] requests a stop,
/// and [`SIGINT`] or [`CTRL_BREAK_EVENT`] send `CTRL_BREAK_EVENT` only to a
/// contained leader that is still alive. Every other signal, including
/// `SIGSTOP` and `CTRL_C_EVENT`, is [`UnsupportedOnWindows`].
pub fn signal_process_tree(process: &mut TrackedProcess, sig: i32) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        posix_killpg(i64::from(process.id()), sig)
    }
    #[cfg(windows)]
    {
        match windows_signal_action(sig) {
            WindowsSignalAction::Kill => kill_process_tree(process),
            WindowsSignalAction::Terminate => terminate_process_tree(process),
            WindowsSignalAction::CtrlBreak => {
                if win::send_ctrl_break(process)? {
                    Ok(())
                } else {
                    Err(not_found(format!(
                        "cannot deliver CTRL_BREAK to process group {}",
                        process.id()
                    )))
                }
            }
            WindowsSignalAction::Unsupported => Err(unsupported(format!(
                "signal {sig} cannot be delivered to a process tree on Windows"
            ))),
        }
    }
}

/// Stop one completed turn without using a normal failure status.
///
/// On POSIX this is `kill(pid, SIGTERM)` for a real child, and the double's
/// own terminate for a value with no handle. A missing process returns
/// `false`. On Windows a real handle is required; the exit status is
/// [`WINDOWS_COMPLETED_TURN_EXIT_CODE`].
pub fn terminate_completed_turn(process: &mut TrackedProcess) -> Result<bool, ProcError> {
    if process.try_wait()?.is_some() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        match process.terminate_direct() {
            Err(error) if error.is_process_lookup() => Ok(false),
            Err(error) => Err(error),
            Ok(()) => Ok(true),
        }
    }
    #[cfg(windows)]
    {
        win::terminate_completed(process)
    }
}

fn command_from_args<I, S>(
    program: &OsStr,
    args: I,
    kwargs: &SpawnKwargs,
) -> Result<Command, ProcError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    if program.is_empty() {
        return Err(invalid("program is empty"));
    }
    let mut command = Command::new(program);
    command.args(args);
    command.stdin(stdio(kwargs.stdin));
    command.stdout(stdio(kwargs.stdout));
    command.stderr(stdio(kwargs.stderr));
    if let Some(cwd) = &kwargs.cwd {
        command.current_dir(cwd);
    }
    if let Some(env) = &kwargs.env {
        command.env_clear();
        command.envs(env);
    }
    Ok(command)
}

fn stdio(mode: StdioMode) -> Stdio {
    match mode {
        StdioMode::Inherit => Stdio::inherit(),
        StdioMode::Null => Stdio::null(),
        StdioMode::Piped => Stdio::piped(),
    }
}

fn spawn_contained(command: Command, kwargs: &SpawnKwargs) -> Result<Child, ProcError> {
    #[cfg(unix)]
    {
        spawn_posix(command, kwargs)
    }
    #[cfg(windows)]
    {
        let flags = kwargs.creationflags.unwrap_or(0);
        win::spawn(command, flags).map_err(ProcError::from)
    }
}

#[cfg(unix)]
fn spawn_posix(mut command: Command, kwargs: &SpawnKwargs) -> Result<Child, ProcError> {
    if kwargs.creationflags.unwrap_or(0) != 0 {
        return Err(invalid(
            "creationflags is only supported on Windows platforms",
        ));
    }
    if kwargs.start_new_session == Some(true) {
        if kwargs.process_group.is_some() {
            return Err(invalid(
                "process_group and start_new_session are mutually exclusive",
            ));
        }
        // ProcessSession calls setsid. into_inner returns the raw child;
        // the session wrapper's drop does not signal the group.
        let mut wrapped = process_wrap::std::StdCommandWrap::from(command);
        wrapped.wrap(process_wrap::std::ProcessSession);
        return Ok(wrapped.spawn()?.into_inner());
    }
    if let Some(group) = kwargs.process_group {
        let group = i32::try_from(group).map_err(|_| invalid("process_group is out of range"))?;
        if group < 0 {
            return Err(invalid("process_group is out of range"));
        }
        use std::os::unix::process::CommandExt;
        command.process_group(group);
    }
    command.spawn().map_err(ProcError::from)
}

#[cfg(unix)]
fn posix_signal(pid: i64, sig: i32) -> Result<(), ProcError> {
    use rustix::process::{
        kill_current_process_group, kill_process, kill_process_group,
        test_kill_current_process_group, test_kill_process, test_kill_process_group,
    };
    let signal = named_signal(sig)?;
    if pid == 0 {
        return match signal {
            None => Ok(test_kill_current_process_group()?),
            Some(signal) => Ok(kill_current_process_group(signal)?),
        };
    }
    if pid > 0 {
        let pid = rustix_pid(pid)?;
        return match signal {
            None => Ok(test_kill_process(pid)?),
            Some(signal) => Ok(kill_process(pid, signal)?),
        };
    }
    let group = pid
        .checked_neg()
        .ok_or_else(|| not_found(format!("invalid pid {pid}")))?;
    let group = rustix_pid(group)?;
    match signal {
        None => Ok(test_kill_process_group(group)?),
        Some(signal) => Ok(kill_process_group(group, signal)?),
    }
}

#[cfg(unix)]
fn named_signal(sig: i32) -> Result<Option<rustix::process::Signal>, ProcError> {
    if sig == 0 {
        return Ok(None);
    }
    rustix::process::Signal::from_named_raw(sig)
        .map(Some)
        .ok_or_else(|| {
            ProcError::Os(io::Error::from_raw_os_error(
                rustix::io::Errno::INVAL.raw_os_error(),
            ))
        })
}

#[cfg(unix)]
fn rustix_pid(pid: i64) -> Result<rustix::process::Pid, ProcError> {
    let raw = i32::try_from(pid).map_err(|_| not_found(format!("invalid pid {pid}")))?;
    rustix::process::Pid::from_raw(raw).ok_or_else(|| not_found(format!("invalid pid {pid}")))
}

#[cfg(unix)]
fn posix_killpg(pgid: i64, sig: i32) -> Result<(), ProcError> {
    if pgid < 0 {
        return Err(ProcError::Os(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid process group {pgid}"),
        )));
    }
    if pgid == 0 {
        posix_signal(0, sig)
    } else {
        let negative = pgid
            .checked_neg()
            .ok_or_else(|| not_found(format!("invalid process group {pgid}")))?;
        posix_signal(negative, sig)
    }
}

#[cfg(windows)]
fn hard_kill_direct(process: &mut TrackedProcess) -> Result<(), ProcError> {
    match process.kill_direct() {
        Err(error) if error.is_process_lookup() || error.is_permission() => Ok(()),
        other => other,
    }
}

#[cfg(any(windows, test))]
const CREATE_SUSPENDED: u32 = 4;
#[cfg(any(windows, test))]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x200;

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WindowsSignalAction {
    Kill,
    Terminate,
    CtrlBreak,
    Unsupported,
}

#[cfg(any(windows, test))]
fn windows_signal_action(sig: i32) -> WindowsSignalAction {
    if sig == SIGKILL {
        WindowsSignalAction::Kill
    } else if sig == SIGTERM {
        WindowsSignalAction::Terminate
    } else if sig == SIGINT || sig == CTRL_BREAK_EVENT {
        WindowsSignalAction::CtrlBreak
    } else {
        WindowsSignalAction::Unsupported
    }
}

#[cfg(any(windows, test))]
fn merge_creationflags(mut kwargs: SpawnKwargs, flags: u32) -> SpawnKwargs {
    kwargs.start_new_session = None;
    kwargs.process_group = None;
    kwargs.creationflags = Some(kwargs.creationflags.unwrap_or(0) | flags);
    kwargs
}

#[cfg(any(windows, test))]
struct Containment<J, E> {
    result: Result<Option<J>, E>,
    kill_suspended_child: bool,
}

#[cfg(any(windows, test))]
fn contain_suspended<J, E>(
    has_handle: bool,
    mut create: impl FnMut() -> Result<J, E>,
    mut assign: impl FnMut(&J) -> Result<(), E>,
    mut resume: impl FnMut(&J) -> Result<(), E>,
    mut close: impl FnMut(&J),
) -> Containment<J, E> {
    if !has_handle {
        return Containment {
            result: Ok(None),
            kill_suspended_child: false,
        };
    }
    let job = match create() {
        Ok(job) => job,
        Err(error) => {
            return Containment {
                result: Err(error),
                kill_suspended_child: true,
            };
        }
    };
    if let Err(error) = assign(&job) {
        close(&job);
        return Containment {
            result: Err(error),
            kill_suspended_child: true,
        };
    }
    if let Err(error) = resume(&job) {
        close(&job);
        return Containment {
            result: Err(error),
            kill_suspended_child: true,
        };
    }
    Containment {
        result: Ok(Some(job)),
        kill_suspended_child: false,
    }
}

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExactKill {
    Kill,
    SpareGone,
    SpareReused,
}

#[cfg(any(windows, test))]
fn exact_kill_decision(running: bool, creation: u64, start_time: u64) -> ExactKill {
    if !running {
        ExactKill::SpareGone
    } else if creation != start_time {
        ExactKill::SpareReused
    } else {
        ExactKill::Kill
    }
}

#[cfg(any(windows, test))]
fn checked_windows_pid(pid: i64) -> Result<u32, ProcError> {
    if pid <= 0 {
        return Err(not_found(format!("invalid pid {pid}")));
    }
    u32::try_from(pid).map_err(|_| not_found(format!("invalid pid {pid}")))
}

#[cfg(any(windows, test))]
fn filetime_ticks(high: u32, low: u32) -> u64 {
    (u64::from(high) << 32) | u64::from(low)
}

#[cfg(any(windows, test))]
fn format_windows_boot_id(kind: u32, value: u32, len: u32) -> Option<String> {
    if kind == 4 && len == 4 {
        Some(format!("windows-boot-{value}"))
    } else {
        None
    }
}

#[cfg(any(windows, test))]
fn job_activity(user: i64, kernel: i64, read: u64, write: u64) -> (f64, u64) {
    let ticks = i128::from(user) + i128::from(kernel);
    let cpu = ticks as f64 / 1e7;
    let bytes = u64::try_from(u128::from(read) + u128::from(write)).unwrap_or(u64::MAX);
    (cpu, bytes)
}

#[cfg(any(windows, test))]
fn win32_error(code: u32, what: &str) -> ProcError {
    const ERROR_INVALID_PARAMETER: u32 = 87;
    const ERROR_ACCESS_DENIED: u32 = 5;
    if code == ERROR_INVALID_PARAMETER {
        return not_found(format!("{what}: no such process"));
    }
    if code == ERROR_ACCESS_DENIED {
        return permission(format!("{what}: access denied"));
    }
    let detail = i32::try_from(code)
        .map(|raw| io::Error::from_raw_os_error(raw).to_string())
        .unwrap_or_else(|_| format!("os error {code}"));
    ProcError::Os(io::Error::other(format!("{what}: {detail}")))
}
