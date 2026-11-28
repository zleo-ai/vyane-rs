//! Win32 Job Object, process-identity, and console-break calls.
//!
//! Linux and macOS CI do not compile this module. Windows CI does, and the
//! unit tests in the parent module run these calls there.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, Command};
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock};

use windows_sys::Win32::Foundation::{
    ERROR_SUCCESS, GetLastError, HANDLE, STILL_ACTIVE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_BASIC_AND_IO_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
    JobObjectBasicAndIoAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Registry::{
    HKEY, HKEY_LOCAL_MACHINE, KEY_QUERY_VALUE, REG_DWORD, RegCloseKey, RegOpenKeyExW,
    RegQueryValueExW,
};
use windows_sys::Win32::System::Threading::{
    GetExitCodeProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, TerminateProcess, WaitForSingleObject,
};

use super::{
    ExactKill, ProcError, TrackedProcess, checked_windows_pid, exact_kill_decision, filetime_ticks,
    format_windows_boot_id, invalid, job_activity, not_found, win32_error,
};

const _: () = {
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED};
    assert!(CREATE_SUSPENDED == 4);
    assert!(CREATE_NEW_PROCESS_GROUP == 0x200);
    assert!(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE == 0x2000);
    assert!(CTRL_BREAK_EVENT == 1);
    assert!(windows_sys::Win32::System::Console::CTRL_C_EVENT == 0);
    assert!(WAIT_OBJECT_0 == 0);
    assert!(WAIT_TIMEOUT == 258);
    assert!(PROCESS_QUERY_LIMITED_INFORMATION == 0x1000);
    assert!(PROCESS_TERMINATE == 1);
    assert!(PROCESS_SYNCHRONIZE == 0x0010_0000);
    assert!(REG_DWORD == 4);
    assert!(STILL_ACTIVE == 259);
    assert!(super::TREE_KILLED_EXIT_CODE == 137);
    assert!(libc::SIGINT == 2);
    assert!(libc::SIGILL == 4);
    assert!(libc::SIGFPE == 8);
    assert!(libc::SIGSEGV == 11);
    assert!(libc::SIGTERM == 15);
    assert!(libc::SIGABRT == 22);
};

const TREE_KILL_STATUS: u32 = 137;

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtResumeProcess(process: HANDLE) -> i32;
}

#[cfg(test)]
thread_local! {
    static FAIL_ASSIGN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn fail_assign(fail: bool) {
    FAIL_ASSIGN.with(|cell| cell.set(fail));
}

pub(crate) fn spawn(mut command: Command, flags: u32) -> io::Result<Child> {
    command.creation_flags(flags);
    command.spawn()
}

pub(crate) fn create_job() -> Result<OwnedHandle, ProcError> {
    // SAFETY: both pointers are null. CreateJobObjectW accepts a null security
    // descriptor and a null name, which creates an anonymous, non-inheritable job.
    let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
    if handle.is_null() {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, "CreateJobObject"));
    }
    // SAFETY: CreateJobObjectW returned a fresh, exclusively owned handle.
    let owned = unsafe { OwnedHandle::from_raw_handle(handle) };
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let size = dword_size::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>()?;
    // SAFETY: `info` is a live extended-limit structure and `size` is its byte length.
    let ok = unsafe {
        SetInformationJobObject(
            owned.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            size,
        )
    };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        drop(owned);
        return Err(win32_error(code, "SetInformationJobObject"));
    }
    Ok(owned)
}

pub(crate) fn assign(job: RawHandle, process: RawHandle) -> Result<(), ProcError> {
    #[cfg(test)]
    if FAIL_ASSIGN.with(|cell| cell.get()) {
        return Err(super::permission("assign refused"));
    }
    if process.is_null() {
        return Err(invalid(
            "AssignProcessToJobObject requires a process handle",
        ));
    }
    // SAFETY: both values are live Win32 handles owned by the caller. Assignment
    // does not take ownership of the process handle.
    let ok = unsafe { AssignProcessToJobObject(job, process) };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, "AssignProcessToJobObject"));
    }
    Ok(())
}

pub(crate) fn active_processes(job: RawHandle) -> Result<u32, ProcError> {
    let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
    query(
        job,
        JobObjectBasicAccountingInformation,
        (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
        dword_size::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>()?,
    )?;
    Ok(info.ActiveProcesses)
}

pub(crate) fn activity(job: RawHandle) -> Result<(f64, u64), ProcError> {
    let mut info = JOBOBJECT_BASIC_AND_IO_ACCOUNTING_INFORMATION::default();
    query(
        job,
        JobObjectBasicAndIoAccountingInformation,
        (&mut info as *mut JOBOBJECT_BASIC_AND_IO_ACCOUNTING_INFORMATION).cast(),
        dword_size::<JOBOBJECT_BASIC_AND_IO_ACCOUNTING_INFORMATION>()?,
    )?;
    Ok(job_activity(
        info.BasicInfo.TotalUserTime,
        info.BasicInfo.TotalKernelTime,
        info.IoInfo.ReadTransferCount,
        info.IoInfo.WriteTransferCount,
    ))
}

pub(crate) fn terminate_job(job: RawHandle, exit_code: u32) -> Result<(), ProcError> {
    // SAFETY: `job` is an open job handle. The exit code is the status stored
    // on every process in the job.
    let ok = unsafe { TerminateJobObject(job, exit_code) };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, "TerminateJobObject"));
    }
    Ok(())
}

pub(crate) fn probe_pid(pid: i64) -> Result<(), ProcError> {
    let pid = checked_windows_pid(pid)?;
    let handle = open_running(pid)?;
    // Drop closes the probe handle. Nothing was signalled.
    drop(handle);
    Ok(())
}

pub(crate) fn process_start_time(pid: i64) -> Result<u64, ProcError> {
    let pid = checked_windows_pid(pid)?;
    let handle = open_running(pid)?;
    creation_time(handle.as_raw_handle(), pid)
}

pub(crate) fn kill_exact(pid: i64, start_time: u64) -> Result<bool, ProcError> {
    let pid = checked_windows_pid(pid)?;
    let handle = match open_process(
        pid,
        PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | PROCESS_SYNCHRONIZE,
    ) {
        Ok(handle) => handle,
        Err(error) if error.is_process_lookup() => return Ok(false),
        Err(error) => return Err(error),
    };
    let running = handle_is_running(handle.as_raw_handle(), true)?;
    let decision = if running {
        let creation = creation_time(handle.as_raw_handle(), pid)?;
        exact_kill_decision(true, creation, start_time)
    } else {
        ExactKill::SpareGone
    };
    if decision != ExactKill::Kill {
        return Ok(false);
    }
    terminate_process(handle.as_raw_handle(), pid, TREE_KILL_STATUS)?;
    Ok(true)
}

pub(crate) fn boot_id() -> Option<String> {
    let subkey = wide(
        r"SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management\PrefetchParameters",
    );
    let mut raw = ptr::null_mut();
    // SAFETY: `subkey` is a null-terminated UTF-16 path. A failure leaves `raw` unused.
    let status = unsafe {
        RegOpenKeyExW(
            HKEY_LOCAL_MACHINE,
            subkey.as_ptr(),
            0,
            KEY_QUERY_VALUE,
            &mut raw,
        )
    };
    if status != ERROR_SUCCESS || raw.is_null() {
        return None;
    }
    let key = RegKey(raw);
    let mut kind = 0u32;
    let mut value = 0u32;
    let mut len = 4u32;
    let name = wide("BootId");
    // SAFETY: `key` is an open registry key and the output buffers match `len`.
    let status = unsafe {
        RegQueryValueExW(
            key.0,
            name.as_ptr(),
            ptr::null(),
            &mut kind,
            (&mut value as *mut u32).cast(),
            &mut len,
        )
    };
    if status != ERROR_SUCCESS {
        return None;
    }
    format_windows_boot_id(kind, value, len)
}

pub(crate) fn contain_process(process: &mut TrackedProcess) -> Result<(), ProcError> {
    let handle = process.raw_handle();
    let pid = process.id();
    let outcome = super::contain_suspended(
        handle.is_some(),
        super::JobObject::new,
        |job| {
            let handle = handle.ok_or_else(|| invalid("missing process handle"))?;
            job.assign(handle)
        },
        |_job| {
            let handle = handle.ok_or_else(|| invalid("missing process handle"))?;
            resume_suspended(handle, pid)
        },
        |job| job.close(),
    );
    if outcome.kill_suspended_child {
        super::hard_kill_direct(process)?;
    }
    match outcome.result {
        Ok(Some(job)) => {
            process.set_job(job);
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) fn send_ctrl_break(process: &mut TrackedProcess) -> Result<bool, ProcError> {
    if !process.group_recorded() {
        return Ok(false);
    }
    if process.try_wait()?.is_some() {
        return Ok(false);
    }
    let Some(handle) = process.raw_handle() else {
        return Ok(false);
    };
    match handle_is_running(handle, true) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => return Ok(false),
    }
    let pid = process.id();
    // SAFETY: `pid` is the leader of a console group this library created, and
    // the handle check above showed that leader is still running.
    let ok = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid) };
    Ok(ok != 0)
}

pub(crate) fn terminate_completed(process: &mut TrackedProcess) -> Result<bool, ProcError> {
    if process.try_wait()?.is_some() {
        return Ok(false);
    }
    let Some(handle) = process.raw_handle() else {
        return Err(invalid(
            "completed-turn termination requires an owned process handle",
        ));
    };
    if let Err(error) = terminate_process(
        handle,
        process.id(),
        super::WINDOWS_COMPLETED_TURN_EXIT_CODE,
    ) {
        if process.try_wait()?.is_some() {
            return Ok(false);
        }
        return Err(error);
    }
    Ok(true)
}

struct RegKey(HKEY);

impl Drop for RegKey {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: the key was opened by RegOpenKeyExW and is closed once.
            unsafe {
                RegCloseKey(self.0);
            }
        }
    }
}

fn query(
    job: RawHandle,
    class: i32,
    buffer: *mut core::ffi::c_void,
    size: u32,
) -> Result<(), ProcError> {
    // SAFETY: `buffer` points at a live structure of `size` bytes. A null return
    // length is accepted by QueryInformationJobObject.
    let ok = unsafe { QueryInformationJobObject(job, class, buffer, size, ptr::null_mut()) };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, "QueryInformationJobObject"));
    }
    Ok(())
}

fn open_running(pid: u32) -> Result<OwnedHandle, ProcError> {
    let (handle, synchronize) =
        match open_process(pid, PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE) {
            Ok(handle) => (handle, true),
            Err(error) if error.is_permission() => {
                (open_process(pid, PROCESS_QUERY_LIMITED_INFORMATION)?, false)
            }
            Err(error) => return Err(error),
        };
    match handle_is_running(handle.as_raw_handle(), synchronize) {
        Ok(true) => Ok(handle),
        Ok(false) => Err(not_found(format!("process {pid} has exited"))),
        Err(error) => Err(error),
    }
}

fn open_process(pid: u32, access: u32) -> Result<OwnedHandle, ProcError> {
    // SAFETY: `pid` was checked to be a positive process id. A null return owns nothing.
    let handle = unsafe { OpenProcess(access, 0, pid) };
    if handle.is_null() {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, &format!("OpenProcess({pid})")));
    }
    // SAFETY: OpenProcess returned a fresh, exclusively owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

fn handle_is_running(handle: RawHandle, synchronize: bool) -> Result<bool, ProcError> {
    if synchronize {
        // SAFETY: `handle` is an open process handle that includes SYNCHRONIZE.
        let result = unsafe { WaitForSingleObject(handle, 0) };
        if result == WAIT_TIMEOUT {
            return Ok(true);
        }
        if result == WAIT_OBJECT_0 {
            return Ok(false);
        }
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, "WaitForSingleObject"));
    }
    let mut code = 0u32;
    // SAFETY: `code` is a live DWORD and `handle` is open for query.
    let ok = unsafe { GetExitCodeProcess(handle, &mut code) };
    if ok == 0 {
        let error = unsafe { GetLastError() };
        return Err(win32_error(error, "GetExitCodeProcess"));
    }
    Ok(i32::try_from(code).ok() == Some(STILL_ACTIVE))
}

fn creation_time(handle: RawHandle, pid: u32) -> Result<u64, ProcError> {
    let mut creation = windows_sys::Win32::Foundation::FILETIME::default();
    let mut exited = windows_sys::Win32::Foundation::FILETIME::default();
    let mut kernel = windows_sys::Win32::Foundation::FILETIME::default();
    let mut user = windows_sys::Win32::Foundation::FILETIME::default();
    // SAFETY: all four FILETIME outputs are live locals.
    let ok = unsafe { GetProcessTimes(handle, &mut creation, &mut exited, &mut kernel, &mut user) };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, &format!("GetProcessTimes({pid})")));
    }
    Ok(filetime_ticks(
        creation.dwHighDateTime,
        creation.dwLowDateTime,
    ))
}

fn terminate_process(handle: RawHandle, pid: u32, exit_code: u32) -> Result<(), ProcError> {
    // SAFETY: `handle` is open with PROCESS_TERMINATE. The call does not close it.
    let ok = unsafe { TerminateProcess(handle, exit_code) };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        return Err(win32_error(code, &format!("TerminateProcess({pid})")));
    }
    Ok(())
}

fn resume_suspended(handle: RawHandle, pid: u32) -> Result<(), ProcError> {
    // SAFETY: `handle` is the creator's process handle for a CREATE_SUSPENDED child.
    // NtResumeProcess resumes every thread; ResumeThread cannot see that thread.
    let status = unsafe { NtResumeProcess(handle) };
    if status < 0 {
        let bits = u32::from_ne_bytes(status.to_ne_bytes());
        return Err(ProcError::Os(io::Error::other(format!(
            "NtResumeProcess({pid}) failed: 0x{bits:08x}"
        ))));
    }
    Ok(())
}

fn dword_size<T>() -> Result<u32, ProcError> {
    u32::try_from(size_of::<T>()).map_err(|_| invalid("Win32 structure is larger than a DWORD"))
}

fn wide(value: &str) -> Vec<u16> {
    OsStr::new(value).encode_wide().chain(Some(0)).collect()
}

struct WinSlot {
    callback: Arc<dyn Fn() + Send + Sync>,
    runtime: tokio::runtime::Handle,
    previous: usize,
}

fn slots() -> &'static Mutex<HashMap<i32, WinSlot>> {
    static SLOTS: OnceLock<Mutex<HashMap<i32, WinSlot>>> = OnceLock::new();
    SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

extern "C" fn c_signal_handler(sig: i32) {
    let scheduled = {
        let guard = slots().lock().unwrap_or_else(|error| error.into_inner());
        guard.get(&sig).map(|slot| {
            // The CRT resets the handler to SIG_DFL before calling it. Put ours
            // back while the registration is still in the map.
            install_c_handler(sig);
            (Arc::clone(&slot.callback), slot.runtime.clone())
        })
    };
    if let Some((callback, runtime)) = scheduled {
        drop(runtime.spawn(async move {
            callback();
        }));
    }
}

fn install_c_handler(sig: i32) -> usize {
    // libc's Windows `signal` stores the handler as `usize`.
    let handler = c_signal_handler as *const () as usize;
    // SAFETY: `handler` is the CRT callback installed for `sig`.
    unsafe { libc::signal(sig, handler) }
}

pub(crate) fn add_loop_signal_handler(
    sig: i32,
    callback: impl Fn() + Send + Sync + 'static,
) -> Result<(), ProcError> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| invalid("add_loop_signal_handler must be called from a running async loop"))?;
    let _ = remove_loop_signal_handler(sig);
    let previous = install_c_handler(sig);
    if previous == libc::SIG_ERR as usize {
        return Err(ProcError::Os(io::Error::last_os_error()));
    }
    let mut guard = slots().lock().unwrap_or_else(|error| error.into_inner());
    guard.insert(
        sig,
        WinSlot {
            callback: Arc::new(callback),
            runtime,
            previous,
        },
    );
    Ok(())
}

pub(crate) fn remove_loop_signal_handler(sig: i32) -> Result<(), ProcError> {
    let previous = {
        let mut guard = slots().lock().unwrap_or_else(|error| error.into_inner());
        guard.remove(&sig).map(|slot| slot.previous)
    };
    if let Some(previous) = previous {
        // SAFETY: `previous` is the handler `signal` returned when this registration
        // was installed. Restoring it undoes that call.
        unsafe {
            libc::signal(sig, previous);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn raise_signal(sig: i32) -> Result<(), ProcError> {
    // SAFETY: `raise` delivers `sig` to this process through the CRT.
    let rc = unsafe { libc::raise(sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(ProcError::Os(io::Error::last_os_error()))
    }
}
