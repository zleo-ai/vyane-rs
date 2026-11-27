#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(windows)]
use std::process::Stdio;
use std::time::{Duration, Instant};

use super::{
    CTRL_BREAK_EVENT, CTRL_C_EVENT, ExactKill, JobObject, ProcError, SIGINT, SIGKILL, SIGTERM,
    SpawnKwargs, StdioMode, TREE_KILLED_EXIT_CODE, TrackedProcess, UnsupportedOnWindows,
    WINDOWS_COMPLETED_TURN_EXIT_CODE, WindowsSignalAction, add_loop_signal_handler,
    available_signals, boot_id, checked_windows_pid, contain_process, contain_suspended,
    contained_spawn_kwargs, create_subprocess_exec_contained, exact_kill_decision, filetime_ticks,
    format_windows_boot_id, job_activity, kill_exact_process, kill_process_tree, pid_running,
    popen_contained, probe_pid, process_job, remove_loop_signal_handler, running_from_probe,
    signal_process_tree, terminate_completed_turn, terminate_process_tree, win32_error,
    windows_process_start_time, windows_signal_action,
};

#[test]
fn constants_match_the_python_contract() {
    assert_eq!(WINDOWS_COMPLETED_TURN_EXIT_CODE, 3_758_102_595);
    assert_eq!(CTRL_C_EVENT, 0);
    assert_eq!(CTRL_BREAK_EVENT, 1);
    assert_eq!(super::IS_WINDOWS, crate::IS_WINDOWS);
    #[cfg(unix)]
    {
        assert_eq!(SIGKILL, rustix::process::Signal::KILL.as_raw());
        assert_eq!(SIGINT, rustix::process::Signal::INT.as_raw());
        assert_eq!(SIGTERM, rustix::process::Signal::TERM.as_raw());
        assert_eq!(TREE_KILLED_EXIT_CODE, -SIGKILL);
    }
    #[cfg(windows)]
    {
        assert_eq!(SIGKILL, 9);
        assert_eq!(SIGINT, 2);
        assert_eq!(SIGTERM, 15);
        assert_eq!(TREE_KILLED_EXIT_CODE, 137);
        assert_ne!(SIGKILL, SIGTERM);
    }
}

#[test]
fn available_signals_drops_unknown_names() {
    let found = available_signals(&["SIGTERM", "SIGHUP", "SIGNOPE"]);
    #[cfg(unix)]
    {
        assert_eq!(found, vec![SIGTERM, rustix::process::Signal::HUP.as_raw()]);
        assert!(available_signals(&["SIGKILL"]).contains(&SIGKILL));
    }
    #[cfg(windows)]
    {
        assert_eq!(found, vec![SIGTERM]);
        assert!(available_signals(&["SIGKILL", "SIGSTOP", "SIGHUP"]).is_empty());
        assert_eq!(
            available_signals(&["SIGINT", "SIGBREAK", "CTRL_BREAK_EVENT", "CTRL_C_EVENT"]),
            vec![SIGINT, 21, CTRL_BREAK_EVENT, CTRL_C_EVENT]
        );
    }
}

#[test]
fn permission_denial_counts_as_alive() {
    let denied = super::permission("probe: access denied");
    assert!(running_from_probe(Err(denied)).unwrap());
    let missing = super::not_found("probe: no such process");
    assert!(!running_from_probe(Err(missing)).unwrap());
    assert!(running_from_probe(Ok(())).unwrap());
    let other = ProcError::Os(std::io::Error::other("broken"));
    assert!(running_from_probe(Err(other)).is_err());
}

#[test]
fn unsupported_is_not_an_os_error() {
    let error = ProcError::from(UnsupportedOnWindows::new(
        "Job Objects exist only on Windows",
    ));
    assert!(error.is_unsupported());
    assert!(!error.is_process_lookup());
    assert!(!error.is_permission());
    assert!(!matches!(error, ProcError::Os(_)));
    assert_eq!(error.to_string(), "Job Objects exist only on Windows");
}

#[test]
fn windows_signal_actions_reject_stop_and_ctrl_c() {
    assert_eq!(windows_signal_action(SIGKILL), WindowsSignalAction::Kill);
    assert_eq!(
        windows_signal_action(SIGTERM),
        WindowsSignalAction::Terminate
    );
    assert_eq!(
        windows_signal_action(SIGINT),
        WindowsSignalAction::CtrlBreak
    );
    assert_eq!(
        windows_signal_action(CTRL_BREAK_EVENT),
        WindowsSignalAction::CtrlBreak
    );
    assert_eq!(
        windows_signal_action(CTRL_C_EVENT),
        WindowsSignalAction::Unsupported
    );
    assert_eq!(windows_signal_action(18), WindowsSignalAction::Unsupported);
    assert_eq!(windows_signal_action(19), WindowsSignalAction::Unsupported);
}

#[test]
fn windows_pid_and_identity_decisions() {
    assert!(checked_windows_pid(0).unwrap_err().is_process_lookup());
    assert!(checked_windows_pid(-1).unwrap_err().is_process_lookup());
    assert_eq!(
        checked_windows_pid(0).unwrap_err().to_string(),
        "invalid pid 0"
    );
    assert_eq!(checked_windows_pid(12).unwrap(), 12);

    assert_eq!(exact_kill_decision(false, 5, 5), ExactKill::SpareGone);
    assert_eq!(exact_kill_decision(true, 6, 5), ExactKill::SpareReused);
    assert_eq!(exact_kill_decision(true, 5, 5), ExactKill::Kill);

    assert_eq!(filetime_ticks(0x1, 0x2), (1_u64 << 32) | 2);
    assert_eq!(
        format_windows_boot_id(4, 9, 4).as_deref(),
        Some("windows-boot-9")
    );
    assert_eq!(format_windows_boot_id(1, 9, 4), None);
    assert_eq!(format_windows_boot_id(4, 9, 8), None);

    let (cpu, bytes) = job_activity(10_000_000, 0, 3, 4);
    assert_eq!(cpu, 1.0);
    assert_eq!(bytes, 7);

    let missing = win32_error(87, "OpenProcess(4)");
    assert!(missing.is_process_lookup());
    assert!(missing.to_string().contains("no such process"));
    let denied = win32_error(5, "OpenProcess(4)");
    assert!(denied.is_permission());
    assert!(denied.to_string().contains("access denied"));
    let other = win32_error(1, "OpenProcess(4)");
    assert!(!other.is_process_lookup());
    assert!(!other.is_permission());
    assert!(!other.is_unsupported());
}

#[test]
fn failed_containment_kills_only_a_real_handle() {
    let closed = std::cell::Cell::new(false);
    let untouched = contain_suspended(
        false,
        || Ok::<u8, &str>(1),
        |_| Ok(()),
        |_| Ok(()),
        |_| closed.set(true),
    );
    assert!(untouched.result.unwrap().is_none());
    assert!(!untouched.kill_suspended_child);
    assert!(!closed.get());

    let created = contain_suspended(
        true,
        || Err::<(), &str>("create"),
        |_| Ok(()),
        |_| Ok(()),
        |_| {},
    );
    assert_eq!(created.result.unwrap_err(), "create");
    assert!(created.kill_suspended_child);

    let closed = std::cell::Cell::new(false);
    let assigned = contain_suspended(
        true,
        || Ok(()),
        |_| Err("assign"),
        |_| Ok(()),
        |_| closed.set(true),
    );
    assert_eq!(assigned.result.unwrap_err(), "assign");
    assert!(assigned.kill_suspended_child);
    assert!(closed.get());

    let closed = std::cell::Cell::new(false);
    let resumed = contain_suspended(
        true,
        || Ok(7_u8),
        |_| Ok(()),
        |_| Err("resume"),
        |_| closed.set(true),
    );
    assert_eq!(resumed.result.unwrap_err(), "resume");
    assert!(resumed.kill_suspended_child);
    assert!(closed.get());

    let closed = std::cell::Cell::new(false);
    let held: super::Containment<u8, &str> =
        contain_suspended(true, || Ok(7), |_| Ok(()), |_| Ok(()), |_| closed.set(true));
    assert_eq!(held.result.unwrap(), Some(7));
    assert!(!held.kill_suspended_child);
    assert!(!closed.get());
}

#[test]
fn contained_kwargs_are_idempotent_and_keep_the_callers_fields() {
    let kwargs = SpawnKwargs {
        env: Some(BTreeMap::new()),
        cwd: Some(PathBuf::from("work")),
        creationflags: Some(0x10),
        process_group: Some(7),
        start_new_session: Some(false),
        ..SpawnKwargs::default()
    };
    let once = contained_spawn_kwargs(kwargs);
    let twice = contained_spawn_kwargs(once.clone());
    assert_eq!(once, twice);
    assert_eq!(once.env.as_ref().map(BTreeMap::len), Some(0));
    assert_eq!(once.cwd.as_deref(), Some(Path::new("work")));
    #[cfg(unix)]
    {
        assert_eq!(once.start_new_session, Some(true));
        assert_eq!(once.process_group, Some(7));
        assert_eq!(once.creationflags, Some(0x10));
    }
    #[cfg(windows)]
    {
        assert_eq!(once.start_new_session, None);
        assert_eq!(once.process_group, None);
        assert_eq!(once.creationflags, Some(0x10 | 4 | 0x200));
    }
    let merged = super::merge_creationflags(
        SpawnKwargs {
            start_new_session: Some(true),
            process_group: Some(4),
            creationflags: Some(0x10),
            ..SpawnKwargs::default()
        },
        super::CREATE_SUSPENDED | super::CREATE_NEW_PROCESS_GROUP,
    );
    assert_eq!(merged.start_new_session, None);
    assert_eq!(merged.process_group, None);
    assert_eq!(merged.creationflags, Some(0x10 | 4 | 0x200));
    assert_eq!(
        super::merge_creationflags(
            merged.clone(),
            super::CREATE_SUSPENDED | super::CREATE_NEW_PROCESS_GROUP,
        ),
        merged
    );
}

#[test]
fn current_process_is_running_and_a_missing_pid_is_not() {
    assert!(pid_running(i64::from(std::process::id())).unwrap());
    assert!(!pid_running(i64::from(i32::MAX)).unwrap());
}

#[test]
fn test_double_is_not_contained_by_pid() {
    let mut process = TrackedProcess::manual(std::process::id());
    contain_process(&mut process).unwrap();
    assert!(process_job(&process).is_none());
    assert!(pid_running(i64::from(std::process::id())).unwrap());
    #[cfg(windows)]
    {
        kill_process_tree(&mut process).unwrap();
        assert!(process.manual_killed());
        assert!(pid_running(i64::from(std::process::id())).unwrap());
    }
}

#[test]
fn signal_handler_requires_a_runtime() {
    let error = add_loop_signal_handler(SIGINT, || {}).unwrap_err();
    assert!(error.to_string().contains("running async loop"));
}

#[cfg(not(windows))]
#[test]
fn posix_only_helpers_reject_windows_calls_without_signalling() {
    let job = JobObject::new().unwrap_err();
    assert!(job.is_unsupported());
    assert_eq!(job.to_string(), "Job Objects exist only on Windows");
    assert!(windows_process_start_time(1).unwrap_err().is_unsupported());
    assert!(kill_exact_process(1, 0).unwrap_err().is_unsupported());
    assert!(boot_id().unwrap_err().is_unsupported());
    assert!(pid_running(i64::from(std::process::id())).unwrap());
}

#[cfg(unix)]
#[test]
fn posix_probe_accepts_the_current_group_and_a_negative_pid() {
    probe_pid(0).unwrap();
    if let Err(error) = probe_pid(-1) {
        assert!(!error.to_string().contains("invalid pid"), "{error}");
    }
}

#[cfg(unix)]
#[test]
fn posix_tree_helpers_are_killpg() {
    let mut process = TrackedProcess::manual(i32::MAX as u32);
    let killed = kill_process_tree(&mut process).unwrap_err();
    assert!(killed.is_process_lookup(), "{killed}");
    assert!(!process.manual_killed());
    let terminated = terminate_process_tree(&mut process).unwrap_err();
    assert!(terminated.is_process_lookup(), "{terminated}");
    assert!(!process.manual_terminated());
    let signalled = signal_process_tree(&mut process, SIGINT).unwrap_err();
    assert!(signalled.is_process_lookup(), "{signalled}");
}

#[cfg(unix)]
#[test]
fn manual_terminate_does_not_signal_its_pid() {
    let mut process = TrackedProcess::manual(std::process::id());
    assert!(terminate_completed_turn(&mut process).unwrap());
    assert!(process.manual_terminated());
    assert!(pid_running(i64::from(std::process::id())).unwrap());
}

#[cfg(unix)]
#[test]
fn conflicting_posix_spawn_fields_are_rejected_before_spawn() {
    let grouped = SpawnKwargs {
        process_group: Some(0),
        ..SpawnKwargs::default()
    };
    match popen_contained("sleep", ["30"], grouped) {
        Ok(mut child) => {
            let _ = kill_process_tree(&mut child);
            let _ = child.wait();
            panic!("process_group and start_new_session were both accepted");
        }
        Err(error) => assert!(error.to_string().contains("mutually exclusive"), "{error}"),
    }

    let flagged = SpawnKwargs {
        creationflags: Some(4),
        ..SpawnKwargs::default()
    };
    match popen_contained("sleep", ["30"], flagged) {
        Ok(mut child) => {
            let _ = kill_process_tree(&mut child);
            let _ = child.wait();
            panic!("creationflags were accepted on POSIX");
        }
        Err(error) => assert!(error.to_string().contains("creationflags"), "{error}"),
    }
}

#[test]
fn liveness_probe_sends_no_signal() {
    let dir = tempfile::tempdir().unwrap();
    let (program, args) = sleeper();
    let mut child = Reap::contained(program, args, dir.path()).unwrap();
    for _ in 0..3 {
        probe_pid(i64::from(child.id())).unwrap();
        assert!(pid_running(i64::from(child.id())).unwrap());
        assert!(child.try_wait().unwrap().is_none());
    }
}

#[cfg(unix)]
#[test]
fn reaped_child_is_gone_and_a_zombie_still_counts() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::contained("sleep", &["30"], dir.path()).unwrap();
    let pid = i64::from(child.id());
    child.kill_direct().unwrap();
    assert!(pid_running(pid).unwrap());
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(SIGKILL));
    assert!(!pid_running(pid).unwrap());
}

#[cfg(unix)]
#[test]
fn contained_child_is_its_own_session() {
    let dir = tempfile::tempdir().unwrap();
    let child = Reap::contained("sleep", &["30"], dir.path()).unwrap();
    let sid = ps_field(child.id(), "sess=").unwrap();
    assert_eq!(sid, child.id().to_string());
}

#[cfg(unix)]
#[test]
fn contained_tree_can_be_killed() {
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "tree.sh",
        "target=$1\npidfile=$2\n(\n  while true; do\n    printf x >> \"$target\"\n    sleep 0.2\n  done\n) &\nprintf '%s\\n' \"$!\" > \"$pidfile\"\nwait\n",
    );
    let heartbeat = dir.path().join("heartbeat");
    let pidfile = dir.path().join("grandchild");
    let mut child = Reap::contained(
        "sh",
        &[
            script.to_str().unwrap(),
            heartbeat.to_str().unwrap(),
            pidfile.to_str().unwrap(),
        ],
        dir.path(),
    )
    .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        std::fs::read(&heartbeat)
            .map(|bytes| !bytes.is_empty())
            .unwrap_or(false)
            && std::fs::read_to_string(&pidfile)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
                .is_some()
    }));
    let grandchild: u32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_ne!(grandchild, child.id());
    let sid = ps_field(child.id(), "sess=").unwrap();
    assert_eq!(sid, child.id().to_string());
    let before = std::fs::read(&heartbeat).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let mid = std::fs::read(&heartbeat).unwrap();
    assert!(mid.len() > before.len(), "heartbeat was not growing");
    signal_process_tree(&mut child, SIGKILL).unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(SIGKILL));
    std::thread::sleep(Duration::from_millis(600));
    let after = std::fs::read(&heartbeat).unwrap();
    std::thread::sleep(Duration::from_millis(600));
    let later = std::fs::read(&heartbeat).unwrap();
    assert_eq!(
        after, later,
        "heartbeat kept growing after the tree was killed"
    );
}

#[cfg(unix)]
#[test]
fn posix_stop_is_delivered_and_can_continue() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::contained("sleep", &["30"], dir.path()).unwrap();
    let stop = rustix::process::Signal::STOP.as_raw();
    let cont = rustix::process::Signal::CONT.as_raw();
    signal_process_tree(&mut child, stop).unwrap();
    assert!(wait_until(Duration::from_secs(3), || {
        ps_field(child.id(), "state=")
            .unwrap_or_default()
            .contains('T')
    }));
    signal_process_tree(&mut child, cont).unwrap();
    assert!(wait_until(Duration::from_secs(3), || {
        ps_field(child.id(), "state=")
            .ok()
            .is_some_and(|state| !state.contains('T'))
    }));
}

#[cfg(unix)]
#[test]
fn terminate_completed_turn_sends_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::contained("sleep", &["30"], dir.path()).unwrap();
    assert!(terminate_completed_turn(&mut child).unwrap());
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(SIGTERM));
    assert!(!terminate_completed_turn(&mut child).unwrap());
}

#[cfg(unix)]
#[test]
fn sigint_is_delivered_to_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::contained("sleep", &["30"], dir.path()).unwrap();
    signal_process_tree(&mut child, SIGINT).unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(SIGINT));
}

#[cfg(windows)]
#[test]
fn windows_probe_rejects_group_pids() {
    for pid in [0_i64, -1] {
        let error = probe_pid(pid).unwrap_err();
        assert!(error.is_process_lookup(), "{error}");
        assert!(error.to_string().contains("invalid pid"), "{error}");
    }
}

#[cfg(windows)]
#[test]
fn empty_job_object_opens_and_closes() {
    let job = JobObject::new().unwrap();
    assert!(!job.closed());
    assert_eq!(job.active_processes().unwrap(), 0);
    let (cpu, bytes) = job.activity().unwrap();
    assert!(cpu >= 0.0);
    assert_eq!(bytes, 0);
    job.terminate().unwrap();
    job.close();
    assert!(job.closed());
    assert!(job.active_processes().is_err());
}

#[cfg(windows)]
#[test]
fn dropping_the_job_kills_the_child() {
    let dir = tempfile::tempdir().unwrap();
    let (program, args) = sleeper();
    let child = popen_contained(program, args.iter().copied(), quiet(dir.path())).unwrap();
    let pid = i64::from(child.id());
    assert!(pid_running(pid).unwrap());
    drop(child);
    assert!(
        wait_until(Duration::from_secs(5), || {
            !pid_running(pid).unwrap_or(true)
        }),
        "closing the job left the child running"
    );
}

#[cfg(windows)]
#[test]
fn stop_signals_are_rejected_and_the_child_stays_alive() {
    let dir = tempfile::tempdir().unwrap();
    let (program, args) = sleeper();
    let mut child = Reap::contained(program, args, dir.path()).unwrap();
    for sig in [19_i32, 18] {
        let error = signal_process_tree(&mut child, sig).unwrap_err();
        assert!(error.is_unsupported(), "{error}");
        assert!(child.try_wait().unwrap().is_none());
    }
}

#[cfg(windows)]
#[test]
fn ctrl_break_is_not_sent_to_an_uncontained_group() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::new(uncontained_sleeper(dir.path()));
    let error = signal_process_tree(&mut child, SIGINT).unwrap_err();
    assert!(error.is_process_lookup(), "{error}");
    assert!(child.try_wait().unwrap().is_none());
    terminate_process_tree(&mut child).unwrap();
    let status = child.wait().unwrap();
    assert!(exit_bits(status).is_some());
}

#[cfg(windows)]
#[test]
fn contained_tree_can_be_killed() {
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "tree.ps1",
        "$ErrorActionPreference = 'Stop'\n\
         $pidfile = $args[0]\n\
         $ping = Join-Path $env:SystemRoot 'System32\\ping.exe'\n\
         $psi = New-Object System.Diagnostics.ProcessStartInfo\n\
         $psi.FileName = $ping\n\
         $psi.Arguments = '-n 30 127.0.0.1'\n\
         $psi.UseShellExecute = $false\n\
         $psi.CreateNoWindow = $true\n\
         $child = [System.Diagnostics.Process]::Start($psi)\n\
         Set-Content -LiteralPath $pidfile -Value $child.Id -NoNewline\n\
         $child.WaitForExit()\n",
    );
    let pidfile = dir.path().join("grandchild");
    let mut child = Reap::contained(
        "powershell",
        &[
            "-NoProfile",
            "-NonInteractive",
            "-File",
            script.to_str().unwrap(),
            pidfile.to_str().unwrap(),
        ],
        dir.path(),
    )
    .unwrap();
    assert!(
        wait_until(Duration::from_secs(8), || {
            process_job(&child)
                .and_then(|job| job.active_processes().ok())
                .unwrap_or(0)
                >= 2
                && std::fs::read_to_string(&pidfile)
                    .ok()
                    .and_then(|text| text.trim().parse::<u32>().ok())
                    .is_some()
        }),
        "contained tree did not report two live processes"
    );
    let grandchild: u32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    signal_process_tree(&mut child, SIGKILL).unwrap();
    let status = child.wait().unwrap();
    assert_eq!(exit_bits(status), Some(137));
    assert!(
        probe_pid(i64::from(grandchild))
            .unwrap_err()
            .is_process_lookup()
    );
}

#[cfg(windows)]
#[test]
fn closing_the_job_kills_a_grandchild_after_the_leader_exits() {
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "orphan.ps1",
        "$ErrorActionPreference = 'Stop'\n\
         $pidfile = $args[0]\n\
         $ping = Join-Path $env:SystemRoot 'System32\\ping.exe'\n\
         $psi = New-Object System.Diagnostics.ProcessStartInfo\n\
         $psi.FileName = $ping\n\
         $psi.Arguments = '-n 30 127.0.0.1'\n\
         $psi.UseShellExecute = $false\n\
         $psi.CreateNoWindow = $true\n\
         $child = [System.Diagnostics.Process]::Start($psi)\n\
         Set-Content -LiteralPath $pidfile -Value $child.Id -NoNewline\n\
         exit 0\n",
    );
    let pidfile = dir.path().join("grandchild");
    let mut child = Reap::contained(
        "powershell",
        &[
            "-NoProfile",
            "-NonInteractive",
            "-File",
            script.to_str().unwrap(),
            pidfile.to_str().unwrap(),
        ],
        dir.path(),
    )
    .unwrap();
    assert!(wait_until(Duration::from_secs(8), || {
        child.try_wait().unwrap().is_some()
            && std::fs::read_to_string(&pidfile)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
                .is_some()
    }));
    let status = child.wait().unwrap();
    assert!(status.success(), "{status}");
    let grandchild: u32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(probe_pid(i64::from(grandchild)).is_ok());
    process_job(&child).unwrap().close();
    assert!(
        wait_until(Duration::from_secs(5), || {
            probe_pid(i64::from(grandchild)).is_err()
        }),
        "closing the job left the grandchild running"
    );
}

#[cfg(windows)]
#[test]
fn failed_containment_kills_the_suspended_child() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        super::win::fail_assign(true);
        let dir = tempfile::tempdir().expect("tempdir");
        let result = popen_contained("ping", ["-n", "30", "127.0.0.1"], quiet(dir.path()));
        super::win::fail_assign(false);
        let _ = tx.send(result.err().map(|error| error.to_string()));
    });
    match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(Some(error)) => assert!(
            error.contains("access denied") || error.contains("assign"),
            "{error}"
        ),
        Ok(None) => panic!("contained spawn succeeded despite a refused job assignment"),
        Err(_) => panic!("contained spawn hung; the suspended child was not killed"),
    }
}

#[cfg(windows)]
#[test]
fn kill_exact_process_spares_a_reused_creation_time() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::new(uncontained_sleeper(dir.path()));
    let pid = i64::from(child.id());
    let start = windows_process_start_time(pid).unwrap();
    assert_eq!(windows_process_start_time(pid).unwrap(), start);
    let parent = windows_process_start_time(i64::from(std::process::id())).unwrap();
    assert!(start > parent);
    assert!(!kill_exact_process(pid, start.wrapping_add(1)).unwrap());
    assert!(child.try_wait().unwrap().is_none());
    assert!(kill_exact_process(pid, start).unwrap());
    let status = child.wait().unwrap();
    assert_eq!(exit_bits(status), Some(137));
    assert!(!kill_exact_process(pid, start).unwrap());
}

#[cfg(windows)]
#[test]
fn boot_id_uses_the_registry_counter_or_reports_it_missing() {
    match boot_id().unwrap() {
        None => {}
        Some(value) => assert!(value.starts_with("windows-boot-"), "{value}"),
    }
}

#[cfg(windows)]
#[test]
fn completed_turn_uses_the_dedicated_status() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Reap::contained("ping", &["-n", "30", "127.0.0.1"], dir.path()).unwrap();
    assert!(terminate_completed_turn(&mut child).unwrap());
    let status = child.wait().unwrap();
    assert_eq!(exit_bits(status), Some(WINDOWS_COMPLETED_TURN_EXIT_CODE));
    assert!(!terminate_completed_turn(&mut child).unwrap());
}

#[cfg(windows)]
#[test]
fn manual_completed_turn_requires_a_handle() {
    let mut process = TrackedProcess::manual(std::process::id());
    let error = terminate_completed_turn(&mut process).unwrap_err();
    assert!(
        matches!(error, ProcError::Invalid(ref message) if message.contains("owned process handle")),
        "{error}"
    );
    assert!(!process.manual_terminated());
    assert!(pid_running(i64::from(std::process::id())).unwrap());
}

#[tokio::test]
async fn contained_exec_prints_ready() {
    let dir = tempfile::tempdir().unwrap();
    let mut kwargs = quiet(dir.path());
    kwargs.stdout = StdioMode::Piped;
    let program = if cfg!(windows) { "cmd" } else { "echo" };
    let args: Vec<&str> = if cfg!(windows) {
        vec!["/c", "echo ready"]
    } else {
        vec!["ready"]
    };
    let mut child = create_subprocess_exec_contained(program, args, kwargs)
        .await
        .unwrap();
    let mut output = String::new();
    child.stdout().unwrap().read_to_string(&mut output).unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "{status}");
    assert_eq!(output.trim(), "ready");
    #[cfg(unix)]
    assert!(process_job(&child).is_none());
    #[cfg(windows)]
    assert!(process_job(&child).is_some());
}

#[tokio::test]
async fn loop_signal_handler_round_trip() {
    let sig = loop_signal();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    add_loop_signal_handler(sig, move || {
        if let Some(sender) = tx.lock().unwrap_or_else(|error| error.into_inner()).take() {
            let _ = sender.send(());
        }
    })
    .unwrap();
    let _guard = RemoveSignal(sig);
    let mut rx = std::pin::pin!(rx);
    let mut delivered = false;
    for _ in 0..5 {
        raise_loop(sig).unwrap();
        tokio::select! {
            result = &mut rx => {
                result.unwrap();
                delivered = true;
                break;
            }
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    assert!(delivered, "signal handler was not called");
}

#[cfg(unix)]
#[tokio::test]
async fn sigkill_cannot_be_caught() {
    let error = add_loop_signal_handler(SIGKILL, || {}).unwrap_err();
    assert!(!error.is_unsupported(), "{error}");
}

struct Reap {
    process: TrackedProcess,
}

impl Reap {
    fn new(process: TrackedProcess) -> Self {
        Self { process }
    }

    fn contained(program: &str, args: &[&str], dir: &Path) -> Result<Self, ProcError> {
        let process = popen_contained(program, args.iter().copied(), quiet(dir))?;
        Ok(Self::new(process))
    }
}

impl Drop for Reap {
    fn drop(&mut self) {
        if matches!(self.process.try_wait(), Ok(Some(_))) {
            return;
        }
        let _ = kill_process_tree(&mut self.process);
        let _ = self.process.wait();
    }
}

impl std::ops::Deref for Reap {
    type Target = TrackedProcess;

    fn deref(&self) -> &TrackedProcess {
        &self.process
    }
}

impl std::ops::DerefMut for Reap {
    fn deref_mut(&mut self) -> &mut TrackedProcess {
        &mut self.process
    }
}

struct RemoveSignal(i32);

impl Drop for RemoveSignal {
    fn drop(&mut self) {
        let _ = remove_loop_signal_handler(self.0);
    }
}

fn quiet(dir: &Path) -> SpawnKwargs {
    SpawnKwargs {
        cwd: Some(dir.to_path_buf()),
        stdin: StdioMode::Null,
        stdout: StdioMode::Null,
        stderr: StdioMode::Null,
        ..SpawnKwargs::default()
    }
}

fn sleeper() -> (&'static str, &'static [&'static str]) {
    #[cfg(unix)]
    {
        ("sleep", &["30"])
    }
    #[cfg(windows)]
    {
        ("ping", &["-n", "30", "127.0.0.1"])
    }
}

#[cfg(windows)]
fn uncontained_sleeper(dir: &Path) -> TrackedProcess {
    let (program, args) = sleeper();
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    TrackedProcess::from_child(command.spawn().unwrap())
}

fn write_file(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    path
}

fn wait_until(timeout: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    ready()
}

#[cfg(unix)]
/// Read one `ps -o` column.
///
/// The keyword has to exist in both procps and BSD `ps`. The session id is
/// `sess`; `sid` is procps-only and macOS rejects it. Process state is `state`.
fn ps_field(pid: u32, field: &str) -> Result<String, String> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", field])
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "ps failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(unix)]
fn loop_signal() -> i32 {
    rustix::process::Signal::USR1.as_raw()
}

#[cfg(windows)]
fn loop_signal() -> i32 {
    SIGINT
}

fn raise_loop(sig: i32) -> Result<(), ProcError> {
    #[cfg(unix)]
    {
        let signal = rustix::process::Signal::from_named_raw(sig).unwrap();
        rustix::process::kill_process(rustix::process::getpid(), signal).map_err(ProcError::from)
    }
    #[cfg(windows)]
    {
        super::win::raise_signal(sig)
    }
}

#[cfg(windows)]
fn exit_bits(status: std::process::ExitStatus) -> Option<u32> {
    status
        .code()
        .map(|code| u32::from_ne_bytes(code.to_ne_bytes()))
}

#[cfg(unix)]
trait UnixSignal {
    fn signal(&self) -> Option<i32>;
}

#[cfg(unix)]
impl UnixSignal for std::process::ExitStatus {
    fn signal(&self) -> Option<i32> {
        std::os::unix::process::ExitStatusExt::signal(self)
    }
}
