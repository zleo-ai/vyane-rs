//! Process-wide ownership primitives shared by `vyane serve` and the resident
//! workflow daemon.

use std::path::Path;

use anyhow::{Context as _, Result};
use fs4::fs_std::FileExt as _;

/// Held for the full lifetime of a process that may own in-process tasks.
///
/// Releasing this lock promises that every tracked initializer and execution
/// future has stopped. Callers must therefore keep it alive through their
/// shutdown drain, not merely through listener lifetime.
#[derive(Debug)]
pub(crate) struct TaskSupervisorLock {
    file: std::fs::File,
}

impl Drop for TaskSupervisorLock {
    fn drop(&mut self) {
        // Release explicitly before close so same-process restart tests and the
        // different flock implementations supported by fs4 behave uniformly.
        let _ = fs4::fs_std::FileExt::unlock(&self.file);
    }
}

/// Acquire exclusive ownership of task supervision below one Vyane data root.
pub(crate) fn acquire_task_supervisor_lock(path: &Path) -> Result<TaskSupervisorLock> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("task supervisor lock has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent).map_err(|error| {
        anyhow::anyhow!(
            "create task supervisor directory {}: {error}",
            parent.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                anyhow::anyhow!(
                    "chmod task supervisor directory {}: {error}",
                    parent.display()
                )
            },
        )?;
    }

    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(|error| {
        anyhow::anyhow!("open task supervisor lock {}: {error}", path.display())
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|error| {
                anyhow::anyhow!("chmod task supervisor lock {}: {error}", path.display())
            })?;
    }
    if !file
        .try_lock_exclusive()
        .map_err(|error| anyhow::anyhow!("lock task supervisor {}: {error}", path.display()))?
    {
        return Err(anyhow::anyhow!(
            "another Vyane supervisor already owns task metadata at {}",
            path.display()
        ));
    }
    Ok(TaskSupervisorLock { file })
}

/// Shutdown handlers whose registration has already completed.
///
/// The daemon constructs this value before publishing its descriptor, so a
/// descriptor is never observable before SIGTERM has a graceful receiver.
pub(crate) struct PreparedShutdownSignal {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl PreparedShutdownSignal {
    /// Register every supported shutdown handler synchronously.
    pub(crate) fn install() -> Result<Self> {
        #[cfg(unix)]
        {
            let interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .context("install SIGINT handler")?;
            let terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .context("install SIGTERM handler")?;
            Ok(Self {
                interrupt,
                terminate,
            })
        }

        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    /// Wait until one installed shutdown signal is delivered.
    pub(crate) async fn wait(self) {
        #[cfg(unix)]
        {
            async fn wait_one(mut signal: tokio::signal::unix::Signal) {
                if signal.recv().await.is_none() {
                    std::future::pending::<()>().await;
                }
            }

            tokio::select! {
                _ = wait_one(self.interrupt) => {}
                _ = wait_one(self.terminate) => {}
            }
        }

        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

async fn wait_for_prepared_shutdown(signal: Result<PreparedShutdownSignal>) {
    match signal {
        Ok(signal) => signal.wait().await,
        Err(error) => {
            tracing::error!(error = %error, "failed to install shutdown handlers");
            // Handler installation failure is not a shutdown request. Remaining
            // alive is safer than releasing supervisor ownership immediately.
            std::future::pending::<()>().await;
        }
    }
}

/// Wait for ctrl-c or SIGTERM so foreground and detached supervisors share the
/// same graceful-shutdown contract.
pub(crate) async fn shutdown_signal() {
    wait_for_prepared_shutdown(PreparedShutdownSignal::install()).await;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn lock_is_private_exclusive_and_released_with_owner() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let path = data_dir.join("task-supervisor.lock");
        let first = acquire_task_supervisor_lock(&path).unwrap();
        let error = acquire_task_supervisor_lock(&path).unwrap_err();
        assert!(error.to_string().contains("already owns"));
        drop(first);
        drop(acquire_task_supervisor_lock(&path).unwrap());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn handler_install_failure_is_not_a_shutdown_request() {
        let wait = wait_for_prepared_shutdown(Err(anyhow::anyhow!("synthetic install failure")));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), wait)
                .await
                .is_err()
        );
    }
}
