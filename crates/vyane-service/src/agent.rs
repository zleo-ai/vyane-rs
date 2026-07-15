use std::sync::Arc;

use anyhow::{Context, Result};
use vyane_agent::{AgentStore, SqliteAgentStore};
use vyane_broker::{AgentEventProjector, BrokerScope};
use vyane_ledger::EventLog;

use crate::StoragePaths;

/// Explicit owner-bound AgentRun lifecycle-projector construction.
///
/// Ordinary dispatch does not open the AgentRun database or start background
/// work. A daemon or embedding application must opt in and own the projector
/// lifecycle itself. The raw [`AgentStore`] remains encapsulated behind the
/// owner-bound projector; a future execution-control facade must preserve that
/// fixed owner rather than exposing a second caller-selected owner string.
#[derive(Clone)]
pub struct AgentProjectionComponents {
    projector: AgentEventProjector,
}

impl AgentProjectionComponents {
    pub fn open(paths: &StoragePaths, owner: impl Into<String>) -> Result<Self> {
        // Validate owner authority before creating persistent state.
        let scope = BrokerScope::new(owner.into())?;
        let store: Arc<dyn AgentStore> = Arc::new(
            SqliteAgentStore::open(paths.agent_metadata_db_path()).with_context(|| {
                format!(
                    "open AgentRun database {}",
                    paths.agent_metadata_db_path().display()
                )
            })?,
        );
        let projector = AgentEventProjector::new(
            scope,
            Arc::clone(&store),
            EventLog::new(paths.event_log_dir()),
        );
        Ok(Self { projector })
    }

    #[must_use]
    pub fn projector(&self) -> &AgentEventProjector {
        &self.projector
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use chrono::Utc;
    use vyane_agent::{NewAgentRun, NewWorker, RunMode};

    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    #[tokio::test]
    async fn construction_is_explicit_owner_bound_and_projects_body_free_events() {
        let directory = tempfile::tempdir().unwrap();
        let paths = StoragePaths::from_data_dir(directory.path().join("data"));

        assert!(AgentProjectionComponents::open(&paths, "   ").is_err());
        assert!(!paths.agent_metadata_db_path().exists());

        let components = AgentProjectionComponents::open(&paths, "alice").unwrap();
        assert!(paths.agent_metadata_db_path().exists());
        let store = SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap();
        for owner in ["alice", "bob"] {
            store
                .create_root(
                    owner,
                    &NewWorker {
                        id: format!("worker-{owner}"),
                        logical_session_id: None,
                    },
                    &NewAgentRun {
                        id: format!("run-{owner}"),
                        worker_id: format!("worker-{owner}"),
                        task_id: None,
                        trace_id: None,
                        parent_run_id: None,
                        execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
                        mode: RunMode::Autonomous,
                        target_key: "provider/model".into(),
                        prompt_digest: digest('a'),
                        policy_digest: digest('b'),
                        available_at: Utc::now(),
                        timeout_seconds: 60,
                        max_resume_attempts: 0,
                    },
                )
                .unwrap();
        }

        let report = components.projector().project_once(10).await.unwrap();
        assert_eq!(report.projected, 2);
        let events = EventLog::new(paths.event_log_dir())
            .read_after("alice", components.projector().stream_id(), 0, 10)
            .await
            .unwrap();
        assert_eq!(events.events.len(), 2);
        assert!(events.events.iter().all(|event| event.owner == "alice"));
        assert_eq!(
            store
                .unprojected_events("bob", components.projector().projector_id(), 10,)
                .unwrap()
                .items
                .len(),
            2
        );
    }
}
