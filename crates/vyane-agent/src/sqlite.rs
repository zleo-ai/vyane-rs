use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use fs4::fs_std::FileExt as _;
use rusqlite::limits::Limit;
use rusqlite::types::Type;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension as _, Row, Transaction, TransactionBehavior, params,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::model::{
    MAX_COMPLETION_KIND_BYTES, MAX_ID_BYTES, MAX_PUBLICATION_KEY_BYTES, MAX_TIMEOUT_SECONDS,
    MAX_TOPOLOGY_NODES, MAX_TREE_CANCEL_RUNS, validate_digest, validate_limit, validate_opaque_key,
    validate_optional_text, validate_owner, validate_projector, validate_text,
};
use crate::{
    ActiveCompletionPermit, ActiveExecutionPermit, AgentEvent, AgentEventKind, AgentRunRecord,
    AgentStore, AgentStoreError, CancelOutcome, CancelPlan, CancelRequest, CancelTicket,
    ClaimedRun, CompletionPermitSnapshot, ControllerKind, ControllerRef, EnqueueResume,
    ExecutionBackend, ExecutionPermitSnapshot, NativeExecutionScope, NewAgentRun, NewRunCompletion,
    NewWorker, OutboxPage, PreparedRunCompletion, ProjectionDeferReason,
    ProjectionQuarantineReason, RecoveryReason, RecoveryTicket, Result, ResumeSessionProof,
    RunCompletionRecord, RunCompletionStatus, RunFailureCode, RunLease, RunLeaseReceipt,
    RunSettlement, RunState, WorkerLifecycle, WorkerRecord, WorkerTopology,
};

pub const SCHEMA_VERSION: u32 = 4;
const RECORD_SCHEMA: u32 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const DATABASE_CREATE_ATTEMPTS: usize = 128;
#[cfg(unix)]
const DATABASE_CREATE_PREFIX: &str = ".vyane-agent-db-create";
#[cfg(unix)]
static DATABASE_CREATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const SQLITE_VALUE_LIMIT: i32 = 512 * 1024;
const MAX_LEASE_SECONDS: u64 = 24 * 60 * 60;
const MAX_PROJECTION_DEFER_SECONDS: u64 = 24 * 60 * 60;
const MIGRATION_0001: &str = include_str!("../migrations/0001_agent.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_completion.sql");
const MIGRATION_0003: &str = include_str!("../migrations/0003_projection_dispositions.sql");
const MIGRATION_0004: &str = include_str!("../migrations/0004_execution_backend.sql");

const WORKER_COLUMNS: &str = "owner, id, parent_id, logical_session_id, lifecycle, \
    created_at_ms, updated_at_ms, released_at_ms, revision, record_schema";
const QUALIFIED_WORKER_COLUMNS: &str = "worker.owner, worker.id, worker.parent_id, \
    worker.logical_session_id, worker.lifecycle, worker.created_at_ms, worker.updated_at_ms, \
    worker.released_at_ms, worker.revision, worker.record_schema";
const RUN_COLUMNS: &str = "owner, id, worker_id, task_id, trace_id, parent_run_id, \
    resume_of_run_id, state, mode, target_key, prompt_digest, policy_digest, available_at_ms, \
    timeout_ms, max_resume_attempts, resume_attempt, created_at_ms, started_at_ms, \
    updated_at_ms, finished_at_ms, revision, worker_generation, controller_kind, \
    controller_id, controller_fingerprint, lease_owner, lease_expires_at_ms, \
    lease_token_hash, last_heartbeat_at_ms, last_activity_at_ms, failure_code, \
    resume_binding_digest, deadline_at_ms, record_schema, execution_backend";
const RUN_COLUMNS_V3: &str = "owner, id, worker_id, task_id, trace_id, parent_run_id, \
    resume_of_run_id, state, mode, target_key, prompt_digest, policy_digest, available_at_ms, \
    timeout_ms, max_resume_attempts, resume_attempt, created_at_ms, started_at_ms, \
    updated_at_ms, finished_at_ms, revision, worker_generation, controller_kind, \
    controller_id, controller_fingerprint, lease_owner, lease_expires_at_ms, \
    lease_token_hash, last_heartbeat_at_ms, last_activity_at_ms, failure_code, \
    resume_binding_digest, deadline_at_ms, record_schema, \
    'legacy_unassigned' AS execution_backend";
const EVENT_COLUMNS: &str = "sequence, event_id, owner, worker_id, run_id, occurred_at_ms, \
    event_type, worker_revision, run_revision, run_state, worker_lifecycle";
const COMPLETION_COLUMNS: &str = "owner, run_id, worker_id, worker_generation, completion_id, \
    sink_kind, publication_key, content_digest, content_bytes, status, prepared_at_ms, \
    prepared_run_revision, committed_at_ms, committed_run_revision, abandoned_at_ms, \
    abandoned_run_revision, committed_by_operation_id, revision, record_schema, execution_backend";
const COMPLETION_COLUMNS_V3: &str = "owner, run_id, worker_id, worker_generation, completion_id, \
    sink_kind, publication_key, content_digest, content_bytes, status, prepared_at_ms, \
    prepared_run_revision, committed_at_ms, committed_run_revision, abandoned_at_ms, \
    abandoned_run_revision, committed_by_operation_id, revision, record_schema, \
    'legacy_unassigned' AS execution_backend";

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaObject {
    kind: String,
    name: String,
    table_name: String,
    sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlOperationKind {
    Cancel,
    LeaseExpired,
    ExecutionTimedOut,
    CancellationAbandoned,
}

impl ControlOperationKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::LeaseExpired => "lease_expired",
            Self::ExecutionTimedOut => "execution_timed_out",
            Self::CancellationAbandoned => "cancellation_abandoned",
        }
    }

    fn recovery_reason(self) -> Result<RecoveryReason> {
        match self {
            Self::LeaseExpired => Ok(RecoveryReason::LeaseExpired),
            Self::ExecutionTimedOut => Ok(RecoveryReason::ExecutionTimedOut),
            Self::CancellationAbandoned => Ok(RecoveryReason::CancellationAbandoned),
            Self::Cancel => Err(AgentStoreError::CorruptData(
                "cancel operation cannot be used as a recovery ticket".into(),
            )),
        }
    }

    const fn from_recovery(reason: RecoveryReason) -> Self {
        match reason {
            RecoveryReason::LeaseExpired => Self::LeaseExpired,
            RecoveryReason::ExecutionTimedOut => Self::ExecutionTimedOut,
            RecoveryReason::CancellationAbandoned => Self::CancellationAbandoned,
        }
    }
}

impl FromStr for ControlOperationKind {
    type Err = AgentStoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "cancel" => Ok(Self::Cancel),
            "lease_expired" => Ok(Self::LeaseExpired),
            "execution_timed_out" => Ok(Self::ExecutionTimedOut),
            "cancellation_abandoned" => Ok(Self::CancellationAbandoned),
            _ => Err(AgentStoreError::CorruptData(
                "unknown run control operation kind".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveControlOperation {
    operation_id: String,
    kind: ControlOperationKind,
    generation: u64,
    revision: u64,
    controller: Option<ControllerRef>,
    token_hash: String,
    lease_owner: String,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CancelTreeHeader {
    root_worker_id: String,
    plan_digest: String,
    worker_count: usize,
    run_count: usize,
    lease_owner: String,
    lease_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelTreeRunAction {
    QueuedCancel,
    ControllerCancel,
}

impl CancelTreeRunAction {
    const fn as_str(self) -> &'static str {
        match self {
            Self::QueuedCancel => "queued_cancel",
            Self::ControllerCancel => "controller_cancel",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CancelTreeRunEntry {
    worker_id: String,
    run_id: String,
    action: CancelTreeRunAction,
}

#[derive(Debug)]
enum PreparedCancelRun {
    Queued {
        before: Box<AgentRunRecord>,
        after: Box<AgentRunRecord>,
    },
    Reuse(CancelTicket),
    Controller {
        before: Box<AgentRunRecord>,
        after: Box<AgentRunRecord>,
        superseded_operation_id: Option<String>,
        operation: ActiveControlOperation,
        token: String,
    },
}

#[derive(Debug)]
struct PreparedCancelWorker {
    before: WorkerRecord,
    after: WorkerRecord,
    changed: bool,
    runs: Vec<PreparedCancelRun>,
}

static EXPECTED_SCHEMA: OnceLock<std::result::Result<Vec<SchemaObject>, String>> = OnceLock::new();
static EXPECTED_SCHEMA_V1: OnceLock<std::result::Result<Vec<SchemaObject>, String>> =
    OnceLock::new();
static EXPECTED_SCHEMA_V2: OnceLock<std::result::Result<Vec<SchemaObject>, String>> =
    OnceLock::new();
static EXPECTED_SCHEMA_V3: OnceLock<std::result::Result<Vec<SchemaObject>, String>> =
    OnceLock::new();

/// Store-owned clock. Production lifecycle timestamps are never caller supplied.
pub trait AgentClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemAgentClock;

impl AgentClock for SystemAgentClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct SqliteAgentStore {
    path: PathBuf,
    clock: Arc<dyn AgentClock>,
}

impl std::fmt::Debug for SqliteAgentStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteAgentStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

struct WriteTransaction<'connection> {
    transaction: Transaction<'connection>,
    lock: File,
}

impl<'connection> WriteTransaction<'connection> {
    fn transaction(&self) -> &Transaction<'connection> {
        &self.transaction
    }

    fn commit(self) -> Result<()> {
        let Self { transaction, lock } = self;
        let result = transaction.commit().map_err(AgentStoreError::from);
        drop(lock);
        result
    }
}

impl SqliteAgentStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_clock(path, Arc::new(SystemAgentClock))
    }

    pub fn open_with_clock(path: impl Into<PathBuf>, clock: Arc<dyn AgentClock>) -> Result<Self> {
        let store = Self {
            path: path.into(),
            clock,
        };
        store.initialize()?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn audit_integrity(&self) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = self.begin_locked_transaction(&mut connection)?;
        audit_database_integrity(transaction.transaction())?;
        transaction.commit()
    }

    fn now(&self) -> Result<DateTime<Utc>> {
        normalize_timestamp(self.clock.now())
    }

    fn operation_now(&self, connection: &Connection, owner: &str) -> Result<DateTime<Utc>> {
        let observed = self.now()?;
        let latest: Option<i64> = connection.query_row(
            "SELECT MAX(occurred_at_ms) FROM agent_events WHERE owner = ?1",
            params![owner],
            |row| row.get(0),
        )?;
        let latest = latest
            .map(|value| {
                DateTime::from_timestamp_millis(value).ok_or_else(|| {
                    AgentStoreError::CorruptData("latest owner event timestamp is invalid".into())
                })
            })
            .transpose()?;
        Ok(latest.map_or(observed, |value| observed.max(value)))
    }

    fn initialize(&self) -> Result<()> {
        prepare_database_path(&self.path)?;
        let mut connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        let found = user_version(&connection)?;
        if found > SCHEMA_VERSION {
            return Err(AgentStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        connection.pragma_update(None, "journal_mode", "WAL")?;
        configure_connection(&connection)?;
        validate_database_files(&self.path)?;
        let transaction = self.begin_locked_transaction(&mut connection)?;
        let found = user_version(transaction.transaction())?;
        if found > SCHEMA_VERSION {
            return Err(AgentStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        if found == 0 {
            transaction.transaction().execute_batch(MIGRATION_0001)?;
            transaction.transaction().execute_batch(MIGRATION_0002)?;
            transaction.transaction().execute_batch(MIGRATION_0003)?;
            transaction.transaction().execute_batch(MIGRATION_0004)?;
            transaction
                .transaction()
                .pragma_update(None, "user_version", SCHEMA_VERSION)?;
        } else if found == 1 {
            validate_schema_definition_v1(transaction.transaction())?;
            audit_database_integrity_v1(transaction.transaction())?;
            transaction.transaction().execute_batch(MIGRATION_0002)?;
            transaction.transaction().execute_batch(MIGRATION_0003)?;
            transaction.transaction().execute_batch(MIGRATION_0004)?;
            transaction
                .transaction()
                .pragma_update(None, "user_version", SCHEMA_VERSION)?;
        } else if found == 2 {
            validate_schema_definition_v2(transaction.transaction())?;
            audit_database_integrity_v2(transaction.transaction())?;
            transaction.transaction().execute_batch(MIGRATION_0003)?;
            transaction.transaction().execute_batch(MIGRATION_0004)?;
            transaction
                .transaction()
                .pragma_update(None, "user_version", SCHEMA_VERSION)?;
        } else if found == 3 {
            validate_schema_definition_v3(transaction.transaction())?;
            audit_database_integrity_v3(transaction.transaction())?;
            transaction.transaction().execute_batch(MIGRATION_0004)?;
            transaction
                .transaction()
                .pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        validate_schema_definition(transaction.transaction())?;
        audit_database_integrity(transaction.transaction())?;
        validate_database_files(&self.path)?;
        transaction.commit()
    }

    fn connection(&self) -> Result<Connection> {
        let connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        configure_connection(&connection)?;
        let found = user_version(&connection)?;
        if found != SCHEMA_VERSION {
            return Err(AgentStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        validate_schema_definition(&connection)?;
        validate_database_files(&self.path)?;
        Ok(connection)
    }

    fn begin_locked_transaction<'connection>(
        &self,
        connection: &'connection mut Connection,
    ) -> Result<WriteTransaction<'connection>> {
        let lock = acquire_write_lock(&self.path)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Ok(WriteTransaction { transaction, lock })
    }

    fn write_transaction<'connection>(
        &self,
        connection: &'connection mut Connection,
    ) -> Result<WriteTransaction<'connection>> {
        let transaction = self.begin_locked_transaction(connection)?;
        let found = user_version(transaction.transaction())?;
        if found != SCHEMA_VERSION {
            return Err(AgentStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        validate_schema_definition(transaction.transaction())?;
        Ok(transaction)
    }

    fn validate_execution_permit_snapshot(
        &self,
        owner: &str,
        permit: &ActiveExecutionPermit,
        expected_policy_digest: &str,
        native_scope: Option<&NativeExecutionScope>,
    ) -> Result<ExecutionPermitSnapshot> {
        validate_owner(owner)?;
        validate_digest("expected execution policy digest", expected_policy_digest).map_err(
            |_| AgentStoreError::InvalidExecutionPermit {
                id: permit.run_id().to_string(),
            },
        )?;
        if permit.owner() != owner || permit.policy_digest() != expected_policy_digest {
            return Err(AgentStoreError::InvalidExecutionPermit {
                id: permit.run_id().to_string(),
            });
        }

        // A single write-locked snapshot prevents cancellation, terminal
        // transition, resume binding, or any frozen native identity from
        // changing between authority checks.
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let run = require_run(transaction.transaction(), owner, permit.run_id())
            .map_err(|error| execution_permit_error(error, permit.run_id()))?;
        let worker = require_worker(transaction.transaction(), owner, permit.worker_id())
            .map_err(|error| execution_permit_error(error, permit.run_id()))?;
        let stored_token: Option<String> = transaction.transaction().query_row(
            "SELECT lease_token_hash FROM agent_runs WHERE owner = ?1 AND id = ?2",
            params![owner, permit.run_id()],
            |row| row.get(0),
        )?;
        let supplied_hash = token_hash(permit.token());
        let lease = run.lease.as_ref();
        let deadline = run.deadline_at;
        let native_identity_matches = native_scope.is_none_or(|scope| {
            let resume_binding_matches = match (
                run.resume_binding_digest.as_deref(),
                scope.resume_binding_digest(),
            ) {
                (None, None) => true,
                (Some(stored), Some(expected)) => {
                    constant_time_eq(stored.as_bytes(), expected.as_bytes())
                }
                _ => false,
            };
            run.target_key == scope.target_key()
                && run.prompt_digest == scope.prompt_digest()
                && run.policy_digest == scope.policy_digest()
                && worker.logical_session_id.as_deref() == scope.logical_session_id()
                && resume_binding_matches
        });
        let completion_prepared =
            get_completion(transaction.transaction(), owner, permit.run_id())?.is_some();
        let valid = run.owner == owner
            && worker.owner == owner
            && worker.id == permit.worker_id()
            && worker.id == run.worker_id
            && worker.lifecycle == WorkerLifecycle::Open
            && run.worker_generation == permit.generation()
            && run.state == RunState::Running
            && run.policy_digest == expected_policy_digest
            && native_identity_matches
            && !completion_prepared
            && lease
                .is_some_and(|lease| lease.owner == permit.lease_owner() && lease.expires_at > now)
            && deadline.is_some_and(|deadline| deadline > now)
            && stored_token.as_deref().is_some_and(|stored| {
                constant_time_eq(stored.as_bytes(), supplied_hash.as_bytes())
            });
        if !valid {
            return Err(AgentStoreError::InvalidExecutionPermit {
                id: permit.run_id().to_string(),
            });
        }

        // Safe after the complete predicate above established both values.
        let lease = lease.ok_or_else(|| AgentStoreError::InvalidExecutionPermit {
            id: permit.run_id().to_string(),
        })?;
        let deadline_at = deadline.ok_or_else(|| AgentStoreError::InvalidExecutionPermit {
            id: permit.run_id().to_string(),
        })?;
        let snapshot = ExecutionPermitSnapshot::from_validated_run(
            &run,
            &lease.owner,
            lease.expires_at,
            deadline_at,
            now,
        );
        transaction.commit()?;
        Ok(snapshot)
    }
}

impl AgentStore for SqliteAgentStore {
    fn create_root(
        &self,
        owner: &str,
        worker: &NewWorker,
        run: &NewAgentRun,
    ) -> Result<(WorkerRecord, AgentRunRecord)> {
        validate_owner(owner)?;
        worker.validate()?;
        run.validate()?;
        if run.worker_id != worker.id {
            return Err(AgentStoreError::InvalidInput(
                "root run worker id does not match the new worker".into(),
            ));
        }
        if run.parent_run_id.is_some() {
            return Err(AgentStoreError::InvalidInput(
                "root run cannot name a parent run".into(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let worker_record = WorkerRecord {
            owner: owner.to_string(),
            id: worker.id.clone(),
            parent_id: None,
            logical_session_id: worker.logical_session_id.clone(),
            lifecycle: WorkerLifecycle::Open,
            created_at: now,
            updated_at: now,
            released_at: None,
            revision: 0,
        };
        let run_record = new_run_record(owner, run, now)?;
        ensure_worker_and_run_absent(
            transaction.transaction(),
            owner,
            &worker_record.id,
            &run_record.id,
        )?;
        insert_worker(transaction.transaction(), &worker_record)?;
        insert_run(transaction.transaction(), &run_record)?;
        insert_event(
            transaction.transaction(),
            &worker_record,
            None,
            AgentEventKind::WorkerCreated,
            now,
        )?;
        insert_event(
            transaction.transaction(),
            &worker_record,
            Some(&run_record),
            AgentEventKind::RunQueued,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok((worker_record, run_record))
    }

    fn spawn_child(
        &self,
        owner: &str,
        parent_worker_id: &str,
        expected_parent_revision: u64,
        child: &NewWorker,
        run: &NewAgentRun,
    ) -> Result<(WorkerRecord, AgentRunRecord)> {
        validate_owner(owner)?;
        validate_text("parent worker id", parent_worker_id, MAX_ID_BYTES)?;
        child.validate()?;
        run.validate()?;
        if child.id == parent_worker_id || run.worker_id != child.id {
            return Err(AgentStoreError::InvalidInput(
                "child identity must be new and match its initial run".into(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let mut parent = require_worker(transaction.transaction(), owner, parent_worker_id)?;
        ensure_revision(&parent.id, parent.revision, expected_parent_revision)?;
        if parent.lifecycle != WorkerLifecycle::Open {
            return Err(AgentStoreError::InvalidTransition {
                id: parent.id,
                from: parent.lifecycle.to_string(),
                to: WorkerLifecycle::Open.to_string(),
            });
        }
        ensure_worker_and_run_absent(transaction.transaction(), owner, &child.id, &run.id)?;
        validate_parent_run_for_worker(
            transaction.transaction(),
            owner,
            run.parent_run_id.as_deref(),
            parent_worker_id,
        )?;

        let child_record = WorkerRecord {
            owner: owner.to_string(),
            id: child.id.clone(),
            parent_id: Some(parent_worker_id.to_string()),
            logical_session_id: child.logical_session_id.clone(),
            lifecycle: WorkerLifecycle::Open,
            created_at: now,
            updated_at: now,
            released_at: None,
            revision: 0,
        };
        let run_record = new_run_record(owner, run, now)?;
        let before_parent = parent.clone();
        parent.revision = next_u64(parent.revision, "parent worker revision")?;
        parent.updated_at = now;
        update_worker(transaction.transaction(), &before_parent, &parent)?;
        insert_worker(transaction.transaction(), &child_record)?;
        insert_run(transaction.transaction(), &run_record)?;
        insert_event(
            transaction.transaction(),
            &parent,
            None,
            AgentEventKind::ChildSpawned,
            now,
        )?;
        insert_event(
            transaction.transaction(),
            &child_record,
            None,
            AgentEventKind::WorkerCreated,
            now,
        )?;
        insert_event(
            transaction.transaction(),
            &child_record,
            Some(&run_record),
            AgentEventKind::RunQueued,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok((child_record, run_record))
    }

    fn enqueue_run(&self, owner: &str, run: &NewAgentRun) -> Result<AgentRunRecord> {
        validate_owner(owner)?;
        run.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let record = new_run_record(owner, run, now)?;
        let worker = require_worker(transaction.transaction(), owner, &run.worker_id)?;
        if worker.lifecycle != WorkerLifecycle::Open {
            return Err(AgentStoreError::InvalidTransition {
                id: worker.id,
                from: worker.lifecycle.to_string(),
                to: WorkerLifecycle::Open.to_string(),
            });
        }
        if get_run(transaction.transaction(), owner, &run.id)?.is_some() {
            return Err(AgentStoreError::AlreadyExists { id: run.id.clone() });
        }
        validate_parent_run_exists(
            transaction.transaction(),
            owner,
            run.parent_run_id.as_deref(),
        )?;
        insert_run(transaction.transaction(), &record)?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&record),
            AgentEventKind::RunQueued,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(record)
    }

    fn get_worker(&self, owner: &str, worker_id: &str) -> Result<Option<WorkerRecord>> {
        validate_owner(owner)?;
        validate_text("worker id", worker_id, MAX_ID_BYTES)?;
        let connection = self.connection()?;
        get_worker(&connection, owner, worker_id)
    }

    fn get_run(&self, owner: &str, run_id: &str) -> Result<Option<AgentRunRecord>> {
        validate_owner(owner)?;
        validate_text("run id", run_id, MAX_ID_BYTES)?;
        let connection = self.connection()?;
        get_run(&connection, owner, run_id)
    }

    fn claim_due(
        &self,
        owner: &str,
        execution_backend: ExecutionBackend,
        lease_owner: &str,
        lease_seconds: u64,
        limit: usize,
    ) -> Result<Vec<ClaimedRun>> {
        validate_owner(owner)?;
        if !execution_backend.is_claimable() {
            return Err(AgentStoreError::InvalidInput(
                "claim requires an assigned execution backend".into(),
            ));
        }
        validate_text("lease owner", lease_owner, 256)?;
        validate_lease_seconds(lease_seconds)?;
        validate_limit(limit, "claim limit")?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let expires_at = add_seconds(now, lease_seconds, "run lease")?;
        let sql = "SELECT r.id FROM agent_runs r \
            JOIN workers w ON w.owner = r.owner AND w.id = r.worker_id \
            WHERE r.owner = ?1 AND r.execution_backend = ?3 \
              AND r.state = 'queued' AND r.available_at_ms <= ?2 \
              AND w.lifecycle = 'open' \
              AND NOT EXISTS (SELECT 1 FROM agent_runs active \
                  WHERE active.owner = r.owner AND active.worker_id = r.worker_id \
                    AND active.state IN ('starting', 'running', 'cancelling')) \
              AND NOT EXISTS (SELECT 1 FROM agent_runs earlier \
                  WHERE earlier.owner = r.owner AND earlier.worker_id = r.worker_id \
                    AND earlier.state = 'queued' AND earlier.available_at_ms <= ?2 \
                    AND (earlier.available_at_ms < r.available_at_ms \
                      OR (earlier.available_at_ms = r.available_at_ms \
                        AND earlier.queue_sequence < r.queue_sequence))) \
            ORDER BY r.available_at_ms, r.queue_sequence LIMIT ?4";
        let mut statement = transaction.transaction().prepare(sql)?;
        let ids = statement
            .query_map(
                params![
                    owner,
                    now.timestamp_millis(),
                    execution_backend.to_string(),
                    usize_to_i64(limit, "claim limit")?
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);

        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            let before = require_run(transaction.transaction(), owner, &id)?;
            let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
            if before.state != RunState::Queued || worker.lifecycle != WorkerLifecycle::Open {
                return Err(AgentStoreError::CorruptData(
                    "due run changed during a locked claim".into(),
                ));
            }
            let generation =
                next_worker_generation(transaction.transaction(), owner, &before.worker_id)?;
            let token = random_token()?;
            let token_hash = token_hash(&token);
            let mut after = before.clone();
            after.state = RunState::Starting;
            after.revision = next_u64(before.revision, "run revision")?;
            after.worker_generation = generation;
            after.updated_at = now;
            after.deadline_at = Some(add_seconds(
                now,
                before.timeout_seconds,
                "run execution deadline",
            )?);
            after.lease = Some(RunLease {
                owner: lease_owner.to_string(),
                expires_at,
            });
            update_run(
                transaction.transaction(),
                &before,
                &after,
                Some(&token_hash),
            )?;
            insert_event(
                transaction.transaction(),
                &worker,
                Some(&after),
                AgentEventKind::RunClaimed,
                now,
            )?;
            claimed.push(ClaimedRun {
                receipt: RunLeaseReceipt {
                    run_id: after.id.clone(),
                    worker_id: after.worker_id.clone(),
                    generation,
                    revision: after.revision,
                    lease_owner: lease_owner.to_string(),
                    token,
                },
                run: after,
            });
        }
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(claimed)
    }

    fn start(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        controller: &ControllerRef,
    ) -> Result<ClaimedRun> {
        validate_owner(owner)?;
        receipt.validate()?;
        controller.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let (before, token_hash) = authenticated_run(
            transaction.transaction(),
            owner,
            receipt,
            now,
            &[RunState::Starting],
        )?;
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        if before.execution_backend.controller_kind() != Some(controller.kind) {
            return Err(AgentStoreError::InvalidInput(
                "controller kind does not match the frozen execution backend".into(),
            ));
        }
        let mut after = before.clone();
        after.state = RunState::Running;
        after.controller = Some(controller.clone());
        after.started_at = Some(now);
        after.last_heartbeat_at = Some(now);
        after.last_activity_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        update_run(
            transaction.transaction(),
            &before,
            &after,
            Some(&token_hash),
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::RunStarted,
            now,
        )?;
        let next_receipt = receipt_for(&after, receipt.token.clone())?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(ClaimedRun {
            run: after,
            receipt: next_receipt,
        })
    }

    fn issue_execution_permit(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        expected_policy_digest: &str,
    ) -> Result<ActiveExecutionPermit> {
        validate_owner(owner)?;
        receipt
            .validate()
            .map_err(|_| AgentStoreError::InvalidExecutionPermit {
                id: receipt.run_id.clone(),
            })?;
        validate_digest("expected execution policy digest", expected_policy_digest).map_err(
            |_| AgentStoreError::InvalidExecutionPermit {
                id: receipt.run_id.clone(),
            },
        )?;

        // Capability issuance uses the same process lock and SQLite snapshot as
        // mutations so the run, worker, and token hash cannot be observed from
        // different concurrent revisions.
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let (run, _) = authenticated_run(
            transaction.transaction(),
            owner,
            receipt,
            now,
            &[RunState::Running],
        )
        .map_err(|error| execution_permit_error(error, &receipt.run_id))?;
        let worker = require_worker(transaction.transaction(), owner, &receipt.worker_id)
            .map_err(|error| execution_permit_error(error, &receipt.run_id))?;
        if run.owner != owner
            || worker.owner != owner
            || worker.id != run.worker_id
            || worker.lifecycle != WorkerLifecycle::Open
            || run.policy_digest != expected_policy_digest
            || get_completion(transaction.transaction(), owner, &run.id)?.is_some()
        {
            return Err(AgentStoreError::InvalidExecutionPermit {
                id: receipt.run_id.clone(),
            });
        }

        let permit = ActiveExecutionPermit::issue(owner, receipt, &run.policy_digest);
        transaction.commit()?;
        Ok(permit)
    }

    fn validate_execution_permit(
        &self,
        owner: &str,
        permit: &ActiveExecutionPermit,
        expected_policy_digest: &str,
    ) -> Result<ExecutionPermitSnapshot> {
        self.validate_execution_permit_snapshot(owner, permit, expected_policy_digest, None)
    }

    fn validate_native_execution_permit(
        &self,
        owner: &str,
        permit: &ActiveExecutionPermit,
        scope: &NativeExecutionScope,
    ) -> Result<ExecutionPermitSnapshot> {
        self.validate_execution_permit_snapshot(owner, permit, scope.policy_digest(), Some(scope))
    }

    fn heartbeat(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        lease_seconds: u64,
    ) -> Result<ClaimedRun> {
        validate_owner(owner)?;
        receipt.validate()?;
        validate_lease_seconds(lease_seconds)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let requested_expiry = add_seconds(now, lease_seconds, "run lease renewal")?;
        let (before, token_hash) = authenticated_run(
            transaction.transaction(),
            owner,
            receipt,
            now,
            &[RunState::Running],
        )?;
        let current_expiry = before
            .lease
            .as_ref()
            .map(|lease| lease.expires_at)
            .ok_or_else(|| AgentStoreError::CorruptData("authenticated run has no lease".into()))?;
        let renewed_until = requested_expiry.max(current_expiry);
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let mut after = before.clone();
        after.lease = Some(RunLease {
            owner: receipt.lease_owner.clone(),
            expires_at: renewed_until,
        });
        after.last_heartbeat_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        update_run(
            transaction.transaction(),
            &before,
            &after,
            Some(&token_hash),
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::RunHeartbeat,
            now,
        )?;
        let next_receipt = receipt_for(&after, receipt.token.clone())?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(ClaimedRun {
            run: after,
            receipt: next_receipt,
        })
    }

    fn record_activity(&self, owner: &str, receipt: &RunLeaseReceipt) -> Result<ClaimedRun> {
        validate_owner(owner)?;
        receipt.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let (before, token_hash) = authenticated_run(
            transaction.transaction(),
            owner,
            receipt,
            now,
            &[RunState::Running],
        )?;
        if get_completion(transaction.transaction(), owner, &before.id)?.is_some() {
            return Err(AgentStoreError::InvalidReceipt {
                id: receipt.run_id.clone(),
            });
        }
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let mut after = before.clone();
        after.last_activity_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        update_run(
            transaction.transaction(),
            &before,
            &after,
            Some(&token_hash),
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::RunActivity,
            now,
        )?;
        let next_receipt = receipt_for(&after, receipt.token.clone())?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(ClaimedRun {
            run: after,
            receipt: next_receipt,
        })
    }

    fn bind_resume_session(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        proof: &ResumeSessionProof,
    ) -> Result<ClaimedRun> {
        validate_owner(owner)?;
        receipt.validate()?;
        proof.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let (before, token_hash) = authenticated_run(
            transaction.transaction(),
            owner,
            receipt,
            now,
            &[RunState::Running],
        )?;
        if get_completion(transaction.transaction(), owner, &before.id)?.is_some() {
            return Err(AgentStoreError::InvalidReceipt {
                id: receipt.run_id.clone(),
            });
        }
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        if worker.logical_session_id.as_deref() != Some(proof.logical_session_id()) {
            return Err(AgentStoreError::InvalidInput(
                "resume binding logical session does not match the worker".into(),
            ));
        }
        if let Some(existing) = &before.resume_binding_digest {
            if existing != proof.binding_digest() {
                return Err(AgentStoreError::InvalidInput(
                    "run already has a different frozen resume binding".into(),
                ));
            }
            return Ok(ClaimedRun {
                run: before,
                receipt: receipt.clone(),
            });
        }
        let mut after = before.clone();
        after.resume_binding_digest = Some(proof.binding_digest().to_string());
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        update_run(
            transaction.transaction(),
            &before,
            &after,
            Some(&token_hash),
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::ResumeBound,
            now,
        )?;
        let next_receipt = receipt_for(&after, receipt.token.clone())?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(ClaimedRun {
            run: after,
            receipt: next_receipt,
        })
    }

    fn prepare_completion(
        &self,
        owner: &str,
        permit: &ActiveExecutionPermit,
        completion: &NewRunCompletion,
    ) -> Result<PreparedRunCompletion> {
        validate_owner(owner)?;
        completion.validate()?;
        if permit.owner() != owner {
            return Err(AgentStoreError::InvalidExecutionPermit {
                id: permit.run_id().to_string(),
            });
        }
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let run = require_run(transaction.transaction(), owner, permit.run_id())
            .map_err(|error| execution_permit_error(error, permit.run_id()))?;
        let worker = require_worker(transaction.transaction(), owner, permit.worker_id())
            .map_err(|error| execution_permit_error(error, permit.run_id()))?;
        let stored_token: Option<String> = transaction.transaction().query_row(
            "SELECT lease_token_hash FROM agent_runs WHERE owner = ?1 AND id = ?2",
            params![owner, permit.run_id()],
            |row| row.get(0),
        )?;
        let supplied_hash = token_hash(permit.token());
        let valid = run.worker_id == permit.worker_id()
            && run.worker_generation == permit.generation()
            && run.state == RunState::Running
            && run.policy_digest == permit.policy_digest()
            && worker.lifecycle == WorkerLifecycle::Open
            && run
                .lease
                .as_ref()
                .is_some_and(|lease| lease.owner == permit.lease_owner() && lease.expires_at > now)
            && run.deadline_at.is_some_and(|deadline| deadline > now)
            && stored_token.as_deref().is_some_and(|stored| {
                constant_time_eq(stored.as_bytes(), supplied_hash.as_bytes())
            });
        if !valid {
            return Err(AgentStoreError::InvalidExecutionPermit {
                id: permit.run_id().to_string(),
            });
        }

        let token = completion_token(permit, &completion.id);
        if let Some(existing) = get_completion(transaction.transaction(), owner, permit.run_id())? {
            if existing.status != RunCompletionStatus::Prepared
                || existing.worker_id != permit.worker_id()
                || existing.worker_generation != permit.generation()
                || !exact_completion_metadata(&existing, completion)
            {
                return Err(AgentStoreError::CompletionConflict {
                    id: permit.run_id().to_string(),
                });
            }
            transaction.commit()?;
            return Ok(PreparedRunCompletion {
                permit: ActiveCompletionPermit::issue(&existing, token),
                record: existing,
            });
        }

        let occupied: bool = transaction.transaction().query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_run_completions WHERE owner = ?1 \
             AND (completion_id = ?2 OR (sink_kind = ?3 AND publication_key = ?4)))",
            params![
                owner,
                completion.id,
                completion.sink_kind,
                completion.publication_key
            ],
            |row| row.get(0),
        )?;
        if occupied {
            return Err(AgentStoreError::CompletionConflict {
                id: permit.run_id().to_string(),
            });
        }

        let record = RunCompletionRecord {
            owner: owner.to_string(),
            run_id: run.id.clone(),
            worker_id: run.worker_id.clone(),
            worker_generation: run.worker_generation,
            execution_backend: run.execution_backend,
            completion_id: completion.id.clone(),
            sink_kind: completion.sink_kind.clone(),
            publication_key: completion.publication_key.clone(),
            content_digest: completion.content_digest.clone(),
            content_bytes: completion.content_bytes,
            status: RunCompletionStatus::Prepared,
            prepared_at: now,
            prepared_run_revision: run.revision,
            committed_at: None,
            committed_run_revision: None,
            abandoned_at: None,
            abandoned_run_revision: None,
            committed_by_operation_id: None,
            revision: 0,
        };
        insert_completion(transaction.transaction(), &record, &token_hash(&token))?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&run),
            AgentEventKind::CompletionPrepared,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(PreparedRunCompletion {
            permit: ActiveCompletionPermit::issue(&record, token),
            record,
        })
    }

    fn validate_completion_permit(
        &self,
        owner: &str,
        permit: &ActiveCompletionPermit,
    ) -> Result<CompletionPermitSnapshot> {
        validate_owner(owner)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let completion = authenticated_completion(transaction.transaction(), owner, permit)?;
        let run = require_run(transaction.transaction(), owner, permit.run_id()).map_err(|_| {
            AgentStoreError::InvalidCompletionPermit {
                id: permit.run_id().to_string(),
            }
        })?;
        let active_control = active_control_operation(transaction.transaction(), owner, &run.id)?;
        let lease = run.lease.as_ref();
        let deadline = run.deadline_at;
        if completion.status != RunCompletionStatus::Prepared
            || run.worker_id != completion.worker_id
            || run.worker_generation != completion.worker_generation
            || run.execution_backend != completion.execution_backend
            || run.state != RunState::Running
            || active_control.is_some()
            || lease.is_none_or(|lease| lease.expires_at <= now)
            || deadline.is_none_or(|deadline| deadline <= now)
        {
            return Err(AgentStoreError::InvalidCompletionPermit {
                id: permit.run_id().to_string(),
            });
        }
        let lease_expires_at = lease.map(|value| value.expires_at).ok_or_else(|| {
            AgentStoreError::InvalidCompletionPermit {
                id: permit.run_id().to_string(),
            }
        })?;
        let deadline_at = deadline.ok_or_else(|| AgentStoreError::InvalidCompletionPermit {
            id: permit.run_id().to_string(),
        })?;
        let snapshot = CompletionPermitSnapshot {
            record: completion,
            run_revision: run.revision,
            lease_expires_at,
            deadline_at,
            validated_at: now,
        };
        transaction.commit()?;
        Ok(snapshot)
    }

    fn commit_completion(
        &self,
        owner: &str,
        permit: &ActiveCompletionPermit,
    ) -> Result<(AgentRunRecord, RunCompletionRecord)> {
        validate_owner(owner)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let before_completion = authenticated_completion(transaction.transaction(), owner, permit)?;
        let before =
            require_run(transaction.transaction(), owner, permit.run_id()).map_err(|_| {
                AgentStoreError::InvalidCompletionPermit {
                    id: permit.run_id().to_string(),
                }
            })?;
        if before_completion.status == RunCompletionStatus::Committed
            && before.state == RunState::Succeeded
            && before.execution_backend == before_completion.execution_backend
        {
            transaction.commit()?;
            return Ok((before, before_completion));
        }
        if before_completion.status != RunCompletionStatus::Prepared
            || before.worker_id != before_completion.worker_id
            || before.worker_generation != before_completion.worker_generation
            || before.execution_backend != before_completion.execution_backend
            || before.state != RunState::Running
            || before
                .lease
                .as_ref()
                .is_none_or(|lease| lease.expires_at <= now)
            || before.deadline_at.is_none_or(|deadline| deadline <= now)
            || active_control_operation(transaction.transaction(), owner, &before.id)?.is_some()
        {
            return Err(AgentStoreError::InvalidCompletionPermit {
                id: permit.run_id().to_string(),
            });
        }
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let mut after_completion = before_completion.clone();
        after_completion.status = RunCompletionStatus::Committed;
        after_completion.committed_at = Some(now);
        after_completion.revision = next_u64(before_completion.revision, "completion revision")?;
        let mut after = before.clone();
        after.state = RunState::Succeeded;
        after.finished_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        after_completion.committed_run_revision = Some(after.revision);
        after.controller = None;
        after.lease = None;
        update_completion(
            transaction.transaction(),
            &before_completion,
            &after_completion,
        )?;
        update_run(transaction.transaction(), &before, &after, None)?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::CompletionCommitted,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok((after, after_completion))
    }

    fn get_completion(&self, owner: &str, run_id: &str) -> Result<Option<RunCompletionRecord>> {
        validate_owner(owner)?;
        validate_text("completion run id", run_id, MAX_ID_BYTES)?;
        let connection = self.connection()?;
        get_completion(&connection, owner, run_id)
    }

    fn completion_for_recovery(
        &self,
        owner: &str,
        ticket: &RecoveryTicket,
    ) -> Result<Option<RunCompletionRecord>> {
        validate_owner(owner)?;
        ticket.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let run = require_run(transaction.transaction(), owner, &ticket.run_id)?;
        let active = active_control_operation(transaction.transaction(), owner, &ticket.run_id)?
            .ok_or_else(|| AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            })?;
        if run.worker_id != ticket.worker_id
            || run.worker_generation != ticket.generation
            || run.revision != ticket.revision
            || run.state != RunState::Cancelling
            || !control_ticket_matches_recovery(&active, ticket, now)
        {
            return Err(AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            });
        }
        let completion = get_completion(transaction.transaction(), owner, &ticket.run_id)?;
        transaction.commit()?;
        Ok(completion)
    }

    fn commit_recovered_completion(
        &self,
        owner: &str,
        ticket: &RecoveryTicket,
        completion_id: &str,
    ) -> Result<(AgentRunRecord, RunCompletionRecord)> {
        validate_owner(owner)?;
        ticket.validate()?;
        validate_text("completion id", completion_id, MAX_ID_BYTES)?;
        if ticket.reason != RecoveryReason::LeaseExpired {
            return Err(AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            });
        }
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let before = require_run(transaction.transaction(), owner, &ticket.run_id)?;
        let before_completion = get_completion(transaction.transaction(), owner, &ticket.run_id)?
            .ok_or_else(|| AgentStoreError::InvalidCompletionPermit {
            id: ticket.run_id.clone(),
        })?;
        if before.state == RunState::Succeeded
            && before_completion.status == RunCompletionStatus::Committed
            && before_completion.completion_id == completion_id
            && before_completion.committed_by_operation_id.as_deref()
                == Some(ticket.operation_id.as_str())
            && before.execution_backend == before_completion.execution_backend
            && settled_recovery_ticket_matches(transaction.transaction(), owner, ticket)?
        {
            transaction.commit()?;
            return Ok((before, before_completion));
        }
        let active = active_control_operation(transaction.transaction(), owner, &ticket.run_id)?
            .ok_or_else(|| AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            })?;
        if before.worker_id != ticket.worker_id
            || before.worker_generation != ticket.generation
            || before.revision != ticket.revision
            || before.state != RunState::Cancelling
            || !control_ticket_matches_recovery(&active, ticket, now)
            || before_completion.status != RunCompletionStatus::Prepared
            || before_completion.completion_id != completion_id
            || before_completion.worker_generation != ticket.generation
            || before.execution_backend != before_completion.execution_backend
        {
            return Err(AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            });
        }
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let mut after_completion = before_completion.clone();
        after_completion.status = RunCompletionStatus::Committed;
        after_completion.committed_at = Some(now);
        after_completion.committed_by_operation_id = Some(ticket.operation_id.clone());
        after_completion.revision = next_u64(before_completion.revision, "completion revision")?;
        let mut after = before.clone();
        after.state = RunState::Succeeded;
        after.failure_code = None;
        after.finished_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        after_completion.committed_run_revision = Some(after.revision);
        after.controller = None;
        after.lease = None;
        update_completion(
            transaction.transaction(),
            &before_completion,
            &after_completion,
        )?;
        update_run(transaction.transaction(), &before, &after, None)?;
        finish_control_operation(
            transaction.transaction(),
            owner,
            &before.id,
            &ticket.operation_id,
            "settled",
            now,
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::CompletionCommitted,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok((after, after_completion))
    }

    fn settle(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        settlement: RunSettlement,
    ) -> Result<AgentRunRecord> {
        validate_owner(owner)?;
        receipt.validate()?;
        let (state, failure_code) = settlement.parts()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let (before, _) = authenticated_run(
            transaction.transaction(),
            owner,
            receipt,
            now,
            &[RunState::Starting, RunState::Running],
        )?;
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let mut after = before.clone();
        after.state = state;
        after.failure_code = failure_code;
        after.finished_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        after.controller = None;
        after.lease = None;
        let completion_abandoned = abandon_completion_if_prepared(
            transaction.transaction(),
            owner,
            &before.id,
            now,
            after.revision,
        )?;
        update_run(transaction.transaction(), &before, &after, None)?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::RunSettled,
            now,
        )?;
        if completion_abandoned {
            insert_event(
                transaction.transaction(),
                &worker,
                Some(&after),
                AgentEventKind::CompletionAbandoned,
                now,
            )?;
        }
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(after)
    }

    fn topology(&self, owner: &str, root_worker_id: &str) -> Result<WorkerTopology> {
        validate_owner(owner)?;
        validate_text("root worker id", root_worker_id, MAX_ID_BYTES)?;
        let connection = self.connection()?;
        let workers = subtree_workers(&connection, owner, root_worker_id)?;
        topology_from_workers(workers, root_worker_id)
    }

    fn request_cancel_tree(
        &self,
        owner: &str,
        root_worker_id: &str,
        request: &CancelRequest,
    ) -> Result<CancelPlan> {
        validate_owner(owner)?;
        validate_text("root worker id", root_worker_id, MAX_ID_BYTES)?;
        request.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let operation_expires_at =
            add_seconds(now, request.lease_seconds, "cancel operation lease")?;
        let workers = subtree_workers(transaction.transaction(), owner, root_worker_id)?;
        let postorder = subtree_postorder(&workers, root_worker_id)?;
        let mut snapshots = Vec::with_capacity(postorder.len());
        let mut current_entries = Vec::new();
        for worker_id in &postorder {
            let worker = require_worker(transaction.transaction(), owner, worker_id)?;
            let runs = nonterminal_runs_for_worker(transaction.transaction(), owner, worker_id)?;
            for run in &runs {
                let action = if run.state == RunState::Queued {
                    CancelTreeRunAction::QueuedCancel
                } else if matches!(
                    run.state,
                    RunState::Starting | RunState::Running | RunState::Cancelling
                ) {
                    CancelTreeRunAction::ControllerCancel
                } else {
                    return Err(AgentStoreError::CorruptData(
                        "nonterminal run has an unknown cancellation state".into(),
                    ));
                };
                current_entries.push(CancelTreeRunEntry {
                    worker_id: worker_id.clone(),
                    run_id: run.id.clone(),
                    action,
                });
            }
            if current_entries.len() > MAX_TREE_CANCEL_RUNS {
                return Err(AgentStoreError::InvalidInput(format!(
                    "tree cancel exceeds the {MAX_TREE_CANCEL_RUNS}-run safety bound"
                )));
            }
            snapshots.push((worker, runs));
        }
        let existing_header =
            load_cancel_tree_header(transaction.transaction(), owner, &request.operation_id)?;
        let plan_is_new = existing_header.is_none();
        if let Some(header) = &existing_header {
            if header.root_worker_id != root_worker_id
                || header.lease_owner != request.lease_owner
                || header.lease_seconds != request.lease_seconds
            {
                return Err(AgentStoreError::InvalidInput(
                    "cancel operation id is already frozen to a different tree request".into(),
                ));
            }
            let stored_workers =
                load_cancel_tree_workers(transaction.transaction(), owner, &request.operation_id)?;
            let stored_entries =
                load_cancel_tree_runs(transaction.transaction(), owner, &request.operation_id)?;
            let stored_digest = cancel_tree_plan_digest(&stored_workers, &stored_entries)?;
            if stored_workers != postorder
                || stored_workers.len() != header.worker_count
                || stored_entries.len() != header.run_count
                || stored_digest != header.plan_digest
                || current_entries
                    .iter()
                    .any(|current| !stored_entries.iter().any(|stored| stored == current))
            {
                return Err(AgentStoreError::CorruptData(
                    "frozen cancel tree plan differs from current tree authority".into(),
                ));
            }
        } else if !request.retry_tickets.is_empty() {
            return Err(AgentStoreError::InvalidInput(
                "new cancel operation cannot carry retry tickets".into(),
            ));
        }

        let mut prepared = Vec::with_capacity(snapshots.len());
        let mut consumed_retry_ids = BTreeSet::new();
        for (before_worker, runs) in snapshots {
            if !plan_is_new && before_worker.lifecycle == WorkerLifecycle::Open {
                return Err(AgentStoreError::CorruptData(
                    "frozen cancel tree contains an open worker".into(),
                ));
            }
            let mut after_worker = before_worker.clone();
            let changed = after_worker.lifecycle == WorkerLifecycle::Open;
            if changed {
                after_worker.lifecycle = WorkerLifecycle::Draining;
                after_worker.updated_at = now;
                after_worker.revision = next_u64(after_worker.revision, "worker revision")?;
            }
            let mut prepared_runs = Vec::with_capacity(runs.len());
            for before in runs {
                if before.state == RunState::Queued {
                    let mut after = before.clone();
                    after.state = RunState::Cancelled;
                    after.failure_code = Some(RunFailureCode::Cancelled);
                    after.finished_at = Some(now);
                    after.updated_at = now;
                    after.revision = next_u64(before.revision, "run revision")?;
                    prepared_runs.push(PreparedCancelRun::Queued {
                        before: Box::new(before),
                        after: Box::new(after),
                    });
                    continue;
                }
                let active =
                    active_control_operation(transaction.transaction(), owner, &before.id)?;
                let mut superseded_operation_id = None;
                if let Some(active) = active {
                    if active.expires_at > now {
                        if active.kind != ControlOperationKind::Cancel
                            || active.operation_id != request.operation_id
                            || active.lease_owner != request.lease_owner
                        {
                            return Err(AgentStoreError::ControlBusy {
                                id: before.id.clone(),
                            });
                        }
                        let retry = request
                            .retry_tickets
                            .iter()
                            .find(|ticket| ticket.run_id == before.id)
                            .ok_or_else(|| AgentStoreError::InvalidCancelTicket {
                                id: before.id.clone(),
                            })?;
                        if retry.worker_id != before.worker_id
                            || !control_ticket_matches_cancel(&active, retry, now)
                        {
                            return Err(AgentStoreError::InvalidCancelTicket {
                                id: before.id.clone(),
                            });
                        }
                        consumed_retry_ids.insert(retry.run_id.clone());
                        prepared_runs.push(PreparedCancelRun::Reuse(retry.clone()));
                        continue;
                    }
                    if active.operation_id == request.operation_id {
                        return Err(AgentStoreError::InvalidCancelTicket {
                            id: before.id.clone(),
                        });
                    }
                    superseded_operation_id = Some(active.operation_id);
                } else if before.state == RunState::Cancelling {
                    return Err(AgentStoreError::CorruptData(
                        "cancelling run has no active control operation".into(),
                    ));
                }

                let token = random_token()?;
                let mut after = before.clone();
                after.state = RunState::Cancelling;
                after.updated_at = now;
                after.revision = next_u64(before.revision, "run revision")?;
                after.lease = None;
                after.failure_code = None;
                let operation = ActiveControlOperation {
                    operation_id: request.operation_id.clone(),
                    kind: ControlOperationKind::Cancel,
                    generation: after.worker_generation,
                    revision: after.revision,
                    controller: after.controller.clone(),
                    token_hash: token_hash(&token),
                    lease_owner: request.lease_owner.clone(),
                    expires_at: operation_expires_at,
                };
                prepared_runs.push(PreparedCancelRun::Controller {
                    before: Box::new(before),
                    after: Box::new(after),
                    superseded_operation_id,
                    operation,
                    token,
                });
            }
            prepared.push(PreparedCancelWorker {
                before: before_worker,
                after: after_worker,
                changed,
                runs: prepared_runs,
            });
        }
        if consumed_retry_ids.len() != request.retry_tickets.len() {
            return Err(AgentStoreError::InvalidCancelTicket {
                id: request.operation_id.clone(),
            });
        }

        if plan_is_new {
            let header = CancelTreeHeader {
                root_worker_id: root_worker_id.to_string(),
                plan_digest: cancel_tree_plan_digest(&postorder, &current_entries)?,
                worker_count: postorder.len(),
                run_count: current_entries.len(),
                lease_owner: request.lease_owner.clone(),
                lease_seconds: request.lease_seconds,
            };
            insert_cancel_tree_plan(
                transaction.transaction(),
                owner,
                &request.operation_id,
                &header,
                &postorder,
                &current_entries,
                now,
            )?;
        }

        let mut tickets = Vec::new();
        for worker_plan in prepared {
            if worker_plan.changed {
                update_worker(
                    transaction.transaction(),
                    &worker_plan.before,
                    &worker_plan.after,
                )?;
            }
            for run_plan in worker_plan.runs {
                match run_plan {
                    PreparedCancelRun::Queued { before, after } => {
                        let before = *before;
                        let after = *after;
                        update_run(transaction.transaction(), &before, &after, None)?;
                        insert_event(
                            transaction.transaction(),
                            &worker_plan.after,
                            Some(&after),
                            AgentEventKind::CancelSettled,
                            now,
                        )?;
                    }
                    PreparedCancelRun::Reuse(ticket) => tickets.push(ticket),
                    PreparedCancelRun::Controller {
                        before,
                        after,
                        superseded_operation_id,
                        operation,
                        token,
                    } => {
                        let before = *before;
                        let after = *after;
                        if let Some(superseded) = superseded_operation_id {
                            finish_control_operation(
                                transaction.transaction(),
                                owner,
                                &before.id,
                                &superseded,
                                "superseded",
                                now,
                            )?;
                        }
                        update_run(transaction.transaction(), &before, &after, None)?;
                        insert_control_operation(
                            transaction.transaction(),
                            owner,
                            &after.id,
                            &operation,
                            now,
                        )?;
                        insert_event(
                            transaction.transaction(),
                            &worker_plan.after,
                            Some(&after),
                            AgentEventKind::CancelRequested,
                            now,
                        )?;
                        tickets.push(CancelTicket {
                            operation_id: operation.operation_id,
                            worker_id: after.worker_id,
                            run_id: after.id,
                            generation: after.worker_generation,
                            revision: after.revision,
                            controller: after.controller,
                            lease_owner: operation.lease_owner,
                            expires_at: operation.expires_at,
                            token,
                        });
                    }
                }
            }
            if worker_plan.changed {
                insert_event(
                    transaction.transaction(),
                    &worker_plan.after,
                    None,
                    AgentEventKind::CancelRequested,
                    now,
                )?;
            }
        }
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(CancelPlan {
            root_worker_id: root_worker_id.to_string(),
            tickets,
        })
    }

    fn settle_cancel(
        &self,
        owner: &str,
        ticket: &CancelTicket,
        outcome: CancelOutcome,
    ) -> Result<AgentRunRecord> {
        validate_owner(owner)?;
        ticket.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let before = require_run(transaction.transaction(), owner, &ticket.run_id)?;
        let active = active_control_operation(transaction.transaction(), owner, &ticket.run_id)?
            .ok_or_else(|| AgentStoreError::InvalidCancelTicket {
                id: ticket.run_id.clone(),
            })?;
        if before.worker_id != ticket.worker_id
            || before.worker_generation != ticket.generation
            || before.revision != ticket.revision
            || before.state != RunState::Cancelling
            || !control_ticket_matches_cancel(&active, ticket, now)
        {
            return Err(AgentStoreError::InvalidCancelTicket {
                id: ticket.run_id.clone(),
            });
        }
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let (state, failure_code) = match outcome {
            CancelOutcome::Cancelled => (RunState::Cancelled, RunFailureCode::Cancelled),
            CancelOutcome::ControllerUnavailable => {
                (RunState::Interrupted, RunFailureCode::ControlUnavailable)
            }
        };
        let mut after = before.clone();
        after.state = state;
        after.failure_code = Some(failure_code);
        after.finished_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        after.controller = None;
        after.lease = None;
        let completion_abandoned = abandon_completion_if_prepared(
            transaction.transaction(),
            owner,
            &before.id,
            now,
            after.revision,
        )?;
        update_run(transaction.transaction(), &before, &after, None)?;
        finish_control_operation(
            transaction.transaction(),
            owner,
            &before.id,
            &ticket.operation_id,
            "settled",
            now,
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::CancelSettled,
            now,
        )?;
        if completion_abandoned {
            insert_event(
                transaction.transaction(),
                &worker,
                Some(&after),
                AgentEventKind::CompletionAbandoned,
                now,
            )?;
        }
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(after)
    }

    fn claim_recovery_due(
        &self,
        owner: &str,
        reconciler: &str,
        lease_seconds: u64,
        limit: usize,
    ) -> Result<Vec<RecoveryTicket>> {
        validate_owner(owner)?;
        validate_text("reconciler", reconciler, 256)?;
        validate_lease_seconds(lease_seconds)?;
        validate_limit(limit, "recovery limit")?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let expires_at = add_seconds(now, lease_seconds, "recovery operation lease")?;
        let mut statement = transaction.transaction().prepare(
            "SELECT r.id FROM agent_runs r \
             LEFT JOIN run_control_operations c ON c.owner = r.owner AND c.run_id = r.id \
               AND c.status = 'active' \
             WHERE r.owner = ?1 AND ( \
               (r.state IN ('starting','running') AND \
                 (r.deadline_at_ms <= ?2 OR r.lease_expires_at_ms <= ?2)) \
               OR (r.state = 'cancelling' AND c.lease_expires_at_ms <= ?2)) \
             ORDER BY CASE \
               WHEN r.deadline_at_ms <= ?2 THEN r.deadline_at_ms \
               WHEN r.state = 'cancelling' THEN c.lease_expires_at_ms \
               ELSE r.lease_expires_at_ms END, r.queue_sequence LIMIT ?3",
        )?;
        let run_ids = statement
            .query_map(
                params![
                    owner,
                    now.timestamp_millis(),
                    usize_to_i64(limit, "recovery limit")?
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);

        let mut tickets = Vec::with_capacity(run_ids.len());
        for run_id in run_ids {
            let before = require_run(transaction.transaction(), owner, &run_id)?;
            let previous_control =
                active_control_operation(transaction.transaction(), owner, &before.id)?;
            let reason = match before.state {
                RunState::Starting | RunState::Running => {
                    if previous_control.is_some() {
                        return Err(AgentStoreError::CorruptData(
                            "lease-controlled run already has a control operation".into(),
                        ));
                    }
                    if before.deadline_at.is_some_and(|deadline| deadline <= now) {
                        RecoveryReason::ExecutionTimedOut
                    } else if before
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.expires_at <= now)
                    {
                        RecoveryReason::LeaseExpired
                    } else {
                        return Err(AgentStoreError::CorruptData(
                            "selected recovery run is not due".into(),
                        ));
                    }
                }
                RunState::Cancelling => {
                    let previous = previous_control.as_ref().ok_or_else(|| {
                        AgentStoreError::CorruptData(
                            "cancelling recovery candidate has no control operation".into(),
                        )
                    })?;
                    if previous.expires_at > now {
                        return Err(AgentStoreError::CorruptData(
                            "selected control recovery is not due".into(),
                        ));
                    }
                    match previous.kind {
                        ControlOperationKind::Cancel => RecoveryReason::CancellationAbandoned,
                        kind => kind.recovery_reason()?,
                    }
                }
                _ => {
                    return Err(AgentStoreError::CorruptData(
                        "selected recovery run has an invalid state".into(),
                    ));
                }
            };
            if let Some(previous) = previous_control {
                finish_control_operation(
                    transaction.transaction(),
                    owner,
                    &before.id,
                    &previous.operation_id,
                    "superseded",
                    now,
                )?;
            }

            let token = random_token()?;
            let operation_id = Uuid::now_v7().to_string();
            let mut after = before.clone();
            after.state = RunState::Cancelling;
            after.updated_at = now;
            after.revision = next_u64(before.revision, "run revision")?;
            after.lease = None;
            after.failure_code = None;
            update_run(transaction.transaction(), &before, &after, None)?;
            let operation = ActiveControlOperation {
                operation_id: operation_id.clone(),
                kind: ControlOperationKind::from_recovery(reason),
                generation: after.worker_generation,
                revision: after.revision,
                controller: after.controller.clone(),
                token_hash: token_hash(&token),
                lease_owner: reconciler.to_string(),
                expires_at,
            };
            insert_control_operation(transaction.transaction(), owner, &after.id, &operation, now)?;
            let worker = require_worker(transaction.transaction(), owner, &after.worker_id)?;
            insert_event(
                transaction.transaction(),
                &worker,
                Some(&after),
                AgentEventKind::RecoveryRequested,
                now,
            )?;
            tickets.push(RecoveryTicket {
                operation_id,
                worker_id: after.worker_id,
                run_id: after.id,
                generation: after.worker_generation,
                revision: after.revision,
                controller: after.controller,
                reason,
                lease_owner: reconciler.to_string(),
                expires_at,
                token,
            });
        }
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(tickets)
    }

    fn confirm_controller_gone(
        &self,
        owner: &str,
        ticket: &RecoveryTicket,
    ) -> Result<AgentRunRecord> {
        validate_owner(owner)?;
        ticket.validate()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let before = require_run(transaction.transaction(), owner, &ticket.run_id)?;
        let active = active_control_operation(transaction.transaction(), owner, &ticket.run_id)?
            .ok_or_else(|| AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            })?;
        if before.worker_id != ticket.worker_id
            || before.worker_generation != ticket.generation
            || before.revision != ticket.revision
            || before.state != RunState::Cancelling
            || !control_ticket_matches_recovery(&active, ticket, now)
        {
            return Err(AgentStoreError::InvalidRecoveryTicket {
                id: ticket.run_id.clone(),
            });
        }
        let (state, failure_code) = match ticket.reason {
            RecoveryReason::LeaseExpired => (RunState::Interrupted, RunFailureCode::ControllerLost),
            RecoveryReason::ExecutionTimedOut => (RunState::TimedOut, RunFailureCode::TimedOut),
            RecoveryReason::CancellationAbandoned => {
                (RunState::Cancelled, RunFailureCode::Cancelled)
            }
        };
        let worker = require_worker(transaction.transaction(), owner, &before.worker_id)?;
        let mut after = before.clone();
        after.state = state;
        after.failure_code = Some(failure_code);
        after.finished_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "run revision")?;
        after.controller = None;
        after.lease = None;
        let completion_abandoned = abandon_completion_if_prepared(
            transaction.transaction(),
            owner,
            &before.id,
            now,
            after.revision,
        )?;
        update_run(transaction.transaction(), &before, &after, None)?;
        finish_control_operation(
            transaction.transaction(),
            owner,
            &before.id,
            &ticket.operation_id,
            "settled",
            now,
        )?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&after),
            AgentEventKind::RecoverySettled,
            now,
        )?;
        if completion_abandoned {
            insert_event(
                transaction.transaction(),
                &worker,
                Some(&after),
                AgentEventKind::CompletionAbandoned,
                now,
            )?;
        }
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(after)
    }

    fn release_worker(
        &self,
        owner: &str,
        worker_id: &str,
        expected_revision: u64,
    ) -> Result<WorkerRecord> {
        validate_owner(owner)?;
        validate_text("worker id", worker_id, MAX_ID_BYTES)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        let before = require_worker(transaction.transaction(), owner, worker_id)?;
        ensure_revision(&before.id, before.revision, expected_revision)?;
        if before.lifecycle != WorkerLifecycle::Draining {
            return Err(AgentStoreError::InvalidTransition {
                id: before.id,
                from: before.lifecycle.to_string(),
                to: WorkerLifecycle::Released.to_string(),
            });
        }
        let has_nonterminal: bool = transaction.transaction().query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_runs WHERE owner = ?1 AND worker_id = ?2 \
             AND state NOT IN ('succeeded','failed','timed_out','cancelled','interrupted'))",
            params![owner, worker_id],
            |row| row.get(0),
        )?;
        if has_nonterminal {
            return Err(AgentStoreError::InvalidTransition {
                id: before.id,
                from: before.lifecycle.to_string(),
                to: WorkerLifecycle::Released.to_string(),
            });
        }
        let has_live_child: bool = transaction.transaction().query_row(
            "SELECT EXISTS(SELECT 1 FROM workers WHERE owner = ?1 AND parent_id = ?2 \
             AND lifecycle <> 'released')",
            params![owner, worker_id],
            |row| row.get(0),
        )?;
        if has_live_child {
            return Err(AgentStoreError::InvalidInput(
                "worker cannot be released before every child is released".into(),
            ));
        }
        let mut after = before.clone();
        after.lifecycle = WorkerLifecycle::Released;
        after.released_at = Some(now);
        after.updated_at = now;
        after.revision = next_u64(before.revision, "worker revision")?;
        update_worker(transaction.transaction(), &before, &after)?;
        insert_event(
            transaction.transaction(),
            &after,
            None,
            AgentEventKind::WorkerReleased,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(after)
    }

    fn enqueue_resume(&self, owner: &str, request: &EnqueueResume) -> Result<AgentRunRecord> {
        validate_owner(owner)?;
        validate_text("new resume run id", &request.new_run_id, MAX_ID_BYTES)?;
        validate_text("source run id", &request.source_run_id, MAX_ID_BYTES)?;
        request.proof.validate()?;
        let available_at = normalize_timestamp(request.available_at)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let now = self.operation_now(transaction.transaction(), owner)?;
        if get_run(transaction.transaction(), owner, &request.new_run_id)?.is_some() {
            return Err(AgentStoreError::AlreadyExists {
                id: request.new_run_id.clone(),
            });
        }
        let source = require_run(transaction.transaction(), owner, &request.source_run_id)?;
        if !source.execution_backend.is_claimable() {
            return Err(AgentStoreError::ResumeRejected {
                id: source.id,
                reason: "source execution backend is unassigned".into(),
            });
        }
        if !source.is_resume_eligible() {
            return Err(AgentStoreError::ResumeRejected {
                id: source.id,
                reason: "source is not a resumable interrupted run or budget is exhausted".into(),
            });
        }
        let worker = require_worker(transaction.transaction(), owner, &source.worker_id)?;
        if worker.lifecycle != WorkerLifecycle::Open {
            return Err(AgentStoreError::ResumeRejected {
                id: source.id,
                reason: "worker is not open".into(),
            });
        }
        if worker.logical_session_id.as_deref()
            != Some(request.proof.session().logical_session_id())
        {
            return Err(AgentStoreError::ResumeRejected {
                id: source.id,
                reason: "logical session proof does not match the worker".into(),
            });
        }
        if source.policy_digest != request.proof.policy_digest() {
            return Err(AgentStoreError::ResumeRejected {
                id: source.id,
                reason: "effective resume policy differs from the frozen source policy".into(),
            });
        }
        let frozen_binding = source.resume_binding_digest.as_deref().ok_or_else(|| {
            AgentStoreError::ResumeRejected {
                id: source.id.clone(),
                reason: "source run has no exact native-session binding".into(),
            }
        })?;
        if frozen_binding != request.proof.session().binding_digest() {
            return Err(AgentStoreError::ResumeRejected {
                id: source.id,
                reason: "native-session binding proof differs from the frozen source".into(),
            });
        }
        let existing_resume: bool = transaction.transaction().query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_runs WHERE owner = ?1 AND resume_of_run_id = ?2)",
            params![owner, request.source_run_id],
            |row| row.get(0),
        )?;
        if existing_resume {
            return Err(AgentStoreError::ResumeRejected {
                id: request.source_run_id.clone(),
                reason: "a resume run already exists for this generation".into(),
            });
        }
        let resume_attempt = source
            .resume_attempt
            .checked_add(1)
            .ok_or_else(|| AgentStoreError::CorruptData("resume attempt overflow".into()))?;
        let record = AgentRunRecord {
            owner: owner.to_string(),
            id: request.new_run_id.clone(),
            worker_id: source.worker_id.clone(),
            task_id: source.task_id.clone(),
            trace_id: source.trace_id.clone(),
            parent_run_id: source.parent_run_id.clone(),
            resume_of_run_id: Some(source.id.clone()),
            execution_backend: source.execution_backend,
            state: RunState::Queued,
            mode: source.mode,
            target_key: source.target_key.clone(),
            prompt_digest: source.prompt_digest.clone(),
            policy_digest: source.policy_digest.clone(),
            resume_binding_digest: source.resume_binding_digest.clone(),
            available_at,
            deadline_at: None,
            timeout_seconds: source.timeout_seconds,
            max_resume_attempts: source.max_resume_attempts,
            resume_attempt,
            created_at: now,
            started_at: None,
            updated_at: now,
            finished_at: None,
            revision: 0,
            worker_generation: 0,
            controller: None,
            lease: None,
            last_heartbeat_at: None,
            last_activity_at: None,
            failure_code: None,
        };
        validate_stored_run(&record)?;
        insert_run(transaction.transaction(), &record)?;
        insert_event(
            transaction.transaction(),
            &worker,
            Some(&record),
            AgentEventKind::ResumeQueued,
            now,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(record)
    }

    fn unprojected_events(&self, owner: &str, projector: &str, limit: usize) -> Result<OutboxPage> {
        validate_owner(owner)?;
        validate_projector(projector)?;
        validate_limit(limit, "outbox limit")?;
        let now = self.now()?;
        let connection = self.connection()?;
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM agent_events e \
             WHERE e.owner = ?1 AND NOT EXISTS (SELECT 1 FROM agent_projector_progress p \
               WHERE p.owner = e.owner AND p.projector = ?2 \
                 AND p.event_sequence = e.sequence) \
               AND NOT EXISTS (SELECT 1 FROM agent_projector_dispositions d \
                 WHERE d.owner = e.owner AND d.projector = ?2 \
                   AND d.event_sequence = e.sequence AND (d.disposition = 'quarantined' \
                     OR (d.disposition = 'deferred' AND d.not_before_ms > ?3))) \
             ORDER BY e.sequence LIMIT ?4"
        );
        let mut statement = connection.prepare(&sql)?;
        let mut items = statement
            .query_map(
                params![
                    owner,
                    projector,
                    now.timestamp_millis(),
                    usize_to_i64(limit.saturating_add(1), "outbox limit")?
                ],
                row_to_event,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        Ok(OutboxPage { items, has_more })
    }

    fn mark_projected(&self, owner: &str, projector: &str, event_id: &str) -> Result<()> {
        validate_owner(owner)?;
        validate_projector(projector)?;
        validate_text("event id", event_id, MAX_ID_BYTES)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let event: Option<(i64, i64)> = transaction
            .transaction()
            .query_row(
            "SELECT sequence, occurred_at_ms FROM agent_events WHERE owner = ?1 AND event_id = ?2",
            params![owner, event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
            .optional()?;
        let Some((sequence, occurred_at_ms)) = event else {
            return Err(AgentStoreError::NotFound {
                id: event_id.to_string(),
            });
        };
        let observed = self.operation_now(transaction.transaction(), owner)?;
        let occurred_at = DateTime::from_timestamp_millis(occurred_at_ms).ok_or_else(|| {
            AgentStoreError::CorruptData("projected event timestamp is invalid".into())
        })?;
        let projected_at = observed.max(occurred_at);
        transaction.transaction().execute(
            "INSERT OR IGNORE INTO agent_projector_progress \
             (owner, projector, event_sequence, projected_at_ms) VALUES (?1, ?2, ?3, ?4)",
            params![owner, projector, sequence, projected_at.timestamp_millis()],
        )?;
        transaction.transaction().execute(
            "DELETE FROM agent_projector_dispositions \
             WHERE owner = ?1 AND projector = ?2 AND event_sequence = ?3",
            params![owner, projector, sequence],
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()
    }

    fn defer_projection(
        &self,
        owner: &str,
        projector: &str,
        event_id: &str,
        reason: ProjectionDeferReason,
        delay: Duration,
    ) -> Result<()> {
        validate_owner(owner)?;
        validate_projector(projector)?;
        validate_text("event id", event_id, MAX_ID_BYTES)?;
        if delay.as_millis() == 0 || delay > Duration::from_secs(MAX_PROJECTION_DEFER_SECONDS) {
            return Err(AgentStoreError::InvalidInput(
                "projection retry delay is invalid".into(),
            ));
        }
        let delay_ms = i64::try_from(delay.as_millis()).map_err(|_| {
            AgentStoreError::InvalidInput("projection retry delay is invalid".into())
        })?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let (sequence, occurred_at_ms) =
            projection_event_source(transaction.transaction(), owner, event_id)?;
        let observed = self.operation_now(transaction.transaction(), owner)?;
        let recorded_at_ms = observed.timestamp_millis().max(occurred_at_ms);
        let not_before_ms = recorded_at_ms.checked_add(delay_ms).ok_or_else(|| {
            AgentStoreError::InvalidInput("projection retry delay is invalid".into())
        })?;
        transaction.transaction().execute(
            "INSERT INTO agent_projector_dispositions (owner, projector, event_sequence, \
                disposition, reason, not_before_ms, recorded_at_ms) \
             SELECT ?1, ?2, ?3, 'deferred', ?4, ?5, ?6 \
             WHERE NOT EXISTS (SELECT 1 FROM agent_projector_progress p \
                WHERE p.owner = ?1 AND p.projector = ?2 AND p.event_sequence = ?3) \
             ON CONFLICT(owner, projector, event_sequence) DO UPDATE SET \
                reason = CASE WHEN disposition = 'quarantined' THEN reason ELSE excluded.reason END, \
                not_before_ms = CASE WHEN disposition = 'quarantined' THEN NULL \
                    ELSE MAX(not_before_ms, excluded.not_before_ms) END, \
                recorded_at_ms = CASE WHEN disposition = 'quarantined' THEN recorded_at_ms \
                    ELSE MAX(recorded_at_ms, excluded.recorded_at_ms) END \
             WHERE NOT EXISTS (SELECT 1 FROM agent_projector_progress p \
                WHERE p.owner = excluded.owner AND p.projector = excluded.projector \
                  AND p.event_sequence = excluded.event_sequence)",
            params![
                owner,
                projector,
                sequence,
                reason.as_str(),
                not_before_ms,
                recorded_at_ms
            ],
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()
    }

    fn quarantine_projection(
        &self,
        owner: &str,
        projector: &str,
        event_id: &str,
        reason: ProjectionQuarantineReason,
    ) -> Result<()> {
        validate_owner(owner)?;
        validate_projector(projector)?;
        validate_text("event id", event_id, MAX_ID_BYTES)?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let (sequence, occurred_at_ms) =
            projection_event_source(transaction.transaction(), owner, event_id)?;
        let observed = self.operation_now(transaction.transaction(), owner)?;
        let recorded_at_ms = observed.timestamp_millis().max(occurred_at_ms);
        transaction.transaction().execute(
            "INSERT INTO agent_projector_dispositions (owner, projector, event_sequence, \
                disposition, reason, not_before_ms, recorded_at_ms) \
             SELECT ?1, ?2, ?3, 'quarantined', ?4, NULL, ?5 \
             WHERE NOT EXISTS (SELECT 1 FROM agent_projector_progress p \
                WHERE p.owner = ?1 AND p.projector = ?2 AND p.event_sequence = ?3) \
             ON CONFLICT(owner, projector, event_sequence) DO UPDATE SET \
                disposition = 'quarantined', \
                reason = CASE WHEN disposition = 'quarantined' THEN reason ELSE excluded.reason END, \
                not_before_ms = NULL, \
                recorded_at_ms = MAX(recorded_at_ms, excluded.recorded_at_ms) \
             WHERE NOT EXISTS (SELECT 1 FROM agent_projector_progress p \
                WHERE p.owner = excluded.owner AND p.projector = excluded.projector \
                  AND p.event_sequence = excluded.event_sequence)",
            params![owner, projector, sequence, reason.as_str(), recorded_at_ms],
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()
    }
}

fn projection_event_source(
    connection: &Connection,
    owner: &str,
    event_id: &str,
) -> Result<(i64, i64)> {
    connection
        .query_row(
            "SELECT sequence, occurred_at_ms FROM agent_events \
             WHERE owner = ?1 AND event_id = ?2",
            params![owner, event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| AgentStoreError::NotFound {
            id: event_id.to_string(),
        })
}

fn new_run_record(owner: &str, run: &NewAgentRun, now: DateTime<Utc>) -> Result<AgentRunRecord> {
    let record = AgentRunRecord {
        owner: owner.to_string(),
        id: run.id.clone(),
        worker_id: run.worker_id.clone(),
        task_id: run.task_id.clone(),
        trace_id: run.trace_id.clone(),
        parent_run_id: run.parent_run_id.clone(),
        resume_of_run_id: None,
        execution_backend: run.execution_backend,
        state: RunState::Queued,
        mode: run.mode,
        target_key: run.target_key.clone(),
        prompt_digest: run.prompt_digest.clone(),
        policy_digest: run.policy_digest.clone(),
        resume_binding_digest: None,
        available_at: normalize_timestamp(run.available_at)?,
        deadline_at: None,
        timeout_seconds: run.timeout_seconds,
        max_resume_attempts: run.max_resume_attempts,
        resume_attempt: 0,
        created_at: now,
        started_at: None,
        updated_at: now,
        finished_at: None,
        revision: 0,
        worker_generation: 0,
        controller: None,
        lease: None,
        last_heartbeat_at: None,
        last_activity_at: None,
        failure_code: None,
    };
    validate_stored_run(&record)?;
    Ok(record)
}

fn ensure_worker_and_run_absent(
    connection: &Connection,
    owner: &str,
    worker_id: &str,
    run_id: &str,
) -> Result<()> {
    if get_worker(connection, owner, worker_id)?.is_some() {
        return Err(AgentStoreError::AlreadyExists {
            id: worker_id.to_string(),
        });
    }
    if get_run(connection, owner, run_id)?.is_some() {
        return Err(AgentStoreError::AlreadyExists {
            id: run_id.to_string(),
        });
    }
    Ok(())
}

fn validate_parent_run_exists(
    connection: &Connection,
    owner: &str,
    parent_run_id: Option<&str>,
) -> Result<()> {
    if let Some(parent_run_id) = parent_run_id {
        if get_run(connection, owner, parent_run_id)?.is_none() {
            return Err(AgentStoreError::NotFound {
                id: parent_run_id.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_parent_run_for_worker(
    connection: &Connection,
    owner: &str,
    parent_run_id: Option<&str>,
    expected_worker_id: &str,
) -> Result<()> {
    if let Some(parent_run_id) = parent_run_id {
        let parent_run = require_run(connection, owner, parent_run_id)?;
        if parent_run.worker_id != expected_worker_id {
            return Err(AgentStoreError::InvalidInput(
                "child parent run does not belong to its parent worker".into(),
            ));
        }
    }
    Ok(())
}

fn insert_worker(connection: &Connection, worker: &WorkerRecord) -> Result<()> {
    validate_stored_worker(worker)?;
    connection.execute(
        "INSERT INTO workers (owner, id, parent_id, logical_session_id, lifecycle, \
         created_at_ms, updated_at_ms, released_at_ms, revision, record_schema) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            worker.owner,
            worker.id,
            worker.parent_id,
            worker.logical_session_id,
            worker.lifecycle.to_string(),
            worker.created_at.timestamp_millis(),
            worker.updated_at.timestamp_millis(),
            worker.released_at.map(|value| value.timestamp_millis()),
            u64_to_i64(worker.revision, "worker revision")?,
            RECORD_SCHEMA,
        ],
    )?;
    Ok(())
}

fn update_worker(
    connection: &Connection,
    before: &WorkerRecord,
    after: &WorkerRecord,
) -> Result<()> {
    validate_stored_worker(after)?;
    if before.owner != after.owner
        || before.id != after.id
        || before.parent_id != after.parent_id
        || before.logical_session_id != after.logical_session_id
        || before.created_at != after.created_at
    {
        return Err(AgentStoreError::CorruptData(
            "worker immutable identity changed".into(),
        ));
    }
    let changed = connection.execute(
        "UPDATE workers SET lifecycle = ?1, updated_at_ms = ?2, released_at_ms = ?3, \
         revision = ?4 WHERE owner = ?5 AND id = ?6 AND revision = ?7",
        params![
            after.lifecycle.to_string(),
            after.updated_at.timestamp_millis(),
            after.released_at.map(|value| value.timestamp_millis()),
            u64_to_i64(after.revision, "worker revision")?,
            before.owner,
            before.id,
            u64_to_i64(before.revision, "worker revision")?,
        ],
    )?;
    ensure_one_change(changed, "worker update")
}

fn insert_run(connection: &Connection, run: &AgentRunRecord) -> Result<()> {
    validate_stored_run(run)?;
    let queue_sequence = next_queue_sequence(connection)?;
    connection.execute(
        "INSERT INTO agent_runs (owner, id, worker_id, task_id, trace_id, parent_run_id, \
         resume_of_run_id, state, mode, target_key, prompt_digest, policy_digest, \
         available_at_ms, timeout_ms, max_resume_attempts, resume_attempt, created_at_ms, \
         started_at_ms, updated_at_ms, finished_at_ms, revision, worker_generation, \
         controller_kind, controller_id, controller_fingerprint, lease_owner, \
         lease_expires_at_ms, lease_token_hash, last_heartbeat_at_ms, last_activity_at_ms, \
         failure_code, resume_binding_digest, deadline_at_ms, queue_sequence, record_schema, \
         execution_backend) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
         ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, NULL, \
         ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35)",
        params![
            run.owner,
            run.id,
            run.worker_id,
            run.task_id,
            run.trace_id,
            run.parent_run_id,
            run.resume_of_run_id,
            run.state.to_string(),
            run.mode.to_string(),
            run.target_key,
            run.prompt_digest,
            run.policy_digest,
            run.available_at.timestamp_millis(),
            timeout_millis(run.timeout_seconds)?,
            i64::from(run.max_resume_attempts),
            i64::from(run.resume_attempt),
            run.created_at.timestamp_millis(),
            run.started_at.map(|value| value.timestamp_millis()),
            run.updated_at.timestamp_millis(),
            run.finished_at.map(|value| value.timestamp_millis()),
            u64_to_i64(run.revision, "run revision")?,
            u64_to_i64(run.worker_generation, "worker generation")?,
            run.controller.as_ref().map(|value| value.kind.to_string()),
            run.controller.as_ref().map(|value| value.id.as_str()),
            run.controller
                .as_ref()
                .and_then(|value| value.fingerprint.as_deref()),
            run.lease.as_ref().map(|value| value.owner.as_str()),
            run.lease
                .as_ref()
                .map(|value| value.expires_at.timestamp_millis()),
            run.last_heartbeat_at.map(|value| value.timestamp_millis()),
            run.last_activity_at.map(|value| value.timestamp_millis()),
            run.failure_code.map(|value| value.to_string()),
            run.resume_binding_digest,
            run.deadline_at.map(|value| value.timestamp_millis()),
            u64_to_i64(queue_sequence, "run queue sequence")?,
            RECORD_SCHEMA,
            run.execution_backend.to_string(),
        ],
    )?;
    Ok(())
}

fn insert_completion(
    connection: &Connection,
    completion: &RunCompletionRecord,
    token_hash: &str,
) -> Result<()> {
    validate_stored_completion(completion)?;
    connection.execute(
        "INSERT INTO agent_run_completions (owner, run_id, worker_id, worker_generation, \
         completion_id, sink_kind, publication_key, content_digest, content_bytes, status, \
         token_hash, prepared_at_ms, prepared_run_revision, committed_at_ms, \
         committed_run_revision, abandoned_at_ms, abandoned_run_revision, \
         committed_by_operation_id, revision, record_schema, execution_backend) VALUES \
         (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
        params![
            completion.owner,
            completion.run_id,
            completion.worker_id,
            u64_to_i64(completion.worker_generation, "completion worker generation")?,
            completion.completion_id,
            completion.sink_kind,
            completion.publication_key,
            completion.content_digest,
            u64_to_i64(completion.content_bytes, "completion content bytes")?,
            completion.status.to_string(),
            token_hash,
            completion.prepared_at.timestamp_millis(),
            u64_to_i64(
                completion.prepared_run_revision,
                "completion prepared run revision"
            )?,
            completion
                .committed_at
                .map(|value| value.timestamp_millis()),
            completion
                .committed_run_revision
                .map(|value| u64_to_i64(value, "completion committed run revision"))
                .transpose()?,
            completion
                .abandoned_at
                .map(|value| value.timestamp_millis()),
            completion
                .abandoned_run_revision
                .map(|value| u64_to_i64(value, "completion abandoned run revision"))
                .transpose()?,
            completion.committed_by_operation_id,
            u64_to_i64(completion.revision, "completion revision")?,
            RECORD_SCHEMA,
            completion.execution_backend.to_string(),
        ],
    )?;
    Ok(())
}

fn update_completion(
    connection: &Connection,
    before: &RunCompletionRecord,
    after: &RunCompletionRecord,
) -> Result<()> {
    validate_stored_completion(after)?;
    if before.owner != after.owner
        || before.run_id != after.run_id
        || before.worker_id != after.worker_id
        || before.worker_generation != after.worker_generation
        || before.execution_backend != after.execution_backend
        || before.completion_id != after.completion_id
        || before.sink_kind != after.sink_kind
        || before.publication_key != after.publication_key
        || before.content_digest != after.content_digest
        || before.content_bytes != after.content_bytes
        || before.prepared_at != after.prepared_at
        || before.prepared_run_revision != after.prepared_run_revision
    {
        return Err(AgentStoreError::CorruptData(
            "completion immutable metadata changed".into(),
        ));
    }
    let changed = connection.execute(
        "UPDATE agent_run_completions SET status = ?1, committed_at_ms = ?2, \
         committed_run_revision = ?3, abandoned_at_ms = ?4, abandoned_run_revision = ?5, \
         committed_by_operation_id = ?6, revision = ?7 \
         WHERE owner = ?8 AND run_id = ?9 AND revision = ?10",
        params![
            after.status.to_string(),
            after.committed_at.map(|value| value.timestamp_millis()),
            after
                .committed_run_revision
                .map(|value| u64_to_i64(value, "completion committed run revision"))
                .transpose()?,
            after.abandoned_at.map(|value| value.timestamp_millis()),
            after
                .abandoned_run_revision
                .map(|value| u64_to_i64(value, "completion abandoned run revision"))
                .transpose()?,
            after.committed_by_operation_id,
            u64_to_i64(after.revision, "completion revision")?,
            before.owner,
            before.run_id,
            u64_to_i64(before.revision, "completion revision")?,
        ],
    )?;
    ensure_one_change(changed, "completion update")
}

fn update_run(
    connection: &Connection,
    before: &AgentRunRecord,
    after: &AgentRunRecord,
    lease_token_hash: Option<&str>,
) -> Result<()> {
    validate_stored_run(after)?;
    if before.owner != after.owner
        || before.id != after.id
        || before.worker_id != after.worker_id
        || before.task_id != after.task_id
        || before.trace_id != after.trace_id
        || before.parent_run_id != after.parent_run_id
        || before.resume_of_run_id != after.resume_of_run_id
        || before.execution_backend != after.execution_backend
        || before.mode != after.mode
        || before.target_key != after.target_key
        || before.prompt_digest != after.prompt_digest
        || before.policy_digest != after.policy_digest
        || before.available_at != after.available_at
        || before.timeout_seconds != after.timeout_seconds
        || before.max_resume_attempts != after.max_resume_attempts
        || before.resume_attempt != after.resume_attempt
        || before.created_at != after.created_at
    {
        return Err(AgentStoreError::CorruptData(
            "run immutable execution metadata changed".into(),
        ));
    }
    if before.resume_binding_digest.is_some()
        && before.resume_binding_digest != after.resume_binding_digest
    {
        return Err(AgentStoreError::CorruptData(
            "frozen resume binding changed or disappeared".into(),
        ));
    }
    match (before.deadline_at, after.deadline_at) {
        (None, None) => {}
        (None, Some(_))
            if before.state == RunState::Queued && after.state == RunState::Starting => {}
        (Some(before), Some(after)) if before == after => {}
        _ => {
            return Err(AgentStoreError::CorruptData(
                "fixed run deadline changed after claim".into(),
            ));
        }
    }
    let changed = connection.execute(
        "UPDATE agent_runs SET state = ?1, started_at_ms = ?2, updated_at_ms = ?3, \
         finished_at_ms = ?4, revision = ?5, worker_generation = ?6, controller_kind = ?7, \
         controller_id = ?8, controller_fingerprint = ?9, lease_owner = ?10, \
         lease_expires_at_ms = ?11, lease_token_hash = ?12, last_heartbeat_at_ms = ?13, \
         last_activity_at_ms = ?14, failure_code = ?15, resume_binding_digest = ?16, \
         deadline_at_ms = ?17 WHERE owner = ?18 AND id = ?19 AND revision = ?20",
        params![
            after.state.to_string(),
            after.started_at.map(|value| value.timestamp_millis()),
            after.updated_at.timestamp_millis(),
            after.finished_at.map(|value| value.timestamp_millis()),
            u64_to_i64(after.revision, "run revision")?,
            u64_to_i64(after.worker_generation, "worker generation")?,
            after
                .controller
                .as_ref()
                .map(|value| value.kind.to_string()),
            after.controller.as_ref().map(|value| value.id.as_str()),
            after
                .controller
                .as_ref()
                .and_then(|value| value.fingerprint.as_deref()),
            after.lease.as_ref().map(|value| value.owner.as_str()),
            after
                .lease
                .as_ref()
                .map(|value| value.expires_at.timestamp_millis()),
            lease_token_hash,
            after
                .last_heartbeat_at
                .map(|value| value.timestamp_millis()),
            after.last_activity_at.map(|value| value.timestamp_millis()),
            after.failure_code.map(|value| value.to_string()),
            after.resume_binding_digest,
            after.deadline_at.map(|value| value.timestamp_millis()),
            before.owner,
            before.id,
            u64_to_i64(before.revision, "run revision")?,
        ],
    )?;
    ensure_one_change(changed, "run update")
}

fn insert_event(
    connection: &Connection,
    worker: &WorkerRecord,
    run: Option<&AgentRunRecord>,
    kind: AgentEventKind,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    if run.is_some_and(|run| run.owner != worker.owner || run.worker_id != worker.id) {
        return Err(AgentStoreError::CorruptData(
            "event source authority does not match its worker".into(),
        ));
    }
    let event_id = Uuid::now_v7().to_string();
    let sequence = next_owner_event_sequence(connection, &worker.owner)?;
    connection.execute(
        "INSERT INTO agent_events (owner, sequence, event_id, worker_id, run_id, occurred_at_ms, \
         event_type, worker_revision, run_revision, run_state, worker_lifecycle) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            worker.owner,
            u64_to_i64(sequence, "owner event sequence")?,
            event_id,
            worker.id,
            run.map(|value| value.id.as_str()),
            occurred_at.timestamp_millis(),
            kind.to_string(),
            u64_to_i64(worker.revision, "event worker revision")?,
            run.map(|value| u64_to_i64(value.revision, "event run revision"))
                .transpose()?,
            run.map(|value| value.state.to_string()),
            worker.lifecycle.to_string(),
        ],
    )?;
    Ok(())
}

fn next_owner_event_sequence(connection: &Connection, owner: &str) -> Result<u64> {
    let current: Option<i64> = connection
        .query_row(
            "SELECT next_sequence FROM owner_agent_event_sequences WHERE owner = ?1",
            params![owner],
            |row| row.get(0),
        )
        .optional()?;
    match current {
        Some(current) => {
            let sequence = u64::try_from(current).map_err(|_| {
                AgentStoreError::CorruptData("owner event sequence is invalid".into())
            })?;
            let next = next_u64(sequence, "owner event sequence")?;
            ensure_one_change(
                connection.execute(
                    "UPDATE owner_agent_event_sequences SET next_sequence = ?1 \
                     WHERE owner = ?2 AND next_sequence = ?3",
                    params![u64_to_i64(next, "owner event sequence")?, owner, current],
                )?,
                "owner event sequence allocation",
            )?;
            Ok(sequence)
        }
        None => {
            connection.execute(
                "INSERT INTO owner_agent_event_sequences (owner, next_sequence) VALUES (?1, 2)",
                params![owner],
            )?;
            Ok(1)
        }
    }
}

fn get_worker(
    connection: &Connection,
    owner: &str,
    worker_id: &str,
) -> Result<Option<WorkerRecord>> {
    let sql = format!("SELECT {WORKER_COLUMNS} FROM workers WHERE owner = ?1 AND id = ?2");
    Ok(connection
        .query_row(&sql, params![owner, worker_id], row_to_worker)
        .optional()?)
}

fn require_worker(connection: &Connection, owner: &str, worker_id: &str) -> Result<WorkerRecord> {
    get_worker(connection, owner, worker_id)?.ok_or_else(|| AgentStoreError::NotFound {
        id: worker_id.to_string(),
    })
}

fn get_run(connection: &Connection, owner: &str, run_id: &str) -> Result<Option<AgentRunRecord>> {
    let sql = format!("SELECT {RUN_COLUMNS} FROM agent_runs WHERE owner = ?1 AND id = ?2");
    Ok(connection
        .query_row(&sql, params![owner, run_id], row_to_run)
        .optional()?)
}

fn require_run(connection: &Connection, owner: &str, run_id: &str) -> Result<AgentRunRecord> {
    get_run(connection, owner, run_id)?.ok_or_else(|| AgentStoreError::NotFound {
        id: run_id.to_string(),
    })
}

fn get_completion(
    connection: &Connection,
    owner: &str,
    run_id: &str,
) -> Result<Option<RunCompletionRecord>> {
    let sql = format!(
        "SELECT {COMPLETION_COLUMNS} FROM agent_run_completions WHERE owner = ?1 AND run_id = ?2"
    );
    Ok(connection
        .query_row(&sql, params![owner, run_id], row_to_completion)
        .optional()?)
}

fn row_to_worker(row: &Row<'_>) -> rusqlite::Result<WorkerRecord> {
    let lifecycle_raw: String = row.get(4)?;
    let schema: i64 = row.get(9)?;
    if schema != i64::from(RECORD_SCHEMA) {
        return Err(data_error(9, Type::Integer, "unsupported worker schema"));
    }
    let record = WorkerRecord {
        owner: row.get(0)?,
        id: row.get(1)?,
        parent_id: row.get(2)?,
        logical_session_id: row.get(3)?,
        lifecycle: parse_enum(4, &lifecycle_raw)?,
        created_at: stored_timestamp(row, 5, "worker created_at")?,
        updated_at: stored_timestamp(row, 6, "worker updated_at")?,
        released_at: optional_timestamp(row, 7, "worker released_at")?,
        revision: stored_u64(row, 8, "worker revision")?,
    };
    validate_stored_worker(&record)
        .map_err(|_| data_error(0, Type::Text, "stored worker violates its contract"))?;
    Ok(record)
}

fn row_to_run(row: &Row<'_>) -> rusqlite::Result<AgentRunRecord> {
    let state_raw: String = row.get(7)?;
    let mode_raw: String = row.get(8)?;
    let controller_kind_raw: Option<String> = row.get(22)?;
    let controller_id: Option<String> = row.get(23)?;
    let controller_fingerprint: Option<String> = row.get(24)?;
    let lease_owner: Option<String> = row.get(25)?;
    let lease_expires_at = optional_timestamp(row, 26, "run lease expires_at")?;
    let failure_raw: Option<String> = row.get(30)?;
    let schema: i64 = row.get(33)?;
    let execution_backend_raw: String = row.get(34)?;
    if schema != i64::from(RECORD_SCHEMA) {
        return Err(data_error(33, Type::Integer, "unsupported run schema"));
    }
    let controller = match (controller_kind_raw, controller_id) {
        (Some(kind), Some(id)) => Some(ControllerRef {
            kind: parse_enum(22, &kind)?,
            id,
            fingerprint: controller_fingerprint,
        }),
        (None, None) if controller_fingerprint.is_none() => None,
        _ => return Err(data_error(22, Type::Text, "partial controller identity")),
    };
    let lease = match (lease_owner, lease_expires_at) {
        (Some(owner), Some(expires_at)) => Some(RunLease { owner, expires_at }),
        (None, None) => None,
        _ => return Err(data_error(25, Type::Text, "partial run lease")),
    };
    let timeout_ms = stored_u64(row, 13, "run timeout")?;
    if timeout_ms == 0 || timeout_ms % 1_000 != 0 {
        return Err(data_error(13, Type::Integer, "invalid run timeout"));
    }
    let timeout_seconds = timeout_ms / 1_000;
    let record = AgentRunRecord {
        owner: row.get(0)?,
        id: row.get(1)?,
        worker_id: row.get(2)?,
        task_id: row.get(3)?,
        trace_id: row.get(4)?,
        parent_run_id: row.get(5)?,
        resume_of_run_id: row.get(6)?,
        execution_backend: parse_enum(34, &execution_backend_raw)?,
        state: parse_enum(7, &state_raw)?,
        mode: parse_enum(8, &mode_raw)?,
        target_key: row.get(9)?,
        prompt_digest: row.get(10)?,
        policy_digest: row.get(11)?,
        resume_binding_digest: row.get(31)?,
        available_at: stored_timestamp(row, 12, "run available_at")?,
        deadline_at: optional_timestamp(row, 32, "run deadline")?,
        timeout_seconds,
        max_resume_attempts: stored_u32(row, 14, "max resume attempts")?,
        resume_attempt: stored_u32(row, 15, "resume attempt")?,
        created_at: stored_timestamp(row, 16, "run created_at")?,
        started_at: optional_timestamp(row, 17, "run started_at")?,
        updated_at: stored_timestamp(row, 18, "run updated_at")?,
        finished_at: optional_timestamp(row, 19, "run finished_at")?,
        revision: stored_u64(row, 20, "run revision")?,
        worker_generation: stored_u64(row, 21, "worker generation")?,
        controller,
        lease,
        last_heartbeat_at: optional_timestamp(row, 28, "run heartbeat")?,
        last_activity_at: optional_timestamp(row, 29, "run activity")?,
        failure_code: failure_raw
            .as_deref()
            .map(|value| parse_enum(30, value))
            .transpose()?,
    };
    validate_stored_run(&record)
        .map_err(|_| data_error(0, Type::Text, "stored run violates its contract"))?;
    Ok(record)
}

fn row_to_event(row: &Row<'_>) -> rusqlite::Result<AgentEvent> {
    let kind_raw: String = row.get(6)?;
    let run_state_raw: Option<String> = row.get(9)?;
    let worker_lifecycle_raw: String = row.get(10)?;
    let event = AgentEvent {
        sequence: stored_u64(row, 0, "event sequence")?,
        event_id: row.get(1)?,
        owner: row.get(2)?,
        worker_id: row.get(3)?,
        run_id: row.get(4)?,
        occurred_at: stored_timestamp(row, 5, "event occurred_at")?,
        kind: parse_enum(6, &kind_raw)?,
        worker_revision: stored_u64(row, 7, "event worker revision")?,
        run_revision: optional_u64(row, 8, "event run revision")?,
        run_state: run_state_raw
            .as_deref()
            .map(|value| parse_enum(9, value))
            .transpose()?,
        worker_lifecycle: parse_enum(10, &worker_lifecycle_raw)?,
    };
    validate_stored_event(&event)
        .map_err(|_| data_error(0, Type::Text, "stored event violates its contract"))?;
    Ok(event)
}

fn row_to_completion(row: &Row<'_>) -> rusqlite::Result<RunCompletionRecord> {
    let status_raw: String = row.get(9)?;
    let record_schema: u32 = stored_u32(row, 18, "completion record schema")?;
    let execution_backend_raw: String = row.get(19)?;
    if record_schema != RECORD_SCHEMA {
        return Err(data_error(
            18,
            Type::Integer,
            "unsupported completion record schema",
        ));
    }
    let record = RunCompletionRecord {
        owner: row.get(0)?,
        run_id: row.get(1)?,
        worker_id: row.get(2)?,
        worker_generation: stored_u64(row, 3, "completion worker generation")?,
        execution_backend: parse_enum(19, &execution_backend_raw)?,
        completion_id: row.get(4)?,
        sink_kind: row.get(5)?,
        publication_key: row.get(6)?,
        content_digest: row.get(7)?,
        content_bytes: stored_u64(row, 8, "completion content bytes")?,
        status: parse_enum(9, &status_raw)?,
        prepared_at: stored_timestamp(row, 10, "completion prepared_at")?,
        prepared_run_revision: stored_u64(row, 11, "completion prepared run revision")?,
        committed_at: optional_timestamp(row, 12, "completion committed_at")?,
        committed_run_revision: optional_u64(row, 13, "completion committed run revision")?,
        abandoned_at: optional_timestamp(row, 14, "completion abandoned_at")?,
        abandoned_run_revision: optional_u64(row, 15, "completion abandoned run revision")?,
        committed_by_operation_id: row.get(16)?,
        revision: stored_u64(row, 17, "completion revision")?,
    };
    validate_stored_completion(&record)
        .map_err(|_| data_error(0, Type::Text, "stored completion violates its contract"))?;
    Ok(record)
}

fn validate_stored_worker(worker: &WorkerRecord) -> Result<()> {
    validate_owner(&worker.owner)?;
    validate_text("worker id", &worker.id, MAX_ID_BYTES)?;
    validate_optional_text(
        "parent worker id",
        worker.parent_id.as_deref(),
        MAX_ID_BYTES,
    )?;
    validate_optional_text(
        "logical session id",
        worker.logical_session_id.as_deref(),
        512,
    )?;
    if worker.parent_id.as_deref() == Some(worker.id.as_str()) {
        return Err(AgentStoreError::CorruptData(
            "worker is its own parent".into(),
        ));
    }
    if worker.updated_at < worker.created_at {
        return Err(AgentStoreError::CorruptData(
            "worker updated_at predates creation".into(),
        ));
    }
    match worker.lifecycle {
        WorkerLifecycle::Released => {
            if worker
                .released_at
                .is_none_or(|value| value < worker.created_at || value > worker.updated_at)
            {
                return Err(AgentStoreError::CorruptData(
                    "released worker has invalid release time".into(),
                ));
            }
        }
        WorkerLifecycle::Open | WorkerLifecycle::Draining => {
            if worker.released_at.is_some() {
                return Err(AgentStoreError::CorruptData(
                    "non-released worker has release time".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_stored_completion(completion: &RunCompletionRecord) -> Result<()> {
    validate_owner(&completion.owner)?;
    validate_text("completion run id", &completion.run_id, MAX_ID_BYTES)?;
    validate_text("completion worker id", &completion.worker_id, MAX_ID_BYTES)?;
    validate_text("completion id", &completion.completion_id, MAX_ID_BYTES)?;
    validate_text(
        "completion sink kind",
        &completion.sink_kind,
        MAX_COMPLETION_KIND_BYTES,
    )?;
    validate_opaque_key("completion sink kind", &completion.sink_kind)?;
    validate_text(
        "completion publication key",
        &completion.publication_key,
        MAX_PUBLICATION_KEY_BYTES,
    )?;
    validate_opaque_key("completion publication key", &completion.publication_key)?;
    validate_digest("completion content digest", &completion.content_digest)?;
    validate_optional_text(
        "completion commit operation id",
        completion.committed_by_operation_id.as_deref(),
        MAX_ID_BYTES,
    )?;
    if completion.worker_generation == 0 || completion.prepared_run_revision == 0 {
        return Err(AgentStoreError::CorruptData(
            "completion has a zero preparation fence".into(),
        ));
    }
    match completion.status {
        RunCompletionStatus::Prepared => {
            if completion.committed_at.is_some()
                || completion.committed_run_revision.is_some()
                || completion.abandoned_at.is_some()
                || completion.abandoned_run_revision.is_some()
                || completion.committed_by_operation_id.is_some()
            {
                return Err(AgentStoreError::CorruptData(
                    "prepared completion carries terminal metadata".into(),
                ));
            }
        }
        RunCompletionStatus::Committed => {
            if completion.committed_at.is_none()
                || completion
                    .committed_run_revision
                    .is_none_or(|revision| revision == 0)
                || completion.abandoned_at.is_some()
                || completion.abandoned_run_revision.is_some()
            {
                return Err(AgentStoreError::CorruptData(
                    "committed completion has invalid terminal metadata".into(),
                ));
            }
        }
        RunCompletionStatus::Abandoned => {
            if completion.committed_at.is_some()
                || completion.committed_run_revision.is_some()
                || completion.abandoned_at.is_none()
                || completion.abandoned_run_revision.is_none()
                || completion.committed_by_operation_id.is_some()
            {
                return Err(AgentStoreError::CorruptData(
                    "abandoned completion has invalid terminal metadata".into(),
                ));
            }
        }
    }
    for terminal in [completion.committed_at, completion.abandoned_at]
        .into_iter()
        .flatten()
    {
        if terminal < completion.prepared_at {
            return Err(AgentStoreError::CorruptData(
                "completion terminal timestamp predates preparation".into(),
            ));
        }
    }
    Ok(())
}

fn validate_stored_run(run: &AgentRunRecord) -> Result<()> {
    validate_owner(&run.owner)?;
    validate_text("run id", &run.id, MAX_ID_BYTES)?;
    validate_text("run worker id", &run.worker_id, MAX_ID_BYTES)?;
    validate_optional_text("task id", run.task_id.as_deref(), 512)?;
    validate_optional_text("trace id", run.trace_id.as_deref(), 512)?;
    validate_optional_text("parent run id", run.parent_run_id.as_deref(), 512)?;
    validate_optional_text("resume source run id", run.resume_of_run_id.as_deref(), 512)?;
    validate_text("target key", &run.target_key, 512)?;
    validate_digest("prompt digest", &run.prompt_digest)?;
    validate_digest("policy digest", &run.policy_digest)?;
    if let Some(binding) = &run.resume_binding_digest {
        validate_digest("resume binding digest", binding)?;
    }
    if run.timeout_seconds == 0 || run.timeout_seconds > MAX_TIMEOUT_SECONDS {
        return Err(AgentStoreError::CorruptData(
            "run timeout violates its bound".into(),
        ));
    }
    if run.max_resume_attempts > crate::model::MAX_RESUME_ATTEMPTS
        || run.resume_attempt > run.max_resume_attempts
    {
        return Err(AgentStoreError::CorruptData(
            "run resume counters are invalid".into(),
        ));
    }
    if run.resume_of_run_id.is_none() != (run.resume_attempt == 0) {
        return Err(AgentStoreError::CorruptData(
            "run resume link contradicts its attempt counter".into(),
        ));
    }
    if run.updated_at < run.created_at {
        return Err(AgentStoreError::CorruptData(
            "run updated_at predates creation".into(),
        ));
    }
    if run.deadline_at.is_some_and(|value| value < run.created_at) {
        return Err(AgentStoreError::CorruptData(
            "run deadline predates creation".into(),
        ));
    }
    for (label, timestamp) in [
        ("started_at", run.started_at),
        ("finished_at", run.finished_at),
        ("last_heartbeat_at", run.last_heartbeat_at),
        ("last_activity_at", run.last_activity_at),
    ] {
        if timestamp.is_some_and(|value| value < run.created_at || value > run.updated_at) {
            return Err(AgentStoreError::CorruptData(format!(
                "run {label} is outside its lifecycle"
            )));
        }
    }
    if let Some(controller) = &run.controller {
        controller.validate()?;
    }
    if let Some(lease) = &run.lease {
        validate_text("run lease owner", &lease.owner, 256)?;
        if lease.expires_at <= run.updated_at {
            return Err(AgentStoreError::CorruptData(
                "run lease is not live at its update time".into(),
            ));
        }
    }

    match run.state {
        RunState::Queued => {
            if run.worker_generation != 0
                || run.started_at.is_some()
                || run.finished_at.is_some()
                || run.controller.is_some()
                || run.lease.is_some()
                || run.last_heartbeat_at.is_some()
                || run.last_activity_at.is_some()
                || run.failure_code.is_some()
                || run.deadline_at.is_some()
            {
                return Err(AgentStoreError::CorruptData(
                    "queued run carries active or terminal state".into(),
                ));
            }
        }
        RunState::Starting => {
            if run.worker_generation == 0
                || run.started_at.is_some()
                || run.finished_at.is_some()
                || run.controller.is_some()
                || run.lease.is_none()
                || run.failure_code.is_some()
                || run.deadline_at.is_none()
            {
                return Err(AgentStoreError::CorruptData(
                    "starting run has inconsistent controller state".into(),
                ));
            }
        }
        RunState::Running => {
            if run.worker_generation == 0
                || run.started_at.is_none()
                || run.finished_at.is_some()
                || run.controller.is_none()
                || run.lease.is_none()
                || run.last_heartbeat_at.is_none()
                || run.last_activity_at.is_none()
                || run.failure_code.is_some()
                || run.deadline_at.is_none()
            {
                return Err(AgentStoreError::CorruptData(
                    "running run has inconsistent controller state".into(),
                ));
            }
        }
        RunState::Cancelling => {
            if run.worker_generation == 0
                || run.finished_at.is_some()
                || run.lease.is_some()
                || run.failure_code.is_some()
                || run.deadline_at.is_none()
            {
                return Err(AgentStoreError::CorruptData(
                    "cancelling run has inconsistent control state".into(),
                ));
            }
        }
        RunState::Succeeded => {
            validate_terminal(run, None)?;
        }
        RunState::Failed => {
            let code = run.failure_code.ok_or_else(|| {
                AgentStoreError::CorruptData("failed run lacks failure code".into())
            })?;
            if matches!(code, RunFailureCode::TimedOut | RunFailureCode::Cancelled) {
                return Err(AgentStoreError::CorruptData(
                    "failed run uses a dedicated terminal code".into(),
                ));
            }
            validate_terminal(run, Some(code))?;
        }
        RunState::TimedOut => validate_terminal(run, Some(RunFailureCode::TimedOut))?,
        RunState::Cancelled => validate_terminal(run, Some(RunFailureCode::Cancelled))?,
        RunState::Interrupted => {
            if run.failure_code.is_none() {
                return Err(AgentStoreError::CorruptData(
                    "interrupted run lacks failure code".into(),
                ));
            }
            validate_terminal(run, run.failure_code)?;
        }
    }
    Ok(())
}

fn validate_terminal(run: &AgentRunRecord, expected: Option<RunFailureCode>) -> Result<()> {
    if run.finished_at.is_none() || run.controller.is_some() || run.lease.is_some() {
        return Err(AgentStoreError::CorruptData(
            "terminal run retains active controller state".into(),
        ));
    }
    if run.failure_code != expected {
        return Err(AgentStoreError::CorruptData(
            "terminal run failure code contradicts its state".into(),
        ));
    }
    Ok(())
}

fn validate_stored_event(event: &AgentEvent) -> Result<()> {
    if event.sequence == 0 {
        return Err(AgentStoreError::CorruptData(
            "event sequence is zero".into(),
        ));
    }
    validate_text("event id", &event.event_id, MAX_ID_BYTES)?;
    validate_owner(&event.owner)?;
    validate_text("event worker id", &event.worker_id, MAX_ID_BYTES)?;
    validate_optional_text("event run id", event.run_id.as_deref(), MAX_ID_BYTES)?;
    if event.run_id.is_some() != event.run_revision.is_some()
        || event.run_id.is_some() != event.run_state.is_some()
    {
        return Err(AgentStoreError::CorruptData(
            "event run fields are only partially present".into(),
        ));
    }
    Ok(())
}

fn authenticated_run(
    connection: &Connection,
    owner: &str,
    receipt: &RunLeaseReceipt,
    now: DateTime<Utc>,
    states: &[RunState],
) -> Result<(AgentRunRecord, String)> {
    let run = require_run(connection, owner, &receipt.run_id)?;
    let stored_token: Option<String> = connection.query_row(
        "SELECT lease_token_hash FROM agent_runs WHERE owner = ?1 AND id = ?2",
        params![owner, receipt.run_id],
        |row| row.get(0),
    )?;
    let supplied_hash = token_hash(&receipt.token);
    let lease_matches = run
        .lease
        .as_ref()
        .is_some_and(|lease| lease.owner == receipt.lease_owner && lease.expires_at > now);
    let deadline_is_live = run.deadline_at.is_some_and(|deadline| deadline > now);
    if run.worker_id != receipt.worker_id
        || run.worker_generation != receipt.generation
        || run.revision != receipt.revision
        || !states.contains(&run.state)
        || !lease_matches
        || !deadline_is_live
        || stored_token
            .as_deref()
            .is_none_or(|stored| !constant_time_eq(stored.as_bytes(), supplied_hash.as_bytes()))
    {
        return Err(AgentStoreError::InvalidReceipt {
            id: receipt.run_id.clone(),
        });
    }
    Ok((run, stored_token.unwrap_or_default()))
}

fn authenticated_completion(
    connection: &Connection,
    owner: &str,
    permit: &ActiveCompletionPermit,
) -> Result<RunCompletionRecord> {
    let completion = get_completion(connection, owner, permit.run_id())?.ok_or_else(|| {
        AgentStoreError::InvalidCompletionPermit {
            id: permit.run_id().to_string(),
        }
    })?;
    let stored_token: String = connection.query_row(
        "SELECT token_hash FROM agent_run_completions WHERE owner = ?1 AND run_id = ?2",
        params![owner, permit.run_id()],
        |row| row.get(0),
    )?;
    let supplied_hash = token_hash(permit.token());
    if permit.owner() != owner
        || completion.worker_id != permit.worker_id()
        || completion.worker_generation != permit.generation()
        || completion.completion_id != permit.completion_id()
        || !constant_time_eq(stored_token.as_bytes(), supplied_hash.as_bytes())
    {
        return Err(AgentStoreError::InvalidCompletionPermit {
            id: permit.run_id().to_string(),
        });
    }
    Ok(completion)
}

fn exact_completion_metadata(record: &RunCompletionRecord, proposed: &NewRunCompletion) -> bool {
    record.completion_id == proposed.id
        && record.sink_kind == proposed.sink_kind
        && record.publication_key == proposed.publication_key
        && record.content_digest == proposed.content_digest
        && record.content_bytes == proposed.content_bytes
}

fn abandon_completion_if_prepared(
    connection: &Connection,
    owner: &str,
    run_id: &str,
    now: DateTime<Utc>,
    terminal_run_revision: u64,
) -> Result<bool> {
    let Some(before) = get_completion(connection, owner, run_id)? else {
        return Ok(false);
    };
    if before.status != RunCompletionStatus::Prepared {
        return Ok(false);
    }
    let mut after = before.clone();
    after.status = RunCompletionStatus::Abandoned;
    after.abandoned_at = Some(now);
    after.abandoned_run_revision = Some(terminal_run_revision);
    after.revision = next_u64(before.revision, "completion revision")?;
    update_completion(connection, &before, &after)?;
    Ok(true)
}

fn execution_permit_error(error: AgentStoreError, run_id: &str) -> AgentStoreError {
    match error {
        AgentStoreError::NotFound { .. }
        | AgentStoreError::InvalidReceipt { .. }
        | AgentStoreError::InvalidExecutionPermit { .. } => {
            AgentStoreError::InvalidExecutionPermit {
                id: run_id.to_string(),
            }
        }
        error => error,
    }
}

fn cancel_tree_plan_digest(
    worker_ids: &[String],
    run_entries: &[CancelTreeRunEntry],
) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"vyane-agent-cancel-tree-plan-v1\0");
    hasher.update(
        u64::try_from(worker_ids.len())
            .map_err(|_| AgentStoreError::InvalidInput("too many cancel workers".into()))?
            .to_be_bytes(),
    );
    hasher.update(
        u64::try_from(run_entries.len())
            .map_err(|_| AgentStoreError::InvalidInput("too many cancel runs".into()))?
            .to_be_bytes(),
    );
    for value in worker_ids {
        hasher.update(b"w");
        let length = u64::try_from(value.len()).map_err(|_| {
            AgentStoreError::InvalidInput("cancel tree plan field is too large".into())
        })?;
        hasher.update(length.to_be_bytes());
        hasher.update(value.as_bytes());
    }
    for entry in run_entries {
        hasher.update(b"r");
        for value in [
            entry.worker_id.as_str(),
            entry.run_id.as_str(),
            entry.action.as_str(),
        ] {
            let length = u64::try_from(value.len()).map_err(|_| {
                AgentStoreError::InvalidInput("cancel tree plan field is too large".into())
            })?;
            hasher.update(length.to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn load_cancel_tree_header(
    connection: &Connection,
    owner: &str,
    operation_id: &str,
) -> Result<Option<CancelTreeHeader>> {
    connection
        .query_row(
            "SELECT root_worker_id, plan_digest, worker_count, run_count, lease_owner, \
             lease_seconds FROM cancel_tree_operations WHERE owner = ?1 AND operation_id = ?2",
            params![owner, operation_id],
            |row| {
                let worker_count = stored_u64(row, 2, "cancel tree worker count")?;
                let run_count = stored_u64(row, 3, "cancel tree run count")?;
                Ok(CancelTreeHeader {
                    root_worker_id: row.get(0)?,
                    plan_digest: row.get(1)?,
                    worker_count: usize::try_from(worker_count).map_err(|_| {
                        data_error(2, Type::Integer, "cancel tree worker count is too large")
                    })?,
                    run_count: usize::try_from(run_count).map_err(|_| {
                        data_error(3, Type::Integer, "cancel tree run count is too large")
                    })?,
                    lease_owner: row.get(4)?,
                    lease_seconds: stored_u64(row, 5, "cancel tree lease seconds")?,
                })
            },
        )
        .optional()
        .map_err(AgentStoreError::from)
}

fn load_cancel_tree_workers(
    connection: &Connection,
    owner: &str,
    operation_id: &str,
) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT worker_id FROM cancel_tree_operation_workers \
         WHERE owner = ?1 AND operation_id = ?2 ORDER BY ordinal",
    )?;
    Ok(statement
        .query_map(params![owner, operation_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn load_cancel_tree_runs(
    connection: &Connection,
    owner: &str,
    operation_id: &str,
) -> Result<Vec<CancelTreeRunEntry>> {
    let mut statement = connection.prepare(
        "SELECT worker_id, run_id, action FROM cancel_tree_operation_runs \
         WHERE owner = ?1 AND operation_id = ?2 ORDER BY ordinal",
    )?;
    let rows = statement.query_map(params![owner, operation_id], |row| {
        let action: String = row.get(2)?;
        let action = match action.as_str() {
            "queued_cancel" => CancelTreeRunAction::QueuedCancel,
            "controller_cancel" => CancelTreeRunAction::ControllerCancel,
            _ => return Err(data_error(2, Type::Text, "invalid cancel tree run action")),
        };
        Ok(CancelTreeRunEntry {
            worker_id: row.get(0)?,
            run_id: row.get(1)?,
            action,
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn insert_cancel_tree_plan(
    connection: &Connection,
    owner: &str,
    operation_id: &str,
    header: &CancelTreeHeader,
    worker_ids: &[String],
    run_entries: &[CancelTreeRunEntry],
    now: DateTime<Utc>,
) -> Result<()> {
    connection.execute(
        "INSERT INTO cancel_tree_operations (owner, operation_id, root_worker_id, plan_digest, \
         worker_count, run_count, lease_owner, lease_seconds, created_at_ms, record_schema) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            owner,
            operation_id,
            header.root_worker_id,
            header.plan_digest,
            usize_to_i64(header.worker_count, "cancel tree worker count")?,
            usize_to_i64(header.run_count, "cancel tree run count")?,
            header.lease_owner,
            u64_to_i64(header.lease_seconds, "cancel tree lease seconds")?,
            now.timestamp_millis(),
            RECORD_SCHEMA,
        ],
    )?;
    for (ordinal, worker_id) in worker_ids.iter().enumerate() {
        connection.execute(
            "INSERT INTO cancel_tree_operation_workers \
             (owner, operation_id, worker_id, ordinal) VALUES (?1, ?2, ?3, ?4)",
            params![
                owner,
                operation_id,
                worker_id,
                usize_to_i64(ordinal, "cancel tree worker ordinal")?
            ],
        )?;
    }
    for (ordinal, entry) in run_entries.iter().enumerate() {
        connection.execute(
            "INSERT INTO cancel_tree_operation_runs \
             (owner, operation_id, run_id, worker_id, action, ordinal) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                owner,
                operation_id,
                entry.run_id,
                entry.worker_id,
                entry.action.as_str(),
                usize_to_i64(ordinal, "cancel tree run ordinal")?
            ],
        )?;
    }
    Ok(())
}

fn active_control_operation(
    connection: &Connection,
    owner: &str,
    run_id: &str,
) -> Result<Option<ActiveControlOperation>> {
    let row = connection
        .query_row(
            "SELECT operation_id, operation_kind, worker_generation, run_revision, \
             controller_kind, controller_id, controller_fingerprint, token_hash, lease_owner, \
             lease_expires_at_ms FROM run_control_operations \
             WHERE owner = ?1 AND run_id = ?2 AND status = 'active'",
            params![owner, run_id],
            |row| {
                let operation_kind: String = row.get(1)?;
                let controller_kind: Option<String> = row.get(4)?;
                let controller_id: Option<String> = row.get(5)?;
                let controller_fingerprint: Option<String> = row.get(6)?;
                let controller = match (controller_kind, controller_id) {
                    (Some(kind), Some(id)) => Some(ControllerRef {
                        kind: parse_enum::<ControllerKind>(4, &kind)?,
                        id,
                        fingerprint: controller_fingerprint,
                    }),
                    (None, None) if controller_fingerprint.is_none() => None,
                    _ => return Err(data_error(4, Type::Text, "partial control controller")),
                };
                Ok(ActiveControlOperation {
                    operation_id: row.get(0)?,
                    kind: parse_enum(1, &operation_kind)?,
                    generation: stored_u64(row, 2, "control generation")?,
                    revision: stored_u64(row, 3, "control run revision")?,
                    controller,
                    token_hash: row.get(7)?,
                    lease_owner: row.get(8)?,
                    expires_at: stored_timestamp(row, 9, "control lease expiry")?,
                })
            },
        )
        .optional()?;
    if let Some(operation) = &row {
        validate_text(
            "control operation id",
            &operation.operation_id,
            MAX_ID_BYTES,
        )?;
        validate_text("control lease owner", &operation.lease_owner, 256)?;
        validate_digest("control token hash", &operation.token_hash)?;
        if operation.generation == 0 || operation.revision == 0 {
            return Err(AgentStoreError::CorruptData(
                "control operation has an invalid fence".into(),
            ));
        }
        if let Some(controller) = &operation.controller {
            controller.validate()?;
        }
    }
    Ok(row)
}

fn insert_control_operation(
    connection: &Connection,
    owner: &str,
    run_id: &str,
    operation: &ActiveControlOperation,
    now: DateTime<Utc>,
) -> Result<()> {
    connection.execute(
        "INSERT INTO run_control_operations (owner, run_id, operation_id, operation_kind, status, \
         worker_generation, run_revision, controller_kind, controller_id, \
         controller_fingerprint, token_hash, lease_owner, lease_expires_at_ms, created_at_ms, \
         updated_at_ms, settled_at_ms, record_schema) VALUES (?1, ?2, ?3, ?4, 'active', ?5, \
         ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13, NULL, ?14)",
        params![
            owner,
            run_id,
            operation.operation_id,
            operation.kind.as_str(),
            u64_to_i64(operation.generation, "control generation")?,
            u64_to_i64(operation.revision, "control run revision")?,
            operation
                .controller
                .as_ref()
                .map(|controller| controller.kind.to_string()),
            operation
                .controller
                .as_ref()
                .map(|controller| controller.id.as_str()),
            operation
                .controller
                .as_ref()
                .and_then(|controller| controller.fingerprint.as_deref()),
            operation.token_hash,
            operation.lease_owner,
            operation.expires_at.timestamp_millis(),
            now.timestamp_millis(),
            RECORD_SCHEMA,
        ],
    )?;
    Ok(())
}

fn finish_control_operation(
    connection: &Connection,
    owner: &str,
    run_id: &str,
    operation_id: &str,
    status: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    if !matches!(status, "settled" | "superseded") {
        return Err(AgentStoreError::CorruptData(
            "invalid control operation terminal status".into(),
        ));
    }
    ensure_one_change(
        connection.execute(
            "UPDATE run_control_operations SET status = ?1, updated_at_ms = ?2, \
             settled_at_ms = ?2 WHERE owner = ?3 AND run_id = ?4 AND operation_id = ?5 \
             AND status = 'active'",
            params![status, now.timestamp_millis(), owner, run_id, operation_id],
        )?,
        "control operation settlement",
    )
}

fn control_ticket_matches_cancel(
    operation: &ActiveControlOperation,
    ticket: &CancelTicket,
    now: DateTime<Utc>,
) -> bool {
    operation.kind == ControlOperationKind::Cancel
        && operation.operation_id == ticket.operation_id
        && operation.generation == ticket.generation
        && operation.revision == ticket.revision
        && operation.controller == ticket.controller
        && operation.lease_owner == ticket.lease_owner
        && operation.expires_at == ticket.expires_at
        && operation.expires_at > now
        && constant_time_eq(
            operation.token_hash.as_bytes(),
            token_hash(&ticket.token).as_bytes(),
        )
}

fn control_ticket_matches_recovery(
    operation: &ActiveControlOperation,
    ticket: &RecoveryTicket,
    now: DateTime<Utc>,
) -> bool {
    operation.operation_id == ticket.operation_id
        && operation
            .kind
            .recovery_reason()
            .is_ok_and(|reason| reason == ticket.reason)
        && operation.generation == ticket.generation
        && operation.revision == ticket.revision
        && operation.controller == ticket.controller
        && operation.lease_owner == ticket.lease_owner
        && operation.expires_at == ticket.expires_at
        && operation.expires_at > now
        && constant_time_eq(
            operation.token_hash.as_bytes(),
            token_hash(&ticket.token).as_bytes(),
        )
}

fn settled_recovery_ticket_matches(
    connection: &Connection,
    owner: &str,
    ticket: &RecoveryTicket,
) -> Result<bool> {
    let stored = connection
        .query_row(
            "SELECT operation_kind, worker_generation, run_revision, controller_kind, \
             controller_id, controller_fingerprint, token_hash, lease_owner, \
             lease_expires_at_ms, status FROM run_control_operations \
             WHERE owner = ?1 AND run_id = ?2 AND operation_id = ?3",
            params![owner, ticket.run_id, ticket.operation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    stored_u64(row, 1, "control generation")?,
                    stored_u64(row, 2, "control run revision")?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    stored_timestamp(row, 8, "control lease expiry")?,
                    row.get::<_, String>(9)?,
                ))
            },
        )
        .optional()?;
    let Some((
        kind,
        generation,
        revision,
        controller_kind,
        controller_id,
        controller_fingerprint,
        stored_token,
        lease_owner,
        expires_at,
        status,
    )) = stored
    else {
        return Ok(false);
    };
    let controller = match (controller_kind, controller_id) {
        (Some(kind), Some(id)) => Some(ControllerRef {
            kind: kind.parse()?,
            id,
            fingerprint: controller_fingerprint,
        }),
        (None, None) if controller_fingerprint.is_none() => None,
        _ => {
            return Err(AgentStoreError::CorruptData(
                "partial control controller".into(),
            ));
        }
    };
    let kind: ControlOperationKind = kind.parse()?;
    Ok(status == "settled"
        && kind
            .recovery_reason()
            .is_ok_and(|reason| reason == ticket.reason)
        && generation == ticket.generation
        && revision == ticket.revision
        && controller == ticket.controller
        && lease_owner == ticket.lease_owner
        && expires_at == ticket.expires_at
        && constant_time_eq(
            stored_token.as_bytes(),
            token_hash(&ticket.token).as_bytes(),
        ))
}

fn receipt_for(run: &AgentRunRecord, token: String) -> Result<RunLeaseReceipt> {
    let lease = run
        .lease
        .as_ref()
        .ok_or_else(|| AgentStoreError::CorruptData("claimed run lost its lease".into()))?;
    Ok(RunLeaseReceipt {
        run_id: run.id.clone(),
        worker_id: run.worker_id.clone(),
        generation: run.worker_generation,
        revision: run.revision,
        lease_owner: lease.owner.clone(),
        token,
    })
}

fn next_worker_generation(connection: &Connection, owner: &str, worker_id: &str) -> Result<u64> {
    let current: i64 = connection.query_row(
        "SELECT COALESCE(MAX(worker_generation), 0) FROM agent_runs \
         WHERE owner = ?1 AND worker_id = ?2",
        params![owner, worker_id],
        |row| row.get(0),
    )?;
    next_u64(
        u64::try_from(current)
            .map_err(|_| AgentStoreError::CorruptData("worker generation is negative".into()))?,
        "worker generation",
    )
}

fn next_queue_sequence(connection: &Connection) -> Result<u64> {
    let current: i64 = connection.query_row(
        "SELECT COALESCE(MAX(queue_sequence), 0) FROM agent_runs",
        [],
        |row| row.get(0),
    )?;
    next_u64(
        u64::try_from(current)
            .map_err(|_| AgentStoreError::CorruptData("run queue sequence is negative".into()))?,
        "run queue sequence",
    )
}

fn nonterminal_runs_for_worker(
    connection: &Connection,
    owner: &str,
    worker_id: &str,
) -> Result<Vec<AgentRunRecord>> {
    let sql = format!(
        "SELECT {RUN_COLUMNS} FROM agent_runs WHERE owner = ?1 AND worker_id = ?2 \
         AND state NOT IN ('succeeded','failed','timed_out','cancelled','interrupted') \
         ORDER BY CASE state WHEN 'cancelling' THEN 0 WHEN 'running' THEN 1 \
           WHEN 'starting' THEN 2 ELSE 3 END, created_at_ms, id"
    );
    let mut statement = connection.prepare(&sql)?;
    Ok(statement
        .query_map(params![owner, worker_id], row_to_run)?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn subtree_workers(
    connection: &Connection,
    owner: &str,
    root_worker_id: &str,
) -> Result<Vec<WorkerRecord>> {
    if get_worker(connection, owner, root_worker_id)?.is_none() {
        return Err(AgentStoreError::NotFound {
            id: root_worker_id.to_string(),
        });
    }
    let sql = format!(
        "WITH RECURSIVE tree(id) AS ( \
          SELECT id FROM workers WHERE owner = ?1 AND id = ?2 \
          UNION SELECT child.id FROM workers child JOIN tree parent \
            ON child.parent_id = parent.id WHERE child.owner = ?1 LIMIT ?3 \
        ) SELECT {QUALIFIED_WORKER_COLUMNS} FROM workers worker JOIN tree ON tree.id = worker.id \
          WHERE worker.owner = ?1 ORDER BY worker.created_at_ms, worker.id"
    );
    let mut statement = connection.prepare(&sql)?;
    let workers = statement
        .query_map(
            params![
                owner,
                root_worker_id,
                usize_to_i64(MAX_TOPOLOGY_NODES + 1, "topology bound")?
            ],
            row_to_worker,
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if workers.len() > MAX_TOPOLOGY_NODES {
        return Err(AgentStoreError::InvalidInput(format!(
            "worker topology exceeds the {MAX_TOPOLOGY_NODES}-node safety bound"
        )));
    }
    Ok(workers)
}

fn topology_from_workers(
    workers: Vec<WorkerRecord>,
    root_worker_id: &str,
) -> Result<WorkerTopology> {
    let by_id = workers
        .into_iter()
        .map(|worker| (worker.id.clone(), worker))
        .collect::<BTreeMap<_, _>>();
    if !by_id.contains_key(root_worker_id) {
        return Err(AgentStoreError::NotFound {
            id: root_worker_id.to_string(),
        });
    }
    let mut children = BTreeMap::<String, Vec<String>>::new();
    for worker in by_id.values() {
        if let Some(parent) = &worker.parent_id {
            children
                .entry(parent.clone())
                .or_default()
                .push(worker.id.clone());
        }
    }
    for values in children.values_mut() {
        values.sort_by(|left, right| {
            let left = &by_id[left];
            let right = &by_id[right];
            (left.created_at, &left.id).cmp(&(right.created_at, &right.id))
        });
    }
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from([root_worker_id.to_string()]);
    while let Some(id) = queue.pop_front() {
        if !seen.insert(id.clone()) {
            return Err(AgentStoreError::CorruptData(
                "worker topology contains a cycle or duplicate path".into(),
            ));
        }
        selected.push(by_id[&id].clone());
        if let Some(child_ids) = children.get(&id) {
            queue.extend(child_ids.iter().cloned());
        }
    }
    Ok(WorkerTopology {
        root_worker_id: root_worker_id.to_string(),
        workers: selected,
    })
}

fn subtree_postorder(workers: &[WorkerRecord], root_worker_id: &str) -> Result<Vec<String>> {
    let topology = topology_from_workers(workers.to_vec(), root_worker_id)?;
    let selected = topology
        .workers
        .iter()
        .map(|worker| worker.id.clone())
        .collect::<BTreeSet<_>>();
    let mut children = BTreeMap::<String, Vec<String>>::new();
    for worker in &topology.workers {
        if let Some(parent) = &worker.parent_id {
            if selected.contains(parent) {
                children
                    .entry(parent.clone())
                    .or_default()
                    .push(worker.id.clone());
            }
        }
    }
    let by_id = topology
        .workers
        .into_iter()
        .map(|worker| (worker.id.clone(), worker))
        .collect::<BTreeMap<_, _>>();
    for child_ids in children.values_mut() {
        child_ids.sort_by(|left, right| {
            let left = &by_id[left];
            let right = &by_id[right];
            (left.created_at, &left.id).cmp(&(right.created_at, &right.id))
        });
    }
    let mut result = Vec::new();
    let mut stack = vec![(root_worker_id.to_string(), false)];
    while let Some((id, expanded)) = stack.pop() {
        if expanded {
            result.push(id);
            continue;
        }
        stack.push((id.clone(), true));
        if let Some(child_ids) = children.get(&id) {
            for child in child_ids.iter().rev() {
                stack.push((child.clone(), false));
            }
        }
    }
    Ok(result)
}

fn configure_connection(connection: &Connection) -> Result<()> {
    connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, SQLITE_VALUE_LIMIT)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    Ok(())
}

fn user_version(connection: &Connection) -> Result<u32> {
    let value: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    u32::try_from(value)
        .map_err(|_| AgentStoreError::CorruptData("database user_version is invalid".into()))
}

fn validate_schema_definition(connection: &Connection) -> Result<()> {
    let actual = schema_manifest(connection)?;
    let expected = expected_schema()?;
    if actual != expected {
        return Err(AgentStoreError::CorruptData(
            "database schema differs from the supported migration manifest".into(),
        ));
    }
    Ok(())
}

fn validate_schema_definition_v1(connection: &Connection) -> Result<()> {
    let actual = schema_manifest(connection)?;
    let expected = expected_schema_v1()?;
    if actual != expected {
        return Err(AgentStoreError::CorruptData(
            "database schema differs from the supported version 1 manifest".into(),
        ));
    }
    Ok(())
}

fn validate_schema_definition_v2(connection: &Connection) -> Result<()> {
    let actual = schema_manifest(connection)?;
    let expected = expected_schema_v2()?;
    if actual != expected {
        return Err(AgentStoreError::CorruptData(
            "database schema differs from the supported version 2 manifest".into(),
        ));
    }
    Ok(())
}

fn validate_schema_definition_v3(connection: &Connection) -> Result<()> {
    let actual = schema_manifest(connection)?;
    let expected = expected_schema_v3()?;
    if actual != expected {
        return Err(AgentStoreError::CorruptData(
            "database schema differs from the supported version 3 manifest".into(),
        ));
    }
    Ok(())
}

fn expected_schema() -> Result<&'static [SchemaObject]> {
    match EXPECTED_SCHEMA.get_or_init(|| {
        let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0001)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0002)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0003)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0004)
            .map_err(|error| error.to_string())?;
        schema_manifest(&connection).map_err(|error| error.to_string())
    }) {
        Ok(manifest) => Ok(manifest.as_slice()),
        Err(error) => Err(AgentStoreError::CorruptData(format!(
            "cannot construct supported schema manifest: {error}"
        ))),
    }
}

fn expected_schema_v3() -> Result<&'static [SchemaObject]> {
    match EXPECTED_SCHEMA_V3.get_or_init(|| {
        let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0001)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0002)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0003)
            .map_err(|error| error.to_string())?;
        schema_manifest(&connection).map_err(|error| error.to_string())
    }) {
        Ok(manifest) => Ok(manifest.as_slice()),
        Err(error) => Err(AgentStoreError::CorruptData(format!(
            "cannot construct version 3 schema manifest: {error}"
        ))),
    }
}

fn expected_schema_v2() -> Result<&'static [SchemaObject]> {
    match EXPECTED_SCHEMA_V2.get_or_init(|| {
        let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0001)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0002)
            .map_err(|error| error.to_string())?;
        schema_manifest(&connection).map_err(|error| error.to_string())
    }) {
        Ok(manifest) => Ok(manifest.as_slice()),
        Err(error) => Err(AgentStoreError::CorruptData(format!(
            "cannot construct version 2 schema manifest: {error}"
        ))),
    }
}

fn expected_schema_v1() -> Result<&'static [SchemaObject]> {
    match EXPECTED_SCHEMA_V1.get_or_init(|| {
        let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0001)
            .map_err(|error| error.to_string())?;
        schema_manifest(&connection).map_err(|error| error.to_string())
    }) {
        Ok(manifest) => Ok(manifest.as_slice()),
        Err(error) => Err(AgentStoreError::CorruptData(format!(
            "cannot construct version 1 schema manifest: {error}"
        ))),
    }
}

fn schema_manifest(connection: &Connection) -> Result<Vec<SchemaObject>> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema \
         WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name",
    )?;
    let rows = statement.query_map([], |row| {
        let sql: String = row.get(3)?;
        Ok(SchemaObject {
            kind: row.get(0)?,
            name: row.get(1)?,
            table_name: row.get(2)?,
            sql: normalize_schema_sql(&sql),
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut quote_end = None;
    let mut pending_space = false;
    for character in sql.trim().trim_end_matches(';').chars() {
        if let Some(end) = quote_end {
            normalized.push(character);
            if character == end {
                quote_end = None;
            }
            continue;
        }
        if character.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space && !normalized.is_empty() {
            normalized.push(' ');
        }
        pending_space = false;
        normalized.push(character.to_ascii_lowercase());
        quote_end = match character {
            '\'' | '"' | '`' => Some(character),
            '[' => Some(']'),
            _ => None,
        };
    }
    normalized
}

fn audit_database_integrity_v1(connection: &Connection) -> Result<()> {
    audit_database_integrity_common(connection, false, true)
}

fn audit_database_integrity_v2(connection: &Connection) -> Result<()> {
    audit_completion_integrity(connection, false, true)
}

fn audit_database_integrity_v3(connection: &Connection) -> Result<()> {
    audit_completion_integrity(connection, true, true)
}

fn audit_database_integrity(connection: &Connection) -> Result<()> {
    audit_completion_integrity(connection, true, false)
}

fn audit_completion_integrity(
    connection: &Connection,
    disposition_schema: bool,
    legacy_backend: bool,
) -> Result<()> {
    audit_database_integrity_common(connection, true, legacy_backend)?;
    let completions = all_completions(connection, legacy_backend)?;
    for completion in &completions {
        validate_stored_completion(completion)?;
    }
    let invalid_completion: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_run_completions c \
         JOIN agent_runs r ON r.owner = c.owner AND r.id = c.run_id WHERE \
           c.worker_id <> r.worker_id OR c.worker_generation <> r.worker_generation \
           OR c.prepared_run_revision > r.revision \
           OR (c.status = 'prepared' AND r.state NOT IN ('running','cancelling')) \
           OR (c.status = 'committed' AND r.state <> 'succeeded') \
           OR (c.status = 'committed' AND c.committed_run_revision <> r.revision) \
           OR (c.status = 'abandoned' AND r.state NOT IN \
                ('failed','timed_out','cancelled','interrupted')) \
           OR (c.status = 'abandoned' AND c.abandoned_run_revision <> r.revision) \
           OR (SELECT COUNT(*) FROM agent_events e WHERE e.owner = c.owner \
                AND e.run_id = c.run_id AND e.event_type = 'completion_prepared' \
                AND e.occurred_at_ms = c.prepared_at_ms \
                AND e.run_revision = c.prepared_run_revision) <> 1 \
           OR ((c.status = 'abandoned') <> ((SELECT COUNT(*) FROM agent_events e \
                WHERE e.owner = c.owner AND e.run_id = c.run_id \
                  AND e.event_type = 'completion_abandoned' \
                  AND e.occurred_at_ms = c.abandoned_at_ms \
                  AND e.run_revision = c.abandoned_run_revision) = 1)) \
           OR ((c.status = 'committed') <> ((SELECT COUNT(*) FROM agent_events e \
                WHERE e.owner = c.owner AND e.run_id = c.run_id \
                  AND e.event_type = 'completion_committed' \
                  AND e.occurred_at_ms = c.committed_at_ms \
                  AND e.run_revision = c.committed_run_revision \
                  AND e.run_state = 'succeeded') = 1)) \
           OR (c.status = 'abandoned' AND NOT EXISTS (SELECT 1 \
                FROM agent_events prepared JOIN agent_events abandoned \
                  ON abandoned.owner = prepared.owner AND abandoned.run_id = prepared.run_id \
                WHERE prepared.owner = c.owner AND prepared.run_id = c.run_id \
                  AND prepared.event_type = 'completion_prepared' \
                  AND abandoned.event_type = 'completion_abandoned' \
                  AND prepared.sequence < abandoned.sequence \
                  AND EXISTS (SELECT 1 FROM agent_events terminal \
                    WHERE terminal.owner = c.owner AND terminal.run_id = c.run_id \
                      AND terminal.run_revision = c.abandoned_run_revision \
                      AND terminal.event_type NOT IN \
                        ('completion_prepared','completion_abandoned') \
                      AND prepared.sequence < terminal.sequence \
                      AND terminal.sequence < abandoned.sequence))) \
           OR (c.committed_by_operation_id IS NOT NULL AND NOT EXISTS ( \
                SELECT 1 FROM run_control_operations o WHERE o.owner = c.owner \
                  AND o.run_id = c.run_id AND o.operation_id = c.committed_by_operation_id \
                  AND o.operation_kind = 'lease_expired' AND o.status = 'settled')))",
        [],
        |row| row.get(0),
    )?;
    if invalid_completion {
        return Err(AgentStoreError::CorruptData(
            "completion contradicts its fenced run".into(),
        ));
    }
    if !legacy_backend {
        let mismatched_backend: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_run_completions c \
             JOIN agent_runs r ON r.owner = c.owner AND r.id = c.run_id \
             WHERE c.execution_backend <> r.execution_backend)",
            [],
            |row| row.get(0),
        )?;
        if mismatched_backend {
            return Err(AgentStoreError::CorruptData(
                "completion execution backend contradicts its fenced run".into(),
            ));
        }
    }
    if disposition_schema {
        audit_projection_dispositions(connection)?;
    }
    Ok(())
}

fn audit_projection_dispositions(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT owner, projector, disposition, reason, not_before_ms, recorded_at_ms \
         FROM agent_projector_dispositions ORDER BY owner, projector, event_sequence",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    for row in rows {
        let (owner, projector, disposition, reason, not_before, recorded_at) = row?;
        validate_owner(&owner).map_err(|_| {
            AgentStoreError::CorruptData("projection disposition owner is invalid".into())
        })?;
        validate_projector(&projector).map_err(|_| {
            AgentStoreError::CorruptData("projection disposition projector is invalid".into())
        })?;
        let valid = match disposition.as_str() {
            "deferred" => {
                ProjectionDeferReason::from_str(&reason).is_ok()
                    && not_before.is_some_and(|value| value > recorded_at)
            }
            "quarantined" => {
                ProjectionQuarantineReason::from_str(&reason).is_ok() && not_before.is_none()
            }
            _ => false,
        };
        if !valid
            || DateTime::<Utc>::from_timestamp_millis(recorded_at).is_none()
            || not_before
                .is_some_and(|value| DateTime::<Utc>::from_timestamp_millis(value).is_none())
        {
            return Err(AgentStoreError::CorruptData(
                "projection disposition violates its bounded contract".into(),
            ));
        }
    }
    drop(statement);

    let invalid_relation: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_projector_dispositions d \
         JOIN agent_events e ON e.owner = d.owner AND e.sequence = d.event_sequence \
         WHERE d.recorded_at_ms < e.occurred_at_ms \
            OR EXISTS (SELECT 1 FROM agent_projector_progress p \
                WHERE p.owner = d.owner AND p.projector = d.projector \
                  AND p.event_sequence = d.event_sequence))",
        [],
        |row| row.get(0),
    )?;
    if invalid_relation {
        return Err(AgentStoreError::CorruptData(
            "projection disposition contradicts its source or successful progress".into(),
        ));
    }
    Ok(())
}

fn audit_database_integrity_common(
    connection: &Connection,
    completion_schema: bool,
    legacy_backend: bool,
) -> Result<()> {
    let quick_check: String =
        connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick_check != "ok" {
        return Err(AgentStoreError::CorruptData(
            "SQLite quick_check failed".into(),
        ));
    }
    let foreign_key_errors: i64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        return Err(AgentStoreError::CorruptData(
            "database contains foreign-key violations".into(),
        ));
    }

    let workers = all_workers(connection)?;
    for worker in &workers {
        validate_stored_worker(worker)?;
    }
    audit_topology(&workers)?;
    audit_cancel_tree_plans(connection)?;

    let runs = all_runs(connection, legacy_backend)?;
    for run in &runs {
        validate_stored_run(run)?;
    }
    let events = all_events(connection)?;
    for event in &events {
        validate_stored_event(event)?;
    }

    let invalid_secret_state: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_runs WHERE \
             (state IN ('starting','running') AND lease_token_hash IS NULL) \
          OR (state NOT IN ('starting','running') AND lease_token_hash IS NOT NULL))",
        [],
        |row| row.get(0),
    )?;
    if invalid_secret_state {
        return Err(AgentStoreError::CorruptData(
            "run secret capabilities contradict lifecycle state".into(),
        ));
    }

    let invalid_control_state: bool = connection.query_row(
        "SELECT EXISTS( \
           SELECT 1 FROM run_control_operations c JOIN agent_runs r \
             ON r.owner = c.owner AND r.id = c.run_id WHERE \
             c.operation_kind NOT IN ('cancel','lease_expired','execution_timed_out', \
                                      'cancellation_abandoned') \
             OR c.status NOT IN ('active','settled','superseded') \
             OR length(c.token_hash) <> 64 OR c.token_hash GLOB '*[^0-9a-f]*' \
             OR c.worker_generation <> r.worker_generation OR c.run_revision > r.revision \
             OR c.updated_at_ms < c.created_at_ms \
             OR c.lease_expires_at_ms <= c.created_at_ms \
             OR (c.status = 'active' AND (c.settled_at_ms IS NOT NULL \
                 OR r.state <> 'cancelling' OR c.run_revision <> r.revision \
                 OR c.controller_kind IS NOT r.controller_kind \
                 OR c.controller_id IS NOT r.controller_id \
                 OR c.controller_fingerprint IS NOT r.controller_fingerprint)) \
             OR (c.status <> 'active' AND (c.settled_at_ms IS NULL \
                 OR c.settled_at_ms < c.created_at_ms \
                 OR c.settled_at_ms <> c.updated_at_ms)) \
             OR (c.operation_kind = 'cancel' AND NOT EXISTS ( \
                 SELECT 1 FROM cancel_tree_operations h \
                 JOIN cancel_tree_operation_runs p ON p.owner = h.owner \
                   AND p.operation_id = h.operation_id \
                 WHERE h.owner = c.owner AND h.operation_id = c.operation_id \
                   AND p.run_id = c.run_id AND p.action = 'controller_cancel')) \
           UNION ALL \
           SELECT 1 FROM agent_runs r WHERE \
             (r.state = 'cancelling' AND NOT EXISTS (SELECT 1 FROM run_control_operations c \
                WHERE c.owner = r.owner AND c.run_id = r.id AND c.status = 'active')) \
             OR (r.state <> 'cancelling' AND EXISTS (SELECT 1 FROM run_control_operations c \
                WHERE c.owner = r.owner AND c.run_id = r.id AND c.status = 'active')) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_control_state {
        return Err(AgentStoreError::CorruptData(
            "run control operation contradicts its fenced run".into(),
        ));
    }

    let invalid_generation_chain: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM (SELECT owner, worker_id, COUNT(*) AS count_rows, \
             MIN(worker_generation) AS min_generation, MAX(worker_generation) AS max_generation \
             FROM agent_runs WHERE worker_generation > 0 GROUP BY owner, worker_id) g \
         WHERE g.min_generation <> 1 OR g.count_rows <> g.max_generation)",
        [],
        |row| row.get(0),
    )?;
    if invalid_generation_chain {
        return Err(AgentStoreError::CorruptData(
            "worker run generations are incomplete".into(),
        ));
    }

    let invalid_fixed_deadline: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_runs r WHERE \
           (r.worker_generation = 0 AND r.deadline_at_ms IS NOT NULL) \
           OR (r.worker_generation > 0 AND (r.deadline_at_ms IS NULL \
             OR NOT EXISTS (SELECT 1 FROM agent_events e WHERE e.owner = r.owner \
               AND e.run_id = r.id AND e.event_type = 'run_claimed' \
               AND e.run_revision = 1 \
               AND r.deadline_at_ms = e.occurred_at_ms + r.timeout_ms))) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_fixed_deadline {
        return Err(AgentStoreError::CorruptData(
            "run deadline is missing or differs from its claim timestamp".into(),
        ));
    }

    let invalid_queue_sequence: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM (SELECT COUNT(*) AS count_rows, \
          MIN(queue_sequence) AS min_sequence, MAX(queue_sequence) AS max_sequence \
          FROM agent_runs) q WHERE q.count_rows > 0 \
          AND (q.min_sequence <> 1 OR q.count_rows <> q.max_sequence))",
        [],
        |row| row.get(0),
    )?;
    if invalid_queue_sequence {
        return Err(AgentStoreError::CorruptData(
            "run queue sequence is incomplete".into(),
        ));
    }

    let invalid_owner_event_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
           SELECT 1 FROM (SELECT owner, COUNT(*) AS count_rows, MIN(sequence) AS min_sequence, \
             MAX(sequence) AS max_sequence FROM agent_events GROUP BY owner) e \
             LEFT JOIN owner_agent_event_sequences s ON s.owner = e.owner \
             WHERE e.min_sequence <> 1 OR e.count_rows <> e.max_sequence \
               OR s.next_sequence IS NULL OR s.next_sequence <> e.max_sequence + 1 \
           UNION ALL \
           SELECT 1 FROM owner_agent_event_sequences s WHERE NOT EXISTS \
             (SELECT 1 FROM agent_events e WHERE e.owner = s.owner) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_owner_event_sequence {
        return Err(AgentStoreError::CorruptData(
            "owner event sequence is incomplete".into(),
        ));
    }

    let regressed_owner_event_time: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM (SELECT occurred_at_ms, \
           LAG(occurred_at_ms) OVER (PARTITION BY owner ORDER BY sequence) AS previous_at_ms \
           FROM agent_events) e WHERE e.previous_at_ms > e.occurred_at_ms)",
        [],
        |row| row.get(0),
    )?;
    if regressed_owner_event_time {
        return Err(AgentStoreError::CorruptData(
            "owner event timestamps regress".into(),
        ));
    }

    let invalid_run_event_chain: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_runs r LEFT JOIN agent_events e \
             ON e.owner = r.owner AND e.run_id = r.id \
             AND e.event_type NOT IN ('completion_prepared','completion_abandoned') \
         GROUP BY r.owner, r.id, r.revision \
         HAVING COUNT(e.sequence) <> r.revision + 1 \
             OR MIN(e.run_revision) <> 0 OR MAX(e.run_revision) <> r.revision \
             OR COUNT(DISTINCT e.run_revision) <> r.revision + 1)",
        [],
        |row| row.get(0),
    )?;
    if invalid_run_event_chain {
        return Err(AgentStoreError::CorruptData(
            "run revision history is incomplete".into(),
        ));
    }

    let invalid_current_run_event_time: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_runs r WHERE NOT EXISTS ( \
           SELECT 1 FROM agent_events e WHERE e.owner = r.owner AND e.run_id = r.id \
             AND e.run_revision = r.revision AND e.occurred_at_ms = r.updated_at_ms))",
        [],
        |row| row.get(0),
    )?;
    if invalid_current_run_event_time {
        return Err(AgentStoreError::CorruptData(
            "current run revision event does not share its operation timestamp".into(),
        ));
    }

    let event_relation_sql = if completion_schema {
        "SELECT EXISTS(SELECT 1 FROM agent_events e \
          JOIN workers w ON w.owner = e.owner AND w.id = e.worker_id \
          LEFT JOIN agent_runs r ON r.owner = e.owner AND r.id = e.run_id \
          LEFT JOIN agent_run_completions c ON c.owner = e.owner AND c.run_id = e.run_id \
          WHERE e.worker_revision > w.revision \
             OR (e.run_id IS NOT NULL AND (r.id IS NULL OR r.worker_id <> e.worker_id \
                 OR e.run_revision > r.revision OR e.occurred_at_ms < r.created_at_ms \
                 OR (e.event_type NOT IN ('completion_prepared','completion_abandoned') \
                     AND e.occurred_at_ms > r.updated_at_ms) \
                 OR (e.event_type = 'completion_prepared' AND (c.run_id IS NULL \
                     OR e.occurred_at_ms <> c.prepared_at_ms \
                     OR e.run_revision <> c.prepared_run_revision \
                     OR e.run_state <> 'running')) \
                 OR (e.event_type = 'completion_committed' AND (c.run_id IS NULL \
                     OR c.status <> 'committed' \
                     OR e.occurred_at_ms <> c.committed_at_ms \
                     OR e.run_revision <> c.committed_run_revision \
                     OR e.run_state <> 'succeeded')) \
                 OR (e.event_type = 'completion_abandoned' AND (c.run_id IS NULL \
                     OR c.status <> 'abandoned' \
                     OR e.occurred_at_ms <> c.abandoned_at_ms \
                     OR e.run_revision <> c.abandoned_run_revision \
                     OR e.run_state NOT IN ('failed','timed_out','cancelled','interrupted'))))))"
    } else {
        "SELECT EXISTS(SELECT 1 FROM agent_events e \
          JOIN workers w ON w.owner = e.owner AND w.id = e.worker_id \
          LEFT JOIN agent_runs r ON r.owner = e.owner AND r.id = e.run_id \
          WHERE e.worker_revision > w.revision \
             OR (e.run_id IS NOT NULL AND (r.id IS NULL OR r.worker_id <> e.worker_id \
                 OR e.run_revision > r.revision OR e.occurred_at_ms < r.created_at_ms \
                 OR e.occurred_at_ms > r.updated_at_ms)))"
    };
    let invalid_event_relation: bool =
        connection.query_row(event_relation_sql, [], |row| row.get(0))?;
    if invalid_event_relation {
        return Err(AgentStoreError::CorruptData(
            "agent event contradicts its source record".into(),
        ));
    }

    let invalid_projection: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_projector_progress p \
          JOIN agent_events e ON e.owner = p.owner AND e.sequence = p.event_sequence \
          WHERE p.projected_at_ms < e.occurred_at_ms)",
        [],
        |row| row.get(0),
    )?;
    if invalid_projection {
        return Err(AgentStoreError::CorruptData(
            "projector progress contradicts its source event".into(),
        ));
    }

    let invalid_released: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM workers w WHERE w.lifecycle = 'released' AND ( \
            EXISTS(SELECT 1 FROM agent_runs r WHERE r.owner = w.owner AND r.worker_id = w.id \
                AND r.state NOT IN ('succeeded','failed','timed_out','cancelled','interrupted')) \
            OR EXISTS(SELECT 1 FROM workers child WHERE child.owner = w.owner \
                AND child.parent_id = w.id AND child.lifecycle <> 'released'))) ",
        [],
        |row| row.get(0),
    )?;
    if invalid_released {
        return Err(AgentStoreError::CorruptData(
            "released worker retains active work or children".into(),
        ));
    }
    Ok(())
}

fn all_workers(connection: &Connection) -> Result<Vec<WorkerRecord>> {
    let sql = format!("SELECT {WORKER_COLUMNS} FROM workers ORDER BY owner, id");
    let mut statement = connection.prepare(&sql)?;
    Ok(statement
        .query_map([], row_to_worker)?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn all_runs(connection: &Connection, legacy_backend: bool) -> Result<Vec<AgentRunRecord>> {
    let columns = if legacy_backend {
        RUN_COLUMNS_V3
    } else {
        RUN_COLUMNS
    };
    let sql = format!("SELECT {columns} FROM agent_runs ORDER BY owner, id");
    let mut statement = connection.prepare(&sql)?;
    Ok(statement
        .query_map([], row_to_run)?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn all_events(connection: &Connection) -> Result<Vec<AgentEvent>> {
    let sql = format!("SELECT {EVENT_COLUMNS} FROM agent_events ORDER BY owner, sequence");
    let mut statement = connection.prepare(&sql)?;
    Ok(statement
        .query_map([], row_to_event)?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn all_completions(
    connection: &Connection,
    legacy_backend: bool,
) -> Result<Vec<RunCompletionRecord>> {
    let columns = if legacy_backend {
        COMPLETION_COLUMNS_V3
    } else {
        COMPLETION_COLUMNS
    };
    let sql = format!("SELECT {columns} FROM agent_run_completions ORDER BY owner, run_id");
    let mut statement = connection.prepare(&sql)?;
    Ok(statement
        .query_map([], row_to_completion)?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn audit_cancel_tree_plans(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT owner, operation_id FROM cancel_tree_operations ORDER BY owner, operation_id",
    )?;
    let keys = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    for (owner, operation_id) in keys {
        validate_owner(&owner)?;
        validate_text("cancel operation id", &operation_id, MAX_ID_BYTES)?;
        let header =
            load_cancel_tree_header(connection, &owner, &operation_id)?.ok_or_else(|| {
                AgentStoreError::CorruptData("cancel tree header disappeared during audit".into())
            })?;
        validate_text(
            "cancel root worker id",
            &header.root_worker_id,
            MAX_ID_BYTES,
        )?;
        validate_digest("cancel tree plan digest", &header.plan_digest)?;
        validate_text("cancel tree lease owner", &header.lease_owner, 256)?;
        validate_lease_seconds(header.lease_seconds)?;
        if header.worker_count == 0
            || header.worker_count > MAX_TOPOLOGY_NODES
            || header.run_count > MAX_TREE_CANCEL_RUNS
        {
            return Err(AgentStoreError::CorruptData(
                "cancel tree header exceeds its safety bounds".into(),
            ));
        }
        let stored_workers = load_cancel_tree_workers(connection, &owner, &operation_id)?;
        let stored_runs = load_cancel_tree_runs(connection, &owner, &operation_id)?;
        let current_workers = subtree_workers(connection, &owner, &header.root_worker_id)?;
        let expected_postorder = subtree_postorder(&current_workers, &header.root_worker_id)?;
        if stored_workers != expected_postorder
            || stored_workers.len() != header.worker_count
            || stored_runs.len() != header.run_count
            || cancel_tree_plan_digest(&stored_workers, &stored_runs)? != header.plan_digest
        {
            return Err(AgentStoreError::CorruptData(
                "cancel tree header, scope, and plan membership differ".into(),
            ));
        }
        for entry in &stored_runs {
            if !stored_workers.contains(&entry.worker_id) {
                return Err(AgentStoreError::CorruptData(
                    "cancel tree run is outside its frozen worker scope".into(),
                ));
            }
            let run = require_run(connection, &owner, &entry.run_id)?;
            if run.worker_id != entry.worker_id {
                return Err(AgentStoreError::CorruptData(
                    "cancel tree run membership contradicts its worker".into(),
                ));
            }
            match entry.action {
                CancelTreeRunAction::QueuedCancel => {
                    if run.state != RunState::Cancelled
                        || run.failure_code != Some(RunFailureCode::Cancelled)
                    {
                        return Err(AgentStoreError::CorruptData(
                            "direct queued cancellation did not remain cancelled".into(),
                        ));
                    }
                }
                CancelTreeRunAction::ControllerCancel => {
                    let exists: bool = connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM run_control_operations WHERE owner = ?1 \
                         AND run_id = ?2 AND operation_id = ?3 AND operation_kind = 'cancel')",
                        params![owner, entry.run_id, operation_id],
                        |row| row.get(0),
                    )?;
                    if !exists {
                        return Err(AgentStoreError::CorruptData(
                            "controller cancellation has no fenced control operation".into(),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn audit_topology(workers: &[WorkerRecord]) -> Result<()> {
    let mut by_owner = BTreeMap::<String, Vec<WorkerRecord>>::new();
    for worker in workers {
        by_owner
            .entry(worker.owner.clone())
            .or_default()
            .push(worker.clone());
    }
    for workers in by_owner.values() {
        let by_id = workers
            .iter()
            .map(|worker| (worker.id.as_str(), worker))
            .collect::<BTreeMap<_, _>>();
        for worker in workers {
            let mut seen = BTreeSet::new();
            let mut cursor = Some(worker.id.as_str());
            while let Some(id) = cursor {
                if !seen.insert(id) {
                    return Err(AgentStoreError::CorruptData(
                        "worker topology contains a cycle".into(),
                    ));
                }
                cursor = by_id.get(id).and_then(|value| value.parent_id.as_deref());
            }
        }
    }
    Ok(())
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| AgentStoreError::InvalidInput("secure lease entropy unavailable".into()))?;
    Ok(hex_lower(&bytes))
}

fn token_hash(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"vyane-agent-capability-v1\0");
    hasher.update(token.as_bytes());
    hex_lower(&hasher.finalize())
}

fn completion_token(permit: &ActiveExecutionPermit, completion_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"vyane-agent-completion-token-v1\0");
    for value in [permit.token(), permit.run_id(), completion_id] {
        hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn validate_lease_seconds(seconds: u64) -> Result<()> {
    if seconds == 0 || seconds > MAX_LEASE_SECONDS {
        return Err(AgentStoreError::InvalidInput(format!(
            "lease seconds must be between 1 and {MAX_LEASE_SECONDS}"
        )));
    }
    Ok(())
}

fn add_seconds(value: DateTime<Utc>, seconds: u64, label: &str) -> Result<DateTime<Utc>> {
    let seconds = i64::try_from(seconds)
        .map_err(|_| AgentStoreError::InvalidInput(format!("{label} is too large")))?;
    value
        .checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or_else(|| AgentStoreError::InvalidInput(format!("{label} exceeds timestamp range")))
}

fn timeout_millis(seconds: u64) -> Result<i64> {
    let millis = seconds
        .checked_mul(1_000)
        .ok_or_else(|| AgentStoreError::InvalidInput("run timeout overflow".into()))?;
    u64_to_i64(millis, "run timeout")
}

fn normalize_timestamp(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value.timestamp_millis()).ok_or_else(|| {
        AgentStoreError::InvalidInput("timestamp is outside SQLite millisecond range".into())
    })
}

fn next_u64(value: u64, label: &str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| AgentStoreError::CorruptData(format!("{label} overflow")))
}

fn ensure_revision(id: &str, actual: u64, expected: u64) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(AgentStoreError::Conflict {
            id: id.to_string(),
            expected,
            actual,
        })
    }
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| AgentStoreError::InvalidInput(format!("{label} exceeds SQLite range")))
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| AgentStoreError::InvalidInput(format!("{label} exceeds SQLite range")))
}

fn ensure_one_change(changed: usize, operation: &str) -> Result<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(AgentStoreError::CorruptData(format!(
            "{operation} did not update exactly one row"
        )))
    }
}

fn parse_enum<T>(index: usize, value: &str) -> rusqlite::Result<T>
where
    T: FromStr<Err = AgentStoreError>,
{
    value
        .parse()
        .map_err(|error: AgentStoreError| data_error(index, Type::Text, error.to_string()))
}

fn stored_timestamp(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<DateTime<Utc>> {
    let value: i64 = row.get(index)?;
    DateTime::from_timestamp_millis(value).ok_or_else(|| data_error(index, Type::Integer, label))
}

fn optional_timestamp(
    row: &Row<'_>,
    index: usize,
    label: &str,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let value: Option<i64> = row.get(index)?;
    value
        .map(|value| {
            DateTime::from_timestamp_millis(value)
                .ok_or_else(|| data_error(index, Type::Integer, label))
        })
        .transpose()
}

fn stored_u64(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| data_error(index, Type::Integer, label))
}

fn optional_u64(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<Option<u64>> {
    let value: Option<i64> = row.get(index)?;
    value
        .map(|value| u64::try_from(value).map_err(|_| data_error(index, Type::Integer, label)))
        .transpose()
}

fn stored_u32(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<u32> {
    let value: i64 = row.get(index)?;
    u32::try_from(value).map_err(|_| data_error(index, Type::Integer, label))
}

fn data_error(index: usize, value_type: Type, message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        value_type,
        Box::new(AgentStoreError::CorruptData(message.into())),
    )
}

fn open_database(path: &Path) -> Result<Connection> {
    #[cfg(unix)]
    {
        reject_symlink_components(path)?;
        reject_existing_sidecar_symlinks(path)?;
        validate_database_files(path)?;
    }
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Ok(Connection::open_with_flags(path, flags)?)
}

fn acquire_write_lock(path: &Path) -> Result<File> {
    let lock_path = companion_path(path, ".write-lock");
    #[cfg(unix)]
    let lock = {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file
    };
    #[cfg(not(unix))]
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    let deadline = Instant::now() + BUSY_TIMEOUT;
    loop {
        if lock.try_lock_exclusive()? {
            return Ok(lock);
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out acquiring agent store write lock",
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn companion_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
fn prepare_database_path(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.try_exists()? {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    validate_private_directory(parent)?;
    reject_symlink_components(path)?;
    reject_existing_sidecar_symlinks(path)?;
    // Never expose the final SQLite path while this process still owns a raw
    // descriptor for its inode. Closing any descriptor for an inode discards
    // this process's POSIX locks, including locks acquired by another SQLite
    // connection after observing the final path.
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let candidate = create_closed_database_candidate(parent)?;
            publish_database_candidate(&candidate, path)?;
        }
        Err(error) => return Err(error.into()),
    }
    validate_database_files(path)
}

#[cfg(unix)]
fn create_closed_database_candidate(parent: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    for _ in 0..DATABASE_CREATE_ATTEMPTS {
        let sequence = DATABASE_CREATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            "{DATABASE_CREATE_PREFIX}-{}-{sequence}",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&candidate)
        {
            Ok(file) => {
                let configured = (|| -> std::io::Result<()> {
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
                    let metadata = file.metadata()?;
                    if !metadata.is_file() || metadata.permissions().mode() & 0o7777 != 0o600 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "new agent database candidate is not a private regular file",
                        ));
                    }
                    Ok(())
                })();
                drop(file);
                if let Err(error) = configured {
                    let _ = std::fs::remove_file(&candidate);
                    return Err(error.into());
                }
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not reserve a unique agent database candidate",
    )
    .into())
}

#[cfg(unix)]
fn publish_database_candidate(candidate: &Path, path: &Path) -> Result<()> {
    let published = std::fs::hard_link(candidate, path);
    let _ = std::fs::remove_file(candidate);
    match published {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_database_files(path)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn prepare_database_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    Ok(())
}

#[cfg(unix)]
fn validate_database_files(path: &Path) -> Result<()> {
    // Keep this metadata-only. SQLite owns the descriptors for these files;
    // opening and dropping a second descriptor can silently destroy its
    // process-wide POSIX locks while a connection or transaction is live.
    validate_database_file(path, true)?;
    validate_database_file(&sqlite_sidecar(path, "-wal"), false)?;
    validate_database_file(&sqlite_sidecar(path, "-shm"), false)
}

#[cfg(not(unix))]
fn validate_database_files(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sqlite_sidecar(path: &Path, suffix: &str) -> PathBuf {
    companion_path(path, suffix)
}

#[cfg(unix)]
fn validate_private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AgentStoreError::InvalidInput(
            "agent database parent must be a real directory".into(),
        ));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(AgentStoreError::InvalidInput(
            "agent database parent must not be group- or world-writable".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AgentStoreError::InvalidInput(
                    "agent database path must not traverse a symlink".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn reject_existing_sidecar_symlinks(path: &Path) -> Result<()> {
    for candidate in [
        sqlite_sidecar(path, "-wal"),
        sqlite_sidecar(path, "-shm"),
        companion_path(path, ".write-lock"),
    ] {
        match std::fs::symlink_metadata(candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AgentStoreError::InvalidInput(
                    "agent database sidecars must not be symlinks".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_database_file(path: &Path, required: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(AgentStoreError::InvalidInput(
                    "agent database files must be regular files".into(),
                ));
            }
            if metadata.permissions().mode() & 0o7777 != 0o600 {
                return Err(AgentStoreError::InvalidInput(
                    "agent database file permissions must be 0600; repair them while the store is offline"
                        .into(),
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::process::Command;
    use std::sync::{Arc, Barrier};

    use rusqlite::ErrorCode;

    use super::*;

    const LOCK_PROBE_DATABASE: &str = "VYANE_AGENT_LOCK_PROBE_DATABASE";

    #[test]
    fn first_creation_closes_candidates_before_atomic_publication() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("first-create.sqlite");
        let candidates = (0..8)
            .map(|_| create_closed_database_candidate(directory.path()).unwrap())
            .collect::<Vec<_>>();
        assert!(
            !path.exists(),
            "the final path must stay hidden while candidates are prepared"
        );
        for candidate in &candidates {
            let metadata = std::fs::symlink_metadata(candidate).unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        }

        let barrier = Arc::new(Barrier::new(candidates.len()));
        let path = Arc::new(path);
        let publishers = candidates
            .into_iter()
            .map(|candidate| {
                let barrier = Arc::clone(&barrier);
                let path = Arc::clone(&path);
                std::thread::spawn(move || {
                    barrier.wait();
                    publish_database_candidate(&candidate, &path).unwrap();
                })
            })
            .collect::<Vec<_>>();
        for publisher in publishers {
            publisher.join().unwrap();
        }

        validate_database_files(&path).unwrap();
        assert_eq!(
            std::fs::symlink_metadata(&*path)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(DATABASE_CREATE_PREFIX)
        }));

        let rejected = create_closed_database_candidate(directory.path()).unwrap();
        let unsafe_winner = directory.path().join("unsafe-winner.sqlite");
        std::fs::write(&unsafe_winner, b"unsafe winner").unwrap();
        std::fs::set_permissions(&unsafe_winner, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            publish_database_candidate(&rejected, &unsafe_winner),
            Err(AgentStoreError::InvalidInput(_))
        ));
        assert!(!rejected.exists());
    }

    #[test]
    fn metadata_validation_does_not_release_a_wal_writer_lock() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lock-probe.sqlite");
        prepare_database_path(&path).unwrap();

        let mut connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .execute_batch("CREATE TABLE lock_probe (value INTEGER NOT NULL);")
            .unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute("INSERT INTO lock_probe (value) VALUES (1)", [])
            .unwrap();

        validate_database_files(&path).unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("sqlite::tests::wal_writer_lock_probe")
            .arg("--exact")
            .arg("--ignored")
            .arg("--nocapture")
            .env(LOCK_PROBE_DATABASE, &path)
            .status()
            .unwrap();
        assert!(status.success(), "external WAL writer acquired the lock");

        transaction.commit().unwrap();
    }

    #[test]
    #[ignore = "subprocess-only WAL writer probe"]
    fn wal_writer_lock_probe() {
        let Some(path) = std::env::var_os(LOCK_PROBE_DATABASE) else {
            return;
        };
        let connection = Connection::open(path).unwrap();
        connection.busy_timeout(Duration::from_millis(100)).unwrap();
        let error = connection
            .execute_batch("BEGIN IMMEDIATE")
            .expect_err("external WAL writer unexpectedly acquired the lock");
        assert!(matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        ));
    }
}
