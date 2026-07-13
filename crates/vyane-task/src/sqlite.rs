use std::path::{Path, PathBuf};
use std::str::FromStr;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::types::{Type, Value};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension as _, Row, Transaction,
    TransactionBehavior, params, params_from_iter,
};

use crate::{
    ControllerRef, FailureCode, Lease, NewTask, Result, TaskCursor, TaskEvent, TaskEventKind,
    TaskPage, TaskQuery, TaskRecord, TaskSettlement, TaskState, TaskStore, TaskStoreError,
    model::{validate_task_digest, validate_text},
};

pub const SCHEMA_VERSION: u32 = 2;
const RECORD_SCHEMA: u32 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const DATABASE_CREATE_ATTEMPTS: usize = 128;
#[cfg(unix)]
const DATABASE_CREATE_PREFIX: &str = ".vyane-task-db-create";
#[cfg(unix)]
static DATABASE_CREATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MIGRATION_0001: &str = include_str!("../migrations/0001_tasks.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_owner_scope.sql");

const SCHEMA_OBJECTS_V1: &[(&str, &str, &str)] = &[
    ("table", "tasks", "tasks"),
    ("index", "tasks_owner_created_idx", "tasks"),
    ("index", "tasks_state_lease_idx", "tasks"),
    ("index", "tasks_ledger_run_idx", "tasks"),
    ("table", "task_events", "task_events"),
    ("index", "task_events_task_idx", "task_events"),
];
const SCHEMA_OBJECTS_V2: &[(&str, &str, &str)] = &[
    ("table", "tasks", "tasks"),
    ("index", "tasks_owner_created_idx", "tasks"),
    ("index", "tasks_owner_state_lease_idx", "tasks"),
    ("index", "tasks_owner_ledger_run_idx", "tasks"),
    ("table", "task_events", "task_events"),
    ("index", "task_events_owner_task_idx", "task_events"),
];

const TASK_COLUMNS: &str = "\
    id, record_schema, owner, kind, origin, state, task_digest, target_key, \
    created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision, executor_epoch, \
    controller_kind, controller_instance_id, controller_pid, controller_pgid, \
    controller_started_at_ms, controller_birth_fingerprint, lease_owner, \
    lease_expires_at_ms, ledger_run_id, failure_code";

/// SQLite-backed durable task metadata.
///
/// The store keeps only a path and opens a short-lived connection per
/// operation. SQLite serializes cross-process writers; every state transition
/// uses an IMMEDIATE transaction and a revision/epoch compare-and-swap.
#[derive(Debug, Clone)]
pub struct SqliteTaskStore {
    path: PathBuf,
}

impl SqliteTaskStore {
    /// Open or create a task database and apply supported migrations.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let store = Self { path: path.into() };
        store.initialize()?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn initialize(&self) -> Result<()> {
        prepare_database_path(&self.path)?;

        let mut connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;

        // Check before changing persistent PRAGMAs. A newer database is
        // rejected without attempting a downgrade or schema write.
        let found = user_version(&connection)?;
        if found > SCHEMA_VERSION {
            return Err(TaskStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }

        enable_wal(&connection)?;
        configure_connection(&connection)?;
        validate_database_files(&self.path)?;

        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let found = user_version(&transaction)?;
        if found > SCHEMA_VERSION {
            return Err(TaskStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        if found == 0 {
            require_empty_schema(&transaction)?;
            transaction.execute_batch(MIGRATION_0001)?;
            transaction.pragma_update(None, "user_version", 1_u32)?;
        }
        let found = user_version(&transaction)?;
        if found == 1 {
            validate_schema_version(&transaction, 1)?;
            migrate_v1_to_v2(&transaction)?;
        }
        validate_schema_version(&transaction, SCHEMA_VERSION)?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        validate_database_files(&self.path)
    }

    fn connection(&self) -> Result<Connection> {
        let connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        require_current_schema(&connection)?;
        configure_connection(&connection)?;
        validate_database_files(&self.path)?;
        Ok(connection)
    }

    #[allow(clippy::too_many_arguments)]
    fn mutate<F>(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        event_kind: TaskEventKind,
        occurred_at: DateTime<Utc>,
        mutation: F,
    ) -> Result<TaskRecord>
    where
        F: FnOnce(&TaskRecord, &mut TaskRecord, DateTime<Utc>) -> Result<Option<String>>,
    {
        let mut connection = self.connection()?;
        let transaction = write_transaction(&mut connection)?;
        validate_text("owner", owner, 256)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| TaskStoreError::NotFound { id: id.to_string() })?;
        ensure_cas(&before, expected_revision, expected_executor_epoch)?;
        let effective_at = std::cmp::max(occurred_at, before.updated_at);

        let mut after = before.clone();
        let actor = mutation(&before, &mut after, effective_at)?;
        after.revision = next_counter(before.revision, "revision")?;
        after.updated_at = effective_at;

        update_snapshot(&transaction, &before, &after)?;
        insert_event(
            &transaction,
            &after.owner,
            &after.id,
            after.revision,
            effective_at,
            event_kind,
            Some(before.state),
            after.state,
            actor.as_deref(),
            after.executor_epoch,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(after)
    }
}

impl TaskStore for SqliteTaskStore {
    fn create(&self, owner: &str, task: NewTask) -> Result<TaskRecord> {
        validate_text("owner", owner, 256)?;
        let mut task = task;
        task.created_at = normalize_timestamp(task.created_at)?;
        task.validate()?;
        let record = TaskRecord::from_new(owner.to_string(), task);
        let mut connection = self.connection()?;
        let transaction = write_transaction(&mut connection)?;

        if get_in_transaction(&transaction, owner, &record.id)?.is_some() {
            return Err(TaskStoreError::AlreadyExists {
                id: record.id.clone(),
            });
        }
        insert_snapshot(&transaction, &record)?;
        insert_event(
            &transaction,
            owner,
            &record.id,
            record.revision,
            record.created_at,
            TaskEventKind::Created,
            None,
            record.state,
            None,
            record.executor_epoch,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(record)
    }

    fn get(&self, owner: &str, id: &str) -> Result<Option<TaskRecord>> {
        validate_text("owner", owner, 256)?;
        let connection = self.connection()?;
        get_in_connection(&connection, owner, id)
    }

    fn list(&self, owner: &str, query: &TaskQuery) -> Result<TaskPage> {
        validate_text("owner", owner, 256)?;
        query.validate()?;
        let connection = self.connection()?;
        let mut clauses = Vec::new();
        let mut values = Vec::<Value>::new();

        clauses.push("owner = ?".to_string());
        values.push(Value::Text(owner.to_string()));
        if !query.kinds.is_empty() {
            let placeholders = std::iter::repeat_n("?", query.kinds.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("kind IN ({placeholders})"));
            values.extend(
                query
                    .kinds
                    .iter()
                    .map(|kind| Value::Text(kind.as_str().to_string())),
            );
        }
        if !query.origins.is_empty() {
            let placeholders = std::iter::repeat_n("?", query.origins.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("origin IN ({placeholders})"));
            values.extend(
                query
                    .origins
                    .iter()
                    .map(|origin| Value::Text(origin.as_str().to_string())),
            );
        }
        if !query.states.is_empty() {
            let placeholders = std::iter::repeat_n("?", query.states.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("state IN ({placeholders})"));
            values.extend(
                query
                    .states
                    .iter()
                    .map(|state| Value::Text(state.as_str().to_string())),
            );
        }
        if let Some(cursor) = &query.cursor {
            clauses.push("(created_at_ms < ? OR (created_at_ms = ? AND id < ?))".to_string());
            let timestamp = cursor.created_at.timestamp_millis();
            values.push(Value::Integer(timestamp));
            values.push(Value::Integer(timestamp));
            values.push(Value::Text(cursor.id.clone()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let fetch_limit = query.limit.saturating_add(1);
        values.push(Value::Integer(usize_to_i64(fetch_limit, "query limit")?));
        let sql = format!(
            "SELECT {TASK_COLUMNS} FROM tasks{where_clause} \
             ORDER BY created_at_ms DESC, id DESC LIMIT ?"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), row_to_task)?;
        let mut items = rows.collect::<std::result::Result<Vec<_>, _>>()?;

        let has_more = items.len() > query.limit;
        if has_more {
            items.pop();
        }
        let next_cursor = if has_more {
            items.last().map(|last| TaskCursor {
                created_at: last.created_at,
                id: last.id.clone(),
            })
        } else {
            None
        };

        Ok(TaskPage { items, next_cursor })
    }

    fn events(&self, owner: &str, id: &str) -> Result<Vec<TaskEvent>> {
        validate_text("owner", owner, 256)?;
        let connection = self.connection()?;
        if get_in_connection(&connection, owner, id)?.is_none() {
            return Err(TaskStoreError::NotFound { id: id.to_string() });
        }
        let mut statement = connection.prepare(
            "SELECT sequence, owner, task_id, revision, occurred_at_ms, kind, from_state, \
                    to_state, actor_instance, executor_epoch \
             FROM task_events WHERE owner = ?1 AND task_id = ?2 ORDER BY revision ASC",
        )?;
        let rows = statement.query_map(params![owner, id], row_to_event)?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn attach_controller(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        controller: ControllerRef,
        lease: Option<Lease>,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord> {
        let at = normalize_timestamp(at)?;
        let controller = normalize_controller(controller)?;
        let lease = lease.map(normalize_lease).transpose()?;
        controller.validate()?;
        self.mutate(
            owner,
            id,
            expected_revision,
            expected_executor_epoch,
            TaskEventKind::ControllerAttached,
            at,
            move |before, after, effective_at| {
                require_state(before, "attach a controller to", &[TaskState::Queued])?;
                if let Some(value) = &lease {
                    value.validate_after(effective_at)?;
                }
                after.state = TaskState::Running;
                after.started_at = Some(effective_at);
                after.executor_epoch = next_counter(before.executor_epoch, "executor epoch")?;
                after.controller = Some(controller.clone());
                after.lease = lease;
                Ok(Some(controller.actor()))
            },
        )
    }

    fn request_cancel(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord> {
        let at = normalize_timestamp(at)?;
        self.mutate(
            owner,
            id,
            expected_revision,
            expected_executor_epoch,
            TaskEventKind::CancelRequested,
            at,
            move |before, after, effective_at| match before.state {
                TaskState::Queued => {
                    after.state = TaskState::Cancelled;
                    after.finished_at = Some(effective_at);
                    after.failure_code = Some(FailureCode::Cancelled);
                    after.lease = None;
                    Ok(None)
                }
                TaskState::Running => {
                    after.state = TaskState::Cancelling;
                    Ok(None)
                }
                _ => Err(TaskStoreError::InvalidState {
                    id: before.id.clone(),
                    operation: "request cancellation of",
                    state: before.state,
                }),
            },
        )
    }

    fn settle(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        settlement: TaskSettlement,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord> {
        let at = normalize_timestamp(at)?;
        settlement.validate()?;
        validate_text("owner", owner, 256)?;
        let mut connection = self.connection()?;
        let transaction = write_transaction(&mut connection)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| TaskStoreError::NotFound { id: id.to_string() })?;
        if before.state.is_terminal() {
            let (state, failure_code, ledger_run_id) = settlement.parts();
            let exact_result = before.state == state
                && before.failure_code == failure_code
                && before.ledger_run_id.as_deref() == ledger_run_id;
            let current_or_commit_retry = expected_revision == before.revision
                || expected_revision.checked_add(1) == Some(before.revision);
            if exact_result
                && expected_executor_epoch == before.executor_epoch
                && current_or_commit_retry
            {
                transaction.commit()?;
                return Ok(before);
            }
            ensure_cas(&before, expected_revision, expected_executor_epoch)?;
            return Err(TaskStoreError::InvalidState {
                id: before.id,
                operation: "settle",
                state: before.state,
            });
        }
        ensure_cas(&before, expected_revision, expected_executor_epoch)?;
        let effective_at = std::cmp::max(at, before.updated_at);
        let (state, failure_code, ledger_run_id) = settlement.parts();
        let allowed = match before.state {
            TaskState::Queued => matches!(state, TaskState::Failed | TaskState::Cancelled),
            TaskState::Running | TaskState::Cancelling => true,
            _ => false,
        };
        if !allowed {
            return Err(TaskStoreError::InvalidState {
                id: before.id,
                operation: "settle",
                state: before.state,
            });
        }
        if let Some(run_id) = ledger_run_id {
            validate_stored_text("ledger run id", run_id, 256)?;
        }
        let mut after = before.clone();
        after.state = state;
        after.finished_at = Some(effective_at);
        after.failure_code = failure_code;
        after.ledger_run_id = ledger_run_id.map(str::to_string);
        after.lease = None;
        after.revision = next_counter(before.revision, "revision")?;
        after.updated_at = effective_at;
        update_snapshot(&transaction, &before, &after)?;
        let actor = before.controller.as_ref().map(ControllerRef::actor);
        insert_event(
            &transaction,
            owner,
            id,
            after.revision,
            effective_at,
            TaskEventKind::Settled,
            Some(before.state),
            after.state,
            actor.as_deref(),
            after.executor_epoch,
        )?;
        validate_database_files(&self.path)?;
        transaction.commit()?;
        Ok(after)
    }

    fn interrupt(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        code: FailureCode,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord> {
        let at = normalize_timestamp(at)?;
        if matches!(code, FailureCode::Cancelled | FailureCode::TimedOut) {
            return Err(TaskStoreError::InvalidInput(
                "interruption cannot use the cancelled or timed_out failure code".into(),
            ));
        }
        self.mutate(
            owner,
            id,
            expected_revision,
            expected_executor_epoch,
            TaskEventKind::Interrupted,
            at,
            move |before, after, effective_at| {
                require_state(
                    before,
                    "interrupt",
                    &[TaskState::Queued, TaskState::Running, TaskState::Cancelling],
                )?;
                after.state = TaskState::Interrupted;
                after.finished_at = Some(effective_at);
                after.failure_code = Some(code);
                after.lease = None;
                Ok(before.controller.as_ref().map(ControllerRef::actor))
            },
        )
    }

    fn claim_expired(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        controller: ControllerRef,
        lease: Lease,
        now: DateTime<Utc>,
    ) -> Result<TaskRecord> {
        let now = normalize_timestamp(now)?;
        let controller = normalize_controller(controller)?;
        let lease = normalize_lease(lease)?;
        controller.validate()?;
        self.mutate(
            owner,
            id,
            expected_revision,
            expected_executor_epoch,
            TaskEventKind::LeaseClaimed,
            now,
            move |before, after, effective_at| {
                require_state(
                    before,
                    "claim",
                    &[TaskState::Running, TaskState::Cancelling],
                )?;
                let Some(current_lease) = &before.lease else {
                    return Err(TaskStoreError::LeaseNotExpired {
                        id: before.id.clone(),
                    });
                };
                if current_lease.expires_at > effective_at {
                    return Err(TaskStoreError::LeaseNotExpired {
                        id: before.id.clone(),
                    });
                }
                lease.validate_after(effective_at)?;
                after.executor_epoch = next_counter(before.executor_epoch, "executor epoch")?;
                after.controller = Some(controller);
                after.lease = Some(lease.clone());
                Ok(Some(lease.owner.clone()))
            },
        )
    }

    fn renew_lease(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        lease_owner: &str,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<TaskRecord> {
        let now = normalize_timestamp(now)?;
        let expires_at = normalize_timestamp(expires_at)?;
        let replacement = Lease {
            owner: lease_owner.to_string(),
            expires_at,
        };
        self.mutate(
            owner,
            id,
            expected_revision,
            expected_executor_epoch,
            TaskEventKind::LeaseRenewed,
            now,
            move |before, after, effective_at| {
                require_state(
                    before,
                    "renew the lease for",
                    &[TaskState::Running, TaskState::Cancelling],
                )?;
                let Some(current) = &before.lease else {
                    return Err(TaskStoreError::LeaseOwnerMismatch {
                        id: before.id.clone(),
                        expected: lease_owner.to_string(),
                        actual: "<none>".to_string(),
                    });
                };
                if current.owner != lease_owner {
                    return Err(TaskStoreError::LeaseOwnerMismatch {
                        id: before.id.clone(),
                        expected: lease_owner.to_string(),
                        actual: current.owner.clone(),
                    });
                }
                if current.expires_at <= effective_at {
                    return Err(TaskStoreError::LeaseAlreadyExpired {
                        id: before.id.clone(),
                    });
                }
                replacement.validate_after(effective_at)?;
                if expires_at <= current.expires_at {
                    return Err(TaskStoreError::InvalidInput(
                        "renewed lease expiry must advance beyond the current expiry".into(),
                    ));
                }
                after.lease = Some(replacement);
                Ok(Some(lease_owner.to_string()))
            },
        )
    }
}

fn configure_connection(connection: &Connection) -> Result<()> {
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn enable_wal(connection: &Connection) -> Result<()> {
    let deadline = std::time::Instant::now() + BUSY_TIMEOUT;
    loop {
        match connection.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if matches!(
                    error.code,
                    ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
                ) && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn write_transaction(connection: &mut Connection) -> Result<Transaction<'_>> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // The connection-level check happens before this process owns SQLite's
    // writer lock. A newer process can migrate in that gap, so user_version
    // must be checked again after BEGIN IMMEDIATE has serialized all writers.
    require_current_schema(&transaction)?;
    Ok(transaction)
}

fn require_current_schema(connection: &Connection) -> Result<()> {
    let found = user_version(connection)?;
    if found > SCHEMA_VERSION {
        return Err(TaskStoreError::UnsupportedSchema {
            found,
            supported: SCHEMA_VERSION,
        });
    }
    if found != SCHEMA_VERSION {
        return Err(TaskStoreError::CorruptData(format!(
            "task database schema is {found}, expected {SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn validate_schema_version(connection: &Connection, version: u32) -> Result<()> {
    let objects = match version {
        1 => SCHEMA_OBJECTS_V1,
        2 => SCHEMA_OBJECTS_V2,
        _ => {
            return Err(TaskStoreError::CorruptData(format!(
                "no schema manifest exists for task schema version {version}"
            )));
        }
    };
    for &(object_type, name, table_name) in objects {
        validate_schema_object(connection, version, object_type, name, table_name)?;
    }
    validate_object_manifest(connection, objects)?;
    validate_schema_columns(connection, version)?;
    let foreign_key_errors: i64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        return Err(TaskStoreError::CorruptData(
            "task schema contains invalid foreign-key relationships".into(),
        ));
    }
    Ok(())
}

fn validate_schema_columns(connection: &Connection, version: u32) -> Result<()> {
    const TASK_V1: &[&str] = &[
        "id",
        "record_schema",
        "owner",
        "kind",
        "origin",
        "state",
        "task_digest",
        "target_key",
        "created_at_ms",
        "started_at_ms",
        "updated_at_ms",
        "finished_at_ms",
        "revision",
        "executor_epoch",
        "controller_kind",
        "controller_instance_id",
        "controller_pid",
        "controller_pgid",
        "controller_started_at_ms",
        "controller_birth_fingerprint",
        "lease_owner",
        "lease_expires_at_ms",
        "ledger_run_id",
        "failure_code",
    ];
    const TASK_V2: &[&str] = &[
        "owner",
        "id",
        "record_schema",
        "kind",
        "origin",
        "state",
        "task_digest",
        "target_key",
        "created_at_ms",
        "started_at_ms",
        "updated_at_ms",
        "finished_at_ms",
        "revision",
        "executor_epoch",
        "controller_kind",
        "controller_instance_id",
        "controller_pid",
        "controller_pgid",
        "controller_started_at_ms",
        "controller_birth_fingerprint",
        "lease_owner",
        "lease_expires_at_ms",
        "ledger_run_id",
        "failure_code",
    ];
    const EVENT_V1: &[&str] = &[
        "sequence",
        "task_id",
        "revision",
        "occurred_at_ms",
        "kind",
        "from_state",
        "to_state",
        "actor_instance",
        "executor_epoch",
    ];
    const EVENT_V2: &[&str] = &[
        "sequence",
        "owner",
        "task_id",
        "revision",
        "occurred_at_ms",
        "kind",
        "from_state",
        "to_state",
        "actor_instance",
        "executor_epoch",
    ];
    let (tasks, events) = match version {
        1 => (TASK_V1, EVENT_V1),
        2 => (TASK_V2, EVENT_V2),
        _ => unreachable!("version manifest selected above"),
    };
    for (table, expected) in [("tasks", tasks), ("task_events", events)] {
        let sql = format!("SELECT name FROM pragma_table_info('{table}') ORDER BY cid");
        let mut statement = connection.prepare(&sql)?;
        let actual = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if actual
            != expected
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>()
        {
            return Err(TaskStoreError::CorruptData(format!(
                "task schema table `{table}` has unexpected columns"
            )));
        }
    }
    Ok(())
}

fn validate_schema_object(
    connection: &Connection,
    version: u32,
    expected_type: &str,
    name: &str,
    expected_table: &str,
) -> Result<()> {
    let actual = connection
        .query_row(
            "SELECT type, tbl_name, sql FROM sqlite_schema WHERE name = ?1",
            [name],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|error| {
            TaskStoreError::CorruptData(format!(
                "could not inspect task schema object `{name}`: {error}"
            ))
        })?
        .ok_or_else(|| {
            TaskStoreError::CorruptData(format!(
                "task schema is missing required {expected_type} `{name}`"
            ))
        })?;

    if actual.0 != expected_type || actual.1 != expected_table {
        return Err(TaskStoreError::CorruptData(format!(
            "task schema object `{name}` is {} for `{}`, expected {expected_type} for `{expected_table}`",
            actual.0, actual.1
        )));
    }
    let actual_sql = actual.2.ok_or_else(|| {
        TaskStoreError::CorruptData(format!("task schema object `{name}` has no defining SQL"))
    })?;
    let expected_sql = migration_schema_statement(version, expected_type, name)?;
    if normalize_schema_sql(&actual_sql) != normalize_schema_sql(&expected_sql) {
        return Err(TaskStoreError::CorruptData(format!(
            "task schema object `{name}` does not match schema version {version}"
        )));
    }
    Ok(())
}

fn migration_schema_statement(version: u32, object_type: &str, name: &str) -> Result<String> {
    let (migration, source_name) = match (version, name) {
        (1, _) => (MIGRATION_0001, name),
        (2, _) => (MIGRATION_0002, name),
        _ => {
            return Err(TaskStoreError::CorruptData(format!(
                "no bundled task schema exists for version {version}"
            )));
        }
    };
    let prefix = format!("CREATE {} {source_name}", object_type.to_ascii_uppercase());
    let statement = migration
        .split(';')
        .map(str::trim)
        .find(|statement| statement.starts_with(&prefix))
        .ok_or_else(|| {
            TaskStoreError::CorruptData(format!(
                "bundled schema is missing definition for `{name}`"
            ))
        })?;
    Ok(statement.to_string())
}

fn validate_object_manifest(connection: &Connection, objects: &[(&str, &str, &str)]) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name FROM sqlite_schema \
         WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
    )?;
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut expected = objects
        .iter()
        .map(|(kind, name, table)| {
            (
                (*kind).to_string(),
                (*name).to_string(),
                (*table).to_string(),
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        return Err(TaskStoreError::CorruptData(
            "task schema contains missing or unexpected objects".into(),
        ));
    }
    Ok(())
}

fn require_empty_schema(connection: &Connection) -> Result<()> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if count != 0 {
        return Err(TaskStoreError::CorruptData(
            "unversioned task database contains schema objects".into(),
        ));
    }
    Ok(())
}

fn migrate_v1_to_v2(transaction: &Transaction<'_>) -> Result<()> {
    let source_tasks: i64 =
        transaction.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
    let source_events: i64 =
        transaction.query_row("SELECT COUNT(*) FROM task_events", [], |row| row.get(0))?;
    let source_max_sequence: Option<i64> =
        transaction.query_row("SELECT MAX(sequence) FROM task_events", [], |row| {
            row.get(0)
        })?;
    let source_sequence: Option<i64> = transaction
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'task_events'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| match error {
            rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::IntegralValueOutOfRange(..) => {
                TaskStoreError::CorruptData("invalid task event sequence state".into())
            }
            other => TaskStoreError::Sqlite(other),
        })?;
    let invalid_sequences: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM task_events WHERE sequence <= 0",
        [],
        |row| row.get(0),
    )?;
    if source_sequence.is_some_and(|sequence| sequence < 0) || invalid_sequences != 0 {
        return Err(TaskStoreError::CorruptData(
            "task event sequence state is invalid".into(),
        ));
    }
    if source_max_sequence.is_some_and(|maximum| source_sequence.is_none_or(|seq| seq < maximum)) {
        return Err(TaskStoreError::CorruptData(
            "task event sequence state is behind stored events".into(),
        ));
    }
    transaction
        .execute_batch(MIGRATION_0002)
        .map_err(|error| match error {
            rusqlite::Error::SqliteFailure(failure, _)
                if failure.code == ErrorCode::ConstraintViolation =>
            {
                TaskStoreError::CorruptData(
                    "task owner migration could not preserve stored data".into(),
                )
            }
            other => TaskStoreError::Sqlite(other),
        })?;
    if let Some(sequence) = source_sequence {
        let changed = transaction.execute(
            "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'task_events'",
            [sequence],
        )?;
        if changed == 0 {
            transaction.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES ('task_events', ?1)",
                [sequence],
            )?;
        }
    }

    let target_tasks: i64 =
        transaction.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
    let target_events: i64 =
        transaction.query_row("SELECT COUNT(*) FROM task_events", [], |row| row.get(0))?;
    let target_max_sequence: Option<i64> =
        transaction.query_row("SELECT MAX(sequence) FROM task_events", [], |row| {
            row.get(0)
        })?;
    let orphaned: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM task_events e LEFT JOIN tasks t \
         ON t.owner = e.owner AND t.id = e.task_id WHERE t.id IS NULL",
        [],
        |row| row.get(0),
    )?;
    if source_tasks != target_tasks
        || source_events != target_events
        || source_max_sequence != target_max_sequence
        || orphaned != 0
    {
        return Err(TaskStoreError::CorruptData(
            "task owner migration did not preserve every task and event".into(),
        ));
    }
    let foreign_key_errors: i64 =
        transaction.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        return Err(TaskStoreError::CorruptData(
            "task owner migration failed foreign-key validation".into(),
        ));
    }
    let sequence_state: Option<i64> = transaction
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'task_events'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let stale_sequence_rows: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM sqlite_sequence WHERE name IN ('task_events_v1', 'task_events_v2')",
        [],
        |row| row.get(0),
    )?;
    if stale_sequence_rows != 0 || sequence_state.unwrap_or(0) != source_sequence.unwrap_or(0) {
        return Err(TaskStoreError::CorruptData(
            "task owner migration did not preserve event sequence state".into(),
        ));
    }
    validate_schema_version(transaction, 2)?;
    transaction.pragma_update(None, "user_version", 2_u32)?;
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    let mut quoted_until = None;
    while let Some(character) = characters.next() {
        if let Some(terminator) = quoted_until {
            normalized.push(character);
            if character == terminator {
                if characters.peek() == Some(&terminator) {
                    if let Some(escaped) = characters.next() {
                        normalized.push(escaped);
                    }
                } else {
                    quoted_until = None;
                }
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => {
                quoted_until = Some(character);
                normalized.push(character);
            }
            '[' => {
                quoted_until = Some(']');
                normalized.push(character);
            }
            ';' => {}
            value if value.is_ascii_whitespace() => {}
            value => normalized.push(value),
        }
    }
    normalized
}

fn open_database(path: &Path) -> Result<Connection> {
    #[cfg(unix)]
    {
        reject_symlink_components(path)?;
        reject_existing_sidecar_symlinks(path)?;
        // Check before SQLite opens the files. SQLite may normalize a sidecar
        // mode during open, which would hide an unsafe at-rest state.
        validate_database_files(path)?;
    }
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Ok(Connection::open_with_flags(path, flags)?)
}

#[cfg(unix)]
fn prepare_database_path(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    reject_symlink_components(parent)?;
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
            .open(&candidate)
        {
            Ok(file) => {
                let configured = (|| -> std::io::Result<()> {
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
                    let metadata = file.metadata()?;
                    if !metadata.is_file() || metadata.permissions().mode() & 0o7777 != 0o600 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "new task database candidate is not a private regular file",
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
        "could not reserve a unique task database candidate",
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
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
fn validate_private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(TaskStoreError::InvalidInput(
            "task database parent must be a real directory".into(),
        ));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(TaskStoreError::InvalidInput(
            "task database parent must not be group- or world-writable".into(),
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
                return Err(TaskStoreError::InvalidInput(
                    "task database path must not traverse a symlink".into(),
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
    for candidate in [sqlite_sidecar(path, "-wal"), sqlite_sidecar(path, "-shm")] {
        match std::fs::symlink_metadata(candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(TaskStoreError::InvalidInput(
                    "task database sidecars must not be symlinks".into(),
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
        Ok(metadata) if metadata.file_type().is_symlink() => Err(TaskStoreError::InvalidInput(
            "task database files must not be symlinks".into(),
        )),
        Ok(metadata) if !metadata.is_file() => Err(TaskStoreError::InvalidInput(
            "task database files must be regular files".into(),
        )),
        Ok(metadata) if metadata.permissions().mode() & 0o7777 != 0o600 => {
            Err(TaskStoreError::InvalidInput(
                "task database file permissions must be 0600; repair them while the store is offline"
                    .into(),
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn normalize_timestamp(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis()).ok_or_else(|| {
        TaskStoreError::InvalidInput("timestamp is outside SQLite millisecond range".into())
    })
}

fn normalize_controller(controller: ControllerRef) -> Result<ControllerRef> {
    match controller {
        ControllerRef::InProcess { instance_id } => Ok(ControllerRef::InProcess { instance_id }),
        ControllerRef::ProcessGroup {
            pid,
            pgid,
            started_at,
            birth_fingerprint,
        } => Ok(ControllerRef::ProcessGroup {
            pid,
            pgid,
            started_at: normalize_timestamp(started_at)?,
            birth_fingerprint,
        }),
    }
}

fn normalize_lease(lease: Lease) -> Result<Lease> {
    Ok(Lease {
        owner: lease.owner,
        expires_at: normalize_timestamp(lease.expires_at)?,
    })
}

fn user_version(connection: &Connection) -> Result<u32> {
    let value: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    u32::try_from(value).map_err(|_| {
        TaskStoreError::CorruptData(format!(
            "invalid negative or oversized user_version {value}"
        ))
    })
}

fn get_in_connection(connection: &Connection, owner: &str, id: &str) -> Result<Option<TaskRecord>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM tasks WHERE owner = ?1 AND id = ?2");
    Ok(connection
        .query_row(&sql, params![owner, id], row_to_task)
        .optional()?)
}

fn get_in_transaction(
    transaction: &Transaction<'_>,
    owner: &str,
    id: &str,
) -> Result<Option<TaskRecord>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM tasks WHERE owner = ?1 AND id = ?2");
    Ok(transaction
        .query_row(&sql, params![owner, id], row_to_task)
        .optional()?)
}

fn insert_snapshot(transaction: &Transaction<'_>, record: &TaskRecord) -> Result<()> {
    let controller = ControllerColumns::from(record.controller.as_ref());
    transaction.execute(
        "INSERT INTO tasks (\
            id, record_schema, owner, kind, origin, state, task_digest, target_key, \
            created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision, executor_epoch, \
            controller_kind, controller_instance_id, controller_pid, controller_pgid, \
            controller_started_at_ms, controller_birth_fingerprint, lease_owner, \
            lease_expires_at_ms, ledger_run_id, failure_code\
         ) VALUES (\
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
            ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24\
         )",
        params![
            record.id,
            i64::from(RECORD_SCHEMA),
            record.owner,
            record.kind.as_str(),
            record.origin.as_str(),
            record.state.as_str(),
            record.task_digest,
            record.target_key,
            record.created_at.timestamp_millis(),
            record.started_at.map(|value| value.timestamp_millis()),
            record.updated_at.timestamp_millis(),
            record.finished_at.map(|value| value.timestamp_millis()),
            u64_to_i64(record.revision, "revision")?,
            u64_to_i64(record.executor_epoch, "executor epoch")?,
            controller.kind,
            controller.instance_id,
            controller.pid,
            controller.pgid,
            controller.started_at_ms,
            controller.birth_fingerprint,
            record.lease.as_ref().map(|value| value.owner.as_str()),
            record
                .lease
                .as_ref()
                .map(|value| value.expires_at.timestamp_millis()),
            record.ledger_run_id,
            record.failure_code.map(FailureCode::as_str),
        ],
    )?;
    Ok(())
}

fn update_snapshot(
    transaction: &Transaction<'_>,
    before: &TaskRecord,
    after: &TaskRecord,
) -> Result<()> {
    let controller = ControllerColumns::from(after.controller.as_ref());
    let changed = transaction.execute(
        "UPDATE tasks SET \
            state = ?1, started_at_ms = ?2, updated_at_ms = ?3, finished_at_ms = ?4, \
            revision = ?5, executor_epoch = ?6, controller_kind = ?7, \
            controller_instance_id = ?8, controller_pid = ?9, controller_pgid = ?10, \
            controller_started_at_ms = ?11, controller_birth_fingerprint = ?12, \
            lease_owner = ?13, lease_expires_at_ms = ?14, ledger_run_id = ?15, \
            failure_code = ?16 \
         WHERE owner = ?17 AND id = ?18 AND revision = ?19 AND executor_epoch = ?20",
        params![
            after.state.as_str(),
            after.started_at.map(|value| value.timestamp_millis()),
            after.updated_at.timestamp_millis(),
            after.finished_at.map(|value| value.timestamp_millis()),
            u64_to_i64(after.revision, "revision")?,
            u64_to_i64(after.executor_epoch, "executor epoch")?,
            controller.kind,
            controller.instance_id,
            controller.pid,
            controller.pgid,
            controller.started_at_ms,
            controller.birth_fingerprint,
            after.lease.as_ref().map(|value| value.owner.as_str()),
            after
                .lease
                .as_ref()
                .map(|value| value.expires_at.timestamp_millis()),
            after.ledger_run_id,
            after.failure_code.map(FailureCode::as_str),
            before.owner,
            before.id,
            u64_to_i64(before.revision, "revision")?,
            u64_to_i64(before.executor_epoch, "executor epoch")?,
        ],
    )?;
    if changed != 1 {
        return Err(TaskStoreError::Conflict {
            id: before.id.clone(),
            expected_revision: before.revision,
            actual_revision: after.revision,
            expected_executor_epoch: before.executor_epoch,
            actual_executor_epoch: after.executor_epoch,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_event(
    transaction: &Transaction<'_>,
    owner: &str,
    task_id: &str,
    revision: u64,
    occurred_at: DateTime<Utc>,
    kind: TaskEventKind,
    from_state: Option<TaskState>,
    to_state: TaskState,
    actor_instance: Option<&str>,
    executor_epoch: u64,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO task_events (\
            owner, task_id, revision, occurred_at_ms, kind, from_state, to_state, \
            actor_instance, executor_epoch\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            owner,
            task_id,
            u64_to_i64(revision, "revision")?,
            occurred_at.timestamp_millis(),
            kind.as_str(),
            from_state.map(TaskState::as_str),
            to_state.as_str(),
            actor_instance,
            u64_to_i64(executor_epoch, "executor epoch")?,
        ],
    )?;
    Ok(())
}

fn row_to_task(row: &Row<'_>) -> rusqlite::Result<TaskRecord> {
    let record_schema: i64 = stored_column(row, 1, "record_schema", Type::Integer)?;
    if record_schema != i64::from(RECORD_SCHEMA) {
        return Err(data_error(
            1,
            Type::Integer,
            format!("unsupported task record schema {record_schema}"),
        ));
    }

    let controller_kind: Option<String> = stored_column(row, 14, "controller_kind", Type::Text)?;
    let controller = match controller_kind.as_deref() {
        None => None,
        Some("in_process") => Some(ControllerRef::InProcess {
            instance_id: required_column(row, 15, "controller_instance_id", Type::Text)?,
        }),
        Some("process_group") => Some(ControllerRef::ProcessGroup {
            pid: i32_column(row, 16, "controller_pid")?,
            pgid: i32_column(row, 17, "controller_pgid")?,
            started_at: timestamp_column(row, 18, "controller_started_at_ms")?,
            birth_fingerprint: stored_column(row, 19, "controller_birth_fingerprint", Type::Text)?,
        }),
        Some(other) => {
            return Err(data_error(
                14,
                Type::Text,
                format!("unknown controller kind `{other}`"),
            ));
        }
    };

    let lease_owner: Option<String> = stored_column(row, 20, "lease_owner", Type::Text)?;
    let lease_expires_at_ms: Option<i64> =
        stored_column(row, 21, "lease_expires_at_ms", Type::Integer)?;
    let lease = match (lease_owner, lease_expires_at_ms) {
        (None, None) => None,
        (Some(owner), Some(expires_at_ms)) => Some(Lease {
            owner,
            expires_at: timestamp_from_millis(21, expires_at_ms)?,
        }),
        _ => {
            return Err(data_error(
                20,
                Type::Text,
                "lease owner and expiry must either both be set or both be null".into(),
            ));
        }
    };

    let failure_raw: Option<String> = stored_column(row, 23, "failure_code", Type::Text)?;
    let kind = stored_column::<String>(row, 3, "kind", Type::Text)?;
    let origin = stored_column::<String>(row, 4, "origin", Type::Text)?;
    let state = stored_column::<String>(row, 5, "state", Type::Text)?;
    let record = TaskRecord {
        id: stored_column(row, 0, "id", Type::Text)?,
        owner: stored_column(row, 2, "owner", Type::Text)?,
        kind: parse_column(&kind, 3)?,
        origin: parse_column(&origin, 4)?,
        state: parse_column(&state, 5)?,
        task_digest: stored_column(row, 6, "task_digest", Type::Text)?,
        target_key: stored_column(row, 7, "target_key", Type::Text)?,
        created_at: timestamp_column(row, 8, "created_at_ms")?,
        started_at: optional_timestamp_column(row, 9, "started_at_ms")?,
        updated_at: timestamp_column(row, 10, "updated_at_ms")?,
        finished_at: optional_timestamp_column(row, 11, "finished_at_ms")?,
        revision: u64_column(row, 12, "revision")?,
        executor_epoch: u64_column(row, 13, "executor_epoch")?,
        controller,
        lease,
        ledger_run_id: stored_column(row, 22, "ledger_run_id", Type::Text)?,
        failure_code: failure_raw
            .as_deref()
            .map(|value| parse_column(value, 23))
            .transpose()?,
    };
    validate_stored_task(&record).map_err(|message| data_error(0, Type::Text, message))?;
    Ok(record)
}

fn row_to_event(row: &Row<'_>) -> rusqlite::Result<TaskEvent> {
    let event_kind = stored_column::<String>(row, 5, "kind", Type::Text)?;
    let from_state: Option<String> = stored_column(row, 6, "from_state", Type::Text)?;
    let to_state = stored_column::<String>(row, 7, "to_state", Type::Text)?;
    let event = TaskEvent {
        sequence: u64_column(row, 0, "sequence")?,
        owner: stored_column(row, 1, "owner", Type::Text)?,
        task_id: stored_column(row, 2, "task_id", Type::Text)?,
        revision: u64_column(row, 3, "revision")?,
        occurred_at: timestamp_column(row, 4, "occurred_at_ms")?,
        kind: parse_column(&event_kind, 5)?,
        from_state: from_state
            .as_deref()
            .map(|value| parse_column(value, 6))
            .transpose()?,
        to_state: parse_column(&to_state, 7)?,
        actor_instance: stored_column(row, 8, "actor_instance", Type::Text)?,
        executor_epoch: u64_column(row, 9, "executor_epoch")?,
    };
    validate_stored_event(&event).map_err(|message| data_error(1, Type::Text, message))?;
    Ok(event)
}

struct ControllerColumns<'a> {
    kind: Option<&'static str>,
    instance_id: Option<&'a str>,
    pid: Option<i64>,
    pgid: Option<i64>,
    started_at_ms: Option<i64>,
    birth_fingerprint: Option<&'a str>,
}

impl<'a> From<Option<&'a ControllerRef>> for ControllerColumns<'a> {
    fn from(value: Option<&'a ControllerRef>) -> Self {
        match value {
            None => Self {
                kind: None,
                instance_id: None,
                pid: None,
                pgid: None,
                started_at_ms: None,
                birth_fingerprint: None,
            },
            Some(ControllerRef::InProcess { instance_id }) => Self {
                kind: Some("in_process"),
                instance_id: Some(instance_id),
                pid: None,
                pgid: None,
                started_at_ms: None,
                birth_fingerprint: None,
            },
            Some(ControllerRef::ProcessGroup {
                pid,
                pgid,
                started_at,
                birth_fingerprint,
            }) => Self {
                kind: Some("process_group"),
                instance_id: None,
                pid: Some(i64::from(*pid)),
                pgid: Some(i64::from(*pgid)),
                started_at_ms: Some(started_at.timestamp_millis()),
                birth_fingerprint: birth_fingerprint.as_deref(),
            },
        }
    }
}

fn ensure_cas(record: &TaskRecord, expected_revision: u64, expected_epoch: u64) -> Result<()> {
    if record.revision != expected_revision || record.executor_epoch != expected_epoch {
        return Err(TaskStoreError::Conflict {
            id: record.id.clone(),
            expected_revision,
            actual_revision: record.revision,
            expected_executor_epoch: expected_epoch,
            actual_executor_epoch: record.executor_epoch,
        });
    }
    Ok(())
}

fn require_state(
    record: &TaskRecord,
    operation: &'static str,
    allowed: &[TaskState],
) -> Result<()> {
    if allowed.contains(&record.state) {
        Ok(())
    } else {
        Err(TaskStoreError::InvalidState {
            id: record.id.clone(),
            operation,
            state: record.state,
        })
    }
}

fn next_counter(value: u64, field: &str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| TaskStoreError::CorruptData(format!("{field} overflow")))
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| TaskStoreError::CorruptData(format!("{field} exceeds SQLite INTEGER")))
}

fn usize_to_i64(value: usize, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| TaskStoreError::InvalidInput(format!("{field} is too large")))
}

fn validate_stored_text(field: &str, value: &str, max_len: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max_len || value.contains('\0') {
        return Err(TaskStoreError::InvalidInput(format!(
            "{field} is empty, oversized, or contains NUL"
        )));
    }
    Ok(())
}

fn validate_stored_task(record: &TaskRecord) -> std::result::Result<(), String> {
    validate_text("task id", &record.id, 256).map_err(|error| error.to_string())?;
    validate_text("owner", &record.owner, 256).map_err(|error| error.to_string())?;
    validate_task_digest(&record.task_digest).map_err(|error| error.to_string())?;
    validate_text("target key", &record.target_key, 512).map_err(|error| error.to_string())?;
    if let Some(controller) = &record.controller {
        controller.validate().map_err(|error| error.to_string())?;
    }
    if let Some(lease) = &record.lease {
        validate_text("lease owner", &lease.owner, 256).map_err(|error| error.to_string())?;
    }
    if let Some(run_id) = &record.ledger_run_id {
        validate_text("ledger run id", run_id, 256).map_err(|error| error.to_string())?;
    }

    if record.updated_at < record.created_at {
        return Err("task updated_at precedes created_at".into());
    }
    if record.started_at.is_some() != record.controller.is_some() {
        return Err(
            "task controller and started_at must either both be set or both be null".into(),
        );
    }
    if let Some(started_at) = record.started_at {
        if started_at < record.created_at || started_at > record.updated_at {
            return Err("task started_at is outside created_at..=updated_at".into());
        }
    }
    if record.controller.is_some() != (record.executor_epoch > 0) {
        return Err("task controller presence does not match executor epoch ownership".into());
    }

    if record.state == TaskState::Queued {
        if record.revision != 0 {
            return Err("queued task must have revision zero".into());
        }
    } else if record.revision == 0 {
        return Err("non-queued task must have a positive revision".into());
    }

    if record.state.is_terminal() {
        let finished_at = record
            .finished_at
            .ok_or_else(|| "terminal task has no finished_at".to_string())?;
        if finished_at != record.updated_at {
            return Err("terminal task finished_at must equal updated_at".into());
        }
        if record.lease.is_some() {
            return Err("terminal task must not retain a lease".into());
        }
    } else if record.finished_at.is_some() {
        return Err("non-terminal task must not have finished_at".into());
    }

    let has_controller = record.controller.is_some();
    match record.state {
        TaskState::Queued => {
            if has_controller || record.lease.is_some() {
                return Err("queued task must not have a controller or lease".into());
            }
        }
        TaskState::Running | TaskState::Cancelling => {
            if !has_controller {
                return Err("active task must have a controller".into());
            }
        }
        TaskState::Succeeded | TaskState::TimedOut => {
            if !has_controller {
                return Err("succeeded or timed-out task must have a controller".into());
            }
        }
        TaskState::Failed | TaskState::Cancelled | TaskState::Interrupted => {}
    }

    match record.state {
        TaskState::Queued | TaskState::Running | TaskState::Cancelling | TaskState::Succeeded => {
            if record.failure_code.is_some() {
                return Err("task state must not carry a failure code".into());
            }
        }
        TaskState::Failed => {
            if record.failure_code.is_none()
                || matches!(
                    record.failure_code,
                    Some(FailureCode::Cancelled | FailureCode::TimedOut)
                )
            {
                return Err("failed task has an invalid failure code".into());
            }
        }
        TaskState::TimedOut => {
            if record.failure_code != Some(FailureCode::TimedOut) {
                return Err("timed-out task must carry timed_out failure code".into());
            }
        }
        TaskState::Cancelled => {
            if record.failure_code != Some(FailureCode::Cancelled) {
                return Err("cancelled task must carry cancelled failure code".into());
            }
        }
        TaskState::Interrupted => {
            if record.failure_code.is_none()
                || matches!(
                    record.failure_code,
                    Some(FailureCode::Cancelled | FailureCode::TimedOut)
                )
            {
                return Err("interrupted task has an invalid failure code".into());
            }
        }
    }

    if matches!(
        record.state,
        TaskState::Queued | TaskState::Running | TaskState::Cancelling | TaskState::Interrupted
    ) && record.ledger_run_id.is_some()
    {
        return Err("non-settled task must not carry a ledger run id".into());
    }
    if record.lease.is_some() && !matches!(record.state, TaskState::Running | TaskState::Cancelling)
    {
        return Err("lease is only valid for an active task".into());
    }
    Ok(())
}

fn validate_stored_event(event: &TaskEvent) -> std::result::Result<(), String> {
    validate_text("event owner", &event.owner, 256).map_err(|error| error.to_string())?;
    validate_text("event task id", &event.task_id, 256).map_err(|error| error.to_string())?;
    if let Some(actor) = &event.actor_instance {
        validate_text("event actor", actor, 256).map_err(|error| error.to_string())?;
    }
    if event.sequence == 0 {
        return Err("event sequence must be positive".into());
    }

    let actor_present = event.actor_instance.is_some();
    let valid = match event.kind {
        TaskEventKind::Created => {
            event.revision == 0
                && event.executor_epoch == 0
                && event.from_state.is_none()
                && event.to_state == TaskState::Queued
                && !actor_present
        }
        TaskEventKind::ControllerAttached => {
            event.revision > 0
                && event.executor_epoch > 0
                && event.from_state == Some(TaskState::Queued)
                && event.to_state == TaskState::Running
                && actor_present
        }
        TaskEventKind::CancelRequested => {
            event.revision > 0
                && !actor_present
                && matches!(
                    (event.from_state, event.to_state, event.executor_epoch),
                    (Some(TaskState::Queued), TaskState::Cancelled, 0)
                )
                || event.revision > 0
                    && !actor_present
                    && event.executor_epoch > 0
                    && event.from_state == Some(TaskState::Running)
                    && event.to_state == TaskState::Cancelling
        }
        TaskEventKind::Settled => {
            let from_queued = event.from_state == Some(TaskState::Queued)
                && event.executor_epoch == 0
                && matches!(event.to_state, TaskState::Failed | TaskState::Cancelled)
                && !actor_present;
            let from_active = matches!(
                event.from_state,
                Some(TaskState::Running | TaskState::Cancelling)
            ) && event.executor_epoch > 0
                && matches!(
                    event.to_state,
                    TaskState::Succeeded
                        | TaskState::Failed
                        | TaskState::TimedOut
                        | TaskState::Cancelled
                )
                && actor_present;
            event.revision > 0 && (from_queued || from_active)
        }
        TaskEventKind::Interrupted => {
            let from_queued = event.from_state == Some(TaskState::Queued)
                && event.executor_epoch == 0
                && !actor_present;
            let from_active = matches!(
                event.from_state,
                Some(TaskState::Running | TaskState::Cancelling)
            ) && event.executor_epoch > 0
                && actor_present;
            event.revision > 0
                && event.to_state == TaskState::Interrupted
                && (from_queued || from_active)
        }
        TaskEventKind::LeaseClaimed | TaskEventKind::LeaseRenewed => {
            event.revision > 0
                && event.executor_epoch > 0
                && actor_present
                && matches!(
                    (event.from_state, event.to_state),
                    (Some(TaskState::Running), TaskState::Running)
                        | (Some(TaskState::Cancelling), TaskState::Cancelling)
                )
        }
    };
    if !valid {
        return Err(format!(
            "event {:?} has an invalid lifecycle transition",
            event.kind
        ));
    }
    Ok(())
}

fn parse_column<T>(value: &str, index: usize) -> rusqlite::Result<T>
where
    T: FromStr<Err = TaskStoreError>,
{
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}

fn timestamp_column(row: &Row<'_>, index: usize, field: &str) -> rusqlite::Result<DateTime<Utc>> {
    timestamp_from_millis(index, stored_column(row, index, field, Type::Integer)?)
}

fn optional_timestamp_column(
    row: &Row<'_>,
    index: usize,
    field: &str,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    stored_column::<Option<i64>>(row, index, field, Type::Integer)?
        .map(|value| timestamp_from_millis(index, value))
        .transpose()
}

fn timestamp_from_millis(index: usize, value: i64) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value).ok_or_else(|| {
        data_error(
            index,
            Type::Integer,
            format!("timestamp milliseconds {value} are out of range"),
        )
    })
}

fn u64_column(row: &Row<'_>, index: usize, field: &str) -> rusqlite::Result<u64> {
    let value: i64 = stored_column(row, index, field, Type::Integer)?;
    u64::try_from(value).map_err(|_| {
        data_error(
            index,
            Type::Integer,
            format!("{field} must not be negative"),
        )
    })
}

fn i32_column(row: &Row<'_>, index: usize, field: &str) -> rusqlite::Result<i32> {
    let value: i64 = required_column(row, index, field, Type::Integer)?;
    i32::try_from(value)
        .map_err(|_| data_error(index, Type::Integer, format!("{field} exceeds an i32")))
}

fn required_column<T>(
    row: &Row<'_>,
    index: usize,
    field: &str,
    value_type: Type,
) -> rusqlite::Result<T>
where
    T: rusqlite::types::FromSql,
{
    let value: Option<T> = stored_column(row, index, field, value_type)?;
    value.ok_or_else(|| {
        data_error(
            index,
            Type::Null,
            format!("required column {field} is null"),
        )
    })
}

fn stored_column<T>(
    row: &Row<'_>,
    index: usize,
    field: &str,
    value_type: Type,
) -> rusqlite::Result<T>
where
    T: rusqlite::types::FromSql,
{
    row.get(index).map_err(|error| {
        data_error(
            index,
            value_type,
            format!("stored column `{field}` cannot be decoded: {error}"),
        )
    })
}

fn data_error(index: usize, value_type: Type, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        value_type,
        Box::new(TaskStoreError::CorruptData(message)),
    )
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::process::Command;
    #[cfg(unix)]
    use std::sync::{Arc, Barrier};

    #[cfg(unix)]
    use rusqlite::ErrorCode;

    use super::*;

    #[cfg(unix)]
    const LOCK_PROBE_DATABASE: &str = "VYANE_TASK_LOCK_PROBE_DATABASE";

    #[cfg(unix)]
    #[test]
    fn first_creation_closes_candidates_before_atomic_publication() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().expect("create temporary database directory");
        let path = directory.path().join("first-create.sqlite3");
        let candidates = (0..8)
            .map(|_| {
                create_closed_database_candidate(directory.path())
                    .expect("create closed database candidate")
            })
            .collect::<Vec<_>>();
        assert!(
            !path.exists(),
            "the final path must stay hidden while candidates are prepared"
        );
        for candidate in &candidates {
            let metadata =
                std::fs::symlink_metadata(candidate).expect("inspect database candidate");
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
                    publish_database_candidate(&candidate, &path)
                        .expect("publish database candidate");
                })
            })
            .collect::<Vec<_>>();
        for publisher in publishers {
            publisher.join().expect("join database publisher");
        }

        validate_database_files(&path).expect("validate published database");
        assert_eq!(
            std::fs::symlink_metadata(&*path)
                .expect("inspect published database")
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        assert!(
            std::fs::read_dir(directory.path())
                .expect("list database directory")
                .all(|entry| {
                    !entry
                        .expect("read database directory entry")
                        .file_name()
                        .to_string_lossy()
                        .starts_with(DATABASE_CREATE_PREFIX)
                })
        );

        let rejected = create_closed_database_candidate(directory.path())
            .expect("create candidate for rejected publication");
        let unsafe_winner = directory.path().join("unsafe-winner.sqlite3");
        std::fs::write(&unsafe_winner, b"unsafe winner").expect("create unsafe winner");
        std::fs::set_permissions(&unsafe_winner, std::fs::Permissions::from_mode(0o644))
            .expect("make winner mode unsafe");
        assert!(matches!(
            publish_database_candidate(&rejected, &unsafe_winner),
            Err(TaskStoreError::InvalidInput(_))
        ));
        assert!(!rejected.exists());
    }

    #[test]
    fn write_transaction_rechecks_schema_after_acquiring_the_writer_lock() {
        let directory = tempfile::TempDir::new().expect("create temporary database directory");
        let path = directory.path().join("schema-race.sqlite3");
        let store = SqliteTaskStore::open(&path).expect("initialize task store");

        // This connection has already passed the ordinary connection-level
        // schema check, exactly like a writer paused immediately before BEGIN.
        let mut stale_connection = store.connection().expect("open prechecked connection");
        let upgraded = Connection::open(&path).expect("open migration connection");
        upgraded
            .pragma_update(None, "user_version", 3)
            .expect("advance database schema version");
        drop(upgraded);

        let error = match write_transaction(&mut stale_connection) {
            Ok(_) => panic!("stale writer must not enter a newer schema"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            TaskStoreError::UnsupportedSchema {
                found: 3,
                supported: 2
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn preparing_an_existing_database_does_not_release_its_posix_writer_lock() {
        let directory = tempfile::TempDir::new().expect("create temporary database directory");
        let path = directory.path().join("lock-probe.sqlite3");
        prepare_database_path(&path).expect("prepare database path");

        let mut connection = open_database(&path).expect("open lock holder");
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .expect("select rollback journal mode");
        connection
            .execute_batch("CREATE TABLE lock_probe (value INTEGER NOT NULL);")
            .expect("create lock probe table");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("acquire writer lock");
        transaction
            .execute("INSERT INTO lock_probe (value) VALUES (1)", [])
            .expect("write inside held transaction");

        prepare_database_path(&path).expect("validate existing database without reopening it");

        let status = Command::new(std::env::current_exe().expect("resolve test executable"))
            .arg("sqlite::tests::posix_writer_lock_probe")
            .arg("--exact")
            .arg("--ignored")
            .arg("--nocapture")
            .env(LOCK_PROBE_DATABASE, &path)
            .status()
            .expect("run external lock probe");
        assert!(status.success(), "external writer acquired the lock");

        transaction.commit().expect("commit held transaction");
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_existing_database_modes_fail_closed_without_repair() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().expect("create temporary database directory");
        let path = directory.path().join("unsafe-mode.sqlite3");
        prepare_database_path(&path).expect("create database file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
            .expect("make database mode unsafe");

        let error = SqliteTaskStore::open(&path).expect_err("unsafe mode must be rejected");
        assert!(matches!(error, TaskStoreError::InvalidInput(_)));
        assert_eq!(
            std::fs::symlink_metadata(&path)
                .expect("read database metadata")
                .permissions()
                .mode()
                & 0o7777,
            0o640,
            "online validation must not repair an existing database"
        );

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restore safe main database mode");
        let wal = sqlite_sidecar(&path, "-wal");
        std::fs::write(&wal, b"unsafe sidecar fixture").expect("create sidecar fixture");
        std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o644))
            .expect("make sidecar mode unsafe");
        let error = SqliteTaskStore::open(&path).expect_err("unsafe sidecar must be rejected");
        assert!(matches!(error, TaskStoreError::InvalidInput(_)));
        assert_eq!(
            std::fs::symlink_metadata(&wal)
                .expect("read sidecar metadata")
                .permissions()
                .mode()
                & 0o7777,
            0o644,
            "online validation must not repair an existing sidecar"
        );
    }

    #[cfg(unix)]
    #[test]
    fn database_paths_and_sidecars_reject_symlinks() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let directory = tempfile::TempDir::new().expect("create temporary database directory");
        let real_parent = directory.path().join("real");
        std::fs::create_dir(&real_parent).expect("create real parent");
        std::fs::set_permissions(&real_parent, std::fs::Permissions::from_mode(0o700))
            .expect("secure real parent");
        let linked_parent = directory.path().join("linked");
        symlink(&real_parent, &linked_parent).expect("create parent symlink");
        let error = SqliteTaskStore::open(linked_parent.join("tasks.sqlite3"))
            .expect_err("symlinked parent must be rejected");
        assert!(matches!(error, TaskStoreError::InvalidInput(_)));

        let main_target = directory.path().join("main-target.sqlite3");
        std::fs::write(&main_target, b"target").expect("create main symlink target");
        std::fs::set_permissions(&main_target, std::fs::Permissions::from_mode(0o600))
            .expect("secure main symlink target");
        let linked_main = directory.path().join("linked-main.sqlite3");
        symlink(&main_target, &linked_main).expect("create main database symlink");
        let error = SqliteTaskStore::open(&linked_main)
            .expect_err("main database symlink must be rejected");
        assert!(matches!(error, TaskStoreError::InvalidInput(_)));

        let non_regular = directory.path().join("directory.sqlite3");
        std::fs::create_dir(&non_regular).expect("create non-regular database fixture");
        let error = SqliteTaskStore::open(&non_regular)
            .expect_err("non-regular database path must be rejected");
        assert!(matches!(error, TaskStoreError::InvalidInput(_)));

        let path = directory.path().join("tasks.sqlite3");
        prepare_database_path(&path).expect("create main database");
        let symlink_target = directory.path().join("sidecar-target");
        std::fs::write(&symlink_target, b"target").expect("create symlink target");
        std::fs::set_permissions(&symlink_target, std::fs::Permissions::from_mode(0o600))
            .expect("secure symlink target");
        symlink(&symlink_target, sqlite_sidecar(&path, "-wal")).expect("create sidecar symlink");
        let error = SqliteTaskStore::open(&path).expect_err("sidecar symlink must be rejected");
        assert!(matches!(error, TaskStoreError::InvalidInput(_)));
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess-only POSIX writer lock probe"]
    fn posix_writer_lock_probe() {
        let Some(path) = std::env::var_os(LOCK_PROBE_DATABASE) else {
            return;
        };
        let connection = Connection::open(path).expect("open probe database");
        connection
            .busy_timeout(Duration::from_millis(100))
            .expect("configure probe timeout");
        let error = connection
            .execute_batch("BEGIN IMMEDIATE")
            .expect_err("external writer unexpectedly acquired the lock");
        assert!(matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        ));
    }
}
