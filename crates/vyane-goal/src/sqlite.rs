use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::types::{Type, Value};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension as _, Row, Transaction,
    TransactionBehavior, params, params_from_iter,
};
use uuid::Uuid;

use crate::{
    AcceptanceCriterion, GoalEvent, GoalEventKind, GoalQuery, GoalRecord, GoalStatus, GoalStore,
    GoalStoreError, NewGoal, Result,
    model::{
        validate_detail, validate_goal_id, validate_optional_reason, validate_owner, validate_stage,
    },
};

pub const SCHEMA_VERSION: u32 = 1;
const RECORD_SCHEMA: u32 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const MIGRATION_0001: &str = include_str!("../migrations/0001_goals.sql");

const GOAL_COLUMNS: &str = "\
    id, owner, title, description, status, priority, parent_goal_id, acceptance_json, \
    created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision, \
    completion_summary, failure_reason, pause_reason, cancel_reason";

#[derive(Debug, Clone)]
pub struct SqliteGoalStore {
    path: PathBuf,
}

impl SqliteGoalStore {
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
        configure_connection(&connection)?;

        let found = user_version(&connection)?;
        if found > SCHEMA_VERSION {
            return Err(GoalStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let found = user_version(&transaction)?;
        if found == 0 {
            require_empty_schema(&transaction)?;
            transaction.execute_batch(MIGRATION_0001)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        validate_schema(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection> {
        let connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        configure_connection(&connection)?;
        let found = user_version(&connection)?;
        if found != SCHEMA_VERSION {
            return Err(if found > SCHEMA_VERSION {
                GoalStoreError::UnsupportedSchema {
                    found,
                    supported: SCHEMA_VERSION,
                }
            } else {
                GoalStoreError::CorruptData(format!(
                    "expected schema {SCHEMA_VERSION}, found schema {found}"
                ))
            });
        }
        Ok(connection)
    }

    fn mutate<F>(
        &self,
        owner: &str,
        id: &str,
        kind: GoalEventKind,
        operation: &'static str,
        occurred_at: DateTime<Utc>,
        mutation: F,
    ) -> Result<(GoalRecord, GoalEvent)>
    where
        F: FnOnce(
            &GoalRecord,
            &mut GoalRecord,
            DateTime<Utc>,
        ) -> Result<(Option<String>, Option<String>)>,
    {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        let occurred_at = normalize_timestamp(occurred_at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        let effective_at = std::cmp::max(occurred_at, before.updated_at);
        let mut after = before.clone();
        let (stage, detail) =
            mutation(&before, &mut after, effective_at).map_err(|error| match error {
                GoalStoreError::InvalidStatus { .. } => GoalStoreError::InvalidStatus {
                    id: id.to_string(),
                    operation,
                    status: before.status,
                },
                other => other,
            })?;
        after.revision = before
            .revision
            .checked_add(1)
            .ok_or_else(|| GoalStoreError::CorruptData("goal revision overflow".into()))?;
        after.updated_at = effective_at;
        update_snapshot(&transaction, &before, &after)?;
        let event = insert_event(
            &transaction,
            &after,
            kind,
            Some(before.status),
            effective_at,
            stage.as_deref(),
            detail.as_deref(),
        )?;
        transaction.commit()?;
        Ok((after, event))
    }
}

impl GoalStore for SqliteGoalStore {
    fn create(&self, owner: &str, mut goal: NewGoal) -> Result<GoalRecord> {
        validate_owner(owner)?;
        goal.created_at = normalize_timestamp(goal.created_at)?;
        goal.validate()?;
        let id = goal
            .id
            .take()
            .unwrap_or_else(|| format!("goal-{}", Uuid::now_v7()));
        validate_goal_id(&id)?;
        let record = GoalRecord {
            id,
            owner: owner.to_string(),
            title: goal.title,
            description: goal.description,
            status: GoalStatus::Queued,
            priority: goal.priority,
            parent_goal_id: goal.parent_goal_id,
            acceptance_criteria: goal.acceptance_criteria,
            created_at: goal.created_at,
            started_at: None,
            updated_at: goal.created_at,
            finished_at: None,
            revision: 0,
            completion_summary: None,
            failure_reason: None,
            pause_reason: None,
            cancel_reason: None,
        };
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let acceptance_json = serde_json::to_string(&record.acceptance_criteria)?;
        let inserted = transaction.execute(
            "INSERT INTO goals (owner, id, record_schema, title, description, status, priority, \
             parent_goal_id, acceptance_json, created_at_ms, started_at_ms, updated_at_ms, \
             finished_at_ms, revision, completion_summary, failure_reason, pause_reason, \
             cancel_reason) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?10, \
             NULL, 0, NULL, NULL, NULL, NULL)",
            params![
                record.owner,
                record.id,
                RECORD_SCHEMA,
                record.title,
                record.description,
                record.status.as_str(),
                record.priority,
                record.parent_goal_id,
                acceptance_json,
                record.created_at.timestamp_millis(),
            ],
        );
        if let Err(error) = inserted {
            return Err(map_create_error(error, &record.id));
        }
        insert_event(
            &transaction,
            &record,
            GoalEventKind::Created,
            None,
            record.created_at,
            None,
            None,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    fn get(&self, owner: &str, id: &str) -> Result<Option<GoalRecord>> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        get_in_connection(&self.connection()?, owner, id)
    }

    fn list(&self, owner: &str, query: &GoalQuery) -> Result<Vec<GoalRecord>> {
        validate_owner(owner)?;
        query.validate()?;
        let connection = self.connection()?;
        let mut sql = format!("SELECT {GOAL_COLUMNS} FROM goals WHERE owner = ?");
        let mut values = vec![Value::Text(owner.to_string())];
        if !query.statuses.is_empty() {
            sql.push_str(" AND status IN (");
            sql.push_str(&vec!["?"; query.statuses.len()].join(","));
            sql.push(')');
            values.extend(
                query
                    .statuses
                    .iter()
                    .map(|status| Value::Text(status.as_str().to_string())),
            );
        }
        if let Some(parent) = &query.parent_goal_id {
            sql.push_str(" AND parent_goal_id = ?");
            values.push(Value::Text(parent.clone()));
        }
        sql.push_str(" ORDER BY priority ASC, updated_at_ms DESC, id ASC");
        if query.limit > 0 {
            sql.push_str(" LIMIT ?");
            values.push(Value::Integer(i64::try_from(query.limit).map_err(
                |_| GoalStoreError::InvalidInput("limit is outside the supported range".into()),
            )?));
        }
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), row_to_record)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(GoalStoreError::from)
    }

    fn next_queued(&self, owner: &str) -> Result<Option<GoalRecord>> {
        validate_owner(owner)?;
        let connection = self.connection()?;
        let sql = format!(
            "SELECT {GOAL_COLUMNS} FROM goals WHERE owner = ?1 AND status = 'queued' \
             ORDER BY priority ASC, created_at_ms ASC, id ASC LIMIT 1"
        );
        connection
            .query_row(&sql, [owner], row_to_record)
            .optional()
            .map_err(GoalStoreError::from)
    }

    fn events(&self, owner: &str, id: &str) -> Result<Vec<GoalEvent>> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        let connection = self.connection()?;
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM goals WHERE owner = ?1 AND id = ?2)",
            params![owner, id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(GoalStoreError::NotFound { id: id.to_string() });
        }
        let mut statement = connection.prepare(
            "SELECT sequence, event_id, owner, goal_id, revision, occurred_at_ms, kind, \
             from_status, to_status, stage, detail FROM goal_events \
             WHERE owner = ?1 AND goal_id = ?2 ORDER BY revision ASC",
        )?;
        let rows = statement.query_map(params![owner, id], row_to_event)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(GoalStoreError::from)
    }

    fn start(&self, owner: &str, id: &str, at: DateTime<Utc>) -> Result<GoalRecord> {
        self.mutate(
            owner,
            id,
            GoalEventKind::Started,
            "start",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::InProgress, "start")?;
                after.status = GoalStatus::InProgress;
                if after.started_at.is_none() {
                    after.started_at = Some(effective_at);
                }
                Ok((None, None))
            },
        )
        .map(|(record, _)| record)
    }

    fn progress(
        &self,
        owner: &str,
        id: &str,
        stage: &str,
        detail: &str,
        at: DateTime<Utc>,
    ) -> Result<GoalEvent> {
        validate_stage(stage)?;
        validate_detail(detail)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Progress,
            "record progress on",
            at,
            |_before, _after, _effective_at| {
                Ok((Some(stage.to_string()), Some(detail.to_string())))
            },
        )
        .map(|(_, event)| event)
    }

    fn pause(
        &self,
        owner: &str,
        id: &str,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_reason("pause reason", reason)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Paused,
            "pause",
            at,
            |before, after, _effective_at| {
                ensure_transition(before, GoalStatus::Paused, "pause")?;
                after.status = GoalStatus::Paused;
                if let Some(reason) = reason {
                    after.pause_reason = Some(reason.to_string());
                }
                Ok((None, reason.map(str::to_string)))
            },
        )
        .map(|(record, _)| record)
    }

    fn resume(&self, owner: &str, id: &str, at: DateTime<Utc>) -> Result<GoalRecord> {
        self.mutate(
            owner,
            id,
            GoalEventKind::Resumed,
            "resume",
            at,
            |before, after, _effective_at| {
                ensure_transition(before, GoalStatus::InProgress, "resume")?;
                after.status = GoalStatus::InProgress;
                Ok((None, None))
            },
        )
        .map(|(record, _)| record)
    }

    fn done(
        &self,
        owner: &str,
        id: &str,
        summary: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_reason("completion summary", summary)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Completed,
            "complete",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Completed, "complete")?;
                after.status = GoalStatus::Completed;
                after.finished_at = Some(effective_at);
                if let Some(summary) = summary {
                    after.completion_summary = Some(summary.to_string());
                }
                Ok((None, summary.map(str::to_string)))
            },
        )
        .map(|(record, _)| record)
    }

    fn fail(&self, owner: &str, id: &str, reason: &str, at: DateTime<Utc>) -> Result<GoalRecord> {
        validate_optional_reason("failure reason", Some(reason))?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Failed,
            "fail",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Failed, "fail")?;
                after.status = GoalStatus::Failed;
                after.finished_at = Some(effective_at);
                after.failure_reason = Some(reason.to_string());
                Ok((None, Some(reason.to_string())))
            },
        )
        .map(|(record, _)| record)
    }

    fn cancel(
        &self,
        owner: &str,
        id: &str,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_reason("cancel reason", reason)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Cancelled,
            "cancel",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Cancelled, "cancel")?;
                after.status = GoalStatus::Cancelled;
                after.finished_at = Some(effective_at);
                if let Some(reason) = reason {
                    after.cancel_reason = Some(reason.to_string());
                }
                Ok((None, reason.map(str::to_string)))
            },
        )
        .map(|(record, _)| record)
    }
}

fn ensure_transition(
    record: &GoalRecord,
    target: GoalStatus,
    operation: &'static str,
) -> Result<()> {
    let allowed = record.status == target
        || matches!(
            (record.status, target),
            (GoalStatus::Queued, GoalStatus::InProgress)
                | (GoalStatus::Queued, GoalStatus::Cancelled)
                | (GoalStatus::InProgress, GoalStatus::Completed)
                | (GoalStatus::InProgress, GoalStatus::Failed)
                | (GoalStatus::InProgress, GoalStatus::Paused)
                | (GoalStatus::InProgress, GoalStatus::Cancelled)
                | (GoalStatus::Paused, GoalStatus::InProgress)
                | (GoalStatus::Paused, GoalStatus::Cancelled)
        );
    if allowed {
        Ok(())
    } else {
        Err(GoalStoreError::InvalidStatus {
            id: record.id.clone(),
            operation,
            status: record.status,
        })
    }
}

fn get_in_connection(connection: &Connection, owner: &str, id: &str) -> Result<Option<GoalRecord>> {
    let sql = format!("SELECT {GOAL_COLUMNS} FROM goals WHERE owner = ?1 AND id = ?2");
    connection
        .query_row(&sql, params![owner, id], row_to_record)
        .optional()
        .map_err(GoalStoreError::from)
}

fn get_in_transaction(
    transaction: &Transaction<'_>,
    owner: &str,
    id: &str,
) -> Result<Option<GoalRecord>> {
    let sql = format!("SELECT {GOAL_COLUMNS} FROM goals WHERE owner = ?1 AND id = ?2");
    transaction
        .query_row(&sql, params![owner, id], row_to_record)
        .optional()
        .map_err(GoalStoreError::from)
}

fn update_snapshot(
    transaction: &Transaction<'_>,
    before: &GoalRecord,
    after: &GoalRecord,
) -> Result<()> {
    let changed = transaction.execute(
        "UPDATE goals SET status = ?1, started_at_ms = ?2, updated_at_ms = ?3, \
         finished_at_ms = ?4, revision = ?5, completion_summary = ?6, failure_reason = ?7, \
         pause_reason = ?8, cancel_reason = ?9 \
         WHERE owner = ?10 AND id = ?11 AND revision = ?12",
        params![
            after.status.as_str(),
            after.started_at.map(|value| value.timestamp_millis()),
            after.updated_at.timestamp_millis(),
            after.finished_at.map(|value| value.timestamp_millis()),
            counter_to_i64(after.revision, "revision")?,
            after.completion_summary,
            after.failure_reason,
            after.pause_reason,
            after.cancel_reason,
            after.owner,
            after.id,
            counter_to_i64(before.revision, "revision")?,
        ],
    )?;
    if changed != 1 {
        return Err(GoalStoreError::CorruptData(
            "goal snapshot changed unexpectedly inside write transaction".into(),
        ));
    }
    Ok(())
}

fn insert_event(
    transaction: &Transaction<'_>,
    record: &GoalRecord,
    kind: GoalEventKind,
    from_status: Option<GoalStatus>,
    occurred_at: DateTime<Utc>,
    stage: Option<&str>,
    detail: Option<&str>,
) -> Result<GoalEvent> {
    let event_id = Uuid::now_v7().to_string();
    transaction.execute(
        "INSERT INTO goal_events (event_id, owner, goal_id, revision, occurred_at_ms, kind, \
         from_status, to_status, stage, detail) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            event_id,
            record.owner,
            record.id,
            counter_to_i64(record.revision, "revision")?,
            occurred_at.timestamp_millis(),
            kind.as_str(),
            from_status.map(GoalStatus::as_str),
            record.status.as_str(),
            stage,
            detail,
        ],
    )?;
    let sequence = u64::try_from(transaction.last_insert_rowid())
        .map_err(|_| GoalStoreError::CorruptData("invalid goal event sequence".into()))?;
    Ok(GoalEvent {
        sequence,
        event_id,
        owner: record.owner.clone(),
        goal_id: record.id.clone(),
        revision: record.revision,
        occurred_at,
        kind,
        from_status,
        to_status: record.status,
        stage: stage.map(str::to_string),
        detail: detail.map(str::to_string),
    })
}

fn row_to_record(row: &Row<'_>) -> rusqlite::Result<GoalRecord> {
    let acceptance_json: String = row.get(7)?;
    let acceptance_criteria: Vec<AcceptanceCriterion> = serde_json::from_str(&acceptance_json)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(7, Type::Text, Box::new(error))
        })?;
    let priority_raw: i64 = row.get(5)?;
    let priority = u8::try_from(priority_raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, Type::Integer, Box::new(error))
    })?;
    Ok(GoalRecord {
        id: row.get(0)?,
        owner: row.get(1)?,
        title: row.get(2)?,
        description: row.get(3)?,
        status: enum_column(row, 4)?,
        priority,
        parent_goal_id: row.get(6)?,
        acceptance_criteria,
        created_at: timestamp_column(row, 8)?,
        started_at: optional_timestamp_column(row, 9)?,
        updated_at: timestamp_column(row, 10)?,
        finished_at: optional_timestamp_column(row, 11)?,
        revision: counter_column(row, 12)?,
        completion_summary: row.get(13)?,
        failure_reason: row.get(14)?,
        pause_reason: row.get(15)?,
        cancel_reason: row.get(16)?,
    })
}

fn row_to_event(row: &Row<'_>) -> rusqlite::Result<GoalEvent> {
    Ok(GoalEvent {
        sequence: counter_column(row, 0)?,
        event_id: row.get(1)?,
        owner: row.get(2)?,
        goal_id: row.get(3)?,
        revision: counter_column(row, 4)?,
        occurred_at: timestamp_column(row, 5)?,
        kind: enum_column(row, 6)?,
        from_status: optional_enum_column(row, 7)?,
        to_status: enum_column(row, 8)?,
        stage: row.get(9)?,
        detail: row.get(10)?,
    })
}

fn enum_column<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<T>
where
    T: FromStr<Err = GoalStoreError>,
{
    let value: String = row.get(index)?;
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}

fn optional_enum_column<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<T>>
where
    T: FromStr<Err = GoalStoreError>,
{
    let value: Option<String> = row.get(index)?;
    value
        .map(|value| {
            value.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
            })
        })
        .transpose()
}

fn counter_column(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Integer, Box::new(error))
    })
}

fn timestamp_column(row: &Row<'_>, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    timestamp_from_millis(index, row.get(index)?)
}

fn optional_timestamp_column(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| timestamp_from_millis(index, value))
        .transpose()
}

fn timestamp_from_millis(index: usize, value: i64) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Integer,
            Box::new(GoalStoreError::CorruptData(format!(
                "timestamp {value} is outside the supported range"
            ))),
        )
    })
}

fn normalize_timestamp(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value.timestamp_millis()).ok_or_else(|| {
        GoalStoreError::InvalidInput("timestamp is outside the supported range".into())
    })
}

fn counter_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| GoalStoreError::CorruptData(format!("{field} is outside SQLite range")))
}

fn map_create_error(error: rusqlite::Error, id: &str) -> GoalStoreError {
    match error {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation =>
        {
            GoalStoreError::AlreadyExists { id: id.to_string() }
        }
        other => GoalStoreError::Sqlite(other),
    }
}

fn configure_connection(connection: &Connection) -> Result<()> {
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn user_version(connection: &Connection) -> Result<u32> {
    let value: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    u32::try_from(value)
        .map_err(|_| GoalStoreError::CorruptData("invalid goal schema version".into()))
}

fn require_empty_schema(connection: &Connection) -> Result<()> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if count == 0 {
        Ok(())
    } else {
        Err(GoalStoreError::CorruptData(
            "unversioned goal database contains schema objects".into(),
        ))
    }
}

fn validate_schema(connection: &Connection) -> Result<()> {
    let found = user_version(connection)?;
    if found != SCHEMA_VERSION {
        return Err(GoalStoreError::CorruptData(format!(
            "expected schema {SCHEMA_VERSION}, found schema {found}"
        )));
    }
    for (kind, name) in [
        ("table", "goals"),
        ("table", "goal_events"),
        ("trigger", "goal_events_immutable_update"),
        ("trigger", "goal_events_immutable_delete"),
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = ?1 AND name = ?2)",
            params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(GoalStoreError::CorruptData(format!(
                "goal schema is missing {kind} `{name}`"
            )));
        }
    }
    Ok(())
}

fn open_database(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Ok(Connection::open_with_flags(path, flags)?)
}

#[cfg(unix)]
fn prepare_database_path(path: &Path) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.try_exists()? {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let parent_metadata = std::fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(GoalStoreError::InvalidInput(
            "goal database parent must be a real directory".into(),
        ));
    }
    if parent_metadata.permissions().mode() & 0o022 != 0 {
        return Err(GoalStoreError::InvalidInput(
            "goal database parent must not be group- or world-writable".into(),
        ));
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(GoalStoreError::InvalidInput(
                "goal database path must not be a symlink".into(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o7777 != 0o600 {
        return Err(GoalStoreError::InvalidInput(
            "goal database must be a private regular file".into(),
        ));
    }
    Ok(())
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
