use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::types::{Type, Value};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension as _, Row, Transaction,
    TransactionBehavior, params, params_from_iter,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    AcceptanceCriterion, AcceptanceVerification, GoalContinuityPolicy, GoalContinuitySignal,
    GoalContinuitySignalResult, GoalContinuityState, GoalContinuityStepStatus, GoalEvent,
    GoalEventKind, GoalPursuitCheckpoint, GoalQuery, GoalQuotaEvent, GoalRecord,
    GoalRecoveryCursor, GoalRecoveryFilter, GoalRecoveryPage, GoalStatus, GoalStore,
    GoalStoreError, GoalVerificationArtifact, NewGoal, PursuitCheckpointStatus, Result,
    TakeoverApproval, TakeoverApprovalRequest, TakeoverApprovalStatus, TakeoverBoundTarget,
    TakeoverDecision, TakeoverFinish, TakeoverRunStatus, TakeoverSandbox,
    continuity::{ready_approval_target, state_for_event, with_ready_signal, with_step_status},
    model::{
        validate_detail, validate_goal_id, validate_lease_seconds, validate_optional_reason,
        validate_owner, validate_stage, validate_worker,
    },
};

pub const SCHEMA_VERSION: u32 = 8;
const RECORD_SCHEMA: u32 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const MIGRATION_0001: &str = include_str!("../migrations/0001_goals.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_claim_lease.sql");
const MIGRATION_0003: &str = include_str!("../migrations/0003_verification_artifacts.sql");
const MIGRATION_0004: &str = include_str!("../migrations/0004_pursuit_checkpoint.sql");
const MIGRATION_0005: &str = include_str!("../migrations/0005_recovery_indexes.sql");
const MIGRATION_0006: &str = include_str!("../migrations/0006_goal_continuity.sql");
const MIGRATION_0007: &str = include_str!("../migrations/0007_takeover_approval.sql");
const MIGRATION_0008: &str = include_str!("../migrations/0008_review_handback.sql");
const MAX_VERIFICATION_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_PLAN_SNAPSHOT_BYTES: usize = 1024 * 1024;
const VERIFICATION_ARTIFACT_PAGE: i64 = 100;

const GOAL_COLUMNS: &str = "\
    id, owner, title, description, status, priority, parent_goal_id, acceptance_json, \
    created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision, \
    completion_summary, failure_reason, pause_reason, cancel_reason, \
    claimed_by, claim_expires_at_ms, claim_generation, continuity_policy_json, \
    continuity_state_json";
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

    /// List one bounded page of resident-recovery candidates using immutable
    /// `(priority, created_at, id)` ordering.
    pub fn list_recovery_page(
        &self,
        owner: &str,
        filter: &GoalRecoveryFilter,
        after: Option<&GoalRecoveryCursor>,
        limit: usize,
    ) -> Result<GoalRecoveryPage> {
        validate_owner(owner)?;
        if !(1..=1_000).contains(&limit) {
            return Err(GoalStoreError::InvalidInput(
                "goal recovery page limit must be between 1 and 1000".into(),
            ));
        }
        if let Some(after) = after {
            after.validate()?;
        }
        let connection = self.connection()?;
        let mut sql = format!(
            "SELECT {GOAL_COLUMNS} FROM goals INDEXED BY goals_owner_queue_idx \
             WHERE owner = ? AND status = 'in_progress'"
        );
        let mut values = vec![Value::Text(owner.to_string())];
        match filter {
            GoalRecoveryFilter::ActiveWorker { worker_id, .. } => validate_worker(worker_id)?,
            GoalRecoveryFilter::Available { .. } => {}
        }
        if let Some(after) = after {
            sql.push_str(
                " AND (priority > ? OR (priority = ? AND created_at_ms > ?) OR \
                 (priority = ? AND created_at_ms = ? AND id > ?))",
            );
            let priority = Value::Integer(i64::from(after.priority));
            let created_at = Value::Integer(after.created_at.timestamp_millis());
            values.extend([
                priority.clone(),
                priority.clone(),
                created_at.clone(),
                priority,
                created_at,
                Value::Text(after.id.clone()),
            ]);
        }
        sql.push_str(" ORDER BY priority ASC, created_at_ms ASC, id ASC LIMIT ?");
        values.push(Value::Integer(i64::try_from(limit).map_err(|_| {
            GoalStoreError::InvalidInput("goal recovery page limit is outside range".into())
        })?));
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), row_to_record)?;
        let examined = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(GoalStoreError::from)?;
        let next = (examined.len() == limit)
            .then(|| examined.last().map(GoalRecoveryCursor::from))
            .flatten();
        let candidates = examined
            .into_iter()
            .filter(|goal| match filter {
                GoalRecoveryFilter::ActiveWorker { worker_id, at } => {
                    goal.claimed_by.as_deref() == Some(worker_id.as_str()) && goal.lease_active(*at)
                }
                GoalRecoveryFilter::Available { at } => !goal.lease_active(*at),
            })
            .collect();
        Ok(GoalRecoveryPage { candidates, next })
    }

    /// Claim queued work only when the same write transaction observes no
    /// resident-recovery candidate. Temporarily cooling goals may be excluded;
    /// the exclusion set is deliberately bounded by the resident supervisor.
    pub fn claim_next_if_no_recovery(
        &self,
        owner: &str,
        worker_id: &str,
        lease_seconds: u64,
        excluded_goal_ids: &[String],
        at: DateTime<Utc>,
    ) -> Result<Option<GoalRecord>> {
        validate_owner(owner)?;
        validate_worker(worker_id)?;
        validate_lease_seconds(lease_seconds)?;
        if excluded_goal_ids.len() > 256 {
            return Err(GoalStoreError::InvalidInput(
                "goal recovery exclusions must contain at most 256 ids".into(),
            ));
        }
        for id in excluded_goal_ids {
            validate_goal_id(id)?;
        }
        let occurred_at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if recovery_candidate_exists(
            &transaction,
            owner,
            worker_id,
            occurred_at,
            excluded_goal_ids,
        )? {
            return Ok(None);
        }
        let sql = format!(
            "SELECT {GOAL_COLUMNS} FROM goals WHERE owner = ?1 AND status = 'queued' \
             ORDER BY priority ASC, created_at_ms ASC, id ASC LIMIT 1"
        );
        let Some(before) = transaction
            .query_row(&sql, [owner], row_to_record)
            .optional()?
        else {
            return Ok(None);
        };
        let (after, _) = mutate_in_transaction(
            &transaction,
            &before,
            GoalEventKind::Claimed,
            "claim",
            occurred_at,
            apply_claim(worker_id, lease_seconds),
        )?;
        transaction.commit()?;
        Ok(Some(after))
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
        let mut found = user_version(&transaction)?;
        if found == 0 {
            require_empty_schema(&transaction)?;
            transaction.execute_batch(MIGRATION_0001)?;
            found = 1;
        }
        if found == 1 {
            transaction.execute_batch(MIGRATION_0002)?;
            found = 2;
        }
        if found == 2 {
            transaction.execute_batch(MIGRATION_0003)?;
            found = 3;
        }
        if found == 3 {
            transaction.execute_batch(MIGRATION_0004)?;
            found = 4;
        }
        if found == 4 {
            transaction.execute_batch(MIGRATION_0005)?;
            found = 5;
        }
        if found == 5 {
            transaction.execute_batch(MIGRATION_0006)?;
            found = 6;
        }
        if found == 6 {
            transaction.execute_batch(MIGRATION_0007)?;
            found = 7;
        }
        if found == 7 {
            transaction.execute_batch(MIGRATION_0008)?;
            found = 8;
        }
        if found != user_version(&transaction)? {
            transaction.pragma_update(None, "user_version", found)?;
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
        let (after, event) = mutate_in_transaction(
            &transaction,
            &before,
            kind,
            operation,
            occurred_at,
            mutation,
        )?;
        transaction.commit()?;
        Ok((after, event))
    }
}

fn recovery_candidate_exists(
    transaction: &Transaction<'_>,
    owner: &str,
    worker_id: &str,
    at: DateTime<Utc>,
    excluded_goal_ids: &[String],
) -> Result<bool> {
    let exclusions = if excluded_goal_ids.is_empty() {
        String::new()
    } else {
        format!(
            " AND id NOT IN ({})",
            vec!["?"; excluded_goal_ids.len()].join(",")
        )
    };
    let checks = [
        (
            "goals_owner_worker_lease_idx",
            "claimed_by = ? AND claim_expires_at_ms > ?",
            Some(worker_id),
        ),
        ("goals_owner_worker_lease_idx", "claimed_by IS NULL", None),
        ("goals_owner_lease_idx", "claim_expires_at_ms <= ?", None),
    ];
    for (index, predicate, worker) in checks {
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM goals INDEXED BY {index} \
             WHERE owner = ? AND status = 'in_progress' AND {predicate}{exclusions})"
        );
        let mut values = vec![Value::Text(owner.to_string())];
        if let Some(worker) = worker {
            values.push(Value::Text(worker.to_string()));
        }
        if predicate.contains("claim_expires_at_ms") {
            values.push(Value::Integer(at.timestamp_millis()));
        }
        values.extend(excluded_goal_ids.iter().cloned().map(Value::Text));
        let exists: bool =
            transaction.query_row(&sql, params_from_iter(values), |row| row.get(0))?;
        if exists {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mutate_in_transaction<F>(
    transaction: &Transaction<'_>,
    before: &GoalRecord,
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
    let effective_at = std::cmp::max(occurred_at, before.updated_at);
    let mut after = before.clone();
    let (stage, detail) =
        mutation(before, &mut after, effective_at).map_err(|error| match error {
            GoalStoreError::InvalidStatus { .. } => GoalStoreError::InvalidStatus {
                id: before.id.clone(),
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
    update_snapshot(transaction, before, &after)?;
    let event = insert_event(
        transaction,
        &after,
        kind,
        Some(before.status),
        effective_at,
        stage.as_deref(),
        detail.as_deref(),
    )?;
    Ok((after, event))
}

/// Event `stage` and `detail` annotations produced by a mutation closure.
type EventAnnotations = (Option<String>, Option<String>);

/// Shared claim mutation used by both `claim` and `claim_next`.
fn apply_claim<'a>(
    worker_id: &'a str,
    lease_seconds: u64,
) -> impl FnOnce(&GoalRecord, &mut GoalRecord, DateTime<Utc>) -> Result<EventAnnotations> + 'a {
    move |before, after, effective_at| {
        match before.status {
            GoalStatus::Queued => {}
            GoalStatus::InProgress if before.claimed_by.is_none() => {}
            GoalStatus::InProgress if before.lease_active(effective_at) => {
                return Err(GoalStoreError::LeaseHeld {
                    id: before.id.clone(),
                    held_by: before
                        .claimed_by
                        .clone()
                        .unwrap_or_else(|| "unknown".into()),
                });
            }
            _ => {
                // An expired tenure keeps its holder identity and must use
                // reclaim. Only genuinely unleased in_progress work (manual
                // start or resume) may establish a fresh claim here.
                return Err(GoalStoreError::InvalidStatus {
                    id: before.id.clone(),
                    operation: "claim",
                    status: before.status,
                });
            }
        }
        grant_lease(after, worker_id, lease_seconds, effective_at)?;
        Ok((None, Some(worker_id.to_string())))
    }
}

fn grant_lease(
    after: &mut GoalRecord,
    worker_id: &str,
    lease_seconds: u64,
    effective_at: DateTime<Utc>,
) -> Result<()> {
    after.status = GoalStatus::InProgress;
    after.claimed_by = Some(worker_id.to_string());
    after.claim_expires_at = Some(lease_expiry(effective_at, lease_seconds)?);
    after.claim_generation = after
        .claim_generation
        .checked_add(1)
        .ok_or_else(|| GoalStoreError::CorruptData("claim generation overflow".into()))?;
    if after.started_at.is_none() {
        after.started_at = Some(effective_at);
    }
    Ok(())
}

/// Fence for every non-claim write path: while an active lease is held, only
/// the holder may mutate the goal. A stale worker whose lease was reclaimed
/// (its `claim_generation` superseded) no longer matches `claimed_by` and is
/// rejected, as is any anonymous caller.
fn ensure_lease_holder(
    record: &GoalRecord,
    worker_id: Option<&str>,
    at: DateTime<Utc>,
) -> Result<()> {
    if record.lease_active(at) {
        let holder = record.claimed_by.as_deref().unwrap_or("unknown");
        if worker_id != Some(holder) {
            return Err(GoalStoreError::LeaseHeld {
                id: record.id.clone(),
                held_by: holder.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_optional_worker(worker_id: Option<&str>) -> Result<()> {
    if let Some(worker_id) = worker_id {
        validate_worker(worker_id)?;
    }
    Ok(())
}

/// Release the lease without touching `claim_generation` (the tenure history
/// stays monotonic and auditable).
fn clear_lease(after: &mut GoalRecord) {
    after.claimed_by = None;
    after.claim_expires_at = None;
}

fn lease_expiry(from: DateTime<Utc>, lease_seconds: u64) -> Result<DateTime<Utc>> {
    let seconds = i64::try_from(lease_seconds)
        .map_err(|_| GoalStoreError::InvalidInput("lease duration overflow".into()))?;
    from.checked_add_signed(chrono::TimeDelta::seconds(seconds))
        .ok_or_else(|| {
            GoalStoreError::InvalidInput("lease expiry is outside the supported range".into())
        })
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
            continuity_policy: goal.continuity_policy,
            continuity_state: None,
            created_at: goal.created_at,
            started_at: None,
            updated_at: goal.created_at,
            finished_at: None,
            revision: 0,
            completion_summary: None,
            failure_reason: None,
            pause_reason: None,
            cancel_reason: None,
            claimed_by: None,
            claim_expires_at: None,
            claim_generation: 0,
        };
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let acceptance_json = serde_json::to_string(&record.acceptance_criteria)?;
        let continuity_policy_json = record
            .continuity_policy
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let inserted = transaction.execute(
            "INSERT INTO goals (owner, id, record_schema, title, description, status, priority, \
             parent_goal_id, acceptance_json, created_at_ms, started_at_ms, updated_at_ms, \
             finished_at_ms, revision, completion_summary, failure_reason, pause_reason, \
             cancel_reason, continuity_policy_json, continuity_state_json) VALUES \
             (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?10, NULL, 0, NULL, NULL, \
              NULL, NULL, ?11, NULL)",
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
                continuity_policy_json,
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

    fn claim(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_worker(worker_id)?;
        validate_lease_seconds(lease_seconds)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Claimed,
            "claim",
            at,
            apply_claim(worker_id, lease_seconds),
        )
        .map(|(record, _)| record)
    }

    fn claim_next(
        &self,
        owner: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<Option<GoalRecord>> {
        validate_owner(owner)?;
        validate_worker(worker_id)?;
        validate_lease_seconds(lease_seconds)?;
        let occurred_at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sql = format!(
            "SELECT {GOAL_COLUMNS} FROM goals WHERE owner = ?1 AND status = 'queued' \
             ORDER BY priority ASC, created_at_ms ASC, id ASC LIMIT 1"
        );
        let Some(before) = transaction
            .query_row(&sql, [owner], row_to_record)
            .optional()?
        else {
            return Ok(None);
        };
        let (after, _event) = mutate_in_transaction(
            &transaction,
            &before,
            GoalEventKind::Claimed,
            "claim",
            occurred_at,
            apply_claim(worker_id, lease_seconds),
        )?;
        transaction.commit()?;
        Ok(Some(after))
    }

    fn renew_lease(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_worker(worker_id)?;
        validate_lease_seconds(lease_seconds)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::LeaseRenewed,
            "renew the lease on",
            at,
            |before, after, effective_at| {
                if before.status != GoalStatus::InProgress {
                    return Err(GoalStoreError::InvalidStatus {
                        id: before.id.clone(),
                        operation: "renew the lease on",
                        status: before.status,
                    });
                }
                let Some(holder) = before.claimed_by.as_deref() else {
                    return Err(GoalStoreError::InvalidStatus {
                        id: before.id.clone(),
                        operation: "renew the lease on",
                        status: before.status,
                    });
                };
                if holder != worker_id {
                    return Err(GoalStoreError::LeaseHeld {
                        id: before.id.clone(),
                        held_by: holder.to_string(),
                    });
                }
                if !before.lease_active(effective_at) {
                    return Err(GoalStoreError::LeaseExpired {
                        id: before.id.clone(),
                    });
                }
                after.claim_expires_at = Some(lease_expiry(effective_at, lease_seconds)?);
                Ok((None, Some(worker_id.to_string())))
            },
        )
        .map(|(record, _)| record)
    }

    fn reclaim(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_worker(worker_id)?;
        validate_lease_seconds(lease_seconds)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Reclaimed,
            "reclaim",
            at,
            |before, after, effective_at| {
                if before.status != GoalStatus::InProgress || before.claimed_by.is_none() {
                    return Err(GoalStoreError::InvalidStatus {
                        id: before.id.clone(),
                        operation: "reclaim",
                        status: before.status,
                    });
                }
                if before.lease_active(effective_at) {
                    return Err(GoalStoreError::LeaseHeld {
                        id: before.id.clone(),
                        held_by: before
                            .claimed_by
                            .clone()
                            .unwrap_or_else(|| "unknown".into()),
                    });
                }
                grant_lease(after, worker_id, lease_seconds, effective_at)?;
                Ok((None, Some(worker_id.to_string())))
            },
        )
        .map(|(record, _)| record)
    }

    fn satisfy_criterion(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        index: usize,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_worker(worker_id)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::CriterionSatisfied,
            "satisfy a criterion on",
            at,
            |before, after, effective_at| {
                if before.status != GoalStatus::InProgress {
                    return Err(GoalStoreError::InvalidStatus {
                        id: before.id.clone(),
                        operation: "satisfy a criterion on",
                        status: before.status,
                    });
                }
                ensure_lease_holder(before, worker_id, effective_at)?;
                let total = before.acceptance_criteria.len();
                let Some(criterion) = after.acceptance_criteria.get_mut(index) else {
                    return Err(GoalStoreError::InvalidInput(format!(
                        "criterion index {index} is out of range for {total} criteria"
                    )));
                };
                if criterion.satisfied_at.is_some() {
                    return Err(GoalStoreError::InvalidInput(format!(
                        "criterion {index} is already satisfied"
                    )));
                }
                criterion.satisfied_at = Some(effective_at);
                let stage = criterion.kind.clone();
                let detail = criterion.target.clone();
                Ok((Some(stage), Some(detail)))
            },
        )
        .map(|(record, _)| record)
    }

    fn record_verification(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        verification: &AcceptanceVerification,
        at: DateTime<Utc>,
    ) -> Result<GoalVerificationArtifact> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        validate_optional_worker(worker_id)?;
        if verification.goal_id != id {
            return Err(GoalStoreError::InvalidInput(
                "verification goal id does not match the persisted goal".into(),
            ));
        }
        let payload_json = serde_json::to_string(verification)?;
        if payload_json.len() > MAX_VERIFICATION_PAYLOAD_BYTES {
            return Err(GoalStoreError::InvalidInput(format!(
                "verification artifact exceeds {MAX_VERIFICATION_PAYLOAD_BYTES} bytes"
            )));
        }
        let payload_sha256 = hex_digest(payload_json.as_bytes());
        let at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let goal = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        let recorded_at = at;
        if goal.status != GoalStatus::InProgress {
            return Err(GoalStoreError::InvalidStatus {
                id: id.to_string(),
                operation: "record verification for",
                status: goal.status,
            });
        }
        ensure_lease_holder(&goal, worker_id, recorded_at)?;
        let artifact = GoalVerificationArtifact {
            sequence: 0,
            verification_id: format!("verification-{}", Uuid::now_v7()),
            owner: owner.to_string(),
            goal_id: id.to_string(),
            recorded_at,
            worker_id: worker_id.map(ToOwned::to_owned),
            verification: verification.clone(),
            payload_sha256,
        };
        transaction.execute(
            "INSERT INTO goal_verifications (verification_id, owner, goal_id, recorded_at_ms, \
             worker_id, payload_json, payload_sha256) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                artifact.verification_id,
                artifact.owner,
                artifact.goal_id,
                artifact.recorded_at.timestamp_millis(),
                artifact.worker_id,
                payload_json,
                artifact.payload_sha256,
            ],
        )?;
        let sequence = u64::try_from(transaction.last_insert_rowid()).map_err(|_| {
            GoalStoreError::CorruptData("verification sequence is outside supported range".into())
        })?;
        transaction.commit()?;
        Ok(GoalVerificationArtifact {
            sequence,
            ..artifact
        })
    }

    fn record_pursuit_verification(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        verification: &AcceptanceVerification,
        checkpoint: &GoalPursuitCheckpoint,
        detail: &str,
        at: DateTime<Utc>,
    ) -> Result<(GoalVerificationArtifact, GoalPursuitCheckpoint, GoalEvent)> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        validate_worker(worker_id)?;
        validate_detail(detail)?;
        checkpoint.validate()?;
        if checkpoint.owner != owner
            || checkpoint.goal_id != id
            || checkpoint.worker_id != worker_id
        {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint identity does not match the write scope".into(),
            ));
        }
        if verification.goal_id != id {
            return Err(GoalStoreError::InvalidInput(
                "verification goal id does not match the persisted goal".into(),
            ));
        }
        let payload_json = serde_json::to_string(verification)?;
        if payload_json.len() > MAX_VERIFICATION_PAYLOAD_BYTES {
            return Err(GoalStoreError::InvalidInput(format!(
                "verification artifact exceeds {MAX_VERIFICATION_PAYLOAD_BYTES} bytes"
            )));
        }
        let occurred_at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        if before.status != GoalStatus::InProgress {
            return Err(GoalStoreError::InvalidStatus {
                id: id.to_string(),
                operation: "record pursuit verification for",
                status: before.status,
            });
        }
        ensure_lease_holder(&before, Some(worker_id), occurred_at)?;
        if !before.lease_active(occurred_at) {
            return Err(GoalStoreError::LeaseExpired { id: id.to_string() });
        }
        if checkpoint.goal_revision != before.revision
            || checkpoint.claim_generation != before.claim_generation
        {
            return Err(GoalStoreError::CheckpointConflict { id: id.to_string() });
        }
        let payload_sha256 = hex_digest(payload_json.as_bytes());
        let mut artifact = GoalVerificationArtifact {
            sequence: 0,
            verification_id: format!("verification-{}", Uuid::now_v7()),
            owner: owner.to_string(),
            goal_id: id.to_string(),
            recorded_at: occurred_at,
            worker_id: Some(worker_id.to_string()),
            verification: verification.clone(),
            payload_sha256,
        };
        transaction.execute(
            "INSERT INTO goal_verifications (verification_id, owner, goal_id, recorded_at_ms, \
             worker_id, payload_json, payload_sha256) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                artifact.verification_id,
                artifact.owner,
                artifact.goal_id,
                artifact.recorded_at.timestamp_millis(),
                artifact.worker_id,
                payload_json,
                artifact.payload_sha256,
            ],
        )?;
        artifact.sequence = u64::try_from(transaction.last_insert_rowid()).map_err(|_| {
            GoalStoreError::CorruptData("verification sequence is outside supported range".into())
        })?;
        let mut current = before;
        for result in &verification.results {
            if result.status == crate::CriterionStatus::Satisfied
                && current
                    .acceptance_criteria
                    .get(result.criterion_index)
                    .is_some_and(|criterion| criterion.satisfied_at.is_none())
            {
                let index = result.criterion_index;
                let total = current.acceptance_criteria.len();
                let (next, _) = mutate_in_transaction(
                    &transaction,
                    &current,
                    GoalEventKind::CriterionSatisfied,
                    "satisfy a criterion on",
                    occurred_at,
                    |_before, after, effective_at| {
                        let Some(criterion) = after.acceptance_criteria.get_mut(index) else {
                            return Err(GoalStoreError::InvalidInput(format!(
                                "criterion index {index} is out of range for {total} criteria"
                            )));
                        };
                        criterion.satisfied_at = Some(effective_at);
                        Ok((Some(criterion.kind.clone()), Some(criterion.target.clone())))
                    },
                )?;
                current = next;
            }
        }
        let mut checkpoint = checkpoint.clone();
        checkpoint.last_verification_id = Some(artifact.verification_id.clone());
        checkpoint.goal_revision = current.revision;
        let (checkpoint, event) = record_pursuit_checkpoint_in_transaction(
            &transaction,
            &current,
            &checkpoint,
            "acceptance.verify",
            detail,
            occurred_at,
        )?;
        transaction.commit()?;
        Ok((artifact, checkpoint, event))
    }

    fn verifications(&self, owner: &str, id: &str) -> Result<Vec<GoalVerificationArtifact>> {
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
            "SELECT sequence, verification_id, owner, goal_id, recorded_at_ms, worker_id, \
             payload_json, payload_sha256 FROM (SELECT sequence, verification_id, owner, goal_id, \
             recorded_at_ms, worker_id, payload_json, payload_sha256 FROM goal_verifications \
             WHERE owner = ?1 AND goal_id = ?2 ORDER BY sequence DESC LIMIT ?3) \
             ORDER BY sequence ASC",
        )?;
        let rows = statement.query_map(
            params![owner, id, VERIFICATION_ARTIFACT_PAGE],
            row_to_verification,
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
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

    fn pursuit_checkpoint(&self, owner: &str, id: &str) -> Result<Option<GoalPursuitCheckpoint>> {
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
        connection
            .query_row(
                "SELECT owner, goal_id, checkpoint_revision, claim_generation, updated_at_ms, \
                 payload_json, payload_sha256 FROM goal_pursuit_checkpoints \
                 WHERE owner = ?1 AND goal_id = ?2",
                params![owner, id],
                row_to_pursuit_checkpoint,
            )
            .optional()
            .map_err(GoalStoreError::from)
    }

    fn record_quota_handoff(
        &self,
        owner: &str,
        id: &str,
        event: &GoalQuotaEvent,
        at: DateTime<Utc>,
    ) -> Result<Option<crate::GoalContinuityState>> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        event.validate()?;
        let at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        let Some(state) = state_for_event(&before, event)? else {
            return Ok(None);
        };
        let persisted = state.clone();
        let (after, _) = mutate_in_transaction(
            &transaction,
            &before,
            GoalEventKind::Progress,
            "record quota handoff",
            at,
            move |_before, after, _effective_at| {
                after.continuity_state = Some(persisted);
                Ok((
                    Some("quota_handoff".into()),
                    Some(format!("quota event {}", event.event_id)),
                ))
            },
        )?;
        transaction.commit()?;
        Ok(after.continuity_state)
    }

    fn record_continuity_signal(
        &self,
        owner: &str,
        id: &str,
        signal: &GoalContinuitySignal,
        at: DateTime<Utc>,
    ) -> Result<GoalContinuitySignalResult> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        signal.validate()?;
        let at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        if before.status != GoalStatus::InProgress {
            return Err(GoalStoreError::InvalidStatus {
                id: id.to_string(),
                operation: "record continuity signal for",
                status: before.status,
            });
        }
        let state = before.continuity_state.as_ref().ok_or_else(|| {
            GoalStoreError::InvalidInput("goal has no visible continuity state".into())
        })?;
        let (next, persisted_signal, changed) = with_ready_signal(state, signal)?;
        if !changed {
            return Ok(GoalContinuitySignalResult {
                goal_id: id.to_string(),
                changed: false,
                signal: persisted_signal,
                state: next,
            });
        }
        let persisted = next.clone();
        let (after, _) = mutate_in_transaction(
            &transaction,
            &before,
            GoalEventKind::Progress,
            "record continuity signal for",
            at,
            move |_before, after, _effective_at| {
                after.continuity_state = Some(persisted);
                Ok((
                    Some("continuity_signal".into()),
                    Some("quota reset signal recorded".into()),
                ))
            },
        )?;
        transaction.commit()?;
        Ok(GoalContinuitySignalResult {
            goal_id: id.to_string(),
            changed: true,
            signal: persisted_signal,
            state: after.continuity_state.ok_or_else(|| {
                GoalStoreError::CorruptData("continuity signal state disappeared".into())
            })?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record_pursuit_checkpoint(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        checkpoint: &GoalPursuitCheckpoint,
        stage: &str,
        detail: &str,
        at: DateTime<Utc>,
    ) -> Result<(GoalPursuitCheckpoint, GoalEvent)> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        validate_worker(worker_id)?;
        validate_stage(stage)?;
        validate_detail(detail)?;
        checkpoint.validate()?;
        if checkpoint.owner != owner
            || checkpoint.goal_id != id
            || checkpoint.worker_id != worker_id
        {
            return Err(GoalStoreError::InvalidInput(
                "pursuit checkpoint identity does not match the write scope".into(),
            ));
        }
        let occurred_at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        let result = record_pursuit_checkpoint_in_transaction(
            &transaction,
            &before,
            checkpoint,
            stage,
            detail,
            occurred_at,
        )?;
        transaction.commit()?;
        Ok(result)
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
        worker_id: Option<&str>,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_worker(worker_id)?;
        validate_optional_reason("pause reason", reason)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Paused,
            "pause",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Paused, "pause")?;
                ensure_lease_holder(before, worker_id, effective_at)?;
                after.status = GoalStatus::Paused;
                // Pausing releases the lease: a paused goal is never leased.
                clear_lease(after);
                if let Some(reason) = reason {
                    after.pause_reason = Some(reason.to_string());
                }
                Ok((None, reason.map(str::to_string)))
            },
        )
        .map(|(record, _)| record)
    }

    fn resume(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_worker(worker_id)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Resumed,
            "resume",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::InProgress, "resume")?;
                ensure_lease_holder(before, worker_id, effective_at)?;
                after.status = GoalStatus::InProgress;
                // Pause already released the lease; clear any stale fields left
                // by older data so a resumed goal is unambiguously unleased.
                clear_lease(after);
                Ok((None, None))
            },
        )
        .map(|(record, _)| record)
    }

    fn done(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        summary: Option<&str>,
        waive_reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_owner(owner)?;
        validate_goal_id(id)?;
        validate_optional_worker(worker_id)?;
        validate_optional_reason("completion summary", summary)?;
        validate_optional_reason("waive reason", waive_reason)?;
        let occurred_at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = get_in_transaction(&transaction, owner, id)?
            .ok_or_else(|| GoalStoreError::NotFound { id: id.to_string() })?;
        ensure_transition(&before, GoalStatus::Completed, "complete")?;
        let effective_at = std::cmp::max(occurred_at, before.updated_at);
        ensure_lease_holder(&before, worker_id, effective_at)?;
        let unsatisfied: Vec<String> = before
            .acceptance_criteria
            .iter()
            .enumerate()
            .filter(|(_, criterion)| criterion.satisfied_at.is_none())
            .map(|(index, criterion)| format!("{index}:{}", criterion.kind))
            .collect();
        let mut base = before;
        if !unsatisfied.is_empty() {
            let Some(reason) = waive_reason else {
                return Err(GoalStoreError::CriteriaUnsatisfied {
                    id: id.to_string(),
                    remaining: unsatisfied.len(),
                });
            };
            let detail = format!("waived [{}]: {reason}", unsatisfied.join(", "));
            let (waived, _event) = mutate_in_transaction(
                &transaction,
                &base,
                GoalEventKind::CriteriaWaived,
                "waive acceptance criteria on",
                occurred_at,
                |_before, _after, _effective_at| Ok((Some("waive".into()), Some(detail))),
            )?;
            base = waived;
        }
        let (after, _event) = mutate_in_transaction(
            &transaction,
            &base,
            GoalEventKind::Completed,
            "complete",
            occurred_at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Completed, "complete")?;
                after.status = GoalStatus::Completed;
                after.finished_at = Some(effective_at);
                // Terminal states release the lease.
                clear_lease(after);
                if let Some(summary) = summary {
                    after.completion_summary = Some(summary.to_string());
                }
                Ok((None, summary.map(str::to_string)))
            },
        )?;
        transaction.commit()?;
        Ok(after)
    }

    fn fail(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        reason: &str,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_worker(worker_id)?;
        validate_optional_reason("failure reason", Some(reason))?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Failed,
            "fail",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Failed, "fail")?;
                ensure_lease_holder(before, worker_id, effective_at)?;
                after.status = GoalStatus::Failed;
                after.finished_at = Some(effective_at);
                // Terminal states release the lease.
                clear_lease(after);
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
        worker_id: Option<&str>,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord> {
        validate_optional_worker(worker_id)?;
        validate_optional_reason("cancel reason", reason)?;
        self.mutate(
            owner,
            id,
            GoalEventKind::Cancelled,
            "cancel",
            at,
            |before, after, effective_at| {
                ensure_transition(before, GoalStatus::Cancelled, "cancel")?;
                ensure_lease_holder(before, worker_id, effective_at)?;
                after.status = GoalStatus::Cancelled;
                after.finished_at = Some(effective_at);
                // Terminal states release the lease.
                clear_lease(after);
                if let Some(reason) = reason {
                    after.cancel_reason = Some(reason.to_string());
                }
                Ok((None, reason.map(str::to_string)))
            },
        )
        .map(|(record, _)| record)
    }

    fn queue_takeover_approval(
        &self,
        owner: &str,
        request: &TakeoverApprovalRequest,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval> {
        validate_owner(owner)?;
        request.validate()?;
        request.validate_live_workdir()?;
        if request.goal_revision > i64::MAX as u64 {
            return Err(GoalStoreError::CorruptData(
                "takeover goal revision is outside SQLite range".into(),
            ));
        }
        let plan_snapshot_json = serde_json::to_string(&request.plan_snapshot)?;
        if plan_snapshot_json.len() > MAX_PLAN_SNAPSHOT_BYTES {
            return Err(GoalStoreError::InvalidInput(
                "takeover plan snapshot exceeds the bounded size".into(),
            ));
        }
        let snapshot_digest = hex_digest(request.snapshot_payload()?.as_bytes());
        let at = normalize_timestamp(at)?;
        let timeout_secs = i64::try_from(request.timeout.as_secs())
            .map_err(|_| GoalStoreError::InvalidInput("takeover timeout is out of range".into()))?;
        let approval_id = format!("continuity-{}", Uuid::now_v7());
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let goal = get_in_transaction(&transaction, owner, &request.goal_id)?.ok_or_else(|| {
            GoalStoreError::NotFound {
                id: request.goal_id.clone(),
            }
        })?;
        validate_takeover_request(request, &goal)?;
        validate_upstream_review_evidence(&transaction, owner, request)?;
        transaction.execute(
            "INSERT INTO goal_takeover_approvals (approval_id, owner, goal_id, step_id, \
             step_kind, quota_event_id, snapshot_digest, target_profile, target_provider, \
             target_protocol, target_harness, target_model, workdir, sandbox, timeout_seconds, \
             goal_revision, plan_snapshot_json, upstream_approval_id, upstream_run_id, \
             upstream_run_status, status, created_at_ms, updated_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, \
             ?17, ?18, ?19, ?20, 'pending', ?21, ?21) \
             ON CONFLICT(owner, snapshot_digest) DO NOTHING",
            params![
                approval_id,
                owner,
                request.goal_id,
                request.step_id,
                request.step_kind,
                request.quota_event_id,
                snapshot_digest,
                request.target.profile,
                request.target.provider,
                request.target.protocol,
                request.target.harness,
                request.target.model,
                request.workdir.to_str().ok_or_else(|| {
                    GoalStoreError::InvalidInput("takeover workdir must be valid UTF-8".into())
                })?,
                request.sandbox.as_str(),
                timeout_secs,
                counter_to_i64(request.goal_revision, "takeover goal revision")?,
                plan_snapshot_json,
                request.upstream_approval_id,
                request.upstream_run_id,
                request.upstream_run_status.map(TakeoverRunStatus::as_str),
                at.timestamp_millis(),
            ],
        )?;
        let approval = select_takeover_approval(&transaction, owner, &snapshot_digest)?;
        transaction.commit()?;
        Ok(approval)
    }

    fn decide_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
        decision: TakeoverDecision,
        decided_by: &str,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval> {
        validate_owner(owner)?;
        validate_goal_id(approval_id)?;
        validate_worker(decided_by)?;
        validate_optional_reason("takeover decision reason", reason)?;
        let at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before =
            select_takeover_approval_by_id(&transaction, owner, approval_id)?.ok_or_else(|| {
                GoalStoreError::TakeoverApprovalNotFound {
                    id: approval_id.to_string(),
                }
            })?;
        if before.status != TakeoverApprovalStatus::Pending {
            return Err(GoalStoreError::TakeoverApprovalAlreadyDecided {
                id: approval_id.to_string(),
            });
        }
        let next_status = match decision {
            TakeoverDecision::Approve => TakeoverApprovalStatus::Approved,
            TakeoverDecision::Reject => TakeoverApprovalStatus::Rejected,
        };
        let changed = transaction.execute(
            "UPDATE goal_takeover_approvals SET status = ?1, decided_by = ?2, \
             decision_reason = ?3, decided_at_ms = ?4, updated_at_ms = ?4 \
             WHERE owner = ?5 AND approval_id = ?6 AND status = 'pending'",
            params![
                next_status.as_str(),
                decided_by,
                reason,
                at.timestamp_millis(),
                owner,
                approval_id,
            ],
        )?;
        if changed != 1 {
            return Err(GoalStoreError::TakeoverApprovalAlreadyDecided {
                id: approval_id.to_string(),
            });
        }
        let after =
            select_takeover_approval_by_id(&transaction, owner, approval_id)?.ok_or_else(|| {
                GoalStoreError::TakeoverApprovalNotFound {
                    id: approval_id.to_string(),
                }
            })?;
        transaction.commit()?;
        Ok(after)
    }

    fn consume_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval> {
        validate_owner(owner)?;
        validate_goal_id(approval_id)?;
        let occurred_at = normalize_timestamp(at)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let approval = select_takeover_approval_by_id(&transaction, owner, approval_id)?
            .ok_or_else(|| GoalStoreError::TakeoverApprovalNotFound {
                id: approval_id.to_string(),
            })?;
        if approval.status != TakeoverApprovalStatus::Approved {
            return Err(GoalStoreError::TakeoverApprovalNotExecutable {
                id: approval_id.to_string(),
                status: approval.status,
            });
        }
        let goal =
            get_in_transaction(&transaction, owner, &approval.goal_id)?.ok_or_else(|| {
                GoalStoreError::NotFound {
                    id: approval.goal_id.clone(),
                }
            })?;
        validate_takeover_boundary(&approval, &goal)?;
        validate_upstream_approval_record(&transaction, owner, &approval)?;
        let (after, _event) = mutate_in_transaction(
            &transaction,
            &goal,
            GoalEventKind::Progress,
            "consume takeover approval for",
            occurred_at,
            |before, after, _effective_at| {
                if before.status != GoalStatus::InProgress {
                    return Err(GoalStoreError::InvalidStatus {
                        id: before.id.clone(),
                        operation: "consume takeover approval for",
                        status: before.status,
                    });
                }
                let Some(state) = before.continuity_state.as_ref() else {
                    return Err(GoalStoreError::TakeoverBoundaryChanged {
                        id: approval_id.to_string(),
                    });
                };
                if ready_approval_target(state, &approval.step_id, &approval.step_kind).is_none() {
                    return Err(GoalStoreError::TakeoverBoundaryChanged {
                        id: approval_id.to_string(),
                    });
                }
                after.continuity_state = Some(with_step_status(
                    state,
                    &approval.step_id,
                    GoalContinuityStepStatus::InFlight,
                )?);
                Ok((
                    Some("takeover.consume".into()),
                    Some(format!("approval {approval_id} consumed")),
                ))
            },
        )?;
        let _ = after;
        let changed = transaction.execute(
            "UPDATE goal_takeover_approvals SET status = 'in_flight', updated_at_ms = ?1 \
             WHERE owner = ?2 AND approval_id = ?3 AND status = 'approved'",
            params![occurred_at.timestamp_millis(), owner, approval_id],
        )?;
        if changed != 1 {
            return Err(GoalStoreError::TakeoverApprovalNotExecutable {
                id: approval_id.to_string(),
                status: approval.status,
            });
        }
        let consumed = select_takeover_approval_by_id(&transaction, owner, approval_id)?
            .ok_or_else(|| GoalStoreError::TakeoverApprovalNotFound {
                id: approval_id.to_string(),
            })?;
        transaction.commit()?;
        Ok(consumed)
    }

    fn finish_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
        finish: &TakeoverFinish,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval> {
        validate_owner(owner)?;
        validate_goal_id(approval_id)?;
        finish.validate()?;
        let occurred_at = normalize_timestamp(at)?;
        let next_status = finish.terminal_approval_status();
        let next_step_status = finish.terminal_step_status();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let approval = select_takeover_approval_by_id(&transaction, owner, approval_id)?
            .ok_or_else(|| GoalStoreError::TakeoverApprovalNotFound {
                id: approval_id.to_string(),
            })?;
        if approval.status != TakeoverApprovalStatus::InFlight {
            return Err(GoalStoreError::TakeoverApprovalNotExecutable {
                id: approval_id.to_string(),
                status: approval.status,
            });
        }
        let goal =
            get_in_transaction(&transaction, owner, &approval.goal_id)?.ok_or_else(|| {
                GoalStoreError::NotFound {
                    id: approval.goal_id.clone(),
                }
            })?;
        let (_, _event) = mutate_in_transaction(
            &transaction,
            &goal,
            GoalEventKind::Progress,
            "finish takeover approval for",
            occurred_at,
            |before, after, _effective_at| {
                let Some(state) = before.continuity_state.as_ref() else {
                    return Err(GoalStoreError::TakeoverBoundaryChanged {
                        id: approval_id.to_string(),
                    });
                };
                if !state.handoff_plan.steps.iter().any(|step| {
                    step.id == approval.step_id && step.status == GoalContinuityStepStatus::InFlight
                }) {
                    return Err(GoalStoreError::TakeoverBoundaryChanged {
                        id: approval_id.to_string(),
                    });
                }
                after.continuity_state = Some(with_step_status(
                    state,
                    &approval.step_id,
                    next_step_status,
                )?);
                Ok((Some("takeover.finish".into()), Some(finish.detail.clone())))
            },
        )?;
        let blocker = if !finish.run_status.is_success() {
            Some(&finish.detail)
        } else {
            None
        };
        let changed = transaction.execute(
            "UPDATE goal_takeover_approvals SET status = ?1, run_id = ?2, run_status = ?3, \
             blocker_reason = ?4, updated_at_ms = ?5 \
             WHERE owner = ?6 AND approval_id = ?7 AND status = 'in_flight'",
            params![
                next_status.as_str(),
                finish.run_id,
                finish.run_status.as_str(),
                blocker,
                occurred_at.timestamp_millis(),
                owner,
                approval_id,
            ],
        )?;
        if changed != 1 {
            return Err(GoalStoreError::TakeoverApprovalNotExecutable {
                id: approval_id.to_string(),
                status: approval.status,
            });
        }
        let settled = select_takeover_approval_by_id(&transaction, owner, approval_id)?
            .ok_or_else(|| GoalStoreError::TakeoverApprovalNotFound {
                id: approval_id.to_string(),
            })?;
        transaction.commit()?;
        Ok(settled)
    }

    fn get_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
    ) -> Result<Option<TakeoverApproval>> {
        validate_owner(owner)?;
        validate_goal_id(approval_id)?;
        let connection = self.connection()?;
        select_takeover_approval_by_id(&connection, owner, approval_id)
    }

    fn list_takeover_approvals(
        &self,
        owner: &str,
        goal_id: Option<&str>,
    ) -> Result<Vec<TakeoverApproval>> {
        validate_owner(owner)?;
        if let Some(goal_id) = goal_id {
            validate_goal_id(goal_id)?;
        }
        let connection = self.connection()?;
        let sql = format!("SELECT {TAKEOVER_COLUMNS} FROM goal_takeover_approvals");
        let mut statement = if let Some(goal_id) = goal_id {
            let mut s = connection.prepare(&format!(
                "{sql} WHERE owner = ?1 AND goal_id = ?2 ORDER BY created_at_ms ASC, approval_id ASC"
            ))?;
            let rows = s.query_map(params![owner, goal_id], row_to_takeover_approval)?;
            return rows
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(GoalStoreError::from);
        } else {
            connection.prepare(&format!(
                "{sql} WHERE owner = ?1 ORDER BY created_at_ms ASC, approval_id ASC"
            ))?
        };
        let rows = statement.query_map(params![owner], row_to_takeover_approval)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(GoalStoreError::from)
    }
}

fn ensure_transition(
    record: &GoalRecord,
    target: GoalStatus,
    operation: &'static str,
) -> Result<()> {
    // Self-transitions are deliberately rejected: a second `start` on an
    // in_progress goal (double start) must fail, as must repeated terminal ops.
    let allowed = matches!(
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

fn record_pursuit_checkpoint_in_transaction(
    transaction: &Transaction<'_>,
    before: &GoalRecord,
    checkpoint: &GoalPursuitCheckpoint,
    stage: &str,
    detail: &str,
    occurred_at: DateTime<Utc>,
) -> Result<(GoalPursuitCheckpoint, GoalEvent)> {
    if before.status != GoalStatus::InProgress {
        return Err(GoalStoreError::InvalidStatus {
            id: before.id.clone(),
            operation: "record pursuit checkpoint for",
            status: before.status,
        });
    }
    ensure_lease_holder(before, Some(&checkpoint.worker_id), occurred_at)?;
    if !before.lease_active(occurred_at) {
        return Err(GoalStoreError::LeaseExpired {
            id: before.id.clone(),
        });
    }
    if checkpoint.goal_revision != before.revision
        || checkpoint.claim_generation != before.claim_generation
    {
        return Err(GoalStoreError::CheckpointConflict {
            id: before.id.clone(),
        });
    }
    let existing = transaction
        .query_row(
            "SELECT owner, goal_id, checkpoint_revision, claim_generation, updated_at_ms, \
             payload_json, payload_sha256 FROM goal_pursuit_checkpoints \
             WHERE owner = ?1 AND goal_id = ?2",
            params![before.owner, before.id],
            row_to_pursuit_checkpoint,
        )
        .optional()?;
    let next_revision = match existing {
        None if checkpoint.checkpoint_revision == 0 => 1,
        Some(existing) if existing.checkpoint_revision == checkpoint.checkpoint_revision => {
            existing.checkpoint_revision.checked_add(1).ok_or_else(|| {
                GoalStoreError::CorruptData("pursuit checkpoint revision overflow".into())
            })?
        }
        None | Some(_) => {
            return Err(GoalStoreError::CheckpointConflict {
                id: before.id.clone(),
            });
        }
    };
    let (event_kind, operation) = match checkpoint.status {
        PursuitCheckpointStatus::Running => {
            (GoalEventKind::Progress, "record pursuit checkpoint for")
        }
        PursuitCheckpointStatus::Paused => (GoalEventKind::Paused, "pause pursuit for"),
        PursuitCheckpointStatus::Achieved => (GoalEventKind::Completed, "complete pursuit for"),
    };
    let (after, event) = mutate_in_transaction(
        transaction,
        before,
        event_kind,
        operation,
        occurred_at,
        |before, after, effective_at| {
            match checkpoint.status {
                PursuitCheckpointStatus::Running => {}
                PursuitCheckpointStatus::Paused => {
                    ensure_transition(before, GoalStatus::Paused, "pause")?;
                    after.status = GoalStatus::Paused;
                    clear_lease(after);
                    after.pause_reason = Some(detail.to_string());
                }
                PursuitCheckpointStatus::Achieved => {
                    ensure_transition(before, GoalStatus::Completed, "complete")?;
                    let remaining = before
                        .acceptance_criteria
                        .iter()
                        .filter(|criterion| criterion.satisfied_at.is_none())
                        .count();
                    if remaining > 0 {
                        return Err(GoalStoreError::CriteriaUnsatisfied {
                            id: before.id.clone(),
                            remaining,
                        });
                    }
                    after.status = GoalStatus::Completed;
                    after.finished_at = Some(effective_at);
                    clear_lease(after);
                    after.completion_summary = Some(detail.to_string());
                }
            }
            Ok((Some(stage.to_string()), Some(detail.to_string())))
        },
    )?;
    let mut next = checkpoint.clone();
    next.checkpoint_revision = next_revision;
    next.goal_revision = after.revision;
    next.updated_at = event.occurred_at;
    let payload_json = serde_json::to_string(&next)?;
    let payload_sha256 = hex_digest(payload_json.as_bytes());
    transaction.execute(
        "INSERT INTO goal_pursuit_checkpoints (owner, goal_id, checkpoint_revision, \
         claim_generation, updated_at_ms, payload_json, payload_sha256) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(owner, goal_id) DO UPDATE SET \
         checkpoint_revision = excluded.checkpoint_revision, \
         claim_generation = excluded.claim_generation, updated_at_ms = excluded.updated_at_ms, \
         payload_json = excluded.payload_json, payload_sha256 = excluded.payload_sha256",
        params![
            before.owner,
            before.id,
            counter_to_i64(next.checkpoint_revision, "checkpoint revision")?,
            counter_to_i64(next.claim_generation, "claim generation")?,
            next.updated_at.timestamp_millis(),
            payload_json,
            payload_sha256,
        ],
    )?;
    Ok((next, event))
}

fn update_snapshot(
    transaction: &Transaction<'_>,
    before: &GoalRecord,
    after: &GoalRecord,
) -> Result<()> {
    let acceptance_json = serde_json::to_string(&after.acceptance_criteria)?;
    let continuity_policy_json = after
        .continuity_policy
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let continuity_state_json = after
        .continuity_state
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let changed = transaction.execute(
        "UPDATE goals SET status = ?1, started_at_ms = ?2, updated_at_ms = ?3, \
         finished_at_ms = ?4, revision = ?5, completion_summary = ?6, failure_reason = ?7, \
         pause_reason = ?8, cancel_reason = ?9, acceptance_json = ?10, claimed_by = ?11, \
         claim_expires_at_ms = ?12, claim_generation = ?13, continuity_policy_json = ?14, \
         continuity_state_json = ?15 WHERE owner = ?16 AND id = ?17 AND revision = ?18",
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
            acceptance_json,
            after.claimed_by,
            after.claim_expires_at.map(|value| value.timestamp_millis()),
            counter_to_i64(after.claim_generation, "claim generation")?,
            continuity_policy_json,
            continuity_state_json,
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
    let continuity_policy: Option<GoalContinuityPolicy> = optional_json_column(row, 20)?;
    let continuity_state: Option<GoalContinuityState> = optional_json_column(row, 21)?;
    if let Some(policy) = &continuity_policy {
        policy.validate().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(20, Type::Text, Box::new(error))
        })?;
    }
    if let Some(state) = &continuity_state {
        state.validate().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(21, Type::Text, Box::new(error))
        })?;
    }
    Ok(GoalRecord {
        id: row.get(0)?,
        owner: row.get(1)?,
        title: row.get(2)?,
        description: row.get(3)?,
        status: enum_column(row, 4)?,
        priority,
        parent_goal_id: row.get(6)?,
        acceptance_criteria,
        continuity_policy,
        continuity_state,
        created_at: timestamp_column(row, 8)?,
        started_at: optional_timestamp_column(row, 9)?,
        updated_at: timestamp_column(row, 10)?,
        finished_at: optional_timestamp_column(row, 11)?,
        revision: counter_column(row, 12)?,
        completion_summary: row.get(13)?,
        failure_reason: row.get(14)?,
        pause_reason: row.get(15)?,
        cancel_reason: row.get(16)?,
        claimed_by: row.get(17)?,
        claim_expires_at: optional_timestamp_column(row, 18)?,
        claim_generation: counter_column(row, 19)?,
    })
}

fn optional_json_column<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    row.get::<_, Option<String>>(index)?
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
            })
        })
        .transpose()
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

fn row_to_verification(row: &Row<'_>) -> rusqlite::Result<GoalVerificationArtifact> {
    let owner: String = row.get(2)?;
    let goal_id: String = row.get(3)?;
    let payload_json: String = row.get(6)?;
    let payload_sha256: String = row.get(7)?;
    if hex_digest(payload_json.as_bytes()) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            7,
            Type::Text,
            "verification artifact digest mismatch".into(),
        ));
    }
    let verification =
        serde_json::from_str::<AcceptanceVerification>(&payload_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(6, Type::Text, Box::new(error))
        })?;
    if verification.goal_id != goal_id {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            6,
            Type::Text,
            "verification payload goal id mismatch".into(),
        ));
    }
    Ok(GoalVerificationArtifact {
        sequence: counter_column(row, 0)?,
        verification_id: row.get(1)?,
        owner,
        goal_id,
        recorded_at: timestamp_column(row, 4)?,
        worker_id: row.get(5)?,
        verification,
        payload_sha256,
    })
}

fn row_to_pursuit_checkpoint(row: &Row<'_>) -> rusqlite::Result<GoalPursuitCheckpoint> {
    let owner: String = row.get(0)?;
    let goal_id: String = row.get(1)?;
    let checkpoint_revision = counter_column(row, 2)?;
    let claim_generation = counter_column(row, 3)?;
    let updated_at = timestamp_column(row, 4)?;
    let payload_json: String = row.get(5)?;
    let payload_sha256: String = row.get(6)?;
    if hex_digest(payload_json.as_bytes()) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            6,
            Type::Text,
            "pursuit checkpoint digest mismatch".into(),
        ));
    }
    let checkpoint =
        serde_json::from_str::<GoalPursuitCheckpoint>(&payload_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
        })?;
    if checkpoint.owner != owner
        || checkpoint.goal_id != goal_id
        || checkpoint.checkpoint_revision != checkpoint_revision
        || checkpoint.claim_generation != claim_generation
        || checkpoint.updated_at != updated_at
    {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            5,
            Type::Text,
            "pursuit checkpoint envelope mismatch".into(),
        ));
    }
    checkpoint.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
    })?;
    Ok(checkpoint)
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

const TAKEOVER_COLUMNS: &str = "\
    approval_id, owner, goal_id, step_id, step_kind, quota_event_id, snapshot_digest, \
    target_profile, target_provider, target_protocol, target_harness, target_model, workdir, \
    sandbox, timeout_seconds, goal_revision, plan_snapshot_json, status, decided_by, \
    decision_reason, run_id, run_status, blocker_reason, created_at_ms, decided_at_ms, updated_at_ms, \
    upstream_approval_id, upstream_run_id, upstream_run_status";

fn select_takeover_approval_by_id(
    connection: &Connection,
    owner: &str,
    approval_id: &str,
) -> Result<Option<TakeoverApproval>> {
    let sql = format!(
        "SELECT {TAKEOVER_COLUMNS} FROM goal_takeover_approvals WHERE owner = ?1 AND approval_id = ?2"
    );
    connection
        .query_row(&sql, params![owner, approval_id], row_to_takeover_approval)
        .optional()
        .map_err(GoalStoreError::from)
}

fn select_takeover_approval(
    transaction: &Transaction<'_>,
    owner: &str,
    snapshot_digest: &str,
) -> Result<TakeoverApproval> {
    let sql = format!(
        "SELECT {TAKEOVER_COLUMNS} FROM goal_takeover_approvals \
         WHERE owner = ?1 AND snapshot_digest = ?2"
    );
    transaction
        .query_row(
            &sql,
            params![owner, snapshot_digest],
            row_to_takeover_approval,
        )
        .map_err(GoalStoreError::from)
}

fn row_to_takeover_approval(row: &Row<'_>) -> rusqlite::Result<TakeoverApproval> {
    let plan_snapshot_json: String = row.get(16)?;
    let plan_snapshot =
        serde_json::from_str::<GoalContinuityState>(&plan_snapshot_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(16, Type::Text, Box::new(error))
        })?;
    let run_status = optional_enum_string(row, 21)?
        .map(|value| TakeoverRunStatus::parse(&value))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(21, Type::Text, Box::new(error))
        })?;
    let upstream_run_status = optional_enum_string(row, 28)?
        .map(|value| TakeoverRunStatus::parse(&value))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(28, Type::Text, Box::new(error))
        })?;
    let status_string: String = row.get(17)?;
    let status = TakeoverApprovalStatus::parse(&status_string).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(17, Type::Text, Box::new(error))
    })?;
    let sandbox_string: String = row.get(13)?;
    let sandbox = TakeoverSandbox::parse(&sandbox_string).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(13, Type::Text, Box::new(error))
    })?;
    let target = TakeoverBoundTarget {
        profile: row.get(7)?,
        provider: row.get(8)?,
        protocol: row.get(9)?,
        harness: row.get(10)?,
        model: row.get(11)?,
    };
    target.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(error))
    })?;
    plan_snapshot.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(16, Type::Text, Box::new(error))
    })?;
    let workdir_string: String = row.get(12)?;
    let workdir = PathBuf::from(workdir_string);
    if !workdir.is_absolute() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            12,
            Type::Text,
            "takeover workdir must be absolute".into(),
        ));
    }
    let timeout_secs: i64 = row.get(14)?;
    let timeout_secs = u64::try_from(timeout_secs).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(14, Type::Integer, Box::new(error))
    })?;
    let approval = TakeoverApproval {
        approval_id: row.get(0)?,
        owner: row.get(1)?,
        goal_id: row.get(2)?,
        step_id: row.get(3)?,
        step_kind: row.get(4)?,
        quota_event_id: row.get(5)?,
        snapshot_digest: row.get(6)?,
        target,
        workdir,
        sandbox,
        timeout_secs,
        goal_revision: counter_column(row, 15)?,
        plan_snapshot,
        upstream_approval_id: row.get(26)?,
        upstream_run_id: row.get(27)?,
        upstream_run_status,
        status,
        decided_by: row.get(18)?,
        decision_reason: row.get(19)?,
        run_id: row.get(20)?,
        run_status,
        blocker_reason: row.get(22)?,
        created_at: timestamp_column(row, 23)?,
        decided_at: optional_timestamp_column(row, 24)?,
        updated_at: timestamp_column(row, 25)?,
    };
    let request = TakeoverApprovalRequest {
        goal_id: approval.goal_id.clone(),
        step_id: approval.step_id.clone(),
        step_kind: approval.step_kind.clone(),
        quota_event_id: approval.quota_event_id.clone(),
        target: approval.target.clone(),
        workdir: approval.workdir.clone(),
        sandbox: approval.sandbox,
        timeout: Duration::from_secs(approval.timeout_secs),
        goal_revision: approval.goal_revision,
        plan_snapshot: approval.plan_snapshot.clone(),
        upstream_approval_id: approval.upstream_approval_id.clone(),
        upstream_run_id: approval.upstream_run_id.clone(),
        upstream_run_status: approval.upstream_run_status,
    };
    request.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(16, Type::Text, Box::new(error))
    })?;
    let digest = request.snapshot_payload().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, Type::Text, Box::new(error))
    })?;
    if hex_digest(digest.as_bytes()) != approval.snapshot_digest {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            6,
            Type::Text,
            "takeover approval snapshot digest mismatch".into(),
        ));
    }
    Ok(approval)
}

fn optional_enum_string(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<String>> {
    row.get::<_, Option<String>>(index)
}

fn validate_takeover_boundary(approval: &TakeoverApproval, goal: &GoalRecord) -> Result<()> {
    if goal.status != GoalStatus::InProgress {
        return Err(GoalStoreError::InvalidStatus {
            id: goal.id.clone(),
            operation: "consume takeover approval for",
            status: goal.status,
        });
    }
    let Some(state) = &goal.continuity_state else {
        return Err(GoalStoreError::TakeoverBoundaryChanged {
            id: approval.approval_id.clone(),
        });
    };
    if goal.revision != approval.goal_revision
        || state != &approval.plan_snapshot
        || state.quota_event_id != approval.quota_event_id
    {
        return Err(GoalStoreError::TakeoverBoundaryChanged {
            id: approval.approval_id.clone(),
        });
    }
    let Some(target) = ready_approval_target(state, &approval.step_id, &approval.step_kind) else {
        return Err(GoalStoreError::TakeoverBoundaryChanged {
            id: approval.approval_id.clone(),
        });
    };
    let bound = TakeoverBoundTarget::from_execution(target);
    if bound != approval.target {
        return Err(GoalStoreError::TakeoverBoundaryChanged {
            id: approval.approval_id.clone(),
        });
    }
    Ok(())
}

fn validate_takeover_request(request: &TakeoverApprovalRequest, goal: &GoalRecord) -> Result<()> {
    if goal.status != GoalStatus::InProgress {
        return Err(GoalStoreError::InvalidStatus {
            id: goal.id.clone(),
            operation: "queue takeover approval for",
            status: goal.status,
        });
    }
    let Some(state) = &goal.continuity_state else {
        return Err(GoalStoreError::InvalidInput(
            "goal has no visible continuity state".into(),
        ));
    };
    let Some(target) = ready_approval_target(state, &request.step_id, &request.step_kind) else {
        return Err(GoalStoreError::InvalidInput(
            "goal has no matching ready continuity step".into(),
        ));
    };
    let requires_approval = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == request.step_id)
        .is_some_and(|step| step.requires_approval);
    if !requires_approval
        || goal.revision != request.goal_revision
        || state != &request.plan_snapshot
        || state.quota_event_id != request.quota_event_id
        || TakeoverBoundTarget::from_execution(target) != request.target
    {
        return Err(GoalStoreError::InvalidInput(
            "takeover approval request does not match the current ready step".into(),
        ));
    }
    Ok(())
}

fn validate_upstream_review_evidence(
    transaction: &Transaction<'_>,
    owner: &str,
    request: &TakeoverApprovalRequest,
) -> Result<()> {
    let Some(upstream_id) = request.upstream_approval_id.as_deref() else {
        return Ok(());
    };
    let upstream =
        select_takeover_approval_by_id(transaction, owner, upstream_id)?.ok_or_else(|| {
            GoalStoreError::InvalidInput("review upstream takeover approval was not found".into())
        })?;
    if request.step_id != "review_takeover"
        || upstream.goal_id != request.goal_id
        || upstream.quota_event_id != request.quota_event_id
        || upstream.step_id != "takeover"
        || upstream.step_kind != "start_takeover"
        || upstream.status != TakeoverApprovalStatus::Done
        || upstream.run_status != Some(TakeoverRunStatus::Success)
        || upstream.run_id != request.upstream_run_id
        || request.upstream_run_status != Some(TakeoverRunStatus::Success)
    {
        return Err(GoalStoreError::InvalidInput(
            "review approval is not bound to the exact successful takeover run".into(),
        ));
    }
    Ok(())
}

fn validate_upstream_approval_record(
    transaction: &Transaction<'_>,
    owner: &str,
    approval: &TakeoverApproval,
) -> Result<()> {
    let request = TakeoverApprovalRequest {
        goal_id: approval.goal_id.clone(),
        step_id: approval.step_id.clone(),
        step_kind: approval.step_kind.clone(),
        quota_event_id: approval.quota_event_id.clone(),
        target: approval.target.clone(),
        workdir: approval.workdir.clone(),
        sandbox: approval.sandbox,
        timeout: Duration::from_secs(approval.timeout_secs),
        goal_revision: approval.goal_revision,
        plan_snapshot: approval.plan_snapshot.clone(),
        upstream_approval_id: approval.upstream_approval_id.clone(),
        upstream_run_id: approval.upstream_run_id.clone(),
        upstream_run_status: approval.upstream_run_status,
    };
    validate_upstream_review_evidence(transaction, owner, &request).map_err(|_| {
        GoalStoreError::TakeoverBoundaryChanged {
            id: approval.approval_id.clone(),
        }
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
        ("table", "goal_verifications"),
        ("table", "goal_pursuit_checkpoints"),
        ("table", "goal_takeover_approvals"),
        ("trigger", "goal_events_immutable_update"),
        ("trigger", "goal_events_immutable_delete"),
        ("trigger", "goal_verifications_immutable_update"),
        ("trigger", "goal_verifications_immutable_delete"),
        ("index", "goals_owner_worker_lease_idx"),
        ("index", "goals_owner_lease_idx"),
        ("index", "goal_takeover_approvals_owner_goal_idx"),
        ("index", "goal_takeover_approvals_owner_status_idx"),
        ("index", "goal_takeover_approvals_upstream_idx"),
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
    for name in ["continuity_policy_json", "continuity_state_json"] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('goals') WHERE name = ?1)",
            [name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(GoalStoreError::CorruptData(format!(
                "goal schema is missing column `goals.{name}`"
            )));
        }
    }
    for name in [
        "upstream_approval_id",
        "upstream_run_id",
        "upstream_run_status",
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('goal_takeover_approvals') WHERE name = ?1)",
            [name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(GoalStoreError::CorruptData(format!(
                "goal schema is missing column `goal_takeover_approvals.{name}`"
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
