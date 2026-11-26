//! Read-only "waiting for you" list assembled from existing stores.
//!
//! The three sources are independent:
//!
//! * `approvals` — kernel approval decisions that are still `pending`
//! * `tasks` — kernel deliveries whose phase is `approval_required`
//! * `projections` — goal continuity projections whose next action is
//!   `decide_approval` or `manual_decision`
//!
//! A source that is turned off or cannot be read is named in `sources` and
//! contributes no rows. The other sources are still returned. This is not the
//! Python `/api/waiting` surface: there is no board inbox, notification scan,
//! secret prompt, snooze, or HTTP route, and nothing here writes.

use std::fmt;
use std::path::Path;

use serde::Serialize;
use vyane_goal::{
    GoalContinuityNextActionKind, GoalQuery, GoalStatus, GoalStore, GoalStoreError,
    SqliteGoalStore, project_continuity_next_action,
};

use crate::StoragePaths;
use crate::kernel_store::{KernelStore, KernelStoreError};

/// Stable document name for the assembled list.
pub const WAITING_LIST_SCHEMA: &str = "waiting-list.v1";

/// Maximum rows taken from one source. One extra row is read to detect truncation.
pub const WAITING_SOURCE_LIMIT: usize = 100;

const READ_LIMIT: usize = WAITING_SOURCE_LIMIT + 1;

/// Which existing store a waiting row came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitingSourceName {
    Approvals,
    Tasks,
    Projections,
}

impl WaitingSourceName {
    /// Stable snake_case token matching the serde rename.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approvals => "approvals",
            Self::Tasks => "tasks",
            Self::Projections => "projections",
        }
    }
}

impl fmt::Display for WaitingSourceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether a source contributed rows, was turned off, or could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitingSourceStatus {
    Ok,
    Off,
    Unavailable,
}

impl WaitingSourceStatus {
    /// Stable snake_case token matching the serde rename.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Off => "off",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for WaitingSourceStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Closed reason for a source that is off or unavailable.
///
/// Reasons name the condition. They do not include paths, SQL text, or row
/// contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitingSourceReason {
    TurnedOff,
    NotFound,
    UnsupportedSchema,
    ReadFailed,
}

impl WaitingSourceReason {
    /// Stable snake_case token matching the serde rename.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TurnedOff => "turned_off",
            Self::NotFound => "not_found",
            Self::UnsupportedSchema => "unsupported_schema",
            Self::ReadFailed => "read_failed",
        }
    }
}

impl fmt::Display for WaitingSourceReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One row that is waiting for a human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WaitingItem {
    pub source: WaitingSourceName,
    pub id: String,
    pub label: String,
    pub kind: String,
}

/// One source's rows, possibly truncated to [`WAITING_SOURCE_LIMIT`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingPage {
    pub items: Vec<WaitingItem>,
    pub truncated: bool,
}

/// Input to the pure assembler. `Off` is a configured choice, not a read error.
#[derive(Debug)]
pub enum WaitingRead {
    Off,
    Ready(Result<WaitingPage, WaitingSourceReason>),
}

/// Named outcome for one source. Present even when that source contributed no rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WaitingSourceReport {
    pub name: WaitingSourceName,
    pub status: WaitingSourceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<WaitingSourceReason>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
}

/// Assembled list. `sources` always names approvals, tasks, and projections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WaitingList {
    pub schema: &'static str,
    pub items: Vec<WaitingItem>,
    pub sources: Vec<WaitingSourceReport>,
}

/// Caller error that prevents every source from being read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitingQueryError {
    InvalidOwner,
}

impl fmt::Display for WaitingQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOwner => formatter.write_str("owner is invalid"),
        }
    }
}

impl std::error::Error for WaitingQueryError {}

/// Where to read, and which sources to leave turned off.
pub struct WaitingQuery<'a> {
    pub owner: &'a str,
    pub goal_db: Option<&'a Path>,
    pub kernel_db: Option<&'a Path>,
    pub without: &'a [WaitingSourceName],
}

/// Continuity next actions that are waiting for a human decision.
///
/// `queue_approval` is an operator step, and `execute_approval` is ready to
/// run. Neither is a decision still waiting on a person.
#[must_use]
pub const fn waits_for_human_decision(action: GoalContinuityNextActionKind) -> bool {
    matches!(
        action,
        GoalContinuityNextActionKind::DecideApproval | GoalContinuityNextActionKind::ManualDecision
    )
}

/// Combine three source results. A failed or disabled source does not drop the others.
#[must_use]
pub fn assemble_waiting_list(
    approvals: WaitingRead,
    tasks: WaitingRead,
    projections: WaitingRead,
) -> WaitingList {
    let mut items = Vec::new();
    let sources = vec![
        take_source(WaitingSourceName::Approvals, approvals, &mut items),
        take_source(WaitingSourceName::Tasks, tasks, &mut items),
        take_source(WaitingSourceName::Projections, projections, &mut items),
    ];
    items.sort_by(|left, right| {
        source_rank(left.source)
            .cmp(&source_rank(right.source))
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    WaitingList {
        schema: WAITING_LIST_SCHEMA,
        items,
        sources,
    }
}

/// Read the configured sources without writing to either database.
///
/// An omitted kernel database turns the kernel sources off. A missing file or
/// a failed query marks only the sources that needed that read.
pub fn collect_waiting_list(query: &WaitingQuery<'_>) -> Result<WaitingList, WaitingQueryError> {
    vyane_core::validate_identifier("owner", query.owner, 128)
        .map_err(|_| WaitingQueryError::InvalidOwner)?;

    let approvals_off = query.without.contains(&WaitingSourceName::Approvals);
    let tasks_off = query.without.contains(&WaitingSourceName::Tasks);
    let projections_off = query.without.contains(&WaitingSourceName::Projections);
    let kernel_omitted = query.kernel_db.is_none();

    let goal = if projections_off {
        Opened::Idle
    } else {
        match open_goal(query.goal_db) {
            Ok(store) => Opened::Ready(store),
            Err(reason) => Opened::Failed(reason),
        }
    };
    let kernel = match query.kernel_db {
        Some(path) if !approvals_off || !tasks_off => match open_kernel(path) {
            Ok(store) => Opened::Ready(store),
            Err(reason) => Opened::Failed(reason),
        },
        Some(_) | None => Opened::Idle,
    };

    let approvals = kernel_source(approvals_off, kernel_omitted, &kernel, |store| {
        read_pending_approvals(store, query.owner)
    });
    let tasks = kernel_source(tasks_off, kernel_omitted, &kernel, |store| {
        read_approval_stage_tasks(store, query.owner)
    });
    let projections = if projections_off {
        WaitingRead::Off
    } else {
        WaitingRead::Ready(match &goal {
            Opened::Ready(store) => read_human_decision_projections(store, query.owner),
            Opened::Failed(reason) => Err(*reason),
            Opened::Idle => Err(WaitingSourceReason::ReadFailed),
        })
    };
    Ok(assemble_waiting_list(approvals, tasks, projections))
}

fn kernel_source(
    turned_off: bool,
    kernel_omitted: bool,
    kernel: &Opened<KernelStore>,
    read: impl FnOnce(&KernelStore) -> Result<WaitingPage, WaitingSourceReason>,
) -> WaitingRead {
    if turned_off || kernel_omitted {
        WaitingRead::Off
    } else {
        WaitingRead::Ready(match kernel {
            Opened::Ready(store) => read(store),
            Opened::Failed(reason) => Err(*reason),
            Opened::Idle => Err(WaitingSourceReason::ReadFailed),
        })
    }
}

/// Human table. Every source is named, including sources that failed or are off.
#[must_use]
pub fn format_waiting_list(list: &WaitingList) -> String {
    let mut text = String::new();
    text.push_str(list.schema);
    text.push('\n');
    for source in &list.sources {
        text.push_str(source.name.as_str());
        text.push(' ');
        text.push_str(source.status.as_str());
        if let Some(reason) = source.reason {
            text.push(' ');
            text.push_str(reason.as_str());
        }
        if source.truncated {
            text.push_str(" truncated");
        }
        text.push('\n');
    }
    if !list.items.is_empty() {
        text.push('\n');
    }
    for item in &list.items {
        text.push_str(item.source.as_str());
        text.push(' ');
        text.push_str(&item.id);
        text.push(' ');
        text.push_str(&item.label);
        text.push(' ');
        text.push_str(&item.kind);
        text.push('\n');
    }
    text
}

enum Opened<T> {
    Idle,
    Failed(WaitingSourceReason),
    Ready(T),
}

fn take_source(
    name: WaitingSourceName,
    read: WaitingRead,
    items: &mut Vec<WaitingItem>,
) -> WaitingSourceReport {
    match read {
        WaitingRead::Off => WaitingSourceReport {
            name,
            status: WaitingSourceStatus::Off,
            reason: Some(WaitingSourceReason::TurnedOff),
            truncated: false,
        },
        WaitingRead::Ready(Err(reason)) => WaitingSourceReport {
            name,
            status: WaitingSourceStatus::Unavailable,
            reason: Some(reason),
            truncated: false,
        },
        WaitingRead::Ready(Ok(page)) => {
            for mut item in page.items {
                item.source = name;
                items.push(item);
            }
            WaitingSourceReport {
                name,
                status: WaitingSourceStatus::Ok,
                reason: None,
                truncated: page.truncated,
            }
        }
    }
}

fn source_rank(name: WaitingSourceName) -> u8 {
    match name {
        WaitingSourceName::Approvals => 0,
        WaitingSourceName::Tasks => 1,
        WaitingSourceName::Projections => 2,
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn open_goal(path: Option<&Path>) -> Result<SqliteGoalStore, WaitingSourceReason> {
    let default_path;
    let path = match path {
        Some(path) => path,
        None => {
            default_path = StoragePaths::resolve()
                .map_err(|_| WaitingSourceReason::ReadFailed)?
                .goal_db_path();
            &default_path
        }
    };
    if !existing_regular_file(path)? {
        return Err(WaitingSourceReason::NotFound);
    }
    SqliteGoalStore::open_readonly(path).map_err(goal_reason)
}

fn open_kernel(path: &Path) -> Result<KernelStore, WaitingSourceReason> {
    if !existing_regular_file(path)? {
        return Err(WaitingSourceReason::NotFound);
    }
    KernelStore::open_readonly(path).map_err(kernel_reason)
}

fn existing_regular_file(path: &Path) -> Result<bool, WaitingSourceReason> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(WaitingSourceReason::ReadFailed)
        }
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(WaitingSourceReason::ReadFailed),
    }
}

fn read_pending_approvals(
    store: &KernelStore,
    owner: &str,
) -> Result<WaitingPage, WaitingSourceReason> {
    let rows = store
        .list_pending_approvals(owner, READ_LIMIT)
        .map_err(kernel_reason)?;
    let truncated = rows.len() > WAITING_SOURCE_LIMIT;
    let items = rows
        .into_iter()
        .take(WAITING_SOURCE_LIMIT)
        .map(|approval| WaitingItem {
            source: WaitingSourceName::Approvals,
            id: approval.approval_id,
            label: approval.receipt_id,
            kind: "pending".to_string(),
        })
        .collect();
    Ok(WaitingPage { items, truncated })
}

fn read_approval_stage_tasks(
    store: &KernelStore,
    owner: &str,
) -> Result<WaitingPage, WaitingSourceReason> {
    let rows = store
        .list_approval_stage_deliveries(owner, READ_LIMIT)
        .map_err(kernel_reason)?;
    let truncated = rows.len() > WAITING_SOURCE_LIMIT;
    let items = rows
        .into_iter()
        .take(WAITING_SOURCE_LIMIT)
        .map(|delivery| WaitingItem {
            source: WaitingSourceName::Tasks,
            id: delivery.receipt_id,
            label: delivery.run_id,
            kind: "approval_required".to_string(),
        })
        .collect();
    Ok(WaitingPage { items, truncated })
}

fn read_human_decision_projections(
    store: &SqliteGoalStore,
    owner: &str,
) -> Result<WaitingPage, WaitingSourceReason> {
    let goals = store
        .list(
            owner,
            &GoalQuery {
                statuses: vec![GoalStatus::InProgress],
                parent_goal_id: None,
                limit: READ_LIMIT,
            },
        )
        .map_err(goal_reason)?;
    let truncated = goals.len() > WAITING_SOURCE_LIMIT;
    let mut items = Vec::new();
    for goal in goals.into_iter().take(WAITING_SOURCE_LIMIT) {
        if goal.continuity_state.is_none() {
            continue;
        }
        let Some(snapshot) = store
            .continuity_projection_snapshot(owner, &goal.id)
            .map_err(goal_reason)?
        else {
            continue;
        };
        let action = project_continuity_next_action(&snapshot.goal, &snapshot.approvals)
            .map_err(goal_reason)?;
        if !waits_for_human_decision(action.action) {
            continue;
        }
        let mut label = one_line(&snapshot.goal.title);
        if label.is_empty() {
            label = snapshot.goal.id.clone();
        }
        items.push(WaitingItem {
            source: WaitingSourceName::Projections,
            id: snapshot.goal.id,
            label,
            kind: action.action.as_str().to_string(),
        });
    }
    Ok(WaitingPage { items, truncated })
}

fn goal_reason(error: GoalStoreError) -> WaitingSourceReason {
    match error {
        GoalStoreError::UnsupportedSchema { .. } => WaitingSourceReason::UnsupportedSchema,
        GoalStoreError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
            WaitingSourceReason::NotFound
        }
        _ => WaitingSourceReason::ReadFailed,
    }
}

fn kernel_reason(error: KernelStoreError) -> WaitingSourceReason {
    match error {
        KernelStoreError::UnsupportedSchema { .. } => WaitingSourceReason::UnsupportedSchema,
        KernelStoreError::Io(message) if message.contains("does not exist") => {
            WaitingSourceReason::NotFound
        }
        _ => WaitingSourceReason::ReadFailed,
    }
}

fn one_line(value: &str) -> String {
    const LIMIT: usize = 160;
    let text = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(LIMIT).collect();
    if chars.next().is_some() {
        let mut shortened: String = head.chars().take(LIMIT - 1).collect();
        shortened.push('…');
        shortened
    } else {
        head
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::{TimeZone as _, Utc};
    use vyane_goal::{
        GoalContinuityMode, GoalContinuityPolicy, GoalExecutionTarget, GoalQuotaEvent, GoalStore,
        NewGoal, SqliteGoalStore, apply_quota_handoff_events,
    };

    use crate::approval_fsm::DeliveryEvent;
    use crate::kernel_store::{ApprovalGrantBinding, KernelStore};
    use crate::test_support::private_tempdir;

    fn item(source: WaitingSourceName, id: &str) -> WaitingItem {
        WaitingItem {
            source,
            id: id.to_string(),
            label: id.to_string(),
            kind: "kind".to_string(),
        }
    }

    fn page(ids: &[&str], source: WaitingSourceName) -> WaitingRead {
        WaitingRead::Ready(Ok(WaitingPage {
            items: ids.iter().map(|id| item(source, id)).collect(),
            truncated: false,
        }))
    }

    #[test]
    fn all_three_sources_stay_visible() {
        let list = assemble_waiting_list(
            page(&["apr-1"], WaitingSourceName::Approvals),
            page(&["rcpt-1"], WaitingSourceName::Tasks),
            page(&["goal-1"], WaitingSourceName::Projections),
        );
        let sources: Vec<_> = list.items.iter().map(|item| item.source.as_str()).collect();
        assert_eq!(sources, ["approvals", "tasks", "projections"]);
        assert!(
            list.sources
                .iter()
                .all(|source| source.status == WaitingSourceStatus::Ok)
        );
        assert_eq!(list.sources.len(), 3);
    }

    #[test]
    fn a_turned_off_source_is_named_and_the_others_remain() {
        let list = assemble_waiting_list(
            WaitingRead::Off,
            page(&["rcpt-1"], WaitingSourceName::Tasks),
            page(&["goal-1"], WaitingSourceName::Projections),
        );
        assert_eq!(
            list.items
                .iter()
                .map(|item| item.source)
                .collect::<Vec<_>>(),
            vec![WaitingSourceName::Tasks, WaitingSourceName::Projections]
        );
        let approvals = &list.sources[0];
        assert_eq!(approvals.name, WaitingSourceName::Approvals);
        assert_eq!(approvals.status, WaitingSourceStatus::Off);
        assert_eq!(approvals.reason, Some(WaitingSourceReason::TurnedOff));
        assert!(
            list.sources[1..]
                .iter()
                .all(|source| source.status == WaitingSourceStatus::Ok)
        );
    }

    #[test]
    fn a_failed_source_is_named_and_does_not_blank_the_list() {
        let list = assemble_waiting_list(
            page(&["apr-1"], WaitingSourceName::Approvals),
            WaitingRead::Ready(Err(WaitingSourceReason::ReadFailed)),
            page(&["goal-1"], WaitingSourceName::Projections),
        );
        assert_eq!(
            list.items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["apr-1", "goal-1"]
        );
        let tasks = &list.sources[1];
        assert_eq!(tasks.name, WaitingSourceName::Tasks);
        assert_eq!(tasks.status, WaitingSourceStatus::Unavailable);
        assert_eq!(tasks.reason, Some(WaitingSourceReason::ReadFailed));
        let text = format_waiting_list(&list);
        assert!(text.contains("tasks unavailable read_failed"));
        assert!(text.contains("approvals apr-1"));
        assert!(text.contains("projections goal-1"));
    }

    #[test]
    fn human_decision_actions_are_only_decide_and_manual() {
        assert!(waits_for_human_decision(
            GoalContinuityNextActionKind::DecideApproval
        ));
        assert!(waits_for_human_decision(
            GoalContinuityNextActionKind::ManualDecision
        ));
        assert!(!waits_for_human_decision(
            GoalContinuityNextActionKind::QueueApproval
        ));
        assert!(!waits_for_human_decision(
            GoalContinuityNextActionKind::ExecuteApproval
        ));
        assert!(!waits_for_human_decision(
            GoalContinuityNextActionKind::WaitForDependency
        ));
    }

    #[test]
    fn one_line_collapses_whitespace_and_bounds_length() {
        assert_eq!(one_line("  need\na   decision  "), "need a decision");
        let long = "x".repeat(180);
        let line = one_line(&long);
        assert_eq!(line.chars().count(), 160);
        assert!(line.ends_with('…'));
    }

    fn at(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    fn target(role: &str) -> GoalExecutionTarget {
        GoalExecutionTarget {
            provider: "provider".into(),
            protocol: "openai_chat".into(),
            harness: "harness".into(),
            model: "model".into(),
            profile: None,
            role: role.into(),
        }
    }

    fn policy(takeover: bool) -> GoalContinuityPolicy {
        GoalContinuityPolicy {
            mode: GoalContinuityMode::QuotaHandoff,
            primary: target("primary"),
            takeover: if takeover {
                vec![target("takeover")]
            } else {
                Vec::new()
            },
            reviewer: None,
            resume_primary_after_reset: false,
            require_review_before_resume: false,
            wait_for_review_checks_before_resume: false,
        }
    }

    fn seed_goal(store: &SqliteGoalStore, id: &str, title: &str, takeover: bool) {
        let mut goal = NewGoal::new(title, at(1_700_000_000));
        goal.id = Some(id.to_string());
        goal.continuity_policy = Some(policy(takeover));
        store.create("local", goal).unwrap();
        store.start("local", id, at(1_700_000_001)).unwrap();
        let applied = apply_quota_handoff_events(
            store,
            "local",
            &[GoalQuotaEvent {
                event_id: format!("quota-{id}"),
                goal_id: Some(id.to_string()),
                provider: "provider".into(),
                harness: "harness".into(),
                model: "model".into(),
                session_id: None,
                observed_at: at(1_700_000_002),
                estimated_reset_at: None,
            }],
            at(1_700_000_003),
        )
        .unwrap();
        assert_eq!(applied.len(), 1, "{id}");
    }

    fn digest(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    fn freeze_sqlite(path: &std::path::Path) {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .unwrap();
    }

    struct Fixture {
        goal_db: std::path::PathBuf,
        kernel_db: std::path::PathBuf,
        _directory: tempfile::TempDir,
    }

    fn seed_fixture() -> Fixture {
        let directory = private_tempdir();
        let goal_db = directory.path().join("goals.sqlite");
        let kernel_db = directory.path().join("kernel.sqlite");
        let goals = SqliteGoalStore::open(&goal_db).unwrap();
        seed_goal(&goals, "decide-goal", "Need a decision", false);
        seed_goal(&goals, "queue-goal", "Still needs a queue", true);
        let mut plain = NewGoal::new("Plain work", at(1_700_000_000));
        plain.id = Some("plain-goal".into());
        goals.create("local", plain).unwrap();
        goals
            .start("local", "plain-goal", at(1_700_000_001))
            .unwrap();
        drop(goals);

        let kernel = KernelStore::open(&kernel_db).unwrap();
        let now = at(1_700_000_010);
        kernel
            .record_approval_required(
                "local",
                "apr-wait",
                "rcpt-wait",
                "run-wait",
                &digest(1),
                1,
                now,
            )
            .unwrap();
        kernel
            .record_approval_required(
                "local",
                "apr-done",
                "rcpt-done",
                "run-done",
                &digest(2),
                1,
                now,
            )
            .unwrap();
        kernel
            .grant_approval(
                &ApprovalGrantBinding {
                    owner: "local".into(),
                    receipt_id: "rcpt-done".into(),
                    run_id: "run-done".into(),
                    request_digest: digest(2),
                    expected_revision: 1,
                    lease_owner: "lease".into(),
                    generation: 1,
                    decided_by: "operator".into(),
                },
                now,
            )
            .unwrap();
        let (_phase, revision) = kernel
            .ensure_delivery_running("local", "rcpt-stage", "run-stage", now)
            .unwrap();
        kernel
            .set_delivery_phase(
                "local",
                "rcpt-stage",
                "run-stage",
                revision,
                DeliveryEvent::AskRequired,
                Some("apr-stage"),
                now,
            )
            .unwrap();
        kernel
            .ensure_delivery_running("local", "rcpt-run", "run-run", now)
            .unwrap();
        let (_phase, revision) = kernel
            .ensure_delivery_running("other", "rcpt-other", "run-other", now)
            .unwrap();
        kernel
            .set_delivery_phase(
                "other",
                "rcpt-other",
                "run-other",
                revision,
                DeliveryEvent::AskRequired,
                Some("apr-other"),
                now,
            )
            .unwrap();
        drop(kernel);
        freeze_sqlite(&goal_db);
        freeze_sqlite(&kernel_db);
        Fixture {
            goal_db,
            kernel_db,
            _directory: directory,
        }
    }

    fn query<'a>(fixture: &'a Fixture, without: &'a [WaitingSourceName]) -> WaitingQuery<'a> {
        WaitingQuery {
            owner: "local",
            goal_db: Some(&fixture.goal_db),
            kernel_db: Some(&fixture.kernel_db),
            without,
        }
    }

    fn file_bytes(path: &std::path::Path) -> Vec<u8> {
        std::fs::read(path).unwrap()
    }

    #[test]
    fn sqlite_sources_show_only_waiting_rows_and_do_not_write() {
        let fixture = seed_fixture();
        let before = (file_bytes(&fixture.goal_db), file_bytes(&fixture.kernel_db));
        let list = collect_waiting_list(&query(&fixture, &[])).unwrap();
        assert_eq!(before.0, file_bytes(&fixture.goal_db));
        assert_eq!(before.1, file_bytes(&fixture.kernel_db));

        let ids: Vec<_> = list
            .items
            .iter()
            .map(|item| (item.source.as_str(), item.id.as_str(), item.kind.as_str()))
            .collect();
        assert_eq!(
            ids,
            vec![
                ("approvals", "apr-wait", "pending"),
                ("tasks", "rcpt-stage", "approval_required"),
                ("projections", "decide-goal", "manual_decision"),
            ]
        );
        assert_eq!(list.items[2].label, "Need a decision");
        assert!(
            list.sources
                .iter()
                .all(|source| source.status == WaitingSourceStatus::Ok)
        );

        let readonly_goal = SqliteGoalStore::open_readonly(&fixture.goal_db).unwrap();
        assert!(
            readonly_goal
                .create("local", NewGoal::new("nope", at(1_700_000_100)))
                .is_err()
        );
        let readonly_kernel = KernelStore::open_readonly(&fixture.kernel_db).unwrap();
        assert!(
            readonly_kernel
                .ensure_delivery_running("local", "rcpt-new", "run-new", at(1_700_000_100))
                .is_err()
        );
        assert_eq!(before.0, file_bytes(&fixture.goal_db));
        assert_eq!(before.1, file_bytes(&fixture.kernel_db));
    }

    #[test]
    fn one_kernel_query_can_fail_without_hiding_the_other_sources() {
        let fixture = seed_fixture();
        {
            let connection = rusqlite::Connection::open(&fixture.kernel_db).unwrap();
            connection
                .execute("DROP TABLE kernel_approvals", [])
                .unwrap();
        }
        freeze_sqlite(&fixture.kernel_db);
        let before = file_bytes(&fixture.kernel_db);
        let list = collect_waiting_list(&query(&fixture, &[])).unwrap();
        assert_eq!(before, file_bytes(&fixture.kernel_db));
        assert_eq!(list.sources[0].name, WaitingSourceName::Approvals);
        assert_eq!(list.sources[0].status, WaitingSourceStatus::Unavailable);
        assert_eq!(
            list.sources[0].reason,
            Some(WaitingSourceReason::ReadFailed)
        );
        assert_eq!(list.sources[1].status, WaitingSourceStatus::Ok);
        assert_eq!(list.sources[2].status, WaitingSourceStatus::Ok);
        let ids: Vec<_> = list.items.iter().map(|item| item.id.as_str()).collect();
        assert_eq!(ids, ["rcpt-stage", "decide-goal"]);
    }

    #[test]
    fn a_missing_goal_database_names_only_the_projection_source() {
        let fixture = seed_fixture();
        let missing = fixture.goal_db.with_file_name("missing-goals.sqlite");
        let list = collect_waiting_list(&WaitingQuery {
            owner: "local",
            goal_db: Some(&missing),
            kernel_db: Some(&fixture.kernel_db),
            without: &[],
        })
        .unwrap();
        assert_eq!(list.sources[0].status, WaitingSourceStatus::Ok);
        assert_eq!(list.sources[1].status, WaitingSourceStatus::Ok);
        assert_eq!(list.sources[2].name, WaitingSourceName::Projections);
        assert_eq!(list.sources[2].status, WaitingSourceStatus::Unavailable);
        assert_eq!(list.sources[2].reason, Some(WaitingSourceReason::NotFound));
        assert!(list.items.iter().any(|item| item.id == "apr-wait"));
        assert!(list.items.iter().any(|item| item.id == "rcpt-stage"));
    }

    #[test]
    fn omitting_the_kernel_database_turns_those_sources_off() {
        let fixture = seed_fixture();
        let list = collect_waiting_list(&WaitingQuery {
            owner: "local",
            goal_db: Some(&fixture.goal_db),
            kernel_db: None,
            without: &[],
        })
        .unwrap();
        assert_eq!(list.sources[0].status, WaitingSourceStatus::Off);
        assert_eq!(list.sources[0].reason, Some(WaitingSourceReason::TurnedOff));
        assert_eq!(list.sources[1].status, WaitingSourceStatus::Off);
        assert_eq!(list.sources[2].status, WaitingSourceStatus::Ok);
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].id, "decide-goal");
    }

    #[test]
    fn approval_stage_pages_report_truncation() {
        let directory = private_tempdir();
        let kernel_db = directory.path().join("kernel.sqlite");
        let kernel = KernelStore::open(&kernel_db).unwrap();
        let now = at(1_700_000_200);
        for index in 0..=WAITING_SOURCE_LIMIT {
            let receipt = format!("rcpt-{index:03}");
            let (phase, revision) = kernel
                .ensure_delivery_running("local", &receipt, "run", now)
                .unwrap();
            assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Running);
            kernel
                .set_delivery_phase(
                    "local",
                    &receipt,
                    "run",
                    revision,
                    DeliveryEvent::AskRequired,
                    None,
                    now,
                )
                .unwrap();
        }
        drop(kernel);
        freeze_sqlite(&kernel_db);
        let list = collect_waiting_list(&WaitingQuery {
            owner: "local",
            goal_db: None,
            kernel_db: Some(&kernel_db),
            without: &[WaitingSourceName::Approvals, WaitingSourceName::Projections],
        })
        .unwrap();
        assert_eq!(list.sources[1].status, WaitingSourceStatus::Ok);
        assert!(list.sources[1].truncated);
        assert_eq!(
            list.items
                .iter()
                .filter(|item| item.source == WaitingSourceName::Tasks)
                .count(),
            WAITING_SOURCE_LIMIT
        );
        assert!(list.items.iter().all(|item| item.id != "rcpt-100"));
    }

    #[test]
    fn a_missing_kernel_database_names_both_kernel_sources() {
        let fixture = seed_fixture();
        let missing = fixture.kernel_db.with_file_name("missing-kernel.sqlite");
        let list = collect_waiting_list(&WaitingQuery {
            owner: "local",
            goal_db: Some(&fixture.goal_db),
            kernel_db: Some(&missing),
            without: &[],
        })
        .unwrap();
        assert_eq!(list.sources[0].status, WaitingSourceStatus::Unavailable);
        assert_eq!(list.sources[0].reason, Some(WaitingSourceReason::NotFound));
        assert_eq!(list.sources[1].status, WaitingSourceStatus::Unavailable);
        assert_eq!(list.sources[1].reason, Some(WaitingSourceReason::NotFound));
        assert_eq!(list.sources[2].status, WaitingSourceStatus::Ok);
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].id, "decide-goal");
    }

    #[test]
    fn an_unrelated_sqlite_file_names_an_unsupported_kernel_schema() {
        let directory = private_tempdir();
        let kernel_db = directory.path().join("unrelated.sqlite");
        rusqlite::Connection::open(&kernel_db)
            .unwrap()
            .execute("CREATE TABLE unrelated(id INTEGER)", [])
            .unwrap();
        let list = collect_waiting_list(&WaitingQuery {
            owner: "local",
            goal_db: None,
            kernel_db: Some(&kernel_db),
            without: &[WaitingSourceName::Projections],
        })
        .unwrap();
        assert_eq!(
            list.sources[0].reason,
            Some(WaitingSourceReason::UnsupportedSchema)
        );
        assert_eq!(
            list.sources[1].reason,
            Some(WaitingSourceReason::UnsupportedSchema)
        );
        assert_eq!(list.sources[2].status, WaitingSourceStatus::Off);
        assert!(list.items.is_empty());
    }

    #[test]
    fn an_invalid_owner_is_rejected_before_any_read() {
        let error = collect_waiting_list(&WaitingQuery {
            owner: " local",
            goal_db: None,
            kernel_db: None,
            without: &[],
        })
        .unwrap_err();
        assert_eq!(error, WaitingQueryError::InvalidOwner);
        assert_eq!(error.to_string(), "owner is invalid");
    }
}
