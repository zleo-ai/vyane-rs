use chrono::{DateTime, Utc};

use crate::{GoalEvent, GoalQuery, GoalRecord, NewGoal, Result};

pub trait GoalStore: Send + Sync {
    fn create(&self, owner: &str, goal: NewGoal) -> Result<GoalRecord>;

    fn get(&self, owner: &str, id: &str) -> Result<Option<GoalRecord>>;

    fn list(&self, owner: &str, query: &GoalQuery) -> Result<Vec<GoalRecord>>;

    fn next_queued(&self, owner: &str) -> Result<Option<GoalRecord>>;

    fn events(&self, owner: &str, id: &str) -> Result<Vec<GoalEvent>>;

    fn start(&self, owner: &str, id: &str, at: DateTime<Utc>) -> Result<GoalRecord>;

    fn progress(
        &self,
        owner: &str,
        id: &str,
        stage: &str,
        detail: &str,
        at: DateTime<Utc>,
    ) -> Result<GoalEvent>;

    fn pause(
        &self,
        owner: &str,
        id: &str,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    fn resume(&self, owner: &str, id: &str, at: DateTime<Utc>) -> Result<GoalRecord>;

    fn done(
        &self,
        owner: &str,
        id: &str,
        summary: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    fn fail(&self, owner: &str, id: &str, reason: &str, at: DateTime<Utc>) -> Result<GoalRecord>;

    fn cancel(
        &self,
        owner: &str,
        id: &str,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;
}
