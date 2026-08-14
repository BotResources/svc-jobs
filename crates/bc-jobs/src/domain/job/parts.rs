use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ids::{JobId, ManualRetryId, ResolutionId};
use crate::domain::references::KnownUser;
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobDeletion {
    deleted_by: KnownUser,
    deleted_at: DateTime<Utc>,
}

impl JobDeletion {
    pub fn new(deleted_by: KnownUser, deleted_at: DateTime<Utc>) -> Self {
        Self {
            deleted_by,
            deleted_at,
        }
    }

    pub fn deleted_by(&self) -> &KnownUser {
        &self.deleted_by
    }

    pub fn deleted_at(&self) -> DateTime<Utc> {
        self.deleted_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualRetryRecord {
    id: ManualRetryId,
    failed_resolution_id: ResolutionId,
    successor_job_id: JobId,
    requested_by: KnownUser,
    requested_at: DateTime<Utc>,
    successor_is_terminal: bool,
}

impl ManualRetryRecord {
    pub fn new(
        id: ManualRetryId,
        failed_resolution_id: ResolutionId,
        successor_job_id: JobId,
        requested_by: KnownUser,
        requested_at: DateTime<Utc>,
        successor_is_terminal: bool,
    ) -> Self {
        Self {
            id,
            failed_resolution_id,
            successor_job_id,
            requested_by,
            requested_at,
            successor_is_terminal,
        }
    }

    pub fn id(&self) -> ManualRetryId {
        self.id
    }

    pub fn failed_resolution_id(&self) -> ResolutionId {
        self.failed_resolution_id
    }

    pub fn successor_job_id(&self) -> JobId {
        self.successor_job_id
    }

    pub fn requested_by(&self) -> &KnownUser {
        &self.requested_by
    }

    pub fn requested_at(&self) -> DateTime<Utc> {
        self.requested_at
    }

    pub fn successor_is_terminal(&self) -> bool {
        self.successor_is_terminal
    }

    pub fn matches(&self, id: ManualRetryId, successor_job_id: JobId) -> bool {
        self.id == id && self.successor_job_id == successor_job_id
    }

    pub fn guard_successor_settled(&self) -> Result<(), JobsError> {
        if self.successor_is_terminal {
            Ok(())
        } else {
            Err(JobsError::SuccessorStillActive {
                successor_job_id: self.successor_job_id.as_uuid(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::keys::DisplayName;
    use br_core_events::UserId;
    use uuid::Uuid;

    fn user() -> KnownUser {
        KnownUser::new(
            UserId(Uuid::now_v7()),
            DisplayName::new("Operator").unwrap(),
        )
        .unwrap()
    }

    fn record(successor_is_terminal: bool) -> ManualRetryRecord {
        ManualRetryRecord::new(
            ManualRetryId::new(Uuid::now_v7()).unwrap(),
            ResolutionId::new(Uuid::now_v7()).unwrap(),
            JobId::new(Uuid::now_v7()).unwrap(),
            user(),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            successor_is_terminal,
        )
    }

    #[test]
    fn a_predecessor_with_a_live_successor_is_not_settled() {
        // Given: a manual-retry successor still running
        let record = record(false);
        // When: the predecessor is asked whether the chain has settled
        let result = record.guard_successor_settled();
        // Then: it refuses, naming the successor still at work
        assert_eq!(
            result,
            Err(JobsError::SuccessorStillActive {
                successor_job_id: record.successor_job_id().as_uuid()
            })
        );
    }

    #[test]
    fn a_predecessor_whose_successor_finished_is_settled() {
        // Given: a manual-retry successor that reached a terminal state
        let record = record(true);
        // When/Then: the chain has settled
        assert_eq!(record.guard_successor_settled(), Ok(()));
    }

    #[test]
    fn an_intervention_matches_only_its_own_identity_and_successor() {
        // Given: a recorded intervention
        let record = record(false);
        // When/Then: a redelivery matches only on both halves of its identity
        assert!(record.matches(record.id(), record.successor_job_id()));
        assert!(!record.matches(
            ManualRetryId::new(Uuid::now_v7()).unwrap(),
            record.successor_job_id()
        ));
        assert!(!record.matches(record.id(), JobId::new(Uuid::now_v7()).unwrap()));
    }
}
