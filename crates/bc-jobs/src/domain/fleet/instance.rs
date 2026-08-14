use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ids::PresenceSessionId;
use crate::domain::keys::{InstanceKey, ReportedStatus, RunnerVersion};
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerInstance {
    key: InstanceKey,
    session_id: PresenceSessionId,
    version: RunnerVersion,
    reported_status: ReportedStatus,
    connected_at: DateTime<Utc>,
    last_observed_at: DateTime<Utc>,
    status_change_number: u32,
}

impl RunnerInstance {
    pub fn hydrate(
        key: InstanceKey,
        session_id: PresenceSessionId,
        version: RunnerVersion,
        reported_status: ReportedStatus,
        connected_at: DateTime<Utc>,
        last_observed_at: DateTime<Utc>,
        status_change_number: u32,
    ) -> Result<Self, JobsError> {
        if last_observed_at < connected_at {
            return Err(JobsError::CorruptState {
                reason_code: "presence_observed_before_connection",
            });
        }
        Ok(Self {
            key,
            session_id,
            version,
            reported_status,
            connected_at,
            last_observed_at,
            status_change_number,
        })
    }

    pub fn key(&self) -> &InstanceKey {
        &self.key
    }

    pub fn session_id(&self) -> PresenceSessionId {
        self.session_id
    }

    pub fn version(&self) -> &RunnerVersion {
        &self.version
    }

    pub fn reported_status(&self) -> &ReportedStatus {
        &self.reported_status
    }

    pub fn connected_at(&self) -> DateTime<Utc> {
        self.connected_at
    }

    pub fn last_observed_at(&self) -> DateTime<Utc> {
        self.last_observed_at
    }

    pub fn status_change_number(&self) -> u32 {
        self.status_change_number
    }

    pub fn reports_the_same_as(&self, version: &RunnerVersion, status: &ReportedStatus) -> bool {
        &self.version == version && &self.reported_status == status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn at(offset: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset, 0).unwrap()
    }

    fn instance(last_observed: i64) -> Result<RunnerInstance, JobsError> {
        RunnerInstance::hydrate(
            InstanceKey::new("pod-7").unwrap(),
            PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            RunnerVersion::new("1.4.2").unwrap(),
            ReportedStatus::new("idle").unwrap(),
            at(0),
            at(last_observed),
            0,
        )
    }

    #[test]
    fn a_presence_observed_before_the_connection_cannot_be_loaded() {
        // Given: a stored session last observed before it opened
        let result = instance(-10);
        // Then: the impossible session is refused at load
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "presence_observed_before_connection"
            })
        );
    }

    #[test]
    fn a_heartbeat_repeating_the_same_report_is_recognised_as_unchanged() {
        // Given: a live instance reporting version 1.4.2 and status idle
        let live = instance(30).unwrap();
        // When: the same report arrives again, then a different one
        // Then: only the differing report counts as a change
        assert!(live.reports_the_same_as(
            &RunnerVersion::new("1.4.2").unwrap(),
            &ReportedStatus::new("idle").unwrap()
        ));
        assert!(!live.reports_the_same_as(
            &RunnerVersion::new("1.4.2").unwrap(),
            &ReportedStatus::new("busy").unwrap()
        ));
    }
}
