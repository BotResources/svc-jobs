use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::status::ReportedStatus;
use crate::domain::ids::PresenceSessionId;
use crate::domain::keys::{InstanceKey, RunnerVersion};
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerInstance {
    key: InstanceKey,
    session_id: PresenceSessionId,
    version: RunnerVersion,
    reported_status: ReportedStatus,
    capacity: Capacity,
    connected_at: DateTime<Utc>,
    last_observed_at: DateTime<Utc>,
    status_change_number: u32,
}

#[derive(Debug, Clone)]
pub struct RunnerInstanceState {
    pub key: InstanceKey,
    pub session_id: PresenceSessionId,
    pub version: RunnerVersion,
    pub reported_status: ReportedStatus,
    pub capacity: Capacity,
    pub connected_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub status_change_number: u32,
}

impl RunnerInstance {
    pub fn hydrate(state: RunnerInstanceState) -> Result<Self, JobsError> {
        if state.last_observed_at < state.connected_at {
            return Err(JobsError::CorruptState {
                reason_code: "presence_observed_before_connection",
            });
        }
        Ok(Self {
            key: state.key,
            session_id: state.session_id,
            version: state.version,
            reported_status: state.reported_status,
            capacity: state.capacity,
            connected_at: state.connected_at,
            last_observed_at: state.last_observed_at,
            status_change_number: state.status_change_number,
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

    pub fn reported_status(&self) -> ReportedStatus {
        self.reported_status
    }

    pub fn capacity(&self) -> Capacity {
        self.capacity
    }

    pub fn accepts_new_work(&self) -> bool {
        self.reported_status.accepts_new_work()
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

    pub fn reports_the_same_as(
        &self,
        version: &RunnerVersion,
        status: ReportedStatus,
        capacity: Capacity,
    ) -> bool {
        &self.version == version && self.reported_status == status && self.capacity == capacity
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
        reporting(ReportedStatus::Ready, 2, last_observed)
    }

    fn reporting(
        status: ReportedStatus,
        capacity: u32,
        last_observed: i64,
    ) -> Result<RunnerInstance, JobsError> {
        RunnerInstance::hydrate(RunnerInstanceState {
            key: InstanceKey::new("pod-7").unwrap(),
            session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            version: RunnerVersion::new("1.4.2").unwrap(),
            reported_status: status,
            capacity: Capacity::new(capacity).unwrap(),
            connected_at: at(0),
            last_observed_at: at(last_observed),
            status_change_number: 0,
        })
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
        // Given: a live instance reporting version 1.4.2, status READY, capacity 2
        let live = instance(30).unwrap();
        let version = RunnerVersion::new("1.4.2").unwrap();
        // When: the same report arrives again, then reports differing on each field
        // Then: only a differing report counts as a change — capacity included, because a
        // runner that widened or narrowed its room changed what the fleet can take
        assert!(live.reports_the_same_as(
            &version,
            ReportedStatus::Ready,
            Capacity::new(2).unwrap()
        ));
        assert!(!live.reports_the_same_as(
            &version,
            ReportedStatus::Draining,
            Capacity::new(2).unwrap()
        ));
        assert!(!live.reports_the_same_as(
            &version,
            ReportedStatus::Ready,
            Capacity::new(5).unwrap()
        ));
    }

    #[test]
    fn a_draining_instance_is_still_live_but_takes_no_new_work() {
        // Given: an instance that announced it is winding down
        let draining = reporting(ReportedStatus::Draining, 2, 30).unwrap();
        // Then: it keeps its session — draining is a status, never a disconnection
        assert_eq!(draining.reported_status(), ReportedStatus::Draining);
        assert!(!draining.accepts_new_work());
    }
}
