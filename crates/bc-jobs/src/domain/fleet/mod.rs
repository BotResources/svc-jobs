pub mod capacity;
pub mod instance;
pub mod lifecycle;
pub mod status;
pub mod view;

use chrono::{DateTime, Utc};

use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::fleet::lifecycle::RunnerTypeLifecycle;
use crate::domain::ids::RunnerTypeId;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::error::JobsError;

#[derive(Debug, Clone)]
pub struct RunnerTypeState {
    pub id: RunnerTypeId,
    pub key: RunnerTypeKey,
    pub registered_at: DateTime<Utc>,
    pub lifecycle: RunnerTypeLifecycle,
    pub instances: Vec<RunnerInstance>,
}

#[derive(Debug, Clone)]
pub struct RunnerType {
    id: RunnerTypeId,
    key: RunnerTypeKey,
    registered_at: DateTime<Utc>,
    lifecycle: RunnerTypeLifecycle,
    instances: Vec<RunnerInstance>,
}

impl RunnerType {
    pub fn hydrate(state: RunnerTypeState) -> Result<Self, JobsError> {
        let mut keys: Vec<&InstanceKey> = state.instances.iter().map(RunnerInstance::key).collect();
        keys.sort();
        if keys.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(JobsError::CorruptState {
                reason_code: "duplicate_instance_key",
            });
        }
        if state
            .instances
            .iter()
            .any(|live| live.connected_at() < state.registered_at)
        {
            return Err(JobsError::CorruptState {
                reason_code: "instance_connected_before_registration",
            });
        }
        Ok(Self {
            id: state.id,
            key: state.key,
            registered_at: state.registered_at,
            lifecycle: state.lifecycle,
            instances: state.instances,
        })
    }

    pub fn id(&self) -> RunnerTypeId {
        self.id
    }

    pub fn key(&self) -> &RunnerTypeKey {
        &self.key
    }

    pub fn registered_at(&self) -> DateTime<Utc> {
        self.registered_at
    }

    pub fn instances(&self) -> &[RunnerInstance] {
        &self.instances
    }

    pub fn lifecycle(&self) -> RunnerTypeLifecycle {
        self.lifecycle
    }

    pub fn instance(&self, key: &InstanceKey) -> Option<&RunnerInstance> {
        self.instances.iter().find(|live| live.key() == key)
    }

    pub fn is_available(&self) -> bool {
        self.lifecycle.accepts_jobs() && self.instances.iter().any(|live| live.accepts_new_work())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fleet::capacity::Capacity;
    use crate::domain::fleet::instance::RunnerInstanceState;
    use crate::domain::fleet::status::ReportedStatus;
    use crate::domain::ids::PresenceSessionId;
    use crate::domain::keys::RunnerVersion;
    use uuid::Uuid;

    fn at(offset: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset, 0).unwrap()
    }

    fn reporting(key: &str, connected: i64, status: ReportedStatus) -> RunnerInstance {
        RunnerInstance::hydrate(RunnerInstanceState {
            key: InstanceKey::new(key).unwrap(),
            session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            version: RunnerVersion::new("1.4.2").unwrap(),
            reported_status: status,
            capacity: Capacity::new(1).unwrap(),
            connected_at: at(connected),
            last_observed_at: at(connected + 5),
            status_change_number: 0,
        })
        .unwrap()
    }

    fn live(key: &str, connected: i64) -> RunnerInstance {
        reporting(key, connected, ReportedStatus::Ready)
    }

    fn state(instances: Vec<RunnerInstance>) -> RunnerTypeState {
        RunnerTypeState {
            id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            key: RunnerTypeKey::new("analyst").unwrap(),
            registered_at: at(0),
            lifecycle: RunnerTypeLifecycle::Active,
            instances,
        }
    }

    #[test]
    fn availability_is_derived_from_presence_alone() {
        // Given: a runner type whose instances have all gone
        let empty = RunnerType::hydrate(state(vec![])).unwrap();
        // When: one instance reappears
        let populated = RunnerType::hydrate(state(vec![live("pod-7", 1)])).unwrap();
        // Then: availability follows presence, never a stored flag
        assert!(!empty.is_available());
        assert!(populated.is_available());
    }

    #[test]
    fn a_type_whose_every_live_instance_is_draining_is_not_available() {
        // Given: a type whose two live instances are both finishing their current work
        let draining = RunnerType::hydrate(state(vec![
            reporting("pod-7", 1, ReportedStatus::Draining),
            reporting("pod-8", 1, ReportedStatus::Draining),
        ]))
        .unwrap();
        // Then: presence alone is not availability — nobody there would take a new run
        assert!(!draining.is_available());
    }

    #[test]
    fn one_ready_instance_among_draining_ones_keeps_the_type_available() {
        // Given: a fleet mid-rollout, one instance drained and one already restarted
        let mixed = RunnerType::hydrate(state(vec![
            reporting("pod-7", 1, ReportedStatus::Draining),
            reporting("pod-8", 1, ReportedStatus::Ready),
        ]))
        .unwrap();
        // Then: a single taker is enough — availability is not unanimity
        assert!(mixed.is_available());
    }

    #[test]
    fn two_instances_sharing_a_key_cannot_be_loaded() {
        // Given: a stored fleet with the same instance key twice
        let result = RunnerType::hydrate(state(vec![live("pod-7", 1), live("pod-7", 2)]));
        // Then: the collision is refused at load — the key is unique within the type
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "duplicate_instance_key"
            })
        );
    }

    #[test]
    fn an_instance_older_than_its_runner_type_cannot_be_loaded() {
        // Given: a stored instance that connected before the type was registered
        let result = RunnerType::hydrate(state(vec![live("pod-7", -30)]));
        // Then: the impossible ordering is refused
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "instance_connected_before_registration"
            })
        );
    }
}
