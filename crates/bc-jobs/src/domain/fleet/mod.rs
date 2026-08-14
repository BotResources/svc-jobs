pub mod instance;

use chrono::{DateTime, Utc};

use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::ids::RunnerTypeId;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::error::JobsError;

#[derive(Debug, Clone)]
pub struct RunnerTypeState {
    pub id: RunnerTypeId,
    pub key: RunnerTypeKey,
    pub registered_at: DateTime<Utc>,
    pub instances: Vec<RunnerInstance>,
}

#[derive(Debug, Clone)]
pub struct RunnerType {
    id: RunnerTypeId,
    key: RunnerTypeKey,
    registered_at: DateTime<Utc>,
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

    pub fn instance(&self, key: &InstanceKey) -> Option<&RunnerInstance> {
        self.instances.iter().find(|live| live.key() == key)
    }

    pub fn is_available(&self) -> bool {
        !self.instances.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::PresenceSessionId;
    use crate::domain::keys::{ReportedStatus, RunnerVersion};
    use uuid::Uuid;

    fn at(offset: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset, 0).unwrap()
    }

    fn live(key: &str, connected: i64) -> RunnerInstance {
        RunnerInstance::hydrate(
            InstanceKey::new(key).unwrap(),
            PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            RunnerVersion::new("1.4.2").unwrap(),
            ReportedStatus::new("idle").unwrap(),
            at(connected),
            at(connected + 5),
            0,
        )
        .unwrap()
    }

    fn state(instances: Vec<RunnerInstance>) -> RunnerTypeState {
        RunnerTypeState {
            id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            key: RunnerTypeKey::new("analyst").unwrap(),
            registered_at: at(0),
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
