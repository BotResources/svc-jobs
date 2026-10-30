pub mod capacity;
pub mod dispatch_mode;
pub mod instance;
pub mod lifecycle;
pub mod run_duration;
pub mod settings;
pub mod status;
pub mod view;

use chrono::{DateTime, Utc};

use crate::domain::fleet::dispatch_mode::DispatchMode;
use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::fleet::lifecycle::RunnerTypeLifecycle;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::fleet::settings::RunnerTypeSettings;
use crate::domain::ids::RunnerTypeId;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::error::JobsError;

#[derive(Debug, Clone)]
pub struct RunnerTypeState {
    pub id: RunnerTypeId,
    pub key: RunnerTypeKey,
    pub registered_at: DateTime<Utc>,
    pub lifecycle: RunnerTypeLifecycle,
    pub settings: RunnerTypeSettings,
    pub instances: Vec<RunnerInstance>,
}

#[derive(Debug, Clone)]
pub struct RunnerType {
    id: RunnerTypeId,
    key: RunnerTypeKey,
    registered_at: DateTime<Utc>,
    lifecycle: RunnerTypeLifecycle,
    settings: RunnerTypeSettings,
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
            settings: state.settings,
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

    pub fn settings(&self) -> RunnerTypeSettings {
        self.settings
    }

    pub fn dispatch_mode(&self) -> DispatchMode {
        self.settings.dispatch_mode
    }

    pub fn claimed_since(&self) -> Option<DateTime<Utc>> {
        match self.settings.dispatch_mode {
            DispatchMode::Claim => Some(
                self.settings
                    .dispatch_mode_since
                    .unwrap_or(self.registered_at),
            ),
            DispatchMode::Push => None,
        }
    }

    pub fn own_max_run_duration(&self) -> Option<MaxRunDuration> {
        self.settings.max_run_duration
    }

    pub fn max_run_duration_or(&self, service_default: MaxRunDuration) -> MaxRunDuration {
        self.settings.max_run_duration.unwrap_or(service_default)
    }

    pub fn instance(&self, key: &InstanceKey) -> Option<&RunnerInstance> {
        self.instances.iter().find(|live| live.key() == key)
    }

    pub fn declared_capacity(&self) -> u32 {
        self.instances
            .iter()
            .filter(|live| live.accepts_new_work())
            .map(|live| live.capacity().get())
            .fold(0u32, u32::saturating_add)
    }

    pub fn free_capacity(&self, current_runs: u32) -> u32 {
        self.declared_capacity().saturating_sub(current_runs)
    }

    pub fn is_available(&self) -> bool {
        self.lifecycle.accepts_jobs() && self.instances.iter().any(|live| live.accepts_new_work())
    }

    pub fn published_lifecycle(&self) -> Option<RunnerTypeLifecycle> {
        self.lifecycle.accepts_jobs().then_some(self.lifecycle)
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
            settings: RunnerTypeSettings::default(),
            instances,
        }
    }

    #[test]
    fn availability_is_derived_from_presence_alone() {
        let empty = RunnerType::hydrate(state(vec![])).unwrap();
        let populated = RunnerType::hydrate(state(vec![live("pod-7", 1)])).unwrap();
        assert!(!empty.is_available());
        assert!(populated.is_available());
    }

    #[test]
    fn a_type_whose_every_live_instance_is_draining_is_not_available() {
        let draining = RunnerType::hydrate(state(vec![
            reporting("pod-7", 1, ReportedStatus::Draining),
            reporting("pod-8", 1, ReportedStatus::Draining),
        ]))
        .unwrap();
        assert!(!draining.is_available());
    }

    #[test]
    fn one_ready_instance_among_draining_ones_keeps_the_type_available() {
        let mixed = RunnerType::hydrate(state(vec![
            reporting("pod-7", 1, ReportedStatus::Draining),
            reporting("pod-8", 1, ReportedStatus::Ready),
        ]))
        .unwrap();
        assert!(mixed.is_available());
    }

    fn with_lifecycle(lifecycle: RunnerTypeLifecycle) -> RunnerType {
        let mut state = state(vec![]);
        state.lifecycle = lifecycle;
        RunnerType::hydrate(state).unwrap()
    }

    #[test]
    fn a_type_is_published_exactly_while_it_accepts_jobs() {
        assert_eq!(
            with_lifecycle(RunnerTypeLifecycle::Active).published_lifecycle(),
            Some(RunnerTypeLifecycle::Active)
        );
        assert_eq!(
            with_lifecycle(RunnerTypeLifecycle::Deprecated).published_lifecycle(),
            Some(RunnerTypeLifecycle::Deprecated)
        );
        assert_eq!(
            with_lifecycle(RunnerTypeLifecycle::Retired).published_lifecycle(),
            None
        );
    }

    #[test]
    fn two_instances_sharing_a_key_cannot_be_loaded() {
        let result = RunnerType::hydrate(state(vec![live("pod-7", 1), live("pod-7", 2)]));
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "duplicate_instance_key"
            })
        );
    }

    #[test]
    fn an_instance_older_than_its_runner_type_cannot_be_loaded() {
        let result = RunnerType::hydrate(state(vec![live("pod-7", -30)]));
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "instance_connected_before_registration"
            })
        );
    }
}
