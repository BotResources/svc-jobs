use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::dispatch_mode::DispatchMode;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::fleet::status::ReportedStatus;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey, RunnerVersion};
use crate::error::JobsError;

pub const RUNNER_TYPE_AGGREGATE_TYPE: &str = "RunnerType";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeRegistered {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    #[serde(default)]
    pub dispatch_mode: DispatchMode,
    #[serde(default = "service_default_duration")]
    pub max_run_duration: MaxRunDuration,
}

/// What a registration recorded before RunnerTypes carried settings reads as.
fn service_default_duration() -> MaxRunDuration {
    MaxRunDuration::SERVICE_DEFAULT
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeDispatchModeChanged {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    pub dispatch_mode: DispatchMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeMaxRunDurationChanged {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    pub max_run_duration: MaxRunDuration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeDeprecated {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeReactivated {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeRetired {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeBecameRetirable {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceConnected {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
    pub session_id: PresenceSessionId,
    pub version: RunnerVersion,
    pub reported_status: ReportedStatus,
    pub capacity: Capacity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceStatusReported {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
    pub session_id: PresenceSessionId,
    pub version: RunnerVersion,
    pub reported_status: ReportedStatus,
    pub capacity: Capacity,
    pub change_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceDisconnected {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
    pub session_id: PresenceSessionId,
    pub reason_code: ReasonCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetEvent {
    RunnerTypeRegistered(RunnerTypeRegistered),
    RunnerTypeDeprecated(RunnerTypeDeprecated),
    RunnerTypeReactivated(RunnerTypeReactivated),
    RunnerTypeRetired(RunnerTypeRetired),
    RunnerTypeBecameRetirable(RunnerTypeBecameRetirable),
    RunnerTypeDispatchModeChanged(RunnerTypeDispatchModeChanged),
    RunnerTypeMaxRunDurationChanged(RunnerTypeMaxRunDurationChanged),
    InstanceConnected(InstanceConnected),
    InstanceStatusReported(InstanceStatusReported),
    InstanceDisconnected(InstanceDisconnected),
}

impl FleetEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::RunnerTypeRegistered(_) => "RunnerTypeRegistered",
            Self::RunnerTypeDeprecated(_) => "RunnerTypeDeprecated",
            Self::RunnerTypeReactivated(_) => "RunnerTypeReactivated",
            Self::RunnerTypeRetired(_) => "RunnerTypeRetired",
            Self::RunnerTypeBecameRetirable(_) => "RunnerTypeBecameRetirable",
            Self::RunnerTypeDispatchModeChanged(_) => "RunnerTypeDispatchModeChanged",
            Self::RunnerTypeMaxRunDurationChanged(_) => "RunnerTypeMaxRunDurationChanged",
            Self::InstanceConnected(_) => "InstanceConnected",
            Self::InstanceStatusReported(_) => "InstanceStatusReported",
            Self::InstanceDisconnected(_) => "InstanceDisconnected",
        }
    }

    pub fn changes_published_catalog(&self) -> bool {
        match self {
            Self::RunnerTypeRegistered(_)
            | Self::RunnerTypeDeprecated(_)
            | Self::RunnerTypeReactivated(_)
            | Self::RunnerTypeRetired(_) => true,
            Self::RunnerTypeBecameRetirable(_)
            | Self::RunnerTypeDispatchModeChanged(_)
            | Self::RunnerTypeMaxRunDurationChanged(_)
            | Self::InstanceConnected(_)
            | Self::InstanceStatusReported(_)
            | Self::InstanceDisconnected(_) => false,
        }
    }

    pub fn runner_type_id(&self) -> RunnerTypeId {
        match self {
            Self::RunnerTypeRegistered(fact) => fact.runner_type_id,
            Self::RunnerTypeDeprecated(fact) => fact.runner_type_id,
            Self::RunnerTypeReactivated(fact) => fact.runner_type_id,
            Self::RunnerTypeRetired(fact) => fact.runner_type_id,
            Self::RunnerTypeBecameRetirable(fact) => fact.runner_type_id,
            Self::RunnerTypeDispatchModeChanged(fact) => fact.runner_type_id,
            Self::RunnerTypeMaxRunDurationChanged(fact) => fact.runner_type_id,
            Self::InstanceConnected(fact) => fact.runner_type_id,
            Self::InstanceStatusReported(fact) => fact.runner_type_id,
            Self::InstanceDisconnected(fact) => fact.runner_type_id,
        }
    }

    pub fn decode(event_type: &str, payload: Value) -> Result<Self, JobsError> {
        let decoded = match event_type {
            "RunnerTypeRegistered" => Self::RunnerTypeRegistered(serde_json::from_value(payload)?),
            "RunnerTypeDeprecated" => Self::RunnerTypeDeprecated(serde_json::from_value(payload)?),
            "RunnerTypeReactivated" => {
                Self::RunnerTypeReactivated(serde_json::from_value(payload)?)
            }
            "RunnerTypeRetired" => Self::RunnerTypeRetired(serde_json::from_value(payload)?),
            "RunnerTypeBecameRetirable" => {
                Self::RunnerTypeBecameRetirable(serde_json::from_value(payload)?)
            }
            "RunnerTypeDispatchModeChanged" => {
                Self::RunnerTypeDispatchModeChanged(serde_json::from_value(payload)?)
            }
            "RunnerTypeMaxRunDurationChanged" => {
                Self::RunnerTypeMaxRunDurationChanged(serde_json::from_value(payload)?)
            }
            "InstanceConnected" => Self::InstanceConnected(serde_json::from_value(payload)?),
            "InstanceStatusReported" => {
                Self::InstanceStatusReported(serde_json::from_value(payload)?)
            }
            "InstanceDisconnected" => Self::InstanceDisconnected(serde_json::from_value(payload)?),
            other => {
                return Err(JobsError::UnknownEnumValue {
                    field: "fleet_event_type",
                    value: other.to_owned(),
                });
            }
        };
        Ok(decoded)
    }

    pub fn runner_type(&self) -> &RunnerTypeKey {
        match self {
            Self::RunnerTypeRegistered(fact) => &fact.runner_type,
            Self::RunnerTypeDeprecated(fact) => &fact.runner_type,
            Self::RunnerTypeReactivated(fact) => &fact.runner_type,
            Self::RunnerTypeRetired(fact) => &fact.runner_type,
            Self::RunnerTypeBecameRetirable(fact) => &fact.runner_type,
            Self::RunnerTypeDispatchModeChanged(fact) => &fact.runner_type,
            Self::RunnerTypeMaxRunDurationChanged(fact) => &fact.runner_type,
            Self::InstanceConnected(fact) => &fact.runner_type,
            Self::InstanceStatusReported(fact) => &fact.runner_type,
            Self::InstanceDisconnected(fact) => &fact.runner_type,
        }
    }

    pub fn instance_key(&self) -> Option<&InstanceKey> {
        match self {
            Self::RunnerTypeRegistered(_) => None,
            Self::RunnerTypeDeprecated(_) => None,
            Self::RunnerTypeReactivated(_) => None,
            Self::RunnerTypeRetired(_) => None,
            Self::RunnerTypeBecameRetirable(_) => None,
            Self::RunnerTypeDispatchModeChanged(_) => None,
            Self::RunnerTypeMaxRunDurationChanged(_) => None,
            Self::InstanceConnected(fact) => Some(&fact.instance_key),
            Self::InstanceStatusReported(fact) => Some(&fact.instance_key),
            Self::InstanceDisconnected(fact) => Some(&fact.instance_key),
        }
    }

    pub fn payload(&self) -> Result<Value, JobsError> {
        match self {
            Self::RunnerTypeRegistered(fact) => serde_json::to_value(fact),
            Self::RunnerTypeDeprecated(fact) => serde_json::to_value(fact),
            Self::RunnerTypeReactivated(fact) => serde_json::to_value(fact),
            Self::RunnerTypeRetired(fact) => serde_json::to_value(fact),
            Self::RunnerTypeBecameRetirable(fact) => serde_json::to_value(fact),
            Self::RunnerTypeDispatchModeChanged(fact) => serde_json::to_value(fact),
            Self::RunnerTypeMaxRunDurationChanged(fact) => serde_json::to_value(fact),
            Self::InstanceConnected(fact) => serde_json::to_value(fact),
            Self::InstanceStatusReported(fact) => serde_json::to_value(fact),
            Self::InstanceDisconnected(fact) => serde_json::to_value(fact),
        }
        .map_err(JobsError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn a_disconnection_carries_the_session_it_closes_and_why() {
        // Given: an instance whose presence entry expired
        let event = FleetEvent::InstanceDisconnected(InstanceDisconnected {
            runner_type_id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            instance_key: InstanceKey::new("pod-7").unwrap(),
            session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            reason_code: ReasonCode::new("presence_expired").unwrap(),
        });
        // When: the fact is rendered
        let payload = event.payload().unwrap();
        // Then: a subscriber learns which session ended and why, without a query
        assert_eq!(event.event_type(), "InstanceDisconnected");
        assert_eq!(payload["reason_code"], "presence_expired");
        assert_eq!(payload["instance_key"], "pod-7");
    }

    #[test]
    fn only_a_lifecycle_fact_moves_the_published_catalog() {
        // Given: an instance reporting its status, and the type being deprecated
        let reported = FleetEvent::InstanceStatusReported(InstanceStatusReported {
            runner_type_id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            instance_key: InstanceKey::new("pod-7").unwrap(),
            session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            version: RunnerVersion::new("1.4.2").unwrap(),
            reported_status: ReportedStatus::Draining,
            capacity: Capacity::new(1).unwrap(),
            change_number: 3,
        });
        let deprecated = FleetEvent::RunnerTypeDeprecated(RunnerTypeDeprecated {
            runner_type_id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
        });
        // Then: presence traffic never reaches the bucket, a lifecycle change always does
        assert!(!reported.changes_published_catalog());
        assert!(deprecated.changes_published_catalog());
    }

    #[test]
    fn a_registration_recorded_before_settings_existed_reads_as_pushed_with_the_default() {
        // Given: a registration payload written by a previous version
        let stored = serde_json::json!({
            "runner_type_id": Uuid::now_v7(),
            "runner_type": "analyst",
        });
        // When: it is decoded
        let decoded = FleetEvent::decode("RunnerTypeRegistered", stored).unwrap();
        // Then: it carries the settings every type had then
        match decoded {
            FleetEvent::RunnerTypeRegistered(fact) => {
                assert_eq!(fact.dispatch_mode, DispatchMode::Push);
                assert_eq!(fact.max_run_duration, MaxRunDuration::SERVICE_DEFAULT);
            }
            other => panic!("expected a registration, got {other:?}"),
        }
    }

    #[test]
    fn a_settings_change_never_moves_the_published_catalog() {
        // Given: a dispatch-mode change
        let changed = FleetEvent::RunnerTypeDispatchModeChanged(RunnerTypeDispatchModeChanged {
            runner_type_id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            dispatch_mode: DispatchMode::Claim,
        });
        // When/Then: the catalog publishes lifecycles only
        assert!(!changed.changes_published_catalog());
        assert_eq!(changed.payload().unwrap()["dispatch_mode"], "CLAIM");
    }
}
