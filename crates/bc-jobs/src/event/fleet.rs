use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::status::ReportedStatus;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey, RunnerVersion};
use crate::error::JobsError;

pub const RUNNER_TYPE_AGGREGATE_TYPE: &str = "RunnerType";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeRegistered {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
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
pub struct RunnerTypeAffordancesChanged {
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
    RunnerTypeAffordancesChanged(RunnerTypeAffordancesChanged),
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
            Self::RunnerTypeAffordancesChanged(_) => "RunnerTypeAffordancesChanged",
            Self::InstanceConnected(_) => "InstanceConnected",
            Self::InstanceStatusReported(_) => "InstanceStatusReported",
            Self::InstanceDisconnected(_) => "InstanceDisconnected",
        }
    }

    pub fn runner_type_id(&self) -> RunnerTypeId {
        match self {
            Self::RunnerTypeRegistered(fact) => fact.runner_type_id,
            Self::RunnerTypeDeprecated(fact) => fact.runner_type_id,
            Self::RunnerTypeReactivated(fact) => fact.runner_type_id,
            Self::RunnerTypeRetired(fact) => fact.runner_type_id,
            Self::RunnerTypeAffordancesChanged(fact) => fact.runner_type_id,
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
            "RunnerTypeAffordancesChanged" => {
                Self::RunnerTypeAffordancesChanged(serde_json::from_value(payload)?)
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
            Self::RunnerTypeAffordancesChanged(fact) => &fact.runner_type,
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
            Self::RunnerTypeAffordancesChanged(_) => None,
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
            Self::RunnerTypeAffordancesChanged(fact) => serde_json::to_value(fact),
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
}
