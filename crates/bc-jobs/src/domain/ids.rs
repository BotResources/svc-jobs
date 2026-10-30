use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::JobsError;

macro_rules! uuid_v7_id {
    ($name:ident, $field:literal) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(try_from = "Uuid", into = "Uuid")]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Result<Self, JobsError> {
                if value.get_version_num() == 7 {
                    Ok(Self(value))
                } else {
                    Err(JobsError::NotUuidV7 {
                        field: $field,
                        value,
                    })
                }
            }

            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = JobsError;

            fn try_from(value: Uuid) -> Result<Self, JobsError> {
                Self::new(value)
            }
        }
    };
}

uuid_v7_id!(EventId, "event_id");
uuid_v7_id!(JobId, "job_id");
uuid_v7_id!(RunId, "run_id");
uuid_v7_id!(ResolutionId, "resolution_id");
uuid_v7_id!(ManualRetryId, "manual_retry_id");
uuid_v7_id!(RetryScheduleId, "retry_schedule_id");
uuid_v7_id!(PlanDeclarationId, "plan_declaration_id");
uuid_v7_id!(RunLogId, "run_log_id");
uuid_v7_id!(SourceEntityId, "source_entity_id");
uuid_v7_id!(KnownUserId, "known_user_id");
uuid_v7_id!(PresenceSessionId, "presence_session_id");
uuid_v7_id!(RunnerTypeId, "runner_type_id");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(from = "Uuid", into = "Uuid")]
pub struct AgentId(Uuid);

impl AgentId {
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<AgentId> for Uuid {
    fn from(value: AgentId) -> Self {
        value.0
    }
}

impl From<Uuid> for AgentId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct ClaimId(Uuid);

impl ClaimId {
    pub fn new(value: Uuid) -> Result<Self, JobsError> {
        if value.is_nil() {
            Err(JobsError::BlankValue { field: "claim_id" })
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<ClaimId> for Uuid {
    fn from(value: ClaimId) -> Self {
        value.0
    }
}

impl TryFrom<Uuid> for ClaimId {
    type Error = JobsError;

    fn try_from(value: Uuid) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_that_is_not_uuid_v7_is_refused() {
        let v4 = Uuid::parse_str("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e").unwrap();
        let result = JobId::new(v4);
        assert_eq!(
            result,
            Err(JobsError::NotUuidV7 {
                field: "job_id",
                value: v4
            })
        );
    }

    #[test]
    fn a_uuid_v7_is_accepted_and_round_trips() {
        let raw = Uuid::now_v7();
        let id = JobId::new(raw).unwrap();
        let wire = serde_json::to_value(id).unwrap();
        assert_eq!(wire, serde_json::json!(raw));
        assert_eq!(serde_json::from_value::<JobId>(wire).unwrap(), id);
    }

    #[test]
    fn deserializing_a_non_v7_id_from_the_wire_is_refused() {
        let v4 = serde_json::json!("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e");
        let result = serde_json::from_value::<RunId>(v4);
        assert!(result.is_err());
    }

    #[test]
    fn an_agent_id_is_any_uuid_because_the_runner_makes_it() {
        let v4 = Uuid::parse_str("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e").unwrap();
        assert_eq!(AgentId::new(v4).as_uuid(), v4);
        let wire = serde_json::to_value(AgentId::new(v4)).unwrap();
        assert_eq!(
            serde_json::from_value::<AgentId>(wire).unwrap().as_uuid(),
            v4
        );
    }

    #[test]
    fn a_claim_identity_is_any_uuid_but_the_nil_one() {
        let v4 = Uuid::parse_str("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e").unwrap();
        assert_eq!(ClaimId::new(v4).unwrap().as_uuid(), v4);
        assert_eq!(
            ClaimId::new(Uuid::nil()),
            Err(JobsError::BlankValue { field: "claim_id" })
        );
    }
}
