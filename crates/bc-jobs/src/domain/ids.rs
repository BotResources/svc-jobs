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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_that_is_not_uuid_v7_is_refused() {
        // Given: a v4 uuid, which no caller of this service is allowed to mint
        let v4 = Uuid::parse_str("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e").unwrap();
        // When: it is offered as a job id
        let result = JobId::new(v4);
        // Then: construction is refused with the stable code
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
        // Given: a v7 uuid minted by a caller
        let raw = Uuid::now_v7();
        // When: it becomes a job id and is serialized
        let id = JobId::new(raw).unwrap();
        let wire = serde_json::to_value(id).unwrap();
        // Then: the wire form is the bare uuid and it parses back
        assert_eq!(wire, serde_json::json!(raw));
        assert_eq!(serde_json::from_value::<JobId>(wire).unwrap(), id);
    }

    #[test]
    fn deserializing_a_non_v7_id_from_the_wire_is_refused() {
        // Given: a stored value that predates or violates the uuid_v7 domain
        let v4 = serde_json::json!("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e");
        // When: it is read back as a run id
        let result = serde_json::from_value::<RunId>(v4);
        // Then: hydration fails rather than admitting an illegal id
        assert!(result.is_err());
    }
}
