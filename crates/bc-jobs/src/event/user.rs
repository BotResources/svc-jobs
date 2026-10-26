use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::ids::KnownUserId;
use crate::domain::priority::Priority;
use crate::domain::references::KnownUser;
use crate::error::JobsError;

/// The aggregate a user's maximum priority is recorded under: the KnownUser.
pub const KNOWN_USER_AGGREGATE_TYPE: &str = "KnownUser";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMaxPrioritySet {
    pub user_id: KnownUserId,
    pub max_priority: Priority,
    pub set_by: KnownUser,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMaxPriorityCleared {
    pub user_id: KnownUserId,
    pub cleared_by: KnownUser,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserEvent {
    MaxPrioritySet(UserMaxPrioritySet),
    MaxPriorityCleared(UserMaxPriorityCleared),
}

impl UserEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::MaxPrioritySet(_) => "UserMaxPrioritySet",
            Self::MaxPriorityCleared(_) => "UserMaxPriorityCleared",
        }
    }

    pub fn user_id(&self) -> KnownUserId {
        match self {
            Self::MaxPrioritySet(fact) => fact.user_id,
            Self::MaxPriorityCleared(fact) => fact.user_id,
        }
    }

    /// The user's own maximum once this fact applies, absent when cleared.
    pub fn resulting_maximum(&self) -> Option<Priority> {
        match self {
            Self::MaxPrioritySet(fact) => Some(fact.max_priority),
            Self::MaxPriorityCleared(_) => None,
        }
    }

    pub fn payload(&self) -> Result<Value, JobsError> {
        match self {
            Self::MaxPrioritySet(fact) => serde_json::to_value(fact),
            Self::MaxPriorityCleared(fact) => serde_json::to_value(fact),
        }
        .map_err(JobsError::from)
    }

    pub fn decode(event_type: &str, payload: Value) -> Result<Self, JobsError> {
        match event_type {
            "UserMaxPrioritySet" => Ok(Self::MaxPrioritySet(serde_json::from_value(payload)?)),
            "UserMaxPriorityCleared" => {
                Ok(Self::MaxPriorityCleared(serde_json::from_value(payload)?))
            }
            other => Err(JobsError::UnknownEnumValue {
                field: "user_event_type",
                value: other.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::user;
    use uuid::Uuid;

    #[test]
    fn a_maximum_fact_round_trips_through_its_stored_form() {
        // Given: a super user setting a user's maximum to 4
        let event = UserEvent::MaxPrioritySet(UserMaxPrioritySet {
            user_id: KnownUserId::new(Uuid::now_v7()).unwrap(),
            max_priority: Priority::new(4).unwrap(),
            set_by: user(),
        });
        // When: it is stored and read back
        let decoded = UserEvent::decode(event.event_type(), event.payload().unwrap()).unwrap();
        // Then: it is the same fact, and it says which maximum now applies
        assert_eq!(decoded, event);
        assert_eq!(decoded.resulting_maximum(), Priority::new(4).ok());
    }

    #[test]
    fn a_cleared_maximum_leaves_the_user_on_the_default() {
        // Given: a super user clearing a user's maximum
        let event = UserEvent::MaxPriorityCleared(UserMaxPriorityCleared {
            user_id: KnownUserId::new(Uuid::now_v7()).unwrap(),
            cleared_by: user(),
        });
        // When/Then: no maximum of the user's own remains
        assert_eq!(event.resulting_maximum(), None);
        assert_eq!(event.event_type(), "UserMaxPriorityCleared");
    }
}
