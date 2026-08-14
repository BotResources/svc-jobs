use br_core_events::UserId;
use serde::{Deserialize, Serialize};

use crate::domain::ids::{KnownUserId, SourceEntityId};
use crate::domain::keys::{DisplayName, ProducerKey};
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownUser {
    id: KnownUserId,
    display_name: DisplayName,
}

impl KnownUser {
    pub fn new(id: UserId, display_name: DisplayName) -> Result<Self, JobsError> {
        Ok(Self {
            id: KnownUserId::new(id.0)?,
            display_name,
        })
    }

    pub fn id(&self) -> UserId {
        UserId(self.id.as_uuid())
    }

    pub fn display_name(&self) -> &DisplayName {
        &self.display_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceReference {
    bc: ProducerKey,
    entity_id: SourceEntityId,
}

impl SourceReference {
    pub fn new(bc: ProducerKey, entity_id: SourceEntityId) -> Self {
        Self { bc, entity_id }
    }

    pub fn bc(&self) -> &ProducerKey {
        &self.bc
    }

    pub fn entity_id(&self) -> SourceEntityId {
        self.entity_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn a_known_user_id_must_be_uuid_v7() {
        // Given: a legacy v4 identity id
        let v4 = Uuid::parse_str("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e").unwrap();
        // When: it is projected into this context
        let result = KnownUser::new(UserId(v4), DisplayName::new("Ada").unwrap());
        // Then: the projection is refused
        assert!(matches!(result, Err(JobsError::NotUuidV7 { .. })));
    }

    #[test]
    fn deserializing_a_non_v7_known_user_is_refused() {
        // Given: a persisted event payload naming a v4 identity id
        let wire = serde_json::json!({
            "id": "6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e",
            "display_name": "Ada"
        });
        // When: the fact is read back off the wire
        let result = serde_json::from_value::<KnownUser>(wire);
        // Then: hydration fails rather than resurrecting an illegal id
        assert!(result.is_err());
    }

    #[test]
    fn a_known_user_round_trips_through_the_wire() {
        // Given: a projected identity minted as uuid v7
        let user =
            KnownUser::new(UserId(Uuid::now_v7()), DisplayName::new("Ada").unwrap()).unwrap();
        // When: it is serialized and read back
        let wire = serde_json::to_value(&user).unwrap();
        // Then: the id travels as the bare uuid and survives the round trip
        assert_eq!(wire["id"], serde_json::json!(user.id().0));
        assert_eq!(serde_json::from_value::<KnownUser>(wire).unwrap(), user);
    }

    #[test]
    fn a_source_reference_pairs_the_producer_with_its_entity() {
        // Given: a producer bounded context and one of its entity ids
        let bc = ProducerKey::new("projects").unwrap();
        let entity = SourceEntityId::new(Uuid::now_v7()).unwrap();
        // When: the reference is formed
        let reference = SourceReference::new(bc.clone(), entity);
        // Then: both halves stay addressable — the pair is the natural key
        assert_eq!(reference.bc(), &bc);
        assert_eq!(reference.entity_id(), entity);
    }
}
