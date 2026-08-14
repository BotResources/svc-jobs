use br_core_events::UserId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::ids::SourceEntityId;
use crate::domain::keys::{DisplayName, ProducerKey};
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownUser {
    id: Uuid,
    display_name: DisplayName,
}

impl KnownUser {
    pub fn new(id: UserId, display_name: DisplayName) -> Result<Self, JobsError> {
        if id.0.get_version_num() == 7 {
            Ok(Self {
                id: id.0,
                display_name,
            })
        } else {
            Err(JobsError::NotUuidV7 {
                field: "known_user_id",
                value: id.0,
            })
        }
    }

    pub fn id(&self) -> UserId {
        UserId(self.id)
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
