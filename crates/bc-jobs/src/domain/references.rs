use br_core_events::UserId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::ids::{KnownUserId, SourceEntityId};
use crate::domain::keys::{DisplayName, ProducerKey};
use crate::error::JobsError;

/// A user Jobs works for or records an act of. Its display name is the one
/// the fact states; a user Jobs was told only the id of has none
/// (`KnownUser::id_only`), and shows its id until a fact names it.
///
/// Stored and serialized, a user known only by id carries its id as its name:
/// the form every earlier version wrote and reads (`stored_name`,
/// `from_stored`). That boundary is the only place the id text stands for "no
/// name"; inside the domain the case is typed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "StoredKnownUser", into = "StoredKnownUser")]
pub struct KnownUser {
    id: KnownUserId,
    display_name: Option<DisplayName>,
}

impl KnownUser {
    /// A user named by a fact. A name that is the user's own id text names
    /// nothing: the user is known only by id.
    pub fn new(id: UserId, display_name: DisplayName) -> Result<Self, JobsError> {
        if is_id_text(id.0, display_name.as_str()) {
            return Self::id_only(id);
        }
        Ok(Self {
            id: KnownUserId::new(id.0)?,
            display_name: Some(display_name),
        })
    }

    /// A user Jobs was told only the id of.
    pub fn id_only(id: UserId) -> Result<Self, JobsError> {
        Ok(Self {
            id: KnownUserId::new(id.0)?,
            display_name: None,
        })
    }

    /// A user as it was stored: by name, or by its id text when known only by id.
    pub fn from_stored(id: UserId, stored_name: &str) -> Result<Self, JobsError> {
        if is_id_text(id.0, stored_name) {
            return Self::id_only(id);
        }
        Self::new(id, DisplayName::new(stored_name)?)
    }

    pub fn id(&self) -> UserId {
        UserId(self.id.as_uuid())
    }

    /// The display name this fact states, `None` for a user known only by id.
    pub fn display_name(&self) -> Option<&DisplayName> {
        self.display_name.as_ref()
    }

    /// The name as it is stored: the display name, or the id text for a user
    /// known only by id.
    pub fn stored_name(&self) -> String {
        self.display_name.as_ref().map_or_else(
            || self.id.as_uuid().to_string(),
            |name| name.as_str().to_owned(),
        )
    }

    /// What a view shows for the user: its display name, or its id when Jobs
    /// knows no name.
    pub fn shown_name(&self) -> String {
        self.stored_name()
    }

    /// The same user, without the name: what a fact that names no user carries.
    pub fn without_name(&self) -> Self {
        Self {
            id: self.id,
            display_name: None,
        }
    }
}

fn is_id_text(id: Uuid, text: &str) -> bool {
    Uuid::parse_str(text.trim()) == Ok(id)
}

/// The serialized form of a KnownUser, the one 0.9.0 and earlier read: the id
/// and always a name, the id text for a user known only by id. A null or
/// missing name reads as known only by id.
#[derive(Serialize, Deserialize)]
struct StoredKnownUser {
    id: Uuid,
    #[serde(default)]
    display_name: Option<String>,
}

impl TryFrom<StoredKnownUser> for KnownUser {
    type Error = JobsError;

    fn try_from(stored: StoredKnownUser) -> Result<Self, Self::Error> {
        match stored.display_name {
            Some(name) => Self::from_stored(UserId(stored.id), &name),
            None => Self::id_only(UserId(stored.id)),
        }
    }
}

impl From<KnownUser> for StoredKnownUser {
    fn from(user: KnownUser) -> Self {
        Self {
            id: user.id.as_uuid(),
            display_name: Some(user.stored_name()),
        }
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
    fn a_user_known_only_by_id_has_no_name_and_shows_its_id() {
        // Given: a user Jobs was told only the id of
        let id = UserId(Uuid::now_v7());
        let user = KnownUser::id_only(id).unwrap();
        // Then: it states no name, and a view shows its id
        assert_eq!(user.display_name(), None);
        assert_eq!(user.shown_name(), id.0.to_string());
        let named = KnownUser::new(id, DisplayName::new("Ada").unwrap()).unwrap();
        assert_eq!(named.shown_name(), "Ada");
        assert_eq!(named.without_name(), user);
    }

    #[test]
    fn a_known_user_stored_before_the_id_only_case_reads_with_its_name() {
        // Given: a KnownUser as 0.9.0 serialized it, always named
        let id = Uuid::now_v7();
        let stored = serde_json::json!({ "id": id, "display_name": "Alice (trial)" });
        // When: it is read back
        let read: KnownUser = serde_json::from_value(stored.clone()).unwrap();
        // Then: it is the same named user, written in the same shape
        assert_eq!(
            read,
            KnownUser::new(UserId(id), DisplayName::new("Alice (trial)").unwrap()).unwrap()
        );
        assert_eq!(serde_json::to_value(&read).unwrap(), stored);
    }

    #[test]
    fn a_user_known_only_by_id_is_written_with_its_id_as_name_and_read_back_by_id() {
        let user = KnownUser::id_only(UserId(Uuid::now_v7())).unwrap();
        let wire = serde_json::to_value(&user).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({ "id": user.id().0, "display_name": user.id().0.to_string() }),
            "the shape every earlier version reads"
        );
        assert_eq!(serde_json::from_value::<KnownUser>(wire).unwrap(), user);
        for unnamed in [
            serde_json::json!({ "id": user.id().0 }),
            serde_json::json!({ "id": user.id().0, "display_name": null }),
        ] {
            assert_eq!(serde_json::from_value::<KnownUser>(unnamed).unwrap(), user);
        }
    }

    #[test]
    fn a_name_that_is_the_users_own_id_names_nothing() {
        let id = UserId(Uuid::now_v7());
        let by_id = DisplayName::new(id.0.to_string()).unwrap();
        assert_eq!(
            KnownUser::new(id, by_id).unwrap(),
            KnownUser::id_only(id).unwrap()
        );
        assert_eq!(
            KnownUser::from_stored(id, &id.0.to_string()).unwrap(),
            KnownUser::id_only(id).unwrap()
        );
        let another_id = Uuid::now_v7().to_string();
        let named = KnownUser::from_stored(id, &another_id).unwrap();
        assert_eq!(
            named.display_name().map(DisplayName::as_str),
            Some(another_id.as_str())
        );
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
