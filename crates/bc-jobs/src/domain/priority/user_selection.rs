use std::collections::HashSet;

use uuid::Uuid;

use crate::domain::ids::KnownUserId;
use crate::error::JobsError;

/// The users one read of maximum priorities answers for: valid user ids, each
/// once, in the order first asked for, and never more than one read can hold.
/// The read is addressed by ids — there is no listing of every user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSelection(Vec<KnownUserId>);

impl UserSelection {
    pub const MAXIMUM: usize = 200;

    pub fn new(requested: &[Uuid]) -> Result<Self, JobsError> {
        let mut seen: HashSet<KnownUserId> = HashSet::with_capacity(requested.len());
        let mut users: Vec<KnownUserId> = Vec::new();
        for id in requested {
            let user_id = KnownUserId::new(*id)?;
            if seen.insert(user_id) {
                users.push(user_id);
            }
        }
        if users.len() > Self::MAXIMUM {
            return Err(JobsError::ValueTooLong {
                field: "user_ids",
                length: users.len(),
                maximum: Self::MAXIMUM,
            });
        }
        Ok(Self(users))
    }

    pub fn ids(&self) -> &[KnownUserId] {
        &self.0
    }

    pub fn contains(&self, user_id: KnownUserId) -> bool {
        self.0.contains(&user_id)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_user_is_answered_once_in_the_order_first_asked_for() {
        // Given: two users, each asked for twice
        let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
        // When: the selection is made
        let selection = UserSelection::new(&[b, a, b, a]).unwrap();
        // Then: b then a, once each
        let ids: Vec<Uuid> = selection.ids().iter().map(KnownUserId::as_uuid).collect();
        assert_eq!(ids, vec![b, a]);
    }

    #[test]
    fn an_id_that_is_not_a_uuid_v7_refuses_the_whole_selection() {
        // Given: one valid id and one UUIDv4
        let v4 = Uuid::from_u128(0x0123_4567_89ab_4def_8123_4567_89ab_cdef);
        // When/Then: the selection is refused on the offending value
        assert_eq!(
            UserSelection::new(&[Uuid::now_v7(), v4]),
            Err(JobsError::NotUuidV7 {
                field: "known_user_id",
                value: v4
            })
        );
    }

    #[test]
    fn a_selection_is_bounded_and_repeats_do_not_count_against_the_bound() {
        // Given: as many distinct users as one read holds, then one more
        let full: Vec<Uuid> = (0..UserSelection::MAXIMUM)
            .map(|_| Uuid::now_v7())
            .collect();
        let mut over = full.clone();
        over.push(Uuid::now_v7());
        let mut repeated = full.clone();
        repeated.push(full[0]);
        // When/Then: the bound holds, one more is refused, a repeat is not one more
        assert_eq!(
            UserSelection::new(&full).unwrap().ids().len(),
            UserSelection::MAXIMUM
        );
        assert_eq!(
            UserSelection::new(&over),
            Err(JobsError::ValueTooLong {
                field: "user_ids",
                length: UserSelection::MAXIMUM + 1,
                maximum: UserSelection::MAXIMUM,
            })
        );
        assert!(UserSelection::new(&repeated).is_ok());
    }

    #[test]
    fn nobody_asked_for_is_an_empty_selection() {
        // Given/When/Then: an empty request selects nobody
        assert!(UserSelection::new(&[]).unwrap().is_empty());
    }
}
