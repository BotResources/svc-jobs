use async_trait::async_trait;

use crate::domain::ids::KnownUserId;
use crate::domain::priority::{MaxPriorityRevision, UserMaxPriority, UserSelection};
use crate::ports::PortError;

/// A user's stored maximum and the revision it was read at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevisedUserMaxPriority {
    pub maximum: UserMaxPriority,
    pub revision: MaxPriorityRevision,
}

/// The one read path for users' stored maxima outside a write transaction.
/// Writers that must hold the maximum while they decide read it under the
/// user's lock instead, inside their own transaction.
#[async_trait]
pub trait UserMaxPriorityReader: Send + Sync {
    /// The stored maximum of every selected user, Jobs knowing them or not,
    /// with the revision it was read at: one entry per user, in the
    /// selection's order, absent maximum included.
    async fn load_selected(
        &self,
        users: &UserSelection,
    ) -> Result<Vec<RevisedUserMaxPriority>, PortError>;

    /// The stored maximum of one user, Jobs knowing them or not.
    async fn load_one(&self, user_id: KnownUserId) -> Result<UserMaxPriority, PortError> {
        let selected = self.load_selected(&UserSelection::one(user_id)).await?;
        Ok(selected.first().map_or_else(
            || UserMaxPriority::new(user_id, None),
            |found| found.maximum,
        ))
    }
}
