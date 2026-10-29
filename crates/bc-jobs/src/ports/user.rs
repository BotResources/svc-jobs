use async_trait::async_trait;

use crate::domain::priority::{UserMaxPriority, UserSelection};
use crate::ports::PortError;

#[async_trait]
pub trait UserMaxPriorityReader: Send + Sync {
    /// The stored maximum of every selected user, Jobs knowing them or not:
    /// one entry per user, in the selection's order, absent maximum included.
    async fn load_selected(&self, users: &UserSelection)
    -> Result<Vec<UserMaxPriority>, PortError>;
}
