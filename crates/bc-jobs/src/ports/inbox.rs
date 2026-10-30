use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::PortError;
use super::mail::Stored;
use crate::domain::ids::AgentId;
use crate::domain::mail::views::UnreadItem;
use crate::domain::runner::work::{MarkReadRefusal, WorkNamed};

#[async_trait]
pub trait Inbox: Send + Sync {
    async fn unread(
        &self,
        work: WorkNamed<'_>,
        address: Option<AgentId>,
    ) -> Stored<Vec<UnreadItem>>;

    async fn mark_read(
        &self,
        run_id: Uuid,
        instance_key: &str,
        runner_type: &str,
        item_ids: &[Uuid],
        at: DateTime<Utc>,
    ) -> Result<Result<(), MarkReadRefusal>, PortError>;
}
