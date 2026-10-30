use bc_jobs::domain::ids::AgentId;
use uuid::Uuid;

use super::{Jobs, Outcome, WorkIds, started};
use bc_jobs::domain::mail::views::UnreadItem;
use bc_jobs::ports::inbox::Inbox;

pub async fn unread(
    jobs: &Jobs,
    work: &WorkIds,
    address: Option<Uuid>,
) -> Outcome<Vec<UnreadItem>> {
    started(jobs, work).await?;
    Ok(jobs
        .store
        .unread(work.named(), address.map(AgentId::new))
        .await?)
}
