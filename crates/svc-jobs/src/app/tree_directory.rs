use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::directory::{ListedEntry, TreeDirectory};

use super::Jobs;
use crate::error::ServiceError;

pub async fn listed_agents_of_tree(
    jobs: &Jobs,
    job_id: JobId,
) -> Result<Vec<ListedEntry>, ServiceError> {
    let entries = jobs
        .store
        .entries_of_tree(job_id)
        .await?
        .ok_or(ServiceError::JobNotFound)?;
    Ok(entries
        .into_iter()
        .filter(|row| row.entry.is_listed())
        .collect())
}
