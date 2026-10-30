use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::PortError;
use bc_jobs::ports::directory::{ListedEntry, TreeDirectory};

use super::Jobs;

pub async fn listed_agents_of_tree(
    jobs: &Jobs,
    job_id: JobId,
) -> Result<Option<Vec<ListedEntry>>, PortError> {
    Ok(jobs.store.entries_of_tree(job_id).await?.map(|entries| {
        entries
            .into_iter()
            .filter(|row| row.entry.is_listed())
            .collect()
    }))
}
