use bc_jobs::ports::auto_finish::AutoFinishReader;
use bc_jobs::ports::job::DUE_WORK_BATCH;

use crate::app::auto_finish::finish_called_job;
use crate::app::{Jobs, service_metadata};
use crate::error::ServiceError;

pub(super) async fn finish_called_jobs(jobs: &Jobs) -> Result<(), ServiceError> {
    let mut after = None;
    let mut first_failure = None;
    loop {
        let batch = AutoFinishReader::candidates_after(&jobs.store, after, DUE_WORK_BATCH).await?;
        let Some(last) = batch.last().copied() else {
            break;
        };
        after = Some(last);
        let reached_the_end = batch.len() < DUE_WORK_BATCH;
        for called in batch {
            if let Err(error) = finish_called_job(jobs, called, &service_metadata()).await {
                tracing::warn!(
                    job_id = %called.as_uuid(),
                    error = %error,
                    "finishing a called job failed; the other jobs go on and the next sweep decides again"
                );
                first_failure.get_or_insert(error);
            }
        }
        if reached_the_end {
            break;
        }
    }
    first_failure.map_or(Ok(()), Err)
}
