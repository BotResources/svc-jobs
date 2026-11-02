use bc_jobs::ports::job::DUE_WORK_BATCH;

use crate::app::Jobs;
use crate::error::ServiceError;

pub(super) async fn erase_cancelled_request_data(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let retention = jobs.limits.retention();
    let batch = i64::try_from(DUE_WORK_BATCH).unwrap_or(i64::MAX);
    let awaiting = jobs
        .store
        .cancelled_jobs_awaiting_erasure(now - retention.cancelled(), batch)
        .await?;
    for job in awaiting {
        if job.request_data_erasure_due(retention, now) || job.has_request_data_erased() {
            jobs.store
                .erase_request_data_of(job.id().as_uuid(), now)
                .await?;
        }
    }
    Ok(())
}
