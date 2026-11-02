use bc_jobs::domain::ids::EventId;
use bc_jobs::ports::job::{CancelledRequestData, ContentErasure, DUE_WORK_BATCH};

use super::each::each_job;
use crate::app::{Jobs, service_metadata};
use crate::error::ServiceError;

pub(super) async fn erase_cancelled_request_data(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let due_before = jobs
        .limits
        .retention()
        .cancelled_request_data_due_before(now);
    let awaiting = jobs
        .store
        .cancelled_jobs_awaiting_erasure(due_before, DUE_WORK_BATCH)
        .await?;
    each_job(
        awaiting,
        "erasing the request data of a cancelled Job",
        |job_id| async move {
            let metadata = service_metadata();
            let event_id = EventId::new(jobs.ids.next())?;
            jobs.store
                .erase_request_data_of(
                    job_id,
                    ContentErasure {
                        at: now,
                        event_id,
                        metadata: &metadata,
                    },
                )
                .await
                .map(drop)
                .map_err(ServiceError::from)
        },
    )
    .await
}
