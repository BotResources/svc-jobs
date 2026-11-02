use bc_jobs::domain::job::Job;
use br_core_events::EventMetadata;

use crate::app::{Jobs, integration, write};
use crate::error::ServiceError;

pub(super) async fn queued_again(
    jobs: &Jobs,
    job: &Job,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    write::publish_again(
        &jobs.store,
        jobs.ids.as_ref(),
        vec![integration::queued_again(job)?],
        metadata,
        jobs.clock.now(),
    )
    .await
}
