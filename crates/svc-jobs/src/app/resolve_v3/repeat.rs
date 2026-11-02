use bc_jobs::domain::job::Job;
use bc_jobs::event::job::JobEvent;
use br_core_events::EventMetadata;

use crate::app::{Jobs, integration, write};
use crate::error::ServiceError;

pub(super) async fn resolution_again(
    jobs: &Jobs,
    event: &JobEvent,
    job: &Job,
    note: Option<&str>,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let published = integration::resolution_again(event, job, note)?;
    write::publish_again(
        &jobs.store,
        jobs.ids.as_ref(),
        published,
        metadata,
        jobs.clock.now(),
    )
    .await
}
