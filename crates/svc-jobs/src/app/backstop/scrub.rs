use bc_jobs::ports::job::DUE_WORK_BATCH;
use bc_jobs::ports::scrub::{ScrubOutcome, TreeScrub, TreeScrubs};

use super::each::each_job;
use crate::app::{Jobs, service_metadata};
use crate::error::ServiceError;

pub(super) async fn scrub_due_trees(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let retention = jobs.limits.retention();
    let due = jobs
        .store
        .trees_due_for_scrub(retention.scrub_due_before(now), DUE_WORK_BATCH)
        .await?;
    each_job(due, "scrubbing a Tree", |initial_job_id| async move {
        let metadata = service_metadata();
        let outcome = jobs
            .store
            .scrub_tree(
                initial_job_id,
                TreeScrub {
                    retention,
                    at: now,
                    ids: jobs.ids.as_ref(),
                    metadata: &metadata,
                },
            )
            .await?;
        match outcome {
            ScrubOutcome::Scrubbed { jobs: scrubbed } => tracing::info!(
                initial_job_id = %initial_job_id.as_uuid(),
                jobs = scrubbed,
                "a Tree was scrubbed"
            ),
            ScrubOutcome::Standing(standing) => tracing::debug!(
                initial_job_id = %initial_job_id.as_uuid(),
                ?standing,
                "a Tree was not scrubbed: it changed since the scan"
            ),
        }
        Ok(())
    })
    .await
}

pub(super) async fn erase_orphan_uploads(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let erased = jobs
        .store
        .erase_orphan_uploads(
            jobs.limits.retention().scrub_due_before(now),
            DUE_WORK_BATCH,
        )
        .await?;
    if erased.payloads + erased.attachments > 0 {
        tracing::info!(
            payloads = erased.payloads,
            attachments = erased.attachments,
            "uploads that no Job or mail ever used were erased"
        );
    }
    Ok(())
}
