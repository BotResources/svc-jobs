use std::sync::atomic::{AtomicUsize, Ordering};

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::policy::Retention;
use bc_jobs::ports::PortError;
use bc_jobs::ports::job::DUE_WORK_BATCH;
use bc_jobs::ports::scrub::{ScrubOutcome, TreeScrub, TreeScrubs};
use chrono::{DateTime, Utc};

use super::each::each_job;
use super::observability;
use crate::app::{Jobs, service_metadata};
use crate::error::ServiceError;

pub(super) async fn scrub_due_trees(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let retention = jobs.limits.retention();
    let due_before = retention.scrub_due_before(now);
    let mut after = None;
    let mut scrubbed = 0;
    let mut first_failure = None;
    while scrubbed < DUE_WORK_BATCH {
        let page = jobs
            .store
            .trees_due_for_scrub(due_before, DUE_WORK_BATCH, after)
            .await?;
        let Some(last) = page.last().copied() else {
            break;
        };
        let full = page.len() >= DUE_WORK_BATCH;
        let done = AtomicUsize::new(0);
        let ids = page.iter().map(|tree| tree.initial_job_id).collect();
        let outcome = each_job(ids, "scrubbing a Tree", |initial_job_id| {
            let done = &done;
            async move {
                scrub_one(jobs, initial_job_id, retention, now, done)
                    .await
                    .inspect_err(|_| observability::scrub_failed())
            }
        })
        .await;
        scrubbed += done.load(Ordering::Relaxed);
        if let Err(error) = outcome {
            first_failure.get_or_insert(error);
        }
        if !full {
            break;
        }
        after = Some(last);
    }
    first_failure.map_or(Ok(()), Err)
}

async fn scrub_one(
    jobs: &Jobs,
    initial_job_id: JobId,
    retention: Retention,
    now: DateTime<Utc>,
    done: &AtomicUsize,
) -> Result<(), ServiceError> {
    let metadata = service_metadata();
    let outcome = match jobs
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
        .await
    {
        Err(PortError::TreeLockTimeout) => {
            tracing::info!(
                initial_job_id = %initial_job_id.as_uuid(),
                "a Tree is held by another writer; the next sweep looks again"
            );
            return Ok(());
        }
        other => other?,
    };
    match outcome {
        ScrubOutcome::Scrubbed { jobs: scrubbed } => {
            done.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                initial_job_id = %initial_job_id.as_uuid(),
                jobs = scrubbed,
                "a Tree was scrubbed"
            );
        }
        ScrubOutcome::Standing(standing) => tracing::debug!(
            initial_job_id = %initial_job_id.as_uuid(),
            ?standing,
            "a Tree was not scrubbed: it changed since the scan"
        ),
    }
    Ok(())
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
