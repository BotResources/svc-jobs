use bc_jobs::domain::ids::{JobId, ResolutionId, RunId};
use bc_jobs::domain::ownership::CancelRequester;
use bc_jobs::event::job::JobEvent;
use br_core_events::EventMetadata;

use super::{Jobs, resolve};
use crate::error::ServiceError;

pub struct EndedRun {
    pub run_id: RunId,
    pub job_id: JobId,
}

pub fn ended_runs(job_id: JobId, events: &[JobEvent]) -> Vec<EndedRun> {
    events
        .iter()
        .filter_map(|event| match event {
            JobEvent::RunFailed(fact) => Some(fact.run_id),
            JobEvent::RunCancelled(fact) => Some(fact.run_id),
            _ => None,
        })
        .map(|run_id| EndedRun { run_id, job_id })
        .collect()
}

pub async fn cancel_jobs_requested_by(
    jobs: &Jobs,
    ended: &EndedRun,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    for requested in jobs.store.jobs_requested_by_run(ended.run_id).await? {
        let Some(job) = jobs.load(requested).await? else {
            continue;
        };
        if job.is_terminal() {
            continue;
        }
        let outcome = resolve::cancel(
            jobs,
            requested,
            ResolutionId::new(jobs.ids.next())?,
            CancelRequester::Cascade {
                originating_job_id: ended.job_id,
            },
            metadata,
        )
        .await;
        if let Err(error) = outcome {
            tracing::warn!(
                job_id = %requested.as_uuid(),
                run_id = %ended.run_id.as_uuid(),
                error = %error,
                "cancelling a job whose requester run ended failed; the next sweep retries"
            );
        }
    }
    Ok(())
}
