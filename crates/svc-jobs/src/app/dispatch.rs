use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::job::Job;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::DueWorkReader;

use super::write::JobChange;
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

pub async fn dispatch_due_work(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let waiting = DueWorkReader::jobs_awaiting_dispatch(&jobs.store, now).await?;
    for job in waiting {
        if let Err(error) = dispatch_one(jobs, &job).await {
            tracing::warn!(
                job_id = %job.id().as_uuid(),
                error = %error,
                "dispatching a waiting job failed; it stays waiting"
            );
        }
    }
    Ok(())
}

async fn dispatch_one(jobs: &Jobs, job: &Job) -> Result<(), ServiceError> {
    let Some(runner_type) = FleetReader::load(&jobs.store, job.runner_type()).await? else {
        return Ok(());
    };
    runner_type.guard_dispatch()?;
    let result = job.dispatch_run(
        DispatchRun {
            run_id: RunId::new(jobs.ids.next())?,
            at: jobs.clock.now(),
        },
        &jobs.limits,
    )?;
    jobs.commit(
        vec![JobChange::new(job.id(), Some(job.clone()), result.events)],
        &service_metadata(),
    )
    .await
}
