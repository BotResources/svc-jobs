use std::collections::HashMap;

use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::domain::fleet::RunnerType;
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
    if waiting.is_empty() {
        return Ok(());
    }
    let fleet: HashMap<String, RunnerType> = FleetReader::load_all(&jobs.store)
        .await?
        .into_iter()
        .map(|runner_type| (runner_type.key().as_str().to_owned(), runner_type))
        .collect();
    for job in waiting {
        if let Err(error) = dispatch_one(jobs, &job, fleet.get(job.runner_type().as_str())).await {
            tracing::warn!(
                job_id = %job.id().as_uuid(),
                error = %error,
                "dispatching a waiting job failed; it stays waiting"
            );
        }
    }
    Ok(())
}

async fn dispatch_one(
    jobs: &Jobs,
    job: &Job,
    runner_type: Option<&RunnerType>,
) -> Result<(), ServiceError> {
    if !runner_type.is_some_and(RunnerType::is_available) {
        return Ok(());
    }
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
