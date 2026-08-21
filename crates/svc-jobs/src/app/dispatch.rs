use std::collections::HashMap;

use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::job::Job;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::{DUE_WORK_BATCH, DueWorkReader};

use super::write::JobChange;
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DispatchPass {
    pub skipped_for_unavailability: bool,
    pub failed: bool,
    pub truncated: bool,
}

enum Outcome {
    Dispatched,
    NobodyWouldTakeIt,
}

pub async fn dispatch_due_work(jobs: &Jobs) -> Result<DispatchPass, ServiceError> {
    let mut pass = DispatchPass::default();
    let now = jobs.clock.now();
    let waiting = DueWorkReader::jobs_awaiting_dispatch(&jobs.store, now).await?;
    if waiting.is_empty() {
        return Ok(pass);
    }
    pass.truncated = waiting.len() >= DUE_WORK_BATCH;
    let fleet: HashMap<String, RunnerType> = FleetReader::load_all(&jobs.store)
        .await?
        .into_iter()
        .map(|runner_type| (runner_type.key().as_str().to_owned(), runner_type))
        .collect();
    for job in waiting {
        match dispatch_one(jobs, &job, fleet.get(job.runner_type().as_str())).await {
            Ok(Outcome::Dispatched) => {}
            Ok(Outcome::NobodyWouldTakeIt) => pass.skipped_for_unavailability = true,
            Err(error) => {
                pass.failed = true;
                tracing::warn!(
                    job_id = %job.id().as_uuid(),
                    error = %error,
                    "dispatching a waiting job failed; it stays waiting"
                );
            }
        }
    }
    Ok(pass)
}

async fn dispatch_one(
    jobs: &Jobs,
    job: &Job,
    runner_type: Option<&RunnerType>,
) -> Result<Outcome, ServiceError> {
    if runner_type.is_none_or(|known| known.guard_dispatch().is_err()) {
        return Ok(Outcome::NobodyWouldTakeIt);
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
    .await?;
    Ok(Outcome::Dispatched)
}
