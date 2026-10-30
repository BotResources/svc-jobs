use std::collections::HashMap;

use bc_jobs::JobsError;
use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::run_duration::MaxRunDuration;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::job::Job;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::{DUE_WORK_BATCH, DispatchWindow, DueWorkReader};

use super::write::JobChange;
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchPass {
    pub dispatchable_types: Vec<String>,
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
    let fleet: HashMap<String, RunnerType> = FleetReader::load_all(&jobs.store)
        .await?
        .into_iter()
        .map(|runner_type| (runner_type.key().as_str().to_owned(), runner_type))
        .collect();
    let in_use = jobs.store.current_runs_by_type().await?;
    let windows: Vec<DispatchWindow> = fleet
        .values()
        .filter(|runner_type| runner_type.guard_push_dispatch().is_ok())
        .filter_map(|runner_type| {
            let used = in_use.get(runner_type.key().as_str()).copied().unwrap_or(0);
            let free = runner_type.free_capacity(used);
            (free > 0).then(|| DispatchWindow {
                runner_type: runner_type.key().clone(),
                free,
            })
        })
        .collect();
    pass.dispatchable_types = windows
        .iter()
        .map(|window| window.runner_type.as_str().to_owned())
        .collect();
    let waiting = DueWorkReader::jobs_awaiting_dispatch(&jobs.store, now, &windows).await?;
    if waiting.is_empty() {
        return Ok(pass);
    }
    pass.truncated = waiting.len() >= DUE_WORK_BATCH;
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
    let Some(runner_type) = runner_type.filter(|known| known.guard_push_dispatch().is_ok()) else {
        return Ok(Outcome::NobodyWouldTakeIt);
    };
    let result = job.dispatch_run(
        DispatchRun {
            run_id: RunId::new(jobs.ids.next())?,
            at: jobs.clock.now(),
            max_run_duration: runner_type
                .max_run_duration_or(MaxRunDuration::from_delta(jobs.limits.max_run_duration())?),
        },
        &jobs.limits,
    )?;
    let change = JobChange::new(job.id(), Some(job.clone()), result.events)
        .requiring_free_capacity(runner_type.key().clone());
    match jobs.commit(vec![change], &service_metadata()).await {
        Ok(()) => Ok(Outcome::Dispatched),
        Err(ServiceError::Domain(JobsError::NoFreeCapacity { .. })) => {
            Ok(Outcome::NobodyWouldTakeIt)
        }
        Err(other) => Err(other),
    }
}
