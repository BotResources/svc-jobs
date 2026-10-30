use bc_jobs::JobsError;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::InstanceKey;
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use contract_jobs::command_v3::acting_run;
use uuid::Uuid;

use super::Jobs;
use crate::error::ServiceError;

#[derive(Debug, Clone)]
pub struct ActingRun {
    pub run_id: RunId,
    pub job: Job,
}

impl ActingRun {
    pub fn job_id(&self) -> JobId {
        self.job.id()
    }
}

pub async fn named(
    jobs: &Jobs,
    run_id: Option<Uuid>,
    instance_key: Option<&str>,
) -> Result<Option<ActingRun>, ServiceError> {
    match acting_run(run_id, instance_key) {
        Err(_) => Err(JobsError::ActingRunMissing.into()),
        Ok(None) => Ok(None),
        Ok(Some((run_id, instance_key))) => live(jobs, run_id, instance_key).await.map(Some),
    }
}

async fn live(jobs: &Jobs, run_id: Uuid, instance_key: &str) -> Result<ActingRun, ServiceError> {
    let Some(job_id) = jobs.store.job_of_run(run_id).await? else {
        return Err(JobsError::ActingRunMissing.into());
    };
    let job = jobs.require(job_id).await?;
    let run_id = RunId::new(run_id)?;
    let run = job
        .find_run(run_id)
        .map_err(|_| JobsError::ActingRunMissing)?;
    let instance = RunnerInstanceReference::new(
        job.runner_type().clone(),
        InstanceKey::new(instance_key).map_err(|_| JobsError::ActingRunNotLive)?,
    );
    if run.is_terminal() || !run.is_executed_by(&instance) {
        return Err(JobsError::ActingRunNotLive.into());
    }
    Ok(ActingRun { run_id, job })
}
