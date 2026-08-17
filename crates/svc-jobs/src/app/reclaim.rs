use bc_jobs::commands::job::backstop::ReclaimRun;
use bc_jobs::domain::ids::{PresenceSessionId, ResolutionId, RetryScheduleId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::policies::fleet::{LostPresenceSession, runs_lost_with_instance};
use bc_jobs::ports::fleet::FleetReader;

use super::write::JobChange;
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

pub async fn runs_of(
    jobs: &Jobs,
    runner_type: &RunnerTypeKey,
    instance_key: &InstanceKey,
    session_id: PresenceSessionId,
) -> Result<(), ServiceError> {
    let Some(window) = FleetReader::closed_presence_session(&jobs.store, session_id).await? else {
        tracing::warn!(
            runner_type = %runner_type.as_str(),
            instance_key = %instance_key.as_str(),
            session_id = %session_id.as_uuid(),
            "the session this pod just closed reads back as open; its runs are left to the \
             run-duration backstop rather than reclaimed under an unknown window"
        );
        return Ok(());
    };
    let instance = RunnerInstanceReference::new(runner_type.clone(), instance_key.clone());
    let lost_session =
        LostPresenceSession::new(instance, window.connected_at, window.disconnected_at);
    let active = jobs.store.active_jobs_of_type(runner_type.as_str()).await?;
    let lost = runs_lost_with_instance(&lost_session, &active);
    for orphan in lost {
        let Some(job) = active.iter().find(|job| job.id() == orphan.job_id) else {
            continue;
        };
        if let Err(error) = one(jobs, job, orphan.run_id).await {
            tracing::warn!(
                job_id = %job.id().as_uuid(),
                run_id = %orphan.run_id.as_uuid(),
                error = %error,
                "reclaiming a run orphaned by a lost instance failed; the remaining orphans are \
                 still reclaimed and the backstop covers this one"
            );
        }
    }
    Ok(())
}

async fn one(jobs: &Jobs, job: &Job, run_id: RunId) -> Result<(), ServiceError> {
    let result = job.fail_run_for_instance_loss(
        ReclaimRun {
            run_id,
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: jobs.clock.now(),
        },
        &jobs.retry,
        &jobs.limits,
    )?;
    jobs.commit(
        vec![JobChange::new(job.id(), Some(job.clone()), result.events)],
        &service_metadata(),
    )
    .await
}
