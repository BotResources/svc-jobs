use bc_jobs::commands::fleet::{ObserveLoss, ObservePresence, observe_loss, observe_presence};
use bc_jobs::commands::job::backstop::ReclaimRun;
use bc_jobs::domain::ids::{PresenceSessionId, ResolutionId, RetryScheduleId, RunnerTypeId};
use bc_jobs::domain::keys::{
    InstanceKey, ReasonCode, ReportedStatus, RunnerTypeKey, RunnerVersion,
};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::policies::fleet::runs_lost_with_instance;
use bc_jobs::ports::fleet::FleetReader;
use contract_jobs::runner as wire;

use super::write::JobChange;
use super::{Jobs, service_metadata, write};
use crate::error::ServiceError;

pub async fn observed(jobs: &Jobs, presence: &wire::Presence) -> Result<(), ServiceError> {
    let key = RunnerTypeKey::new(&presence.runner_type)?;
    let known = FleetReader::load(&jobs.store, &key).await?;
    let runner_type_id = match &known {
        Some(runner_type) => runner_type.id(),
        None => RunnerTypeId::new(jobs.store.ensure_runner_type(&key).await?)?,
    };
    let command = ObservePresence {
        runner_type_id,
        runner_type: key,
        instance_key: InstanceKey::new(&presence.instance_key)?,
        session_id: PresenceSessionId::new(jobs.ids.next())?,
        version: RunnerVersion::new(&presence.runner_version)?,
        reported_status: ReportedStatus::new(&presence.status)?,
    };
    let result = observe_presence(known.as_ref(), command)?;
    write::commit_fleet_events(
        &jobs.store,
        runner_type_id,
        &result.events,
        &service_metadata(),
        jobs.clock.now(),
    )
    .await
}

pub async fn lost(
    jobs: &Jobs,
    runner_type: &str,
    instance_key: &str,
    reason_code: &str,
) -> Result<(), ServiceError> {
    let key = RunnerTypeKey::new(runner_type)?;
    let instance_key = InstanceKey::new(instance_key)?;
    let Some(known) = FleetReader::load(&jobs.store, &key).await? else {
        return Ok(());
    };
    if known.instance(&instance_key).is_none() {
        return Ok(());
    }
    let result = observe_loss(
        &known,
        ObserveLoss {
            instance_key: instance_key.clone(),
            reason_code: ReasonCode::new(reason_code)?,
        },
    )?;
    write::commit_fleet_events(
        &jobs.store,
        known.id(),
        &result.events,
        &service_metadata(),
        jobs.clock.now(),
    )
    .await?;
    reclaim_runs(jobs, &key, &instance_key).await
}

async fn reclaim_runs(
    jobs: &Jobs,
    runner_type: &RunnerTypeKey,
    instance_key: &InstanceKey,
) -> Result<(), ServiceError> {
    let instance = RunnerInstanceReference::new(runner_type.clone(), instance_key.clone());
    let active = jobs.store.active_jobs_of_type(runner_type.as_str()).await?;
    let lost = runs_lost_with_instance(&instance, &active);
    for orphan in lost {
        let Some(job) = active.iter().find(|job| job.id() == orphan.job_id) else {
            continue;
        };
        let result = job.fail_run_for_instance_loss(
            ReclaimRun {
                run_id: orphan.run_id,
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
        .await?;
    }
    Ok(())
}
