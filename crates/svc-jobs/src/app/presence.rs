use bc_jobs::commands::fleet::{ObserveLoss, ObservePresence, observe_loss, observe_presence};
use bc_jobs::commands::job::backstop::ReclaimRun;
use bc_jobs::domain::ids::{PresenceSessionId, ResolutionId, RetryScheduleId, RunId, RunnerTypeId};
use bc_jobs::domain::job::Job;
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
        runner_type: key.clone(),
        instance_key: InstanceKey::new(&presence.instance_key)?,
        session_id: PresenceSessionId::new(jobs.ids.next())?,
        version: RunnerVersion::new(&presence.runner_version)?,
        reported_status: ReportedStatus::new(&presence.status)?,
    };
    let result = observe_presence(known.as_ref(), command)?;
    recorded(
        write::commit_fleet_events(
            &jobs.store,
            jobs.ids.as_ref(),
            write::FleetChange {
                runner_type_id,
                runner_type: &key,
                decided_on: known.as_ref(),
                events: &result.events,
            },
            &service_metadata(),
            jobs.clock.now(),
        )
        .await,
    )
    .map(|_| ())
}

fn recorded(outcome: Result<(), ServiceError>) -> Result<bool, ServiceError> {
    match outcome {
        Ok(()) => Ok(true),
        Err(ServiceError::Contended) => Ok(false),
        Err(other) => Err(other),
    }
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
    let this_pod_recorded_the_loss = recorded(
        write::commit_fleet_events(
            &jobs.store,
            jobs.ids.as_ref(),
            write::FleetChange {
                runner_type_id: known.id(),
                runner_type: &key,
                decided_on: Some(&known),
                events: &result.events,
            },
            &service_metadata(),
            jobs.clock.now(),
        )
        .await,
    )?;
    if !this_pod_recorded_the_loss {
        return Ok(());
    }
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
        if let Err(error) = reclaim_one(jobs, job, orphan.run_id).await {
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

async fn reclaim_one(jobs: &Jobs, job: &Job, run_id: RunId) -> Result<(), ServiceError> {
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
