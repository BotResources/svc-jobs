use bc_jobs::commands::job::backstop::{FailForInactivity, ReclaimRun};
use bc_jobs::domain::ids::{ResolutionId, RetryScheduleId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::run::Run;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::DueWorkReader;

use super::write::JobChange;
use super::{Jobs, reconcile, service_metadata};
use crate::error::ServiceError;

pub async fn sweep(jobs: &Jobs) -> Result<(), ServiceError> {
    if let Err(error) = reconcile::presence_sessions(jobs).await {
        tracing::warn!(
            error = %error,
            "reconciling the presence bucket with the open sessions failed; the sweep still runs \
             the backstops that need PostgreSQL alone, and the next sweep reconciles again"
        );
    }
    reclaim_outrun_runs(jobs).await?;
    reclaim_lost_claims(jobs).await?;
    reclaim_stranded_triggers(jobs).await?;
    fail_inactive_jobs(jobs).await
}

async fn reclaim_outrun_runs(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    for job in
        DueWorkReader::jobs_with_outrun_runs(&jobs.store, now, jobs.limits.max_run_duration())
            .await?
    {
        let Some(run) = job.active_run().filter(|run| Run::has_started(run)) else {
            continue;
        };
        let command = ReclaimRun {
            run_id: run.id(),
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: now,
        };
        match job.fail_run_for_max_duration(command, &jobs.retry, &jobs.limits) {
            Err(refusal) => tracing::debug!(
                job_id = %job.id().as_uuid(),
                code = refusal.code(),
                "the run-duration backstop left this run alone"
            ),
            Ok(result) => commit(jobs, &job, result.events).await?,
        }
    }
    Ok(())
}

async fn reclaim_lost_claims(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let delay = jobs.claims.reservation_delay;
    for job in DueWorkReader::jobs_with_lost_claims(&jobs.store, now - delay).await? {
        let Some(run) = job.active_run().filter(|run| run.is_reserved()) else {
            continue;
        };
        let command = ReclaimRun {
            run_id: run.id(),
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: now,
        };
        match job.fail_run_for_lost_claim(command, delay, &jobs.retry, &jobs.limits) {
            Err(refusal) => tracing::debug!(
                job_id = %job.id().as_uuid(),
                code = refusal.code(),
                "the lost-claim backstop left this run alone"
            ),
            Ok(result) => settle_one(jobs, &job, result.events, "lost-claim").await?,
        }
    }
    Ok(())
}

/// Runs published on the trigger stream before their type switched to
/// `CLAIM`, that nobody started: past the reservation delay they fail as
/// transient (`trigger_stranded`) and their retry waits to be claimed.
async fn reclaim_stranded_triggers(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let delay = jobs.claims.reservation_delay;
    let stranded = DueWorkReader::jobs_with_stranded_triggers(&jobs.store, now - delay).await?;
    if stranded.is_empty() {
        return Ok(());
    }
    for job in stranded {
        let Some(run) = job.active_run() else {
            continue;
        };
        let Some(runner_type) = FleetReader::load(&jobs.store, job.runner_type()).await? else {
            continue;
        };
        let command = ReclaimRun {
            run_id: run.id(),
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: now,
        };
        match job.fail_run_for_stranded_trigger(
            command,
            runner_type.dispatch_mode(),
            delay,
            &jobs.retry,
            &jobs.limits,
        ) {
            Err(refusal) => tracing::debug!(
                job_id = %job.id().as_uuid(),
                code = refusal.code(),
                "the stranded-trigger backstop left this run alone"
            ),
            Ok(result) => settle_one(jobs, &job, result.events, "stranded-trigger").await?,
        }
    }
    Ok(())
}

/// One job a backstop settles: a write race on it is logged and the sweep
/// moves on — the next sweep sees it again — while an outage still stops it.
async fn settle_one(
    jobs: &Jobs,
    job: &Job,
    events: Vec<bc_jobs::event::job::JobEvent>,
    backstop: &'static str,
) -> Result<(), ServiceError> {
    match commit(jobs, job, events).await {
        Err(ServiceError::Contended) => {
            tracing::info!(
                job_id = %job.id().as_uuid(),
                backstop,
                "a backstop lost a write race on this job; the next sweep looks again"
            );
            Ok(())
        }
        outcome => outcome,
    }
}

async fn fail_inactive_jobs(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let cutoff = now - jobs.limits.inactivity_timeout();
    for job in DueWorkReader::jobs_idle_since(&jobs.store, cutoff).await? {
        let command = FailForInactivity {
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            at: now,
        };
        match job.fail_for_inactivity(command, &jobs.limits) {
            Err(refusal) => tracing::debug!(
                job_id = %job.id().as_uuid(),
                code = refusal.code(),
                "the inactivity backstop left this job alone"
            ),
            Ok(result) => commit(jobs, &job, result.events).await?,
        }
    }
    Ok(())
}

async fn commit(
    jobs: &Jobs,
    job: &Job,
    events: Vec<bc_jobs::event::job::JobEvent>,
) -> Result<(), ServiceError> {
    jobs.commit(
        vec![JobChange::new(job.id(), Some(job.clone()), events)],
        &service_metadata(),
    )
    .await
}
