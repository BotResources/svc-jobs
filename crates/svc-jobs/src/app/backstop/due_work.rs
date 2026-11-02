use bc_jobs::commands::job::backstop::{FailForInactivity, ReclaimRun, WithdrawStrandedTrigger};
use bc_jobs::domain::ids::{ResolutionId, RetryScheduleId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::run::Run;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::DueWorkReader;

use super::each::each_loaded_job;
use crate::app::write::JobChange;
use crate::app::{Jobs, service_metadata};
use crate::error::ServiceError;

pub(super) async fn reclaim_outrun_runs(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let outrun =
        DueWorkReader::jobs_with_outrun_runs(&jobs.store, now, jobs.limits.max_run_duration())
            .await?;
    each_loaded_job(outrun, "reclaiming an outrun run", |job| async move {
        let Some(run) = job.active_run().filter(|run| Run::has_started(run)) else {
            return Ok(());
        };
        let command = ReclaimRun {
            run_id: run.id(),
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: now,
        };
        match job.fail_run_for_max_duration(command, &jobs.retry, &jobs.limits) {
            Err(refusal) => {
                tracing::debug!(
                    job_id = %job.id().as_uuid(),
                    code = refusal.code(),
                    "the run-duration backstop left this run alone"
                );
                Ok(())
            }
            Ok(result) => commit(jobs, &job, result.events).await,
        }
    })
    .await
}

pub(super) async fn reclaim_lost_claims(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let delay = jobs.claims.reservation_delay;
    let lost = DueWorkReader::jobs_with_lost_claims(&jobs.store, now - delay).await?;
    each_loaded_job(lost, "reclaiming a lost claim", |job| async move {
        let Some(run) = job.active_run().filter(|run| run.is_reserved()) else {
            return Ok(());
        };
        let command = ReclaimRun {
            run_id: run.id(),
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: now,
        };
        match job.fail_run_for_lost_claim(command, delay, &jobs.retry, &jobs.limits) {
            Err(refusal) => {
                tracing::debug!(
                    job_id = %job.id().as_uuid(),
                    code = refusal.code(),
                    "the lost-claim backstop left this run alone"
                );
                Ok(())
            }
            Ok(result) => settle_one(jobs, &job, result.events, "lost-claim").await,
        }
    })
    .await
}

pub(super) async fn reclaim_stranded_triggers(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let delay = jobs.claims.reservation_delay;
    let stranded = DueWorkReader::jobs_with_stranded_triggers(&jobs.store, now - delay).await?;
    each_loaded_job(
        stranded,
        "withdrawing a stranded trigger",
        |job| async move {
            let Some(run) = job.active_run() else {
                return Ok(());
            };
            let Some(runner_type) = FleetReader::load(&jobs.store, job.runner_type()).await? else {
                return Ok(());
            };
            let command = WithdrawStrandedTrigger {
                run_id: run.id(),
                at: now,
            };
            match job.withdraw_stranded_trigger(command, runner_type.claimed_since(), delay) {
                Err(refusal) => {
                    tracing::debug!(
                        job_id = %job.id().as_uuid(),
                        code = refusal.code(),
                        "the stranded-trigger backstop left this run alone"
                    );
                    Ok(())
                }
                Ok(result) => settle_one(jobs, &job, result.events, "stranded-trigger").await,
            }
        },
    )
    .await
}

async fn settle_one(
    jobs: &Jobs,
    job: &Job,
    events: Vec<bc_jobs::event::job::JobEvent>,
    backstop: &'static str,
) -> Result<(), ServiceError> {
    match commit(jobs, job, events).await {
        Err(ServiceError::Contended(_)) => {
            tracing::info!(
                job_id = %job.id().as_uuid(),
                backstop,
                "a backstop was contended on this job; the next sweep looks again"
            );
            Ok(())
        }
        outcome => outcome,
    }
}

pub(super) async fn fail_inactive_jobs(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let cutoff = now - jobs.limits.inactivity_timeout();
    let idle = DueWorkReader::jobs_idle_since(&jobs.store, cutoff).await?;
    each_loaded_job(idle, "failing an inactive job", |job| async move {
        let command = FailForInactivity {
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            at: now,
        };
        match job.fail_for_inactivity(command, &jobs.limits) {
            Err(refusal) => {
                tracing::debug!(
                    job_id = %job.id().as_uuid(),
                    code = refusal.code(),
                    "the inactivity backstop left this job alone"
                );
                Ok(())
            }
            Ok(result) => commit(jobs, &job, result.events).await,
        }
    })
    .await
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
