use bc_jobs::commands::job::backstop::{FailForInactivity, ReclaimRun, WithdrawStrandedTrigger};
use bc_jobs::domain::ids::{ResolutionId, RetryScheduleId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::run::Run;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::DueWorkReader;

use super::write::JobChange;
use super::{Jobs, reconcile, service_metadata};
use crate::error::ServiceError;

const REPAIR_LOOKBACK: chrono::TimeDelta = chrono::TimeDelta::minutes(10);

pub async fn repair_every_access_path(jobs: &Jobs) -> Result<(), ServiceError> {
    let candidates = jobs.store.every_job_on_a_path().await?;
    let repaired = jobs.store.repair_access_paths(&candidates).await?;
    if !repaired.is_empty() {
        tracing::warn!(
            ?repaired,
            "the access paths drifted from the ledger and were repaired"
        );
    }
    Ok(())
}

async fn repair_recent_access_paths(jobs: &Jobs) -> Result<(), ServiceError> {
    let since = jobs.clock.now() - REPAIR_LOOKBACK;
    let candidates = jobs.store.jobs_with_path_facts_since(since).await?;
    let repaired = jobs.store.repair_access_paths(&candidates).await?;
    if !repaired.is_empty() {
        tracing::warn!(
            ?repaired,
            "the access paths drifted from the ledger (a pod that does not maintain them \
             recorded facts) and were repaired"
        );
    }
    jobs.store.purge_idle_turns(since).await?;
    Ok(())
}

pub async fn sweep(jobs: &Jobs) -> Result<(), ServiceError> {
    let mut steps = Steps::default();
    steps.record(
        "repairing the access paths",
        repair_recent_access_paths(jobs).await,
    );
    if let Err(error) = reconcile::presence_sessions(jobs).await {
        tracing::warn!(
            error = %error,
            "reconciling the presence bucket with the open sessions failed; the sweep still runs \
             the backstops that need PostgreSQL alone, and the next sweep reconciles again"
        );
    }
    steps.record(
        "reclaiming the runs past their maximum duration",
        reclaim_outrun_runs(jobs).await,
    );
    steps.record(
        "reclaiming the lost claims",
        reclaim_lost_claims(jobs).await,
    );
    steps.record(
        "withdrawing the stranded triggers",
        reclaim_stranded_triggers(jobs).await,
    );
    steps.record("failing the inactive jobs", fail_inactive_jobs(jobs).await);
    steps.record(
        "purging the expired Results",
        jobs.store
            .purge_expired_results(jobs.clock.now())
            .await
            .map(drop)
            .map_err(ServiceError::from),
    );
    jobs.start_due_restarts().await;
    steps.outcome()
}

#[derive(Default)]
struct Steps {
    first_failure: Option<ServiceError>,
}

impl Steps {
    fn record(&mut self, step: &str, result: Result<(), ServiceError>) {
        if let Err(error) = result {
            tracing::warn!(step, error = %error, "a backstop step failed; the others still run and the next sweep tries again");
            self.first_failure.get_or_insert(error);
        }
    }

    fn outcome(self) -> Result<(), ServiceError> {
        self.first_failure.map_or(Ok(()), Err)
    }
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

async fn reclaim_stranded_triggers(jobs: &Jobs) -> Result<(), ServiceError> {
    let now = jobs.clock.now();
    let delay = jobs.claims.reservation_delay;
    let stranded = DueWorkReader::jobs_with_stranded_triggers(&jobs.store, now - delay).await?;
    for job in stranded {
        let Some(run) = job.active_run() else {
            continue;
        };
        let Some(runner_type) = FleetReader::load(&jobs.store, job.runner_type()).await? else {
            continue;
        };
        let command = WithdrawStrandedTrigger {
            run_id: run.id(),
            at: now,
        };
        match job.withdraw_stranded_trigger(command, runner_type.claimed_since(), delay) {
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
