use std::future::Future;
use std::pin::Pin;

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

type Pending<'a> = Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backstop {
    AccessPaths,
    PresenceSessions,
    OutrunRuns,
    LostClaims,
    StrandedTriggers,
    InactiveJobs,
    ExpiredResults,
    DueRestarts,
}

impl Backstop {
    const SWEEP: [Backstop; 8] = [
        Backstop::AccessPaths,
        Backstop::PresenceSessions,
        Backstop::OutrunRuns,
        Backstop::LostClaims,
        Backstop::StrandedTriggers,
        Backstop::InactiveJobs,
        Backstop::ExpiredResults,
        Backstop::DueRestarts,
    ];

    fn describe(self) -> &'static str {
        match self {
            Backstop::AccessPaths => "repairing the access paths",
            Backstop::PresenceSessions => "reconciling the presence bucket with the open sessions",
            Backstop::OutrunRuns => "reclaiming the runs past their maximum duration",
            Backstop::LostClaims => "reclaiming the lost claims",
            Backstop::StrandedTriggers => "withdrawing the stranded triggers",
            Backstop::InactiveJobs => "failing the inactive jobs",
            Backstop::ExpiredResults => "purging the expired Results",
            Backstop::DueRestarts => "starting the due restarts",
        }
    }

    fn run(self, jobs: &Jobs) -> Pending<'_> {
        match self {
            Backstop::AccessPaths => Box::pin(repair_recent_access_paths(jobs)),
            Backstop::PresenceSessions => Box::pin(async move {
                if let Err(error) = reconcile::presence_sessions(jobs).await {
                    tracing::warn!(
                        error = %error,
                        "reconciling the presence bucket with the open sessions failed; the next \
                         sweep reconciles again"
                    );
                }
                Ok(())
            }),
            Backstop::OutrunRuns => Box::pin(reclaim_outrun_runs(jobs)),
            Backstop::LostClaims => Box::pin(reclaim_lost_claims(jobs)),
            Backstop::StrandedTriggers => Box::pin(reclaim_stranded_triggers(jobs)),
            Backstop::InactiveJobs => Box::pin(fail_inactive_jobs(jobs)),
            Backstop::ExpiredResults => Box::pin(async move {
                jobs.store
                    .purge_expired_results(jobs.clock.now())
                    .await
                    .map(drop)
                    .map_err(ServiceError::from)
            }),
            Backstop::DueRestarts => Box::pin(async move {
                jobs.start_due_restarts().await;
                Ok(())
            }),
        }
    }
}

pub async fn sweep(jobs: &Jobs) -> Result<(), ServiceError> {
    run_in_order(&Backstop::SWEEP, |backstop| backstop.run(jobs)).await
}

async fn run_in_order<'a>(
    order: &[Backstop],
    mut run: impl FnMut(Backstop) -> Pending<'a>,
) -> Result<(), ServiceError> {
    let mut steps = Steps::default();
    for backstop in order {
        steps.record(backstop.describe(), run(*backstop).await);
    }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn failing(reason: &'static str) -> Pending<'static> {
        Box::pin(async move { Err(ServiceError::Infra(reason.to_owned())) })
    }

    fn succeeding() -> Pending<'static> {
        Box::pin(async { Ok(()) })
    }

    #[test]
    fn the_restart_wakes_run_after_every_safety_backstop() {
        let position = |wanted: Backstop| {
            Backstop::SWEEP
                .iter()
                .position(|backstop| *backstop == wanted)
                .expect("the backstop is in the sweep")
        };
        for backstop in Backstop::SWEEP {
            if backstop != Backstop::DueRestarts {
                assert!(
                    position(backstop) < position(Backstop::DueRestarts),
                    "{backstop:?} runs before the restart wakes"
                );
            }
        }
    }

    #[test]
    fn no_backstop_runs_twice_in_the_sweep() {
        for backstop in Backstop::SWEEP {
            let occurrences = Backstop::SWEEP
                .iter()
                .filter(|other| **other == backstop)
                .count();
            assert_eq!(occurrences, 1, "{backstop:?}");
        }
    }

    #[tokio::test]
    async fn a_failing_safety_backstop_does_not_stop_the_backstops_after_it() {
        let ran = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&ran);

        let outcome = run_in_order(&Backstop::SWEEP, |backstop| {
            observed.lock().unwrap().push(backstop);
            match backstop {
                Backstop::AccessPaths => failing("the access paths"),
                _ => succeeding(),
            }
        })
        .await;

        assert_eq!(*ran.lock().unwrap(), Backstop::SWEEP.to_vec());
        assert!(
            matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "the access paths")
        );
    }

    #[tokio::test]
    async fn the_restart_wakes_run_although_every_safety_backstop_failed() {
        let ran = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&ran);

        let outcome = run_in_order(&Backstop::SWEEP, |backstop| {
            observed.lock().unwrap().push(backstop);
            match backstop {
                Backstop::DueRestarts => succeeding(),
                _ => failing("a safety backstop"),
            }
        })
        .await;

        assert_eq!(ran.lock().unwrap().last(), Some(&Backstop::DueRestarts));
        assert!(outcome.is_err());
    }

    #[tokio::test]
    async fn the_sweep_reports_the_first_failure_and_no_later_one() {
        let outcome = run_in_order(&Backstop::SWEEP, |backstop| match backstop {
            Backstop::LostClaims => failing("first"),
            Backstop::InactiveJobs => failing("second"),
            _ => succeeding(),
        })
        .await;

        assert!(matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "first"));
    }

    #[tokio::test]
    async fn a_sweep_without_failures_succeeds() {
        assert!(
            run_in_order(&Backstop::SWEEP, |_| succeeding())
                .await
                .is_ok()
        );
    }
}
