use std::future::Future;
use std::pin::Pin;

use super::{Jobs, reconcile};
use crate::error::ServiceError;
use due_work::{
    fail_inactive_jobs, reclaim_lost_claims, reclaim_outrun_runs, reclaim_stranded_triggers,
};

mod due_work;
mod erasure;

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
    CancelledRequestData,
    InactiveJobs,
    ExpiredResults,
    DueRestarts,
}

impl Backstop {
    const SWEEP: [Backstop; 9] = [
        Backstop::AccessPaths,
        Backstop::PresenceSessions,
        Backstop::OutrunRuns,
        Backstop::LostClaims,
        Backstop::StrandedTriggers,
        Backstop::CancelledRequestData,
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
            Backstop::CancelledRequestData => "erasing the request data of the cancelled Jobs",
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
            Backstop::CancelledRequestData => Box::pin(erasure::erase_cancelled_request_data(jobs)),
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

#[cfg(test)]
mod tests;
