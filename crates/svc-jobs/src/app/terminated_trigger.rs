use std::time::Duration;

use bc_jobs::commands::job::backstop::ReclaimRun;
use bc_jobs::domain::ids::{ResolutionId, RetryScheduleId};
use bc_jobs::ports::transport::TriggerSequence;
use bc_jobs::ports::trigger_ledger::{AwaitedRun, TriggerLedger};

use super::trigger_outcomes::Outcome;
use super::write::JobChange;
use super::{Jobs, retry, service_metadata};
use crate::error::ServiceError;

const LEDGER_LOOKUPS: u32 = 5;
const LEDGER_LOOKUP_PAUSE: Duration = Duration::from_millis(200);
const WRITE_ATTEMPTS: usize = 5;

pub async fn terminated(jobs: &Jobs, sequence: TriggerSequence) -> Result<Outcome, ServiceError> {
    let Some(awaited) = awaiting_run(jobs, sequence).await? else {
        tracing::info!(
            sequence = sequence.get(),
            "a terminated trigger belongs to no run this service dispatched"
        );
        return Ok(Outcome::NoRun);
    };
    retry::on_write_race(WRITE_ATTEMPTS, || fail_once(jobs, awaited)).await
}

async fn fail_once(jobs: &Jobs, awaited: AwaitedRun) -> Result<Outcome, ServiceError> {
    let Some(job) = jobs.load(awaited.job_id).await? else {
        return Ok(Outcome::Ignored);
    };
    let result = job.fail_run_for_terminated_trigger(
        ReclaimRun {
            run_id: awaited.run_id,
            retry_schedule_id: RetryScheduleId::new(jobs.ids.next())?,
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            jitter: jobs.jitter.draw(),
            at: jobs.clock.now(),
        },
        &jobs.retry,
        &jobs.limits,
    )?;
    if result.events.is_empty() {
        return Ok(Outcome::Ignored);
    }
    jobs.commit(
        vec![JobChange::new(job.id(), Some(job.clone()), result.events)],
        &service_metadata(),
    )
    .await?;
    Ok(Outcome::RunFailed)
}

async fn awaiting_run(
    jobs: &Jobs,
    sequence: TriggerSequence,
) -> Result<Option<AwaitedRun>, ServiceError> {
    for lookup in 1..=LEDGER_LOOKUPS {
        if let Some(awaited) = jobs.store.run_of_trigger_sequence(sequence).await? {
            return Ok(Some(awaited));
        }
        if lookup < LEDGER_LOOKUPS {
            tokio::time::sleep(LEDGER_LOOKUP_PAUSE).await;
        }
    }
    Ok(None)
}
