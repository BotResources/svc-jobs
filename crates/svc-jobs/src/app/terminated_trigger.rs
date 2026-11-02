use std::time::Duration;

use bc_jobs::commands::job::backstop::ReclaimRun;
use bc_jobs::domain::ids::{ResolutionId, RetryScheduleId, RunId};
use bc_jobs::ports::PortError;
use bc_jobs::ports::transport::{RunTrigger, TriggerSequence};
use bc_jobs::ports::trigger_ledger::{AwaitedRun, TriggerLedger};

use super::write::JobChange;
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

const LEDGER_LOOKUPS: u32 = 5;
const LEDGER_LOOKUP_PAUSE: Duration = Duration::from_millis(200);

pub async fn dispatch(jobs: &Jobs, run_id: RunId, trigger: &RunTrigger) -> Result<(), PortError> {
    let sequence = jobs.transport.dispatch(trigger).await?;
    if let Err(error) = jobs.store.record_trigger_sequence(run_id, sequence).await {
        tracing::error!(
            run_id = %run_id.as_uuid(),
            sequence = sequence.get(),
            error = %error,
            "the stream sequence of a dispatched trigger could not be stored; a termination of \
             this trigger will not fail its run"
        );
    }
    Ok(())
}

pub async fn terminated(jobs: &Jobs, sequence: TriggerSequence) -> Result<(), ServiceError> {
    let Some(awaited) = awaiting_run(jobs, sequence).await? else {
        tracing::debug!(
            sequence = sequence.get(),
            "a terminated trigger belongs to no run this service dispatched"
        );
        return Ok(());
    };
    let Some(job) = jobs.load(awaited.job_id).await? else {
        return Ok(());
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
        return Ok(());
    }
    jobs.commit(
        vec![JobChange::new(job.id(), Some(job.clone()), result.events)],
        &service_metadata(),
    )
    .await
}

async fn awaiting_run(
    jobs: &Jobs,
    sequence: TriggerSequence,
) -> Result<Option<AwaitedRun>, ServiceError> {
    for lookup in 1..=LEDGER_LOOKUPS {
        if let Some(awaited) = jobs.store.run_awaiting_trigger(sequence).await? {
            return Ok(Some(awaited));
        }
        if lookup < LEDGER_LOOKUPS {
            tokio::time::sleep(LEDGER_LOOKUP_PAUSE).await;
        }
    }
    Ok(None)
}
