use std::time::Duration;

use bc_jobs::domain::ids::RunId;
use bc_jobs::ports::PortError;
use bc_jobs::ports::transport::{RunTrigger, TriggerSequence};
use bc_jobs::ports::trigger_ledger::TriggerLedger;

use super::Jobs;
use super::trigger_outcomes::{self, Outcome};

const STORE_ATTEMPTS: u32 = 3;
const STORE_PAUSE: Duration = Duration::from_millis(100);

pub async fn dispatch(jobs: &Jobs, run_id: RunId, trigger: &RunTrigger) -> Result<(), PortError> {
    let sequence = jobs.transport.dispatch(trigger).await?;
    if let Err(error) = store_sequence(jobs, run_id, sequence).await {
        trigger_outcomes::record(Outcome::StoreFailed);
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

async fn store_sequence(
    jobs: &Jobs,
    run_id: RunId,
    sequence: TriggerSequence,
) -> Result<(), PortError> {
    let mut attempt = 1;
    loop {
        match jobs.store.record_trigger_sequence(run_id, sequence).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt == STORE_ATTEMPTS => return Err(error),
            Err(_) => {
                attempt += 1;
                tokio::time::sleep(STORE_PAUSE).await;
            }
        }
    }
}
