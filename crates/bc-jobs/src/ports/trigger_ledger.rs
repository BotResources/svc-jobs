use async_trait::async_trait;

use crate::domain::ids::{JobId, RunId};
use crate::ports::PortError;
use crate::ports::transport::TriggerSequence;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AwaitedRun {
    pub job_id: JobId,
    pub run_id: RunId,
}

#[async_trait]
pub trait TriggerLedger: Send + Sync {
    async fn record_trigger_sequence(
        &self,
        run_id: RunId,
        sequence: TriggerSequence,
    ) -> Result<(), PortError>;

    async fn run_of_trigger_sequence(
        &self,
        sequence: TriggerSequence,
    ) -> Result<Option<AwaitedRun>, PortError>;
}
