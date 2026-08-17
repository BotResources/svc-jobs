use async_trait::async_trait;

use crate::domain::log::RunLogLine;
use crate::ports::PortError;

#[async_trait]
pub trait RunLogWriter: Send + Sync {
    async fn append(&self, lines: &[RunLogLine]) -> Result<(), PortError>;
}
