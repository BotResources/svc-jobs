use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::PortError;
use super::mail::Stored;
use crate::domain::ownership::ActorRef;
use crate::domain::runner::kept_payload::{KeptPayload, StoredPayload, UploadAdmission};
use crate::domain::runner::work::WorkNamed;

pub enum Uploader<'a> {
    Run(WorkNamed<'a>),
    Service(ActorRef),
}

#[async_trait]
pub trait KeptPayloads: Send + Sync {
    async fn upload_payload(
        &self,
        uploader: Uploader<'_>,
        for_job_id: Uuid,
        payload: KeptPayload,
        at: DateTime<Utc>,
    ) -> Stored<UploadAdmission>;

    async fn stored_payload(&self, job_id: Uuid) -> Result<Option<StoredPayload>, PortError>;

    async fn fetch_payload(&self, work: WorkNamed<'_>) -> Stored<KeptPayload>;
}
