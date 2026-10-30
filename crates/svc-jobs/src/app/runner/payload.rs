use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::payload::PayloadAttachment;
use bc_jobs::domain::runner::kept_payload::{KeptPayload, UploadAdmission};
use uuid::Uuid;

use super::{Jobs, Outcome, WorkIds, started};
use bc_jobs::ports::kept_payload::{KeptPayloads, Uploader as StoreUploader};

pub enum Uploader {
    Run(WorkIds),
    Service(ActorRef),
}

pub async fn upload_payload(
    jobs: &Jobs,
    uploader: &Uploader,
    for_job_id: Uuid,
    message: String,
    attachments: Vec<PayloadAttachment>,
) -> Outcome<UploadAdmission> {
    let store_uploader = match uploader {
        Uploader::Run(work) => {
            started(jobs, work).await?;
            StoreUploader::Run(work.named())
        }
        Uploader::Service(actor) => StoreUploader::Service(*actor),
    };
    Ok(jobs
        .store
        .upload_payload(
            store_uploader,
            for_job_id,
            KeptPayload {
                message,
                attachments,
            },
            jobs.clock.now(),
        )
        .await?)
}

pub async fn fetch_payload(jobs: &Jobs, work: &WorkIds) -> Outcome<KeptPayload> {
    started(jobs, work).await?;
    Ok(jobs.store.fetch_payload(work.named()).await?)
}
