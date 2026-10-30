use async_trait::async_trait;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use super::PortError;
use super::environment::IdFactory;
use crate::domain::ids::{AgentId, AttachmentId, MailId};
use crate::domain::mail::content::{AttachmentBody, MailContent};
use crate::domain::mail::delivery::RefusedRestart;
use crate::domain::mail::views::MailRead;
use crate::domain::runner::refusal::RunnerRefusal;
use crate::domain::runner::work::WorkNamed;

pub type Stored<T> = Result<Result<T, RunnerRefusal>, PortError>;

pub struct SendMail<'a> {
    pub work: WorkNamed<'a>,
    pub content: MailContent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    pub refused_restarts: Vec<RefusedRestart>,
    pub wakes_restart: bool,
}

pub struct Minting<'a> {
    pub ids: &'a dyn IdFactory,
    pub metadata: &'a EventMetadata,
    pub at: DateTime<Utc>,
}

pub struct UploadAttachment<'a> {
    pub work: WorkNamed<'a>,
    pub mail_id: MailId,
    pub attachment_id: AttachmentId,
    pub from: AgentId,
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[async_trait]
pub trait MailSender: Send + Sync {
    async fn send_mail(&self, request: SendMail<'_>, minting: Minting<'_>) -> Stored<Sent>;
}

#[async_trait]
pub trait MailReader: Send + Sync {
    async fn read_mail(
        &self,
        work: WorkNamed<'_>,
        mail_id: MailId,
        address: AgentId,
    ) -> Stored<MailRead>;
}

#[async_trait]
pub trait AttachmentStore: Send + Sync {
    async fn upload_attachment(
        &self,
        request: UploadAttachment<'_>,
        at: DateTime<Utc>,
    ) -> Stored<()>;

    async fn fetch_attachment(
        &self,
        work: WorkNamed<'_>,
        mail_id: MailId,
        attachment_id: AttachmentId,
    ) -> Stored<AttachmentBody>;
}
