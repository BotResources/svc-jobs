use bc_jobs::domain::ids::{AgentId, AttachmentId, MailId};
use bc_jobs::domain::mail::author::{MailAuthor, SourceConversation};
use bc_jobs::domain::mail::content::{AttachmentRef, MailContent};
use uuid::Uuid;

use super::{Jobs, Outcome, WorkIds, started};
use crate::app::service_metadata;
use crate::db::mail::{AttachmentContent, MailRead, Minting, SendMail, Sent, UploadAttachment};

pub struct SendMailInput {
    pub work: WorkIds,
    pub mail_id: Uuid,
    pub from: Uuid,
    pub author: String,
    pub source_conversation_id: Uuid,
    pub subject: String,
    pub body: String,
    pub to: Vec<Uuid>,
    pub cc: Vec<Uuid>,
    pub attachments: Vec<(Uuid, String)>,
}

pub async fn send_mail(jobs: &Jobs, input: SendMailInput) -> Outcome<Sent> {
    started(jobs, &input.work).await?;
    let author = match MailAuthor::new(input.author) {
        Ok(author) => author,
        Err(refusal) => return Ok(Err(refusal.into())),
    };
    let source_conversation = match SourceConversation::new(input.source_conversation_id) {
        Ok(source) => source,
        Err(refusal) => return Ok(Err(refusal.into())),
    };
    let content = MailContent {
        mail_id: MailId::new(input.mail_id),
        from: AgentId::new(input.from),
        author,
        source_conversation,
        subject: input.subject,
        body: input.body,
        to: input.to.into_iter().map(AgentId::new).collect(),
        cc: input.cc.into_iter().map(AgentId::new).collect(),
        attachments: input
            .attachments
            .into_iter()
            .map(|(attachment_id, name)| AttachmentRef {
                attachment_id: AttachmentId::new(attachment_id),
                name,
            })
            .collect(),
    }
    .canonical();
    let metadata = service_metadata();
    Ok(jobs
        .store
        .send_mail(
            SendMail {
                work: input.work.named(),
                content,
            },
            Minting {
                ids: jobs.ids.as_ref(),
                metadata: &metadata,
                at: jobs.clock.now(),
            },
        )
        .await?)
}

#[allow(clippy::too_many_arguments)]
pub async fn upload_attachment(
    jobs: &Jobs,
    work: &WorkIds,
    mail_id: Uuid,
    attachment_id: Uuid,
    from: Uuid,
    name: String,
    content_type: String,
    content: String,
) -> Outcome<()> {
    started(jobs, work).await?;
    Ok(jobs
        .store
        .upload_attachment(
            UploadAttachment {
                work: work.named(),
                mail_id: MailId::new(mail_id),
                attachment_id: AttachmentId::new(attachment_id),
                from: AgentId::new(from),
                name,
                content_type,
                content,
            },
            jobs.clock.now(),
        )
        .await?)
}

pub async fn read_mail(
    jobs: &Jobs,
    work: &WorkIds,
    mail_id: Uuid,
    address: Uuid,
) -> Outcome<MailRead> {
    started(jobs, work).await?;
    Ok(jobs
        .store
        .read_mail(work.named(), MailId::new(mail_id), AgentId::new(address))
        .await?)
}

pub async fn fetch_attachment(
    jobs: &Jobs,
    work: &WorkIds,
    mail_id: Uuid,
    attachment_id: Uuid,
) -> Outcome<AttachmentContent> {
    started(jobs, work).await?;
    Ok(jobs
        .store
        .fetch_attachment(
            work.named(),
            MailId::new(mail_id),
            AttachmentId::new(attachment_id),
        )
        .await?)
}
