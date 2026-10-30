use async_graphql::SimpleObject;
use bc_jobs::domain::mail::attachment::AttachmentBody;
use bc_jobs::domain::mail::refusal::BadAddress;
use bc_jobs::domain::mail::views::{AttachmentMeta as MailAttachmentMeta, MailRead, UnreadItem};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use uuid::Uuid;

use super::output::GqlContentAttachment;
use super::unread::GqlInboxItem;

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsMailAttachment")]
pub struct GqlMailAttachment {
    pub attachment_id: Uuid,
    pub name: String,
    pub content_type: String,
    pub size: i64,
}

impl From<MailAttachmentMeta> for GqlMailAttachment {
    fn from(meta: MailAttachmentMeta) -> Self {
        Self {
            attachment_id: meta.attachment_id.as_uuid(),
            name: meta.name,
            content_type: meta.content_type,
            size: meta.size,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsBadAddress")]
pub struct GqlBadAddress {
    pub address: Uuid,
    pub reason: String,
    pub message: String,
}

impl From<&BadAddress> for GqlBadAddress {
    fn from(bad: &BadAddress) -> Self {
        Self {
            address: bad.address.as_uuid(),
            reason: bad.reason.code().to_owned(),
            message: bad.reason.message().to_owned(),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsRestartRefused")]
pub struct GqlRestartRefused {
    pub address: Uuid,
    pub reason: String,
    pub message: String,
}

#[derive(SimpleObject, Clone, Default)]
#[graphql(name = "JobsSendMailResult")]
pub struct GqlSendMailResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub bad_addresses: Vec<GqlBadAddress>,
    pub restart_refused: Vec<GqlRestartRefused>,
}

impl GqlSendMailResult {
    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
            bad_addresses: refusal.bad_addresses().iter().map(Into::into).collect(),
            restart_refused: Vec::new(),
        }
    }
}

#[derive(SimpleObject, Clone, Default)]
#[graphql(name = "JobsAttachmentResult")]
pub struct GqlAttachmentResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub attachment: Option<GqlContentAttachment>,
}

impl GqlAttachmentResult {
    pub fn found(attachment: AttachmentBody) -> Self {
        Self {
            success: true,
            attachment: Some(GqlContentAttachment {
                name: attachment.name,
                content_type: attachment.content_type,
                content: attachment.content,
            }),
            ..Self::default()
        }
    }

    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
            attachment: None,
        }
    }
}

#[derive(SimpleObject, Clone, Default)]
#[graphql(name = "JobsUnreadResult")]
pub struct GqlUnreadResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub items: Vec<GqlInboxItem>,
}

impl GqlUnreadResult {
    pub fn found(items: Vec<UnreadItem>) -> Self {
        Self {
            success: true,
            items: items.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
            items: Vec::new(),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsMailView")]
pub struct GqlMailView {
    pub mail_id: Uuid,
    pub from: Uuid,
    pub author: String,
    pub source_conversation_id: Uuid,
    pub subject: String,
    pub body: String,
    pub to: Vec<Uuid>,
    pub cc: Vec<Uuid>,
    pub attachments: Vec<GqlMailAttachment>,
}

#[derive(SimpleObject, Clone, Default)]
#[graphql(name = "JobsMailResult")]
pub struct GqlMailResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub mail: Option<GqlMailView>,
}

impl GqlMailResult {
    pub fn found(read: MailRead) -> Self {
        let content = read.view.content;
        Self {
            success: true,
            mail: Some(GqlMailView {
                mail_id: content.mail_id.as_uuid(),
                from: content.from.as_uuid(),
                author: content.author.as_str().to_owned(),
                source_conversation_id: content.source_conversation.as_uuid(),
                subject: content.subject,
                body: content.body,
                to: content.to.iter().map(|address| address.as_uuid()).collect(),
                cc: content.cc.iter().map(|address| address.as_uuid()).collect(),
                attachments: read.attachments.into_iter().map(Into::into).collect(),
            }),
            ..Self::default()
        }
    }

    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
            mail: None,
        }
    }
}
