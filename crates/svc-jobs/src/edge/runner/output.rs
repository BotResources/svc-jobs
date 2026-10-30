use async_graphql::{Json, SimpleObject};
use bc_jobs::domain::mail::refusal::BadAddress;
use bc_jobs::domain::payload::PayloadAttachment;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use serde_json::Value;
use uuid::Uuid;

use super::unread::GqlInboxItem;
use crate::db::inbox::UnreadItem;
use crate::db::mail::{AttachmentContent, AttachmentMeta as MailAttachmentMeta, MailRead};
use crate::db::saved_states::SavedStateView;

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsContentAttachment")]
pub struct GqlContentAttachment {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

impl From<PayloadAttachment> for GqlContentAttachment {
    fn from(attachment: PayloadAttachment) -> Self {
        Self {
            name: attachment.name,
            content_type: attachment.content_type,
            content: attachment.content,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsContent")]
pub struct GqlContent {
    pub message: String,
    pub attachments: Vec<GqlContentAttachment>,
}

impl GqlContent {
    pub fn new(message: String, attachments: Vec<PayloadAttachment>) -> Self {
        Self {
            message,
            attachments: attachments.into_iter().map(Into::into).collect(),
        }
    }
}

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
#[graphql(name = "JobsRunnerResult")]
pub struct GqlRunnerResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl GqlRunnerResult {
    pub fn ok() -> Self {
        Self {
            success: true,
            ..Self::default()
        }
    }

    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
        }
    }
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
#[graphql(name = "JobsContentResult")]
pub struct GqlContentResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub content: Option<GqlContent>,
}

impl GqlContentResult {
    pub fn found(content: GqlContent) -> Self {
        Self {
            success: true,
            content: Some(content),
            ..Self::default()
        }
    }

    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
            content: None,
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
    pub fn found(attachment: AttachmentContent) -> Self {
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

#[derive(SimpleObject, Clone, Default)]
#[graphql(name = "JobsSavedStateResult")]
pub struct GqlSavedStateResult {
    pub success: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub state: Option<Json<Value>>,
    pub agent_ids: Vec<Uuid>,
    pub mail_addresses: Vec<Uuid>,
}

impl GqlSavedStateResult {
    pub fn found(saved: SavedStateView) -> Self {
        Self {
            success: true,
            state: Some(Json(saved.state)),
            agent_ids: saved.agent_ids.iter().map(|id| id.as_uuid()).collect(),
            mail_addresses: saved.mail_addresses.iter().map(|id| id.as_uuid()).collect(),
            ..Self::default()
        }
    }

    pub fn refused(refusal: &RunnerRefusal) -> Self {
        Self {
            success: false,
            reason: Some(refusal.code().to_owned()),
            message: Some(refusal.message().to_owned()),
            ..Self::default()
        }
    }
}
