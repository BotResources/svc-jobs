use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::content::MailContent;
use super::delivery::RecipientKind;
use super::inbox::{ItemKind, Notice};
use crate::domain::ids::{AgentId, AttachmentId, ItemId, MailId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentMeta {
    pub attachment_id: AttachmentId,
    pub name: String,
    pub content_type: String,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct MailView {
    pub content: MailContent,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MailRead {
    pub view: MailView,
    pub attachments: Vec<AttachmentMeta>,
    pub read_as: RecipientKind,
}

#[derive(Debug, Clone)]
pub struct UnreadMail {
    pub mail_id: MailId,
    pub from: AgentId,
    pub author: String,
    pub source_conversation: Uuid,
    pub subject: String,
    pub body: String,
    pub attachments: Vec<AttachmentMeta>,
}

#[derive(Debug, Clone)]
pub struct UnreadItem {
    pub item_id: ItemId,
    pub seq: i64,
    pub kind: ItemKind,
    pub address: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub accepted_at: DateTime<Utc>,
    pub mail_id: Option<MailId>,
    pub subject: Option<String>,
    pub mail: Option<UnreadMail>,
    pub notice: Option<Notice>,
}
