use async_graphql::{Enum, Json, SimpleObject};
use bc_jobs::domain::mail::inbox::{ItemKind, NoticeType};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::mail_output::GqlMailAttachment;
use bc_jobs::domain::mail::views::UnreadItem;

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsItemKind")]
pub enum GqlItemKind {
    Mail,
    CcNotice,
    Notice,
}

impl From<ItemKind> for GqlItemKind {
    fn from(kind: ItemKind) -> Self {
        match kind {
            ItemKind::Mail => Self::Mail,
            ItemKind::CcNotice => Self::CcNotice,
            ItemKind::Notice => Self::Notice,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsNoticeType")]
pub enum GqlNoticeType {
    Accepted,
    QueuePosition,
    Started,
    RunCompleted,
    JobFailed,
}

impl From<NoticeType> for GqlNoticeType {
    fn from(notice: NoticeType) -> Self {
        match notice {
            NoticeType::Accepted => Self::Accepted,
            NoticeType::QueuePosition => Self::QueuePosition,
            NoticeType::Started => Self::Started,
            NoticeType::RunCompleted => Self::RunCompleted,
            NoticeType::JobFailed => Self::JobFailed,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsInboxItem")]
pub struct GqlInboxItem {
    pub item_id: Uuid,
    pub seq: i64,
    pub item_kind: GqlItemKind,
    pub address: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub accepted_at: DateTime<Utc>,
    pub mail_id: Option<Uuid>,
    pub subject: Option<String>,
    pub from: Option<Uuid>,
    pub author: Option<String>,
    pub source_conversation_id: Option<Uuid>,
    pub body: Option<String>,
    pub attachments: Option<Vec<GqlMailAttachment>>,
    pub notice_type: Option<GqlNoticeType>,
    pub job_id: Option<Uuid>,
    pub position: Option<i32>,
    pub failure_report: Option<Json<serde_json::Value>>,
    pub message: Option<String>,
    pub answer: Option<Json<serde_json::Value>>,
    pub result_id: Option<Uuid>,
}

impl From<UnreadItem> for GqlInboxItem {
    fn from(item: UnreadItem) -> Self {
        let mail = item.mail;
        let notice = item.notice;
        Self {
            item_id: item.item_id.as_uuid(),
            seq: item.seq,
            item_kind: item.kind.into(),
            address: item.address,
            run_id: item.run_id,
            accepted_at: item.accepted_at,
            mail_id: item.mail_id.map(|id| id.as_uuid()),
            subject: item.subject,
            from: mail.as_ref().map(|mail| mail.from.as_uuid()),
            author: mail.as_ref().map(|mail| mail.author.clone()),
            source_conversation_id: mail.as_ref().map(|mail| mail.source_conversation),
            body: mail.as_ref().map(|mail| mail.body.clone()),
            attachments: mail.map(|mail| mail.attachments.into_iter().map(Into::into).collect()),
            notice_type: notice.as_ref().map(|notice| notice.notice_type.into()),
            job_id: notice.as_ref().map(|notice| notice.job_id.as_uuid()),
            position: notice
                .as_ref()
                .and_then(|notice| notice.position)
                .and_then(|position| i32::try_from(position).ok()),
            failure_report: notice
                .as_ref()
                .and_then(|notice| notice.failure_report.clone())
                .map(Json),
            message: notice.as_ref().and_then(|notice| notice.message.clone()),
            answer: notice
                .as_ref()
                .and_then(|notice| notice.answer.clone())
                .map(Json),
            result_id: notice
                .and_then(|notice| notice.result_id)
                .map(|id| id.as_uuid()),
        }
    }
}
