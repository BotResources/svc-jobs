use async_graphql::{InputObject, Json};
use bc_jobs::domain::payload::PayloadAttachment;
use serde_json::Value;
use uuid::Uuid;

use crate::app::runner::WorkIds;

#[derive(InputObject, Clone)]
#[graphql(name = "JobsWorkInput")]
pub struct GqlWorkInput {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub instance_key: String,
}

impl From<GqlWorkInput> for WorkIds {
    fn from(work: GqlWorkInput) -> Self {
        Self {
            job_id: work.job_id,
            run_id: work.run_id,
            instance_key: work.instance_key,
        }
    }
}

#[derive(InputObject, Clone)]
#[graphql(name = "JobsContentAttachmentInput")]
pub struct GqlContentAttachmentInput {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

impl From<GqlContentAttachmentInput> for PayloadAttachment {
    fn from(attachment: GqlContentAttachmentInput) -> Self {
        Self {
            name: attachment.name,
            content_type: attachment.content_type,
            content: attachment.content,
        }
    }
}

#[derive(InputObject, Clone)]
#[graphql(name = "JobsContentInput")]
pub struct GqlContentInput {
    pub message: String,
    pub attachments: Vec<GqlContentAttachmentInput>,
}

impl GqlContentInput {
    pub fn into_parts(self) -> (String, Vec<PayloadAttachment>) {
        (
            self.message,
            self.attachments.into_iter().map(Into::into).collect(),
        )
    }
}

#[derive(InputObject)]
#[graphql(name = "JobsUploadPayloadInput")]
pub struct GqlUploadPayloadInput {
    pub for_job_id: Uuid,
    pub content: GqlContentInput,
    pub work: Option<GqlWorkInput>,
}

#[derive(InputObject)]
#[graphql(name = "JobsUploadResultInput")]
pub struct GqlUploadResultInput {
    pub work: GqlWorkInput,
    pub answer: GqlContentInput,
}

#[derive(InputObject)]
#[graphql(name = "JobsReadResultInput")]
pub struct GqlReadResultInput {
    pub work: GqlWorkInput,
    pub result_id: Uuid,
    #[graphql(default = false)]
    pub keep: bool,
}

#[derive(InputObject)]
#[graphql(name = "JobsUploadAttachmentInput")]
pub struct GqlUploadAttachmentInput {
    pub work: GqlWorkInput,
    pub mail_id: Uuid,
    pub attachment_id: Uuid,
    pub from_address: Uuid,
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "JobsMailAttachmentInput")]
pub struct GqlMailAttachmentInput {
    pub attachment_id: Uuid,
    pub name: String,
}

#[derive(InputObject)]
#[graphql(name = "JobsSendMailInput")]
pub struct GqlSendMailInput {
    pub work: GqlWorkInput,
    pub mail_id: Uuid,
    pub from_address: Uuid,
    pub author: String,
    pub source_conversation_id: Uuid,
    pub subject: String,
    pub body: String,
    pub to: Vec<Uuid>,
    #[graphql(default)]
    pub cc: Vec<Uuid>,
    #[graphql(default)]
    pub attachments: Vec<GqlMailAttachmentInput>,
}

#[derive(InputObject)]
#[graphql(name = "JobsSaveStateInput")]
pub struct GqlSaveStateInput {
    pub work: GqlWorkInput,
    pub state: Json<Value>,
}
