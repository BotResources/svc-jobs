use async_graphql::{Json, SimpleObject};
use bc_jobs::domain::payload::PayloadAttachment;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::views::SavedStateView;
use serde_json::Value;
use uuid::Uuid;

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
