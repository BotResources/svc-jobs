use async_graphql::{Enum, Json, SimpleObject};
use bc_jobs::domain::job::{Job, JobCall};
use bc_jobs::domain::passport::{JobPassport, PassportSubject};
use bc_jobs::domain::payload::{CallMode, JobPayload};
use bc_jobs::domain::requester::Requester;
use uuid::Uuid;

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsPassportSubjectKind")]
pub enum GqlPassportSubjectKind {
    User,
    Service,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobPassport")]
pub struct GqlJobPassport {
    pub subject_kind: GqlPassportSubjectKind,
    pub subject_id: Uuid,
    pub document: Json<serde_json::Value>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsRequesterKind")]
pub enum GqlRequesterKind {
    Service,
    Run,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobRequester")]
pub struct GqlJobRequester {
    pub kind: GqlRequesterKind,
    pub service_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsCallMode")]
pub enum GqlCallMode {
    Sync,
    Background,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobCall")]
pub struct GqlJobCall {
    pub mode: GqlCallMode,
    pub caller_address: Option<Uuid>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsPayloadForm")]
pub enum GqlPayloadForm {
    Inline,
    Kept,
    Reference,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsPayloadAttachment")]
pub struct GqlPayloadAttachment {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobPayload")]
pub struct GqlJobPayload {
    pub form: GqlPayloadForm,
    pub message: Option<String>,
    pub attachments: Vec<GqlPayloadAttachment>,
    pub service: Option<String>,
    pub document_id: Option<String>,
}

pub fn passport_of(job: &Job) -> Option<GqlJobPassport> {
    job.passport().map(passport_view)
}

fn passport_view(passport: &JobPassport) -> GqlJobPassport {
    let (subject_kind, subject_id) = match passport.subject() {
        PassportSubject::User(id) => (GqlPassportSubjectKind::User, id),
        PassportSubject::Service(id) => (GqlPassportSubjectKind::Service, id),
    };
    GqlJobPassport {
        subject_kind,
        subject_id,
        document: Json(passport.document().clone()),
    }
}

pub fn requester_of(job: &Job) -> GqlJobRequester {
    match job.requester() {
        Requester::Service(actor) => GqlJobRequester {
            kind: GqlRequesterKind::Service,
            service_id: actor.map(|actor| actor.as_uuid()),
            run_id: None,
        },
        Requester::Run(run_id) => GqlJobRequester {
            kind: GqlRequesterKind::Run,
            service_id: None,
            run_id: Some(run_id.as_uuid()),
        },
    }
}

pub fn call_of(job: &Job) -> Option<GqlJobCall> {
    job.call().map(call_view)
}

fn call_view(call: &JobCall) -> GqlJobCall {
    GqlJobCall {
        mode: match call.mode {
            CallMode::Sync => GqlCallMode::Sync,
            CallMode::Background => GqlCallMode::Background,
        },
        caller_address: call.caller_address,
    }
}

pub fn payload_of(job: &Job) -> Option<GqlJobPayload> {
    job.payload().map(|payload| match payload {
        JobPayload::Inline {
            message,
            attachments,
        } => GqlJobPayload {
            form: GqlPayloadForm::Inline,
            message: Some(message.clone()),
            attachments: attachments
                .iter()
                .map(|attachment| GqlPayloadAttachment {
                    name: attachment.name.clone(),
                    content_type: attachment.content_type.clone(),
                    content: attachment.content.clone(),
                })
                .collect(),
            service: None,
            document_id: None,
        },
        JobPayload::Kept => GqlJobPayload {
            form: GqlPayloadForm::Kept,
            message: None,
            attachments: Vec::new(),
            service: None,
            document_id: None,
        },
        JobPayload::Reference {
            service,
            document_id,
        } => GqlJobPayload {
            form: GqlPayloadForm::Reference,
            message: None,
            attachments: Vec::new(),
            service: Some(service.clone()),
            document_id: Some(document_id.clone()),
        },
    })
}
