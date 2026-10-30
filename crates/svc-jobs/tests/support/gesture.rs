use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, TestNats};
use serde_json::{Value, json};
use uuid::Uuid;

use super::runner::{self, FakeRunner};

#[derive(Clone)]
pub struct Work {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub instance_key: String,
}

impl Work {
    pub fn of(runner: &FakeRunner<'_>, trigger: &Value) -> Self {
        Self {
            job_id: runner::job_id(trigger),
            run_id: runner::run_id(trigger),
            instance_key: runner.instance_key.clone(),
        }
    }

    pub fn wire(&self) -> Value {
        json!({
            "jobId": self.job_id.to_string(),
            "runId": self.run_id.to_string(),
            "instanceKey": self.instance_key,
        })
    }

    pub fn as_instance(&self, instance_key: &str) -> Self {
        Self {
            instance_key: instance_key.to_owned(),
            ..self.clone()
        }
    }
}

pub const UPLOAD_PAYLOAD: &str = "mutation($input: JobsUploadPayloadInput!) { jobsUploadPayload(input: $input) { success reason message } }";
pub const PAYLOAD: &str = "query($work: JobsWorkInput!) { jobsPayload(work: $work) { success reason message content { message attachments { name contentType content } } } }";
pub const UPLOAD_RESULT: &str = "mutation($input: JobsUploadResultInput!) { jobsUploadResult(input: $input) { success reason message } }";
pub const READ_RESULT: &str = "mutation($input: JobsReadResultInput!) { jobsReadResult(input: $input) { success reason message content { message attachments { name contentType content } } } }";
pub const UPLOAD_ATTACHMENT: &str = "mutation($input: JobsUploadAttachmentInput!) { jobsUploadAttachment(input: $input) { success reason message } }";
pub const ATTACHMENT: &str = "query($work: JobsWorkInput!, $mailId: UUID!, $attachmentId: UUID!) { jobsAttachment(work: $work, mailId: $mailId, attachmentId: $attachmentId) { success reason message attachment { name contentType content } } }";
pub const SEND_MAIL: &str = "mutation($input: JobsSendMailInput!) { jobsSendMail(input: $input) { success reason message badAddresses { address reason message } restartRefused { address reason message } } }";
pub const UNREAD: &str = "query($work: JobsWorkInput!, $address: UUID) { jobsUnread(work: $work, address: $address) { success reason message items { itemId seq itemKind address runId mailId subject from author sourceConversationId body attachments { attachmentId name contentType size } noticeType jobId position message resultId failureReport answer } } }";
pub const MAIL: &str = "query($work: JobsWorkInput!, $mailId: UUID!, $address: UUID!) { jobsMail(work: $work, mailId: $mailId, address: $address) { success reason message mail { mailId from author sourceConversationId subject body to cc attachments { attachmentId name contentType size } } } }";
pub const SAVE_STATE: &str = "mutation($input: JobsSaveStateInput!) { jobsSaveState(input: $input) { success reason message } }";
pub const SAVED_STATE: &str = "query($work: JobsWorkInput!) { jobsSavedState(work: $work) { success reason message state agentIds mailAddresses } }";

pub fn content(message: &str, attachments: &[(&str, &str, &str)]) -> Value {
    json!({
        "message": message,
        "attachments": attachments
            .iter()
            .map(|(name, content_type, content)| json!({
                "name": name, "contentType": content_type, "content": content,
            }))
            .collect::<Vec<_>>(),
    })
}

pub async fn call(gql: &GraphqlClient, passport: &Passport, document: &str, vars: Value) -> Value {
    gql.query(passport, document, vars).await
}

pub fn field<'a>(response: &'a Value, name: &str) -> &'a Value {
    assert!(
        response.get("errors").is_none_or(Value::is_null),
        "the gesture {name} answered an error: {response}"
    );
    &response["data"][name]
}

pub fn refusal(response: &Value, name: &str) -> String {
    let outcome = field(response, name);
    assert_eq!(outcome["success"], false, "{name} was refused: {response}");
    outcome["reason"].as_str().unwrap_or_default().to_owned()
}

pub fn accepted(response: &Value, name: &str) {
    assert_eq!(
        field(response, name)["success"],
        true,
        "{name} was accepted: {response}"
    );
}

pub async fn upload_payload(
    gql: &GraphqlClient,
    passport: &Passport,
    for_job_id: Uuid,
    message: &str,
    work: Option<&Work>,
) -> Value {
    let mut input = json!({
        "forJobId": for_job_id.to_string(),
        "content": content(message, &[("note.txt", "text/plain", "hello")]),
    });
    if let Some(work) = work {
        input["work"] = work.wire();
    }
    call(gql, passport, UPLOAD_PAYLOAD, json!({ "input": input })).await
}

pub async fn payload(gql: &GraphqlClient, passport: &Passport, work: &Work) -> Value {
    call(gql, passport, PAYLOAD, json!({ "work": work.wire() })).await
}

pub struct Mail {
    pub mail_id: Uuid,
    pub from: Uuid,
    pub to: Vec<Uuid>,
    pub cc: Vec<Uuid>,
    pub subject: String,
    pub body: String,
    pub attachments: Vec<(Uuid, String)>,
}

impl Mail {
    pub fn new(from: Uuid, to: &[Uuid]) -> Self {
        Self {
            mail_id: Uuid::now_v7(),
            from,
            to: to.to_vec(),
            cc: Vec::new(),
            subject: "subject".to_owned(),
            body: "body".to_owned(),
            attachments: Vec::new(),
        }
    }

    pub fn cc(mut self, cc: &[Uuid]) -> Self {
        self.cc = cc.to_vec();
        self
    }

    pub fn with_body(mut self, body: &str) -> Self {
        self.body = body.to_owned();
        self
    }

    pub fn with_attachment(mut self, attachment_id: Uuid, name: &str) -> Self {
        self.attachments.push((attachment_id, name.to_owned()));
        self
    }

    pub fn input(&self, work: &Work) -> Value {
        json!({
            "work": work.wire(),
            "mailId": self.mail_id.to_string(),
            "fromAddress": self.from.to_string(),
            "author": "the author",
            "sourceConversationId": Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001).to_string(),
            "subject": self.subject,
            "body": self.body,
            "to": self.to.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "cc": self.cc.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "attachments": self.attachments.iter().map(|(id, name)| json!({
                "attachmentId": id.to_string(), "name": name,
            })).collect::<Vec<_>>(),
        })
    }
}

pub async fn send_mail(
    gql: &GraphqlClient,
    passport: &Passport,
    work: &Work,
    mail: &Mail,
) -> Value {
    call(
        gql,
        passport,
        SEND_MAIL,
        json!({ "input": mail.input(work) }),
    )
    .await
}

pub async fn upload_attachment(
    gql: &GraphqlClient,
    passport: &Passport,
    work: &Work,
    mail: &Mail,
    attachment_id: Uuid,
    name: &str,
    content: &str,
) -> Value {
    call(
        gql,
        passport,
        UPLOAD_ATTACHMENT,
        json!({ "input": {
            "work": work.wire(),
            "mailId": mail.mail_id.to_string(),
            "attachmentId": attachment_id.to_string(),
            "fromAddress": mail.from.to_string(),
            "name": name,
            "contentType": "text/plain",
            "content": content,
        } }),
    )
    .await
}

pub async fn unread(
    gql: &GraphqlClient,
    passport: &Passport,
    work: &Work,
    address: Option<Uuid>,
) -> Value {
    call(
        gql,
        passport,
        UNREAD,
        json!({ "work": work.wire(), "address": address.map(|a| a.to_string()) }),
    )
    .await
}

pub fn items(response: &Value) -> Vec<Value> {
    field(response, "jobsUnread")["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

pub async fn read_mail(
    gql: &GraphqlClient,
    passport: &Passport,
    work: &Work,
    mail_id: Uuid,
    address: Uuid,
) -> Value {
    call(
        gql,
        passport,
        MAIL,
        json!({ "work": work.wire(), "mailId": mail_id.to_string(), "address": address.to_string() }),
    )
    .await
}

pub async fn mark_read(
    nats: &TestNats,
    runner: &FakeRunner<'_>,
    run_id: Uuid,
    items: &[Uuid],
) -> Value {
    let reply = tokio::time::timeout(
        super::LONG,
        nats.client().request(
            format!("jobs.runner.mark_read.{}", runner.runner_type),
            json!({
                "version": 1,
                "run_id": run_id.to_string(),
                "instance_key": runner.instance_key,
                "item_ids": items.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            })
            .to_string()
            .into(),
        ),
    )
    .await
    .expect("Jobs answers a read marker")
    .expect("a read marker finds a responder");
    serde_json::from_slice(&reply.payload).expect("a surface reply is JSON")
}
