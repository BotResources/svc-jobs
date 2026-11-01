use serde_json::Value;

use super::db::{self, Durable};
use super::fixture::JobsFixture;
use super::producer::{JobDeclaration, Producer};
use super::runner::{self, FakeRunner};
use super::surface;
use super::{LONG, dispatch, wire};

fn leaves(value: &Value, found: &mut Vec<String>) {
    match value {
        Value::String(text) => found.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| leaves(item, found)),
        Value::Object(fields) => fields.values().for_each(|field| leaves(field, found)),
        _ => {}
    }
}

fn assert_names_nothing_of_the_run(reply: &Value, trigger: &Value) {
    let words = reply.to_string().to_lowercase();
    let mut secrets = Vec::new();
    for field in ["job_id", "initial_job_id", "parent_job_id", "passport"] {
        leaves(&trigger[field], &mut secrets);
    }
    for secret in secrets.iter().filter(|secret| secret.len() >= 8) {
        assert!(
            !words.contains(&secret.to_lowercase()),
            "a refusal names nothing of the Job, its parent, its Initial Job or its Passport: \
             {reply} names {secret}"
        );
    }
}

pub fn refused(reply: &Value, reason: &str, trigger: &Value) {
    assert_eq!(surface::reason(reply), reason, "{reply}");
    assert!(
        reply["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "a refusal says why in plain words: {reply}"
    );
    assert_names_nothing_of_the_run(reply, trigger);
}

pub fn accepted(reply: &Value) {
    assert_eq!(reply["accepted"], true, "{reply}");
}

pub struct Scene<'a> {
    pub fixture: &'a JobsFixture,
    pub runner_type: String,
    pub durable: Durable,
}

impl<'a> Scene<'a> {
    pub async fn open(fixture: &'a JobsFixture, label: &str) -> Scene<'a> {
        Scene {
            fixture,
            runner_type: wire::unique_runner_type(label),
            durable: Durable::open(&fixture.app_url()).await,
        }
    }

    pub fn instance(&self, key: &str, capacity: u32) -> FakeRunner<'a> {
        FakeRunner::new(self.fixture.nats(), &self.runner_type, key).declaring_capacity(capacity)
    }

    pub async fn live(&self, instances: usize) {
        dispatch::await_live_instances(
            &self.fixture.gql(),
            self.fixture.admin(),
            &self.runner_type,
            instances,
            LONG,
        )
        .await;
    }

    pub async fn declare(&self) -> JobDeclaration {
        let declaration = JobDeclaration::new(&self.runner_type);
        Producer::new(self.fixture.fabric(), "projects")
            .declare_v2(&declaration)
            .await;
        declaration
    }

    pub async fn ended(&self, runner: &FakeRunner<'_>, trigger: &Value) {
        runner.complete_run(trigger).await;
        self.durable
            .await_count(
                db::TERMINALS_OF_RUN,
                runner::run_id(trigger),
                1,
                "the end of the run",
            )
            .await;
    }
}
