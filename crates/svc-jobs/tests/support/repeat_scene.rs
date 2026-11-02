use serde_json::{Value, json};
use uuid::Uuid;

use super::events::{EventLog, ObservedEvent};
use super::fixture::{JobsFixture, Knobs};
use super::producer::{JobDeclaration, Producer};
use super::{LONG, gql, wire};

pub struct Scene {
    pub fixture: JobsFixture,
    pub first: EventLog,
    pub current: EventLog,
}

impl Scene {
    pub async fn start() -> Self {
        Self::start_with(Knobs::default()).await
    }

    pub async fn start_with(knobs: Knobs) -> Self {
        Self::around(JobsFixture::start_with(knobs).await).await
    }

    pub async fn start_cluster(instance_count: usize) -> Self {
        Self::around(JobsFixture::start_cluster(instance_count, Knobs::default()).await).await
    }

    async fn around(fixture: JobsFixture) -> Self {
        Self {
            first: EventLog::open(fixture.fabric()).await,
            current: EventLog::open_v2(fixture.fabric()).await,
            fixture,
        }
    }

    pub fn producer(&self, bc: &str) -> Producer<'_> {
        Producer::new(self.fixture.fabric(), bc)
    }

    pub async fn declared_by(&self, producer: &Producer<'_>) -> Uuid {
        let declaration = JobDeclaration::new(&wire::unique_runner_type("repeated"));
        producer.declare(&declaration).await;
        self.current
            .expect_exactly(wire::FACT_QUEUED, declaration.job_id, 1, LONG)
            .await;
        declaration.job_id
    }

    pub async fn status(&self, job: Uuid) -> String {
        gql::status_of(&self.fixture.gql(), self.fixture.admin(), job).await
    }

    pub async fn refusals(&self, job: Uuid, count: usize) -> Vec<String> {
        self.current
            .expect_exactly(wire::FACT_COMMAND_REJECTED, job, count, LONG)
            .await
            .iter()
            .map(|refusal| {
                refusal.payload()["reason_code"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    }

    pub async fn stop(self) {
        self.first.stop().await;
        self.current.stop().await;
        self.fixture.shutdown().await;
    }
}

pub fn payload(job: Uuid) -> Value {
    json!({ "job_id": job.to_string() })
}

pub fn event_ids(events: &[ObservedEvent]) -> Vec<String> {
    let mut ids: Vec<String> = events
        .iter()
        .map(|event| {
            event.envelope["event_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

pub fn correlations(events: &[ObservedEvent]) -> Vec<String> {
    events.iter().map(ObservedEvent::correlation_id).collect()
}
