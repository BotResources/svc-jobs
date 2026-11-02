use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use super::LONG;
use super::db::Durable;
use super::fixture::{JobsFixture, Knobs};
use super::gesture::Work;
use super::lineage::live_runner;
use super::producer::{JobDeclaration, Producer};
use super::runner::FakeRunner;
use super::scrub_scene::long_runs;

pub fn three_days() -> Knobs {
    Knobs {
        cancelled_retention_days: Some("3".to_owned()),
        ..long_runs()
    }
}

pub async fn age_cancel(durable: &Durable, job: Uuid, days: i64) {
    sqlx::query(
        "UPDATE job_resolutions SET occurred_at = now() - make_interval(days => $2::int) \
         WHERE job_id = $1 AND kind = 'CANCELLED'",
    )
    .bind(job)
    .bind(i32::try_from(days).unwrap())
    .execute(durable.pool())
    .await
    .expect("the cancel is aged");
}

pub async fn erased_at(durable: &Durable, job: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT content_erased_at FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(durable.pool())
        .await
        .expect("the time of the erasure reads")
}

pub struct Trees<'a> {
    pub root: Uuid,
    pub root_work: Work,
    pub _runner: FakeRunner<'a>,
}

pub async fn root_with_request_data<'a>(fixture: &'a JobsFixture, label: &str) -> Trees<'a> {
    let mut runner = live_runner(fixture, label).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let declaration = JobDeclaration::new(&runner.runner_type)
        .with_config(json!({ "model": "m" }))
        .with_max_attempts(1);
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "INLINE", "message": "the brief", "attachments": [] });
    producer.declare_payload(body).await;
    let trigger = runner.next_trigger(LONG).await;
    runner.start_applied(fixture, &trigger).await;
    Trees {
        root: declaration.job_id,
        root_work: Work::of(&runner, &trigger),
        _runner: runner,
    }
}
