mod support;

use contract_jobs::event::{JobFailed, JobPlanDeclared, JobStepStarted};
use contract_jobs::event_v2::{JobFailedV2, JobPlanDeclaredV2, JobStepStartedV2};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::Row;
use support::db::Durable;
use support::erasure_scene::build;
use support::fixture::JobsFixture;
use support::scrub::{age_tree, await_scrubbed};
use support::scrub_scene::finish;
use uuid::Uuid;

const PUBLISHED_OF_JOB: &str = "SELECT subject, payload -> 'payload' FROM integration_outbox \
     WHERE status = 'PUBLISHED' AND subject = $1 AND payload -> 'payload' ->> 'job_id' = $2";
const PENDING_OF_TREE: &str = "SELECT count(*) FROM integration_outbox WHERE status <> 'PUBLISHED'";

async fn published<T: DeserializeOwned>(
    durable: &Durable,
    kind: &str,
    version: u8,
    job: Uuid,
) -> Vec<T> {
    sqlx::query(PUBLISHED_OF_JOB)
        .bind(format!("integration.evt.jobs.job.{kind}.v{version}"))
        .bind(job.to_string())
        .fetch_all(durable.pool())
        .await
        .expect("the published rows read")
        .into_iter()
        .map(|row| {
            let body: Value = row.get(1);
            serde_json::from_value(body.clone())
                .unwrap_or_else(|error| panic!("a published {kind} row decodes: {error}: {body}"))
        })
        .collect()
}

#[tokio::test]
async fn the_published_events_of_a_scrubbed_tree_keep_their_shape_and_lose_their_text() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let s = build(&fixture, &durable, "scrub-events").await;
    finish(&fixture, &durable, &s).await;
    let waited = tokio::time::Instant::now();
    while sqlx::query_scalar::<_, i64>(PENDING_OF_TREE)
        .fetch_one(durable.pool())
        .await
        .unwrap()
        > 0
    {
        assert!(waited.elapsed() < support::LONG, "the outbox drains");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let staged = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO integration_outbox (id, subject, payload, status, published_at) \
         VALUES ($1, 'integration.evt.jobs.job.queued.v1', $2, 'PUBLISHED', now())",
    )
    .bind(staged)
    .bind(json!({
        "payload": {
            "job_id": s.e.to_string(),
            "runner_type": "kept",
            "config": { "model": "m" },
            "passport": { "subject": "someone" },
            "payload": { "message": "the brief" },
        },
        "event_type": "job.queued",
        "version": 1,
    }))
    .execute(durable.pool())
    .await
    .expect("a row is staged");

    let plans_before: Vec<JobPlanDeclared> = published(&durable, "plan_declared", 1, s.a).await;
    assert_eq!(plans_before[0].steps, ["first step", "second step"]);
    let failures_before: Vec<JobFailed> = published(&durable, "failed", 1, s.b).await;
    assert!(
        !failures_before[0]
            .failure_report
            .as_ref()
            .expect("a report")
            .diagnostic
            .is_null()
    );

    age_tree(&durable, s.a, 31).await;
    await_scrubbed(&durable, s.a, "the Tree is scrubbed").await;

    let plans: Vec<JobPlanDeclared> = published(&durable, "plan_declared", 1, s.a).await;
    let plans_v2: Vec<JobPlanDeclaredV2> = published(&durable, "plan_declared", 2, s.a).await;
    assert_eq!(plans.len(), plans_before.len());
    assert_eq!(plans[0].steps, ["erased", "erased"]);
    assert_eq!(plans_v2[0].steps, ["erased", "erased"]);
    let steps: Vec<JobStepStarted> = published(&durable, "step_started", 1, s.a).await;
    let steps_v2: Vec<JobStepStartedV2> = published(&durable, "step_started", 2, s.a).await;
    assert_eq!(steps[0].label, "erased");
    assert_eq!(steps_v2[0].label, "erased");
    let failures: Vec<JobFailed> = published(&durable, "failed", 1, s.b).await;
    let failures_v2: Vec<JobFailedV2> = published(&durable, "failed", 2, s.b).await;
    for report in [
        failures[0].failure_report.clone().expect("a report"),
        failures_v2[0].failure_report.clone().expect("a report"),
    ] {
        assert_eq!(report.reason_code, "provider_refused");
        assert_eq!(report.params, json!({}));
        assert_eq!(report.diagnostic, json!({}));
    }
    let row: Value = sqlx::query_scalar("SELECT payload FROM integration_outbox WHERE id = $1")
        .bind(staged)
        .fetch_one(durable.pool())
        .await
        .unwrap();
    for key in ["config", "passport", "payload"] {
        assert!(row["payload"].get(key).is_none(), "{key} stays: {row}");
    }
    assert_eq!(row["payload"]["runner_type"], "kept", "{row}");
    durable.close().await;
    fixture.shutdown().await;
}
