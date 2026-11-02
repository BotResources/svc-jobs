mod support;

use bc_jobs::event::job::JobEvent;
use contract_jobs::event::{JobFailed, JobPlanDeclared, JobStepStarted};
use contract_jobs::event_v2::{JobFailedV2, JobPlanDeclaredV2, JobStepStartedV2};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::Row;
use support::db::Durable;
use support::erasure_scene::build;
use support::fixture::JobsFixture;
use support::scrub::{age_tree, await_scrubbed};
use support::scrub_scene::{end_run_and_finish, finish, live_root_for, long_runs};
use uuid::Uuid;

const PUBLISHED_OF_JOB: &str = "SELECT subject, payload -> 'payload' FROM integration_outbox \
     WHERE status = 'PUBLISHED' AND subject = $1 AND payload -> 'payload' ->> 'job_id' = $2";
const EVENTS_OF_TREE: &str = "SELECT event_type, payload FROM domain_events \
     WHERE aggregate_type = 'Job' \
       AND aggregate_id IN (SELECT id FROM jobs WHERE root_job_id = $1) ORDER BY id";
const PENDING_OF_TREE: &str = "SELECT count(*) FROM integration_outbox WHERE status <> 'PUBLISHED'";

async fn recorded_events(durable: &Durable, root: Uuid) -> Vec<(String, Value)> {
    let rows = sqlx::query(EVENTS_OF_TREE)
        .bind(root)
        .fetch_all(durable.pool())
        .await
        .expect("the recorded events read");
    assert!(!rows.is_empty(), "the Tree has recorded events");
    rows.into_iter()
        .map(|row| (row.get::<String, _>(0), row.get::<Value, _>(1)))
        .collect()
}

fn assert_every_event_decodes(events: &[(String, Value)]) {
    for (event_type, payload) in events {
        JobEvent::decode(event_type, payload.clone()).unwrap_or_else(|error| {
            panic!("a recorded {event_type} decodes after the scrub: {error}: {payload}")
        });
    }
}

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

    assert_every_event_decodes(&recorded_events(&durable, s.a).await);
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

    let after = recorded_events(&durable, s.a).await;
    assert_every_event_decodes(&after);
    assert!(
        after
            .iter()
            .any(|(event_type, _)| event_type == "JobContentErased"),
        "the scrub recorded the erasure"
    );
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

#[tokio::test]
async fn the_name_of_the_user_recorded_with_a_creation_is_replaced_and_the_event_still_decodes() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let user = Uuid::now_v7();
    let live = live_root_for(&fixture, "scrub-user", 1, Some(user)).await;
    end_run_and_finish(&fixture, &durable, &live).await;

    let queued = |events: &[(String, Value)]| {
        events
            .iter()
            .find(|(event_type, _)| event_type == "JobQueued")
            .map(|(_, payload)| payload.clone())
            .expect("the Job was queued")
    };
    let before = queued(&recorded_events(&durable, live.root).await);
    assert_eq!(
        before["triggered_by"]["id"],
        json!(user.to_string()),
        "{before}"
    );
    assert_ne!(
        before["triggered_by"]["display_name"],
        json!("erased"),
        "{before}"
    );
    assert!(
        before["config"].is_object() && before["passport"].is_object(),
        "{before}"
    );

    age_tree(&durable, live.root, 31).await;
    await_scrubbed(&durable, live.root, "the Tree is scrubbed").await;

    let events = recorded_events(&durable, live.root).await;
    assert_every_event_decodes(&events);
    let after = queued(&events);
    assert_eq!(
        after["triggered_by"]["id"],
        json!(user.to_string()),
        "{after}"
    );
    assert_eq!(
        after["triggered_by"]["display_name"],
        json!("erased"),
        "{after}"
    );
    for key in ["config", "passport", "payload"] {
        assert!(after.get(key).is_none(), "{key} stays: {after}");
    }
    durable.close().await;
    fixture.shutdown().await;
}
