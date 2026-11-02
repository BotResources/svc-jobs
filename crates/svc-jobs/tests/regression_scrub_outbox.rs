mod support;

use bc_jobs::domain::erasure::ERASED_TEXT;
use bc_jobs::ports::scrub::ScrubOutcome;
use serde_json::{Value, json};
use uuid::Uuid;

use support::contention::Fixture;
use support::scrub_race::{a_failed_job, scrub};

fn event_of(job_id: Uuid) -> Value {
    json!({
        "payload": {
            "job_id": job_id.to_string(),
            "kept": "stays",
            "config": {"a": 1},
            "passport": {"b": 2},
            "payload": {"c": 3},
            "text": "t",
            "subject": "s",
            "body": "b",
            "note": "n",
            "label": "a label",
            "steps": ["first", "second"],
            "params": {"d": 4},
            "failure_report": {"params": {"e": 5}, "diagnostic": {"f": 6}, "kind": "PERMANENT"}
        }
    })
}

async fn outbox(fixture: &Fixture, status: &str, event: &Value) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO integration_outbox (id, subject, payload, status, published_at) \
         VALUES ($1, 'integration.evt.jobs.job.failed.v1', $2, $3, \
                 CASE WHEN $3 = 'PUBLISHED' THEN now() END)",
    )
    .bind(id)
    .bind(event)
    .bind(status)
    .execute(&fixture.pool)
    .await
    .expect("the outbox row inserts");
    id
}

async fn payload_of(fixture: &Fixture, id: Uuid) -> Value {
    sqlx::query_scalar("SELECT payload FROM integration_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(&fixture.pool)
        .await
        .expect("the outbox row reads")
}

#[tokio::test]
async fn the_published_events_of_a_scrubbed_tree_lose_exactly_their_text_and_nothing_else_changes()
{
    let fixture = Fixture::start().await;
    let scrubbed = a_failed_job(&fixture).await;
    let other = a_failed_job(&fixture).await;
    let published = outbox(&fixture, "PUBLISHED", &event_of(scrubbed.id().as_uuid())).await;
    let pending = outbox(&fixture, "PENDING", &event_of(scrubbed.id().as_uuid())).await;
    let of_another_tree = outbox(&fixture, "PUBLISHED", &event_of(other.id().as_uuid())).await;

    assert_eq!(
        scrub(&fixture, scrubbed.id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );

    assert_eq!(
        payload_of(&fixture, published).await,
        json!({
            "payload": {
                "job_id": scrubbed.id().as_uuid().to_string(),
                "kept": "stays",
                "label": ERASED_TEXT,
                "steps": [ERASED_TEXT, ERASED_TEXT],
                "params": {},
                "failure_report": {"params": {}, "diagnostic": {}, "kind": "PERMANENT"}
            }
        })
    );
    assert_eq!(
        payload_of(&fixture, pending).await,
        event_of(scrubbed.id().as_uuid()),
        "an event that waits to be published is published as it was"
    );
    assert_eq!(
        payload_of(&fixture, of_another_tree).await,
        event_of(other.id().as_uuid())
    );
    fixture.shutdown().await;
}
