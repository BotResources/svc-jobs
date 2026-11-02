mod support;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use support::contention::Fixture;
use support::scrub_race::{a_queued_job_under, failed};

async fn settled_at(fixture: &Fixture, job: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT settled_at FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(&fixture.pool)
        .await
        .expect("the settled time reads")
}

#[tokio::test]
async fn the_resolution_of_a_job_settles_it_in_the_same_transaction() {
    let fixture = Fixture::start().await;
    let queued = a_queued_job_under(&fixture, None).await;
    let job = queued.id().as_uuid();
    assert_eq!(settled_at(&fixture, job).await, None);

    let resolved = failed(&fixture, queued).await;

    let resolution = resolved.resolution().expect("the Job is resolved");
    assert_eq!(
        settled_at(&fixture, job).await,
        Some(resolution.occurred_at())
    );
    fixture.shutdown().await;
}
