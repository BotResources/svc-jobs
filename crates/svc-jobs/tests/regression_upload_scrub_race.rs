mod support;

use std::time::Duration;

use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::kept_payload::KeptPayload;
use bc_jobs::ports::kept_payload::{KeptPayloads, Uploader};
use chrono::Utc;
use svc_jobs::db::tree_lock::lock_tree;
use uuid::Uuid;

use support::contention::{Fixture, a_queued_job};

const RUNNER_TYPE: &str = "archivist";

async fn a_lock_wait_exists(fixture: &Fixture) -> bool {
    sqlx::query_scalar(
        "SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND NOT l.granted AND a.datname = current_database()",
    )
    .fetch_one(&fixture.pool)
    .await
    .expect("the lock-wait probe runs")
}

#[tokio::test]
async fn a_payload_upload_for_a_job_whose_tree_is_being_scrubbed_is_refused_not_stored() {
    let fixture = Fixture::start().await;
    let actor = ActorRef::new(Uuid::from_u128(0x7e57_0000_0000_7000_8000_0000_0000_0007));
    let job = a_queued_job(&fixture, RUNNER_TYPE).await;
    sqlx::query(
        "UPDATE jobs SET requester = $2, payload = '{\"form\":\"KEPT\"}'::jsonb WHERE id = $1",
    )
    .bind(job.id().as_uuid())
    .bind(serde_json::to_value(Requester::Service(Some(actor))).expect("a requester"))
    .execute(&fixture.pool)
    .await
    .expect("the Job awaits a kept payload of that uploader");
    let mut scrub = fixture.pool.begin().await.expect("the scrub transaction");
    lock_tree(&mut scrub, job.initial_job_id())
        .await
        .expect("the scrub holds the Tree");
    sqlx::query("UPDATE jobs SET payload = NULL, content_erased_at = now() WHERE id = $1")
        .bind(job.id().as_uuid())
        .execute(&mut *scrub)
        .await
        .expect("the scrub erases the request data");

    let upload = fixture.store.upload_payload(
        Uploader::Service(actor),
        job.id().as_uuid(),
        KeptPayload {
            message: "late".to_owned(),
            attachments: Vec::new(),
        },
        Utc::now(),
    );
    let finish_the_scrub = async {
        let mut probes = 0;
        while !a_lock_wait_exists(&fixture).await && probes < 40 {
            probes += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        scrub.commit().await.expect("the scrub ends");
    };
    let (outcome, ()) = tokio::join!(upload, finish_the_scrub);

    assert!(
        matches!(outcome, Ok(Err(_))),
        "the upload is refused once the scrub is committed: {outcome:?}"
    );
    let stored = fixture
        .store
        .stored_payload(job.id().as_uuid())
        .await
        .expect("the stored payload reads");
    assert!(stored.is_none(), "no payload is stored on an erased Job");
    fixture.shutdown().await;
}
