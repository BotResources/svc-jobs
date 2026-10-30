mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::tree::correspondents;
use uuid::Uuid;

async fn keep_a_result(fixture: &JobsFixture, work: &Work, durable: &Durable) -> Uuid {
    let uploaded = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": work.wire(), "answer": gesture::content("the answer", &[]) } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
    sqlx::query_scalar("SELECT result_id::uuid FROM job_results WHERE job_id = $1")
        .bind(work.job_id)
        .fetch_one(durable.pool())
        .await
        .expect("the answer is kept as a Result")
}

#[tokio::test]
async fn a_result_nobody_reads_is_gone_after_seven_days_and_is_purged() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = correspondents(&fixture, "result-expiry").await;
    let result_id = keep_a_result(&fixture, &tree.child_work, &durable).await;
    let expiry_from_keeping: bool = sqlx::query_scalar(
        "SELECT expires_at = kept_at + interval '7 days' FROM job_results WHERE result_id = $1",
    )
    .bind(result_id)
    .fetch_one(durable.pool())
    .await
    .expect("the expiry reads");
    assert!(
        expiry_from_keeping,
        "the service stamps seven days from the keeping"
    );

    sqlx::query(
        "UPDATE job_results SET kept_at = now() - interval '8 days', \
         expires_at = now() - interval '1 day' WHERE result_id = $1",
    )
    .bind(result_id)
    .execute(durable.pool())
    .await
    .expect("the Result is aged");

    let read = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::READ_RESULT,
            json!({ "input": { "work": tree.parent_work.wire(), "resultId": result_id.to_string() } }),
        )
        .await;
    assert_eq!(gesture::refusal(&read, "jobsReadResult"), "result_unknown");
    let purged = br_test_harness::wait_until(support::LONG, || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_results WHERE result_id = $1")
            .bind(result_id)
            .fetch_one(durable.pool())
            .await
            .is_ok_and(|left| left == 0)
    })
    .await;
    assert!(purged, "the sweep erases an expired Result");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_result_read_with_keep_lives_seven_days_from_that_read() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = correspondents(&fixture, "result-keep").await;
    let result_id = keep_a_result(&fixture, &tree.child_work, &durable).await;
    let client = fixture.gql();
    let passport = runner_access();
    let read = |keep: bool| {
        let client = &client;
        let passport = &passport;
        let work = tree.parent_work.wire();
        async move {
            client
                .query(
                    passport,
                    gesture::READ_RESULT,
                    json!({ "input": { "work": work, "resultId": result_id.to_string(), "keep": keep } }),
                )
                .await
        }
    };

    gesture::accepted(&read(true).await, "jobsReadResult");
    let expiry_from_read: bool = sqlx::query_scalar(
        "SELECT read_at IS NOT NULL AND expires_at = read_at + interval '7 days' \
         FROM job_results WHERE result_id = $1",
    )
    .bind(result_id)
    .fetch_one(durable.pool())
    .await
    .expect("the expiry reads");
    assert!(
        expiry_from_read,
        "a read with keep stamps seven days from that read"
    );
    sqlx::query(
        "UPDATE job_results SET kept_at = now() - interval '9 days', \
         read_at = now() - interval '3 days', expires_at = now() + interval '4 days' \
         WHERE result_id = $1",
    )
    .bind(result_id)
    .execute(durable.pool())
    .await
    .expect("the Result is aged");
    gesture::accepted(&read(true).await, "jobsReadResult");

    sqlx::query(
        "UPDATE job_results SET read_at = now() - interval '8 days', \
         expires_at = now() - interval '1 day' WHERE result_id = $1",
    )
    .bind(result_id)
    .execute(durable.pool())
    .await
    .expect("the read is aged");
    let expired = read(false).await;
    assert_eq!(
        gesture::refusal(&expired, "jobsReadResult"),
        "result_unknown"
    );
    durable.close().await;
    fixture.shutdown().await;
}
