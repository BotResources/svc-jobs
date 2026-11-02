mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::db::Durable;
use support::erasure::assert_request_data_erased;
use support::fixture::JobsFixture;
use support::lineage::{create, live_runner};
use support::retention::{age_cancel, root_with_request_data, three_days};
use support::{JOB_CHANGED, LONG, SHORT, gql, stream, subs};
use uuid::Uuid;

#[tokio::test]
async fn an_open_job_subscription_gets_the_erased_view_when_the_sweep_erases_the_request_data() {
    let fixture = JobsFixture::start_with(three_days()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = root_with_request_data(&fixture, "retention-sub").await;
    let mut b_runner = live_runner(&fixture, "retention-sub-b").await;
    let body = json!({ "mode": "SYNC", "max_attempts": 1, "config": { "model": "m" },
        "payload": { "form": "INLINE", "message": "the brief", "attachments": [] } });
    let b = create(
        &fixture,
        &tree.root_work,
        &b_runner.runner_type,
        tree.root,
        body,
    )
    .await;
    let b_trigger = b_runner.next_trigger(LONG).await;
    b_runner.start_applied(&fixture, &b_trigger).await;
    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), b).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), b, "CANCELLED", LONG).await;

    let mut watch =
        SseSubscription::open(fixture.url(), fixture.admin(), &subs::job_request_data(b)).await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    assert!(opening["job"]["passport"].is_object(), "{opening}");
    assert!(opening["job"]["payload"].is_object(), "{opening}");
    assert!(opening["job"]["contentErasedAt"].is_null(), "{opening}");

    age_cancel(&durable, b, 4).await;
    let delta =
        stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobContentErasedEvent", LONG).await;
    assert_eq!(delta["event"]["jobId"], json!(b.to_string()));
    assert!(delta["event"]["erasedAt"].is_string(), "{delta}");
    assert!(delta["job"]["passport"].is_null(), "{delta}");
    assert!(delta["job"]["payload"].is_null(), "{delta}");
    assert!(delta["job"]["config"].is_null(), "{delta}");
    assert_eq!(
        delta["job"]["contentErasedAt"], delta["event"]["erasedAt"],
        "the view carries the time of the first erasure: {delta}"
    );
    assert_eq!(delta["job"]["status"], json!("CANCELLED"));
    assert_request_data_erased(&fixture, &durable, b, "B").await;
    durable.close().await;
    fixture.shutdown().await;
}
