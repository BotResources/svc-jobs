mod support;

use br_test_harness::SseSubscription;
use support::db::Durable;
use support::delta;
use support::fixture::JobsFixture;
use support::producer::Producer;
use support::scrub::{age_tree, await_scrubbed};
use support::scrub_scene::{ENDED_RUN, live_root, long_runs};
use support::{JOB_CHANGED, JOBS_CHANGED, LONG, SHORT, gql, stream, subs};

#[tokio::test]
async fn subscribers_of_a_failed_job_receive_the_erased_view_and_a_retry_blocked_by_the_scrub() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "scrub-subs", 1).await;
    live.runner.complete_run(&live.trigger).await;
    durable
        .await_count(ENDED_RUN, live.work.run_id, 1, "the Run ends")
        .await;
    Producer::new(fixture.fabric(), "projects")
        .fail_v2(live.root, Some("the producer gave up"))
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), live.root, "FAILED", LONG).await;

    let mut detail = SseSubscription::open(
        fixture.url(),
        fixture.admin(),
        &subs::job_changed(live.root),
    )
    .await;
    let mut request_data = SseSubscription::open(
        fixture.url(),
        fixture.admin(),
        &subs::job_request_data(live.root),
    )
    .await;
    let mut listing = SseSubscription::open(
        fixture.url(),
        fixture.admin(),
        &subs::jobs_changed(&live.runner.runner_type),
    )
    .await;
    let opening = stream::snapshot(&mut detail, JOB_CHANGED, SHORT).await;
    gql::assert_allowed(&opening, "manual_retry");
    let opened = stream::snapshot(&mut request_data, JOB_CHANGED, SHORT).await;
    assert!(opened["job"]["payload"].is_object(), "{opened}");
    assert!(opened["job"]["contentErasedAt"].is_null(), "{opened}");
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    age_tree(&durable, live.root, 31).await;
    await_scrubbed(&durable, live.root, "the Tree is scrubbed").await;

    let erased = stream::await_delta(
        &mut request_data,
        JOB_CHANGED,
        "JobsJobContentErasedEvent",
        LONG,
    )
    .await;
    assert!(erased["job"]["payload"].is_null(), "{erased}");
    assert!(erased["job"]["passport"].is_null(), "{erased}");
    assert!(erased["job"]["config"].is_null(), "{erased}");
    assert_eq!(
        erased["job"]["contentErasedAt"], erased["event"]["erasedAt"],
        "{erased}"
    );

    let blocked = stream::await_delta(
        &mut detail,
        JOB_CHANGED,
        "JobsJobAffordancesChangedEvent",
        LONG,
    )
    .await;
    assert_eq!(
        gql::assert_blocked(&blocked, "manual_retry"),
        "tree_scrubbed"
    );

    let listed = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        "JobsJobAffordancesChangedEvent",
        live.root,
        LONG,
    )
    .await;
    let view = delta::assert_upserted_summary(&listed, live.root, "FAILED");
    assert_eq!(gql::assert_blocked(&view, "manual_retry"), "tree_scrubbed");
    durable.close().await;
    fixture.shutdown().await;
}
