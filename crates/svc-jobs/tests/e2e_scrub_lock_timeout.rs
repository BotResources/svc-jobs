mod support;

use support::db::{self, Durable};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::scrub::{age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::tree_hold::TreeHold;
use support::{LONG, QUIET, gql, metrics, wire};

#[tokio::test]
async fn a_tree_another_pod_holds_is_left_for_the_next_sweep_and_is_no_scrub_failure() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let declaration = JobDeclaration::new(&wire::unique_runner_type("scrub-held"));
    producer.declare_v2(&declaration).await;
    durable
        .await_count(
            db::JOBS_WITH_ID,
            declaration.job_id,
            1,
            "the Job is created",
        )
        .await;
    producer.fail_v2(declaration.job_id, Some("gave up")).await;
    gql::wait_for_status(&client, fixture.admin(), declaration.job_id, "FAILED", LONG).await;
    let failures_before = metrics::scrub_failures(&client).await;
    let timeouts_before = metrics::tree_lock_timeouts(&client).await;
    let hold = TreeHold::take(&fixture.app_url(), declaration.job_id).await;

    age_tree(&durable, declaration.job_id, 31).await;

    metrics::await_tree_lock_timeouts_above(&client, timeouts_before).await;
    assert_not_scrubbed_for(&durable, declaration.job_id, QUIET, "the Tree is held").await;
    assert_eq!(
        metrics::scrub_failures(&client).await,
        failures_before,
        "a Tree that is busy is not a Tree that cannot be scrubbed"
    );
    hold.release().await;
    await_scrubbed(&durable, declaration.job_id, "the Tree is free again").await;
    durable.close().await;
    fixture.shutdown().await;
}
