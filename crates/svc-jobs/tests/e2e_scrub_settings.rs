mod support;

use support::db::Durable;
use support::erasure::{
    RESULTS_OF_JOB, assert_request_data_erased, assert_request_data_held, view_of,
};
use support::erasure_scene::build;
use support::fixture::{JobsFixture, Knobs};
use support::scrub::{SCRUBBED_MARK, age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::scrub_scene::{finish, tree_jobs};
use support::{QUIET, gql};

fn days(days: &str) -> Knobs {
    Knobs {
        retention_days: Some(days.to_owned()),
        ..Knobs::default()
    }
}

#[tokio::test]
async fn a_lowered_retention_scrubs_the_trees_it_now_covers_and_a_raised_one_restores_nothing() {
    let mut fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let s = build(&fixture, &durable, "scrub-settings").await;
    finish(&fixture, &durable, &s).await;
    let jobs = tree_jobs(&s);
    let (a, b, e) = (s.a, s.b, s.e);
    drop(s);
    age_tree(&durable, a, 10).await;
    assert_not_scrubbed_for(&durable, a, QUIET, "10 days is inside 30 days").await;
    durable
        .assert_count(RESULTS_OF_JOB, e, 1, "the Result of E waits unread")
        .await;
    assert_request_data_held(&fixture, &durable, a, "A").await;

    fixture.restart_first(&days("5")).await;
    await_scrubbed(&durable, a, "10 days is past 5 days").await;
    durable
        .assert_count(RESULTS_OF_JOB, e, 0, "the unread Result of E is erased")
        .await;
    for (job, what) in jobs {
        assert_request_data_erased(&fixture, &durable, job, what).await;
    }
    let erased_at = view_of(&fixture, b).await["contentErasedAt"].clone();

    fixture.restart_first(&days("395")).await;
    tokio::time::sleep(QUIET).await;
    durable
        .assert_count(SCRUBBED_MARK, a, 1, "the scrub is not undone")
        .await;
    for (job, what) in jobs {
        assert_request_data_erased(&fixture, &durable, job, what).await;
    }
    assert_eq!(view_of(&fixture, b).await["contentErasedAt"], erased_at);
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), b).await,
        "FAILED"
    );
    durable.close().await;
    fixture.shutdown().await;
}
