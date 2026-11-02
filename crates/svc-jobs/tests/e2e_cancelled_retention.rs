mod support;

use chrono::{DateTime, Utc};
use serde_json::json;
use support::db::Durable;
use support::erasure::{
    WAITING_FOR_ERASURE, assert_request_data_erased, assert_request_data_held, view_of,
};
use support::fixture::{JobsFixture, Knobs};
use support::gesture::Work;
use support::lineage::{act, create, live_runner};
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

fn three_days() -> Knobs {
    Knobs {
        cancelled_retention_days: Some("3".to_owned()),
        ..Knobs::default()
    }
}

async fn age_cancel(durable: &Durable, job: Uuid, days: i64) {
    sqlx::query(
        "UPDATE job_resolutions SET occurred_at = now() - make_interval(days => $2::int) \
         WHERE job_id = $1 AND kind = 'CANCELLED'",
    )
    .bind(job)
    .bind(i32::try_from(days).unwrap())
    .execute(durable.pool())
    .await
    .expect("the cancel is aged");
}

async fn erased_at(durable: &Durable, job: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT content_erased_at FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(durable.pool())
        .await
        .expect("the time of the erasure reads")
}

struct Trees<'a> {
    root: Uuid,
    root_work: Work,
    _runner: FakeRunner<'a>,
}

async fn root_with_request_data<'a>(fixture: &'a JobsFixture, label: &str) -> Trees<'a> {
    let mut runner = live_runner(fixture, label).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let declaration = JobDeclaration::new(&runner.runner_type)
        .with_config(json!({ "model": "m" }))
        .with_max_attempts(1);
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "INLINE", "message": "the brief", "attachments": [] });
    producer.declare_payload(body).await;
    let trigger = runner.next_trigger(LONG).await;
    runner.start_applied(fixture, &trigger).await;
    Trees {
        root: declaration.job_id,
        root_work: Work::of(&runner, &trigger),
        _runner: runner,
    }
}

#[tokio::test]
async fn the_cancelled_retention_setting_delays_the_erasure_and_the_sweep_keeps_the_first_time() {
    let fixture = JobsFixture::start_with(three_days()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = root_with_request_data(&fixture, "retention-a").await;
    let (mut b_runner, mut c_runner) = (
        live_runner(&fixture, "retention-b").await,
        live_runner(&fixture, "retention-c").await,
    );
    let body = json!({ "mode": "SYNC", "max_attempts": 1, "config": { "model": "m" },
        "payload": { "form": "INLINE", "message": "the brief", "attachments": [] } });
    let b = create(
        &fixture,
        &tree.root_work,
        &b_runner.runner_type,
        tree.root,
        body.clone(),
    )
    .await;
    let c = create(
        &fixture,
        &tree.root_work,
        &c_runner.runner_type,
        tree.root,
        body,
    )
    .await;
    let b_trigger = b_runner.next_trigger(LONG).await;
    b_runner.start_applied(&fixture, &b_trigger).await;
    let c_trigger = c_runner.next_trigger(LONG).await;
    c_runner.start_applied(&fixture, &c_trigger).await;
    for (job, what) in [(tree.root, "A"), (b, "B"), (c, "C")] {
        assert_request_data_held(&fixture, &durable, job, what).await;
    }

    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), b).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), b, "CANCELLED", LONG).await;

    durable
        .assert_count(
            WAITING_FOR_ERASURE,
            b,
            1,
            "B waits for the cancelled retention",
        )
        .await;
    tokio::time::sleep(QUIET).await;
    assert_request_data_held(&fixture, &durable, b, "B, just cancelled").await;

    age_cancel(&durable, b, 2).await;
    tokio::time::sleep(QUIET).await;
    assert_request_data_held(&fixture, &durable, b, "B, two days after the cancel").await;

    age_cancel(&durable, b, 4).await;
    durable
        .await_count(
            WAITING_FOR_ERASURE,
            b,
            0,
            "the sweep erases B once three days have passed",
        )
        .await;
    assert_request_data_erased(&fixture, &durable, b, "B, four days after the cancel").await;
    let first = erased_at(&durable, b).await;
    assert!(first.is_some());
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), b).await,
        "CANCELLED"
    );
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        erased_at(&durable, b).await,
        first,
        "a later sweep keeps the time of the first erasure"
    );
    assert!(view_of(&fixture, b).await["contentErasedAt"].is_string());

    assert_request_data_held(&fixture, &durable, tree.root, "A keeps its request data").await;
    assert_request_data_held(&fixture, &durable, c, "C keeps its request data").await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), c).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cancel_of_a_finished_job_starts_no_delay_and_the_cascade_cancels_the_live_child() {
    let fixture = JobsFixture::start_with(three_days()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = root_with_request_data(&fixture, "retention-z").await;
    let (mut g_runner, mut h_runner) = (
        live_runner(&fixture, "retention-g").await,
        live_runner(&fixture, "retention-h").await,
    );
    let body = json!({ "mode": "SYNC", "max_attempts": 1, "config": { "model": "m" },
        "payload": { "form": "INLINE", "message": "the brief", "attachments": [] } });
    let g = create(
        &fixture,
        &tree.root_work,
        &g_runner.runner_type,
        tree.root,
        body.clone(),
    )
    .await;
    let g_trigger = g_runner.next_trigger(LONG).await;
    g_runner.start_applied(&fixture, &g_trigger).await;
    let h = create(
        &fixture,
        &Work::of(&g_runner, &g_trigger),
        &h_runner.runner_type,
        g,
        body,
    )
    .await;
    let h_trigger = h_runner.next_trigger(LONG).await;
    h_runner.start_applied(&fixture, &h_trigger).await;
    act(&fixture, wire::VERB_FINISH, g, &tree.root_work).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), g, "COMPLETED", LONG).await;

    act(&fixture, wire::VERB_CANCEL, g, &tree.root_work).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), h, "CANCELLED", LONG).await;

    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), g).await,
        "COMPLETED",
        "a cancel of a finished Job leaves it as it is"
    );
    durable
        .assert_count(WAITING_FOR_ERASURE, g, 0, "G starts no delay")
        .await;
    durable
        .assert_count(
            WAITING_FOR_ERASURE,
            h,
            1,
            "H waits for the cancelled retention",
        )
        .await;
    age_cancel(&durable, h, 4).await;
    durable
        .await_count(WAITING_FOR_ERASURE, h, 0, "H is erased after three days")
        .await;
    assert_request_data_erased(&fixture, &durable, h, "H").await;
    tokio::time::sleep(QUIET).await;
    assert_request_data_held(&fixture, &durable, g, "G keeps its request data").await;
    assert_request_data_held(&fixture, &durable, tree.root, "Z keeps its request data").await;
    durable.close().await;
    fixture.shutdown().await;
}
