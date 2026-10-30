mod support;

use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::lineage::{act, create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::{LONG, gql, wire};

#[tokio::test]
async fn cancel_comes_from_the_requester_or_the_working_runner_and_reaches_every_job_below() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let (mut x, mut y, mut z) = (
        live_runner(&fixture, "cancel-x").await,
        live_runner(&fixture, "cancel-y").await,
        live_runner(&fixture, "cancel-z").await,
    );
    let r = JobDeclaration::new(&x.runner_type).with_max_attempts(1);
    producer.declare_v2(&r).await;
    let (r_trigger, _) = started_with_agent(&fixture, &mut x).await;
    let r_work = Work::of(&x, &r_trigger);
    let c = create(
        &fixture,
        &r_work,
        &y.runner_type,
        r.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (c_trigger, _) = started_with_agent(&fixture, &mut y).await;
    let c_work = Work::of(&y, &c_trigger);
    let d = create(
        &fixture,
        &r_work,
        &y.runner_type,
        r.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (d_trigger, _) = started_with_agent(&fixture, &mut y).await;
    let d_work = Work::of(&y, &d_trigger);
    let g = create(
        &fixture,
        &c_work,
        &z.runner_type,
        c,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (g_trigger, _) = started_with_agent(&fixture, &mut z).await;
    let g_work = Work::of(&z, &g_trigger);
    let queued = create(
        &fixture,
        &d_work,
        &z.runner_type,
        d,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let queued_trigger = z.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&queued_trigger), queued);
    let stranger_root = JobDeclaration::new(&z.runner_type).with_max_attempts(1);
    producer.declare_v2(&stranger_root).await;
    let (stranger_trigger, _) = started_with_agent(&fixture, &mut z).await;
    let stranger = Work::of(&z, &stranger_trigger);

    act(&fixture, wire::VERB_CANCEL, g, &g_work).await;
    gql::wait_for_status(&client, admin, g, "CANCELLED", LONG).await;
    z.await_cancel_entry(g_work.run_id, LONG).await;
    for live in [r.job_id, c, d] {
        assert_eq!(
            gql::status_of(&client, admin, live).await,
            "IN_PROGRESS",
            "only G is cancelled"
        );
    }

    act(&fixture, wire::VERB_CANCEL, c, &stranger).await;
    let refused = answers
        .expect_one(wire::FACT_COMMAND_REJECTED, c, LONG)
        .await;
    assert_eq!(
        refused.payload()["reason_code"],
        "runner_not_working_the_job"
    );
    assert!(
        refused.payload()["message"]
            .as_str()
            .is_some_and(|words| !words.is_empty())
    );
    assert_eq!(gql::status_of(&client, admin, c).await, "IN_PROGRESS");

    act(&fixture, wire::VERB_CANCEL, c, &c_work).await;
    gql::wait_for_status(&client, admin, c, "CANCELLED", LONG).await;
    y.await_cancel_entry(c_work.run_id, LONG).await;
    assert_eq!(
        gql::status_of(&client, admin, r.job_id).await,
        "IN_PROGRESS",
        "R stays"
    );
    assert_eq!(
        gql::status_of(&client, admin, d).await,
        "IN_PROGRESS",
        "D stays"
    );

    producer
        .send_v3(wire::VERB_FINISH, json!({ "job_id": r.job_id.to_string() }))
        .await;
    gql::wait_for_status(&client, admin, r.job_id, "COMPLETED", LONG).await;
    producer
        .send_v3(wire::VERB_CANCEL, json!({ "job_id": r.job_id.to_string() }))
        .await;
    gql::wait_for_status(&client, admin, d, "CANCELLED", LONG).await;
    gql::wait_for_status(&client, admin, queued, "CANCELLED", LONG).await;
    y.await_cancel_entry(d_work.run_id, LONG).await;
    assert_eq!(
        gql::status_of(&client, admin, r.job_id).await,
        "COMPLETED",
        "R stays as it is"
    );
    let withdrawn = gql::job(&client, admin, queued).await;
    assert_eq!(
        withdrawn["runs"][0]["status"], "CANCELLED",
        "the queued trigger is withdrawn: {withdrawn}"
    );

    producer.cancel_v2(stranger_root.job_id).await;
    answers
        .expect_one(wire::FACT_CANCELLED, stranger_root.job_id, LONG)
        .await;
    for cancelled in [g, c, d, queued] {
        assert_eq!(
            answers.count_of(wire::FACT_CANCELLED, cancelled),
            1,
            "each cancelled Job is announced once"
        );
    }
    assert_eq!(
        answers.count_of(wire::FACT_CANCELLED, r.job_id),
        0,
        "R is not announced as cancelled"
    );
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_restart_job_cancels_the_live_jobs_of_the_runs_it_acts_for() {
    let fixture = JobsFixture::start().await;
    let durable = support::db::Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let mut scene = support::restart::running_child(&fixture, "cancel-restart").await;
    let grandchild = create(
        &fixture,
        &scene.child_work,
        &scene.pair.parent.runner_type,
        scene.child_work.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let grandchild_trigger = scene.pair.parent.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&grandchild_trigger), grandchild);
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let restart = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &restart).await;
    assert_eq!(
        gql::status_of(&client, fixture.admin(), grandchild).await,
        "IN_PROGRESS"
    );

    let cancelled = gql::cancel_job(
        &client,
        fixture.admin(),
        uuid::Uuid::now_v7(),
        runner::job_id(&restart),
    )
    .await;
    assert!(
        cancelled.get("errors").is_none_or(|e| e.is_null()),
        "{cancelled}"
    );
    gql::wait_for_status(
        &client,
        fixture.admin(),
        runner::job_id(&restart),
        "CANCELLED",
        LONG,
    )
    .await;
    gql::wait_for_status(&client, fixture.admin(), grandchild, "CANCELLED", LONG).await;
    durable.close().await;
    fixture.shutdown().await;
}
