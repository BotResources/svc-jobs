mod support;

use serde_json::json;
use support::fixture::JobsFixture;
use support::runner::{self, FakeRunner};
use support::started::{Scene, accepted, refused};
use support::surface::{self, Agent};
use support::{LONG, db, dispatch, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_run_is_held_for_one_start_and_every_other_start_is_refused() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-hold").await;
    let mut holder = scene.instance("instance-i", 1);
    let mut other = scene.instance("instance-j", 1);
    holder.connect().await;
    other.connect().await;
    scene.live(2).await;
    scene.declare().await;
    let trigger = holder.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);
    let (s1, s2) = (Uuid::now_v7(), Uuid::now_v7());
    let nats = fixture.nats();

    accepted(&holder.send_started(&trigger, s1).await);
    assert_eq!(scene.durable.count(db::STARTS_OF_RUN, run).await, 1);
    assert_eq!(scene.durable.start_id_of_run(run).await, Some(s1));

    accepted(&holder.send_started(&trigger, s1).await);
    assert_eq!(scene.durable.count(db::STARTS_OF_RUN, run).await, 1);
    assert_eq!(scene.durable.start_id_of_run(run).await, Some(s1));

    refused(
        &holder.send_started(&trigger, s2).await,
        "run_held",
        &trigger,
    );
    refused(
        &other.send_started(&trigger, s1).await,
        "run_held",
        &trigger,
    );
    refused(
        &other.send_started(&trigger, s2).await,
        "run_held",
        &trigger,
    );
    assert_eq!(scene.durable.start_id_of_run(run).await, Some(s1));
    assert_eq!(
        scene.durable.holder_of_run(run).await.as_deref(),
        Some("instance-i")
    );
    assert_eq!(scene.durable.count(db::STARTS_UNDER_START_ID, s2).await, 0);

    accepted(&surface::list(nats, &holder, run).await);
    let stranger = surface::list(nats, &other, run).await;
    assert_eq!(
        surface::reason(&stranger),
        "instance_mismatch",
        "{stranger}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_registration_right_after_the_request_hold_finds_the_run_started() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-register").await;
    let mut holder = scene.instance("instance-i", 1);
    holder.connect().await;
    scene.live(1).await;
    scene.declare().await;
    let trigger = holder.next_trigger(LONG).await;

    accepted(&holder.send_started(&trigger, Uuid::now_v7()).await);
    let registration = surface::register(
        fixture.nats(),
        &holder,
        runner::run_id(&trigger),
        &[Agent::with_inbox()],
    )
    .await;

    accepted(&registration);
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_instance_without_presence_is_refused_until_its_presence_is_live() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-presence").await;
    let mut receiver = scene.instance("instance-i", 1);
    receiver.connect().await;
    scene.live(1).await;
    scene.declare().await;
    let trigger = receiver.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);
    let mut late = scene.instance("instance-late", 1);
    let start = Uuid::now_v7();

    refused(
        &late.send_started(&trigger, start).await,
        "instance_unknown",
        &trigger,
    );
    assert_eq!(scene.durable.count(db::STARTS_OF_RUN, run).await, 0);

    late.connect().await;
    scene.live(2).await;
    accepted(&late.send_started(&trigger, start).await);
    assert_eq!(scene.durable.start_id_of_run(run).await, Some(start));
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_final_refusal_wins_over_an_unknown_instance_so_the_runner_does_not_resend() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-ranking").await;
    let mut holder = scene.instance("instance-i", 2);
    holder.connect().await;
    scene.live(1).await;
    scene.declare().await;
    scene.declare().await;
    let held = holder.next_trigger(LONG).await;
    let ended = holder.next_trigger(LONG).await;
    accepted(&holder.send_started(&held, Uuid::now_v7()).await);
    accepted(&holder.send_started(&ended, Uuid::now_v7()).await);
    scene.ended(&holder, &ended).await;
    let unknown = scene.instance("instance-unknown", 0);

    refused(
        &unknown.send_started(&held, Uuid::now_v7()).await,
        "run_held",
        &held,
    );
    refused(
        &unknown.send_started(&ended, Uuid::now_v7()).await,
        "run_not_pending",
        &ended,
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_ended_run_refuses_every_start_also_the_repeat_and_a_redelivered_trigger() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-ended").await;
    let mut holder = scene.instance("instance-i", 1);
    holder.connect().await;
    scene.live(1).await;
    scene.declare().await;
    let trigger = holder.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);
    let start = Uuid::now_v7();
    accepted(&holder.send_started(&trigger, start).await);
    scene.ended(&holder, &trigger).await;

    refused(
        &holder.send_started(&trigger, start).await,
        "run_not_pending",
        &trigger,
    );
    refused(
        &holder.send_started(&trigger, Uuid::now_v7()).await,
        "run_not_pending",
        &trigger,
    );
    assert_eq!(scene.durable.count(db::STARTS_OF_RUN, run).await, 1);
    assert_eq!(scene.durable.start_id_of_run(run).await, Some(start));
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_request_for_another_type_an_unknown_run_or_unreadable_is_refused_and_changes_nothing() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-malformed").await;
    let mut holder = scene.instance("instance-i", 1);
    holder.connect().await;
    scene.live(1).await;
    scene.declare().await;
    let trigger = holder.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);
    let nats = fixture.nats();
    let other_type = wire::unique_runner_type("started-other-type");
    let mut foreign = FakeRunner::new(nats, &other_type, "instance-x");
    foreign.connect().await;
    dispatch::await_live_instances(&fixture.gql(), fixture.admin(), &other_type, 1, LONG).await;

    refused(
        &foreign.send_started(&trigger, Uuid::now_v7()).await,
        "run_unknown",
        &trigger,
    );
    let unknown = surface::started(nats, &holder, Uuid::now_v7(), Uuid::now_v7()).await;
    refused(&unknown, "run_unknown", &trigger);
    let wrong_version = surface::raw(
        nats,
        "started",
        &holder,
        json!({
            "version": 2,
            "run_id": run.to_string(),
            "instance_key": "instance-i",
            "start_id": Uuid::now_v7().to_string(),
        }),
    )
    .await;
    refused(&wrong_version, "malformed", &trigger);
    let not_v7 = surface::started(nats, &holder, run, Uuid::nil()).await;
    refused(&not_v7, "malformed", &trigger);
    let unreadable = surface::raw(nats, "started", &holder, json!({ "run_id": "no" })).await;
    refused(&unreadable, "malformed", &trigger);
    assert_eq!(scene.durable.count(db::STARTS_OF_RUN, run).await, 0);
    fixture.shutdown().await;
}
