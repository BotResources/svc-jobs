mod support;

use std::time::Duration;

use serde_json::json;
use support::claim;
use support::fixture::{JobsFixture, Knobs};
use support::runner;
use support::started::{Scene, accepted, refused};
use support::surface::{self, Agent};
use support::tree_hold::TreeHold;
use support::{LONG, QUIET, db, dispatch};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(6);
const ANSWERED_WHILE_THE_TREE_IS_HELD: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_reserves_the_run_for_one_instance_and_only_that_instance_starts_it() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-claim").await;
    let client = fixture.gql();
    let mut reserved = scene.instance("instance-a", 1);
    let mut stranger = scene.instance("instance-b", 1);
    reserved.connect().await;
    stranger.connect().await;
    scene.live(2).await;
    dispatch::switch_to_claim(&client, fixture.admin(), &scene.runner_type).await;
    scene.declare().await;
    let trigger = claim::claim(fixture.nats(), &reserved, CLAIM_WAIT)
        .await
        .expect_run("the claim of the instance the run is reserved for");
    let run = runner::run_id(&trigger);
    let start = Uuid::now_v7();

    refused(
        &stranger.send_started(&trigger, start).await,
        "run_reserved_elsewhere",
        &trigger,
    );
    stranger.start_run(&trigger).await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        scene.durable.count(db::STARTS_OF_RUN, run).await,
        0,
        "a started fact of the status stream for a run reserved for another instance changes nothing"
    );

    accepted(&reserved.send_started(&trigger, start).await);
    assert_eq!(
        scene.durable.holder_of_run(run).await.as_deref(),
        Some("instance-a")
    );
    assert_eq!(scene.durable.start_id_of_run(run).await, Some(start));
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_status_stream_start_holds_the_run_without_a_start_id_and_a_request_is_refused() {
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-legacy").await;
    let mut legacy = scene.instance("instance-k", 1);
    let mut other = scene.instance("instance-i", 1);
    legacy.connect().await;
    other.connect().await;
    scene.live(2).await;
    scene.declare().await;
    let trigger = legacy.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);

    legacy.start_applied(&fixture, &trigger).await;
    assert_eq!(
        scene
            .durable
            .count(db::STARTS_WITHOUT_START_ID_OF_RUN, run)
            .await,
        1
    );

    refused(
        &legacy.send_started(&trigger, Uuid::now_v7()).await,
        "run_held",
        &trigger,
    );
    refused(
        &other.send_started(&trigger, Uuid::now_v7()).await,
        "run_held",
        &trigger,
    );
    legacy.start_run(&trigger).await;
    other.start_run(&trigger).await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(scene.durable.count(db::STARTS_OF_RUN, run).await, 1);
    assert_eq!(
        scene.durable.holder_of_run(run).await.as_deref(),
        Some("instance-k")
    );
    assert_eq!(scene.durable.start_id_of_run(run).await, None);
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_status_stream_start_not_yet_applied_holds_the_run_before_a_request_is_judged() {
    const RUNS: usize = 12;
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-catch-up").await;
    let mut legacy = scene.instance("instance-k", RUNS as u32);
    legacy.connect().await;
    scene.live(1).await;
    for _ in 0..RUNS {
        scene.declare().await;
    }
    let mut triggers = Vec::new();
    for _ in 0..RUNS {
        triggers.push(legacy.next_trigger(LONG).await);
    }

    for trigger in &triggers {
        legacy.start_run(trigger).await;
    }
    for trigger in triggers.iter().rev() {
        let reply = legacy.send_started(trigger, Uuid::now_v7()).await;
        refused(&reply, "run_held", trigger);
    }
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_instances_starting_the_same_run_at_once_leave_exactly_one_holder() {
    const RUNS: usize = 6;
    let fixture = JobsFixture::start().await;
    let scene = Scene::open(&fixture, "started-race").await;
    let mut first = scene.instance("instance-i", RUNS as u32);
    let mut second = scene.instance("instance-j", RUNS as u32);
    first.connect().await;
    second.connect().await;
    scene.live(2).await;
    let mut triggers = Vec::new();
    for _ in 0..RUNS {
        scene.declare().await;
    }
    for _ in 0..RUNS {
        triggers.push(first.next_trigger(LONG).await);
    }

    for trigger in &triggers {
        let (s1, s2) = (Uuid::now_v7(), Uuid::now_v7());
        let (from_first, from_second) = tokio::join!(
            first.send_started(trigger, s1),
            second.send_started(trigger, s2)
        );
        let outcomes = [(&from_first, s1), (&from_second, s2)];
        let winners: Vec<Uuid> = outcomes
            .iter()
            .filter(|(reply, _)| reply["accepted"] == true)
            .map(|(_, start)| *start)
            .collect();
        assert_eq!(winners.len(), 1, "{from_first} {from_second}");
        let loser = outcomes
            .iter()
            .find(|(reply, _)| reply["accepted"] == false)
            .map(|(reply, _)| *reply)
            .expect("one request was refused");
        refused(loser, "run_held", trigger);
        assert_eq!(
            scene.durable.start_id_of_run(runner::run_id(trigger)).await,
            Some(winners[0])
        );
    }
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_short_operations_are_answered_while_a_request_waits_for_its_tree() {
    let fixture = JobsFixture::start_with(Knobs {
        claim_max_held: Some(1),
        ..Knobs::default()
    })
    .await;
    let scene = Scene::open(&fixture, "started-slots").await;
    let mut holder = scene.instance("instance-i", 3);
    holder.connect().await;
    scene.live(1).await;
    for _ in 0..3 {
        scene.declare().await;
    }
    let parked = holder.next_trigger(LONG).await;
    let free = holder.next_trigger(LONG).await;
    let fresh = holder.next_trigger(LONG).await;
    holder.start_applied(&fixture, &parked).await;
    holder.start_applied(&fixture, &free).await;
    let nats = fixture.nats();
    let hold = TreeHold::take(&fixture.app_url(), runner::job_id(&parked)).await;
    let mut waiting = surface::send_without_waiting(
        nats,
        format!("jobs.runner.register_agents.{}", scene.runner_type),
        json!({
            "version": 1,
            "run_id": runner::run_id(&parked).to_string(),
            "instance_key": "instance-i",
            "agents": [],
        }),
    )
    .await;
    assert!(waiting.silent_for(Duration::from_millis(500)).await);

    let listing = tokio::time::timeout(
        ANSWERED_WHILE_THE_TREE_IS_HELD,
        surface::list(nats, &holder, runner::run_id(&free)),
    )
    .await
    .expect("a listing is answered while another request waits for its Tree");
    accepted(&listing);
    let marked = tokio::time::timeout(
        ANSWERED_WHILE_THE_TREE_IS_HELD,
        surface::raw(
            nats,
            "mark_read",
            &holder,
            json!({
                "version": 1,
                "run_id": runner::run_id(&free).to_string(),
                "instance_key": "instance-i",
                "item_ids": [],
            }),
        ),
    )
    .await
    .expect("a read marker is answered while another request waits for its Tree");
    accepted(&marked);
    let started = tokio::time::timeout(
        ANSWERED_WHILE_THE_TREE_IS_HELD,
        holder.send_started(&fresh, Uuid::now_v7()),
    )
    .await
    .expect("a started request is answered while another request waits for its Tree");
    accepted(&started);
    let registered = tokio::time::timeout(
        ANSWERED_WHILE_THE_TREE_IS_HELD,
        surface::register(nats, &holder, runner::run_id(&free), &[Agent::with_inbox()]),
    )
    .await
    .expect("a registration is answered while another request waits for its Tree");
    accepted(&registered);

    hold.release().await;
    fixture.shutdown().await;
}
