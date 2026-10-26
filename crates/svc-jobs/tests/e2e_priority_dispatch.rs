mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::claim;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, gql, stream,
    subs, wire,
};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(4);

#[tokio::test]
async fn the_next_free_slot_runs_the_higher_priority_job_first() {
    // Given: a CLAIM type with two idle instances, nothing claimed yet
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let claim_type = wire::unique_runner_type("ordered");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut first_slot = FakeRunner::new(fixture.nats(), &claim_type, "instance-a");
    let mut second_slot = FakeRunner::new(fixture.nats(), &claim_type, "instance-b");
    first_slot.connect().await;
    second_slot.connect().await;
    dispatch::await_live_instances(&client, admin, &claim_type, 2, LONG).await;
    dispatch::switch_to_claim(&client, admin, &claim_type).await;

    // Given: two jobs waiting, the later one with the higher priority
    let earlier_low = JobDeclaration::new(&claim_type).with_priority(2);
    let later_high = JobDeclaration::new(&claim_type).with_priority(4);
    for declaration in [&earlier_low, &later_high] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }
    let mut low_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(earlier_low.job_id),
    )
    .await;
    stream::snapshot(&mut low_watch, JOB_CHANGED, SHORT).await;
    let mut high_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(later_high.job_id),
    )
    .await;
    stream::snapshot(&mut high_watch, JOB_CHANGED, SHORT).await;
    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed_with_dispatch(&claim_type),
    )
    .await;
    let fleet = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(
        fleet["runnerTypes"][0]["runnerType"]["waitingJobCount"],
        json!(2)
    );

    // When: the first slot claims
    let answer = claim::claim(fixture.nats(), &first_slot, CLAIM_WAIT).await;

    // Then: it is served the higher-priority job, at once
    let run = answer.expect_run("the first free slot");
    answer.expect_answered_at_once(CLAIM_WAIT, "a claim while jobs wait");
    assert_eq!(
        runner::job_id(&run),
        later_high.job_id,
        "the waiting job of highest effective priority goes first, whatever its creation time",
    );
    let dispatched =
        stream::await_delta(&mut high_watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    assert_eq!(
        delta::event_of(&dispatched, wire::EVT_RUN_DISPATCHED, later_high.job_id)["runId"],
        json!(runner::run_id(&run).to_string()),
    );
    stream::expect_no_delta(&mut low_watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, QUIET).await;
    assert_eq!(
        gql::status_of(&client, admin, earlier_low.job_id).await,
        "PENDING"
    );
    let stopped =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_WAITING, LONG).await;
    assert_eq!(
        delta::fleet_projection(&stopped, &claim_type)["waitingJobCount"],
        json!(1)
    );
    first_slot.expect_no_trigger(QUIET).await;

    // When: the second slot claims
    let next = claim::claim(fixture.nats(), &second_slot, CLAIM_WAIT)
        .await
        .expect_run("the second free slot");

    // Then: the lower-priority job follows
    assert_eq!(runner::job_id(&next), earlier_low.job_id);
    let low_dispatched =
        stream::await_delta(&mut low_watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    assert_eq!(
        delta::event_of(
            &low_dispatched,
            wire::EVT_RUN_DISPATCHED,
            earlier_low.job_id
        )["runId"],
        json!(runner::run_id(&next).to_string()),
    );

    // Given: the same two jobs on a PUSH type nobody serves yet
    let push_type = wire::unique_runner_type("pushed");
    let push_low = JobDeclaration::new(&push_type).with_priority(2);
    let push_high = JobDeclaration::new(&push_type).with_priority(4);
    for declaration in [&push_low, &push_high] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }

    // When: an instance of the PUSH type becomes available
    let mut pusher = FakeRunner::new(fixture.nats(), &push_type, "pusher-a");
    pusher.connect().await;

    // Then: the first trigger published is the higher-priority job's, the lower one after it
    let first_trigger = pusher.next_trigger(LONG).await;
    assert_eq!(
        runner::job_id(&first_trigger),
        push_high.job_id,
        "in PUSH mode the next trigger published is the waiting job of highest priority",
    );
    pusher.start_run(&first_trigger).await;
    pusher.complete_run(&first_trigger).await;
    producer.finish(push_high.job_id).await;
    let second_trigger = pusher.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&second_trigger), push_low.job_id);

    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn claims_alternate_between_root_jobs_of_equal_priority() {
    // Given: two roots at the same priority waiting on a type nobody serves, the first one with
    // an intermediate child of its own
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let root_type = wire::unique_runner_type("roots");
    let page_type = wire::unique_runner_type("fanned");
    let producer = Producer::new(fixture.fabric(), "projects");

    let first_root = JobDeclaration::new(&root_type);
    let intermediate = JobDeclaration::new(&root_type).with_parent(first_root.job_id);
    let second_root = JobDeclaration::new(&root_type);
    for declaration in [&first_root, &intermediate, &second_root] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }

    // Given: a CLAIM type with eight idle instances
    let mut slots = Vec::new();
    for index in 1..=8 {
        let mut slot = FakeRunner::new(fixture.nats(), &page_type, &format!("slot-{index}"));
        slot.connect().await;
        slots.push(slot);
    }
    dispatch::await_live_instances(&client, admin, &page_type, 8, LONG).await;
    dispatch::switch_to_claim(&client, admin, &page_type).await;

    // Given: the first root's tree fans out four pages first — from the root and from its
    // intermediate child alternately — then the second root fans out four
    let mut first_tree = Vec::new();
    for parent in [
        first_root.job_id,
        intermediate.job_id,
        first_root.job_id,
        intermediate.job_id,
    ] {
        let page = JobDeclaration::new(&page_type).with_parent(parent);
        producer.declare_v2(&page).await;
        events
            .expect_one(wire::FACT_QUEUED, page.job_id, LONG)
            .await;
        first_tree.push(page.job_id);
    }
    let mut second_tree = Vec::new();
    for _ in 0..4 {
        let page = JobDeclaration::new(&page_type).with_parent(second_root.job_id);
        producer.declare_v2(&page).await;
        events
            .expect_one(wire::FACT_QUEUED, page.job_id, LONG)
            .await;
        second_tree.push(page.job_id);
    }
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&page_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    // When: the eight slots claim one after the other
    let mut served: Vec<Uuid> = Vec::new();
    for slot in &slots {
        served.push(runner::job_id(
            &claim::claim(fixture.nats(), slot, CLAIM_WAIT)
                .await
                .expect_run("a slot claims while pages wait"),
        ));
    }

    // Then: dispatch alternates between the two roots, each tree in creation order — the root,
    // not the direct parent, is the unit of fairness
    let expected: Vec<Uuid> = first_tree
        .iter()
        .zip(second_tree.iter())
        .flat_map(|(first, second)| [*first, *second])
        .collect();
    assert_eq!(
        served, expected,
        "within one priority, claims alternate between root jobs instead of draining one first",
    );

    // Then: the list stream carries the dispatches in that same order
    let mut dispatched_order = Vec::new();
    while dispatched_order.len() < expected.len() {
        let dispatched =
            stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
        dispatched_order.push(
            Uuid::parse_str(dispatched["event"]["jobId"].as_str().unwrap_or_default())
                .expect("a dispatch event names its job"),
        );
    }
    assert_eq!(dispatched_order, expected);
    stream::expect_no_delta(&mut listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, QUIET).await;

    events.stop().await;
    fixture.shutdown().await;
}
