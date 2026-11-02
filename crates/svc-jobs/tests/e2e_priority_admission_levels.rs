mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use support::claim;
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::subs_max_priority;
use support::{
    JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, gql, priority, stream, subs, wire,
};

const CLAIM_WAIT: Duration = Duration::from_secs(4);

#[tokio::test]
async fn a_job_without_priority_requests_three_and_an_uncapped_user_meets_the_default_maximum() {
    let fixture = JobsFixture::start_with(Knobs {
        default_max_priority: Some(4),
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let user = fixture::ordinary_member();
    let user_id = user.actor_id();
    let runner_type = wire::unique_runner_type("defaults");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut own = SseSubscription::open(
        fixture.url(),
        &user,
        &subs_max_priority::my_max_priority_changed(),
    )
    .await;
    assert_eq!(
        priority::opening_max(&mut own, SHORT).await,
        4,
        "a user without a maximum has JOBS_DEFAULT_MAX_PRIORITY",
    );
    assert_eq!(priority::my_max_priority(&client, &user).await, 4);

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    let by_v1 = JobDeclaration::new(&runner_type).triggered_by_user(user_id);
    producer.declare(&by_v1).await;
    let by_v2 = JobDeclaration::new(&runner_type).triggered_by_user(user_id);
    producer.declare_v2(&by_v2).await;
    let above_default = JobDeclaration::new(&runner_type)
        .triggered_by_user(user_id)
        .with_priority(5);
    producer.declare_v2(&above_default).await;
    let autonomous_high = JobDeclaration::new(&runner_type).with_priority(5);
    producer.declare_v2(&autonomous_high).await;
    let autonomous_low = JobDeclaration::new(&runner_type).with_priority(2);
    producer.declare_v2(&autonomous_low).await;

    let admitted = [
        by_v1.job_id,
        by_v2.job_id,
        above_default.job_id,
        autonomous_high.job_id,
        autonomous_low.job_id,
    ];
    let queued_rows = stream::await_deltas_of_each(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_QUEUED,
        &admitted,
        LONG,
    )
    .await;
    for ((job_id, requested, effective, what), queued) in [
        (
            by_v1.job_id,
            3,
            3,
            "a v1 creation requests the default priority",
        ),
        (
            by_v2.job_id,
            3,
            3,
            "a v2 creation without a priority requests 3",
        ),
        (
            above_default.job_id,
            5,
            4,
            "a user without a maximum is capped by JOBS_DEFAULT_MAX_PRIORITY",
        ),
        (
            autonomous_high.job_id,
            5,
            4,
            "a job without triggered_by is capped by JOBS_DEFAULT_MAX_PRIORITY",
        ),
        (
            autonomous_low.job_id,
            2,
            2,
            "a request under the cap is kept as requested",
        ),
    ]
    .into_iter()
    .zip(&queued_rows)
    {
        events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
        events
            .expect_none(wire::FACT_CREATION_REJECTED, job_id, QUIET)
            .await;
        priority::assert_priorities(
            &delta::assert_upserted_summary(queued, job_id, "PENDING")["job"],
            requested,
            effective,
            None,
            what,
        );
        priority::assert_priorities(
            &priority::job(&client, admin, job_id).await,
            requested,
            effective,
            None,
            what,
        );
    }

    stream::expect_total_silence(
        &mut own,
        "admitting jobs never changes the maximum of the user they run for",
        QUIET,
    )
    .await;
    assert_eq!(priority::my_max_priority(&client, &user).await, 4);

    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_wait_at_their_parents_effective_priority_whatever_they_request() {
    let fixture = JobsFixture::start_with(Knobs {
        default_max_priority: Some(5),
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let owner = fixture::ordinary_member();
    let parent_type = wire::unique_runner_type("fanning");
    let child_type = wire::unique_runner_type("pages");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut parent_runner = FakeRunner::new(fixture.nats(), &parent_type, "parent-a");
    parent_runner.connect().await;
    let parent = JobDeclaration::new(&parent_type)
        .triggered_by_user(owner.actor_id())
        .with_priority(5);
    let parent_id = parent.job_id;
    producer.declare_v2(&parent).await;
    let parent_trigger = parent_runner.next_trigger(LONG).await;
    parent_runner.start_run(&parent_trigger).await;
    gql::wait_for_status(&client, admin, parent_id, "IN_PROGRESS", LONG).await;
    priority::assert_priorities(
        &priority::job(&client, admin, parent_id).await,
        5,
        5,
        None,
        "the parent",
    );

    let mut slots = Vec::new();
    for index in 1..=4 {
        let mut slot = FakeRunner::new(fixture.nats(), &child_type, &format!("slot-{index}"));
        slot.connect().await;
        slots.push(slot);
    }
    dispatch::await_live_instances(&client, admin, &child_type, 4, LONG).await;
    dispatch::switch_to_claim(&client, admin, &child_type).await;

    let unrelated = JobDeclaration::new(&child_type)
        .triggered_by_user(owner.actor_id())
        .with_priority(4);
    let unrelated_id = unrelated.job_id;
    producer.declare_v2(&unrelated).await;
    events
        .expect_one(wire::FACT_QUEUED, unrelated_id, LONG)
        .await;

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&child_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    let lowest = JobDeclaration::new(&child_type)
        .with_parent(parent_id)
        .triggered_by_user(owner.actor_id())
        .with_priority(1);
    producer.declare_v2(&lowest).await;
    events
        .expect_one(wire::FACT_QUEUED, lowest.job_id, LONG)
        .await;
    let for_capped_user = JobDeclaration::new(&child_type)
        .with_parent(parent_id)
        .triggered_by_user(owner.actor_id())
        .with_priority(2);
    producer.declare_v2(&for_capped_user).await;
    events
        .expect_one(wire::FACT_QUEUED, for_capped_user.job_id, LONG)
        .await;
    let by_v1 = JobDeclaration::new(&child_type).with_parent(parent_id);
    producer.declare(&by_v1).await;
    events
        .expect_one(wire::FACT_QUEUED, by_v1.job_id, LONG)
        .await;
    let children = [lowest.job_id, for_capped_user.job_id, by_v1.job_id];

    let queued_rows = stream::await_deltas_of_each(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_QUEUED,
        &children,
        LONG,
    )
    .await;
    for (child_id, queued) in children.into_iter().zip(&queued_rows) {
        priority::assert_priorities(
            &delta::assert_upserted_summary(queued, child_id, "PENDING")["job"],
            5,
            5,
            None,
            "a child takes its parent's effective priority, as requested and effective",
        );
        priority::assert_priorities(
            &priority::job(&client, admin, child_id).await,
            5,
            5,
            None,
            "a child takes its parent's effective priority, whoever it runs for",
        );
    }

    let mut served = Vec::new();
    for slot in &slots {
        let answer = claim::claim(fixture.nats(), slot, CLAIM_WAIT).await;
        served.push(runner::job_id(
            &answer.expect_run("a slot claims the next job"),
        ));
    }
    assert_eq!(
        served,
        vec![children[0], children[1], children[2], unrelated_id],
        "the children wait at 5, ahead of the root at 4, in creation order under their one root",
    );
    stream::expect_no_delta(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        QUIET,
    )
    .await;

    events.stop().await;
    fixture.shutdown().await;
}
