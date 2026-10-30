mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::claim;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, gql, priority, stream, subs,
    wire,
};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(4);

#[tokio::test]
async fn a_user_capped_at_three_is_admitted_at_three_whatever_they_request() {
    // Given: a service default maximum of 5, and a super user capping a user at 3, the user
    // watching their own maximum
    let fixture = JobsFixture::start_with(Knobs {
        default_max_priority: Some(5),
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let user = fixture::ordinary_member();
    let user_id = user.actor_id();
    let runner_type = wire::unique_runner_type("capped");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut own =
        SseSubscription::open(fixture.url(), &user, &subs::my_max_priority_changed()).await;
    assert_eq!(
        priority::opening_max(&mut own, SHORT).await,
        5,
        "a user without a maximum of their own has the service default",
    );
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user_id, 3).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user at 3",
    );
    priority::await_max_change(&mut own, 3, LONG).await;
    assert_eq!(priority::my_max_priority(&client, &user).await, 3);

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    // When: a job on the user's behalf requests 5 through job.create.v2
    let declaration = JobDeclaration::new(&runner_type)
        .triggered_by_user(user_id)
        .with_priority(5)
        .with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;

    // Then: the job is accepted with the unchanged job.queued.v1 payload
    let accepted = events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    assert_eq!(
        accepted.payload(),
        json!({ "job_id": job_id.to_string(), "runner_type": runner_type }),
        "job.queued.v1 stays as is in 0.4 — the priority is never published to producers",
    );
    events
        .expect_none(wire::FACT_CREATION_REJECTED, job_id, QUIET)
        .await;

    // Then: every read carries requested 5, effective 3, nothing forced
    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    let row = delta::assert_upserted_summary(&queued, job_id, "PENDING");
    priority::assert_priorities(&row["job"], 5, 3, None, "the queued list row");
    gql::assert_allowed(&row, wire::ACTION_FORCE_PRIORITY);
    let view = priority::job_view(&client, admin, job_id).await;
    priority::assert_priorities(&view["job"], 5, 3, None, "the job detail");
    delta::assert_active_affordances(&view);
    gql::assert_allowed(&view, wire::ACTION_FORCE_PRIORITY);
    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(job_id),
    )
    .await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    priority::assert_priorities(&opening["job"], 5, 3, None, "the job stream's snapshot");
    priority::assert_priorities(
        &priority::listed(&client, admin, &runner_type).await[0]["job"],
        5,
        3,
        None,
        "the list read",
    );

    // When: a producer requests a level outside 1..=5
    for level in [0, 6, -1] {
        let out_of_range = JobDeclaration::new(&runner_type)
            .triggered_by_user(user_id)
            .with_priority(level);
        let refused_id = out_of_range.job_id;
        producer.declare_v2(&out_of_range).await;

        // Then: a definite refusal under its stable reason, and no job anywhere
        let rejected = events
            .expect_one(wire::FACT_CREATION_REJECTED, refused_id, LONG)
            .await;
        assert_eq!(
            rejected.payload()["reason_code"],
            json!(wire::REASON_INVALID_PRIORITY),
            "priority {level} is outside 1..=5: {}",
            rejected.payload(),
        );
        events
            .expect_none(wire::FACT_QUEUED, refused_id, QUIET)
            .await;
        stream::expect_no_delta_of(
            &mut listing,
            JOBS_CHANGED,
            wire::EVT_QUEUED,
            refused_id,
            QUIET,
        )
        .await;
        durable
            .assert_count(
                db::JOBS_WITH_ID,
                refused_id,
                0,
                "a refused creation leaves no job",
            )
            .await;
    }

    // When: the job fails for good, the super user lowers the cap to 2, then retries it manually
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), job_id);
    instance.start_run(&trigger).await;
    instance
        .fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user_id, 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user lowers the cap to 2",
    );
    priority::await_max_change(&mut own, 2, LONG).await;
    priority::assert_priorities(
        &priority::job(&client, admin, job_id).await,
        5,
        3,
        None,
        "a terminal job is not waiting, so a cap change leaves it as admitted",
    );
    let failed = gql::job(&client, admin, job_id).await;
    let resolution_id = Uuid::parse_str(
        failed["resolution"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("a failed job carries its resolution: {failed}")),
    )
    .expect("a resolution id is a UUID");
    let successor_id = Uuid::now_v7();
    gql::expect_success(
        &gql::manual_retry_job(
            &client,
            admin,
            Uuid::now_v7(),
            job_id,
            successor_id,
            resolution_id,
        )
        .await,
        wire::FIELD_MANUAL_RETRY_JOB,
        "an administrator manually retries the failed job",
    );

    // Then: the successor copies the requested 5 and is capped again, at today's 2
    let successor_trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&successor_trigger), successor_id);
    priority::assert_priorities(
        &priority::job(&client, admin, successor_id).await,
        5,
        2,
        None,
        "a manual-retry successor copies the requested priority, then the cap applies again",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, job_id).await,
        5,
        3,
        None,
        "the predecessor keeps the priority it was admitted with",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_without_priority_requests_three_and_an_uncapped_user_meets_the_default_maximum() {
    // Given: the service default maximum set to 4, and a user who has no maximum of their own
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

    let mut own =
        SseSubscription::open(fixture.url(), &user, &subs::my_max_priority_changed()).await;
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

    // When: jobs are created by v1, by v2 without a priority, and by v2 above the default maximum,
    // with and without a triggering user
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

    // Then: each is admitted at the level the default rules give it
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

    // Then: reading the defaults changed nothing about the user's own maximum
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

#[tokio::test]
async fn children_wait_at_their_parents_effective_priority_whatever_they_request() {
    // Given: a service default maximum of 5, a parent at effective priority 5 executing on its own
    // type, and a CLAIM type its children target, on which an unrelated root at 4 already waits
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

    // Then: every child stores its parent's effective priority as both requested and effective,
    // nothing forced
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

    // Then: the free slots serve the three children before the unrelated root at 4
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
