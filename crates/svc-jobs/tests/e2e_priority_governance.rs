mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::claim;
use support::events::EventLog;
use support::fixture::{self, JobsFixture};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::subs_max_priority;
use support::{
    JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, gql, priority, stream, subs,
    wire,
};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(4);

#[tokio::test]
async fn raising_a_users_maximum_recalculates_their_waiting_unforced_jobs_only() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let capped = fixture::ordinary_member();
    let other = fixture::ordinary_member();
    let runner_type = wire::unique_runner_type("recalculated");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, capped.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user at 2",
    );
    let mut capped_own = SseSubscription::open(
        fixture.url(),
        &capped,
        &subs_max_priority::my_max_priority_changed(),
    )
    .await;
    assert_eq!(priority::opening_max(&mut capped_own, SHORT).await, 2);
    let mut other_own = SseSubscription::open(
        fixture.url(),
        &other,
        &subs_max_priority::my_max_priority_changed(),
    )
    .await;
    assert_eq!(
        priority::opening_max(&mut other_own, SHORT).await,
        wire::SERVICE_DEFAULT_MAX_PRIORITY,
        "with JOBS_DEFAULT_MAX_PRIORITY unset, a user without a maximum has 3",
    );

    let running = JobDeclaration::new(&runner_type)
        .triggered_by_user(capped.actor_id())
        .with_priority(5);
    let running_id = running.job_id;
    producer.declare_v2(&running).await;
    events.expect_one(wire::FACT_QUEUED, running_id, LONG).await;
    let reserved = claim::claim(fixture.nats(), &instance, CLAIM_WAIT)
        .await
        .expect_run("the only waiting job is served");
    assert_eq!(runner::job_id(&reserved), running_id);
    instance.start_run(&reserved).await;
    events
        .expect_one(wire::FACT_STARTED, running_id, LONG)
        .await;

    let raised = JobDeclaration::new(&runner_type)
        .triggered_by_user(capped.actor_id())
        .with_priority(4);
    let forced = JobDeclaration::new(&runner_type)
        .triggered_by_user(capped.actor_id())
        .with_priority(5);
    let low = JobDeclaration::new(&runner_type)
        .triggered_by_user(capped.actor_id())
        .with_priority(1);
    let others = JobDeclaration::new(&runner_type)
        .triggered_by_user(other.actor_id())
        .with_priority(4);
    for declaration in [&raised, &forced, &low, &others] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }
    gql::expect_success(
        &priority::force_job_priority(&client, admin, forced.job_id, 5).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a super user forces one of the capped user's jobs to 5",
    );

    let inheriting = JobDeclaration::new(&runner_type).with_parent(raised.job_id);
    let forced_child = JobDeclaration::new(&runner_type).with_parent(raised.job_id);
    for declaration in [&inheriting, &forced_child] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }
    gql::expect_success(
        &priority::force_job_priority(&client, admin, forced_child.job_id, 1).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a super user forces one child down to 1",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, inheriting.job_id).await,
        2,
        2,
        None,
        "a child stores its parent's effective priority as requested and effective",
    );

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(raised.job_id),
    )
    .await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    priority::assert_priorities(&opening["job"], 4, 2, None, "a job capped at admission");

    let raise = priority::set_user_max_priority(&client, admin, capped.actor_id(), 4).await;
    gql::expect_success(
        &raise,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user raises the user's maximum",
    );

    let recalculated = stream::await_delta_of(
        &mut watch,
        JOB_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        raised.job_id,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(
            &recalculated,
            wire::EVT_PRIORITY_RECALCULATED,
            raised.job_id
        )["effectivePriority"],
        json!(4),
    );
    priority::assert_priorities(
        &delta::projection(&recalculated, raised.job_id, "PENDING"),
        4,
        4,
        None,
        "the pushed projection is already the recalculated job",
    );
    let listed = stream::await_deltas_of_each(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        &[raised.job_id, inheriting.job_id],
        LONG,
    )
    .await;
    priority::assert_priorities(
        &delta::assert_upserted_summary(&listed[0], raised.job_id, "PENDING")["job"],
        4,
        4,
        None,
        "the list row",
    );

    priority::assert_priorities(
        &delta::assert_upserted_summary(&listed[1], inheriting.job_id, "PENDING")["job"],
        4,
        4,
        None,
        "a waiting, non-forced descendant follows its recalculated root",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, inheriting.job_id).await,
        4,
        4,
        None,
        "a waiting, non-forced descendant follows its recalculated root",
    );

    stream::expect_no_delta(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        QUIET,
    )
    .await;
    for (job_id, requested, effective, forced_level, what) in [
        (
            forced.job_id,
            5,
            5,
            Some(5),
            "a forced job keeps its forced level",
        ),
        (
            low.job_id,
            1,
            1,
            None,
            "a job under the old cap stays as requested",
        ),
        (
            others.job_id,
            4,
            3,
            None,
            "another user's job is not that user's",
        ),
        (running_id, 5, 2, None, "an executing job is not waiting"),
        (
            forced_child.job_id,
            2,
            1,
            Some(1),
            "a forced descendant keeps its forced level",
        ),
    ] {
        priority::assert_priorities(
            &priority::job(&client, admin, job_id).await,
            requested,
            effective,
            forced_level,
            what,
        );
    }

    priority::await_max_change(&mut capped_own, 4, LONG).await;
    assert_eq!(priority::my_max_priority(&client, &capped).await, 4);
    stream::expect_total_silence(
        &mut other_own,
        "another user's maximum never reaches this user",
        QUIET,
    )
    .await;
    events
        .expect_exactly(wire::FACT_QUEUED, raised.job_id, 1, QUIET)
        .await;

    gql::expect_success(
        &priority::clear_user_max_priority(&client, admin, capped.actor_id()).await,
        wire::FIELD_CLEAR_USER_MAX_PRIORITY,
        "a super user clears the user's maximum",
    );

    let cleared = stream::await_delta_of(
        &mut watch,
        JOB_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        raised.job_id,
        LONG,
    )
    .await;
    priority::assert_priorities(
        &delta::projection(&cleared, raised.job_id, "PENDING"),
        4,
        3,
        None,
        "a cleared maximum falls back to JOBS_DEFAULT_MAX_PRIORITY",
    );
    let cleared_rows = stream::await_deltas_of_each(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        &[raised.job_id, inheriting.job_id],
        LONG,
    )
    .await;
    priority::assert_priorities(
        &delta::assert_upserted_summary(&cleared_rows[1], inheriting.job_id, "PENDING")["job"],
        3,
        3,
        None,
        "the cascade follows the cleared maximum too",
    );
    priority::await_max_change(&mut capped_own, 3, LONG).await;
    assert_eq!(priority::my_max_priority(&client, &capped).await, 3);
    priority::assert_priorities(
        &priority::job(&client, admin, forced.job_id).await,
        5,
        5,
        Some(5),
        "a forced job still keeps its forced level",
    );

    for level in [0, 6] {
        let refused =
            priority::set_user_max_priority(&client, admin, capped.actor_id(), level).await;
        assert_eq!(
            gql::mutation_error_reason(&refused, "a maximum outside 1..=5"),
            wire::REASON_INVALID_PRIORITY,
        );
    }

    stream::expect_total_silence(&mut capped_own, "a refused maximum changes nothing", QUIET).await;
    stream::expect_no_delta(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        QUIET,
    )
    .await;
    assert_eq!(priority::my_max_priority(&client, &capped).await, 3);

    let newcomer = Uuid::now_v7();
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, newcomer, 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user Jobs has never observed",
    );
    let first_of_newcomer = JobDeclaration::new(&runner_type)
        .triggered_by_user(newcomer)
        .with_priority(5);
    producer.declare_v2(&first_of_newcomer).await;
    events
        .expect_one(wire::FACT_QUEUED, first_of_newcomer.job_id, LONG)
        .await;

    let arrived = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_QUEUED,
        first_of_newcomer.job_id,
        LONG,
    )
    .await;
    priority::assert_priorities(
        &delta::assert_upserted_summary(&arrived, first_of_newcomer.job_id, "PENDING")["job"],
        5,
        2,
        None,
        "a cap set on a never-seen user applies to their first job",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, first_of_newcomer.job_id).await,
        5,
        2,
        None,
        "a cap set on a never-seen user applies to their first job",
    );

    events.stop().await;
    fixture.shutdown().await;
}
