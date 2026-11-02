mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::claim;
use support::events::EventLog;
use support::fixture::{self, JobsFixture};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, gql, priority, stream, subs,
    wire,
};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(4);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_super_user_forces_a_waiting_job_and_its_waiting_descendants_ahead_of_the_queue() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let capped = fixture::ordinary_member();
    let uncapped = fixture::ordinary_member();
    let runner_type = wire::unique_runner_type("forced");
    let side_type = wire::unique_runner_type("side");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut slots = Vec::new();
    for index in 1..=5 {
        let mut slot = FakeRunner::new(fixture.nats(), &runner_type, &format!("slot-{index}"));
        slot.connect().await;
        slots.push(slot);
    }
    dispatch::await_live_instances(&client, admin, &runner_type, 5, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    let mut side = FakeRunner::new(fixture.nats(), &side_type, "side-a");
    side.connect().await;
    dispatch::await_live_instances(&client, admin, &side_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &side_type).await;

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, capped.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user at 2",
    );

    let competitor = JobDeclaration::new(&runner_type)
        .triggered_by_user(uncapped.actor_id())
        .with_priority(4);
    let job = JobDeclaration::new(&runner_type)
        .triggered_by_user(capped.actor_id())
        .with_priority(5);
    let child_one = JobDeclaration::new(&runner_type)
        .with_parent(job.job_id)
        .with_priority(1);
    let child_two = JobDeclaration::new(&runner_type)
        .with_parent(job.job_id)
        .with_priority(1);
    let grandchild = JobDeclaration::new(&runner_type)
        .with_parent(child_one.job_id)
        .with_priority(1);
    let reserved_child = JobDeclaration::new(&side_type)
        .with_parent(job.job_id)
        .with_priority(1);
    for declaration in [
        &competitor,
        &job,
        &child_one,
        &child_two,
        &grandchild,
        &reserved_child,
    ] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }
    let side_run = claim::claim(fixture.nats(), &side, CLAIM_WAIT)
        .await
        .expect_run("the side child is reserved before the force");
    assert_eq!(runner::job_id(&side_run), reserved_child.job_id);

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut watches = Vec::new();
    for job_id in [
        job.job_id,
        child_one.job_id,
        child_two.job_id,
        grandchild.job_id,
    ] {
        let mut watch = SseSubscription::open(
            fixture.url(),
            admin,
            &subs::job_changed_with_priority(job_id),
        )
        .await;
        let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
        let requested = if job_id == job.job_id { 5 } else { 2 };
        priority::assert_priorities(
            &opening["job"],
            requested,
            2,
            None,
            "before the force: descendants store the effective level they inherited",
        );
        gql::assert_allowed(&opening, wire::ACTION_FORCE_PRIORITY);
        watches.push((job_id, watch));
    }
    let mut reserved_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(reserved_child.job_id),
    )
    .await;
    stream::snapshot(&mut reserved_watch, JOB_CHANGED, SHORT).await;

    gql::expect_success(
        &priority::force_job_priority(&client, admin, job.job_id, 5).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a super user forces a waiting job to 5",
    );

    for (job_id, watch) in watches.iter_mut() {
        let forced =
            stream::await_delta_of(watch, JOB_CHANGED, wire::EVT_PRIORITY_FORCED, *job_id, LONG)
                .await;
        assert_eq!(
            delta::event_of(&forced, wire::EVT_PRIORITY_FORCED, *job_id)["forcedPriority"],
            json!(5),
        );
        priority::assert_effective(
            &delta::projection(&forced, *job_id, "PENDING"),
            5,
            Some(5),
            "forcing a job forces its waiting descendants",
        );
    }
    priority::assert_priorities(
        &priority::job(&client, admin, job.job_id).await,
        5,
        5,
        Some(5),
        "the forced job",
    );

    stream::expect_no_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_FORCED,
        competitor.job_id,
        QUIET,
    )
    .await;
    stream::expect_no_delta(
        &mut reserved_watch,
        JOB_CHANGED,
        wire::EVT_PRIORITY_FORCED,
        QUIET,
    )
    .await;
    priority::assert_priorities(
        &priority::job(&client, admin, reserved_child.job_id).await,
        2,
        2,
        None,
        "a descendant that no longer waits is not forced",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, child_one.job_id).await,
        5,
        5,
        Some(5),
        "a force cascades: the waiting descendant takes the forced root's level as requested and \
         effective, and is forced",
    );
    assert_eq!(
        priority::my_max_priority(&client, &capped).await,
        2,
        "forcing a job never changes the maximum of its user",
    );

    let first = claim::claim(fixture.nats(), &slots[0], CLAIM_WAIT)
        .await
        .expect_run("the first free slot");
    gql::expect_success(
        &priority::force_job_priority(&client, admin, child_two.job_id, 1).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a forced priority holds until it is forced again",
    );
    let (_, child_two_watch) = &mut watches[2];
    let reforced = stream::await_delta_of(
        child_two_watch,
        JOB_CHANGED,
        wire::EVT_PRIORITY_FORCED,
        child_two.job_id,
        LONG,
    )
    .await;
    priority::assert_priorities(
        &delta::projection(&reforced, child_two.job_id, "PENDING"),
        5,
        1,
        Some(1),
        "the second force replaces the first",
    );
    let relisted = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_FORCED,
        child_two.job_id,
        LONG,
    )
    .await;
    priority::assert_priorities(
        &delta::assert_upserted_summary(&relisted, child_two.job_id, "PENDING")["job"],
        5,
        1,
        Some(1),
        "the list row follows the second force",
    );

    let mut served = vec![runner::job_id(&first)];
    for slot in &slots[1..] {
        served.push(runner::job_id(
            &claim::claim(fixture.nats(), slot, CLAIM_WAIT)
                .await
                .expect_run("the next free slot"),
        ));
    }
    assert_eq!(
        served,
        vec![
            job.job_id,
            child_one.job_id,
            grandchild.job_id,
            competitor.job_id,
            child_two.job_id
        ],
        "dispatch follows the forced levels, then creation time under the one root",
    );

    let cancelled = JobDeclaration::new(&side_type);
    producer.declare_v2(&cancelled).await;
    events
        .expect_one(wire::FACT_QUEUED, cancelled.job_id, LONG)
        .await;
    gql::expect_success(
        &gql::cancel_job(&client, admin, Uuid::now_v7(), cancelled.job_id).await,
        wire::FIELD_CANCEL_JOB,
        "an administrator cancels a waiting job",
    );
    gql::wait_for_status(&client, admin, cancelled.job_id, "CANCELLED", LONG).await;
    let terminal_view = priority::job_view(&client, admin, cancelled.job_id).await;
    assert_eq!(
        gql::assert_blocked(&terminal_view, wire::ACTION_FORCE_PRIORITY),
        wire::REASON_JOB_ALREADY_TERMINAL,
    );
    let before = priority::job_view(&client, admin, cancelled.job_id).await;
    for (job_id, level, reason, what) in [
        (
            cancelled.job_id,
            5,
            wire::REASON_JOB_ALREADY_TERMINAL,
            "forcing a terminal job",
        ),
        (
            Uuid::now_v7(),
            5,
            wire::REASON_JOB_NOT_FOUND,
            "forcing an unknown job",
        ),
        (
            child_two.job_id,
            0,
            wire::REASON_INVALID_PRIORITY,
            "forcing to 0",
        ),
        (
            child_two.job_id,
            6,
            wire::REASON_INVALID_PRIORITY,
            "forcing to 6",
        ),
    ] {
        let refused = priority::force_job_priority(&client, admin, job_id, level).await;
        assert_eq!(gql::mutation_error_reason(&refused, what), reason, "{what}");
    }

    stream::expect_no_delta(&mut listing, JOBS_CHANGED, wire::EVT_PRIORITY_FORCED, QUIET).await;
    assert_eq!(
        priority::job_view(&client, admin, cancelled.job_id).await,
        before
    );
    priority::assert_effective(
        &priority::job(&client, admin, child_two.job_id).await,
        1,
        Some(1),
        "a refused force leaves the last forced level",
    );

    events.stop().await;
    fixture.shutdown().await;
}
