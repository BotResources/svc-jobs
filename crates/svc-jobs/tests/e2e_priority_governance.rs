mod support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use br_test_harness::{SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs, impersonating_member, machine_caller};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, docs, gql, priority, refusal,
    stream, subs, wire,
};
use support::{claim, clock};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(4);
const FORBIDDEN: &str = "FORBIDDEN";
const UNAUTHENTICATED: &str = "UNAUTHENTICATED";

#[tokio::test]
async fn raising_a_users_maximum_recalculates_their_waiting_unforced_jobs_only() {
    // Given: JOBS_DEFAULT_MAX_PRIORITY left unset (service default 3), a CLAIM type, a user capped
    // at 2, and another user without a maximum
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
    let mut capped_own =
        SseSubscription::open(fixture.url(), &capped, &subs::my_max_priority_changed()).await;
    assert_eq!(priority::opening_max(&mut capped_own, SHORT).await, 2);
    let mut other_own =
        SseSubscription::open(fixture.url(), &other, &subs::my_max_priority_changed()).await;
    assert_eq!(
        priority::opening_max(&mut other_own, SHORT).await,
        wire::SERVICE_DEFAULT_MAX_PRIORITY,
        "with JOBS_DEFAULT_MAX_PRIORITY unset, a user without a maximum has 3",
    );

    // Given: one of the capped user's jobs already executing — it no longer waits
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

    // Given: three waiting jobs of the capped user — one of them forced to 5 — and one of the other
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

    // Given: two waiting children of the job about to be recalculated — one inheriting its
    // parent's level, one forced down to 1
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

    // When: the super user raises the capped user's maximum to 4
    let raise = priority::set_user_max_priority(&client, admin, capped.actor_id(), 4).await;
    gql::expect_success(
        &raise,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user raises the user's maximum",
    );

    // Then: the one waiting, unforced job capped below its request is recalculated, on its stream
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

    // Then: the recalculation cascades to the waiting, unforced child that inherits from it —
    // requested and effective both take the root's new effective priority
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

    // Then: no other job moved — forced, already under the cap, another user's, or executing
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

    // Then: the capped user sees their own new maximum; the other user hears nothing
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

    // When: the super user clears the maximum — the user falls back to the default, 3
    gql::expect_success(
        &priority::clear_user_max_priority(&client, admin, capped.actor_id()).await,
        wire::FIELD_CLEAR_USER_MAX_PRIORITY,
        "a super user clears the user's maximum",
    );

    // Then: the same job is recalculated to the default, the others stay put
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

    // When: the super user sets a maximum outside 1..=5
    for level in [0, 6] {
        let refused =
            priority::set_user_max_priority(&client, admin, capped.actor_id(), level).await;
        assert_eq!(
            gql::mutation_error_reason(&refused, "a maximum outside 1..=5"),
            wire::REASON_INVALID_PRIORITY,
        );
    }

    // Then: nothing moved
    stream::expect_total_silence(&mut capped_own, "a refused maximum changes nothing", QUIET).await;
    stream::expect_no_delta(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_PRIORITY_RECALCULATED,
        QUIET,
    )
    .await;
    assert_eq!(priority::my_max_priority(&client, &capped).await, 3);

    // When: the super user caps a user no job has ever named, then that user's first job arrives
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

    // Then: the cap set in advance applies to that first job
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_super_user_forces_a_waiting_job_and_its_waiting_descendants_ahead_of_the_queue() {
    // Given: a CLAIM type with five free slots, a user capped at 2 and an uncapped one
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

    // Given: a competitor at 4, then the capped user's job at 2 with waiting children and a
    // waiting grandchild, and one child already reserved on another type
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

    // When: the super user forces the job to 5, above its user's maximum
    gql::expect_success(
        &priority::force_job_priority(&client, admin, job.job_id, 5).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a super user forces a waiting job to 5",
    );

    // Then: the job and every waiting descendant are forced to 5, each on its own stream
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

    // Then: the competitor, the reserved child and the user's maximum are untouched
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

    // When: a slot frees, then the super user forces the second child down to 1
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

    // Then: the forced job runs first, its forced descendants next, the competitor, then the
    // child forced down
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

    // When: the super user forces a terminal job, an unknown job, or outside 1..=5
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

    // Then: nothing moved
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

#[tokio::test]
async fn only_super_users_force_a_priority_or_set_a_maximum() {
    // Given: a user capped at 2 with one waiting job, watched on every channel
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let member = fixture.member();
    let runner_type = wire::unique_runner_type("guarded");
    let producer = Producer::new(fixture.fabric(), "projects");

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, member.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps the member at 2",
    );
    let declaration = JobDeclaration::new(&runner_type)
        .triggered_by_user(member.actor_id())
        .with_priority(4);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut own =
        SseSubscription::open(fixture.url(), member, &subs::my_max_priority_changed()).await;
    assert_eq!(priority::opening_max(&mut own, SHORT).await, 2);
    let before = priority::job_view(&client, admin, job_id).await;
    priority::assert_priorities(&before["job"], 4, 2, None, "before the attack");

    // When: every caller who is not a super user forces the job, sets, or clears a maximum —
    // against a target that exists and one that does not.
    // The Passport carries one administrator bit (is_super_admin); "a platform administrator who
    // is not a super user" cannot be forged until that population is defined — open question.
    let hostile = [
        (member.clone(), "the capped user themselves"),
        (fixture::ordinary_member(), "another ordinary user"),
        (
            impersonating_member(admin),
            "a member impersonating an administrator",
        ),
        (machine_caller(), "a machine passport"),
    ];
    let mut codes = Vec::new();
    for (caller, who) in &hostile {
        for (target_job, target_user, existence) in [
            (job_id, member.actor_id(), "a known"),
            (Uuid::now_v7(), Uuid::now_v7(), "an unknown"),
        ] {
            for (refused, action) in [
                (
                    priority::force_job_priority(&client, caller, target_job, 5).await,
                    "forcing",
                ),
                (
                    priority::set_user_max_priority(&client, caller, target_user, 5).await,
                    "setting a maximum of",
                ),
                (
                    priority::clear_user_max_priority(&client, caller, target_user).await,
                    "clearing a maximum of",
                ),
            ] {
                let what = format!("{who} {action} {existence} target");
                codes.push(verdict::expect_code_shaped(&refused, &what));
                assert_nothing_leaked(&refused, job_id, &what);
            }
        }
    }
    for code in &codes {
        assert_eq!(
            code, FORBIDDEN,
            "one stable authorization code, whatever the action and whether the target exists: \
             {codes:?}",
        );
    }

    // When: a caller presents no passport, or one the edge cannot decode
    for forged in [None, Some("not-a-passport")] {
        for (document, input) in [
            (
                docs::FORCE_JOB_PRIORITY,
                json!({ "jobId": job_id.to_string(), "priority": 5 }),
            ),
            (
                docs::SET_USER_MAX_PRIORITY,
                json!({ "userId": member.actor_id().to_string(), "maxPriority": 5 }),
            ),
            (
                docs::CLEAR_USER_MAX_PRIORITY,
                json!({ "userId": member.actor_id().to_string() }),
            ),
        ] {
            let mut headers: Vec<(&str, &str)> = Vec::new();
            if let Some(value) = forged {
                headers.push(("X-Passport", value));
            }
            let (status, body) = client
                .post_raw(
                    "/graphql",
                    &headers,
                    json!({ "query": document, "variables": { "input": input } }),
                )
                .await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "closed at the trust boundary: {body}"
            );
            assert_eq!(
                verdict::mutation_error_code(&body).as_deref(),
                Some(UNAUTHENTICATED),
                "a missing and an undecodable passport are one closed door: {body}",
            );
            assert_nothing_leaked(&body, job_id, "an unauthenticated priority mutation");
        }
    }

    // Then: nothing moved on any channel
    stream::expect_total_silence(&mut watch, "a refused caller reaches no job stream", QUIET).await;
    stream::expect_total_silence(&mut listing, "a refused caller reaches no list", QUIET).await;
    stream::expect_total_silence(&mut own, "a refused caller changes no maximum", QUIET).await;
    assert_eq!(priority::job_view(&client, admin, job_id).await, before);
    assert_eq!(priority::my_max_priority(&client, member).await, 2);
    events
        .expect_exactly(wire::FACT_QUEUED, job_id, 1, QUIET)
        .await;

    // Then: the user surface reads the caller's own maximum and nothing else
    let supplied = client
        .query(
            member,
            docs::MY_MAX_PRIORITY_FOR_SUPPLIED_USER,
            json!({ "userId": admin.actor_id().to_string() }),
        )
        .await;
    assert!(
        !supplied["errors"].is_null() && supplied["data"].is_null(),
        "the user is the caller of the passport and can never be supplied: {supplied}",
    );
    let member_job_read = client
        .query(
            member,
            docs::JOB_DETAIL,
            json!({ "id": job_id.to_string() }),
        )
        .await;
    assert_eq!(
        verdict::expect_code_shaped(&member_job_read, "a user reading a job"),
        FORBIDDEN,
        "the user surface exposes no job, not even one running on the user's behalf",
    );
    assert_nothing_leaked(&member_job_read, job_id, "a user reading a job");

    let machine = machine_caller();
    let machine_read = priority::my_max_priority_response(&client, &machine).await;
    assert_eq!(
        verdict::expect_code_shaped(&machine_read, "a service account reading a maximum"),
        FORBIDDEN,
        "service accounts have no access to the user surface",
    );
    assert!(
        machine_read["data"].is_null() || machine_read["data"]["jobsMyMaxPriority"].is_null(),
        "a refused read carries no maximum: {machine_read}",
    );
    let machine_header = machine.to_header();
    let (status, body) = client
        .post_raw(
            "/graphql",
            &[
                ("X-Passport", machine_header.as_str()),
                ("Accept", "text/event-stream"),
            ],
            json!({ "query": subs::my_max_priority_changed() }),
        )
        .await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a service account is refused the user subscription during establishment: {body}",
    );

    events.stop().await;
    fixture.shutdown().await;
}

fn assert_nothing_leaked(response: &Value, job_id: Uuid, what: &str) {
    refusal::assert_nothing_leaked(response, &[job_id.to_string()], what);
}

#[tokio::test]
async fn a_job_back_to_waiting_after_a_transient_failure_meets_the_users_current_cap() {
    // Given: a CLAIM type, a user capped at 4, and two of the user's jobs executing — one at
    // its admission level, one forced to 5
    let fixture = JobsFixture::start_with(Knobs {
        retry_base_delay_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let user = fixture::ordinary_member();
    let runner_type = wire::unique_runner_type("retried");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(4);
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user.actor_id(), 4).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps the user at 4",
    );
    let capped = JobDeclaration::new(&runner_type)
        .triggered_by_user(user.actor_id())
        .with_priority(5)
        .with_max_attempts(2);
    let forced = JobDeclaration::new(&runner_type)
        .triggered_by_user(user.actor_id())
        .with_priority(5)
        .with_max_attempts(2);
    let mut watches = Vec::new();
    for declaration in [&capped, &forced] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
        let mut watch = SseSubscription::open(
            fixture.url(),
            admin,
            &subs::job_changed_with_priority(declaration.job_id),
        )
        .await;
        stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
        watches.push(watch);
    }
    gql::expect_success(
        &priority::force_job_priority(&client, admin, forced.job_id, 5).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a super user forces one of the jobs to 5",
    );
    let mut runs = Vec::new();
    for _ in 0..2 {
        let run = claim::claim(fixture.nats(), &instance, CLAIM_WAIT)
            .await
            .expect_run("a slot claims one of the jobs");
        instance.start_run(&run).await;
        runs.push(run);
    }
    for declaration in [&capped, &forced] {
        events
            .expect_one(wire::FACT_STARTED, declaration.job_id, LONG)
            .await;
    }

    // When: the super user lowers the user's maximum to 2 while both execute
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user lowers the cap while the jobs execute",
    );

    // Then: an executing job is not waiting, so it keeps its level for now
    priority::assert_priorities(
        &priority::job(&client, admin, capped.job_id).await,
        5,
        4,
        None,
        "an executing job",
    );

    // When: both runs fail transiently, and the jobs go back to waiting for their retry
    for run in &runs {
        instance
            .fail_run(run, "TRANSIENT", "provider_busy", None)
            .await;
    }
    for (declaration, watch) in [&capped, &forced].into_iter().zip(watches.iter_mut()) {
        let scheduled = stream::await_delta_of(
            watch,
            JOB_CHANGED,
            wire::EVT_RETRY_SCHEDULED,
            declaration.job_id,
            LONG,
        )
        .await;
        delta::event_of(&scheduled, wire::EVT_RETRY_SCHEDULED, declaration.job_id);
    }

    // Then: the job back to waiting re-derives its effective priority from today's cap; the
    // forced job keeps its force
    let rederived = br_test_harness::wait_until(LONG, || async {
        priority::job(&client, admin, capped.job_id).await["effectivePriority"] == json!(2)
    })
    .await;
    assert!(
        rederived,
        "a job back to waiting meets the user's current cap: {}",
        priority::job(&client, admin, capped.job_id).await
    );
    priority::assert_priorities(
        &priority::job(&client, admin, capped.job_id).await,
        5,
        2,
        None,
        "a cap lowered while the job ran applies at its retry",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, forced.job_id).await,
        5,
        5,
        Some(5),
        "a forced job keeps its force through a retry",
    );

    // Then: once both retries are due and both jobs wait again — retry jitter may set their due
    // times apart — dispatch follows the levels, the forced job's retry first
    let both_waiting = br_test_harness::wait_until(LONG, || async {
        let mut due = true;
        for job_id in [capped.job_id, forced.job_id] {
            let next = gql::job(&client, admin, job_id).await["nextAttemptAt"].clone();
            if !next.is_null() && delta::instant(&next) > clock::now() {
                due = false;
            }
        }
        due && dispatch::runner_type_view(&client, admin, &runner_type).await["runnerType"]
            ["waitingJobCount"]
            == json!(2)
    })
    .await;
    assert!(
        both_waiting,
        "both retries fall due and both jobs wait again within {LONG:?}: {}",
        dispatch::runner_type_view(&client, admin, &runner_type).await
    );
    let mut retried = Vec::new();
    for _ in 0..2 {
        let run = claim::claim_until_served(
            fixture.nats(),
            &instance,
            CLAIM_WAIT,
            LONG,
            "a slot claims a retry",
        )
        .await;
        assert_eq!(runner::attempt_number(&run), 2);
        retried.push(runner::job_id(&run));
    }
    assert_eq!(retried, vec![forced.job_id, capped.job_id]);

    events.stop().await;
    fixture.shutdown().await;
}
