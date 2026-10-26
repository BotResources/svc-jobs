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
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, dispatch, gql, stream,
    subs, wire,
};
use uuid::Uuid;

const LONG_POLL: Duration = Duration::from_secs(6);
const SHORT_POLL: Duration = Duration::from_secs(3);
const ARRIVAL_DELAY: Duration = Duration::from_secs(1);
const RESERVATION_SECONDS: u64 = 3;

#[tokio::test]
async fn a_claim_waiting_for_work_is_served_the_moment_a_job_arrives() {
    // Given: a CLAIM type with one idle instance and nothing waiting
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("served");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;

    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed_with_dispatch(&runner_type),
    )
    .await;
    let fleet = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(
        fleet["runnerTypes"][0]["runnerType"]["dispatchMode"],
        json!(wire::MODE_CLAIM)
    );
    assert_eq!(
        fleet["runnerTypes"][0]["runnerType"]["waitingJobCount"],
        json!(0)
    );
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    let user = fixture::ordinary_member();
    let declaration = JobDeclaration::new(&runner_type)
        .with_config(json!({ "pages": [1, 2] }))
        .triggered_by_user(user.actor_id())
        .with_priority(4);
    let job_id = declaration.job_id;

    // When: the instance claims with a long wait, and a job is created during that wait
    let (answer, ()) = tokio::join!(claim::claim(fixture.nats(), &instance, LONG_POLL), async {
        tokio::time::sleep(ARRIVAL_DELAY).await;
        producer.declare_v2(&declaration).await;
    });

    // Then: the claim is answered at once with the reserved run, carrying what a trigger carries
    let run = answer.expect_run("a claim held while a job arrives");
    answer.expect_answered_at_once(LONG_POLL, "a claim served by a job created mid-wait");
    assert_eq!(runner::job_id(&run), job_id);
    assert_eq!(runner::attempt_number(&run), 1);
    assert_eq!(
        run["config"],
        json!({ "pages": [1, 2] }),
        "the configuration is forwarded unchanged"
    );
    assert_eq!(run["triggered_by"], json!(user.actor_id().to_string()));
    let run_id = runner::run_id(&run);

    // Then: the job, the list, the fleet and the producer agree on one reservation
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    events.expect_none(wire::FACT_STARTED, job_id, QUIET).await;
    let queued =
        stream::await_delta_of(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, job_id, LONG).await;
    delta::assert_upserted_summary(&queued, job_id, "PENDING");
    let dispatched = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        job_id,
        LONG,
    )
    .await;
    assert_eq!(dispatched["event"]["runId"], json!(run_id.to_string()));
    delta::assert_active_affordances(&delta::assert_upserted_summary(
        &dispatched,
        job_id,
        "IN_PROGRESS",
    ));
    let job = gql::job(&client, admin, job_id).await;
    assert_eq!(job["runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(job["runs"][0]["id"], json!(run_id.to_string()));
    assert_eq!(
        job["runs"][0]["status"],
        json!("PENDING"),
        "a reserved run has not started yet"
    );
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await;
    let stopped =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_WAITING, LONG).await;
    assert_eq!(
        delta::fleet_projection(&stopped, &runner_type)["waitingJobCount"],
        json!(0)
    );
    instance.expect_no_trigger(QUIET).await;
    durable
        .assert_count(db::RUNS_OF_JOB, job_id, 1, "one reserved run")
        .await;

    // When: the instance starts the run it was served
    instance.start_run(&run).await;

    // Then: execution proceeds exactly as for a triggered run
    let started = events.expect_one(wire::FACT_STARTED, job_id, LONG).await;
    assert_eq!(
        started.payload(),
        json!({ "job_id": job_id.to_string(), "run_id": run_id.to_string() })
    );
    let started_delta = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_RUN_STARTED,
        job_id,
        LONG,
    )
    .await;
    assert_eq!(started_delta["event"]["runId"], json!(run_id.to_string()));
    let executing =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;
    assert_eq!(
        delta::fleet_projection(&executing, &runner_type)["executingJobCount"],
        json!(1)
    );
    assert_eq!(
        delta::fleet_projection(&executing, &runner_type)["instances"][0]["currentRunIds"],
        json!([run_id.to_string()]),
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_claim_nobody_serves_is_answered_empty_when_its_wait_ends() {
    // Given: a CLAIM type with one idle instance and nothing waiting, and a PUSH type beside it
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("idle");
    let push_type = wire::unique_runner_type("pushed");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    let mut pusher = FakeRunner::new(fixture.nats(), &push_type, "pusher-a");
    pusher.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::await_live_instances(&client, admin, &push_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    // When: the instance claims and nothing arrives during its wait
    let answer = claim::claim(fixture.nats(), &instance, SHORT_POLL).await;

    // Then: the reply is empty, sent at the end of the wait, and no run was created
    answer.expect_empty("a claim whose wait expired");
    answer.expect_held_for_the_wait(SHORT_POLL, "a claim whose wait expired");
    stream::expect_total_silence(&mut listing, "an expired claim moves no job", QUIET).await;

    // When: a job arrives after the claim expired
    let late = JobDeclaration::new(&runner_type);
    producer.declare_v2(&late).await;
    events
        .expect_one(wire::FACT_QUEUED, late.job_id, LONG)
        .await;

    // Then: the expired claim is gone — nobody reserves the job, it waits
    stream::expect_no_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        late.job_id,
        QUIET,
    )
    .await;
    assert_eq!(gql::status_of(&client, admin, late.job_id).await, "PENDING");
    durable
        .assert_count(
            db::RUNS_OF_JOB,
            late.job_id,
            0,
            "no run for an expired claim",
        )
        .await;
    assert_eq!(
        dispatch::runner_type_view(&client, admin, &runner_type).await["runnerType"]["waitingJobCount"],
        json!(1),
    );

    // When: an instance with no live presence claims while the job waits
    let ghost = claim::claim_as(
        fixture.nats(),
        &runner_type,
        "ghost",
        Uuid::now_v7(),
        SHORT_POLL,
    )
    .await;

    // Then: it is refused as unknown and reserves nothing
    ghost.expect_refused(wire::REASON_INSTANCE_UNKNOWN, "a claim without presence");
    assert_eq!(gql::status_of(&client, admin, late.job_id).await, "PENDING");
    durable
        .assert_count(
            db::RUNS_OF_JOB,
            late.job_id,
            0,
            "no run for an unknown instance",
        )
        .await;

    // When: an instance of a PUSH type claims while a job of its type is created
    let pushed = JobDeclaration::new(&push_type);
    producer.declare_v2(&pushed).await;
    let refused = claim::claim(fixture.nats(), &pusher, SHORT_POLL).await;

    // Then: the claim is refused as not in claim mode, and the job still goes by trigger
    refused.expect_refused(wire::REASON_NOT_CLAIM_MODE, "a claim on a PUSH type");
    let trigger = pusher.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), pushed.job_id);
    durable
        .assert_count(db::RUNS_OF_JOB, pushed.job_id, 1, "one run, by trigger")
        .await;

    // Then: the waiting job is served to the next claim of a live instance, untouched
    let served = claim::claim(fixture.nats(), &instance, SHORT_POLL)
        .await
        .expect_run("a live instance claims the job that kept waiting");
    assert_eq!(runner::job_id(&served), late.job_id);
    assert_eq!(runner::attempt_number(&served), 1);

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_repeated_claim_receives_the_run_already_reserved_to_it() {
    // Given: a CLAIM type with two idle instances and one waiting job
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("repeated");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(2);
    let mut neighbour = FakeRunner::new(fixture.nats(), &runner_type, "instance-b");
    instance.connect().await;
    neighbour.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 2, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    let first_job = JobDeclaration::new(&runner_type);
    producer.declare_v2(&first_job).await;
    events
        .expect_one(wire::FACT_QUEUED, first_job.job_id, LONG)
        .await;
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    // When: the instance claims one slot and the reply is lost on the way back
    let first_slot = Uuid::now_v7();
    claim::claim_and_lose_the_reply(fixture.nats(), &instance, first_slot, SHORT_POLL).await;
    let dispatched =
        stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    let reserved_run = dispatched["event"]["runId"].clone();
    gql::wait_for_status(&client, admin, first_job.job_id, "IN_PROGRESS", LONG).await;

    // Then: another instance cannot take the job reserved to the first
    claim::claim(fixture.nats(), &neighbour, SHORT_POLL)
        .await
        .expect_empty("another instance claims a job already reserved");

    // When: the instance repeats the same claim id, twice
    for repeat in 1..=2 {
        let answer = claim::claim_again(fixture.nats(), &instance, first_slot, SHORT_POLL).await;

        // Then: it receives the very same reserved run, at once
        let run = answer.expect_run("a repeated claim");
        answer.expect_answered_at_once(SHORT_POLL, "a repeated claim");
        assert_eq!(
            json!(runner::run_id(&run).to_string()),
            reserved_run,
            "repeat {repeat}: the same claim id returns the run already reserved, never a second \
             one",
        );
        assert_eq!(runner::attempt_number(&run), 1);
    }
    durable
        .assert_count(
            db::RUNS_OF_JOB,
            first_job.job_id,
            1,
            "a repeat creates no run",
        )
        .await;
    stream::expect_no_delta(&mut listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, QUIET).await;

    // When: the same instance claims its second slot under a new claim id, nothing else waiting
    claim::claim(fixture.nats(), &instance, SHORT_POLL)
        .await
        .expect_empty("a new claim id is a new slot, never the run reserved to another one");

    // When: a second job arrives and the instance claims its second slot under a new claim id
    let second_job = JobDeclaration::new(&runner_type);
    producer.declare_v2(&second_job).await;
    events
        .expect_one(wire::FACT_QUEUED, second_job.job_id, LONG)
        .await;
    let second_answer = claim::claim(fixture.nats(), &instance, SHORT_POLL).await;

    // Then: one instance fills two slots with two jobs
    let second_run = second_answer.expect_run("the second slot of the same instance");
    second_answer.expect_answered_at_once(SHORT_POLL, "the second slot of the same instance");
    assert_eq!(runner::job_id(&second_run), second_job.job_id);
    assert_ne!(json!(runner::run_id(&second_run).to_string()), reserved_run);
    let second_dispatch = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        second_job.job_id,
        LONG,
    )
    .await;
    assert_eq!(
        second_dispatch["event"]["runId"],
        json!(runner::run_id(&second_run).to_string())
    );

    // Then: each claim id keeps answering with its own run
    for (claim_id, expected, what) in [
        (first_slot, reserved_run.clone(), "the first slot"),
        (
            second_answer.claim_id,
            json!(runner::run_id(&second_run).to_string()),
            "the second slot",
        ),
    ] {
        let repeated = claim::claim_again(fixture.nats(), &instance, claim_id, SHORT_POLL)
            .await
            .expect_run(what);
        assert_eq!(
            json!(runner::run_id(&repeated).to_string()),
            expected,
            "{what}"
        );
    }
    for job_id in [first_job.job_id, second_job.job_id] {
        durable
            .assert_count(db::RUNS_OF_JOB, job_id, 1, "one run per slot")
            .await;
    }
    stream::expect_no_delta(&mut listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, QUIET).await;

    // When: the instance starts the first run, then repeats that slot's claim id
    let run = claim::claim_again(fixture.nats(), &instance, first_slot, SHORT_POLL)
        .await
        .expect_run("the reserved run once more");
    instance.start_run(&run).await;
    let started =
        stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    assert_eq!(started["event"]["runId"], reserved_run);
    let after_start = claim::claim_again(fixture.nats(), &instance, first_slot, SHORT_POLL).await;

    // Then: a started run is never handed out again, and the spent claim id is refused
    after_start.expect_refused("claim_spent", "a claim repeated after its reserved run started");
    events
        .expect_exactly(wire::FACT_STARTED, first_job.job_id, 1, QUIET)
        .await;
    durable
        .assert_count(db::RUNS_OF_JOB, first_job.job_id, 1, "still one run")
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_reservation_never_started_fails_as_claim_lost_and_the_job_waits_again() {
    // Given: a reservation delay of three seconds, a CLAIM type and a job with two attempts
    let fixture = JobsFixture::start_with(Knobs {
        claim_reservation_seconds: RESERVATION_SECONDS,
        backstop_interval_seconds: 1,
        retry_base_delay_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("lost");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(2);
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
    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed_with_dispatch(&runner_type),
    )
    .await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: the instance is served a run and never starts it
    let lost = claim::claim(fixture.nats(), &instance, SHORT_POLL)
        .await
        .expect_run("the first claim");
    let lost_run = runner::run_id(&lost);
    let reserved =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    assert_eq!(
        delta::event_of(&reserved, wire::EVT_RUN_DISPATCHED, job_id)["runId"],
        json!(lost_run.to_string()),
    );

    // Then: past the reservation delay Jobs writes the lost run's cancel entry, so a late
    // instance stops it, and fails the run as transient, reason claim_lost
    let entry = instance.await_cancel_entry(lost_run, LONG).await;
    assert_eq!(
        entry["run_id"],
        json!(lost_run.to_string()),
        "the cancel entry of a lost reservation sits at the run's own key",
    );
    let failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let event = delta::event_of(&failed, wire::EVT_RUN_FAILED, job_id);
    assert_eq!(event["runId"], json!(lost_run.to_string()));
    assert_eq!(event["failureKind"], json!("TRANSIENT"));
    assert_eq!(event["reasonCode"], json!(wire::REASON_CLAIM_LOST));
    let scheduled =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RETRY_SCHEDULED, LONG).await;
    assert_eq!(
        delta::event_of(&scheduled, wire::EVT_RETRY_SCHEDULED, job_id)["failedRunId"],
        json!(lost_run.to_string()),
        "a claim_lost failure buys another attempt like any transient failure",
    );
    let job = gql::job(&client, admin, job_id).await;
    assert_eq!(job["status"], json!("IN_PROGRESS"));
    let report = gql::run_by_id(&job, lost_run);
    assert_eq!(report["status"], json!("FAILED"));
    assert_eq!(report["failureReport"]["kind"], json!("TRANSIENT"));
    assert_eq!(
        report["failureReport"]["reasonCode"],
        json!(wire::REASON_CLAIM_LOST)
    );
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;
    events.expect_none(wire::FACT_STARTED, job_id, QUIET).await;

    // Then: the job waits again
    let waiting =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await;
    assert_eq!(
        delta::fleet_projection(&waiting, &runner_type)["waitingJobCount"],
        json!(1)
    );

    // When: the instance reports a late start of the lost run
    instance.start_run(&lost).await;

    // Then: the late fact is discarded
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_STARTED, QUIET).await;
    assert_eq!(
        gql::run_by_id(&gql::job(&client, admin, job_id).await, lost_run)["status"],
        json!("FAILED"),
    );
    events.expect_none(wire::FACT_STARTED, job_id, QUIET).await;

    // When: the instance claims again
    let retried = claim::claim(fixture.nats(), &instance, SHORT_POLL)
        .await
        .expect_run("the claim after the lost reservation");

    // Then: it is served a second attempt, on a new run, and the job completes normally
    assert_eq!(runner::job_id(&retried), job_id);
    assert_eq!(runner::attempt_number(&retried), 2);
    assert_ne!(runner::run_id(&retried), lost_run);
    instance.start_run(&retried).await;
    instance.complete_run(&retried).await;
    producer.finish(job_id).await;
    events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;
    durable
        .assert_count(db::RUNS_OF_JOB, job_id, 2, "the lost run and its retry")
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
