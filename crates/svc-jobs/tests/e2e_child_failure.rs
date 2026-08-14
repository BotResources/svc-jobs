mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, delta, gql, stream,
    subs, wire,
};
use uuid::Uuid;

#[tokio::test]
async fn a_child_failure_informs_its_parent_without_deciding_the_parents_fate() {
    // Given: a parent job watched from the list and fleet streams before it exists
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let parent_type = wire::unique_runner_type("orchestrator");
    let child_type = wire::unique_runner_type("worker");
    let producer = Producer::new(fixture.fabric(), "projects");
    let owner = Producer::new(fixture.fabric(), "jobs");
    let mut orchestrator = FakeRunner::new(fixture.nats(), &parent_type, "orchestrator-a");
    let mut worker = FakeRunner::new(fixture.nats(), &child_type, "worker-a");

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_for(&[&parent_type, &child_type]),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&parent_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;
    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    delta::event_of(&queued, wire::EVT_QUEUED, parent_id);
    delta::assert_active_affordances(&delta::assert_upserted_summary(
        &queued, parent_id, "PENDING",
    ));
    assert_eq!(
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await["event"]
            ["jobId"],
        json!(parent_id.to_string())
    );

    let mut parent_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(parent_id)).await;
    let opening = stream::snapshot(&mut parent_watch, JOB_CHANGED, SHORT).await;
    assert_eq!(opening["job"]["status"], json!("PENDING"));
    delta::assert_active_affordances(&opening);

    // When: the parent runner claims its work and reports its progression
    orchestrator.connect().await;
    let parent_trigger = orchestrator.next_trigger(LONG).await;
    let parent_run = runner::run_id(&parent_trigger);
    let parent_dispatched = stream::await_delta(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(&parent_dispatched, wire::EVT_RUN_DISPATCHED, parent_id)["runId"],
        json!(parent_run.to_string())
    );
    orchestrator.start_run(&parent_trigger).await;
    let started =
        stream::await_delta(&mut parent_watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    delta::projection(&started, parent_id, "IN_PROGRESS");
    delta::assert_active_affordances(&started);
    orchestrator
        .declare_plan(&parent_trigger, &["fan out"])
        .await;
    let planned = stream::await_delta(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_PLAN_DECLARED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::projection(&planned, parent_id, "IN_PROGRESS")["progression"]["plan"]["items"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    orchestrator.start_step(&parent_trigger, 0, "fan out").await;
    let stepped =
        stream::await_delta(&mut parent_watch, JOB_CHANGED, wire::EVT_STEP_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&stepped, wire::EVT_STEP_STARTED, parent_id)["stepIndex"],
        json!(0)
    );
    let parent_executing =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;
    delta::assert_instance(
        &delta::assert_fleet(&parent_executing, &parent_type, 0, 1, 1, 0),
        "orchestrator-a",
        true,
        &[parent_run],
        &orchestrator.version,
    );

    // When: the parent runner spawns a child and an independent sibling
    let child = JobDeclaration::new(&child_type).with_parent(parent_id);
    let child_id = child.job_id;
    let sibling = JobDeclaration::new(&child_type).with_parent(parent_id);
    let sibling_id = sibling.job_id;
    owner.declare(&child).await;
    owner.declare(&sibling).await;
    for descendant in [child_id, sibling_id] {
        events.expect_one(wire::FACT_QUEUED, descendant, LONG).await;
        let listed = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
        delta::assert_active_affordances(&delta::upserted(&listed, descendant));
    }
    let tree = gql::job_view(&client, admin, parent_id).await;
    assert_eq!(tree["job"]["status"], json!("IN_PROGRESS"));
    for descendant in [child_id, sibling_id] {
        delta::assert_active_affordances(&gql::child_of(&tree["job"], descendant));
    }

    // When: the child logs, then fails permanently with a structured report
    let mut child_tail =
        SseSubscription::open(fixture.url(), admin, &subs::log_tail(child_id)).await;
    stream::snapshot(&mut child_tail, LOG_TAIL, SHORT).await;
    let mut child_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(child_id)).await;
    stream::snapshot(&mut child_watch, JOB_CHANGED, SHORT).await;

    worker.connect().await;
    let child_trigger = worker.next_trigger(LONG).await;
    let child_run = runner::run_id(&child_trigger);
    worker.start_run(&child_trigger).await;
    worker.start_step(&child_trigger, 0, "convert").await;
    worker
        .log_line(&child_trigger, None, "INFO", "run level")
        .await;
    worker
        .log_line(&child_trigger, Some(0), "ERROR", "provider refused")
        .await;
    let appended = stream::drain_appended(&mut child_tail, LOG_TAIL, 2, LONG).await;
    delta::log_line(&appended[0], child_run, None);
    delta::log_line(&appended[1], child_run, Some(0));

    worker
        .fail_run(&child_trigger, "PERMANENT", "provider_refused", None)
        .await;
    let run_failed =
        stream::await_delta(&mut child_watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let report = delta::event_of(&run_failed, wire::EVT_RUN_FAILED, child_id);
    assert_eq!(report["failureKind"], json!("PERMANENT"));
    assert_eq!(report["reasonCode"], json!("provider_refused"));
    let child_failed =
        stream::await_delta(&mut child_watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    delta::projection(&child_failed, child_id, "FAILED");
    delta::assert_failed_affordances(&child_failed);

    let escalated = events.expect_one(wire::FACT_FAILED, child_id, LONG).await;
    let payload = escalated.payload();
    assert_eq!(payload["failure_cause"], json!("TERMINAL_RUN_FAILURE"));
    assert_eq!(
        payload["failure_report"]["reason_code"],
        json!("provider_refused"),
        "the run's report is escalated to the owner as it was reported: {payload}",
    );
    assert_eq!(payload["failure_report"]["kind"], json!("PERMANENT"));
    assert!(
        gql::job(&client, admin, child_id).await["nextAttemptAt"].is_null(),
        "a permanent failure ends automatic retry immediately",
    );
    worker.expect_no_trigger(QUIET).await;

    // Then: neither the parent nor the sibling moved
    let parent_view = gql::job_view(&client, admin, parent_id).await;
    assert_eq!(
        parent_view["job"]["status"],
        json!("IN_PROGRESS"),
        "a child's failure does not change its parent",
    );
    assert!(parent_view["job"]["resolution"].is_null());
    delta::assert_active_affordances(&parent_view);
    assert_eq!(
        gql::job(&client, admin, sibling_id).await["status"],
        json!("PENDING"),
        "no sibling is failed or cancelled automatically",
    );
    for job_id in [parent_id, sibling_id] {
        for fact in [wire::FACT_FAILED, wire::FACT_CANCELLED] {
            events.expect_none(fact, job_id, QUIET).await;
        }
    }
    stream::expect_no_delta(&mut parent_watch, JOB_CHANGED, wire::EVT_JOB_FAILED, QUIET).await;

    // When: the parent runner decides to spawn an alternative child instead
    let alternative = JobDeclaration::new(&child_type).with_parent(parent_id);
    let alternative_id = alternative.job_id;
    owner.declare(&alternative).await;
    events
        .expect_one(wire::FACT_QUEUED, alternative_id, LONG)
        .await;
    let mut alternative_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(alternative_id)).await;
    stream::snapshot(&mut alternative_watch, JOB_CHANGED, SHORT).await;
    let alternative_trigger = worker.next_trigger(LONG).await;
    let alternative_dispatched = stream::await_delta(
        &mut alternative_watch,
        JOB_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(
            &alternative_dispatched,
            wire::EVT_RUN_DISPATCHED,
            alternative_id
        )["runId"],
        json!(runner::run_id(&alternative_trigger).to_string())
    );
    worker.start_run(&alternative_trigger).await;
    let alternative_started = stream::await_delta(
        &mut alternative_watch,
        JOB_CHANGED,
        wire::EVT_RUN_STARTED,
        LONG,
    )
    .await;
    delta::projection(&alternative_started, alternative_id, "IN_PROGRESS");
    worker.complete_run(&alternative_trigger).await;
    let alternative_done = stream::await_delta(
        &mut alternative_watch,
        JOB_CHANGED,
        wire::EVT_RUN_COMPLETED,
        LONG,
    )
    .await;
    delta::projection(&alternative_done, alternative_id, "IN_PROGRESS");

    // When: the owner finishes the alternative child and then the parent
    owner.finish(alternative_id, Uuid::now_v7()).await;
    events
        .expect_one(wire::FACT_COMPLETED, alternative_id, LONG)
        .await;
    assert_eq!(
        gql::status_of(&client, admin, parent_id).await,
        "IN_PROGRESS",
        "a completed child never completes its parent — success is declared, never inferred",
    );
    events
        .expect_none(wire::FACT_COMPLETED, parent_id, QUIET)
        .await;
    stream::expect_no_delta(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        QUIET,
    )
    .await;

    let parent_resolution = Uuid::now_v7();
    producer.finish(parent_id, parent_resolution).await;
    let parent_completed = stream::await_delta(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(&parent_completed, wire::EVT_JOB_COMPLETED, parent_id)["resolutionId"],
        json!(parent_resolution.to_string())
    );
    delta::projection(&parent_completed, parent_id, "COMPLETED");
    delta::assert_completed_affordances(&parent_completed);
    events
        .expect_one(wire::FACT_COMPLETED, parent_id, LONG)
        .await;

    durable
        .assert_all(
            parent_id,
            &[
                (db::JOBS_WITH_ID, 1, "one parent"),
                (db::RUNS_OF_JOB, 1, "one run per dispatch"),
                (db::RESOLUTIONS_OF_JOB, 1, "one owner-declared resolution"),
            ],
        )
        .await;
    durable
        .assert_all(
            child_id,
            &[
                (db::RUNS_OF_JOB, 1, "one child run"),
                (db::RESOLUTIONS_OF_JOB, 1, "the child's terminal failure"),
            ],
        )
        .await;
    durable
        .assert_all(
            sibling_id,
            &[(
                db::RESOLUTIONS_OF_JOB,
                0,
                "no resolution ever reached the untouched sibling",
            )],
        )
        .await;
    assert_eq!(
        gql::run_by_id(&gql::job(&client, admin, parent_id).await, parent_run)["status"],
        json!("STARTED"),
        "the parent's own run is never terminated by a child's fate",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_owner_declares_its_own_job_failed_after_an_unrecoverable_child_report() {
    // Given: a running parent whose child has just failed permanently
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let parent_type = wire::unique_runner_type("orchestrator");
    let child_type = wire::unique_runner_type("worker");
    let producer = Producer::new(fixture.fabric(), "projects");
    let owner = Producer::new(fixture.fabric(), "jobs");
    let mut orchestrator = FakeRunner::new(fixture.nats(), &parent_type, "orchestrator-a");
    let mut worker = FakeRunner::new(fixture.nats(), &child_type, "worker-a");
    orchestrator.connect().await;
    worker.connect().await;

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;
    let parent_trigger = orchestrator.next_trigger(LONG).await;
    orchestrator.start_run(&parent_trigger).await;
    gql::wait_for_status(&client, admin, parent_id, "IN_PROGRESS", LONG).await;

    let child = JobDeclaration::new(&child_type).with_parent(parent_id);
    let child_id = child.job_id;
    owner.declare(&child).await;
    events.expect_one(wire::FACT_QUEUED, child_id, LONG).await;
    let child_trigger = worker.next_trigger(LONG).await;
    worker.start_run(&child_trigger).await;
    worker
        .fail_run(&child_trigger, "PERMANENT", "provider_refused", None)
        .await;
    let report = events.expect_one(wire::FACT_FAILED, child_id, LONG).await;
    assert_eq!(
        report.payload()["failure_cause"],
        json!("TERMINAL_RUN_FAILURE")
    );

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(parent_id)).await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    assert_eq!(
        opening["job"]["status"],
        json!("IN_PROGRESS"),
        "a child's failure leaves the parent's fate to its Owner: {opening}",
    );
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_for(&[&parent_type]),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    // When: the parent's Owner judges the report unrecoverable and fails its own job on the bus
    let resolution_id = Uuid::now_v7();
    producer.fail(parent_id, resolution_id).await;

    // Then: the failure is published as declared, carrying no run report the parent never produced
    let declared = events.expect_one(wire::FACT_FAILED, parent_id, LONG).await;
    let payload = declared.payload();
    assert_eq!(
        payload["failure_cause"],
        json!("DECLARED_BY_OWNER"),
        "an Owner-declared failure names its own cause, never the cause of a run: {payload}",
    );
    assert!(
        payload["failure_report"].is_null(),
        "no run of this job failed, so there is no report to escalate — a copied child report \
         would tell the producer a run failed here when none did: {payload}",
    );

    let failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    let pushed = delta::event_of(&failed, wire::EVT_JOB_FAILED, parent_id);
    assert_eq!(pushed["failureCause"], json!("DECLARED_BY_OWNER"));
    assert_eq!(pushed["resolutionId"], json!(resolution_id.to_string()));
    delta::projection(&failed, parent_id, "FAILED");
    delta::assert_failed_affordances(&failed);
    delta::assert_failed_affordances(&delta::assert_upserted_summary(
        &stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_FAILED, LONG).await,
        parent_id,
        "FAILED",
    ));

    let view = gql::job_view(&client, admin, parent_id).await;
    let resolution = view["job"]["resolution"].clone();
    assert_eq!(resolution["id"], json!(resolution_id.to_string()));
    assert_eq!(resolution["kind"], json!("FAILED"));
    assert_eq!(resolution["failureCause"], json!("DECLARED_BY_OWNER"));
    assert!(
        resolution["causedByRunId"].is_null(),
        "no run caused this failure — the Owner did: {resolution}",
    );
    delta::assert_failed_affordances(&view);

    // Then: a late fact from the parent's own runner cannot revive it
    orchestrator.complete_run(&parent_trigger).await;
    events
        .expect_none(wire::FACT_COMPLETED, parent_id, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, QUIET).await;
    assert_eq!(
        gql::status_of(&client, admin, parent_id).await,
        "FAILED",
        "a status fact for a run whose job is already terminal is acknowledged and discarded",
    );
    events
        .expect_exactly(wire::FACT_FAILED, parent_id, 1, QUIET)
        .await;

    durable
        .assert_all(
            parent_id,
            &[
                (db::RUNS_OF_JOB, 1, "the parent's single dispatch"),
                (
                    db::RESOLUTIONS_OF_JOB,
                    1,
                    "one immutable Owner-declared resolution",
                ),
            ],
        )
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
