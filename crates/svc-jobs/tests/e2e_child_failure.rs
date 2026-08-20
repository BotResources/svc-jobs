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
    let sibling_type = wire::unique_runner_type("sibling_worker");
    let producer = Producer::new(fixture.fabric(), "projects");
    let owner = Producer::new(fixture.fabric(), "jobs");
    let mut orchestrator = FakeRunner::new(fixture.nats(), &parent_type, "orchestrator-a");
    let mut worker = FakeRunner::new(fixture.nats(), &child_type, "worker-a");
    let mut sibling_worker = FakeRunner::new(fixture.nats(), &sibling_type, "sibling-a");

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_for(&[&parent_type, &child_type, &sibling_type]),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&parent_type)).await;
    let opening_fleet = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(opening_fleet["runnerTypes"], json!([]));

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;
    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    delta::event_of(&queued, wire::EVT_QUEUED, parent_id);
    delta::assert_active_affordances(&delta::assert_upserted_summary(
        &queued, parent_id, "PENDING",
    ));
    fleet_watch
        .expect_silence(
            "queued work cannot materialize an unregistered runner type",
            QUIET,
        )
        .await;
    assert!(
        gql::fleet_of(&client, admin, &parent_type).await.is_empty(),
        "fleet reads omit a routing key until its first presence registers the entity",
    );
    drop(fleet_watch);
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&parent_type)).await;
    let post_declaration = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(post_declaration["runnerTypes"], json!([]));

    let mut parent_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(parent_id)).await;
    let opening = stream::snapshot(&mut parent_watch, JOB_CHANGED, SHORT).await;
    assert_eq!(opening["job"]["status"], json!("PENDING"));
    delta::assert_active_affordances(&opening);

    // When: the parent runner claims its work and reports its progression
    orchestrator.connect().await;
    let registered =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_REGISTERED, LONG).await;
    delta::assert_fleet(&registered, &parent_type, 1, 0, 0, 1);
    gql::assert_allowed(&registered, wire::ACTION_DISPATCH);
    gql::assert_allowed(&registered, wire::ACTION_DEPRECATE);
    assert_eq!(
        gql::assert_blocked(&registered, wire::ACTION_REACTIVATE),
        "runner_type_not_deprecated",
    );
    assert_eq!(
        gql::assert_blocked(&registered, wire::ACTION_RETIRE),
        "runner_type_not_deprecated",
    );
    let connected =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    let connected_type = delta::fleet_projection(&connected, &parent_type);
    assert_eq!(connected_type["isAvailable"], json!(true));
    delta::assert_instance(
        &connected_type,
        "orchestrator-a",
        false,
        &[],
        &orchestrator.version,
    );
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
    let sibling = JobDeclaration::new(&sibling_type).with_parent(parent_id);
    let sibling_id = sibling.job_id;
    owner.declare(&child).await;
    owner.declare(&sibling).await;
    for descendant in [child_id, sibling_id] {
        events.expect_one(wire::FACT_QUEUED, descendant, LONG).await;
        let listed = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
        delta::assert_active_affordances(&delta::upserted(&listed, descendant));
    }
    sibling_worker.connect().await;
    let sibling_trigger = sibling_worker.next_trigger(LONG).await;
    let sibling_run = runner::run_id(&sibling_trigger);
    sibling_worker.start_run(&sibling_trigger).await;
    gql::wait_for_status(&client, admin, sibling_id, "IN_PROGRESS", LONG).await;

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
    let sibling_view = gql::job(&client, admin, sibling_id).await;
    assert_eq!(
        sibling_view["status"],
        json!("IN_PROGRESS"),
        "no sibling is failed or cancelled automatically",
    );
    assert_eq!(
        gql::run_by_id(&sibling_view, sibling_run)["status"],
        json!("STARTED"),
        "a brother executing at the moment of the failure keeps its own run alive",
    );
    sibling_worker
        .expect_no_cancel_entry(sibling_run, QUIET)
        .await;
    assert_eq!(
        gql::run_by_id(&gql::job(&client, admin, parent_id).await, parent_run)["status"],
        json!("STARTED"),
        "the parent's own run is never terminated by a child's fate",
    );
    for job_id in [parent_id, sibling_id] {
        for fact in [wire::FACT_FAILED, wire::FACT_CANCELLED] {
            events.expect_none(fact, job_id, QUIET).await;
        }
    }
    stream::expect_no_delta_of(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_JOB_FAILED,
        parent_id,
        QUIET,
    )
    .await;

    // When: the parent runner decides to spawn an alternative child instead
    let alternative = JobDeclaration::new(&child_type).with_parent(parent_id);
    let alternative_id = alternative.job_id;
    owner.declare(&alternative).await;
    events
        .expect_one(wire::FACT_QUEUED, alternative_id, LONG)
        .await;
    let alternative_queued = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_QUEUED,
        alternative_id,
        LONG,
    )
    .await;
    delta::assert_active_affordances(&delta::upserted(&alternative_queued, alternative_id));
    let alternative_trigger = worker.next_trigger(LONG).await;
    let alternative_dispatched = stream::await_delta_of(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        alternative_id,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(
            &alternative_dispatched,
            wire::EVT_RUN_DISPATCHED,
            alternative_id
        )["runId"],
        json!(runner::run_id(&alternative_trigger).to_string()),
        "the dispatch of the alternative child is pushed as its own delta, so a client never has \
         to refetch to learn a run exists",
    );
    let mut alternative_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(alternative_id)).await;
    let alternative_opening = stream::snapshot(&mut alternative_watch, JOB_CHANGED, SHORT).await;
    assert_eq!(
        alternative_opening["job"]["runs"][0]["id"],
        json!(runner::run_id(&alternative_trigger).to_string()),
        "an administrator arriving after the dispatch opens on the run the trigger carries: \
         {alternative_opening}",
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
    owner.finish(alternative_id).await;
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
    stream::expect_no_delta_of(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        parent_id,
        QUIET,
    )
    .await;

    // When: the owner closes the parent while its own run is still executing
    producer.finish(parent_id).await;
    let parent_stopped = stream::await_delta(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_RUN_CANCELLED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(&parent_stopped, wire::EVT_RUN_CANCELLED, parent_id)["runId"],
        json!(parent_run.to_string()),
        "a job resolved by its owner withdraws the run it left executing, before it closes",
    );
    let stop_order = orchestrator.await_cancel_entry(parent_run, LONG).await;
    assert_eq!(
        stop_order["run_id"],
        json!(parent_run.to_string()),
        "the instance still executing the parent is told to stop through the desired-state \
         bucket — without that entry it burns compute for ever on a job that is already \
         COMPLETED: {stop_order}",
    );
    let parent_completed = stream::await_delta(
        &mut parent_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        LONG,
    )
    .await;
    let parent_resolution = delta::event_of(&parent_completed, wire::EVT_JOB_COMPLETED, parent_id)
        ["resolutionId"]
        .clone();
    assert!(
        parent_resolution
            .as_str()
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .is_some(),
        "the finish command declares only the job id, so jobs identifies the resolution itself: \
         {parent_completed}",
    );
    delta::projection(&parent_completed, parent_id, "COMPLETED");
    delta::assert_completed_affordances(&parent_completed);
    events
        .expect_one(wire::FACT_COMPLETED, parent_id, LONG)
        .await;

    let closed_parent = gql::job(&client, admin, parent_id).await;
    assert_eq!(
        closed_parent["resolution"]["id"], parent_resolution,
        "the read answers with the very resolution the stream announced: {closed_parent}",
    );
    let withdrawn = gql::run_by_id(&closed_parent, parent_run);
    assert_eq!(
        withdrawn["status"],
        json!("CANCELLED"),
        "the run the owner's decision withdrew is closed, never left STARTED for ever: \
         {closed_parent}",
    );
    assert!(
        !withdrawn["cancellationRequestedAt"].is_null(),
        "the withdrawal is recorded on the run as a request, not only as a terminal status: \
         {withdrawn}",
    );
    support::views::assert_views_agree(
        &durable,
        &client,
        admin,
        parent_id,
        "a job closed over a run that was still executing",
    )
    .await;

    durable
        .assert_all(
            parent_id,
            &[
                (db::JOBS_WITH_ID, 1, "one parent"),
                (db::RUNS_OF_JOB, 1, "one run per dispatch"),
                (
                    db::CANCEL_REQUESTS_OF_JOB,
                    1,
                    "one durable stop request for the run the resolution withdrew",
                ),
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
            &[
                (
                    db::RUNS_OF_JOB,
                    1,
                    "the sibling's own dispatch, still alive",
                ),
                (
                    db::RESOLUTIONS_OF_JOB,
                    0,
                    "no resolution ever reached the untouched sibling",
                ),
            ],
        )
        .await;
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
    let note = "the provider refused the only source this job had";
    producer.fail(parent_id, Some(note)).await;

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
    assert_eq!(
        payload["note"],
        json!(note),
        "the Owner's own words on why are carried to every consumer of the failure — they are the \
         only explanation an Owner-declared failure has: {payload}",
    );

    let failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    let pushed = delta::event_of(&failed, wire::EVT_JOB_FAILED, parent_id);
    assert_eq!(pushed["failureCause"], json!("DECLARED_BY_OWNER"));
    let resolution_id = pushed["resolutionId"].clone();
    assert!(
        resolution_id
            .as_str()
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .is_some(),
        "the fail command declares the job and the Owner's note, so the resolution identity is \
         jobs' own: {pushed}",
    );
    delta::projection(&failed, parent_id, "FAILED");
    delta::assert_failed_affordances(&failed);
    delta::assert_failed_affordances(&delta::assert_upserted_summary(
        &stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_FAILED, LONG).await,
        parent_id,
        "FAILED",
    ));

    let view = gql::job_view(&client, admin, parent_id).await;
    let resolution = view["job"]["resolution"].clone();
    assert_eq!(
        resolution["id"], resolution_id,
        "the read answers with the very resolution the stream announced: {resolution}",
    );
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
