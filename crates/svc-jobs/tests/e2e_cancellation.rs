mod support;

use br_test_harness::{SseSubscription, verdict};
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
async fn an_administrator_cancels_a_job_tree_without_leaving_work_running_or_queued() {
    // Given: a root with one executing descendant and one waiting for an unavailable type
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let parent_type = wire::unique_runner_type("tree_root");
    let running_type = wire::unique_runner_type("tree_running");
    let unserved_type = wire::unique_runner_type("tree_unserved");
    let claimable_type = wire::unique_runner_type("tree_claimable");
    let producer = Producer::new(fixture.fabric(), "projects");
    let owner = Producer::new(fixture.fabric(), "jobs");

    let mut root = FakeRunner::new(fixture.nats(), &parent_type, "root-a");
    let mut busy = FakeRunner::new(fixture.nats(), &running_type, "busy-a");
    let mut latecomer = FakeRunner::new(fixture.nats(), &unserved_type, "late-a");
    let mut idler = FakeRunner::new(fixture.nats(), &claimable_type, "idle-a");
    root.connect().await;
    busy.connect().await;
    idler.connect().await;

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;
    let parent_trigger = root.next_trigger(LONG).await;
    let parent_run = runner::run_id(&parent_trigger);
    root.start_run(&parent_trigger).await;
    gql::wait_for_status(&client, admin, parent_id, "IN_PROGRESS", LONG).await;

    let running_child = JobDeclaration::new(&running_type).with_parent(parent_id);
    let running_child_id = running_child.job_id;
    owner.declare(&running_child).await;
    let running_trigger = busy.next_trigger(LONG).await;
    let running_child_run = runner::run_id(&running_trigger);
    busy.start_run(&running_trigger).await;
    gql::wait_for_status(&client, admin, running_child_id, "IN_PROGRESS", LONG).await;

    let queued_child = JobDeclaration::new(&unserved_type).with_parent(parent_id);
    let queued_child_id = queued_child.job_id;
    owner.declare(&queued_child).await;
    events
        .expect_one(wire::FACT_QUEUED, queued_child_id, LONG)
        .await;
    assert_eq!(
        gql::job(&client, admin, queued_child_id).await["attemptCount"],
        json!(0),
        "dispatch waits while the runner type is unavailable",
    );
    latecomer.expect_no_trigger(QUIET).await;

    // Given: a third child whose type is served, dispatched but claimed by nobody
    let dispatched_child = JobDeclaration::new(&claimable_type).with_parent(parent_id);
    let dispatched_child_id = dispatched_child.job_id;
    owner.declare(&dispatched_child).await;
    events
        .expect_one(wire::FACT_QUEUED, dispatched_child_id, LONG)
        .await;
    let unclaimed_trigger = idler.next_trigger(LONG).await;
    let unclaimed_run = runner::run_id(&unclaimed_trigger);
    assert_eq!(
        idler.trigger_count().await,
        1,
        "the third child's trigger is in line, waiting for an instance to claim it",
    );
    assert_eq!(
        gql::run_by_id(
            &gql::job(&client, admin, dispatched_child_id).await,
            unclaimed_run
        )["status"],
        json!("PENDING"),
        "this run is dispatched and never started — that is the state a withdrawal acts on",
    );

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(parent_id)).await;
    let tree = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    assert_eq!(
        tree["job"]["children"].as_array().map(Vec::len),
        Some(3),
        "the opening snapshot is the complete tree: {tree}",
    );
    let mut unclaimed_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed(dispatched_child_id),
    )
    .await;
    stream::snapshot(&mut unclaimed_watch, JOB_CHANGED, SHORT).await;
    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&running_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut tail = SseSubscription::open(fixture.url(), admin, &subs::log_tail(parent_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&running_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: the administrator cancels the root
    let resolution_id = Uuid::now_v7();
    let ack = gql::cancel_job(&client, admin, resolution_id, parent_id).await;
    verdict::expect_ack(&ack, "an administrator cancels a non-terminal job");
    assert_eq!(
        ack["data"]["jobsCancelJob"],
        json!({ "success": true }),
        "the mutation answers a verdict, never the cancelled state",
    );

    // Then: one cancellation per job, downward, each under its own id
    for job_id in [
        parent_id,
        running_child_id,
        queued_child_id,
        dispatched_child_id,
    ] {
        let cancelled = events.expect_one(wire::FACT_CANCELLED, job_id, LONG).await;
        assert_eq!(
            cancelled.payload()["job_id"],
            json!(job_id.to_string()),
            "cancellation travels downward, one event per cancelled job under its own id",
        );
        gql::wait_for_status(&client, admin, job_id, "CANCELLED", LONG).await;
    }

    let root_cancelled =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_CANCELLED, LONG).await;
    assert_eq!(
        delta::event_of(&root_cancelled, wire::EVT_JOB_CANCELLED, parent_id)["resolutionId"],
        json!(resolution_id.to_string()),
        "the root carries the resolution id the administrator supplied",
    );
    let cancelled_tree = delta::projection(&root_cancelled, parent_id, "CANCELLED");
    delta::assert_cancelled_affordances(&root_cancelled);
    for child_id in [running_child_id, queued_child_id, dispatched_child_id] {
        let child = gql::child_of(&cancelled_tree, child_id);
        assert_eq!(
            child["job"]["status"],
            json!("CANCELLED"),
            "every formerly non-terminal descendant is cancelled in the pushed tree",
        );
        delta::assert_cancelled_affordances(&child);
    }
    assert_eq!(
        cancelled_tree["runs"].as_array().map(Vec::len),
        Some(1),
        "the execution history stays readable on a cancelled job: {cancelled_tree}",
    );

    let run_cancelled =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_CANCELLED, LONG).await;
    assert_eq!(
        delta::event_of(&run_cancelled, wire::EVT_RUN_CANCELLED, parent_id)["runId"],
        json!(parent_run.to_string()),
        "the root's in-flight run is terminated by its own event",
    );

    let listed =
        stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_CANCELLED, LONG).await;
    delta::assert_cancelled_affordances(&delta::assert_upserted_summary(
        &listed,
        running_child_id,
        "CANCELLED",
    ));
    delta::assert_fleet(
        &stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_EXECUTING, LONG).await,
        &running_type,
        0,
        0,
        0,
        1,
    );

    // Then: the transport carries the stop request only where a run is in flight
    let parent_entry = root.await_cancel_entry(parent_run, LONG).await;
    assert_eq!(parent_entry["run_id"], json!(parent_run.to_string()));
    busy.await_cancel_entry(running_child_run, LONG).await;

    let withdrawn = gql::job(&client, admin, queued_child_id).await;
    assert_eq!(withdrawn["status"], json!("CANCELLED"));
    assert_eq!(
        withdrawn["runs"].as_array().map(Vec::len),
        Some(0),
        "queued work is withdrawn rather than left in line: {withdrawn}",
    );
    assert_eq!(
        latecomer.trigger_count().await,
        0,
        "a job waiting on an unavailable type never had a trigger to withdraw",
    );

    // Then: the dispatched-but-unclaimed attempt is withdrawn, not stopped
    let unclaimed_cancelled = stream::await_delta(
        &mut unclaimed_watch,
        JOB_CHANGED,
        wire::EVT_RUN_CANCELLED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(
            &unclaimed_cancelled,
            wire::EVT_RUN_CANCELLED,
            dispatched_child_id
        )["runId"],
        json!(unclaimed_run.to_string()),
    );
    delta::projection(&unclaimed_cancelled, dispatched_child_id, "CANCELLED");
    let withdrawn_run = gql::run_by_id(
        &gql::job(&client, admin, dispatched_child_id).await,
        unclaimed_run,
    );
    assert_eq!(withdrawn_run["status"], json!("CANCELLED"));
    assert!(
        withdrawn_run["startedAt"].is_null(),
        "queued work withdrawn before it starts goes straight from PENDING to CANCELLED: \
         {withdrawn_run}",
    );
    idler.expect_no_cancel_entry(unclaimed_run, QUIET).await;
    assert_eq!(
        idler.trigger_count().await,
        0,
        "cancelling queued work withdraws its undelivered trigger — one left in line is executed \
         by the next instance to pull, and its status facts are then silently discarded, so the \
         zombie work is invisible to every observer",
    );

    // When: the formerly unavailable runner type finally appears
    latecomer.connect().await;
    latecomer.expect_no_trigger(QUIET).await;
    events
        .expect_none(wire::FACT_STARTED, queued_child_id, QUIET)
        .await;
    assert_eq!(
        gql::job(&client, admin, queued_child_id).await["attemptCount"],
        json!(0),
        "presence never resurrects cancelled work",
    );

    // When: a late runner fact races the cancellation
    busy.complete_run(&running_trigger).await;
    busy.start_run(&running_trigger).await;
    busy.fail_run(&running_trigger, "TRANSIENT", "late_noise", None)
        .await;
    events
        .expect_none(wire::FACT_COMPLETED, running_child_id, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, QUIET).await;

    let reconciled = gql::job(&client, admin, running_child_id).await;
    assert_eq!(reconciled["status"], json!("CANCELLED"));
    assert_ne!(
        gql::run_by_id(&reconciled, running_child_run)["status"],
        json!("COMPLETED"),
        "a late fact against a terminal job is acknowledged and discarded",
    );
    assert_eq!(
        reconciled["runs"].as_array().map(Vec::len),
        Some(1),
        "a discarded late fact creates no run",
    );
    assert_eq!(root.trigger_count().await, 1);
    assert_eq!(busy.trigger_count().await, 1);

    // Then: the desired-state entries are withdrawn once their runs are terminal
    root.await_no_cancel_entry(parent_run, LONG).await;
    busy.await_no_cancel_entry(running_child_run, LONG).await;

    let repeat = gql::cancel_job(&client, admin, Uuid::now_v7(), parent_id).await;
    verdict::expect_code_shaped(&repeat, "cancelling an already terminal job");
    events
        .expect_exactly(wire::FACT_CANCELLED, parent_id, 1, QUIET)
        .await;
    tail.expect_silence("cancellation appends no log line", QUIET)
        .await;

    // Then: the audit trail the edge cannot show — one resolution per job, one request per run
    for job_id in [
        parent_id,
        running_child_id,
        queued_child_id,
        dispatched_child_id,
    ] {
        durable
            .assert_count(
                db::RESOLUTIONS_OF_JOB,
                job_id,
                1,
                "one cancellation resolution — a second row on a job a late fact touched would be \
                 invisible to a read that shows only the latest",
            )
            .await;
    }
    for (job_id, what) in [
        (parent_id, "the root's in-flight run"),
        (running_child_id, "the descendant's in-flight run"),
    ] {
        durable
            .assert_count(
                db::CANCEL_REQUESTS_OF_JOB,
                job_id,
                1,
                &format!(
                    "{what} carries its durable stop request — the bucket entry is replayed from \
                     it, so an unrecorded request cannot survive a restart"
                ),
            )
            .await;
    }

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
