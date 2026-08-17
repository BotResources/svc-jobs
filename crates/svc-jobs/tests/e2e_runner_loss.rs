mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::clock;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, delta, gql, infra,
    stream, subs, wire,
};

const BASE_DELAY_SECONDS: u64 = 2;

#[tokio::test]
async fn a_job_survives_the_loss_of_its_runner_without_administrator_intervention() {
    // Given: a job with retry capacity, watched from the list and fleet before it exists
    let fixture = JobsFixture::start_with(Knobs {
        retry_base_delay_seconds: BASE_DELAY_SECONDS,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("volatile");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut lost = FakeRunner::new(fixture.nats(), &runner_type, "instance-lost");
    let mut replacement = FakeRunner::new(fixture.nats(), &runner_type, "instance-replacement");
    let announced_version = lost.version.clone();

    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(3);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    delta::assert_active_affordances(&delta::assert_upserted_summary(
        &stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await,
        job_id,
        "PENDING",
    ));
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await;

    let mut job_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    stream::snapshot(&mut job_watch, JOB_CHANGED, SHORT).await;
    let mut tail = SseSubscription::open(fixture.url(), admin, &subs::log_tail(job_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;

    // When: an instance appears, claims the job and reports its first progress
    lost.connect().await;
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    let first = lost.next_trigger(LONG).await;
    let first_run = runner::run_id(&first);
    let first_dispatch =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    assert_eq!(
        delta::event_of(&first_dispatch, wire::EVT_RUN_DISPATCHED, job_id)["runId"],
        json!(first_run.to_string())
    );
    lost.start_run(&first).await;
    let started =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    delta::projection(&started, job_id, "IN_PROGRESS");
    delta::assert_active_affordances(&started);
    lost.start_step(&first, 0, "convert").await;
    let stepped =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_STEP_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&stepped, wire::EVT_STEP_STARTED, job_id)["stepIndex"],
        json!(0)
    );
    lost.log_line(&first, Some(0), "INFO", "first attempt")
        .await;
    delta::log_line(
        &stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await[0],
        first_run,
        Some(0),
    );
    let executing =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;
    let carrying = delta::assert_fleet(&executing, &runner_type, 0, 1, 1, 0);
    let saturated = delta::assert_instance(
        &carrying,
        "instance-lost",
        true,
        &[first_run],
        &announced_version,
    );
    assert_eq!(
        (
            saturated["capacity"].clone(),
            carrying["totalCapacity"].clone()
        ),
        (json!(1), json!(1)),
        "an instance carrying as many runs as it declared is busy exactly at saturation, and its \
         declaration is what the type totals: {executing}",
    );

    // When: the instance reports a new self-declared status, then dies without a goodbye
    lost.announce(wire::STATUS_DRAINING).await;
    let reported =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_STATUS_REPORTED, LONG)
            .await;
    let rewritten = delta::fleet_projection(&reported, &runner_type);
    assert_eq!(
        rewritten["instances"][0]["reportedStatus"],
        json!(wire::STATUS_DRAINING),
        "a presence rewrite carries the instance's self-reported status: {reported}",
    );
    assert_eq!(
        rewritten["totalCapacity"],
        json!(0),
        "a draining instance declares no room the type can offer, however wide it is: {reported}",
    );
    assert_eq!(
        rewritten["isAvailable"],
        json!(false),
        "an instance winding down takes no new work, so its type stops being available while it \
         is the only one live: {reported}",
    );
    assert_eq!(
        gql::assert_blocked(&reported, wire::ACTION_DISPATCH),
        wire::REASON_RUNNER_TYPE_UNAVAILABLE,
        "the drain flip is pushed with the backend's own dispatch verdict, so no client has to \
         infer 'nothing can go out' from a flag: {reported}",
    );
    delta::assert_instance(
        &rewritten,
        "instance-lost",
        true,
        &[first_run],
        &announced_version,
    );

    lost.crash();
    replacement.resume_after(&lost);
    tokio::time::sleep(infra::PRESENCE_TTL).await;

    let disconnected =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_DISCONNECTED, LONG).await;
    assert_eq!(
        disconnected["event"]["instanceKey"],
        json!("instance-lost"),
        "this instance issued no delete and no purge: it simply stopped refreshing its presence \
         entry, which is all a crashed process ever does. The entry died of its own TTL, and that \
         eviction is a disconnection signal in its own right — a service that only reacts to an \
         explicit removal never notices a real crash, and its runs hang for ever",
    );
    let empty_fleet = delta::fleet_projection(&disconnected, &runner_type);
    assert_eq!(empty_fleet["isAvailable"], json!(false));
    assert_eq!(empty_fleet["instances"], json!([]));

    let run_failed =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let orphaned = delta::event_of(&run_failed, wire::EVT_RUN_FAILED, job_id);
    assert_eq!(orphaned["runId"], json!(first_run.to_string()));
    assert_eq!(orphaned["failureKind"], json!("TRANSIENT"));
    assert_eq!(
        orphaned["reasonCode"],
        json!(wire::REASON_INSTANCE_LOST),
        "losing an instance fails its runs under the reason the contract names",
    );

    let scheduled =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RETRY_SCHEDULED, LONG).await;
    let due_at =
        delta::instant(&delta::event_of(&scheduled, wire::EVT_RETRY_SCHEDULED, job_id)["dueAt"]);
    let surviving = delta::projection(&scheduled, job_id, "IN_PROGRESS");
    assert_eq!(delta::instant(&surviving["nextAttemptAt"]), due_at);
    delta::assert_active_affordances(&scheduled);
    delta::assert_active_affordances(&delta::assert_upserted_summary(
        &stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_RETRY_SCHEDULED, LONG).await,
        job_id,
        "IN_PROGRESS",
    ));
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;

    // Then: no trigger is produced while the type is unavailable, due time or not
    let past_due = (due_at - clock::now())
        .to_std()
        .unwrap_or(Duration::from_secs(0))
        + Duration::from_secs(BASE_DELAY_SECONDS + 2);
    replacement.expect_no_trigger(past_due).await;
    assert!(
        clock::now() > due_at,
        "the silence window must outlast the recorded due time to prove anything",
    );
    assert_eq!(
        gql::status_of(&client, admin, job_id).await,
        "IN_PROGRESS",
        "no administrator touched this job",
    );

    // When: a replacement instance appears
    replacement.connect().await;
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    let second = replacement.next_trigger(LONG).await;
    let second_run = runner::run_id(&second);
    assert_eq!(runner::attempt_number(&second), 2);
    assert_eq!(runner::job_id(&second), job_id);
    assert_ne!(
        second_run, first_run,
        "a retry is a new run, never a rewrite"
    );
    let retry_dispatch =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    assert_eq!(
        delta::event_of(&retry_dispatch, wire::EVT_RUN_DISPATCHED, job_id)["attemptNumber"],
        json!(2)
    );
    replacement.start_run(&second).await;
    let retry_started =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&retry_started, wire::EVT_RUN_STARTED, job_id)["instanceKey"],
        json!("instance-replacement")
    );
    let retry_executing =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;
    delta::assert_instance(
        &delta::assert_fleet(&retry_executing, &runner_type, 0, 1, 1, 0),
        "instance-replacement",
        true,
        &[second_run],
        &announced_version,
    );

    replacement
        .log_line(&second, None, "INFO", "second attempt")
        .await;
    delta::log_line(
        &stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await[0],
        second_run,
        None,
    );
    replacement.complete_run(&second).await;
    let completed =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, LONG).await;
    delta::projection(&completed, job_id, "IN_PROGRESS");
    let stopped =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_EXECUTING, LONG).await;
    delta::assert_instance(
        &delta::assert_fleet(&stopped, &runner_type, 0, 0, 0, 1),
        "instance-replacement",
        false,
        &[],
        &announced_version,
    );

    // When: the replacement leaves gracefully, carrying no run any more
    replacement.disconnect().await;
    let gracefully_gone =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_DISCONNECTED, LONG).await;
    assert_eq!(
        gracefully_gone["event"]["instanceKey"],
        json!("instance-replacement"),
        "graceful removal of the presence entry is the other disconnection signal",
    );
    delta::assert_fleet(&gracefully_gone, &runner_type, 0, 0, 0, 0);
    stream::expect_no_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_FAILED, QUIET).await;
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;

    // When: the owner finishes the job
    producer.finish(job_id).await;
    let job_completed =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_JOB_COMPLETED, LONG).await;
    let final_projection = delta::projection(&job_completed, job_id, "COMPLETED");
    assert_eq!(
        final_projection["runs"].as_array().map(Vec::len),
        Some(2),
        "the final projection holds both attempts: {job_completed}",
    );
    delta::assert_completed_affordances(&job_completed);
    events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;

    let logs = gql::logs_of(&client, admin, job_id).await;
    assert_eq!(
        logs.iter()
            .filter(|line| line["runId"] == json!(first_run.to_string()))
            .count(),
        1,
        "each attempt keeps its own log lines: {logs:?}",
    );
    assert_eq!(
        logs.iter()
            .filter(|line| line["runId"] == json!(second_run.to_string()))
            .count(),
        1,
        "a log is associated only with the run that produced it: {logs:?}",
    );

    for run in [first_run, second_run] {
        let of_run = gql::logs_of_run(&client, admin, job_id, run).await;
        assert_eq!(
            of_run.len(),
            1,
            "reading the log by run returns that attempt's lines alone, never the job's union: \
             {of_run:?}",
        );
        assert_eq!(of_run[0]["runId"], json!(run.to_string()));
    }

    // Then: the log tail reads backwards from its end without losing or repeating a line
    let tail_page = gql::log_page(&client, admin, job_id, json!({ "last": 1 })).await;
    let tail_edges = gql::edges_of(&tail_page);
    assert_eq!(tail_edges.len(), 1);
    assert_eq!(
        tail_edges[0]["node"]["runId"],
        json!(second_run.to_string()),
        "reading the last line answers the newest attempt's line: {tail_page}",
    );
    assert_eq!(
        tail_page["pageInfo"]["hasPreviousPage"],
        json!(true),
        "a tail that does not hold the whole log must say so: {tail_page}",
    );
    let start_cursor = tail_page["pageInfo"]["startCursor"]
        .as_str()
        .unwrap_or_else(|| panic!("a non-empty log page carries its start cursor: {tail_page}"))
        .to_string();
    let older_page = gql::log_page(
        &client,
        admin,
        job_id,
        json!({ "last": 1, "before": start_cursor }),
    )
    .await;
    let older_edges = gql::edges_of(&older_page);
    assert_eq!(older_edges.len(), 1);
    assert_eq!(
        older_edges[0]["node"]["runId"],
        json!(first_run.to_string()),
        "walking backwards from the cursor hands over the previous line, never the same one again: \
         {older_page}",
    );
    assert_eq!(older_page["pageInfo"]["hasPreviousPage"], json!(false));

    // Then: each run names the instance that executed it, and its own terminal status
    let audited = gql::job(&client, admin, job_id).await;
    for (run, instance_key) in [
        (first_run, "instance-lost"),
        (second_run, "instance-replacement"),
    ] {
        assert_eq!(
            gql::run_by_id(&audited, run)["instance"],
            json!({ "runnerType": runner_type, "instanceKey": instance_key }),
            "a run keeps the instance that started it, so an administrator can still tell which \
             process abandoned the work: {audited}",
        );
    }
    assert_eq!(
        gql::run_by_id(&audited, first_run)["status"],
        json!("FAILED")
    );
    assert_eq!(
        gql::run_by_id(&audited, second_run)["status"],
        json!("COMPLETED"),
        "the run the replacement finished is projected as completed, not left started: {audited}",
    );

    durable
        .assert_all(
            job_id,
            &[
                (db::JOBS_WITH_ID, 1, "one job"),
                (db::RUNS_OF_JOB, 2, "two runs"),
                (db::RETRY_SCHEDULES_OF_JOB, 1, "one recorded retry schedule"),
                (db::LOGS_OF_JOB, 2, "one log line per attempt"),
            ],
        )
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
