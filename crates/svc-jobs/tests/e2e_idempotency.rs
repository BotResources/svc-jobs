mod support;

use br_test_harness::{SseSubscription, wait_until};
use chrono::Utc;
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, codes, delta, gql,
    stream, subs, wire,
};
use uuid::Uuid;

#[tokio::test]
async fn delivery_retries_and_administrator_reconnection_do_not_duplicate_a_jobs_history() {
    // Given: every channel watched before the producer's command is even sent
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("idempotent");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: the producer loses its acknowledgement and the same command is delivered again
    let declaration = JobDeclaration::new(&runner_type).with_config(json!({ "seed": 1 }));
    let job_id = declaration.job_id;
    let command = producer.declare(&declaration).await;
    producer.redeliver(&command).await;
    producer.redeliver(&command).await;

    events
        .expect_exactly(wire::FACT_QUEUED, job_id, 1, QUIET)
        .await;
    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    delta::assert_active_affordances(&delta::assert_upserted_summary(&queued, job_id, "PENDING"));
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await;
    listing
        .expect_silence("an absorbed redelivery upserts the window once", QUIET)
        .await;
    fleet_watch
        .expect_silence("an absorbed redelivery moves the fleet once", QUIET)
        .await;

    let mut watch = SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let mut tail = SseSubscription::open(fixture.url(), admin, &subs::log_tail(job_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;
    durable
        .assert_all(job_id, &[(db::JOBS_WITH_ID, 1, "one job")])
        .await;

    // When: the same id comes back with different content
    let conflicting = JobDeclaration::new(&runner_type)
        .with_id(job_id)
        .with_config(json!({ "seed": 2 }));
    producer
        .reuse_command_id_with(&command, conflicting.payload("projects"))
        .await;
    let rejection = events
        .expect_one(wire::FACT_CREATION_REJECTED, job_id, LONG)
        .await;
    let code = rejection.payload()["reason_code"]
        .as_str()
        .expect("a rejection carries a stable reason code")
        .to_string();
    codes::assert_stable_reason(&code, "conflicting reuse of a known job id");
    assert_eq!(
        code,
        wire::REASON_ID_REUSE,
        "conflicting reuse is rejected under the literal the offer names",
    );
    watch
        .expect_silence("a rejected reuse changes nothing on the job", QUIET)
        .await;
    listing
        .expect_silence("a rejected reuse reaches no list subscriber", QUIET)
        .await;
    fleet_watch
        .expect_silence("a rejected reuse moves no fleet count", QUIET)
        .await;
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        gql::job(&client, admin, job_id).await["config"],
        json!({ "seed": 1 }),
        "conflicting content never overwrites the accepted declaration",
    );

    // When: the runner redelivers each of its status facts
    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);

    instance.start_run(&trigger).await;
    instance.start_run(&trigger).await;
    instance.declare_plan(&trigger, &["convert"]).await;
    instance.declare_plan(&trigger, &["convert"]).await;
    instance.start_step(&trigger, 0, "convert").await;
    instance.start_step(&trigger, 0, "convert").await;

    let started = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    let first_start = delta::projection(&started, job_id, "IN_PROGRESS")["runs"][0]["id"].clone();
    let planned = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_PLAN_DECLARED, LONG).await;
    assert_eq!(
        delta::event_of(&planned, wire::EVT_PLAN_DECLARED, job_id)["declarationNumber"],
        json!(1),
        "an absorbed redelivery never opens a second declaration",
    );
    let stepped = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_STEP_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&stepped, wire::EVT_STEP_STARTED, job_id)["stepIndex"],
        json!(0)
    );
    for repeated in [
        wire::EVT_RUN_STARTED,
        wire::EVT_PLAN_DECLARED,
        wire::EVT_STEP_STARTED,
    ] {
        stream::expect_no_delta(&mut watch, JOB_CHANGED, repeated, QUIET).await;
    }
    events
        .expect_exactly(wire::FACT_STARTED, job_id, 1, QUIET)
        .await;
    events
        .expect_exactly(wire::FACT_PLAN_DECLARED, job_id, 1, QUIET)
        .await;
    events
        .expect_exactly(wire::FACT_STEP_STARTED, job_id, 1, QUIET)
        .await;
    assert_eq!(first_start, json!(run.to_string()));

    // When: the administrator disconnects while the job keeps moving
    drop(watch);
    drop(tail);
    drop(listing);
    drop(fleet_watch);

    let logged_at = Utc::now().to_rfc3339();
    for _ in 0..2 {
        instance
            .log_line_at(
                &trigger,
                Some(0),
                "INFO",
                "one line, delivered twice",
                logged_at.clone(),
            )
            .await;
    }
    instance
        .log_line(&trigger, Some(0), "INFO", "the line that follows the pair")
        .await;
    instance.start_step(&trigger, 1, "store").await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;

    let landed = wait_until(LONG, || async {
        gql::logs_of(&client, admin, job_id)
            .await
            .iter()
            .any(|line| line["message"] == json!("the line that follows the pair"))
    })
    .await;
    assert!(
        landed,
        "the line published behind the pair must reach the audit record within {LONG:?} — the log \
         stream is consumed in order, so its arrival is what proves both deliveries of the pair \
         were already applied, without waiting on a clock"
    );
    let logs = gql::logs_of(&client, admin, job_id).await;
    let repeated = logs
        .iter()
        .filter(|line| line["message"] == json!("one line, delivered twice"))
        .count();
    assert_eq!(
        repeated, 1,
        "the sealed LogLine carries no identity field of its own, so the identity a redelivery \
         repeats is the one the stream carries: the same line delivered twice is one line in the \
         audit record, and it is never lost: {logs:?}"
    );

    // Then: reopening reconstructs the state that moved, without one query
    let mut watch = SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    let snapshot = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    assert_eq!(snapshot["job"]["status"], json!("IN_PROGRESS"));
    assert_eq!(snapshot["job"]["attemptCount"], json!(1));
    assert_eq!(
        snapshot["job"]["progression"]["currentStep"]["index"],
        json!(1),
        "the reconnection snapshot carries the progression that advanced while it was away",
    );
    assert_eq!(snapshot["job"]["runs"].as_array().map(Vec::len), Some(1));
    delta::assert_active_affordances(&snapshot);

    let mut tail = SseSubscription::open(fixture.url(), admin, &subs::log_tail(job_id)).await;
    let tailed = stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;
    let mut ids: Vec<String> = tailed["logs"]["edges"]
        .as_array()
        .expect("a log snapshot carries its edges")
        .iter()
        .map(|edge| edge["node"]["id"].to_string())
        .collect();
    let before_dedup = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(
        (ids.len(), before_dedup),
        (2, 2),
        "a reconnecting log tail returns each of the two logical lines exactly once, the \
         redelivered one included: {tailed}",
    );
    tail.expect_silence(
        "a redelivered log line never appends a second time behind the snapshot",
        QUIET,
    )
    .await;

    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    let relisted = stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let rows = relisted["jobs"]["edges"]
        .as_array()
        .expect("a list snapshot carries its edges");
    assert_eq!(
        rows.iter()
            .filter(|edge| edge["node"]["job"]["id"] == json!(job_id.to_string()))
            .count(),
        1,
        "the reopened window represents the job exactly once: {relisted}",
    );
    delta::assert_active_affordances(&rows[0]["node"]);
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    let refleeted = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(
        refleeted["runnerTypes"][0]["runnerType"]["executingJobCount"],
        json!(1),
        "the reopened fleet snapshot is the current live state: {refleeted}",
    );

    // When: execution resumes live, then completes twice over
    instance
        .log_line(&trigger, Some(1), "INFO", "after the reconnection")
        .await;
    delta::log_line(
        &stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await[0],
        run,
        Some(1),
    );
    watch
        .expect_silence("a log fact produces no job delta", QUIET)
        .await;

    instance.complete_run(&trigger).await;
    instance.complete_run(&trigger).await;
    let completed =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, LONG).await;
    delta::assert_active_affordances(&completed);
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, QUIET).await;

    let finish_command = Uuid::now_v7();
    producer.finish_as(finish_command, job_id).await;
    producer.finish_as(finish_command, job_id).await;
    let job_completed =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_COMPLETED, LONG).await;
    let resolution_id =
        delta::event_of(&job_completed, wire::EVT_JOB_COMPLETED, job_id)["resolutionId"].clone();
    delta::projection(&job_completed, job_id, "COMPLETED");
    delta::assert_completed_affordances(&job_completed);
    events
        .expect_exactly(wire::FACT_COMPLETED, job_id, 1, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_COMPLETED, QUIET).await;
    assert_eq!(
        gql::job(&client, admin, job_id).await["resolution"]["id"],
        resolution_id,
        "the second finish command settles nothing of its own — the job keeps the single terminal \
         resolution the first one produced",
    );

    durable
        .assert_all(
            job_id,
            &[
                (db::JOBS_WITH_ID, 1, "one job"),
                (db::RUNS_OF_JOB, 1, "one logical run"),
                (db::PLAN_DECLARATIONS_OF_JOB, 1, "one plan declaration"),
                (db::LOGS_OF_JOB, 3, "one copy of each log line"),
                (db::RESOLUTIONS_OF_JOB, 1, "one terminal resolution"),
            ],
        )
        .await;
    assert_eq!(
        instance.trigger_count().await,
        1,
        "an absorbed redelivery dispatches no second attempt",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
