mod support;

use br_test_harness::GraphqlClient;
use reqwest::StatusCode;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, gql, wire};

/// The word a runner actually published on `jobs.status.scaffold.failed` in dev on 2026-08-25.
/// It is not a retry kind and never was — but the contract typed the field as free text, so it
/// travelled all the way to the receiver before anyone could object, and the job it belonged to
/// stayed in progress with nothing left to move it.
const OUTSIDE_THE_VOCABULARY: &str = "scaffold";

const DISCARDED_TOTAL: &str = "jobs_runner_facts_discarded_total";

#[tokio::test]
async fn a_terminal_fact_naming_an_unknown_failure_kind_is_dropped_counted_and_leaves_the_stream_open()
 {
    // Given: a job in progress on a live instance, watched by the owner's event log
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("inventive");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let trigger = instance.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;

    // Given: nothing has been dropped yet, and both terminal series already exist at zero. An
    // alert reads an absent series as nothing to say, so a counter born on its first drop stays
    // silent in exactly the situation it is written for
    for terminal in ["failed", "completed"] {
        assert_eq!(
            discarded_series(&client, terminal).await,
            vec![0.0],
            "the {terminal} discard series must be registered at boot, not on first use",
        );
    }

    // When: the runner reports the run failed under a kind the published vocabulary does not
    // define
    instance
        .fail_run(
            &trigger,
            OUTSIDE_THE_VOCABULARY,
            "provider_unavailable",
            None,
        )
        .await;

    // Then: nothing is guessed from it. A report Jobs cannot read is never rounded to a retry
    // decision, so the job keeps the state it had and its owner is told nothing that is untrue
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;
    let waiting = gql::job(&client, admin, job_id).await;
    assert_eq!(
        waiting["status"],
        json!("IN_PROGRESS"),
        "an unreadable terminal fact must not move the job: {waiting}",
    );
    assert_eq!(
        gql::run_by_id(&waiting, run)["status"],
        json!("STARTED"),
        "the run stays open — the backstops, not a guess, are what end it: {waiting}",
    );

    // Then: the loss is counted where an operator can see it. This fact carried the end of a run
    // and no longer exists anywhere; a warning line in a pod's logs is not a record of that
    assert_eq!(
        discarded_series(&client, "failed").await,
        vec![1.0],
        "a dropped terminal fact must raise its own series, or the only trace of a job stalled \
         by a non-conforming runner is a log line nothing alerts on\nlogs:\n{}",
        fixture.logs(),
    );

    // Then: the unreadable frame did not park the stream behind it — the next conforming fact
    // from the same runner is applied normally
    instance
        .fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    let escalated = events.expect_one(wire::FACT_FAILED, job_id, LONG).await;
    assert_eq!(
        escalated.payload()["failure_report"]["kind"],
        json!("PERMANENT"),
        "the readable report reaches the owner as-is",
    );
    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;

    events.stop().await;
    fixture.shutdown().await;
}

/// Every value the discard counter renders for one fact, by label rather than by rendered line:
/// the exporter is free to order labels as it likes, and a test that pins the ordering breaks on
/// a dependency bump instead of on a regression. A `Vec` rather than a sum, so "the series is
/// absent" and "the series reads zero" cannot be mistaken for each other.
async fn discarded_series(gql: &GraphqlClient, fact: &str) -> Vec<f64> {
    let (status, body) = gql.get_raw("/metrics").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/metrics must answer — a counter nobody can scrape records nothing",
    );
    let wanted = format!("fact=\"{fact}\"");
    body.lines()
        .filter(|line| line.starts_with(DISCARDED_TOTAL) && line.contains(&wanted))
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_else(|| panic!("a counter line ends in its value: {line}"))
        })
        .collect()
}
