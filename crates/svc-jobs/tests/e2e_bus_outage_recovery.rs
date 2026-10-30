//! The background NATS consumers recover from a bus outage of any length, on
//! their own.
//!
//! Up to 0.5.0 a supervised task was restarted at most ten times in a row: an
//! outage longer than that (about two minutes on the shipped backoff) left the
//! pod NOT READY for good while `/livez` stayed OK, so nothing ever restarted it
//! and no job was created, cancelled, completed or dispatched until an operator
//! did. This scenario replays that outage against the real binary, on a backoff
//! of 10 ms up to 1 s so that the waits add nothing: what paces "more than ten
//! failed attempts" (about two minutes) is each attempt timing out against the
//! dead broker, as it does in production.
//!
//! 1. the service boots ready on a broker the scenario owns;
//! 2. the broker is stopped, long enough for the job-creation consumer to fail
//!    and be restarted well past the old budget — the pod is NOT READY and names
//!    it, `/livez` still answers;
//! 3. the broker comes back on the same endpoint with its streams;
//! 4. with no restart of the process, `/readyz` turns ready by itself, and a job
//!    declared afterwards is dispatched, run and completed end to end.
//!
//! A regression suite for the breach behind jobs 0.5.1, not a registry gesture.

mod support;

use std::time::Duration;

use br_test_harness::{E2eDatabase, FabricTestNats, TestNats, wait_until};
use support::broker::RestartableNats;
use support::events::EventLog;
use support::fixture::{self, APP_PASSWORD, APP_ROLE, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, infra, wire};

/// The quick restarts 0.5.0 allowed before giving a task up for good.
const OLD_RESTART_BUDGET: u64 = 10;
/// The supervised task this scenario watches: the first a producer's job meets.
const WATCHED_TASK: &str = "job-creation consumer";
/// A consumer notices a dead broker when it misses its idle heartbeats: about
/// 30 s after the broker stops.
const LOSS_PATIENCE: Duration = Duration::from_secs(90);
/// Every later attempt fails at the JetStream request timeout (about 10 s), then
/// parks for 1 s at most: eleven restarts take about two minutes.
const OUTAGE_PATIENCE: Duration = Duration::from_secs(300);
/// One more failed attempt and its park.
const ONE_MORE_ATTEMPT: Duration = Duration::from_secs(60);
/// The longest park (1 s) plus the time to bind every consumer again.
const RECOVERY_PATIENCE: Duration = Duration::from_secs(60);

#[tokio::test]
async fn the_consumers_rebind_after_an_outage_longer_than_the_old_budget_without_a_restart() {
    fixture::require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let mut broker = RestartableNats::start().await;
    provision(&broker.url()).await;

    // Given: the service booted and ready, on a restart backoff of 10 ms doubling up to 1 s
    let knobs = Knobs {
        task_restart_initial_backoff_milliseconds: Some(10),
        task_restart_max_backoff_seconds: Some(1),
        ..Knobs::default()
    };
    let (service, url) = fixture::spawn_instance(&db, &broker.url(), &knobs).await;
    assert_eq!(
        probe(&format!("{url}/readyz")).await.map(|(s, _)| s),
        Some(200)
    );

    // When: the broker goes away
    broker.stop().await;

    // Then: the pod leaves rotation, and names the consumer that lost its bus
    let named = wait_until(LOSS_PATIENCE, || async {
        matches!(
            probe(&format!("{url}/readyz")).await,
            Some((503, body)) if body.contains(WATCHED_TASK)
        )
    })
    .await;
    assert!(
        named,
        "/readyz must turn NOT READY and name the lost {WATCHED_TASK}\nlogs:\n{}",
        service.logs()
    );

    // When: the outage outlasts the old restart budget
    let past_the_old_budget = wait_until(OUTAGE_PATIENCE, || async {
        restarts_of(&url, WATCHED_TASK).await > OLD_RESTART_BUDGET
    })
    .await;
    assert!(
        past_the_old_budget,
        "the {WATCHED_TASK} was restarted {} times in {OUTAGE_PATIENCE:?} — the outage never \
         outlasted the old budget, so this scenario proves nothing\nlogs:\n{}",
        restarts_of(&url, WATCHED_TASK).await,
        service.logs()
    );

    // Then: the pod is still out of rotation, yet alive — a dependency outage never fails
    // liveness, which would only turn it into a restart storm
    assert_eq!(
        probe(&format!("{url}/readyz")).await.map(|(s, _)| s),
        Some(503)
    );
    assert_eq!(
        probe(&format!("{url}/livez")).await.map(|(s, _)| s),
        Some(200),
        "/livez is not tied to the bus"
    );
    assert_eq!(gauge_up(&url, WATCHED_TASK).await, Some(0.0));

    // And: it keeps trying — restarts go on climbing past the old budget
    let before = restarts_of(&url, WATCHED_TASK).await;
    let climbing = wait_until(ONE_MORE_ATTEMPT, || async {
        restarts_of(&url, WATCHED_TASK).await > before
    })
    .await;
    assert!(
        climbing,
        "the {WATCHED_TASK} stopped being retried at {before} restarts\nlogs:\n{}",
        service.logs()
    );

    // When: the broker comes back on the same endpoint, with its streams
    broker.resume().await;

    // Then: with no restart of the process, the pod turns ready by itself
    let recovered = wait_until(RECOVERY_PATIENCE, || async {
        matches!(probe(&format!("{url}/readyz")).await, Some((200, _)))
    })
    .await;
    assert!(
        recovered,
        "/readyz never returned to 200 on its own: the consumers did not bind again\nlogs:\n{}",
        service.logs()
    );
    assert_eq!(gauge_up(&url, WATCHED_TASK).await, Some(1.0));
    let logs = service.logs();
    assert!(
        logs.contains("bound to its source of work again"),
        "each recovery is logged\nlogs:\n{logs}"
    );

    // Then: and it serves — a job declared now is dispatched, run and completed end to end
    let fabric = FabricTestNats::connect(&broker.url()).await;
    let nats = TestNats::setup_on(&broker.url()).await;
    let events = EventLog::open(&fabric).await;
    let producer = Producer::new(&fabric, "projects");
    let runner_type = wire::unique_runner_type("outage");
    let mut instance = FakeRunner::new(&nats, &runner_type, "instance-a");
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), job_id);
    instance.start_run(&trigger).await;
    events.expect_one(wire::FACT_STARTED, job_id, LONG).await;
    instance.complete_run(&trigger).await;
    producer.finish(job_id).await;
    events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;

    instance.disconnect().await;
    events.stop().await;
    nats.cleanup().await;
    fabric.shutdown().await;
    service.shutdown().await;
    broker.shutdown().await;
    db.cleanup().await;
}

/// The infrastructure a running platform declares before the service starts:
/// the fixed integration streams, the published-language bucket and the runner
/// transport. It lives in the broker's store, so it survives the outage.
async fn provision(url: &str) {
    let fabric = FabricTestNats::connect(url)
        .await
        .with_published_language()
        .await;
    let nats = TestNats::setup_on(url).await;
    infra::provision_runner_transport(&nats, infra::STEADY_PRESENCE_TTL).await;
    nats.cleanup().await;
    fabric.shutdown().await;
}

/// A probe's status and body, or `None` while the listener does not answer.
async fn probe(url: &str) -> Option<(u16, String)> {
    let response = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    let status = response.status().as_u16();
    Some((status, response.text().await.unwrap_or_default()))
}

/// The value of one series of `/metrics` for a task, if it is exposed.
async fn series(url: &str, metric: &str, task: &str) -> Option<f64> {
    let (_, body) = probe(&format!("{url}/metrics")).await?;
    let label = format!("task=\"{task}\"");
    body.lines()
        .filter(|line| line.starts_with(metric) && line.contains(&label))
        .find_map(|line| line.rsplit(' ').next()?.parse().ok())
}

async fn restarts_of(url: &str, task: &str) -> u64 {
    series(url, "jobs_background_task_restarts_total", task)
        .await
        .map_or(0, |value| value as u64)
}

async fn gauge_up(url: &str, task: &str) -> Option<f64> {
    series(url, "jobs_background_task_up", task).await
}
