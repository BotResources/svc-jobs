mod support;

use std::time::Duration;

use br_test_harness::{E2eDatabase, FabricTestNats, TestNats, wait_until};
use support::broker::RestartableNats;
use support::events::EventLog;
use support::fixture::{self, APP_PASSWORD, APP_ROLE, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, infra, wire};

const OLD_RESTART_BUDGET: u64 = 10;
const WATCHED_TASK: &str = "job-creation consumer";
const LOSS_PATIENCE: Duration = Duration::from_secs(90);
const OUTAGE_PATIENCE: Duration = Duration::from_secs(300);
const ONE_MORE_ATTEMPT: Duration = Duration::from_secs(60);
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

    broker.stop().await;

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

    broker.resume().await;

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
