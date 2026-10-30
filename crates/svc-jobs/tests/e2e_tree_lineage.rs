mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::lineage::{create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn every_event_of_a_tree_names_its_job_its_parent_and_its_initial_job() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let (mut x, mut y) = (
        live_runner(&fixture, "lineage-x").await,
        live_runner(&fixture, "lineage-y").await,
    );
    let root = JobDeclaration::new(&x.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let (root_trigger, _) = started_with_agent(&fixture, &mut x).await;
    let root_work = Work::of(&x, &root_trigger);

    let other = JobDeclaration::new(&x.runner_type).with_max_attempts(1);
    producer.declare_v2(&other).await;
    x.next_trigger(LONG).await;
    let mismatched = create(
        &fixture,
        &root_work,
        &y.runner_type,
        root.job_id,
        json!({ "mode": "SYNC", "initial_job_id": other.job_id.to_string() }),
    )
    .await;
    let rejected = answers
        .expect_one(wire::FACT_CREATION_REJECTED, mismatched, LONG)
        .await;
    assert_eq!(rejected.payload()["reason_code"], "initial_job_mismatch");
    durable
        .assert_count(db::JOBS_WITH_ID, mismatched, 0, "no Job")
        .await;

    let child = create(
        &fixture,
        &root_work,
        &y.runner_type,
        root.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let trigger = y.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), child);
    y.start_applied(&fixture, &trigger).await;
    y.declare_plan(&trigger, &["read", "write"]).await;
    y.start_step(&trigger, 0, "read").await;
    y.complete_run(&trigger).await;
    let failing = create(
        &fixture,
        &root_work,
        &y.runner_type,
        root.job_id,
        json!({ "mode": "SYNC", "max_attempts": 1 }),
    )
    .await;
    let doomed = y.next_trigger(LONG).await;
    y.start_applied(&fixture, &doomed).await;
    y.fail_run(&doomed, "PERMANENT", "provider_refused", None)
        .await;
    answers.expect_one(wire::FACT_FAILED, failing, LONG).await;
    producer
        .send_v3(
            wire::VERB_FINISH,
            json!({ "job_id": root.job_id.to_string() }),
        )
        .await;
    answers
        .expect_one(wire::FACT_COMPLETED, root.job_id, LONG)
        .await;
    producer.cancel_v2(other.job_id).await;
    answers
        .expect_one(wire::FACT_CANCELLED, other.job_id, LONG)
        .await;
    answers
        .expect_one(wire::FACT_STEP_STARTED, child, LONG)
        .await;

    let expected = |job: Uuid| {
        if job == child || job == failing {
            (root.job_id, root.job_id)
        } else {
            (job, job)
        }
    };
    let mut facts = std::collections::BTreeSet::new();
    for event in answers.all() {
        let payload = event.payload();
        let Some(job) = payload["job_id"]
            .as_str()
            .and_then(|raw| raw.parse::<Uuid>().ok())
        else {
            continue;
        };
        if ![root.job_id, other.job_id, child, failing, mismatched].contains(&job) {
            continue;
        }
        let (parent, initial) = if job == mismatched {
            (root.job_id, other.job_id)
        } else {
            expected(job)
        };
        assert_eq!(
            payload["parent_job_id"],
            parent.to_string(),
            "{} of {job}: {payload}",
            event.fact
        );
        assert_eq!(
            payload["initial_job_id"],
            initial.to_string(),
            "{} of {job}: {payload}",
            event.fact
        );
        facts.insert(event.fact.clone());
    }
    for fact in [
        "queued",
        "started",
        "plan_declared",
        "step_started",
        "completed",
        "failed",
        "cancelled",
        "creation_rejected",
    ] {
        assert!(
            facts.contains(fact),
            "a {fact} event was checked: {facts:?}"
        );
    }
    answers.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
