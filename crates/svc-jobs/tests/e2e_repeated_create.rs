mod support;

use serde_json::json;
use support::fixture::Knobs;
use support::lineage::live_runner;
use support::producer::JobDeclaration;
use support::repeat_scene::{Scene, correlations, event_ids, payload};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_create_repeated_after_the_dispatch_gets_the_queued_event_again() {
    let scene = Scene::start().await;
    let mut instance = live_runner(&scene.fixture, "repeated-create").await;
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&instance.runner_type);
    let job = declaration.job_id;
    let command = producer.declare(&declaration).await;
    instance.next_trigger(LONG).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;

    let repeat_correlation = Uuid::now_v7();
    producer
        .send_v3_as(
            command.command_id,
            repeat_correlation,
            wire::VERB_CREATE,
            declaration.payload("projects"),
        )
        .await;

    let queued = scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 2, LONG)
        .await;
    assert_eq!(
        event_ids(&queued).len(),
        2,
        "the repeat has a fresh event id"
    );
    assert!(
        correlations(&queued).contains(&repeat_correlation.to_string()),
        "the repeat carries the correlation id of the repeat"
    );
    assert_eq!(
        queued[0].payload()["runner_type"],
        queued[1].payload()["runner_type"],
        "the repeat tells what the first told"
    );
    scene
        .first
        .expect_exactly(wire::FACT_QUEUED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_CREATION_REJECTED, job, QUIET)
        .await;
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(scene.status(job).await, "IN_PROGRESS");
    scene.stop().await;
}

#[tokio::test]
async fn a_creation_refused_for_a_retired_type_is_judged_again_and_accepted_after_the_reactivation()
{
    let scene = Scene::start_with(Knobs {
        retirement_quiet_period_seconds: 3,
        runner_type_impact_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let instance = live_runner(&scene.fixture, "repeated-retired").await;
    let (client, admin) = (scene.fixture.gql(), scene.fixture.admin());
    gql::deprecate_runner_type(&client, admin, &instance.runner_type).await;
    let retired = br_test_harness::wait_until(LONG, || async {
        gql::retire_runner_type(&client, admin, &instance.runner_type).await["data"]
            [wire::FIELD_RETIRE_RUNNER_TYPE]["success"]
            == json!(true)
    })
    .await;
    assert!(retired, "the type is retired once the quiet period ends");
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&instance.runner_type);
    let job = declaration.job_id;
    let command = producer.declare(&declaration).await;
    let refusal = scene
        .first
        .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
        .await;
    assert_eq!(
        refusal.payload()["reason_code"],
        json!(wire::REASON_RUNNER_TYPE_RETIRED)
    );
    scene
        .current
        .expect_none(wire::FACT_QUEUED, job, QUIET)
        .await;

    let reactivated = gql::reactivate_runner_type(&client, admin, &instance.runner_type).await;
    gql::expect_success(
        &reactivated,
        wire::FIELD_REACTIVATE_RUNNER_TYPE,
        "reactivating",
    );
    producer
        .send_v3_as(
            command.command_id,
            Uuid::now_v7(),
            wire::VERB_CREATE,
            declaration.payload("projects"),
        )
        .await;

    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_CREATION_REJECTED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "IN_PROGRESS");
    scene.stop().await;
}
#[tokio::test]
async fn a_create_repeated_after_the_erasure_of_its_request_data_compares_the_skeleton_only() {
    let scene = Scene::start().await;
    let durable = support::db::Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&wire::unique_runner_type("repeated-erased"))
        .with_config(json!({ "seed": 1 }));
    let job = declaration.job_id;
    let command = producer.declare(&declaration).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;
    producer.send_v3(wire::VERB_CANCEL, payload(job)).await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, LONG)
        .await;
    durable
        .assert_count(
            support::erasure::ERASED_STAMP,
            job,
            1,
            "the request data went with the cancel",
        )
        .await;

    producer
        .send_v3_as(
            command.command_id,
            Uuid::now_v7(),
            wire::VERB_CREATE,
            declaration.payload("projects"),
        )
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 2, LONG)
        .await;
    scene
        .first
        .expect_none(wire::FACT_CREATION_REJECTED, job, QUIET)
        .await;

    let another = JobDeclaration::new(&declaration.runner_type)
        .with_id(job)
        .with_max_attempts(2);
    producer.declare(&another).await;
    let refusal = scene
        .first
        .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
        .await;
    assert_eq!(
        refusal.payload()["reason_code"],
        json!(wire::REASON_ID_REUSE)
    );
    durable.close().await;
    scene.stop().await;
}

#[tokio::test]
async fn a_create_of_a_child_job_repeated_gets_the_queued_event_again_with_its_lineage() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let parent = JobDeclaration::new(&wire::unique_runner_type("repeated-parent"));
    producer.declare(&parent).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, parent.job_id, 1, LONG)
        .await;
    let child =
        JobDeclaration::new(&wire::unique_runner_type("repeated-child")).with_parent(parent.job_id);
    let command = producer.declare(&child).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, child.job_id, 1, LONG)
        .await;

    producer
        .send_v3_as(
            command.command_id,
            Uuid::now_v7(),
            wire::VERB_CREATE,
            child.payload("projects"),
        )
        .await;

    let queued = scene
        .current
        .expect_exactly(wire::FACT_QUEUED, child.job_id, 2, LONG)
        .await;
    for event in &queued {
        assert_eq!(
            event.payload()["parent_job_id"],
            json!(parent.job_id.to_string())
        );
    }
    scene
        .first
        .expect_none(wire::FACT_CREATION_REJECTED, child.job_id, QUIET)
        .await;
    scene.stop().await;
}
