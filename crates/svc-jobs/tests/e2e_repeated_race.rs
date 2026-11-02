mod support;

use futures::future::join_all;
use serde_json::{Value, json};
use support::db::Durable;
use support::producer::{JobDeclaration, Producer};
use support::repeat_scene::{Scene, event_ids, payload};
use support::{LONG, QUIET, wire};
use uuid::Uuid;

const DELIVERIES: usize = 12;
const RESOLUTIONS_OF_THE_JOB: &str = "SELECT count(*) FROM job_resolutions WHERE job_id = $1";

async fn deliver_at_once(producer: &Producer<'_>, command: Uuid, verb: &str, body: Value) {
    join_all(
        (0..DELIVERIES).map(|_| producer.send_v3_as(command, Uuid::now_v7(), verb, body.clone())),
    )
    .await;
}

async fn assert_one_resolution_answered_everywhere(
    scene: &Scene,
    durable: &Durable,
    job: Uuid,
    fact: &str,
) {
    let answers = scene
        .current
        .expect_exactly(fact, job, DELIVERIES, LONG)
        .await;
    assert_eq!(event_ids(&answers).len(), DELIVERIES);
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    durable
        .assert_count(
            RESOLUTIONS_OF_THE_JOB,
            job,
            1,
            "one resolution however many deliveries raced",
        )
        .await;
}

#[tokio::test]
async fn a_finish_delivered_many_times_at_once_to_two_pods_resolves_the_job_once() {
    let scene = Scene::start_cluster(2).await;
    let durable = Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;

    deliver_at_once(&producer, Uuid::now_v7(), wire::VERB_FINISH, payload(job)).await;

    assert_one_resolution_answered_everywhere(&scene, &durable, job, wire::FACT_COMPLETED).await;
    scene
        .first
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "COMPLETED");
    durable.close().await;
    scene.stop().await;
}

#[tokio::test]
async fn a_fail_delivered_many_times_at_once_to_two_pods_resolves_the_job_once() {
    let scene = Scene::start_cluster(2).await;
    let durable = Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;

    deliver_at_once(
        &producer,
        Uuid::now_v7(),
        wire::VERB_FAIL,
        json!({ "job_id": job.to_string(), "note": "raced" }),
    )
    .await;

    assert_one_resolution_answered_everywhere(&scene, &durable, job, wire::FACT_FAILED).await;
    scene
        .first
        .expect_exactly(wire::FACT_FAILED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "FAILED");
    durable.close().await;
    scene.stop().await;
}

#[tokio::test]
async fn a_cancel_delivered_many_times_at_once_to_two_pods_resolves_the_job_once() {
    let scene = Scene::start_cluster(2).await;
    let durable = Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;

    deliver_at_once(&producer, Uuid::now_v7(), wire::VERB_CANCEL, payload(job)).await;

    assert_one_resolution_answered_everywhere(&scene, &durable, job, wire::FACT_CANCEL_ACCEPTED)
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "CANCELLED");
    durable.close().await;
    scene.stop().await;
}

#[tokio::test]
async fn a_create_delivered_many_times_at_once_to_two_pods_queues_the_job_once() {
    let scene = Scene::start_cluster(2).await;
    let durable = Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&wire::unique_runner_type("repeated-race"));
    let job = declaration.job_id;

    deliver_at_once(
        &producer,
        Uuid::now_v7(),
        wire::VERB_CREATE,
        declaration.payload("projects"),
    )
    .await;

    let queued = scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, DELIVERIES, LONG)
        .await;
    assert_eq!(event_ids(&queued).len(), DELIVERIES);
    scene
        .current
        .expect_none(wire::FACT_CREATION_REJECTED, job, QUIET)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_QUEUED, job, 1, QUIET)
        .await;
    durable
        .assert_count(
            support::db::JOBS_WITH_ID,
            job,
            1,
            "one Job however many deliveries raced",
        )
        .await;
    durable.close().await;
    scene.stop().await;
}
