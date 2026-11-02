mod support;

use support::db::Durable;
use support::producer::JobDeclaration;
use support::repeat_scene::{Scene, event_ids, payload};
use support::{LONG, QUIET, wire};
use uuid::Uuid;

const WITHOUT_RECORDED_REQUESTER: &str = "UPDATE jobs SET requester = NULL WHERE id = $1";

#[tokio::test]
async fn a_cancel_repeated_under_its_command_id_gets_the_acceptance_again() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 1, LONG)
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, LONG)
        .await;
    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;

    let accepted = scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 2, LONG)
        .await;
    assert_eq!(event_ids(&accepted).len(), 2);
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, QUIET)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_CANCELLED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "CANCELLED");
    scene.stop().await;
}

#[tokio::test]
async fn the_owner_of_a_job_without_a_recorded_requester_repeating_a_cancel_gets_the_acceptance_again()
 {
    let scene = Scene::start().await;
    let durable = Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    sqlx::query(WITHOUT_RECORDED_REQUESTER)
        .bind(job)
        .execute(durable.pool())
        .await
        .expect("the Job loses its recorded requester");
    let command = Uuid::now_v7();

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, LONG)
        .await;
    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;

    scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 2, LONG)
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "CANCELLED");

    producer
        .send_v3_as(
            Uuid::now_v7(),
            Uuid::now_v7(),
            wire::VERB_CANCEL,
            payload(job),
        )
        .await;
    assert_eq!(scene.refusals(job, 1).await, vec!["job_terminal"]);
    durable.close().await;
    scene.stop().await;
}

#[tokio::test]
async fn a_cancel_of_a_tree_repeated_gets_the_acceptance_again_and_cancels_nothing_more() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let parent = scene.declared_by(&producer).await;
    let child = JobDeclaration::new(&wire::unique_runner_type("repeated-tree")).with_parent(parent);
    producer.declare(&child).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, child.job_id, 1, LONG)
        .await;
    let command = Uuid::now_v7();

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(parent))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, child.job_id, 1, LONG)
        .await;
    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(parent))
        .await;

    scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, parent, 2, LONG)
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, parent, 1, QUIET)
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, child.job_id, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, parent, QUIET)
        .await;
    assert_eq!(scene.status(child.job_id).await, "CANCELLED");
    scene.stop().await;
}
