mod support;

use serde_json::json;
use support::gesture::Work;
use support::lineage::{act_as, create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::repeat_scene::{Scene, correlations, event_ids, payload};
use support::{LONG, QUIET, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_finish_repeated_under_its_command_id_gets_the_completion_again() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 1, LONG)
        .await;
    let repeat_correlation = Uuid::now_v7();
    producer
        .send_v3_as(command, repeat_correlation, wire::VERB_FINISH, payload(job))
        .await;

    let completed = scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 2, LONG)
        .await;
    assert_eq!(event_ids(&completed).len(), 2);
    assert!(correlations(&completed).contains(&repeat_correlation.to_string()));
    scene
        .first
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "COMPLETED");

    producer
        .send_v3_as(
            Uuid::now_v7(),
            Uuid::now_v7(),
            wire::VERB_FINISH,
            payload(job),
        )
        .await;
    assert_eq!(scene.refusals(job, 1).await, vec!["job_terminal"]);
    scene.stop().await;
}

#[tokio::test]
async fn a_fail_repeated_under_its_command_id_gets_the_failure_again_with_the_note_of_the_repeat() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();
    let body = |note: &str| json!({ "job_id": job.to_string(), "note": note });

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FAIL, body("first"))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_FAILED, job, 1, LONG)
        .await;
    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FAIL, body("again"))
        .await;

    let failed = scene
        .current
        .expect_exactly(wire::FACT_FAILED, job, 2, LONG)
        .await;
    let mut notes: Vec<String> = failed
        .iter()
        .map(|event| {
            event.payload()["note"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    notes.sort();
    assert_eq!(notes, vec!["again", "first"]);
    for event in &failed {
        assert_eq!(event.payload()["failure_cause"], json!("DECLARED_BY_OWNER"));
    }
    scene
        .first
        .expect_exactly(wire::FACT_FAILED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "FAILED");
    scene.stop().await;
}

#[tokio::test]
async fn only_an_actor_that_may_send_the_command_gets_the_first_outcome_and_a_refusal_is_judged_again()
 {
    let scene = Scene::start().await;
    let owner = scene.producer("projects");
    let stranger = scene.producer("billing");
    let job = scene.declared_by(&owner).await;
    let command = Uuid::now_v7();

    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    assert_eq!(scene.refusals(job, 1).await, vec!["not_requester"]);
    assert_eq!(scene.status(job).await, "PENDING");

    owner
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 1, LONG)
        .await;

    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    assert_eq!(
        scene.refusals(job, 2).await,
        vec!["not_requester", "not_requester"]
    );
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;
    scene
        .current
        .expect_none(wire::FACT_CANCEL_ACCEPTED, job, QUIET)
        .await;
    scene.stop().await;
}

#[tokio::test]
async fn a_stranger_repeating_the_fail_of_the_owner_gets_a_refusal_and_no_second_failure() {
    let scene = Scene::start().await;
    let owner = scene.producer("projects");
    let stranger = scene.producer("billing");
    let job = scene.declared_by(&owner).await;
    let command = Uuid::now_v7();
    let body = json!({ "job_id": job.to_string(), "note": "the owner" });

    owner
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FAIL, body.clone())
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_FAILED, job, 1, LONG)
        .await;
    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FAIL, body)
        .await;

    assert_eq!(scene.refusals(job, 1).await, vec!["not_requester"]);
    scene
        .current
        .expect_exactly(wire::FACT_FAILED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "FAILED");
    scene.stop().await;
}

#[tokio::test]
async fn a_stranger_repeating_the_cancel_of_the_owner_gets_a_refusal_and_no_second_acceptance() {
    let scene = Scene::start().await;
    let owner = scene.producer("projects");
    let stranger = scene.producer("billing");
    let job = scene.declared_by(&owner).await;
    let command = Uuid::now_v7();

    owner
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 1, LONG)
        .await;
    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;

    assert_eq!(scene.refusals(job, 1).await, vec!["not_requester"]);
    scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "CANCELLED");
    scene.stop().await;
}

#[tokio::test]
async fn a_burst_of_the_same_command_is_answered_for_each_delivery_and_refused_for_none() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();

    for _ in 0..4 {
        producer
            .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
            .await;
    }

    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 4, LONG)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "COMPLETED");
    scene.stop().await;
}

#[tokio::test]
async fn a_run_repeating_its_finish_of_a_called_job_gets_the_completion_again() {
    let scene = Scene::start().await;
    let (mut caller, mut called) = (
        live_runner(&scene.fixture, "repeated-caller").await,
        live_runner(&scene.fixture, "repeated-called").await,
    );
    let root = JobDeclaration::new(&caller.runner_type);
    Producer::new(scene.fixture.fabric(), "projects")
        .declare(&root)
        .await;
    let (trigger, _) = started_with_agent(&scene.fixture, &mut caller).await;
    let caller_work = Work::of(&caller, &trigger);
    let child = create(
        &scene.fixture,
        &caller_work,
        &called.runner_type,
        root.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    called.next_trigger(LONG).await;
    let command = Uuid::now_v7();

    act_as(
        &scene.fixture,
        command,
        wire::VERB_FINISH,
        child,
        &caller_work,
    )
    .await;
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, child, 1, LONG)
        .await;
    act_as(
        &scene.fixture,
        command,
        wire::VERB_FINISH,
        child,
        &caller_work,
    )
    .await;

    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, child, 2, LONG)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, child, QUIET)
        .await;
    assert_eq!(scene.status(child).await, "COMPLETED");
    scene.stop().await;
}
