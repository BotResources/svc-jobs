mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, Knobs, runner_access};
use support::gesture::{self};
use support::producer::{JobDeclaration, Producer};
use support::restart::*;
use support::runner;
use support::tree::{Pair, call_child, pair};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

#[tokio::test]
async fn the_restart_of_an_initial_job_stays_in_its_tree_without_a_parent() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-initial").await;
    save_state(&fixture, &scene.parent_work, json!({ "turn": 1 })).await;
    scene.pair.parent.complete_run(&scene.parent_trigger).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            scene.parent_run(),
            2,
            "the root agents finish",
        )
        .await;

    let mail = gesture::Mail::new(scene.receiver, &[scene.sender]);
    let sent = gesture::send_mail(&fixture.gql(), &runner_access(), &scene.child_work, &mail).await;
    gesture::accepted(&sent, "jobsSendMail");

    let trigger = scene.pair.parent.next_trigger(LONG).await;
    assert_eq!(
        trigger["restart"]["restart_of_run_id"],
        json!(scene.parent_run().to_string())
    );
    assert_eq!(trigger["initial_job_id"], json!(scene.root.to_string()));
    let (parent, tree): (Option<Uuid>, Uuid) = sqlx::query_as(
        "SELECT parent_job_id::uuid, COALESCE(root_job_id, id)::uuid FROM jobs WHERE id = $1",
    )
    .bind(runner::job_id(&trigger))
    .fetch_one(durable.pool())
    .await
    .expect("the restart Job exists");
    assert_eq!(parent, None, "an Initial Job has no parent to keep");
    assert_eq!(
        tree, scene.root,
        "the restart stays in the Tree of its origin"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_retired_type_leaves_the_mail_unread_and_names_the_refused_restart() {
    let fixture = JobsFixture::start_with(Knobs {
        retirement_quiet_period_seconds: 3,
        runner_type_impact_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-retired").await;
    gql::deprecate_runner_type(
        &fixture.gql(),
        fixture.admin(),
        &scene.pair.child.runner_type,
    )
    .await;
    scene.finish_child(&durable).await;
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_FINISH,
            json!({
                "job_id": scene.child_work.job_id.to_string(),
                "acting_run_id": scene.parent_run().to_string(),
                "acting_instance_key": scene.pair.parent.instance_key,
            }),
        )
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        scene.child_work.job_id,
        "COMPLETED",
        LONG,
    )
    .await;
    let retired = br_test_harness::wait_until(LONG, || async {
        let answer = gql::retire_runner_type(
            &fixture.gql(),
            fixture.admin(),
            &scene.pair.child.runner_type,
        )
        .await;
        answer["data"][wire::FIELD_RETIRE_RUNNER_TYPE]["success"] == json!(true)
    })
    .await;
    assert!(retired, "the type is retired once the quiet period ends");

    let mail = gesture::Mail::new(scene.sender, &[scene.receiver]);
    let sent =
        gesture::send_mail(&fixture.gql(), &runner_access(), &scene.parent_work, &mail).await;
    let outcome = gesture::field(&sent, "jobsSendMail");
    assert_eq!(outcome["success"], true, "the mail is delivered: {sent}");
    assert_eq!(
        outcome["restartRefused"][0]["address"],
        json!(scene.receiver.to_string()),
        "the refusal of the restart is named in the reply"
    );
    assert_eq!(
        outcome["restartRefused"][0]["reason"], "restart_refused_type_retired",
        "the refusal carries a reason an agent can read"
    );
    assert!(
        outcome["restartRefused"][0]["message"]
            .as_str()
            .is_some_and(|words| words.contains("retired")),
        "{sent}"
    );
    assert_eq!(
        scene.restartable_now(&fixture, scene.receiver).await,
        false,
        "the list shows q1 not restartable"
    );
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            support::restart::UNREAD_AT,
            scene.receiver,
            1,
            "the mail stays unread",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_failed_job_reaches_the_manual_retry_successor_and_leaves_the_failed_job_as_it_is()
 {
    let fixture = JobsFixture::start().await;
    let mut pair: Pair<'_> = pair(&fixture, "cancel-successor").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &parent).await;
    let parent_run = runner::run_id(&parent);

    let failed = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    let failed_job = runner::job_id(&failed);
    pair.child.start_applied(&fixture, &failed).await;
    pair.child
        .fail_run(&failed, "PERMANENT", "boom", None)
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), failed_job, "FAILED", LONG).await;
    let resolution = Uuid::parse_str(
        gql::job_view(&fixture.gql(), fixture.admin(), failed_job).await["job"]["resolution"]["id"]
            .as_str()
            .expect("a failed job names its resolution"),
    )
    .unwrap();
    let successor = Uuid::now_v7();
    let retried = gql::manual_retry_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        failed_job,
        successor,
        resolution,
    )
    .await;
    assert!(
        retried.get("errors").is_none_or(Value::is_null),
        "{retried}"
    );
    let retry = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retry), successor);

    producer
        .send_v3(
            wire::VERB_CANCEL,
            json!({
                "job_id": failed_job.to_string(),
                "acting_run_id": parent_run.to_string(),
                "acting_instance_key": pair.parent.instance_key,
            }),
        )
        .await;

    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        successor,
        "CANCELLED",
        LONG,
    )
    .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), failed_job).await,
        "FAILED",
        "a terminal Job stays as it is"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_job_cancels_the_non_terminal_restart_jobs_that_stem_from_it() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-cancel").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    let restart_job = runner::job_id(&trigger);
    scene.pair.child.start_applied(&fixture, &trigger).await;

    let cancelled = gql::cancel_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        scene.child_work.job_id,
    )
    .await;
    assert!(
        cancelled.get("errors").is_none_or(|e| e.is_null()),
        "{cancelled}"
    );

    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        scene.child_work.job_id,
        "CANCELLED",
        LONG,
    )
    .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        restart_job,
        "CANCELLED",
        LONG,
    )
    .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cancel_naming_a_run_of_the_caller_that_ended_with_success_is_refused() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let answers = support::events::EventLog::open_v2(fixture.fabric()).await;
    let scene = running_child(&fixture, "cancel-ended-caller").await;
    scene.pair.parent.complete_run(&scene.parent_trigger).await;
    durable
        .await_count(
            "SELECT count(*) FROM run_terminals WHERE run_id = $1 AND kind = 'COMPLETED'",
            scene.parent_run(),
            1,
            "the caller Run ends with success",
        )
        .await;

    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CANCEL,
            json!({
                "job_id": scene.child_work.job_id.to_string(),
                "acting_run_id": scene.parent_run().to_string(),
                "acting_instance_key": scene.pair.parent.instance_key,
            }),
        )
        .await;

    let refused = answers
        .expect_one(wire::FACT_COMMAND_REJECTED, scene.child_work.job_id, LONG)
        .await;
    assert!(
        refused.payload()["reason_code"].is_string(),
        "the refusal names a reason: {}",
        refused.payload()
    );
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), scene.child_work.job_id).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}
