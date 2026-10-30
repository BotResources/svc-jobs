mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture;
use support::restart::running_child;
use support::runner;
use support::surface::{self, Agent};
use support::{LONG, gql};
use uuid::Uuid;

#[tokio::test]
async fn a_manual_retry_gets_new_agents_and_does_not_inherit_the_ones_of_the_failed_run() {
    let fixture = JobsFixture::start().await;
    let mut scene = running_child(&fixture, "retry-new-agents").await;
    let failed_job = scene.child_work.job_id;
    scene
        .pair
        .child
        .fail_run(&scene.child_trigger, "PERMANENT", "boom", None)
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
        retried.get("errors").is_none_or(|e| e.is_null()),
        "{retried}"
    );
    let retry = scene.pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retry), successor);
    assert!(
        retry["restart"].is_null(),
        "a retry is not a restart: it carries no origin Run"
    );
    scene.pair.child.start_applied(&fixture, &retry).await;
    let retry_run = runner::run_id(&retry);

    let refused = gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        &scene.parent_work,
        &gesture::Mail::new(scene.sender, &[scene.receiver]),
    )
    .await;
    assert_eq!(
        gesture::field(&refused, "jobsSendMail")["badAddresses"][0]["reason"],
        "recipient_removed",
        "the agents of the failed Run left the directory, so a retry does not inherit them"
    );
    let fresh = surface::register(
        fixture.nats(),
        &scene.pair.child,
        retry_run,
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(
        fresh["accepted"], true,
        "a retry registers new agents: {fresh}"
    );

    let state = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVED_STATE,
            json!({ "work": support::gesture::Work::of(&scene.pair.child, &retry).wire() }),
        )
        .await;
    assert_eq!(
        gesture::refusal(&state, "jobsSavedState"),
        "not_a_restart_run",
        "a retry reads no saved state: only a restart does"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn items_that_arrived_unread_are_all_returned_once_in_order_and_never_again_after_their_marker()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let scene = running_child(&fixture, "reconnection").await;
    let gql = fixture.gql();
    let passport = runner_access();

    let first = gesture::Mail::new(scene.sender, &[scene.receiver]);
    let second = gesture::Mail::new(scene.peer, &[scene.receiver]);
    gesture::accepted(
        &gesture::send_mail(&gql, &passport, &scene.parent_work, &first).await,
        "jobsSendMail",
    );
    gesture::accepted(
        &gesture::send_mail(&gql, &passport, &scene.parent_work, &second).await,
        "jobsSendMail",
    );

    let waiting = gesture::items(
        &gesture::unread(&gql, &passport, &scene.child_work, Some(scene.receiver)).await,
    );
    let mails: Vec<String> = waiting
        .iter()
        .map(|item| item["mailId"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        mails,
        vec![first.mail_id.to_string(), second.mail_id.to_string()],
        "both mails are returned, in order"
    );
    let rings = durable.rings_for(scene.child_run(), None).await;
    assert_eq!(rings, 2, "each arrival was announced once to the live Run");

    let ids: Vec<Uuid> = waiting
        .iter()
        .map(|item| Uuid::parse_str(item["itemId"].as_str().unwrap()).unwrap())
        .collect();
    let marked =
        gesture::mark_read(fixture.nats(), &scene.pair.child, scene.child_run(), &ids).await;
    assert_eq!(marked["accepted"], true, "{marked}");
    let after = gesture::items(
        &gesture::unread(&gql, &passport, &scene.child_work, Some(scene.receiver)).await,
    );
    assert!(
        after.is_empty(),
        "nothing is returned twice after its marker"
    );

    let foreign = gesture::unread(&gql, &passport, &scene.child_work, Some(scene.sender)).await;
    assert!(
        gesture::items(&foreign).is_empty()
            && foreign["data"]["jobsUnread"]["success"] == json!(false),
        "what is addressed to another Run is not read by this one: {foreign}"
    );
    durable.close().await;
    fixture.shutdown().await;
}
