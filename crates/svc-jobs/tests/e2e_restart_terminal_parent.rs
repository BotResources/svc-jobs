mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN, running_child, save_state};
use support::runner;
use support::surface::{self, Agent};
use support::tree::{call_child, pair};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

const FAILURE_NOTICES_AT: &str = "SELECT count(*) FROM inbox_items \
     WHERE address = $1 AND kind = 'NOTICE' AND read_at IS NULL \
     AND notice->>'type' = 'JOB_FAILED'";

#[tokio::test]
async fn an_agent_under_a_terminal_parent_refuses_mail_keeps_its_failure_notice_unread_and_never_restarts()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "terminal-parent-notice").await;
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let root_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &root_trigger).await;
    let root_work = Work::of(&pair.parent, &root_trigger);
    let sender = Agent::with_inbox();
    let sender_id = sender.agent_id;
    surface::register(fixture.nats(), &pair.parent, root_work.run_id, &[sender]).await;

    let job = call_child(&fixture, &mut pair, root.job_id, root_work.run_id).await;
    pair.child.start_applied(&fixture, &job).await;
    let job_work = Work::of(&pair.child, &job);
    let r1 = Agent::with_inbox();
    let r1_id = r1.agent_id;
    surface::register(fixture.nats(), &pair.child, job_work.run_id, &[r1]).await;
    let called = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": called.to_string(),
                "runner_type": pair.parent.runner_type,
                "producer": "projects",
                "parent_job_id": job_work.job_id.to_string(),
                "mode": "BACKGROUND",
                "caller_address": r1_id.to_string(),
                "max_attempts": 1,
                "acting_run_id": job_work.run_id.to_string(),
                "acting_instance_key": pair.child.instance_key,
            }),
        )
        .await;
    let called_trigger = pair.parent.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&called_trigger), called);
    save_state(&fixture, &job_work, json!({ "turn": 3 })).await;
    pair.child.complete_run(&job).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            job_work.run_id,
            1,
            "r1 finished with a saved state",
        )
        .await;
    producer.finish_v2(root.job_id).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        root.job_id,
        "COMPLETED",
        LONG,
    )
    .await;

    let mail = Mail::new(sender_id, &[r1_id]);
    let refused = gesture::send_mail(&fixture.gql(), &runner_access(), &root_work, &mail).await;
    assert_eq!(
        gesture::refusal(&refused, "jobsSendMail"),
        "bad_addresses",
        "{refused}"
    );
    let bad = &gesture::field(&refused, "jobsSendMail")["badAddresses"];
    assert_eq!(bad[0]["address"], r1_id.to_string());
    assert_eq!(
        bad[0]["reason"], "recipient_cannot_restart",
        "the same reason as a finished agent that cannot restart"
    );
    assert!(
        bad[0]["message"]
            .as_str()
            .is_some_and(|words| !words.is_empty())
    );

    pair.parent.start_applied(&fixture, &called_trigger).await;
    pair.parent
        .fail_run(&called_trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), called, "FAILED", LONG).await;
    durable
        .await_count(
            FAILURE_NOTICES_AT,
            r1_id,
            1,
            "the failure notice is kept for r1",
        )
        .await;
    pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            job_work.run_id,
            0,
            "a failure notice under a terminal parent starts no restart",
        )
        .await;
    durable
        .assert_count(
            FAILURE_NOTICES_AT,
            r1_id,
            1,
            "the failure notice stays unread",
        )
        .await;

    let mut live = running_child(&fixture, "terminal-parent-control").await;
    live.finish_child(&durable).await;
    live.mail_receiver(&fixture, live.sender).await;
    live.pair.child.next_trigger(LONG).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            live.child_run(),
            1,
            "a mail to a restartable agent of a live type and parent starts a restart",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
