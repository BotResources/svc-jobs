mod support;

use serde_json::json;
use support::db::Durable;
use support::events::EventLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::lineage::{act, live_runner};
use support::producer::Producer;
use support::runner;
use support::scrub::{age_orphan_uploads, age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::scrub_scene::{ENDED_RUN, live_root, long_runs};
use support::surface::{self, Agent};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

const KEPT_OF_JOB: &str = "SELECT count(*) FROM kept_payloads WHERE job_id = $1";
const ATTACHMENTS_OF_MAIL: &str = "SELECT count(*) FROM mail_attachments WHERE mail_id = $1";

async fn create_kept(
    fixture: &JobsFixture,
    acting: &Work,
    runner_type: &str,
    parent: Uuid,
    job: Uuid,
) {
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": job.to_string(),
                "runner_type": runner_type,
                "producer": "projects",
                "parent_job_id": parent.to_string(),
                "mode": "SYNC",
                "max_attempts": 1,
                "payload": { "form": "KEPT" },
                "acting_run_id": acting.run_id.to_string(),
                "acting_instance_key": acting.instance_key,
            }),
        )
        .await;
}

async fn upload(fixture: &JobsFixture, work: &Work, job: Uuid) {
    gesture::accepted(
        &gesture::upload_payload(
            &fixture.gql(),
            &runner_access(),
            job,
            "the brief",
            Some(work),
        )
        .await,
        "jobsUploadPayload",
    );
}

#[tokio::test]
async fn an_upload_that_no_job_or_mail_ever_uses_is_erased_after_the_retention() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let live = live_root(&fixture, "orphans", 1).await;
    let (sender, recipient) = (Agent::with_inbox(), Agent::with_inbox());
    let mail = Mail::new(sender.agent_id, &[recipient.agent_id])
        .with_attachment(Uuid::now_v7(), "a.txt")
        .with_attachment(Uuid::now_v7(), "b.txt");
    surface::register(
        fixture.nats(),
        &live.runner,
        live.work.run_id,
        &[sender, recipient],
    )
    .await;
    let mut k_runner = live_runner(&fixture, "orphans-k").await;
    let unavailable = wire::unique_runner_type("orphans-none");
    let (never_created, created_late, kept) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    for job in [never_created, created_late, kept] {
        upload(&fixture, &live.work, job).await;
    }
    for (attachment, name) in mail.attachments.clone() {
        gesture::accepted(
            &gesture::upload_attachment(
                &fixture.gql(),
                &runner_access(),
                &live.work,
                &mail,
                attachment,
                &name,
                "the file",
            )
            .await,
            "jobsUploadAttachment",
        );
    }
    create_kept(&fixture, &live.work, &k_runner.runner_type, live.root, kept).await;
    let k_trigger = k_runner.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&k_trigger), kept);
    k_runner.start_applied(&fixture, &k_trigger).await;

    age_orphan_uploads(&durable, 29).await;
    tokio::time::sleep(QUIET).await;
    durable
        .assert_count(KEPT_OF_JOB, never_created, 1, "29 days: the payload stays")
        .await;
    durable
        .assert_count(
            ATTACHMENTS_OF_MAIL,
            mail.mail_id,
            2,
            "29 days: the attachments stay",
        )
        .await;
    create_kept(&fixture, &live.work, &unavailable, live.root, created_late).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        created_late,
        "PENDING",
        LONG,
    )
    .await;

    age_orphan_uploads(&durable, 2).await;
    durable
        .await_count(
            KEPT_OF_JOB,
            never_created,
            0,
            "31 days: the payload is erased",
        )
        .await;
    durable
        .await_count(
            ATTACHMENTS_OF_MAIL,
            mail.mail_id,
            0,
            "31 days: the attachments are erased",
        )
        .await;
    durable
        .assert_count(KEPT_OF_JOB, kept, 1, "the payload of K stays")
        .await;
    durable
        .assert_count(
            KEPT_OF_JOB,
            created_late,
            1,
            "the payload of a created Job stays",
        )
        .await;

    create_kept(&fixture, &live.work, &unavailable, live.root, never_created).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, never_created, LONG)
        .await;
    assert_eq!(refused.payload()["reason_code"], "payload_not_uploaded");
    let sent = gesture::send_mail(&fixture.gql(), &runner_access(), &live.work, &mail).await;
    assert_eq!(
        gesture::refusal(&sent, "jobsSendMail"),
        "attachment_not_uploaded"
    );

    gql::cancel_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        created_late,
    )
    .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        created_late,
        "CANCELLED",
        LONG,
    )
    .await;
    k_runner.complete_run(&k_trigger).await;
    durable
        .await_count(
            ENDED_RUN,
            runner::run_id(&k_trigger),
            1,
            "the Run of K ends",
        )
        .await;
    act(&fixture, wire::VERB_FINISH, kept, &live.work).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), kept, "COMPLETED", LONG).await;
    live.runner.complete_run(&live.trigger).await;
    durable
        .await_count(ENDED_RUN, live.work.run_id, 1, "the Run of the root ends")
        .await;
    Producer::new(fixture.fabric(), "projects")
        .finish_v2(live.root)
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        live.root,
        "COMPLETED",
        LONG,
    )
    .await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "K ended just now").await;
    durable
        .assert_count(
            KEPT_OF_JOB,
            kept,
            1,
            "K ended: its payload stays until the scrub",
        )
        .await;
    age_tree(&durable, live.root, 31).await;
    await_scrubbed(&durable, live.root, "the Tree of K is scrubbed").await;
    durable
        .assert_count(KEPT_OF_JOB, kept, 0, "the scrub erased the payload of K")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
