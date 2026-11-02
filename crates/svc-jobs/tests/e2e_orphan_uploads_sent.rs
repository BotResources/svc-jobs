mod support;

use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::producer::Producer;
use support::scrub::{age_orphan_uploads, age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::scrub_scene::{ENDED_RUN, live_root, long_runs};
use support::surface::{self, Agent};
use support::{LONG, QUIET, gql};
use uuid::Uuid;

const ATTACHMENTS_OF_MAIL: &str = "SELECT count(*) FROM mail_attachments WHERE mail_id = $1";

#[tokio::test]
async fn the_attachments_of_a_sent_mail_stay_however_old_until_the_tree_is_scrubbed() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "sent-attachments", 1).await;
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
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &live.work, &mail).await,
        "jobsSendMail",
    );

    age_orphan_uploads(&durable, 40).await;
    tokio::time::sleep(QUIET).await;
    durable
        .assert_count(
            ATTACHMENTS_OF_MAIL,
            mail.mail_id,
            2,
            "40 days old but sent: the attachments stay",
        )
        .await;

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
    assert_not_scrubbed_for(&durable, live.root, QUIET, "the Tree ended just now").await;
    durable
        .assert_count(
            ATTACHMENTS_OF_MAIL,
            mail.mail_id,
            2,
            "the Tree ended: the attachments stay until the scrub",
        )
        .await;

    age_tree(&durable, live.root, 31).await;
    await_scrubbed(&durable, live.root, "the Tree is scrubbed").await;
    durable
        .assert_count(
            ATTACHMENTS_OF_MAIL,
            mail.mail_id,
            0,
            "the scrub erased the attachments",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
