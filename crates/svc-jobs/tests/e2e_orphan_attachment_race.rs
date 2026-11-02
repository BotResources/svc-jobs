mod support;

use sqlx::Row;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::orphan_race::await_a_writer_blocked_on_an_advisory_lock;
use support::restart::running_child;
use svc_jobs::db::mail::lock_mail_attachments;
use uuid::Uuid;

#[tokio::test]
async fn a_mail_sent_while_the_sweep_erases_its_attachments_is_refused_and_leaves_no_mail() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let scene = running_child(&fixture, "attachment-race").await;
    let attachment = Uuid::now_v7();
    let mail = Mail::new(scene.sender, &[scene.peer])
        .with_subject("a file")
        .with_attachment(attachment, "a.txt");
    gesture::accepted(
        &gesture::upload_attachment(
            &gql,
            &runner,
            &scene.parent_work,
            &mail,
            attachment,
            "a.txt",
            "content",
        )
        .await,
        "jobsUploadAttachment",
    );
    let mut sweeping = durable.pool().begin().await.expect("a transaction opens");
    lock_mail_attachments(&mut sweeping, mail.mail_id)
        .await
        .expect("the sweep holds the attachments of the mail");

    let sending = gesture::send_mail(&gql, &runner, &scene.parent_work, &mail);
    let erasure = async {
        await_a_writer_blocked_on_an_advisory_lock(durable.pool()).await;
        sqlx::query("DELETE FROM mail_attachments WHERE mail_id = $1")
            .bind(mail.mail_id)
            .execute(&mut *sweeping)
            .await
            .expect("the sweep erases the attachments");
        sweeping.commit().await.expect("the sweep ends");
    };
    let (sent, ()) = tokio::join!(sending, erasure);

    assert_eq!(
        gesture::refusal(&sent, "jobsSendMail"),
        "attachment_not_uploaded",
        "{sent}"
    );
    let mails: i64 = sqlx::query("SELECT count(*) FROM mails WHERE mail_id = $1")
        .bind(mail.mail_id)
        .fetch_one(durable.pool())
        .await
        .expect("the mails count")
        .get(0);
    assert_eq!(mails, 0, "no mail is left without its attachment");
    durable.close().await;
    fixture.shutdown().await;
}
