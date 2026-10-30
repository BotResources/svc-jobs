mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, ATTACHMENT, Mail, Work};
use support::restart::{RESTARTS_OF_RUN, UNREAD_AT, running_child};
use support::tree::correspondents;
use support::{LONG, QUIET};
use uuid::Uuid;

async fn fetch(fixture: &JobsFixture, work: &Work, mail_id: Uuid, attachment: Uuid) -> Value {
    gesture::call(
        &fixture.gql(),
        &runner_access(),
        ATTACHMENT,
        json!({
            "work": work.wire(),
            "mailId": mail_id.to_string(),
            "attachmentId": attachment.to_string(),
        }),
    )
    .await
}

#[tokio::test]
async fn a_cc_recipient_gets_a_notice_reads_the_mail_when_its_agent_next_starts_and_attachments_travel_apart()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let mut scene = running_child(&fixture, "mail-attachments").await;
    scene.finish_child(&durable).await;
    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
    let mail = Mail::new(scene.sender, &[scene.peer])
        .cc(&[scene.bystander])
        .with_subject("two files")
        .with_body("see attached")
        .with_attachment(first, "a.txt")
        .with_attachment(second, "b.txt");
    for (id, name, content) in [
        (first, "a.txt", "first file"),
        (second, "b.txt", "second file"),
    ] {
        let uploaded =
            gesture::upload_attachment(&gql, &runner, &scene.parent_work, &mail, id, name, content)
                .await;
        gesture::accepted(&uploaded, "jobsUploadAttachment");
    }
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &scene.parent_work, &mail).await,
        "jobsSendMail",
    );

    let direct =
        gesture::items(&gesture::unread(&gql, &runner, &scene.parent_work, Some(scene.peer)).await);
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0]["subject"], "two files");
    assert_eq!(direct[0]["from"], scene.sender.to_string());
    assert_eq!(direct[0]["body"], "see attached");
    let listed: Vec<&Value> = direct[0]["attachments"]
        .as_array()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(
        listed.len(),
        2,
        "the mail lists its attachments: {direct:?}"
    );
    let names: Vec<&str> = listed.iter().filter_map(|a| a["name"].as_str()).collect();
    assert_eq!(names, vec!["a.txt", "b.txt"]);
    let with_content = gesture::call(
        &gql,
        &runner,
        "query($work: JobsWorkInput!) { jobsUnread(work: $work) { items { attachments { content } } } }",
        json!({ "work": scene.parent_work.wire() }),
    )
    .await;
    let refusal = with_content["errors"][0]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("the query is refused with an error: {with_content}"));
    assert!(
        refusal.contains("Unknown field \"content\"") && refusal.contains("JobsMailAttachment"),
        "the content of an attachment is in no mail and no notice, so the schema has no such field \
         on the mail attachment: {with_content}"
    );
    let fetched = fetch(&fixture, &scene.parent_work, mail.mail_id, first).await;
    let outcome = gesture::field(&fetched, "jobsAttachment");
    assert_eq!(outcome["success"], true, "{fetched}");
    assert_eq!(outcome["attachment"]["name"], "a.txt");
    assert_eq!(outcome["attachment"]["content"], "first file");

    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            scene.child_run(),
            0,
            "the cc notice restarts nothing",
        )
        .await;
    durable
        .assert_count(UNREAD_AT, scene.bystander, 1, "the notice waits unread")
        .await;

    scene.mail_receiver(&fixture, scene.sender).await;
    let restart = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &restart).await;
    let restarted = Work::of(&scene.pair.child, &restart);
    let notice =
        gesture::items(&gesture::unread(&gql, &runner, &restarted, Some(scene.bystander)).await);
    assert_eq!(
        notice.len(),
        1,
        "the notice waited for the next start of R's agent"
    );
    assert_eq!(notice[0]["itemKind"], "CC_NOTICE");
    assert_eq!(notice[0]["mailId"], mail.mail_id.to_string());
    assert_eq!(notice[0]["subject"], "two files");
    assert!(notice[0]["body"].is_null(), "a cc notice is not the mail");
    assert!(
        notice[0]["attachments"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "a cc notice carries no attachment"
    );
    let read = gesture::read_mail(&gql, &runner, &restarted, mail.mail_id, scene.bystander).await;
    let read = gesture::field(&read, "jobsMail");
    assert_eq!(read["success"], true, "{read}");
    assert_eq!(read["mail"]["body"], "see attached");

    let outsider =
        gesture::read_mail(&gql, &runner, &restarted, mail.mail_id, scene.receiver).await;
    assert_eq!(gesture::refusal(&outsider, "jobsMail"), "mail_not_yours");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn another_mail_under_a_used_mail_id_is_refused_and_leaves_the_inbox_unchanged() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-id-reuse").await;
    let unread = || async {
        gesture::items(&gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await)
    };
    let mail = Mail::new(tree.sender, &[tree.receiver]);
    for _ in 0..2 {
        gesture::accepted(
            &gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await,
            "jobsSendMail",
        );
    }
    let before = unread().await;
    assert_eq!(
        before.len(),
        1,
        "the same mail sent again is absorbed: Q has one copy"
    );

    let reused = |altered: Mail| Mail {
        mail_id: mail.mail_id,
        ..altered
    };
    for (what, altered) in [
        (
            "subject",
            reused(Mail::new(tree.sender, &[tree.receiver]).with_subject("other")),
        ),
        (
            "body",
            reused(Mail::new(tree.sender, &[tree.receiver]).with_body("other")),
        ),
        (
            "recipients",
            reused(Mail::new(tree.sender, &[tree.receiver, tree.bystander])),
        ),
        (
            "attachments",
            reused(
                Mail::new(tree.sender, &[tree.receiver]).with_attachment(Uuid::now_v7(), "x.txt"),
            ),
        ),
    ] {
        let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &altered).await;
        assert_eq!(
            gesture::refusal(&refused, "jobsSendMail"),
            "mail_id_reused",
            "another {what} under a used mail id: {refused}"
        );
        assert!(
            !gesture::field(&refused, "jobsSendMail")["message"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
        );
        assert_eq!(
            unread().await,
            before,
            "Q's unread mail is unchanged after another {what}"
        );
    }
    assert!(
        gesture::items(
            &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.bystander)).await
        )
        .is_empty(),
        "the refused recipient change reached nobody"
    );
    fixture.shutdown().await;
}
