mod support;

use serde_json::json;
use support::LONG;
use support::events::DoorbellLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::tree::correspondents;
use uuid::Uuid;

#[tokio::test]
async fn a_direct_mail_waits_unread_for_its_addressee_rings_the_doorbell_and_is_marked_read_over_nats()
 {
    let fixture = JobsFixture::start().await;
    let bell = DoorbellLog::open(fixture.fabric()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-direct").await;

    let first = Mail::new(tree.sender, &[tree.receiver]).with_body("first");
    let second = Mail::new(tree.sender, &[tree.receiver]).with_body("second");
    for mail in [&first, &second] {
        let sent = gesture::send_mail(&gql, &runner, &tree.parent_work, mail).await;
        gesture::accepted(&sent, "jobsSendMail");
    }

    let unread = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(unread.len(), 2);
    assert_eq!(unread[0]["itemKind"], "MAIL");
    assert_eq!(unread[0]["mailId"], first.mail_id.to_string());
    assert_eq!(unread[1]["mailId"], second.mail_id.to_string());
    assert_eq!(unread[0]["body"], "first");
    assert_eq!(unread[0]["from"], tree.sender.to_string());
    assert!(
        unread[0]["seq"].as_i64() < unread[1]["seq"].as_i64(),
        "unread items come in arrival order"
    );

    let rings = bell
        .expect_rings(support::runner::run_id(&tree.child_trigger), 2, LONG)
        .await;
    assert!(
        rings
            .iter()
            .all(|ring| ring["address"] == tree.receiver.to_string() && ring["item_kind"] == "MAIL")
    );

    let item = Uuid::parse_str(unread[0]["itemId"].as_str().expect("an item id")).unwrap();
    let marked = gesture::mark_read(
        fixture.nats(),
        &tree.pair.child,
        tree.child_work.run_id,
        &[item],
    )
    .await;
    assert_eq!(marked["accepted"], true, "{marked}");
    let after = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(after.len(), 1);
    assert_eq!(after[0]["mailId"], second.mail_id.to_string());

    let strange = gesture::mark_read(
        fixture.nats(),
        &tree.pair.parent,
        tree.parent_work.run_id,
        &[Uuid::parse_str(after[0]["itemId"].as_str().unwrap()).unwrap()],
    )
    .await;
    assert_eq!(support::surface::reason(&strange), "item_not_yours");
    bell.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cc_recipient_gets_a_notice_and_reads_the_mail_by_its_id_afterwards() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-cc").await;

    let mail = Mail::new(tree.sender, &[tree.receiver]).cc(&[tree.bystander]);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await,
        "jobsSendMail",
    );

    let direct = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(direct[0]["itemKind"], "MAIL");
    assert_eq!(direct[0]["body"], "body");
    let copied = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.bystander)).await,
    );
    assert_eq!(copied.len(), 1);
    assert_eq!(copied[0]["itemKind"], "CC_NOTICE");
    assert_eq!(copied[0]["mailId"], mail.mail_id.to_string());
    assert_eq!(copied[0]["subject"], "subject");
    assert!(
        copied[0]["body"].is_null(),
        "a cc recipient gets the mail id and the subject, not the mail"
    );

    let read = gesture::read_mail(
        &gql,
        &runner,
        &tree.child_work,
        mail.mail_id,
        tree.bystander,
    )
    .await;
    let body = &gesture::field(&read, "jobsMail");
    assert_eq!(body["success"], true, "{read}");
    assert_eq!(body["mail"]["body"], "body");
    assert_eq!(body["mail"]["cc"], json!([tree.bystander.to_string()]));
    fixture.shutdown().await;
}

#[tokio::test]
async fn one_bad_recipient_refuses_the_whole_mail_and_names_the_bad_address() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-bad").await;
    let nobody = Uuid::now_v7();

    let mail = Mail::new(tree.sender, &[tree.receiver, nobody]);
    let sent = gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await;
    let outcome = gesture::field(&sent, "jobsSendMail");
    assert_eq!(outcome["success"], false, "{sent}");
    let bad = outcome["badAddresses"].as_array().expect("bad addresses");
    assert_eq!(bad.len(), 1);
    assert_eq!(bad[0]["address"], nobody.to_string());

    let unread = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert!(unread.is_empty(), "no recipient received the refused mail");
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_id_repeated_with_the_same_content_is_absorbed_and_with_other_content_refused() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-reuse").await;

    let mail = Mail::new(tree.sender, &[tree.receiver]);
    for _ in 0..2 {
        gesture::accepted(
            &gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await,
            "jobsSendMail",
        );
    }
    let unread = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(unread.len(), 1, "a repeated mail is delivered once");

    let altered = Mail {
        body: "other".to_owned(),
        ..Mail::new(tree.sender, &[tree.receiver])
    };
    let altered = Mail {
        mail_id: mail.mail_id,
        ..altered
    };
    let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &altered).await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "mail_id_reused");
    fixture.shutdown().await;
}

#[tokio::test]
async fn mail_belongs_to_its_addressee_alone_and_administrators_read_none() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-private").await;

    let mail = Mail::new(tree.sender, &[tree.receiver]);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await,
        "jobsSendMail",
    );

    let intruder = gesture::read_mail(
        &gql,
        &runner,
        &tree.parent_work,
        mail.mail_id,
        tree.receiver,
    )
    .await;
    assert_eq!(gesture::refusal(&intruder, "jobsMail"), "address_not_yours");
    let sender =
        gesture::read_mail(&gql, &runner, &tree.parent_work, mail.mail_id, tree.sender).await;
    assert_eq!(gesture::refusal(&sender, "jobsMail"), "mail_not_yours");

    let administrator = gesture::read_mail(
        &gql,
        fixture.admin(),
        &tree.child_work,
        mail.mail_id,
        tree.receiver,
    )
    .await;
    assert!(
        administrator["errors"].is_array(),
        "an administrator is refused before any resolver runs: {administrator}"
    );
    assert!(administrator["data"].is_null() || administrator["data"]["jobsMail"].is_null());
    fixture.shutdown().await;
}
