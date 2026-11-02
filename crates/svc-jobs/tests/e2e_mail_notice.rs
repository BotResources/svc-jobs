mod support;

use serde_json::Value;
use support::LONG;
use support::call_notices::{mail_notices, wait_for_mail_notices};
use support::db::Durable;
use support::events::DoorbellLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::tree::correspondents;
use uuid::Uuid;

fn item_id(item: &Value) -> Uuid {
    item["itemId"]
        .as_str()
        .and_then(|raw| raw.parse().ok())
        .expect("an item has a UUID item id")
}

#[tokio::test]
async fn a_refused_mail_leaves_one_mail_notice_at_the_sender_and_rings_in_version_two_only() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-notice").await;
    let run = tree.parent_work.run_id;
    let bell = DoorbellLog::open(fixture.fabric()).await;
    let bell_v2 = DoorbellLog::open_v2(fixture.fabric()).await;
    let never_held = Uuid::now_v7();

    let bad = Mail::new(tree.sender, &[tree.receiver, never_held]).with_subject("do not leak");
    let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &bad).await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");

    let seen = wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 1).await;
    assert_eq!(seen.len(), 1, "one refusal, one mail notice: {seen:?}");
    let notice = &seen[0];
    assert_eq!(notice["noticeType"], "REFUSED");
    assert_eq!(notice["mailId"], bad.mail_id.to_string());
    assert_eq!(notice["address"], tree.sender.to_string());
    assert_eq!(notice["runId"], run.to_string());
    assert!(notice["jobId"].is_null(), "a mail notice names no Job");
    assert!(
        notice["subject"].is_null(),
        "a mail notice carries no subject"
    );
    assert!(notice["body"].is_null());
    let words = notice["message"].as_str().unwrap_or_default();
    assert!(
        words.contains(&never_held.to_string()),
        "the reason names the bad address: {words}"
    );
    assert!(!words.contains("do not leak"));
    for job in [tree.root, tree.parent_work.job_id, tree.child_work.job_id] {
        assert!(
            !words.contains(&job.to_string()),
            "the words name no Job of the Tree: {words}"
        );
    }

    let rang = bell_v2.expect_rings(run, 1, LONG).await;
    assert_eq!(rang[0]["item_kind"], "MAIL_NOTICE");
    assert_eq!(rang[0]["address"], tree.sender.to_string());
    assert_eq!(rang[0]["item_id"], notice["itemId"]);
    assert_eq!(durable.rings_of_item_v2(item_id(notice)).await, 1);
    assert_eq!(
        durable.rings_of_item(item_id(notice)).await,
        0,
        "the first version of the doorbell never announces a mail notice"
    );
    assert!(bell.rings_for(run).is_empty());

    let again = gesture::send_mail(&gql, &runner, &tree.parent_work, &bad).await;
    assert_eq!(gesture::refusal(&again, "jobsSendMail"), "bad_addresses");
    tokio::time::sleep(support::QUIET).await;
    assert_eq!(
        mail_notices(&fixture, &tree.parent_work, Some(tree.sender))
            .await
            .len(),
        1,
        "the same refused mail sent again leaves no second mail notice"
    );

    let other = Mail::new(tree.sender, &[Uuid::now_v7()]);
    gesture::send_mail(&gql, &runner, &tree.parent_work, &other).await;
    let two = wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 2).await;
    assert_eq!(two.len(), 2, "another refused mail leaves its own notice");

    let by_run = mail_notices(&fixture, &tree.parent_work, None).await;
    assert_eq!(by_run.len(), 2, "the read of the whole Run lists them too");
    let marked =
        gesture::mark_read(fixture.nats(), &tree.pair.parent, run, &[item_id(notice)]).await;
    assert_eq!(marked["accepted"], true, "{marked}");
    let left = mail_notices(&fixture, &tree.parent_work, Some(tree.sender)).await;
    assert_eq!(
        left.iter().map(item_id).collect::<Vec<_>>(),
        vec![item_id(&two[1])],
        "the read marker covers the mail notice it names and no other"
    );

    assert!(
        gesture::items(
            &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await
        )
        .is_empty(),
        "the receiver got nothing from a refused mail"
    );
    bell.stop().await;
    bell_v2.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_good_mail_leaves_no_mail_notice_and_a_reused_id_says_the_id_belongs_to_an_accepted_mail()
{
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-notice-reuse").await;

    let first = Mail::new(tree.sender, &[tree.receiver, Uuid::now_v7()]);
    gesture::send_mail(&gql, &runner, &tree.parent_work, &first).await;
    wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 1).await;

    let fixed = Mail {
        mail_id: first.mail_id,
        ..Mail::new(tree.sender, &[tree.receiver])
    };
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &fixed).await,
        "jobsSendMail",
    );
    assert_eq!(
        mail_notices(&fixture, &tree.parent_work, Some(tree.sender))
            .await
            .len(),
        1,
        "the accepted mail leaves no notice of its own"
    );

    let different = Mail {
        mail_id: first.mail_id,
        ..Mail::new(tree.sender, &[tree.receiver]).with_body("another body")
    };
    let reused = gesture::send_mail(&gql, &runner, &tree.parent_work, &different).await;
    assert_eq!(gesture::refusal(&reused, "jobsSendMail"), "mail_id_reused");
    let seen = wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 2).await;
    let reuse = seen
        .iter()
        .find(|item| {
            item["message"]
                .as_str()
                .is_some_and(|words| words.contains("accepted"))
        })
        .unwrap_or_else(|| panic!("the reason says the id belongs to an accepted mail: {seen:?}"));
    assert_eq!(reuse["mailId"], first.mail_id.to_string());
    assert!(reuse["subject"].is_null());

    gesture::send_mail(&gql, &runner, &tree.parent_work, &different).await;
    tokio::time::sleep(support::QUIET).await;
    assert_eq!(
        mail_notices(&fixture, &tree.parent_work, Some(tree.sender))
            .await
            .len(),
        2,
        "the same refused reuse sent again leaves no second notice"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_refusal_that_does_not_come_from_a_sender_of_the_run_on_its_instance_leaves_no_notice() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-notice-foreign").await;
    let unknown = Uuid::now_v7();

    let from_another_instance = Mail::new(tree.sender, &[unknown]);
    let refused = gesture::send_mail(
        &gql,
        &runner,
        &tree.parent_work.as_instance("instance-elsewhere"),
        &from_another_instance,
    )
    .await;
    assert!(
        !gesture::field(&refused, "jobsSendMail")["success"]
            .as_bool()
            .unwrap_or(true),
        "{refused}"
    );

    for from in [tree.receiver, Uuid::now_v7()] {
        let forged = Mail::new(from, &[unknown]);
        let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &forged).await;
        assert!(
            ["sender_not_of_run", "sender_unknown"]
                .contains(&gesture::refusal(&refused, "jobsSendMail").as_str()),
            "{refused}"
        );
    }

    tokio::time::sleep(support::QUIET).await;
    durable
        .assert_count(
            "SELECT count(*) FROM inbox_items WHERE kind = 'MAIL_NOTICE' AND run_id = $1",
            tree.parent_work.run_id,
            0,
            "no mail notice for a call that fails the checks on the Run and its sender",
        )
        .await;
    durable
        .assert_count(
            "SELECT count(*) FROM inbox_items WHERE kind = 'MAIL_NOTICE' AND run_id = $1",
            tree.child_work.run_id,
            0,
            "no mail notice reaches the Run of the address a forged sender named",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_refused_for_its_author_leaves_a_mail_notice_too() {
    let fixture = JobsFixture::start().await;
    let tree = correspondents(&fixture, "mail-notice-author").await;
    let blank = Mail::new(tree.sender, &[tree.receiver]).by("   ", Uuid::now_v7());
    let refused =
        gesture::send_mail(&fixture.gql(), &runner_access(), &tree.parent_work, &blank).await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "author_blank");
    let seen = wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 1).await;
    assert_eq!(seen[0]["mailId"], blank.mail_id.to_string());
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_to_a_hundred_bad_recipients_is_refused_with_the_typed_refusal_and_leaves_one_mail_notice()
 {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-notice-many").await;
    let strangers: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();

    let mail = Mail::new(tree.sender, &strangers);
    let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");
    let seen = wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 1).await;
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(seen[0]["mailId"], mail.mail_id.to_string());

    let again = gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await;
    assert_eq!(gesture::refusal(&again, "jobsSendMail"), "bad_addresses");
    tokio::time::sleep(support::QUIET).await;
    assert_eq!(
        mail_notices(&fixture, &tree.parent_work, Some(tree.sender))
            .await
            .len(),
        1,
        "the same refused mail sent again leaves no second mail notice"
    );
    fixture.shutdown().await;
}
