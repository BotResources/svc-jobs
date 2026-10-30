mod support;

use serde_json::{Value, json};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, SEND_MAIL};
use support::tree::correspondents;
use uuid::Uuid;

fn authored(mail: &Value, author: &str, conversation: Uuid) {
    assert_eq!(
        mail["author"], author,
        "the author exactly as given: {mail}"
    );
    assert_eq!(
        mail["sourceConversationId"],
        conversation.to_string(),
        "the source conversation exactly as given: {mail}"
    );
}

#[tokio::test]
async fn a_mail_carries_its_author_and_source_conversation_to_its_readers_and_not_to_a_cc_notice() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-author").await;
    let conversation = Uuid::now_v7();
    let author = "Ada \u{e9}crivaine";

    let mail = Mail::new(tree.sender, &[tree.receiver])
        .cc(&[tree.bystander])
        .by(author, conversation);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await,
        "jobsSendMail",
    );

    let unread = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(unread.len(), 1);
    authored(&unread[0], author, conversation);
    let alone =
        gesture::read_mail(&gql, &runner, &tree.child_work, mail.mail_id, tree.receiver).await;
    authored(
        &gesture::field(&alone, "jobsMail")["mail"],
        author,
        conversation,
    );

    let notice = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.bystander)).await,
    );
    assert_eq!(notice.len(), 1);
    assert_eq!(notice[0]["itemKind"], "CC_NOTICE");
    assert!(
        notice[0]["author"].is_null(),
        "a cc notice names no author: {notice:?}"
    );
    assert!(
        notice[0]["sourceConversationId"].is_null(),
        "a cc notice names no source conversation: {notice:?}"
    );
    let after_notice = gesture::read_mail(
        &gql,
        &runner,
        &tree.child_work,
        mail.mail_id,
        tree.bystander,
    )
    .await;
    authored(
        &gesture::field(&after_notice, "jobsMail")["mail"],
        author,
        conversation,
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_author_and_a_source_conversation_are_checked_for_shape_only() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-author-shape").await;
    let unread_count = || async {
        gesture::items(&gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await)
            .len()
    };

    let longest = "a".repeat(256);
    let never_seen = Uuid::now_v7();
    let accepted = Mail::new(tree.sender, &[tree.receiver]).by(&longest, never_seen);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &accepted).await,
        "jobsSendMail",
    );
    assert_eq!(
        unread_count().await,
        1,
        "an author of 256 characters and a conversation Jobs never saw are accepted"
    );

    let not_v7 = Uuid::from_u128(0x0190_0000_0000_4000_8000_0000_0000_0001);
    for (mail, reason) in [
        (
            Mail::new(tree.sender, &[tree.receiver]).by(&"a".repeat(257), never_seen),
            "author_too_long",
        ),
        (
            Mail::new(tree.sender, &[tree.receiver]).by("   ", never_seen),
            "author_blank",
        ),
        (
            Mail::new(tree.sender, &[tree.receiver]).by("", never_seen),
            "author_blank",
        ),
        (
            Mail::new(tree.sender, &[tree.receiver]).by("the author", not_v7),
            "source_conversation_invalid",
        ),
    ] {
        let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await;
        assert_eq!(
            gesture::refusal(&refused, "jobsSendMail"),
            reason,
            "{refused}"
        );
        assert!(
            !gesture::field(&refused, "jobsSendMail")["message"]
                .as_str()
                .unwrap_or_default()
                .is_empty(),
            "the refusal carries its words: {refused}"
        );
        assert_eq!(
            unread_count().await,
            1,
            "a refused mail leaves Q's unread mail unchanged"
        );
    }

    for missing in ["author", "sourceConversationId"] {
        let mut input = Mail::new(tree.sender, &[tree.receiver]).input(&tree.parent_work);
        input.as_object_mut().expect("a mail input").remove(missing);
        let refused = gesture::call(&gql, &runner, SEND_MAIL, json!({ "input": input })).await;
        let errors = refused["errors"].as_array().cloned().unwrap_or_default();
        assert!(
            errors.iter().any(|error| {
                error["message"]
                    .as_str()
                    .is_some_and(|words| words.contains(missing))
            }),
            "a mail without {missing} is refused and the refusal names it: {refused}"
        );
        assert!(refused["data"].is_null(), "{refused}");
        assert_eq!(
            unread_count().await,
            1,
            "a mail without {missing} leaves Q's mail unchanged"
        );
    }
    fixture.shutdown().await;
}
