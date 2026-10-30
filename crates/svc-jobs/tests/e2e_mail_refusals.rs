mod support;

use serde_json::Value;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::restart::FINISHED_OF_RUN;
use support::runner;
use support::surface::{self, Agent};
use support::tree::{Correspondents, call_child, correspondents};
use uuid::Uuid;

const AGENTS_OF_RUN: &str = "SELECT count(*) FROM directory_agents WHERE run_id = $1";

async fn child_agent_that(
    fixture: &JobsFixture,
    tree: &mut Correspondents<'_>,
    durable: &Durable,
    fails: bool,
) -> Uuid {
    let trigger = call_child(
        fixture,
        &mut tree.pair,
        tree.root,
        runner::run_id(&tree.parent_trigger),
    )
    .await;
    tree.pair.child.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    let run = runner::run_id(&trigger);
    surface::register(fixture.nats(), &tree.pair.child, run, &[agent]).await;
    if fails {
        tree.pair
            .child
            .fail_run(&trigger, "PERMANENT", "provider_refused", None)
            .await;
        durable
            .await_count(AGENTS_OF_RUN, run, 0, "the failed Run leaves the directory")
            .await;
    } else {
        tree.pair.child.complete_run(&trigger).await;
        durable
            .await_count(
                FINISHED_OF_RUN,
                run,
                1,
                "the agent finished without a saved state",
            )
            .await;
    }
    address
}

fn reasons_by_address(outcome: &Value) -> Vec<(String, String, bool)> {
    let mut named: Vec<(String, String, bool)> = outcome["badAddresses"]
        .as_array()
        .expect("the refusal names the bad addresses")
        .iter()
        .map(|bad| {
            (
                bad["address"].as_str().unwrap_or_default().to_owned(),
                bad["reason"].as_str().unwrap_or_default().to_owned(),
                bad["message"]
                    .as_str()
                    .is_some_and(|words| !words.is_empty()),
            )
        })
        .collect();
    named.sort();
    named
}

#[tokio::test]
async fn a_mail_with_a_removed_an_unknown_and_an_unrestartable_address_is_refused_whole() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let mut tree = correspondents(&fixture, "mail-three-bad").await;
    let removed = child_agent_that(&fixture, &mut tree, &durable, true).await;
    let unrestartable = child_agent_that(&fixture, &mut tree, &durable, false).await;
    let never_held = Uuid::now_v7();

    let bad = Mail::new(
        tree.sender,
        &[tree.receiver, removed, never_held, unrestartable],
    );
    let sent = gesture::send_mail(&gql, &runner, &tree.parent_work, &bad).await;
    assert_eq!(
        gesture::refusal(&sent, "jobsSendMail"),
        "bad_addresses",
        "{sent}"
    );
    let mut expected = vec![
        (removed.to_string(), "recipient_removed".to_owned(), true),
        (never_held.to_string(), "recipient_unknown".to_owned(), true),
        (
            unrestartable.to_string(),
            "recipient_cannot_restart".to_owned(),
            true,
        ),
    ];
    expected.sort();
    assert_eq!(
        reasons_by_address(gesture::field(&sent, "jobsSendMail")),
        expected,
        "each bad address is named with its reason and its words"
    );
    let unread = || async {
        gesture::items(&gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await)
    };
    assert!(unread().await.is_empty(), "Q's inbox is unchanged");

    let good = Mail::new(tree.sender, &[tree.receiver]);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &good).await,
        "jobsSendMail",
    );
    let after = unread().await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0]["mailId"], good.mail_id.to_string());
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_stays_inside_its_tree_and_leaves_only_from_an_address_of_the_run() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-one-tree").await;
    let other = correspondents(&fixture, "mail-other-tree").await;
    let stranger = other.receiver;

    let inside = Mail::new(tree.sender, &[tree.receiver])
        .with_subject("inside the tree")
        .with_body("hello Q");
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &inside).await,
        "jobsSendMail",
    );
    let read = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(read.len(), 1);
    assert_eq!(read[0]["subject"], "inside the tree");
    assert_eq!(read[0]["from"], tree.sender.to_string());
    assert_eq!(read[0]["body"], "hello Q");

    let across = Mail::new(tree.sender, &[stranger]);
    let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &across).await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");
    assert_eq!(
        reasons_by_address(gesture::field(&refused, "jobsSendMail")),
        vec![(stranger.to_string(), "other_tree".to_owned(), true)],
        "the refusal names T"
    );
    assert!(
        gesture::items(&gesture::unread(&gql, &runner, &other.child_work, Some(stranger)).await)
            .is_empty(),
        "T receives nothing"
    );

    for (from, reason) in [
        (tree.receiver, "sender_not_of_run"),
        (Uuid::now_v7(), "sender_unknown"),
    ] {
        let forged = Mail::new(from, &[tree.bystander]);
        let refused = gesture::send_mail(&gql, &runner, &tree.parent_work, &forged).await;
        assert_eq!(
            gesture::refusal(&refused, "jobsSendMail"),
            reason,
            "{refused}"
        );
    }
    assert!(
        gesture::items(
            &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.bystander)).await
        )
        .is_empty(),
        "a mail from an address the Run does not hold reaches nobody"
    );

    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &inside).await,
        "jobsSendMail",
    );
    assert_eq!(
        gesture::items(
            &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await
        )
        .len(),
        1,
        "resending the identical mail is absorbed"
    );
    fixture.shutdown().await;
}
