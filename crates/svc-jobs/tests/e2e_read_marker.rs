mod support;

use serde_json::Value;
use support::call_notices::{background_call, caller, notices, wait_for_notice};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::restart::running_child;
use support::runner::FakeRunner;
use uuid::Uuid;

fn ids(items: &[Value], key: &str) -> Vec<String> {
    items
        .iter()
        .map(|item| item[key].as_str().unwrap_or_default().to_owned())
        .collect()
}

fn item_id(item: &Value) -> Uuid {
    Uuid::parse_str(item["itemId"].as_str().expect("an item id")).expect("an item id is a uuid")
}

async fn mark(fixture: &JobsFixture, runner: &FakeRunner<'_>, work: &Work, item: Uuid) {
    let marked = gesture::mark_read(fixture.nats(), runner, work.run_id, &[item]).await;
    assert_eq!(marked["accepted"], true, "{marked}");
}

#[tokio::test]
async fn a_read_marker_keeps_the_unread_mail_across_a_crash_in_order_of_acceptance() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let mut scene = running_child(&fixture, "read-marker").await;
    let mails: Vec<Mail> = ["M1", "M2", "M3"]
        .iter()
        .map(|body| Mail::new(scene.sender, &[scene.receiver]).with_body(body))
        .collect();
    for mail in &mails {
        gesture::accepted(
            &gesture::send_mail(&gql, &runner, &scene.parent_work, mail).await,
            "jobsSendMail",
        );
    }
    let read = || async {
        gesture::items(
            &gesture::unread(&gql, &runner, &scene.child_work, Some(scene.receiver)).await,
        )
    };
    let first = read().await;
    assert_eq!(ids(&first, "body"), vec!["M1", "M2", "M3"]);
    mark(
        &fixture,
        &scene.pair.child,
        &scene.child_work,
        item_id(&first[0]),
    )
    .await;

    scene.pair.child.crash();
    scene.pair.child.connect().await;
    let after_crash = read().await;
    assert_eq!(
        ids(&after_crash, "body"),
        vec!["M2", "M3"],
        "M2 and M3 again, in this order, not M1"
    );

    mark(
        &fixture,
        &scene.pair.child,
        &scene.child_work,
        item_id(&after_crash[0]),
    )
    .await;
    let once = read().await;
    mark(
        &fixture,
        &scene.pair.child,
        &scene.child_work,
        item_id(&after_crash[0]),
    )
    .await;
    assert_eq!(
        read().await,
        once,
        "setting the marker of M2 twice changes nothing"
    );
    assert_eq!(ids(&once, "body"), vec!["M3"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_notice_follows_the_same_marker_rule() {
    let fixture = JobsFixture::start().await;
    let mut calling = caller(&fixture, "notice-marker").await;
    let call = background_call(&fixture, &mut calling).await;
    calling.pair.child.start_applied(&fixture, &call).await;
    let seen = wait_for_notice(&fixture, &calling.work, Some(calling.address), "STARTED").await;
    let order = ids(&seen, "noticeType");
    assert_eq!(order, vec!["ACCEPTED", "QUEUE_POSITION", "STARTED"]);
    mark(
        &fixture,
        &calling.pair.parent,
        &calling.work,
        item_id(&seen[0]),
    )
    .await;
    calling.pair.parent.crash();
    calling.pair.parent.connect().await;
    let again = notices(&fixture, &calling.work, Some(calling.address)).await;
    assert_eq!(
        ids(&again, "noticeType"),
        vec!["QUEUE_POSITION", "STARTED"],
        "in order, without the marked one"
    );
    mark(
        &fixture,
        &calling.pair.parent,
        &calling.work,
        item_id(&again[0]),
    )
    .await;
    mark(
        &fixture,
        &calling.pair.parent,
        &calling.work,
        item_id(&again[0]),
    )
    .await;
    assert_eq!(
        ids(
            &notices(&fixture, &calling.work, Some(calling.address)).await,
            "noticeType"
        ),
        vec!["STARTED"]
    );
    fixture.shutdown().await;
}
