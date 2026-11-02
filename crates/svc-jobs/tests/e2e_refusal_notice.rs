mod support;

use serde_json::{Value, json};
use support::call_notices::{Caller, caller, notices, wait_for_job_notice};
use support::db::Durable;
use support::events::{DoorbellLog, EventLog};
use support::fixture::JobsFixture;
use support::producer::Producer;
use support::{LONG, QUIET, runner, wire};
use uuid::Uuid;

const REFUSALS_OF_RUN: &str = "SELECT count(*) FROM inbox_items \
     WHERE kind = 'NOTICE' AND run_id = $1 AND notice ->> 'type' = 'REFUSED'";

fn call_body(caller: &Caller<'_>, job: Uuid, extra: Value) -> Value {
    let mut body = json!({
        "job_id": job.to_string(),
        "runner_type": caller.pair.child.runner_type,
        "producer": "projects",
        "parent_job_id": caller.root.to_string(),
        "mode": "BACKGROUND",
        "caller_address": caller.address.to_string(),
        "acting_run_id": runner::run_id(&caller.trigger).to_string(),
        "acting_instance_key": caller.pair.parent.instance_key,
        "max_attempts": 1,
    });
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    body
}

fn refusals(items: &[Value]) -> Vec<Value> {
    items
        .iter()
        .filter(|item| item["noticeType"] == "REFUSED")
        .cloned()
        .collect()
}

#[tokio::test]
async fn a_refused_background_call_leaves_one_refusal_notice_besides_the_rejection() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let bell = DoorbellLog::open(fixture.fabric()).await;
    let bell_v2 = DoorbellLog::open_v2(fixture.fabric()).await;
    let caller = caller(&fixture, "refusal-notice").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let parent_run = runner::run_id(&caller.trigger);
    let strange_parent = Uuid::now_v7();

    let job = Uuid::now_v7();
    let command = producer
        .send_v3(
            wire::VERB_CREATE,
            call_body(
                &caller,
                job,
                json!({ "parent_job_id": strange_parent.to_string() }),
            ),
        )
        .await;
    events
        .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
        .await;
    let seen = wait_for_job_notice(
        &fixture,
        &caller.work,
        Some(caller.address),
        "REFUSED",
        Some(job),
    )
    .await;
    let told = refusals(&seen);
    assert_eq!(told.len(), 1, "{seen:?}");
    assert_eq!(told[0]["itemKind"], "NOTICE");
    assert_eq!(told[0]["address"], caller.address.to_string());
    assert_eq!(told[0]["jobId"], job.to_string());
    let words = told[0]["message"].as_str().unwrap_or_default();
    assert!(
        words.contains(&job.to_string()),
        "the notice names the Job the call was to create: {words}"
    );
    for forbidden in [strange_parent, caller.root] {
        assert!(
            !words.contains(&forbidden.to_string()),
            "the words name no parent Job and no Job of the Run: {words}"
        );
    }
    let item: Uuid = told[0]["itemId"]
        .as_str()
        .unwrap_or_default()
        .parse()
        .unwrap();
    bell.expect_rings(parent_run, 1, LONG).await;
    bell_v2.expect_rings(parent_run, 1, LONG).await;
    assert_eq!(durable.rings_of_item(item).await, 1);
    assert_eq!(durable.rings_of_item_v2(item).await, 1);

    producer.send(wire::VERB_CREATE, &command).await;
    events
        .expect_exactly(wire::FACT_CREATION_REJECTED, job, 2, LONG)
        .await;
    tokio::time::sleep(QUIET).await;
    durable
        .assert_count(
            REFUSALS_OF_RUN,
            parent_run,
            1,
            "the same refused call sent again leaves no second refusal notice",
        )
        .await;

    let other = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            call_body(&caller, other, json!({ "priority": 99 })),
        )
        .await;
    let seen = wait_for_job_notice(
        &fixture,
        &caller.work,
        Some(caller.address),
        "REFUSED",
        Some(other),
    )
    .await;
    assert_eq!(
        refusals(&seen).len(),
        2,
        "another call refused for another reason leaves its own notice"
    );
    bell.stop().await;
    bell_v2.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_call_that_fails_the_checks_on_the_run_or_the_address_or_waits_in_sync_leaves_no_refusal_notice()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let caller = caller(&fixture, "refusal-none").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let parent_run = runner::run_id(&caller.trigger);
    let strange_parent = Uuid::now_v7().to_string();

    let foreign_instance = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            call_body(
                &caller,
                foreign_instance,
                json!({ "parent_job_id": strange_parent, "acting_instance_key": "instance-elsewhere" }),
            ),
        )
        .await;
    let foreign_run = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            call_body(
                &caller,
                foreign_run,
                json!({ "parent_job_id": strange_parent, "acting_run_id": Uuid::now_v7().to_string() }),
            ),
        )
        .await;
    let not_held = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            call_body(
                &caller,
                not_held,
                json!({ "caller_address": Uuid::now_v7().to_string() }),
            ),
        )
        .await;
    let sync = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            call_body(
                &caller,
                sync,
                json!({ "mode": "SYNC", "caller_address": null, "priority": 99 }),
            ),
        )
        .await;
    for job in [foreign_instance, foreign_run, not_held, sync] {
        events
            .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
            .await;
    }
    tokio::time::sleep(QUIET).await;
    durable
        .assert_count(
            REFUSALS_OF_RUN,
            parent_run,
            0,
            "no refusal notice for a Run Jobs does not hold on that Instance, an address the Run does not hold, or a sync call",
        )
        .await;
    assert!(
        refusals(&notices(&fixture, &caller.work, None).await).is_empty(),
        "the Caller reads no refusal notice"
    );
    durable.close().await;
    fixture.shutdown().await;
}
