mod support;

use br_test_harness::wait_until;
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::surface::{self, Agent};
use support::tree::pair;
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_background_call_whose_caller_address_is_removed_before_the_job_commits_is_refused() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair = pair(&fixture, "caller-removal").await;
    let parent = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(2);
    producer.declare_v2(&parent).await;
    let trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &trigger).await;
    let run = runner::run_id(&trigger);
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    let registered = surface::register(fixture.nats(), &pair.parent, run, &[agent]).await;
    assert_eq!(registered["accepted"], true, "{registered}");

    let child_job = Uuid::now_v7();
    let uploader = fixture.gql();
    let stored = gesture::upload_payload(
        &uploader,
        &runner_access(),
        child_job,
        "the brief",
        Some(&Work::of(&pair.parent, &trigger)),
    )
    .await;
    gesture::accepted(&stored, "jobsUploadPayload");

    let mut stall = durable.pool().begin().await.expect("a connection opens");
    sqlx::query("LOCK TABLE kept_payloads IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *stall)
        .await
        .expect("the stall lock is taken");

    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": child_job.to_string(),
                "runner_type": pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": parent.job_id.to_string(),
                "mode": "BACKGROUND",
                "caller_address": address.to_string(),
                "acting_run_id": run.to_string(),
                "acting_instance_key": pair.parent.instance_key,
                "payload": { "form": "KEPT" },
            }),
        )
        .await;
    let stalled = wait_until(LONG, || async {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'relation' \
             AND relation = 'kept_payloads'::regclass",
        )
        .fetch_one(durable.pool())
        .await
        .expect("the lock table reads");
        waiting > 0
    })
    .await;
    assert!(stalled, "the creation waits behind the stall lock");

    pair.parent
        .fail_run(&trigger, "TRANSIENT", "provider_timeout", None)
        .await;
    durable
        .await_count(
            db::AGENTS_OF_RUN,
            run,
            0,
            "the failed Run leaves the directory",
        )
        .await;
    stall.rollback().await.expect("the stall lock is released");

    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, child_job, LONG)
        .await;
    assert_eq!(refused.payload()["reason_code"], "caller_address_not_held");
    durable
        .assert_count(
            db::JOBS_WITH_ID,
            child_job,
            0,
            "no Job names the removed address",
        )
        .await;
    answers.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
