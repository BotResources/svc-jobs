mod support;

use serde_json::json;
use support::call_notices::{caller, wait_for_job_notice};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::Producer;
use support::runner;
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_failure_report_names_what_failed_and_that_the_retries_are_spent() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut caller = caller(&fixture, "failure-report").await;
    let caller_run = runner::run_id(&caller.trigger);
    let child_type = caller.pair.child.runner_type.clone();
    let caller_instance = caller.pair.parent.instance_key.clone();
    let create = |job: Uuid| {
        json!({
            "job_id": job.to_string(),
            "runner_type": child_type,
            "producer": "projects",
            "parent_job_id": caller.root.to_string(),
            "mode": "SYNC",
            "max_attempts": 2,
            "acting_run_id": caller_run.to_string(),
            "acting_instance_key": caller_instance,
        })
    };

    let spent = Uuid::now_v7();
    producer.send_v3(wire::VERB_CREATE, create(spent)).await;
    let mut last_run = Uuid::nil();
    for attempt in 1..=2 {
        let trigger = caller.pair.child.next_trigger(LONG).await;
        assert_eq!(runner::attempt_number(&trigger), attempt);
        caller.pair.child.start_applied(&fixture, &trigger).await;
        caller
            .pair
            .child
            .fail_run(&trigger, "TRANSIENT", "provider_unavailable", Some(0))
            .await;
        last_run = runner::run_id(&trigger);
    }
    let failed = answers
        .expect_one(wire::FACT_FAILED, spent, LONG)
        .await
        .payload();
    assert_eq!(failed["failure_cause"], "TERMINAL_RUN_FAILURE");
    assert_eq!(failed["failure_report"]["kind"], "TRANSIENT");
    assert_eq!(
        failed["failure_report"]["reason_code"],
        "provider_unavailable"
    );
    assert_eq!(failed["run_id"], last_run.to_string());
    let message = failed["message"]
        .as_str()
        .expect("the event carries a message")
        .to_owned();
    for named in [
        spent.to_string(),
        last_run.to_string(),
        "provider_unavailable".to_owned(),
    ] {
        assert!(
            message.contains(&named),
            "the message names {named}: {message}"
        );
    }
    assert!(
        message.contains("attempts are spent"),
        "the message says the retries are spent: {message}"
    );

    let notices =
        wait_for_job_notice(&fixture, &caller.work, None, "JOB_FAILED", Some(spent)).await;
    let notice = notices
        .iter()
        .find(|item| item["noticeType"] == "JOB_FAILED" && item["jobId"] == spent.to_string())
        .expect("the failure notice of the spent Job");
    assert_eq!(notice["failureReport"]["kind"], "TRANSIENT");
    assert_eq!(
        notice["failureReport"]["reason_code"],
        "provider_unavailable"
    );
    assert_eq!(
        notice["message"],
        message.as_str(),
        "the notice carries the same message"
    );

    let declared = Uuid::now_v7();
    producer.send_v3(wire::VERB_CREATE, create(declared)).await;
    caller.pair.child.next_trigger(LONG).await;
    producer
        .send_v3(
            wire::VERB_FAIL,
            json!({
                "job_id": declared.to_string(),
                "acting_run_id": caller_run.to_string(),
                "acting_instance_key": caller.pair.parent.instance_key,
            }),
        )
        .await;
    let by_owner = answers
        .expect_one(wire::FACT_FAILED, declared, LONG)
        .await
        .payload();
    assert_eq!(by_owner["failure_cause"], "DECLARED_BY_OWNER");
    assert!(
        by_owner["message"].as_str().is_some_and(
            |words| words.contains(&declared.to_string()) && words.contains("requester")
        ),
        "{by_owner}"
    );
    let notices =
        wait_for_job_notice(&fixture, &caller.work, None, "JOB_FAILED", Some(declared)).await;
    assert!(
        notices
            .iter()
            .any(|item| item["jobId"] == declared.to_string() && item["noticeType"] == "JOB_FAILED"),
        "the Requester's own failure is noticed too"
    );
    answers.stop().await;
    fixture.shutdown().await;
}
