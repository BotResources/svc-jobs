mod support;

use serde_json::{Value, json};
use support::call_notices::{
    mail_notices, notices, wait_for_job_notice, wait_for_mail_notices, wait_for_notice,
};
use support::fixture::JobsFixture;
use support::gesture::{self, Mail, Work};
use support::lineage::{create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::{gql, runner};
use uuid::Uuid;

fn words_of(item: &Value) -> String {
    let parts = [
        item["message"].as_str(),
        item["answer"]["message"].as_str(),
        item["failureReport"]["diagnostic"].as_str(),
    ];
    parts.into_iter().flatten().collect::<Vec<_>>().join(" ")
}

#[tokio::test]
async fn no_notice_text_names_a_job_of_the_run_or_carries_a_passport() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let (mut x, mut m, mut y, mut z) = (
        live_runner(&fixture, "texts-x").await,
        live_runner(&fixture, "texts-m").await,
        live_runner(&fixture, "texts-y").await,
        live_runner(&fixture, "texts-z").await,
    );
    let triggering_user = Uuid::now_v7();
    let tree_root = JobDeclaration::new(&x.runner_type)
        .triggered_by(triggering_user, "Ada Lovelace")
        .with_max_attempts(1);
    producer.declare_v2(&tree_root).await;
    let (root_trigger, _) = started_with_agent(&fixture, &mut x).await;
    let root_work = Work::of(&x, &root_trigger);
    let middle_job = create(
        &fixture,
        &root_work,
        &m.runner_type,
        tree_root.job_id,
        json!({ "mode": "SYNC", "max_attempts": 1 }),
    )
    .await;
    let (middle_trigger, _) = started_with_agent(&fixture, &mut m).await;
    assert_eq!(runner::job_id(&middle_trigger), middle_job);
    let middle_work = Work::of(&m, &middle_trigger);
    let caller_job = create(
        &fixture,
        &middle_work,
        &y.runner_type,
        middle_job,
        json!({ "mode": "SYNC", "max_attempts": 1 }),
    )
    .await;
    let (caller_trigger, address) = started_with_agent(&fixture, &mut y).await;
    assert_eq!(runner::job_id(&caller_trigger), caller_job);
    let caller_work = Work::of(&y, &caller_trigger);
    let strange_parent = Uuid::now_v7();
    let background = |parent: Uuid| {
        json!({ "mode": "BACKGROUND", "caller_address": address.to_string(), "max_attempts": 1,
            "parent_job_id": parent.to_string() })
    };

    let completed = create(
        &fixture,
        &caller_work,
        &z.runner_type,
        caller_job,
        background(caller_job),
    )
    .await;
    let trigger = z.next_trigger(support::LONG).await;
    z.start_applied(&fixture, &trigger).await;
    z.complete_run(&trigger).await;
    wait_for_job_notice(
        &fixture,
        &caller_work,
        Some(address),
        "RUN_COMPLETED",
        Some(completed),
    )
    .await;

    let failed = create(
        &fixture,
        &caller_work,
        &z.runner_type,
        caller_job,
        background(caller_job),
    )
    .await;
    let trigger = z.next_trigger(support::LONG).await;
    z.start_applied(&fixture, &trigger).await;
    z.fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    wait_for_job_notice(
        &fixture,
        &caller_work,
        Some(address),
        "JOB_FAILED",
        Some(failed),
    )
    .await;

    let refused = create(
        &fixture,
        &caller_work,
        &z.runner_type,
        strange_parent,
        background(strange_parent),
    )
    .await;
    wait_for_job_notice(
        &fixture,
        &caller_work,
        Some(address),
        "REFUSED",
        Some(refused),
    )
    .await;

    let unknown = Uuid::now_v7();
    let mail = Mail::new(address, &[unknown]);
    let sent = gesture::send_mail(
        &fixture.gql(),
        &support::fixture::runner_access(),
        &caller_work,
        &mail,
    )
    .await;
    assert_eq!(gesture::refusal(&sent, "jobsSendMail"), "bad_addresses");
    wait_for_mail_notices(&fixture, &caller_work, Some(address), 1).await;

    let cancelled = create(
        &fixture,
        &caller_work,
        &z.runner_type,
        caller_job,
        background(caller_job),
    )
    .await;
    let trigger = z.next_trigger(support::LONG).await;
    z.start_applied(&fixture, &trigger).await;
    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), cancelled).await;
    wait_for_notice(&fixture, &caller_work, Some(address), "JOB_CANCELLED").await;

    let mut items = notices(&fixture, &caller_work, Some(address)).await;
    items.extend(mail_notices(&fixture, &caller_work, Some(address)).await);
    let kinds: Vec<&str> = items
        .iter()
        .filter_map(|item| match item["itemKind"].as_str() {
            Some("MAIL_NOTICE") => Some("MAIL_NOTICE"),
            _ => item["noticeType"].as_str(),
        })
        .collect();
    for wanted in [
        "ACCEPTED",
        "RUN_COMPLETED",
        "JOB_FAILED",
        "JOB_CANCELLED",
        "REFUSED",
        "MAIL_NOTICE",
    ] {
        assert!(kinds.contains(&wanted), "no {wanted} notice in {kinds:?}");
    }
    let jobs_of_the_run = [tree_root.job_id, middle_job, caller_job, strange_parent];
    let passport_marks = [
        triggering_user.to_string(),
        "ada lovelace".to_owned(),
        "passport".to_owned(),
    ];
    let mut told = 0;
    for item in &items {
        let words = words_of(item);
        for job in jobs_of_the_run {
            assert!(
                !words.contains(&job.to_string()),
                "a text names the Job {job} of the Run: {item}"
            );
        }
        for mark in &passport_marks {
            assert!(
                !words.to_lowercase().contains(mark),
                "a text carries {mark}: {item}"
            );
        }
        told += usize::from(!words.is_empty());
    }
    assert!(
        told >= 4,
        "the notices tell their story in words: {items:?}"
    );
    fixture.shutdown().await;
}
