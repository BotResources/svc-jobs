mod support;

use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::lineage::{act, create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, gql, wire};
use uuid::Uuid;

async fn root_run(
    fixture: &JobsFixture,
    runner: &mut FakeRunner<'_>,
    attempts: i64,
) -> (Uuid, Work, serde_json::Value) {
    let root = JobDeclaration::new(&runner.runner_type).with_max_attempts(attempts);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&root)
        .await;
    let (trigger, _) = started_with_agent(fixture, runner).await;
    (root.job_id, Work::of(runner, &trigger), trigger)
}

async fn reasons(answers: &EventLog, job: Uuid, count: usize) -> Vec<String> {
    let mut found: Vec<String> = answers
        .expect_exactly(wire::FACT_COMMAND_REJECTED, job, count, LONG)
        .await
        .iter()
        .map(|refusal| {
            refusal.payload()["reason_code"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    found.sort();
    found
}

#[tokio::test]
async fn only_the_requester_finishes_or_fails_a_job_and_only_while_it_is_live() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let client = fixture.gql();
    let (mut x, mut y, mut z) = (
        live_runner(&fixture, "rights-x").await,
        live_runner(&fixture, "rights-y").await,
        live_runner(&fixture, "rights-z").await,
    );
    let (a, a_work, a_trigger) = root_run(&fixture, &mut x, 1).await;
    let (_, third, _) = root_run(&fixture, &mut z, 1).await;
    let b = create(
        &fixture,
        &a_work,
        &y.runner_type,
        a,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let b_trigger = y.next_trigger(LONG).await;
    y.start_applied(&fixture, &b_trigger).await;
    let b_runner = Work::of(&y, &b_trigger);
    let later = create(
        &fixture,
        &a_work,
        &y.runner_type,
        a,
        json!({ "mode": "SYNC" }),
    )
    .await;
    y.next_trigger(LONG).await;

    for verb in [wire::VERB_FINISH, wire::VERB_FAIL] {
        act(&fixture, verb, b, &third).await;
        act(&fixture, verb, b, &b_runner).await;
        Producer::new(fixture.fabric(), "billing")
            .send_v3(verb, json!({ "job_id": b.to_string() }))
            .await;
    }
    assert_eq!(reasons(&answers, b, 6).await, vec!["not_requester"; 6]);
    assert_eq!(
        gql::status_of(&client, fixture.admin(), b).await,
        "IN_PROGRESS",
        "B does not change"
    );

    act(&fixture, wire::VERB_FINISH, b, &a_work).await;
    gql::wait_for_status(&client, fixture.admin(), b, "COMPLETED", LONG).await;
    act(&fixture, wire::VERB_FINISH, b, &a_work).await;
    assert!(
        reasons(&answers, b, 7)
            .await
            .contains(&"job_terminal".to_owned()),
        "a terminal Job is not finished again"
    );

    x.complete_run(&a_trigger).await;
    gql::wait_for_status(&client, fixture.admin(), later, "IN_PROGRESS", LONG).await;
    let ended = br_test_harness::wait_until(LONG, || async {
        gql::job(&client, fixture.admin(), a).await["activeRunId"].is_null()
    })
    .await;
    assert!(ended, "A's Run has ended with success");
    for verb in [wire::VERB_FINISH, wire::VERB_FAIL] {
        act(&fixture, verb, later, &a_work).await;
    }
    assert_eq!(
        reasons(&answers, later, 2).await,
        vec!["acting_run_not_live"; 2],
        "a finish or a fail naming a Run that ended with success is refused"
    );
    assert_eq!(
        gql::status_of(&client, fixture.admin(), later).await,
        "IN_PROGRESS"
    );
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_retry_cannot_resolve_what_its_failed_first_run_created() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let client = fixture.gql();
    let (mut x, mut y) = (
        live_runner(&fixture, "retry-x").await,
        live_runner(&fixture, "retry-y").await,
    );
    let (a, first, first_trigger) = root_run(&fixture, &mut x, 2).await;
    let child = create(
        &fixture,
        &first,
        &y.runner_type,
        a,
        json!({ "mode": "SYNC" }),
    )
    .await;
    y.next_trigger(LONG).await;
    x.fail_run(&first_trigger, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    gql::wait_for_status(&client, fixture.admin(), child, "CANCELLED", LONG).await;
    let (retry, _) = started_with_agent(&fixture, &mut x).await;
    let retry_work = Work::of(&x, &retry);
    for verb in [wire::VERB_FINISH, wire::VERB_FAIL] {
        act(&fixture, verb, child, &retry_work).await;
    }
    assert_eq!(reasons(&answers, child, 2).await, vec!["job_terminal"; 2]);
    assert_eq!(
        gql::status_of(&client, fixture.admin(), child).await,
        "CANCELLED"
    );
    answers.stop().await;
    fixture.shutdown().await;
}
