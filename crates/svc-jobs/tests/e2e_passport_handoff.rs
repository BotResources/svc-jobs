mod support;

use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::lineage::{create, live_runner, mail, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, save_state};
use support::runner;
use support::{LONG, gql, wire};
use uuid::Uuid;

fn root_body(runner_type: &str, user: Uuid, known: Option<Uuid>) -> (Uuid, Value, Value) {
    let declaration = JobDeclaration::new(runner_type)
        .triggered_by_user(user)
        .with_max_attempts(2);
    let mut body = declaration.payload("projects");
    let passport = body["passport"].clone();
    body["triggered_by"] = known.map_or(Value::Null, |id| json!(id.to_string()));
    (declaration.job_id, body, passport)
}

#[tokio::test]
async fn the_whole_passport_of_a_job_reaches_every_run_its_retry_and_its_restart() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let (mut x, mut y) = (
        live_runner(&fixture, "pass-x").await,
        live_runner(&fixture, "pass-y").await,
    );
    let user = Uuid::now_v7();
    let (job, body, passport) = root_body(&x.runner_type, user, Some(user));
    producer.declare_payload(body).await;

    let first = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&first), job);
    assert_eq!(
        first["passport"], passport,
        "the trigger carries the whole Passport"
    );
    assert_eq!(first["triggered_by"], user.to_string());
    x.start_applied(&fixture, &first).await;
    x.fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    let (retry, agent) = started_with_agent(&fixture, &mut x).await;
    assert_eq!(runner::attempt_number(&retry), 2);
    assert_eq!(
        retry["passport"], passport,
        "a retry carries the same Passport"
    );
    let retry_work = Work::of(&x, &retry);
    create(
        &fixture,
        &retry_work,
        &y.runner_type,
        job,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (child, c1) = started_with_agent(&fixture, &mut y).await;
    assert_eq!(
        child["passport"], passport,
        "a child created without a Passport takes its parent's"
    );
    save_state(&fixture, &retry_work, json!({ "turn": 1 })).await;
    x.complete_run(&retry).await;
    durable
        .await_count(FINISHED_OF_RUN, retry_work.run_id, 1, "the agent finished")
        .await;
    mail(&fixture, &Work::of(&y, &child), c1, agent).await;
    let restart = x.next_trigger(LONG).await;
    assert_ne!(runner::job_id(&restart), job);
    assert_eq!(
        restart["passport"], passport,
        "a restart carries the same Passport"
    );

    let view = gql::job_view(&fixture.gql(), fixture.admin(), job).await;
    assert!(view.to_string().contains(&user.to_string()), "{view}");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_known_user_follows_the_passport_and_a_creation_without_a_user_carries_the_services() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut x = live_runner(&fixture, "known-x").await;
    let user = Uuid::now_v7();

    let (implied, body, _) = root_body(&x.runner_type, user, None);
    producer.declare_payload(body).await;
    let trigger = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), implied);
    assert_eq!(
        trigger["triggered_by"],
        user.to_string(),
        "the user of the Passport is the KnownUser"
    );
    let triggered: Value = gql::job(&fixture.gql(), fixture.admin(), implied).await;
    assert_eq!(triggered["triggeredBy"]["id"], user.to_string());

    let (mismatched, body, _) = root_body(&x.runner_type, user, Some(Uuid::now_v7()));
    producer.declare_payload(body).await;
    let rejected = answers
        .expect_one(wire::FACT_CREATION_REJECTED, mismatched, LONG)
        .await;
    assert_eq!(rejected.payload()["reason_code"], "passport_user_mismatch");
    durable
        .assert_count(db::JOBS_WITH_ID, mismatched, 0, "no Job")
        .await;

    let for_nobody = JobDeclaration::new(&x.runner_type);
    let service_passport = for_nobody.payload("projects")["passport"].clone();
    assert_eq!(service_passport["kind"], "service");
    producer.declare_v2(&for_nobody).await;
    let service_trigger = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&service_trigger), for_nobody.job_id);
    assert_eq!(
        service_trigger["passport"], service_passport,
        "the requesting service's Passport"
    );
    x.start_applied(&fixture, &service_trigger).await;

    let legacy_child = Uuid::now_v7();
    producer
        .send_v1(
            wire::VERB_CREATE,
            json!({
                "job_id": legacy_child.to_string(),
                "runner_type": x.runner_type,
                "producer": "projects",
                "parent_job_id": for_nobody.job_id.to_string(),
            }),
        )
        .await;
    let below = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&below), legacy_child);
    assert_eq!(
        below["passport"], service_passport,
        "a creation in the former form takes the Passport of its parent below another Job"
    );
    answers.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
