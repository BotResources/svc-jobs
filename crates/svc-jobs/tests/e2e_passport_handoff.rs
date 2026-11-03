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
use support::{LONG, QUIET, gql, priority, wire};
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

#[tokio::test]
async fn a_call_with_the_passport_of_its_parent_is_queued_whatever_known_user_it_names() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "front-agents");
    let (mut x, mut y) = (
        live_runner(&fixture, "reply-x").await,
        live_runner(&fixture, "ask-y").await,
    );
    let user = Uuid::now_v7();

    // Given: a root Job created with job.create.v3, a user's Passport and a named KnownUser
    let root = JobDeclaration::new(&x.runner_type).triggered_by(user, "Alice (trial)");
    let body = root.payload("front-agents");
    let passport = body["passport"].clone();
    producer.declare_payload(body).await;
    let trigger = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), root.job_id);
    x.start_applied(&fixture, &trigger).await;
    let work = Work::of(&x, &trigger);

    // When: its Run calls with the same Passport document and `triggered_by: null`
    let unnamed = create(
        &fixture,
        &work,
        &y.runner_type,
        root.job_id,
        json!({ "mode": "SYNC", "passport": passport, "triggered_by": null }),
    )
    .await;
    // Then: the call is queued below the root, not refused
    let queued = answers.expect_one(wire::FACT_QUEUED, unnamed, LONG).await;
    assert_eq!(queued.payload()["parent_job_id"], root.job_id.to_string());
    answers
        .expect_none(wire::FACT_CREATION_REJECTED, unnamed, QUIET)
        .await;
    // And: it runs for the user of the Passport under the name its parent knew
    let view = gql::job(&fixture.gql(), fixture.admin(), unnamed).await;
    assert_eq!(view["triggeredBy"]["id"], user.to_string(), "{view}");
    assert_eq!(
        view["triggeredBy"]["displayName"], "Alice (trial)",
        "{view}"
    );
    // And: the called runner receives the Passport of the root unchanged
    let handed = y.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&handed), unnamed);
    assert_eq!(handed["passport"], passport, "the Passport of the parent");
    assert_eq!(handed["triggered_by"], user.to_string());

    // When: its Run calls again with the same Passport and another name for the same user
    let renamed = create(
        &fixture,
        &work,
        &y.runner_type,
        root.job_id,
        json!({
            "mode": "SYNC",
            "passport": passport,
            "triggered_by": { "id": user.to_string(), "display_name": "Alice" },
        }),
    )
    .await;
    // Then: the name takes no part in the Passport check
    answers.expect_one(wire::FACT_QUEUED, renamed, LONG).await;
    answers
        .expect_none(wire::FACT_CREATION_REJECTED, renamed, QUIET)
        .await;
    let handed = y.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&handed), renamed);
    assert_eq!(handed["passport"], passport, "the Passport of the parent");
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_user_known_by_name_keeps_it_when_a_later_creation_gives_only_the_id() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("named");
    let user = Uuid::now_v7();

    // Given: a user first seen with a real name
    let named = JobDeclaration::new(&runner_type).triggered_by(user, "Alice (trial)");
    producer.declare(&named).await;
    answers
        .expect_one(wire::FACT_QUEUED, named.job_id, LONG)
        .await;

    // When: a creation names the same user by id only, and another names no KnownUser
    let (by_id, body, _) = root_body(&runner_type, user, Some(user));
    producer.declare_payload(body).await;
    let (unnamed, body, _) = root_body(&runner_type, user, None);
    producer.declare_payload(body).await;
    for job in [by_id, unnamed] {
        answers.expect_one(wire::FACT_QUEUED, job, LONG).await;
    }

    // Then: every Job of that user still shows the real name
    for job in [named.job_id, by_id, unnamed] {
        let view = gql::job(&fixture.gql(), fixture.admin(), job).await;
        assert_eq!(view["triggeredBy"]["id"], user.to_string(), "{view}");
        assert_eq!(
            view["triggeredBy"]["displayName"], "Alice (trial)",
            "{view}"
        );
    }

    // When: a later creation states another name
    let renamed = JobDeclaration::new(&runner_type).triggered_by(user, "Alice");
    producer.declare(&renamed).await;
    answers
        .expect_one(wire::FACT_QUEUED, renamed.job_id, LONG)
        .await;
    // Then: a stated name still replaces the one Jobs knew
    let view = gql::job(&fixture.gql(), fixture.admin(), named.job_id).await;
    assert_eq!(view["triggeredBy"]["displayName"], "Alice", "{view}");

    // And: a user Jobs knows only by id shows its id, stored as its id text
    let durable = Durable::open(&fixture.app_url()).await;
    let stranger = Uuid::now_v7();
    let (alone, body, _) = root_body(&runner_type, stranger, Some(stranger));
    producer.declare_payload(body).await;
    answers.expect_one(wire::FACT_QUEUED, alone, LONG).await;
    let view = gql::job(&fixture.gql(), fixture.admin(), alone).await;
    assert_eq!(
        view["triggeredBy"]["displayName"],
        stranger.to_string(),
        "{view}"
    );
    durable
        .assert_count(
            "SELECT count(*) FROM known_users WHERE id = $1 AND display_name = $1::text",
            stranger,
            1,
            "a user known only by id is stored under its id text, the form every version reads",
        )
        .await;
    durable
        .assert_count(
            "SELECT count(*) FROM known_users WHERE id = $1 AND display_name = 'Alice'",
            user,
            1,
            "the named user keeps the name last stated",
        )
        .await;
    durable.close().await;
    answers.stop().await;
    fixture.shutdown().await;
}

// An end-to-end guard; the proof that a successor carries its user without a
// name is the unit tests in `manual_retry/tests/successor.rs` and `restart/tests.rs`.
#[tokio::test]
async fn a_manual_retry_keeps_the_name_its_user_was_renamed_to_after_the_failure() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut x = live_runner(&fixture, "renamed-x").await;
    let (client, admin) = (fixture.gql(), fixture.admin());
    let user = Uuid::now_v7();

    // Given: a Job of a user named "Amelie" that failed for good
    let origin = JobDeclaration::new(&x.runner_type)
        .triggered_by(user, "Amelie")
        .with_max_attempts(1);
    producer.declare(&origin).await;
    let trigger = x.next_trigger(LONG).await;
    x.start_applied(&fixture, &trigger).await;
    x.fail_run(&trigger, "PERMANENT", "unsupported_format", None)
        .await;
    gql::wait_for_status(&client, admin, origin.job_id, "FAILED", LONG).await;

    // And: a later creation renames the user
    let renaming = JobDeclaration::new(&x.runner_type).triggered_by(user, "Amelie Martin");
    producer.declare(&renaming).await;
    answers
        .expect_one(wire::FACT_QUEUED, renaming.job_id, LONG)
        .await;

    // When: an administrator retries the failed Job
    let failed = gql::job(&client, admin, origin.job_id).await;
    let resolution: Uuid = failed["resolution"]["id"]
        .as_str()
        .and_then(|raw| raw.parse().ok())
        .expect("a failed Job has a resolution");
    let successor = Uuid::now_v7();
    gql::expect_success(
        &gql::manual_retry_job(
            &client,
            admin,
            Uuid::now_v7(),
            origin.job_id,
            successor,
            resolution,
        )
        .await,
        wire::FIELD_MANUAL_RETRY_JOB,
        "an administrator retries the failed Job",
    );

    // Then: the successor runs for the same user, and every view keeps the latest name
    gql::wait_for_status(&client, admin, successor, "IN_PROGRESS", LONG).await;
    for job in [origin.job_id, successor] {
        let view = gql::job(&client, admin, job).await;
        assert_eq!(view["triggeredBy"]["id"], user.to_string(), "{view}");
        assert_eq!(
            view["triggeredBy"]["displayName"], "Amelie Martin",
            "{view}"
        );
    }
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_administrator_without_a_display_name_claim_keeps_the_name_jobs_knows() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let client = fixture.gql();
    let runner_type = wire::unique_runner_type("admin-named");
    let admin_id = Uuid::now_v7();
    let unnamed_admin = |id: Uuid| {
        br_test_harness::PassportBuilder::new()
            .user_id(id)
            .super_admin(true)
            .build()
    };

    // Given: an administrator Jobs already knows by a real name
    let named = JobDeclaration::new(&runner_type).triggered_by(admin_id, "Ada Admin");
    producer.declare(&named).await;
    answers
        .expect_one(wire::FACT_QUEUED, named.job_id, LONG)
        .await;

    // When: they set a maximum with a Passport that has no `display_name` claim
    let capped = Uuid::now_v7();
    gql::expect_success(
        &priority::set_user_max_priority(&client, &unnamed_admin(admin_id), capped, 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "an administrator without a display name sets a maximum",
    );

    // Then: the name Jobs knows for them is kept, in the store and in the views
    durable
        .assert_count(
            "SELECT count(*) FROM known_users WHERE id = $1 AND display_name = 'Ada Admin'",
            admin_id,
            1,
            "an administrator known only by id writes no name over the one Jobs knows",
        )
        .await;
    let view = gql::job(&client, fixture.admin(), named.job_id).await;
    assert_eq!(view["triggeredBy"]["displayName"], "Ada Admin", "{view}");

    // And: an administrator Jobs never saw is recorded under their id
    let stranger = Uuid::now_v7();
    gql::expect_success(
        &priority::set_user_max_priority(&client, &unnamed_admin(stranger), capped, 3).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "an unseen administrator without a display name sets a maximum",
    );
    durable
        .assert_count(
            "SELECT count(*) FROM known_users WHERE id = $1 AND display_name = $1::text",
            stranger,
            1,
            "an administrator known only by id is stored under their id text",
        )
        .await;
    durable.close().await;
    answers.stop().await;
    fixture.shutdown().await;
}
