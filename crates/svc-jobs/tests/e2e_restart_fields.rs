mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::lineage::{create, live_runner, mail, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, save_state};
use support::runner;
use support::{LONG, priority};
use uuid::Uuid;

const ORIGIN: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job {
      id status parentJobId initialJobId config
      source { bc entityId }
      manualRetry { predecessorJobId }
      passport { subjectKind subjectId document }
      requester { kind serviceId runId }
      call { mode callerAddress }
      payload { form message attachments { name contentType content } service documentId }
      requestedPriority effectivePriority forcedPriority
    }
  }
}
"#;

async fn view(fixture: &JobsFixture, job: Uuid) -> Value {
    let response = fixture
        .gql()
        .query(fixture.admin(), ORIGIN, json!({ "id": job.to_string() }))
        .await;
    assert!(response["errors"].is_null(), "{response}");
    response["data"]["jobsJob"]["job"].clone()
}

#[tokio::test]
async fn a_restart_job_keeps_the_parent_requester_passport_config_payload_mode_and_caller_of_its_origin()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let (mut x, mut y, mut z) = (
        live_runner(&fixture, "fields-x").await,
        live_runner(&fixture, "fields-y").await,
        live_runner(&fixture, "fields-z").await,
    );
    let a = JobDeclaration::new(&x.runner_type).with_max_attempts(1);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (a_trigger, a_agent) = started_with_agent(&fixture, &mut x).await;
    let a_work = Work::of(&x, &a_trigger);
    let forced = priority::force_job_priority(&fixture.gql(), fixture.admin(), a.job_id, 5).await;
    assert!(forced["errors"].is_null(), "{forced}");

    let payload = json!({ "form": "INLINE", "message": "the brief",
        "attachments": [{ "name": "n.txt", "content_type": "text/plain", "content": "aGk=" }] });
    let b = create(
        &fixture,
        &a_work,
        &y.runner_type,
        a.job_id,
        json!({
            "mode": "BACKGROUND", "caller_address": a_agent.to_string(),
            "config": { "model": "m" }, "payload": payload, "max_attempts": 1,
        }),
    )
    .await;
    let (b_trigger, b1) = started_with_agent(&fixture, &mut y).await;
    assert_eq!(runner::job_id(&b_trigger), b);
    assert_eq!(b_trigger["config"], json!({ "model": "m" }));
    assert_eq!(b_trigger["payload"]["message"], "the brief");
    assert_eq!(b_trigger["mode"], "BACKGROUND");
    assert_eq!(b_trigger["caller_address"], a_agent.to_string());
    assert!(b_trigger["passport"].is_object());
    let b_work = Work::of(&y, &b_trigger);
    save_state(&fixture, &b_work, json!({ "turn": 1 })).await;
    create(
        &fixture,
        &b_work,
        &z.runner_type,
        b,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (z_trigger, z1) = started_with_agent(&fixture, &mut z).await;
    y.complete_run(&b_trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, b_work.run_id, 1, "b1 finished")
        .await;
    let b_before = view(&fixture, b).await;

    mail(&fixture, &Work::of(&z, &z_trigger), z1, b1).await;
    let restart = y.next_trigger(LONG).await;
    let rb = runner::job_id(&restart);
    assert_ne!(rb, b, "a new Job id");
    for key in ["config", "payload", "passport", "mode", "caller_address"] {
        assert_eq!(
            restart[key], b_trigger[key],
            "the restart trigger carries the {key} of B"
        );
    }
    assert_eq!(restart["parent_job_id"], a.job_id.to_string());
    assert_eq!(restart["initial_job_id"], a.job_id.to_string());

    let rb_view = view(&fixture, rb).await;
    for key in [
        "parentJobId",
        "initialJobId",
        "config",
        "passport",
        "requester",
        "call",
        "payload",
    ] {
        assert_eq!(
            rb_view[key], b_before[key],
            "the restart Job keeps the {key} of B"
        );
    }
    assert_eq!(rb_view["parentJobId"], a.job_id.to_string());
    assert_eq!(
        rb_view["requester"]["runId"],
        a_work.run_id.to_string(),
        "the Requester is the Run of A"
    );
    assert!(rb_view["manualRetry"].is_null(), "no predecessor");
    assert!(rb_view["source"].is_null(), "no source reference");
    priority::assert_effective(
        &rb_view,
        5,
        Some(5),
        "born forced at the level of its forced parent",
    );
    assert_eq!(view(&fixture, b).await, b_before, "B is unchanged");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_restart_of_an_initial_job_is_its_own_parent_and_asks_its_priority_again_under_the_users_cap()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let (mut x, mut y) = (
        live_runner(&fixture, "own-x").await,
        live_runner(&fixture, "own-y").await,
    );
    let user = Uuid::now_v7();
    let capped = priority::set_user_max_priority(&fixture.gql(), fixture.admin(), user, 4).await;
    assert!(capped["errors"].is_null(), "{capped}");
    let a = JobDeclaration::new(&x.runner_type)
        .with_max_attempts(1)
        .triggered_by_user(user)
        .with_priority(5);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (a_trigger, a_agent) = started_with_agent(&fixture, &mut x).await;
    let a_work = Work::of(&x, &a_trigger);
    priority::assert_effective(
        &view(&fixture, a.job_id).await,
        4,
        None,
        "A is admitted under the cap of 4",
    );
    create(
        &fixture,
        &a_work,
        &y.runner_type,
        a.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (c_trigger, c1) = started_with_agent(&fixture, &mut y).await;
    save_state(&fixture, &a_work, json!({ "turn": 1 })).await;
    x.complete_run(&a_trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, a_work.run_id, 1, "A's agent finished")
        .await;
    let lowered = priority::set_user_max_priority(&fixture.gql(), fixture.admin(), user, 2).await;
    assert!(lowered["errors"].is_null(), "{lowered}");

    mail(&fixture, &Work::of(&y, &c_trigger), c1, a_agent).await;
    let restart = x.next_trigger(LONG).await;
    let ra = runner::job_id(&restart);
    assert_ne!(ra, a.job_id);
    assert_eq!(
        restart["parent_job_id"],
        ra.to_string(),
        "the restart Job is its own parent"
    );
    assert_eq!(
        restart["initial_job_id"],
        a.job_id.to_string(),
        "and keeps the Initial Job"
    );
    let ra_view = view(&fixture, ra).await;
    assert_eq!(ra_view["initialJobId"], a.job_id.to_string());
    priority::assert_priorities(
        &ra_view,
        5,
        2,
        None,
        "it asks for 5 again, capped again by the maximum of 2",
    );
    durable.close().await;
    fixture.shutdown().await;
}
