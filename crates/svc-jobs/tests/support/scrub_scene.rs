use serde_json::json;
use uuid::Uuid;

use super::db::Durable;
use super::erasure_scene::Scene;
use super::fixture::{JobsFixture, Knobs};
use super::gesture::Work;
use super::lineage::{act, live_runner};
use super::producer::{JobDeclaration, Producer};
use super::runner::FakeRunner;
use super::scrub::trigger_of;
use super::{LONG, gql, wire};
use serde_json::Value;

const LOG_LINES_OF_RUN: &str = "SELECT count(*) FROM run_logs WHERE run_id = $1";
pub const ENDED_RUN: &str = "SELECT count(*) FROM run_terminals WHERE run_id = $1";
const STEPS_OF_RUN: &str = "SELECT count(*) FROM steps WHERE run_id = $1";

pub async fn finish(fixture: &JobsFixture, durable: &Durable, s: &Scene<'_>) {
    let a_trigger = trigger_of(&s.a_work);
    s.x.declare_plan(&a_trigger, &["first step", "second step"])
        .await;
    s.x.start_step(&a_trigger, 0, "first step").await;
    s.x.log_line(&a_trigger, Some(0), "INFO", "a line of A")
        .await;
    s.y.log_line(&trigger_of(&s.b_work), None, "ERROR", "a line of B")
        .await;
    durable
        .await_count(STEPS_OF_RUN, s.a_work.run_id, 1, "the step of A")
        .await;
    durable
        .await_count(LOG_LINES_OF_RUN, s.a_work.run_id, 1, "the line of A")
        .await;
    durable
        .await_count(LOG_LINES_OF_RUN, s.b_work.run_id, 1, "the line of B")
        .await;
    s.w.complete_run(&trigger_of(&s.c_work)).await;
    s.y.fail_run(
        &trigger_of(&s.b_work),
        "PERMANENT",
        "provider_refused",
        None,
    )
    .await;
    for (work, what) in [(&s.b_work, "B"), (&s.c_work, "C")] {
        durable
            .await_count(
                ENDED_RUN,
                work.run_id,
                1,
                &format!("the Run of {what} ends"),
            )
            .await;
    }
    act(fixture, wire::VERB_FINISH, s.c, &s.a_work).await;
    act(fixture, wire::VERB_FINISH, s.e, &s.a_work).await;
    for job in [s.c, s.e] {
        gql::wait_for_status(&fixture.gql(), fixture.admin(), job, "COMPLETED", LONG).await;
    }
    s.x.complete_run(&a_trigger).await;
    durable
        .await_count(ENDED_RUN, s.a_work.run_id, 1, "the Run of A ends")
        .await;
    Producer::new(fixture.fabric(), "projects")
        .finish_v2(s.a)
        .await;
    for (job, status) in [
        (s.a, "COMPLETED"),
        (s.b, "FAILED"),
        (s.c, "COMPLETED"),
        (s.e, "COMPLETED"),
    ] {
        gql::wait_for_status(&fixture.gql(), fixture.admin(), job, status, LONG).await;
    }
}

pub fn tree_jobs(s: &Scene<'_>) -> [(Uuid, &'static str); 4] {
    [(s.a, "A"), (s.b, "B"), (s.c, "C"), (s.e, "E")]
}

pub struct LiveRoot<'a> {
    pub runner: FakeRunner<'a>,
    pub root: Uuid,
    pub trigger: Value,
    pub work: Work,
}

pub async fn live_root<'a>(
    fixture: &'a JobsFixture,
    label: &str,
    max_attempts: i64,
) -> LiveRoot<'a> {
    live_root_for(fixture, label, max_attempts, None).await
}

pub async fn live_root_for<'a>(
    fixture: &'a JobsFixture,
    label: &str,
    max_attempts: i64,
    user: Option<Uuid>,
) -> LiveRoot<'a> {
    let mut runner = live_runner(fixture, label).await;
    let mut declaration = JobDeclaration::new(&runner.runner_type)
        .with_config(json!({ "model": "m" }))
        .with_max_attempts(max_attempts);
    if let Some(user) = user {
        declaration = declaration.triggered_by_user(user);
    }
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "INLINE", "message": "the brief", "attachments": [] });
    Producer::new(fixture.fabric(), "projects")
        .declare_payload(body)
        .await;
    let trigger = runner.next_trigger(LONG).await;
    runner.start_applied(fixture, &trigger).await;
    let work = Work::of(&runner, &trigger);
    LiveRoot {
        runner,
        root: declaration.job_id,
        trigger,
        work,
    }
}

pub async fn end_run_and_finish(fixture: &JobsFixture, durable: &Durable, live: &LiveRoot<'_>) {
    live.runner.complete_run(&live.trigger).await;
    durable
        .await_count(ENDED_RUN, live.work.run_id, 1, "the Run ends")
        .await;
    Producer::new(fixture.fabric(), "projects")
        .finish_v2(live.root)
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        live.root,
        "COMPLETED",
        LONG,
    )
    .await;
}

pub async fn finished_root(fixture: &JobsFixture, durable: &Durable, label: &str) -> Uuid {
    let live = live_root(fixture, label, 1).await;
    end_run_and_finish(fixture, durable, &live).await;
    live.root
}

pub fn long_runs() -> Knobs {
    Knobs {
        run_max_duration_seconds: 300 * 86_400,
        inactivity_timeout_seconds: 300 * 86_400,
        ..Knobs::default()
    }
}
