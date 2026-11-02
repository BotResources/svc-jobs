use serde_json::{Value, json};
use uuid::Uuid;

use super::call_notices::{Caller, background_call, wait_for_job_notice};
use super::db::Durable;
use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Mail, Work};
use super::runner::{self, FakeRunner};
use super::surface::{self, Agent};
use super::tree::call_child;
use super::{LONG, gql};

pub const RESOLUTIONS_OF_JOB: &str = "SELECT count(*) FROM job_resolutions WHERE job_id = $1";
pub const COMPLETIONS_OF_JOB: &str =
    "SELECT count(*) FROM job_resolutions WHERE job_id = $1 AND kind = 'COMPLETED'";

pub struct Called {
    pub job: Uuid,
    pub trigger: Value,
    pub work: Work,
}

fn item_id(item: &Value) -> Uuid {
    Uuid::parse_str(item["itemId"].as_str().expect("an item id")).expect("an item id is a uuid")
}

pub async fn sync_call(fixture: &JobsFixture, caller: &mut Caller<'_>) -> Called {
    let trigger = call_child(
        fixture,
        &mut caller.pair,
        caller.root,
        runner::run_id(&caller.trigger),
    )
    .await;
    caller.pair.child.start_applied(fixture, &trigger).await;
    Called {
        job: runner::job_id(&trigger),
        work: Work::of(&caller.pair.child, &trigger),
        trigger,
    }
}

pub async fn background(fixture: &JobsFixture, caller: &mut Caller<'_>) -> Called {
    let trigger = background_call(fixture, caller).await;
    caller.pair.child.start_applied(fixture, &trigger).await;
    Called {
        job: runner::job_id(&trigger),
        work: Work::of(&caller.pair.child, &trigger),
        trigger,
    }
}

pub async fn register_author(fixture: &JobsFixture, caller: &Caller<'_>, called: &Called) -> Uuid {
    let author = Agent::with_inbox();
    let author_id = author.agent_id;
    surface::register(
        fixture.nats(),
        &caller.pair.child,
        called.work.run_id,
        &[author],
    )
    .await;
    author_id
}

pub async fn mail_the_answer(fixture: &JobsFixture, caller: &Caller<'_>, called: &Called) -> Uuid {
    let author = register_author(fixture, caller, called).await;
    send_answer(fixture, caller, called, author).await
}

pub async fn send_answer(
    fixture: &JobsFixture,
    caller: &Caller<'_>,
    called: &Called,
    from: Uuid,
) -> Uuid {
    send_answer_as(fixture, &called.work, from, caller.address).await
}

pub async fn send_answer_as(fixture: &JobsFixture, from_run: &Work, from: Uuid, to: Uuid) -> Uuid {
    let mail = Mail::new(from, &[to]).with_body("the answer");
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), from_run, &mail).await,
        "jobsSendMail",
    );
    mail.mail_id
}

pub struct Restartable {
    pub called: Called,
    pub agent: Uuid,
}

pub async fn restartable(
    fixture: &JobsFixture,
    caller: &mut Caller<'_>,
    in_background: bool,
) -> Restartable {
    let called = if in_background {
        background(fixture, caller).await
    } else {
        sync_call(fixture, caller).await
    };
    let agent = Agent::with_inbox();
    let agent_id = agent.agent_id;
    surface::register(
        fixture.nats(),
        &caller.pair.child,
        called.work.run_id,
        &[agent],
    )
    .await;
    super::restart::save_state(fixture, &called.work, json!({ "turn": 1 })).await;
    Restartable {
        called,
        agent: agent_id,
    }
}

pub async fn reply_to(fixture: &JobsFixture, caller: &Caller<'_>, to: Uuid) -> Mail {
    let mail = Mail::new(caller.address, &[to]).with_body("a reply");
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &caller.work, &mail).await,
        "jobsSendMail",
    );
    mail
}

pub async fn started_restart(
    fixture: &JobsFixture,
    runner: &mut FakeRunner<'_>,
    agent: Uuid,
) -> Called {
    let trigger = runner.next_trigger(LONG).await;
    start_restart(fixture, runner, trigger, agent).await
}

pub async fn start_restart(
    fixture: &JobsFixture,
    runner: &FakeRunner<'_>,
    trigger: Value,
    agent: Uuid,
) -> Called {
    runner.start_applied(fixture, &trigger).await;
    surface::register(
        fixture.nats(),
        runner,
        runner::run_id(&trigger),
        &[Agent::with_inbox().with_id(agent)],
    )
    .await;
    Called {
        job: runner::job_id(&trigger),
        work: Work::of(runner, &trigger),
        trigger,
    }
}

pub async fn complete(caller: &Caller<'_>, called: &Called) {
    caller.pair.child.complete_run(&called.trigger).await;
}

pub async fn mark_the_notice(fixture: &JobsFixture, caller: &Caller<'_>, called: &Called) {
    mark_the_notice_as(fixture, &caller.pair.parent, &caller.work, called).await;
}

pub async fn mark_the_notice_as(
    fixture: &JobsFixture,
    runner: &FakeRunner<'_>,
    reading: &Work,
    called: &Called,
) {
    let seen = wait_for_job_notice(fixture, reading, None, "RUN_COMPLETED", Some(called.job)).await;
    let item = seen
        .iter()
        .find(|item| item["jobId"] == json!(called.job.to_string()))
        .expect("the notice that the Run completed");
    mark(fixture, runner, reading, item_id(item)).await;
}

pub async fn mark_the_mail(fixture: &JobsFixture, caller: &Caller<'_>, mail_id: Uuid) {
    mark_the_mail_as(
        fixture,
        &caller.pair.parent,
        &caller.work,
        caller.address,
        mail_id,
    )
    .await;
}

pub async fn unread_mails(fixture: &JobsFixture, reading: &Work, address: Uuid) -> Vec<Value> {
    gesture::items(&gesture::unread(&fixture.gql(), &runner_access(), reading, Some(address)).await)
        .into_iter()
        .filter(|item| item["itemKind"] == "MAIL")
        .collect()
}

pub async fn mark_the_mail_as(
    fixture: &JobsFixture,
    runner: &FakeRunner<'_>,
    reading: &Work,
    address: Uuid,
    mail_id: Uuid,
) {
    let waited = std::time::Instant::now();
    loop {
        let unread = unread_mails(fixture, reading, address).await;
        if let Some(item) = unread
            .iter()
            .find(|item| item["mailId"] == json!(mail_id.to_string()))
        {
            mark(fixture, runner, reading, item_id(item)).await;
            return;
        }
        assert!(
            waited.elapsed() < LONG,
            "the mail never arrived: {unread:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn mark(fixture: &JobsFixture, runner: &FakeRunner<'_>, reading: &Work, item: Uuid) {
    let marked = gesture::mark_read(fixture.nats(), runner, reading.run_id, &[item]).await;
    assert_eq!(marked["accepted"], true, "{marked}");
}

pub async fn stays(fixture: &JobsFixture, jobs: &[Uuid], status: &str) {
    tokio::time::sleep(super::QUIET).await;
    for job in jobs {
        assert_eq!(
            gql::status_of(&fixture.gql(), fixture.admin(), *job).await,
            status,
            "job {job} keeps its status through the sweeps"
        );
    }
}

pub async fn becomes_completed(fixture: &JobsFixture, jobs: &[Uuid]) {
    for job in jobs {
        gql::wait_for_status(&fixture.gql(), fixture.admin(), *job, "COMPLETED", LONG).await;
    }
}

pub async fn one_completion_each(durable: &Durable, jobs: &[Uuid]) {
    for job in jobs {
        durable
            .assert_count(
                COMPLETIONS_OF_JOB,
                *job,
                1,
                "the Job has one completed resolution",
            )
            .await;
        durable
            .assert_count(
                RESOLUTIONS_OF_JOB,
                *job,
                1,
                "the Job has no other resolution",
            )
            .await;
    }
}
