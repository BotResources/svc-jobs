use serde_json::{Value, json};
use uuid::Uuid;

use super::LONG;

use super::db::Durable;
use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Work};
use super::producer::{JobDeclaration, Producer};
use super::runner;
use super::surface::{self, Agent};
use super::tree::{Pair, call_child, pair};

pub const RESTARTS_OF_RUN: &str = "SELECT count(*) FROM restart_origins WHERE restarts_run_id = $1";
pub const FINISHED_OF_RUN: &str =
    "SELECT count(*) FROM directory_agents WHERE run_id = $1 AND state = 'FINISHED'";
pub const UNREAD_AT: &str =
    "SELECT count(*) FROM inbox_items WHERE address = $1 AND read_at IS NULL";

pub struct Scene<'a> {
    pub pair: Pair<'a>,
    pub root: Uuid,
    pub parent_trigger: Value,
    pub parent_work: Work,
    pub child_trigger: Value,
    pub child_work: Work,
    pub sender: Uuid,
    pub peer: Uuid,
    pub receiver: Uuid,
    pub bystander: Uuid,
}

pub async fn running_child<'a>(fixture: &'a JobsFixture, label: &str) -> Scene<'a> {
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(fixture, label).await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(fixture, &parent_trigger).await;
    let (sender, peer) = (Agent::with_inbox(), Agent::with_inbox());
    let (sender_id, peer_id) = (sender.agent_id, peer.agent_id);
    surface::register(
        nats,
        &pair.parent,
        runner::run_id(&parent_trigger),
        &[sender, peer],
    )
    .await;
    let child_trigger = call_child(
        fixture,
        &mut pair,
        root.job_id,
        runner::run_id(&parent_trigger),
    )
    .await;
    pair.child.start_applied(fixture, &child_trigger).await;
    let (receiver, bystander) = (Agent::with_inbox(), Agent::with_inbox());
    let (receiver_id, bystander_id) = (receiver.agent_id, bystander.agent_id);
    surface::register(
        nats,
        &pair.child,
        runner::run_id(&child_trigger),
        &[receiver, bystander],
    )
    .await;
    let child_work = Work::of(&pair.child, &child_trigger);
    let saved = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": child_work.wire(), "state": { "turn": 7 } } }),
        )
        .await;
    gesture::accepted(&saved, "jobsSaveState");
    Scene {
        parent_work: Work::of(&pair.parent, &parent_trigger),
        child_work,
        pair,
        root: root.job_id,
        parent_trigger,
        child_trigger,
        sender: sender_id,
        peer: peer_id,
        receiver: receiver_id,
        bystander: bystander_id,
    }
}

impl Scene<'_> {
    pub fn child_run(&self) -> Uuid {
        runner::run_id(&self.child_trigger)
    }

    pub fn parent_run(&self) -> Uuid {
        runner::run_id(&self.parent_trigger)
    }

    pub async fn finish_child(&self, durable: &Durable) {
        self.pair.child.complete_run(&self.child_trigger).await;
        durable
            .await_count(
                FINISHED_OF_RUN,
                self.child_run(),
                2,
                "the agents of the ended child Run finish",
            )
            .await;
    }

    pub async fn restartable_now(&self, fixture: &JobsFixture, agent: Uuid) -> Value {
        let listing = surface::list(fixture.nats(), &self.pair.parent, self.parent_run()).await;
        listing["agents"]
            .as_array()
            .and_then(|agents| {
                agents
                    .iter()
                    .find(|listed| listed["agent_id"] == agent.to_string())
            })
            .map(|listed| listed["restartable_now"].clone())
            .unwrap_or_else(|| panic!("the agent is listed: {listing}"))
    }

    pub async fn mail_receiver(&self, fixture: &JobsFixture, from: Uuid) -> gesture::Mail {
        let mail = gesture::Mail::new(from, &[self.receiver]);
        let sent =
            gesture::send_mail(&fixture.gql(), &runner_access(), &self.parent_work, &mail).await;
        gesture::accepted(&sent, "jobsSendMail");
        mail
    }
}

pub async fn save_state(fixture: &JobsFixture, work: &Work, state: Value) {
    let saved = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": work.wire(), "state": state } }),
        )
        .await;
    gesture::accepted(&saved, "jobsSaveState");
}

pub async fn upload_result(fixture: &JobsFixture, work: &Work, message: &str) {
    let uploaded = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": work.wire(), "answer": gesture::content(message, &[]) } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
}

pub async fn result_for(durable: &Durable, job_id: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT result_id::uuid FROM job_results WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(durable.pool())
        .await
        .expect("the answer is kept as a Result")
}

pub async fn read_result(fixture: &JobsFixture, work: &Work, result_id: Uuid) -> Value {
    fixture
        .gql()
        .query(
            &runner_access(),
            gesture::READ_RESULT,
            json!({ "input": { "work": work.wire(), "resultId": result_id.to_string(), "keep": true } }),
        )
        .await
}

pub async fn register_same(fixture: &JobsFixture, pair: &Pair<'_>, run: Uuid, agent: Uuid) {
    let registered = surface::register(
        fixture.nats(),
        &pair.parent,
        run,
        &[Agent::with_inbox().with_id(agent)],
    )
    .await;
    assert_eq!(registered["accepted"], true, "{registered}");
}

pub async fn retry_manually(fixture: &JobsFixture, failed_job: Uuid) -> Uuid {
    super::gql::wait_for_status(&fixture.gql(), fixture.admin(), failed_job, "FAILED", LONG).await;
    let resolution = Uuid::parse_str(
        super::gql::job_view(&fixture.gql(), fixture.admin(), failed_job).await["job"]["resolution"]
            ["id"]
            .as_str()
            .expect("a failed job names its resolution"),
    )
    .expect("the resolution id is a uuid");
    let successor = Uuid::now_v7();
    let retried = super::gql::manual_retry_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        failed_job,
        successor,
        resolution,
    )
    .await;
    assert!(
        retried.get("errors").is_none_or(|errors| errors.is_null()),
        "{retried}"
    );
    successor
}
