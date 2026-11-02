use serde_json::{Value, json};
use uuid::Uuid;

use super::db::Durable;
use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Work};
use super::lineage::{create, live_runner};
use super::producer::{JobDeclaration, Producer};
use super::restart::{FINISHED_OF_RUN, save_state};
use super::runner::{self, FakeRunner};
use super::surface::{self, Agent};
use super::{LONG, gql};

pub const CANCEL_NOTICES_OF_RUN: &str = "SELECT count(*) FROM inbox_items \
     WHERE run_id = $1 AND notice ->> 'type' = 'JOB_CANCELLED'";
pub const CANCEL_NOTICES_AT: &str = "SELECT count(*) FROM inbox_items \
     WHERE address = $1 AND notice ->> 'type' = 'JOB_CANCELLED'";
pub const REMOVED_RUN: &str = "SELECT count(*) FROM directory_removed_runs WHERE run_id = $1";
pub const WAKES_OF_RUN: &str = "SELECT count(*) FROM restart_wakes WHERE run_id = $1";

pub struct Chain<'a> {
    pub a: FakeRunner<'a>,
    pub b: FakeRunner<'a>,
    pub c: FakeRunner<'a>,
    pub a_job: Uuid,
    pub a_trigger: Value,
    pub a_work: Work,
    pub a_address: Uuid,
    pub b_job: Uuid,
    pub b_trigger: Value,
    pub b_work: Work,
    pub b_address: Uuid,
    pub c_job: Uuid,
    pub c_trigger: Value,
    pub c_work: Work,
}

pub async fn chain<'a>(fixture: &'a JobsFixture, label: &str, c_mode: &str) -> Chain<'a> {
    let (mut a, mut b, mut c) = (
        live_runner(fixture, &format!("{label}-a")).await,
        live_runner(fixture, &format!("{label}-b")).await,
        live_runner(fixture, &format!("{label}-c")).await,
    );
    let declared = JobDeclaration::new(&a.runner_type).with_max_attempts(1);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&declared)
        .await;
    let a_trigger = a.next_trigger(LONG).await;
    a.start_applied(fixture, &a_trigger).await;
    let a_agent = Agent::with_inbox();
    let a_address = a_agent.agent_id;
    surface::register(fixture.nats(), &a, runner::run_id(&a_trigger), &[a_agent]).await;
    let a_work = Work::of(&a, &a_trigger);

    let b_job = create(
        fixture,
        &a_work,
        &b.runner_type,
        declared.job_id,
        json!({ "mode": "BACKGROUND", "caller_address": a_address.to_string(), "max_attempts": 1 }),
    )
    .await;
    let b_trigger = b.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&b_trigger), b_job);
    b.start_applied(fixture, &b_trigger).await;
    let b_agent = Agent::with_inbox();
    let b_address = b_agent.agent_id;
    surface::register(fixture.nats(), &b, runner::run_id(&b_trigger), &[b_agent]).await;
    let b_work = Work::of(&b, &b_trigger);
    save_state(fixture, &b_work, json!({ "turn": 1 })).await;

    let mut extra = json!({ "mode": c_mode, "max_attempts": 1 });
    if c_mode == "BACKGROUND" {
        extra["caller_address"] = json!(b_address.to_string());
    }
    let c_job = create(fixture, &b_work, &c.runner_type, b_job, extra).await;
    let c_trigger = c.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&c_trigger), c_job);
    c.start_applied(fixture, &c_trigger).await;
    let c_work = Work::of(&c, &c_trigger);
    Chain {
        a,
        b,
        c,
        a_job: declared.job_id,
        a_trigger,
        a_work,
        a_address,
        b_job,
        b_trigger,
        b_work,
        b_address,
        c_job,
        c_trigger,
        c_work,
    }
}

impl Chain<'_> {
    pub async fn finish_a(&self, durable: &Durable) {
        self.a.complete_run(&self.a_trigger).await;
        durable
            .await_count(
                FINISHED_OF_RUN,
                self.a_work.run_id,
                1,
                "the agent of A finishes",
            )
            .await;
    }

    pub async fn finish_b(&self, durable: &Durable) {
        self.b.complete_run(&self.b_trigger).await;
        durable
            .await_count(
                FINISHED_OF_RUN,
                self.b_work.run_id,
                1,
                "the agent of B finishes",
            )
            .await;
    }

    pub async fn restart_b(&mut self, fixture: &JobsFixture) -> Value {
        let messenger = Agent::with_inbox();
        let messenger_id = messenger.agent_id;
        surface::register(fixture.nats(), &self.c, self.c_work.run_id, &[messenger]).await;
        let wake = gesture::Mail::new(messenger_id, &[self.b_address]);
        gesture::accepted(
            &gesture::send_mail(&fixture.gql(), &runner_access(), &self.c_work, &wake).await,
            "jobsSendMail",
        );
        let restart = self.b.next_trigger(LONG).await;
        self.b.start_applied(fixture, &restart).await;
        restart
    }

    pub async fn cancel_as_administrator(&self, fixture: &JobsFixture, job: Uuid) {
        gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), job).await;
    }

    pub async fn await_cancelled(&self, fixture: &JobsFixture, jobs: &[Uuid]) {
        for job in jobs {
            gql::wait_for_status(&fixture.gql(), fixture.admin(), *job, "CANCELLED", LONG).await;
        }
    }
}
