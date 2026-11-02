use serde_json::{Value, json};
use uuid::Uuid;

use super::db::Durable;
use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Mail, Work};
use super::lineage::{create, live_runner, started_with_agent};
use super::producer::{JobDeclaration, Producer};
use super::restart::{result_for, save_state, upload_result};
use super::runner::{self, FakeRunner};

pub fn request_data() -> Value {
    json!({ "config": { "model": "m" },
        "payload": { "form": "INLINE", "message": "the brief", "attachments": [] } })
}

pub fn child_body(mode: &str) -> Value {
    let mut body = request_data();
    body["mode"] = json!(mode);
    body["max_attempts"] = json!(1);
    body
}

pub struct Scene<'a> {
    pub x: FakeRunner<'a>,
    pub y: FakeRunner<'a>,
    pub z: FakeRunner<'a>,
    pub w: FakeRunner<'a>,
    pub a: Uuid,
    pub a_work: Work,
    pub p: Uuid,
    pub b: Uuid,
    pub b_work: Work,
    pub q: Uuid,
    pub c: Uuid,
    pub c_work: Work,
    pub s: Uuid,
    pub e: Uuid,
    pub e_work: Work,
    pub b_result: Uuid,
    pub mail_to_q: Uuid,
    pub mail_p_s: Uuid,
    pub attachment: Uuid,
}

pub async fn build<'a>(fixture: &'a JobsFixture, durable: &Durable, label: &str) -> Scene<'a> {
    let (mut x, mut y, mut z, mut w) = (
        live_runner(fixture, &format!("{label}-x")).await,
        live_runner(fixture, &format!("{label}-y")).await,
        live_runner(fixture, &format!("{label}-z")).await,
        live_runner(fixture, &format!("{label}-w")).await,
    );
    let declaration = JobDeclaration::new(&x.runner_type)
        .with_config(json!({ "model": "m" }))
        .with_max_attempts(1);
    let mut body = declaration.payload("projects");
    body["payload"] = request_data()["payload"].clone();
    Producer::new(fixture.fabric(), "projects")
        .declare_payload(body)
        .await;
    let (a_trigger, p) = started_with_agent(fixture, &mut x).await;
    let a_work = Work::of(&x, &a_trigger);
    let a = declaration.job_id;

    let b = create(fixture, &a_work, &y.runner_type, a, child_body("SYNC")).await;
    let (b_trigger, q) = started_with_agent(fixture, &mut y).await;
    assert_eq!(runner::job_id(&b_trigger), b);
    let b_work = Work::of(&y, &b_trigger);
    save_state(fixture, &b_work, json!({ "turn": 1 })).await;
    let to_q = Mail::new(p, &[q]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &a_work, &to_q).await,
        "jobsSendMail",
    );
    upload_result(fixture, &b_work, "the answer of B").await;
    let b_result = result_for(durable, b).await;

    let e = create(fixture, &a_work, &z.runner_type, a, child_body("SYNC")).await;
    let (e_trigger, _) = started_with_agent(fixture, &mut z).await;
    let e_work = Work::of(&z, &e_trigger);
    upload_result(fixture, &e_work, "the answer of E").await;
    z.complete_run(&e_trigger).await;

    let mut background = child_body("BACKGROUND");
    background["caller_address"] = json!(p.to_string());
    let c = create(fixture, &a_work, &w.runner_type, a, background).await;
    let (c_trigger, s) = started_with_agent(fixture, &mut w).await;
    let c_work = Work::of(&w, &c_trigger);
    let attachment = Uuid::now_v7();
    let p_s = Mail::new(p, &[s]).with_attachment(attachment, "a.txt");
    gesture::accepted(
        &gesture::upload_attachment(
            &fixture.gql(),
            &runner_access(),
            &a_work,
            &p_s,
            attachment,
            "a.txt",
            "the file",
        )
        .await,
        "jobsUploadAttachment",
    );
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &a_work, &p_s).await,
        "jobsSendMail",
    );
    Scene {
        x,
        y,
        z,
        w,
        a,
        a_work,
        p,
        b,
        b_work,
        q,
        c,
        c_work,
        s,
        e,
        e_work,
        b_result,
        mail_to_q: to_q.mail_id,
        mail_p_s: p_s.mail_id,
        attachment,
    }
}
