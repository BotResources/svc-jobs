mod support;

use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::surface::{self, Agent};
use support::tree::pair;
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_background_call_must_name_a_mail_address_of_the_calling_run() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-caller").await;
    let nats = fixture.nats();
    let first = JobDeclaration::new(&pair.parent.runner_type);
    let second = JobDeclaration::new(&pair.parent.runner_type);
    producer.declare_v2(&first).await;
    producer.declare_v2(&second).await;
    let first_trigger = pair.parent.next_trigger(LONG).await;
    let second_trigger = pair.parent.next_trigger(LONG).await;
    let (first_job, first_run) = (
        runner::job_id(&first_trigger),
        runner::run_id(&first_trigger),
    );
    let (_, second_run) = (
        runner::job_id(&second_trigger),
        runner::run_id(&second_trigger),
    );
    pair.parent.start_applied(&fixture, &first_trigger).await;
    pair.parent.start_applied(&fixture, &second_trigger).await;
    let own = Agent::with_inbox();
    let own_address = own.agent_id;
    let foreign = Agent::with_inbox();
    let foreign_address = foreign.agent_id;
    surface::register(nats, &pair.parent, first_run, &[own]).await;
    surface::register(nats, &pair.parent, second_run, &[foreign]).await;

    let call = |address: Uuid| {
        let job_id = Uuid::now_v7();
        (
            job_id,
            json!({
                "job_id": job_id.to_string(),
                "runner_type": pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": first_job.to_string(),
                "mode": "BACKGROUND",
                "caller_address": address.to_string(),
                "acting_run_id": first_run.to_string(),
                "acting_instance_key": pair.parent.instance_key,
            }),
        )
    };
    let (unheld_job, unheld) = call(Uuid::now_v7());
    producer.send_v3(wire::VERB_CREATE, unheld).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, unheld_job, LONG)
        .await;
    assert_eq!(refused.payload()["reason_code"], "caller_address_not_held");

    let (foreign_job, other_run) = call(foreign_address);
    producer.send_v3(wire::VERB_CREATE, other_run).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, foreign_job, LONG)
        .await;
    assert_eq!(
        refused.payload()["reason_code"],
        "caller_address_not_of_run"
    );

    let (accepted_job, accepted) = call(own_address);
    producer.send_v3(wire::VERB_CREATE, accepted).await;
    let trigger = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), accepted_job);
    assert_eq!(trigger["mode"], "BACKGROUND");
    assert_eq!(trigger["caller_address"], own_address.to_string());
    answers.stop().await;
    fixture.shutdown().await;
}
