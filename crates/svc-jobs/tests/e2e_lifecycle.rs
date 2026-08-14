mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";
const LOG_TAIL: &str = "jobsJobLogTail";

#[tokio::test]
async fn an_administrator_follows_a_job_from_declaration_to_audited_deletion() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("lifecycle");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let entity_id = Uuid::now_v7();
    let operator_id = Uuid::now_v7();
    let declaration = JobDeclaration::new(&runner_type)
        .with_config(json!({ "prompt": "summarise", "depth": 2 }))
        .triggered_by(operator_id, "Amelie")
        .with_source("projects", entity_id)
        .with_max_attempts(2);
    let job_id = declaration.job_id;

    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let view = gql::job_view(&client, admin, job_id).await;
    let job = view["job"].clone();
    assert_eq!(job["status"], json!("PENDING"));
    assert_eq!(job["attemptCount"], json!(0));
    assert_eq!(job["runnerType"], json!(runner_type));
    assert_eq!(job["producer"], json!("projects"));
    assert_eq!(job["config"], json!({ "prompt": "summarise", "depth": 2 }));
    assert_eq!(job["maxAttempts"], json!(2));
    assert_eq!(job["triggeredBy"]["id"], json!(operator_id.to_string()));
    assert_eq!(job["source"]["entityId"], json!(entity_id.to_string()));
    gql::assert_allowed(&view, wire::ACTION_CANCEL);
    gql::assert_blocked(&view, wire::ACTION_DELETE);
    gql::assert_blocked(&view, wire::ACTION_MANUAL_RETRY);

    assert!(
        gql::fleet_of(&client, admin, &runner_type).await.is_empty(),
        "a runner type appears only on the first presence signal of one of its instances",
    );
    instance.expect_no_trigger(QUIET).await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    assert_eq!(opening["job"]["status"], json!("PENDING"));

    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&trigger), 1);
    assert_eq!(runner::job_id(&trigger), job_id);
    assert_eq!(
        trigger["config"],
        json!({ "prompt": "summarise", "depth": 2 }),
        "the runner configuration is forwarded unchanged",
    );
    let run = runner::run_id(&trigger);

    let fleet = gql::fleet_of(&client, admin, &runner_type).await;
    assert_eq!(fleet.len(), 1, "the presence signal registers the type");
    assert_eq!(fleet[0]["runnerType"]["typeKey"], json!(runner_type));
    assert_eq!(fleet[0]["runnerType"]["isAvailable"], json!(true));

    instance.start_run(&trigger).await;
    events.expect_one(wire::FACT_STARTED, job_id, LONG).await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsRunStartedEvent", LONG).await;
    assert_eq!(
        gql::job(&client, admin, job_id).await["activeRunId"],
        json!(run.to_string())
    );

    let mut tail =
        SseSubscription::open(fixture.url(), admin, &docs::log_tail_subscription(job_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;

    instance
        .declare_plan(&trigger, &["fetch", "summarise", "store"])
        .await;
    events
        .expect_one(wire::FACT_PLAN_DECLARED, job_id, LONG)
        .await;
    instance.start_step(&trigger, 0, "fetch").await;
    instance
        .log_line(&trigger, Some(0), "INFO", "fetching")
        .await;
    instance.start_step(&trigger, 1, "summarise").await;
    instance
        .log_line(&trigger, Some(1), "WARNING", "slow provider")
        .await;
    events
        .expect_exactly(wire::FACT_STEP_STARTED, job_id, 2, LONG)
        .await;

    let appended = stream::drain_appended(&mut tail, LOG_TAIL, 2, LONG).await;
    assert_eq!(appended.len(), 2);

    let progressed = gql::job(&client, admin, job_id).await;
    let attempt = gql::run_by_attempt(&progressed, 1);
    assert_eq!(attempt["status"], json!("STARTED"));
    assert_eq!(attempt["declaredPlan"]["declarationNumber"], json!(1));
    assert_eq!(attempt["instance"]["instanceKey"], json!("instance-a"));
    assert_eq!(
        progressed["progression"]["currentStep"]["index"],
        json!(1),
        "starting step 1 implicitly closes step 0",
    );
    let logs = gql::logs_of(&client, admin, job_id).await;
    assert_eq!(
        logs.len(),
        2,
        "a job's log is the ordered union of its runs'"
    );
    assert_eq!(logs[0]["level"], json!("INFO"));
    assert_eq!(logs[1]["level"], json!("WARNING"));

    instance.complete_run(&trigger).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsRunCompletedEvent", LONG).await;
    assert_eq!(
        gql::status_of(&client, admin, job_id).await,
        "IN_PROGRESS",
        "a successful run does not complete its job — only the owner does",
    );
    events
        .expect_none(wire::FACT_COMPLETED, job_id, QUIET)
        .await;

    let resolution_id = Uuid::now_v7();
    producer.finish(job_id, resolution_id).await;
    gql::wait_for_status(&client, admin, job_id, "COMPLETED", LONG).await;
    let completed = events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;
    let payload = completed.payload();
    assert!(
        payload["result"].is_null() && payload["output"].is_null(),
        "a completion event never carries a work product: {payload}",
    );
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobCompletedEvent", LONG).await;

    let terminal = gql::job_view(&client, admin, job_id).await;
    assert_eq!(terminal["job"]["resolution"]["kind"], json!("COMPLETED"));
    assert_eq!(
        terminal["job"]["resolution"]["id"],
        json!(resolution_id.to_string())
    );
    gql::assert_blocked(&terminal, wire::ACTION_CANCEL);
    gql::assert_allowed(&terminal, wire::ACTION_DELETE);

    let deletion = gql::delete_job(&client, admin, job_id).await;
    verdict::expect_ack(
        &deletion,
        "an administrator deletes an eligible terminal job",
    );
    assert_eq!(
        deletion["data"]["jobsDeleteJob"],
        json!({ "success": true }),
        "a mutation returns a verdict, never the mutated state",
    );
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobDeletedEvent", LONG).await;

    let audited = gql::job_view(&client, admin, job_id).await;
    assert_eq!(audited["job"]["isDeleted"], json!(true));
    assert_eq!(
        audited["job"]["deletion"]["deletedBy"]["id"],
        json!(admin.actor_id().to_string())
    );
    assert_eq!(audited["job"]["runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(audited["job"]["resolution"]["kind"], json!("COMPLETED"));
    assert_eq!(
        gql::logs_of(&client, admin, job_id).await.len(),
        2,
        "logs remain part of the audit record after soft deletion",
    );
    gql::assert_blocked(&audited, wire::ACTION_DELETE);

    let default_list = gql::list_jobs(&client, admin, json!({})).await;
    assert!(
        !default_list
            .iter()
            .any(|node| node["job"]["id"] == json!(job_id.to_string())),
        "a deleted job is excluded from the default listing",
    );
    let only_deleted = gql::list_jobs(&client, admin, json!({ "deleted": "ONLY_DELETED" })).await;
    assert!(
        only_deleted
            .iter()
            .any(|node| node["job"]["id"] == json!(job_id.to_string())),
        "a deleted job stays reachable through the deleted filter",
    );

    let repeat = gql::delete_job(&client, admin, job_id).await;
    verdict::expect_code_shaped(&repeat, "deleting an already-deleted job");

    let pending = JobDeclaration::new(&runner_type);
    let pending_id = pending.job_id;
    producer.declare(&pending).await;
    events.expect_one(wire::FACT_QUEUED, pending_id, LONG).await;
    let refused = gql::delete_job(&client, admin, pending_id).await;
    verdict::expect_code_shaped(&refused, "deleting a non-terminal job");
    assert_eq!(
        gql::job(&client, admin, pending_id).await["isDeleted"],
        json!(false),
        "a refused deletion leaves no trace on the job",
    );

    events.stop().await;
    fixture.shutdown().await;
}
