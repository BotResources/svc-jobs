mod support;

use std::time::Duration;

use br_test_harness::wait_until;
use serde_json::json;
use support::claim::{self, ClaimAnswer};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, dispatch, gql, wire};
use uuid::Uuid;

const CLAIM_WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn a_refused_claim_answers_one_reason_of_a_closed_vocabulary_and_reserves_nothing() {
    // Given: a CLAIM type with a live instance and one to be drained, a PUSH type, and a CLAIM
    // type about to be retired
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let main_type = wire::unique_runner_type("refusing");
    let push_type = wire::unique_runner_type("pushed");
    let retiring_type = wire::unique_runner_type("retiring");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut live = FakeRunner::new(fixture.nats(), &main_type, "live").declaring_capacity(2);
    let mut drainer = FakeRunner::new(fixture.nats(), &main_type, "drainer");
    let mut pusher = FakeRunner::new(fixture.nats(), &push_type, "pusher");
    let mut retiring = FakeRunner::new(fixture.nats(), &retiring_type, "retiring-a");
    for instance in [&mut live, &mut drainer, &mut pusher, &mut retiring] {
        instance.connect().await;
    }
    dispatch::await_live_instances(&client, admin, &main_type, 2, LONG).await;
    dispatch::await_live_instances(&client, admin, &push_type, 1, LONG).await;
    dispatch::await_live_instances(&client, admin, &retiring_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &main_type).await;
    dispatch::switch_to_claim(&client, admin, &retiring_type).await;
    gql::expect_success(
        &gql::deprecate_runner_type(&client, admin, &retiring_type).await,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "a platform administrator deprecates the type",
    );
    gql::expect_success(
        &gql::retire_runner_type(&client, admin, &retiring_type).await,
        wire::FIELD_RETIRE_RUNNER_TYPE,
        "a platform administrator retires the type",
    );

    // Given: a run the live instance claimed under one claim id and started
    let started_job = JobDeclaration::new(&main_type);
    producer.declare_v2(&started_job).await;
    events
        .expect_one(wire::FACT_QUEUED, started_job.job_id, LONG)
        .await;
    let first = claim::claim(fixture.nats(), &live, CLAIM_WAIT).await;
    let run = first.expect_run("the live instance claims the waiting job");
    live.start_run(&run).await;
    events
        .expect_one(wire::FACT_STARTED, started_job.job_id, LONG)
        .await;

    // Given: the second instance announces it is draining, and a job waits on the CLAIM type
    drainer.announce(wire::STATUS_DRAINING).await;
    let drained = wait_until(LONG, || async {
        dispatch::runner_type_view(&client, admin, &main_type).await["runnerType"]["instances"]
            .as_array()
            .is_some_and(|instances| {
                instances.iter().any(|instance| {
                    instance["instanceKey"] == json!("drainer")
                        && instance["reportedStatus"] == json!(wire::STATUS_DRAINING)
                })
            })
    })
    .await;
    assert!(drained, "the draining announcement reaches the fleet");
    let waiting = JobDeclaration::new(&main_type);
    producer.declare_v2(&waiting).await;
    events
        .expect_one(wire::FACT_QUEUED, waiting.job_id, LONG)
        .await;

    // When: each refusable claim is sent
    let nil_claim = serde_json::to_vec(&json!({
        "version": 1,
        "instance_key": "live",
        "claim_id": Uuid::nil().to_string(),
        "wait_ms": 3000,
    }))
    .expect("a claim with a nil id serializes");
    let refusals: [(ClaimAnswer, &str, &str); 7] = [
        (
            claim::claim_as(
                fixture.nats(),
                &main_type,
                "ghost",
                Uuid::now_v7(),
                CLAIM_WAIT,
            )
            .await,
            wire::REASON_INSTANCE_UNKNOWN,
            "an instance with no live presence",
        ),
        (
            claim::claim(fixture.nats(), &pusher, CLAIM_WAIT).await,
            wire::REASON_NOT_CLAIM_MODE,
            "an instance of a PUSH type",
        ),
        (
            claim::claim_again(fixture.nats(), &live, first.claim_id, CLAIM_WAIT).await,
            wire::REASON_CLAIM_SPENT,
            "a claim id repeated after its run started",
        ),
        (
            claim::claim_raw(
                fixture.nats(),
                &main_type,
                b"{not json".to_vec(),
                CLAIM_WAIT,
            )
            .await,
            wire::REASON_MALFORMED_CLAIM,
            "a request that is not JSON",
        ),
        (
            claim::claim_raw(fixture.nats(), &main_type, nil_claim, CLAIM_WAIT).await,
            wire::REASON_MALFORMED_CLAIM,
            "a claim with a nil claim id",
        ),
        (
            claim::claim(fixture.nats(), &drainer, CLAIM_WAIT).await,
            wire::REASON_INSTANCE_DRAINING,
            "an instance that announced it is draining",
        ),
        (
            claim::claim(fixture.nats(), &retiring, CLAIM_WAIT).await,
            wire::REASON_RUNNER_TYPE_RETIRED,
            "an instance of a retired type",
        ),
    ];

    // Then: each is refused under its own reason, taken from the closed vocabulary
    for (answer, reason, what) in &refusals {
        answer.expect_refused(reason, what);
        assert!(
            wire::CLAIM_REFUSALS.contains(reason),
            "{what}: a refused claim answers one reason of the closed vocabulary"
        );
    }

    // Then: no refusal reserved the waiting job; the next well-formed claim serves it
    assert_eq!(
        gql::status_of(&client, admin, waiting.job_id).await,
        "PENDING"
    );
    durable
        .assert_count(
            db::RUNS_OF_JOB,
            waiting.job_id,
            0,
            "no run for a refused claim",
        )
        .await;
    let served = claim::claim(fixture.nats(), &live, CLAIM_WAIT)
        .await
        .expect_run("a well-formed claim of the live instance");
    assert_eq!(runner::job_id(&served), waiting.job_id);

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
