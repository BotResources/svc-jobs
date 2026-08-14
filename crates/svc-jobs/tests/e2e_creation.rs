mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::events::{EventLog, ObservedEvent};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_producing_service_receives_a_definite_rejection_without_orphaned_work() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("creation");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let entity_id = Uuid::now_v7();
    let accepted = JobDeclaration::new(&runner_type).with_source("projects", entity_id);
    let accepted_id = accepted.job_id;
    let accepted_command = producer.declare(&accepted).await;
    events
        .expect_one(wire::FACT_QUEUED, accepted_id, LONG)
        .await;
    instance.next_trigger(LONG).await;

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::jobs_changed_subscription(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, "jobsChanged", SHORT).await;

    let duplicate_source = JobDeclaration::new(&runner_type).with_source("projects", entity_id);
    let duplicate_id = duplicate_source.job_id;
    producer.declare(&duplicate_source).await;
    let refusal = events
        .expect_one(wire::FACT_CREATION_REJECTED, duplicate_id, LONG)
        .await;
    assert_stable_reason(
        &refusal,
        "a second non-terminal job for one source reference",
    );
    assert_absent(&client, admin, duplicate_id).await;
    events
        .expect_none(wire::FACT_QUEUED, duplicate_id, QUIET)
        .await;

    let by_source = gql::job_by_source(&client, admin, "projects", entity_id).await;
    assert_eq!(
        by_source["job"]["id"],
        json!(accepted_id.to_string()),
        "the source lookup keeps answering with the single non-terminal job",
    );

    let conflicting_reuse = JobDeclaration::new(&runner_type).with_id(accepted_id);
    producer
        .reuse_command_id_with(&accepted_command, conflicting_reuse.payload("projects"))
        .await;
    let reuse_refusal = events
        .expect_one(wire::FACT_CREATION_REJECTED, accepted_id, LONG)
        .await;
    assert_stable_reason(
        &reuse_refusal,
        "reusing a known job id with different input",
    );
    events
        .expect_exactly(wire::FACT_QUEUED, accepted_id, 1, QUIET)
        .await;

    let over_budget = JobDeclaration::new(&runner_type).with_max_attempts(10);
    let over_budget_id = over_budget.job_id;
    producer.declare(&over_budget).await;
    let budget_refusal = events
        .expect_one(wire::FACT_CREATION_REJECTED, over_budget_id, LONG)
        .await;
    assert_stable_reason(
        &budget_refusal,
        "a maximum attempt count above the service ceiling",
    );
    assert_absent(&client, admin, over_budget_id).await;

    let not_time_ordered = JobDeclaration::new(&runner_type).with_id(Uuid::new_v4());
    let not_time_ordered_id = not_time_ordered.job_id;
    producer.declare(&not_time_ordered).await;
    let id_refusal = events
        .expect_one(wire::FACT_CREATION_REJECTED, not_time_ordered_id, LONG)
        .await;
    assert_stable_reason(&id_refusal, "a job id that is not a UUIDv7");
    assert_absent(&client, admin, not_time_ordered_id).await;

    let orphan = JobDeclaration::new(&runner_type).with_parent(Uuid::now_v7());
    let orphan_id = orphan.job_id;
    producer.declare(&orphan).await;
    let parent_refusal = events
        .expect_one(wire::FACT_CREATION_REJECTED, orphan_id, LONG)
        .await;
    assert_stable_reason(&parent_refusal, "a parent job that does not exist");
    assert_absent(&client, admin, orphan_id).await;

    let unknown_type = wire::unique_runner_type("nobody_serves_this");
    let waiting = JobDeclaration::new(&unknown_type);
    let waiting_id = waiting.job_id;
    producer.declare(&waiting).await;
    events.expect_one(wire::FACT_QUEUED, waiting_id, LONG).await;
    events
        .expect_none(wire::FACT_CREATION_REJECTED, waiting_id, QUIET)
        .await;
    assert_eq!(
        gql::status_of(&client, admin, waiting_id).await,
        "PENDING",
        "an unavailable runner type makes dispatch wait, it never refuses a creation",
    );
    let mut nobody = FakeRunner::new(fixture.nats(), &unknown_type, "absent");
    nobody.expect_no_trigger(QUIET).await;

    assert_eq!(
        instance.trigger_count().await,
        1,
        "a refused creation leaves no dispatched work behind",
    );
    listing
        .expect_silence("a refused creation reaches no list subscriber", QUIET)
        .await;

    events.stop().await;
    fixture.shutdown().await;
}

fn assert_stable_reason(event: &ObservedEvent, what: &str) {
    let payload = event.payload();
    let reason = payload["reason_code"]
        .as_str()
        .unwrap_or_else(|| panic!("{what}: a rejection carries a stable reason code: {payload}"));
    assert!(
        verdict::is_code_shaped(reason),
        "{what}: the rejection reason must be a code, not prose: {reason}"
    );
}

async fn assert_absent(
    client: &br_test_harness::GraphqlClient,
    admin: &br_core_auth::Passport,
    job_id: Uuid,
) {
    let response = client
        .query(admin, docs::JOB_DETAIL, json!({ "id": job_id.to_string() }))
        .await;
    assert!(
        response["data"]["jobsJob"].is_null(),
        "a refused creation must leave no job behind: {response}"
    );
}
