mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::db::{self, Durable};
use support::events::{EventLog, ObservedEvent};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, codes, delta, gql, stream, subs,
    wire,
};
use uuid::Uuid;

#[tokio::test]
async fn a_producing_service_receives_a_definite_rejection_without_orphaned_work() {
    // Given: a valid job declared for a runner type nobody serves yet
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("creation");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    let opening_fleet = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(opening_fleet["runnerTypes"], json!([]));

    let entity_id = Uuid::now_v7();
    let accepted = JobDeclaration::new(&runner_type).with_source("projects", entity_id);
    let accepted_id = accepted.job_id;
    let accepted_command = producer.declare(&accepted).await;
    events
        .expect_one(wire::FACT_QUEUED, accepted_id, LONG)
        .await;

    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    delta::event_of(&queued, wire::EVT_QUEUED, accepted_id);
    delta::assert_active_affordances(&delta::assert_upserted_summary(
        &queued,
        accepted_id,
        "PENDING",
    ));
    stream::expect_total_silence(
        &mut fleet_watch,
        "accepted work cannot materialize an unregistered runner type",
        QUIET,
    )
    .await;
    assert!(
        gql::fleet_of(&client, admin, &runner_type).await.is_empty(),
        "the fleet omits a routing key until first presence registers the entity",
    );
    drop(fleet_watch);
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    let post_declaration = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(post_declaration["runnerTypes"], json!([]));

    let mut valid_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(accepted_id)).await;
    let opening = stream::snapshot(&mut valid_watch, JOB_CHANGED, SHORT).await;
    assert_eq!(opening["job"]["status"], json!("PENDING"));
    delta::assert_active_affordances(&opening);
    instance.expect_no_trigger(QUIET).await;

    // When: the producer sends one invalid declaration after another
    let before = gql::job_view(&client, admin, accepted_id).await;
    let mut refusals: Vec<(&str, String)> = Vec::new();

    let conflicting = JobDeclaration::new(&runner_type)
        .with_id(accepted_id)
        .with_config(json!({ "different": true }));
    producer
        .reuse_command_id_with(&accepted_command, conflicting.payload("projects"))
        .await;
    let reuse = events
        .expect_one(wire::FACT_CREATION_REJECTED, accepted_id, LONG)
        .await;
    let reuse_code = rejection_code(
        &reuse,
        "reusing a known job id with different input",
        &[&accepted_id.to_string()],
    );
    assert_eq!(
        reuse_code,
        wire::REASON_ID_REUSE,
        "the offer names the id-reuse refusal by this literal, so a producer can branch on it",
    );
    refusals.push(("id reuse", reuse_code));
    events
        .expect_exactly(wire::FACT_QUEUED, accepted_id, 1, QUIET)
        .await;

    let over_budget = JobDeclaration::new(&runner_type).with_max_attempts(10);
    let over_budget_id = over_budget.job_id;
    producer.declare(&over_budget).await;
    let budget = events
        .expect_one(wire::FACT_CREATION_REJECTED, over_budget_id, LONG)
        .await;
    refusals.push((
        "max attempts above the ceiling",
        rejection_code(
            &budget,
            "a maximum attempt count above the service ceiling",
            &["3"],
        ),
    ));
    assert_absent(&client, admin, over_budget_id).await;

    let duplicate_source = JobDeclaration::new(&runner_type).with_source("projects", entity_id);
    let duplicate_id = duplicate_source.job_id;
    producer.declare(&duplicate_source).await;
    let duplicate = events
        .expect_one(wire::FACT_CREATION_REJECTED, duplicate_id, LONG)
        .await;
    refusals.push((
        "a second non-terminal job for one source",
        rejection_code(
            &duplicate,
            "a second non-terminal job for one source reference",
            &[&entity_id.to_string()],
        ),
    ));
    assert_absent(&client, admin, duplicate_id).await;

    let not_time_ordered = JobDeclaration::new(&runner_type).with_id(Uuid::new_v4());
    let not_time_ordered_id = not_time_ordered.job_id;
    producer.declare(&not_time_ordered).await;
    let malformed = events
        .expect_one(wire::FACT_CREATION_REJECTED, not_time_ordered_id, LONG)
        .await;
    refusals.push((
        "a client-supplied id that is not a UUIDv7",
        rejection_code(
            &malformed,
            "a job id that is not a UUIDv7",
            &[&not_time_ordered_id.to_string()],
        ),
    ));
    assert_absent(&client, admin, not_time_ordered_id).await;

    let orphan_parent = Uuid::now_v7();
    let orphan = JobDeclaration::new(&runner_type).with_parent(orphan_parent);
    let orphan_id = orphan.job_id;
    producer.declare(&orphan).await;
    let unknown_parent = events
        .expect_one(wire::FACT_CREATION_REJECTED, orphan_id, LONG)
        .await;
    refusals.push((
        "a forged parent job id",
        rejection_code(
            &unknown_parent,
            "a parent job that does not exist",
            &[&orphan_parent.to_string()],
        ),
    ));
    assert_absent(&client, admin, orphan_id).await;

    let unattributed = JobDeclaration::new(&runner_type);
    let unattributed_id = unattributed.job_id;
    let mut payload = unattributed.payload("projects");
    payload
        .as_object_mut()
        .expect("a creation payload is a JSON object")
        .remove("producer");
    producer.declare_payload(payload).await;
    let anonymous = events
        .expect_one(wire::FACT_CREATION_REJECTED, unattributed_id, LONG)
        .await;
    refusals.push((
        "a declaration naming no producer",
        rejection_code(
            &anonymous,
            "a creation that names no producing bounded context",
            &[&unattributed_id.to_string()],
        ),
    ));
    assert_absent(&client, admin, unattributed_id).await;

    // Then: the six causes stay distinguishable, and nothing else moved
    codes::assert_pairwise_distinct(&refusals);
    assert_eq!(
        gql::job_view(&client, admin, accepted_id).await,
        before,
        "a refused creation leaves the valid job's projection and affordances identical",
    );
    stream::expect_total_silence(
        &mut listing,
        "a refused creation reaches no list subscriber",
        QUIET,
    )
    .await;
    stream::expect_total_silence(
        &mut valid_watch,
        "a refused creation touches no other job",
        QUIET,
    )
    .await;
    stream::expect_total_silence(
        &mut fleet_watch,
        "a refused creation moves no fleet count",
        QUIET,
    )
    .await;
    instance.expect_no_trigger(QUIET).await;

    for rejected in [
        over_budget_id,
        duplicate_id,
        not_time_ordered_id,
        orphan_id,
        unattributed_id,
    ] {
        durable
            .assert_all(
                rejected,
                &[
                    (db::JOBS_WITH_ID, 0, "no partial job"),
                    (db::RUNS_OF_JOB, 0, "no run"),
                ],
            )
            .await;
        assert_eq!(
            durable
                .count_like(db::OUTBOX_MENTIONING, &format!("%{rejected}%"))
                .await,
            1,
            "a rejected id owes exactly its refusal on the outbox, no other consequence",
        );
    }
    assert_eq!(
        durable.count(db::SOURCE_CLAIMS_OF_ENTITY, entity_id).await,
        1,
        "a refused creation never claims the source entity — a stale claim would silently block \
         every future job for that source",
    );

    // When: another bounded context declares work on an entity id of its own that happens to match
    let cross_producer = Producer::new(fixture.fabric(), "chat");
    let cross_type = wire::unique_runner_type("creation_cross");
    let cross = JobDeclaration::new(&cross_type).with_source("chat", entity_id);
    let cross_id = cross.job_id;
    cross_producer.declare(&cross).await;
    events.expect_one(wire::FACT_QUEUED, cross_id, LONG).await;
    events
        .expect_none(wire::FACT_CREATION_REJECTED, cross_id, QUIET)
        .await;
    assert_eq!(
        gql::job_by_source(&client, admin, "chat", entity_id).await["job"]["id"],
        json!(cross_id.to_string()),
        "a source reference is the producer and the entity together, so each context looks its \
         own work up",
    );
    assert_eq!(
        gql::job_by_source(&client, admin, "projects", entity_id).await["job"]["id"],
        json!(accepted_id.to_string()),
    );
    assert_eq!(
        durable.count(db::SOURCE_CLAIMS_OF_ENTITY, entity_id).await,
        2,
        "one claim per producer for the same entity id — a uniqueness constraint set on the \
         entity alone would lock every other context out",
    );

    // When: the runner type finally becomes available
    instance.connect().await;
    let registered =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_REGISTERED, LONG).await;
    delta::assert_fleet(&registered, &runner_type, 1, 0, 0, 1);
    gql::assert_allowed(&registered, wire::ACTION_DISPATCH);
    gql::assert_allowed(&registered, wire::ACTION_DEPRECATE);
    assert_eq!(
        gql::assert_blocked(&registered, wire::ACTION_REACTIVATE),
        "runner_type_already_active",
    );
    assert_eq!(
        gql::assert_blocked(&registered, wire::ACTION_RETIRE),
        "runner_type_not_deprecated",
    );
    let connected =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    let connected_type = delta::fleet_projection(&connected, &runner_type);
    assert_eq!(connected_type["isAvailable"], json!(true));
    delta::assert_instance(&connected_type, "instance-a", false, &[], &instance.version);

    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), accepted_id);
    assert_eq!(runner::attempt_number(&trigger), 1);
    let dispatched = stream::await_delta(
        &mut valid_watch,
        JOB_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(&dispatched, wire::EVT_RUN_DISPATCHED, accepted_id)["runId"],
        json!(runner::run_id(&trigger).to_string())
    );

    instance.start_run(&trigger).await;
    let started =
        stream::await_delta(&mut valid_watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    delta::projection(&started, accepted_id, "IN_PROGRESS");
    delta::assert_active_affordances(&started);
    delta::assert_fleet(
        &stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await,
        &runner_type,
        0,
        1,
        1,
        0,
    );
    assert_eq!(
        instance.trigger_count().await,
        1,
        "only the one valid declaration was ever dispatched",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

fn rejection_code(event: &ObservedEvent, what: &str, must_mention: &[&str]) -> String {
    let payload = event.payload();
    let code = payload["reason_code"]
        .as_str()
        .unwrap_or_else(|| panic!("{what}: a rejection carries a stable reason code: {payload}"))
        .to_string();
    codes::assert_stable_reason(&code, what);
    let params = &payload["params"];
    assert!(
        params.is_object(),
        "{what}: a rejection carries structured params the producer can act on: {payload}"
    );
    let rendered = params.to_string();
    for expected in must_mention {
        assert!(
            rendered.contains(expected),
            "{what}: the params must name what the producer has to change ({expected}): {payload}"
        );
    }
    code
}

async fn assert_absent(
    client: &br_test_harness::GraphqlClient,
    admin: &br_core_auth::Passport,
    job_id: Uuid,
) {
    let response = client
        .query(
            admin,
            support::docs::JOB_DETAIL,
            json!({ "id": job_id.to_string() }),
        )
        .await;
    assert!(
        response["data"]["jobsJob"].is_null(),
        "a refused creation must leave no job behind: {response}"
    );
    verdict::expect_code_shaped(
        &response,
        "reading a job whose creation was refused — the field is non-nullable, so absence owes a \
         structured refusal, never a silent null an internal error would produce just as well",
    );
}
