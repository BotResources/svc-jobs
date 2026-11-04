mod support;

use br_test_harness::SseSubscription;
use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{JOB_CHANGED, LONG, QUIET, SHORT, delta, gql, stream, subs, wire};
use uuid::Uuid;

#[tokio::test]
async fn two_live_jobs_of_one_source_reference_are_both_accepted_dispatched_and_listed() {
    // Given: a runner type served by an instance with room for several Runs
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("same_source");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(3);
    instance.connect().await;
    let entity_id = Uuid::now_v7();

    // When: the producer declares a job for an entity of its own, then a second
    // one for the same entity while the first is still live
    let first = JobDeclaration::new(&runner_type)
        .with_source("projects", entity_id)
        .with_max_attempts(1);
    let first_id = first.job_id;
    producer.declare(&first).await;
    events.expect_one(wire::FACT_QUEUED, first_id, LONG).await;
    let second = JobDeclaration::new(&runner_type).with_source("projects", entity_id);
    let second_id = second.job_id;
    producer.declare(&second).await;

    // Then: both are accepted, neither is refused
    events.expect_one(wire::FACT_QUEUED, second_id, LONG).await;
    for job_id in [first_id, second_id] {
        events
            .expect_none(wire::FACT_CREATION_REJECTED, job_id, QUIET)
            .await;
    }

    // Then: both are dispatched and run at the same time
    let mut watches = Vec::new();
    for job_id in [first_id, second_id] {
        let mut watch =
            SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
        stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
        watches.push((job_id, watch));
    }
    let mut triggers: Vec<Value> = Vec::new();
    for _ in 0..2 {
        triggers.push(instance.next_trigger(LONG).await);
    }
    let mut dispatched: Vec<Uuid> = triggers.iter().map(runner::job_id).collect();
    dispatched.sort();
    let mut declared = vec![first_id, second_id];
    declared.sort();
    assert_eq!(
        dispatched, declared,
        "each live job of the source gets its own trigger: {triggers:?}",
    );
    for trigger in &triggers {
        assert_eq!(runner::attempt_number(trigger), 1);
        instance.start_run(trigger).await;
    }
    for (job_id, watch) in &mut watches {
        let started = stream::await_delta(watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
        delta::projection(&started, *job_id, "IN_PROGRESS");
    }

    // Then: the administrator finds both under their source, newest first
    let of_source = gql::jobs_by_source(&client, admin, "projects", entity_id).await;
    assert_eq!(
        gql::ids_of(&of_source),
        vec![json!(second_id.to_string()), json!(first_id.to_string())],
        "the jobs of a source read newest first: {of_source:?}",
    );
    for row in &of_source {
        assert_eq!(row["job"]["status"], json!("IN_PROGRESS"), "{row}");
        assert_eq!(
            row["job"]["source"],
            json!({ "bc": "projects", "entityId": entity_id.to_string() }),
        );
        delta::assert_active_affordances(row);
        gql::assert_affordances_well_formed(row, "a row of the source lookup");
    }
    assert_eq!(
        gql::job_by_source(&client, admin, "projects", entity_id).await["job"]["id"],
        json!(second_id.to_string()),
        "the deprecated single lookup answers with the newest non-terminal job",
    );
    assert_eq!(
        durable.count(db::SOURCE_CLAIMS_OF_ENTITY, entity_id).await,
        1,
        "the jobs of one source share one source row",
    );

    // When: the first job fails for good while the second still runs, and an
    // administrator retries it
    let first_trigger = triggers
        .iter()
        .find(|trigger| runner::job_id(trigger) == first_id)
        .expect("the first job was dispatched");
    instance
        .fail_run(first_trigger, "PERMANENT", "unsupported_format", None)
        .await;
    events.expect_one(wire::FACT_FAILED, first_id, LONG).await;
    let failed = gql::job_view(&client, admin, first_id).await;
    let failed_resolution_id = uuid_at(&failed["job"]["resolution"]["id"]);
    let successor_id = Uuid::now_v7();
    let retried = gql::manual_retry_job(
        &client,
        admin,
        Uuid::now_v7(),
        first_id,
        successor_id,
        failed_resolution_id,
    )
    .await;

    // Then: the successor carries the same source and is dispatched beside the live job
    gql::expect_success(
        &retried,
        wire::FIELD_MANUAL_RETRY_JOB,
        "a manual retry whose source another live job carries",
    );
    events
        .expect_one(wire::FACT_QUEUED, successor_id, LONG)
        .await;
    let successor_trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&successor_trigger), successor_id);
    assert_eq!(
        gql::job(&client, admin, second_id).await["status"],
        json!("IN_PROGRESS"),
        "the job already running for the source is left as it was",
    );

    // Then: the source lists its three jobs, newest first, a page at a time
    let all = vec![
        json!(successor_id.to_string()),
        json!(second_id.to_string()),
        json!(first_id.to_string()),
    ];
    assert_eq!(
        gql::ids_of(&gql::jobs_by_source(&client, admin, "projects", entity_id).await),
        all,
    );
    let page = gql::jobs_by_source_page(&client, admin, "projects", entity_id, 2, None).await;
    let first_page = gql::edges_of(&page);
    assert_eq!(
        first_page
            .iter()
            .map(|edge| edge["node"]["job"]["id"].clone())
            .collect::<Vec<_>>(),
        all[..2].to_vec(),
    );
    assert_eq!(page["pageInfo"]["hasNextPage"], json!(true));
    let cursor = first_page[1]["cursor"]
        .as_str()
        .expect("an edge carries its cursor")
        .to_owned();
    let rest =
        gql::jobs_by_source_page(&client, admin, "projects", entity_id, 2, Some(&cursor)).await;
    assert_eq!(
        gql::edges_of(&rest)
            .iter()
            .map(|edge| edge["node"]["job"]["id"].clone())
            .collect::<Vec<_>>(),
        all[2..].to_vec(),
    );
    assert_eq!(rest["pageInfo"]["hasNextPage"], json!(false));
    assert!(
        gql::jobs_by_source(&client, admin, "chat", entity_id)
            .await
            .is_empty(),
        "another bounded context's reference to the same entity id holds none of these jobs",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

fn uuid_at(value: &Value) -> Uuid {
    Uuid::parse_str(
        value
            .as_str()
            .unwrap_or_else(|| panic!("expected a UUID, got: {value}")),
    )
    .expect("a resolution id is a UUID")
}
