mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, codes, delta, gql,
    stream, subs, wire,
};
use uuid::Uuid;

#[tokio::test]
async fn an_administrator_manually_retries_a_job_after_its_owner_accepts_a_terminal_failure() {
    // Given: a job with no automatic retry left, watched on every channel
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("retryable");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let entity_id = Uuid::now_v7();
    let operator_id = Uuid::now_v7();
    let declaration = JobDeclaration::new(&runner_type)
        .with_config(json!({ "prompt": "convert" }))
        .triggered_by(operator_id, "Amelie")
        .with_source("projects", entity_id)
        .with_max_attempts(1);
    let predecessor_id = declaration.job_id;
    producer.declare(&declaration).await;
    events
        .expect_one(wire::FACT_QUEUED, predecessor_id, LONG)
        .await;

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(predecessor_id)).await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut tail =
        SseSubscription::open(fixture.url(), admin, &subs::log_tail(predecessor_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    let trigger = instance.next_trigger(LONG).await;
    let failed_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    instance.declare_plan(&trigger, &["convert"]).await;
    instance.start_step(&trigger, 0, "convert").await;
    instance
        .log_line(&trigger, Some(0), "ERROR", "unsupported format")
        .await;
    delta::log_line(
        &stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await[0],
        failed_run,
        Some(0),
    );
    instance
        .fail_run(&trigger, "PERMANENT", "unsupported_format", None)
        .await;

    let run_failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    assert_eq!(
        delta::event_of(&run_failed, wire::EVT_RUN_FAILED, predecessor_id)["reasonCode"],
        json!("unsupported_format")
    );
    let failed_delta =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    delta::projection(&failed_delta, predecessor_id, "FAILED");
    delta::assert_failed_affordances(&failed_delta);
    assert_eq!(
        events
            .expect_one(wire::FACT_FAILED, predecessor_id, LONG)
            .await
            .payload()["failure_report"]["reason_code"],
        json!("unsupported_format")
    );

    // When: the owner also declares the failure it just received
    producer.fail(predecessor_id, None).await;
    events
        .expect_exactly(wire::FACT_FAILED, predecessor_id, 1, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_FAILED, QUIET).await;
    instance.expect_no_trigger(QUIET).await;

    let failed_view = gql::job_view(&client, admin, predecessor_id).await;
    assert_eq!(
        failed_view["job"]["triggeredBy"],
        json!({ "id": operator_id.to_string(), "displayName": "Amelie" }),
        "the user the work runs on behalf of is named as the declaration named them — an \
         administrator reading a failed job sees a person, not an opaque id: {failed_view}",
    );
    let failed_resolution_id = uuid_at(&failed_view["job"]["resolution"]["id"]);
    assert_eq!(
        failed_view["job"]["resolution"]["causedByRunId"],
        json!(failed_run.to_string())
    );
    delta::assert_failed_affordances(&failed_view);

    // When: the client-minted ids are offered in a shape this domain never mints
    let mut malformed_refusals = Vec::new();
    for (intervention, successor, what) in [
        (
            Uuid::new_v4(),
            Uuid::now_v7(),
            "an intervention id that is not a UUIDv7",
        ),
        (
            Uuid::now_v7(),
            Uuid::new_v4(),
            "a successor job id that is not a UUIDv7",
        ),
    ] {
        let refused = gql::manual_retry_job(
            &client,
            admin,
            intervention,
            predecessor_id,
            successor,
            failed_resolution_id,
        )
        .await;
        malformed_refusals.push((what, verdict::expect_code_shaped(&refused, what)));
        events
            .expect_none(wire::FACT_QUEUED, successor, QUIET)
            .await;
        durable
            .assert_count(
                db::JOBS_WITH_ID,
                successor,
                0,
                "a refused intervention creates no successor job — an id the service forwarded \
                 unchecked would surface as a database constraint violation instead of a stable \
                 code the caller can branch on",
            )
            .await;
    }
    instance.expect_no_trigger(QUIET).await;
    watch
        .expect_silence("a refused intervention pushes nothing", QUIET)
        .await;
    let untouched = gql::job_view(&client, admin, predecessor_id).await;
    assert!(
        untouched["job"]["manualRetry"].is_null(),
        "a refused intervention records nothing: {untouched}",
    );
    gql::assert_allowed(&untouched, wire::ACTION_MANUAL_RETRY);

    // When: the administrator retries it as a fresh successor job
    let intervention_id = Uuid::now_v7();
    let successor_id = Uuid::now_v7();
    let ack = gql::manual_retry_job(
        &client,
        admin,
        intervention_id,
        predecessor_id,
        successor_id,
        failed_resolution_id,
    )
    .await;
    verdict::expect_ack(&ack, "an administrator retries an eligible failed job");
    assert_eq!(
        ack["data"]["jobsManualRetryJob"],
        json!({ "success": true })
    );

    let intervention = stream::await_delta(
        &mut watch,
        JOB_CHANGED,
        wire::EVT_MANUAL_RETRY_STARTED,
        LONG,
    )
    .await;
    let recorded = delta::event_of(
        &intervention,
        wire::EVT_MANUAL_RETRY_STARTED,
        predecessor_id,
    );
    assert_eq!(recorded["successorJobId"], json!(successor_id.to_string()));
    assert_eq!(
        recorded["manualRetryId"],
        json!(intervention_id.to_string())
    );
    assert!(
        !recorded["runId"].is_null(),
        "the successor's first run is dispatched with the \
         intervention, even though the predecessor exhausted its budget: {intervention}"
    );
    let still_failed = delta::projection(&intervention, predecessor_id, "FAILED");
    assert_eq!(
        still_failed["manualRetry"]["successorJobId"],
        json!(successor_id.to_string())
    );
    gql::assert_blocked(&intervention, wire::ACTION_CANCEL);
    gql::assert_blocked(&intervention, wire::ACTION_MANUAL_RETRY);
    gql::assert_blocked(&intervention, wire::ACTION_DELETE);

    // When: the administrator tries the deletion that affordance just refused
    let chained = gql::delete_job(&client, admin, predecessor_id).await;
    let chained_code = verdict::expect_code_shaped(
        &chained,
        "deleting a predecessor whose manual-retry successor is still non-terminal",
    );
    let non_terminal = gql::delete_job(&client, admin, successor_id).await;
    let non_terminal_code =
        verdict::expect_code_shaped(&non_terminal, "deleting a non-terminal successor");
    codes::assert_pairwise_distinct(&[
        (
            "deleting a predecessor whose successor is still running",
            chained_code,
        ),
        ("deleting a non-terminal job", non_terminal_code),
    ]);
    watch
        .expect_silence("a refused deletion pushes nothing", QUIET)
        .await;
    durable
        .assert_count(
            db::DELETIONS_OF_JOB,
            predecessor_id,
            0,
            "the predecessor is terminal, so the ordinary 'not terminal yet' guard does not cover \
             it: a deletion accepted here breaks the audited retry chain and orphans the \
             successor's link back",
        )
        .await;
    let kept = gql::job_view(&client, admin, predecessor_id).await;
    assert_eq!(kept["job"]["isDeleted"], json!(false));
    assert!(kept["job"]["deletion"].is_null());
    gql::assert_blocked(&kept, wire::ACTION_DELETE);

    // Then: exactly one successor, at attempt one, followed live
    events
        .expect_one(wire::FACT_QUEUED, successor_id, LONG)
        .await;
    let successor_trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&successor_trigger), successor_id);
    assert_eq!(runner::attempt_number(&successor_trigger), 1);
    let successor_run = runner::run_id(&successor_trigger);
    assert_eq!(recorded["runId"], json!(successor_run.to_string()));

    let mut successor_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(successor_id)).await;
    let successor_snapshot = stream::snapshot(&mut successor_watch, JOB_CHANGED, SHORT).await;
    assert_eq!(
        successor_snapshot["job"]["runs"].as_array().map(Vec::len),
        Some(1),
        "the successor opens with its dispatched first run: {successor_snapshot}",
    );
    delta::assert_active_affordances(&successor_snapshot);
    instance.start_run(&successor_trigger).await;
    let successor_started = stream::await_delta(
        &mut successor_watch,
        JOB_CHANGED,
        wire::EVT_RUN_STARTED,
        LONG,
    )
    .await;
    delta::projection(&successor_started, successor_id, "IN_PROGRESS");
    delta::assert_active_affordances(&successor_started);
    delta::assert_fleet(
        &stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await,
        &runner_type,
        0,
        1,
        1,
        0,
    );
    instance.start_step(&successor_trigger, 0, "convert").await;
    let successor_stepped = stream::await_delta(
        &mut successor_watch,
        JOB_CHANGED,
        wire::EVT_STEP_STARTED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(&successor_stepped, wire::EVT_STEP_STARTED, successor_id)["stepIndex"],
        json!(0)
    );
    let mut successor_tail =
        SseSubscription::open(fixture.url(), admin, &subs::log_tail(successor_id)).await;
    stream::snapshot(&mut successor_tail, LOG_TAIL, SHORT).await;
    instance
        .log_line(&successor_trigger, Some(0), "INFO", "converting again")
        .await;
    delta::log_line(
        &stream::drain_appended(&mut successor_tail, LOG_TAIL, 1, LONG).await[0],
        successor_run,
        Some(0),
    );
    instance.complete_run(&successor_trigger).await;
    let successor_run_done = stream::await_delta(
        &mut successor_watch,
        JOB_CHANGED,
        wire::EVT_RUN_COMPLETED,
        LONG,
    )
    .await;
    delta::projection(&successor_run_done, successor_id, "IN_PROGRESS");
    delta::assert_fleet(
        &stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_EXECUTING, LONG).await,
        &runner_type,
        0,
        0,
        0,
        1,
    );

    let predecessor_row = stream::await_delta(
        &mut listing,
        JOBS_CHANGED,
        wire::EVT_MANUAL_RETRY_STARTED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::upserted(&predecessor_row, predecessor_id)["job"]["successorJobId"],
        json!(successor_id.to_string()),
        "the list window carries the predecessor's new link, not a refetch hint",
    );
    let predecessor = gql::job(&client, admin, predecessor_id).await;
    let successor = gql::job(&client, admin, successor_id).await;
    for (field, expected, what) in [
        (
            "config",
            json!({ "prompt": "convert" }),
            "the runner configuration",
        ),
        ("runnerType", json!(&runner_type), "the runner type"),
        (
            "maxAttempts",
            json!(1),
            "the producer's own attempt ceiling — a successor that lost it would silently inherit \
             the service ceiling and widen the retry policy nobody asked to widen",
        ),
    ] {
        assert_eq!(
            successor[field], expected,
            "the successor copies {what}: {successor}",
        );
    }
    assert_eq!(
        successor["triggeredBy"]["id"],
        json!(operator_id.to_string()),
        "the successor copies whose behalf the work runs on, or the audit trail breaks: \
         {successor}",
    );
    assert_eq!(
        successor["source"],
        json!({ "bc": "projects", "entityId": entity_id.to_string() }),
        "the successor copies the source reference: {successor}",
    );
    assert_eq!(
        successor["parentJobId"], predecessor["parentJobId"],
        "the successor keeps the predecessor's place in the tree, so a cancellation of the \
         ancestor still reaches it: {successor}",
    );
    let listed = gql::list_jobs(&client, admin, json!({ "runnerTypes": [&runner_type] })).await;
    let successor_row = listed
        .iter()
        .find(|row| row["job"]["id"] == json!(successor_id.to_string()))
        .unwrap_or_else(|| panic!("the successor is listed among its type's jobs: {listed:?}"));
    assert_eq!(
        successor_row["job"]["predecessorJobId"],
        json!(predecessor_id.to_string()),
        "the chain of manual retries reads from either end: {successor_row}",
    );
    let by_source = gql::job_by_source(&client, admin, "projects", entity_id).await;
    assert_eq!(
        by_source["job"]["id"],
        json!(successor_id.to_string()),
        "the source lookup answers with the single non-terminal job, successor included",
    );

    // When: the successor's owner finishes it, the predecessor's affordances change alone
    producer.finish(successor_id).await;
    let successor_completed = stream::await_delta(
        &mut successor_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        LONG,
    )
    .await;
    delta::projection(&successor_completed, successor_id, "COMPLETED");
    events
        .expect_one(wire::FACT_COMPLETED, successor_id, LONG)
        .await;

    let reopened =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_AFFORDANCES_CHANGED, LONG).await;
    assert_eq!(
        delta::event_of(&reopened, wire::EVT_AFFORDANCES_CHANGED, predecessor_id)["causedByJobId"],
        json!(successor_id.to_string()),
        "when only the affordances move, the client is told so by its own event",
    );
    delta::projection(&reopened, predecessor_id, "FAILED");
    gql::assert_allowed(&reopened, wire::ACTION_DELETE);
    gql::assert_blocked(&reopened, wire::ACTION_MANUAL_RETRY);
    instance.expect_no_trigger(QUIET).await;

    // Then: every conflicting reuse is refused without a trace
    let absorbed = gql::manual_retry_job(
        &client,
        admin,
        intervention_id,
        predecessor_id,
        successor_id,
        failed_resolution_id,
    )
    .await;
    verdict::expect_ack(&absorbed, "an identical redelivery of the intervention");

    let mut state_refusals = Vec::new();
    for (intervention, candidate, resolution, what) in [
        (
            intervention_id,
            Uuid::now_v7(),
            failed_resolution_id,
            "reusing an intervention id for another successor",
        ),
        (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            "a stale failed resolution",
        ),
        (
            Uuid::now_v7(),
            Uuid::now_v7(),
            failed_resolution_id,
            "a second manual retry of the same failed resolution",
        ),
        (
            Uuid::now_v7(),
            predecessor_id,
            failed_resolution_id,
            "a successor reusing the predecessor's job id",
        ),
    ] {
        let refused = gql::manual_retry_job(
            &client,
            admin,
            intervention,
            predecessor_id,
            candidate,
            resolution,
        )
        .await;
        state_refusals.push(verdict::expect_code_shaped(&refused, what));
        assert_eq!(
            gql::job(&client, admin, predecessor_id).await["manualRetry"]["id"],
            json!(intervention_id.to_string()),
            "{what} leaves the recorded intervention untouched",
        );
    }
    for (what, code) in &malformed_refusals {
        assert!(
            !state_refusals.contains(code),
            "{what} must answer its own code: a malformed id answered by a state-conflict code \
             tells the caller to fix the wrong thing — {code} is already how a conflicting reuse \
             is refused",
        );
    }
    watch
        .expect_silence("a refused intervention pushes nothing", QUIET)
        .await;
    instance.expect_no_trigger(QUIET).await;

    durable
        .assert_all(
            predecessor_id,
            &[
                (db::RUNS_OF_JOB, 1, "no same-job retry run"),
                (db::RESOLUTIONS_OF_JOB, 1, "one immutable failed resolution"),
            ],
        )
        .await;
    durable
        .assert_all(
            successor_id,
            &[
                (db::JOBS_WITH_ID, 1, "one successor job"),
                (db::RUNS_OF_JOB, 1, "exactly one first run on the successor"),
            ],
        )
        .await;

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
