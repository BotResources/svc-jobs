mod support;

use br_core_auth::{Passport, PassportHeader};
use br_test_harness::{GraphqlClient, SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, impersonating_admin, impersonating_member, machine_caller};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, delta, docs, gql,
    stream, subs, wire,
};
use uuid::Uuid;

#[tokio::test]
async fn platform_administrators_alone_can_observe_and_operate_jobs() {
    // Given: one job in flight and one already failed, so each action is attacked where
    // an administrator would be offered it
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let member = fixture.member();
    let runner_type = wire::unique_runner_type("authz");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let source_entity = Uuid::now_v7();
    let running = JobDeclaration::new(&runner_type).with_source("projects", source_entity);
    let running_id = running.job_id;
    producer.declare(&running).await;
    events.expect_one(wire::FACT_QUEUED, running_id, LONG).await;
    let trigger = instance.next_trigger(LONG).await;
    let running_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    gql::wait_for_status(&client, admin, running_id, "IN_PROGRESS", LONG).await;

    let failed = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let failed_id = failed.job_id;
    producer.declare(&failed).await;
    let failed_trigger = instance.next_trigger(LONG).await;
    instance.start_run(&failed_trigger).await;
    instance
        .fail_run(&failed_trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&client, admin, failed_id, "FAILED", LONG).await;

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(running_id)).await;
    let admin_opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    delta::assert_active_affordances(&admin_opening);
    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut tail = SseSubscription::open(fixture.url(), admin, &subs::log_tail(running_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // Then: the administrator sees everything, with state-dependent affordances
    delta::assert_active_affordances(&gql::job_view(&client, admin, running_id).await);
    delta::assert_failed_affordances(&gql::job_view(&client, admin, failed_id).await);
    let listed = gql::list_jobs(&client, admin, json!({ "runnerTypes": [&runner_type] })).await;
    let listed_running = gql::listed_row(&listed, running_id);
    delta::assert_active_affordances(&listed_running);
    gql::assert_affordances_well_formed(&listed_running, "the list row of an executing job");
    let listed_failed = gql::listed_row(&listed, failed_id);
    delta::assert_failed_affordances(&listed_failed);
    gql::assert_affordances_well_formed(&listed_failed, "the list row of a failed job");

    let by_source = gql::job_by_source(&client, admin, "projects", source_entity).await;
    assert_eq!(by_source["job"]["id"], json!(running_id.to_string()));
    delta::assert_active_affordances(&by_source);
    gql::assert_affordances_well_formed(&by_source, "the source lookup");

    let fleet = gql::fleet_of(&client, admin, &runner_type).await;
    assert_eq!(
        fleet.len(),
        1,
        "the fleet read answers for exactly the runner type it was asked about: {fleet:?}",
    );
    let live_type = delta::assert_fleet(&fleet[0], &runner_type, 0, 1, 1, 0);
    assert_eq!(
        live_type["isAvailable"],
        json!(true),
        "an instance has signalled presence, so the type serves work: {live_type}",
    );
    delta::assert_instance(
        &live_type,
        "instance-a",
        true,
        &[running_run],
        &instance.version,
    );
    gql::assert_affordances_well_formed(&fleet[0], "the fleet read");
    let admin_snapshot = gql::job_view(&client, admin, running_id).await;

    // When: an ordinary member reads anything at all
    let mut read_refusals = Vec::new();
    for (document, variables, what) in [
        (
            docs::JOB_DETAIL,
            json!({ "id": running_id.to_string() }),
            "the job detail",
        ),
        (
            docs::JOBS_LIST,
            json!({ "filter": Value::Null, "first": 50 }),
            "the job list",
        ),
        (
            docs::JOB_BY_SOURCE,
            json!({ "bc": "projects", "entityId": source_entity.to_string() }),
            "the source lookup",
        ),
        (
            docs::JOB_LOGS,
            json!({ "jobId": running_id.to_string(), "first": 50 }),
            "the log read",
        ),
        (
            docs::FLEET,
            json!({ "runnerType": runner_type.clone() }),
            "the fleet read",
        ),
    ] {
        let refused = client.query(member, document, variables).await;
        read_refusals.push(verdict::expect_code_shaped(&refused, what));
        assert_no_jobs_state_leaked(&refused, running_id, what);
    }

    let known = client
        .query(
            member,
            docs::JOB_DETAIL,
            json!({ "id": running_id.to_string() }),
        )
        .await;
    let unknown = client
        .query(
            member,
            docs::JOB_DETAIL,
            json!({ "id": Uuid::now_v7().to_string() }),
        )
        .await;
    assert_eq!(
        known, unknown,
        "the refusal must not reveal whether a supplied id exists",
    );

    for caller in [
        member.clone(),
        impersonating_member(admin),
        machine_caller(),
    ] {
        let refused = client
            .query(
                &caller,
                docs::JOB_DETAIL,
                json!({ "id": running_id.to_string() }),
            )
            .await;
        read_refusals.push(verdict::expect_code_shaped(
            &refused,
            "a caller who is not a platform administrator",
        ));
        assert_no_jobs_state_leaked(&refused, running_id, "a non-administrator read");
    }

    // When: an ordinary member tries to establish any of the four subscriptions
    for (what, document) in [
        ("the job stream", subs::job_changed(running_id)),
        ("the list stream", subs::jobs_changed(&runner_type)),
        ("the log tail", subs::log_tail(running_id)),
        ("the fleet stream", subs::fleet_changed(&runner_type)),
    ] {
        let (status, body) = client
            .post_raw(
                "/graphql",
                &[
                    ("X-Passport", &member.to_header()),
                    ("Accept", "text/event-stream"),
                ],
                json!({ "query": document }),
            )
            .await;
        read_refusals.push(subscription_refusal(status, &body, what));
        assert_no_jobs_state_leaked(&body, running_id, what);
    }

    // When: a caller presents no passport at all, or one the edge cannot decode
    let mut anonymous_refusals = Vec::new();
    for (forged, what) in [
        (None, "no passport at all"),
        (Some("not-a-passport"), "an undecodable passport"),
    ] {
        let mut headers: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = forged {
            headers.push(("X-Passport", value));
        }
        let (status, body) = client
            .post_raw(
                "/graphql",
                &headers,
                json!({
                    "query": docs::JOB_DETAIL,
                    "variables": { "id": running_id.to_string() },
                }),
            )
            .await;
        anonymous_refusals.push(unauthenticated_refusal(
            status,
            &body,
            &format!("a query carrying {what}"),
        ));
        assert_no_jobs_state_leaked(&body, running_id, "an unauthenticated query");

        headers.push(("Accept", "text/event-stream"));
        let (status, body) = client
            .post_raw(
                "/graphql",
                &headers,
                json!({ "query": subs::job_changed(running_id) }),
            )
            .await;
        anonymous_refusals.push(unauthenticated_refusal(
            status,
            &body,
            &format!("a subscription carrying {what}"),
        ));
        assert_no_jobs_state_leaked(&body, running_id, "an unauthenticated subscription");
    }
    let closed_door = anonymous_refusals[0].clone();
    for code in &anonymous_refusals {
        assert_eq!(
            code, &closed_door,
            "a missing passport and an undecodable one are the same closed door, under one stable \
             code — an edge that decodes leniently into an empty caller would answer differently: \
             {anonymous_refusals:?}",
        );
    }

    // When: an ordinary member attacks each mutation in the state that offers it
    let mut action_refusals = Vec::new();
    let cancel = gql::cancel_job(&client, member, Uuid::now_v7(), running_id).await;
    action_refusals.push(verdict::expect_code_shaped(
        &cancel,
        "an ordinary member cancelling a cancellable job",
    ));
    let retry = gql::manual_retry_job(
        &client,
        member,
        Uuid::now_v7(),
        failed_id,
        Uuid::now_v7(),
        resolution_of(&client, admin, failed_id).await,
    )
    .await;
    action_refusals.push(verdict::expect_code_shaped(
        &retry,
        "an ordinary member retrying a failed retryable job",
    ));
    let delete = gql::delete_job(&client, member, failed_id).await;
    action_refusals.push(verdict::expect_code_shaped(
        &delete,
        "an ordinary member deleting a terminal deletable job",
    ));

    // When: a bounded context that owns nothing here issues the deprecated v1 Owner-only commands
    let intruder = Producer::new(fixture.fabric(), "chat");
    intruder.finish(running_id).await;
    intruder.fail(running_id, None).await;
    intruder.cancel(running_id).await;

    // Then: one authorization code, shared by every refusal and distinct from a state refusal
    let authorization_code = action_refusals[0].clone();
    for code in read_refusals.iter().chain(action_refusals.iter()) {
        assert_eq!(
            code, &authorization_code,
            "every refusal on the user-facing surface shares one stable authorization code: \
             {read_refusals:?} / {action_refusals:?}",
        );
    }
    let state_refusal = verdict::expect_code_shaped(
        &gql::delete_job(&client, admin, running_id).await,
        "an administrator deleting a non-terminal job",
    );
    assert_ne!(
        state_refusal, authorization_code,
        "a refusal of state must never be mistaken for a refusal of authority — otherwise an \
         unguarded mutation passes this scenario the day the state allows it",
    );

    // Then: nothing moved on any channel, neither for the edge nor for the intruding context
    stream::expect_total_silence(
        &mut watch,
        "a refused caller reaches no job subscriber",
        QUIET,
    )
    .await;
    stream::expect_total_silence(
        &mut listing,
        "a refused caller reaches no list subscriber",
        QUIET,
    )
    .await;
    stream::expect_total_silence(
        &mut fleet_watch,
        "a refused caller moves no fleet count",
        QUIET,
    )
    .await;
    for refused in [
        wire::FACT_CANCELLED,
        wire::FACT_COMPLETED,
        wire::FACT_FAILED,
    ] {
        events.expect_none(refused, running_id, QUIET).await;
    }
    durable
        .assert_count(
            db::RESOLUTIONS_OF_JOB,
            running_id,
            0,
            "a context that is not the Owner resolves nothing through the deprecated v1 contract — \
             one accepted here would violate that compatibility guarantee",
        )
        .await;
    instance.expect_no_cancel_entry(running_run, QUIET).await;
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        gql::job_view(&client, admin, running_id).await,
        admin_snapshot,
        "a refused attempt leaves the administrator's snapshot and affordances identical",
    );

    // Then: the execution plane never noticed the user-facing boundary
    instance
        .log_line(&trigger, None, "INFO", "the plane keeps running")
        .await;
    delta::log_line(
        &stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await[0],
        running_run,
        None,
    );
    instance.complete_run(&trigger).await;
    let run_completed =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, LONG).await;
    delta::projection(&run_completed, running_id, "IN_PROGRESS");
    delta::assert_fleet(
        &stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_EXECUTING, LONG).await,
        &runner_type,
        0,
        0,
        0,
        1,
    );
    producer.finish(running_id).await;
    let completed =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_COMPLETED, LONG).await;
    delta::projection(&completed, running_id, "COMPLETED");
    delta::assert_completed_affordances(&delta::assert_upserted_summary(
        &stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_COMPLETED, LONG).await,
        running_id,
        "COMPLETED",
    ));
    events
        .expect_one(wire::FACT_COMPLETED, running_id, LONG)
        .await;

    // Then: an impersonated administrator is audited as the passport says, not as the operator
    let (impersonated, operator_id) = impersonating_admin();
    gql::expect_success(
        &gql::delete_job(&client, &impersonated, failed_id).await,
        wire::FIELD_DELETE_JOB,
        "a super administrator acting through an impersonated passport",
    );
    let audited = gql::job(&client, admin, failed_id).await;
    assert_eq!(
        audited["deletion"]["deletedBy"]["id"],
        json!(impersonated.actor_id().to_string()),
        "the recorded actor binds from the trusted passport itself",
    );
    assert_ne!(
        audited["deletion"]["deletedBy"]["id"],
        json!(operator_id.to_string()),
        "the impersonator behind the passport is never the recorded actor",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

async fn resolution_of(client: &GraphqlClient, admin: &Passport, job_id: Uuid) -> Uuid {
    let job = gql::job(client, admin, job_id).await;
    Uuid::parse_str(
        job["resolution"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("a terminal job carries its resolution id: {job}")),
    )
    .expect("a resolution id is a UUID")
}

fn unauthenticated_refusal(status: StatusCode, body: &Value, what: &str) -> String {
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "{what} must be closed at the trust boundary, never answered on a permissive default: \
         {body}"
    );
    let code = verdict::mutation_error_code(body).unwrap_or_else(|| {
        panic!("{what}: a refusal at the boundary still owes a structured code: {body}")
    });
    assert!(
        verdict::is_code_shaped(&code),
        "{what}: the refusal code must be a stable code, not prose: {code}"
    );
    code
}

fn subscription_refusal(status: StatusCode, body: &Value, what: &str) -> String {
    assert_ne!(
        status,
        StatusCode::OK,
        "{what} must be refused during establishment, never opened: {body}"
    );
    let code = verdict::mutation_error_code(body).unwrap_or_else(|| {
        panic!("{what}: a refused subscription still owes a structured code: {body}")
    });
    assert!(
        verdict::is_code_shaped(&code),
        "{what}: the refusal code must be a stable code, not prose: {code}"
    );
    code
}

fn assert_no_jobs_state_leaked(response: &Value, job_id: Uuid, what: &str) {
    let rendered = response.to_string();
    for secret in [job_id.to_string().as_str(), "IN_PROGRESS", "COMPLETED"] {
        assert!(
            !rendered.contains(secret),
            "{what} must not leak Jobs state to a caller who may not read it ({secret}): {response}"
        );
    }
    let data = &response["data"];
    assert!(
        data.is_null()
            || data
                .as_object()
                .is_some_and(|fields| fields.values().all(Value::is_null)),
        "{what} must return no Jobs data at all: {response}"
    );
}
