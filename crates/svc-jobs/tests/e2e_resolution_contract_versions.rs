mod support;

use br_test_harness::SseSubscription;
use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::{JOB_CHANGED, LONG, QUIET, SHORT, delta, gql, stream, subs, wire};
use uuid::Uuid;

#[tokio::test]
async fn v2_resolution_commands_treat_the_actor_as_attribution_without_weakening_the_lifecycle() {
    // Given: three pending jobs declared by one producer and watched by an administrator
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let owner = Producer::new(fixture.fabric(), "projects");
    let declarant = Producer::new(fixture.fabric(), "not-the-owner");
    let runner_type = wire::unique_runner_type("resolution_contracts");

    let completion = JobDeclaration::new(&runner_type);
    let failure = JobDeclaration::new(&runner_type);
    let cancellation = JobDeclaration::new(&runner_type);
    for declaration in [&completion, &failure, &cancellation] {
        owner.declare(declaration).await;
        events
            .expect_exactly(wire::FACT_QUEUED, declaration.job_id, 1, LONG)
            .await;
    }

    let mut completion_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(completion.job_id)).await;
    let mut failure_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(failure.job_id)).await;
    let mut cancellation_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed(cancellation.job_id),
    )
    .await;
    stream::snapshot(&mut completion_watch, JOB_CHANGED, SHORT).await;
    stream::snapshot(&mut failure_watch, JOB_CHANGED, SHORT).await;
    stream::snapshot(&mut cancellation_watch, JOB_CHANGED, SHORT).await;

    // Given: the v1 consumer is demonstrably live, so a later silence proves refusal, not absence
    let v1_control = JobDeclaration::new(&runner_type);
    owner.declare(&v1_control).await;
    events
        .expect_exactly(wire::FACT_QUEUED, v1_control.job_id, 1, LONG)
        .await;
    owner.finish(v1_control.job_id).await;
    events
        .expect_exactly(wire::FACT_COMPLETED, v1_control.job_id, 1, LONG)
        .await;
    assert_eq!(
        gql::job(&client, admin, v1_control.job_id).await["status"],
        json!("COMPLETED"),
    );

    // When: the other producer sends the deprecated v1 resolution commands
    declarant.finish(completion.job_id).await;
    declarant
        .fail(failure.job_id, Some("v1 must still check ownership"))
        .await;
    declarant.cancel(cancellation.job_id).await;

    // Then: v1 preserves its compatibility guard and produces no state or event
    events
        .expect_none(wire::FACT_COMPLETED, completion.job_id, QUIET)
        .await;
    events
        .expect_none(wire::FACT_FAILED, failure.job_id, QUIET)
        .await;
    events
        .expect_none(wire::FACT_CANCELLED, cancellation.job_id, QUIET)
        .await;
    stream::expect_total_silence(&mut completion_watch, "a refused v1 finish command", QUIET).await;
    stream::expect_total_silence(&mut failure_watch, "a refused v1 fail command", QUIET).await;
    stream::expect_total_silence(
        &mut cancellation_watch,
        "a refused v1 cancel command",
        QUIET,
    )
    .await;
    for job_id in [completion.job_id, failure.job_id, cancellation.job_id] {
        let unchanged = gql::job_view(&client, admin, job_id).await;
        assert_eq!(unchanged["job"]["status"], json!("PENDING"));
        delta::assert_active_affordances(&unchanged);
    }

    // When: the same declarant sends each v2 envelope twice with one stable command id.
    // The v1 and v2 helpers build their bodies through the same payload constructors.
    let finish_command = Uuid::now_v7();
    let fail_command = Uuid::now_v7();
    let cancel_command = Uuid::now_v7();
    for _ in 0..2 {
        declarant
            .finish_v2_as(finish_command, completion.job_id)
            .await;
        declarant
            .fail_v2_as(
                fail_command,
                failure.job_id,
                Some("the owner convention is not authorization"),
            )
            .await;
        declarant
            .cancel_v2_as(cancel_command, cancellation.job_id)
            .await;
    }

    // Then: each redelivered command settles exactly once and retains actor attribution
    for (fact, job_id) in [
        (wire::FACT_COMPLETED, completion.job_id),
        (wire::FACT_FAILED, failure.job_id),
        (wire::FACT_CANCELLED, cancellation.job_id),
    ] {
        let mut observed = events.expect_exactly(fact, job_id, 1, LONG).await;
        assert_eq!(
            observed.remove(0).envelope["metadata"]["actor_id"],
            json!(declarant.account_id.to_string()),
            "the v2 command actor is retained as attribution on {fact}",
        );
    }
    let declared_failure = events.of(wire::FACT_FAILED, failure.job_id).remove(0);
    assert_eq!(
        declared_failure.payload()["failure_cause"],
        json!("DECLARED_BY_OWNER"),
    );
    assert_eq!(
        declared_failure.payload()["note"],
        json!("the owner convention is not authorization"),
    );

    let completed = stream::await_delta(
        &mut completion_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        LONG,
    )
    .await;
    let completed_event = delta::event_of(&completed, wire::EVT_JOB_COMPLETED, completion.job_id);
    let completion_resolution = completed_event["resolutionId"].clone();
    assert_uuid(&completion_resolution, "the completion resolution");
    delta::projection(&completed, completion.job_id, "COMPLETED");
    delta::assert_completed_affordances(&completed);

    let failed =
        stream::await_delta(&mut failure_watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    let failed_event = delta::event_of(&failed, wire::EVT_JOB_FAILED, failure.job_id);
    let failure_resolution = failed_event["resolutionId"].clone();
    assert_uuid(&failure_resolution, "the failure resolution");
    assert_eq!(failed_event["failureCause"], json!("DECLARED_BY_OWNER"));
    delta::projection(&failed, failure.job_id, "FAILED");
    delta::assert_failed_affordances(&failed);

    let cancelled = stream::await_delta(
        &mut cancellation_watch,
        JOB_CHANGED,
        wire::EVT_JOB_CANCELLED,
        LONG,
    )
    .await;
    let cancelled_event = delta::event_of(&cancelled, wire::EVT_JOB_CANCELLED, cancellation.job_id);
    let cancellation_resolution = cancelled_event["resolutionId"].clone();
    assert_uuid(&cancellation_resolution, "the cancellation resolution");
    delta::projection(&cancelled, cancellation.job_id, "CANCELLED");
    delta::assert_cancelled_affordances(&cancelled);

    let terminal = [
        (
            completion.job_id,
            "COMPLETED",
            &completion_resolution,
            wire::FACT_COMPLETED,
        ),
        (
            failure.job_id,
            "FAILED",
            &failure_resolution,
            wire::FACT_FAILED,
        ),
        (
            cancellation.job_id,
            "CANCELLED",
            &cancellation_resolution,
            wire::FACT_CANCELLED,
        ),
    ];
    for (job_id, status, resolution_id, _) in terminal {
        let view = gql::job_view(&client, admin, job_id).await;
        assert_eq!(view["job"]["status"], json!(status));
        assert_eq!(view["job"]["resolution"]["id"], *resolution_id);
        assert_eq!(view["job"]["resolution"]["kind"], json!(status));
        if status == "FAILED" {
            assert_eq!(
                view["job"]["resolution"]["failureCause"],
                json!("DECLARED_BY_OWNER"),
            );
        }
        durable
            .assert_count(
                db::RESOLUTIONS_OF_JOB,
                job_id,
                1,
                "a redelivered v2 command creates one terminal resolution",
            )
            .await;
    }

    // When: fresh v2 commands target the jobs after they became terminal
    let terminal_snapshots = terminal_views(&client, admin, &terminal).await;
    declarant.finish_v2(completion.job_id).await;
    declarant.fail_v2(failure.job_id, None).await;
    declarant.cancel_v2(cancellation.job_id).await;

    // Then: lifecycle immutability, not actor ownership, is the remaining guard
    assert_resolution_counts(&events, &terminal).await;
    expect_all_silent(
        &mut completion_watch,
        &mut failure_watch,
        &mut cancellation_watch,
        "fresh v2 commands against terminal jobs",
    )
    .await;
    assert_eq!(
        terminal_views(&client, admin, &terminal).await,
        terminal_snapshots,
        "fresh commands cannot replace terminal projections or resolution identities",
    );

    // When: an administrator soft-deletes those terminal audit records
    for (job_id, _, _, _) in terminal {
        let response = gql::delete_job(&client, admin, job_id).await;
        gql::expect_success(
            &response,
            wire::FIELD_DELETE_JOB,
            "an administrator soft-deletes a terminal job used by the v2 lifecycle oracle",
        );
    }
    for (watch, job_id, status) in [
        (&mut completion_watch, completion.job_id, "COMPLETED"),
        (&mut failure_watch, failure.job_id, "FAILED"),
        (&mut cancellation_watch, cancellation.job_id, "CANCELLED"),
    ] {
        let deleted = stream::await_delta(watch, JOB_CHANGED, wire::EVT_JOB_DELETED, LONG).await;
        assert_eq!(
            delta::projection(&deleted, job_id, status)["isDeleted"],
            json!(true),
        );
    }
    let deleted_snapshots = terminal_views(&client, admin, &terminal).await;
    for ((job_id, _, _, _), view) in terminal.into_iter().zip(&deleted_snapshots) {
        assert_eq!(view["job"]["isDeleted"], json!(true));
        durable
            .assert_count(
                db::DELETIONS_OF_JOB,
                job_id,
                1,
                "one soft-deletion audit row remains after the v2 lifecycle probe",
            )
            .await;
    }

    // When: fresh v2 commands target the now soft-deleted jobs
    declarant.finish_v2(completion.job_id).await;
    declarant.fail_v2(failure.job_id, None).await;
    declarant.cancel_v2(cancellation.job_id).await;

    // Then: no command mutates the retained audit state or emits another resolution
    assert_resolution_counts(&events, &terminal).await;
    expect_all_silent(
        &mut completion_watch,
        &mut failure_watch,
        &mut cancellation_watch,
        "fresh v2 commands against soft-deleted jobs",
    )
    .await;
    assert_eq!(
        terminal_views(&client, admin, &terminal).await,
        deleted_snapshots,
        "soft-deleted jobs retain their exact terminal audit state",
    );
    for (job_id, _, _, _) in terminal {
        durable
            .assert_count(
                db::RESOLUTIONS_OF_JOB,
                job_id,
                1,
                "terminal and deleted v2 commands never append a resolution",
            )
            .await;
    }

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

type Terminal<'a> = [(Uuid, &'a str, &'a Value, &'a str); 3];

fn assert_uuid(value: &Value, what: &str) {
    assert!(
        value
            .as_str()
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .is_some(),
        "{what} must be a UUID: {value}",
    );
}

async fn terminal_views(
    client: &br_test_harness::GraphqlClient,
    admin: &br_core_auth::Passport,
    terminal: &Terminal<'_>,
) -> Vec<Value> {
    let mut views = Vec::new();
    for (job_id, _, _, _) in terminal {
        views.push(gql::job_view(client, admin, *job_id).await);
    }
    views
}

async fn assert_resolution_counts(events: &EventLog, terminal: &Terminal<'_>) {
    for (job_id, _, _, fact) in terminal {
        events.expect_exactly(fact, *job_id, 1, QUIET).await;
    }
}

async fn expect_all_silent(
    completion: &mut SseSubscription,
    failure: &mut SseSubscription,
    cancellation: &mut SseSubscription,
    what: &str,
) {
    stream::expect_total_silence(completion, what, QUIET).await;
    stream::expect_total_silence(failure, what, QUIET).await;
    stream::expect_total_silence(cancellation, what, QUIET).await;
}
