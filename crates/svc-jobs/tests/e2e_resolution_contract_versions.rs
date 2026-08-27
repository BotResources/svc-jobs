mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::{JOB_CHANGED, LONG, QUIET, SHORT, delta, gql, stream, subs, wire};

#[tokio::test]
async fn v2_resolution_commands_treat_the_actor_as_attribution_while_v1_keeps_its_owner_guard() {
    // Given: three pending jobs declared by one producer and watched by an administrator
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
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
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
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

    // When: the same declarant sends the v2 commands with unchanged payload shapes
    declarant.finish_v2(completion.job_id).await;
    declarant
        .fail_v2(
            failure.job_id,
            Some("the owner convention is not authorization"),
        )
        .await;
    declarant.cancel_v2(cancellation.job_id).await;

    // Then: Jobs validates lifecycle only, publishes each fact, and retains the actor as attribution
    for (fact, job_id) in [
        (wire::FACT_COMPLETED, completion.job_id),
        (wire::FACT_FAILED, failure.job_id),
        (wire::FACT_CANCELLED, cancellation.job_id),
    ] {
        let observed = events.expect_one(fact, job_id, LONG).await;
        assert_eq!(
            observed.envelope["metadata"]["actor_id"],
            json!(declarant.account_id.to_string()),
            "the v2 command actor is retained as attribution on {fact}",
        );
    }

    let completed = stream::await_delta(
        &mut completion_watch,
        JOB_CHANGED,
        wire::EVT_JOB_COMPLETED,
        LONG,
    )
    .await;
    delta::projection(&completed, completion.job_id, "COMPLETED");
    delta::assert_completed_affordances(&completed);

    let failed =
        stream::await_delta(&mut failure_watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    delta::projection(&failed, failure.job_id, "FAILED");
    delta::assert_failed_affordances(&failed);

    let cancelled = stream::await_delta(
        &mut cancellation_watch,
        JOB_CHANGED,
        wire::EVT_JOB_CANCELLED,
        LONG,
    )
    .await;
    delta::projection(&cancelled, cancellation.job_id, "CANCELLED");
    delta::assert_cancelled_affordances(&cancelled);

    for (job_id, status) in [
        (completion.job_id, "COMPLETED"),
        (failure.job_id, "FAILED"),
        (cancellation.job_id, "CANCELLED"),
    ] {
        assert_eq!(
            gql::job(&client, admin, job_id).await["status"],
            json!(status),
        );
    }

    events.stop().await;
    fixture.shutdown().await;
}
