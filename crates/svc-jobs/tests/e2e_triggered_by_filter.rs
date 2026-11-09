mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::{JOBS_CHANGED, LONG, QUIET, SHORT, delta, gql, stream, subs, wire};
use uuid::Uuid;

#[tokio::test]
async fn an_administrator_lists_and_watches_the_jobs_that_run_for_given_users() {
    // Given: a runner type nobody serves, so every job declared for it waits,
    // and an administrator already watching the jobs that run for Alice —
    // all of them, and the waiting ones only
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("triggered_by");
    let producer = Producer::new(fixture.fabric(), "projects");
    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();
    let carol = Uuid::now_v7();

    let mut for_alice = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_filtered(&format!(
            r#"runnerTypes: ["{runner_type}"], triggeredBy: ["{alice}"]"#
        )),
    )
    .await;
    let opening = stream::snapshot(&mut for_alice, JOBS_CHANGED, SHORT).await;
    gql::assert_lists_exactly(
        &opening["jobs"],
        &[],
        "a window over a user with no job yet",
    );
    let mut for_alice_waiting = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_filtered(&format!(
            r#"runnerTypes: ["{runner_type}"], statuses: [PENDING], triggeredBy: ["{alice}"]"#
        )),
    )
    .await;
    stream::snapshot(&mut for_alice_waiting, JOBS_CHANGED, SHORT).await;

    // When: the producer declares a job that runs for nobody, one that runs for
    // Bob, then two that run for Alice
    let for_nobody = declare(&producer, &events, JobDeclaration::new(&runner_type)).await;
    let for_bob = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type).triggered_by_user(bob),
    )
    .await;
    let first_for_alice = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type).triggered_by_user(alice),
    )
    .await;

    // Then: the first thing Alice's window receives is Alice's job — the jobs of
    // Bob and of nobody, declared before it, never entered the window
    let entered = for_alice.expect_event_on(JOBS_CHANGED, LONG).await;
    delta::event_of(&entered, wire::EVT_QUEUED, first_for_alice);
    delta::assert_upserted_summary(&entered, first_for_alice, "PENDING");
    let second_for_alice = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type).triggered_by_user(alice),
    )
    .await;
    let entered = stream::await_delta_of(
        &mut for_alice,
        JOBS_CHANGED,
        wire::EVT_QUEUED,
        second_for_alice,
        LONG,
    )
    .await;
    delta::assert_upserted_summary(&entered, second_for_alice, "PENDING");
    for outsider in [for_nobody, for_bob] {
        stream::expect_no_delta_of(
            &mut for_alice,
            JOBS_CHANGED,
            wire::EVT_QUEUED,
            outsider,
            QUIET,
        )
        .await;
    }

    // Then: the list answers with exactly the jobs that run for the users named
    let alices = vec![first_for_alice, second_for_alice];
    for (filter, expected, what) in [
        (
            json!({ "runnerTypes": [&runner_type], "triggeredBy": [alice] }),
            alices.clone(),
            "filtering by the user two jobs run for",
        ),
        (
            json!({ "runnerTypes": [&runner_type], "triggeredBy": [alice, bob] }),
            vec![first_for_alice, second_for_alice, for_bob],
            "filtering by either of two users",
        ),
        (
            json!({ "runnerTypes": [&runner_type], "triggeredBy": [carol] }),
            vec![],
            "filtering by a user no job runs for",
        ),
        (
            json!({ "runnerTypes": [&runner_type], "statuses": ["PENDING"], "triggeredBy": [bob] }),
            vec![for_bob],
            "filtering by a user and the status the job holds",
        ),
        (
            json!({ "runnerTypes": [&runner_type], "statuses": ["FAILED"], "triggeredBy": [alice] }),
            vec![],
            "filtering by a user and a status none of the user's jobs holds",
        ),
        (
            json!({ "runnerTypes": [&runner_type] }),
            vec![first_for_alice, second_for_alice, for_bob, for_nobody],
            "no user filter at all",
        ),
    ] {
        gql::assert_lists_exactly(
            &gql::list_page(&client, admin, filter, 50, None).await,
            &expected,
            what,
        );
    }
    gql::assert_lists_none_of(
        &gql::list_page(
            &client,
            admin,
            json!({ "runnerTypes": [&runner_type], "triggeredBy": [alice, bob, carol] }),
            50,
            None,
        )
        .await,
        &[for_nobody],
        "a job that runs for nobody never matches a user filter",
    );

    // Then: a window opened now starts on exactly the jobs of the users named
    let mut for_both = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_filtered(&format!(
            r#"runnerTypes: ["{runner_type}"], statuses: [PENDING], triggeredBy: ["{alice}", "{bob}"]"#
        )),
    )
    .await;
    let both_opening = stream::snapshot(&mut for_both, JOBS_CHANGED, SHORT).await;
    gql::assert_lists_exactly(
        &both_opening["jobs"],
        &[first_for_alice, second_for_alice, for_bob],
        "the snapshot of a window over two users",
    );

    // When: an administrator cancels one of Alice's jobs, Bob's job and the job
    // that runs for nobody
    for job_id in [first_for_alice, for_bob, for_nobody] {
        gql::expect_success(
            &gql::cancel_job(&client, admin, Uuid::now_v7(), job_id).await,
            wire::FIELD_CANCEL_JOB,
            "an administrator cancels a waiting job",
        );
    }

    // Then: Alice's job leaves the window of her waiting jobs, stays in the
    // window of all her jobs as cancelled, and the other cancels reach neither
    let left = stream::await_delta_of(
        &mut for_alice_waiting,
        JOBS_CHANGED,
        wire::EVT_JOB_CANCELLED,
        first_for_alice,
        LONG,
    )
    .await;
    delta::assert_removes(&left, first_for_alice);
    let kept = stream::await_delta_of(
        &mut for_alice,
        JOBS_CHANGED,
        wire::EVT_JOB_CANCELLED,
        first_for_alice,
        LONG,
    )
    .await;
    delta::assert_upserted_summary(&kept, first_for_alice, "CANCELLED");
    for watch in [&mut for_alice, &mut for_alice_waiting] {
        for outsider in [for_bob, for_nobody] {
            stream::expect_no_delta_of(
                watch,
                JOBS_CHANGED,
                wire::EVT_JOB_CANCELLED,
                outsider,
                QUIET,
            )
            .await;
        }
    }
    let bob_left = stream::await_delta_of(
        &mut for_both,
        JOBS_CHANGED,
        wire::EVT_JOB_CANCELLED,
        for_bob,
        LONG,
    )
    .await;
    delta::assert_removes(&bob_left, for_bob);
    stream::expect_no_delta_of(
        &mut for_both,
        JOBS_CHANGED,
        wire::EVT_JOB_CANCELLED,
        for_nobody,
        QUIET,
    )
    .await;

    // Then: the read agrees with the windows, status and user combined
    for (filter, expected, what) in [
        (
            json!({ "runnerTypes": [&runner_type], "statuses": ["PENDING"], "triggeredBy": [alice] }),
            vec![second_for_alice],
            "Alice's waiting jobs after the cancel",
        ),
        (
            json!({ "runnerTypes": [&runner_type], "statuses": ["CANCELLED"], "triggeredBy": [alice] }),
            vec![first_for_alice],
            "Alice's cancelled jobs",
        ),
        (
            json!({ "runnerTypes": [&runner_type], "statuses": ["CANCELLED"], "triggeredBy": [alice, bob] }),
            vec![first_for_alice, for_bob],
            "the cancelled jobs of Alice and Bob, not the one that runs for nobody",
        ),
    ] {
        gql::assert_lists_exactly(
            &gql::list_page(&client, admin, filter, 50, None).await,
            &expected,
            what,
        );
    }

    events.stop().await;
    fixture.shutdown().await;
}

async fn declare(producer: &Producer<'_>, events: &EventLog, declaration: JobDeclaration) -> Uuid {
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    job_id
}
