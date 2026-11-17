//! Jobs 0.9 — the user surface over the Jobs that run for the caller: list,
//! live list, inspection, logs and cancel, for those Jobs and no other.

mod support;

use br_test_harness::{SseSubscription, wait_until};
use serde_json::json;
use support::events::EventLog;
use support::fixture::{JobsFixture, machine_caller};
use support::own_jobs::{self, MY_JOBS_CHANGED};
use support::producer::{JobDeclaration, Producer};
use support::refusal::subscription_refusal;
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, delta, gql, stream, wire};
use uuid::Uuid;

const NOT_FOUND: (&str, &str) = ("NOT_FOUND", "job_not_found");

#[tokio::test]
async fn a_user_follows_only_the_jobs_that_run_for_them() {
    // Given: a runner type nobody serves, so the jobs declared for it wait, a
    // runner type served by one instance, and Alice already watching her jobs
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let waiting_type = wire::unique_runner_type("own_waiting");
    let served_type = wire::unique_runner_type("own_served");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &served_type, "instance-a");
    instance.connect().await;
    let (alice_id, bob_id) = (Uuid::now_v7(), Uuid::now_v7());
    let alice = own_jobs::user(alice_id, "Alice");

    let mut watch =
        SseSubscription::open(fixture.url(), &alice, &own_jobs::my_jobs_changed(None)).await;
    let opening = stream::snapshot(&mut watch, MY_JOBS_CHANGED, SHORT).await;
    gql::assert_lists_exactly(&opening["jobs"], &[], "the list of a user no job runs for");

    // When: a job is declared for nobody, one for Bob, then two for Alice
    let for_nobody = declare(&producer, &events, JobDeclaration::new(&waiting_type)).await;
    let for_bob = declare(
        &producer,
        &events,
        JobDeclaration::new(&waiting_type).triggered_by_user(bob_id),
    )
    .await;
    let first = declare(
        &producer,
        &events,
        JobDeclaration::new(&waiting_type).triggered_by_user(alice_id),
    )
    .await;

    // Then: the first thing Alice's live list receives is her own job
    let entered = watch.expect_event_on(MY_JOBS_CHANGED, LONG).await;
    delta::event_of(&entered, wire::EVT_QUEUED, first);
    let row = delta::assert_upserted_summary(&entered, first, "PENDING");
    gql::assert_allowed(&row, wire::ACTION_CANCEL);
    let second = declare(
        &producer,
        &events,
        JobDeclaration::new(&waiting_type).triggered_by_user(alice_id),
    )
    .await;
    let entered =
        stream::await_delta_of(&mut watch, MY_JOBS_CHANGED, wire::EVT_QUEUED, second, LONG).await;
    delta::assert_upserted_summary(&entered, second, "PENDING");
    for outsider in [for_nobody, for_bob] {
        stream::expect_no_delta_of(
            &mut watch,
            MY_JOBS_CHANGED,
            wire::EVT_QUEUED,
            outsider,
            QUIET,
        )
        .await;
    }

    // Then: the list holds exactly her jobs, newest first, by page and by status
    let listed = own_jobs::rows(&own_jobs::my_jobs(&client, &alice, None, 50, None).await);
    assert_eq!(
        own_jobs::ids(&listed),
        own_jobs::strings(&[second, first]),
        "Alice's list holds her two jobs, newest first, and nothing of Bob or of nobody"
    );
    let page = own_jobs::my_jobs(&client, &alice, None, 1, None).await;
    assert_eq!(
        own_jobs::ids(&own_jobs::rows(&page)),
        own_jobs::strings(&[second])
    );
    assert_eq!(
        page["data"]["jobsMyJobs"]["pageInfo"]["hasNextPage"],
        json!(true)
    );
    let cursor = page["data"]["jobsMyJobs"]["pageInfo"]["endCursor"]
        .as_str()
        .expect("a page carries its end cursor")
        .to_owned();
    let next = own_jobs::my_jobs(&client, &alice, None, 1, Some(&cursor)).await;
    assert_eq!(
        own_jobs::ids(&own_jobs::rows(&next)),
        own_jobs::strings(&[first])
    );
    assert_eq!(
        next["data"]["jobsMyJobs"]["pageInfo"]["hasNextPage"],
        json!(false)
    );
    let failed_only = own_jobs::my_jobs(&client, &alice, Some(&["FAILED"]), 50, None).await;
    assert!(own_jobs::rows(&failed_only).is_empty());

    // When: a job of Alice fails on its only attempt
    let failing = JobDeclaration::new(&served_type)
        .triggered_by_user(alice_id)
        .with_max_attempts(1);
    let failed = failing.job_id;
    producer.declare(&failing).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_run(&trigger).await;
    instance
        .fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    let ended = stream::await_delta_of(
        &mut watch,
        MY_JOBS_CHANGED,
        wire::EVT_JOB_FAILED,
        failed,
        LONG,
    )
    .await;
    delta::assert_upserted_summary(&ended, failed, "FAILED");

    // Then: Alice inspects it — its Run and the failure report, and the
    // administrators' gestures shown as not hers
    let inspected = gql::data_of(
        &own_jobs::my_job(&client, &alice, failed).await,
        "jobsMyJob",
    );
    let job = &inspected["job"];
    assert_eq!(job["status"], json!("FAILED"));
    assert_eq!(
        job["resolution"]["failureCause"],
        json!("TERMINAL_RUN_FAILURE")
    );
    assert_eq!(
        job["runs"][0]["id"],
        json!(runner::run_id(&trigger).to_string())
    );
    assert_eq!(job["runs"][0]["failureReport"]["kind"], json!("PERMANENT"));
    assert_eq!(
        job["runs"][0]["failureReport"]["reasonCode"],
        json!("provider_refused")
    );
    assert_eq!(
        gql::assert_blocked(&inspected, wire::ACTION_CANCEL),
        "job_already_terminal"
    );
    assert_eq!(
        gql::assert_blocked(&inspected, wire::ACTION_MANUAL_RETRY),
        "not_platform_administrator"
    );
    assert_eq!(
        gql::assert_blocked(&inspected, wire::ACTION_DELETE),
        "not_platform_administrator"
    );
    assert_eq!(
        gql::assert_blocked(&inspected, wire::ACTION_FORCE_PRIORITY),
        "not_super_user"
    );
    let reaching_for_the_passport = client
        .query(
            &alice,
            "query($id: UUID!) { jobsMyJob(id: $id) { job { passport { subjectId } } } }",
            json!({ "id": failed.to_string() }),
        )
        .await;
    assert!(
        reaching_for_the_passport["errors"].is_array(),
        "the user's view of a job carries no Passport at all: {reaching_for_the_passport}"
    );

    // Then: Bob's job, the job of nobody and a job that does not exist are all
    // unknown to Alice, under one code
    for (job_id, what) in [
        (for_bob, "Bob's job"),
        (for_nobody, "a job that runs for nobody"),
        (Uuid::now_v7(), "a job that does not exist"),
    ] {
        let (code, reason) =
            own_jobs::refusal(&own_jobs::my_job(&client, &alice, job_id).await, what);
        assert_eq!((code.as_str(), reason.as_str()), NOT_FOUND, "{what}");
    }

    // When: an administrator soft-deletes Alice's failed job
    gql::expect_success(
        &gql::delete_job(&client, admin, failed).await,
        "jobsDeleteJob",
        "an administrator deletes a terminal job",
    );

    // Then: it leaves her live list and her list, and is unknown to her
    let left = stream::await_delta_of(
        &mut watch,
        MY_JOBS_CHANGED,
        wire::EVT_JOB_DELETED,
        failed,
        LONG,
    )
    .await;
    delta::assert_removes(&left, failed);
    let listed = own_jobs::rows(&own_jobs::my_jobs(&client, &alice, None, 50, None).await);
    assert_eq!(own_jobs::ids(&listed), own_jobs::strings(&[second, first]));
    let (code, reason) = own_jobs::refusal(
        &own_jobs::my_job(&client, &alice, failed).await,
        "a soft-deleted job",
    );
    assert_eq!((code.as_str(), reason.as_str()), NOT_FOUND);

    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_user_reads_the_logs_of_their_own_jobs_only() {
    // Given: a job of Alice and a job of Bob, both executing and logging
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("own_logs");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(2);
    instance.connect().await;
    let (alice_id, bob_id) = (Uuid::now_v7(), Uuid::now_v7());
    let alice = own_jobs::user(alice_id, "Alice");

    let for_alice = JobDeclaration::new(&runner_type).triggered_by_user(alice_id);
    let alice_job = for_alice.job_id;
    producer.declare(&for_alice).await;
    let alice_trigger = instance.next_trigger(LONG).await;
    let for_bob = JobDeclaration::new(&runner_type).triggered_by_user(bob_id);
    let bob_job = for_bob.job_id;
    producer.declare(&for_bob).await;
    let bob_trigger = instance.next_trigger(LONG).await;
    let (alice_trigger, bob_trigger) = if runner::job_id(&alice_trigger) == alice_job {
        (alice_trigger, bob_trigger)
    } else {
        (bob_trigger, alice_trigger)
    };
    instance.start_run(&alice_trigger).await;
    instance.start_run(&bob_trigger).await;

    // When: the runner logs three lines for Alice's job and one for Bob's
    for message in ["fetching the document", "parsing page 1", "parsing page 2"] {
        instance
            .log_line(&alice_trigger, None, "INFO", message)
            .await;
    }
    instance
        .log_line(&bob_trigger, None, "WARNING", "bob's secret line")
        .await;
    let landed = wait_until(LONG, || async {
        gql::logs_of(&client, admin, alice_job).await.len() == 3
            && gql::logs_of(&client, admin, bob_job).await.len() == 1
    })
    .await;
    assert!(landed, "the log lines reach Jobs");

    // Then: Alice reads her lines in order, by Job, by Run and by page
    let all = own_jobs::my_job_logs(&client, &alice, alice_job, None, 50, None).await;
    let messages: Vec<_> = gql::edges_of(&gql::data_of(&all, "jobsMyJobLogs"))
        .iter()
        .map(|edge| edge["node"]["message"].clone())
        .collect();
    assert_eq!(
        messages,
        vec![
            json!("fetching the document"),
            json!("parsing page 1"),
            json!("parsing page 2")
        ]
    );
    let of_run = own_jobs::my_job_logs(
        &client,
        &alice,
        alice_job,
        Some(runner::run_id(&alice_trigger)),
        50,
        None,
    )
    .await;
    assert_eq!(
        gql::edges_of(&gql::data_of(&of_run, "jobsMyJobLogs")).len(),
        3
    );
    let first_page = gql::data_of(
        &own_jobs::my_job_logs(&client, &alice, alice_job, None, 2, None).await,
        "jobsMyJobLogs",
    );
    assert_eq!(gql::edges_of(&first_page).len(), 2);
    let cursor = first_page["pageInfo"]["endCursor"]
        .as_str()
        .expect("a log page carries its end cursor")
        .to_owned();
    let rest = gql::data_of(
        &own_jobs::my_job_logs(&client, &alice, alice_job, None, 2, Some(&cursor)).await,
        "jobsMyJobLogs",
    );
    assert_eq!(
        gql::edges_of(&rest)
            .iter()
            .map(|edge| edge["node"]["message"].clone())
            .collect::<Vec<_>>(),
        vec![json!("parsing page 2")]
    );

    // Then: Bob's logs are unknown to Alice and none of his lines is given
    let refused = own_jobs::my_job_logs(&client, &alice, bob_job, None, 50, None).await;
    let (code, reason) = own_jobs::refusal(&refused, "the logs of Bob's job");
    assert_eq!((code.as_str(), reason.as_str()), NOT_FOUND);
    assert!(!refused.to_string().contains("bob's secret line"));

    fixture.shutdown().await;
}

#[tokio::test]
async fn a_user_cancels_their_own_job_and_nobody_elses() {
    // Given: a waiting job of Alice with a waiting child, a waiting job of Bob,
    // and Alice watching her jobs
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("own_cancel");
    let producer = Producer::new(fixture.fabric(), "projects");
    let (alice_id, bob_id) = (Uuid::now_v7(), Uuid::now_v7());
    let alice = own_jobs::user(alice_id, "Alice");
    let parent = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type).triggered_by_user(alice_id),
    )
    .await;
    let child = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type)
            .with_parent(parent)
            .triggered_by_user(alice_id),
    )
    .await;
    let for_bob = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type).triggered_by_user(bob_id),
    )
    .await;
    let mut watch =
        SseSubscription::open(fixture.url(), &alice, &own_jobs::my_jobs_changed(None)).await;
    let opening = stream::snapshot(&mut watch, MY_JOBS_CHANGED, SHORT).await;
    gql::assert_lists_exactly(&opening["jobs"], &[parent, child], "Alice's jobs");

    // Then: her view offers the cancel and shows the administrators' gestures
    // as not hers
    let view = gql::data_of(
        &own_jobs::my_job(&client, &alice, parent).await,
        "jobsMyJob",
    );
    gql::assert_allowed(&view, wire::ACTION_CANCEL);
    for (action, reason) in [
        (wire::ACTION_MANUAL_RETRY, "not_platform_administrator"),
        (wire::ACTION_DELETE, "not_platform_administrator"),
        (wire::ACTION_FORCE_PRIORITY, "not_super_user"),
    ] {
        assert_eq!(gql::assert_blocked(&view, action), reason, "{action}");
    }

    // When: Alice cancels Bob's job, then forces, retries and deletes her own
    let refused = gql::cancel_job(&client, &alice, Uuid::now_v7(), for_bob).await;
    assert_eq!(
        gql::mutation_error_reason(&refused, "Alice cancelling Bob's job"),
        "not_job_user"
    );
    assert_eq!(
        refused["errors"][0]["extensions"]["code"],
        json!("FORBIDDEN")
    );
    let forced = client
        .query(
            &alice,
            "mutation($input: JobsForceJobPriorityInput!) { jobsForceJobPriority(input: $input) { success } }",
            json!({ "input": { "jobId": parent.to_string(), "priority": 5 } }),
        )
        .await;
    br_test_harness::verdict::expect_rejected(&forced);
    for response in [
        gql::delete_job(&client, &alice, parent).await,
        gql::manual_retry_job(
            &client,
            &alice,
            Uuid::now_v7(),
            parent,
            Uuid::now_v7(),
            Uuid::now_v7(),
        )
        .await,
    ] {
        br_test_harness::verdict::expect_rejected(&response);
    }

    // Then: Bob's job is untouched
    stream::expect_no_delta_of(
        &mut watch,
        MY_JOBS_CHANGED,
        wire::EVT_JOB_CANCELLED,
        for_bob,
        QUIET,
    )
    .await;
    assert_eq!(gql::status_of(&client, admin, for_bob).await, "PENDING");
    let untouched = gql::data_of(
        &own_jobs::my_job(&client, &alice, parent).await,
        "jobsMyJob",
    );
    assert_eq!(untouched["job"]["forcedPriority"], json!(null));
    assert_eq!(untouched["job"]["status"], json!("PENDING"));

    // When: Alice cancels her own job
    gql::expect_success(
        &gql::cancel_job(&client, &alice, Uuid::now_v7(), parent).await,
        wire::FIELD_CANCEL_JOB,
        "Alice cancels her waiting job",
    );

    // Then: the job and its child are cancelled, in her live list too, and the
    // cancel now shows as not allowed because the job is terminal
    events.expect_one(wire::FACT_CANCELLED, parent, LONG).await;
    events.expect_one(wire::FACT_CANCELLED, child, LONG).await;
    for job_id in [parent, child] {
        let moved = stream::await_delta_of(
            &mut watch,
            MY_JOBS_CHANGED,
            wire::EVT_JOB_CANCELLED,
            job_id,
            LONG,
        )
        .await;
        let row = delta::assert_upserted_summary(&moved, job_id, "CANCELLED");
        assert_eq!(
            gql::assert_blocked(&row, wire::ACTION_CANCEL),
            "job_already_terminal"
        );
    }
    let view = gql::data_of(
        &own_jobs::my_job(&client, &alice, parent).await,
        "jobsMyJob",
    );
    assert_eq!(view["job"]["status"], json!("CANCELLED"));
    assert_eq!(
        gql::assert_blocked(&view, wire::ACTION_CANCEL),
        "job_already_terminal"
    );

    // Then: a super user still lists every job and cancels Bob's
    let everything = gql::list_page(
        &client,
        admin,
        json!({ "runnerTypes": [&runner_type] }),
        50,
        None,
    )
    .await;
    gql::assert_lists_exactly(
        &everything,
        &[parent, child, for_bob],
        "the super user's list",
    );
    gql::expect_success(
        &gql::cancel_job(&client, admin, Uuid::now_v7(), for_bob).await,
        wire::FIELD_CANCEL_JOB,
        "a super user cancels Bob's job",
    );
    events.expect_one(wire::FACT_CANCELLED, for_bob, LONG).await;

    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_deactivated_user_or_a_service_account_has_no_user_surface_on_jobs() {
    // Given: a waiting job that runs for a user who is now deactivated
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("own_refused");
    let producer = Producer::new(fixture.fabric(), "projects");
    let dana_id = Uuid::now_v7();
    let job_id = declare(
        &producer,
        &events,
        JobDeclaration::new(&runner_type).triggered_by_user(dana_id),
    )
    .await;

    for (passport, who) in [
        (own_jobs::deactivated_user(dana_id), "a deactivated user"),
        (machine_caller(), "a service account"),
    ] {
        // When: they list, watch, inspect, read the logs of and cancel it
        let attempts = [
            (
                "the list",
                own_jobs::my_jobs(&client, &passport, None, 50, None).await,
            ),
            (
                "the inspection",
                own_jobs::my_job(&client, &passport, job_id).await,
            ),
            (
                "the logs",
                own_jobs::my_job_logs(&client, &passport, job_id, None, 50, None).await,
            ),
            (
                "the cancel",
                gql::cancel_job(&client, &passport, Uuid::now_v7(), job_id).await,
            ),
        ];

        // Then: each is refused with the structured authorization error
        for (what, response) in attempts {
            let code =
                br_test_harness::verdict::expect_code_shaped(&response, &format!("{who}: {what}"));
            assert_eq!(code, "FORBIDDEN", "{who}: {what}: {response}");
            assert!(
                !response.to_string().contains(&job_id.to_string()),
                "{who}: {what} leaks nothing of the job: {response}"
            );
        }
        let (status, body) =
            own_jobs::raw_subscription(&client, &passport, &own_jobs::my_jobs_changed(None)).await;
        assert_eq!(
            subscription_refusal(status, &body, &format!("{who}: the live list")),
            "FORBIDDEN"
        );
    }

    // Then: nothing changed
    events
        .expect_none(wire::FACT_CANCELLED, job_id, QUIET)
        .await;
    assert_eq!(gql::status_of(&client, admin, job_id).await, "PENDING");

    events.stop().await;
    fixture.shutdown().await;
}

async fn declare(producer: &Producer<'_>, events: &EventLog, declaration: JobDeclaration) -> Uuid {
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    job_id
}
