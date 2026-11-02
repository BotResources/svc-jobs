mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::subs_max_priority;
use support::{
    JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, gql, priority, stream, subs, wire,
};
use uuid::Uuid;

#[tokio::test]
async fn a_user_capped_at_three_is_admitted_at_three_whatever_they_request() {
    let fixture = JobsFixture::start_with(Knobs {
        default_max_priority: Some(5),
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let user = fixture::ordinary_member();
    let user_id = user.actor_id();
    let runner_type = wire::unique_runner_type("capped");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut own = SseSubscription::open(
        fixture.url(),
        &user,
        &subs_max_priority::my_max_priority_changed(),
    )
    .await;
    assert_eq!(
        priority::opening_max(&mut own, SHORT).await,
        5,
        "a user without a maximum of their own has the service default",
    );
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user_id, 3).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user at 3",
    );
    priority::await_max_change(&mut own, 3, LONG).await;
    assert_eq!(priority::my_max_priority(&client, &user).await, 3);

    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;

    let declaration = JobDeclaration::new(&runner_type)
        .triggered_by_user(user_id)
        .with_priority(5)
        .with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;

    let accepted = events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    assert_eq!(
        accepted.payload(),
        json!({ "job_id": job_id.to_string(), "runner_type": runner_type }),
        "job.queued.v1 stays as is in 0.4 — the priority is never published to producers",
    );
    events
        .expect_none(wire::FACT_CREATION_REJECTED, job_id, QUIET)
        .await;

    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    let row = delta::assert_upserted_summary(&queued, job_id, "PENDING");
    priority::assert_priorities(&row["job"], 5, 3, None, "the queued list row");
    gql::assert_allowed(&row, wire::ACTION_FORCE_PRIORITY);
    let view = priority::job_view(&client, admin, job_id).await;
    priority::assert_priorities(&view["job"], 5, 3, None, "the job detail");
    delta::assert_active_affordances(&view);
    gql::assert_allowed(&view, wire::ACTION_FORCE_PRIORITY);
    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(job_id),
    )
    .await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    priority::assert_priorities(&opening["job"], 5, 3, None, "the job stream's snapshot");
    priority::assert_priorities(
        &priority::listed(&client, admin, &runner_type).await[0]["job"],
        5,
        3,
        None,
        "the list read",
    );

    for level in [0, 6, -1] {
        let out_of_range = JobDeclaration::new(&runner_type)
            .triggered_by_user(user_id)
            .with_priority(level);
        let refused_id = out_of_range.job_id;
        producer.declare_v2(&out_of_range).await;

        let rejected = events
            .expect_one(wire::FACT_CREATION_REJECTED, refused_id, LONG)
            .await;
        assert_eq!(
            rejected.payload()["reason_code"],
            json!(wire::REASON_INVALID_PRIORITY),
            "priority {level} is outside 1..=5: {}",
            rejected.payload(),
        );
        events
            .expect_none(wire::FACT_QUEUED, refused_id, QUIET)
            .await;
        stream::expect_no_delta_of(
            &mut listing,
            JOBS_CHANGED,
            wire::EVT_QUEUED,
            refused_id,
            QUIET,
        )
        .await;
        durable
            .assert_count(
                db::JOBS_WITH_ID,
                refused_id,
                0,
                "a refused creation leaves no job",
            )
            .await;
    }

    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), job_id);
    instance.start_run(&trigger).await;
    instance
        .fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user_id, 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user lowers the cap to 2",
    );
    priority::await_max_change(&mut own, 2, LONG).await;
    priority::assert_priorities(
        &priority::job(&client, admin, job_id).await,
        5,
        3,
        None,
        "a terminal job is not waiting, so a cap change leaves it as admitted",
    );
    let failed = gql::job(&client, admin, job_id).await;
    let resolution_id = Uuid::parse_str(
        failed["resolution"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("a failed job carries its resolution: {failed}")),
    )
    .expect("a resolution id is a UUID");
    let successor_id = Uuid::now_v7();
    gql::expect_success(
        &gql::manual_retry_job(
            &client,
            admin,
            Uuid::now_v7(),
            job_id,
            successor_id,
            resolution_id,
        )
        .await,
        wire::FIELD_MANUAL_RETRY_JOB,
        "an administrator manually retries the failed job",
    );

    let successor_trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&successor_trigger), successor_id);
    priority::assert_priorities(
        &priority::job(&client, admin, successor_id).await,
        5,
        2,
        None,
        "a manual-retry successor copies the requested priority, then the cap applies again",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, job_id).await,
        5,
        3,
        None,
        "the predecessor keeps the priority it was admitted with",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
