mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{JOB_CHANGED, LONG, SHORT, delta, dispatch, gql, priority, stream, subs, wire};
use support::{claim, clock};

const CLAIM_WAIT: Duration = Duration::from_secs(4);

#[tokio::test]
async fn a_job_back_to_waiting_after_a_transient_failure_meets_the_users_current_cap() {
    let fixture = JobsFixture::start_with(Knobs {
        retry_base_delay_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let user = fixture::ordinary_member();
    let runner_type = wire::unique_runner_type("retried");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(4);
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user.actor_id(), 4).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps the user at 4",
    );
    let capped = JobDeclaration::new(&runner_type)
        .triggered_by_user(user.actor_id())
        .with_priority(5)
        .with_max_attempts(2);
    let forced = JobDeclaration::new(&runner_type)
        .triggered_by_user(user.actor_id())
        .with_priority(5)
        .with_max_attempts(2);
    let mut watches = Vec::new();
    for declaration in [&capped, &forced] {
        producer.declare_v2(declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
        let mut watch = SseSubscription::open(
            fixture.url(),
            admin,
            &subs::job_changed_with_priority(declaration.job_id),
        )
        .await;
        stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
        watches.push(watch);
    }
    gql::expect_success(
        &priority::force_job_priority(&client, admin, forced.job_id, 5).await,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a super user forces one of the jobs to 5",
    );
    let mut runs = Vec::new();
    for _ in 0..2 {
        let run = claim::claim(fixture.nats(), &instance, CLAIM_WAIT)
            .await
            .expect_run("a slot claims one of the jobs");
        instance.start_run(&run).await;
        runs.push(run);
    }
    for declaration in [&capped, &forced] {
        events
            .expect_one(wire::FACT_STARTED, declaration.job_id, LONG)
            .await;
    }

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user lowers the cap while the jobs execute",
    );

    priority::assert_priorities(
        &priority::job(&client, admin, capped.job_id).await,
        5,
        4,
        None,
        "an executing job",
    );

    for run in &runs {
        instance
            .fail_run(run, "TRANSIENT", "provider_busy", None)
            .await;
    }
    for (declaration, watch) in [&capped, &forced].into_iter().zip(watches.iter_mut()) {
        let scheduled = stream::await_delta_of(
            watch,
            JOB_CHANGED,
            wire::EVT_RETRY_SCHEDULED,
            declaration.job_id,
            LONG,
        )
        .await;
        delta::event_of(&scheduled, wire::EVT_RETRY_SCHEDULED, declaration.job_id);
    }

    let rederived = br_test_harness::wait_until(LONG, || async {
        priority::job(&client, admin, capped.job_id).await["effectivePriority"] == json!(2)
    })
    .await;
    assert!(
        rederived,
        "a job back to waiting meets the user's current cap: {}",
        priority::job(&client, admin, capped.job_id).await
    );
    priority::assert_priorities(
        &priority::job(&client, admin, capped.job_id).await,
        5,
        2,
        None,
        "a cap lowered while the job ran applies at its retry",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, forced.job_id).await,
        5,
        5,
        Some(5),
        "a forced job keeps its force through a retry",
    );

    let both_waiting = br_test_harness::wait_until(LONG, || async {
        let mut due = true;
        for job_id in [capped.job_id, forced.job_id] {
            let next = gql::job(&client, admin, job_id).await["nextAttemptAt"].clone();
            if !next.is_null() && delta::instant(&next) > clock::now() {
                due = false;
            }
        }
        due && dispatch::runner_type_view(&client, admin, &runner_type).await["runnerType"]
            ["waitingJobCount"]
            == json!(2)
    })
    .await;
    assert!(
        both_waiting,
        "both retries fall due and both jobs wait again within {LONG:?}: {}",
        dispatch::runner_type_view(&client, admin, &runner_type).await
    );
    let mut retried = Vec::new();
    for _ in 0..2 {
        let run = claim::claim_until_served(
            fixture.nats(),
            &instance,
            CLAIM_WAIT,
            LONG,
            "a slot claims a retry",
        )
        .await;
        assert_eq!(runner::attempt_number(&run), 2);
        retried.push(runner::job_id(&run));
    }
    assert_eq!(retried, vec![forced.job_id, capped.job_id]);

    events.stop().await;
    fixture.shutdown().await;
}
