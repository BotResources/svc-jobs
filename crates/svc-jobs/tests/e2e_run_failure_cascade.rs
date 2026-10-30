mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::lineage::{act, create, mail, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, retry_manually, save_state};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, dispatch, gql, wire};

async fn runner_of<'a>(fixture: &'a JobsFixture, label: &str, capacity: u32) -> FakeRunner<'a> {
    let mut runner = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(label),
        "instance-a",
    )
    .declaring_capacity(capacity);
    runner.connect().await;
    dispatch::await_live_instances(
        &fixture.gql(),
        fixture.admin(),
        &runner.runner_type,
        1,
        LONG,
    )
    .await;
    runner
}

#[tokio::test]
async fn a_failed_run_cancels_what_it_created_and_what_answers_to_it_but_not_what_was_already_over()
{
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let (mut x, mut y, mut z) = (
        runner_of(&fixture, "fail-x", 3).await,
        runner_of(&fixture, "fail-y", 1).await,
        runner_of(&fixture, "fail-z", 6).await,
    );
    let a = JobDeclaration::new(&x.runner_type).with_max_attempts(2);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (first, a_agent) = started_with_agent(&fixture, &mut x).await;
    let run1 = Work::of(&x, &first);
    let sync = json!({ "mode": "SYNC", "max_attempts": 1 });

    let executing = create(&fixture, &run1, &y.runner_type, a.job_id, sync.clone()).await;
    let executing_trigger = y.next_trigger(LONG).await;
    y.start_applied(&fixture, &executing_trigger).await;
    let waiting = create(&fixture, &run1, &y.runner_type, a.job_id, sync.clone()).await;
    y.expect_no_trigger(QUIET).await;

    let over = create(&fixture, &run1, &z.runner_type, a.job_id, sync.clone()).await;
    let (over_trigger, _) = started_with_agent(&fixture, &mut z).await;
    let over_work = Work::of(&z, &over_trigger);
    let below = create(&fixture, &over_work, &z.runner_type, over, sync.clone()).await;
    z.next_trigger(LONG).await;
    save_state(&fixture, &over_work, json!({ "turn": 1 })).await;
    z.complete_run(&over_trigger).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            over_work.run_id,
            1,
            "the agent of the finished child",
        )
        .await;
    act(&fixture, wire::VERB_FINISH, over, &run1).await;
    gql::wait_for_status(&client, admin, over, "COMPLETED", LONG).await;

    let restarted = create(&fixture, &run1, &z.runner_type, a.job_id, sync.clone()).await;
    let (restarted_trigger, restarted_agent) = started_with_agent(&fixture, &mut z).await;
    let restarted_work = Work::of(&z, &restarted_trigger);
    save_state(&fixture, &restarted_work, json!({ "turn": 2 })).await;
    z.complete_run(&restarted_trigger).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            restarted_work.run_id,
            1,
            "its agent finished",
        )
        .await;
    mail(&fixture, &run1, a_agent, restarted_agent).await;
    let restart_job = runner::job_id(&z.next_trigger(LONG).await);

    let failed = create(&fixture, &run1, &z.runner_type, a.job_id, sync.clone()).await;
    let failed_trigger = z.next_trigger(LONG).await;
    z.start_applied(&fixture, &failed_trigger).await;
    z.fail_run(&failed_trigger, "PERMANENT", "provider_refused", None)
        .await;
    let successor = retry_manually(&fixture, failed).await;
    z.next_trigger(LONG).await;

    x.fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    for cancelled in [executing, waiting, below, restarted, restart_job, successor] {
        gql::wait_for_status(&client, admin, cancelled, "CANCELLED", LONG).await;
    }
    y.await_cancel_entry(runner::run_id(&executing_trigger), LONG)
        .await;
    assert_eq!(
        gql::status_of(&client, admin, over).await,
        "COMPLETED",
        "a terminal Job stays as it is"
    );
    durable
        .await_count(
            db::AGENTS_OF_RUN,
            over_work.run_id,
            0,
            "its Runs leave the directory",
        )
        .await;

    let (retry, _) = started_with_agent(&fixture, &mut x).await;
    assert_eq!(runner::attempt_number(&retry), 2);
    let own = create(
        &fixture,
        &Work::of(&x, &retry),
        &y.runner_type,
        a.job_id,
        sync,
    )
    .await;
    y.next_trigger(LONG).await;
    assert_eq!(
        gql::status_of(&client, admin, own).await,
        "IN_PROGRESS",
        "a retry of A creates its own children"
    );
    act(&fixture, wire::VERB_CANCEL, own, &Work::of(&x, &retry)).await;
    answers.expect_one(wire::FACT_CANCELLED, own, LONG).await;
    for cancelled in [executing, waiting] {
        assert_eq!(
            answers.count_of(wire::FACT_CANCELLED, cancelled),
            1,
            "each is announced once as cancelled"
        );
    }
    answers.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
