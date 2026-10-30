mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::*;
use support::runner;
use support::surface::{self, Agent};
use support::tree::{Pair, call_child, pair};
use support::{LONG, gql};
use uuid::Uuid;

#[tokio::test]
async fn a_restart_run_reads_the_results_addressed_to_every_run_it_acts_for() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair: Pair<'_> = pair(&fixture, "acts-for").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let first = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &first).await;
    let first_run = runner::run_id(&first);
    let speaker = Uuid::now_v7();
    register_same(&fixture, &pair, first_run, speaker).await;
    save_state(
        &fixture,
        &Work::of(&pair.parent, &first),
        json!({ "turn": 1 }),
    )
    .await;

    let one = call_child(&fixture, &mut pair, root.job_id, first_run).await;
    let two = call_child(&fixture, &mut pair, root.job_id, first_run).await;
    pair.child.start_applied(&fixture, &one).await;
    pair.child.start_applied(&fixture, &two).await;
    let (one_work, two_work) = (Work::of(&pair.child, &one), Work::of(&pair.child, &two));
    let caller = Uuid::now_v7();
    surface::register(
        fixture.nats(),
        &pair.child,
        runner::run_id(&one),
        &[Agent::with_inbox().with_id(caller)],
    )
    .await;
    upload_result(&fixture, &one_work, "first answer").await;
    upload_result(&fixture, &two_work, "second answer").await;
    let (first_result, second_result) = (
        result_for(&durable, one_work.job_id).await,
        result_for(&durable, two_work.job_id).await,
    );

    pair.parent.complete_run(&first).await;
    durable
        .await_count(FINISHED_OF_RUN, first_run, 1, "the speaker finishes")
        .await;
    let wake = gesture::Mail::new(caller, &[speaker]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &one_work, &wake).await,
        "jobsSendMail",
    );
    let second = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &second).await;
    let second_run = runner::run_id(&second);
    let second_work = Work::of(&pair.parent, &second);

    let read = read_result(&fixture, &second_work, first_result).await;
    assert_eq!(
        gesture::field(&read, "jobsReadResult")["content"]["message"],
        "first answer",
        "a restart Run reads what was addressed to the Run it restarts"
    );

    register_same(&fixture, &pair, second_run, speaker).await;
    save_state(&fixture, &second_work, json!({ "turn": 2 })).await;
    pair.parent.complete_run(&second).await;
    durable
        .await_count(FINISHED_OF_RUN, second_run, 1, "the speaker finishes again")
        .await;
    let again = gesture::Mail::new(caller, &[speaker]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &one_work, &again).await,
        "jobsSendMail",
    );
    let third = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &third).await;
    let third_work = Work::of(&pair.parent, &third);

    let far = read_result(&fixture, &third_work, second_result).await;
    assert_eq!(
        gesture::field(&far, "jobsReadResult")["content"]["message"],
        "second answer",
        "the right to read crosses two restarts"
    );
    durable
        .assert_count(RESTARTS_OF_RUN, first_run, 1, "the first restart")
        .await;
    durable
        .assert_count(RESTARTS_OF_RUN, second_run, 1, "the second restart")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_failing_restart_run_cancels_what_it_created_and_not_what_the_earlier_run_created() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair: Pair<'_> = pair(&fixture, "restart-run-end").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let first = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &first).await;
    let first_run = runner::run_id(&first);
    let speaker = Uuid::now_v7();
    register_same(&fixture, &pair, first_run, speaker).await;
    save_state(
        &fixture,
        &Work::of(&pair.parent, &first),
        json!({ "turn": 1 }),
    )
    .await;

    let earlier = call_child(&fixture, &mut pair, root.job_id, first_run).await;
    pair.child.start_applied(&fixture, &earlier).await;
    let earlier_work = Work::of(&pair.child, &earlier);
    let caller = Uuid::now_v7();
    surface::register(
        fixture.nats(),
        &pair.child,
        runner::run_id(&earlier),
        &[Agent::with_inbox().with_id(caller)],
    )
    .await;

    pair.parent.complete_run(&first).await;
    durable
        .await_count(FINISHED_OF_RUN, first_run, 1, "the speaker finishes")
        .await;
    let wake = gesture::Mail::new(caller, &[speaker]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &earlier_work, &wake).await,
        "jobsSendMail",
    );
    let second = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &second).await;
    let second_run = runner::run_id(&second);
    register_same(&fixture, &pair, second_run, speaker).await;

    let later = call_child(&fixture, &mut pair, runner::job_id(&second), second_run).await;
    pair.child.start_applied(&fixture, &later).await;

    pair.parent
        .fail_run(&second, "PERMANENT", "boom", None)
        .await;

    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        runner::job_id(&later),
        "CANCELLED",
        LONG,
    )
    .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), earlier_work.job_id).await,
        "IN_PROGRESS",
        "the failure of a Run that acts for earlier Runs does not cancel their Jobs"
    );
    durable
        .await_count(
            FINISHED_OF_RUN,
            first_run,
            1,
            "the speaker is back to FINISHED in its own Run",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
