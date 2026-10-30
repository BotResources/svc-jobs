mod support;

use serde_json::{Value, json};
use support::call_notices::notices;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::lineage::{act, create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, register_same, result_for, save_state, upload_result};
use support::runner::FakeRunner;
use support::tree::Pair;
use support::{LONG, gql, wire};
use uuid::Uuid;

async fn read_all(fixture: &JobsFixture, runner: &FakeRunner<'_>, work: &Work, address: Uuid) {
    let items: Vec<Uuid> = gesture::items(
        &gesture::unread(&fixture.gql(), &runner_access(), work, Some(address)).await,
    )
    .iter()
    .filter_map(|item| item["itemId"].as_str().and_then(|raw| raw.parse().ok()))
    .collect();
    if !items.is_empty() {
        let marked = gesture::mark_read(fixture.nats(), runner, work.run_id, &items).await;
        assert_eq!(marked["accepted"], true, "{marked}");
    }
}

async fn restart_of(
    fixture: &JobsFixture,
    pair: &mut Pair<'_>,
    from: &Work,
    sender: Uuid,
    speaker: Uuid,
) -> (Value, Work) {
    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            from,
            &Mail::new(sender, &[speaker]),
        )
        .await,
        "jobsSendMail",
    );
    let trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(fixture, &trigger).await;
    let work = Work::of(&pair.parent, &trigger);
    register_same(fixture, pair, work.run_id, speaker).await;
    (trigger, work)
}

#[tokio::test]
async fn the_run_of_a_second_restart_acts_for_the_first_run_and_the_first_restart_run() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let mut pair = Pair {
        parent: live_runner(&fixture, "chain-x").await,
        child: live_runner(&fixture, "chain-y").await,
    };
    let a = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (first, _) = started_with_agent(&fixture, &mut pair.parent).await;
    let run1 = Work::of(&pair.parent, &first);
    let speaker = Uuid::now_v7();
    register_same(&fixture, &pair, run1.run_id, speaker).await;
    let sync = json!({ "mode": "SYNC" });
    let (j1, j1b) = (
        create(
            &fixture,
            &run1,
            &pair.child.runner_type,
            a.job_id,
            sync.clone(),
        )
        .await,
        create(
            &fixture,
            &run1,
            &pair.child.runner_type,
            a.job_id,
            sync.clone(),
        )
        .await,
    );
    let (j1_trigger, sender) = started_with_agent(&fixture, &mut pair.child).await;
    let j1_work = Work::of(&pair.child, &j1_trigger);
    pair.child.next_trigger(LONG).await;
    save_state(&fixture, &run1, json!({ "turn": 1 })).await;
    pair.parent.complete_run(&first).await;
    durable
        .await_count(FINISHED_OF_RUN, run1.run_id, 2, "the speaker finishes")
        .await;

    let (second, run2) = restart_of(&fixture, &mut pair, &j1_work, sender, speaker).await;
    let (j2, j2b) = (
        create(
            &fixture,
            &run2,
            &pair.child.runner_type,
            run2.job_id,
            sync.clone(),
        )
        .await,
        create(&fixture, &run2, &pair.child.runner_type, run2.job_id, sync).await,
    );
    let (j2_trigger, _) = started_with_agent(&fixture, &mut pair.child).await;
    pair.child.next_trigger(LONG).await;
    read_all(&fixture, &pair.parent, &run2, speaker).await;
    save_state(&fixture, &run2, json!({ "turn": 2 })).await;
    pair.parent.complete_run(&second).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            run2.run_id,
            1,
            "the speaker finishes again",
        )
        .await;

    let (_, run3) = restart_of(&fixture, &mut pair, &j1_work, sender, speaker).await;
    upload_result(&fixture, &j1_work, "first answer").await;
    pair.child.complete_run(&j1_trigger).await;
    upload_result(
        &fixture,
        &Work::of(&pair.child, &j2_trigger),
        "second answer",
    )
    .await;
    pair.child.complete_run(&j2_trigger).await;

    for job in [j1, j2] {
        act(&fixture, wire::VERB_FINISH, job, &run3).await;
        gql::wait_for_status(&client, fixture.admin(), job, "COMPLETED", LONG).await;
    }
    for job in [j1b, j2b] {
        act(&fixture, wire::VERB_CANCEL, job, &run3).await;
        gql::wait_for_status(&client, fixture.admin(), job, "CANCELLED", LONG).await;
    }
    for (job, answer) in [(j1, "first answer"), (j2, "second answer")] {
        let result = result_for(&durable, job).await;
        let read = support::restart::read_result(&fixture, &run3, result).await;
        assert_eq!(
            gesture::field(&read, "jobsReadResult")["content"]["message"],
            answer
        );
    }
    let seen = notices(&fixture, &run3, None).await;
    for job in [j1, j2] {
        assert!(
            seen.iter()
                .any(|item| item["jobId"] == job.to_string()
                    && item["noticeType"] == "RUN_COMPLETED"),
            "the second restart Run reads the notices of {job}: {seen:?}"
        );
    }
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_retry_after_a_failed_run_gets_none_of_the_rights_of_a_restart() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let (mut x, mut y) = (
        live_runner(&fixture, "plain-x").await,
        live_runner(&fixture, "plain-y").await,
    );
    let a = JobDeclaration::new(&x.runner_type).with_max_attempts(2);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (first, _) = started_with_agent(&fixture, &mut x).await;
    let run1 = Work::of(&x, &first);
    save_state(&fixture, &run1, json!({ "turn": 1 })).await;
    let child = create(
        &fixture,
        &run1,
        &y.runner_type,
        a.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (child_trigger, _) = started_with_agent(&fixture, &mut y).await;
    upload_result(&fixture, &Work::of(&y, &child_trigger), "for run 1").await;
    let result = result_for(&durable, child).await;
    x.fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    let (retry, _) = started_with_agent(&fixture, &mut x).await;
    let retry_work = Work::of(&x, &retry);

    let state = client_saved(&fixture, &retry_work).await;
    assert_eq!(
        gesture::refusal(&state, "jobsSavedState"),
        "not_a_restart_run"
    );
    let read = support::restart::read_result(&fixture, &retry_work, result).await;
    assert_eq!(
        gesture::refusal(&read, "jobsReadResult"),
        "not_addressed_to_you"
    );
    durable.close().await;
    fixture.shutdown().await;
}

async fn client_saved(fixture: &JobsFixture, work: &Work) -> Value {
    fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVED_STATE,
            json!({ "work": work.wire() }),
        )
        .await
}
