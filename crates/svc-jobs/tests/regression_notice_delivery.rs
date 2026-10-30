mod support;

use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::mail::inbox::{Notice, NoticeType};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use svc_jobs::app::write::JobChange;
use svc_jobs::db::call_notices::{Delivering, NoticeFor, insert_notice};

use support::clock;
use support::contention::{
    Fixture, a_job_with_one_dispatched_run, a_registered_instance, commit, ids, metadata,
};
use support::db::RINGS_OF_ITEM;

const NOTICES_OF_RUN: &str =
    "SELECT count(*) FROM inbox_items WHERE kind = 'NOTICE' AND run_id::text = $1";

fn notice(job_id: JobId) -> Notice {
    Notice {
        notice_type: NoticeType::Accepted,
        job_id,
        position: None,
        failure_report: None,
        message: None,
        answer: None,
        result_id: None,
    }
}

async fn deliver(
    fixture: &Fixture,
    run_id: bc_jobs::domain::ids::RunId,
    job_id: JobId,
) -> bc_jobs::domain::ids::ItemId {
    let mut tx = fixture.store.begin().await.expect("a transaction opens");
    let ids = ids();
    let metadata = metadata();
    let item = insert_notice(
        &mut tx,
        NoticeFor {
            run_id,
            address: None,
        },
        &notice(job_id),
        Delivering {
            ids: &ids,
            metadata: &metadata,
            at: clock::now(),
        },
    )
    .await
    .expect("the notice is kept")
    .expect("the run is not removed");
    tx.commit().await.expect("the notice commits");
    item
}

#[tokio::test]
async fn a_notice_is_always_kept_and_rings_the_doorbell_only_for_a_live_run() {
    let fixture = Fixture::start().await;
    let runner_type = "analyst";
    a_registered_instance(&fixture, runner_type).await;
    let (job, run_id) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let run_text = run_id.as_uuid().to_string();

    let quiet = deliver(&fixture, run_id, job.id()).await;
    assert_eq!(fixture.count(NOTICES_OF_RUN, &run_text).await, 1);
    assert_eq!(
        fixture
            .count(RINGS_OF_ITEM, &quiet.as_uuid().to_string())
            .await,
        0,
        "a run that has not started has no one to ring"
    );

    let started = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: RunnerInstanceReference::new(
                RunnerTypeKey::new(runner_type).expect("a valid runner type"),
                InstanceKey::new("instance-a").expect("a valid instance key"),
            ),
        })
        .expect("the start is recorded");
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the run starts");

    let item = deliver(&fixture, run_id, job.id()).await;
    assert_eq!(fixture.count(NOTICES_OF_RUN, &run_text).await, 2);
    assert_eq!(
        fixture
            .count(RINGS_OF_ITEM, &item.as_uuid().to_string())
            .await,
        1,
        "a live run is rung once for the notice"
    );
    fixture.shutdown().await;
}
