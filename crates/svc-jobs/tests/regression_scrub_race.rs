mod support;

use bc_jobs::JobsError;
use bc_jobs::commands::job::manual_retry::{
    ManualRetryJob, ManualRetryOutcome, SuccessorDispatch, manual_retry,
};
use bc_jobs::commands::job::resolution::FailJob;
use bc_jobs::domain::ids::{JobId, ManualRetryId, ResolutionId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::job::parenting::ParentContext;
use bc_jobs::domain::ownership::ResolutionRequester;
use bc_jobs::domain::policy::Retention;
use bc_jobs::domain::priority::Priority;
use bc_jobs::fixtures::user;
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::scrub::{ScrubOutcome, TreeScrub, TreeScrubs};
use chrono::TimeDelta;
use svc_jobs::app::write::JobChange;
use uuid::Uuid;

use support::clock;
use support::contention::{Fixture, a_queued_job, commit, ids, metadata};

async fn a_failed_job(fixture: &Fixture) -> Job {
    let queued = a_queued_job(fixture, "archivist").await;
    let failed = queued
        .declare_failed(FailJob {
            resolution_id: ResolutionId::new(Uuid::now_v7()).expect("a v7 resolution id"),
            requester: ResolutionRequester::Declarant,
        })
        .expect("a queued job fails");
    commit(
        fixture,
        vec![JobChange::new(
            queued.id(),
            Some(queued.clone()),
            failed.events,
        )],
    )
    .await
    .expect("the failure lands");
    reload(fixture, queued.id()).await
}

async fn reload(fixture: &Fixture, job_id: JobId) -> Job {
    JobReader::load(&fixture.store, job_id)
        .await
        .expect("the job loads")
        .expect("the job exists")
}

fn retry_of(predecessor: &Job, successor: JobId) -> Result<ManualRetryOutcome, JobsError> {
    manual_retry(
        predecessor,
        ParentContext::of(None),
        None,
        ManualRetryJob {
            manual_retry_id: ManualRetryId::new(Uuid::now_v7()).expect("a v7 retry id"),
            failed_resolution_id: predecessor
                .resolution()
                .expect("the predecessor failed")
                .id(),
            successor_job_id: successor,
            dispatch: SuccessorDispatch::Wait,
            requested_by: user(),
            applicable_maximum: Priority::DEFAULT,
        },
    )
}

async fn scrub(fixture: &Fixture, tree: JobId) -> ScrubOutcome {
    fixture
        .store
        .scrub_tree(
            tree,
            TreeScrub {
                retention: Retention::default(),
                at: clock::now() + TimeDelta::days(40),
                ids: &ids(),
                metadata: &metadata(),
            },
        )
        .await
        .expect("the scrub runs")
}

#[tokio::test]
async fn a_manual_retry_decided_before_a_scrub_and_committed_after_it_is_refused_and_creates_no_job()
 {
    let fixture = Fixture::start().await;
    let predecessor = a_failed_job(&fixture).await;
    let successor_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    let Ok(ManualRetryOutcome::Started(stale_plan)) = retry_of(&predecessor, successor_id) else {
        panic!("a failed job of a Tree that is not scrubbed is retried");
    };

    assert_eq!(
        scrub(&fixture, predecessor.initial_job_id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );

    let committed = commit(
        &fixture,
        vec![
            JobChange::new(successor_id, None, stale_plan.successor.events),
            JobChange::new(
                predecessor.id(),
                Some(predecessor.clone()),
                stale_plan.predecessor.events,
            ),
        ],
    )
    .await;
    assert!(
        matches!(&committed, Err(error) if error.is_write_race()),
        "the plan decided on the content that the scrub erased is a lost write race, got {committed:?}"
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM jobs WHERE id::text = $1",
                &successor_id.as_uuid().to_string()
            )
            .await,
        0,
        "no Job carries the content again"
    );

    let after = reload(&fixture, predecessor.id()).await;
    assert!(after.tree_scrubbed());
    assert!(matches!(
        retry_of(&after, JobId::new(Uuid::now_v7()).expect("a v7 job id")),
        Err(JobsError::TreeScrubbed)
    ));
    fixture.shutdown().await;
}
