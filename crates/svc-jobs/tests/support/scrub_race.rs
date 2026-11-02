use bc_jobs::commands::job::resolution::{FailJob, FinishJob};
use bc_jobs::commands::job::run_outcome::RunCompletedFact;
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::ids::{JobId, ResolutionId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::ownership::ResolutionRequester;
use bc_jobs::domain::policy::Retention;
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::scrub::{ScrubOutcome, TreeScrub, TreeScrubs};
use chrono::TimeDelta;
use svc_jobs::app::write::JobChange;
use uuid::Uuid;

use super::clock;
use super::contention::{
    Fixture, a_job_with_one_dispatched_run, a_queued_job, a_registered_instance, commit, ids,
    metadata,
};

pub const RUNNER_TYPE: &str = "archivist";

pub async fn reload(fixture: &Fixture, job_id: JobId) -> Job {
    JobReader::load(&fixture.store, job_id)
        .await
        .expect("the job loads")
        .expect("the job exists")
}

pub fn resolution() -> ResolutionId {
    ResolutionId::new(Uuid::now_v7()).expect("a v7 resolution id")
}

pub async fn a_failed_job(fixture: &Fixture) -> Job {
    let queued = a_queued_job(fixture, RUNNER_TYPE).await;
    let failed = queued
        .declare_failed(FailJob {
            resolution_id: resolution(),
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

pub async fn a_completed_job_with_an_ended_run(fixture: &Fixture) -> (Job, RunId) {
    a_registered_instance(fixture, RUNNER_TYPE).await;
    let (job, run_id) = a_job_with_one_dispatched_run(fixture, RUNNER_TYPE).await;
    let started = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: RunnerInstanceReference::new(
                RunnerTypeKey::new(RUNNER_TYPE).expect("a valid runner type"),
                InstanceKey::new("instance-a").expect("a valid instance key"),
            ),
        })
        .expect("the run starts");
    commit(
        fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the start lands");
    let job = reload(fixture, job.id()).await;
    let ended = job
        .record_run_completed(RunCompletedFact { run_id })
        .expect("the run completes");
    commit(
        fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), ended.events)],
    )
    .await
    .expect("the end lands");
    let job = reload(fixture, job.id()).await;
    let finished = job
        .finish(FinishJob {
            resolution_id: resolution(),
            requester: ResolutionRequester::Declarant,
        })
        .expect("the job finishes");
    commit(
        fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), finished.events)],
    )
    .await
    .expect("the finish lands");
    (reload(fixture, job.id()).await, run_id)
}

pub async fn scrub(fixture: &Fixture, tree: JobId) -> ScrubOutcome {
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
