use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::commands::job::resolution::{FailJob, FinishJob};
use bc_jobs::commands::job::run_outcome::RunCompletedFact;
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::ids::{JobId, ResolutionId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{InstanceKey, ProducerKey, RunnerTypeKey};
use bc_jobs::domain::ownership::ResolutionRequester;
use bc_jobs::domain::policy::Retention;
use bc_jobs::domain::priority::Priority;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::scrub::{ScrubOutcome, TreeScrub, TreeScrubs};
use chrono::TimeDelta;
use svc_jobs::app::write::JobChange;
use uuid::Uuid;

use super::clock;
use super::contention::{
    Fixture, a_job_with_one_dispatched_run, a_queued_job, a_registered_instance, commit, ids,
    limits, metadata, service_passport,
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

pub async fn a_queued_job_under(fixture: &Fixture, parent: Option<&Job>) -> Job {
    let job_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    let declaration = CreateJob {
        id: job_id,
        runner_type: RunnerTypeKey::new(RUNNER_TYPE).expect("a valid runner type"),
        producer: ProducerKey::new("projects").expect("a valid producer"),
        config: None,
        parent_job_id: parent.map(Job::id),
        triggered_by: None,
        source_entity_id: None,
        max_attempts: None,
        requested_priority: Priority::DEFAULT,
        initial_job_id: None,
        requester: Requester::Service(None),
        passport: Some(service_passport()),
        payload: None,
        call: None,
    };
    let CreateOutcome::Queued(queued) =
        create_job(declaration, None, None, parent, None, &limits())
            .expect("the declaration is accepted")
    else {
        panic!("a fresh job id is queued, never already queued");
    };
    commit(fixture, vec![JobChange::new(job_id, None, queued.events)])
        .await
        .expect("the job is queued");
    reload(fixture, job_id).await
}

pub async fn a_failed_job(fixture: &Fixture) -> Job {
    let queued = a_queued_job(fixture, RUNNER_TYPE).await;
    failed(fixture, queued).await
}

pub async fn failed(fixture: &Fixture, queued: Job) -> Job {
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

pub async fn a_job_with_a_started_run(fixture: &Fixture) -> (Job, RunId) {
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
    (reload(fixture, job.id()).await, run_id)
}

pub async fn a_completed_job_with_an_ended_run(fixture: &Fixture) -> (Job, RunId) {
    a_registered_instance(fixture, RUNNER_TYPE).await;
    let (job, run_id) = a_job_with_a_started_run(fixture).await;
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
