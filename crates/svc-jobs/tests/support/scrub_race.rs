use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::commands::job::resolution::{FailJob, FinishJob};
use bc_jobs::commands::job::run_outcome::{RunCompletedFact, RunFailureFact};
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::ids::{JobId, ResolutionId, RetryScheduleId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{InstanceKey, ProducerKey, ReasonCode, RunnerTypeKey};
use bc_jobs::domain::ownership::ResolutionRequester;
use bc_jobs::domain::policy::{Jitter, Retention, RetryPolicy};
use bc_jobs::domain::priority::Priority;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::run::failure::{RunFailureKind, RunFailureReport};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::ports::PortError;
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::scrub::{ScrubOutcome, TreeScrub, TreeScrubs};
use chrono::TimeDelta;
use serde_json::json;
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

pub async fn a_failed_job_with_a_retry_due_in(fixture: &Fixture, days: i64) -> Job {
    let (job, run_id) = a_job_with_a_started_run(fixture).await;
    let report = RunFailureReport::new(
        RunFailureKind::Transient,
        ReasonCode::new("provider_timeout").expect("a valid reason code"),
        json!({}),
        json!({}),
        Some(TimeDelta::days(days)),
    )
    .expect("a valid report");
    let waiting = job
        .record_run_failed(
            RunFailureFact {
                run_id,
                report,
                retry_schedule_id: RetryScheduleId::new(Uuid::now_v7()).expect("a v7 id"),
                resolution_id: resolution(),
                jitter: Jitter::MIDPOINT,
                at: clock::now(),
            },
            &RetryPolicy::default(),
            &limits(),
        )
        .expect("the run fails and a retry is scheduled");
    commit(
        fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), waiting.events)],
    )
    .await
    .expect("the failure lands");
    failed(fixture, reload(fixture, job.id()).await).await
}

pub async fn scrub(fixture: &Fixture, tree: JobId) -> ScrubOutcome {
    try_scrub(fixture, tree).await.expect("the scrub runs")
}

pub async fn try_scrub(fixture: &Fixture, tree: JobId) -> Result<ScrubOutcome, PortError> {
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
}

pub async fn register_the_runner_type(fixture: &Fixture) {
    a_registered_instance(fixture, RUNNER_TYPE).await;
}

pub async fn an_agent_waiting_for_a_restart(fixture: &Fixture, run: RunId, tree: JobId) {
    let agent = Uuid::now_v7();
    for sql in [
        "INSERT INTO saved_states (run_id, state, saved_at) VALUES ($1, '{}'::jsonb, now())",
        "INSERT INTO directory_agents \
            (agent_id, run_id, initial_job_id, has_inbox, declares_restart, state, registered_at) \
         VALUES ($2, $1, $3, true, true, 'FINISHED', now())",
        "INSERT INTO saved_state_agents (run_id, agent_id, has_inbox) VALUES ($1, $2, true)",
        "INSERT INTO inbox_items (item_id, kind, run_id, notice, accepted_at) \
         VALUES ($4, 'NOTICE', $1, jsonb_build_object('type', 'REFUSED', 'job_id', $3::text), now())",
        "INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, now())",
    ] {
        sqlx::query(sql)
            .bind(run.as_uuid())
            .bind(agent)
            .bind(tree.as_uuid())
            .bind(Uuid::now_v7())
            .execute(&fixture.pool)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
