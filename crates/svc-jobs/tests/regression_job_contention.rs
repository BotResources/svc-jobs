mod support;

use bc_jobs::commands::job::run_outcome::RunCompletedFact;
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::ports::job::JobReader;
use svc_jobs::ServiceError;
use svc_jobs::app::write::JobChange;

use support::contention::{Fixture, a_job_with_one_dispatched_run, a_registered_instance, commit};

#[tokio::test]
async fn two_instances_replaying_the_same_run_start_write_one_fact_and_publish_it_once() {
    // Given: a job with one dispatched run, and the state both instances hydrated before deciding
    let fixture = Fixture::start().await;
    let runner_type = "analyst";
    a_registered_instance(&fixture, runner_type).await;
    let (decided_on, run_id) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let fact = RunStartedFact {
        run_id,
        instance: RunnerInstanceReference::new(
            RunnerTypeKey::new(runner_type).expect("a valid runner type"),
            InstanceKey::new("instance-a").expect("a valid instance key"),
        ),
    };

    // When: both instances decide RunStarted on that same state and commit at the same time
    let first = decided_on
        .record_run_started(fact.clone())
        .expect("the first instance decides the fact");
    let second = decided_on
        .record_run_started(fact)
        .expect("the second instance decides the same fact");
    assert_eq!(first.events.len(), 1);
    assert_eq!(second.events.len(), 1);
    let job_id = decided_on.id();
    let (left, right) = tokio::join!(
        commit(
            &fixture,
            vec![JobChange::new(
                job_id,
                Some(decided_on.clone()),
                first.events
            )]
        ),
        commit(
            &fixture,
            vec![JobChange::new(job_id, Some(decided_on), second.events)]
        ),
    );

    // Then: exactly one of them wrote, the other is told the state moved under it
    let outcomes = [left, right];
    let refused: Vec<&ServiceError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "exactly one duplicate must be refused, got {outcomes:?}",
    );
    assert!(
        matches!(refused[0], ServiceError::Contended),
        "the loser learns the state moved, not an infrastructure failure: {:?}",
        refused[0],
    );

    // Then: the history holds one RunStarted and the bus is offered one job.started.v1
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "RunStarted",
            )
            .await,
        1,
        "a duplicated runner fact never doubles the domain history",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM integration_outbox WHERE subject = $1",
                "integration.evt.jobs.job.started.v1",
            )
            .await,
        1,
        "a duplicated runner fact never publishes the integration event twice",
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_event_is_refused_when_the_run_lifecycle_moved_under_the_decision() {
    // Given: a job with one dispatched run, hydrated before any start
    let fixture = Fixture::start().await;
    let runner_type = "auditor";
    a_registered_instance(&fixture, runner_type).await;
    let (decided_on, run_id) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let instance = RunnerInstanceReference::new(
        RunnerTypeKey::new("auditor").expect("a valid runner type"),
        InstanceKey::new("instance-b").expect("a valid instance key"),
    );

    // When: the run starts, and only then does a decision taken on the older state try to commit
    let winner = decided_on
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance.clone(),
        })
        .expect("the run start is decided");
    commit(
        &fixture,
        vec![JobChange::new(
            decided_on.id(),
            Some(decided_on.clone()),
            winner.events,
        )],
    )
    .await
    .expect("the first writer records the start");
    let latecomer = decided_on
        .record_run_started(RunStartedFact { run_id, instance })
        .expect("the stale decision still produces its fact");

    // Then: the write is refused because the run lifecycle no longer matches what was read
    let outcome = commit(
        &fixture,
        vec![JobChange::new(
            decided_on.id(),
            Some(decided_on),
            latecomer.events,
        )],
    )
    .await;
    assert!(
        matches!(outcome, Err(ServiceError::Contended)),
        "a decision read on a run without a start may not land after that run started: {outcome:?}",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "RunStarted",
            )
            .await,
        1,
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_matching_lifecycle_still_admits_the_next_fact() {
    // Given: a job whose run has started, re-read after the write
    let fixture = Fixture::start().await;
    let runner_type = "planner";
    a_registered_instance(&fixture, runner_type).await;
    let (job, run_id) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let started = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: RunnerInstanceReference::new(
                RunnerTypeKey::new("planner").expect("a valid runner type"),
                InstanceKey::new("instance-c").expect("a valid instance key"),
            ),
        })
        .expect("the run start is decided");
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the start is recorded");

    // When: the next fact is decided on the state as it now stands
    let current = JobReader::load(&fixture.store, job.id())
        .await
        .expect("the started job loads")
        .expect("the started job exists");
    let completed = current
        .record_run_completed(RunCompletedFact { run_id })
        .expect("the run completes");

    // Then: the guard admits it — it refuses stale decisions, never fresh ones
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(current), completed.events)],
    )
    .await
    .expect("a decision taken on the current state is admitted");
    let settled = JobReader::load(&fixture.store, job.id())
        .await
        .expect("the settled job loads")
        .expect("the settled job exists");
    assert!(
        settled
            .find_run(run_id)
            .expect("the run is still there")
            .is_terminal(),
    );
    fixture.shutdown().await;
}
