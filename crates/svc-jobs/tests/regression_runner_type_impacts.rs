mod support;

use bc_jobs::commands::fleet::deprecate;
use bc_jobs::commands::job::cancellation::CancelJob;
use bc_jobs::commands::job::run_outcome::RunCompletedFact;
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::ids::ResolutionId;
use bc_jobs::domain::keys::{DisplayName, InstanceKey, RunnerTypeKey};
use bc_jobs::domain::ownership::CancelRequester;
use bc_jobs::domain::references::KnownUser;
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use br_core_events::UserId;
use svc_jobs::app::runner_type_impacts;
use svc_jobs::app::write::{self, FleetChange, JobChange};
use uuid::Uuid;

use bc_jobs::domain::actions::fleet::RetirementWindow;
use chrono::TimeDelta;

use support::clock;
use support::contention::{
    Fixture, a_job_with_one_dispatched_run, a_registered_instance, commit, ids, metadata,
};

#[tokio::test]
async fn an_elapsed_quiet_period_emits_one_durable_affordance_only_fact() {
    let fixture = Fixture::start().await;
    let key = RunnerTypeKey::new("temporal").expect("a valid runner type");
    let fleet = a_registered_instance(&fixture, key.as_str()).await;
    let deprecated = deprecate(&fleet).expect("an active runner type deprecates");
    write::commit_fleet_events(
        &fixture.store,
        &ids(),
        FleetChange {
            runner_type_id: fleet.id(),
            runner_type: &key,
            decided_on: Some(&fleet),
            events: &deprecated.events,
        },
        &metadata(),
        clock::now(),
    )
    .await
    .expect("deprecation lands");

    let (job, run_id) = a_job_with_one_dispatched_run(&fixture, key.as_str()).await;
    let started = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: RunnerInstanceReference::new(
                key.clone(),
                InstanceKey::new("instance-a").unwrap(),
            ),
        })
        .expect("the run starts");
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the start lands");
    let running = JobReader::load(&fixture.store, job.id())
        .await
        .unwrap()
        .unwrap();
    let completed = running
        .record_run_completed(RunCompletedFact { run_id })
        .expect("the run completes");
    commit(
        &fixture,
        vec![JobChange::new(
            running.id(),
            Some(running.clone()),
            completed.events,
        )],
    )
    .await
    .expect("the terminal run and its durable impact land atomically");
    let awaiting_resolution = JobReader::load(&fixture.store, job.id())
        .await
        .unwrap()
        .unwrap();
    let cancelled = awaiting_resolution
        .cancel(CancelJob {
            resolution_id: ResolutionId::new(Uuid::now_v7()).unwrap(),
            requester: CancelRequester::Administrator(
                KnownUser::new(
                    UserId(Uuid::now_v7()),
                    DisplayName::new("Ada Admin").unwrap(),
                )
                .unwrap(),
            ),
        })
        .expect("the settled run leaves a cancellable unresolved job");
    commit(
        &fixture,
        vec![JobChange::new(
            awaiting_resolution.id(),
            Some(awaiting_resolution),
            cancelled.events,
        )],
    )
    .await
    .expect("the job settles");

    sqlx::query(
        "UPDATE run_terminals SET occurred_at = now() - interval '25 hours' WHERE run_id = $1",
    )
    .bind(run_id.as_uuid())
    .execute(&fixture.pool)
    .await
    .expect("the adapter regression advances the historical quiet-period fact");
    sqlx::query(
        "UPDATE runner_type_affordance_impacts SET terminal_run_at = now() - interval '25 hours' \
         WHERE runner_type_id = $1",
    )
    .bind(fleet.id().as_uuid())
    .execute(&fixture.pool)
    .await
    .expect("the durable impact carries the terminal instant the sweeper ages itself");

    let left_ids = ids();
    let right_ids = ids();
    let clock = svc_jobs::app::environment::SystemClock;
    let (left, right) = tokio::join!(
        runner_type_impacts::sweep(&fixture.store, &left_ids, &clock, QUIET_PERIOD),
        runner_type_impacts::sweep(&fixture.store, &right_ids, &clock, QUIET_PERIOD),
    );
    left.expect("the first pod completes its impact sweep");
    right.expect("the second pod completes its impact sweep");
    runner_type_impacts::sweep(&fixture.store, &ids(), &clock, QUIET_PERIOD)
        .await
        .expect("redelivery is absorbed");

    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "RunnerTypeBecameRetirable",
            )
            .await,
        1,
        "a claimed impact is durable and idempotent across pods",
    );
    let current = FleetReader::load(&fixture.store, &key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        current
            .guard_retire(fixture.store.decision_facts(&key, window()).await.unwrap(),)
            .is_ok(),
        "the typed delta is emitted only when the shared retirement decision actually opens",
    );

    fixture.shutdown().await;
}

const QUIET_PERIOD: TimeDelta = TimeDelta::hours(24);

fn window() -> RetirementWindow {
    RetirementWindow {
        evaluated_at: clock::now(),
        quiet_period: QUIET_PERIOD,
    }
}
