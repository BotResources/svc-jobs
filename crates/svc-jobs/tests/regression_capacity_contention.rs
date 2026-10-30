mod support;

use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::domain::fleet::run_duration::MaxRunDuration;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::keys::RunnerTypeKey;
use svc_jobs::app::write::JobChange;
use uuid::Uuid;

use support::clock;
use support::contention::{Fixture, a_queued_job, a_registered_instance, commit, limits};

const DISPATCHERS: usize = 6;

#[tokio::test]
async fn dispatchers_racing_for_the_last_slot_of_a_type_start_exactly_one_run() {
    let fixture = Fixture::start().await;
    let runner_type = "analyst";
    a_registered_instance(&fixture, runner_type).await;
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let mut changes = Vec::new();
    for _ in 0..DISPATCHERS {
        let job = a_queued_job(&fixture, runner_type).await;
        let dispatched = job
            .dispatch_run(
                DispatchRun {
                    run_id: RunId::new(Uuid::now_v7()).expect("a v7 run id"),
                    at: clock::now(),
                    max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
                },
                &limits(),
            )
            .expect("the run dispatches on the state each dispatcher read");
        changes.push(
            JobChange::new(job.id(), Some(job), dispatched.events)
                .requiring_free_capacity(key.clone()),
        );
    }

    let outcomes = futures::future::join_all(
        changes
            .into_iter()
            .map(|change| commit(&fixture, vec![change])),
    )
    .await;

    let committed = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert_eq!(
        committed, 1,
        "a type with one free slot admits one run, whatever the dispatchers read: {outcomes:?}"
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) FROM current_runs c \
                 JOIN runner_types t ON t.id = c.runner_type_id WHERE t.type_key = $1",
                runner_type,
            )
            .await,
        1
    );
    fixture.shutdown().await;
}
