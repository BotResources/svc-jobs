mod support;

use bc_jobs::domain::ids::RunId;
use svc_jobs::db::apply::directory::remove_job_subtree;
use svc_jobs::db::removal::removed_with_jobs;
use uuid::Uuid;

use support::clock;
use support::contention::{Fixture, a_job_with_one_dispatched_run, a_registered_instance};

const WAKES_OF_RUN: &str = "SELECT count(*) FROM restart_wakes WHERE run_id::text = $1";
const STATES_OF_RUN: &str = "SELECT count(*) FROM saved_states WHERE run_id::text = $1";
const REMOVED_RUN: &str = "SELECT count(*) FROM directory_removed_runs WHERE run_id::text = $1";
const AGENTS_OF_RUN: &str = "SELECT count(*) FROM directory_agents WHERE run_id::text = $1";

async fn queue_a_restart_and_keep_a_state(fixture: &Fixture, run: RunId) {
    sqlx::query("INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, $2)")
        .bind(run.as_uuid())
        .bind(clock::now())
        .execute(&fixture.pool)
        .await
        .expect("the restart is queued");
    sqlx::query("INSERT INTO saved_states (run_id, state, saved_at) VALUES ($1, '{}'::jsonb, $2)")
        .bind(run.as_uuid())
        .bind(clock::now())
        .execute(&fixture.pool)
        .await
        .expect("the state is saved");
}

async fn an_agent_at(fixture: &Fixture, run: RunId, tree: Uuid) {
    sqlx::query(
        "INSERT INTO directory_agents \
            (agent_id, run_id, initial_job_id, has_inbox, declares_restart, state, registered_at) \
         VALUES ($1, $2, $3, true, true, 'FINISHED', $4)",
    )
    .bind(Uuid::now_v7())
    .bind(run.as_uuid())
    .bind(tree)
    .bind(clock::now())
    .execute(&fixture.pool)
    .await
    .expect("the agent is registered");
}

#[tokio::test]
async fn the_removal_of_runs_drops_their_queued_restarts_and_states_and_keeps_those_of_other_runs()
{
    let fixture = Fixture::start().await;
    a_registered_instance(&fixture, "analyst").await;
    let (_, removed) = a_job_with_one_dispatched_run(&fixture, "analyst").await;
    let (_, kept) = a_job_with_one_dispatched_run(&fixture, "analyst").await;
    for run in [removed, kept] {
        queue_a_restart_and_keep_a_state(&fixture, run).await;
    }

    let mut tx = fixture.pool.begin().await.expect("the removal opens");
    remove_job_subtree(&mut tx, &[removed.as_uuid()], clock::now())
        .await
        .expect("the runs are removed");
    tx.commit().await.expect("the removal commits");

    let (removed, kept) = (removed.as_uuid().to_string(), kept.as_uuid().to_string());
    assert_eq!(fixture.count(WAKES_OF_RUN, &removed).await, 0);
    assert_eq!(fixture.count(STATES_OF_RUN, &removed).await, 0);
    assert_eq!(fixture.count(REMOVED_RUN, &removed).await, 1);
    assert_eq!(fixture.count(WAKES_OF_RUN, &kept).await, 1);
    assert_eq!(fixture.count(STATES_OF_RUN, &kept).await, 1);
    assert_eq!(fixture.count(REMOVED_RUN, &kept).await, 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_runs_a_removal_reports_are_exactly_the_runs_the_deletion_removes() {
    let fixture = Fixture::start().await;
    a_registered_instance(&fixture, "analyst").await;
    let (job, run) = a_job_with_one_dispatched_run(&fixture, "analyst").await;
    let (other_job, other_run) = a_job_with_one_dispatched_run(&fixture, "analyst").await;
    an_agent_at(&fixture, run, job.id().as_uuid()).await;
    an_agent_at(&fixture, other_run, other_job.id().as_uuid()).await;

    let mut tx = fixture.pool.begin().await.expect("the removal opens");
    let reported = removed_with_jobs(&mut tx, &[job.id().as_uuid()])
        .await
        .expect("the removal is computed");
    let reported: Vec<Uuid> = reported.runs().map(|run| run.as_uuid()).collect();
    assert_eq!(reported, vec![run.as_uuid()]);
    remove_job_subtree(&mut tx, &reported, clock::now())
        .await
        .expect("the reported runs are removed");
    tx.commit().await.expect("the removal commits");

    let (run, other) = (run.as_uuid().to_string(), other_run.as_uuid().to_string());
    assert_eq!(fixture.count(REMOVED_RUN, &run).await, 1);
    assert_eq!(fixture.count(AGENTS_OF_RUN, &run).await, 0);
    assert_eq!(fixture.count(REMOVED_RUN, &other).await, 0);
    assert_eq!(fixture.count(AGENTS_OF_RUN, &other).await, 1);
    fixture.shutdown().await;
}
