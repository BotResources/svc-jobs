mod support;

use bc_jobs::commands::job::restart::{RestartJob, restart_job};
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::job::parenting::ParentContext;
use bc_jobs::domain::priority::Priority;
use bc_jobs::ports::scrub::ScrubOutcome;
use svc_jobs::ServiceError;
use svc_jobs::app::write::JobChange;
use svc_jobs::db::restart::restart_still_due;
use uuid::Uuid;

use support::contention::{Fixture, commit};
use support::scrub_race::{a_completed_job_with_an_ended_run, scrub};

async fn an_agent_waiting_for_a_restart(fixture: &Fixture, run: RunId, tree: JobId) {
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

#[tokio::test]
async fn a_restart_decided_before_a_scrub_and_committed_after_it_creates_no_job() {
    let fixture = Fixture::start().await;
    let (origin, run) = a_completed_job_with_an_ended_run(&fixture).await;
    let tree = origin.initial_job_id();
    an_agent_waiting_for_a_restart(&fixture, run, tree).await;
    let mut connection = fixture.pool.acquire().await.expect("a connection");
    assert!(
        restart_still_due(&mut connection, run.as_uuid())
            .await
            .expect("the standing is read"),
        "the agent is due for a restart before the scrub"
    );
    drop(connection);
    let restart_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    let stale = restart_job(
        &origin,
        ParentContext::of(None),
        RestartJob {
            restart_job_id: restart_id,
            origin_run_id: run,
            applicable_maximum: Priority::DEFAULT,
        },
    )
    .expect("the restart is decided on the origin before the scrub");

    assert_eq!(
        scrub(&fixture, tree).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );

    let committed = commit(
        &fixture,
        vec![JobChange::new(restart_id, None, stale.events).restarting(run.as_uuid(), tree)],
    )
    .await;
    assert!(
        matches!(committed, Err(ServiceError::RestartNotDue)),
        "got {committed:?}"
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM jobs WHERE id::text = $1",
                &restart_id.as_uuid().to_string()
            )
            .await,
        0,
        "no Job carries the erased content again"
    );
    fixture.shutdown().await;
}
