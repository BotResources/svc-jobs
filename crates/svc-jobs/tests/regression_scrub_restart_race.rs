mod support;

use bc_jobs::commands::job::restart::{RestartJob, restart_job};
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::parenting::ParentContext;
use bc_jobs::domain::priority::Priority;
use bc_jobs::ports::scrub::ScrubOutcome;
use svc_jobs::ServiceError;
use svc_jobs::app::write::JobChange;
use svc_jobs::db::restart::restart_still_due;
use uuid::Uuid;

use support::contention::{Fixture, commit};
use support::scrub_race::{
    a_completed_job_with_an_ended_run, an_agent_waiting_for_a_restart, register_the_runner_type,
    scrub,
};

#[tokio::test]
async fn a_restart_decided_before_a_scrub_and_committed_after_it_creates_no_job() {
    let fixture = Fixture::start().await;
    register_the_runner_type(&fixture).await;
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
