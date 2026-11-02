mod support;

use std::time::Duration;

use bc_jobs::domain::ids::{AgentId, JobId, RunId};
use bc_jobs::domain::payload::CallMode;
use bc_jobs::domain::runner::call_notices::CallNotices;
use serde_json::json;
use svc_jobs::app::environment::UuidV7Factory;
use svc_jobs::app::integration::Lineage;
use svc_jobs::app::write::{CreationRefusal, RefusalNotice, publish_rejection};
use svc_jobs::db::apply::directory::run_finished;
use svc_jobs::db::tree_lock::lock_tree;
use uuid::Uuid;

use support::clock;
use support::contention::{
    Fixture, a_job_with_one_dispatched_run, a_registered_instance, metadata,
};

const WAKES_OF_RUN: &str = "SELECT count(*) FROM restart_wakes WHERE run_id::text = $1";
const REFUSALS_OF_RUN: &str = "SELECT count(*) FROM inbox_items \
     WHERE kind = 'NOTICE' AND run_id::text = $1 AND notice ->> 'type' = 'REFUSED'";

async fn an_agent_that_can_restart(fixture: &Fixture, run: RunId, tree: JobId) -> AgentId {
    let agent = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO directory_agents \
            (agent_id, run_id, initial_job_id, has_inbox, declares_restart, state, registered_at) \
         VALUES ($1, $2, $3, true, true, 'RUNNING', $4)",
    )
    .bind(agent)
    .bind(run.as_uuid())
    .bind(tree.as_uuid())
    .bind(clock::now())
    .execute(&fixture.pool)
    .await
    .expect("the agent is registered");
    sqlx::query("INSERT INTO saved_states (run_id, state, saved_at) VALUES ($1, '{}'::jsonb, $2)")
        .bind(run.as_uuid())
        .bind(clock::now())
        .execute(&fixture.pool)
        .await
        .expect("the state is saved");
    sqlx::query(
        "INSERT INTO saved_state_agents (run_id, agent_id, has_inbox) VALUES ($1, $2, true)",
    )
    .bind(run.as_uuid())
    .bind(agent)
    .execute(&fixture.pool)
    .await
    .expect("the agent holds the saved state");
    AgentId::new(agent)
}

#[tokio::test]
async fn a_refusal_notice_written_while_the_run_ends_is_never_lost_to_the_restart() {
    let fixture = Fixture::start().await;
    let runner_type = "analyst";
    a_registered_instance(&fixture, runner_type).await;
    let (job, run) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let agent = an_agent_that_can_restart(&fixture, run, job.id()).await;
    let run_text = run.as_uuid().to_string();

    let mut ending = fixture
        .pool
        .begin()
        .await
        .expect("the end of the Run opens");
    lock_tree(&mut ending, job.id())
        .await
        .expect("the end of the Run takes the Tree");
    run_finished(&mut ending, run.as_uuid(), clock::now())
        .await
        .expect("the agent is finished in the end transaction");

    let refused_job = Uuid::now_v7();
    let told = CallNotices {
        caller_run: run,
        mode: CallMode::Background,
        address: Some(agent),
    }
    .refusal_notice(refused_job, "The call was refused.".to_owned())
    .expect("a background call with an address is told");
    let store = fixture.store.clone();
    let refusal = tokio::spawn(async move {
        publish_rejection(
            &store,
            &UuidV7Factory,
            CreationRefusal {
                job_id: refused_job,
                lineage: Lineage::declared(refused_job, None, None),
                reason_code: "parent_job_unknown",
                params: json!({ "jobId": refused_job }),
                notice: Some(RefusalNotice {
                    told,
                    command: None,
                }),
            },
            &metadata(),
            clock::now(),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !refusal.is_finished(),
        "the refusal waits for the Tree while the Run ends"
    );
    ending.commit().await.expect("the end of the Run commits");

    let woke = refusal
        .await
        .expect("the refusal task ends")
        .expect("the refusal is written");
    assert!(woke, "the refusal sees the finished agent and wakes it");
    assert_eq!(fixture.count(REFUSALS_OF_RUN, &run_text).await, 1);
    assert_eq!(
        fixture.count(WAKES_OF_RUN, &run_text).await,
        1,
        "the finished agent restarts for its refusal notice"
    );
    fixture.shutdown().await;
}
