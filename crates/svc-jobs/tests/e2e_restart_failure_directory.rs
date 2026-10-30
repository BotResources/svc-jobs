mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::lineage::{create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::running_child;
use support::runner;
use support::surface::{self, Agent};
use support::{LONG, gql};
use uuid::Uuid;

const ENTRY: &str = "SELECT count(*) FROM directory_agents WHERE agent_id = $1";
const ENTRY_ON_RUN: &str =
    "SELECT count(*) FROM directory_agents WHERE agent_id = $1 AND run_id = $2";
const FINISHED: &str =
    "SELECT count(*) FROM directory_agents WHERE agent_id = $1 AND state = 'FINISHED'";
const ITEMS_AT: &str = "SELECT count(*) FROM inbox_items WHERE address = $1";
const IN_TREE: &str = "SELECT count(*) FROM directory_agents WHERE initial_job_id = $1";

async fn entry_on(durable: &Durable, agent: Uuid, run: Uuid) -> i64 {
    sqlx::query_scalar(ENTRY_ON_RUN)
        .bind(agent)
        .bind(run)
        .fetch_one(durable.pool())
        .await
        .expect("the directory reads")
}

#[tokio::test]
async fn a_failed_restart_run_takes_only_what_it_added_and_a_cancel_of_the_tree_removes_the_rest() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-fails").await;
    let mut z = live_runner(&fixture, "restart-fails-z").await;
    create(
        &fixture,
        &scene.child_work,
        &z.runner_type,
        scene.child_work.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (_, g1) = started_with_agent(&fixture, &mut z).await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let restart = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &restart).await;
    let restart_work = Work::of(&scene.pair.child, &restart);
    let added = Agent::with_inbox();
    let n1 = added.agent_id;
    surface::register(
        fixture.nats(),
        &scene.pair.child,
        restart_work.run_id,
        &[Agent::with_inbox().with_id(scene.receiver), added],
    )
    .await;
    assert_eq!(
        entry_on(&durable, scene.receiver, restart_work.run_id).await,
        1,
        "q1 now points to the restart Run"
    );
    durable
        .assert_count(
            ENTRY,
            scene.receiver,
            1,
            "one entry for q1, its address unchanged",
        )
        .await;
    create(
        &fixture,
        &restart_work,
        &z.runner_type,
        restart_work.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (_, h1) = started_with_agent(&fixture, &mut z).await;

    scene
        .pair
        .child
        .fail_run(&restart, "PERMANENT", "boom", None)
        .await;
    durable
        .await_count(ENTRY, n1, 0, "the agent the restart Run added leaves")
        .await;
    durable
        .await_count(ENTRY, h1, 0, "the Run the restart Run created leaves")
        .await;
    durable
        .assert_count(ENTRY, g1, 1, "the Run the earlier Run created stays")
        .await;
    assert_eq!(
        entry_on(&durable, scene.receiver, scene.child_run()).await,
        1,
        "q1 is back on its Run"
    );
    durable
        .assert_count(FINISHED, scene.receiver, 1, "and FINISHED")
        .await;

    let cancelled =
        gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), scene.root).await;
    assert!(
        cancelled.get("errors").is_none_or(|e| e.is_null()),
        "{cancelled}"
    );
    durable
        .await_count(ENTRY, scene.receiver, 0, "a cancel of the Tree removes q1")
        .await;
    durable
        .await_count(
            IN_TREE,
            scene.root,
            0,
            "and the whole directory of the Tree",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_retry_gets_new_agents_and_the_failed_run_takes_its_agent_and_unread_mail() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let (mut x, mut y) = (
        live_runner(&fixture, "retry-agents-x").await,
        live_runner(&fixture, "retry-agents-y").await,
    );
    let a = JobDeclaration::new(&x.runner_type).with_max_attempts(2);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (first, a1) = started_with_agent(&fixture, &mut x).await;
    let run1 = Work::of(&x, &first);
    create(
        &fixture,
        &run1,
        &y.runner_type,
        a.job_id,
        json!({ "mode": "BACKGROUND", "caller_address": a1.to_string() }),
    )
    .await;
    let (child, writer) = started_with_agent(&fixture, &mut y).await;
    let child_work = Work::of(&y, &child);
    let mail = Mail::new(writer, &[a1]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &child_work, &mail).await,
        "jobsSendMail",
    );
    assert!(
        durable.count(ITEMS_AT, a1).await >= 1,
        "a1 has unread items"
    );

    x.fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    let (retry, a2) = started_with_agent(&fixture, &mut x).await;
    assert_eq!(runner::attempt_number(&retry), 2);
    assert_ne!(a2, a1, "the retry registers its own agent");
    durable.await_count(ENTRY, a1, 0, "a1 is gone").await;
    durable
        .assert_count(ITEMS_AT, a1, 0, "with its unread mail")
        .await;
    let late = Mail::new(a2, &[a1]);
    let refused = gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        &Work::of(&x, &retry),
        &late,
    )
    .await;
    assert_eq!(
        gesture::refusal(&refused, "jobsSendMail"),
        "bad_addresses",
        "and with its address"
    );
    durable.close().await;
    fixture.shutdown().await;
}
