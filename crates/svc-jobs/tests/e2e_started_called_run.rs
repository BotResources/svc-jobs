mod support;

use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::started::{accepted, refused};
use support::tree::{call_child, pair};
use support::{LONG, db, dispatch};
use uuid::Uuid;

#[tokio::test]
async fn the_refusals_of_a_called_run_name_neither_its_job_nor_its_parent_nor_the_initial_job() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut tree = pair(&fixture, "started-p7").await;
    let root = JobDeclaration::new(&tree.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = tree.parent.next_trigger(LONG).await;
    accepted(
        &tree
            .parent
            .send_started(&parent_trigger, Uuid::now_v7())
            .await,
    );
    let trigger = call_child(
        &fixture,
        &mut tree,
        root.job_id,
        runner::run_id(&parent_trigger),
    )
    .await;
    assert!(trigger["parent_job_id"].is_string(), "{trigger}");
    assert!(trigger["initial_job_id"].is_string(), "{trigger}");
    assert!(trigger["passport"].is_object(), "{trigger}");
    let mut second = FakeRunner::new(fixture.nats(), &tree.child.runner_type, "instance-b");
    second.connect().await;
    dispatch::await_live_instances(
        &fixture.gql(),
        fixture.admin(),
        &tree.child.runner_type,
        2,
        LONG,
    )
    .await;
    let durable = support::db::Durable::open(&fixture.app_url()).await;

    accepted(&tree.child.send_started(&trigger, Uuid::now_v7()).await);
    refused(
        &second.send_started(&trigger, Uuid::now_v7()).await,
        "run_held",
        &trigger,
    );
    tree.child.complete_run(&trigger).await;
    durable
        .await_count(
            db::TERMINALS_OF_RUN,
            runner::run_id(&trigger),
            1,
            "the end of the called run",
        )
        .await;
    refused(
        &second.send_started(&trigger, Uuid::now_v7()).await,
        "run_not_pending",
        &trigger,
    );
    refused(
        &tree.child.send_started(&trigger, Uuid::now_v7()).await,
        "run_not_pending",
        &trigger,
    );
    fixture.shutdown().await;
}
