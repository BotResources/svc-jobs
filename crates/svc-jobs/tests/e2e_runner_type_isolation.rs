mod support;

use serde_json::json;
use support::fixture::{JobsFixture, runner_access};
use support::gesture;
use support::restart::running_child;
use support::surface::{self, Agent};

#[tokio::test]
async fn a_graphql_call_names_the_runner_type_and_the_instance_of_another_type_is_refused() {
    let fixture = JobsFixture::start().await;
    let scene = running_child(&fixture, "type-isolation").await;
    let holder = &scene.pair.parent;
    let other_type = &scene.pair.child;
    assert_ne!(holder.runner_type, other_type.runner_type);
    assert_eq!(
        holder.instance_key, other_type.instance_key,
        "the two runner types run an Instance under the same key"
    );

    let registered = surface::register_as(
        fixture.nats(),
        &other_type.runner_type,
        &other_type.instance_key,
        scene.parent_work.run_id,
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(surface::reason(&registered), "instance_mismatch");

    let as_other_type = scene.parent_work.as_runner_type(&other_type.runner_type);
    let refused = gesture::unread(
        &fixture.gql(),
        &runner_access(),
        &as_other_type,
        Some(scene.peer),
    )
    .await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUnread"),
        "instance_mismatch",
        "the NATS surface and the GraphQL surface refuse the Instance of another type alike"
    );

    let saved = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": as_other_type.wire(), "state": { "stolen": true } } }),
        )
        .await;
    assert_eq!(
        gesture::refusal(&saved, "jobsSaveState"),
        "instance_mismatch"
    );

    let by_the_holder = gesture::unread(
        &fixture.gql(),
        &runner_access(),
        &scene.parent_work,
        Some(scene.peer),
    )
    .await;
    assert!(
        by_the_holder["errors"].is_null()
            && gesture::field(&by_the_holder, "jobsUnread")
                .get("items")
                .is_some(),
        "the Instance that holds the Run, named with its own type, is served: {by_the_holder}"
    );

    let another_key = scene.parent_work.as_instance("another-instance");
    gesture::refusal(
        &gesture::unread(
            &fixture.gql(),
            &runner_access(),
            &another_key,
            Some(scene.peer),
        )
        .await,
        "jobsUnread",
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_graphql_call_that_names_no_runner_type_is_judged_by_its_instance_key() {
    let fixture = JobsFixture::start().await;
    let scene = running_child(&fixture, "type-optional").await;
    let untyped = scene.parent_work.without_runner_type();
    let served =
        gesture::unread(&fixture.gql(), &runner_access(), &untyped, Some(scene.peer)).await;
    assert!(
        served["errors"].is_null() && gesture::field(&served, "jobsUnread").get("items").is_some(),
        "{served}"
    );
    fixture.shutdown().await;
}
