mod support;

use support::fixture::{JobsFixture, runner_access};
use support::gesture;
use support::restart::running_child;
use support::surface::{self, Agent};

#[tokio::test]
async fn a_graphql_call_of_another_runner_type_with_the_same_instance_key_is_not_refused_known_gap()
{
    let fixture = JobsFixture::start().await;
    let scene = running_child(&fixture, "isolation-gap").await;
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
    assert_eq!(
        surface::reason(&registered),
        "instance_mismatch",
        "the NATS surface names the runner type, so the Instance of the other type is refused"
    );

    let by_another_key = scene.parent_work.as_instance("another-instance");
    let refused = gesture::unread(
        &fixture.gql(),
        &runner_access(),
        &by_another_key,
        Some(scene.peer),
    )
    .await;
    gesture::refusal(&refused, "jobsUnread");

    let read = gesture::unread(
        &fixture.gql(),
        &runner_access(),
        &scene.parent_work.as_instance(&other_type.instance_key),
        Some(scene.peer),
    )
    .await;
    assert!(
        read["errors"].is_null() && gesture::field(&read, "jobsUnread").get("items").is_some(),
        "KNOWN GAP: a GraphQL call carries no runner type, so the key shared by the Instance of \
         the other type is taken for the Instance that holds the Run: {read}"
    );
    fixture.shutdown().await;
}
