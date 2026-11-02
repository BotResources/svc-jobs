mod support;

use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, SseSubscription, verdict};
use serde_json::json;
use support::events::EventLog;
use support::fixture::{
    self, JobsFixture, Knobs, deactivated_super_user, impersonating_member, machine_caller,
};
use support::producer::{JobDeclaration, Producer};
use support::refusal::{self};
use support::subs_max_priority;
use support::{LONG, QUIET, SHORT, gql, priority, stream, wire};
use uuid::Uuid;

const FORBIDDEN: &str = "FORBIDDEN";
const INVALID_STATE: &str = "INVALID_STATE";

#[tokio::test]
async fn administrators_read_users_maximums_with_their_affordances() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let super_user = fixture.admin();
    let user_a = fixture::ordinary_member();
    let user_b = fixture::ordinary_member();
    let (a, b) = (user_a.actor_id(), user_b.actor_id());
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, a, 5).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user gives user A a maximum of 5",
    );
    let of_b = JobDeclaration::new(&wire::unique_runner_type("maxima")).triggered_by_user(b);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&of_b)
        .await;
    events
        .expect_one(wire::FACT_QUEUED, of_b.job_id, LONG)
        .await;
    let mut own_b = SseSubscription::open(
        fixture.url(),
        &user_b,
        &subs_max_priority::my_max_priority_changed(),
    )
    .await;
    assert_eq!(
        priority::opening_max(&mut own_b, SHORT).await,
        wire::SERVICE_DEFAULT_MAX_PRIORITY
    );

    let read = priority::user_max_priorities(&client, super_user, &[a, b]).await;

    assert_eq!(
        read.iter()
            .map(|entry| entry["userId"].clone())
            .collect::<Vec<_>>(),
        vec![json!(a.to_string()), json!(b.to_string())],
        "one entry per user asked for, in the order asked for: {read:?}",
    );
    let read_a = priority::maximum_of(&read, a);
    let read_b = priority::maximum_of(&read, b);
    priority::assert_maximum(&read_a, 5, false, "user A has a maximum of their own");
    priority::assert_maximum(&read_b, 3, true, "user B falls back to the default");

    gql::assert_allowed(&read_a, wire::ACTION_SET_MAX_PRIORITY);
    gql::assert_allowed(&read_a, wire::ACTION_CLEAR_MAX_PRIORITY);
    gql::assert_allowed(&read_b, wire::ACTION_SET_MAX_PRIORITY);
    assert_eq!(
        gql::assert_blocked(&read_b, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
        "a user on the default has no maximum of their own to clear",
    );
    assert_eq!(
        gql::affordance(&read_b, wire::ACTION_CLEAR_MAX_PRIORITY)["params"],
        json!({ "userId": b.to_string() }),
        "the refusal names the user it is about",
    );

    let refused = priority::clear_user_max_priority(&client, super_user, b).await;

    assert_eq!(
        verdict::expect_code_shaped(&refused, "clearing a maximum that is not set"),
        INVALID_STATE,
    );
    assert_eq!(
        gql::mutation_error_reason(&refused, "clearing a maximum that is not set"),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );
    assert_eq!(
        gql::mutation_error_params(&refused, "clearing a maximum that is not set"),
        json!({ "userId": b.to_string() }),
    );
    refusal::assert_nothing_leaked(&refused, &[], "a refused clear");
    stream::expect_total_silence(&mut own_b, "a refused clear changes no maximum", QUIET).await;
    assert_eq!(
        priority::user_max_priorities(&client, super_user, &[a, b]).await,
        read,
        "a refused clear leaves both reads as they were",
    );

    gql::expect_success(
        &priority::clear_user_max_priority(&client, super_user, a).await,
        wire::FIELD_CLEAR_USER_MAX_PRIORITY,
        "a super user clears user A's maximum",
    );

    let after = priority::user_max_priorities(&client, super_user, &[a, b]).await;
    let cleared_a = priority::maximum_of(&after, a);
    priority::assert_maximum(
        &cleared_a,
        3,
        true,
        "a cleared maximum falls back to the default",
    );
    gql::assert_allowed(&cleared_a, wire::ACTION_SET_MAX_PRIORITY);
    assert_eq!(
        gql::assert_blocked(&cleared_a, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
        "once cleared, nothing is left to clear",
    );
    assert_eq!(
        priority::maximum_of(&after, b),
        read_b,
        "user B was not touched"
    );
    assert_eq!(priority::my_max_priority(&client, &user_a).await, 3);

    let unknown = Uuid::now_v7();
    let read_unknown = priority::maximum_of(
        &priority::user_max_priorities(&client, super_user, &[unknown]).await,
        unknown,
    );

    priority::assert_maximum(&read_unknown, 3, true, "an unknown user is on the default");
    gql::assert_allowed(&read_unknown, wire::ACTION_SET_MAX_PRIORITY);
    assert_eq!(
        gql::assert_blocked(&read_unknown, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );

    for (caller, ids, who) in [
        (user_b.clone(), vec![a, b], "a plain user reading A and B"),
        (
            user_b.clone(),
            vec![b],
            "a plain user reading only themselves",
        ),
    ] {
        assert_read_refused(&client, &caller, &ids, &[a, b], who).await;
    }

    for (caller, who) in [
        (
            impersonating_member(super_user),
            "a member impersonating an administrator",
        ),
        (deactivated_super_user(), "a deactivated super user"),
        (machine_caller(), "a machine passport"),
    ] {
        assert_read_refused(&client, &caller, &[a, b], &[a, b], who).await;
    }

    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_read_says_which_maximum_applies_whatever_the_service_default() {
    let fixture = JobsFixture::start_with(Knobs {
        default_max_priority: Some(2),
        ..Knobs::default()
    })
    .await;
    let client = fixture.gql();
    let super_user = fixture.admin();
    let user_d = fixture::ordinary_member();
    let (c, d) = (Uuid::now_v7(), user_d.actor_id());
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, c, 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps C at the very level of the default",
    );

    let read = priority::user_max_priorities(&client, super_user, &[c, d]).await;

    let read_c = priority::maximum_of(&read, c);
    let read_d = priority::maximum_of(&read, d);
    priority::assert_maximum(&read_c, 2, false, "C's own maximum, equal to the default");
    gql::assert_allowed(&read_c, wire::ACTION_CLEAR_MAX_PRIORITY);
    priority::assert_maximum(&read_d, 2, true, "D on JOBS_DEFAULT_MAX_PRIORITY");
    assert_eq!(
        gql::assert_blocked(&read_d, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );

    assert_eq!(priority::my_max_priority(&client, &user_d).await, 2);

    fixture.shutdown().await;
}

async fn assert_read_refused(
    client: &GraphqlClient,
    caller: &Passport,
    ids: &[Uuid],
    secrets: &[Uuid],
    who: &str,
) {
    let response = priority::user_max_priorities_response(client, caller, ids).await;
    assert_eq!(
        verdict::expect_code_shaped(&response, who),
        FORBIDDEN,
        "{who}: reading users' maxima is an administrator gesture",
    );
    let secrets: Vec<String> = secrets.iter().map(Uuid::to_string).collect();
    refusal::assert_nothing_leaked(&response, &secrets, who);
}
