mod support;

use br_core_auth::PassportHeader;
use br_test_harness::{SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::json;
use support::fixture::{
    self, JobsFixture, Knobs, deactivated_super_user, impersonating_member, machine_caller,
};
use support::refusal::{self, subscription_refusal};
use support::subs_max_priority;
use support::{LONG, QUIET, SHORT, gql, priority, stream, wire};
use uuid::Uuid;

const FORBIDDEN: &str = "FORBIDDEN";
const BAD_USER_INPUT: &str = "BAD_USER_INPUT";

#[tokio::test]
async fn the_maxima_read_answers_each_user_once_and_refuses_malformed_or_oversized_requests() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let super_user = fixture.admin();
    let capped = Uuid::now_v7();
    let other = Uuid::now_v7();
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, capped, 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user at 2",
    );

    let repeated =
        priority::user_max_priorities(&client, super_user, &[other, capped, other, capped]).await;
    let nobody = priority::user_max_priorities(&client, super_user, &[]).await;
    let mut full: Vec<Uuid> = (0..199).map(|_| Uuid::now_v7()).collect();
    full.push(capped);
    let at_the_bound = priority::user_max_priorities(&client, super_user, &full).await;

    assert_eq!(
        repeated
            .iter()
            .map(|entry| entry["userId"].clone())
            .collect::<Vec<_>>(),
        vec![json!(other.to_string()), json!(capped.to_string())],
    );
    priority::assert_maximum(&priority::maximum_of(&repeated, capped), 2, false, "capped");
    assert!(
        nobody.is_empty(),
        "an empty request reads nothing: {nobody:?}"
    );
    assert_eq!(at_the_bound.len(), 200, "exactly 200 users fit in one read");
    priority::assert_maximum(
        &priority::maximum_of(&at_the_bound, capped),
        2,
        false,
        "the last of 200",
    );

    let not_v7 = Uuid::from_u128(0x0123_4567_89ab_4def_8123_4567_89ab_cdef);
    let oversized: Vec<Uuid> = (0..201).map(|_| Uuid::now_v7()).collect();
    for (ids, reason, what) in [
        (
            vec![capped, not_v7],
            wire::REASON_NOT_UUID_V7,
            "a user id that is not a UUIDv7",
        ),
        (
            oversized,
            wire::REASON_VALUE_TOO_LONG,
            "more than 200 users in one read",
        ),
    ] {
        let response = priority::user_max_priorities_response(&client, super_user, &ids).await;

        assert_eq!(verdict::expect_code_shaped(&response, what), BAD_USER_INPUT);
        assert_eq!(
            gql::mutation_error_reason(&response, what),
            reason,
            "{what}"
        );
        refusal::assert_nothing_leaked(&response, &[capped.to_string()], what);

        let (status, body) = client
            .post_raw(
                "/graphql",
                &[
                    ("X-Passport", super_user.to_header().as_str()),
                    ("Accept", "text/event-stream"),
                ],
                json!({ "query": subs_max_priority::user_max_priorities_changed(&ids) }),
            )
            .await;
        let frame = refusal::first_stream_frame(&body, what);
        assert_eq!(
            (status, verdict::expect_code_shaped(&frame, what)),
            (StatusCode::OK, BAD_USER_INPUT.to_owned()),
            "{what}: the stream's one frame is the refusal",
        );
        assert_eq!(gql::mutation_error_reason(&frame, what), reason, "{what}");
        refusal::assert_nothing_leaked(&frame, &[capped.to_string()], what);
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn an_administrators_view_of_users_maximums_follows_every_change_live() {
    let fixture = JobsFixture::start_cluster(2, Knobs::default()).await;
    let client = fixture.gql();
    let super_user = fixture.admin();
    let (a, b, unwatched) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, a, 5).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user gives user A a maximum of 5",
    );

    let mut watch = SseSubscription::open(
        fixture.url_of(1),
        super_user,
        &subs_max_priority::user_max_priorities_changed(&[a, b]),
    )
    .await;

    let opening = priority::opening_maxima(&mut watch, SHORT).await;
    assert_eq!(
        opening,
        priority::user_max_priorities(&client, super_user, &[a, b]).await,
        "the snapshot is the initial read, affordances included",
    );

    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, a, 4).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user lowers user A to 4",
    );

    let (lowered, lowered_cursor) = priority::await_maximum_upsert(&mut watch, a, LONG).await;
    priority::assert_maximum(&lowered, 4, false, "the pushed view of A");
    gql::assert_allowed(&lowered, wire::ACTION_SET_MAX_PRIORITY);
    gql::assert_allowed(&lowered, wire::ACTION_CLEAR_MAX_PRIORITY);
    assert_eq!(
        lowered,
        priority::maximum_of(
            &priority::user_max_priorities(&client, super_user, &[a]).await,
            a
        ),
        "the pushed view is the one the read gives",
    );

    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, a, 4).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "setting the maximum A already has",
    );
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, unwatched, 1).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps a user outside the watched set",
    );

    stream::expect_total_silence(
        &mut watch,
        "an unchanged maximum and an unwatched user push nothing",
        QUIET,
    )
    .await;

    gql::expect_success(
        &priority::clear_user_max_priority(&client, super_user, a).await,
        wire::FIELD_CLEAR_USER_MAX_PRIORITY,
        "a super user clears user A's maximum",
    );

    let (cleared, cleared_cursor) = priority::await_maximum_upsert(&mut watch, a, LONG).await;
    priority::assert_maximum(&cleared, 3, true, "the pushed view of a cleared A");
    gql::assert_allowed(&cleared, wire::ACTION_SET_MAX_PRIORITY);
    assert_eq!(
        gql::assert_blocked(&cleared, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );
    assert_eq!(
        cleared,
        priority::maximum_of(
            &priority::user_max_priorities(&client, super_user, &[a]).await,
            a
        ),
    );
    assert_ne!(
        cleared_cursor, lowered_cursor,
        "each change is delivered under its own cursor"
    );

    drop(watch);
    let mut reconnected = SseSubscription::open(
        fixture.url_of(1),
        super_user,
        &subs_max_priority::user_max_priorities_changed(&[a, b]),
    )
    .await;

    assert_eq!(
        priority::opening_maxima(&mut reconnected, SHORT).await,
        priority::user_max_priorities(&client, super_user, &[a, b]).await,
    );

    for (caller, who) in [
        (fixture::ordinary_member(), "a plain user"),
        (
            impersonating_member(super_user),
            "a member impersonating an administrator",
        ),
        (deactivated_super_user(), "a deactivated super user"),
        (machine_caller(), "a machine passport"),
    ] {
        let header = caller.to_header();
        let (status, body) = client
            .post_raw(
                "/graphql",
                &[
                    ("X-Passport", header.as_str()),
                    ("Accept", "text/event-stream"),
                ],
                json!({ "query": subs_max_priority::user_max_priorities_changed(&[a, b]) }),
            )
            .await;

        assert_eq!(
            (status, subscription_refusal(status, &body, who)),
            (StatusCode::FORBIDDEN, FORBIDDEN.to_owned()),
            "{who}",
        );
        refusal::assert_nothing_leaked(&body, &[a.to_string(), b.to_string()], who);
    }

    fixture.shutdown().await;
}
