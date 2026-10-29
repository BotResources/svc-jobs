//! The administrator's read of users' maximum priorities: the level that
//! applies to each user asked for — their own or the service default, and which
//! of the two — with the backend-owned affordances of setting and clearing it.

mod support;

use br_test_harness::{SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};
use support::fixture::{self, JobsFixture, Knobs, impersonating_member, machine_caller};
use support::{LONG, QUIET, SHORT, gql, priority, stream, subs, wire};
use uuid::Uuid;

const FORBIDDEN: &str = "FORBIDDEN";
const BAD_USER_INPUT: &str = "BAD_USER_INPUT";

#[tokio::test]
async fn administrators_read_users_maximums_with_their_affordances() {
    // Given: JOBS_DEFAULT_MAX_PRIORITY left unset (the default is 3), user A with a maximum of 5
    // and user B with none
    let fixture = JobsFixture::start().await;
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
    let mut own_b =
        SseSubscription::open(fixture.url(), &user_b, &subs::my_max_priority_changed()).await;
    assert_eq!(
        priority::opening_max(&mut own_b, SHORT).await,
        wire::SERVICE_DEFAULT_MAX_PRIORITY
    );

    // When: a platform administrator who is not a super user reads A and B.
    // The Passport carries one administrator bit (is_super_admin): today every platform
    // administrator is a super user, so this caller cannot be forged — the same open question as
    // the 0.4 scenario `only_super_users_force_a_priority_or_set_a_maximum`. The domain decision
    // table (bc-jobs `domain::actions::user`) pins that a platform administrator who is not a super
    // user reads both maxima with setting and clearing refused `not_super_user`.

    // When: a super user reads A and B
    let read = priority::user_max_priorities(&client, super_user, &[a, b]).await;

    // Then: A at 5, its own; B at 3, the default — in the order asked for
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

    // Then: setting is allowed on both, clearing on A only — B has no maximum of their own to clear
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

    // When: the super user clears B's maximum anyway
    let refused = priority::clear_user_max_priority(&client, super_user, b).await;

    // Then: the command refuses with the very code the affordance announced, and nothing moves
    assert_eq!(
        gql::mutation_error_reason(&refused, "clearing a maximum that is not set"),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );
    assert_eq!(
        gql::mutation_error_params(&refused, "clearing a maximum that is not set"),
        json!({ "userId": b.to_string() }),
    );
    stream::expect_total_silence(&mut own_b, "a refused clear changes no maximum", QUIET).await;
    assert_eq!(
        priority::user_max_priorities(&client, super_user, &[a, b]).await,
        read,
        "a refused clear leaves both reads as they were",
    );

    // When: the super user clears A's maximum
    gql::expect_success(
        &priority::clear_user_max_priority(&client, super_user, a).await,
        wire::FIELD_CLEAR_USER_MAX_PRIORITY,
        "a super user clears user A's maximum",
    );

    // Then: A now reads 3, the default, with nothing left to clear; B is untouched
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

    // When: the super user reads a user id Jobs has never heard of
    let unknown = Uuid::now_v7();
    let read_unknown = priority::maximum_of(
        &priority::user_max_priorities(&client, super_user, &[unknown]).await,
        unknown,
    );

    // Then: it reads at the default, settable, with nothing to clear
    priority::assert_maximum(&read_unknown, 3, true, "an unknown user is on the default");
    gql::assert_allowed(&read_unknown, wire::ACTION_SET_MAX_PRIORITY);
    assert_eq!(
        gql::assert_blocked(&read_unknown, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );

    // When: a plain user — user B themselves — and every other caller who is not a platform
    // administrator read A and B
    let hostile = [
        (user_b.clone(), "a plain user"),
        (
            impersonating_member(super_user),
            "a member impersonating an administrator",
        ),
        (machine_caller(), "a machine passport"),
    ];
    for (caller, who) in &hostile {
        let response = priority::user_max_priorities_response(&client, caller, &[a, b]).await;

        // Then: the read is refused under one closed code and carries no maximum at all
        assert_eq!(
            verdict::expect_code_shaped(&response, &format!("{who} reading users' maxima")),
            FORBIDDEN,
            "reading users' maxima is an administrator gesture",
        );
        assert_nothing_read(&response, &[a, b], who);
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn the_maxima_read_answers_each_user_once_and_refuses_malformed_or_oversized_requests() {
    // Given: one user with a maximum of 2
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

    // When: the same users are asked for twice, and nobody is asked for
    let repeated =
        priority::user_max_priorities(&client, super_user, &[other, capped, other, capped]).await;
    let nobody = priority::user_max_priorities(&client, super_user, &[]).await;

    // Then: each user is answered once, in the order first asked for; nobody asked, nobody read
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

    // When: a request names an id that is not a UUIDv7, or more users than one read answers
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

        // Then: the whole read is refused as bad input, with a stable reason and no partial answer
        assert_eq!(verdict::expect_code_shaped(&response, what), BAD_USER_INPUT);
        assert_eq!(
            gql::mutation_error_reason(&response, what),
            reason,
            "{what}"
        );
        assert!(
            response["data"].is_null()
                || response["data"][wire::FIELD_USER_MAX_PRIORITIES].is_null(),
            "{what}: a refused read answers no maximum: {response}"
        );
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn an_administrators_view_of_users_maximums_follows_every_change_live() {
    // Given: two Jobs pods, user A with a maximum of 5, user B with none, and a third user nobody
    // watches. The watcher holds its subscription on the second pod; every gesture goes through
    // the first, so each change reaches the watcher across pods.
    let fixture = JobsFixture::start_cluster(2, Knobs::default()).await;
    let client = fixture.gql();
    let super_user = fixture.admin();
    let (a, b, unwatched) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, a, 5).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user gives user A a maximum of 5",
    );

    // When: a super user subscribes to the maxima of A and B, on the second pod
    let mut watch = SseSubscription::open(
        fixture.url_of(1),
        super_user,
        &subs::user_max_priorities_changed(&[a, b]),
    )
    .await;

    // Then: it opens on a snapshot equal to the read
    let opening = priority::opening_maxima(&mut watch, SHORT).await;
    assert_eq!(
        opening,
        priority::user_max_priorities(&client, super_user, &[a, b]).await,
        "the snapshot is the initial read, affordances included",
    );

    // When: the super user lowers A to 4
    gql::expect_success(
        &priority::set_user_max_priority(&client, super_user, a, 4).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user lowers user A to 4",
    );

    // Then: the complete new view of A is pushed — level, origin and affordances — with no refetch
    let lowered = priority::await_maximum_upsert(&mut watch, a, LONG).await;
    priority::assert_maximum(&lowered, 4, false, "the pushed view of A");
    gql::assert_allowed(&lowered, wire::ACTION_CLEAR_MAX_PRIORITY);
    assert_eq!(
        lowered,
        priority::maximum_of(
            &priority::user_max_priorities(&client, super_user, &[a]).await,
            a
        ),
        "the pushed view is the one the read gives",
    );

    // When: the super user sets A to the level it already has, and caps a user nobody watches
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

    // Then: nothing reaches the watcher — no change of A, nothing of a user it does not watch
    stream::expect_total_silence(
        &mut watch,
        "an unchanged maximum and an unwatched user push nothing",
        QUIET,
    )
    .await;

    // When: the super user clears A's maximum
    gql::expect_success(
        &priority::clear_user_max_priority(&client, super_user, a).await,
        wire::FIELD_CLEAR_USER_MAX_PRIORITY,
        "a super user clears user A's maximum",
    );

    // Then: A is pushed back on the default, its clear affordance now blocked
    let cleared = priority::await_maximum_upsert(&mut watch, a, LONG).await;
    priority::assert_maximum(&cleared, 3, true, "the pushed view of a cleared A");
    assert_eq!(
        gql::assert_blocked(&cleared, wire::ACTION_CLEAR_MAX_PRIORITY),
        wire::REASON_NO_MAX_PRIORITY_SET,
    );

    // When: the watcher reconnects
    drop(watch);
    let mut reconnected = SseSubscription::open(
        fixture.url_of(1),
        super_user,
        &subs::user_max_priorities_changed(&[a, b]),
    )
    .await;

    // Then: it starts over on a fresh snapshot equal to the current read
    assert_eq!(
        priority::opening_maxima(&mut reconnected, SHORT).await,
        priority::user_max_priorities(&client, super_user, &[a, b]).await,
    );

    // When: callers who are not platform administrators try to open the same subscription
    for (caller, who) in [
        (fixture::ordinary_member(), "a plain user"),
        (
            impersonating_member(super_user),
            "a member impersonating an administrator",
        ),
        (machine_caller(), "a machine passport"),
    ] {
        let header = br_core_auth::PassportHeader::to_header(&caller);
        let (status, body) = client
            .post_raw(
                "/graphql",
                &[
                    ("X-Passport", header.as_str()),
                    ("Accept", "text/event-stream"),
                ],
                json!({ "query": subs::user_max_priorities_changed(&[a, b]) }),
            )
            .await;

        // Then: it is refused during establishment and reads nothing
        assert_ne!(
            status,
            StatusCode::OK,
            "{who} is refused the subscription during establishment: {body}"
        );
        assert_nothing_read(&body, &[a, b], who);
    }

    fixture.shutdown().await;
}

fn assert_nothing_read(response: &Value, users: &[Uuid], who: &str) {
    let rendered = response.to_string();
    for user in users {
        assert!(
            !rendered.contains(&user.to_string()),
            "{who}: a refused read leaks no user it was asked about: {response}"
        );
    }
    let data = &response["data"];
    assert!(
        data.is_null()
            || data
                .as_object()
                .is_some_and(|fields| fields.values().all(Value::is_null)),
        "{who}: a refused read returns no data at all: {response}"
    );
}
