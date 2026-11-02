mod support;

use br_core_auth::PassportHeader;
use br_test_harness::{SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};
use support::events::EventLog;
use support::fixture::{self, JobsFixture, impersonating_member, machine_caller};
use support::producer::{JobDeclaration, Producer};
use support::subs_max_priority;
use support::{
    JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, docs, gql, priority, refusal, stream, subs, wire,
};
use uuid::Uuid;

const FORBIDDEN: &str = "FORBIDDEN";
const UNAUTHENTICATED: &str = "UNAUTHENTICATED";

#[tokio::test]
async fn only_super_users_force_a_priority_or_set_a_maximum() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let member = fixture.member();
    let runner_type = wire::unique_runner_type("guarded");
    let producer = Producer::new(fixture.fabric(), "projects");

    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, member.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps the member at 2",
    );
    let declaration = JobDeclaration::new(&runner_type)
        .triggered_by_user(member.actor_id())
        .with_priority(4);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let mut listing = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_with_priority(&runner_type),
    )
    .await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut own = SseSubscription::open(
        fixture.url(),
        member,
        &subs_max_priority::my_max_priority_changed(),
    )
    .await;
    assert_eq!(priority::opening_max(&mut own, SHORT).await, 2);
    let before = priority::job_view(&client, admin, job_id).await;
    priority::assert_priorities(&before["job"], 4, 2, None, "before the attack");

    let hostile = [
        (member.clone(), "the capped user themselves"),
        (fixture::ordinary_member(), "another ordinary user"),
        (
            impersonating_member(admin),
            "a member impersonating an administrator",
        ),
        (machine_caller(), "a machine passport"),
    ];
    let mut codes = Vec::new();
    for (caller, who) in &hostile {
        for (target_job, target_user, existence) in [
            (job_id, member.actor_id(), "a known"),
            (Uuid::now_v7(), Uuid::now_v7(), "an unknown"),
        ] {
            for (refused, action) in [
                (
                    priority::force_job_priority(&client, caller, target_job, 5).await,
                    "forcing",
                ),
                (
                    priority::set_user_max_priority(&client, caller, target_user, 5).await,
                    "setting a maximum of",
                ),
                (
                    priority::clear_user_max_priority(&client, caller, target_user).await,
                    "clearing a maximum of",
                ),
            ] {
                let what = format!("{who} {action} {existence} target");
                codes.push(verdict::expect_code_shaped(&refused, &what));
                assert_nothing_leaked(&refused, job_id, &what);
            }
        }
    }
    for code in &codes {
        assert_eq!(
            code, FORBIDDEN,
            "one stable authorization code, whatever the action and whether the target exists: \
             {codes:?}",
        );
    }

    for forged in [None, Some("not-a-passport")] {
        for (document, input) in [
            (
                docs::FORCE_JOB_PRIORITY,
                json!({ "jobId": job_id.to_string(), "priority": 5 }),
            ),
            (
                docs::SET_USER_MAX_PRIORITY,
                json!({ "userId": member.actor_id().to_string(), "maxPriority": 5 }),
            ),
            (
                docs::CLEAR_USER_MAX_PRIORITY,
                json!({ "userId": member.actor_id().to_string() }),
            ),
        ] {
            let mut headers: Vec<(&str, &str)> = Vec::new();
            if let Some(value) = forged {
                headers.push(("X-Passport", value));
            }
            let (status, body) = client
                .post_raw(
                    "/graphql",
                    &headers,
                    json!({ "query": document, "variables": { "input": input } }),
                )
                .await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "closed at the trust boundary: {body}"
            );
            assert_eq!(
                verdict::mutation_error_code(&body).as_deref(),
                Some(UNAUTHENTICATED),
                "a missing and an undecodable passport are one closed door: {body}",
            );
            assert_nothing_leaked(&body, job_id, "an unauthenticated priority mutation");
        }
    }

    stream::expect_total_silence(&mut watch, "a refused caller reaches no job stream", QUIET).await;
    stream::expect_total_silence(&mut listing, "a refused caller reaches no list", QUIET).await;
    stream::expect_total_silence(&mut own, "a refused caller changes no maximum", QUIET).await;
    assert_eq!(priority::job_view(&client, admin, job_id).await, before);
    assert_eq!(priority::my_max_priority(&client, member).await, 2);
    events
        .expect_exactly(wire::FACT_QUEUED, job_id, 1, QUIET)
        .await;

    let supplied = client
        .query(
            member,
            docs::MY_MAX_PRIORITY_FOR_SUPPLIED_USER,
            json!({ "userId": admin.actor_id().to_string() }),
        )
        .await;
    assert!(
        !supplied["errors"].is_null() && supplied["data"].is_null(),
        "the user is the caller of the passport and can never be supplied: {supplied}",
    );
    let member_job_read = client
        .query(
            member,
            docs::JOB_DETAIL,
            json!({ "id": job_id.to_string() }),
        )
        .await;
    assert_eq!(
        verdict::expect_code_shaped(&member_job_read, "a user reading a job"),
        FORBIDDEN,
        "the user surface exposes no job, not even one running on the user's behalf",
    );
    assert_nothing_leaked(&member_job_read, job_id, "a user reading a job");

    let machine = machine_caller();
    let machine_read = priority::my_max_priority_response(&client, &machine).await;
    assert_eq!(
        verdict::expect_code_shaped(&machine_read, "a service account reading a maximum"),
        FORBIDDEN,
        "service accounts have no access to the user surface",
    );
    assert!(
        machine_read["data"].is_null() || machine_read["data"]["jobsMyMaxPriority"].is_null(),
        "a refused read carries no maximum: {machine_read}",
    );
    let machine_header = machine.to_header();
    let (status, body) = client
        .post_raw(
            "/graphql",
            &[
                ("X-Passport", machine_header.as_str()),
                ("Accept", "text/event-stream"),
            ],
            json!({ "query": subs_max_priority::my_max_priority_changed() }),
        )
        .await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a service account is refused the user subscription during establishment: {body}",
    );

    events.stop().await;
    fixture.shutdown().await;
}

fn assert_nothing_leaked(response: &Value, job_id: Uuid, what: &str) {
    refusal::assert_nothing_leaked(response, &[job_id.to_string()], what);
}
