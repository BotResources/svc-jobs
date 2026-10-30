mod support;

use br_core_auth::PassportHeader;
use br_test_harness::{PassportBuilder, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};
use support::fixture::{JobsFixture, machine_caller, runner_access};
use support::gesture::{self, Work};
use support::refusal::unauthenticated_refusal;
use support::{docs, refusal};
use uuid::Uuid;

fn unknown_work() -> Work {
    Work {
        job_id: Uuid::now_v7(),
        run_id: Uuid::now_v7(),
        instance_key: "instance-a".to_owned(),
    }
}

fn mixed_document() -> String {
    "query($work: JobsWorkInput!, $id: UUID!) { \
        jobsUnread(work: $work) { success } \
        jobsJob(id: $id) { job { id } } }"
        .to_owned()
}

#[tokio::test]
async fn the_runner_surface_admits_the_holders_of_the_runner_scope_alone() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let work = unknown_work();

    let (status, body) = client
        .post_raw(
            "/graphql",
            &[],
            json!({ "query": gesture::UNREAD, "variables": { "work": work.wire() } }),
        )
        .await;
    unauthenticated_refusal(status, &body, "a runner gesture with no passport");

    let inactive = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .active(false)
        .claim("scopes", vec!["jobs:runner"])
        .build();
    let human_with_scope = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .claim("scopes", vec!["jobs:runner"])
        .build();
    let super_user_with_scope = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .claim("scopes", vec!["jobs:runner"])
        .build();
    let other_scope = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .claim("scopes", vec!["threads:runner"])
        .build_service();
    for (caller, what) in [
        (fixture.admin().clone(), "a platform administrator"),
        (fixture.member().clone(), "an ordinary member"),
        (machine_caller(), "a service without the scope"),
        (other_scope, "a service holding another scope"),
        (inactive, "an inactive holder of the scope"),
        (human_with_scope, "a human holding the scope"),
        (super_user_with_scope, "a super user holding the scope"),
    ] {
        let refused = gesture::call(
            &client,
            &caller,
            gesture::UNREAD,
            json!({ "work": work.wire() }),
        )
        .await;
        refusal::assert_nothing_leaked(&refused, &[work.run_id.to_string()], what);
        verdict::expect_code_shaped(&refused, what);
    }

    let holder = gesture::unread(&client, &runner_access(), &work, None).await;
    assert_eq!(gesture::refusal(&holder, "jobsUnread"), "run_unknown");
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_request_mixing_runner_fields_with_others_is_refused_whoever_sends_it() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let work = unknown_work();
    let variables = json!({ "work": work.wire(), "id": Uuid::now_v7().to_string() });

    for (caller, what) in [
        (runner_access(), "a holder of the runner scope"),
        (fixture.admin().clone(), "a platform administrator"),
    ] {
        let refused = client
            .query(&caller, &mixed_document(), variables.clone())
            .await;
        verdict::expect_code_shaped(&refused, what);
        assert!(
            refused["data"].is_null() || refused["data"].as_object().is_some_and(|d| d.is_empty()),
            "{what}: no part of a mixed request is answered: {refused}"
        );
    }

    let admin_field_as_runner = client
        .query(
            &runner_access(),
            docs::JOB_DETAIL,
            json!({ "id": Uuid::now_v7().to_string() }),
        )
        .await;
    verdict::expect_code_shaped(
        &admin_field_as_runner,
        "an administrator field with the scope",
    );

    let (status, _): (StatusCode, Value) = client
        .post_raw(
            "/graphql",
            &[
                ("X-Passport", &runner_access().to_header()),
                ("Accept", "text/event-stream"),
            ],
            json!({ "query": "subscription { jobsChanged(window: {first: 1}) { __typename } }" }),
        )
        .await;
    assert_ne!(status, StatusCode::OK, "the scope opens no subscription");
    fixture.shutdown().await;
}
