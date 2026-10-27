use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, wait_until};
use serde_json::{Value, json};

use super::docs::{
    CLEAR_RUNNER_TYPE_MAX_RUN_DURATION, FLEET_DISPATCH, SET_RUNNER_TYPE_DISPATCH_MODE,
    SET_RUNNER_TYPE_MAX_RUN_DURATION,
};
use super::{gql, wire};

pub async fn set_dispatch_mode(
    client: &GraphqlClient,
    passport: &Passport,
    runner_type: &str,
    mode: &str,
) -> Value {
    client
        .query(
            passport,
            SET_RUNNER_TYPE_DISPATCH_MODE,
            json!({ "input": { "runnerType": runner_type, "dispatchMode": mode } }),
        )
        .await
}

pub async fn set_max_run_duration(
    client: &GraphqlClient,
    passport: &Passport,
    runner_type: &str,
    seconds: i64,
) -> Value {
    client
        .query(
            passport,
            SET_RUNNER_TYPE_MAX_RUN_DURATION,
            json!({ "input": { "runnerType": runner_type, "maxRunDurationSeconds": seconds } }),
        )
        .await
}

pub async fn clear_max_run_duration(
    client: &GraphqlClient,
    passport: &Passport,
    runner_type: &str,
) -> Value {
    client
        .query(
            passport,
            CLEAR_RUNNER_TYPE_MAX_RUN_DURATION,
            json!({ "input": { "runnerType": runner_type } }),
        )
        .await
}

pub async fn fleet_views(
    client: &GraphqlClient,
    admin: &Passport,
    runner_type: &str,
) -> Vec<Value> {
    let response = client
        .query(admin, FLEET_DISPATCH, json!({ "runnerType": runner_type }))
        .await;
    gql::data_of(&response, "jobsFleet")
        .as_array()
        .unwrap_or_else(|| panic!("the fleet read returns a list: {response}"))
        .clone()
}

pub async fn runner_type_view(
    client: &GraphqlClient,
    admin: &Passport,
    runner_type: &str,
) -> Value {
    let views = fleet_views(client, admin, runner_type).await;
    assert_eq!(
        views.len(),
        1,
        "the fleet read answers for exactly the runner type it was asked about: {views:?}"
    );
    views[0].clone()
}

pub async fn await_live_instances(
    client: &GraphqlClient,
    admin: &Passport,
    runner_type: &str,
    expected: usize,
    timeout: Duration,
) {
    let live = wait_until(timeout, || async {
        fleet_views(client, admin, runner_type)
            .await
            .first()
            .and_then(|view| view["runnerType"]["instances"].as_array().map(Vec::len))
            == Some(expected)
    })
    .await;
    assert!(
        live,
        "runner type '{runner_type}' never showed {expected} live instances within {timeout:?}: {:?}",
        fleet_views(client, admin, runner_type).await
    );
}

pub async fn switch_to_claim(client: &GraphqlClient, admin: &Passport, runner_type: &str) {
    gql::expect_success(
        &set_dispatch_mode(client, admin, runner_type, wire::MODE_CLAIM).await,
        wire::FIELD_SET_DISPATCH_MODE,
        "a platform administrator switches a registered runner type to CLAIM",
    );
    assert_eq!(
        runner_type_view(client, admin, runner_type).await["runnerType"]["dispatchMode"],
        json!(wire::MODE_CLAIM),
        "an acknowledged switch is already the state the next read answers with",
    );
}
