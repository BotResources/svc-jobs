use br_test_harness::GraphqlClient;

use super::LONG;

const CLAIMS_HELD: &str = "jobs_claims_held";

pub async fn claims_held(client: &GraphqlClient, runner_type: &str) -> f64 {
    let (status, body) = client.get_raw("/metrics").await;
    assert!(status.is_success(), "/metrics answers: {status}");
    let label = format!("runner_type=\"{runner_type}\"");
    body.lines()
        .filter(|line| line.starts_with(CLAIMS_HELD) && line.contains(&label))
        .find_map(|line| line.rsplit(' ').next()?.parse().ok())
        .unwrap_or(0.0)
}

pub async fn await_claims_held(client: &GraphqlClient, runner_type: &str, expected: f64) {
    let reached = br_test_harness::wait_until(LONG, || async {
        (claims_held(client, runner_type).await - expected).abs() < f64::EPSILON
    })
    .await;
    assert!(
        reached,
        "the pod never held {expected} claims of {runner_type}: the interleaving this scenario \
         needs never happened and it would prove nothing"
    );
}

const TREE_LOCK_TIMEOUTS: &str = "jobs_tree_lock_timeouts_total";

pub async fn tree_lock_timeouts(client: &GraphqlClient) -> f64 {
    let (status, body) = client.get_raw("/metrics").await;
    assert!(status.is_success(), "/metrics answers: {status}");
    body.lines()
        .find(|line| line.starts_with(TREE_LOCK_TIMEOUTS))
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
        .unwrap_or_else(|| panic!("the series {TREE_LOCK_TIMEOUTS} exists from boot: {body}"))
}

pub async fn await_tree_lock_timeouts_above(client: &GraphqlClient, floor: f64) {
    let reached =
        br_test_harness::wait_until(LONG, || async { tree_lock_timeouts(client).await > floor })
            .await;
    assert!(
        reached,
        "no write ever timed out on the Tree lock: the interleaving this scenario needs never \
         happened and it would prove nothing"
    );
}
