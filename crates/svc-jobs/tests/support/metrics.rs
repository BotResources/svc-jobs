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
