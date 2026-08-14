use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::{Value, json};

pub async fn snapshot(watch: &mut SseSubscription, field: &str, timeout: Duration) -> Value {
    let event = watch.expect_event(field, timeout).await;
    let message = event[field].clone();
    assert!(
        !message.is_null(),
        "the first subscription message must carry '{field}': {event}"
    );
    let kind = message["__typename"].as_str().unwrap_or_default();
    assert!(
        kind.ends_with("Snapshot"),
        "a subscription opens on a snapshot equivalent to its initial read, got '{kind}': {message}"
    );
    message
}

#[must_use]
pub async fn await_delta(
    watch: &mut SseSubscription,
    field: &str,
    event_type: &str,
    timeout: Duration,
) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen: Vec<String> = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "no {event_type} delta on '{field}' within {timeout:?}; deltas seen: {seen:?}"
        );
        let Some(event) = watch.next_event(remaining).await else {
            panic!("no {event_type} delta on '{field}' within {timeout:?}; deltas seen: {seen:?}");
        };
        let message = event[field].clone();
        let carried = message["event"]["__typename"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if carried == event_type {
            return message;
        }
        if !carried.is_empty() {
            seen.push(carried);
        }
    }
}

pub async fn await_message(
    watch: &mut SseSubscription,
    field: &str,
    what: &str,
    mut predicate: impl FnMut(&Value) -> bool,
    timeout: Duration,
) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen: Vec<Value> = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "no message matching {what} on '{field}' within {timeout:?}; seen: {seen:?}"
        );
        let Some(event) = watch.next_event(remaining).await else {
            panic!("no message matching {what} on '{field}' within {timeout:?}; seen: {seen:?}");
        };
        let message = event[field].clone();
        if predicate(&message) {
            return message;
        }
        seen.push(message);
    }
}

pub async fn expect_no_delta(
    watch: &mut SseSubscription,
    field: &str,
    event_type: &str,
    quiet: Duration,
) {
    let deadline = tokio::time::Instant::now() + quiet;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        let Some(event) = watch.next_event(remaining).await else {
            return;
        };
        let message = event[field].clone();
        assert_ne!(
            message["event"]["__typename"],
            json!(event_type),
            "no {event_type} may reach a subscriber here: {message}"
        );
    }
}

#[must_use]
pub async fn drain_appended(
    watch: &mut SseSubscription,
    field: &str,
    wanted: usize,
    timeout: Duration,
) -> Vec<Value> {
    let mut collected = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while collected.len() < wanted {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "expected {wanted} appended messages on '{field}', got {}",
            collected.len()
        );
        let Some(event) = watch.next_event(remaining).await else {
            panic!(
                "expected {wanted} appended messages on '{field}', the stream ended after {}",
                collected.len()
            );
        };
        collected.push(event[field].clone());
    }
    collected
}

pub async fn await_fleet_event(
    watch: &mut SseSubscription,
    kind: &str,
    timeout: Duration,
) -> Value {
    await_message(
        watch,
        super::FLEET_CHANGED,
        kind,
        |message| message["event"]["kind"] == json!(kind),
        timeout,
    )
    .await
}

pub async fn expect_no_fleet_event(watch: &mut SseSubscription, kind: &str, quiet: Duration) {
    let deadline = tokio::time::Instant::now() + quiet;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        let Some(event) = watch.next_event(remaining).await else {
            return;
        };
        let message = event[super::FLEET_CHANGED].clone();
        assert_ne!(
            message["event"]["kind"],
            json!(kind),
            "no {kind} fleet event may reach a subscriber here: {message}"
        );
    }
}
