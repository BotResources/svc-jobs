use std::time::Duration;

use br_test_harness::SseSubscription;
use serde_json::{Value, json};
use tokio::time::Instant;
use uuid::Uuid;

const STREAM_END_MARGIN_CEILING: Duration = Duration::from_millis(500);
const STREAM_END_MARGIN_DIVISOR: u32 = 4;

pub fn margin_for(timeout: Duration) -> Duration {
    (timeout / STREAM_END_MARGIN_DIVISOR).min(STREAM_END_MARGIN_CEILING)
}

pub fn is_stream_end(timeout: Duration, remaining: Duration) -> bool {
    remaining > margin_for(timeout)
}

struct Budget {
    started: Instant,
    deadline: Instant,
    timeout: Duration,
}

impl Budget {
    fn new(timeout: Duration) -> Self {
        let started = Instant::now();
        Self {
            started,
            deadline: started + timeout,
            timeout,
        }
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    fn ended_before_the_budget_ran_out(&self) -> bool {
        is_stream_end(self.timeout, self.remaining())
    }

    fn ended_early(&self, waiting_for: &str) {
        if self.ended_before_the_budget_ran_out() {
            panic!(
                "the service ended the subscription after {:?} while waiting for {waiting_for} — \
                 {:?} of the {:?} budget was still unspent, so this is a closed stream and not a \
                 timeout. A subscriber whose stream ends mid-scenario observes nothing further: \
                 every presence it would have proved is lost and every absence it would have \
                 proved is vacuous",
                self.started.elapsed(),
                self.remaining(),
                self.timeout,
            );
        }
    }
}

pub async fn expect_total_silence(watch: &mut SseSubscription, what: &str, quiet: Duration) {
    let budget = Budget::new(quiet);
    if let Some(event) = watch.next_event(quiet).await {
        panic!("{what}: no message at all may reach this subscriber here, got: {event}");
    }
    budget.ended_early(&format!("{quiet:?} of total silence — {what}"));
}

pub async fn snapshot(watch: &mut SseSubscription, field: &str, timeout: Duration) -> Value {
    let message = watch.expect_event_on(field, timeout).await;
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
    let budget = Budget::new(timeout);
    let mut seen: Vec<String> = Vec::new();
    loop {
        let remaining = budget.remaining();
        assert!(
            !remaining.is_zero(),
            "no {event_type} delta on '{field}' within {timeout:?}; deltas seen: {seen:?}"
        );
        let Some(event) = watch.next_event(remaining).await else {
            budget.ended_early(&format!("a {event_type} delta on '{field}'"));
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

#[must_use]
pub async fn await_delta_of(
    watch: &mut SseSubscription,
    field: &str,
    event_type: &str,
    job_id: Uuid,
    timeout: Duration,
) -> Value {
    await_message(
        watch,
        field,
        &format!("{event_type} for job {job_id}"),
        |message| {
            message["event"]["__typename"] == json!(event_type)
                && message["event"]["jobId"] == json!(job_id.to_string())
        },
        timeout,
    )
    .await
}

pub async fn await_message(
    watch: &mut SseSubscription,
    field: &str,
    what: &str,
    mut predicate: impl FnMut(&Value) -> bool,
    timeout: Duration,
) -> Value {
    let budget = Budget::new(timeout);
    let mut seen: Vec<Value> = Vec::new();
    loop {
        let remaining = budget.remaining();
        assert!(
            !remaining.is_zero(),
            "no message matching {what} on '{field}' within {timeout:?}; seen: {seen:?}"
        );
        let Some(event) = watch.next_event(remaining).await else {
            budget.ended_early(&format!("a message matching {what} on '{field}'"));
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
    expect_no_message(
        watch,
        field,
        &format!("{event_type} delta"),
        |message| message["event"]["__typename"] == json!(event_type),
        quiet,
    )
    .await;
}

pub async fn expect_no_delta_of(
    watch: &mut SseSubscription,
    field: &str,
    event_type: &str,
    job_id: Uuid,
    quiet: Duration,
) {
    expect_no_message(
        watch,
        field,
        &format!("{event_type} delta for job {job_id}"),
        |message| {
            message["event"]["__typename"] == json!(event_type)
                && message["event"]["jobId"] == json!(job_id.to_string())
        },
        quiet,
    )
    .await;
}

async fn expect_no_message(
    watch: &mut SseSubscription,
    field: &str,
    what: &str,
    mut forbidden: impl FnMut(&Value) -> bool,
    quiet: Duration,
) {
    let budget = Budget::new(quiet);
    loop {
        let remaining = budget.remaining();
        if remaining.is_zero() {
            return;
        }
        let Some(event) = watch.next_event(remaining).await else {
            budget.ended_early(&format!("{quiet:?} of silence about {what} on '{field}'"));
            return;
        };
        let message = event[field].clone();
        assert!(
            !forbidden(&message),
            "no {what} may reach a subscriber on '{field}' here: {message}"
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
    let budget = Budget::new(timeout);
    while collected.len() < wanted {
        let remaining = budget.remaining();
        assert!(
            !remaining.is_zero(),
            "expected {wanted} appended messages on '{field}', got {}",
            collected.len()
        );
        let Some(event) = watch.next_event(remaining).await else {
            budget.ended_early(&format!(
                "appended message {} of {wanted} on '{field}'",
                collected.len() + 1
            ));
            panic!(
                "expected {wanted} appended messages on '{field}', the stream stopped delivering \
                 after {}",
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
    expect_no_message(
        watch,
        super::FLEET_CHANGED,
        &format!("{kind} fleet event"),
        |message| message["event"]["kind"] == json!(kind),
        quiet,
    )
    .await;
}
