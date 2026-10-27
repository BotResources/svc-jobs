use std::collections::BTreeSet;
use std::time::Duration;

use br_test_harness::TestNats;
use contract_jobs::runner::ClaimReply;
use contract_jobs::runner_transport::claim_subject;
use contract_jobs::segment::SubjectSegment;
use serde_json::{Value, json};
use tokio::time::Instant;
use uuid::Uuid;

use super::runner::FakeRunner;
use super::wire;

/// How much longer than its declared wait an instance listens for the answer. Jobs owes a reply
/// at the end of the wait at the latest, so a claim still unanswered after this margin is a claim
/// Jobs dropped — never a slow empty reply.
pub const REPLY_MARGIN: Duration = Duration::from_secs(2);

/// The harness connection answers a core request within this ceiling at most. Every wait a
/// scenario declares stays below it, so the instance's own timeout is the one that fires.
const CLIENT_REQUEST_CEILING: Duration = Duration::from_secs(10);

const TRIGGER_FIELDS: [&str; 6] = [
    "version",
    "run_id",
    "job_id",
    "config",
    "attempt",
    "triggered_by",
];

pub struct ClaimAnswer {
    pub claim_id: Uuid,
    pub reply: Value,
    pub elapsed: Duration,
}

impl ClaimAnswer {
    pub fn expect_run(&self, what: &str) -> Value {
        assert!(
            self.reply["reason"].is_null(),
            "{what}: a claim served with a run carries no refusal reason: {}",
            self.reply
        );
        let run = self.reply["run"].clone();
        assert!(
            run.is_object(),
            "{what}: a job was waiting, so the claim is answered with the run reserved to this \
             instance: {}",
            self.reply
        );
        assert_trigger_shaped(&run, what);
        run
    }

    pub fn expect_empty(&self, what: &str) {
        assert!(
            self.reply.get("run").is_none_or(Value::is_null),
            "{what}: nothing waited during the claim, so the reply carries no run: {}",
            self.reply
        );
        assert!(
            self.reply.get("reason").is_none_or(Value::is_null),
            "{what}: an expired wait is not a refusal — the reply is empty, without a reason: {}",
            self.reply
        );
    }

    pub fn expect_refused(&self, reason: &str, what: &str) {
        assert!(
            self.reply.get("run").is_none_or(Value::is_null),
            "{what}: a refused claim reserves nothing: {}",
            self.reply
        );
        assert_eq!(
            self.reply["reason"],
            json!(reason),
            "{what}: a refused claim names its stable reason: {}",
            self.reply
        );
    }

    /// A claim the pod has no room to hold: refused `busy`, with a positive hint of when to retry.
    pub fn expect_busy(&self, what: &str) -> u64 {
        self.expect_refused(wire::REASON_BUSY, what);
        let retry_after_ms = self.reply["retry_after_ms"].as_u64().unwrap_or_else(|| {
            panic!(
                "{what}: a busy refusal carries its retry_after_ms: {}",
                self.reply
            )
        });
        assert!(
            retry_after_ms > 0,
            "{what}: a busy refusal tells the instance to wait before retrying: {}",
            self.reply
        );
        retry_after_ms
    }

    pub fn expect_answered_at_once(&self, wait: Duration, what: &str) {
        assert!(
            self.elapsed < wait / 2,
            "{what}: Jobs reserves a waiting job as soon as it can and replies then, it never sits \
             on a served claim until the wait expires — answered after {:?} of a {wait:?} wait",
            self.elapsed
        );
    }

    pub fn expect_held_for_the_wait(&self, wait: Duration, what: &str) {
        assert!(
            self.elapsed + REPLY_MARGIN >= wait,
            "{what}: an unanswered claim is held for the bounded wait the instance declared, not \
             answered empty at once — answered after {:?} of a {wait:?} wait",
            self.elapsed
        );
    }
}

pub fn subject(runner_type: &str) -> String {
    let hand_built = wire::claim_subject(runner_type);
    let segment = SubjectSegment::runner_type(runner_type)
        .unwrap_or_else(|error| panic!("'{runner_type}' is a valid subject segment: {error:?}"));
    assert_eq!(
        claim_subject(&segment),
        hand_built,
        "the claim subject is a frozen offer, not an implementation detail: every runner harness \
         renders 'jobs.claim.{{runner_type}}' itself, so a renamed subject strands every CLAIM \
         instance at once",
    );
    hand_built
}

pub fn request(instance_key: &str, claim_id: Uuid, wait: Duration) -> Vec<u8> {
    let wait_ms = u64::try_from(wait.as_millis()).expect("a test-scale wait fits in milliseconds");
    serde_json::to_vec(&json!({
        "version": 1,
        "instance_key": instance_key,
        "claim_id": claim_id.to_string(),
        "wait_ms": wait_ms,
    }))
    .expect("a claim request serializes")
}

/// One free slot asks for its next run: a fresh claim id per slot.
pub async fn claim(nats: &TestNats, runner: &FakeRunner<'_>, wait: Duration) -> ClaimAnswer {
    claim_again(nats, runner, Uuid::now_v7(), wait).await
}

/// A free slot keeps claiming — each expired claim is a new one — until it is served, or `within`
/// runs out. Every miss must be a plain empty reply: a refusal ends the wait at once.
pub async fn claim_until_served(
    nats: &TestNats,
    runner: &FakeRunner<'_>,
    wait: Duration,
    within: Duration,
    what: &str,
) -> Value {
    let deadline = Instant::now() + within;
    loop {
        let answer = claim(nats, runner, wait).await;
        if answer.reply["run"].is_object() {
            return answer.expect_run(what);
        }
        answer.expect_empty(what);
        assert!(
            Instant::now() < deadline,
            "{what}: no claim was served within {within:?} — the job never waited again"
        );
    }
}

/// The same slot asks again under the claim id it already used.
pub async fn claim_again(
    nats: &TestNats,
    runner: &FakeRunner<'_>,
    claim_id: Uuid,
    wait: Duration,
) -> ClaimAnswer {
    claim_as(
        nats,
        &runner.runner_type,
        &runner.instance_key,
        claim_id,
        wait,
    )
    .await
}

pub async fn claim_as(
    nats: &TestNats,
    runner_type: &str,
    instance_key: &str,
    claim_id: Uuid,
    wait: Duration,
) -> ClaimAnswer {
    exchange(
        nats,
        runner_type,
        &format!("instance '{instance_key}'"),
        claim_id,
        request(instance_key, claim_id, wait),
        wait,
    )
    .await
}

/// Sends bytes that are not a well-formed claim, as a buggy runner harness would.
pub async fn claim_raw(
    nats: &TestNats,
    runner_type: &str,
    payload: Vec<u8>,
    wait: Duration,
) -> ClaimAnswer {
    exchange(
        nats,
        runner_type,
        "a malformed request",
        Uuid::nil(),
        payload,
        wait,
    )
    .await
}

async fn exchange(
    nats: &TestNats,
    runner_type: &str,
    who: &str,
    claim_id: Uuid,
    payload: Vec<u8>,
    wait: Duration,
) -> ClaimAnswer {
    let bound = wait + REPLY_MARGIN;
    assert!(
        bound < CLIENT_REQUEST_CEILING,
        "a scenario declares a wait the harness connection can outlast ({bound:?})"
    );
    let started = Instant::now();
    let answered = tokio::time::timeout(
        bound,
        nats.client().request(subject(runner_type), payload.into()),
    )
    .await;
    let elapsed = started.elapsed();
    let message = match answered {
        Ok(Ok(message)) => message,
        Ok(Err(error)) => {
            panic!("the claim of {who} on '{runner_type}' found no answering Jobs: {error}")
        }
        Err(_) => panic!(
            "the claim of {who} on '{runner_type}' was never answered within {bound:?} — a claim \
             is answered with a run or, at the end of its wait, empty; silence leaves the instance \
             unable to tell a lost reply from a lost request"
        ),
    };
    let reply: Value = serde_json::from_slice(&message.payload)
        .unwrap_or_else(|error| panic!("a claim reply is JSON: {error}"));
    assert_eq!(
        reply["version"],
        json!(1),
        "a claim reply names its wire version: {reply}"
    );
    let typed: ClaimReply = serde_json::from_value(reply.clone()).unwrap_or_else(|error| {
        panic!("the reply reads back as the published ClaimReply: {error} — {reply}")
    });
    assert_eq!(
        typed.run.map(|run| run.run_id.to_string()),
        reply["run"]["run_id"].as_str().map(str::to_owned),
        "the typed contract and the wire agree on the run carried: {reply}"
    );
    ClaimAnswer {
        claim_id,
        reply,
        elapsed,
    }
}

pub async fn claim_and_lose_the_reply(
    nats: &TestNats,
    runner: &FakeRunner<'_>,
    claim_id: Uuid,
    wait: Duration,
) {
    let nobody_listens = format!("_INBOX.lost.{}", Uuid::now_v7().simple());
    nats.client()
        .publish_with_reply(
            subject(&runner.runner_type),
            nobody_listens,
            request(&runner.instance_key, claim_id, wait).into(),
        )
        .await
        .expect("publishing a claim whose reply nobody reads");
    nats.client()
        .flush()
        .await
        .expect("flushing the claim whose reply is lost");
}

fn assert_trigger_shaped(run: &Value, what: &str) {
    let fields: BTreeSet<&str> = run
        .as_object()
        .unwrap_or_else(|| panic!("{what}: a served run is an object: {run}"))
        .keys()
        .map(String::as_str)
        .collect();
    let allowed: BTreeSet<&str> = TRIGGER_FIELDS.into_iter().collect();
    assert!(
        fields.is_subset(&allowed),
        "{what}: a claim reply carries what a trigger carries, nothing more: {run}"
    );
    for required in ["run_id", "job_id", "attempt"] {
        assert!(
            fields.contains(required),
            "{what}: a served run carries its '{required}' exactly as a trigger does: {run}"
        );
    }
    for id in ["run_id", "job_id"] {
        let raw = run[id].as_str().unwrap_or_default();
        assert!(
            Uuid::parse_str(raw).is_ok_and(|parsed| parsed.get_version_num() == 7),
            "{what}: '{id}' is a UUIDv7: {run}"
        );
    }
}
