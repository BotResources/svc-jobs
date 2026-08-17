use std::time::Duration;

use br_test_harness::{TestNats, recreate_stream};
use serde_json::{Value, json};

use super::wire;

pub const PRESENCE_TTL: Duration = Duration::from_secs(4);
pub const PRESENCE_MARKER_TTL: Duration = Duration::from_secs(1);
pub const PRESENCE_REFRESH: Duration = Duration::from_secs(1);

pub async fn provision_runner_transport(nats: &TestNats) {
    let js = nats.jetstream();
    recreate_stream(js, wire::TRIGGER_STREAM, &[wire::TRIGGER_BIND]).await;
    recreate_stream(js, wire::STATUS_STREAM, &[wire::STATUS_BIND]).await;
    recreate_stream(js, wire::LOG_STREAM, &[wire::LOG_BIND]).await;
    nats.create_kv(wire::CANCEL_BUCKET).await;
    declare_expiring_presence_bucket(nats).await;
}

pub async fn evict_presence_entry_unnoticed(
    nats: &TestNats,
    runner_type: &str,
    instance_key: &str,
) {
    let purged = jetstream_api(
        nats,
        &format!("$JS.API.STREAM.PURGE.KV_{}", wire::PRESENCE_BUCKET),
        json!({ "filter": format!("$KV.{}.{}", wire::PRESENCE_BUCKET, wire::presence_key(runner_type, instance_key)) }),
    )
    .await;
    assert!(
        purged["error"].is_null(),
        "the entry must leave the bucket without a delete marker, which is the one thing a \
         watching service never learns about: an eviction it was not there to see. {purged}"
    );
}

async fn declare_expiring_presence_bucket(nats: &TestNats) {
    let stream = format!("KV_{}", wire::PRESENCE_BUCKET);
    jetstream_api(nats, &format!("$JS.API.STREAM.DELETE.{stream}"), json!({})).await;
    let created = jetstream_api(
        nats,
        &format!("$JS.API.STREAM.CREATE.{stream}"),
        json!({
            "name": stream,
            "subjects": [format!("$KV.{}.>", wire::PRESENCE_BUCKET)],
            "retention": "limits",
            "discard": "new",
            "storage": "file",
            "num_replicas": 1,
            "max_msgs_per_subject": 1,
            "max_age": nanos(PRESENCE_TTL),
            "allow_rollup_hdrs": true,
            "deny_delete": true,
            "deny_purge": false,
            "allow_direct": true,
            "allow_msg_ttl": true,
            "subject_delete_marker_ttl": nanos(PRESENCE_MARKER_TTL),
        }),
    )
    .await;
    assert!(
        created["error"].is_null(),
        "the presence bucket is expiring live state: an entry its instance stops refreshing must \
         die on its own, because TTL eviction is the disconnection signal a crashed instance ever \
         sends. Declared without max_age, a dead instance stays live for ever: {created}"
    );
}

async fn jetstream_api(nats: &TestNats, subject: &str, request: Value) -> Value {
    let payload = serde_json::to_vec(&request).expect("a jetstream api request serializes");
    let reply = nats
        .client()
        .request(subject.to_string(), payload.into())
        .await
        .unwrap_or_else(|error| panic!("jetstream api call {subject}: {error}"));
    serde_json::from_slice(&reply.payload)
        .unwrap_or_else(|error| panic!("a jetstream api reply is JSON ({subject}): {error}"))
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("a test-scale duration fits in nanoseconds")
}
