use std::time::Duration;

use br_test_harness::{FabricTestNats, wait_until};
use br_util_nats_fabric::KvKey;
use contract_jobs::catalog::{
    RunnerType as PublishedRunnerType, RunnerTypeLifecycle, runner_type_key,
};
use contract_jobs::runner::WIRE_VERSION;

use super::wire;

pub async fn entry(fabric: &FabricTestNats, runner_type: &str) -> Option<PublishedRunnerType> {
    let reader = fabric.pl_reader::<PublishedRunnerType>().await;
    reader
        .get(&key(runner_type))
        .await
        .unwrap_or_else(|error| panic!("reading published runner type '{runner_type}': {error}"))
}

pub async fn wait_for_lifecycle(
    fabric: &FabricTestNats,
    runner_type: &str,
    lifecycle: RunnerTypeLifecycle,
    timeout: Duration,
) -> PublishedRunnerType {
    let arrived = wait_until(timeout, || async {
        entry(fabric, runner_type)
            .await
            .is_some_and(|published| published.lifecycle == lifecycle)
    })
    .await;
    assert!(
        arrived,
        "runner type '{runner_type}' never reached {lifecycle:?} in Published Language within {timeout:?}"
    );
    entry(fabric, runner_type)
        .await
        .expect("the entry whose lifecycle arrived remains readable")
}

pub async fn wait_until_absent(fabric: &FabricTestNats, runner_type: &str, timeout: Duration) {
    let absent = wait_until(timeout, || async {
        entry(fabric, runner_type).await.is_none()
    })
    .await;
    assert!(
        absent,
        "retired runner type '{runner_type}' remained in Published Language after {timeout:?}"
    );
}

pub async fn retract(fabric: &FabricTestNats, runner_type: &str) {
    let publisher = fabric.pl_publisher::<PublishedRunnerType>().await;
    publisher
        .retract(&key(runner_type))
        .await
        .unwrap_or_else(|error| {
            panic!("retracting published runner type '{runner_type}': {error}")
        });
}

pub async fn put(fabric: &FabricTestNats, runner_type: &str, lifecycle: RunnerTypeLifecycle) {
    let publisher = fabric.pl_publisher::<PublishedRunnerType>().await;
    publisher
        .put(
            &key(runner_type),
            &PublishedRunnerType {
                runner_type: runner_type.to_owned(),
                lifecycle,
                version: WIRE_VERSION,
            },
        )
        .await
        .unwrap_or_else(|error| {
            panic!("putting stale published runner type '{runner_type}': {error}")
        });
}

pub async fn make_unavailable(fabric: &FabricTestNats) {
    fabric.delete_published_language().await;
    assert!(
        !fabric.published_language_present().await,
        "the PUBLISHED_LANGUAGE bucket remains available after deletion",
    );
}

pub async fn restore_empty_bucket(fabric: &FabricTestNats) {
    let attached = FabricTestNats::connect(&fabric.url())
        .await
        .with_published_language()
        .await;
    attached.shutdown().await;
    assert!(
        fabric.published_language_present().await,
        "the declared PUBLISHED_LANGUAGE bucket was not restored",
    );
}

fn key(runner_type: &str) -> KvKey {
    let published = runner_type_key(runner_type);
    assert_eq!(
        published,
        wire::runner_type_catalog_key(runner_type),
        "the Published Language key is a frozen offer, not an implementation detail: every \
         consumer hand-builds '{}{{runner_type}}' to read this catalog, so a renamed prefix \
         orphans all of them at once and no test that asks the contract for its own key would \
         ever notice",
        wire::RUNNER_TYPE_CATALOG_PREFIX,
    );
    KvKey::new(published).expect("the contract renders a valid KV key")
}
