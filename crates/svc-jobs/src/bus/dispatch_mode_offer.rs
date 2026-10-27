//! The `jobs.dispatch_mode.{runner_type}` offer: how the runs of each
//! RunnerType reach its Instances, so a runner learns whether to consume its
//! trigger stream or to claim without asking.
//!
//! The entry exists for every RunnerType that accepts Jobs and is retracted
//! when the type retires. The ledger is the truth: the entry is written after
//! the commit that switched the mode, and a boot and periodic reconciliation
//! repairs any entry a failed write or a lost pod left behind — the fabric's
//! outbox stages events, not Key-Value puts.

use std::collections::BTreeMap;

use async_trait::async_trait;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::dispatch_mode::DispatchMode;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::DispatchModeOfferWriter;
use br_util_nats_fabric::{Fabric, KvKey, KvPrefix, PublishedLanguagePublisher};
use contract_jobs::dispatch_mode::{
    DISPATCH_MODE_PREFIX, DispatchMode as PublishedMode, RunnerTypeDispatchMode, dispatch_mode_key,
};
use contract_jobs::runner::WIRE_VERSION;

use crate::supervision::Supervisor;

const OFFER_DEPENDENCY: &str = "runner type dispatch mode Published Language offer";

pub struct PublishedDispatchModes {
    publisher: PublishedLanguagePublisher<RunnerTypeDispatchMode>,
    supervisor: Supervisor,
}

impl PublishedDispatchModes {
    pub async fn open(fabric: &Fabric, supervisor: Supervisor) -> Result<Self, PortError> {
        Ok(Self {
            publisher: PublishedLanguagePublisher::open(fabric)
                .await
                .map_err(unavailable)?,
            supervisor,
        })
    }

    async fn publish(&self, runner_type: &RunnerType) -> Result<(), PortError> {
        let key = key_of(runner_type)?;
        match entry(runner_type) {
            Some(value) => self.publisher.put(&key, &value).await.map_err(unavailable),
            None => self.publisher.retract(&key).await.map_err(unavailable),
        }
    }

    async fn repair_drift(&self, runner_types: &[RunnerType]) -> Result<(), PortError> {
        let mut desired = BTreeMap::new();
        for runner_type in runner_types {
            if let Some(value) = entry(runner_type) {
                desired.insert(key_of(runner_type)?, value);
            }
        }
        let prefix = KvPrefix::new(DISPATCH_MODE_PREFIX).map_err(unavailable)?;
        self.publisher
            .repair_drift(&prefix, &desired)
            .await
            .map_err(unavailable)
    }
}

#[async_trait]
impl DispatchModeOfferWriter for PublishedDispatchModes {
    async fn project_current(&self, runner_type: &RunnerType) -> Result<(), PortError> {
        let outcome = self.publish(runner_type).await;
        if outcome.is_err() {
            self.supervisor.mark_down(OFFER_DEPENDENCY);
        }
        outcome
    }

    async fn reconcile(&self, runner_types: &[RunnerType]) -> Result<(), PortError> {
        let outcome = self.repair_drift(runner_types).await;
        match outcome {
            Ok(()) => self.supervisor.mark_up(OFFER_DEPENDENCY),
            Err(_) => self.supervisor.mark_down(OFFER_DEPENDENCY),
        }
        outcome
    }
}

fn key_of(runner_type: &RunnerType) -> Result<KvKey, PortError> {
    KvKey::new(dispatch_mode_key(runner_type.key().as_str())).map_err(unavailable)
}

/// The entry of a type the domain offers — one that accepts Jobs — or none.
fn entry(runner_type: &RunnerType) -> Option<RunnerTypeDispatchMode> {
    runner_type.published_lifecycle()?;
    Some(RunnerTypeDispatchMode {
        version: WIRE_VERSION,
        runner_type: runner_type.key().as_str().to_owned(),
        dispatch_mode: match runner_type.dispatch_mode() {
            DispatchMode::Push => PublishedMode::Push,
            DispatchMode::Claim => PublishedMode::Claim,
        },
    })
}

fn unavailable(error: impl std::fmt::Display) -> PortError {
    PortError::Unavailable {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bc_jobs::domain::fleet::RunnerTypeState;
    use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
    use bc_jobs::domain::fleet::settings::RunnerTypeSettings;
    use bc_jobs::domain::ids::RunnerTypeId;
    use bc_jobs::domain::keys::RunnerTypeKey;
    use chrono::DateTime;
    use uuid::Uuid;

    fn runner_type(lifecycle: RunnerTypeLifecycle, dispatch_mode: DispatchMode) -> RunnerType {
        RunnerType::hydrate(RunnerTypeState {
            id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            key: RunnerTypeKey::new("analyst").unwrap(),
            registered_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            lifecycle,
            settings: RunnerTypeSettings {
                dispatch_mode,
                ..RunnerTypeSettings::default()
            },
            instances: vec![],
        })
        .unwrap()
    }

    #[test]
    fn an_offered_type_publishes_its_current_mode() {
        let published = entry(&runner_type(
            RunnerTypeLifecycle::Active,
            DispatchMode::Claim,
        ));
        assert_eq!(
            published.map(|entry| entry.dispatch_mode),
            Some(PublishedMode::Claim)
        );
    }

    #[test]
    fn a_retired_type_publishes_no_mode() {
        assert!(
            entry(&runner_type(
                RunnerTypeLifecycle::Retired,
                DispatchMode::Push
            ))
            .is_none()
        );
    }
}
