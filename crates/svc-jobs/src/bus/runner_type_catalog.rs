use std::collections::BTreeMap;

use async_trait::async_trait;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::RunnerTypeCatalogWriter;
use br_util_nats_fabric::{Fabric, KvKey, KvPrefix, PublishedLanguagePublisher};
use contract_jobs::catalog::{
    RUNNER_TYPE_PREFIX, RunnerType as PublishedRunnerType,
    RunnerTypeLifecycle as PublishedLifecycle, runner_type_key,
};
use contract_jobs::runner::WIRE_VERSION;

use crate::supervision::Supervisor;

const CATALOG_DEPENDENCY: &str = "runner type Published Language catalog";

pub struct PublishedRunnerTypeCatalog {
    publisher: PublishedLanguagePublisher<PublishedRunnerType>,
    supervisor: Supervisor,
}

impl PublishedRunnerTypeCatalog {
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
        match entry(runner_type)? {
            Some(value) => self.publisher.put(&key, &value).await.map_err(unavailable),
            None => self.publisher.retract(&key).await.map_err(unavailable),
        }
    }

    async fn repair_drift(&self, runner_types: &[RunnerType]) -> Result<(), PortError> {
        let mut desired = BTreeMap::new();
        for runner_type in runner_types {
            if let Some(value) = entry(runner_type)? {
                desired.insert(key_of(runner_type)?, value);
            }
        }
        let prefix = KvPrefix::new(RUNNER_TYPE_PREFIX).map_err(unavailable)?;
        self.publisher
            .repair_drift(&prefix, &desired)
            .await
            .map_err(unavailable)
    }
}

#[async_trait]
impl RunnerTypeCatalogWriter for PublishedRunnerTypeCatalog {
    async fn project_current(&self, runner_type: &RunnerType) -> Result<(), PortError> {
        let outcome = self.publish(runner_type).await;
        if outcome.is_err() {
            self.supervisor.mark_down(CATALOG_DEPENDENCY);
        }
        outcome
    }

    async fn reconcile(&self, runner_types: &[RunnerType]) -> Result<(), PortError> {
        let outcome = self.repair_drift(runner_types).await;
        match outcome {
            Ok(()) => self.supervisor.mark_up(CATALOG_DEPENDENCY),
            Err(_) => self.supervisor.mark_down(CATALOG_DEPENDENCY),
        }
        outcome
    }
}

fn key_of(runner_type: &RunnerType) -> Result<KvKey, PortError> {
    KvKey::new(runner_type_key(runner_type.key().as_str())).map_err(unavailable)
}

fn entry(runner_type: &RunnerType) -> Result<Option<PublishedRunnerType>, PortError> {
    let Some(lifecycle) = runner_type.published_lifecycle() else {
        return Ok(None);
    };
    let lifecycle = match lifecycle {
        RunnerTypeLifecycle::Active => PublishedLifecycle::Active,
        RunnerTypeLifecycle::Deprecated => PublishedLifecycle::Deprecated,
        RunnerTypeLifecycle::Retired => {
            return Err(bc_jobs::JobsError::CorruptState {
                reason_code: "retired_runner_type_offered_for_publication",
            }
            .into());
        }
    };
    Ok(Some(PublishedRunnerType {
        runner_type: runner_type.key().as_str().to_owned(),
        lifecycle,
        version: WIRE_VERSION,
    }))
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
    use bc_jobs::domain::ids::RunnerTypeId;
    use bc_jobs::domain::keys::RunnerTypeKey;
    use chrono::DateTime;
    use uuid::Uuid;

    fn runner_type(lifecycle: RunnerTypeLifecycle) -> RunnerType {
        RunnerType::hydrate(RunnerTypeState {
            id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            key: RunnerTypeKey::new("analyst").unwrap(),
            registered_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            lifecycle,
            instances: vec![],
        })
        .unwrap()
    }

    #[test]
    fn a_type_the_domain_does_not_publish_has_no_entry() {
        assert!(
            entry(&runner_type(RunnerTypeLifecycle::Retired))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_published_type_carries_the_lifecycle_the_domain_decided() {
        assert_eq!(
            entry(&runner_type(RunnerTypeLifecycle::Deprecated))
                .unwrap()
                .unwrap()
                .lifecycle,
            PublishedLifecycle::Deprecated
        );
    }
}
