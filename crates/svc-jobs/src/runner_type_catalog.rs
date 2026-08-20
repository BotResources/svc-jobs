use std::collections::BTreeMap;

use async_trait::async_trait;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::RunnerTypeCatalogWriter;
use br_util_nats_fabric::{Fabric, KvKey, KvPrefix, PublishedLanguagePublisher};
use contract_jobs::catalog::{
    PublishedRunnerType, RUNNER_TYPE_PREFIX, RunnerTypeLifecycle as PublishedLifecycle,
    runner_type_key,
};

use crate::db::PgStore;
use crate::supervision::Supervisor;

const CATALOG_DEPENDENCY: &str = "runner type Published Language catalog";
const CATALOG_LOCK: &str = "jobs.runner_type.catalog.v1";

pub struct PublishedRunnerTypeCatalog {
    publisher: PublishedLanguagePublisher<PublishedRunnerType>,
    store: PgStore,
    supervisor: Supervisor,
}

impl PublishedRunnerTypeCatalog {
    pub async fn open(
        fabric: &Fabric,
        store: PgStore,
        supervisor: Supervisor,
    ) -> Result<Self, PortError> {
        Ok(Self {
            publisher: PublishedLanguagePublisher::open(fabric)
                .await
                .map_err(unavailable)?,
            store,
            supervisor,
        })
    }

    async fn lock(tx: &mut sqlx::PgConnection) -> Result<(), PortError> {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(CATALOG_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    async fn project_locked(&self, key: &RunnerTypeKey) -> Result<(), PortError> {
        let mut tx = self.store.begin().await?;
        Self::lock(&mut tx).await?;
        if let Some(runner_type) = PgStore::load_fleet_in(&mut tx, key).await? {
            self.publish(&runner_type).await?;
        }
        tx.commit().await.map_err(unavailable)
    }

    async fn reconcile_locked(&self) -> Result<(), PortError> {
        let mut tx = self.store.begin().await?;
        Self::lock(&mut tx).await?;
        let runner_types = PgStore::load_all_fleet_in(&mut tx).await?;
        let mut desired = BTreeMap::new();
        for runner_type in &runner_types {
            if let Some(value) = published(runner_type) {
                desired.insert(key_of(runner_type)?, value);
            }
        }
        let prefix = KvPrefix::new(RUNNER_TYPE_PREFIX).map_err(unavailable)?;
        self.publisher
            .repair_drift(&prefix, &desired)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }

    async fn publish(&self, runner_type: &RunnerType) -> Result<(), PortError> {
        let key = key_of(runner_type)?;
        match published(runner_type) {
            Some(value) => self.publisher.put(&key, &value).await.map_err(unavailable),
            None => self.publisher.retract(&key).await.map_err(unavailable),
        }
    }
}

#[async_trait]
impl RunnerTypeCatalogWriter for PublishedRunnerTypeCatalog {
    async fn project_current(&self, key: &RunnerTypeKey) -> Result<(), PortError> {
        let outcome = self.project_locked(key).await;
        if outcome.is_err() {
            self.supervisor.dependency_down(CATALOG_DEPENDENCY);
        }
        outcome
    }

    async fn reconcile(&self) -> Result<(), PortError> {
        let outcome = self.reconcile_locked().await;
        if outcome.is_ok() {
            self.supervisor.dependency_up(CATALOG_DEPENDENCY);
        } else {
            self.supervisor.dependency_down(CATALOG_DEPENDENCY);
        }
        outcome
    }
}

fn key_of(runner_type: &RunnerType) -> Result<KvKey, PortError> {
    KvKey::new(runner_type_key(runner_type.key().as_str())).map_err(unavailable)
}

fn published(runner_type: &RunnerType) -> Option<PublishedRunnerType> {
    let lifecycle = match runner_type.lifecycle() {
        RunnerTypeLifecycle::Active => PublishedLifecycle::Active,
        RunnerTypeLifecycle::Deprecated => PublishedLifecycle::Deprecated,
        RunnerTypeLifecycle::Retired => return None,
    };
    Some(PublishedRunnerType {
        runner_type: runner_type.key().as_str().to_owned(),
        lifecycle,
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
    use bc_jobs::domain::fleet::{RunnerType, RunnerTypeState};
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
    fn retired_types_have_no_published_entry() {
        assert!(published(&runner_type(RunnerTypeLifecycle::Retired)).is_none());
    }

    #[test]
    fn deprecated_types_remain_published_with_their_warning_lifecycle() {
        assert_eq!(
            published(&runner_type(RunnerTypeLifecycle::Deprecated))
                .unwrap()
                .lifecycle,
            PublishedLifecycle::Deprecated
        );
    }
}
