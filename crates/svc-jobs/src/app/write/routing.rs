use std::collections::BTreeMap;

use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::environment::IdFactory;

use super::job_changes::JobChange;
use crate::db::fleet::load;
use crate::error::ServiceError;

pub struct RoutedRunnerType<'a> {
    pub(super) key: &'a RunnerTypeKey,
    pub(super) lifecycle: Option<RunnerTypeLifecycle>,
}

pub(super) async fn resolve_runner_type_routes<'a>(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    changes: &'a [JobChange],
) -> Result<Vec<RoutedRunnerType<'a>>, ServiceError> {
    let mut claimed: BTreeMap<&str, &RunnerTypeKey> = BTreeMap::new();
    for change in changes {
        for event in &change.events {
            if let JobEvent::JobQueued(fact) = event {
                claimed.insert(fact.runner_type.as_str(), &fact.runner_type);
            }
        }
    }
    let mut routed = Vec::with_capacity(claimed.len());
    for key in claimed.into_values() {
        let route = load::upsert_route(&mut *tx, ids.next(), key).await?;
        routed.push(RoutedRunnerType {
            key,
            lifecycle: load::registered_lifecycle(&mut *tx, route).await?,
        });
    }
    Ok(routed)
}
