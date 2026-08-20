use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::ids::{EventId, RunnerTypeId};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::event::fleet::{FleetEvent, RunnerTypeAffordancesChanged};
use chrono::{DateTime, Utc};
use sqlx::Row;

use bc_jobs::ports::environment::{Clock, IdFactory};

use super::service_metadata;
use crate::db::{PgStore, apply};
use crate::error::ServiceError;

const IMPACT_BATCH: usize = 100;

pub async fn sweep(
    store: &PgStore,
    ids: &dyn IdFactory,
    clock: &dyn Clock,
) -> Result<(), ServiceError> {
    for _ in 0..IMPACT_BATCH {
        if !emit_one_due(store, ids, clock).await? {
            break;
        }
    }
    Ok(())
}

async fn emit_one_due(
    store: &PgStore,
    ids: &dyn IdFactory,
    clock: &dyn Clock,
) -> Result<bool, ServiceError> {
    let at = clock.now();
    let mut tx = store.begin().await?;
    let row = sqlx::query(
        "SELECT impact.runner_type_id::uuid AS runner_type_id, rt.type_key, impact.eligible_at \
         FROM runner_type_affordance_impacts impact \
         JOIN registered_runner_types registered \
              ON registered.runner_type_id = impact.runner_type_id \
         JOIN runner_types rt ON rt.id = impact.runner_type_id \
         WHERE impact.emitted_at IS NULL AND impact.eligible_at <= $1 \
         ORDER BY impact.eligible_at, impact.runner_type_id \
         LIMIT 1",
    )
    .bind(at)
    .fetch_optional(&mut *tx)
    .await
    .map_err(crate::db::hydrate::unavailable)?;
    let Some(row) = row else {
        return Ok(false);
    };
    let id = RunnerTypeId::new(row.get("runner_type_id"))?;
    let key = RunnerTypeKey::new(row.get::<String, _>("type_key"))?;
    let eligible_at: DateTime<Utc> = row.get("eligible_at");
    // Terminal recording takes the canonical route row before inserting its impact. The sweeper
    // uses that same order, then locks the exact impact, so neither path can deadlock and every
    // aggregate version is appended under one shared serialization point across pods.
    sqlx::query("SELECT id FROM runner_types WHERE id = $1 FOR UPDATE")
        .bind(id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(crate::db::hydrate::unavailable)?;
    let still_due: Option<bool> = sqlx::query_scalar(
        "SELECT emitted_at IS NULL AND eligible_at <= $3 \
         FROM runner_type_affordance_impacts \
         WHERE runner_type_id = $1 AND eligible_at = $2 FOR UPDATE",
    )
    .bind(id.as_uuid())
    .bind(eligible_at)
    .bind(at)
    .fetch_optional(&mut *tx)
    .await
    .map_err(crate::db::hydrate::unavailable)?;
    if still_due != Some(true) {
        tx.commit().await.map_err(crate::db::hydrate::unavailable)?;
        return Ok(true);
    }
    let known = PgStore::load_fleet_in(&mut tx, &key)
        .await?
        .ok_or(ServiceError::Contended)?;
    let facts = PgStore::runner_type_decision_facts_in(&mut tx, &key, at).await?;
    if known.lifecycle() == RunnerTypeLifecycle::Deprecated && known.guard_retire(facts).is_ok() {
        let event = FleetEvent::RunnerTypeAffordancesChanged(RunnerTypeAffordancesChanged {
            runner_type_id: id,
            runner_type: key,
        });
        apply::apply_fleet_events(
            &mut tx,
            id,
            &[(EventId::new(ids.next())?, event)],
            &service_metadata(),
            at,
        )
        .await?;
    }
    sqlx::query(
        "UPDATE runner_type_affordance_impacts SET emitted_at = $3 \
         WHERE runner_type_id = $1 AND eligible_at = $2 AND emitted_at IS NULL",
    )
    .bind(id.as_uuid())
    .bind(eligible_at)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(crate::db::hydrate::unavailable)?;
    tx.commit().await.map_err(crate::db::hydrate::unavailable)?;
    Ok(true)
}
