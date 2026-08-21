use bc_jobs::commands::fleet::became_retirable;
use bc_jobs::domain::actions::fleet::RetirementWindow;
use bc_jobs::domain::ids::EventId;
use bc_jobs::ports::environment::{Clock, IdFactory};
use chrono::TimeDelta;

use super::service_metadata;
use crate::db::{PgStore, apply, impacts};
use crate::error::ServiceError;

const IMPACT_BATCH: usize = 100;

pub async fn sweep(
    store: &PgStore,
    ids: &dyn IdFactory,
    clock: &dyn Clock,
    quiet_period: TimeDelta,
) -> Result<(), ServiceError> {
    for _ in 0..IMPACT_BATCH {
        if !emit_one_due(store, ids, clock, quiet_period).await? {
            break;
        }
    }
    Ok(())
}

async fn emit_one_due(
    store: &PgStore,
    ids: &dyn IdFactory,
    clock: &dyn Clock,
    quiet_period: TimeDelta,
) -> Result<bool, ServiceError> {
    let at = clock.now();
    let mut tx = store.begin().await?;
    let Some(due) = impacts::next_due(&mut tx, at, quiet_period).await? else {
        return Ok(false);
    };
    if impacts::consume_due(&mut tx, &due, at, quiet_period).await? == 0 {
        tx.commit().await?;
        return Ok(true);
    }
    let known = PgStore::load_fleet_in(&mut tx, &due.runner_type)
        .await?
        .ok_or(ServiceError::Contended)?;
    let window = RetirementWindow {
        evaluated_at: at,
        quiet_period,
    };
    let facts = PgStore::runner_type_decision_facts_in(&mut tx, &due.runner_type, window).await?;
    let mut recorded = Vec::new();
    for event in became_retirable(&known, facts).events {
        recorded.push((EventId::new(ids.next())?, event));
    }
    apply::apply_fleet_events(
        &mut tx,
        due.runner_type_id,
        &recorded,
        &service_metadata(),
        at,
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}
